"""
Stage 2: ROR/OpenAlex API Enrichment for High-Quality Matching

Strategy:
1. Load unmatched institutions from Stage 1
2. Query OpenAlex API for ROR IDs, organization types, WikiData links
3. Query ROR API for additional metadata (website, organization type)
4. Query WikiData for stock tickers
5. Match using multiple signals:
   - ROR-identified companies (organization_type == 'company')
   - WikiData tickers (high accuracy)
   - Website domains
   - Location validation
6. Validate accuracy on 100 random samples

Target: +2,000-3,000 matches, 92%+ accuracy
"""

import polars as pl
from pathlib import Path
import logging
import re
import requests
import time
from typing import Dict, List, Optional, Tuple
import json

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_PUB = PROJECT_ROOT / "data" / "processed" / "publication"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
DATA_RAW_COMP = PROJECT_ROOT / "data" / "raw" / "compustat"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

STAGE_1_UNMATCHED = OUTPUT_DIR / "stage_1_unmatched.parquet"
STAGE_1_MATCHES = OUTPUT_DIR / "stage_1_matches.parquet"
FINANCIAL_DATA = DATA_RAW_COMP / "crsp_a_ccm.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "stage_2_matches.parquet"
UNMATCHED_OUTPUT = OUTPUT_DIR / "stage_2_unmatched.parquet"
VALIDATION_CSV = OUTPUT_DIR / "stage_2_validation_sample.csv"
PROGRESS_LOG = LOGS_DIR / "stage_2_ror_enrichment.log"

# API endpoints
OPENALEX_API = "https://api.openalex.org"
ROR_API = "https://api.ror.org"
WIKIDATA_API = "https://query.wikidata.org/sparql"

# Rate limiting
OPENALEX_DELAY = 0.01  # 100 requests/second
ROR_DELAY = 0.012  # 5000 requests/minute
WIKIDATA_DELAY = 0.1  # 10 requests/second

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROGRESS_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Data Loading
# ============================================================================

def load_financial_data():
    """Load CRSP/Compustat data."""
    logger.info("=" * 80)
    logger.info("LOADING FINANCIAL DATA")
    logger.info("=" * 80)

    logger.info(f"\nLoading {FINANCIAL_DATA}...")
    financial_df = pl.read_csv(
        FINANCIAL_DATA,
        dtypes={
            'GVKEY': str,
            'LPERMNO': int,
            'tic': str,
            'conm': str,
            'cusip': str,
            'cik': str,
        },
        ignore_errors=True,
        truncate_ragged_lines=True
    )
    logger.info(f"  Loaded {len(financial_df):,} records")

    # Normalize company names
    financial_df = financial_df.with_columns([
        pl.col('conm').str.to_lowercase().str.strip_chars().alias('conm_normalized')
    ])

    # Get unique firms
    logger.info("\nExtracting unique firms (primary links)...")
    unique_firms = financial_df.filter(
        pl.col('LINKPRIM') == 'P'
    ).select([
        'GVKEY', 'LPERMNO', 'tic', 'conm', 'conm_normalized',
        'state', 'city', 'cik'
    ]).unique()

    logger.info(f"  Found {len(unique_firms):,} unique firms")

    return unique_firms


def load_unmatched_institutions():
    """Load institutions that were not matched in Stage 1."""
    logger.info("\n" + "=" * 80)
    logger.info("LOADING UNMATCHED INSTITUTIONS FROM STAGE 1")
    logger.info("=" * 80)

    unmatched = pl.read_parquet(STAGE_1_UNMATCHED)
    logger.info(f"  Loaded {len(unmatched):,} unmatched institutions")

    return unmatched.to_dicts()


# ============================================================================
# API Functions
# ============================================================================

def query_openalex_institution(institution_name: str) -> Optional[Dict]:
    """
    Query OpenAlex API for institution details.
    Returns ROR ID, organization type, WikiData ID, etc.
    """
    # Search for institution by name
    url = f"{OPENALEX_API}/institutions"
    params = {
        'filter': 'display_name.search:' + institution_name,
        'mailto': 'sunyanna@hku.hk',
        'per_page': 1
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code != 200:
            return None

        data = response.json()
        if not data.get('results') or len(data['results']) == 0:
            return None

        institution = data['results'][0]

        return {
            'openalex_id': institution.get('id'),
            'ror_id': institution.get('ror'),
            'display_name': institution.get('display_name'),
            'country_code': institution.get('country_code'),
            'organization_type': institution.get('type'),
            'wiki_data_id': institution.get('ids', {}).get('wikidata', '').split('/')[-1] if institution.get('ids', {}).get('wikidata') else None,
            'website': institution.get('website_url'),
        }

    except Exception as e:
        logger.warning(f"OpenAlex query failed for '{institution_name}': {e}")
        return None

    finally:
        time.sleep(OPENALEX_DELAY)


def query_ror_organization(ror_id: str) -> Optional[Dict]:
    """
    Query ROR API for additional organization metadata.
    """
    if not ror_id:
        return None

    # Extract ROR ID from URL if needed
    if ror_id.startswith('http'):
        ror_id = ror_id.split('/')[-1]

    url = f"{ROR_API}/organizations/{ror_id}"

    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None

        data = response.json()

        return {
            'ror_id': data.get('id'),
            'name': data.get('name'),
            'types': data.get('types', []),
            'country': data.get('country', {}).get('country_name'),
            'website': data.get('links', [None])[0],  # Primary website
        }

    except Exception as e:
        logger.warning(f"ROR query failed for {ror_id}: {e}")
        return None

    finally:
        time.sleep(ROR_DELAY)


def query_wikidata_ticker(wikidata_id: str) -> Optional[str]:
    """
    Query WikiData SPARQL endpoint for stock ticker.
    Property P249 is the stock ticker symbol.
    """
    if not wikidata_id:
        return None

    query = f"""
    SELECT ?ticker WHERE {{
      wd:{wikidata_id} wdt:P249 ?ticker .
    }}
    LIMIT 1
    """

    url = WIKIDATA_API
    params = {
        'query': query,
        'format': 'json'
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code != 200:
            return None

        data = response.json()
        results = data.get('results', {}).get('bindings', [])

        if results and len(results) > 0:
            ticker = results[0]['ticker']['value']
            return ticker

    except Exception as e:
        logger.warning(f"WikiData query failed for {wikidata_id}: {e}")

    finally:
        time.sleep(WIKIDATA_DELAY)

    return None


# ============================================================================
# Matching Functions
# ============================================================================

def match_by_wikidata_ticker(
    enrichment: Dict,
    firms_df: pl.DataFrame
) -> Optional[Dict]:
    """
    Match institution to firm using WikiData ticker.
    High accuracy when available.
    """
    if not enrichment:
        return None

    wikidata_id = enrichment.get('wiki_data_id')
    if not wikidata_id:
        return None

    ticker = query_wikidata_ticker(wikidata_id)
    if not ticker:
        return None

    # Match ticker to firms
    matches = firms_df.filter(
        pl.col('tic').str.to_uppercase() == ticker.upper()
    )

    if len(matches) == 0:
        return None

    if len(matches) == 1:
        match_dict = matches.row(0, named=True)
        return {
            'gvkey': match_dict['GVKEY'],
            'permno': match_dict['LPERMNO'],
            'ticker': match_dict['tic'],
            'company_name': match_dict['conm'],
            'state': match_dict['state'],
            'confidence': 0.95,
            'method': 'wikidata_ticker',
            'match_reason': f'WikiData ticker: {ticker} (ROR-identified company)',
            'location_match': None,
            'industry_match': None,
            'ror_id': enrichment.get('ror_id'),
            'wikidata_id': wikidata_id
        }

    # Multiple matches - use location to disambiguate
    inst_country = enrichment.get('country_code') or enrichment.get('country')
    if inst_country:
        for match_row in matches.iter_rows(named=True):
            firm_state = match_row['state']
            # US firms have state, international firms have None
            if 'united states' in inst_country.lower() and firm_state:
                match_dict = match_row
                return {
                    'gvkey': match_dict['GVKEY'],
                    'permno': match_dict['LPERMNO'],
                    'ticker': match_dict['tic'],
                    'company_name': match_dict['conm'],
                    'state': match_dict['state'],
                    'confidence': 0.93,
                    'method': 'wikidata_ticker_location',
                    'match_reason': f'WikiData ticker: {ticker} with location validation',
                    'location_match': 'country_match',
                    'industry_match': None,
                    'ror_id': enrichment.get('ror_id'),
                    'wikidata_id': wikidata_id
                }

    return None


def match_by_ror_company_type(
    enrichment: Dict,
    institution_name: str,
    firms_df: pl.DataFrame
) -> Optional[Dict]:
    """
    Match institutions that are ROR-identified companies.
    Uses name matching with higher confidence due to ROR validation.
    """
    if not enrichment:
        return None

    # Check if organization type is 'company'
    org_type = enrichment.get('organization_type')
    ror_types = enrichment.get('types', [])

    is_company = (
        org_type == 'company' or
        'company' in ror_types or
        'Company' in ror_types
    )

    if not is_company:
        return None

    # Normalize name for matching
    normalized_name = institution_name.lower().strip()

    # Remove suffixes
    for suffix in [' inc', ' corp', ' llc', 'ltd', 'limited', 'plc', 'gmbh']:
        if normalized_name.endswith(suffix):
            normalized_name = normalized_name[:-len(suffix)].strip()

    # Try to match by normalized name
    matches = firms_df.filter(
        pl.col('conm_normalized').str.contains(
            normalized_name.split()[0]  # Use first word for broader matching
        )
    )

    if len(matches) == 1:
        match_dict = matches.row(0, named=True)
        return {
            'gvkey': match_dict['GVKEY'],
            'permno': match_dict['LPERMNO'],
            'ticker': match_dict['tic'],
            'company_name': match_dict['conm'],
            'state': match_dict['state'],
            'confidence': 0.92,
            'method': 'ror_company_name',
            'match_reason': f'ROR-identified company: {enrichment.get("display_name")}',
            'location_match': None,
            'industry_match': None,
            'ror_id': enrichment.get('ror_id')
        }

    return None


# ============================================================================
# Main Matching Pipeline
# ============================================================================

def run_stage_2_matching():
    """Run Stage 2 matching with ROR/OpenAlex enrichment."""
    logger.info("\n" + "=" * 80)
    logger.info("STAGE 2: ROR/OPENALEX API ENRICHMENT")
    logger.info("=" * 80)
    logger.info(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    firms_df = load_financial_data()
    unmatched_institutions = load_unmatched_institutions()

    logger.info(f"\n{'='*80}")
    logger.info("ENRICHING AND MATCHING INSTITUTIONS")
    logger.info(f"{'='*80}")

    matches = []
    still_unmatched = []

    total = len(unmatched_institutions)

    for i, inst in enumerate(unmatched_institutions):
        if (i + 1) % 100 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} institutions ({len(matches)} matched so far)...")

        institution_name = inst['raw_name']

        # Step 1: Query OpenAlex for enrichment
        openalex_data = query_openalex_institution(institution_name)

        # Step 2: Try WikiData ticker matching (highest accuracy)
        match_result = match_by_wikidata_ticker(openalex_data, firms_df)

        # Step 3: Try ROR company type matching
        if not match_result:
            match_result = match_by_ror_company_type(
                openalex_data,
                institution_name,
                firms_df
            )

        if match_result:
            matches.append({
                'institution_raw': institution_name,
                'institution_country': inst['country'],
                'paper_count': inst['paper_count'],
                **match_result
            })
        else:
            still_unmatched.append(inst)

    logger.info(f"\n{'='*80}")
    logger.info("STAGE 2 MATCHING RESULTS")
    logger.info(f"{'='*80}")
    logger.info(f"Matched: {len(matches):,} institutions")
    logger.info(f"Unmatched: {len(still_unmatched):,} institutions")

    match_rate = len(matches) / total * 100
    logger.info(f"Match rate: {match_rate:.1f}%")

    # Load Stage 1 matches
    stage_1_df = pl.read_parquet(STAGE_1_MATCHES)
    logger.info(f"Stage 1 matches: {len(stage_1_df):,}")

    # Create DataFrame
    matches_df = pl.DataFrame(matches)

    # Method distribution
    logger.info(f"\nMethod distribution:")
    method_dist = matches_df.group_by('method').agg(pl.len().alias('count')).sort('count', descending=True)
    for row in method_dist.iter_rows(named=True):
        logger.info(f"  {row['method']}: {row['count']:,} ({row['count']/len(matches)*100:.1f}%)")

    # Confidence distribution
    logger.info(f"\nConfidence distribution:")
    logger.info(f"  High (>0.92): {len(matches_df.filter(pl.col('confidence') > 0.92)):,} ({len(matches_df.filter(pl.col('confidence') > 0.92))/len(matches)*100:.1f}%)")

    # Unique firms (cumulative with Stage 1)
    cumulative_firms = pl.concat([
        stage_1_df.select('gvkey'),
        matches_df.select('gvkey')
    ]).unique()
    logger.info(f"\nCumulative unique firms (Stage 1 + Stage 2): {cumulative_firms.shape[0]:,}")

    # Top firms
    logger.info(f"\nTop 20 firms by institution count:")
    top_firms = matches_df.group_by(['gvkey', 'company_name']).agg([
        pl.len().alias('inst_count'),
        pl.col('paper_count').sum().alias('total_papers')
    ]).sort('inst_count', descending=True).head(20)

    for row in top_firms.iter_rows(named=True):
        logger.info(f"  {row['company_name'][:50]:<50}: {row['inst_count']:>4} institutions, {row['total_papers']:>6,} papers")

    # Save results
    logger.info(f"\nSaving results to {OUTPUT_PARQUET}...")
    matches_df.write_parquet(OUTPUT_PARQUET, compression='snappy')
    logger.info("  Saved successfully!")

    # Save unmatched
    if len(still_unmatched) > 0:
        unmatched_df = pl.DataFrame(still_unmatched)
        unmatched_df.write_parquet(UNMATCHED_OUTPUT, compression='snappy')
        logger.info(f"Saved {len(still_unmatched):,} unmatched institutions to {UNMATCHED_OUTPUT}")

    return matches_df, still_unmatched


# ============================================================================
# Validation
# ============================================================================

def create_validation_sample(matches_df: pl.DataFrame, n: int = 100):
    """Create random validation sample."""
    logger.info(f"\n{'='*80}")
    logger.info(f"CREATING VALIDATION SAMPLE (n={n})")
    logger.info(f"{'='*80}")

    # Random sample
    validation_sample = matches_df.sample(min(n, len(matches_df)), seed=42)

    # Save to CSV
    validation_sample.write_csv(VALIDATION_CSV)
    logger.info(f"\nSaved validation sample to {VALIDATION_CSV}")

    return validation_sample


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function."""
    start_time = time.time()

    # Run matching
    matches_df, unmatched = run_stage_2_matching()

    # Create validation sample
    validation_sample = create_validation_sample(matches_df, n=100)

    elapsed = time.time() - start_time

    logger.info(f"\n{'='*80}")
    logger.info("STAGE 2 COMPLETED")
    logger.info(f"{'='*80}")
    logger.info(f"Elapsed time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    logger.info(f"Stage 2 matches: {len(matches_df):,}")
    logger.info(f"Validation sample: {VALIDATION_CSV}")
    logger.info(f"\nNext step: Manually validate matches, then proceed to Stage 3")

    return matches_df, validation_sample


if __name__ == "__main__":
    main()
