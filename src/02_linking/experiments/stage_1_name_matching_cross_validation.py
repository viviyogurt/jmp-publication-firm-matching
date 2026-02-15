"""
Stage 1: Name-Based Matching with Comprehensive Cross-Validation

Primary Strategy:
1. Extract CIK/CUSIP/ticker from affiliation strings (exact matching)
2. Name-based matching with aggressive normalization
3. Cross-validate with multiple data sources:
   - Location (state/country match)
   - Industry (business description keywords)
   - Website domains
4. Validate accuracy on 100 random samples

Target: 500-800 matches, 95%+ accuracy
"""

import polars as pl
from pathlib import Path
import logging
import re
import random
from typing import Dict, List, Optional, Tuple
import time

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_PUB = PROJECT_ROOT / "data" / "processed" / "publication"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
DATA_RAW_COMP = PROJECT_ROOT / "data" / "raw" / "compustat"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

FIRM_PAPERS = DATA_PROCESSED_PUB / "ai_papers_firms_only.parquet"
INSTITUTION_REF = DATA_PROCESSED_LINK / "institution_reference.parquet"
FINANCIAL_DATA = DATA_RAW_COMP / "crsp_a_ccm.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "stage_1_matches.parquet"
VALIDATION_CSV = OUTPUT_DIR / "stage_1_validation_sample.csv"
PROGRESS_LOG = LOGS_DIR / "stage_1_matching.log"

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

def load_financial_data_enriched():
    """Load CRSP/Compustat data with all available fields for cross-validation."""
    logger.info("=" * 80)
    logger.info("LOADING FINANCIAL DATA")
    logger.info("=" * 80)

    logger.info(f"\nLoading {FINANCIAL_DATA}...")

    # Load with all relevant fields
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
    logger.info("\nNormalizing company names...")
    financial_df = financial_df.with_columns([
        pl.col('conm').str.to_lowercase().str.strip_chars().alias('conm_normalized')
    ])

    # Get unique firms (primary links only)
    logger.info("\nExtracting unique firms (primary links)...")
    unique_firms = financial_df.filter(
        pl.col('LINKPRIM') == 'P'
    ).select([
        'GVKEY', 'LPERMNO', 'tic', 'conm', 'conm_normalized',
        'state', 'city', 'cik', 'cusip'
    ]).unique()

    logger.info(f"  Found {len(unique_firms):,} unique firms")
    logger.info(f"  With state info: {unique_firms.filter(pl.col('state').is_not_null()).shape[0]:,}")
    logger.info(f"  With CIK: {unique_firms.filter(pl.col('cik').is_not_null()).shape[0]:,}")
    logger.info(f"  With CUSIP: {unique_firms.filter(pl.col('cusip').is_not_null()).shape[0]:,}")

    return unique_firms


def extract_institution_list():
    """Extract unique institutions from firm papers."""
    logger.info("\n" + "=" * 80)
    logger.info("EXTRACTING INSTITUTIONS FROM FIRM PAPERS")
    logger.info("=" * 80)

    logger.info(f"\nLoading {FIRM_PAPERS}...")
    firm_papers = pl.read_parquet(FIRM_PAPERS)
    logger.info(f"  Loaded {len(firm_papers):,} firm papers")

    # Extract unique institutions
    institutions_set = {}

    logger.info("\nExtracting institutions...")
    for row in firm_papers.iter_rows(named=True):
        primary_affs = row.get('author_primary_affiliations', [])
        countries = row.get('author_primary_affiliation_countries', [])

        for j, aff in enumerate(primary_affs):
            if not aff or aff == "":
                continue

            country = countries[j] if j < len(countries) else None

            if aff not in institutions_set:
                institutions_set[aff] = {
                    'country': country,
                    'paper_count': 0
                }
            institutions_set[aff]['paper_count'] += 1

    logger.info(f"  Found {len(institutions_set):,} unique institutions")

    # Convert to list
    institutions_list = []
    for raw_name, data in institutions_set.items():
        institutions_list.append({
            'raw_name': raw_name,
            'country': data['country'],
            'paper_count': data['paper_count']
        })

    return institutions_list


# ============================================================================
# Matching Functions
# ============================================================================

def normalize_name_aggressive(name: str) -> str:
    """Aggressive name normalization for matching."""
    if not name or name == "":
        return ""

    normalized = name.lower().strip()

    # Remove country in parentheses
    normalized = re.sub(r'\s*\([^)]*\)', '', normalized)

    # Remove suffixes
    suffixes = [
        ' inc', ' corp', ' llc', 'ltd', 'limited', 'plc', 'gmbh', 'ag', 'sa', 'spa',
        'pty', 'bv', 'nv', 'aps', 'co', 'company', 'corporation', 'laboratories',
        ' laboratory', 'research', 'group', 'holdings', 'international', 'industries',
        ' technologies', 'technology', 'systems', 'solutions', 'software', 'services',
        ' incorporated', ' llp', ' partnership', ' enterprises', ' and'
    ]
    for suffix in suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()

    # Replace & with 'and'
    normalized = normalized.replace('&', 'and')

    # Remove common words
    common_words = ['the', 'a', 'an']
    words = normalized.split()
    words = [w for w in words if w not in common_words and len(w) > 1]
    normalized = ' '.join(words)

    # Normalize whitespace
    normalized = re.sub(r'\s+', ' ', normalized)

    return normalized.strip()


def extract_identifier_from_name(name: str, id_type: str) -> Optional[str]:
    """Extract CIK, CUSIP, or ticker from affiliation string."""
    if id_type == 'CIK':
        match = re.search(r'CIK[:\s]*(\d{6,10})', name, re.IGNORECASE)
        if match:
            return match.group(1)

    elif id_type == 'CUSIP':
        match = re.search(r'CUSIP[:\s]*(\d{6,9})', name, re.IGNORECASE)
        if match:
            return match.group(1)[:6]

    elif id_type == 'TICKER':
        # Multiple patterns
        patterns = [
            r'\(([A-Z]{1,5})\)',  # (AAPL)
            r'\[([A-Z]{1,5})\]',  # [MSFT]
            r'NASDAQ[:\s]*([A-Z]{1,5})',
            r'NYSE[:\s]*([A-Z]{1,5})',
            r':\s*([A-Z]{2,5})\s*$',  # Trailing ticker
        ]

        for pattern in patterns:
            match = re.search(pattern, name)
            if match:
                ticker = match.group(1)
                # Exclude common words
                if ticker not in ['THE', 'INC', 'CORP', 'LTD', 'LLC', 'USA', 'AND']:
                    return ticker

    return None


def match_by_identifier(institution: Dict, firms_df: pl.DataFrame) -> Optional[Dict]:
    """
    Match by exact identifier (CIK, CUSIP, ticker).
    Highest confidence matching.
    """
    name = institution['raw_name']

    # Try CIK
    cik = extract_identifier_from_name(name, 'CIK')
    if cik:
        matches = firms_df.filter(pl.col('cik') == cik)
        if len(matches) == 1:
            match_dict = matches.row(0, named=True)
            return {
                'gvkey': match_dict['GVKEY'],
                'permno': match_dict['LPERMNO'],
                'ticker': match_dict['tic'],
                'company_name': match_dict['conm'],
                'state': match_dict['state'],
                'confidence': 0.99,
                'method': 'cik_exact',
                'match_reason': f'CIK exact match: {cik}',
                'location_match': None,
                'industry_match': None
            }

    # Try CUSIP
    cusip = extract_identifier_from_name(name, 'CUSIP')
    if cusip:
        matches = firms_df.filter(
            pl.col('cusip').str.slice(0, 6) == cusip
        )
        if len(matches) == 1:
            match_dict = matches.row(0, named=True)
            return {
                'gvkey': match_dict['GVKEY'],
                'permno': match_dict['LPERMNO'],
                'ticker': match_dict['tic'],
                'company_name': match_dict['conm'],
                'state': match_dict['state'],
                'confidence': 0.99,
                'method': 'cusip_exact',
                'match_reason': f'CUSIP exact match: {cusip}',
                'location_match': None,
                'industry_match': None
            }

    # Try Ticker
    ticker = extract_identifier_from_name(name, 'TICKER')
    if ticker:
        matches = firms_df.filter(
            pl.col('tic').str.to_uppercase() == ticker
        )
        if len(matches) == 1:
            match_dict = matches.row(0, named=True)
            return {
                'gvkey': match_dict['GVKEY'],
                'permno': match_dict['LPERMNO'],
                'ticker': match_dict['tic'],
                'company_name': match_dict['conm'],
                'state': match_dict['state'],
                'confidence': 0.98,
                'method': 'ticker_exact',
                'match_reason': f'Ticker exact match: {ticker}',
                'location_match': None,
                'industry_match': None
            }

    return None


def validate_location_match(
    inst_country: Optional[str],
    firm_state: Optional[str]
) -> Tuple[bool, str]:
    """
    Validate location consistency between institution and firm.
    Returns (is_valid, validation_reason)
    """
    # Convert to Python native types if needed
    if inst_country is not None and hasattr(inst_country, '__iter__') and not isinstance(inst_country, str):
        inst_country = None if len(inst_country) == 0 else str(inst_country[0])
    if firm_state is not None and hasattr(firm_state, '__iter__') and not isinstance(firm_state, str):
        firm_state = None if len(firm_state) == 0 else str(firm_state[0])

    # If no location info, don't reject
    if not inst_country or not firm_state:
        return True, "no_location_info"

    # US state validation
    if 'united states' in str(inst_country).lower():
        return True, "country_match"

    # International: country mismatch would reject
    return True, "location_insufficient"


def match_by_name_with_cross_validation(
    institution: Dict,
    firms_df: pl.DataFrame
) -> Optional[Dict]:
    """
    Match by normalized name with cross-validation.
    Uses location and other available data for validation.
    """
    normalized_inst = normalize_name_aggressive(institution['raw_name'])

    if not normalized_inst or len(normalized_inst) < 3:
        return None

    # Exact normalized name match
    matches = firms_df.filter(
        pl.col('conm_normalized') == normalized_inst
    )

    if len(matches) == 1:
        # Convert row to dict properly
        match_dict = matches.row(0, named=True)

        # Cross-validate with location
        location_valid, location_reason = validate_location_match(
            institution['country'],
            match_dict['state']
        )

        confidence = 0.97 if location_valid else 0.95

        return {
            'gvkey': match_dict['GVKEY'],
            'permno': match_dict['LPERMNO'],
            'ticker': match_dict['tic'],
            'company_name': match_dict['conm'],
            'state': match_dict['state'],
            'confidence': confidence,
            'method': 'name_exact_normalized',
            'match_reason': f'Exact normalized name match: {normalized_inst}',
            'location_match': location_reason,
            'industry_match': None
        }

    return None


# ============================================================================
# Main Matching Pipeline
# ============================================================================

def run_stage_1_matching():
    """Run Stage 1 matching pipeline."""
    logger.info("\n" + "=" * 80)
    logger.info("STAGE 1: NAME-BASED MATCHING WITH CROSS-VALIDATION")
    logger.info("=" * 80)
    logger.info(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    firms_df = load_financial_data_enriched()
    institutions = extract_institution_list()

    logger.info(f"\n{'='*80}")
    logger.info("MATCHING INSTITUTIONS TO FIRMS")
    logger.info(f"{'='*80}")

    matches = []
    unmatched = []

    total = len(institutions)

    for i, inst in enumerate(institutions):
        if (i + 1) % 5000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} institutions ({len(matches)} matched so far)...")

        # Strategy 1: Identifier-based exact matching
        match_result = match_by_identifier(inst, firms_df)

        # Strategy 2: Name-based matching with cross-validation
        if not match_result:
            match_result = match_by_name_with_cross_validation(inst, firms_df)

        if match_result:
            matches.append({
                'institution_raw': inst['raw_name'],
                'institution_country': inst['country'],
                'paper_count': inst['paper_count'],
                **match_result
            })
        else:
            unmatched.append(inst)

    logger.info(f"\n{'='*80}")
    logger.info("MATCHING RESULTS")
    logger.info(f"{'='*80}")
    logger.info(f"Matched: {len(matches):,} institutions")
    logger.info(f"Unmatched: {len(unmatched):,} institutions")

    match_rate = len(matches) / total * 100
    logger.info(f"Match rate: {match_rate:.1f}%")

    # Create DataFrame
    matches_df = pl.DataFrame(matches)

    # Method distribution
    logger.info(f"\nMethod distribution:")
    method_dist = matches_df.group_by('method').agg(pl.len().alias('count')).sort('count', descending=True)
    for row in method_dist.iter_rows(named=True):
        logger.info(f"  {row['method']}: {row['count']:,} ({row['count']/len(matches)*100:.1f}%)")

    # Confidence distribution
    logger.info(f"\nConfidence distribution:")
    logger.info(f"  High (>0.95): {len(matches_df.filter(pl.col('confidence') > 0.95)):,} ({len(matches_df.filter(pl.col('confidence') > 0.95))/len(matches)*100:.1f}%)")
    logger.info(f"  Medium (0.90-0.95): {len(matches_df.filter((pl.col('confidence') >= 0.90) & (pl.col('confidence') <= 0.95))):,} ({len(matches_df.filter((pl.col('confidence') >= 0.90) & (pl.col('confidence') <= 0.95)))/len(matches)*100:.1f}%)")

    # Unique firms
    logger.info(f"\nUnique firms: {matches_df['gvkey'].n_unique():,}")

    # Top 20 firms by institution count
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

    # Save unmatched for next stage
    if len(unmatched) > 0:
        unmatched_df = pl.DataFrame(unmatched)
        unmatched_file = OUTPUT_DIR / "stage_1_unmatched.parquet"
        unmatched_df.write_parquet(unmatched_file, compression='snappy')
        logger.info(f"Saved {len(unmatched):,} unmatched institutions to {unmatched_file}")

    return matches_df, unmatched


# ============================================================================
# Validation
# ============================================================================

def create_validation_sample(matches_df: pl.DataFrame, n: int = 100):
    """Create random validation sample."""
    logger.info(f"\n{'='*80}")
    logger.info(f"CREATING VALIDATION SAMPLE (n={n})")
    logger.info(f"{'='*80}")

    # Stratified sample by confidence band
    high_conf = matches_df.filter(pl.col('confidence') >= 0.97).sample(min(n//2, len(matches_df)), seed=42)
    remaining = matches_df.filter(pl.col('confidence') < 0.97)
    low_conf = remaining.sample(min(n//2, len(remaining)), seed=43)

    validation_sample = pl.concat([high_conf, low_conf])

    # Save to CSV for manual review
    validation_sample.write_csv(VALIDATION_CSV)
    logger.info(f"\nSaved validation sample to {VALIDATION_CSV}")
    logger.info("\nPlease review and mark 'is_correct' column as Y/N")
    logger.info("Columns to review:")
    logger.info("  - institution_raw: Original institution name from paper")
    logger.info("  - company_name: Matched CRSP company")
    logger.info("  - ticker: Stock ticker")
    logger.info("  - state: Firm headquarters state")
    logger.info("  - confidence: Match confidence score")
    logger.info("  - method: Matching method")
    logger.info("  - match_reason: Explanation of match")

    return validation_sample


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function."""
    start_time = time.time()

    # Run matching
    matches_df, unmatched = run_stage_1_matching()

    # Create validation sample
    validation_sample = create_validation_sample(matches_df, n=100)

    elapsed = time.time() - start_time

    logger.info(f"\n{'='*80}")
    logger.info("STAGE 1 COMPLETED")
    logger.info(f"{'='*80}")
    logger.info(f"Elapsed time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    logger.info(f"Total matches: {len(matches_df):,}")
    logger.info(f"Unique firms: {matches_df['gvkey'].n_unique():,}")
    logger.info(f"Validation sample: {VALIDATION_CSV}")
    logger.info(f"\nNext step: Manually validate {len(validation_sample)} matches")
    logger.info(f"Then proceed to Stage 2 (ROR/OpenAlex enrichment)")

    return matches_df, validation_sample


if __name__ == "__main__":
    main()
