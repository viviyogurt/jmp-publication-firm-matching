"""
STAGE 1: Direct Name Matching (CRSP→OpenAlex Institutions)

Approach:
1. Load CRSP/Compustat firms as PRIMARY reference
2. For each firm, search OpenAlex institutions for exact name matches
3. Use firm attributes (ticker, location) to validate
4. Assign confidence scores based on match strength

Target: >95% accuracy
"""

import polars as pl
from pathlib import Path
import logging
import re
from typing import Dict, List, Optional, Set
import time

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_PUB = PROJECT_ROOT / "data" / "processed" / "publication"
DATA_RAW_COMP = PROJECT_ROOT / "data" / "raw" / "compustat"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

FIRM_PAPERS = DATA_PROCESSED_PUB / "ai_papers_firms_only.parquet"
FINANCIAL_DATA = DATA_RAW_COMP / "crsp_a_ccm.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "stage1_direct_name_matches.parquet"
PROGRESS_LOG = LOGS_DIR / "stage1_direct_matching.log"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROGRESS_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_firm_reference_data():
    """Load CRSP/Compustat firms with all attributes."""
    logger.info("=" * 80)
    logger.info("LOADING CRSP/COMPUSTAT FIRM REFERENCE DATA")
    logger.info("=" * 80)

    logger.info(f"\nLoading {FINANCIAL_DATA}...")
    crsp_df = pl.read_csv(
        FINANCIAL_DATA,
        dtypes={
            'GVKEY': str,
            'LPERMNO': int,
            'tic': str,
            'conm': str,  # Company name
            'conml': str,  # Legal company name
            'cusip': str,
            'cik': str,
            'incorp': str,  # Location of incorporation
            'state': str,  # State headquarters
            'city': str,    # City headquarters
            'zip': str,     # ZIP code
            'sic': int,      # Industry code
            'busdesc': str,  # Business description
            'fyear': int,    # Fiscal year
            'fic': str,     # Foreign industry code
        },
        ignore_errors=True,
        truncate_ragged_lines=True
    )

    logger.info(f"  Loaded {len(crsp_df):,} records")

    # Get unique firms (primary links only)
    logger.info("\nExtracting unique firms (primary links)...")
    unique_firms = crsp_df.filter(
        pl.col('LINKPRIM') == 'P'
    ).select([
        'GVKEY', 'LPERMNO', 'tic', 'conm', 'conml',
        'state', 'city', 'incorp',
        'fic', 'busdesc', 'weburl'
    ]).unique()

    logger.info(f"  Found {len(unique_firms):,} unique firms")
    logger.info(f"  With ticker: {unique_firms.filter(pl.col('tic').is_not_null()).shape[0]:,}")
    logger.info(f"  With industry (FIC): {unique_firms.filter(pl.col('fic').is_not_null()).shape[0]:,}")
    logger.info(f"  With business description: {unique_firms.filter(pl.col('busdesc').is_not_null()).shape[0]:,}")
    logger.info(f"  With website: {unique_firms.filter(pl.col('weburl').is_not_null()).shape[0]:,}")

    return unique_firms


def load_institution_dataset():
    """Load unique institutions from OpenAlex papers."""
    logger.info("\n" + "=" * 80)
    logger.info("LOADING OPENALEX INSTITUTION DATASET")
    logger.info("=" * 80)

    logger.info(f"\nLoading {FIRM_PAPERS}...")
    firm_papers = pl.read_parquet(FIRM_PAPERS)
    logger.info(f"  Loaded {len(firm_papers):,} firm papers")

    # Extract unique institutions with paper counts
    logger.info("\nExtracting institutions with paper counts...")
    institutions = {}

    for row in firm_papers.iter_rows(named=True):
        primary_affs = row.get('author_primary_affiliations', [])
        countries = row.get('author_primary_affiliation_countries', [])

        for j, aff in enumerate(primary_affs):
            if not aff or aff == "":
                continue

            country = countries[j] if j < len(countries) else None

            if aff not in institutions:
                institutions[aff] = {
                    'country': country,
                    'paper_count': 0,
                    'openalex_ids': set()
                }
            institutions[aff]['paper_count'] += 1

    # Convert to list
    institution_list = []
    for inst_name, data in institutions.items():
        institution_list.append({
            'institution_name': inst_name,
            'country': data['country'],
            'paper_count': data['paper_count']
        })

    logger.info(f"  Found {len(institution_list):,} unique institutions")

    return institution_list


def normalize_name(name: str) -> str:
    """Normalize name for matching."""
    if not name:
        return ""

    normalized = name.lower().strip()

    # Remove country in parentheses
    normalized = re.sub(r'\s*\([^)]*\)', '', normalized)

    # Remove common suffixes
    suffixes = [' inc', 'corp', 'llc', 'ltd', 'limited', 'plc', 'gmbh', 'ag', 'sa']
    for suffix in suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()

    # Replace & with 'and'
    normalized = normalized.replace('&', 'and')

    return normalized


def exact_name_match(firm_name: str, firm_data: Dict, institutions: List[Dict]) -> List[Dict]:
    """
    Find institutions that exactly match firm name.
    Returns list of matches with confidence scores.

    Much more conservative than before to avoid false positives.
    """
    firm_normalized = normalize_name(firm_name)
    firm_lower = firm_name.lower()

    # Extract the core name (first word or two) for matching
    firm_words = firm_normalized.split()[:2]  # Take first 2 words max
    firm_core = ' '.join(firm_words) if firm_words else firm_normalized

    matches = []

    for inst in institutions:
        inst_normalized = normalize_name(inst['institution_name'])
        inst_lower = inst['institution_name'].lower()

        # Strategy 1: Exact match after normalization (strict)
        if inst_normalized == firm_normalized:
            confidence = 0.98
            matches.append({
                'institution_name': inst['institution_name'],
                'country': inst['country'],
                'paper_count': inst['paper_count'],
                'gvkey': firm_data['GVKEY'],
                'permno': firm_data['LPERMNO'],
                'ticker': firm_data['tic'],
                'conm': firm_data['conm'],
                'match_type': 'exact_normalized_name',
                'confidence': confidence,
                'match_reason': f'Exact normalized name match: {firm_normalized}'
                })

        # Strategy 2: Firm core name contained in institution name (subsidiary)
        # Only match if institution name STARTS with firm core name
        # This matches "Microsoft Research" → "Microsoft" but NOT "International" → "IBM"
        elif firm_lower in inst_lower and inst_lower.startswith(firm_core + ' '):
            # Institution starts with firm name (e.g., "Microsoft Research Asia" → "Microsoft")
            # Only match if firm is at least 3 chars and institution is longer
            if len(firm_core) >= 3 and len(inst_lower) > len(firm_lower) * 0.8:
                confidence = 0.95
                matches.append({
                    'institution_name': inst['institution_name'],
                    'country': inst['country'],
                    'paper_count': inst['paper_count'],
                    'gvkey': firm_data['GVKEY'],
                    'permno': firm_data['LPERMNO'],
                    'ticker': firm_data['tic'],
                    'conm': firm_data['conm'],
                    'match_type': 'subsidiary_pattern',
                    'confidence': confidence,
                    'match_reason': f'Firm name prefix in institution: {firm_core} in {inst_lower[:40]}'
                })

    return matches


def ticker_match(firm_ticker: str, firm_data: Dict, institutions: List[Dict]) -> List[Dict]:
    """
    Find institutions that contain the firm's ticker.
    High confidence when ticker is in institution name.
    """
    if not firm_ticker:
        return []

    ticker_upper = firm_ticker.upper()
    matches = []

    for inst in institutions:
        inst_name = inst['institution_name'].lower()

        # Look for ticker pattern: (TICKER) or [TICKER]
        ticker_pattern = rf'[\(\[{ticker_upper}\]\)]'
        if re.search(ticker_pattern, inst_name):
            matches.append({
                'institution_name': inst['institution_name'],
                'country': inst['country'],
                'paper_count': inst['paper_count'],
                'gvkey': firm_data['GVKEY'],
                'permno': firm_data['LPERMNO'],
                'ticker': firm_data['tic'],
                'conm': firm_data['conm'],
                'match_type': 'ticker_in_name',
                'confidence': 0.97,
                'match_reason': f'Ticker {firm_ticker} found in institution name'
            })

    return matches


def location_validated_match(firm_data: Dict, institutions: List[Dict]) -> List[Dict]:
    """
    Match institutions by location (state/country).
    Lower confidence but useful for subsidiaries.
    """
    firm_state = firm_data.get('state')
    firm_city = firm_data.get('city')

    if not firm_state and not firm_city:
        return []

    matches = []

    for inst in institutions:
        inst_country = inst.get('country')

        # Country match (for international firms)
        if inst_country and firm_state:
            # US firms - check if institution is US-based
            inst_lower = inst_country.lower()
            if 'united states' in inst_lower:
                # Could try state matching if we had that info
                pass

        # If firm city is known, look for institutions in that city
        if firm_city and inst_country:
            # City/country match
            pass  # Would need more granular location data

    return matches


def run_stage1_direct_matching():
    """Run Stage 1: Direct name matching from firms to institutions."""
    logger.info("=" * 80)
    logger.info("STAGE 1: DIRECT NAME MATCHING (CRSP → OPENALEX INSTITUTIONS)")
    logger.info("=" * 80)
    logger.info(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    firms_df_data = load_firm_reference_data()
    institutions_list = load_institution_dataset()

    # Convert firms to list of dicts
    firms = []
    for row in firms_df_data.iter_rows(named=True):
        firms.append({
            'GVKEY': row['GVKEY'],
            'LPERMNO': row['LPERMNO'],
            'tic': row['tic'],
            'conm': row['conm'],
            'state': row['state'],
            'city': row['city'],
            'fic': row['fic'],
            'busdesc': row['busdesc'],
            'weburl': row['weburl'],
        })

    logger.info(f"\nProcessing {len(firms):,} firms...")

    # Match each firm to institutions
    all_matches = []

    for i, firm in enumerate(firms):
        if (i + 1) % 100 == 0:
            logger.info(f"  Processed {i+1:,}/{len(firms):,} firms ({len(all_matches)} matches so far)...")

        # Strategy 1: Exact name match
        matches = exact_name_match(firm['conm'], firm, institutions_list)

        # Strategy 2: Ticker match
        ticker_matches = ticker_match(firm['tic'], firm, institutions_list)
        matches.extend(ticker_matches)

        # Strategy 3: Location-based (for state-specific firms)
        # TODO: Implement if needed

        # Add to all matches
        all_matches.extend(matches)

    logger.info(f"\n{'='*80}")
    logger.info("STAGE 1 MATCHING RESULTS")
    logger.info(f"{'='*80}")
    logger.info(f"Total matches: {len(all_matches):,}")

    # Convert to DataFrame
    matches_df = pl.DataFrame(all_matches)

    # Statistics
    logger.info(f"\nUnique firms matched: {matches_df['gvkey'].n_unique():,}")
    logger.info(f"Unique institutions matched: {matches_df['institution_name'].n_unique():,}")

    # Confidence distribution
    logger.info(f"\nConfidence distribution:")
    conf_dist = matches_df.group_by('confidence').agg(pl.len().alias('count')).sort('confidence', descending=True)
    for row in conf_dist.iter_rows(named=True):
        logger.info(f"  {row['confidence']:.2f}: {row['count']:,}")

    # Match type distribution
    logger.info(f"\nMatch type distribution:")
    type_dist = matches_df.group_by('match_type').agg(pl.len().alias('count')).sort('count', descending=True)
    for row in type_dist.iter_rows(named=True):
        logger.info(f"  {row['match_type']}: {row['count']:,}")

    # Top firms by institution count
    logger.info(f"\nTop 20 firms by institution count:")
    top_firms = matches_df.group_by(['gvkey', 'conm']).agg([
        pl.len().alias('inst_count'),
        pl.col('paper_count').sum().alias('total_papers')
    ]).sort('inst_count', descending=True).head(20)

    for row in top_firms.iter_rows(named=True):
        logger.info(f"  {row['conm'][:45]:<45}: {row['inst_count']:>4} institutions, {row['total_papers']:>6,} papers")

    # Save results
    matches_df.write_parquet(OUTPUT_PARQUET)
    logger.info(f"\nSaved to {OUTPUT_PARQUET}")

    return matches_df


def create_validation_sample(matches_df: pl.DataFrame, n: int = 100):
    """Create random validation sample."""
    logger.info(f"\n{'='*80}")
    logger.info(f"CREATING VALIDATION SAMPLE (n={n})")
    logger.info(f"{'='*80}")

    # Stratified by confidence
    high_conf = matches_df.filter(pl.col('confidence') >= 0.95).sample(min(n//3, len(matches_df)), seed=42)
    mid_conf = matches_df.filter((pl.col('confidence') >= 0.90) & (pl.col('confidence') < 0.95)).sample(min(n//3, len(matches_df)), seed=43)
    low_conf = matches_df.filter(pl.col('confidence') < 0.90).sample(min(n//3, len(matches_df)), seed=44)

    validation_sample = pl.concat([high_conf, mid_conf, low_conf])

    # Save to CSV
    validation_csv = OUTPUT_DIR / "stage1_validation_sample.csv"
    validation_sample.write_csv(validation_csv)
    logger.info(f"\nSaved validation sample to {validation_csv}")
    logger.info(f"Total samples: {len(validation_sample):,}")

    return validation_sample


def main():
    """Main execution."""
    start_time = time.time()

    matches_df = run_stage1_direct_matching()
    validation_sample = create_validation_sample(matches_df, n=100)

    elapsed = time.time() - start_time

    logger.info(f"\n{'='*80}")
    logger.info("STAGE 1 COMPLETED")
    logger.info(f"{'='*80}")
    logger.info(f"Elapsed time: {elapsed:.1f} seconds")
    logger.info(f"Total matches: {len(matches_df):,}")
    logger.info(f"Unique firms: {matches_df['gvkey'].n_unique():,}")
    logger.info(f"Validation sample: {len(validation_sample):,}")
    logger.info(f"\n✅ STAGE 1 TARGET: >95% accuracy")
    logger.info(f"Next: Review validation sample, then proceed to Stage 2")


if __name__ == "__main__":
    main()
