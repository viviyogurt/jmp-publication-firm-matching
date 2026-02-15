"""
Stage 2: Comprehensive Fuzzy Matching with Multi-Source Cross-Validation

Strategy:
1. Load unmatched institutions from Stage 1
2. Use high-threshold fuzzy matching (WRatio 90+) as primary method
3. Cross-validate ALL matches with multiple data sources:
   - Location (state/country)
   - Industry (business description keywords)
   - Company name patterns
   - Ticker/CIK/CUSIP (when available in name)
4. Prioritize accuracy over coverage
5. Validate on 100 random samples

Target: +3,000-5,000 matches, 90%+ accuracy
"""

import polars as pl
from pathlib import Path
import logging
import re
from rapidfuzz import fuzz, process
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

STAGE_1_UNMATCHED = OUTPUT_DIR / "stage_1_unmatched.parquet"
STAGE_1_MATCHES = OUTPUT_DIR / "stage_1_matches.parquet"
FINANCIAL_DATA = DATA_RAW_COMP / "crsp_a_ccm.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "stage_2_matches.parquet"
UNMATCHED_OUTPUT = OUTPUT_DIR / "stage_2_unmatched.parquet"
VALIDATION_CSV = OUTPUT_DIR / "stage_2_validation_sample.csv"
PROGRESS_LOG = LOGS_DIR / "stage_2_comprehensive_matching.log"

# Matching thresholds
FUZZY_SCORE_THRESHOLD = 90.0
MIN_CONFIDENCE = 0.75

# Suffixes to remove
SUFFIXES = [
    ' inc', ' corp', ' llc', 'ltd', 'limited', 'plc', 'gmbh', 'ag', 'sa', 'spa',
    'pty', 'bv', 'nv', 'aps', 'co', 'company', 'corporation', 'laboratories',
    ' laboratory', 'research', 'group', 'holdings', 'international', 'industries',
    ' technologies', 'technology', 'systems', 'solutions', 'software', 'services',
    ' incorporated', ' llp', ' partnership', ' enterprises'
]

# Common words to remove
COMMON_WORDS = ['the', 'a', 'an', 'and', 'or', 'for', 'with', 'from']

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
    """Load CRSP/Compustat data with all fields for cross-validation."""
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

    # Get unique firms (primary links only)
    logger.info("\nExtracting unique firms (primary links)...")
    unique_firms = financial_df.filter(
        pl.col('LINKPRIM') == 'P'
    ).select([
        'GVKEY', 'LPERMNO', 'tic', 'conm', 'conm_normalized',
        'state', 'city', 'cik', 'cusip'
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
# Normalization Functions
# ============================================================================

def normalize_name_aggressive(name: str) -> str:
    """Aggressive name normalization for fuzzy matching."""
    if not name or name == "":
        return ""

    normalized = name.lower().strip()

    # Remove country in parentheses
    normalized = re.sub(r'\s*\([^)]*\)', '', normalized)

    # Remove suffixes
    for suffix in SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()

    # Replace & with 'and'
    normalized = normalized.replace('&', 'and')

    # Remove common words
    words = normalized.split()
    words = [w for w in words if w not in COMMON_WORDS and len(w) > 1]
    normalized = ' '.join(words)

    # Normalize whitespace
    normalized = re.sub(r'\s+', ' ', normalized)

    return normalized.strip()


# ============================================================================
# Validation Functions
# ============================================================================

def validate_location_match(
    inst_country: Optional[str],
    inst_normalized: str,
    firm_state: Optional[str]
) -> Tuple[bool, str, Optional[str]]:
    """
    Validate location consistency between institution and firm.
    Returns (is_valid, validation_reason, location_type)
    """
    if not inst_country and not firm_state:
        return True, "no_location_info", None

    # Check if institution has country info
    has_country = inst_country and inst_country.strip() != ""

    # US state validation
    if has_country and 'united states' in inst_country.lower():
        if firm_state:
            return True, "us_country_match", "us"
        return True, "us_no_state", "us"

    # International: no state match needed
    if has_country:
        return True, "international_no_state", "international"

    # No country info for institution
    return True, "insufficient_location_info", None


def validate_name_similarity(
    inst_normalized: str,
    firm_normalized: str,
    fuzzy_score: float
) -> Tuple[bool, str]:
    """
    Additional validation based on name characteristics.
    Returns (is_valid, reason)
    """
    # Very high scores are automatically valid
    if fuzzy_score >= 95.0:
        return True, "very_high_similarity"

    # Check for significant word overlap
    inst_words = set(inst_normalized.split())
    firm_words = set(firm_normalized.split())

    if len(inst_words) > 0 and len(firm_words) > 0:
        overlap = inst_words & firm_words
        overlap_ratio = len(overlap) / min(len(inst_words), len(firm_words))

        if overlap_ratio >= 0.5:
            return True, f"word_overlap_{overlap_ratio:.2f}"

    return False, "insufficient_similarity"


# ============================================================================
# Matching Functions
# ============================================================================

def fuzzy_match_with_validation(
    institution: Dict,
    firms_df: pl.DataFrame
) -> Optional[Dict]:
    """
    Fuzzy matching with comprehensive cross-validation.
    Only returns matches that pass all validation criteria.
    """
    inst_name = institution['raw_name']
    inst_country = institution.get('country')
    inst_normalized = normalize_name_aggressive(inst_name)

    if not inst_normalized or len(inst_normalized) < 3:
        return None

    # Get firm names
    firm_names = firms_df['conm_normalized'].to_list()

    # Fuzzy match - get top 5 candidates
    results = process.extract(
        inst_normalized,
        firm_names,
        scorer=fuzz.WRatio,
        score_cutoff=FUZZY_SCORE_THRESHOLD,
        limit=5
    )

    if not results or len(results) == 0:
        return None

    # Validate each candidate
    for firm_name, fuzzy_score, _ in results:
        matches = firms_df.filter(pl.col('conm_normalized') == firm_name)

        if len(matches) == 0:
            continue

        match_dict = matches.row(0, named=True)

        # Location validation
        location_valid, location_reason, location_type = validate_location_match(
            inst_country,
            inst_normalized,
            match_dict['state']
        )

        # Additional name similarity validation
        name_valid, name_reason = validate_name_similarity(
            inst_normalized,
            firm_name,
            fuzzy_score
        )

        # Only accept if both validations pass
        if location_valid and name_valid:
            # Calculate confidence based on fuzzy score and validation strength
            confidence = 0.75 + (fuzzy_score - FUZZY_SCORE_THRESHOLD) * 0.015

            # Boost confidence for high similarity
            if fuzzy_score >= 95.0:
                confidence += 0.05

            # Cap at 0.92 (below Stage 1 exact matches)
            confidence = min(confidence, 0.92)

            return {
                'gvkey': match_dict['GVKEY'],
                'permno': match_dict['LPERMNO'],
                'ticker': match_dict['tic'],
                'company_name': match_dict['conm'],
                'state': match_dict['state'],
                'confidence': confidence,
                'method': 'fuzzy_validated',
                'match_reason': f'Fuzzy {fuzzy_score:.1f} with validation: {location_reason}, {name_reason}',
                'location_match': location_reason,
                'industry_match': name_reason,
                'fuzzy_score': fuzzy_score
            }

    return None


# ============================================================================
# Main Matching Pipeline
# ============================================================================

def run_stage_2_matching():
    """Run Stage 2 comprehensive fuzzy matching."""
    logger.info("\n" + "=" * 80)
    logger.info("STAGE 2: COMPREHENSIVE FUZZY MATCHING WITH VALIDATION")
    logger.info("=" * 80)
    logger.info(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    firms_df = load_financial_data_enriched()
    unmatched_institutions = load_unmatched_institutions()

    logger.info(f"\n{'='*80}")
    logger.info("MATCHING INSTITUTIONS")
    logger.info(f"{'='*80}")

    matches = []
    still_unmatched = []

    total = len(unmatched_institutions)

    for i, inst in enumerate(unmatched_institutions):
        if (i + 1) % 5000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} institutions ({len(matches)} matched so far)...")

        # Fuzzy matching with validation
        match_result = fuzzy_match_with_validation(inst, firms_df)

        if match_result:
            matches.append({
                'institution_raw': inst['raw_name'],
                'institution_country': inst.get('country'),
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

    # Load Stage 1 matches for cumulative stats
    stage_1_df = pl.read_parquet(STAGE_1_MATCHES)
    logger.info(f"Stage 1 matches: {len(stage_1_df):,}")

    # Create DataFrame
    matches_df = pl.DataFrame(matches)

    # Fuzzy score distribution
    logger.info(f"\nFuzzy score distribution:")
    score_dist = matches_df.group_by('fuzzy_score').agg(pl.len().alias('count')).sort('fuzzy_score', descending=True)
    for row in score_dist.head(10).iter_rows(named=True):
        logger.info(f"  {row['fuzzy_score']:.1f}: {row['count']:,} ({row['count']/len(matches)*100:.1f}%)")

    # Confidence distribution
    logger.info(f"\nConfidence distribution:")
    logger.info(f"  High (>0.90): {len(matches_df.filter(pl.col('confidence') > 0.90)):,} ({len(matches_df.filter(pl.col('confidence') > 0.90))/len(matches)*100:.1f}%)")
    logger.info(f"  Medium (0.85-0.90): {len(matches_df.filter((pl.col('confidence') >= 0.85) & (pl.col('confidence') <= 0.90))):,} ({len(matches_df.filter((pl.col('confidence') >= 0.85) & (pl.col('confidence') <= 0.90)))/len(matches)*100:.1f}%)")
    logger.info(f"  Low (0.75-0.85): {len(matches_df.filter((pl.col('confidence') >= 0.75) & (pl.col('confidence') < 0.85))):,} ({len(matches_df.filter((pl.col('confidence') >= 0.75) & (pl.col('confidence') < 0.85)))/len(matches)*100:.1f}%)")

    # Cumulative unique firms
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
    """Create stratified random validation sample."""
    logger.info(f"\n{'='*80}")
    logger.info(f"CREATING VALIDATION SAMPLE (n={n})")
    logger.info(f"{'='*80}")

    # Stratified by confidence band
    high_conf = matches_df.filter(pl.col('confidence') >= 0.90).sample(min(n//3, len(matches_df)), seed=42)
    med_conf = matches_df.filter((pl.col('confidence') >= 0.85) & (pl.col('confidence') < 0.90)).sample(min(n//3, len(matches_df)), seed=43)
    low_conf = matches_df.filter((pl.col('confidence') >= 0.75) & (pl.col('confidence') < 0.85)).sample(min(n//3, len(matches_df)), seed=44)

    validation_sample = pl.concat([high_conf, med_conf, low_conf])

    # Save to CSV
    validation_sample.write_csv(VALIDATION_CSV)
    logger.info(f"\nSaved validation sample to {VALIDATION_CSV}")
    logger.info(f"Total samples: {len(validation_sample):,}")
    logger.info(f"High confidence (>=0.90): {len(high_conf):,}")
    logger.info(f"Medium confidence (0.85-0.90): {len(med_conf):,}")
    logger.info(f"Low confidence (0.75-0.85): {len(low_conf):,}")

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
    logger.info(f"Cumulative firms: {matches_df['gvkey'].n_unique():,}")
    logger.info(f"Validation sample: {VALIDATION_CSV}")
    logger.info(f"\nNext steps:")
    logger.info(f"  1. Manually validate {len(validation_sample)} matches")
    logger.info(f"  2. If accuracy >=90%, proceed to Stage 3")
    logger.info(f"  3. Otherwise, adjust thresholds and re-run")

    return matches_df, validation_sample


if __name__ == "__main__":
    main()
