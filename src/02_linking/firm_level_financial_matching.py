"""
Firm-Level Financial Matching System

Matches institutions to CRSP/Compustat firms at the FIRM LEVEL.
Final output will be firm-year panel with paper counts, patent counts, and financial metrics.

Target: Match as many firm-affiliated papers to financial firms as possible
"""

import polars as pl
from pathlib import Path
import logging
import json
import re
from rapidfuzz import fuzz, process
from collections import defaultdict
import time

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "compustat"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

FIRM_PAPERS = DATA_PROCESSED / "ai_papers_firms_only.parquet"
FINANCIAL_DATA = DATA_RAW / "crsp_a_ccm.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "institution_to_firm_matches.parquet"
PROGRESS_LOG = LOGS_DIR / "firm_financial_matching.log"

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
# Name Normalization
# ============================================================================

SUFFIXES = [
    ' inc', ' corp', ' llc', 'ltd', 'limited', 'plc', 'gmbh', 'ag', 'sa', 'spa',
    'pty', 'bv', 'nv', 'aps', 'co', 'company', 'corporation', 'laboratories',
    ' laboratory', 'research', 'group', 'holdings', 'international', 'industries',
    ' technologies', 'technology', 'systems', 'solutions', 'software', 'services'
]

ABBREVIATIONS = {
    'ibm': 'international business machines',
    'ge': 'general electric',
    'gm': 'general motors',
    'hp': 'hewlett packard',
    'at&t': 'at and t',
    '3m': 'three m',
}

def normalize_name(name: str) -> str:
    """Normalize company name for matching."""
    if not name or name == "":
        return ""

    normalized = name.lower().strip()

    # Remove country in parentheses
    normalized = re.sub(r'\s*\([^)]*\)', '', normalized)

    # Replace abbreviations
    for abbr, expanded in ABBREVIATIONS.items():
        normalized = normalized.replace(abbr, expanded)

    # Remove suffixes
    for suffix in SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()

    # Replace & with 'and'
    normalized = normalized.replace('&', 'and')

    # Normalize whitespace
    normalized = re.sub(r'\s+', ' ', normalized)

    return normalized.strip()


# ============================================================================
# Load Financial Data with Error Handling
# ============================================================================

def load_financial_data():
    """Load CRSP-CCM data with proper error handling."""
    logger.info("=" * 80)
    logger.info("LOADING FINANCIAL DATA (CRSP-CCM)")
    logger.info("=" * 80)

    logger.info(f"\nLoading {FINANCIAL_DATA}...")

    # Load with error handling for problematic CUSIP column
    try:
        financial_df = pl.read_csv(
            FINANCIAL_DATA,
            ignore_errors=True,  # Skip malformed rows
            truncate_ragged_lines=True,
            infer_schema_length=10000
        )
        logger.info(f"  Loaded {len(financial_df):,} records")
    except Exception as e:
        logger.error(f"  Error: {e}")
        logger.info("  Trying with dtypes override...")

        # Try with specific dtypes
        financial_df = pl.read_csv(
            FINANCIAL_DATA,
            dtypes={
                'GVKEY': str,
                'LPERMNO': int,
                'tic': str,
                'conm': str,
                'cusip': str,  # Load as string to avoid parsing errors
            },
            ignore_errors=True,
            truncate_ragged_lines=True
        )
        logger.info(f"  Loaded {len(financial_df):,} records")

    # Normalize company names
    logger.info("\nNormalizing company names...")
    financial_df = financial_df.with_columns([
        pl.col('conm').str.to_lowercase().str.strip().alias('conm_normalized')
    ])

    # Get unique firms (primary links only)
    logger.info("\nExtracting unique firms...")
    unique_firms = financial_df.filter(
        pl.col('LINKPRIM') == 'P'
    ).select([
        'GVKEY', 'LPERMNO', 'tic', 'conm', 'conm_normalized',
        'state', 'city', 'cik'
    ]).unique()

    logger.info(f"  Found {len(unique_firms):,} unique firms (primary links)")

    return unique_firms


# ============================================================================
# Matching Strategies (Optimized for Firm Level)
# ============================================================================

def exact_match(institution_normalized: str, firms_df: pl.DataFrame) -> dict:
    """Exact name matching."""
    matches = firms_df.filter(
        pl.col('conm_normalized') == institution_normalized
    )

    if len(matches) > 0:
        match = matches[0]
        # Get state if column exists
        state_val = match['state'] if 'state' in match.schema else None

        return {
            'gvkey': match['GVKEY'],
            'permno': match['LPERMNO'],
            'ticker': match['tic'],
            'company_name': match['conm'],
            'state': state_val,
            'confidence': 0.98,
            'method': 'exact'
        }
    return None


def ticker_match(institution_name: str, firms_df: pl.DataFrame) -> dict:
    """Ticker-based matching."""
    # Extract ticker from name
    ticker_match = re.search(r'\(([A-Z]{1,5})\)', institution_name)
    if ticker_match:
        ticker = ticker_match.group(1)

        matches = firms_df.filter(
            pl.col('tic').str.to_uppercase() == ticker
        )

        if len(matches) > 0:
            match = matches[0]
            # Get state if column exists
            state_val = match['state'] if 'state' in match.schema else None

            return {
                'gvkey': match['GVKEY'],
                'permno': match['LPERMNO'],
                'ticker': match['tic'],
                'company_name': match['conm'],
                'state': state_val,
                'confidence': 0.95,
                'method': 'ticker'
            }

    return None


def fuzzy_match_optimized(institution_normalized: str, firms_df: pl.DataFrame, top_n: int = 100) -> dict:
    """Optimized fuzzy matching - only search top firms by market cap."""
    # Filter to US firms for efficiency
    us_firms = firms_df.filter(
        pl.col('state').is_not_null()
    ).unique(subset=['conm_normalized'])

    # Limit search size for performance
    if len(us_firms) > top_n:
        # For very common names, we need to search more
        if len(institution_normalized) < 10:  # Short names
            us_firms = us_firms
        else:
            us_firms = us_firms[:top_n]

    # Get company names
    company_names = us_firms['conm_normalized'].to_list()

    # Use rapidfuzz with score cutoff
    result = process.extract(
        institution_normalized,
        company_names,
        scorer=fuzz.WRatio,
        score_cutoff=87,  # 87% threshold
        limit=1
    )

    if result and len(result) > 0:
        best_name, score, _ = result[0]
        matches = us_firms.filter(pl.col('conm_normalized') == best_name)

        if len(matches) > 0:
            match = matches[0]
            confidence = 0.70 + (score - 87) * 0.01
            confidence = min(confidence, 0.90)

            # Get state if column exists
            state_val = None
            if 'state' in match.schema:
                state_val = match['state']

            return {
                'gvkey': match['GVKEY'],
                'permno': match['LPERMNO'],
                'ticker': match['tic'],
                'company_name': match['conm'],
                'state': state_val,
                'confidence': confidence,
                'method': 'fuzzy'
            }

    return None


# ============================================================================
# Main Matching Function
# ============================================================================

def match_institutions_to_firms():
    """Match institutions to financial firms at firm level."""
    logger.info("=" * 80)
    logger.info("FIRM-LEVEL FINANCIAL MATCHING")
    logger.info("=" * 80)

    # Load data
    logger.info("\nLoading institution data...")
    inst_ref = pl.read_parquet(DATA_PROCESSED / "ai_papers_firms_only.parquet")
    logger.info(f"  Loaded {len(inst_ref):,} firm papers")

    logger.info("\nLoading financial data...")
    firms_df = load_financial_data()

    # Extract unique institutions from papers
    logger.info("\nExtracting unique institutions from papers...")
    institutions_set = {}

    for row in inst_ref.iter_rows(named=True):
        primary_affs = row.get('author_primary_affiliations', [])
        countries = row.get('author_primary_affiliation_countries', [])

        for j, aff in enumerate(primary_affs):
            if not aff or aff == "":
                continue

            country = countries[j] if j < len(countries) else None

            # Create normalized name
            normalized = normalize_name(aff)

            # Store canonical name and normalized name
            if aff not in institutions_set:
                institutions_set[aff] = {
                    'normalized_name': normalized,
                    'country': country,
                    'paper_count': 0
                }
            institutions_set[aff]['paper_count'] += 1

    logger.info(f"  Found {len(institutions_set):,} unique institutions")

    # Convert to list for processing
    institutions_list = []
    for raw_name, data in institutions_set.items():
        institutions_list.append({
            'raw_name': raw_name,
            'normalized_name': data['normalized_name'],
            'country': data['country'],
            'paper_count': data['paper_count']
        })

    logger.info(f"  Total institutions: {len(institutions_list):,}")

    # Match institutions to firms
    logger.info("\nMatching institutions to financial firms...")
    matches = []
    unmatched = []

    total = len(institutions_list)
    for i, inst in enumerate(institutions_list):
        if (i + 1) % 5000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} institutions...")

        inst_normalized = inst['normalized_name']

        # Try matching strategies in order
        match_result = None

        # Strategy 1: Ticker match
        if not match_result:
            match_result = ticker_match(inst['raw_name'], firms_df)

        # Strategy 2: Exact name match
        if not match_result:
            match_result = exact_match(inst_normalized, firms_df)

        # Strategy 3: Fuzzy match
        if not match_result:
            match_result = fuzzy_match_optimized(inst_normalized, firms_df)

        if match_result:
            matches.append({
                'institution_raw': inst['raw_name'],
                'institution_normalized': inst_normalized,
                'institution_country': inst['country'],
                'paper_count': inst['paper_count'],
                **match_result
            })
        else:
            unmatched.append(inst)

    logger.info(f"\nMatched: {len(matches):,} institutions")
    logger.info(f"Unmatched: {len(unmatched):,} institutions")

    match_rate = len(matches) / total * 100
    logger.info(f"Match rate: {match_rate:.1f}%")

    # Save matches
    matches_df = pl.DataFrame(matches)
    matches_df.write_parquet(OUTPUT_PARQUET, compression='snappy')
    logger.info(f"\nSaved to {OUTPUT_PARQUET}")

    # Save unmatched for reference
    if len(unmatched) > 0:
        unmatched_df = pl.DataFrame(unmatched)
        unmatched_file = OUTPUT_DIR / "unmatched_institutions.parquet"
        unmatched_df.write_parquet(unmatched_file, compression='snappy')
        logger.info(f"Saved {len(unmatched):,} unmatched institutions to {unmatched_file}")

    # Statistics
    logger.info("\n" + "=" * 80)
    logger.info("MATCHING STATISTICS")
    logger.info("=" * 80)

    logger.info(f"\nMethod distribution:")
    if len(matches) > 0:
        method_dist = matches_df.group_by('method').agg(pl.len().alias('count')).sort('count', descending=True)
        for row in method_dist.iter_rows(named=True):
            logger.info(f"  {row['method']}: {row['count']:,} ({row['count']/len(matches)*100:.1f}%)")

        logger.info(f"\nConfidence distribution:")
        logger.info(f"  High (>0.90): {len(matches_df.filter(pl.col('confidence') > 0.90)):,} ({len(matches_df.filter(pl.col('confidence') > 0.90))/len(matches)*100:.1f}%)")
        logger.info(f"  Medium (0.70-0.90): {len(matches_df.filter((pl.col('confidence') >= 0.70) & (pl.col('confidence') <= 0.90))):,} ({len(matches_df.filter((pl.col('confidence') >= 0.70) & (pl.col('confidence') <= 0.90)))/len(matches)*100:.1f}%)")

        logger.info(f"\nUnique financial firms matched: {matches_df['gvkey'].n_unique():,}")
        logger.info(f"Unique PERMNOs: {matches_df['permno'].n_unique():,}")
        # Note: ticker column may be list type, skip counting for now

        logger.info(f"\nTop 20 firms by institution count:")
        top_firms = matches_df.group_by(['gvkey', 'company_name']).agg([
            pl.len().alias('inst_count'),
            pl.col('paper_count').sum().alias('total_papers')
        ]).sort('inst_count', descending=True).head(20)

        for row in top_firms.iter_rows(named=True):
            logger.info(f"  {row['company_name'][:50]:<50}: {row['inst_count']:>4} institutions, {row['total_papers']:>6,} papers")

    return matches_df, match_rate


def main():
    """Main execution function."""
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("FIRM-LEVEL FINANCIAL MATCHING")
    logger.info("=" * 80)
    logger.info(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    matches_df, match_rate = match_institutions_to_firms()

    elapsed = time.time() - start_time
    logger.info(f"\nElapsed time: {elapsed:.1f} seconds")

    logger.info("\n" + "=" * 80)
    logger.info("FINANCIAL MATCHING COMPLETED")
    logger.info("=" * 80)
    logger.info(f"Match rate: {match_rate:.1f}%")
    logger.info(f"Unique firms: {matches_df['gvkey'].n_unique():,}")
    logger.info(f"Output: {OUTPUT_PARQUET}")
    logger.info(f"Finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    return matches_df, match_rate


if __name__ == "__main__":
    main()
