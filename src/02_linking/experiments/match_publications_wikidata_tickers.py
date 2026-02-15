"""
Method 1.1: Wikidata Ticker Matching

This script matches institutions to firms using ticker data from Wikidata.
It extracts tickers from Wikidata structured data and matches them to Compustat tickers.

Methodology:
1. Extract tickers from publication_institutions_wikidata_structured.parquet
2. Create ticker→GVKEY lookup dictionary from Compustat firms
3. Exact ticker match → 0.98 confidence
4. Cross-validate with name similarity (Jaro-Winkler ≥0.80)
5. If name similarity <0.80, reject (prevent ticker collisions like "CP")

Target: 1,800-2,200 firms @ 99.5% accuracy

Reference: src/02_linking/match_patents_to_firms_stage1.py (ticker logic, lines 106-126)
"""

import polars as pl
from pathlib import Path
import logging
from rapidfuzz import fuzz
from typing import Dict, List, Optional, Tuple

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_WIKIDATA = DATA_INTERIM / "publication_institutions_wikidata_structured.parquet"
INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_wikidata_tickers.parquet"
PROGRESS_LOG = LOGS_DIR / "match_publications_wikidata_tickers.log"

DATA_PROCESSED_LINK.mkdir(parents=True, exist_ok=True)
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

# Configuration
MIN_JARO_WINKLER = 0.80  # Cross-validation threshold
CONFIDENCE = 0.98  # Base confidence for ticker matches


def extract_tickers_from_wikidata(tickers_list: List) -> List[str]:
    """
    Extract and normalize tickers from Wikidata tickers field.

    Args:
        tickers_list: List of tickers from Wikidata (may be None or empty)

    Returns:
        List of normalized ticker strings (uppercase, stripped)
    """
    if not tickers_list:
        return []

    normalized = []
    for ticker in tickers_list:
        if ticker and isinstance(ticker, str):
            ticker_clean = ticker.strip().upper()
            if ticker_clean:
                normalized.append(ticker_clean)

    return normalized


def create_ticker_lookup(firms_df: pl.DataFrame) -> Dict[str, List[Dict]]:
    """
    Create ticker→GVKEY lookup dictionary from Compustat firms.

    Args:
        firms_df: Standardized Compustat firms DataFrame

    Returns:
        Dictionary mapping ticker → list of firm records (multiple firms may have same ticker)
    """
    lookup = {}

    for row in firms_df.iter_rows(named=True):
        gvkey = row['GVKEY']
        lpermno = row.get('LPERMNO')
        conm = row['conm']
        tic = row['tic']
        conm_clean = row.get('conm_clean')

        # Skip if no ticker
        if not tic or not tic.strip():
            continue

        ticker = tic.strip().upper()

        if ticker not in lookup:
            lookup[ticker] = []

        lookup[ticker].append({
            'GVKEY': gvkey,
            'LPERMNO': lpermno,
            'conm': conm,
            'conm_clean': conm_clean,
            'tic': ticker
        })

    return lookup


def jaro_winkler_similarity(str1: Optional[str], str2: Optional[str]) -> float:
    """
    Calculate Jaro-Winkler similarity between two strings.

    Args:
        str1: First string (may be None)
        str2: Second string (may be None)

    Returns:
        Similarity score between 0.0 and 1.0
    """
    if not str1 or not str2:
        return 0.0

    return fuzz.JaroWinkler.similarity(str1, str2)


def match_institution_by_ticker(institution_row: Dict,
                                 ticker_lookup: Dict[str, List[Dict]],
                                 institutions_master_df: pl.DataFrame) -> List[Dict]:
    """
    Match institution to firms by ticker with cross-validation.

    Args:
        institution_row: Institution record with tickers
        ticker_lookup: Dictionary mapping ticker → firm records
        institutions_master_df: Master institutions table (for display names)

    Returns:
        List of match records (may be empty or have multiple matches)
    """
    matches = []

    institution_id = institution_row['institution_id']
    tickers_raw = institution_row.get('tickers')

    # Extract and normalize tickers
    tickers = extract_tickers_from_wikidata(tickers_raw)

    if not tickers:
        return matches

    # Get institution display name from master table
    inst_master = institutions_master_df.filter(
        pl.col('institution_id') == institution_id
    )

    if inst_master.is_empty():
        logger.warning(f"Institution {institution_id} not found in master table")
        return matches

    display_name = inst_master['display_name'][0]
    paper_count = inst_master['paper_count'][0] if 'paper_count' in inst_master.columns else 0

    # Try each ticker
    for ticker in tickers:
        if ticker not in ticker_lookup:
            continue

        # Check all firms with this ticker (handle ticker collisions)
        for firm_record in ticker_lookup[ticker]:
            gvkey = firm_record['GVKEY']
            lpermno = firm_record['LPERMNO']
            firm_conm = firm_record['conm']
            firm_conm_clean = firm_record['conm_clean']

            # Cross-validate with name similarity
            # Compare institution display_name to firm conm_clean
            name_similarity = jaro_winkler_similarity(display_name, firm_conm_clean)

            # Require minimum name similarity to prevent ticker collisions
            if name_similarity < MIN_JARO_WINKLER:
                # Skip this match - likely a ticker collision (e.g., "CP")
                continue

            # Valid match
            matches.append({
                'GVKEY': gvkey,
                'LPERMNO': lpermno,
                'firm_conm': firm_conm,
                'institution_id': institution_id,
                'institution_display_name': display_name,
                'match_type': 'wikidata_ticker',
                'match_confidence': CONFIDENCE,
                'match_method': 'wikidata_ticker_cross_validated',
                'matched_ticker': ticker,
                'name_similarity': name_similarity,
                'institution_paper_count': paper_count,
            })

    return matches


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("METHOD 1.1: WIKIDATA TICKER MATCHING")
    logger.info("=" * 80)

    # Step 1: Load data
    logger.info("\n[1/5] Loading data...")

    if not INSTITUTIONS_WIKIDATA.exists():
        raise FileNotFoundError(f"Wikidata structured file not found: {INSTITUTIONS_WIKIDATA}")
    if not INSTITUTIONS_MASTER.exists():
        raise FileNotFoundError(f"Institutions master file not found: {INSTITUTIONS_MASTER}")
    if not COMPUSTAT_STANDARDIZED.exists():
        raise FileNotFoundError(f"Compustat standardized file not found: {COMPUSTAT_STANDARDIZED}")

    institutions_wikidata = pl.read_parquet(INSTITUTIONS_WIKIDATA)
    institutions_master = pl.read_parquet(INSTITUTIONS_MASTER)
    firms_df = pl.read_parquet(COMPUSTAT_STANDARDIZED)

    logger.info(f"  Loaded {len(institutions_wikidata):,} institutions with Wikidata data")
    logger.info(f"  Loaded {len(institutions_master):,} institutions in master table")
    logger.info(f"  Loaded {len(firms_df):,} firms")

    # Step 2: Create ticker lookup
    logger.info("\n[2/5] Creating ticker→GVKEY lookup dictionary...")

    ticker_lookup = create_ticker_lookup(firms_df)

    logger.info(f"  Created lookup for {len(ticker_lookup):,} unique tickers")

    # Log some statistics
    ticker_collision_count = sum(1 for firms in ticker_lookup.values() if len(firms) > 1)
    logger.info(f"  Tickers with collisions (multiple firms): {ticker_collision_count:,}")

    # Step 3: Match institutions to firms
    logger.info("\n[3/5] Matching institutions to firms by ticker...")

    all_matches = []
    total_institutions = len(institutions_wikidata)
    institutions_with_tickers = 0
    rejected_low_similarity = 0

    for i, inst_row in enumerate(institutions_wikidata.iter_rows(named=True)):
        if (i + 1) % 500 == 0:
            logger.info(f"  Processed {i+1:,}/{total_institutions:,} institutions ({len(all_matches):,} matches so far)...")

        # Count institutions with tickers
        tickers = extract_tickers_from_wikidata(inst_row.get('tickers'))
        if tickers:
            institutions_with_tickers += 1

        matches = match_institution_by_ticker(inst_row, ticker_lookup, institutions_master)

        if not matches and tickers:
            # Tickers found but no matches (likely rejected due to low similarity)
            rejected_low_similarity += 1

        all_matches.extend(matches)

    logger.info(f"  Completed matching.")
    logger.info(f"  Institutions with tickers: {institutions_with_tickers:,}")
    logger.info(f"  Rejected (low name similarity): {rejected_low_similarity:,}")
    logger.info(f"  Total matches: {len(all_matches):,}")

    # Step 4: Deduplicate
    logger.info("\n[4/5] Deduplicating matches...")

    if not all_matches:
        logger.warning("  No matches found!")
        matches_df = pl.DataFrame()
    else:
        matches_df = pl.DataFrame(all_matches)

        # If same institution matched to same firm multiple times, keep highest name_similarity
        matches_df = (
            matches_df
            .sort(['institution_id', 'GVKEY', 'name_similarity'], descending=[False, False, True])
            .unique(subset=['institution_id', 'GVKEY'], keep='first')
        )

        logger.info(f"  After deduplication: {len(matches_df):,} unique institution-firm matches")
        logger.info(f"  Unique institutions matched: {matches_df['institution_id'].n_unique():,}")
        logger.info(f"  Unique firms matched: {matches_df['GVKEY'].n_unique():,}")

    # Step 5: Save output
    logger.info("\n[5/5] Saving matches...")
    logger.info(f"Output: {OUTPUT_FILE}")

    if len(matches_df) > 0:
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df):,} matches")

        # Summary statistics
        logger.info("\n" + "=" * 80)
        logger.info("WIKIDATA TICKER MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df):,}")
        logger.info(f"Unique institutions matched: {matches_df['institution_id'].n_unique():,}")
        logger.info(f"Unique firms matched: {matches_df['GVKEY'].n_unique():,}")

        # Name similarity statistics
        logger.info(f"\nName similarity statistics:")
        logger.info(f"  Mean: {matches_df['name_similarity'].mean():.3f}")
        logger.info(f"  Median: {matches_df['name_similarity'].median():.3f}")
        logger.info(f"  Min: {matches_df['name_similarity'].min():.3f}")
        logger.info(f"  Max: {matches_df['name_similarity'].max():.3f}")

        # Show examples
        logger.info("\nExample matches:")
        for i, row in enumerate(matches_df.head(15).iter_rows(named=True), 1):
            logger.info(f"  {i}. {row['institution_display_name'][:50]}")
            logger.info(f"     Ticker: {row['matched_ticker']}")
            logger.info(f"     → {row['firm_conm'][:50]}")
            logger.info(f"     Name similarity: {row['name_similarity']:.3f}")

    else:
        logger.warning("  No matches to save!")

    logger.info("\n" + "=" * 80)
    logger.info("WIKIDATA TICKER MATCHING COMPLETE")
    logger.info("=" * 80)

    return matches_df


if __name__ == "__main__":
    main()
