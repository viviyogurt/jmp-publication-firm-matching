"""
Match Institutions to Firms Using Wikipedia/Wikidata Ticker Symbols

This is HIGH-CONFIDENCE matching (0.98-0.99) because ticker symbols are unique identifiers.

Input:
- Institutions with extracted ticker symbols from Wikidata
- Compustat firms with ticker symbols

Matching Logic:
1. Extract ticker(s) from Wikidata (e.g., GOOGL, GOOG for Google)
2. Match to Compustat firms by exact ticker match
3. Assign 0.98-0.99 confidence (very high)

Expected: 1,000-2,000 matches with >95% accuracy
"""
import polars as pl
from pathlib import Path
import logging

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

WIKIDATA_FILE = DATA_INTERIM / "publication_institutions_wikidata_structured.parquet"
COMPUSTAT_FIRMS = DATA_INTERIM / "compustat_firms_standardized.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_wikidata_tickers.parquet"
PROGRESS_LOG = LOGS_DIR / "match_wikidata_tickers.log"

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

# ============================================================================
# Main Processing
# ============================================================================

def main():
    """Main matching workflow using Wikidata ticker symbols."""

    logger.info("=" * 80)
    logger.info("MATCHING INSTITUTIONS TO FIRMS USING WIKIDATA TICKER SYMBOLS")
    logger.info("=" * 80)
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    logger.info("\n[1/4] Loading data...")

    # Load institutions with Wikidata ticker symbols
    logger.info(f"  Loading institutions from {WIKIDATA_FILE}")
    if not WIKIDATA_FILE.exists():
        logger.error(f"  Wikidata file not found: {WIKIDATA_FILE}")
        logger.error("  Please run extract_wikipedia_structured_data.py first")
        return

    institutions_df = pl.read_parquet(WIKIDATA_FILE)
    logger.info(f"  Loaded {len(institutions_df):,} institutions")

    # Filter institutions with ticker symbols
    has_tickers = institutions_df.filter(
        pl.col('tickers').list.len() > 0
    )
    logger.info(f"  {len(has_tickers):,} institutions have ticker symbols")

    # Load Compustat firms
    logger.info(f"  Loading firms from {COMPUSTAT_FIRMS}")
    firms_df = pl.read_parquet(COMPUSTAT_FIRMS)
    logger.info(f"  Loaded {len(firms_df):,} firms")

    # Create ticker-to-firm mapping
    logger.info("\n[2/4] Creating ticker-to-firm mapping...")

    ticker_to_firms = {}

    for firm_row in firms_df.iter_rows(named=True):
        gvkey = firm_row['GVKEY']
        tic = firm_row.get('tic')  # Ticker symbol

        if tic and tic != '':
            # Normalize ticker (uppercase, trim)
            tic_normalized = tic.upper().strip()

            if tic_normalized not in ticker_to_firms:
                ticker_to_firms[tic_normalized] = []

            ticker_to_firms[tic_normalized].append({
                'GVKEY': gvkey,
                'LPERMNO': firm_row.get('LPERMNO'),
                'conm': firm_row.get('conm'),
                'tic': tic
            })

    logger.info(f"  Created mapping for {len(ticker_to_firms):,} unique tickers")

    # Match institutions to firms
    logger.info("\n[3/4] Matching institutions to firms by ticker...")

    matches = []
    match_count = 0

    for inst_row in has_tickers.iter_rows(named=True):
        institution_id = inst_row['institution_id']
        tickers = inst_row['tickers']  # List of tickers

        # Try each ticker
        for ticker in tickers:
            ticker_normalized = ticker.upper().strip()

            if ticker_normalized in ticker_to_firms:
                # Found match(es) for this ticker
                firms = ticker_to_firms[ticker_normalized]

                for firm in firms:
                    matches.append({
                        'institution_id': institution_id,
                        'GVKEY': firm['GVKEY'],
                        'LPERMNO': firm['LPERMNO'],
                        'firm_conm': firm['conm'],
                        'match_type': 'wikidata_ticker',
                        'match_method': 'wikidata_ticker_exact',
                        'match_confidence': 0.98,  # Very high confidence for ticker match
                        'matched_ticker': ticker,
                        'match_source': 'wikidata'
                    })

                    match_count += 1

    logger.info(f"  Found {match_count:,} matches")

    if len(matches) == 0:
        logger.warning("\n  No matches found!")
        return

    # Convert to DataFrame
    matches_df = pl.DataFrame(matches)

    # Deduplicate (keep highest confidence - all same at 0.98)
    logger.info("\n[4/4] Saving matches...")

    # Remove duplicates (same institution + firm)
    unique_matches = matches_df.unique(
        subset=['institution_id', 'GVKEY'],
        keep='first'
    )

    logger.info(f"  Deduplicated: {len(matches):,} -> {len(unique_matches):,} matches")

    # Save to file
    unique_matches.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved to {OUTPUT_FILE}")

    # Statistics
    logger.info("\n" + "=" * 80)
    logger.info("MATCHING STATISTICS")
    logger.info("=" * 80)

    unique_institutions = unique_matches['institution_id'].n_unique()
    unique_firms = unique_matches['GVKEY'].n_unique()

    logger.info(f"Total matches: {len(unique_matches):,}")
    logger.info(f"Unique institutions: {unique_institutions:,}")
    logger.info(f"Unique firms: {unique_firms:,}")

    # Top matched firms
    logger.info("\nTop 10 firms by number of institutions:")
    top_firms = unique_matches.group_by(['GVKEY', 'firm_conm']).agg(
        pl.len().alias('num_institutions')
    ).sort('num_institutions', descending=True).head(10)

    for row in top_firms.iter_rows(named=True):
        logger.info(f"  {row['GVKEY']:>10} | {row['firm_conm'][:40]:40s} | {row['num_institutions']:>4} institutions")

    # Sample matches
    logger.info("\nSample matches (first 10):")
    samples = unique_matches.head(10)
    for i, row in enumerate(samples.iter_rows(named=True), 1):
        logger.info(f"  {i}. {row['institution_id']:50s} -> {row['firm_conm']:40s} (ticker: {row['matched_ticker']})")

    logger.info("\n" + "=" * 80)
    logger.info("WIKIDATA TICKER MATCHING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    from datetime import datetime
    main()
