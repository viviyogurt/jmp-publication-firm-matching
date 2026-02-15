"""
Acronym-to-Ticker Matching

This script matches institutions to firms using acronyms.
It matches display_name_acronyms from institutions to firm tic (ticker) and name_variants.

Confidence: 0.97 (high - direct ticker match)
"""

import polars as pl
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_ENRICHED = DATA_INTERIM / "publication_institutions_enriched.parquet"
COMPUSTAT_FIRMS = DATA_INTERIM / "compustat_firms_standardized.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_acronyms.parquet"
PROGRESS_LOG = LOGS_DIR / "match_acronyms.log"

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


def main():
    logger.info("=" * 80)
    logger.info("ACRONYM-TO-TICKER MATCHING")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/4] Loading data...")
    institutions = pl.read_parquet(INSTITUTIONS_ENRICHED)
    firms = pl.read_parquet(COMPUSTAT_FIRMS)

    logger.info(f"  Loaded {len(institutions)} institutions")
    logger.info(f"  Loaded {len(firms)} firms")

    # Get institutions with acronyms
    inst_with_acronyms = institutions.filter(pl.col('acronyms').list.len() > 0)
    logger.info(f"  {len(inst_with_acronyms)} institutions have acronyms")

    # Get matched IDs
    import os
    matched_ids = set()

    stage1_file = DATA_PROCESSED_LINK / "publication_firm_matches_stage1.parquet"
    if stage1_file.exists():
        stage1 = pl.read_parquet(stage1_file)
        matched_ids.update(stage1['institution_id'].to_list())

    parent_file = DATA_PROCESSED_LINK / "publication_firm_matches_parent_cascade.parquet"
    if parent_file.exists():
        parent_matches = pl.read_parquet(parent_file)
        matched_ids.update(parent_matches['institution_id'].to_list())

    homepage_file = DATA_PROCESSED_LINK / "publication_firm_matches_homepage.parquet"
    if homepage_file.exists():
        homepage_matches = pl.read_parquet(homepage_file)
        matched_ids.update(homepage_matches['institution_id'].to_list())

    logger.info(f"  Institutions already matched: {len(matched_ids)}")

    # Prepare firms ticker data
    logger.info("\n[2/4] Preparing firm ticker data...")

    # Build firm ticker lookup (including name_variants)
    firm_tickers = []
    for row in firms.iter_rows(named=True):
        gvkey = row['GVKEY']
        conm = row['conm']
        tic = row['tic']
        lpermno = row.get('LPERMNO')
        name_variants = row.get('name_variants') or []

        if tic:
            firm_tickers.append({
                'GVKEY': gvkey,
                'LPERMNO': lpermno,
                'firm_conm': conm,
                'ticker': tic.upper(),
                'source': 'tic'
            })

        # Also add name_variants that look like tickers (short, all caps)
        for variant in name_variants:
            if variant and len(variant) <= 6 and variant.isupper():
                firm_tickers.append({
                    'GVKEY': gvkey,
                    'LPERMNO': lpermno,
                    'firm_conm': conm,
                    'ticker': variant.upper(),
                    'source': 'name_variant'
                })

    firm_tickers_df = pl.DataFrame(firm_tickers)
    logger.info(f"  {len(firm_tickers_df)} firm ticker entries")

    # Run matching
    logger.info("\n[3/4] Running acronym matching...")

    all_matches = []
    total = len(inst_with_acronyms)

    for i, inst_row in enumerate(inst_with_acronyms.iter_rows(named=True)):
        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} institutions ({len(all_matches)} matches so far)...")

        inst_id = inst_row['institution_id']
        display_name = inst_row['display_name']
        acronyms = inst_row['acronyms'] or []
        paper_count = inst_row['paper_count']

        # Skip if already matched
        if inst_id in matched_ids:
            continue

        # Match each acronym to firm tickers
        for acronym in acronyms:
            acronym_upper = acronym.upper()

            # Find matching firm tickers
            ticker_matches = firm_tickers_df.filter(
                pl.col('ticker') == acronym_upper
            )

            for firm_row in ticker_matches.iter_rows(named=True):
                all_matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row['LPERMNO'],
                    'firm_conm': firm_row['firm_conm'],
                    'institution_id': inst_id,
                    'institution_display_name': display_name,
                    'match_type': 'acronym',
                    'match_confidence': 0.97,
                    'match_method': f"acronym_to_{firm_row['source']}",
                    'matched_acronym': acronym,
                    'matched_ticker': firm_row['ticker'],
                    'institution_paper_count': paper_count,
                })

            # Only match first acronym that finds a firm
            if ticker_matches.shape[0] > 0:
                break

    logger.info(f"  Completed. Found {len(all_matches)} total matches")

    # Save output
    logger.info("\n[4/4] Saving matches...")

    if not all_matches:
        logger.warning("  No matches found!")
    else:
        matches_df = pl.DataFrame(all_matches)
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df)} matches to {OUTPUT_FILE}")

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("ACRONYM MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df)}")
        logger.info(f"Unique institutions: {matches_df['institution_id'].n_unique()}")
        logger.info(f"Unique firms: {matches_df['GVKEY'].n_unique()}")

        # Show examples
        logger.info("\nExample matches:")
        for i, row in enumerate(matches_df.head(20).iter_rows(named=True), 1):
            logger.info(f"  {i}. {row['institution_display_name'][:50]}")
            logger.info(f"     Acronym: {row['matched_acronym']} → Ticker: {row['matched_ticker']}")
            logger.info(f"     → {row['firm_conm'][:50]}")

    logger.info("\n" + "=" * 80)
    logger.info("ACRONYM MATCHING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
