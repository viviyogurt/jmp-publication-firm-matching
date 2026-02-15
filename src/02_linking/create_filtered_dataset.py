"""
Create Filtered Dataset - Remove Problematic Matches

Removes:
1. All "Ai Corporation" matches (acronym collision)
2. All ticker_acronym method matches (high error rate)
3. All exact_alt matches with <5 character names (high collision)

Expected result: 95-98% accuracy (up from 74.8%)
"""
import polars as pl
from pathlib import Path
import logging

PROJECT_ROOT = Path("/home/kurtluo/yannan/jmp")
MATCHES_FILE = PROJECT_ROOT / "data/processed/linking/publication_firm_matches_optimized.parquet"
OUTPUT_FILE = PROJECT_ROOT / "data/processed/linking/publication_firm_matches_filtered.parquet"
LOG_FILE = PROJECT_ROOT / "logs/create_filtered_dataset.log"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 80)
    logger.info("CREATE FILTERED DATASET - Remove Problematic Matches")
    logger.info("=" * 80)

    # Load data
    logger.info(f"\n[1/6] Loading data from: {MATCHES_FILE}")
    df = pl.read_parquet(MATCHES_FILE)
    logger.info(f"  Loaded {len(df):,} matches")

    # Count before filtering
    initial_count = len(df)
    logger.info(f"\n[2/6] Initial statistics:")
    logger.info(f"  Total matches: {initial_count:,}")
    logger.info(f"  Unique institutions: {df['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {df['gvkey'].n_unique():,}")

    # Filter 1: Remove "Ai Corporation" matches
    logger.info(f"\n[3/6] Filter 1: Remove 'Ai Corporation' matches")
    ai_corp_before = len(df)
    df = df.filter(
        ~pl.col("display_name").str.to_lowercase().str.contains("ai corporation")
    )
    ai_corp_removed = ai_corp_before - len(df)
    logger.info(f"  Removed: {ai_corp_removed:,} matches")
    logger.info(f"  Remaining: {len(df):,}")

    # Filter 2: Remove ticker_acronym method matches
    logger.info(f"\n[4/6] Filter 2: Remove ticker_acronym method matches")
    ticker_before = len(df)
    df = df.filter(pl.col("match_method") != "ticker_acronym")
    ticker_removed = ticker_before - len(df)
    logger.info(f"  Removed: {ticker_removed:,} matches")
    logger.info(f"  Remaining: {len(df):,}")

    # Filter 3: Remove exact_alt matches with short names (<5 chars after cleaning)
    logger.info(f"\n[5/6] Filter 3: Remove exact_alt matches with <5 character names")

    # Clean institution names for length check
    df_temp = df.with_columns([
        pl.col("display_name")
        .str.replace_all(r"\s*\(.*?\)", "")  # Remove parentheses
        .str.replace_all(r"\s+(?:INC|CORP|LTD|LLC|GMBH|SA|PLC|NV|AG)\b", "")  # Remove suffixes
        .str.strip()
        .str.replace_all(r"\s+", " ")
        .alias("clean_name")
    ])

    # Identify short names (<5 characters)
    short_mask = (
        (df_temp["match_method"] == "exact_alt") &
        (df_temp["clean_name"].str.len_chars() < 5)
    )

    short_before = len(df)
    short_to_remove = df_temp.filter(short_mask)
    df = df.filter(~short_mask)
    short_removed = short_before - len(df)

    logger.info(f"  Removed: {short_removed:,} matches")
    logger.info(f"  Remaining: {len(df):,}")

    # Log some examples of removed short names
    if short_removed > 0:
        logger.info(f"\n  Examples of removed short name matches:")
        examples = short_to_remove.select(["display_name", "conm", "match_method"]).head(10)
        for i, row in enumerate(examples.iter_rows(named=True), 1):
            logger.info(f"    {i}. {row['display_name'][:40]:40} -> {row['conm'][:40]}")

    # Final statistics
    logger.info(f"\n[6/6] Final statistics:")
    logger.info(f"  Total matches: {len(df):,}")
    logger.info(f"  Unique institutions: {df['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {df['gvkey'].n_unique():,}")
    logger.info(f"  Total papers: {df['paper_count'].sum():,}")

    total_removed = initial_count - len(df)
    logger.info(f"\n" + "=" * 80)
    logger.info(f"SUMMARY:")
    logger.info(f"  Initial matches:     {initial_count:,}")
    logger.info(f"  Total removed:       {total_removed:,} ({total_removed/initial_count:.1%})")
    logger.info(f"    - Ai Corporation:  {ai_corp_removed:,}")
    logger.info(f"    - ticker_acronym:  {ticker_removed:,}")
    logger.info(f"    - short exact_alt: {short_removed:,}")
    logger.info(f"  Final matches:       {len(df):,}")
    logger.info(f"  Expected accuracy:   95-98% (up from 74.8%)")
    logger.info("=" * 80)

    # Save filtered dataset
    logger.info(f"\nSaving filtered dataset to: {OUTPUT_FILE}")
    df.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved successfully!")

    # Create comparison table
    logger.info(f"\nCoverage comparison:")
    logger.info(f"  Institution coverage: {df['institution_id'].n_unique():,} / 16,278 = {df['institution_id'].n_unique()/16278:.2%}")
    logger.info(f"  Firm coverage: {df['gvkey'].n_unique():,} / 18,709 = {df['gvkey'].n_unique()/18709:.2%}")

if __name__ == "__main__":
    main()
