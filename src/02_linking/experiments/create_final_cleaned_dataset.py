"""
Create Final Cleaned Dataset - Remove Problematic Matches

Removes:
1. All 29 exact_alt method matches (89.7% error rate)
2. 6 specific incorrect homepage_exact matches

Expected result: 98.5% accuracy (up from 95.4%)
"""
import polars as pl
from pathlib import Path
import logging

PROJECT_ROOT = Path("/home/kurtluo/yannan/jmp")
FILTERED_MATCHES = PROJECT_ROOT / "data/processed/linking/publication_firm_matches_filtered.parquet"
OUTPUT_FILE = PROJECT_ROOT / "data/processed/linking/publication_firm_matches_cleaned.parquet"
LOG_FILE = PROJECT_ROOT / "logs/create_final_cleaned_dataset.log"

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
    logger.info("CREATE FINAL CLEANED DATASET - Remove Problematic Matches")
    logger.info("=" * 80)

    # Load data
    logger.info(f"\n[1/5] Loading data from: {FILTERED_MATCHES}")
    df = pl.read_parquet(FILTERED_MATCHES)
    logger.info(f"  Loaded {len(df):,} matches")

    # Count before filtering
    initial_count = len(df)
    logger.info(f"\n[2/5] Initial statistics:")
    logger.info(f"  Total matches: {initial_count:,}")
    logger.info(f"  Unique institutions: {df['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {df['gvkey'].n_unique():,}")

    # Filter 1: Remove all exact_alt method matches
    logger.info(f"\n[3/5] Filter 1: Remove all exact_alt method matches")
    exact_alt_before = len(df)
    df = df.filter(pl.col("match_method") != "exact_alt")
    exact_alt_removed = exact_alt_before - len(df)
    logger.info(f"  Removed: {exact_alt_removed:,} matches")
    logger.info(f"  Remaining: {len(df):,}")

    # Filter 2: Remove 6 specific incorrect homepage_exact matches
    logger.info(f"\n[4/5] Filter 2: Remove 6 specific incorrect homepage_exact matches")

    # Define the 6 incorrect matches to remove
    # Format: (institution_id, gvkey, institution_name)
    incorrect_matches = [
        ("https://openalex.org/I4210158356", "160282", "Andritz (Germany)"),  # Andritz → XERIUM (wrong)
        ("https://openalex.org/I4210161497", "004371", "BASF (United Kingdom)"),  # BASF → ENGELHARD (wrong)
        ("https://openalex.org/I4210148404", "017688", "Bristol-Myers Squibb (Japan)"),  # BMS → RECEPTOS (wrong)
        ("https://openalex.org/I4210161051", "186342", "Duro Felguera (Spain)"),  # Duro → LinkedIn (wrong)
        ("https://openalex.org/I4405258830", "039752", "Gen Digital Inc. (United States)"),  # Gen Digital → MoneyLion (wrong)
        ("https://openalex.org/I4210097976", "015487", "Nabors Industries (United States)"),  # Nabors → Pool Energy (wrong)
    ]

    # Get the GVKEYs to remove
    gvkeys_to_remove = [gvkey for _, gvkey, _ in incorrect_matches]

    # Filter out these specific matches
    before_filter2 = len(df)
    df = df.filter(~pl.col("gvkey").is_in(gvkeys_to_remove))
    specific_removed = before_filter2 - len(df)

    logger.info(f"  Removed: {specific_removed} specific incorrect matches")
    logger.info(f"  Remaining: {len(df):,}")

    # Log the 6 removed matches
    logger.info(f"\n  Removed specific matches:")
    for i, (inst_id, gvkey, inst_name) in enumerate(incorrect_matches, 1):
        logger.info(f"    {i}. {inst_name}")
        logger.info(f"       GVKEY: {gvkey} → REMOVED")

    # Final statistics
    logger.info(f"\n[5/5] Final statistics:")
    logger.info(f"  Total matches: {len(df):,}")
    logger.info(f"  Unique institutions: {df['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {df['gvkey'].n_unique():,}")
    logger.info(f"  Total papers: {df['paper_count'].sum():,}")

    total_removed = initial_count - len(df)
    logger.info(f"\n" + "=" * 80)
    logger.info(f"SUMMARY:")
    logger.info(f"  Initial matches:     {initial_count:,}")
    logger.info(f"  Total removed:       {total_removed:,} ({total_removed/initial_count:.1%})")
    logger.info(f"    - exact_alt:        {exact_alt_removed:,}")
    logger.info(f"    - specific wrong:   {specific_removed}")
    logger.info(f"  Final matches:       {len(df):,}")
    logger.info(f"  Expected accuracy:   98.5% (up from 95.4%)")
    logger.info("=" * 80)

    # Save cleaned dataset
    logger.info(f"\nSaving cleaned dataset to: {OUTPUT_FILE}")
    df.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved successfully!")

    # Coverage comparison
    logger.info(f"\nCoverage comparison:")
    logger.info(f"  Institution coverage: {df['institution_id'].n_unique():,} / 16,278 = {df['institution_id'].n_unique()/16278:.2%}")
    logger.info(f"  Firm coverage: {df['gvkey'].n_unique():,} / 18,709 = {df['gvkey'].n_unique()/18709:.2%}")

if __name__ == "__main__":
    main()
