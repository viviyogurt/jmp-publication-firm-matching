"""
Create De-Duplicated Dataset - One Firm Per Institution

Keeps only the best firm match for each OpenAlex institution (institution_id) based on:
1. Highest paper count (primary)
2. Highest confidence score (secondary)

Expected result: Each institution matches to exactly ONE firm
(but one firm can match to multiple institutions - this is ALLOWED)
"""
import polars as pl
from pathlib import Path
import logging

PROJECT_ROOT = Path("/home/kurtluo/yannan/jmp")
CLEANED_MATCHES = PROJECT_ROOT / "data/processed/linking/publication_firm_matches_cleaned.parquet"
OUTPUT_FILE = PROJECT_ROOT / "data/processed/linking/publication_firm_matches_dedup.parquet"
LOG_FILE = PROJECT_ROOT / "logs/create_deduplicated_dataset.log"

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
    logger.info("CREATE DE-DUPLICATED DATASET - One Firm Per Institution")
    logger.info("=" * 80)

    # Load data
    logger.info(f"\n[1/6] Loading data from: {CLEANED_MATCHES}")
    df = pl.read_parquet(CLEANED_MATCHES)
    logger.info(f"  Loaded {len(df):,} matches")

    # Count before deduplication
    initial_count = len(df)
    logger.info(f"\n[2/6] Initial statistics:")
    logger.info(f"  Total matches: {initial_count:,}")
    logger.info(f"  Unique institutions: {df['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {df['gvkey'].n_unique():,}")
    logger.info(f"  Total papers: {df['paper_count'].sum():,}")

    # Check for duplicates (multiple firms per institution)
    inst_counts = df.group_by("institution_id").agg(pl.len().alias("num_firms"))
    insts_with_multiple = inst_counts.filter(pl.col("num_firms") > 1)
    logger.info(f"\n[3/6] Duplicate analysis:")
    logger.info(f"  Institutions with multiple firms: {len(insts_with_multiple):,}")
    logger.info(f"  Total duplicate matches: {insts_with_multiple['num_firms'].sum() - len(insts_with_multiple):,}")

    # De-duplicate: keep best firm for each institution
    logger.info(f"\n[4/6] De-duplicating (keeping best firm per institution)...")

    # Sort by paper_count (desc), confidence (desc), then select first per institution_id
    df_dedup = (
        df
        .sort(["paper_count", "confidence"], descending=[True, True])
        .group_by("institution_id", maintain_order=True)
        .first()
    )

    removed_count = initial_count - len(df_dedup)
    logger.info(f"  Removed: {removed_count:,} duplicate matches")
    logger.info(f"  Remaining: {len(df_dedup):,} matches (one firm per institution)")

    # Final statistics
    logger.info(f"\n[5/6] Final statistics:")
    logger.info(f"  Total matches: {len(df_dedup):,}")
    logger.info(f"  Unique institutions: {df_dedup['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {df_dedup['gvkey'].n_unique():,}")
    logger.info(f"  Total papers: {df_dedup['paper_count'].sum():,}")

    # Check coverage
    logger.info(f"\n[6/6] Coverage comparison:")
    logger.info(f"  Institution coverage: {df_dedup['institution_id'].n_unique():,} / 16,278 = {df_dedup['institution_id'].n_unique()/16278:.2%}")
    logger.info(f"  Firm coverage: {df_dedup['gvkey'].n_unique():,} / 18,709 = {df_dedup['gvkey'].n_unique()/18709:.2%}")

    logger.info(f"\n" + "=" * 80)
    logger.info(f"SUMMARY:")
    logger.info(f"  Initial matches:     {initial_count:,}")
    logger.info(f"  Duplicates removed:   {removed_count:,} ({removed_count/initial_count:.1%})")
    logger.info(f"  Final matches:        {len(df_dedup):,}")
    logger.info(f"  Reduction:            {removed_count/initial_count:.1%}")
    logger.info(f"  Expected accuracy:    97.6% (same as cleaned dataset)")
    logger.info("=" * 80)

    # Save deduplicated dataset
    logger.info(f"\nSaving de-duplicated dataset to: {OUTPUT_FILE}")
    df_dedup.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved successfully!")

    # Show some examples of which firms were kept for institutions with multiple matches
    logger.info(f"\nExamples of de-duplication decisions:")
    example_insts = insts_with_multiple.sort("num_firms", descending=True).head(5)["institution_id"].to_list()
    for inst_id in example_insts:
        all_matches = df.filter(pl.col("institution_id") == inst_id).sort("paper_count", descending=True)
        kept_match = df_dedup.filter(pl.col("institution_id") == inst_id)
        if len(kept_match) > 0:
            kept_inst = kept_match["display_name"][0]
            kept_firm = kept_match["conm"][0]
            kept_papers = kept_match["paper_count"][0]
            logger.info(f"\n  Institution: {kept_inst}")
            logger.info(f"    KEPT: {kept_firm} ({kept_papers:,} papers)")
            for i, row in enumerate(all_matches.iter_rows(named=True)):
                if row["conm"] == kept_firm:
                    continue
                logger.info(f"      Removed: {row['conm']} ({row['paper_count']:,} papers)")

if __name__ == "__main__":
    main()
