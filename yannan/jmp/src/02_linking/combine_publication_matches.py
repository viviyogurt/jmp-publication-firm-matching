"""
Stage 4: Combine and Deduplicate Publication-Firm Matches

This script combines Stage 1, 2, and 3 matches and creates the final match table.

Process:
1. Load Stage 1, 2, and 3 matches
2. Union all matches
3. Deduplicate: keep highest confidence per institution-firm pair
4. Tiebreaker: Stage 1 > Stage 2 > Stage 3
5. Quality checks and flags
6. Output final matches table
"""

import polars as pl
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

STAGE1_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_stage1.parquet"
STAGE2_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_stage2.parquet"
STAGE3_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_stage3.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_final.parquet"
PROGRESS_LOG = LOGS_DIR / "combine_publication_matches.log"

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
    logger.info("STAGE 4: COMBINE AND DEDUPLICATE PUBLICATION-FIRM MATCHES")
    logger.info("=" * 80)

    # Step 1: Load all stages
    logger.info("\n[1/5] Loading Stage 1, 2, and 3 matches...")

    stage1_exists = STAGE1_FILE.exists()
    stage2_exists = STAGE2_FILE.exists()
    stage3_exists = STAGE3_FILE.exists()

    if not stage1_exists:
        logger.error(f"Stage 1 file not found: {STAGE1_FILE}")
        return None

    stage1 = pl.read_parquet(STAGE1_FILE)
    logger.info(f"  Stage 1: {len(stage1):,} matches")

    all_matches = [stage1]

    if stage2_exists:
        stage2 = pl.read_parquet(STAGE2_FILE)
        logger.info(f"  Stage 2: {len(stage2):,} matches")
        all_matches.append(stage2)
    else:
        logger.warning("  Stage 2 file not found, skipping...")

    if stage3_exists:
        stage3 = pl.read_parquet(STAGE3_FILE)
        logger.info(f"  Stage 3: {len(stage3):,} matches")
        all_matches.append(stage3)
    else:
        logger.warning("  Stage 3 file not found, skipping...")

    # Step 2: Combine all stages
    logger.info("\n[2/5] Combining all stages...")

    # Add missing columns to make schemas compatible
    # Stage 1 has: 10 columns (no similarity_score, validation_flags, manual_rule)
    # Stage 2 has: 12 columns (+ similarity_score, validation_flags)
    # Stage 3 has: 11 columns (+ manual_rule)

    # Define the canonical column order
    ALL_COLUMNS = [
        'GVKEY', 'LPERMNO', 'firm_conm',
        'institution_id', 'institution_display_name', 'institution_clean_name',
        'match_type', 'match_confidence', 'match_method',
        'similarity_score', 'validation_flags', 'manual_rule',
        'institution_paper_count'
    ]

    processed_matches = []
    for df in all_matches:
        # Add similarity_score column (null for non-Stage 2)
        if 'similarity_score' not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias('similarity_score'))

        # Add validation_flags column (empty list for non-Stage 2)
        if 'validation_flags' not in df.columns:
            df = df.with_columns(pl.lit([]).cast(pl.List(pl.String)).alias('validation_flags'))

        # Add manual_rule column (null for non-Stage 3)
        if 'manual_rule' not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.String).alias('manual_rule'))

        # Reorder columns to canonical order
        df = df.select(ALL_COLUMNS)
        processed_matches.append(df)

    combined = pl.concat(processed_matches, how='vertical')
    logger.info(f"  Total matches before deduplication: {len(combined):,}")

    # Step 3: Deduplicate
    logger.info("\n[3/5] Deduplicating matches...")

    # Add stage priority for tiebreaking
    # Stage 1 = 3 (highest), Stage 2 = 2, Stage 3 = 1
    def get_stage_priority(match_type: str) -> int:
        if match_type == 'stage1':
            return 3
        elif match_type == 'stage2':
            return 2
        elif match_type == 'stage3':
            return 1
        else:
            return 0

    # Add priority column
    combined = combined.with_columns(
        pl.col('match_type').map_elements(get_stage_priority).alias('stage_priority')
    )

    # Sort by confidence (desc), then stage_priority (desc), keep first
    combined_dedup = (
        combined
        .sort(['institution_id', 'GVKEY', 'match_confidence', 'stage_priority'],
               descending=[False, False, True, True])
        .unique(subset=['institution_id', 'GVKEY'], keep='first')
        .drop('stage_priority')
    )

    logger.info(f"  After deduplication: {len(combined_dedup):,} unique institution-firm pairs")

    # Step 4: Quality checks
    logger.info("\n[4/5] Performing quality checks...")

    # Check for institutions with multiple firm matches
    inst_firm_counts = combined_dedup.group_by('institution_id').agg(
        pl.len().alias('num_firms')
    )
    multi_firm_institutions = inst_firm_counts.filter(pl.col('num_firms') > 1)
    logger.info(f"  Institutions matched to multiple firms: {len(multi_firm_institutions):,}")
    if len(multi_firm_institutions) > 0:
        logger.info("  Top 10 institutions with most firm matches:")
        top_multi = multi_firm_institutions.sort('num_firms', descending=True).head(10)
        for row in top_multi.iter_rows(named=True):
            logger.info(f"    {row['institution_id']}: {row['num_firms']} firms")

    # Check for firms with multiple institution matches (subsidiaries)
    firm_inst_counts = combined_dedup.group_by('GVKEY').agg(
        pl.len().alias('num_institutions')
    )
    multi_inst_firms = firm_inst_counts.filter(pl.col('num_institutions') > 1)
    logger.info(f"  Firms matched to multiple institutions: {len(multi_inst_firms):,}")
    if len(multi_inst_firms) > 0:
        logger.info("  Top 10 firms with most institution matches:")
        top_multi = multi_inst_firms.sort('num_institutions', descending=True).head(10)
        for row in top_multi.iter_rows(named=True):
            logger.info(f"    {row['GVKEY']}: {row['num_institutions']} institutions")

    # Add is_primary_match flag (first match for each institution is primary)
    combined_dedup = combined_dedup.with_columns(
        pl.lit(True).alias('is_primary_match')
    )

    # Step 5: Save output
    logger.info("\n[5/5] Saving final matches...")
    combined_dedup.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved {len(combined_dedup):,} matches to {OUTPUT_FILE}")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("COMBINED MATCHES SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total unique institution-firm pairs: {len(combined_dedup):,}")
    logger.info(f"Unique institutions matched: {combined_dedup['institution_id'].n_unique():,}")
    logger.info(f"Unique firms matched: {combined_dedup['GVKEY'].n_unique():,}")

    # Match type distribution
    if 'match_type' in combined_dedup.columns:
        type_counts = combined_dedup.group_by('match_type').agg(
            pl.len().alias('count')
        ).sort('count', descending=True)
        logger.info("\nMatch type distribution:")
        for row in type_counts.iter_rows(named=True):
            logger.info(f"  {row['match_type']}: {row['count']:,} matches")

    # Confidence distribution
    logger.info(f"\nConfidence statistics:")
    logger.info(f"  Mean: {combined_dedup['match_confidence'].mean():.3f}")
    logger.info(f"  Median: {combined_dedup['match_confidence'].median():.3f}")
    logger.info(f"  Min: {combined_dedup['match_confidence'].min():.3f}")
    logger.info(f"  Max: {combined_dedup['match_confidence'].max():.3f}")

    # Paper count statistics
    if 'institution_paper_count' in combined_dedup.columns:
        total_papers = combined_dedup['institution_paper_count'].sum()
        logger.info(f"\nTotal papers covered: {total_papers:,}")

    logger.info("\n" + "=" * 80)
    logger.info("STAGE 4 COMBINE COMPLETE")
    logger.info("=" * 80)

    return combined_dedup


if __name__ == "__main__":
    main()
