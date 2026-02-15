"""
Filter Stage 1 Enhanced Matches to Remove False Positives

This script applies quality filters to remove problematic matches identified
during validation:
- C-CUBE MICROSYSTEMS: 1,031 matches (over-matching via domain)
- PLC SYSTEMS: 305 matches (over-matching generic term "PLC")
- CAMBRIDGE NEUROSCIENCE: 254 matches (over-matching .ac.uk domains)
- Multi-firm institutions: 175 institutions matched to 2+ firms

Expected output: ~6,000-7,000 high-quality matches (80-90% accuracy)
"""
import polars as pl
from pathlib import Path
import logging

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_stage1_enhanced.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_stage1_final.parquet"
PROGRESS_LOG = LOGS_DIR / "filter_stage1_matches.log"

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

# Problematic firms to remove
PROBLEMATIC_FIRMS = {
    'N/A',  # C-CUBE MICROSYSTEMS (GVKEY is None)
    '025013',  # PLC SYSTEMS INC
    # Add more if needed
}

# Problematic match methods
PROBLEMATIC_METHODS = {
    'homepage_root_domain',  # Over-matches academic institutions
    # Keep 'wikipedia_name' and 'homepage_exact' as they're higher quality
}


# ============================================================================
# Main Filtering
# ============================================================================

def main():
    """Main filtering workflow."""

    logger.info("=" * 80)
    logger.info("FILTERING STAGE 1 ENHANCED MATCHES - QUALITY IMPROVEMENT")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/6] Loading Stage 1 Enhanced matches...")
    df = pl.read_parquet(INPUT_FILE)
    logger.info(f"  Loaded {len(df):,} matches")
    logger.info(f"  Unique institutions: {df['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {df['GVKEY'].n_unique():,}")

    # Filter 1: Remove problematic firms
    logger.info("\n[2/6] Removing problematic firms...")
    df_before = len(df)

    df = df.filter(
        ~pl.col('GVKEY').is_in(PROBLEMATIC_FIRMS)
    )

    removed_firms = df_before - len(df)
    logger.info(f"  Removed {removed_firms:,} matches (problematic firms)")

    # Filter 2: Remove problematic match methods
    logger.info("\n[3/6] Removing problematic match methods...")
    df_before = len(df)

    df = df.filter(
        ~pl.col('match_method').is_in(PROBLEMATIC_METHODS)
    )

    removed_methods = df_before - len(df)
    logger.info(f"  Removed {removed_methods:,} matches (problematic methods)")

    # Filter 3: Remove institutions matched to multiple firms (ambiguous)
    logger.info("\n[4/6] Removing institutions with multiple firm matches...")
    df_before = len(df)
    df_before_inst = df['institution_id'].n_unique()

    # Count firms per institution
    inst_firm_counts = df.group_by('institution_id').agg(
        pl.len().alias('num_firms')
    )

    # Get institutions with only 1 firm
    single_firm_insts = inst_firm_counts.filter(
        pl.col('num_firms') == 1
    )['institution_id'].to_list()

    df = df.filter(
        pl.col('institution_id').is_in(single_firm_insts)
    )

    removed_multi = df_before - len(df)
    removed_insts = df_before_inst - df['institution_id'].n_unique()
    logger.info(f"  Removed {removed_multi:,} matches")
    logger.info(f"  Removed {removed_insts:,} institutions with multiple firm matches")

    # Filter 4: Apply confidence threshold
    logger.info("\n[5/6] Applying confidence threshold (≥0.95)...")
    df_before = len(df)

    df = df.filter(
        pl.col('match_confidence') >= 0.95
    )

    removed_conf = df_before - len(df)
    logger.info(f"  Removed {removed_conf:,} matches below 0.95 confidence")

    # Save filtered results
    logger.info("\n[6/6] Saving filtered results...")
    df.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved to {OUTPUT_FILE}")

    # Statistics
    logger.info("\n" + "=" * 80)
    logger.info("FILTERING RESULTS")
    logger.info("=" * 80)

    unique_institutions = df['institution_id'].n_unique()
    unique_firms = df['GVKEY'].n_unique()
    total_papers = df['institution_paper_count'].sum()

    logger.info(f"\nOriginal matches: {df_before:,}")
    logger.info(f"Final matches: {len(df):,}")
    logger.info(f"Reduction: {(df_before - len(df)):,} ({(df_before - len(df)) / df_before * 100:.1f}%)")
    logger.info(f"\nUnique institutions: {unique_institutions:,}")
    logger.info(f"Unique firms: {unique_firms:,}")
    logger.info(f"Total papers covered: {total_papers:,}")

    # Coverage of total institutions
    TOTAL_INSTITUTIONS = 115138
    coverage_pct = unique_institutions / TOTAL_INSTITUTIONS * 100
    logger.info(f"\nCoverage of all institutions: {coverage_pct:.2f}%")

    # Match type breakdown
    logger.info("\nMatch type breakdown:")
    type_counts = df.group_by('match_type').agg(
        pl.len().alias('count'),
        pl.col('institution_paper_count').sum().alias('papers')
    ).sort('count', descending=True)

    for row in type_counts.iter_rows(named=True):
        logger.info(f"  {row['match_type']:30s}: {row['count']:>6,} matches, {row['papers']:>10,} papers")

    # Top 10 firms
    logger.info("\nTop 10 firms by number of institutions:")
    top_firms = df.group_by(['GVKEY', 'firm_conm']).agg(
        pl.len().alias('num_institutions')
    ).sort('num_institutions', descending=True).head(10)

    for i, row in enumerate(top_firms.iter_rows(named=True), 1):
        logger.info(f"  {i:2}. {row['GVKEY']:>10} | {row['firm_conm'][:40]:40s} | {row['num_institutions']:>4} institutions")

    logger.info("\n" + "=" * 80)
    logger.info("FILTERING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
