"""
Combine All Publication Matching Methods

This script combines results from all matching stages and deduplicates them.

Logic:
1. Concatenate all stage match files
2. For each (institution_id, GVKEY) pair, keep highest confidence
3. If tie: prefer earlier stage (1 > 2)
4. Quality checks: flag multi-matches
5. Output: Final dataset with summary statistics

Expected Output:
- 6,500-7,500 unique firm matches (target)
- 8,000-9,500 unique institutions
- 34.7-40.1% coverage of CRSP firms
"""

import polars as pl
from pathlib import Path
import logging
from typing import Dict, List

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"

OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_final.parquet"
SUMMARY_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_summary.txt"
PROGRESS_LOG = LOGS_DIR / "combine_publication_matches_final.log"

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

# Stage priority (lower number = higher priority)
STAGE_PRIORITY = {
    'wikidata_ticker': 1,
    'homepage_domain': 1,
    'homepage_domain_enhanced': 1,
    'contained_name': 1,
    'acronym': 1,
    'acronym_enhanced': 1,
    'fuzzy_conservative': 2,
    'parent_cascade': 3,
}


def load_all_match_files() -> pl.DataFrame:
    """Load all match files and concatenate them."""
    logger.info("\n[1/6] Loading all match files...")

    match_files = [
        'publication_firm_matches_wikidata_tickers.parquet',
        'publication_firm_matches_homepage.parquet',
        'publication_firm_matches_homepage_enhanced.parquet',
        'publication_firm_matches_contained_name.parquet',
        'publication_firm_matches_acronyms_enhanced.parquet',
        'publication_firm_matches_fuzzy_conservative.parquet',
        # Add other files as they become available
    ]

    # Core columns that must be present
    core_columns = ['GVKEY', 'LPERMNO', 'firm_conm', 'institution_id', 'institution_display_name', 'match_type', 'match_confidence', 'match_method']

    all_dfs = []
    file_stats = []

    for filename in match_files:
        filepath = DATA_PROCESSED_LINK / filename
        if not filepath.exists():
            logger.info(f"  {filename}: NOT FOUND")
            continue

        df = pl.read_parquet(filepath)

        # Select only core columns that exist
        available_cols = [col for col in core_columns if col in df.columns]
        df = df.select(available_cols)

        all_dfs.append(df)

        stats = {
            'file': filename,
            'matches': len(df),
            'unique_institutions': df['institution_id'].n_unique(),
            'unique_firms': df['GVKEY'].n_unique(),
            'mean_confidence': df['match_confidence'].mean() if 'match_confidence' in df.columns else None,
        }
        file_stats.append(stats)

        logger.info(f"  {filename}: {len(df):,} matches, {df['institution_id'].n_unique():,} institutions, {df['GVKEY'].n_unique():,} firms")

    if not all_dfs:
        raise ValueError("No match files found!")

    # Concatenate all (now they all have the same core columns)
    combined = pl.concat(all_dfs, how='vertical')

    logger.info(f"\n  Total raw matches: {len(combined):,}")

    # Show file statistics
    logger.info("\n  File Statistics:")
    logger.info("  " + "-" * 80)
    logger.info(f"  {'File':<50} {'Matches':>10} {'Inst':>8} {'Firms':>8} {'Conf':>8}")
    logger.info("  " + "-" * 80)
    for stat in file_stats:
        conf_str = f"{stat['mean_confidence']:.3f}" if stat['mean_confidence'] else "N/A"
        logger.info(f"  {stat['file']:<50} {stat['matches']:>10,} {stat['unique_institutions']:>8,} {stat['unique_firms']:>8,} {conf_str:>8}")

    return combined, file_stats


def deduplicate_matches(matches_df: pl.DataFrame) -> pl.DataFrame:
    """
    Deduplicate matches by keeping highest confidence per institution-firm pair.

    For ties, prefer earlier stage (lower stage number).
    """
    logger.info("\n[2/6] Deduplicating matches...")

    logger.info(f"  Before deduplication: {len(matches_df):,} matches")

    # Add stage priority column
    def get_stage_priority(match_type: str) -> int:
        for key, priority in STAGE_PRIORITY.items():
            if key in match_type:
                return priority
        return 999  # Unknown stage

    # Extract stage priority
    if 'match_type' in matches_df.columns:
        match_types = matches_df['match_type'].to_list()
        stage_priorities = [get_stage_priority(mt) for mt in match_types]
        matches_df = matches_df.with_columns(
            pl.Series('stage_priority', stage_priorities)
        )

    # Sort by confidence (descending), then stage_priority (ascending)
    matches_df = matches_df.sort(
        ['institution_id', 'GVKEY', 'match_confidence', 'stage_priority'],
        descending=[False, False, True, False]
    )

    # Keep first (highest confidence, lowest stage priority)
    matches_df = matches_df.unique(subset=['institution_id', 'GVKEY'], keep='first')

    logger.info(f"  After deduplication: {len(matches_df):,} matches")
    logger.info(f"  Unique institutions: {matches_df['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {matches_df['GVKEY'].n_unique():,}")

    return matches_df


def quality_checks(matches_df: pl.DataFrame) -> Dict:
    """
    Perform quality checks and flag potential issues.

    Returns:
        Dictionary with quality metrics
    """
    logger.info("\n[3/6] Performing quality checks...")

    quality_metrics = {}

    # Check 1: Institutions matched to multiple firms
    inst_counts = matches_df.group_by('institution_id').agg(pl.len().alias('num_matches'))
    multi_match_inst = inst_counts.filter(pl.col('num_matches') > 1)
    quality_metrics['multi_match_institutions'] = len(multi_match_inst)
    logger.info(f"  Institutions matched to multiple firms: {len(multi_match_inst):,} ({len(multi_match_inst)/len(inst_counts)*100:.2f}%)")

    # Check 2: Firms matched to many institutions (>10)
    firm_counts = matches_df.group_by('GVKEY').agg(pl.len().alias('num_institutions'))
    multi_match_firms = firm_counts.filter(pl.col('num_institutions') > 10)
    quality_metrics['firms_with_many_institutions'] = len(multi_match_firms)
    logger.info(f"  Firms matched to >10 institutions: {len(multi_match_firms):,}")

    # Show top firms with most institutions
    if len(multi_match_firms) > 0:
        logger.info("\n  Top 10 firms by institution count:")
        top_firms = multi_match_firms.sort('num_institutions', descending=True).head(10)
        for i, row in enumerate(top_firms.iter_rows(named=True), 1):
            # Get firm name
            firm_row = matches_df.filter(pl.col('GVKEY') == row['GVKEY']).row(0, named=True)
            logger.info(f"    {i}. {firm_row['firm_conm'][:40]:<40} {row['num_institutions']:>4} institutions")

    # Check 3: Confidence distribution
    if 'match_confidence' in matches_df.columns:
        conf_dist = matches_df['match_confidence'].describe()
        quality_metrics['confidence_mean'] = matches_df['match_confidence'].mean()
        quality_metrics['confidence_min'] = matches_df['match_confidence'].min()
        quality_metrics['confidence_max'] = matches_df['match_confidence'].max()
        logger.info(f"\n  Confidence distribution:")
        logger.info(f"    Mean: {quality_metrics['confidence_mean']:.3f}")
        logger.info(f"    Min: {quality_metrics['confidence_min']:.3f}")
        logger.info(f"    Max: {quality_metrics['confidence_max']:.3f}")

    # Check 4: Match type distribution
    if 'match_type' in matches_df.columns:
        type_counts = matches_df.group_by('match_type').agg(pl.len().alias('count'))
        type_counts = type_counts.sort('count', descending=True)
        logger.info(f"\n  Match type distribution:")
        for row in type_counts.iter_rows(named=True):
            logger.info(f"    {row['match_type']:<40} {row['count']:>6,} ({row['count']/len(matches_df)*100:>5.1f}%)")

    return quality_metrics


def calculate_coverage(matches_df: pl.DataFrame, total_crsp_firms: int = 18709) -> Dict:
    """Calculate coverage statistics."""
    logger.info("\n[4/6] Calculating coverage...")

    unique_firms = matches_df['GVKEY'].n_unique()

    coverage = {
        'unique_firms': unique_firms,
        'total_crsp_firms': total_crsp_firms,
        'coverage_percent': unique_firms / total_crsp_firms * 100,
        'unique_institutions': matches_df['institution_id'].n_unique(),
        'total_matches': len(matches_df),
    }

    logger.info(f"  Unique firms matched: {coverage['unique_firms']:,}")
    logger.info(f"  Total CRSP firms: {coverage['total_crsp_firms']:,}")
    logger.info(f"  Coverage: {coverage['coverage_percent']:.2f}%")
    logger.info(f"  Unique institutions: {coverage['unique_institutions']:,}")
    logger.info(f"  Total matches: {coverage['total_matches']:,}")

    return coverage


def save_results(matches_df: pl.DataFrame, coverage: Dict, quality_metrics: Dict, file_stats: List[Dict]):
    """Save final results and summary."""
    logger.info("\n[5/6] Saving results...")

    # Save parquet
    matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved matches to: {OUTPUT_FILE}")

    # Save text summary
    with open(SUMMARY_FILE, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("PUBLICATION FIRM MATCHING - FINAL RESULTS\n")
        f.write("=" * 80 + "\n\n")

        f.write("COVERAGE SUMMARY\n")
        f.write("-" * 40 + "\n")
        f.write(f"Unique firms matched:     {coverage['unique_firms']:,}\n")
        f.write(f"Total CRSP firms:         {coverage['total_crsp_firms']:,}\n")
        f.write(f"Coverage:                 {coverage['coverage_percent']:.2f}%\n")
        f.write(f"Unique institutions:      {coverage['unique_institutions']:,}\n")
        f.write(f"Total matches:            {coverage['total_matches']:,}\n\n")

        f.write("QUALITY METRICS\n")
        f.write("-" * 40 + "\n")
        f.write(f"Multi-match institutions: {quality_metrics.get('multi_match_institutions', 0):,}\n")
        f.write(f"Firms with >10 insts:     {quality_metrics.get('firms_with_many_institutions', 0):,}\n")
        if 'confidence_mean' in quality_metrics:
            f.write(f"Mean confidence:          {quality_metrics['confidence_mean']:.3f}\n")
            f.write(f"Min confidence:           {quality_metrics['confidence_min']:.3f}\n")
            f.write(f"Max confidence:           {quality_metrics['confidence_max']:.3f}\n")
        f.write("\n")

        f.write("INPUT FILES\n")
        f.write("-" * 40 + "\n")
        f.write(f"{'File':<50} {'Matches':>10} {'Inst':>8} {'Firms':>8}\n")
        f.write("-" * 80 + "\n")
        for stat in file_stats:
            f.write(f"{stat['file']:<50} {stat['matches']:>10,} {stat['unique_institutions']:>8,} {stat['unique_firms']:>8,}\n")
        f.write("\n")

        f.write("=" * 80 + "\n")
        f.write("Generated by: combine_publication_matches_final.py\n")
        f.write("=" * 80 + "\n")

    logger.info(f"  Saved summary to: {SUMMARY_FILE}")


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("COMBINE ALL PUBLICATION MATCHING METHODS")
    logger.info("=" * 80)

    # Step 1: Load all match files
    matches_df, file_stats = load_all_match_files()

    # Step 2: Deduplicate
    matches_df = deduplicate_matches(matches_df)

    # Step 3: Quality checks
    quality_metrics = quality_checks(matches_df)

    # Step 4: Calculate coverage
    coverage = calculate_coverage(matches_df)

    # Step 5: Save results
    save_results(matches_df, coverage, quality_metrics, file_stats)

    # Step 6: Final summary
    logger.info("\n[6/6] Final Summary")
    logger.info("=" * 80)
    logger.info(f"FINAL COVERAGE: {coverage['unique_firms']:,} firms ({coverage['coverage_percent']:.2f}% of CRSP)")
    logger.info(f"Target was: 6,500-7,500 firms (34.7-40.1%)")

    if coverage['unique_firms'] < 6000:
        logger.warning(f"⚠️  Coverage ({coverage['unique_firms']:,}) is below minimum target (6,000)")
    elif coverage['unique_firms'] < 6500:
        logger.warning(f"⚠️  Coverage ({coverage['unique_firms']:,}) is below target (6,500-7,500)")
    else:
        logger.info(f"✅ Coverage target met or exceeded!")

    logger.info("=" * 80)
    logger.info("COMBINATION COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
