"""
Generate Patent Matching Summary Statistics

This script generates comprehensive summary statistics and documentation
for the patent-firm matching process.
"""

import polars as pl
from pathlib import Path
import logging
import json

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
DATA_PROCESSED_ANALYSIS = PROJECT_ROOT / "data" / "processed" / "analysis"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
LOGS_DIR = PROJECT_ROOT / "logs"

ADJUSTED_MATCHES = DATA_PROCESSED_LINK / "patent_firm_matches_adjusted.parquet"
PATENT_PANEL = DATA_PROCESSED_ANALYSIS / "firm_year_panel_with_patents.parquet"
VALIDATION_REPORT = DATA_PROCESSED_LINK / "validation_report.json"
OUTPUT_FILE = DATA_PROCESSED_ANALYSIS / "patent_matching_summary.json"
PROGRESS_LOG = LOGS_DIR / "generate_patent_matching_summary.log"

DATA_PROCESSED_ANALYSIS.mkdir(parents=True, exist_ok=True)
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
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("GENERATING PATENT MATCHING SUMMARY")
    logger.info("=" * 80)
    
    # Step 1: Load data
    logger.info("\n[1/3] Loading data...")
    
    matches_df = pl.read_parquet(ADJUSTED_MATCHES)
    patent_panel = pl.read_parquet(PATENT_PANEL)
    
    # Load validation report if available
    validation_report = {}
    if VALIDATION_REPORT.exists():
        with open(VALIDATION_REPORT, 'r') as f:
            validation_report = json.load(f)
    
    logger.info(f"  Loaded {len(matches_df):,} matches")
    logger.info(f"  Loaded {len(patent_panel):,} firm-year observations")
    
    # Step 2: Calculate summary statistics
    logger.info("\n[2/3] Calculating summary statistics...")
    
    # Match statistics
    match_stats = {
        'total_matches': len(matches_df),
        'unique_firms': int(matches_df['GVKEY'].n_unique()),
        'unique_assignees': int(matches_df['assignee_id'].n_unique()),
        'matches_by_stage': {}
    }
    
    # By stage
    for stage in matches_df['match_type'].unique().to_list():
        stage_matches = matches_df.filter(pl.col('match_type') == stage)
        match_stats['matches_by_stage'][stage] = {
            'count': len(stage_matches),
            'unique_firms': int(stage_matches['GVKEY'].n_unique()),
            'mean_confidence': float(stage_matches['match_confidence'].mean()),
        }
    
    # Panel statistics
    panel_stats = {
        'total_observations': len(patent_panel),
        'unique_firms': int(patent_panel['GVKEY'].n_unique()),
        'year_range': {
            'min': int(patent_panel['year'].min()),
            'max': int(patent_panel['year'].max()),
        },
        'total_patents': int(patent_panel['total_ai_patents'].sum()),
        'mean_patents_per_firm_year': float(patent_panel['total_ai_patents'].mean()),
        'median_patents_per_firm_year': float(patent_panel['total_ai_patents'].median()),
    }
    
    # Coverage by year
    patents_by_year = (
        patent_panel
        .group_by('year')
        .agg([
            pl.col('total_ai_patents').sum().alias('patents'),
            pl.col('GVKEY').n_unique().alias('firms'),
        ])
        .sort('year')
    )
    
    coverage_by_year = {}
    for row in patents_by_year.iter_rows(named=True):
        coverage_by_year[int(row['year'])] = {
            'patents': int(row['patents']),
            'firms': int(row['firms']),
        }
    
    # Step 3: Compile summary
    logger.info("\n[3/3] Compiling summary...")
    
    summary = {
        'matching_statistics': match_stats,
        'panel_statistics': panel_stats,
        'coverage_by_year': coverage_by_year,
        'validation': validation_report,
        'methodology': {
            'approach': 'Multi-stage matching following Arora et al. (2021)',
            'stages': [
                'Stage 1: Exact and high-confidence matches (>95% accuracy)',
                'Stage 2: Fuzzy matching with validation (85-90% accuracy)',
                'Stage 3: Manual mappings for edge cases (near 100% accuracy)',
            ],
            'm_a_adjustment': 'Conservative approach (no reassignment without M&A data)',
            'validation': 'Stratified sampling with bias testing (Lerner & Seru 2022)',
        },
    }
    
    # Save summary
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"  Saved summary to: {OUTPUT_FILE}")
    
    # Print key statistics
    logger.info("\n" + "=" * 80)
    logger.info("PATENT MATCHING SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total matches: {match_stats['total_matches']:,}")
    logger.info(f"Unique firms matched: {match_stats['unique_firms']:,}")
    logger.info(f"Unique assignees matched: {match_stats['unique_assignees']:,}")
    logger.info(f"\nPanel statistics:")
    logger.info(f"  Total firm-year observations: {panel_stats['total_observations']:,}")
    logger.info(f"  Unique firms: {panel_stats['unique_firms']:,}")
    logger.info(f"  Year range: {panel_stats['year_range']['min']} - {panel_stats['year_range']['max']}")
    logger.info(f"  Total patents: {panel_stats['total_patents']:,}")
    logger.info(f"  Mean patents per firm-year: {panel_stats['mean_patents_per_firm_year']:.2f}")
    
    if 'accuracy_by_stage' in validation_report:
        logger.info(f"\nEstimated accuracy by stage:")
        for stage, stats in validation_report['accuracy_by_stage'].items():
            logger.info(f"  {stage}: {stats['estimated_accuracy']:.1%} ({stats['total_matches']:,} matches)")
    
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY GENERATION COMPLETE")
    logger.info("=" * 80)
    
    return summary


if __name__ == "__main__":
    main()
