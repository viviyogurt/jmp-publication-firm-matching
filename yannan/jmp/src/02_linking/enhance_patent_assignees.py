"""
Enhance Patent Assignee Data

This script creates a master table of unique patent assignees from the
patent panel, including temporal coverage and patent counts.
"""

import polars as pl
from pathlib import Path
import logging

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
LOGS_DIR = PROJECT_ROOT / "logs"

PATENT_PANEL = DATA_INTERIM / "patents_panel.parquet"
OUTPUT_FILE = DATA_INTERIM / "patent_assignees_master.parquet"
PROGRESS_LOG = LOGS_DIR / "enhance_patent_assignees.log"

DATA_INTERIM.mkdir(parents=True, exist_ok=True)
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
    logger.info("ENHANCING PATENT ASSIGNEE DATA")
    logger.info("=" * 80)
    
    # Step 1: Load patent panel
    logger.info("\n[1/3] Loading patent panel...")
    logger.info(f"Reading from: {PATENT_PANEL}")
    
    if not PATENT_PANEL.exists():
        raise FileNotFoundError(f"Patent panel file not found: {PATENT_PANEL}")
    
    patent_panel = pl.read_parquet(PATENT_PANEL)
    logger.info(f"  Loaded {len(patent_panel):,} assignee-year observations")
    
    # Step 2: Create assignee master table
    logger.info("\n[2/3] Creating assignee master table...")
    
    assignee_master = (
        patent_panel
        .group_by(['assignee_id', 'clean_name'])
        .agg([
            pl.col('total_ai_patents').sum().alias('patent_count_total'),
            pl.col('year').min().alias('first_year'),
            pl.col('year').max().alias('last_year'),
            pl.col('avg_ai_score').mean().alias('avg_ai_score_overall'),
            pl.len().alias('year_count'),  # Number of years with patents
        ])
        .sort('patent_count_total', descending=True)
    )
    
    logger.info(f"  Found {len(assignee_master):,} unique assignees")
    logger.info(f"  Total patents: {assignee_master['patent_count_total'].sum():,}")
    logger.info(f"  Year range: {assignee_master['first_year'].min()} - {assignee_master['first_year'].max()}")
    
    # Step 3: Save output
    logger.info("\n[3/3] Saving assignee master table...")
    logger.info(f"Output: {OUTPUT_FILE}")
    
    assignee_master.write_parquet(OUTPUT_FILE, compression='snappy')
    
    logger.info(f"  Saved {len(assignee_master):,} assignees")
    
    # Summary statistics
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY STATISTICS")
    logger.info("=" * 80)
    logger.info(f"Total unique assignees: {len(assignee_master):,}")
    logger.info(f"Total patents: {assignee_master['patent_count_total'].sum():,}")
    logger.info(f"Mean patents per assignee: {assignee_master['patent_count_total'].mean():.1f}")
    logger.info(f"Median patents per assignee: {assignee_master['patent_count_total'].median():.1f}")
    logger.info(f"Max patents per assignee: {assignee_master['patent_count_total'].max():,}")
    
    # Top assignees
    logger.info("\nTop 20 assignees by patent count:")
    top_assignees = assignee_master.head(20)
    for row in top_assignees.iter_rows(named=True):
        logger.info(f"  {row['clean_name'][:50]:<50} {row['patent_count_total']:>6,} patents ({row['first_year']}-{row['last_year']})")
    
    logger.info("\n" + "=" * 80)
    logger.info("ENHANCEMENT COMPLETE")
    logger.info("=" * 80)
    
    return assignee_master


if __name__ == "__main__":
    main()
