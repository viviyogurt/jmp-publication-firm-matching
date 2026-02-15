"""
Create Patent Firm-Year Panel

This script aggregates patent-firm matches to firm-year level,
creating a panel with patent counts per firm per year.
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
DATA_PROCESSED_ANALYSIS = PROJECT_ROOT / "data" / "processed" / "analysis"
LOGS_DIR = PROJECT_ROOT / "logs"

ADJUSTED_MATCHES = DATA_PROCESSED_LINK / "patent_firm_matches_adjusted.parquet"
PATENT_PANEL = DATA_INTERIM / "patents_panel.parquet"
OUTPUT_FILE = DATA_PROCESSED_ANALYSIS / "patent_firm_year_panel.parquet"
PROGRESS_LOG = LOGS_DIR / "create_patent_firm_year_panel.log"

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
    logger.info("CREATING PATENT FIRM-YEAR PANEL")
    logger.info("=" * 80)
    
    # Step 1: Load adjusted matches
    logger.info("\n[1/3] Loading patent-firm matches...")
    
    if not ADJUSTED_MATCHES.exists():
        raise FileNotFoundError(f"Adjusted matches file not found: {ADJUSTED_MATCHES}")
    
    matches_df = pl.read_parquet(ADJUSTED_MATCHES)
    logger.info(f"  Loaded {len(matches_df):,} matches")
    logger.info(f"  Unique firms: {matches_df['GVKEY'].n_unique():,}")
    logger.info(f"  Unique assignees: {matches_df['assignee_id'].n_unique():,}")
    
    # Step 2: Load patent panel and join
    logger.info("\n[2/3] Loading patent panel and creating firm-year aggregation...")
    
    if not PATENT_PANEL.exists():
        raise FileNotFoundError(f"Patent panel file not found: {PATENT_PANEL}")
    
    patent_panel = pl.read_parquet(PATENT_PANEL)
    logger.info(f"  Loaded {len(patent_panel):,} assignee-year observations")
    
    # Join patent panel with matches to get GVKEY
    patent_with_firms = patent_panel.join(
        matches_df.select(['assignee_id', 'GVKEY', 'LPERMNO', 'firm_conm', 'match_confidence']),
        on='assignee_id',
        how='inner'
    )
    
    logger.info(f"  Patents matched to firms: {len(patent_with_firms):,}")
    
    # Step 3: Aggregate to firm-year level
    logger.info("\n[3/3] Aggregating to firm-year level...")
    
    firm_year_panel = (
        patent_with_firms
        .group_by(['GVKEY', 'LPERMNO', 'firm_conm', 'year'])
        .agg([
            pl.col('total_ai_patents').sum().alias('total_ai_patents'),
            pl.col('avg_ai_score').mean().alias('avg_ai_score'),
            pl.col('assignee_id').n_unique().alias('unique_assignees'),
            pl.col('match_confidence').mean().alias('avg_match_confidence'),
        ])
        .sort(['GVKEY', 'year'])
    )
    
    logger.info(f"  Created {len(firm_year_panel):,} firm-year observations")
    logger.info(f"  Unique firms: {firm_year_panel['GVKEY'].n_unique():,}")
    logger.info(f"  Year range: {firm_year_panel['year'].min()} - {firm_year_panel['year'].max()}")
    
    # Save output
    logger.info(f"\nSaving firm-year panel to: {OUTPUT_FILE}")
    firm_year_panel.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved {len(firm_year_panel):,} firm-year observations")
    
    # Summary statistics
    logger.info("\n" + "=" * 80)
    logger.info("FIRM-YEAR PANEL SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total firm-year observations: {len(firm_year_panel):,}")
    logger.info(f"Unique firms: {firm_year_panel['GVKEY'].n_unique():,}")
    logger.info(f"Year range: {firm_year_panel['year'].min()} - {firm_year_panel['year'].max()}")
    logger.info(f"Total patents: {firm_year_panel['total_ai_patents'].sum():,}")
    logger.info(f"Mean patents per firm-year: {firm_year_panel['total_ai_patents'].mean():.2f}")
    logger.info(f"Median patents per firm-year: {firm_year_panel['total_ai_patents'].median():.1f}")
    logger.info(f"Max patents per firm-year: {firm_year_panel['total_ai_patents'].max():,}")
    
    # Top firms by total patents
    logger.info("\nTop 20 firms by total patents:")
    top_firms = (
        firm_year_panel
        .group_by(['GVKEY', 'firm_conm'])
        .agg(pl.col('total_ai_patents').sum().alias('total_patents'))
        .sort('total_patents', descending=True)
        .head(20)
    )
    
    for row in top_firms.iter_rows(named=True):
        logger.info(f"  {row['firm_conm'][:50]:<50} {row['total_patents']:>6,} patents")
    
    logger.info("\n" + "=" * 80)
    logger.info("PANEL CREATION COMPLETE")
    logger.info("=" * 80)
    
    return firm_year_panel


if __name__ == "__main__":
    main()
