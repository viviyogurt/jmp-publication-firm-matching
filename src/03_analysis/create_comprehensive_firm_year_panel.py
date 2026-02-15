"""
Create Comprehensive Firm-Year Panel

This script integrates the patent panel with existing paper panel to create
a unified firm-year panel with both paper counts and patent counts.
"""

import polars as pl
from pathlib import Path
import logging

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_ANALYSIS = PROJECT_ROOT / "data" / "processed" / "analysis"
LOGS_DIR = PROJECT_ROOT / "logs"

PATENT_PANEL = DATA_PROCESSED_ANALYSIS / "firm_year_panel_with_patents.parquet"
PAPER_PANEL = DATA_PROCESSED_ANALYSIS / "firm_year_panel.parquet"  # Existing paper panel
OUTPUT_FILE = DATA_PROCESSED_ANALYSIS / "comprehensive_firm_year_panel.parquet"
PROGRESS_LOG = LOGS_DIR / "create_comprehensive_firm_year_panel.log"

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
    logger.info("CREATING COMPREHENSIVE FIRM-YEAR PANEL")
    logger.info("=" * 80)
    
    # Step 1: Load patent panel
    logger.info("\n[1/3] Loading patent panel...")
    
    if not PATENT_PANEL.exists():
        raise FileNotFoundError(f"Patent panel file not found: {PATENT_PANEL}")
    
    patent_panel = pl.read_parquet(PATENT_PANEL)
    logger.info(f"  Loaded {len(patent_panel):,} firm-year observations")
    logger.info(f"  Unique firms: {patent_panel['GVKEY'].n_unique():,}")
    logger.info(f"  Year range: {patent_panel['year'].min()} - {patent_panel['year'].max()}")
    
    # Step 2: Load paper panel (if exists)
    logger.info("\n[2/3] Loading paper panel...")
    
    paper_panel = None
    if PAPER_PANEL.exists():
        paper_panel = pl.read_parquet(PAPER_PANEL)
        logger.info(f"  Loaded {len(paper_panel):,} firm-year observations")
        logger.info(f"  Unique firms: {paper_panel['gvkey'].n_unique():,}")
    else:
        logger.warning("  Paper panel not found - creating panel with patents only")
    
    # Step 3: Merge panels
    logger.info("\n[3/3] Merging panels...")
    
    if paper_panel is not None:
        # Merge on GVKEY and year
        # Note: paper panel uses 'gvkey' (lowercase), patent panel uses 'GVKEY'
        comprehensive_panel = patent_panel.join(
            paper_panel,
            left_on=['GVKEY', 'year'],
            right_on=['gvkey', 'year'],
            how='outer',
            suffix='_paper'
        )
        
        # Standardize GVKEY column
        comprehensive_panel = comprehensive_panel.with_columns([
            pl.coalesce([pl.col('GVKEY'), pl.col('gvkey')]).alias('GVKEY')
        ]).drop('gvkey')
        
        logger.info(f"  Merged panel: {len(comprehensive_panel):,} observations")
        logger.info(f"  Observations with patents: {comprehensive_panel.filter(pl.col('total_ai_patents').is_not_null()).shape[0]:,}")
        logger.info(f"  Observations with papers: {comprehensive_panel.filter(pl.col('paper_count').is_not_null()).shape[0]:,}")
    else:
        # No paper panel - just use patent panel
        comprehensive_panel = patent_panel
        logger.info("  Using patent panel only (no paper data)")
    
    # Fill missing values with 0 for counts
    count_columns = ['total_ai_patents', 'paper_count']
    for col in count_columns:
        if col in comprehensive_panel.columns:
            comprehensive_panel = comprehensive_panel.with_columns([
                pl.col(col).fill_null(0).alias(col)
            ])
    
    # Save output
    logger.info(f"\nSaving comprehensive panel to: {OUTPUT_FILE}")
    comprehensive_panel.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved {len(comprehensive_panel):,} observations")
    
    # Summary statistics
    logger.info("\n" + "=" * 80)
    logger.info("COMPREHENSIVE PANEL SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total observations: {len(comprehensive_panel):,}")
    logger.info(f"Unique firms: {comprehensive_panel['GVKEY'].n_unique():,}")
    
    if 'year' in comprehensive_panel.columns:
        logger.info(f"Year range: {comprehensive_panel['year'].min()} - {comprehensive_panel['year'].max()}")
    
    if 'total_ai_patents' in comprehensive_panel.columns:
        total_patents = comprehensive_panel['total_ai_patents'].sum()
        logger.info(f"Total patents: {total_patents:,}")
        logger.info(f"Firm-years with patents: {comprehensive_panel.filter(pl.col('total_ai_patents') > 0).shape[0]:,}")
    
    if 'paper_count' in comprehensive_panel.columns:
        total_papers = comprehensive_panel['paper_count'].sum()
        logger.info(f"Total papers: {total_papers:,}")
        logger.info(f"Firm-years with papers: {comprehensive_panel.filter(pl.col('paper_count') > 0).shape[0]:,}")
    
    # Innovation intensity
    if 'total_ai_patents' in comprehensive_panel.columns and 'paper_count' in comprehensive_panel.columns:
        both_innovation = comprehensive_panel.filter(
            (pl.col('total_ai_patents') > 0) & (pl.col('paper_count') > 0)
        )
        logger.info(f"Firm-years with both patents and papers: {len(both_innovation):,}")
    
    logger.info("\n" + "=" * 80)
    logger.info("COMPREHENSIVE PANEL CREATION COMPLETE")
    logger.info("=" * 80)
    
    return comprehensive_panel


if __name__ == "__main__":
    main()
