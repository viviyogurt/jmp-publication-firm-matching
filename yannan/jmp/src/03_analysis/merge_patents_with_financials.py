"""
Merge Patent Firm-Year Panel with Financial Data

This script merges the patent firm-year panel with Compustat financial data
(R&D, sales, assets, market cap, etc.).
"""

import polars as pl
from pathlib import Path
import logging

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_ANALYSIS = PROJECT_ROOT / "data" / "processed" / "analysis"
DATA_RAW_COMP = PROJECT_ROOT / "data" / "raw" / "compustat"
LOGS_DIR = PROJECT_ROOT / "logs"

PATENT_PANEL = DATA_PROCESSED_ANALYSIS / "patent_firm_year_panel.parquet"
FINANCIAL_DATA = DATA_RAW_COMP / "crsp_a_ccm.csv"
OUTPUT_FILE = DATA_PROCESSED_ANALYSIS / "firm_year_panel_with_patents.parquet"
PROGRESS_LOG = LOGS_DIR / "merge_patents_with_financials.log"

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
    logger.info("MERGING PATENT PANEL WITH FINANCIAL DATA")
    logger.info("=" * 80)
    
    # Step 1: Load patent panel
    logger.info("\n[1/3] Loading patent firm-year panel...")
    
    if not PATENT_PANEL.exists():
        raise FileNotFoundError(f"Patent panel file not found: {PATENT_PANEL}")
    
    patent_panel = pl.read_parquet(PATENT_PANEL)
    logger.info(f"  Loaded {len(patent_panel):,} firm-year observations")
    
    # Step 2: Load financial data
    logger.info("\n[2/3] Loading Compustat financial data...")
    
    if not FINANCIAL_DATA.exists():
        raise FileNotFoundError(f"Financial data file not found: {FINANCIAL_DATA}")
    
    financial_data = pl.read_csv(
        FINANCIAL_DATA,
        dtypes={
            'GVKEY': str,
            'LPERMNO': pl.Int64,
            'fyear': pl.Int64,
            'tic': str,
            'conm': str,
        },
        ignore_errors=True,
        truncate_ragged_lines=True
    )
    
    # Filter to primary links
    financial_data = financial_data.filter(pl.col('LINKPRIM') == 'P')
    
    logger.info(f"  Loaded {len(financial_data):,} financial records")
    
    # Select key financial variables
    key_vars = ['GVKEY', 'LPERMNO', 'fyear', 'tic', 'conm']
    
    # Add optional financial variables if they exist
    optional_vars = ['at', 'sale', 'xrd', 'mkvalt', 'prcc_f', 'csho', 'capx', 'emp']
    for var in optional_vars:
        if var in financial_data.columns:
            key_vars.append(var)
    
    financial_subset = financial_data.select(key_vars)
    logger.info(f"  Selected {len(key_vars)} variables")
    logger.info(f"  Variables: {', '.join(key_vars)}")
    
    # Step 3: Merge patent panel with financial data
    logger.info("\n[3/3] Merging patent panel with financial data...")
    
    # Cast year to match fyear type
    patent_panel = patent_panel.with_columns([
        pl.col('year').cast(pl.Int64).alias('year')
    ])
    
    # Merge on GVKEY and year (fyear in financial data)
    merged_panel = patent_panel.join(
        financial_subset,
        left_on=['GVKEY', 'year'],
        right_on=['GVKEY', 'fyear'],
        how='left',
        suffix='_financial'
    )
    
    logger.info(f"  Merged panel: {len(merged_panel):,} observations")
    logger.info(f"  Observations with financial data: {merged_panel.filter(pl.col('tic').is_not_null()).shape[0]:,}")
    
    # Calculate coverage
    coverage_pct = merged_panel.filter(pl.col('tic').is_not_null()).shape[0] / len(merged_panel) * 100
    logger.info(f"  Financial data coverage: {coverage_pct:.1f}%")
    
    # Save output
    logger.info(f"\nSaving merged panel to: {OUTPUT_FILE}")
    merged_panel.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved {len(merged_panel):,} observations")
    
    # Summary statistics
    logger.info("\n" + "=" * 80)
    logger.info("MERGED PANEL SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total observations: {len(merged_panel):,}")
    logger.info(f"Unique firms: {merged_panel['GVKEY'].n_unique():,}")
    logger.info(f"Year range: {merged_panel['year'].min()} - {merged_panel['year'].max()}")
    logger.info(f"Total patents: {merged_panel['total_ai_patents'].sum():,}")
    logger.info(f"Financial data coverage: {coverage_pct:.1f}%")
    
    # Financial variables summary (for observations with data)
    financial_vars = ['at', 'sale', 'xrd', 'mkvalt']
    available_vars = [v for v in financial_vars if v in merged_panel.columns]
    
    if available_vars:
        logger.info("\nFinancial variables (for observations with data):")
        for var in available_vars:
            non_null = merged_panel.filter(pl.col(var).is_not_null())
            if len(non_null) > 0:
                logger.info(f"  {var}: {len(non_null):,} observations, mean = {non_null[var].mean():,.0f}")
    
    logger.info("\n" + "=" * 80)
    logger.info("MERGE COMPLETE")
    logger.info("=" * 80)
    
    return merged_panel


if __name__ == "__main__":
    main()
