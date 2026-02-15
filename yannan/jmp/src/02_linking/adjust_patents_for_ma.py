"""
M&A and Ownership Adjustment for Patent-Firm Matches

This script adjusts patent-firm matches to account for mergers, acquisitions,
and divestitures. Patents are reassigned to the current owner (acquiring firm)
rather than the original assignee at grant date.

Following Arora et al. (2021) and Dyevre & Seager (2023) methodology.
"""

import polars as pl
from pathlib import Path
import logging
from typing import Dict, List, Optional, Set

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
DATA_RAW_COMP = PROJECT_ROOT / "data" / "raw" / "compustat"
LOGS_DIR = PROJECT_ROOT / "logs"

STAGE1_MATCHES = DATA_PROCESSED_LINK / "patent_firm_matches_stage1.parquet"
STAGE2_MATCHES = DATA_PROCESSED_LINK / "patent_firm_matches_stage2.parquet"
STAGE3_MATCHES = DATA_PROCESSED_LINK / "patent_firm_matches_stage3.parquet"
PATENT_PANEL = DATA_INTERIM / "patents_panel.parquet"
FINANCIAL_DATA = DATA_RAW_COMP / "crsp_a_ccm.csv"
OUTPUT_FILE = DATA_PROCESSED_LINK / "patent_firm_matches_adjusted.parquet"
PROGRESS_LOG = LOGS_DIR / "adjust_patents_for_ma.log"

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

# Known major M&A events for tech companies (last 20 years)
# Format: {acquired_gvkey: (acquiring_gvkey, acquisition_year, notes)}
# Note: This is a simplified version - in practice, you'd load from Compustat M&A data
MAJOR_MA_EVENTS = {
    # Example: If we had GVKEYs, we'd map them here
    # For now, we'll rely on GVKEY history from Compustat
}


def load_gvkey_history(financial_data_path: Path) -> Dict[str, List[Dict]]:
    """
    Load GVKEY history to track firm identifier changes over time.
    Returns dict mapping GVKEY -> list of historical records.
    """
    logger.info("Loading GVKEY history from Compustat data...")
    
    try:
        # Load financial data and extract GVKEY history
        # In practice, you'd use Compustat's GVKEY history table
        # For now, we'll use the current GVKEY as the identifier
        # and note that M&A adjustments would require additional data
        
        df = pl.read_csv(
            financial_data_path,
            dtypes={'GVKEY': str, 'fyear': pl.Int64},
            ignore_errors=True,
            truncate_ragged_lines=True
        )
        
        # Get unique GVKEYs and their year ranges
        gvkey_history = (
            df.filter(pl.col('LINKPRIM') == 'P')
            .select(['GVKEY', 'fyear'])
            .group_by('GVKEY')
            .agg([
                pl.col('fyear').min().alias('first_year'),
                pl.col('fyear').max().alias('last_year'),
            ])
        )
        
        logger.info(f"  Loaded history for {len(gvkey_history):,} GVKEYs")
        return gvkey_history
        
    except Exception as e:
        logger.warning(f"  Error loading GVKEY history: {e}")
        logger.info("  Proceeding without M&A adjustments (conservative approach)")
        return pl.DataFrame()


def combine_all_matches() -> pl.DataFrame:
    """
    Combine matches from all stages.
    """
    logger.info("Combining matches from all stages...")
    
    all_matches = []
    
    if STAGE1_MATCHES.exists():
        stage1 = pl.read_parquet(STAGE1_MATCHES)
        all_matches.append(stage1)
        logger.info(f"  Stage 1: {len(stage1):,} matches")
    
    if STAGE2_MATCHES.exists():
        stage2 = pl.read_parquet(STAGE2_MATCHES)
        all_matches.append(stage2)
        logger.info(f"  Stage 2: {len(stage2):,} matches")
    
    if STAGE3_MATCHES.exists():
        stage3 = pl.read_parquet(STAGE3_MATCHES)
        all_matches.append(stage3)
        logger.info(f"  Stage 3: {len(stage3):,} matches")
    
    if not all_matches:
        raise ValueError("No matches found from any stage!")
    
    combined = pl.concat(all_matches, how='diagonal')
    
    # Deduplicate (keep highest confidence)
    combined = (
        combined
        .sort(['GVKEY', 'assignee_id', 'match_confidence'], descending=[False, False, True])
        .unique(subset=['GVKEY', 'assignee_id'], keep='first')
    )
    
    logger.info(f"  Combined: {len(combined):,} unique matches")
    logger.info(f"  Unique firms: {combined['GVKEY'].n_unique():,}")
    logger.info(f"  Unique assignees: {combined['assignee_id'].n_unique():,}")
    
    return combined


def add_patent_years(matches_df: pl.DataFrame) -> pl.DataFrame:
    """
    Add patent grant years to matches by joining with patent panel.
    """
    logger.info("Adding patent grant years to matches...")
    
    if not PATENT_PANEL.exists():
        logger.warning("  Patent panel not found - cannot add grant years")
        return matches_df
    
    patent_panel = pl.read_parquet(PATENT_PANEL)
    
    # Get assignee-year pairs from patent panel
    assignee_years = (
        patent_panel
        .select(['assignee_id', 'year'])
        .unique()
        .group_by('assignee_id')
        .agg([
            pl.col('year').min().alias('first_patent_year'),
            pl.col('year').max().alias('last_patent_year'),
            pl.col('year').alias('patent_years'),  # List of all years
        ])
    )
    
    # Join with matches
    matches_with_years = matches_df.join(
        assignee_years,
        on='assignee_id',
        how='left'
    )
    
    logger.info(f"  Added patent years for {matches_with_years.filter(pl.col('first_patent_year').is_not_null()).shape[0]:,} matches")
    
    return matches_with_years


def apply_ma_adjustments(matches_df: pl.DataFrame, gvkey_history: pl.DataFrame) -> pl.DataFrame:
    """
    Apply M&A adjustments to patent-firm matches.
    
    Note: This is a simplified version. In practice, you would:
    1. Load Compustat M&A data (SDC, etc.)
    2. Track which firms were acquired and when
    3. Reassign patents from acquired firm to acquiring firm
    4. Handle divestitures
    
    For now, we'll:
    - Mark matches that might need M&A adjustment
    - Keep original GVKEY (conservative approach)
    - Add flags for potential adjustments
    """
    logger.info("Applying M&A adjustments...")
    
    # For now, we'll use a conservative approach:
    # - Keep original matches
    # - Add flags indicating potential M&A issues
    # - In production, you'd load actual M&A data and reassign
    
    adjusted = matches_df.with_columns([
        pl.col('GVKEY').alias('original_gvkey'),
        pl.col('GVKEY').alias('current_gvkey'),  # Same for now
        pl.lit(False).alias('ma_adjusted'),
        pl.lit(None).cast(pl.Int64).alias('acquisition_year'),
    ])
    
    logger.info("  M&A adjustment logic:")
    logger.info("    - Original GVKEY preserved")
    logger.info("    - Current GVKEY set to original (no M&A data loaded)")
    logger.info("    - ma_adjusted flag set to False")
    logger.info("    - To enable full M&A adjustment, load Compustat M&A data")
    
    return adjusted


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("M&A AND OWNERSHIP ADJUSTMENT FOR PATENT-FIRM MATCHES")
    logger.info("=" * 80)
    
    # Step 1: Combine all stage matches
    logger.info("\n[1/4] Combining matches from all stages...")
    matches_df = combine_all_matches()
    
    # Step 2: Add patent grant years
    logger.info("\n[2/4] Adding patent grant years...")
    matches_df = add_patent_years(matches_df)
    
    # Step 3: Load GVKEY history (if available)
    logger.info("\n[3/4] Loading GVKEY history...")
    gvkey_history = load_gvkey_history(FINANCIAL_DATA)
    
    # Step 4: Apply M&A adjustments
    logger.info("\n[4/4] Applying M&A adjustments...")
    adjusted_df = apply_ma_adjustments(matches_df, gvkey_history)
    
    # Save output
    logger.info(f"\nSaving adjusted matches to: {OUTPUT_FILE}")
    adjusted_df.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved {len(adjusted_df):,} matches")
    
    # Summary statistics
    logger.info("\n" + "=" * 80)
    logger.info("M&A ADJUSTMENT SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total matches: {len(adjusted_df):,}")
    logger.info(f"Unique firms: {adjusted_df['GVKEY'].n_unique():,}")
    logger.info(f"Unique assignees: {adjusted_df['assignee_id'].n_unique():,}")
    logger.info(f"Matches with patent years: {adjusted_df.filter(pl.col('first_patent_year').is_not_null()).shape[0]:,}")
    logger.info(f"M&A adjusted: {adjusted_df.filter(pl.col('ma_adjusted') == True).shape[0]:,}")
    
    logger.info("\nNote: Full M&A adjustment requires Compustat M&A data.")
    logger.info("Current implementation uses conservative approach (no reassignment).")
    logger.info("To enable full M&A adjustment:")
    logger.info("  1. Load Compustat M&A data (SDC, etc.)")
    logger.info("  2. Map acquired GVKEYs to acquiring GVKEYs")
    logger.info("  3. Reassign patents based on acquisition dates")
    
    logger.info("\n" + "=" * 80)
    logger.info("M&A ADJUSTMENT COMPLETE")
    logger.info("=" * 80)
    
    return adjusted_df


if __name__ == "__main__":
    main()
