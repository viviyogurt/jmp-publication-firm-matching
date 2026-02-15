"""
Create Random Sample of 1000 Matches for Manual Validation

This script randomly samples 1000 patent-firm matches for manual validation
to check if assignees are correctly linked to CRSP firms.
"""

import polars as pl
from pathlib import Path
import logging
import random

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_ANALYSIS = PROJECT_ROOT / "data" / "processed" / "analysis"
LOGS_DIR = PROJECT_ROOT / "logs"

ADJUSTED_MATCHES = DATA_PROCESSED_LINK / "patent_firm_matches_adjusted.parquet"
PATENT_ASSIGNEES_MASTER = DATA_INTERIM / "patent_assignees_master.parquet"
COMPUSTAT_FIRMS = DATA_INTERIM / "compustat_firms_standardized.parquet"
PATENT_PANEL = DATA_INTERIM / "patents_panel.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "validation_sample_1000.csv"
PROGRESS_LOG = LOGS_DIR / "create_validation_sample_1000.log"

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
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("CREATING VALIDATION SAMPLE (1000 RANDOM MATCHES)")
    logger.info("=" * 80)
    
    # Step 1: Load matches
    logger.info("\n[1/4] Loading patent-firm matches...")
    
    if not ADJUSTED_MATCHES.exists():
        raise FileNotFoundError(f"Adjusted matches file not found: {ADJUSTED_MATCHES}")
    
    matches_df = pl.read_parquet(ADJUSTED_MATCHES)
    logger.info(f"  Loaded {len(matches_df):,} total matches")
    
    # Step 2: Load additional context data
    logger.info("\n[2/4] Loading context data...")
    
    assignees_master = pl.read_parquet(PATENT_ASSIGNEES_MASTER)
    compustat_firms = pl.read_parquet(COMPUSTAT_FIRMS)
    
    # Get patent year information
    patent_panel = pl.read_parquet(PATENT_PANEL)
    assignee_years = (
        patent_panel
        .group_by('assignee_id')
        .agg([
            pl.col('year').min().alias('first_patent_year'),
            pl.col('year').max().alias('last_patent_year'),
            pl.col('total_ai_patents').sum().alias('total_patents'),
        ])
    )
    
    logger.info(f"  Loaded assignee master: {len(assignees_master):,} assignees")
    logger.info(f"  Loaded Compustat firms: {len(compustat_firms):,} firms")
    
    # Step 3: Enrich matches with context
    logger.info("\n[3/4] Enriching matches with context...")
    
    # Join with assignee master for original name
    enriched = matches_df.join(
        assignees_master.select(['assignee_id', 'clean_name', 'patent_count_total']),
        on='assignee_id',
        how='left'
    )
    
    # Join with Compustat for firm details
    enriched = enriched.join(
        compustat_firms.select(['GVKEY', 'conm', 'tic', 'state', 'city', 'busdesc']),
        on='GVKEY',
        how='left'
    )
    
    # Join with assignee years
    enriched = enriched.join(
        assignee_years,
        on='assignee_id',
        how='left'
    )
    
    logger.info(f"  Enriched {len(enriched):,} matches")
    
    # Step 4: Random sample 1000 matches
    logger.info("\n[4/4] Creating random sample...")
    
    # Stratify by match_type to ensure representation
    sample_size = 1000
    samples = []
    
    for match_type in enriched['match_type'].unique().to_list():
        type_matches = enriched.filter(pl.col('match_type') == match_type)
        type_count = len(type_matches)
        type_proportion = type_count / len(enriched)
        type_sample_size = max(1, int(sample_size * type_proportion))
        type_sample_size = min(type_sample_size, type_count)  # Don't exceed available
        
        if type_sample_size > 0:
            sampled = type_matches.sample(n=type_sample_size, seed=42)
            samples.append(sampled)
            logger.info(f"  {match_type}: {type_sample_size} samples (from {type_count:,} matches)")
    
    # Combine and ensure exactly 1000
    validation_sample = pl.concat(samples, how='diagonal')
    
    if len(validation_sample) > sample_size:
        validation_sample = validation_sample.sample(n=sample_size, seed=42)
    elif len(validation_sample) < sample_size:
        # Fill remaining with random samples
        remaining = sample_size - len(validation_sample)
        additional = enriched.sample(n=min(remaining, len(enriched)), seed=123)
        validation_sample = pl.concat([validation_sample, additional], how='diagonal')
        validation_sample = validation_sample.unique(subset=['GVKEY', 'assignee_id'], keep='first')
    
    logger.info(f"  Final sample size: {len(validation_sample):,}")
    
    # Select and order columns for easy review
    review_columns = [
        'GVKEY',
        'LPERMNO',
        'firm_conm',  # Company name from matches
        'conm',  # Company name from Compustat (may differ)
        'tic',  # Ticker
        'state',
        'city',
        'busdesc',  # Business description
        'assignee_id',
        'assignee_clean_name',  # Normalized assignee name
        'clean_name',  # From assignee master (should match)
        'match_type',
        'match_method',
        'match_confidence',
        'assignee_patent_count',
        'patent_count_total',  # Total patents for this assignee
        'first_patent_year',
        'last_patent_year',
        'total_patents',  # From patent panel
    ]
    
    # Select available columns
    available_columns = [col for col in review_columns if col in validation_sample.columns]
    validation_sample = validation_sample.select(available_columns)
    
    # Add validation columns
    validation_sample = validation_sample.with_columns([
        pl.lit('').alias('is_correct'),  # 'Yes', 'No', 'Uncertain'
        pl.lit('').alias('validation_notes'),  # Notes from reviewer
        pl.lit('').alias('reviewer_name'),  # Optional
    ])
    
    # Sort by confidence (lowest first for review priority)
    validation_sample = validation_sample.sort('match_confidence')
    
    # Add row numbers
    validation_sample = validation_sample.with_row_index('row_number', offset=1)
    
    # Reorder columns
    final_columns = ['row_number', 'is_correct', 'validation_notes', 'reviewer_name'] + [c for c in available_columns if c not in ['is_correct', 'validation_notes', 'reviewer_name']]
    validation_sample = validation_sample.select(final_columns)
    
    # Save to CSV
    logger.info(f"\nSaving validation sample to: {OUTPUT_FILE}")
    validation_sample.write_csv(OUTPUT_FILE)
    logger.info(f"  Saved {len(validation_sample):,} matches")
    
    # Summary statistics
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SAMPLE SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total samples: {len(validation_sample):,}")
    
    # By match type
    logger.info("\nSamples by match type:")
    type_counts = validation_sample.group_by('match_type').agg(pl.len().alias('count')).sort('count', descending=True)
    for row in type_counts.iter_rows(named=True):
        logger.info(f"  {row['match_type']}: {row['count']:,}")
    
    # By confidence level
    logger.info("\nSamples by confidence level:")
    validation_sample = validation_sample.with_columns([
        pl.when(pl.col('match_confidence') >= 0.98)
        .then(pl.lit('Very High (≥0.98)'))
        .when(pl.col('match_confidence') >= 0.95)
        .then(pl.lit('High (0.95-0.98)'))
        .when(pl.col('match_confidence') >= 0.90)
        .then(pl.lit('Medium (0.90-0.95)'))
        .otherwise(pl.lit('Low (<0.90)'))
        .alias('confidence_level')
    ])
    
    conf_counts = validation_sample.group_by('confidence_level').agg(pl.len().alias('count')).sort('count', descending=True)
    for row in conf_counts.iter_rows(named=True):
        logger.info(f"  {row['confidence_level']}: {row['count']:,}")
    
    logger.info(f"\nMean confidence: {validation_sample['match_confidence'].mean():.3f}")
    logger.info(f"Min confidence: {validation_sample['match_confidence'].min():.3f}")
    logger.info(f"Max confidence: {validation_sample['match_confidence'].max():.3f}")
    
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SAMPLE CREATED")
    logger.info("=" * 80)
    logger.info(f"\nFile: {OUTPUT_FILE}")
    logger.info("\nInstructions for validation:")
    logger.info("  1. Open the CSV file in Excel or similar")
    logger.info("  2. For each row, check if the assignee name matches the firm")
    logger.info("  3. Mark 'is_correct' column: 'Yes', 'No', or 'Uncertain'")
    logger.info("  4. Add notes in 'validation_notes' column if needed")
    logger.info("  5. Save and return for accuracy calculation")
    
    return validation_sample


if __name__ == "__main__":
    main()
