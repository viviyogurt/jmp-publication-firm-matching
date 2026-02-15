"""
Merge All ArXiv Datasets into One Comprehensive Dataset

This script combines multiple ArXiv datasets with overlapping coverage to create
the most comprehensive dataset possible. Uses Polars for efficient processing.

Priority order (use largest/most complete first):
1. arxiv_complete_kaggle.parquet (3.6M records, 1988-2025) - PRIMARY
2. arxiv_kaggle.parquet (2.6M records, 1988-2025)
3. claude_arxiv_complete.parquet (1.8M records, 1990-2025)
4. arxiv_2021_2025.parquet (807K records, 2021-2025)
5. claude_arxiv.parquet (1M records, 1990-2021)

Date: 2025
"""

import polars as pl
from pathlib import Path
import logging
from typing import Optional, List
import re

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"

# Input files in priority order
ARXIV_FILES = [
    DATA_RAW / "arxiv_complete_kaggle.parquet",  # Priority 1: Largest, most complete
    DATA_RAW / "arxiv_kaggle.parquet",            # Priority 2
    DATA_RAW / "claude_arxiv_complete.parquet",   # Priority 3
    DATA_RAW / "arxiv_2021_2025.parquet",         # Priority 4
    DATA_RAW / "claude_arxiv.parquet",             # Priority 5
]

# Output file
OUTPUT_FILE = DATA_PROCESSED / "merged_arxiv_complete.parquet"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "logs" / "merge_arxiv_datasets.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# Helper Functions
# ============================================================================

def standardize_arxiv_id(arxiv_id: str) -> str:
    """
    Standardize ArXiv ID format for deduplication.
    
    Handles:
    - Versioned IDs: "9201303v1" -> "9201303"
    - Old format: "9108028" -> "9108028"
    - New format: "2104.11612" -> "2104.11612"
    - With prefix: "arXiv:2104.11612" -> "2104.11612"
    - With version: "2104.11612v2" -> "2104.11612"
    """
    if not arxiv_id or arxiv_id is None:
        return None
    
    arxiv_id = str(arxiv_id).strip()
    
    # Remove "arXiv:" prefix if present
    arxiv_id = re.sub(r'^arXiv:', '', arxiv_id, flags=re.IGNORECASE)
    
    # Remove version suffix (v1, v2, etc.)
    arxiv_id = re.sub(r'v\d+$', '', arxiv_id, flags=re.IGNORECASE)
    
    return arxiv_id


def check_file_exists(file_path: Path) -> bool:
    """Check if file exists and is readable."""
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return False
    
    try:
        # Quick check: try to read schema
        pl.scan_parquet(file_path).schema
        return True
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return False


def get_file_info(file_path: Path) -> Optional[dict]:
    """Get basic information about a parquet file."""
    try:
        df = pl.scan_parquet(file_path)
        schema = df.schema
        
        # Count rows efficiently
        row_count = df.select(pl.count()).collect().item()
        
        # Check if file is empty
        if row_count == 0:
            logger.warning(f"File is empty: {file_path}")
            return None
        
        return {
            'path': file_path,
            'rows': row_count,
            'columns': len(schema),
            'column_names': list(schema.keys())
        }
    except Exception as e:
        logger.error(f"Error getting info for {file_path}: {e}")
        return None


def load_arxiv_file(file_path: Path, priority: int) -> Optional[pl.DataFrame]:
    """
    Load an ArXiv parquet file with standardization.
    
    Returns None if file is empty or cannot be read.
    """
    logger.info(f"Loading file {priority}: {file_path.name}")
    
    if not check_file_exists(file_path):
        return None
    
    file_info = get_file_info(file_path)
    if file_info is None:
        return None
    
    logger.info(f"  Rows: {file_info['rows']:,}, Columns: {file_info['columns']}")
    
    try:
        # Read file first
        df = pl.read_parquet(file_path)
        
        # Standardize arxiv_id efficiently using vectorized operations
        # Convert to string first, handling nulls
        df = df.with_columns([
            pl.when(pl.col("arxiv_id").is_not_null())
            .then(pl.col("arxiv_id").cast(pl.Utf8).str.strip())
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("arxiv_id_clean")
        ])
        
        # Remove version suffix and arXiv: prefix
        # Use str.replace with literal=False for regex matching
        df = df.with_columns([
            pl.when(pl.col("arxiv_id_clean").is_not_null())
            .then(
                pl.col("arxiv_id_clean")
                .str.replace(r"^arXiv:", "", literal=False)
                .str.replace(r"v\d+$", "", literal=False)
            )
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("arxiv_id_std")
        ])
        
        # Add metadata columns
        df = df.with_columns([
            pl.lit(priority).alias("source_priority"),
            pl.lit(file_path.name).alias("source_file")
        ])
        
        # Drop temporary column
        df = df.drop("arxiv_id_clean")
        
        # Filter out rows with null arxiv_id_std (can't deduplicate without ID)
        initial_count = len(df)
        df = df.filter(pl.col("arxiv_id_std").is_not_null())
        filtered_count = initial_count - len(df)
        
        if filtered_count > 0:
            logger.warning(f"  Filtered out {filtered_count:,} rows with null arxiv_id")
        
        logger.info(f"  Successfully loaded {len(df):,} rows")
        return df
        
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return None


# ============================================================================
# Main Processing
# ============================================================================

def merge_arxiv_datasets():
    """
    Merge all ArXiv datasets into one comprehensive dataset.
    """
    logger.info("=" * 80)
    logger.info("MERGING ARXIV DATASETS")
    logger.info("=" * 80)
    logger.info("")
    
    # Step 1: Load all available files
    logger.info("Step 1: Loading ArXiv datasets...")
    datasets = []
    
    for priority, file_path in enumerate(ARXIV_FILES, start=1):
        df = load_arxiv_file(file_path, priority)
        if df is not None and len(df) > 0:
            datasets.append(df)
            logger.info(f"  ✓ Loaded {file_path.name}: {len(df):,} rows")
        else:
            logger.info(f"  ✗ Skipped {file_path.name}")
    
    if not datasets:
        logger.error("No valid ArXiv datasets found!")
        return
    
    logger.info(f"\nLoaded {len(datasets)} datasets")
    total_rows = sum(len(df) for df in datasets)
    logger.info(f"Total rows before merging: {total_rows:,}")
    logger.info("")
    
    # Step 2: Combine all datasets
    logger.info("Step 2: Combining datasets...")
    
    # Combine all datasets (diagonal allows different schemas)
    combined = pl.concat(datasets, how="diagonal")
    
    logger.info(f"Combined dataset: {len(combined):,} rows")
    logger.info("")
    
    # Step 3: Deduplicate based on standardized arxiv_id
    logger.info("Step 3: Deduplicating by arxiv_id...")
    
    # Sort by priority (lower number = higher priority) and harvest_date (most recent first)
    # Then keep first record for each standardized arxiv_id
    combined_sorted = combined.sort(
        ["arxiv_id_std", "source_priority", "harvest_date"],
        descending=[False, False, True]
    )
    
    # Use group_by().first() to keep highest priority record for each arxiv_id
    # This is efficient for large datasets
    deduplicated = (
        combined_sorted
        .group_by("arxiv_id_std", maintain_order=True)
        .first()
    )
    
    logger.info(f"After deduplication: {len(deduplicated):,} unique papers")
    logger.info("")
    
    # Step 4: Fill missing values from other sources (for same arxiv_id)
    logger.info("Step 4: Filling missing values from other sources...")
    
    # This is complex - for now, we'll keep the highest priority record
    # Future enhancement: could merge fields from multiple sources
    
    # Step 5: Clean up and finalize
    logger.info("Step 5: Finalizing dataset...")
    
    # Remove the standardization column (keep original arxiv_id)
    # But first, ensure arxiv_id is properly set (use standardized version if original is missing)
    if "arxiv_id" in deduplicated.columns:
        final_df = deduplicated.with_columns([
            pl.when(pl.col("arxiv_id").is_null())
            .then(pl.col("arxiv_id_std"))
            .otherwise(pl.col("arxiv_id"))
            .alias("arxiv_id")
        ])
    else:
        final_df = deduplicated.with_columns([
            pl.col("arxiv_id_std").alias("arxiv_id")
        ])
    
    # Remove temporary columns
    final_df = final_df.drop(["arxiv_id_std"])
    
    # Ensure arxiv_id is the first column
    if "arxiv_id" in final_df.columns:
        cols = ["arxiv_id"] + [c for c in final_df.columns if c != "arxiv_id"]
        final_df = final_df.select(cols)
    
    # Sort by published date
    if "published" in final_df.columns:
        final_df = final_df.sort("published", nulls_last=True)
    elif "submission_date" in final_df.columns:
        final_df = final_df.sort("submission_date", nulls_last=True)
    
    # Step 6: Statistics
    logger.info("")
    logger.info("=" * 80)
    logger.info("MERGE STATISTICS")
    logger.info("=" * 80)
    logger.info(f"Final dataset size: {len(final_df):,} papers")
    logger.info(f"Columns: {len(final_df.columns)}")
    
    # Coverage by source
    if "source_file" in final_df.columns:
        source_counts = final_df.group_by("source_file").agg(pl.count().alias("count")).sort("count", descending=True)
        logger.info("\nPapers by source:")
        for row in source_counts.iter_rows(named=True):
            logger.info(f"  {row['source_file']}: {row['count']:,} papers")
    
    # Date range
    if "published" in final_df.columns:
        date_stats = final_df.select([
            pl.col("published").min().alias("min_date"),
            pl.col("published").max().alias("max_date")
        ])
        stats = date_stats.row(0)
        logger.info(f"\nDate range: {stats[0]} to {stats[1]}")
    
    # Abstract coverage
    if "abstract" in final_df.columns:
        abstract_coverage = final_df.select([
            (pl.col("abstract").is_not_null().sum() / pl.count() * 100).alias("coverage_pct")
        ]).item()
        logger.info(f"Abstract coverage: {abstract_coverage:.2f}%")
    
    logger.info("")
    
    # Step 7: Save output
    logger.info("Step 6: Saving merged dataset...")
    
    # Ensure output directory exists
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Write with compression
    final_df.write_parquet(
        OUTPUT_FILE,
        compression="snappy",
        use_pyarrow=True
    )
    
    file_size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    logger.info(f"Saved to: {OUTPUT_FILE}")
    logger.info(f"File size: {file_size_mb:.2f} MB")
    logger.info("")
    logger.info("=" * 80)
    logger.info("MERGE COMPLETE")
    logger.info("=" * 80)


def main():
    """Main entry point."""
    try:
        merge_arxiv_datasets()
    except Exception as e:
        logger.error(f"Error during merge: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
