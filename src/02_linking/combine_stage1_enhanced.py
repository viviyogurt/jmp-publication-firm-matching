"""
Combine all Stage 1 enhanced matches from multiple sources.

This script combines:
1. Original Stage 1 exact matches
2. Parent cascade matches
3. Smart URL matches

It handles schema differences, removes duplicates, and tracks the source of each match.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Define file paths
BASE_DIR = Path("/home/kurtluo/yannan/jmp")
DATA_DIR = BASE_DIR / "data/processed/linking"

INPUT_FILES = {
    "stage1": DATA_DIR / "publication_firm_matches_stage1.parquet",
    "parent_cascade": DATA_DIR / "publication_firm_matches_parent_cascade.parquet",
    "smart_urls": DATA_DIR / "publication_firm_matches_smart_urls_filtered.parquet"  # Use filtered to avoid 67% false positives
}

OUTPUT_FILE = DATA_DIR / "publication_firm_matches_stage1_enhanced.parquet"


def load_match_file(file_path: Path, source_name: str) -> Optional[pl.DataFrame]:
    """
    Load a match file if it exists, add match_source column.

    Args:
        file_path: Path to the parquet file
        source_name: Name of the match source to add as column

    Returns:
        DataFrame with match_source column, or None if file doesn't exist
    """
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return None

    try:
        logger.info(f"Loading {source_name} from {file_path}")
        df = pl.read_parquet(file_path)
        df = df.with_columns(
            pl.lit(source_name).alias("match_source")
        )
        logger.info(f"  Loaded {len(df)} matches from {source_name}")
        return df
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return None


def get_common_schema(dataframes: List[pl.DataFrame]) -> List[str]:
    """
    Get the union of all columns across dataframes.

    Args:
        dataframes: List of DataFrames to compare

    Returns:
        List of unique column names across all DataFrames
    """
    all_columns = set()
    for df in dataframes:
        all_columns.update(df.columns)
    return sorted(list(all_columns))


def normalize_schema(df: pl.DataFrame, common_columns: List[str], reference_schema: Dict[str, pl.DataType]) -> pl.DataFrame:
    """
    Add missing columns to DataFrame with properly typed null values.

    Args:
        df: DataFrame to normalize
        common_columns: List of columns that should exist
        reference_schema: Schema dict with column names and their target types

    Returns:
        DataFrame with all common columns
    """
    missing_cols = set(common_columns) - set(df.columns)
    if missing_cols:
        logger.debug(f"  Adding missing columns: {missing_cols}")
        for col in missing_cols:
            # Use the correct dtype from reference schema
            dtype = reference_schema.get(col, pl.String)
            df = df.with_columns(
                pl.lit(None, dtype=dtype).alias(col)
            )
    return df.select(common_columns)


def remove_duplicates(df: pl.DataFrame) -> pl.DataFrame:
    """
    Remove duplicate institution_id + GVKEY pairs, keeping highest confidence.

    Args:
        df: DataFrame with potential duplicates

    Returns:
        DataFrame with duplicates removed
    """
    logger.info("Removing duplicate institution_id + GVKEY pairs...")

    # Count before deduplication
    total_before = len(df)
    unique_pairs_before = df.select(["institution_id", "GVKEY"]).n_unique()

    # Sort by match_confidence descending, then drop duplicates
    df_deduped = df.sort(
        ["match_confidence", "match_source"],
        descending=[True, False]
    ).unique(
        subset=["institution_id", "GVKEY"],
        keep="first"
    )

    # Count after deduplication
    total_after = len(df_deduped)
    unique_pairs_after = df_deduped.select(["institution_id", "GVKEY"]).n_unique()

    removed = total_before - total_after
    logger.info(f"  Removed {removed} duplicate matches")
    logger.info(f"  Total matches: {total_before} -> {total_after}")
    logger.info(f"  Unique institution-GVKEY pairs: {unique_pairs_before} -> {unique_pairs_after}")

    return df_deduped


def log_statistics(df: pl.DataFrame) -> None:
    """
    Log comprehensive statistics about the combined dataset.

    Args:
        df: Combined DataFrame
    """
    logger.info("=" * 60)
    logger.info("COMBINED STAGE 1 ENHANCED MATCHES - STATISTICS")
    logger.info("=" * 60)

    # Total matches
    total_matches = len(df)
    logger.info(f"Total matches: {total_matches:,}")

    # Unique institutions
    unique_institutions = df["institution_id"].n_unique()
    logger.info(f"Unique institutions: {unique_institutions:,}")

    # Unique firms
    unique_firms = df["GVKEY"].n_unique()
    logger.info(f"Unique firms (GVKEYs): {unique_firms:,}")

    # Unique institution-GVKEY pairs
    unique_pairs = df.select(["institution_id", "GVKEY"]).n_unique()
    logger.info(f"Unique institution-GVKEY pairs: {unique_pairs:,}")

    # Breakdown by match source
    logger.info("\nBreakdown by match source:")
    source_stats = df.group_by("match_source").agg(
        pl.len().alias("count"),
        pl.col("institution_id").n_unique().alias("unique_institutions"),
        pl.col("GVKEY").n_unique().alias("unique_firms"),
        pl.col("match_confidence").mean().alias("avg_confidence")
    ).sort("count", descending=True)

    for row in source_stats.iter_rows(named=True):
        logger.info(
            f"  {row['match_source']:15s}: "
            f"{row['count']:,} matches, "
            f"{row['unique_institutions']:,} institutions, "
            f"{row['unique_firms']:,} firms, "
            f"avg confidence: {row['avg_confidence']:.3f}"
        )

    # Breakdown by match_type
    logger.info("\nBreakdown by match_type:")
    type_stats = df.group_by("match_type").agg(
        pl.len().alias("count")
    ).sort("count", descending=True)

    for row in type_stats.iter_rows(named=True):
        logger.info(f"  {row['match_type']:20s}: {row['count']:,} matches")

    # Confidence distribution
    logger.info("\nConfidence distribution:")
    confidence_stats = df.select(
        pl.col("match_confidence").min().alias("min"),
        pl.col("match_confidence").max().alias("max"),
        pl.col("match_confidence").mean().alias("mean"),
        pl.col("match_confidence").median().alias("median")
    ).row(0)

    logger.info(
        f"  Min: {confidence_stats[0]:.3f}, "
        f"Max: {confidence_stats[1]:.3f}, "
        f"Mean: {confidence_stats[2]:.3f}, "
        f"Median: {confidence_stats[3]:.3f}"
    )

    logger.info("=" * 60)


def main():
    """Main execution function."""
    logger.info("Starting Stage 1 enhanced match combination...")
    logger.info(f"Output directory: {DATA_DIR}")

    # Load all available match files
    dataframes = []
    for source_name, file_path in INPUT_FILES.items():
        df = load_match_file(file_path, source_name)
        if df is not None:
            dataframes.append(df)

    if not dataframes:
        logger.error("No match files found! Exiting.")
        return

    logger.info(f"Loaded {len(dataframes)} match file(s)")

    # Get common schema across all dataframes
    common_columns = get_common_schema(dataframes)
    logger.info(f"Common schema includes {len(common_columns)} columns: {common_columns}")

    # Build reference schema by merging schemas from all dataframes
    reference_schema = {}
    for df in dataframes:
        for col, dtype in df.schema.items():
            if col not in reference_schema:
                reference_schema[col] = dtype

    logger.info(f"Reference schema: {reference_schema}")

    # Normalize schemas and combine
    logger.info("Normalizing schemas and combining dataframes...")
    normalized_dfs = [normalize_schema(df, common_columns, reference_schema) for df in dataframes]

    # Use vertical concat since we've normalized schemas
    combined_df = pl.concat(normalized_dfs, how="vertical")

    logger.info(f"Combined dataset has {len(combined_df)} matches before deduplication")

    # Remove duplicates (keep highest confidence)
    combined_df = remove_duplicates(combined_df)

    # Log final statistics
    log_statistics(combined_df)

    # Save combined dataset
    logger.info(f"Saving combined dataset to {OUTPUT_FILE}")
    try:
        combined_df.write_parquet(OUTPUT_FILE)
        logger.info(f"Successfully saved {len(combined_df)} matches to {OUTPUT_FILE}")

        # File size info
        file_size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
        logger.info(f"File size: {file_size_mb:.2f} MB")
    except Exception as e:
        logger.error(f"Error saving combined dataset: {e}")
        return

    logger.info("Stage 1 enhanced match combination completed successfully!")


if __name__ == "__main__":
    main()
