"""
Build Patent Assignee Panel

This script loads patent assignee data from g_assignee_disambiguated and
g_persistent_assignee, standardizes names, and builds an assignee-year panel
with patent counts and year ranges.

Output: data/interim/patent_assignee_panel.parquet

Date: 2025
"""

import polars as pl
from pathlib import Path
import logging
import re
from typing import Dict, Set
import time

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "patents"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
LOGS_DIR = PROJECT_ROOT / "logs"

OUTPUT_PARQUET = DATA_INTERIM / "patent_assignee_panel.parquet"
PROGRESS_LOG = LOGS_DIR / "patent_panel_progress.log"

# Patent data files
ASSIGNEE_DISAMBIGUATED = DATA_RAW / "g_assignee_disambiguated.tsv.zip"
PERSISTENT_ASSIGNEE = DATA_RAW / "g_persistent_assignee.tsv.zip"

# Ensure directories exist
DATA_INTERIM.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROGRESS_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Assignee Name Standardization
# ============================================================================

SUFFIXES_TO_REMOVE = [
    ' inc', ' corp', ' llc', 'ltd', 'limited', 'plc', 'gmbh', 'ag', 'sa', 'spa',
    'pty', 'bv', 'nv', 'aps', 'sro', 'co', 'company', 'corporation',
    ' technologies', 'technology', 'laboratories', 'laboratory', 'labs',
    ' research', 'group', 'holdings', 'international', 'industries',
    ' systems', 'solutions', 'software', 'services'
]


def normalize_assignee_name(name: str) -> str:
    """Normalize patent assignee name for matching."""
    if not name or name == "":
        return ""

    normalized = name.lower().strip()

    # Remove common suffixes
    for suffix in SUFFIXES_TO_REMOVE:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()

    # Replace & with 'and'
    normalized = normalized.replace('&', 'and')

    # Normalize whitespace
    normalized = re.sub(r'\s+', ' ', normalized)

    return normalized.strip()


# ============================================================================
# Main Processing Functions
# ============================================================================

def load_assignee_data():
    """Load patent assignee data files."""
    logger.info("=" * 80)
    logger.info("LOADING PATENT ASSIGNEE DATA")
    logger.info("=" * 80)

    # Check if files exist
    if not ASSIGNEE_DISAMBIGUATED.exists():
        logger.warning(f"  Assignee disambiguated file not found: {ASSIGNEE_DISAMBIGUATED}")
        logger.warning("  Will try to use alternative data sources")

    if not PERSISTENT_ASSIGNEE.exists():
        logger.warning(f"  Persistent assignee file not found: {PERSISTENT_ASSIGNEE}")

    # Load assignee data
    assignee_dfs = []

    # Try to load disambiguated assignees
    if ASSIGNEE_DISAMBIGUATED.exists():
        logger.info(f"\nLoading {ASSIGNEE_DISAMBIGUATED.name}...")
        try:
            # This is a large TSV file, read in chunks
            assignee_df = pl.read_csv(
                ASSIGNEE_DISAMBIGUATED,
                separator='\t',
                n_rows=1000000,  # Limit to 1M for now
                truncate_ragged_lines=True,
                ignore_errors=True
            )
            logger.info(f"  Loaded {len(assignee_df):,} assignee records")

            # Standardize names
            if 'assignee_name' in assignee_df.columns:
                normalized_names = [normalize_assignee_name(name) for name in assignee_df['assignee_name']]
                assignee_df = assignee_df.with_columns([
                    pl.Series(normalized_names).alias('normalized_name')
                ])

            assignee_dfs.append(assignee_df)
        except Exception as e:
            logger.error(f"  Error loading disambiguated assignees: {e}")

    # Try to load persistent assignees
    if PERSISTENT_ASSIGNEE.exists():
        logger.info(f"\nLoading {PERSISTENT_ASSIGNEE.name}...")
        try:
            persistent_df = pl.read_csv(
                PERSISTENT_ASSIGNEE,
                separator='\t',
                n_rows=1000000,
                truncate_ragged_lines=True,
                ignore_errors=True
            )
            logger.info(f"  Loaded {len(persistent_df):,} persistent assignee records")

            # Standardize names
            if 'name' in persistent_df.columns:
                normalized_names = [normalize_assignee_name(name) for name in persistent_df['name']]
                persistent_df = persistent_df.with_columns([
                    pl.Series(normalized_names).alias('normalized_name')
                ])

            assignee_dfs.append(persistent_df)
        except Exception as e:
            logger.error(f"  Error loading persistent assignees: {e}")

    if not assignee_dfs:
        logger.error("  No assignee data loaded!")
        return None

    # Merge all assignee data
    logger.info("\nMerging assignee data...")
    if len(assignee_dfs) == 1:
        merged_df = assignee_dfs[0]
    else:
        merged_df = pl.concat(assignee_dfs, how='diagonal')

    logger.info(f"  Merged: {len(merged_df):,} total records")

    return merged_df


def build_assignee_panel(assignee_df):
    """Build assignee-year panel with patent counts."""
    logger.info("\n" + "=" * 80)
    logger.info("BUILDING ASSIGNEE-YEAR PANEL")
    logger.info("=" * 80)

    # Get unique assignees
    logger.info("\nExtracting unique assignees...")

    # Try different column names for assignee ID
    id_col = None
    for col in ['assignee_id', 'id', 'patent_id', 'disambiguated_assignee_id']:
        if col in assignee_df.columns:
            id_col = col
            break

    # Try different column names for assignee name
    name_col = None
    for col in ['assignee_name', 'name', 'assignee']:
        if col in assignee_df.columns:
            name_col = col
            break

    logger.info(f"  ID column: {id_col}")
    logger.info(f"  Name column: {name_col}")

    if not name_col:
        logger.error("  No name column found!")
        return None

    # Get unique assignees with their canonical names
    unique_assignees = assignee_df.select([
        pl.col(name_col).alias('raw_name'),
        pl.col('normalized_name')
    ]).unique()

    logger.info(f"  Found {len(unique_assignees):,} unique assignees")

    # Count patents per assignee
    logger.info("\nCounting patents per assignee...")
    assignee_counts = assignee_df.group_by('normalized_name').agg([
        pl.len().alias('patent_count')
    ])

    # Merge with unique assignees
    assignee_panel = unique_assignees.join(assignee_counts, on='normalized_name', how='left')

    # Sort by patent count
    assignee_panel = assignee_panel.sort('patent_count', descending=True)

    logger.info(f"\nAssignee panel: {len(assignee_panel):,} assignees")

    # Add assignee ID (sequential)
    assignee_panel = assignee_panel.with_row_index("assignee_id", offset=1)

    # Save to parquet
    logger.info(f"\nSaving to {OUTPUT_PARQUET}...")
    assignee_panel.write_parquet(OUTPUT_PARQUET, compression='snappy')
    logger.info(f"  Saved {len(assignee_panel):,} assignees")

    # Statistics
    logger.info("\n" + "=" * 80)
    logger.info("PATENT ASSIGNEE STATISTICS")
    logger.info("=" * 80)

    logger.info(f"\nTotal unique assignees: {len(assignee_panel):,}")
    logger.info(f"Total patents: {assignee_panel['patent_count'].sum():,}")
    logger.info(f"Mean patents per assignee: {assignee_panel['patent_count'].mean():.1f}")
    logger.info(f"Median patents per assignee: {assignee_panel['patent_count'].median():.1f}")

    logger.info("\nTop 30 assignees by patent count:")
    top_assignees = assignee_panel.head(30)
    for row in top_assignees.iter_rows(named=True):
        logger.info(f"  {row['assignee_id']:5d} {row['raw_name'][:60]:<60} {row['patent_count']:>8,} patents")

    return assignee_panel


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("PATENT ASSIGNEE PANEL BUILDER")
    logger.info("=" * 80)
    logger.info(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    assignee_df = load_assignee_data()
    if assignee_df is None:
        logger.error("Failed to load assignee data")
        return

    assignee_panel = build_assignee_panel(assignee_df)
    if assignee_panel is None:
        logger.error("Failed to build assignee panel")
        return

    logger.info("\n" + "=" * 80)
    logger.info("PANEL BUILDER COMPLETED")
    logger.info("=" * 80)
    logger.info(f"Total assignees: {len(assignee_panel):,}")
    logger.info(f"Output saved to: {OUTPUT_PARQUET}")
    logger.info(f"Finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
