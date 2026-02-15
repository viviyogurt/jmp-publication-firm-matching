"""
Merge arxiv_index_enhanced Batch Files

This script merges all batch parquet files into a single comprehensive parquet file.

Usage:
    python merge_arxiv_index_enhanced_batches.py [--output OUTPUT_FILE]
"""

import polars as pl
from pathlib import Path
import argparse
import logging
import glob

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BATCH_DIR = PROJECT_ROOT / "data" / "raw" / "publication" / "arxiv_index_enhanced_batches"
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "publication"
DEFAULT_OUTPUT = OUTPUT_DIR / "openalex_claude_arxiv_index_enhanced.parquet"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "logs" / "merge_arxiv_index_enhanced_batches.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def merge_batches(batch_dir: Path, output_file: Path, verify: bool = True):
    """
    Merge all batch parquet files into a single file.
    
    Args:
        batch_dir: Directory containing batch files
        output_file: Output file path
        verify: Whether to verify row counts before merging
    """
    logger.info("=" * 80)
    logger.info("MERGING arxiv_index_enhanced BATCH FILES")
    logger.info("=" * 80)
    logger.info(f"Batch directory: {batch_dir}")
    logger.info(f"Output file: {output_file}")
    logger.info("")
    
    # Find all batch files
    batch_files = sorted(glob.glob(str(batch_dir / "batch_*.parquet")))
    
    if not batch_files:
        logger.error(f"No batch files found in {batch_dir}")
        return False
    
    logger.info(f"Found {len(batch_files)} batch files")
    logger.info("")
    
    # Verify batch files
    if verify:
        logger.info("Step 1: Verifying batch files...")
        valid_batches = []
        total_rows = 0
        
        for i, batch_file in enumerate(batch_files, 1):
            try:
                # Quick read to verify file
                df = pl.read_parquet(batch_file)
                rows = len(df)
                total_rows += rows
                valid_batches.append(batch_file)
                logger.info(f"  Batch {i}/{len(batch_files)}: {rows:,} rows - OK")
            except Exception as e:
                logger.warning(f"  Batch {i}/{len(batch_files)}: Invalid file - {e}")
        
        logger.info(f"  Valid batches: {len(valid_batches)}/{len(batch_files)}")
        logger.info(f"  Total rows: {total_rows:,}")
        logger.info("")
        
        if not valid_batches:
            logger.error("No valid batch files found!")
            return False
        
        batch_files = valid_batches
    else:
        logger.info("Skipping batch verification (--no-verify specified)")
        logger.info("")
    
    # Merge batches
    logger.info("Step 2: Merging batches...")
    logger.info("  This may take a while for large datasets...")
    logger.info("")
    
    try:
        # Use lazy evaluation for memory efficiency
        lazy_frames = []
        for i, batch_file in enumerate(batch_files, 1):
            if i % 100 == 0 or i == len(batch_files):
                logger.info(f"  Loading batch {i}/{len(batch_files)}...")
            lazy_frames.append(pl.scan_parquet(batch_file))
        
        logger.info("")
        logger.info("  Concatenating batches...")
        
        # Concatenate all lazy frames
        combined = pl.concat(lazy_frames)
        
        logger.info("  Collecting and writing final file...")
        
        # Collect and write
        combined.collect().write_parquet(
            output_file,
            compression="snappy",
            use_pyarrow=True
        )
        
        logger.info("")
        logger.info("✓ Merge completed successfully!")
        logger.info("")
        
        # Verify output
        logger.info("Step 3: Verifying merged file...")
        df_final = pl.read_parquet(output_file)
        final_rows = len(df_final)
        final_size = output_file.stat().st_size / (1024 * 1024 * 1024)  # GB
        
        logger.info(f"  Final row count: {final_rows:,}")
        logger.info(f"  Final file size: {final_size:.2f} GB")
        logger.info(f"  Columns: {len(df_final.columns)}")
        logger.info("")
        
        # Show column names
        logger.info("  Column names:")
        for col in df_final.columns:
            logger.info(f"    - {col}")
        logger.info("")
        
        logger.info("=" * 80)
        logger.info("MERGE COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Output file: {output_file}")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"Error during merge: {e}", exc_info=True)
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Merge arxiv_index_enhanced batch files into a single parquet file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Merge with default output file
  python merge_arxiv_index_enhanced_batches.py
  
  # Specify custom output file
  python merge_arxiv_index_enhanced_batches.py --output /path/to/output.parquet
  
  # Skip batch verification (faster but less safe)
  python merge_arxiv_index_enhanced_batches.py --no-verify
        """
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=str(DEFAULT_OUTPUT),
        help=f'Output file path (default: {DEFAULT_OUTPUT})'
    )
    
    parser.add_argument(
        '--no-verify',
        action='store_true',
        help='Skip batch file verification before merging (faster but less safe)'
    )
    
    args = parser.parse_args()
    
    output_file = Path(args.output)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    success = merge_batches(
        BATCH_DIR,
        output_file,
        verify=not args.no_verify
    )
    
    if not success:
        exit(1)


if __name__ == "__main__":
    main()
