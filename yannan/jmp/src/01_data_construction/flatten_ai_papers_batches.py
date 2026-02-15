analyze"""
Flatten ai_papers batch files by expanding full_json column

This script uses multiprocessing to flatten all batch files in parallel,
expanding the full_json column into flattened columns.

Uses PyArrow directly for robust handling of large sparse schemas.
"""

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import json
from pathlib import Path
import logging
from datetime import datetime
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import argparse
import glob
from typing import Dict, Any, List, Set
from collections import defaultdict

# Configuration
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BATCH_DIR = PROJECT_ROOT / "data" / "raw" / "publication" / "ai_papers_batches_noduplication"
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "publication" / "ai_papers_batches_noduplication_flattened"
LOG_DIR = PROJECT_ROOT / "logs"

# Default settings
DEFAULT_NUM_WORKERS = min(2, mp.cpu_count())  # Conservative to avoid memory issues
CHUNK_SIZE = 5000  # Smaller chunks for better memory management
PROGRESS_FILE = LOG_DIR / "flatten_ai_papers_progress.json"

# Create directories
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "flatten_ai_papers_batches.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def flatten_dict(d: Dict, parent_key: str = '', sep: str = '_', max_depth: int = 20, current_depth: int = 0) -> Dict[str, Any]:
    """
    Recursively flatten a nested dictionary.
    
    Returns all values as strings to avoid type conflicts.
    """
    if current_depth >= max_depth:
        return {parent_key: json.dumps(d, default=str) if parent_key else 'max_depth_reached'}
    
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
        # Special handling for abstract_inverted_index - keep as JSON string
        if k == 'abstract_inverted_index' and isinstance(v, dict):
            items.append((new_key, json.dumps(v, default=str) if v else None))
            continue
        
        if v is None:
            items.append((new_key, None))
        elif isinstance(v, bytes):
            try:
                items.append((new_key, v.decode('utf-8')))
            except:
                items.append((new_key, str(v)))
        elif isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep, max_depth, current_depth + 1).items())
        elif isinstance(v, list):
            if len(v) == 0:
                items.append((new_key, None))
            elif isinstance(v[0], dict):
                # Handle list of dictionaries - limit to first 100 items
                max_list_items = 100
                for idx, item in enumerate(v[:max_list_items]):
                    if isinstance(item, dict):
                        items.extend(flatten_dict(item, f"{new_key}_{idx}", sep, max_depth, current_depth + 1).items())
                    elif isinstance(item, bytes):
                        try:
                            items.append((f"{new_key}_{idx}", item.decode('utf-8')))
                        except:
                            items.append((f"{new_key}_{idx}", str(item)))
                    else:
                        items.append((f"{new_key}_{idx}", str(item) if item is not None else None))
                if len(v) > max_list_items:
                    items.append((f"{new_key}_remaining_count", str(len(v) - max_list_items)))
            else:
                # List of primitives - convert to JSON string
                normalized_list = []
                for item in v:
                    if isinstance(item, bytes):
                        try:
                            normalized_list.append(item.decode('utf-8'))
                        except:
                            normalized_list.append(str(item))
                    else:
                        normalized_list.append(item)
                items.append((new_key, json.dumps(normalized_list, default=str) if normalized_list else None))
        else:
            # Convert everything to string
            items.append((new_key, str(v) if v is not None else None))
    
    return dict(items)


def flatten_batch_file(batch_file_path: Path) -> Dict[str, Any]:
    """
    Flatten a single batch file using PyArrow for robust schema handling.
    """
    batch_name = batch_file_path.stem
    output_file = OUTPUT_DIR / f"{batch_name}_flatten.parquet"
    
    # Check if already exists and is valid (non-empty)
    if output_file.exists():
        try:
            # Use PyArrow to check if file is valid and non-empty
            table = pq.read_table(output_file)
            if len(table) > 0:
                return {
                    'batch_file': str(batch_file_path),
                    'output_file': str(output_file),
                    'success': True,
                    'rows': len(table),
                    'message': f'Already exists ({len(table):,} rows)',
                    'skipped': True
                }
            else:
                # File exists but is empty, delete and re-process
                logger.warning(f"{batch_name}: Existing file is empty, deleting and re-processing...")
                output_file.unlink()
        except Exception as e:
            # File exists but is corrupted, delete and re-process
            logger.warning(f"{batch_name}: Existing file is corrupted, deleting and re-processing...")
            try:
                output_file.unlink()
            except:
                pass
    
    start_time = time.time()
    
    try:
        # Read the batch file
        df = pl.read_parquet(batch_file_path)
        
        # Convert full_json to string if needed
        if 'full_json' in df.columns:
            if df['full_json'].dtype == pl.Binary:
                df = df.with_columns(pl.col('full_json').cast(pl.Utf8))
        
        total_rows = len(df)
        
        # First pass: discover all columns by processing a sample
        logger.debug(f"Discovering schema for {batch_name}...")
        all_columns = set()
        sample_size = min(1000, total_rows)
        sample_df = df.head(sample_size)
        
        for row in sample_df.iter_rows(named=True):
            flat_row = {k: str(v) if v is not None else None 
                       for k, v in row.items() if k != 'full_json'}
            json_str = row.get('full_json')
            if json_str:
                try:
                    if isinstance(json_str, bytes):
                        json_str = json_str.decode('utf-8')
                    json_data = json.loads(json_str) if isinstance(json_str, str) else json_str
                    flattened_json = flatten_dict(json_data)
                    for k, v in flattened_json.items():
                        flat_row[k] = str(v) if v is not None else None
                except:
                    pass
            all_columns.update(flat_row.keys())
        
        # Add original columns
        for col in df.columns:
            if col != 'full_json':
                all_columns.add(col)
        
        # Build PyArrow schema - all columns as string (Utf8)
        column_names = sorted(all_columns)
        schema_dict = {col: pa.string() for col in column_names}
        schema = pa.schema(schema_dict)
        
        # Open Parquet writer for incremental writing
        writer = None
        total_written_rows = 0
        
        try:
            writer = pq.ParquetWriter(output_file, schema, compression='snappy')
            
            # Process in chunks and write incrementally
            for chunk_start in range(0, total_rows, CHUNK_SIZE):
                chunk_end = min(chunk_start + CHUNK_SIZE, total_rows)
                chunk_df = df.slice(chunk_start, chunk_end - chunk_start)
                
                # Process chunk
                chunk_rows = []
                for row in chunk_df.iter_rows(named=True):
                    try:
                        # Start with original columns (excluding full_json)
                        flat_row = {k: str(v) if v is not None else None 
                                   for k, v in row.items() if k != 'full_json'}
                        
                        # Flatten JSON if present
                        json_str = row.get('full_json')
                        if json_str:
                            try:
                                if isinstance(json_str, bytes):
                                    json_str = json_str.decode('utf-8')
                                json_data = json.loads(json_str) if isinstance(json_str, str) else json_str
                                flattened_json = flatten_dict(json_data)
                                for k, v in flattened_json.items():
                                    flat_row[k] = str(v) if v is not None else None
                            except Exception as parse_error:
                                flat_row['flatten_error'] = f"JSON parse error: {str(parse_error)[:200]}"
                        
                        chunk_rows.append(flat_row)
                        
                    except Exception as e:
                        flat_row = {k: str(v) if v is not None else None 
                                   for k, v in row.items() if k != 'full_json'}
                        flat_row['flatten_error'] = str(e)[:200]
                        chunk_rows.append(flat_row)
                
                # Build arrays for this chunk and write
                if chunk_rows:
                    arrays = []
                    for col_name in column_names:
                        col_values = []
                        for row in chunk_rows:
                            val = row.get(col_name)
                            col_values.append(str(val) if val is not None else None)
                        arrays.append(pa.array(col_values, type=pa.string()))
                    
                    # Create table and write
                    chunk_table = pa.Table.from_arrays(arrays, schema=schema)
                    writer.write_table(chunk_table)
                    total_written_rows += len(chunk_rows)
        
        finally:
            # Always close writer
            if writer is not None:
                writer.close()
        
        # Read back with Polars to get row count
        df_flat = pl.read_parquet(output_file)
        
        elapsed = time.time() - start_time
        
        return {
            'batch_file': str(batch_file_path),
            'output_file': str(output_file),
            'success': True,
            'rows': len(df_flat),
            'columns': len(df_flat.columns) if len(df_flat) > 0 else 0,
            'message': f'Flattened {len(df_flat):,} rows, {len(df_flat.columns) if len(df_flat) > 0 else 0} columns in {elapsed:.1f}s',
            'skipped': False,
            'elapsed': elapsed
        }
        
    except MemoryError as e:
        error_msg = f"Memory error: {str(e)}"
        logger.error(f"Memory error flattening {batch_name}: {error_msg}")
        return {
            'batch_file': str(batch_file_path),
            'output_file': str(output_file),
            'success': False,
            'rows': 0,
            'message': f'Memory error: {error_msg}',
            'error': 'memory_error'
        }
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error flattening {batch_name}: {error_msg}", exc_info=True)
        return {
            'batch_file': str(batch_file_path),
            'output_file': str(output_file),
            'success': False,
            'rows': 0,
            'message': f'Error: {error_msg[:200]}',
            'error': 'processing_error'
        }


def update_progress(progress_data: Dict):
    """Update progress file."""
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress_data, f, indent=2, default=str)
    except:
        pass


def monitor_progress(total_files: int, futures: Dict) -> Dict[str, Any]:
    """Monitor and report progress of parallel flattening."""
    completed = 0
    successful = 0
    failed = 0
    skipped = 0
    total_rows = 0
    start_time = time.time()
    
    progress_data = {
        'start_time': datetime.now().isoformat(),
        'total_files': total_files,
        'completed': 0,
        'successful': 0,
        'failed': 0,
        'skipped': 0,
        'total_rows': 0,
        'files': {}
    }
    
    logger.info("=" * 80)
    logger.info("MONITORING PARALLEL FLATTENING PROGRESS")
    logger.info("=" * 80)
    logger.info("")
    
    for future in as_completed(futures):
        try:
            result = future.result()
            completed += 1
            
            file_name = Path(result['batch_file']).name
            progress_data['files'][file_name] = result
            progress_data['completed'] = completed
            
            if result['success']:
                if result.get('skipped', False):
                    skipped += 1
                    progress_data['skipped'] = skipped
                    logger.info(f"[{completed}/{total_files}] {file_name}: {result['message']}")
                else:
                    successful += 1
                    progress_data['successful'] = successful
                    total_rows += result['rows']
                    progress_data['total_rows'] = total_rows
                    elapsed = time.time() - start_time
                    rate = total_rows / elapsed if elapsed > 0 else 0
                    logger.info(f"[{completed}/{total_files}] {file_name}: {result['message']} | "
                              f"Total: {total_rows:,} rows | Rate: {rate:.0f} rows/sec")
            else:
                failed += 1
                progress_data['failed'] = failed
                logger.error(f"[{completed}/{total_files}] {file_name}: FAILED - {result['message']}")
            
            # Update progress file
            progress_pct = (completed / total_files * 100) if total_files > 0 else 0
            progress_data['progress_pct'] = progress_pct
            progress_data['elapsed_seconds'] = time.time() - start_time
            update_progress(progress_data)
            
        except Exception as e:
            logger.error(f"Error processing future result: {e}", exc_info=True)
            completed += 1
            failed += 1
            progress_data['failed'] = failed
    
    return {
        'completed': completed,
        'successful': successful,
        'failed': failed,
        'skipped': skipped,
        'total_rows': total_rows
    }


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description="Flatten ai_papers batch files by expanding full_json",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default settings (2 workers, chunked processing)
  python flatten_ai_papers_batches.py
  
  # Use 1 worker (safer for memory-constrained systems)
  python flatten_ai_papers_batches.py --num-workers 1
  
  # Resume (skip already flattened files)
  python flatten_ai_papers_batches.py --resume
        """
    )
    
    parser.add_argument(
        '--num-workers',
        type=int,
        default=DEFAULT_NUM_WORKERS,
        help=f'Number of parallel workers (default: {DEFAULT_NUM_WORKERS})'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from existing flattened files (skip already processed files)'
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("FLATTENING AI PAPERS BATCH FILES")
    logger.info("=" * 80)
    logger.info(f"Batch directory: {BATCH_DIR}")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info(f"Workers: {args.num_workers}")
    logger.info(f"Chunk size: {CHUNK_SIZE:,} rows")
    logger.info("")
    
    # Find batch files (exclude already flattened ones)
    batch_files = sorted([Path(f) for f in glob.glob(str(BATCH_DIR / "batch_*.parquet")) 
                         if not f.endswith("_flatten.parquet")])
    
    if not batch_files:
        logger.error(f"No batch files found in {BATCH_DIR}")
        return 1
    
    logger.info(f"Found {len(batch_files)} batch files to flatten")
    logger.info("")
    
    # Check for existing flattened files if resuming
    if args.resume:
        logger.info("Checking for existing flattened files...")
        existing_count = 0
        for batch_file in batch_files:
            output_file = OUTPUT_DIR / f"{batch_file.stem}_flatten.parquet"
            if output_file.exists():
                existing_count += 1
        logger.info(f"Found {existing_count} already flattened files")
        logger.info("")
    
    # Start parallel flattening
    logger.info("Starting parallel flattening...")
    logger.info(f"  Using {args.num_workers} workers")
    logger.info(f"  Processing {len(batch_files)} files")
    logger.info("")
    
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=args.num_workers) as executor:
        # Submit all files
        futures = {executor.submit(flatten_batch_file, batch_file): batch_file 
                  for batch_file in batch_files}
        
        # Monitor progress
        stats = monitor_progress(len(batch_files), futures)
    
    elapsed = time.time() - start_time
    
    # Final summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("FLATTENING COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total files: {len(batch_files)}")
    logger.info(f"Successful: {stats['successful']}")
    logger.info(f"Skipped (already existed): {stats['skipped']}")
    logger.info(f"Failed: {stats['failed']}")
    logger.info(f"Total rows flattened: {stats['total_rows']:,}")
    logger.info(f"Total time: {elapsed/3600:.2f} hours ({elapsed:.0f} seconds)")
    logger.info(f"Average rate: {stats['total_rows']/elapsed:.0f} rows/sec" if elapsed > 0 else "N/A")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info("")
    
    if stats['failed'] > 0:
        logger.warning(f"{stats['failed']} files failed. Check log file: {LOG_DIR / 'flatten_ai_papers_batches.log'}")
        logger.info("")
    
    logger.info("=" * 80)
    
    return 0 if stats['failed'] == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
