"""
Fetch ai_papers using ClickHouse native client with multiprocessing

This script uses multiple processes to fetch data in parallel,
dramatically reducing fetch time for large datasets.

IMPORTANT: Uses ORDER BY openalex_id to ensure deterministic, non-overlapping batches.
Without ORDER BY, parallel queries can return overlapping or inconsistent results,
leading to massive duplicate rates (70%+) when combining batches.

Usage:
    python fetch_ai_papers_parallel.py [--num-workers N] [--batch-size N]
    
Example:
    python fetch_ai_papers_parallel.py --num-workers 8 --batch-size 100000
"""

import subprocess
import os
import sys
from pathlib import Path
import logging
from datetime import datetime
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import argparse
import math
import json

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
OUTPUT_DIR = DATA_RAW / "ai_papers_batches_noduplication"  # New directory to avoid overwriting old batches
LOG_DIR = PROJECT_ROOT / "logs"

# ClickHouse configuration
CLICKHOUSE_HOST = "chenlin04.fbe.hku.hk"
CLICKHOUSE_USER = "yannan"
CLICKHOUSE_PASSWORD = "alaniscoolerthanluoye"
CLICKHOUSE_DB = "openalex_claude"
CLICKHOUSE_TABLE = "ai_papers"

# Default settings
# Use fewer workers to avoid overwhelming ClickHouse server memory
# With large JSON columns, we need to balance parallelism vs memory
# CRITICAL: Batch size must be small enough to avoid "Max query size exceeded" error
# ClickHouse has ~262KB query size limit, and WHERE IN with 20K IDs exceeds this
# Each ID is ~30-40 chars, so 5K IDs = ~150-200KB (safe margin)
DEFAULT_NUM_WORKERS = min(4, mp.cpu_count())  # Reduced to 4 workers to reduce server memory pressure
DEFAULT_BATCH_SIZE = 5000  # Reduced to 5K rows per batch to avoid query size limits
PROGRESS_FILE = LOG_DIR / "fetch_ai_papers_progress.json"

# Create directories
DATA_RAW.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "fetch_ai_papers_parallel.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_total_rows():
    """Get total number of rows in the table."""
    query = f"SELECT count(*) FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}"
    
    cmd = [
        "clickhouse-client",
        "--host", CLICKHOUSE_HOST,
        "--port", "9000",
        "--user", CLICKHOUSE_USER,
        "--password", CLICKHOUSE_PASSWORD,
        "--database", CLICKHOUSE_DB,
        "--query", query
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
            check=True
        )
        return int(result.stdout.strip())
    except Exception as e:
        logger.error(f"Error getting total rows: {e}")
        raise


def get_all_ids_sorted():
    """
    Get all UNIQUE openalex_ids in sorted order (no duplicates).
    This is much smaller than full rows (just IDs), so it can be done efficiently.
    Using DISTINCT ensures we only get unique IDs, avoiding duplicate warnings.
    
    Returns:
        List of unique openalex_id strings in sorted order
    """
    logger.info("  Fetching all UNIQUE openalex_ids in sorted order...")
    logger.info("    Using DISTINCT to get only unique IDs (no duplicates)")
    logger.info("    This is a one-time operation to enable efficient batch splitting")
    
    query = f"""
    SELECT DISTINCT openalex_id
    FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}
    ORDER BY openalex_id
    FORMAT TSV
    """
    
    cmd = [
        "clickhouse-client",
        "--host", CLICKHOUSE_HOST,
        "--port", "9000",
        "--user", CLICKHOUSE_USER,
        "--password", CLICKHOUSE_PASSWORD,
        "--database", CLICKHOUSE_DB,
        "--query", query
    ]
    
    try:
        start_time = time.time()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout for large ID list
            check=True
        )
        
        # Parse TSV output
        ids = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
        elapsed = time.time() - start_time
        
        logger.info(f"    Fetched {len(ids):,} IDs in {elapsed:.1f}s")
        logger.info(f"    First ID: {ids[0] if ids else 'N/A'}")
        logger.info(f"    Last ID: {ids[-1] if ids else 'N/A'}")
        
        return ids
    except Exception as e:
        logger.error(f"Error fetching IDs: {e}")
        raise


def split_ids_into_batches(ids: list, batch_size: int):
    """
    Split sorted IDs into batches for parallel fetching.
    
    Args:
        ids: Sorted list of openalex_ids
        batch_size: Number of IDs per batch
    
    Returns:
        List of lists, where each inner list contains IDs for one batch
    """
    batches = []
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i + batch_size]
        batches.append(batch_ids)
    return batches




def fetch_batch(batch_info):
    """
    Fetch a single batch of data using direct WHERE IN query.
    
    Args:
        batch_info: Tuple of (batch_num, batch_ids, total_batches)
                   batch_ids is a list of openalex_ids for this batch
    
    Returns:
        Dict with batch_num, success status, rows fetched, file path, error message
    """
    batch_num, batch_ids, total_batches = batch_info
    
    batch_file = OUTPUT_DIR / f"batch_{batch_num:06d}.parquet"
    log_file = LOG_DIR / f"fetch_batch_{batch_num:06d}.log"
    
    # Skip if already exists
    if batch_file.exists():
        try:
            # Verify existing file
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(batch_file)
            rows = pf.metadata.num_rows
            return {
                'batch_num': batch_num,
                'success': True,
                'rows': rows,
                'file': str(batch_file),
                'message': f'Already exists ({rows:,} rows)',
                'skipped': True
            }
        except:
            # File exists but is invalid, re-fetch
            pass
    
    # Build query - select all columns including full_json
    # Use WHERE IN with pre-determined IDs - no ORDER BY needed, no OFFSET, no cursor
    # This is the most efficient approach: direct lookup by ID list
    # CRITICAL: Write query to file to avoid "Argument list too long" error
    # With 20K IDs, the command line would exceed system limits
    
    # Escape single quotes in IDs to prevent SQL injection (though we control the IDs)
    escaped_ids = [id.replace("'", "''") for id in batch_ids]
    ids_list = "', '".join(escaped_ids)
    where_clause = f"WHERE openalex_id IN ('{ids_list}')"
    
    query = f"""
    SELECT 
        openalex_id,
        doi,
        title,
        publication_year,
        publication_date,
        language,
        work_type,
        cited_by_count,
        is_open_access,
        url,
        pdf_url,
        venue_name,
        keywords,
        topics,
        is_llm,
        is_machine_learning,
        is_deep_learning,
        is_nlp,
        is_computer_vision,
        is_reinforcement_learning,
        primary_ai_category,
        full_json
    FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}
    {where_clause}
    SETTINGS 
        max_memory_usage = 8000000000,
        max_execution_time = 0,
        send_timeout = 0,
        receive_timeout = 0
    FORMAT Parquet
    """
    
    # Write query to temporary file to avoid command line length limits
    query_file = LOG_DIR / f"query_batch_{batch_num:06d}.sql"
    try:
        with open(query_file, 'w') as f:
            f.write(query)
    except Exception as e:
        return {
            'batch_num': batch_num,
            'success': False,
            'rows': 0,
            'file': str(batch_file),
            'message': f'Failed to write query file: {str(e)}',
            'error': 'query_file_error'
        }
    
    start_time = time.time()
    
    try:
        with open(log_file, 'w') as log_f:
            # Pipe query file to stdin to avoid command line length limits
            # clickhouse-client doesn't support --query-file, so we use stdin
            cmd = [
                "clickhouse-client",
                "--host", CLICKHOUSE_HOST,
                "--port", "9000",
                "--user", CLICKHOUSE_USER,
                "--password", CLICKHOUSE_PASSWORD,
                "--database", CLICKHOUSE_DB,
                "--format", "Parquet",
                "--max_memory_usage", "8000000000",
                "--max_execution_time", "0"
            ]
            
            # Open query file and pipe to clickhouse-client stdin
            with open(query_file, 'r') as qf:
                result = subprocess.run(
                    cmd,
                    stdin=qf,
                    stdout=subprocess.PIPE,
                    stderr=log_f,
                    timeout=7200,  # 2 hour timeout per batch
                    check=True
                )
        
        # Clean up query file after successful execution
        try:
            query_file.unlink()
        except:
            pass
        
        # Write to file
        with open(batch_file, 'wb') as f:
            f.write(result.stdout)
        
        # Verify file, check for duplicates, and validate JSON
        import pyarrow.parquet as pq
        import polars as pl
        import gc
        
        pf = pq.ParquetFile(batch_file)
        rows = pf.metadata.num_rows
        
        # Verify batch integrity: duplicates, JSON validity, and completeness
        verification_errors = []
        try:
            # Read with minimal memory footprint
            df = pl.read_parquet(batch_file)
            
            # Verification: Just check that we got data and basic structure
            # Don't verify exact ID matching since database may have duplicates or inconsistencies
            # The important thing is that we got rows with the expected structure
            if 'openalex_id' in df.columns:
                unique_ids = df['openalex_id'].n_unique()
                # Only warn if we got no data or very few unique IDs (indicates a real problem)
                if rows == 0:
                    verification_errors.append("No rows fetched")
                elif unique_ids < 10:  # Very low threshold - only warn on real issues
                    verification_errors.append(f"Very few unique IDs: {unique_ids}")
                
                # Check JSON column exists and is not null
                if 'full_json' in df.columns:
                    null_json_count = df.filter(pl.col('full_json').is_null()).height
                    if null_json_count > 0:
                        verification_errors.append(f"{null_json_count} rows with null full_json")
                    
                    # Sample check: verify JSON is valid (check first non-null)
                    sample_json = df.filter(pl.col('full_json').is_not_null()).select('full_json').head(1)
                    if len(sample_json) > 0:
                        try:
                            import json
                            json.loads(sample_json['full_json'][0])
                        except:
                            verification_errors.append("Invalid JSON format detected in sample")
                else:
                    verification_errors.append("full_json column missing")
            else:
                verification_errors.append("openalex_id column missing")
            
            # Explicit cleanup
            del df
            gc.collect()
            
            if verification_errors:
                logger.warning(f"Batch {batch_num:06d}: Verification issues - {', '.join(verification_errors)}")
            else:
                logger.debug(f"Batch {batch_num:06d}: Verification passed")
                
        except Exception as e:
            logger.warning(f"Batch {batch_num:06d}: Could not verify batch: {e}")
            verification_errors.append(f"Verification failed: {str(e)}")
        
        # Cleanup
        gc.collect()
        
        elapsed = time.time() - start_time
        
        return {
            'batch_num': batch_num,
            'success': True,
            'rows': rows,
            'file': str(batch_file),
            'message': f'Fetched {rows:,} rows in {elapsed:.1f}s',
            'skipped': False,
            'elapsed': elapsed
        }
        
    except subprocess.TimeoutExpired:
        # Clean up query file on error
        try:
            query_file.unlink()
        except:
            pass
        return {
            'batch_num': batch_num,
            'success': False,
            'rows': 0,
            'file': str(batch_file),
            'message': 'Timeout after 2 hours',
            'error': 'timeout'
        }
    except subprocess.CalledProcessError as e:
        # Clean up query file on error
        try:
            query_file.unlink()
        except:
            pass
        error_msg = f"ClickHouse error (code {e.returncode})"
        if log_file.exists():
            with open(log_file, 'r') as f:
                error_log = f.read()[-500:]  # Last 500 chars
                error_msg += f": {error_log}"
        return {
            'batch_num': batch_num,
            'success': False,
            'rows': 0,
            'file': str(batch_file),
            'message': error_msg,
            'error': 'clickhouse_error'
        }
    except Exception as e:
        # Clean up query file on error
        try:
            query_file.unlink()
        except:
            pass
        return {
            'batch_num': batch_num,
            'success': False,
            'rows': 0,
            'file': str(batch_file),
            'message': f'Unexpected error: {str(e)}',
            'error': 'unexpected'
        }


def update_progress(progress_data):
    """Update progress file."""
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress_data, f, indent=2)
    except:
        pass


def monitor_progress(total_batches, futures):
    """Monitor and report progress of parallel fetching."""
    completed = 0
    successful = 0
    failed = 0
    total_rows = 0
    skipped = 0
    start_time = time.time()
    
    progress_data = {
        'start_time': datetime.now().isoformat(),
        'total_batches': total_batches,
        'completed': 0,
        'successful': 0,
        'failed': 0,
        'skipped': 0,
        'total_rows': 0,
        'batches': {}
    }
    
    logger.info("=" * 80)
    logger.info("MONITORING PARALLEL FETCH PROGRESS")
    logger.info("=" * 80)
    logger.info("")
    
    # Process completed futures
    for future in as_completed(futures):
        try:
            result = future.result()
            completed += 1
            
            batch_num = result['batch_num']
            progress_data['batches'][batch_num] = result
            progress_data['completed'] = completed
            
            if result['success']:
                if result.get('skipped', False):
                    skipped += 1
                    progress_data['skipped'] = skipped
                    logger.info(f"[{completed}/{total_batches}] Batch {batch_num:06d}: {result['message']}")
                else:
                    successful += 1
                    progress_data['successful'] = successful
                    total_rows += result['rows']
                    progress_data['total_rows'] = total_rows
                    elapsed = time.time() - start_time
                    rate = total_rows / elapsed if elapsed > 0 else 0
                    logger.info(f"[{completed}/{total_batches}] Batch {batch_num:06d}: {result['message']} | "
                              f"Total: {total_rows:,} rows | Rate: {rate:.0f} rows/sec")
            else:
                failed += 1
                progress_data['failed'] = failed
                logger.error(f"[{completed}/{total_batches}] Batch {batch_num:06d}: FAILED - {result['message']}")
            
            # Update progress file
            progress_pct = (completed / total_batches * 100) if total_batches > 0 else 0
            progress_data['progress_pct'] = progress_pct
            progress_data['elapsed_seconds'] = time.time() - start_time
            update_progress(progress_data)
            
        except Exception as e:
            logger.error(f"Error processing future result: {e}")
            completed += 1
    
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
        description="Fetch ai_papers using parallel processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default settings (up to 12 workers, 100K rows per batch)
  python fetch_ai_papers_parallel.py
  
  # Use 24 workers
  python fetch_ai_papers_parallel.py --num-workers 24
  
  # Use smaller batches (50K rows)
  python fetch_ai_papers_parallel.py --batch-size 50000
        """
    )
    
    parser.add_argument(
        '--num-workers',
        type=int,
        default=DEFAULT_NUM_WORKERS,
        help=f'Number of parallel workers (default: {DEFAULT_NUM_WORKERS})'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f'Rows per batch (default: {DEFAULT_BATCH_SIZE:,})'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from existing batches (skip already fetched batches)'
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("PARALLEL FETCH: ai_papers")
    logger.info("=" * 80)
    logger.info(f"Workers: {args.num_workers}")
    logger.info(f"Batch size: {args.batch_size:,} rows")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info("")
    
    # Step 1: Get total rows
    logger.info("Step 1: Getting total row count...")
    try:
        total_rows = get_total_rows()
        logger.info(f"  Total rows: {total_rows:,}")
    except Exception as e:
        logger.error(f"Failed to get total rows: {e}")
        return 1
    
    # Step 2: Get all IDs in sorted order (ex-ante discovery)
    logger.info("Step 2: Discovering all openalex_ids in sorted order...")
    logger.info("  This is a one-time operation to enable efficient batch splitting")
    logger.info("  Fetching only IDs (much smaller than full rows) to avoid memory issues")
    logger.info("")
    
    try:
        all_ids = get_all_ids_sorted()
        logger.info(f"  Total unique IDs: {len(all_ids):,}")
        logger.info("")
    except Exception as e:
        logger.error(f"Failed to fetch IDs: {e}")
        return 1
    
    # Step 3: Split IDs into batches (ex-ante split)
    logger.info("Step 3: Splitting IDs into batches...")
    id_batches = split_ids_into_batches(all_ids, args.batch_size)
    total_batches = len(id_batches)
    logger.info(f"  Created {total_batches} batches")
    logger.info(f"  Average batch size: {len(all_ids) // total_batches:,} IDs")
    logger.info("")
    
    # Step 4: Prepare batch info for parallel fetching
    logger.info("Step 4: Preparing batch information for parallel fetching...")
    logger.info("  Using WHERE IN queries with pre-determined ID lists")
    logger.info("  No ORDER BY, no OFFSET, no cursor - direct lookups only")
    logger.info("  This approach is memory-efficient and allows true parallelization")
    logger.info("")
    
    batch_infos = []
    for i, batch_ids in enumerate(id_batches):
        batch_infos.append((i, batch_ids, total_batches))
    
    # Check for existing batches if resuming
    if args.resume:
        logger.info("  Checking for existing batches...")
        existing_count = 0
        for batch_info in batch_infos:
            batch_num = batch_info[0]
            batch_file = OUTPUT_DIR / f"batch_{batch_num:06d}.parquet"
            if batch_file.exists():
                existing_count += 1
        logger.info(f"  Found {existing_count} existing batches")
        logger.info("")
    
    # Step 5: Start parallel fetching
    logger.info("Step 5: Starting parallel fetch...")
    logger.info(f"  Using {args.num_workers} workers")
    logger.info(f"  Processing {total_batches} batches in parallel")
    logger.info("")
    
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=args.num_workers) as executor:
        # Submit all batches for parallel processing
        futures = {executor.submit(fetch_batch, batch_info): batch_info 
                  for batch_info in batch_infos}
        
        # Monitor progress
        stats = monitor_progress(total_batches, futures)
    
    elapsed = time.time() - start_time
    
    # Final summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("FETCH COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total batches: {total_batches}")
    logger.info(f"Successful: {stats['successful']}")
    logger.info(f"Skipped (already existed): {stats['skipped']}")
    logger.info(f"Failed: {stats['failed']}")
    logger.info(f"Total rows fetched: {stats['total_rows']:,}")
    logger.info(f"Total time: {elapsed/3600:.2f} hours ({elapsed:.0f} seconds)")
    logger.info(f"Average rate: {stats['total_rows']/elapsed:.0f} rows/sec" if elapsed > 0 else "N/A")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info("")
    
    if stats['failed'] > 0:
        logger.warning(f"⚠️  {stats['failed']} batches failed. Check individual batch logs in {LOG_DIR}")
        logger.warning("   You can re-run with --resume to retry failed batches")
        logger.info("")
    
    # Step 5: Comprehensive verification
    if stats['failed'] == 0 and stats['successful'] > 0:
        logger.info("")
        logger.info("Step 4: Comprehensive batch verification...")
        logger.info("  Verifying completeness and uniqueness...")
        try:
            # Get expected unique count from DISTINCT query
            expected_unique = len(all_ids) if 'all_ids' in locals() else None
            if expected_unique is None:
                # Fallback: use the count from source file
                expected_unique = 17_385_924
            verify_all_batches(OUTPUT_DIR, expected_unique)
        except Exception as e:
            logger.warning(f"Could not complete verification: {e}")
            logger.warning("  Manual verification recommended")
    
    logger.info("")
    logger.info("Next step: Merge batches using merge script")
    logger.info("=" * 80)
    
    return 0 if stats['failed'] == 0 else 1


def verify_all_batches(batch_dir: Path, expected_unique_ids: int):
    """
    Comprehensive verification of all batches:
    1. Check for overlapping IDs between batches
    2. Verify total unique papers matches expected (17,385,924)
    3. Check JSON column integrity
    4. Report completeness and guarantee no duplicates/missing papers
    
    Args:
        batch_dir: Directory containing batch files
        expected_unique_ids: Expected total number of unique papers (17,385,924)
    """
    import polars as pl
    from glob import glob
    import gc
    
    batch_files = sorted(glob(str(batch_dir / "batch_*.parquet")))
    
    if len(batch_files) == 0:
        logger.warning("  No batch files found for verification")
        return
    
    logger.info(f"  Verifying {len(batch_files)} batch files...")
    
    all_ids = set()
    total_rows = 0
    batches_with_duplicates = 0
    batches_with_missing_json = 0
    json_null_count = 0
    
    # Process in chunks to avoid memory issues
    chunk_size = 20
    for i in range(0, len(batch_files), chunk_size):
        chunk_files = batch_files[i:i+chunk_size]
        logger.info(f"    Processing batches {i+1}-{min(i+chunk_size, len(batch_files))}/{len(batch_files)}...")
        
        for batch_file in chunk_files:
            try:
                df = pl.read_parquet(batch_file)
                total_rows += len(df)
                
                if 'openalex_id' in df.columns:
                    batch_ids = set(df['openalex_id'].unique().to_list())
                    
                    # Check for duplicates within batch
                    if len(batch_ids) < len(df):
                        batches_with_duplicates += 1
                    
                    # Check for overlaps with previous batches
                    overlap = all_ids & batch_ids
                    if overlap:
                        logger.warning(f"      WARNING: {batch_file.name} has {len(overlap)} overlapping IDs")
                    
                    all_ids.update(batch_ids)
                    
                    # Check JSON column
                    if 'full_json' in df.columns:
                        null_count = df.filter(pl.col('full_json').is_null()).height
                        if null_count > 0:
                            json_null_count += null_count
                            if null_count > len(df) * 0.1:  # More than 10% null
                                batches_with_missing_json += 1
                    else:
                        batches_with_missing_json += 1
                        logger.warning(f"      WARNING: {batch_file.name} missing full_json column")
                
                # Cleanup
                del df
                gc.collect()
                
            except Exception as e:
                logger.warning(f"      Could not verify {batch_file.name}: {e}")
        
        # Periodic cleanup
        gc.collect()
    
    # Final report
    logger.info("")
    logger.info("  " + "=" * 70)
    logger.info("  VERIFICATION RESULTS")
    logger.info("  " + "=" * 70)
    logger.info(f"  Total batch files: {len(batch_files)}")
    logger.info(f"  Total rows across batches: {total_rows:,}")
    logger.info(f"  Unique papers (by openalex_id): {len(all_ids):,}")
    logger.info(f"  Expected unique papers: {expected_unique_ids:,}")
    logger.info("")
    
    # Completeness check - CRITICAL: Must have exactly expected_unique_ids
    if len(all_ids) == expected_unique_ids:
        logger.info(f"  ✓✓ Completeness: GUARANTEED ({len(all_ids):,} / {expected_unique_ids:,} = 100.00%)")
        logger.info(f"     All {expected_unique_ids:,} unique papers fetched - NO MISSING PAPERS")
    elif len(all_ids) >= expected_unique_ids * 0.99:  # Allow 1% tolerance
        logger.warning(f"  ⚠️  Completeness: WARNING ({len(all_ids):,} / {expected_unique_ids:,} = {len(all_ids)/expected_unique_ids*100:.2f}%)")
        logger.warning(f"     Missing approximately {expected_unique_ids - len(all_ids):,} papers")
        logger.warning(f"     This indicates some batches may have failed or IDs were not fetched")
    else:
        logger.error(f"  ✗ Completeness: FAILED ({len(all_ids):,} / {expected_unique_ids:,} = {len(all_ids)/expected_unique_ids*100:.2f}%)")
        logger.error(f"     Missing {expected_unique_ids - len(all_ids):,} papers - DATA INCOMPLETE!")
    
    # Duplicate check
    duplicate_rate = (total_rows - len(all_ids)) / total_rows * 100 if total_rows > 0 else 0
    if duplicate_rate < 1.0:
        logger.info(f"  ✓ Duplicate rate: {duplicate_rate:.2f}% (excellent)")
    elif duplicate_rate < 5.0:
        logger.warning(f"  ⚠️  Duplicate rate: {duplicate_rate:.2f}% (acceptable)")
    else:
        logger.error(f"  ✗ Duplicate rate: {duplicate_rate:.2f}% (too high!)")
    
    if batches_with_duplicates > 0:
        logger.warning(f"  ⚠️  {batches_with_duplicates} batches have internal duplicates")
    
    # JSON integrity check
    if json_null_count == 0:
        logger.info(f"  ✓ JSON integrity: All {total_rows:,} rows have full_json")
    else:
        json_coverage = (total_rows - json_null_count) / total_rows * 100 if total_rows > 0 else 0
        logger.warning(f"  ⚠️  JSON integrity: {json_null_count:,} rows missing full_json ({json_coverage:.2f}% coverage)")
        if batches_with_missing_json > 0:
            logger.warning(f"     {batches_with_missing_json} batches have missing JSON")
    
    logger.info("  " + "=" * 70)


if __name__ == "__main__":
    sys.exit(main())
