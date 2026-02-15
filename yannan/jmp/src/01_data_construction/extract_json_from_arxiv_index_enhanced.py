#!/usr/bin/env python3
"""
Extract JSON from arxiv_index_enhanced - OPTIMIZED MULTIPROCESSING VERSION

Features:
- Writes each batch to a separate parquet file (no memory explosion)
- Small batch size (20K rows) for safety with 13K+ columns
- Efficient multiprocessing: reads batch once, pre-splits, passes to workers (no I/O contention)
- Uses 64 workers to fully utilize 96-core server
- Tracks progress in checkpoint file
- Can resume from last completed batch if interrupted
- Thread-safe: sets OMP/MKL/OPENBLAS threads to 1 before imports

Usage:
    python extract_json_from_arxiv_index_enhanced.py

To read final data:
    df = pl.scan_parquet("output_dir/*.parquet").collect()
"""

# --- CRUCIAL: Set thread environment variables BEFORE importing any numerical libraries ---
# This prevents thread oversubscription, where each worker process spawns its own
# thread pool, leading to massive contention and slower-than-serial performance.
# These MUST be set before importing polars, numpy, or any other numerical libraries.
import os
os.environ['OMP_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['VECLIB_MAXIMUM_THREADS'] = '1'
os.environ['NUMEXPR_NUM_THREADS'] = '1'

import polars as pl
import json
from pathlib import Path
import logging
import sys
from datetime import datetime
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import gc
from typing import Dict, Any, List, Tuple

# Try to use orjson for faster JSON parsing (2-3x faster than standard json)
try:
    import orjson
    JSON_LOAD = orjson.loads
    JSON_DUMP = lambda x: orjson.dumps(x).decode('utf-8')
    JSON_AVAILABLE = "orjson"
except ImportError:
    JSON_LOAD = json.loads
    JSON_DUMP = json.dumps
    JSON_AVAILABLE = "json"

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"

INPUT_FILE = DATA_RAW / "openalex_claude_arxiv_index_enhanced.parquet"
OUTPUT_DIR = DATA_PROCESSED / "arxiv_flattened_batches"
PROGRESS_FILE = OUTPUT_DIR / "_progress.json"
SCHEMA_FILE = OUTPUT_DIR / "_schema.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = PROJECT_ROOT / "logs" / "extract_json_arxiv_index_enhanced.log"
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# OPTIMIZED Configuration - Use more cores effectively
# ============================================================================
NUM_WORKERS = 64          # Use 64 workers (2/3 of 96 cores, leaving room for system)
ROWS_PER_WORKER = 500     # Smaller chunks per worker for better load balancing
BATCH_SIZE = 20000        # 20K rows per batch (safe with predefined schema)

# ============================================================================
# Predefined Schema - Only useful columns (excludes abstract_inverted_index)
# ============================================================================
# This dramatically speeds up processing by avoiding schema inference and
# excluding 12,986 abstract_inverted_index columns (word positions, not useful)
USEFUL_COLUMNS = [
    # Base identifiers (from input file)
    'openalex_id', 'arxiv_id', 'doi',
    # Core metadata
    'id', 'doi_registration_agency', 'display_name', 'title',
    'publication_year', 'publication_date', 'language', 'language_id',
    'type', 'type_crossref', 'type_id', 'indexed_in',
    # Authors & affiliations
    'authorships', 'authors_count', 'corresponding_author_ids',
    'corresponding_institution_ids', 'institution_assertions',
    'institutions_distinct_count', 'countries_distinct_count',
    # Citations & impact
    'cited_by_count', 'cited_by_api_url',
    'cited_by_percentile_year.max', 'cited_by_percentile_year.min',
    'citation_normalized_percentile', 'citation_normalized_percentile.value',
    'citation_normalized_percentile.is_in_top_1_percent',
    'citation_normalized_percentile.is_in_top_10_percent',
    'summary_stats.cited_by_count', 'summary_stats.2yr_cited_by_count',
    'referenced_works', 'referenced_works_count', 'related_works',
    # Topics & concepts
    'topics', 'topics_count', 'primary_topic', 'primary_topic.id',
    'primary_topic.display_name', 'primary_topic.score',
    'primary_topic.domain.id', 'primary_topic.domain.display_name',
    'primary_topic.field.id', 'primary_topic.field.display_name',
    'primary_topic.subfield.id', 'primary_topic.subfield.display_name',
    'concepts', 'concepts_count', 'keywords',
    # Geographic & locations
    'locations', 'locations_count', 'best_oa_location',
    # Publication venue
    'primary_location.doi', 'primary_location.is_accepted',
    'primary_location.is_oa', 'primary_location.is_published',
    'primary_location.landing_page_url', 'primary_location.license',
    'primary_location.license_id', 'primary_location.pdf_url',
    'primary_location.version',
    'primary_location.source.id', 'primary_location.source.display_name',
    'primary_location.source.issn', 'primary_location.source.issn_l',
    'primary_location.source.publisher', 'primary_location.source.type',
    'primary_location.source.type_id', 'primary_location.source.is_core',
    'primary_location.source.is_in_doaj', 'primary_location.source.is_oa',
    'primary_location.source.is_indexed_in_scopus',
    'primary_location.source.host_organization',
    'primary_location.source.host_organization_name',
    'primary_location.source.host_organization_lineage',
    'primary_location.source.host_organization_lineage_names',
    # Open access
    'open_access.is_oa', 'open_access.oa_status', 'open_access.oa_url',
    'open_access.any_repository_has_fulltext',
    # Funding
    'grants',
    # Bibliographic
    'biblio.volume', 'biblio.issue', 'biblio.first_page', 'biblio.last_page',
    # Other metadata
    'ids.openalex', 'ids.doi', 'has_fulltext', 'is_retracted', 'is_paratext',
    'datasets', 'versions', 'mesh', 'fwci', 'apc_paid', 'apc_list',
    'sustainable_development_goals', 'counts_by_year',
    'created_date', 'updated', 'updated_date',
]

# Create predefined schema (all as Utf8 for safety, can cast later)
PREDEFINED_SCHEMA = {col: pl.Utf8 for col in USEFUL_COLUMNS}


# ============================================================================
# Progress Management
# ============================================================================

def load_progress() -> Dict:
    """Load progress from checkpoint file."""
    if PROGRESS_FILE.exists():
        try:
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
        except:
            return {"completed_batches": [], "total_batches": 0, "total_rows": 0}
    return {"completed_batches": [], "total_batches": 0, "total_rows": 0}


def save_progress(progress: Dict):
    """Save progress to checkpoint file."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def get_batch_file(batch_num: int) -> Path:
    """Get the output file path for a batch."""
    return OUTPUT_DIR / f"batch_{batch_num:05d}.parquet"


# ============================================================================
# Schema Discovery - Discover all possible column names from a sample
# ============================================================================

def discover_schema_from_sample(sample_size: int = 1000, max_rows_to_check: int = 10000) -> Tuple[set, Dict[str, int]]:
    """
    Discover all possible JSON column names from a sample of rows.
    
    This is MUCH faster than inferring schema from all rows during DataFrame creation.
    
    Args:
        sample_size: Number of rows to sample for initial discovery
        max_rows_to_check: Maximum rows to check if sample_size doesn't find all columns
        
    Returns:
        Tuple of (set of all column names, dict of column name -> occurrence count)
    """
    logger.info(f"Discovering schema from sample of {sample_size:,} rows...")
    
    all_keys = set()
    key_counts = {}
    rows_checked = 0
    
    try:
        # Read sample rows
        df_sample = (
            pl.scan_parquet(INPUT_FILE)
            .head(sample_size)
            .collect()
        )
        
        rows_checked = len(df_sample)
        logger.info(f"  Processing {rows_checked:,} rows for schema discovery...")
        
        # Process each row to discover all keys
        for idx, row in enumerate(df_sample.iter_rows(named=True)):
            json_str = row.get('json')
            if json_str:
                try:
                    # Handle bytes if needed
                    if isinstance(json_str, bytes):
                        json_str = json_str.decode('utf-8')
                    
                    # Parse JSON
                    work = JSON_LOAD(json_str)
                    
                    # Flatten using optimized iterative approach
                    # Filter to only useful columns (excludes abstract_inverted_index)
                    useful_columns_set = set(USEFUL_COLUMNS[3:])  # Skip base columns (openalex_id, arxiv_id, doi)
                    flattened = flatten_json_iterative(work, filter_columns=useful_columns_set)
                    
                    # Track all keys and their occurrences
                    for key in flattened.keys():
                        all_keys.add(key)
                        key_counts[key] = key_counts.get(key, 0) + 1
                    
                    # Progress update every 100 rows
                    if (idx + 1) % 100 == 0:
                        logger.info(f"    Processed {idx + 1:,}/{rows_checked:,} rows, found {len(all_keys):,} unique columns so far...")
                        
                except Exception as e:
                    # Skip rows with JSON parsing errors
                    if idx < 10:  # Log first few errors for debugging
                        logger.debug(f"    Row {idx} JSON parsing failed: {str(e)[:100]}")
                    continue
        
        logger.info(f"  Schema discovery complete: {len(all_keys):,} unique columns found")
        logger.info(f"  Columns found in {rows_checked:,} rows")
        
        # Log some statistics
        if key_counts:
            sorted_keys = sorted(key_counts.items(), key=lambda x: x[1], reverse=True)
            logger.info(f"  Top 10 most common columns:")
            for key, count in sorted_keys[:10]:
                pct = (count / rows_checked) * 100
                logger.info(f"    {key}: {count:,} rows ({pct:.1f}%)")
        
        return all_keys, key_counts
        
    except Exception as e:
        logger.error(f"Error during schema discovery: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return set(), {}


def create_schema_from_keys(all_keys: set) -> Dict[str, pl.DataType]:
    """
    Create a Polars schema from discovered column names.
    
    Args:
        all_keys: Set of all column names discovered
        
    Returns:
        Dictionary mapping column names to Polars data types
    """
    # Base columns
    schema = {
        'openalex_id': pl.Utf8,
        'arxiv_id': pl.Utf8,
        'doi': pl.Utf8,
    }
    
    # Add all JSON columns as Utf8 (safest for mixed types, can be cast later if needed)
    for key in sorted(all_keys):
        schema[key] = pl.Utf8
    
    return schema


def save_schema(schema: Dict[str, pl.DataType], schema_file: Path):
    """Save discovered schema to JSON file."""
    # Convert Polars types to strings for JSON serialization
    schema_dict = {k: str(v) for k, v in schema.items()}
    with open(schema_file, 'w') as f:
        json.dump(schema_dict, f, indent=2)
    logger.info(f"Schema saved to {schema_file}")


def load_schema(schema_file: Path) -> Dict[str, pl.DataType]:
    """Load schema from JSON file."""
    if not schema_file.exists():
        return None
    
    with open(schema_file, 'r') as f:
        schema_dict = json.load(f)
    
    # Convert string types back to Polars types
    # For now, we'll use Utf8 for all (simplest approach)
    schema = {}
    for key, type_str in schema_dict.items():
        schema[key] = pl.Utf8
    
    return schema


# ============================================================================
# Worker Functions - Process pre-split data chunks (like BLP script pattern)
# ============================================================================

def flatten_json_recursive(work: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Recursively flatten a nested JSON dictionary."""
    flattened = {}
    for key, value in work.items():
        new_key = f"{prefix}.{key}" if prefix else key
        if value is None:
            flattened[new_key] = None
        elif isinstance(value, dict):
            flattened.update(flatten_json_recursive(value, new_key))
        elif isinstance(value, list):
            if len(value) == 0:
                flattened[new_key] = None
            elif all(isinstance(item, (str, int, float, bool)) for item in value):
                flattened[new_key] = "; ".join(str(v) for v in value)
            else:
                flattened[new_key] = JSON_DUMP(value)
        elif isinstance(value, (str, int, float, bool)):
            flattened[new_key] = value
        else:
            flattened[new_key] = str(value)
    return flattened


def flatten_json_iterative(work: Dict[str, Any], filter_columns: set = None) -> Dict[str, Any]:
    """
    Optimized iterative flattening - faster than recursive for deep structures.
    Uses a stack instead of recursion to avoid stack overflow and improve performance.
    
    Args:
        work: The JSON dictionary to flatten
        filter_columns: Set of column names to keep (None = keep all)
        
    Returns:
        Flattened dictionary with dot-separated keys (filtered if filter_columns provided)
    """
    flattened = {}
    stack = [(work, "")]
    
    while stack:
        obj, prefix = stack.pop()
        for key, value in obj.items():
            new_key = f"{prefix}.{key}" if prefix else key
            
            # Skip abstract_inverted_index columns (word positions, not useful)
            # and filter to only useful columns if filter_columns is provided
            if filter_columns is not None:
                # Check if this key (or any parent key) is in our useful columns
                # We need to check both the exact key and if it's a prefix of any useful column
                if new_key not in filter_columns:
                    # Check if this is a prefix of any useful column (for nested structures)
                    is_prefix = any(useful_col.startswith(new_key + '.') for useful_col in filter_columns)
                    if not is_prefix:
                        continue
            
            if value is None:
                flattened[new_key] = None
            elif isinstance(value, dict):
                stack.append((value, new_key))
            elif isinstance(value, list):
                if len(value) == 0:
                    flattened[new_key] = None
                elif all(isinstance(item, (str, int, float, bool)) for item in value):
                    # Use join() - faster than repeated concatenation
                    flattened[new_key] = "; ".join(str(v) for v in value)
                else:
                    # Use JSON_DUMP (orjson if available, else json)
                    flattened[new_key] = JSON_DUMP(value)
            elif isinstance(value, (str, int, float, bool)):
                flattened[new_key] = value
            else:
                flattened[new_key] = str(value)
    
    return flattened


def process_rows_chunk(rows_chunk: List[Dict]) -> List[Dict[str, Any]]:
    """
    Process a chunk of rows - receives pre-split data (like BLP script pattern).
    This avoids I/O contention from multiple workers reading the same file.
    
    CRITICAL: Thread environment variables are set at module level (before imports)
    to prevent thread oversubscription. Do NOT set them here in the worker function.
    
    Args:
        rows_chunk: List of row dictionaries (pre-split in main process)
        
    Returns:
        List of flattened row dictionaries
    """
    results = []
    errors = []
    
    for idx, row in enumerate(rows_chunk):
        json_str = row.get('json')
        flattened = {}
        
        if json_str:
            try:
                # Handle bytes if needed
                if isinstance(json_str, bytes):
                    json_str = json_str.decode('utf-8')
                
                # Use fast JSON parser (orjson if available)
                work = JSON_LOAD(json_str)
        
                # Flatten using optimized iterative approach
                # Filter to only useful columns (excludes abstract_inverted_index)
                useful_columns_set = set(USEFUL_COLUMNS[3:])  # Skip base columns (openalex_id, arxiv_id, doi)
                flattened = flatten_json_iterative(work, filter_columns=useful_columns_set)
                
            except Exception as e:
                # Log error details for debugging (but don't fail the whole chunk)
                error_msg = f"Row {idx} JSON parsing failed: {type(e).__name__}: {str(e)[:200]}"
                errors.append(error_msg)
                # Continue processing - will just have empty flattened dict
        
        result = {
            'openalex_id': row.get('openalex_id'),
            'arxiv_id': row.get('arxiv_id'),
            'doi': row.get('doi')
        }
        result.update(flattened)
        
        # Convert all values to strings (handle bytes, None, etc.)
        # This is necessary because Polars schema expects consistent types
        cleaned_result = {}
        for key, value in result.items():
            if value is None:
                cleaned_result[key] = None
            elif isinstance(value, bytes):
                # Convert bytes to string
                try:
                    cleaned_result[key] = value.decode('utf-8')
                except:
                    cleaned_result[key] = str(value)
            elif isinstance(value, (dict, list)):
                # Complex types should already be JSON strings from flatten_json_iterative
                cleaned_result[key] = str(value) if not isinstance(value, str) else value
            else:
                cleaned_result[key] = value
        
        results.append(cleaned_result)
    
    # Return results and errors (errors will be logged in main process)
    return results, errors


# ============================================================================
# Batch Processing
# ============================================================================

def process_single_batch(batch_num: int, batch_start: int, batch_size: int) -> bool:
    """
    Process a single batch using Polars native JSON operations - MUCH faster.
    
    Uses Polars native JSON decode and struct unnest instead of Python dict operations.
    Falls back to multiprocessing if Polars JSON operations fail.
    """
    output_file = get_batch_file(batch_num)
    
    try:
        logger.info(f"  Reading batch {batch_num} (rows {batch_start:,} to {batch_start + batch_size:,})...")
        
        # Read batch - use head/tail for sequential reading (faster than row index filtering)
        df_batch = (
            pl.scan_parquet(INPUT_FILE)
            .head(batch_start + batch_size)
            .tail(batch_size)
            .collect()
        )
        
        actual_rows = len(df_batch)
        
        if actual_rows == 0:
            logger.info(f"  Batch {batch_num}: No rows to process")
            return True
        
        logger.info(f"  Processing batch with optimized multiprocessing (iterative flattening)...")
        
        # Convert to list of dicts for distribution to workers
        batch_rows = df_batch.to_dicts()
        del df_batch
        gc.collect()
        
        # Pre-split into chunks for workers
        row_chunks = []
        for i in range(0, len(batch_rows), ROWS_PER_WORKER):
            chunk = batch_rows[i:i + ROWS_PER_WORKER]
            if chunk:
                row_chunks.append(chunk)
        
        del batch_rows
        gc.collect()
        
        # Process in parallel - workers receive pre-split data
        all_results = []
        worker_errors = []
        
        logger.info(f"  Processing {len(row_chunks)} chunks with {NUM_WORKERS} workers (using {JSON_AVAILABLE} for JSON parsing)...")
        
        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = {executor.submit(process_rows_chunk, chunk): i 
                      for i, chunk in enumerate(row_chunks)}
            
            for future in as_completed(futures):
                chunk_idx = futures[future]
                try:
                    result, chunk_errors = future.result()
                    all_results.extend(result)
                    if chunk_errors:
                        worker_errors.extend([f"Chunk {chunk_idx}: {e}" for e in chunk_errors])
                except Exception as e:
                    worker_errors.append(f"Chunk {chunk_idx} failed completely: {type(e).__name__}: {str(e)[:200]}")
        
        # Log errors in main process (but don't fail the batch)
        if worker_errors:
            logger.warning(f"  {len(worker_errors)} errors encountered during processing (batch will continue):")
            # Log first 10 errors to avoid log spam
            for error in worker_errors[:10]:
                logger.warning(f"    {error}")
            if len(worker_errors) > 10:
                logger.warning(f"    ... and {len(worker_errors) - 10} more errors")
        
        gc.collect()
        
        # Write to individual batch file
        if all_results:
            # Use predefined schema for MUCH faster DataFrame creation
            # This avoids scanning all rows to infer schema (10-50x faster)
            try:
                # Ensure all dictionaries have the same keys (add missing keys as None)
                # This is required for Polars schema to work correctly
                standardized_results = []
                for result in all_results:
                    standardized = {col: result.get(col, None) for col in USEFUL_COLUMNS}
                    standardized_results.append(standardized)
                
                df_result = pl.DataFrame(standardized_results, schema=PREDEFINED_SCHEMA)
                df_result.write_parquet(output_file, compression='snappy')
                logger.info(f"  Written {len(all_results):,} rows to {output_file.name} ({len(df_result.columns)} columns)")
                del df_result, standardized_results
            except Exception as e:
                # Report detailed error for debugging
                logger.error(f"  DataFrame creation failed: {type(e).__name__}: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                
                # Debug: Check first result structure
                if all_results:
                    first_result = all_results[0]
                    logger.error(f"  First result keys: {list(first_result.keys())[:20]}...")
                    logger.error(f"  First result sample values:")
                    for key, val in list(first_result.items())[:10]:
                        val_type = type(val).__name__
                        val_preview = str(val)[:50] if val is not None else "None"
                        logger.error(f"    {key}: {val_type} = {val_preview}")
                
                # Remove partial file if it exists
                if output_file.exists():
                    try:
                        output_file.unlink()
                    except:
                        pass
                return False
        
        del all_results
        gc.collect()
        
        return True
    
    except Exception as e:
        logger.error(f"  Batch {batch_num} failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # Remove partial file if it exists
        if output_file.exists():
            try:
                output_file.unlink()
            except:
                pass
        return False


# ============================================================================
# Main Processing
# ============================================================================

def extract_all_json_fields():
    """Main extraction with resume capability."""
    logger.info("=" * 80)
    logger.info("OPTIMIZED MULTIPROCESSING JSON Extraction")
    logger.info("=" * 80)
    logger.info(f"Input: {INPUT_FILE}")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info(f"Workers: {NUM_WORKERS}")
    logger.info(f"Rows per worker: {ROWS_PER_WORKER:,}")
    logger.info(f"Batch size: {BATCH_SIZE:,} rows")
    logger.info(f"JSON parser: {JSON_AVAILABLE} ({'fast' if JSON_AVAILABLE == 'orjson' else 'standard'})")
    logger.info(f"Columns: {len(USEFUL_COLUMNS):,} (predefined schema, excludes abstract_inverted_index)")
    logger.info("")
    
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        sys.exit(1)
    
    file_size_gb = INPUT_FILE.stat().st_size / (1024 ** 3)
    logger.info(f"Input file size: {file_size_gb:.2f} GB")
    
    # Get total rows
    logger.info("Counting total rows...")
    total_rows = pl.scan_parquet(INPUT_FILE).select(pl.len()).collect()[0, 0]
    total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
    logger.info(f"Total rows: {total_rows:,}")
    logger.info(f"Total batches: {total_batches:,}")
    
    # Load progress
    progress = load_progress()
    completed = set(progress.get("completed_batches", []))
    
    if completed:
        logger.info(f"RESUMING: {len(completed):,} batches already completed ({len(completed)*100/total_batches:.1f}%)")
        remaining = set(range(total_batches)) - completed
        if remaining:
            logger.info(f"Starting from batch {min(remaining) + 1}")
    
    logger.info("")
    
    # Update progress file
    progress["total_batches"] = total_batches
    progress["total_rows"] = total_rows
    progress["batch_size"] = BATCH_SIZE
    save_progress(progress)
    
    start_time = datetime.now()
    session_start_batches = len(completed)
    
    # Process each batch
    for batch_num in range(total_batches):
        # Skip already completed batches
        if batch_num in completed:
            continue
        
        batch_start = batch_num * BATCH_SIZE
        batch_end = min(batch_start + BATCH_SIZE, total_rows)
        current_batch_size = batch_end - batch_start
        
        logger.info(f"Batch {batch_num + 1}/{total_batches}: rows {batch_start:,} to {batch_end:,}")
        
        # Process batch
        batch_start_time = datetime.now()
        success = process_single_batch(batch_num, batch_start, current_batch_size)
        batch_elapsed = (datetime.now() - batch_start_time).total_seconds()
        
        if success:
            # Update progress
            completed.add(batch_num)
            progress["completed_batches"] = sorted(list(completed))
            progress["last_updated"] = datetime.now().isoformat()
            progress["last_batch"] = batch_num
            save_progress(progress)
    
            # Calculate stats
            session_batches = len(completed) - session_start_batches
            elapsed = (datetime.now() - start_time).total_seconds()
            batches_remaining = total_batches - len(completed)
            
            if session_batches > 0 and elapsed > 0:
                avg_batch_time = elapsed / session_batches
                eta_seconds = batches_remaining * avg_batch_time
                eta_hours = eta_seconds / 3600
                rows_done = len(completed) * BATCH_SIZE
                rate = (session_batches * BATCH_SIZE) / elapsed
                progress_pct = (len(completed) / total_batches * 100)
                
                logger.info(f"  Progress: {progress_pct:.1f}% | "
                          f"Batches: {len(completed):,}/{total_batches:,} | "
                          f"Rate: {rate:.0f}/sec | "
                          f"Batch time: {batch_elapsed:.1f}s | "
                          f"ETA: {eta_hours:.1f}h")
        else:
            logger.error(f"  Batch {batch_num + 1} FAILED - will retry on next run")
    
    # Summary
    elapsed_hours = (datetime.now() - start_time).total_seconds() / 3600
    logger.info("")
    logger.info("=" * 80)
    logger.info("SESSION COMPLETED")
    logger.info("=" * 80)
    logger.info(f"Completed batches: {len(completed):,}/{total_batches:,}")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info(f"Session time: {elapsed_hours:.2f} hours")
                
    if len(completed) == total_batches:
        logger.info("")
        logger.info("ALL BATCHES COMPLETED!")
        logger.info("To read all data:")
        logger.info(f"  df = pl.scan_parquet('{OUTPUT_DIR}/*.parquet').collect()")
    else:
        logger.info("")
        logger.info(f"Remaining: {total_batches - len(completed):,} batches")
        logger.info("Run again to continue from checkpoint")
    
    logger.info("=" * 80)


def main():
    # Use fork on Linux for faster startup (spawn is slower but more compatible)
    try:
        mp.set_start_method('fork', force=True)
    except RuntimeError:
        try:
            mp.set_start_method('spawn', force=True)
        except:
            pass
    
    try:
        extract_all_json_fields()
    except KeyboardInterrupt:
        logger.warning("")
        logger.warning("INTERRUPTED - Progress saved!")
        logger.warning("Run again to resume from checkpoint")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
