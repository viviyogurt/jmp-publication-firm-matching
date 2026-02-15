#!/usr/bin/env python3
"""
Extract and Flatten JSON Data from arxiv_index_enhanced

This script reads the arxiv_index_enhanced.parquet file and extracts all
information from the JSON column, then merges it with the base columns
(openalex_id, arxiv_id, doi) to create a fully flattened dataset.

The script is optimized for large files (40M+ rows) using:
- Multiprocessing with all available CPU cores
- Polars streaming/chunked processing
- Efficient JSON parsing
- Memory-efficient operations

Date: 2025
"""

# Set thread limits for libraries to avoid contention in multiprocessing
# This is critical when using multiprocessing with libraries that are themselves
# multi-threaded (e.g., NumPy with MKL or OpenBLAS). By forcing each process
# to use a single thread for its linear algebra operations, we avoid contention
# and context-switching overhead.
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
from typing import Dict, Any, Optional, List, Tuple
import sys
from datetime import datetime
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"

# Input file
INPUT_FILE = DATA_RAW / "openalex_claude_arxiv_index_enhanced.parquet"

# Output file
OUTPUT_FILE = DATA_PROCESSED / "arxiv_index_enhanced_flattened.parquet"

# Ensure output directory exists
DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

# Set up logging
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

    # Processing configuration - Optimized for speed and reliability
CHUNK_SIZE = 50000  # Large chunks for efficiency
BATCH_WRITE_SIZE = 1000000  # Reasonable batch size to balance memory and I/O


# ============================================================================
# Helper Functions
# ============================================================================

def flatten_json(work: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    Recursively flatten a nested JSON dictionary.
    
    Parameters:
    -----------
    work : Dict[str, Any]
        The JSON work object to flatten
    prefix : str
        Prefix for nested keys (used in recursion)
        
    Returns:
    --------
    Dict[str, Any]
        Flattened dictionary with dot-separated keys
    """
    flattened = {}
    
    for key, value in work.items():
        new_key = f"{prefix}.{key}" if prefix else key
        
        if value is None:
            flattened[new_key] = None
        elif isinstance(value, dict):
            # Recursively flatten nested dictionaries
            flattened.update(flatten_json(value, new_key))
        elif isinstance(value, list):
            # For lists, we have several strategies:
            # 1. If empty, store as None
            # 2. If all items are simple (str, int, float, bool), join them
            # 3. If items are dicts, convert to JSON string
            if len(value) == 0:
                flattened[new_key] = None
            elif all(isinstance(item, (str, int, float, bool)) for item in value):
                # Simple list - join with semicolon
                flattened[new_key] = "; ".join(str(v) for v in value)
            else:
                # Complex list (e.g., authorships) - store as JSON string
                flattened[new_key] = json.dumps(value)
        elif isinstance(value, (str, int, float, bool)):
            flattened[new_key] = value
        else:
            # Fallback: convert to string
            flattened[new_key] = str(value)
    
    return flattened


def extract_json_fields(json_str: Optional[str]) -> Dict[str, Any]:
    """
    Extract and flatten all fields from a JSON string.
    
    Parameters:
    -----------
    json_str : Optional[str]
        JSON string to parse
        
    Returns:
    --------
    Dict[str, Any]
        Flattened dictionary of all JSON fields
    """
    if not json_str:
        return {}
    
    try:
        if isinstance(json_str, bytes):
            json_str = json_str.decode('utf-8')
        
        work = json.loads(json_str)
        return flatten_json(work)
    except json.JSONDecodeError as e:
        logger.warning(f"JSON decode error: {str(e)[:100]}")
        return {}
    except Exception as e:
        logger.warning(f"Error parsing JSON: {str(e)[:100]}")
        return {}


def process_chunk_worker(chunk_data: bytes) -> Optional[pl.DataFrame]:
    """
    Worker function to process a chunk of data in a separate process.
    Receives serialized chunk data to avoid reading the full file in each worker.
    
    Parameters:
    -----------
    chunk_data : bytes
        Pickled/serialized chunk DataFrame
        
    Returns:
    --------
    Optional[pl.DataFrame]
        DataFrame with extracted JSON fields merged with base columns, or None on error
    """
    import pickle
    import gc  # Garbage collection for memory efficiency
    
    try:
        # Deserialize the chunk
        df_chunk = pickle.loads(chunk_data)
        
        # Extract JSON fields for each row
        all_flattened = []
        
        for row in df_chunk.iter_rows(named=True):
            json_str = row.get('json')
            flattened_json = extract_json_fields(json_str)
            
            # Merge base columns with flattened JSON
            merged_row = {
                'openalex_id': row.get('openalex_id'),
                'arxiv_id': row.get('arxiv_id'),
                'doi': row.get('doi')
            }
            merged_row.update(flattened_json)
            all_flattened.append(merged_row)
        
        if not all_flattened:
            return None
        
        # Convert to DataFrame
        try:
            result_df = pl.DataFrame(all_flattened)
            # Force garbage collection to free memory
            gc.collect()
            return result_df
        except Exception as e:
            # Fallback: try with schema inference disabled
            try:
                result_df = pl.DataFrame(all_flattened, infer_schema_length=None)
                gc.collect()
                return result_df
            except Exception as e2:
                print(f"Fallback DataFrame creation failed: {str(e2)[:100]}", file=sys.stderr)
                return None
    
    except Exception as e:
        # Log error but don't fail the whole process
        print(f"Error processing chunk: {str(e)[:200]}", file=sys.stderr)
        gc.collect()
        return None


def process_chunk(df_chunk: pl.DataFrame) -> pl.DataFrame:
    """
    Process a chunk of data to extract JSON fields (non-multiprocessing version).
    
    Parameters:
    -----------
    df_chunk : pl.DataFrame
        Chunk of arxiv_index_enhanced data
        
    Returns:
    --------
    pl.DataFrame
        DataFrame with extracted JSON fields merged with base columns
    """
    # Get base columns (excluding json)
    base_cols = ['openalex_id', 'arxiv_id', 'doi']
    base_df = df_chunk.select(base_cols)
    
    # Extract JSON fields for each row
    all_flattened = []
    
    for row in df_chunk.iter_rows(named=True):
        json_str = row.get('json')
        flattened_json = extract_json_fields(json_str)
        
        # Merge base columns with flattened JSON
        merged_row = {
            'openalex_id': row.get('openalex_id'),
            'arxiv_id': row.get('arxiv_id'),
            'doi': row.get('doi')
        }
        merged_row.update(flattened_json)
        all_flattened.append(merged_row)
    
    if not all_flattened:
        return pl.DataFrame()
    
    # Convert to DataFrame
    # We need to handle schema inference carefully for large datasets
    try:
        result_df = pl.DataFrame(all_flattened)
        return result_df
    except Exception as e:
        logger.warning(f"Error creating DataFrame from chunk: {str(e)[:100]}")
        # Fallback: try with schema inference disabled
        return pl.DataFrame(all_flattened, infer_schema_length=None)


def get_all_json_keys(sample_size: int = 1000) -> List[str]:
    """
    Scan a sample of rows to identify all possible JSON keys.
    This helps us create a consistent schema.
    
    Parameters:
    -----------
    sample_size : int
        Number of rows to sample
        
    Returns:
    --------
    List[str]
        List of all unique JSON keys found
    """
    logger.info(f"Scanning {sample_size} rows to identify JSON schema...")
    
    all_keys = set()
    
    try:
        df_sample = pl.read_parquet(INPUT_FILE, n_rows=sample_size)
        
        for row in df_sample.iter_rows(named=True):
            json_str = row.get('json')
            if json_str:
                flattened = extract_json_fields(json_str)
                all_keys.update(flattened.keys())
        
        logger.info(f"Found {len(all_keys)} unique JSON keys")
        return sorted(list(all_keys))
    except Exception as e:
        logger.warning(f"Error scanning JSON keys: {str(e)}")
        return []


# ============================================================================
# Main Processing
# ============================================================================

def extract_all_json_fields():
    """
    Extract all JSON fields from arxiv_index_enhanced and merge with base columns.
    """
    logger.info("=" * 80)
    logger.info("Extracting JSON Fields from arxiv_index_enhanced")
    logger.info("=" * 80)
    logger.info(f"Input: {INPUT_FILE}")
    logger.info(f"Output: {OUTPUT_FILE}")
    logger.info("")
    
    # Check if input file exists
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        sys.exit(1)
    
    # Get file size
    file_size_gb = INPUT_FILE.stat().st_size / (1024 ** 3)
    logger.info(f"Input file size: {file_size_gb:.2f} GB")
    
    # Get total row count (approximate)
    logger.info("Counting total rows...")
    try:
        # Use a quick scan to get row count
        df_sample = pl.scan_parquet(INPUT_FILE).select(pl.len().alias('count')).collect()
        total_rows = df_sample['count'][0]
        logger.info(f"  Total rows: {total_rows:,}")
    except Exception as e:
        logger.warning(f"Could not get row count: {str(e)}")
        total_rows = None
    
    logger.info("")
    
    # Scan for JSON keys to understand schema
    json_keys = get_all_json_keys(sample_size=1000)
    logger.info("")
    
    # Process in chunks using optimized streaming approach
    logger.info(f"Processing in chunks of {CHUNK_SIZE:,} rows...")
    logger.info("Using optimized streaming approach for reliability...")
    logger.info("This will take several hours for 40M+ rows...")
    logger.info("")

    # Use reliable sequential streaming processing
    all_results = []
    processed = 0
    chunk_num = 0
    start_time = datetime.now()

    # Use Polars streaming for memory efficiency
    df_stream = pl.scan_parquet(INPUT_FILE)

    # Get exact row count if not provided
    if total_rows is None:
        try:
            total_rows = df_stream.select(pl.len()).collect()[0, 0]
            logger.info(f"Exact row count: {total_rows:,}")
        except:
            logger.warning("Could not get exact count, using estimate")
            total_rows = 40000000

    # Process in streaming chunks
    offset = 0
    while offset < total_rows:
        chunk_num += 1
        chunk_end = min(offset + CHUNK_SIZE, total_rows)

        logger.info(f"Processing chunk {chunk_num}: rows {offset:,} to {chunk_end:,}...")

        try:
            # Read chunk using streaming
            df_chunk = df_stream.slice(offset, CHUNK_SIZE).collect(streaming=True)

            if len(df_chunk) == 0:
                break

            # Process chunk
            chunk_results = process_chunk(df_chunk)

            if len(chunk_results) > 0:
                all_results.append(chunk_results)

            processed = chunk_end

            # Progress update
            progress_pct = (processed / total_rows * 100) if total_rows > 0 else 0
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = processed / elapsed if elapsed > 0 else 0
            eta_seconds = (total_rows - processed) / rate if rate > 0 else 0
            eta_hours = eta_seconds / 3600


def extract_all_json_fields():
    """
    Extract all JSON fields from arxiv_index_enhanced and merge with base columns.
    """
    logger.info("=" * 80)
    logger.info("Extracting JSON Fields from arxiv_index_enhanced")
    logger.info("=" * 80)
    logger.info(f"Input: {INPUT_FILE}")
    logger.info(f"Output: {OUTPUT_FILE}")
    logger.info("")
    
    # Check if input file exists
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        sys.exit(1)
    
    # Get file size
    file_size_gb = INPUT_FILE.stat().st_size / (1024 ** 3)
    logger.info(f"Input file size: {file_size_gb:.2f} GB")
    
    # Get total row count (approximate)
    logger.info("Counting total rows...")
    try:
        # Use a quick scan to get row count
        df_sample = pl.scan_parquet(INPUT_FILE).select(pl.len()).collect()
        total_rows = df_sample['count'][0]
        logger.info(f"  Total rows: {total_rows:,}")
    except Exception as e:
        logger.warning(f"Could not get row count: {str(e)}")
        total_rows = None
    
    logger.info("")
    
    # Process in chunks using optimized streaming approach
    logger.info(f"Processing in chunks of {CHUNK_SIZE:,} rows...")
    logger.info("Using optimized streaming approach for reliability...")
    logger.info("This will take several hours for 40M+ rows...")
    logger.info("")
    
    all_results = []
    processed = 0
    chunk_num = 0
    start_time = datetime.now()
    
    # Use Polars streaming for memory efficiency
    df_stream = pl.scan_parquet(INPUT_FILE)
    
    # Get exact row count if not provided
    if total_rows is None:
        try:
            total_rows = df_stream.select(pl.len()).collect()[0, 0]
            logger.info(f"Exact row count: {total_rows:,}")
        except:
            logger.warning("Could not get exact count, using estimate")
            total_rows = 40000000
    
    # Process in streaming chunks
    offset = 0
    while offset < total_rows:
        chunk_num += 1
        chunk_end = min(offset + CHUNK_SIZE, total_rows)
        
        logger.info(f"Processing chunk {chunk_num}: rows {offset:,} to {chunk_end:,}...")
        
        try:
            # Read chunk using streaming
            df_chunk = df_stream.slice(offset, CHUNK_SIZE).collect(streaming=True)
            
            if len(df_chunk) == 0:
                break
            
            # Process chunk
            chunk_results = process_chunk(df_chunk)
            
            if len(chunk_results) > 0:
                all_results.append(chunk_results)
            
            processed = chunk_end
            
            # Progress update
            progress_pct = (processed / total_rows * 100) if total_rows > 0 else 0
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = processed / elapsed if elapsed > 0 else 0
            eta_seconds = (total_rows - processed) / rate if rate > 0 else 0
            eta_hours = eta_seconds / 3600
            
            logger.info(f"  Progress: {progress_pct:.1f}% | "
                      f"Processed: {processed:,}/{total_rows:,} | "
                      f"Rate: {rate:.0f} rows/sec | "
                      f"ETA: {eta_hours:.1f} hours")
            
            # Write intermediate batches to manage memory
            if len(all_results) >= (BATCH_WRITE_SIZE // CHUNK_SIZE):
                logger.info("  Writing intermediate batch...")
                batch_df = pl.concat(all_results)
                
                if OUTPUT_FILE.exists():
                    existing_df = pl.read_parquet(OUTPUT_FILE)
                    batch_df = pl.concat([existing_df, batch_df])
                
                batch_df.write_parquet(OUTPUT_FILE, compression='snappy')
                all_results = []  # Clear memory
                logger.info("  Intermediate batch written")
            
            offset = chunk_end
        
        except Exception as e:
            logger.error(f"Error processing chunk {chunk_num}: {str(e)}")
            offset += CHUNK_SIZE
            continue
    
    # Write final results
    if all_results:
        logger.info("Writing final batch...")
        final_df = pl.concat(all_results)
        
        if OUTPUT_FILE.exists():
            existing_df = pl.read_parquet(OUTPUT_FILE)
            final_df = pl.concat([existing_df, final_df])
        
        final_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info("Final batch written")
    
    # Summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("EXTRACTION COMPLETED")
    logger.info("=" * 80)
    
    if OUTPUT_FILE.exists():
        file_size_gb = OUTPUT_FILE.stat().st_size / (1024 ** 3)
        logger.info(f"Output file size: {file_size_gb:.2f} GB")
        
        # Get row count
        try:
            df_final = pl.read_parquet(OUTPUT_FILE, n_rows=1)
            logger.info(f"Output columns: {len(df_final.columns)}")
            logger.info(f"Sample columns: {df_final.columns[:10]}")
        except Exception as e:
            logger.warning(f"Could not read output file: {str(e)}")
    
    elapsed = (datetime.now() - start_time).total_seconds() / 3600
    logger.info(f"Total processing time: {elapsed:.2f} hours")
    logger.info("=" * 80)

def main():
    """Main execution function"""
    try:
        extract_all_json_fields()
        logger.info("\n✓ JSON extraction completed successfully!")
    except KeyboardInterrupt:
        logger.warning("\n✗ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
