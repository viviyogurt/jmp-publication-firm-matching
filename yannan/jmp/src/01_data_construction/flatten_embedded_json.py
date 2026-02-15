#!/usr/bin/env python3
"""
Flatten embedded JSON fields in parquet files.

This script processes parquet files to flatten embedded JSON fields:
- topics, concepts, keywords, locations, sustainable_development_goals, authorships

Features:
- Multiprocessing with 32 workers to process multiple files in parallel
- Uses predefined schema from discovery script for speed
- Robust error handling for missing columns/exceptions
- Keeps original columns, adds new flattened columns
- Outputs to same directory with "_flatten" suffix
"""

# Set thread environment variables BEFORE importing numerical libraries
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
from concurrent.futures import ProcessPoolExecutor, as_completed
import gc
from typing import Dict, Any, List, Optional, Tuple
import multiprocessing as mp

# Try to use orjson for faster JSON parsing
try:
    import orjson
    JSON_LOAD = orjson.loads
    JSON_DUMP = lambda x: orjson.dumps(x).decode('utf-8')
    JSON_AVAILABLE = "orjson"
except ImportError:
    JSON_LOAD = json.loads
    JSON_DUMP = json.dumps
    JSON_AVAILABLE = "json"

# Configuration
INPUT_DIR = Path("/home/kurtluo/yannan/jmp/data/processed/publication/arxiv_flattened_batches")
SCHEMA_FILE = INPUT_DIR / "_embedded_json_schema.json"
PROGRESS_FILE = INPUT_DIR / "_flatten_progress.json"
LOG_FILE = Path("/home/kurtluo/yannan/jmp/logs/flatten_embedded_json.log")
JSON_COLUMNS = ['topics', 'concepts', 'keywords', 'locations', 'sustainable_development_goals', 'authorships']
NUM_WORKERS = 32
MAX_ITEMS_PER_LIST = 50  # Maximum items to extract per list field

# Setup logging
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


def load_schema() -> Optional[Dict[str, Any]]:
    """Load the discovered schema from file."""
    if not SCHEMA_FILE.exists():
        logger.warning(f"Schema file not found: {SCHEMA_FILE}")
        logger.warning("Running without predefined schema (slower but will work)")
        return None
    
    try:
        with open(SCHEMA_FILE, 'r') as f:
            schema = json.load(f)
        logger.info(f"Loaded schema from {SCHEMA_FILE}")
        return schema
    except Exception as e:
        logger.error(f"Error loading schema: {e}")
        return None


def build_predefined_schema(original_columns: List[str], schema_info: Optional[Dict[str, Any]]) -> Dict[str, pl.DataType]:
    """
    Build a predefined Polars schema from original columns + flattened JSON columns.
    """
    schema = {}
    
    # Add all original columns as Utf8 (safest for mixed types)
    for col in original_columns:
        if col not in JSON_COLUMNS:  # Exclude JSON columns (we'll add flattened versions)
            schema[col] = pl.Utf8
    
    # Add flattened columns from schema info
    if schema_info and "fields" in schema_info:
        for json_col in JSON_COLUMNS:
            if json_col not in schema_info["fields"]:
                continue
            
            field_info = schema_info["fields"][json_col]
            max_items = min(field_info.get("max_items_observed", 0), MAX_ITEMS_PER_LIST)
            all_keys = field_info.get("all_unique_keys", [])
            
            # Add count column
            schema[f"{json_col}_count"] = pl.Utf8
            
            # Add columns for each item position and key
            for idx in range(max_items + 1):
                for key in all_keys:
                    schema[f"{json_col}_{idx}_{key}"] = pl.Utf8
    
    return schema


def get_all_expected_columns(original_columns: List[str], schema_info: Optional[Dict[str, Any]]) -> List[str]:
    """
    Get list of all expected column names (original + flattened).
    """
    expected = [col for col in original_columns if col not in JSON_COLUMNS]
    
    if schema_info and "fields" in schema_info:
        for json_col in JSON_COLUMNS:
            if json_col not in schema_info["fields"]:
                continue
            
            field_info = schema_info["fields"][json_col]
            max_items = min(field_info.get("max_items_observed", 0), MAX_ITEMS_PER_LIST)
            all_keys = field_info.get("all_unique_keys", [])
            
            # Add count column
            expected.append(f"{json_col}_count")
            
            # Add columns for each item position and key
            for idx in range(max_items + 1):
                for key in all_keys:
                    expected.append(f"{json_col}_{idx}_{key}")
    
    return expected


def flatten_dict_iterative(d: Dict[str, Any], prefix: str = "", max_depth: int = 5) -> Dict[str, Any]:
    """
    Iteratively flatten a nested dictionary (avoids recursion limits).
    Converts lists to JSON strings to avoid schema issues.
    """
    if max_depth <= 0:
        return {}
    
    result = {}
    stack = [(d, prefix, max_depth)]
    
    while stack:
        current_dict, current_prefix, depth = stack.pop()
        
        if depth <= 0:
            continue
        
        for key, value in current_dict.items():
            new_key = f"{current_prefix}_{key}" if current_prefix else key
            
            if isinstance(value, dict):
                stack.append((value, new_key, depth - 1))
            elif isinstance(value, list):
                # Convert lists to JSON string to avoid schema issues
                try:
                    result[new_key] = JSON_DUMP(value) if hasattr(JSON_LOAD, '__module__') and 'orjson' in str(JSON_LOAD.__module__) else json.dumps(value)
                except:
                    result[new_key] = str(value)
            else:
                # Convert to string for consistency
                if value is None:
                    result[new_key] = None
                else:
                    result[new_key] = str(value)
    
    return result


def flatten_json_list(json_str: str, field_name: str, max_items: int = MAX_ITEMS_PER_LIST) -> Dict[str, Any]:
    """
    Flatten a JSON list field (e.g., topics, concepts) into columns.
    Returns a dictionary with flattened columns like: {field_name}_0_id, field_name}_0_display_name, etc.
    """
    result = {}
    
    if not json_str:
        return result
    
    try:
        if isinstance(json_str, bytes):
            json_str = json_str.decode('utf-8')
        
        parsed = JSON_LOAD(json_str)
        
        # Handle both list and dict cases
        if isinstance(parsed, list):
            items = parsed
        elif isinstance(parsed, dict):
            items = [parsed]  # Treat single dict as list with one item
        else:
            return result
        
        # Flatten each item in the list
        for idx, item in enumerate(items[:max_items]):
            if isinstance(item, dict):
                flattened = flatten_dict_iterative(item, prefix="", max_depth=5)
                for key, value in flattened.items():
                    result[f"{field_name}_{idx}_{key}"] = value
        
        # Add count column
        result[f"{field_name}_count"] = len(items)
    
    except Exception as e:
        # Silently handle errors - return empty dict
        pass
    
    return result


def process_single_file(file_path: Path, schema_info: Optional[Dict[str, Any]] = None) -> Tuple[bool, str, int]:
    """
    Process a single parquet file to flatten embedded JSON fields.
    Returns: (success, message, num_rows)
    """
    output_path = file_path.parent / f"{file_path.stem}_flatten.parquet"
    
    try:
        # Read the parquet file
        df = pl.read_parquet(file_path)
        num_rows = len(df)
        
        if num_rows == 0:
            return True, f"Empty file, skipping", 0
        
        # Convert to dicts for processing
        rows = df.to_dicts()
        original_columns = df.columns
        
        # Build predefined schema and expected columns
        predefined_schema = build_predefined_schema(original_columns, schema_info)
        expected_columns = get_all_expected_columns(original_columns, schema_info)
        
        # Process each row
        processed_rows = []
        for row in rows:
            new_row = {}
            
            # Copy original columns (except JSON columns)
            for col in original_columns:
                if col not in JSON_COLUMNS:
                    # Convert to string for consistency
                    val = row.get(col)
                    new_row[col] = str(val) if val is not None else None
            
            # Flatten each JSON column
            for json_col in JSON_COLUMNS:
                if json_col not in row:
                    continue
                
                json_value = row.get(json_col)
                
                # Flatten the JSON
                flattened = flatten_json_list(json_value, json_col, max_items=MAX_ITEMS_PER_LIST)
                new_row.update(flattened)
            
            # Standardize: ensure all expected columns exist (fill missing with None)
            standardized_row = {col: new_row.get(col, None) for col in expected_columns}
            processed_rows.append(standardized_row)
        
        # Create new DataFrame with predefined schema
        df_result = pl.DataFrame(processed_rows, schema=predefined_schema)
        
        # Write output
        df_result.write_parquet(output_path, compression='snappy')
        
        num_new_columns = len(df_result.columns)
        num_original_columns = len(original_columns)
        
        return True, f"Processed {num_rows} rows, {num_original_columns} -> {num_new_columns} columns", num_rows
    
    except Exception as e:
        error_msg = f"Error processing {file_path.name}: {type(e).__name__}: {str(e)[:200]}"
        logger.error(error_msg)
        import traceback
        logger.error(traceback.format_exc())
        return False, error_msg, 0


def load_progress() -> set:
    """Load completed files from progress file."""
    if not PROGRESS_FILE.exists():
        return set()
    
    try:
        with open(PROGRESS_FILE, 'r') as f:
            progress = json.load(f)
        return set(progress.get("completed_files", []))
    except Exception as e:
        logger.warning(f"Error loading progress: {e}")
        return set()


def save_progress(completed_files: set):
    """Save progress to file."""
    try:
        progress = {
            "completed_files": sorted(list(completed_files)),
            "last_updated": datetime.now().isoformat(),
            "total_completed": len(completed_files)
        }
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving progress: {e}")


def main():
    """Main function to process all parquet files."""
    logger.info("=" * 70)
    logger.info("FLATTEN EMBEDDED JSON FIELDS")
    logger.info("=" * 70)
    logger.info(f"Input directory: {INPUT_DIR}")
    logger.info(f"Number of workers: {NUM_WORKERS}")
    logger.info(f"JSON parser: {JSON_AVAILABLE}")
    logger.info(f"Max items per list: {MAX_ITEMS_PER_LIST}")
    
    # Load schema
    schema_info = load_schema()
    
    # Get all parquet files (excluding already processed ones)
    all_files = sorted(INPUT_DIR.glob("batch_*.parquet"))
    
    if not all_files:
        logger.error(f"No parquet files found in {INPUT_DIR}")
        return
    
    # Filter out files that already have _flatten versions
    files_to_process = []
    for f in all_files:
        if f.name.endswith("_flatten.parquet"):
            continue
        flatten_path = f.parent / f"{f.stem}_flatten.parquet"
        if not flatten_path.exists():
            files_to_process.append(f)
    
    logger.info(f"Found {len(files_to_process)} files to process (out of {len(all_files)} total)")
    
    if not files_to_process:
        logger.info("All files already processed!")
        return
    
    # Load progress
    completed_files = load_progress()
    files_to_process = [f for f in files_to_process if f.name not in completed_files]
    
    if not files_to_process:
        logger.info("All files already processed (according to progress file)!")
        return
    
    logger.info(f"Processing {len(files_to_process)} files...")
    
    # Process files in parallel
    total_rows = 0
    successful = 0
    failed = 0
    
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(process_single_file, file_path, schema_info): file_path
                  for file_path in files_to_process}
        
        for future in as_completed(futures):
            file_path = futures[future]
            try:
                success, message, num_rows = future.result()
                
                if success:
                    completed_files.add(file_path.name)
                    successful += 1
                    total_rows += num_rows
                    logger.info(f"✓ {file_path.name}: {message}")
                else:
                    failed += 1
                    logger.error(f"✗ {file_path.name}: {message}")
            
            except Exception as e:
                failed += 1
                logger.error(f"✗ {file_path.name}: Exception: {type(e).__name__}: {str(e)[:200]}")
            
            # Save progress periodically
            if (successful + failed) % 10 == 0:
                save_progress(completed_files)
                logger.info(f"Progress: {successful} successful, {failed} failed, {total_rows:,} total rows")
    
    # Final progress save
    save_progress(completed_files)
    
    logger.info("=" * 70)
    logger.info("PROCESSING COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Total rows processed: {total_rows:,}")
    logger.info(f"Progress saved to: {PROGRESS_FILE}")


if __name__ == "__main__":
    main()
