#!/usr/bin/env python3
"""
Schema Discovery Script - Discover all JSON column names from a sample

This script samples rows from the input file and discovers all possible
column names that can be extracted from the JSON field.

Usage:
    python discover_schema.py [sample_size]
    
Example:
    python discover_schema.py 1000
"""

# Set thread environment variables BEFORE importing any numerical libraries
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
from typing import Dict, Set, Tuple

# Try to use orjson for faster JSON parsing
try:
    import orjson
    JSON_LOAD = orjson.loads
    JSON_DUMP = lambda x: orjson.dumps(x).decode('utf-8')
    JSON_AVAILABLE = "orjson"
except ImportError:
    import json
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
SCHEMA_FILE = OUTPUT_DIR / "_schema.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# JSON Flattening Functions
# ============================================================================

def flatten_json_iterative(work: Dict[str, any]) -> Dict[str, any]:
    """
    Optimized iterative flattening - faster than recursive for deep structures.
    """
    flattened = {}
    stack = [(work, "")]
    
    while stack:
        obj, prefix = stack.pop()
        for key, value in obj.items():
            new_key = f"{prefix}.{key}" if prefix else key
            
            if value is None:
                flattened[new_key] = None
            elif isinstance(value, dict):
                stack.append((value, new_key))
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


# ============================================================================
# Schema Discovery
# ============================================================================

def discover_schema_from_sample(sample_size: int = 1000) -> Tuple[Set[str], Dict[str, int]]:
    """
    Discover all possible JSON column names from a sample of rows.
    
    Args:
        sample_size: Number of rows to sample for discovery
        
    Returns:
        Tuple of (set of all column names, dict of column name -> occurrence count)
    """
    logger.info("=" * 80)
    logger.info("SCHEMA DISCOVERY")
    logger.info("=" * 80)
    logger.info(f"Input file: {INPUT_FILE}")
    logger.info(f"Sample size: {sample_size:,} rows")
    logger.info(f"JSON parser: {JSON_AVAILABLE}")
    logger.info("")
    
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        sys.exit(1)
    
    file_size_gb = INPUT_FILE.stat().st_size / (1024 ** 3)
    logger.info(f"Input file size: {file_size_gb:.2f} GB")
    logger.info("")
    
    all_keys = set()
    key_counts = {}
    rows_processed = 0
    rows_with_json = 0
    errors = 0
    
    start_time = datetime.now()
    
    try:
        # Read sample rows
        logger.info(f"Reading {sample_size:,} rows...")
        df_sample = (
            pl.scan_parquet(INPUT_FILE)
            .head(sample_size)
            .collect()
        )
        
        total_rows = len(df_sample)
        logger.info(f"  Read {total_rows:,} rows")
        logger.info("")
        
        # Process each row to discover all keys
        logger.info("Processing rows to discover schema...")
        for idx, row in enumerate(df_sample.iter_rows(named=True)):
            rows_processed += 1
            json_str = row.get('json')
            
            if json_str:
                rows_with_json += 1
                try:
                    # Handle bytes if needed
                    if isinstance(json_str, bytes):
                        json_str = json_str.decode('utf-8')
                    
                    # Parse JSON
                    work = JSON_LOAD(json_str)
                    
                    # Flatten to get all keys
                    flattened = flatten_json_iterative(work)
                    
                    # Track all keys and their occurrences
                    for key in flattened.keys():
                        all_keys.add(key)
                        key_counts[key] = key_counts.get(key, 0) + 1
                    
                    # Progress update every 100 rows
                    if (idx + 1) % 100 == 0:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        rate = (idx + 1) / elapsed if elapsed > 0 else 0
                        logger.info(f"  Processed {idx + 1:,}/{total_rows:,} rows "
                                  f"({rate:.0f} rows/sec) - "
                                  f"Found {len(all_keys):,} unique columns so far...")
                        
                except Exception as e:
                    errors += 1
                    if errors <= 5:  # Log first few errors
                        logger.warning(f"    Row {idx} JSON parsing failed: {type(e).__name__}: {str(e)[:100]}")
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info("")
        logger.info("=" * 80)
        logger.info("SCHEMA DISCOVERY COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Rows processed: {rows_processed:,}")
        logger.info(f"Rows with JSON: {rows_with_json:,}")
        logger.info(f"Parsing errors: {errors:,}")
        logger.info(f"Unique columns found: {len(all_keys):,}")
        logger.info(f"Time taken: {elapsed:.1f} seconds")
        logger.info("")
        
        # Log statistics
        if key_counts:
            sorted_keys = sorted(key_counts.items(), key=lambda x: x[1], reverse=True)
            logger.info("Top 20 most common columns:")
            for i, (key, count) in enumerate(sorted_keys[:20], 1):
                pct = (count / rows_with_json) * 100 if rows_with_json > 0 else 0
                logger.info(f"  {i:2d}. {key:50s} : {count:6,} rows ({pct:5.1f}%)")
        
        logger.info("")
        logger.info(f"Total unique columns: {len(all_keys):,}")
        
        return all_keys, key_counts
        
    except Exception as e:
        logger.error(f"Error during schema discovery: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return set(), {}


def save_schema_info(all_keys: Set[str], key_counts: Dict[str, int], schema_file: Path):
    """Save discovered schema information to JSON file."""
    schema_info = {
        "total_columns": len(all_keys),
        "columns": sorted(list(all_keys)),
        "column_counts": {k: v for k, v in sorted(key_counts.items(), key=lambda x: x[1], reverse=True)},
        "discovery_date": datetime.now().isoformat(),
    }
    
    with open(schema_file, 'w') as f:
        json.dump(schema_info, f, indent=2)
    
    logger.info(f"Schema information saved to {schema_file}")
    logger.info(f"  Total columns: {len(all_keys):,}")
    logger.info(f"  File size: {schema_file.stat().st_size / 1024:.1f} KB")


def main():
    """Main function for schema discovery."""
    sample_size = 1000
    if len(sys.argv) > 1:
        try:
            sample_size = int(sys.argv[1])
        except ValueError:
            logger.error(f"Invalid sample size: {sys.argv[1]}")
            sys.exit(1)
    
    # Discover schema
    all_keys, key_counts = discover_schema_from_sample(sample_size)
    
    if all_keys:
        # Save schema information
        save_schema_info(all_keys, key_counts, SCHEMA_FILE)
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("NEXT STEPS")
        logger.info("=" * 80)
        logger.info("1. Review the discovered schema in:")
        logger.info(f"   {SCHEMA_FILE}")
        logger.info("")
        logger.info("2. Use this schema in the main extraction script to speed up processing")
        logger.info("   (10-50x faster than dynamic schema inference)")
        logger.info("")
    else:
        logger.error("No columns discovered! Check input file and JSON format.")
        sys.exit(1)


if __name__ == "__main__":
    main()
