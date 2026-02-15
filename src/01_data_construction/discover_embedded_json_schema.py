#!/usr/bin/env python3
"""
Discover schema for embedded JSON fields in flattened parquet files.

This script samples a few parquet files to understand the structure of
embedded JSON fields (topics, concepts, keywords, locations, sustainable_development_goals, authorships)
and creates a schema file for efficient processing.
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
from typing import Dict, Any, Set, List
from collections import defaultdict
import logging
from datetime import datetime

# Try to use orjson for faster JSON parsing
try:
    import orjson
    JSON_LOAD = orjson.loads
    JSON_AVAILABLE = "orjson"
except ImportError:
    JSON_LOAD = json.loads
    JSON_AVAILABLE = "json"

# Configuration
INPUT_DIR = Path("/home/kurtluo/yannan/jmp/data/processed/publication/arxiv_flattened_batches")
SCHEMA_FILE = INPUT_DIR / "_embedded_json_schema.json"
JSON_COLUMNS = ['topics', 'concepts', 'keywords', 'locations', 'sustainable_development_goals', 'authorships']
SAMPLE_FILES = 10  # Number of files to sample
MAX_ITEMS_PER_LIST = 50  # Maximum number of items to extract per list field

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def flatten_dict(d: Dict[str, Any], prefix: str = "", max_depth: int = 5, depth: int = 0) -> Dict[str, Any]:
    """
    Recursively flatten a nested dictionary.
    """
    if depth >= max_depth:
        return {}
    
    result = {}
    for key, value in d.items():
        new_key = f"{prefix}_{key}" if prefix else key
        
        if isinstance(value, dict):
            result.update(flatten_dict(value, new_key, max_depth, depth + 1))
        elif isinstance(value, list):
            # For lists, we'll handle them separately in the main function
            # Here we just record that it's a list
            result[new_key] = value
        else:
            result[new_key] = value
    
    return result


def discover_list_item_schema(items: List[Dict], field_name: str, max_items: int = MAX_ITEMS_PER_LIST) -> Dict[str, Set[str]]:
    """
    Discover all possible keys in list items for a given field.
    Returns a dict mapping item_index to set of flattened keys.
    """
    schema = defaultdict(set)
    
    for idx, item in enumerate(items[:max_items]):
        if isinstance(item, dict):
            flattened = flatten_dict(item, prefix="")
            schema[idx].update(flattened.keys())
    
    return dict(schema)


def discover_embedded_json_schema(sample_files: List[Path]) -> Dict[str, Any]:
    """
    Discover the schema of embedded JSON fields by sampling files.
    """
    logger.info(f"Discovering embedded JSON schema from {len(sample_files)} sample files...")
    
    # Track all discovered keys for each JSON column
    all_keys = {col: defaultdict(set) for col in JSON_COLUMNS}
    item_counts = {col: [] for col in JSON_COLUMNS}
    
    for file_idx, file_path in enumerate(sample_files):
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            continue
        
        logger.info(f"Processing file {file_idx + 1}/{len(sample_files)}: {file_path.name}")
        
        try:
            df = pl.read_parquet(file_path)
            
            for col in JSON_COLUMNS:
                if col not in df.columns:
                    continue
                
                # Get non-null values
                non_null = df.select(pl.col(col)).filter(pl.col(col).is_not_null())
                
                for row in non_null.iter_rows(named=True):
                    json_str = row[col]
                    if not json_str:
                        continue
                    
                    try:
                        if isinstance(json_str, bytes):
                            json_str = json_str.decode('utf-8')
                        
                        parsed = JSON_LOAD(json_str)
                        
                        if isinstance(parsed, list):
                            item_counts[col].append(len(parsed))
                            
                            # Discover schema for each item in the list
                            for idx, item in enumerate(parsed[:MAX_ITEMS_PER_LIST]):
                                if isinstance(item, dict):
                                    flattened = flatten_dict(item, prefix="")
                                    all_keys[col][idx].update(flattened.keys())
                        elif isinstance(parsed, dict):
                            # If it's a dict instead of list, treat as single item
                            flattened = flatten_dict(parsed, prefix="")
                            all_keys[col][0].update(flattened.keys())
                            item_counts[col].append(1)
                    
                    except Exception as e:
                        logger.warning(f"Error parsing {col} in {file_path.name}: {e}")
                        continue
        
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            continue
    
    # Build final schema
    schema_info = {
        "discovery_date": datetime.now().isoformat(),
        "sample_files": len(sample_files),
        "json_parser": JSON_AVAILABLE,
        "max_items_per_list": MAX_ITEMS_PER_LIST,
        "fields": {}
    }
    
    for col in JSON_COLUMNS:
        if not all_keys[col]:
            continue
        
        # Find the maximum number of items seen
        max_items = max(item_counts[col]) if item_counts[col] else 0
        max_items = min(max_items, MAX_ITEMS_PER_LIST)
        
        # Collect all unique keys across all item positions
        all_unique_keys = set()
        for idx in range(max_items + 1):
            all_unique_keys.update(all_keys[col][idx])
        
        # For each item position, determine which keys exist
        item_schemas = {}
        for idx in range(max_items + 1):
            if idx in all_keys[col] and all_keys[col][idx]:
                item_schemas[idx] = sorted(list(all_keys[col][idx]))
        
        schema_info["fields"][col] = {
            "max_items_observed": max_items,
            "avg_items": sum(item_counts[col]) / len(item_counts[col]) if item_counts[col] else 0,
            "total_samples": len(item_counts[col]),
            "all_unique_keys": sorted(list(all_unique_keys)),
            "item_schemas": {str(k): v for k, v in item_schemas.items()}
        }
    
    return schema_info


def main():
    """Main function to discover schema and save to file."""
    # Get sample files
    all_files = sorted(INPUT_DIR.glob("batch_*.parquet"))
    
    if not all_files:
        logger.error(f"No parquet files found in {INPUT_DIR}")
        return
    
    sample_files = all_files[:SAMPLE_FILES]
    logger.info(f"Sampling {len(sample_files)} files from {len(all_files)} total files")
    
    # Discover schema
    schema_info = discover_embedded_json_schema(sample_files)
    
    # Save schema
    import json as json_module
    with open(SCHEMA_FILE, 'w') as f:
        json_module.dump(schema_info, f, indent=2)
    
    logger.info(f"Schema saved to {SCHEMA_FILE}")
    
    # Print summary
    print("\n" + "=" * 70)
    print("EMBEDDED JSON SCHEMA DISCOVERY SUMMARY")
    print("=" * 70)
    for col, info in schema_info["fields"].items():
        print(f"\n{col}:")
        print(f"  Max items observed: {info['max_items_observed']}")
        print(f"  Average items: {info['avg_items']:.1f}")
        print(f"  Total unique keys: {len(info['all_unique_keys'])}")
        print(f"  Sample keys: {info['all_unique_keys'][:10]}")
        if len(info['all_unique_keys']) > 10:
            print(f"    ... and {len(info['all_unique_keys']) - 10} more")
    
    print(f"\n{'=' * 70}")
    print(f"Schema file: {SCHEMA_FILE}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
