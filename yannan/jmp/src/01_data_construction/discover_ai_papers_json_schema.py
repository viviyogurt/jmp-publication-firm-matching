"""
Discover the full schema of full_json column in ai_papers batches

This script analyzes the JSON structure to understand all nested fields
and creates a comprehensive schema mapping for flattening.
"""

import polars as pl
import json
from pathlib import Path
from collections import defaultdict
import logging
from typing import Any, Dict, List, Set

# Configuration
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BATCH_DIR = PROJECT_ROOT / "data" / "raw" / "publication" / "ai_papers_batches"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "publication"
SCHEMA_FILE = OUTPUT_DIR / "ai_papers_json_schema.json"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "logs" / "discover_ai_papers_json_schema.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def flatten_dict(d: Dict, parent_key: str = '', sep: str = '_', max_depth: int = 10, current_depth: int = 0) -> Dict[str, Any]:
    """
    Recursively flatten a nested dictionary.
    
    Args:
        d: Dictionary to flatten
        parent_key: Parent key prefix
        sep: Separator for nested keys
        max_depth: Maximum recursion depth
        current_depth: Current recursion depth
    
    Returns:
        Flattened dictionary
    """
    if current_depth >= max_depth:
        return {parent_key: str(d) if parent_key else 'max_depth_reached'}
    
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
        if v is None:
            items.append((new_key, None))
        elif isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep, max_depth, current_depth + 1).items())
        elif isinstance(v, list):
            if len(v) == 0:
                items.append((new_key, []))
            elif isinstance(v[0], dict):
                # Handle list of dictionaries - create indexed keys
                for idx, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(flatten_dict(item, f"{new_key}_{idx}", sep, max_depth, current_depth + 1).items())
                    else:
                        items.append((f"{new_key}_{idx}", item))
            else:
                # List of primitives - keep as is or convert to string
                items.append((new_key, v))
        else:
            items.append((new_key, v))
    
    return dict(items)


def analyze_json_structure(json_str: str) -> Dict[str, Any]:
    """
    Analyze a JSON string and return its flattened structure.
    
    Args:
        json_str: JSON string to analyze
    
    Returns:
        Dictionary with flattened keys and their types
    """
    try:
        data = json.loads(json_str)
        flattened = flatten_dict(data, max_depth=20)
        
        # Analyze types
        structure = {}
        for key, value in flattened.items():
            if value is None:
                structure[key] = {'type': 'null', 'sample': None}
            elif isinstance(value, bool):
                structure[key] = {'type': 'bool', 'sample': value}
            elif isinstance(value, int):
                structure[key] = {'type': 'int', 'sample': value}
            elif isinstance(value, float):
                structure[key] = {'type': 'float', 'sample': value}
            elif isinstance(value, str):
                structure[key] = {'type': 'str', 'sample': value[:100] if len(value) > 100 else value}
            elif isinstance(value, list):
                if len(value) > 0:
                    structure[key] = {'type': 'list', 'item_type': type(value[0]).__name__, 'length': len(value), 'sample': value[:3]}
                else:
                    structure[key] = {'type': 'list', 'item_type': 'unknown', 'length': 0, 'sample': []}
            else:
                structure[key] = {'type': type(value).__name__, 'sample': str(value)[:100]}
        
        return structure
    except Exception as e:
        logger.warning(f"Error parsing JSON: {e}")
        return {}


def discover_schema_from_batches(batch_dir: Path, sample_size: int = 1000) -> Dict[str, Any]:
    """
    Discover schema by analyzing multiple JSON records from batch files.
    
    Args:
        batch_dir: Directory containing batch files
        sample_size: Number of records to sample
    
    Returns:
        Comprehensive schema dictionary
    """
    logger.info("=" * 80)
    logger.info("DISCOVERING AI PAPERS JSON SCHEMA")
    logger.info("=" * 80)
    logger.info(f"Batch directory: {batch_dir}")
    logger.info(f"Sample size: {sample_size:,} records")
    logger.info("")
    
    # Find batch files
    batch_files = sorted(batch_dir.glob("batch_*.parquet"))
    
    if not batch_files:
        logger.error(f"No batch files found in {batch_dir}")
        return {}
    
    logger.info(f"Found {len(batch_files)} batch files")
    logger.info("")
    
    # Collect all unique keys and their types
    all_keys = defaultdict(set)
    key_types = defaultdict(lambda: {'types': set(), 'samples': []})
    total_analyzed = 0
    
    logger.info("Step 1: Analyzing JSON structures...")
    
    for batch_file in batch_files[:5]:  # Analyze first 5 batches
        logger.info(f"  Processing {batch_file.name}...")
        
        try:
            df = pl.read_parquet(batch_file)
            
            # Sample records
            sample_df = df.head(min(sample_size // len(batch_files[:5]), len(df)))
            
            for idx, row in enumerate(sample_df.iter_rows(named=True)):
                json_str = row.get('full_json')
                if not json_str:
                    continue
                
                structure = analyze_json_structure(json_str)
                
                for key, info in structure.items():
                    all_keys[key].add(info['type'])
                    key_types[key]['types'].add(info['type'])
                    if len(key_types[key]['samples']) < 3:
                        key_types[key]['samples'].append(info.get('sample'))
                
                total_analyzed += 1
                
                if (idx + 1) % 100 == 0:
                    logger.info(f"    Analyzed {idx + 1:,} records...")
        
        except Exception as e:
            logger.error(f"Error processing {batch_file.name}: {e}")
            continue
    
    logger.info("")
    logger.info(f"Total records analyzed: {total_analyzed:,}")
    logger.info(f"Unique flattened keys found: {len(all_keys)}")
    logger.info("")
    
    # Build comprehensive schema
    logger.info("Step 2: Building schema dictionary...")
    
    schema = {
        'total_records_analyzed': total_analyzed,
        'total_unique_keys': len(all_keys),
        'keys': {}
    }
    
    for key in sorted(all_keys.keys()):
        types = list(key_types[key]['types'])
        schema['keys'][key] = {
            'types': types,
            'primary_type': types[0] if len(types) == 1 else 'mixed',
            'samples': key_types[key]['samples'][:3]
        }
    
    logger.info(f"  Schema built with {len(schema['keys'])} keys")
    logger.info("")
    
    # Show top-level structure
    logger.info("Step 3: Top-level structure summary...")
    top_level_keys = [k for k in schema['keys'].keys() if '_' not in k or k.split('_')[0] in ['id', 'doi', 'title', 'display', 'publication', 'language', 'type', 'open', 'is', 'url', 'pdf', 'venue', 'cited', 'keywords', 'topics', 'primary', 'authorships', 'concepts', 'abstract', 'locations', 'referenced', 'related', 'versions', 'datasets', 'grants', 'apc', 'mesh', 'sustainable', 'counts', 'updated', 'created', 'has']]
    
    logger.info(f"  Top-level keys: {len([k for k in schema['keys'].keys() if not '_' in k or k.count('_') == 1])}")
    logger.info("")
    
    # Show some examples
    logger.info("Sample flattened keys (first 30):")
    for i, key in enumerate(sorted(schema['keys'].keys())[:30]):
        info = schema['keys'][key]
        logger.info(f"  {key}: {info['primary_type']}")
    logger.info("")
    
    return schema


def save_schema(schema: Dict, output_file: Path):
    """Save schema to JSON file."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(schema, f, indent=2, default=str)
    
    logger.info(f"Schema saved to: {output_file}")
    logger.info("")


def main():
    """Main execution."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Discover schema
    schema = discover_schema_from_batches(BATCH_DIR, sample_size=1000)
    
    if schema:
        # Save schema
        save_schema(schema, SCHEMA_FILE)
        
        logger.info("=" * 80)
        logger.info("SCHEMA DISCOVERY COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Total keys discovered: {schema['total_unique_keys']}")
        logger.info(f"Schema file: {SCHEMA_FILE}")
        logger.info("")
    else:
        logger.error("Failed to discover schema")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
