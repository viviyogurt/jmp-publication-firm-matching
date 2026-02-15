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
        
