"""
Download All Institutions from OpenAlex API
============================================

Downloads the complete OpenAlex institutions database using multiple sort orders
to work around the 50-page limit per query. Uses page-based pagination with
different sort fields to ensure comprehensive coverage.

Based on OpenAlex API documentation: https://api.openalex.org/institutions

Author: JMP Research Team
Date: 2025-02-08
"""

import json
import gzip
import requests
import time
import logging
from pathlib import Path
from typing import Optional, Set

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
LOGS_DIR = PROJECT_ROOT / "logs"

OUTPUT_FILE = DATA_RAW / "institutions_all.jsonl.gz"
PROGRESS_LOG = LOGS_DIR / "institutions_download.log"

API_BASE = "https://api.openalex.org"
PER_PAGE = 200  # Maximum per page (OpenAlex limit)
MAX_PAGES_PER_SORT = 50  # OpenAlex limit per query
EMAIL = "sunyanna@hku.hk"  # For polite pool access
SLEEP_BETWEEN_PAGES = 0.5  # Seconds to sleep between API calls
PROGRESS_REPORT_INTERVAL = 10  # Report progress every N pages

# Multiple sort orders to work around 50-page limit
# Each sort order can get up to 50 pages (10,000 results)
SORT_ORDERS = [
    {'sort': 'id', 'order': 'asc'},      # Sort by ID ascending
    {'sort': 'id', 'order': 'desc'},     # Sort by ID descending  
    {'sort': 'display_name', 'order': 'asc'},  # Sort by name ascending
    {'sort': 'display_name', 'order': 'desc'}, # Sort by name descending
    {'sort': 'works_count', 'order': 'desc'},  # Sort by works count (most active)
    {'sort': 'cited_by_count', 'order': 'desc'}, # Sort by citations
]

# Type-based filters to get remaining institutions
# Each type filter can get up to 50 pages (10,000 results)
# Using multiple sort orders per type to maximize coverage
TYPE_FILTERS = [
    # Company type (31,366 total) - need multiple sorts
    {'filter': 'type:company', 'sort': 'id', 'order': 'asc'},
    {'filter': 'type:company', 'sort': 'id', 'order': 'desc'},
    {'filter': 'type:company', 'sort': 'display_name', 'order': 'asc'},
    {'filter': 'type:company', 'sort': 'works_count', 'order': 'desc'},
    # Education type (23,885 total)
    {'filter': 'type:education', 'sort': 'id', 'order': 'asc'},
    {'filter': 'type:education', 'sort': 'id', 'order': 'desc'},
    {'filter': 'type:education', 'sort': 'display_name', 'order': 'asc'},
    # Nonprofit type (16,571 total)
    {'filter': 'type:nonprofit', 'sort': 'id', 'order': 'asc'},
    {'filter': 'type:nonprofit', 'sort': 'id', 'order': 'desc'},
    # Facility type (13,617 total)
    {'filter': 'type:facility', 'sort': 'id', 'order': 'asc'},
    {'filter': 'type:facility', 'sort': 'id', 'order': 'desc'},
    # Healthcare type (14,351 total)
    {'filter': 'type:healthcare', 'sort': 'id', 'order': 'asc'},
    {'filter': 'type:healthcare', 'sort': 'id', 'order': 'desc'},
    # Government type (7,722 total)
    {'filter': 'type:government', 'sort': 'id', 'order': 'asc'},
    {'filter': 'type:government', 'sort': 'id', 'order': 'desc'},
    # Other types
    {'filter': 'type:archive', 'sort': 'id', 'order': 'asc'},
    {'filter': 'type:funder', 'sort': 'id', 'order': 'asc'},
    {'filter': 'type:other', 'sort': 'id', 'order': 'asc'},
    {'filter': 'type:other', 'sort': 'id', 'order': 'desc'},
]

# Works count range filters to capture institutions by activity level
# These help get institutions that might be missed by type filters
WORKS_COUNT_FILTERS = [
    {'filter': 'works_count:>1000', 'sort': 'id', 'order': 'asc'},      # High activity (23,862)
    {'filter': 'works_count:>1000', 'sort': 'id', 'order': 'desc'},
    {'filter': 'works_count:100-1000', 'sort': 'id', 'order': 'asc'},  # Medium activity (31,342)
    {'filter': 'works_count:100-1000', 'sort': 'id', 'order': 'desc'},
    {'filter': 'works_count:1-100', 'sort': 'id', 'order': 'asc'},     # Low activity (47,887)
    {'filter': 'works_count:1-100', 'sort': 'id', 'order': 'desc'},
    {'filter': 'works_count:0', 'sort': 'id', 'order': 'asc'},          # No works (17,683)
    {'filter': 'works_count:0', 'sort': 'id', 'order': 'desc'},
]

# Ensure directories exist
DATA_RAW.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROGRESS_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_total_count() -> int:
    """Get total number of institutions from OpenAlex API."""
    url = f"{API_BASE}/institutions?per_page=1&mailto={EMAIL}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        meta = response.json().get('meta', {})
        total_count = meta.get('count', 0)
        logger.info(f"Total institutions in OpenAlex: {total_count:,}")
        return total_count
    except Exception as e:
        logger.error(f"Failed to get total count: {e}")
        raise


def count_existing_institutions(file_path: Path) -> int:
    """Count institutions in existing file."""
    if not file_path.exists():
        return 0
    
    try:
        with gzip.open(file_path, 'rt') as f:
            count = sum(1 for _ in f)
        return count
    except Exception as e:
        logger.warning(f"Could not read existing file: {e}")
        return 0


def load_existing_ids(file_path: Path) -> Set[str]:
    """Load existing institution IDs from file to avoid duplicates."""
    downloaded_ids = set()
    if not file_path.exists():
        return downloaded_ids
    
    logger.info("Loading existing institution IDs to avoid duplicates...")
    try:
        with gzip.open(file_path, 'rt') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    inst = json.loads(line)
                    inst_id = inst.get('id')
                    if inst_id:
                        downloaded_ids.add(inst_id)
                except json.JSONDecodeError:
                    continue
                except Exception:
                    continue
                
                if line_num % 10000 == 0:
                    logger.info(f"  Loaded {line_num:,} lines, {len(downloaded_ids):,} unique IDs...")
        
        logger.info(f"Loaded {len(downloaded_ids):,} existing institution IDs")
    except Exception as e:
        logger.warning(f"Could not load existing IDs: {e}")
        downloaded_ids = set()
    
    return downloaded_ids


def download_with_sort_order(sort_config: dict, downloaded_ids: Set[str], 
                             f, start_time: float, total_count: int, 
                             filter_str: Optional[str] = None) -> tuple:
    """
    Download institutions using a specific sort order and optional filter.
    
    Parameters:
    -----------
    sort_config : dict
        Sort configuration with 'sort' and 'order' keys
    downloaded_ids : Set[str]
        Set of already downloaded institution IDs
    f : file handle
        Output file handle
    start_time : float
        Start time for progress calculation
    total_count : int
        Total expected count
    filter_str : str, optional
        Filter string (e.g., 'type:company')
    
    Returns:
    --------
    tuple: (new_count, skipped_count, institutions_count)
    """
    sort_field = sort_config['sort']
    sort_order = sort_config['order']
    sort_str = f"{sort_field}:{sort_order}"
    
    if filter_str:
        logger.info(f"  Downloading with filter: {filter_str}, sort: {sort_str}")
    else:
        logger.info(f"  Downloading with sort: {sort_str}")
    
    new_count_total = 0
    skipped_count_total = 0
    institutions_count = 0
    page_num = 0
    last_report_time = start_time
    
    for page in range(1, MAX_PAGES_PER_SORT + 1):
        retries = 0
        max_retries = 5
        success = False
        
        # Build URL with sort and page, and optional filter
        if filter_str:
            url = f"{API_BASE}/institutions?per_page={PER_PAGE}&page={page}&filter={filter_str}&sort={sort_str}&mailto={EMAIL}"
        else:
            url = f"{API_BASE}/institutions?per_page={PER_PAGE}&page={page}&sort={sort_str}&mailto={EMAIL}"
        
        while retries < max_retries:
            try:
                response = requests.get(url, timeout=30)
                
                if response.status_code != 200:
                    if response.status_code == 400 and page > 1:
                        # Likely hit the limit or end of results
                        logger.info(f"    Page {page}: HTTP {response.status_code} - likely end of results for this sort")
                        return new_count_total, skipped_count_total, institutions_count
                    
                    logger.warning(
                        f"    Page {page}: HTTP {response.status_code} (retry {retries+1}/{max_retries})"
                    )
                    retries += 1
                    time.sleep(2 ** retries)
                    continue
                
                data = response.json()
                institutions = data.get('results', [])
                
                if not institutions:
                    logger.info(f"    No more institutions for sort {sort_str}")
                    return new_count_total, skipped_count_total, institutions_count
                
                # Write each institution as a line (JSONL format)
                new_count = 0
                skipped_count = 0
                for inst in institutions:
                    inst_id = inst.get('id')
                    
                    # Skip if already downloaded
                    if inst_id in downloaded_ids:
                        skipped_count += 1
                        continue
                    
                    f.write(json.dumps(inst) + '\n')
                    downloaded_ids.add(inst_id)
                    new_count += 1
                    institutions_count += 1
                
                f.flush()  # Ensure data is written immediately
                
                new_count_total += new_count
                skipped_count_total += skipped_count
                page_num += 1
                success = True
                
                # Progress update
                elapsed = time.time() - start_time
                rate = institutions_count / elapsed if elapsed > 0 else 0
                pct_complete = (institutions_count / total_count) * 100 if total_count > 0 else 0
                
                # Report progress periodically
                if (page_num % PROGRESS_REPORT_INTERVAL == 0 or 
                    new_count < PER_PAGE or 
                    time.time() - last_report_time > 300):  # Every 5 minutes
                    logger.info(
                        f"    Page {page} ({pct_complete:.1f}%) - "
                        f"{institutions_count:,} total - "
                        f"New: {new_count}, Skipped: {skipped_count} - "
                        f"Rate: {rate:.1f}/sec"
                    )
                    last_report_time = time.time()
                
                break  # Success, move to next page
            
            except requests.exceptions.Timeout:
                logger.warning(f"    Page {page}: Timeout (retry {retries+1}/{max_retries})")
                retries += 1
                time.sleep(2 ** retries)
            except requests.exceptions.ConnectionError:
                logger.warning(f"    Page {page}: Connection error (retry {retries+1}/{max_retries})")
                retries += 1
                time.sleep(2 ** retries)
            except Exception as e:
                logger.warning(f"    Page {page}: {type(e).__name__}: {e} (retry {retries+1}/{max_retries})")
                retries += 1
                time.sleep(2 ** retries)
        
        if not success and retries >= max_retries:
            logger.warning(f"    Page {page}: Failed after {max_retries} retries, moving to next sort order")
            break
        
        # Rate limiting
        time.sleep(SLEEP_BETWEEN_PAGES)
    
    return new_count_total, skipped_count_total, institutions_count


def download_all_institutions(resume: bool = True) -> int:
    """
    Download all institutions from OpenAlex API using multiple sort orders.
    
    Parameters:
    -----------
    resume : bool
        If True, resume from existing file. If False, start fresh.
    
    Returns:
    --------
    int
        Total number of institutions downloaded
    """
    logger.info("=" * 80)
    logger.info("DOWNLOADING ALL OPENALEX INSTITUTIONS")
    logger.info("=" * 80)
    logger.info(f"API: {API_BASE}/institutions")
    logger.info(f"Output: {OUTPUT_FILE}")
    logger.info(f"Sleep between pages: {SLEEP_BETWEEN_PAGES}s")
    logger.info(f"Progress report every {PROGRESS_REPORT_INTERVAL} pages")
    logger.info(f"Using {len(SORT_ORDERS)} sort orders + {len(TYPE_FILTERS)} type filters + {len(WORKS_COUNT_FILTERS)} works_count filters")
    
    # Get total count
    total_count = get_total_count()
    logger.info(f"Records per page: {PER_PAGE:,}")
    logger.info(f"Max pages per sort: {MAX_PAGES_PER_SORT} (={MAX_PAGES_PER_SORT * PER_PAGE:,} results)")
    
    # Check for existing file
    institutions_count = 0
    downloaded_ids = set()
    
    if resume and OUTPUT_FILE.exists():
        institutions_count = count_existing_institutions(OUTPUT_FILE)
        if institutions_count > 0:
            logger.info(f"Found existing file with {institutions_count:,} institutions")
            downloaded_ids = load_existing_ids(OUTPUT_FILE)
            mode = 'at'
        else:
            logger.info("Existing file is empty or corrupted, starting fresh")
            OUTPUT_FILE.unlink()
            mode = 'wt'
    else:
        if OUTPUT_FILE.exists():
            logger.info("Resume disabled, removing existing file")
            OUTPUT_FILE.unlink()
        logger.info("Starting fresh download")
        mode = 'wt'
    
    start_time = time.time()
    total_new = 0
    total_skipped = 0
    
    with gzip.open(OUTPUT_FILE, mode) as f:
        # Phase 1: Download with general sort orders (no filters)
        logger.info("")
        logger.info("=" * 80)
        logger.info("PHASE 1: General Sort Orders (No Filters)")
        logger.info("=" * 80)
        
        for sort_idx, sort_config in enumerate(SORT_ORDERS, 1):
            logger.info("")
            logger.info(f"[{sort_idx}/{len(SORT_ORDERS)}] Sort order: {sort_config['sort']} ({sort_config['order']})")
            
            new_count, skipped_count, count_from_sort = download_with_sort_order(
                sort_config, downloaded_ids, f, start_time, total_count, filter_str=None
            )
            
            total_new += new_count
            total_skipped += skipped_count
            institutions_count = len(downloaded_ids)
            
            logger.info(
                f"  Completed: {new_count:,} new, {skipped_count:,} skipped, "
                f"{count_from_sort:,} from this sort, {institutions_count:,} total unique"
            )
            
            # Sleep between sort orders
            time.sleep(1.0)
        
        # Phase 2: Download with type-based filters to get remaining institutions
        logger.info("")
        logger.info("=" * 80)
        logger.info("PHASE 2: Type-Based Filters (To Capture Remaining Institutions)")
        logger.info("=" * 80)
        
        for filter_idx, filter_config in enumerate(TYPE_FILTERS, 1):
            logger.info("")
            filter_str = filter_config['filter']
            sort_config = {'sort': filter_config['sort'], 'order': filter_config['order']}
            logger.info(f"[{filter_idx}/{len(TYPE_FILTERS)}] Filter: {filter_str}, Sort: {sort_config['sort']} ({sort_config['order']})")
            
            new_count, skipped_count, count_from_filter = download_with_sort_order(
                sort_config, downloaded_ids, f, start_time, total_count, filter_str=filter_str
            )
            
            total_new += new_count
            total_skipped += skipped_count
            institutions_count = len(downloaded_ids)
            
            logger.info(
                f"  Completed: {new_count:,} new, {skipped_count:,} skipped, "
                f"{count_from_filter:,} from this filter, {institutions_count:,} total unique"
            )
            
            # If we got very few new institutions, this filter may be exhausted
            if new_count < 50:
                logger.info(f"  Low new count ({new_count}), filter may be exhausted")
            
            # Sleep between filters
            time.sleep(1.0)
        
        # Phase 3: Download with works_count range filters
        logger.info("")
        logger.info("=" * 80)
        logger.info("PHASE 3: Works Count Range Filters (To Capture Remaining Institutions)")
        logger.info("=" * 80)
        
        for filter_idx, filter_config in enumerate(WORKS_COUNT_FILTERS, 1):
            logger.info("")
            filter_str = filter_config['filter']
            sort_config = {'sort': filter_config['sort'], 'order': filter_config['order']}
            logger.info(f"[{filter_idx}/{len(WORKS_COUNT_FILTERS)}] Filter: {filter_str}, Sort: {sort_config['sort']} ({sort_config['order']})")
            
            new_count, skipped_count, count_from_filter = download_with_sort_order(
                sort_config, downloaded_ids, f, start_time, total_count, filter_str=filter_str
            )
            
            total_new += new_count
            total_skipped += skipped_count
            institutions_count = len(downloaded_ids)
            
            logger.info(
                f"  Completed: {new_count:,} new, {skipped_count:,} skipped, "
                f"{count_from_filter:,} from this filter, {institutions_count:,} total unique"
            )
            
            # If we got very few new institutions, this filter may be exhausted
            if new_count < 50:
                logger.info(f"  Low new count ({new_count}), filter may be exhausted")
            
            # Sleep between filters
            time.sleep(1.0)
    
    elapsed = time.time() - start_time
    file_size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("DOWNLOAD COMPLETE!")
    logger.info("=" * 80)
    logger.info(f"Total unique institutions downloaded: {institutions_count:,}")
    logger.info(f"New institutions: {total_new:,}")
    logger.info(f"Skipped (duplicates): {total_skipped:,}")
    logger.info(f"Coverage: {(institutions_count / total_count * 100):.1f}% of {total_count:,} total")
    logger.info(f"Time elapsed: {elapsed/60:.1f} minutes ({elapsed:.1f} seconds)")
    logger.info(f"Average rate: {institutions_count/elapsed:.1f} institutions/second")
    logger.info(f"Output file: {OUTPUT_FILE}")
    logger.info(f"File size: {file_size_mb:.1f} MB")
    logger.info(f"Progress log: {PROGRESS_LOG}")
    logger.info("=" * 80)
    
    return institutions_count


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Download all institutions from OpenAlex API"
    )
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Start fresh download (ignore existing file)'
    )
    
    args = parser.parse_args()
    
    resume = not args.no_resume
    download_all_institutions(resume=resume)
