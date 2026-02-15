"""
Fetch Missing ArXiv Papers (2021-04-01 to 2025)

This script fetches ArXiv papers from 2021-04-01 to 2025 to complement
the existing dataset. It uses ArXiv's API with date-based queries and
includes checkpoint/resume capability and incremental saving for safe data collection.

IMPROVEMENTS:
- Completeness tracking to avoid repeated scraping
- Adaptive chunking that splits when limits are exceeded
- Verification to ensure all papers are fetched
- Gap detection to identify and fill missing date ranges

Date: 2025
"""

import arxiv
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import time
import logging
import json
import sys
from typing import List, Dict, Tuple, Optional
import signal

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
CHECKPOINT_DIR = PROJECT_ROOT / "data" / "checkpoints" / "arxiv_scraping"
LOGS_DIR = PROJECT_ROOT / "logs"

# Ensure directories exist
DATA_RAW.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Output file for new data
OUTPUT_FILE = DATA_RAW / "arxiv_2021_2025.parquet"
CHECKPOINT_FILE = CHECKPOINT_DIR / "arxiv_scraping_checkpoint.json"
COMPLETENESS_TRACKER_FILE = CHECKPOINT_DIR / "completeness_tracker.json"
TEMP_OUTPUT = CHECKPOINT_DIR / "arxiv_temp.parquet"

# Date range to fetch
START_DATE = datetime(2021, 4, 1)
END_DATE = datetime(2025, 12, 31)

# ArXiv API settings
# REMOVED MAX_RESULTS_PER_QUERY limit to ensure completeness
# The arxiv library will handle pagination automatically
DELAY_BETWEEN_QUERIES = 3  # seconds (be respectful to API - ArXiv recommends 3s)
BATCH_SAVE_SIZE = 500  # Save every N papers (smaller batches for safety)
MAX_RETRIES = 5  # Max retries for failed requests

# Completeness and chunking settings
MAX_PAPERS_PER_CHUNK = 1500  # If a chunk exceeds this, split it
EXPECTED_PAPERS_PER_DAY = 600  # Conservative estimate for verification (ArXiv ~573/day)
MIN_CHUNK_DAYS = 1  # Minimum chunk size (1 day)
DEFAULT_CHUNK_DAYS = 7  # Default chunk size (1 week)
COMPLETENESS_THRESHOLD = 0.99  # If we get 85%+ of expected, consider complete

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "arxiv_scraping.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logger.warning("Shutdown signal received. Saving checkpoint and exiting...")
    shutdown_requested = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ============================================================================
# Helper Functions
# ============================================================================

def load_checkpoint() -> Dict:
    """Load checkpoint data if it exists"""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                checkpoint = json.load(f)
            logger.info(f"Loaded checkpoint: {checkpoint}")
            return checkpoint
        except Exception as e:
            logger.warning(f"Error loading checkpoint: {e}")
            return {}
    return {}


def save_checkpoint(checkpoint_data: Dict):
    """Save checkpoint data"""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint_data, f, indent=2, default=str)
        logger.debug(f"Checkpoint saved: {checkpoint_data}")
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")


def load_completeness_tracker() -> Dict:
    """Load tracker of which date ranges are complete"""
    if COMPLETENESS_TRACKER_FILE.exists():
        try:
            with open(COMPLETENESS_TRACKER_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Error loading completeness tracker: {e}")
            return {}
    return {}


def save_completeness_tracker(tracker: Dict):
    """Save completeness tracker"""
    try:
        with open(COMPLETENESS_TRACKER_FILE, 'w') as f:
            json.dump(tracker, f, indent=2, default=str)
    except Exception as e:
        logger.error(f"Error saving completeness tracker: {e}")


def mark_range_complete(start_date: str, end_date: str, paper_count: int, total_expected: int):
    """Mark a date range as complete"""
    tracker = load_completeness_tracker()
    key = f"{start_date}_{end_date}"
    tracker[key] = {
        "start_date": start_date,
        "end_date": end_date,
        "paper_count": paper_count,
        "total_expected": total_expected,
        "completeness_ratio": paper_count / total_expected if total_expected > 0 else 1.0,
        "complete": True,
        "timestamp": datetime.now().isoformat()
    }
    save_completeness_tracker(tracker)
    logger.info(f"  Marked range {start_date} to {end_date} as complete ({paper_count} papers)")


def is_range_complete(start_date: str, end_date: str) -> bool:
    """Check if a date range is marked as complete"""
    tracker = load_completeness_tracker()
    key = f"{start_date}_{end_date}"
    return tracker.get(key, {}).get("complete", False)


def get_existing_arxiv_ids() -> set:
    """Get set of already-fetched arxiv_ids to avoid duplicates"""
    existing_ids = set()
    
    # Check existing main file
    existing_file = DATA_RAW / "claude_arxiv.parquet"
    if existing_file.exists():
        try:
            df = pl.read_parquet(existing_file)
            existing_ids.update(df["arxiv_id"].unique().to_list())
            logger.info(f"Loaded {len(existing_ids):,} existing arxiv_ids from main file")
        except Exception as e:
            logger.warning(f"Error reading existing file: {e}")
    
    # Check new output file if it exists
    if OUTPUT_FILE.exists():
        try:
            df = pl.read_parquet(OUTPUT_FILE)
            existing_ids.update(df["arxiv_id"].unique().to_list())
            logger.info(f"Loaded {len(existing_ids):,} arxiv_ids from output file (total: {len(existing_ids):,})")
        except Exception as e:
            logger.warning(f"Error reading output file: {e}")
    
    return existing_ids


def parse_arxiv_id(arxiv_url: str) -> str:
    """Extract arxiv_id from ArXiv URL or ID"""
    # Handle different formats: "http://arxiv.org/abs/1234.5678v1" or "1234.5678v1"
    if "/" in arxiv_url:
        parts = arxiv_url.split("/")
        arxiv_id = parts[-1]
    else:
        arxiv_id = arxiv_url
    
    # Remove version suffix if present (e.g., "1234.5678v1" -> "1234.5678")
    if "v" in arxiv_id and arxiv_id[-1].isdigit():
        arxiv_id = arxiv_id.rsplit("v", 1)[0]
    
    return arxiv_id


def arxiv_result_to_dict(result: arxiv.Result) -> Dict:
    """Convert ArXiv API result to dictionary matching existing schema"""
    # Parse arxiv_id
    arxiv_id = parse_arxiv_id(result.entry_id)
    
    # Format authors as string (comma-separated)
    authors_str = ", ".join([author.name for author in result.authors])
    
    # Format categories
    categories_str = "; ".join(result.categories) if result.categories else ""
    primary_category = result.primary_category if hasattr(result, 'primary_category') else None
    
    # Handle dates
    published = result.published if result.published else None
    updated = result.updated if result.updated else None
    
    # Get submission date (may not be available, use published as fallback)
    submission_date = published  # ArXiv API doesn't provide separate submission date
    
    # Current harvest date
    harvest_date = datetime.now()
    
    return {
        "arxiv_id": arxiv_id,
        "title": result.title or "",
        "authors": authors_str,
        "categories": categories_str,
        "abstract": result.summary or "",
        "published": published,
        "updated": updated,
        "pdf_url": result.pdf_url or "",
        "doi": result.doi or None,
        "journal_ref": result.journal_ref if hasattr(result, 'journal_ref') else None,
        "comments": result.comment if hasattr(result, 'comment') else None,
        "primary_category": primary_category,
        "submission_date": submission_date,
        "harvest_date": harvest_date,
    }


def detect_gaps_in_data() -> List[Tuple[datetime, datetime]]:
    """
    Analyze existing data to find date ranges with missing papers.
    Returns list of (start_date, end_date) tuples that need re-scraping.
    """
    gaps = []
    
    if not OUTPUT_FILE.exists():
        logger.info("No existing data file, will fetch all ranges")
        return [(START_DATE, END_DATE)]
    
    try:
        df = pl.read_parquet(OUTPUT_FILE)
        
        # Get date range in data
        if len(df) == 0:
            return [(START_DATE, END_DATE)]
        
        min_date = df["published"].min()
        max_date = df["published"].max()
        
        # Check completeness tracker for incomplete ranges
        tracker = load_completeness_tracker()
        incomplete_ranges = []
        
        for key, info in tracker.items():
            if not info.get("complete", False):
                try:
                    start = datetime.fromisoformat(info["start_date"])
                    end = datetime.fromisoformat(info["end_date"])
                    incomplete_ranges.append((start, end))
                except Exception as e:
                    logger.warning(f"Error parsing range from tracker: {e}")
        
        if incomplete_ranges:
            logger.info(f"Found {len(incomplete_ranges)} incomplete ranges in tracker")
            return incomplete_ranges
        
        # Analyze by weekly chunks to find suspicious gaps
        logger.info("Analyzing data for gaps...")
        
        # Group by week and check counts
        df_with_week = df.with_columns([
            (pl.col("published").dt.ordinal_day() - pl.col("published").dt.ordinal_day().min()).alias("days_from_start")
        ])
        
        # Check for weeks with suspiciously low counts
        # Expected: ~4000 papers per week (573/day * 7)
        current = START_DATE
        while current <= END_DATE:
            week_end = min(current + timedelta(days=6), END_DATE)
            
            # Count papers in this week
            week_papers = df_with_week.filter(
                (pl.col("published") >= current) & (pl.col("published") <= week_end)
            )
            count = len(week_papers)
            
            # Expected papers for this week
            days = (week_end - current).days + 1
            expected = days * EXPECTED_PAPERS_PER_DAY
            
            # If significantly below expected, mark as gap
            if count < expected * COMPLETENESS_THRESHOLD:
                logger.warning(f"  Gap detected: {current.date()} to {week_end.date()}: {count} papers (expected ~{expected:.0f})")
                gaps.append((current, week_end))
            
            current = week_end + timedelta(days=1)
        
        if not gaps:
            logger.info("No gaps detected in existing data")
        else:
            logger.info(f"Detected {len(gaps)} date ranges with potential gaps")
        
    except Exception as e:
        logger.error(f"Error detecting gaps: {e}", exc_info=True)
        return [(START_DATE, END_DATE)]  # If error, re-fetch everything
    
    return gaps if gaps else []


def fetch_chunk_with_verification(
    start_date: datetime,
    end_date: datetime,
    existing_ids: set,
    adaptive_chunking: bool = True
) -> Tuple[List[Dict], bool, bool]:
    """
    Fetch papers for a date range with verification.
    
    Returns: (papers_list, is_complete, needs_splitting)
    - is_complete: True if we're confident we got all papers
    - needs_splitting: True if chunk was too large and should be split
    """
    papers = []
    start_str = start_date.strftime('%Y%m%d') + '000000'
    end_str = end_date.strftime('%Y%m%d') + '235959'
    query = f"submittedDate:[{start_str} TO {end_str}]"
    
    # Calculate expected papers (conservative estimate)
    days = (end_date - start_date).days + 1
    expected_papers = days * EXPECTED_PAPERS_PER_DAY
    
    # Check if this range is already complete
    range_key = f"{start_date.date()}_{end_date.date()}"
    if is_range_complete(str(start_date.date()), str(end_date.date())):
        logger.info(f"  Range {range_key} already marked complete, skipping")
        return [], True, False
    
    logger.debug(f"  Query: {query}")
    logger.debug(f"  Expected papers: ~{expected_papers:.0f} (based on {days} days)")
    
    try:
        # Create search WITHOUT max_results limit to get ALL papers
        # The arxiv library handles pagination automatically
        search = arxiv.Search(
            query=query,
            max_results=None,  # No limit - get all results
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Ascending
        )
        
        client = arxiv.Client(
            page_size=100,  # Smaller page size for stability
            delay_seconds=3.0,
            num_retries=3
        )
        
        papers_fetched = 0
        papers_new = 0
        needs_splitting = False
        
        try:
            for result in client.results(search):
                if shutdown_requested:
                    break
                
                paper_dict = arxiv_result_to_dict(result)
                arxiv_id = paper_dict["arxiv_id"]
                
                if arxiv_id in existing_ids:
                    continue
                
                papers.append(paper_dict)
                existing_ids.add(arxiv_id)
                papers_new += 1
                papers_fetched += 1
                
                # If we're getting too many papers, this chunk might be too large
                if adaptive_chunking and papers_fetched > MAX_PAPERS_PER_CHUNK:
                    logger.warning(f"  Chunk exceeded {MAX_PAPERS_PER_CHUNK} papers ({papers_fetched}), may need splitting")
                    needs_splitting = True
                    # Continue fetching to get accurate count, but mark for splitting
                
                # Log progress for large chunks
                if papers_fetched % 500 == 0:
                    logger.info(f"    Fetched {papers_fetched} papers so far...")
        
        except arxiv.UnexpectedEmptyPageError:
            # This can happen when we've reached the end of results
            logger.debug("Reached end of results for this chunk")
        
        # Verify completeness
        completeness_ratio = papers_fetched / expected_papers if expected_papers > 0 else 1.0
        
        logger.info(f"  Fetched {papers_fetched} papers (expected ~{expected_papers:.0f}, ratio: {completeness_ratio:.2%})")
        
        # Determine if complete
        is_complete = False
        if completeness_ratio >= COMPLETENESS_THRESHOLD:
            # Got at least threshold % of expected, mark as complete
            mark_range_complete(
                str(start_date.date()),
                str(end_date.date()),
                papers_fetched,
                expected_papers
            )
            is_complete = True
        elif papers_fetched == 0:
            # No papers found - might be a date range with no submissions
            # Mark as complete to avoid re-checking
            logger.info(f"  No papers found for this range, marking as complete")
            mark_range_complete(
                str(start_date.date()),
                str(end_date.date()),
                0,
                expected_papers
            )
            is_complete = True
        
        # If chunk was too large, needs splitting
        if needs_splitting:
            return papers, is_complete, True
        
        return papers, is_complete, False
        
    except Exception as e:
        logger.error(f"Error fetching chunk {range_key}: {e}", exc_info=True)
        return papers, False, False


def fetch_papers_by_date_range_improved(
    start_date: datetime,
    end_date: datetime,
    existing_ids: set,
    checkpoint: Dict
) -> List[Dict]:
    """
    Improved version that:
    1. Detects gaps and only fetches incomplete ranges
    2. Uses adaptive chunking (splits when too large)
    3. Verifies completeness
    4. Avoids re-scraping complete ranges
    """
    all_papers = []
    current_date = start_date
    
    # Resume from checkpoint
    if "last_processed_date" in checkpoint:
        try:
            checkpoint_date = datetime.fromisoformat(checkpoint["last_processed_date"])
            if checkpoint_date >= start_date:
                current_date = checkpoint_date + timedelta(days=1)
                logger.info(f"Resuming from checkpoint date: {current_date}")
        except Exception as e:
            logger.warning(f"Error parsing checkpoint date: {e}")
    
    # Detect gaps first
    logger.info("Detecting gaps in existing data...")
    gaps = detect_gaps_in_data()
    
    if gaps:
        logger.info(f"Found {len(gaps)} incomplete date ranges to process")
        # Process gaps instead of full range
        date_ranges_to_process = gaps
    else:
        # No gaps detected, process normally from checkpoint
        logger.info("No gaps detected, processing from checkpoint")
        date_ranges_to_process = [(current_date, end_date)]
    
    # Process each date range
    for range_start, range_end in date_ranges_to_process:
        if shutdown_requested:
            break
        
        # Use adaptive chunking: start with default, reduce if needed
        chunk_days = DEFAULT_CHUNK_DAYS
        range_current = range_start
        
        while range_current <= range_end and not shutdown_requested:
            chunk_end = min(range_current + timedelta(days=chunk_days - 1), range_end)
            
            logger.info(f"Fetching papers from {range_current.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')} (chunk: {chunk_days} days)")
            
            # Check if already complete
            if is_range_complete(str(range_current.date()), str(chunk_end.date())):
                logger.info(f"  Range already marked complete, skipping")
                range_current = chunk_end + timedelta(days=1)
                chunk_days = DEFAULT_CHUNK_DAYS  # Reset to default
                continue
            
            # Fetch with verification
            chunk_papers, is_complete, needs_splitting = fetch_chunk_with_verification(
                range_current,
                chunk_end,
                existing_ids,
                adaptive_chunking=True
            )
            
            if chunk_papers:
                all_papers.extend(chunk_papers)
                
                # Save batch periodically
                if len(all_papers) >= BATCH_SAVE_SIZE:
                    save_papers_batch(all_papers, append=True)
                    all_papers = []
            
            # If chunk was too large, split it
            if needs_splitting and chunk_days > MIN_CHUNK_DAYS:
                logger.info(f"  Chunk too large, reducing chunk size from {chunk_days} to {chunk_days // 2} days")
                chunk_days = max(MIN_CHUNK_DAYS, chunk_days // 2)  # Halve chunk size
                # Don't advance date, retry with smaller chunk
                continue
            else:
                # Chunk complete or already at minimum size, move to next
                range_current = chunk_end + timedelta(days=1)
                chunk_days = DEFAULT_CHUNK_DAYS  # Reset to default
            
            # Update checkpoint
            checkpoint["last_processed_date"] = chunk_end.isoformat()
            checkpoint["total_papers_fetched"] = checkpoint.get("total_papers_fetched", 0) + len(chunk_papers)
            checkpoint["last_checkpoint_time"] = datetime.now().isoformat()
            save_checkpoint(checkpoint)
            
            time.sleep(DELAY_BETWEEN_QUERIES)
    
    return all_papers


def save_papers_batch(papers: List[Dict], append: bool = False):
    """Save a batch of papers to parquet file"""
    if not papers:
        return
    
    try:
        if append and OUTPUT_FILE.exists():
            # Read existing file and get its schema first
            existing_df = pl.read_parquet(OUTPUT_FILE)
            existing_schema = existing_df.schema
            
            # Create new DataFrame and align it to existing schema
            # Use infer_schema_length=None to check all rows for proper type inference
            df_new = pl.DataFrame(papers, infer_schema_length=None)
            
            # Ensure datetime columns are properly typed first
            datetime_cols = ["published", "updated", "submission_date", "harvest_date"]
            for col in datetime_cols:
                if col in df_new.columns:
                    df_new = df_new.with_columns(
                        pl.col(col).cast(pl.Datetime(time_unit="ns"))
                    )
            
            # Align schema: cast each column in new DataFrame to match existing schema
            aligned_cols = []
            for col_name, col_type in existing_schema.items():
                if col_name in df_new.columns:
                    # Cast to match existing type
                    aligned_cols.append(pl.col(col_name).cast(col_type, strict=False))
                else:
                    # Column missing in new data, fill with null
                    aligned_cols.append(pl.lit(None).cast(col_type).alias(col_name))
            
            df_aligned = df_new.select(aligned_cols)
            
            # Ensure column order matches existing schema
            df_aligned = df_aligned.select(list(existing_schema.keys()))
            
            # Concatenate - use how="diagonal" to handle any remaining type mismatches gracefully
            try:
                combined_df = pl.concat([existing_df, df_aligned], how="diagonal")
            except Exception as concat_error:
                # Fallback: try with strict alignment
                logger.warning(f"Diagonal concat failed, trying strict: {concat_error}")
                combined_df = pl.concat([existing_df, df_aligned])
            
            # Remove any duplicates that might have been introduced
            combined_df = combined_df.unique(subset=["arxiv_id"], keep="first")
            
            combined_df.write_parquet(OUTPUT_FILE, compression="snappy")
            logger.info(f"  Appended {len(papers)} papers. Total in file: {len(combined_df):,}")
        else:
            # Create new file
            df = pl.DataFrame(papers, infer_schema_length=None)
            
            # Ensure datetime columns are properly typed
            datetime_cols = ["published", "updated", "submission_date", "harvest_date"]
            for col in datetime_cols:
                if col in df.columns:
                    df = df.with_columns(
                        pl.col(col).cast(pl.Datetime(time_unit="ns"))
                    )
            
            df.write_parquet(OUTPUT_FILE, compression="snappy")
            logger.info(f"  Saved {len(papers)} papers to {OUTPUT_FILE}")
        
    except Exception as e:
        logger.error(f"Error saving papers batch: {str(e)}")
        logger.error(f"  Error type: {type(e).__name__}", exc_info=True)
        raise


def merge_with_existing_data():
    """Merge new data with existing claude_arxiv.parquet"""
    existing_file = DATA_RAW / "claude_arxiv.parquet"
    new_file = OUTPUT_FILE
    
    if not new_file.exists():
        logger.warning("No new data to merge")
        return
    
    try:
        logger.info("Merging new data with existing dataset...")
        
        # Read both files
        existing_df = pl.read_parquet(existing_file)
        new_df = pl.read_parquet(new_file)
        
        # Combine
        combined_df = pl.concat([existing_df, new_df])
        
        # Remove duplicates (keep first occurrence)
        combined_df = combined_df.unique(subset=["arxiv_id"], keep="first")
        
        # Sort by published date
        combined_df = combined_df.sort("published")
        
        # Save merged file
        merged_file = DATA_RAW / "claude_arxiv_complete.parquet"
        combined_df.write_parquet(merged_file, compression="snappy")
        
        logger.info(f"Merged dataset: {len(combined_df):,} papers")
        logger.info(f"  Original: {len(existing_df):,} papers")
        logger.info(f"  New: {len(new_df):,} papers")
        logger.info(f"  Saved to: {merged_file}")
        
    except Exception as e:
        logger.error(f"Error merging data: {str(e)}")
        raise


def main():
    """Main scraping function"""
    logger.info("=" * 80)
    logger.info("ArXiv Data Scraping: 2021-04-01 to 2025 (IMPROVED VERSION)")
    logger.info("Features: Completeness tracking, Adaptive chunking, Gap detection")
    logger.info("=" * 80)
    logger.info(f"Output file: {OUTPUT_FILE}")
    logger.info(f"Checkpoint file: {CHECKPOINT_FILE}")
    logger.info(f"Completeness tracker: {COMPLETENESS_TRACKER_FILE}")
    logger.info("")
    
    try:
        # Load checkpoint
        checkpoint = load_checkpoint()
        
        # Get existing IDs to avoid duplicates
        logger.info("Loading existing arxiv_ids to avoid duplicates...")
        existing_ids = get_existing_arxiv_ids()
        logger.info(f"  Found {len(existing_ids):,} existing papers")
        
        # Fetch papers using improved method
        logger.info("Starting paper fetching with gap detection and adaptive chunking...")
        papers = fetch_papers_by_date_range_improved(
            START_DATE,
            END_DATE,
            existing_ids,
            checkpoint
        )
        
        # Save remaining papers
        if papers:
            logger.info(f"Saving final batch of {len(papers)} papers...")
            save_papers_batch(papers, append=True)
        
        # Final checkpoint
        checkpoint["status"] = "completed"
        checkpoint["completion_time"] = datetime.now().isoformat()
        save_checkpoint(checkpoint)
        
        logger.info("=" * 80)
        logger.info("Scraping completed successfully!")
        logger.info(f"Total papers fetched: {checkpoint.get('total_papers_fetched', 0):,}")
        logger.info("=" * 80)
        
        # Optionally merge with existing data
        if OUTPUT_FILE.exists():
            merge_with_existing_data()
        
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
        checkpoint["status"] = "interrupted"
        checkpoint["interrupt_time"] = datetime.now().isoformat()
        save_checkpoint(checkpoint)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        checkpoint["status"] = "error"
        checkpoint["error"] = str(e)
        checkpoint["error_time"] = datetime.now().isoformat()
        save_checkpoint(checkpoint)
        sys.exit(1)


if __name__ == "__main__":
    main()
