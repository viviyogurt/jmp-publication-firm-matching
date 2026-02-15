"""
Fetch ArXiv Papers from Kaggle Dataset

This script downloads the ArXiv dataset from Kaggle (Cornell University)
and processes it to match the existing schema format.

The Kaggle dataset is a comprehensive snapshot of ArXiv papers that may
be more complete than API-based scraping.

Date: 2025
"""

import kagglehub
import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime
from email.utils import parsedate_to_datetime
import logging
import json
from typing import Dict, List, Optional
import sys

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
LOGS_DIR = PROJECT_ROOT / "logs"

# Ensure directories exist
DATA_RAW.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Output file
OUTPUT_FILE = DATA_RAW / "arxiv_kaggle.parquet"
MERGED_OUTPUT = DATA_RAW / "arxiv_complete_kaggle.parquet"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "arxiv_kaggle_download.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def download_kaggle_dataset() -> Path:
    """
    Download ArXiv dataset from Kaggle.
    
    Returns:
        Path to the downloaded dataset directory
    """
    logger.info("=" * 80)
    logger.info("Downloading ArXiv dataset from Kaggle")
    logger.info("Dataset: Cornell-University/arxiv")
    logger.info("=" * 80)
    
    try:
        # Download latest version
        path = kagglehub.dataset_download("Cornell-University/arxiv")
        logger.info(f"Dataset downloaded to: {path}")
        return Path(path)
    except Exception as e:
        logger.error(f"Error downloading dataset: {e}")
        logger.error("Make sure you have:")
        logger.error("  1. Installed kagglehub: pip install kagglehub")
        logger.error("  2. Set up Kaggle API credentials (kaggle.json or environment variables)")
        raise


def explore_dataset_structure(dataset_path: Path):
    """Explore the structure of the downloaded dataset"""
    logger.info("Exploring dataset structure...")
    
    # List all files in the dataset
    files = list(dataset_path.glob("*"))
    logger.info(f"Found {len(files)} files/directories in dataset:")
    for f in files:
        size = f.stat().st_size / (1024 * 1024) if f.is_file() else 0
        logger.info(f"  {f.name}: {size:.2f} MB" if f.is_file() else f"  {f.name}/ (directory)")
    
    # Look for common file patterns
    json_files = list(dataset_path.glob("*.json"))
    csv_files = list(dataset_path.glob("*.csv"))
    parquet_files = list(dataset_path.glob("*.parquet"))
    
    if json_files:
        logger.info(f"Found {len(json_files)} JSON files")
        # Check if it's a JSONL (one JSON per line)
        sample_file = json_files[0]
        with open(sample_file, 'r') as f:
            first_line = f.readline()
            try:
                sample_data = json.loads(first_line)
                logger.info(f"Sample JSON structure (first record):")
                for key in list(sample_data.keys())[:10]:
                    logger.info(f"  {key}: {type(sample_data[key]).__name__}")
            except json.JSONDecodeError:
                logger.info("  Appears to be JSONL format (one JSON per line)")
    
    if csv_files:
        logger.info(f"Found {len(csv_files)} CSV files")
        # Read first few rows to understand structure
        sample_df = pd.read_csv(csv_files[0], nrows=5)
        logger.info(f"CSV columns: {list(sample_df.columns)}")
        logger.info(f"Sample row:\n{sample_df.head(1).to_string()}")
    
    if parquet_files:
        logger.info(f"Found {len(parquet_files)} Parquet files")
        sample_df = pl.read_parquet(parquet_files[0])
        logger.info(f"Parquet columns: {sample_df.columns}")
        logger.info(f"Sample row:\n{sample_df.head(1)}")
    
    return {
        "json_files": json_files,
        "csv_files": csv_files,
        "parquet_files": parquet_files
    }


def parse_arxiv_id(arxiv_id: str) -> str:
    """Extract clean arxiv_id from various formats"""
    if not arxiv_id:
        return ""
    
    # Handle different formats
    arxiv_id = str(arxiv_id).strip()
    
    # Remove URL prefix if present
    if "/" in arxiv_id:
        parts = arxiv_id.split("/")
        arxiv_id = parts[-1]
    
    # Remove version suffix if present (e.g., "1234.5678v1" -> "1234.5678")
    if "v" in arxiv_id and arxiv_id[-1].isdigit():
        arxiv_id = arxiv_id.rsplit("v", 1)[0]
    
    return arxiv_id


def convert_kaggle_to_schema(kaggle_record: Dict) -> Optional[Dict]:
    """
    Convert a Kaggle dataset record to match existing schema.
    
    Expected Kaggle fields (based on ArXiv metadata):
    - id: ArXiv ID
    - submitter: Submitter name
    - authors: List of authors
    - title: Paper title
    - comments: Comments
    - journal-ref: Journal reference
    - doi: DOI
    - abstract: Abstract
    - categories: Categories
    - versions: Version history
    - update_date: Update date
    - versions: List of versions with dates
    """
    try:
        # Parse arxiv_id
        arxiv_id = parse_arxiv_id(kaggle_record.get("id", ""))
        if not arxiv_id:
            return None
        
        # Format authors
        authors = kaggle_record.get("authors", [])
        if isinstance(authors, list):
            authors_str = ", ".join([str(a) for a in authors])
        elif isinstance(authors, str):
            authors_str = authors
        else:
            authors_str = ""
        
        # Format categories
        categories = kaggle_record.get("categories", "")
        if isinstance(categories, list):
            categories_str = "; ".join([str(c) for c in categories])
        elif isinstance(categories, str):
            categories_str = categories
        else:
            categories_str = ""
        
        # Get primary category (first category)
        primary_category = ""
        if categories:
            if isinstance(categories, list) and len(categories) > 0:
                primary_category = str(categories[0])
            elif isinstance(categories, str):
                primary_category = categories.split(";")[0].strip() if ";" in categories else categories
        
        # Parse dates
        # Kaggle dataset uses RFC 2822 format in versions['created']: "Mon, 2 Apr 2007 19:18:42 GMT"
        # Also has update_date in format "2008-11-26"
        published = None
        updated = None
        submission_date = None
        
        versions = kaggle_record.get("versions", [])
        if versions and isinstance(versions, list) and len(versions) > 0:
            # First version is usually the submission date
            first_version = versions[0]
            if isinstance(first_version, dict):
                version_date_str = first_version.get("created", "")
                if version_date_str:
                    try:
                        # Try RFC 2822 format first (e.g., "Mon, 2 Apr 2007 19:18:42 GMT")
                        submission_date = parsedate_to_datetime(version_date_str)
                        published = submission_date  # Use submission as published if no separate field
                    except (ValueError, TypeError):
                        # Fallback to ISO format
                        try:
                            submission_date = datetime.fromisoformat(str(version_date_str).replace("Z", "+00:00"))
                            published = submission_date
                        except:
                            pass
            
            # Last version is the update date
            last_version = versions[-1]
            if isinstance(last_version, dict):
                version_date_str = last_version.get("created", "")
                if version_date_str:
                    try:
                        # Try RFC 2822 format first
                        updated = parsedate_to_datetime(version_date_str)
                    except (ValueError, TypeError):
                        # Fallback to ISO format
                        try:
                            updated = datetime.fromisoformat(str(version_date_str).replace("Z", "+00:00"))
                        except:
                            pass
        
        # Try alternative date fields
        if not published:
            # Check update_date field (format: "2008-11-26")
            update_date_str = kaggle_record.get("update_date", "")
            if update_date_str:
                try:
                    # Try simple date format first
                    if len(str(update_date_str)) == 10:  # YYYY-MM-DD
                        published = datetime.strptime(str(update_date_str), "%Y-%m-%d")
                        submission_date = published
                    else:
                        published = datetime.fromisoformat(str(update_date_str).replace("Z", "+00:00"))
                        submission_date = published
                except:
                    pass
            
            # Try other date fields
            for date_field in ["submitted", "published", "created"]:
                date_str = kaggle_record.get(date_field, "")
                if date_str:
                    try:
                        # Try RFC 2822 first
                        published = parsedate_to_datetime(str(date_str))
                        submission_date = published
                        break
                    except (ValueError, TypeError):
                        # Fallback to ISO
                        try:
                            published = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
                            submission_date = published
                            break
                        except:
                            continue
        
        if not updated:
            update_str = kaggle_record.get("updated", "") or kaggle_record.get("update_date", "")
            if update_str:
                try:
                    # Try RFC 2822 first
                    updated = parsedate_to_datetime(str(update_str))
                except (ValueError, TypeError):
                    # Fallback to ISO or simple date
                    try:
                        if len(str(update_str)) == 10:  # YYYY-MM-DD
                            updated = datetime.strptime(str(update_str), "%Y-%m-%d")
                        else:
                            updated = datetime.fromisoformat(str(update_str).replace("Z", "+00:00"))
                    except:
                        pass
        
        # Current harvest date
        harvest_date = datetime.now()
        
        # Build PDF URL
        pdf_url = f"http://arxiv.org/pdf/{arxiv_id}.pdf"
        
        return {
            "arxiv_id": arxiv_id,
            "title": str(kaggle_record.get("title", "")),
            "authors": authors_str,
            "categories": categories_str,
            "abstract": str(kaggle_record.get("abstract", "")),
            "published": published,
            "updated": updated,
            "pdf_url": pdf_url,
            "doi": kaggle_record.get("doi") or None,
            "journal_ref": kaggle_record.get("journal-ref") or None,
            "comments": kaggle_record.get("comments") or None,
            "primary_category": primary_category,
            "submission_date": submission_date,
            "harvest_date": harvest_date,
        }
    except Exception as e:
        logger.warning(f"Error converting record: {e}")
        return None


def process_kaggle_dataset(dataset_path: Path, output_file: Path):
    """
    Process the Kaggle dataset and convert to our schema format.
    """
    logger.info("=" * 80)
    logger.info("Processing Kaggle dataset")
    logger.info("=" * 80)
    
    # Explore structure first
    structure = explore_dataset_structure(dataset_path)
    
    all_papers = []
    
    # Try to process based on file types found
    if structure["parquet_files"]:
        logger.info("Processing Parquet files...")
        for parquet_file in structure["parquet_files"]:
            logger.info(f"  Reading {parquet_file.name}...")
            df = pl.read_parquet(parquet_file)
            logger.info(f"    Found {len(df):,} records")
            
            # Convert to pandas for easier dict conversion
            df_pd = df.to_pandas()
            
            for idx, row in df_pd.iterrows():
                record = convert_kaggle_to_schema(row.to_dict())
                if record:
                    all_papers.append(record)
                
                if (idx + 1) % 10000 == 0:
                    logger.info(f"    Processed {idx + 1:,} records...")
    
    elif structure["json_files"]:
        logger.info("Processing JSON files...")
        for json_file in structure["json_files"]:
            logger.info(f"  Reading {json_file.name}...")
            
            # Check if it's JSONL (one JSON per line)
            with open(json_file, 'r') as f:
                first_line = f.readline()
                try:
                    json.loads(first_line)
                    is_jsonl = True
                except json.JSONDecodeError:
                    is_jsonl = False
            
            if is_jsonl:
                # JSONL format
                with open(json_file, 'r') as f:
                    for line_num, line in enumerate(f):
                        try:
                            record = json.loads(line)
                            converted = convert_kaggle_to_schema(record)
                            if converted:
                                all_papers.append(converted)
                        except json.JSONDecodeError as e:
                            logger.warning(f"  Error parsing line {line_num}: {e}")
                        
                        if (line_num + 1) % 10000 == 0:
                            logger.info(f"    Processed {line_num + 1:,} records...")
            else:
                # Regular JSON (might be a list or single object)
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        for record in data:
                            converted = convert_kaggle_to_schema(record)
                            if converted:
                                all_papers.append(converted)
                    elif isinstance(data, dict):
                        converted = convert_kaggle_to_schema(data)
                        if converted:
                            all_papers.append(converted)
    
    elif structure["csv_files"]:
        logger.info("Processing CSV files...")
        for csv_file in structure["csv_files"]:
            logger.info(f"  Reading {csv_file.name}...")
            # Read in chunks to handle large files
            chunk_size = 10000
            for chunk_num, chunk_df in enumerate(pd.read_csv(csv_file, chunksize=chunk_size)):
                for idx, row in chunk_df.iterrows():
                    record = convert_kaggle_to_schema(row.to_dict())
                    if record:
                        all_papers.append(record)
                
                logger.info(f"    Processed chunk {chunk_num + 1} ({len(all_papers):,} total records so far)...")
    
    else:
        logger.error("No recognized data files found in dataset!")
        return
    
    logger.info(f"Total papers processed: {len(all_papers):,}")
    
    # Convert to DataFrame and save
    if all_papers:
        logger.info("Converting to DataFrame and saving...")
        df = pl.DataFrame(all_papers)
        
        # Ensure datetime columns are properly typed
        datetime_cols = ["published", "updated", "submission_date", "harvest_date"]
        for col in datetime_cols:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.Datetime(time_unit="ns"))
                )
        
        # Remove duplicates
        df = df.unique(subset=["arxiv_id"], keep="first")
        logger.info(f"After deduplication: {len(df):,} papers")
        
        # Sort by published date
        df = df.sort("published")
        
        # Save
        df.write_parquet(output_file, compression="snappy")
        logger.info(f"Saved {len(df):,} papers to {output_file}")
        logger.info(f"File size: {output_file.stat().st_size / (1024 * 1024):.2f} MB")
    else:
        logger.warning("No papers were processed!")


def merge_with_existing_data(kaggle_file: Path):
    """Merge Kaggle data with existing ArXiv data"""
    existing_file = DATA_RAW / "claude_arxiv.parquet"
    improved_file = DATA_RAW / "arxiv_2021_2025.parquet"
    
    logger.info("=" * 80)
    logger.info("Merging with existing data")
    logger.info("=" * 80)
    
    all_dfs = []
    
    # Load existing files
    if existing_file.exists():
        logger.info(f"Loading {existing_file.name}...")
        df_existing = pl.read_parquet(existing_file)
        logger.info(f"  {len(df_existing):,} papers")
        all_dfs.append(df_existing)
    
    if improved_file.exists():
        logger.info(f"Loading {improved_file.name}...")
        df_improved = pl.read_parquet(improved_file)
        logger.info(f"  {len(df_improved):,} papers")
        all_dfs.append(df_improved)
    
    if kaggle_file.exists():
        logger.info(f"Loading {kaggle_file.name}...")
        df_kaggle = pl.read_parquet(kaggle_file)
        logger.info(f"  {len(df_kaggle):,} papers")
        all_dfs.append(df_kaggle)
    
    if not all_dfs:
        logger.warning("No data files to merge!")
        return
    
    # Combine all
    logger.info("Combining all datasets...")
    combined_df = pl.concat(all_dfs)
    logger.info(f"  Total before deduplication: {len(combined_df):,} papers")
    
    # Remove duplicates (keep first occurrence)
    combined_df = combined_df.unique(subset=["arxiv_id"], keep="first")
    logger.info(f"  Total after deduplication: {len(combined_df):,} papers")
    
    # Sort by published date
    combined_df = combined_df.sort("published")
    
    # Save merged file
    combined_df.write_parquet(MERGED_OUTPUT, compression="snappy")
    logger.info(f"Saved merged dataset to {MERGED_OUTPUT}")
    logger.info(f"File size: {MERGED_OUTPUT.stat().st_size / (1024 * 1024):.2f} MB")
    
    # Show year distribution
    if "published" in combined_df.columns:
        year_dist = combined_df.select(
            pl.col("published").dt.year().alias("year")
        ).group_by("year").agg(pl.len().alias("count")).sort("year")
        
        logger.info("\nYear distribution:")
        for row in year_dist.iter_rows():
            logger.info(f"  {row[0]}: {row[1]:,} papers")


def main():
    """Main function"""
    logger.info("=" * 80)
    logger.info("ArXiv Data Download from Kaggle")
    logger.info("=" * 80)
    
    try:
        # Download dataset
        dataset_path = download_kaggle_dataset()
        
        # Process dataset
        process_kaggle_dataset(dataset_path, OUTPUT_FILE)
        
        # Merge with existing data
        if OUTPUT_FILE.exists():
            merge_with_existing_data(OUTPUT_FILE)
        
        logger.info("=" * 80)
        logger.info("Process completed successfully!")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

