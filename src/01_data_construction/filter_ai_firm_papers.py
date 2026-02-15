"""
Filter AI Papers to AI-Firm-Affiliated Papers

This script:
1. Loads institutions from OpenAlex institutions_all.jsonl.gz
2. Creates a lookup table for institution types (company vs others)
3. For each AI paper, calculates firm_ratio (percentage of unique institutions that are companies)
4. Filters papers where firm_ratio > 0

Efficient approach:
- Load institutions once into memory (O(1) lookup)
- Process papers in chunks to handle large datasets
- Use set operations for fast institution type checking
- Vectorized operations where possible
"""

import gzip
import json
import logging
from pathlib import Path
from typing import Dict, Set, List, Optional
import polars as pl
from collections import defaultdict
import sys

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
LOGS_DIR = PROJECT_ROOT / "data" / "processed" / "logs"

INSTITUTIONS_FILE = DATA_RAW / "institutions_all.jsonl.gz"
AI_PAPERS_FILE = DATA_PROCESSED / "ai_papers_condensed.parquet"
OUTPUT_FILE = DATA_PROCESSED / "ai_papers_firm_affiliated.parquet"

# Processing configuration
CHUNK_SIZE = 50000  # Process papers in chunks to manage memory
LOG_INTERVAL = 10000  # Log progress every N papers

# Ensure logs directory exists
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "filter_firm_papers.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Load Institutions Database
# ============================================================================

def normalize_institution_id(inst_id: str) -> Optional[str]:
    """
    Normalize institution ID to extract just the ID part.
    
    Handles formats:
    - https://openalex.org/I123456
    - I123456
    - https://openalex.org/I4210109390
    
    Returns:
        Normalized ID (e.g., "I123456") or None if invalid
    """
    if not inst_id or not isinstance(inst_id, str):
        return None
    
    inst_id = inst_id.strip()
    
    # If it's a full URL, extract the ID part
    if inst_id.startswith('https://openalex.org/I'):
        return inst_id.split('/')[-1]
    
    # If it already starts with 'I' and looks like an ID
    if inst_id.startswith('I') and len(inst_id) > 1:
        return inst_id
    
    return None

def load_institutions_lookup(file_path: Path) -> Dict[str, str]:
    """
    Load institutions from JSONL.gz and create a lookup table.
    
    Returns:
        Dictionary mapping normalized_institution_id -> institution_type
        Only includes institutions with valid IDs
    """
    logger.info("=" * 80)
    logger.info("Loading Institutions Database")
    logger.info("=" * 80)
    logger.info(f"Reading from: {file_path}")
    
    if not file_path.exists():
        raise FileNotFoundError(f"Institutions file not found: {file_path}")
    
    lookup: Dict[str, str] = {}
    type_counts = defaultdict(int)
    total_loaded = 0
    
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                inst = json.loads(line.strip())
                total_loaded += 1
                
                inst_id = inst.get('id', '')
                inst_type = inst.get('type') or 'unknown'  # Handle None values
                
                # Normalize ID
                normalized_id = normalize_institution_id(inst_id)
                if normalized_id:
                    lookup[normalized_id] = inst_type
                    type_counts[inst_type] += 1
                    
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse line {line_num}: {e}")
                continue
            except Exception as e:
                logger.warning(f"Error processing line {line_num}: {e}")
                continue
    
    logger.info(f"Loaded {total_loaded:,} institution records")
    logger.info(f"Created lookup table with {len(lookup):,} valid institution entries")
    
    logger.info("\nInstitution type distribution:")
    for inst_type, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        inst_type_str = str(inst_type) if inst_type is not None else 'unknown'
        logger.info(f"  {inst_type_str:20s} {count:10,} institutions")
    
    company_count = type_counts.get('company', 0)
    logger.info(f"\nTotal company institutions: {company_count:,}")
    logger.info("=" * 80)
    
    return lookup

# ============================================================================
# Calculate Firm Affiliation Metrics
# ============================================================================

def calculate_firm_metrics(
    author_affiliation_ids: List[List[str]],
    institutions_lookup: Dict[str, str]
) -> Dict[str, float]:
    """
    Calculate firm affiliation metrics for a paper.
    
    Args:
        author_affiliation_ids: Nested list of institution IDs per author
                               Format: [[inst_id1, inst_id2], [inst_id3], ...]
        institutions_lookup: Dictionary mapping normalized_id -> type
    
    Returns:
        Dictionary with:
        - firm_count: Number of unique company institutions
        - total_institutions: Number of unique institutions (all types)
        - firm_ratio: firm_count / total_institutions (0.0 to 1.0)
    """
    # Collect all unique institution IDs from all authors
    all_institution_ids: Set[str] = set()
    
    for author_insts in author_affiliation_ids:
        if not isinstance(author_insts, list):
            continue
        
        for inst_id in author_insts:
            normalized_id = normalize_institution_id(inst_id)
            if normalized_id:
                all_institution_ids.add(normalized_id)
    
    # If no valid institutions found
    if not all_institution_ids:
        return {
            'firm_count': 0,
            'total_institutions': 0,
            'firm_ratio': 0.0
        }
    
    # Count companies
    firm_count = 0
    for inst_id in all_institution_ids:
        inst_type = institutions_lookup.get(inst_id, 'unknown')
        if inst_type == 'company':
            firm_count += 1
    
    total_institutions = len(all_institution_ids)
    firm_ratio = firm_count / total_institutions if total_institutions > 0 else 0.0
    
    return {
        'firm_count': firm_count,
        'total_institutions': total_institutions,
        'firm_ratio': firm_ratio
    }

def process_papers_chunk(
    papers_chunk: pl.DataFrame,
    institutions_lookup: Dict[str, str]
) -> pl.DataFrame:
    """
    Process a chunk of papers and calculate firm metrics.
    
    Args:
        papers_chunk: DataFrame chunk with papers
        institutions_lookup: Institution type lookup table
    
    Returns:
        DataFrame with added columns: firm_count, total_institutions, firm_ratio
    """
    # Convert to pandas for easier row-wise processing (polars doesn't handle nested lists well)
    # Or use polars with apply if needed
    results = []
    
    for row in papers_chunk.iter_rows(named=True):
        author_affiliation_ids = row.get('author_affiliation_ids', [])
        
        metrics = calculate_firm_metrics(author_affiliation_ids, institutions_lookup)
        
        # Add metrics to row
        row.update(metrics)
        results.append(row)
    
    # Convert back to polars DataFrame
    result_df = pl.DataFrame(results)
    
    return result_df

# ============================================================================
# Main Processing
# ============================================================================

def filter_firm_affiliated_papers():
    """
    Main function to filter AI papers to firm-affiliated papers.
    """
    logger.info("=" * 80)
    logger.info("Filtering AI Papers to Firm-Affiliated Papers")
    logger.info("=" * 80)
    
    # Step 1: Load institutions lookup
    logger.info("\n[Step 1/4] Loading institutions database...")
    institutions_lookup = load_institutions_lookup(INSTITUTIONS_FILE)
    
    # Step 2: Load AI papers
    logger.info("\n[Step 2/4] Loading AI papers...")
    if not AI_PAPERS_FILE.exists():
        raise FileNotFoundError(f"AI papers file not found: {AI_PAPERS_FILE}")
    
    logger.info(f"Reading from: {AI_PAPERS_FILE}")
    ai_papers = pl.read_parquet(AI_PAPERS_FILE)
    total_papers = len(ai_papers)
    logger.info(f"Loaded {total_papers:,} AI papers")
    
    # Step 3: Calculate firm metrics for all papers
    logger.info("\n[Step 3/4] Calculating firm affiliation metrics...")
    logger.info(f"Processing in chunks of {CHUNK_SIZE:,} papers...")
    
    all_results = []
    processed = 0
    
    for i in range(0, total_papers, CHUNK_SIZE):
        chunk_end = min(i + CHUNK_SIZE, total_papers)
        papers_chunk = ai_papers[i:chunk_end]
        
        # Process chunk
        chunk_results = process_papers_chunk(papers_chunk, institutions_lookup)
        all_results.append(chunk_results)
        
        processed = chunk_end
        progress_pct = (processed / total_papers * 100) if total_papers > 0 else 0
        
        if processed % LOG_INTERVAL < CHUNK_SIZE or processed == total_papers:
            logger.info(f"  Processed {processed:,} / {total_papers:,} papers ({progress_pct:.1f}%)")
    
    # Combine all chunks
    logger.info("\nCombining results...")
    papers_with_metrics = pl.concat(all_results)
    logger.info(f"Calculated metrics for {len(papers_with_metrics):,} papers")
    
    # Step 4: Filter papers with firm_ratio > 0
    logger.info("\n[Step 4/4] Filtering papers with firm_ratio > 0...")
    firm_papers = papers_with_metrics.filter(pl.col('firm_ratio') > 0)
    
    firm_papers_count = len(firm_papers)
    logger.info(f"Found {firm_papers_count:,} firm-affiliated papers ({firm_papers_count/total_papers*100:.2f}% of total)")
    
    # Summary statistics
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY STATISTICS")
    logger.info("=" * 80)
    
    if firm_papers_count > 0:
        stats = firm_papers.select([
            pl.col('firm_count').mean().alias('avg_firm_count'),
            pl.col('firm_ratio').mean().alias('avg_firm_ratio'),
            pl.col('firm_ratio').median().alias('median_firm_ratio'),
            pl.col('firm_count').max().alias('max_firm_count'),
        ])
        
        stats_dict = stats.to_dicts()[0]
        logger.info(f"Average firm count per paper: {stats_dict['avg_firm_count']:.2f}")
        logger.info(f"Average firm ratio: {stats_dict['avg_firm_ratio']:.4f}")
        logger.info(f"Median firm ratio: {stats_dict['median_firm_ratio']:.4f}")
        logger.info(f"Maximum firm count: {stats_dict['max_firm_count']}")
    
    # Save results
    logger.info(f"\nSaving firm-affiliated papers to: {OUTPUT_FILE}")
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    firm_papers.write_parquet(OUTPUT_FILE, compression='snappy')
    
    file_size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    logger.info(f"  Saved successfully ({file_size_mb:.2f} MB)")
    
    logger.info("\n" + "=" * 80)
    logger.info("Filtering completed successfully!")
    logger.info("=" * 80)
    
    return firm_papers

# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    try:
        filter_firm_affiliated_papers()
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)
