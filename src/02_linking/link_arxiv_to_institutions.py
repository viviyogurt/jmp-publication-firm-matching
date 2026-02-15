"""
Link ArXiv Papers to Institutions

This script creates a linkage table between ArXiv papers and institutions
by joining ArXiv data with the extracted institution data.

For each ArXiv paper, it shows all associated institutions (no filtering).
This provides the base dataset for further analysis and firm matching.

Date: 2025
"""

import polars as pl
from pathlib import Path
import logging
import sys

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"

# Input files
ARXIV_PATH = DATA_RAW / "arxiv_kaggle.parquet"  # Using deduplicated Kaggle data (most complete)
ARXIV_INDEX_PATH = DATA_RAW / "openalex_claude_arxiv_index.parquet"
PAPER_INSTITUTIONS_PATH = DATA_PROCESSED / "paper_institutions.parquet"

# Output files
ARXIV_INSTITUTIONS_OUTPUT = DATA_PROCESSED / "arxiv_paper_institutions.parquet"

# Ensure output directory exists
DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Main Processing
# ============================================================================

def link_arxiv_to_institutions():
    """
    Link ArXiv papers to institutions through OpenAlex.
    """
    logger.info("=" * 80)
    logger.info("Linking ArXiv Papers to Institutions")
    logger.info("=" * 80)
    logger.info("")
    
    # Check input files
    required_files = [
        ARXIV_PATH,
        ARXIV_INDEX_PATH,
        PAPER_INSTITUTIONS_PATH
    ]
    
    for file_path in required_files:
        if not file_path.exists():
            logger.error(f"Required file not found: {file_path}")
            sys.exit(1)
    
    # Load data
    logger.info("Loading data files...")
    logger.info(f"  Loading ArXiv papers from {ARXIV_PATH.name}...")
    arxiv = pl.read_parquet(ARXIV_PATH)
    logger.info(f"    Loaded {len(arxiv):,} ArXiv papers")
    
    logger.info(f"  Loading ArXiv-OpenAlex index from {ARXIV_INDEX_PATH.name}...")
    arxiv_index = pl.read_parquet(ARXIV_INDEX_PATH)
    logger.info(f"    Loaded {len(arxiv_index):,} index entries")
    
    logger.info(f"  Loading paper institutions from {PAPER_INSTITUTIONS_PATH.name}...")
    paper_institutions = pl.read_parquet(PAPER_INSTITUTIONS_PATH)
    logger.info(f"    Loaded {len(paper_institutions):,} institution records")
    logger.info("")
    
    # Step 1: Join ArXiv with arxiv_index to get openalex_id
    logger.info("Step 1: Linking ArXiv papers to OpenAlex IDs...")
    arxiv_with_openalex = arxiv.join(
        arxiv_index.select(['arxiv_id', 'openalex_id']),
        on='arxiv_id',
        how='left'
    )
    
    # Count papers with OpenAlex matches
    papers_with_openalex = arxiv_with_openalex.filter(
        pl.col('openalex_id').is_not_null()
    )
    logger.info(f"  ArXiv papers with OpenAlex matches: {len(papers_with_openalex):,} / {len(arxiv):,}")
    logger.info("")
    
    # Step 2: Join with paper_institutions to get institution data
    logger.info("Step 2: Linking to institution data...")
    arxiv_paper_institutions = papers_with_openalex.join(
        paper_institutions,
        on='openalex_id',
        how='left'
    )
    
    logger.info(f"  Total records after join: {len(arxiv_paper_institutions):,}")
    logger.info("")
    
    # Step 3: Create summary statistics
    logger.info("Step 3: Creating summary statistics...")
    
    # Papers with institutions
    papers_with_inst = arxiv_paper_institutions.filter(
        pl.col('has_institution') == True
    ).select('arxiv_id').n_unique()
    
    # Papers without institutions
    papers_without_inst = arxiv_paper_institutions.filter(
        (pl.col('has_institution') == False) | (pl.col('has_institution').is_null())
    ).select('arxiv_id').n_unique()
    
    logger.info(f"  Papers with institutions: {papers_with_inst:,}")
    logger.info(f"  Papers without institutions: {papers_without_inst:,}")
    logger.info("")
    
    # Step 4: Select and organize columns for output
    logger.info("Step 4: Organizing output columns...")
    
    # Select relevant columns, keeping all institution data
    output_columns = [
        # ArXiv paper identifiers
        'arxiv_id',
        'title',
        'published',
        'updated',
        'submission_date',
        
        # OpenAlex linkage
        'openalex_id',
        
        # Publication year (from institution data)
        'publication_year',
        'publication_date',
        
        # Author information
        'author_id',
        'author_display_name',
        'author_position',
        'is_corresponding',
        
        # Institution information
        'institution_id',
        'institution_name',
        'institution_type',
        'institution_ror',
        'country_code',
        'institution_lineage',
        'has_institution',
        
        # Additional ArXiv metadata
        'categories',
        'primary_category',
        'doi',
        'journal_ref',
    ]
    
    # Select only columns that exist
    available_columns = [col for col in output_columns if col in arxiv_paper_institutions.columns]
    
    arxiv_institutions_final = arxiv_paper_institutions.select(available_columns)
    
    logger.info(f"  Selected {len(available_columns)} columns for output")
    logger.info("")
    
    # Step 5: Save results
    logger.info(f"Step 5: Saving results to {ARXIV_INSTITUTIONS_OUTPUT}...")
    arxiv_institutions_final.write_parquet(
        ARXIV_INSTITUTIONS_OUTPUT,
        compression='snappy'
    )
    
    file_size_mb = ARXIV_INSTITUTIONS_OUTPUT.stat().st_size / (1024 * 1024)
    logger.info(f"  Saved successfully ({file_size_mb:.2f} MB)")
    logger.info("")
    
    # Final summary
    logger.info("=" * 80)
    logger.info("LINKAGE SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total ArXiv papers: {len(arxiv):,}")
    logger.info(f"Papers with OpenAlex matches: {len(papers_with_openalex):,}")
    logger.info(f"Papers with institutions: {papers_with_inst:,}")
    logger.info(f"Papers without institutions: {papers_without_inst:,}")
    logger.info(f"Total paper-institution records: {len(arxiv_institutions_final):,}")
    logger.info("")
    
    # Institution coverage
    unique_institutions = arxiv_institutions_final.filter(
        pl.col('institution_id').is_not_null()
    ).select('institution_id').n_unique()
    logger.info(f"Unique institutions linked: {unique_institutions:,}")
    
    # Institution types
    inst_types = arxiv_institutions_final.filter(
        pl.col('institution_type').is_not_null()
    ).group_by('institution_type').agg(
        pl.count().alias('count')
    ).sort('count', descending=True)
    
    logger.info("\nInstitution types in linked data:")
    for row in inst_types.iter_rows(named=True):
        logger.info(f"  {row['institution_type']:20s} {row['count']:>10,}")
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("Linking completed successfully!")
    logger.info("=" * 80)


def main():
    """Main execution function"""
    try:
        link_arxiv_to_institutions()
        logger.info("\n✓ ArXiv-institution linking completed successfully!")
    except KeyboardInterrupt:
        logger.warning("\n✗ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

