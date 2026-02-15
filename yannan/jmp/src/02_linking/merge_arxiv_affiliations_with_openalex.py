"""
Merge ArXiv Affiliations with OpenAlex Institution Data

This script combines:
1. OpenAlex institution data (from extract_institutions_from_json.py)
2. ArXiv affiliations from authors field (from extract_affiliations_from_arxiv_authors.py)

The goal is to maximize coverage by using all available sources.

Date: 2025
"""

import polars as pl
from pathlib import Path
import logging

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"

# Input files
OPENALEX_INSTITUTIONS = DATA_PROCESSED / "paper_institutions.parquet"
ARXIV_AFFILIATIONS = DATA_PROCESSED / "arxiv_affiliations_from_authors.parquet"
ARXIV_PAPER_INSTITUTIONS = DATA_PROCESSED / "arxiv_paper_institutions.parquet"

# Output file
MERGED_AFFILIATIONS = DATA_PROCESSED / "merged_paper_affiliations.parquet"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def merge_affiliations():
    """
    Merge OpenAlex institution data with ArXiv affiliations.
    """
    print("=" * 80)
    print("MERGING ARXIV AFFILIATIONS WITH OPENALEX DATA")
    print("=" * 80)
    
    # Load OpenAlex data
    print("\n1. Loading OpenAlex institution data...")
    if not OPENALEX_INSTITUTIONS.exists():
        logger.error(f"OpenAlex institutions file not found: {OPENALEX_INSTITUTIONS}")
        return
    
    openalex_df = pl.read_parquet(OPENALEX_INSTITUTIONS)
    print(f"   Loaded {len(openalex_df):,} OpenAlex institution records")
    
    # Get unique papers from OpenAlex (need to link via arxiv_id)
    # First, we need to link OpenAlex IDs to ArXiv IDs
    print("\n2. Loading ArXiv-OpenAlex linking data...")
    if not ARXIV_PAPER_INSTITUTIONS.exists():
        logger.warning("ArXiv-OpenAlex linking file not found. Will only use ArXiv affiliations.")
        arxiv_linking = None
    else:
        arxiv_linking = pl.read_parquet(ARXIV_PAPER_INSTITUTIONS)
        print(f"   Loaded {len(arxiv_linking):,} linking records")
    
    # Load ArXiv affiliations
    print("\n3. Loading ArXiv affiliations from authors field...")
    if not ARXIV_AFFILIATIONS.exists():
        logger.warning("ArXiv affiliations file not found. Will only use OpenAlex data.")
        arxiv_aff_df = None
    else:
        arxiv_aff_df = pl.read_parquet(ARXIV_AFFILIATIONS)
        print(f"   Loaded {len(arxiv_aff_df):,} ArXiv affiliation records")
        print(f"   Papers with affiliations: {arxiv_aff_df.select('arxiv_id').n_unique():,}")
    
    # Statistics
    print("\n" + "=" * 80)
    print("COVERAGE STATISTICS")
    print("=" * 80)
    
    if arxiv_linking is not None:
        # Count papers with OpenAlex institutions
        papers_with_openalex = arxiv_linking.filter(
            pl.col('has_institution') == True
        ).select('arxiv_id').n_unique()
        total_linked_papers = arxiv_linking.select('arxiv_id').n_unique()
        print(f"\nOpenAlex Coverage:")
        print(f"  Papers with institutions: {papers_with_openalex:,} / {total_linked_papers:,} ({papers_with_openalex/total_linked_papers*100:.1f}%)")
    
    if arxiv_aff_df is not None:
        papers_with_arxiv_aff = arxiv_aff_df.filter(
            pl.col('has_affiliation') == True
        ).select('arxiv_id').n_unique()
        total_arxiv_papers = arxiv_aff_df.select('arxiv_id').n_unique()
        print(f"\nArXiv Authors Field Coverage:")
        print(f"  Papers with affiliations: {papers_with_arxiv_aff:,} / {total_arxiv_papers:,} ({papers_with_arxiv_aff/total_arxiv_papers*100:.1f}%)")
        
        # Big Tech breakdown
        big_tech = arxiv_aff_df.filter(pl.col('big_tech_firm').is_not_null())
        if len(big_tech) > 0:
            print(f"\nBig Tech from ArXiv authors field:")
            big_tech_summary = big_tech.group_by('big_tech_firm').agg([
                pl.len().alias('author_count'),
                pl.n_unique('arxiv_id').alias('paper_count')
            ]).sort('author_count', descending=True)
            print(big_tech_summary)
    
    # If we have both sources, show overlap
    if arxiv_linking is not None and arxiv_aff_df is not None:
        papers_openalex = set(arxiv_linking.filter(pl.col('has_institution') == True).select('arxiv_id').unique().to_series().to_list())
        papers_arxiv = set(arxiv_aff_df.filter(pl.col('has_affiliation') == True).select('arxiv_id').unique().to_series().to_list())
        
        overlap = papers_openalex & papers_arxiv
        only_openalex = papers_openalex - papers_arxiv
        only_arxiv = papers_arxiv - papers_openalex
        combined = papers_openalex | papers_arxiv
        
        print(f"\nOverlap Analysis:")
        print(f"  Papers with OpenAlex only: {len(only_openalex):,}")
        print(f"  Papers with ArXiv only: {len(only_arxiv):,}")
        print(f"  Papers with both: {len(overlap):,}")
        print(f"  Combined coverage: {len(combined):,} papers")
        print(f"  Improvement: +{len(only_arxiv):,} papers ({len(only_arxiv)/len(combined)*100:.1f}% additional)")
    
    print("\n" + "=" * 80)
    print("Merge completed!")
    print("=" * 80)
    print("\nNote: This script provides statistics. To actually merge the data,")
    print("you would need to:")
    print("  1. Link OpenAlex openalex_id to arxiv_id")
    print("  2. Combine institution records from both sources")
    print("  3. Deduplicate and prioritize (OpenAlex > ArXiv authors field)")


if __name__ == "__main__":
    merge_affiliations()

