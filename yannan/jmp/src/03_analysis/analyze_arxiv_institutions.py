"""
Analyze ArXiv Papers with Institution Data

This script performs comprehensive analysis on the ArXiv paper-institution
linkage data, including:
- Papers per year
- Papers with/without institutions
- Citation distribution
- Institution type distribution
- Country distribution
- And other relevant statistics

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
LOGS_DIR = PROJECT_ROOT / "logs"

# Input files
ARXIV_INSTITUTIONS_PATH = DATA_PROCESSED / "arxiv_paper_institutions.parquet"
# Note: Using arxiv_kaggle.parquet (most complete) - not needed for analysis, but kept for reference
ARXIV_PATH = DATA_RAW / "arxiv_kaggle.parquet"
AI_PAPERS_PATH = DATA_RAW / "openalex_claude_ai_papers.parquet"

# Output file
ANALYSIS_OUTPUT = LOGS_DIR / "arxiv_institutions_analysis.txt"

# Ensure directories exist
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "analyze_arxiv_institutions.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# Analysis Functions
# ============================================================================

def analyze_papers_per_year(df: pl.DataFrame) -> pl.DataFrame:
    """Analyze papers per year"""
    return df.group_by('publication_year').agg([
        pl.count().alias('total_papers'),
        pl.n_unique('arxiv_id').alias('unique_papers')
    ]).sort('publication_year')


def analyze_institution_coverage(df: pl.DataFrame) -> dict:
    """Analyze institution coverage statistics"""
    total_papers = df.select('arxiv_id').n_unique()
    
    papers_with_inst = df.filter(
        pl.col('has_institution') == True
    ).select('arxiv_id').n_unique()
    
    papers_without_inst = df.filter(
        (pl.col('has_institution') == False) | (pl.col('has_institution').is_null())
    ).select('arxiv_id').n_unique()
    
    return {
        'total_papers': total_papers,
        'papers_with_institutions': papers_with_inst,
        'papers_without_institutions': papers_without_inst,
        'coverage_rate': papers_with_inst / total_papers if total_papers > 0 else 0
    }


def analyze_citation_distribution(df: pl.DataFrame, ai_papers: pl.DataFrame) -> dict:
    """Analyze citation distribution by joining with ai_papers"""
    # Join with ai_papers to get citation data
    df_with_citations = df.join(
        ai_papers.select(['openalex_id', 'cited_by_count']),
        on='openalex_id',
        how='left'
    )
    
    # Filter to paper level (one row per paper)
    papers_with_citations = df_with_citations.select([
        'arxiv_id',
        'cited_by_count',
        'has_institution'
    ]).unique(subset=['arxiv_id'])
    
    stats = {
        'papers_with_citation_data': papers_with_citations.filter(
            pl.col('cited_by_count').is_not_null()
        ).height,
        'papers_without_citation_data': papers_with_citations.filter(
            pl.col('cited_by_count').is_null()
        ).height,
    }
    
    # Citation statistics for papers with citations
    cited_papers = papers_with_citations.filter(pl.col('cited_by_count').is_not_null())
    if len(cited_papers) > 0:
        citation_stats = cited_papers.select([
            pl.col('cited_by_count').min().alias('min_citations'),
            pl.col('cited_by_count').max().alias('max_citations'),
            pl.col('cited_by_count').mean().alias('mean_citations'),
            pl.col('cited_by_count').median().alias('median_citations'),
            pl.col('cited_by_count').quantile(0.25).alias('p25_citations'),
            pl.col('cited_by_count').quantile(0.75).alias('p75_citations'),
            pl.col('cited_by_count').quantile(0.90).alias('p90_citations'),
            pl.col('cited_by_count').quantile(0.95).alias('p95_citations'),
            pl.col('cited_by_count').quantile(0.99).alias('p99_citations'),
        ])
        stats.update(citation_stats.to_dicts()[0])
    
    # Citations by institution status
    if 'has_institution' in papers_with_citations.columns:
        citations_by_inst = papers_with_citations.filter(
            pl.col('cited_by_count').is_not_null()
        ).group_by('has_institution').agg([
            pl.col('cited_by_count').mean().alias('mean_citations'),
            pl.col('cited_by_count').median().alias('median_citations'),
            pl.count().alias('paper_count')
        ])
        stats['citations_by_institution_status'] = citations_by_inst
    
    return stats


def analyze_institution_types(df: pl.DataFrame) -> pl.DataFrame:
    """Analyze distribution of institution types"""
    return df.filter(
        pl.col('institution_type').is_not_null()
    ).group_by('institution_type').agg([
        pl.count().alias('record_count'),
        pl.n_unique('arxiv_id').alias('unique_papers'),
        pl.n_unique('institution_id').alias('unique_institutions')
    ]).sort('record_count', descending=True)


def analyze_countries(df: pl.DataFrame) -> pl.DataFrame:
    """Analyze country distribution"""
    return df.filter(
        pl.col('country_code').is_not_null()
    ).group_by('country_code').agg([
        pl.count().alias('record_count'),
        pl.n_unique('arxiv_id').alias('unique_papers'),
        pl.n_unique('institution_id').alias('unique_institutions')
    ]).sort('record_count', descending=True)


def analyze_institutions_per_paper(df: pl.DataFrame) -> dict:
    """Analyze how many institutions per paper"""
    inst_per_paper = df.filter(
        pl.col('has_institution') == True
    ).group_by('arxiv_id').agg([
        pl.count().alias('institution_count')
    ])
    
    stats = inst_per_paper.select([
        pl.col('institution_count').min().alias('min_institutions'),
        pl.col('institution_count').max().alias('max_institutions'),
        pl.col('institution_count').mean().alias('mean_institutions'),
        pl.col('institution_count').median().alias('median_institutions'),
    ]).to_dicts()[0]
    
    # Distribution
    distribution = inst_per_paper.group_by('institution_count').agg(
        pl.count().alias('paper_count')
    ).sort('institution_count')
    
    stats['distribution'] = distribution
    
    return stats


def analyze_authors_per_paper(df: pl.DataFrame) -> dict:
    """Analyze author statistics per paper"""
    authors_per_paper = df.group_by('arxiv_id').agg([
        pl.n_unique('author_id').alias('author_count'),
        pl.n_unique('institution_id').alias('institution_count')
    ])
    
    stats = authors_per_paper.select([
        pl.col('author_count').mean().alias('mean_authors'),
        pl.col('author_count').median().alias('median_authors'),
        pl.col('author_count').max().alias('max_authors'),
        pl.col('institution_count').mean().alias('mean_institutions'),
    ]).to_dicts()[0]
    
    return stats


# ============================================================================
# Main Analysis
# ============================================================================

def run_analysis():
    """
    Run comprehensive analysis on ArXiv-institution data.
    """
    logger.info("=" * 80)
    logger.info("ArXiv-Institution Data Analysis")
    logger.info("=" * 80)
    logger.info("")
    
    # Check input file
    if not ARXIV_INSTITUTIONS_PATH.exists():
        logger.error(f"Input file not found: {ARXIV_INSTITUTIONS_PATH}")
        logger.error("Please run link_arxiv_to_institutions.py first")
        sys.exit(1)
    
    # Load data
    logger.info("Loading data...")
    logger.info(f"  Loading from {ARXIV_INSTITUTIONS_PATH.name}...")
    df = pl.read_parquet(ARXIV_INSTITUTIONS_PATH)
    logger.info(f"    Loaded {len(df):,} records")
    logger.info("")
    
    # Load ai_papers for citation data
    ai_papers = None
    if AI_PAPERS_PATH.exists():
        logger.info(f"  Loading citation data from {AI_PAPERS_PATH.name}...")
        ai_papers = pl.read_parquet(AI_PAPERS_PATH)
        logger.info(f"    Loaded {len(ai_papers):,} AI papers")
    else:
        logger.warning(f"  Citation data not found: {AI_PAPERS_PATH}")
    logger.info("")
    
    # Run analyses
    logger.info("Running analyses...")
    logger.info("")
    
    results = {}
    
    # 1. Papers per year
    logger.info("1. Analyzing papers per year...")
    results['papers_per_year'] = analyze_papers_per_year(df)
    logger.info(f"   Analyzed {len(results['papers_per_year'])} years")
    
    # 2. Institution coverage
    logger.info("2. Analyzing institution coverage...")
    results['coverage'] = analyze_institution_coverage(df)
    logger.info(f"   Coverage rate: {results['coverage']['coverage_rate']:.2%}")
    
    # 3. Citation distribution
    if ai_papers is not None:
        logger.info("3. Analyzing citation distribution...")
        results['citations'] = analyze_citation_distribution(df, ai_papers)
        logger.info("   Citation analysis complete")
    else:
        logger.info("3. Skipping citation analysis (data not available)")
    
    # 4. Institution types
    logger.info("4. Analyzing institution types...")
    results['institution_types'] = analyze_institution_types(df)
    logger.info(f"   Found {len(results['institution_types'])} institution types")
    
    # 5. Countries
    logger.info("5. Analyzing country distribution...")
    results['countries'] = analyze_countries(df)
    logger.info(f"   Found {len(results['countries'])} countries")
    
    # 6. Institutions per paper
    logger.info("6. Analyzing institutions per paper...")
    results['institutions_per_paper'] = analyze_institutions_per_paper(df)
    logger.info("   Analysis complete")
    
    # 7. Authors per paper
    logger.info("7. Analyzing authors per paper...")
    results['authors_per_paper'] = analyze_authors_per_paper(df)
    logger.info("   Analysis complete")
    
    logger.info("")
    
    # Write results to file
    logger.info(f"Writing analysis results to {ANALYSIS_OUTPUT}...")
    with open(ANALYSIS_OUTPUT, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("ARXIV-INSTITUTION DATA ANALYSIS REPORT\n")
        f.write("=" * 80 + "\n\n")
        
        # Coverage statistics
        f.write("INSTITUTION COVERAGE\n")
        f.write("-" * 80 + "\n")
        cov = results['coverage']
        f.write(f"Total ArXiv papers: {cov['total_papers']:,}\n")
        f.write(f"Papers with institutions: {cov['papers_with_institutions']:,}\n")
        f.write(f"Papers without institutions: {cov['papers_without_institutions']:,}\n")
        f.write(f"Coverage rate: {cov['coverage_rate']:.2%}\n\n")
        
        # Papers per year
        f.write("PAPERS PER YEAR\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Year':<10} {'Total Records':<15} {'Unique Papers':<15}\n")
        f.write("-" * 80 + "\n")
        for row in results['papers_per_year'].iter_rows(named=True):
            year = row['publication_year'] if row['publication_year'] is not None else 'N/A'
            total = row['total_papers'] if row['total_papers'] is not None else 0
            unique = row['unique_papers'] if row['unique_papers'] is not None else 0
            f.write(f"{year!s:<10} {total:<15,} {unique:<15,}\n")
        f.write("\n")
        
        # Citation statistics
        if 'citations' in results:
            f.write("CITATION STATISTICS\n")
            f.write("-" * 80 + "\n")
            cit = results['citations']
            f.write(f"Papers with citation data: {cit.get('papers_with_citation_data', 0):,}\n")
            f.write(f"Papers without citation data: {cit.get('papers_without_citation_data', 0):,}\n")
            if 'mean_citations' in cit:
                f.write(f"\nCitation Distribution:\n")
                f.write(f"  Minimum: {cit.get('min_citations', 0):,.0f}\n")
                f.write(f"  25th percentile: {cit.get('p25_citations', 0):,.0f}\n")
                f.write(f"  Median: {cit.get('median_citations', 0):,.0f}\n")
                f.write(f"  Mean: {cit.get('mean_citations', 0):,.1f}\n")
                f.write(f"  75th percentile: {cit.get('p75_citations', 0):,.0f}\n")
                f.write(f"  90th percentile: {cit.get('p90_citations', 0):,.0f}\n")
                f.write(f"  95th percentile: {cit.get('p95_citations', 0):,.0f}\n")
                f.write(f"  99th percentile: {cit.get('p99_citations', 0):,.0f}\n")
                f.write(f"  Maximum: {cit.get('max_citations', 0):,.0f}\n")
            f.write("\n")
        
        # Institution types
        f.write("INSTITUTION TYPES\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Type':<25} {'Records':<15} {'Unique Papers':<15} {'Unique Inst.':<15}\n")
        f.write("-" * 80 + "\n")
        for row in results['institution_types'].iter_rows(named=True):
            f.write(f"{row['institution_type']:<25} {row['record_count']:<15,} {row['unique_papers']:<15,} {row['unique_institutions']:<15,}\n")
        f.write("\n")
        
        # Top countries
        f.write("TOP 20 COUNTRIES\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Country':<10} {'Records':<15} {'Unique Papers':<15} {'Unique Inst.':<15}\n")
        f.write("-" * 80 + "\n")
        for row in results['countries'].head(20).iter_rows(named=True):
            f.write(f"{row['country_code']:<10} {row['record_count']:<15,} {row['unique_papers']:<15,} {row['unique_institutions']:<15,}\n")
        f.write("\n")
        
        # Institutions per paper
        f.write("INSTITUTIONS PER PAPER\n")
        f.write("-" * 80 + "\n")
        inst_pp = results['institutions_per_paper']
        f.write(f"Minimum: {inst_pp.get('min_institutions', 0)}\n")
        f.write(f"Mean: {inst_pp.get('mean_institutions', 0):.2f}\n")
        f.write(f"Median: {inst_pp.get('median_institutions', 0)}\n")
        f.write(f"Maximum: {inst_pp.get('max_institutions', 0)}\n")
        f.write("\nDistribution:\n")
        f.write(f"{'Institutions':<15} {'Paper Count':<15}\n")
        f.write("-" * 40 + "\n")
        for row in inst_pp['distribution'].iter_rows(named=True):
            f.write(f"{row['institution_count']:<15} {row['paper_count']:<15,}\n")
        f.write("\n")
        
        # Authors per paper
        f.write("AUTHORS PER PAPER\n")
        f.write("-" * 80 + "\n")
        auth_pp = results['authors_per_paper']
        f.write(f"Mean authors: {auth_pp.get('mean_authors', 0):.2f}\n")
        f.write(f"Median authors: {auth_pp.get('median_authors', 0)}\n")
        f.write(f"Max authors: {auth_pp.get('max_authors', 0)}\n")
        f.write(f"Mean institutions: {auth_pp.get('mean_institutions', 0):.2f}\n")
        f.write("\n")
        
        f.write("=" * 80 + "\n")
        f.write("Analysis completed successfully!\n")
        f.write("=" * 80 + "\n")
    
    logger.info(f"  Analysis report saved to {ANALYSIS_OUTPUT}")
    logger.info("")
    logger.info("=" * 80)
    logger.info("Analysis completed successfully!")
    logger.info("=" * 80)


def main():
    """Main execution function"""
    try:
        run_analysis()
        logger.info("\n✓ Analysis completed successfully!")
    except KeyboardInterrupt:
        logger.warning("\n✗ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

