"""
Analyze Institution Types in arxiv_paper_institutions.parquet

1. Check distribution of institution types
2. For institution_type='firm', count papers from big tech companies
"""

import polars as pl
from pathlib import Path
import logging

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
OUTPUT_DIR = PROJECT_ROOT / "output" / "tables"

INPUT_FILE = DATA_PROCESSED / "arxiv_paper_institutions.parquet"
OUTPUT_CSV = OUTPUT_DIR / "institution_type_distribution.csv"
OUTPUT_BIGTECH = OUTPUT_DIR / "bigtech_papers_analysis.csv"

# Big tech company names (case-insensitive matching)
BIG_TECH_COMPANIES = [
    "GOOGLE",
    "MICROSOFT", 
    "META",
    "AMAZON",
    "APPLE"
]

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "logs" / "analyze_institution_types.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def analyze_institution_types():
    """
    Analyze institution types and big tech papers.
    """
    logger.info("=" * 80)
    logger.info("ANALYZING INSTITUTION TYPES IN ARXIV PAPER INSTITUTIONS")
    logger.info("=" * 80)
    logger.info("")
    
    # Step 1: Load data
    logger.info("Step 1: Loading arxiv_paper_institutions.parquet...")
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        return
    
    df = pl.read_parquet(INPUT_FILE)
    logger.info(f"  Loaded {len(df):,} rows")
    logger.info(f"  Columns: {df.columns}")
    logger.info("")
    
    # Step 2: Check institution type distribution
    logger.info("Step 2: Analyzing institution type distribution...")
    
    # Count by institution_type
    type_dist = (
        df
        .group_by("institution_type")
        .agg([
            pl.len().alias("count"),
            pl.col("arxiv_id").n_unique().alias("unique_papers"),
            pl.col("institution_name").n_unique().alias("unique_institutions")
        ])
        .sort("count", descending=True)
    )
    
    logger.info("\nInstitution Type Distribution:")
    logger.info("-" * 80)
    for row in type_dist.iter_rows(named=True):
        inst_type = row['institution_type'] if row['institution_type'] else "NULL/Missing"
        logger.info(f"  {inst_type}:")
        logger.info(f"    Total rows: {row['count']:,}")
        logger.info(f"    Unique papers: {row['unique_papers']:,}")
        logger.info(f"    Unique institutions: {row['unique_institutions']:,}")
    logger.info("")
    
    # Save distribution
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    type_dist.write_csv(OUTPUT_CSV)
    logger.info(f"  Distribution saved to: {OUTPUT_CSV}")
    logger.info("")
    
    # Step 3: Filter for company type (note: dataset uses "company" not "firm")
    logger.info("Step 3: Analyzing 'company' institution type...")
    
    company_df = df.filter(pl.col("institution_type") == "company")
    logger.info(f"  Total rows with institution_type='company': {len(company_df):,}")
    logger.info(f"  Unique papers with company affiliations: {company_df['arxiv_id'].n_unique():,}")
    logger.info(f"  Unique company institutions: {company_df['institution_name'].n_unique():,}")
    logger.info("")
    
    # Step 4: Identify big tech companies
    logger.info("Step 4: Identifying big tech companies...")
    
    # Create a column to identify big tech (check each company separately)
    bigtech_conditions = []
    for company in BIG_TECH_COMPANIES:
        bigtech_conditions.append(
            pl.col("institution_name").str.to_uppercase().str.contains(company, literal=True)
        )
    
    # Combine conditions with OR
    is_bigtech = bigtech_conditions[0]
    for condition in bigtech_conditions[1:]:
        is_bigtech = is_bigtech | condition
    
    company_df = company_df.with_columns([
        is_bigtech.alias("is_bigtech")
    ])
    
    # Count big tech papers
    bigtech_df = company_df.filter(pl.col("is_bigtech") == True)
    
    logger.info(f"  Rows from big tech companies: {len(bigtech_df):,}")
    logger.info(f"  Unique papers from big tech: {bigtech_df['arxiv_id'].n_unique():,}")
    logger.info("")
    
    # Step 5: Breakdown by company
    logger.info("Step 5: Breakdown by big tech company...")
    
    company_counts = []
    for company in BIG_TECH_COMPANIES:
        # For META, be more specific to avoid false positives
        if company == "META":
            # Match "Meta" or "Facebook" but exclude common false positives
            company_rows = company_df.filter(
                (
                    (pl.col("institution_name").str.to_uppercase().str.contains("^META ", literal=False)) |
                    (pl.col("institution_name").str.to_uppercase().str.contains(" META", literal=False)) |
                    (pl.col("institution_name").str.to_uppercase().str.contains("^META$", literal=False)) |
                    (pl.col("institution_name").str.to_uppercase().str.contains("FACEBOOK", literal=True))
                ) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METAL", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METAMATERIAL", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METALL", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METRIC", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("KENNAMETAL", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("SUMITOMO", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("PRIMETALS", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("RHEINMETALL", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METABOLIC", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("MIMETAS", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METASENSING", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METAMATERIA", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("META VISION", literal=True))
            )
        else:
            company_rows = company_df.filter(
                pl.col("institution_name").str.to_uppercase().str.contains(company, literal=True)
            )
        
        unique_papers = company_rows['arxiv_id'].n_unique()
        unique_institutions = company_rows['institution_name'].n_unique()
        
        company_counts.append({
            'company': company,
            'total_rows': len(company_rows),
            'unique_papers': unique_papers,
            'unique_institutions': unique_institutions
        })
        
        logger.info(f"  {company}:")
        logger.info(f"    Total rows: {len(company_rows):,}")
        logger.info(f"    Unique papers: {unique_papers:,}")
        logger.info(f"    Unique institution names: {unique_institutions:,}")
    
    logger.info("")
    
    # Step 6: Get sample institution names for each big tech
    logger.info("Step 6: Sample institution names for each big tech company...")
    
    for company in BIG_TECH_COMPANIES:
        # For META, use the same refined filter
        if company == "META":
            company_rows = company_df.filter(
                (
                    (pl.col("institution_name").str.to_uppercase().str.contains("^META ", literal=False)) |
                    (pl.col("institution_name").str.to_uppercase().str.contains(" META", literal=False)) |
                    (pl.col("institution_name").str.to_uppercase().str.contains("^META$", literal=False)) |
                    (pl.col("institution_name").str.to_uppercase().str.contains("FACEBOOK", literal=True))
                ) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METAL", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METAMATERIAL", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METALL", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METRIC", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("KENNAMETAL", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("SUMITOMO", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("PRIMETALS", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("RHEINMETALL", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METABOLIC", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("MIMETAS", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METASENSING", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("METAMATERIA", literal=True)) &
                (~pl.col("institution_name").str.to_uppercase().str.contains("META VISION", literal=True))
            )
        else:
            company_rows = company_df.filter(
                pl.col("institution_name").str.to_uppercase().str.contains(company, literal=True)
            )
        unique_names = company_rows['institution_name'].unique().head(10).to_list()
        logger.info(f"  {company} institution names (sample):")
        for name in unique_names:
            logger.info(f"    - {name}")
        logger.info("")
    
    # Step 7: Save big tech analysis
    bigtech_summary = pl.DataFrame(company_counts)
    bigtech_summary.write_csv(OUTPUT_BIGTECH)
    logger.info(f"  Big tech analysis saved to: {OUTPUT_BIGTECH}")
    logger.info("")
    
    # Step 8: Summary
    logger.info("=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total rows in dataset: {len(df):,}")
    logger.info(f"Rows with institution_type='company': {len(company_df):,}")
    logger.info(f"Unique papers with company affiliations: {company_df['arxiv_id'].n_unique():,}")
    logger.info(f"Rows from big tech companies: {len(bigtech_df):,}")
    logger.info(f"Unique papers from big tech: {bigtech_df['arxiv_id'].n_unique():,}")
    logger.info("")
    
    total_bigtech_papers = sum(c['unique_papers'] for c in company_counts)
    logger.info(f"Total unique papers from big tech (sum): {total_bigtech_papers:,}")
    logger.info("")
    logger.info("=" * 80)


def main():
    """Main entry point."""
    try:
        analyze_institution_types()
    except Exception as e:
        logger.error(f"Error during analysis: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
