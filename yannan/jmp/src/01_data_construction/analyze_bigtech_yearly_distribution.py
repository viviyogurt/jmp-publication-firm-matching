"""
Analyze Year-over-Year Distribution of Big Tech Papers

For the 3,111 papers from big tech companies, generate:
- Distribution by year and company
- Total papers per year per company
"""

import polars as pl
from pathlib import Path
import logging
from datetime import datetime

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
OUTPUT_DIR = PROJECT_ROOT / "output" / "tables"

INPUT_FILE = DATA_PROCESSED / "arxiv_paper_institutions.parquet"
OUTPUT_CSV = OUTPUT_DIR / "bigtech_yearly_distribution.csv"
OUTPUT_PIVOT = OUTPUT_DIR / "bigtech_yearly_distribution_pivot.csv"
OUTPUT_SUMMARY = OUTPUT_DIR / "bigtech_yearly_summary.md"

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
        logging.FileHandler(PROJECT_ROOT / "logs" / "analyze_bigtech_yearly_distribution.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def identify_bigtech_company(institution_name):
    """Identify which big tech company an institution belongs to."""
    if not institution_name:
        return None
    
    name_upper = str(institution_name).upper()
    
    # Check each company (order matters - check more specific first)
    if "MICROSOFT" in name_upper:
        return "MICROSOFT"
    elif "FACEBOOK" in name_upper:
        return "META"
    elif name_upper.startswith("META ") or name_upper == "META" or name_upper.endswith(" META"):
        # Exclude false positives
        if any(x in name_upper for x in ["METAL", "METAMATERIAL", "METALL", "METRIC", 
                                          "KENNAMETAL", "SUMITOMO", "PRIMETALS", "RHEINMETALL",
                                          "METABOLIC", "MIMETAS", "METASENSING", "METAMATERIA", "META VISION"]):
            return None
        return "META"
    elif "AMAZON" in name_upper:
        return "AMAZON"
    elif "GOOGLE" in name_upper:
        return "GOOGLE"
    elif "APPLE" in name_upper:
        return "APPLE"
    
    return None


def analyze_bigtech_yearly_distribution():
    """
    Analyze year-over-year distribution of big tech papers.
    """
    logger.info("=" * 80)
    logger.info("ANALYZING YEAR-OVER-YEAR DISTRIBUTION OF BIG TECH PAPERS")
    logger.info("=" * 80)
    logger.info("")
    
    # Step 1: Load data
    logger.info("Step 1: Loading arxiv_paper_institutions.parquet...")
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        return
    
    df = pl.read_parquet(INPUT_FILE)
    logger.info(f"  Loaded {len(df):,} rows")
    logger.info("")
    
    # Step 2: Filter for company type
    logger.info("Step 2: Filtering for company institution type...")
    company_df = df.filter(pl.col("institution_type") == "company")
    logger.info(f"  Rows with institution_type='company': {len(company_df):,}")
    logger.info("")
    
    # Step 3: Identify big tech companies
    logger.info("Step 3: Identifying big tech companies...")
    
    # Add big tech company identifier
    company_df = company_df.with_columns([
        pl.col("institution_name")
        .map_batches(
            lambda s: pl.Series([identify_bigtech_company(x) for x in s]),
            return_dtype=pl.Utf8
        )
        .alias("bigtech_company")
    ])
    
    # Filter for big tech only
    bigtech_df = company_df.filter(pl.col("bigtech_company").is_not_null())
    logger.info(f"  Rows from big tech companies: {len(bigtech_df):,}")
    logger.info(f"  Unique papers from big tech: {bigtech_df['arxiv_id'].n_unique():,}")
    logger.info("")
    
    # Step 4: Extract publication year
    logger.info("Step 4: Extracting publication year...")
    
    # Use publication_year if available, otherwise extract from published date
    bigtech_df = bigtech_df.with_columns([
        pl.when(pl.col("publication_year").is_not_null())
        .then(pl.col("publication_year").cast(pl.Int64))
        .otherwise(
            pl.col("published").dt.year()
        )
        .alias("year")
    ])
    
    # Filter out null years
    bigtech_df = bigtech_df.filter(pl.col("year").is_not_null())
    logger.info(f"  Rows with valid year: {len(bigtech_df):,}")
    logger.info(f"  Year range: {bigtech_df['year'].min()} to {bigtech_df['year'].max()}")
    logger.info("")
    
    # Step 5: Count papers by year and company
    logger.info("Step 5: Counting papers by year and company...")
    
    # Count unique papers per year per company
    yearly_dist = (
        bigtech_df
        .group_by(["year", "bigtech_company"])
        .agg([
            pl.col("arxiv_id").n_unique().alias("unique_papers"),
            pl.len().alias("total_rows")  # Author-paper-institution combinations
        ])
        .sort(["year", "bigtech_company"])
    )
    
    logger.info(f"  Generated {len(yearly_dist)} year-company combinations")
    logger.info("")
    
    # Step 6: Create pivot table (years as rows, companies as columns)
    logger.info("Step 6: Creating pivot table...")
    
    # Get all years and companies
    all_years = sorted(bigtech_df['year'].unique().to_list())
    all_companies = sorted(BIG_TECH_COMPANIES)
    
    # Create pivot
    pivot_data = []
    for year in all_years:
        row = {"year": year}
        year_data = yearly_dist.filter(pl.col("year") == year)
        
        for company in all_companies:
            company_data = year_data.filter(pl.col("bigtech_company") == company)
            if len(company_data) > 0:
                row[company] = company_data['unique_papers'].item()
            else:
                row[company] = 0
        
        # Calculate total for the year
        row["TOTAL"] = sum(row[company] for company in all_companies)
        pivot_data.append(row)
    
    pivot_df = pl.DataFrame(pivot_data)
    logger.info(f"  Pivot table created: {len(pivot_df)} years")
    logger.info("")
    
    # Step 7: Summary statistics
    logger.info("Step 7: Calculating summary statistics...")
    
    # Total by company
    company_totals = (
        yearly_dist
        .group_by("bigtech_company")
        .agg([
            pl.col("unique_papers").sum().alias("total_papers"),
            pl.col("year").min().alias("first_year"),
            pl.col("year").max().alias("last_year")
        ])
        .sort("total_papers", descending=True)
    )
    
    logger.info("\nTotal papers by company:")
    for row in company_totals.iter_rows(named=True):
        logger.info(f"  {row['bigtech_company']}: {row['total_papers']:,} papers "
                   f"({row['first_year']}-{row['last_year']})")
    logger.info("")
    
    # Yearly totals
    yearly_totals = (
        yearly_dist
        .group_by("year")
        .agg([
            pl.col("unique_papers").sum().alias("total_papers")
        ])
        .sort("year")
    )
    
    logger.info("Top 10 years by total big tech papers:")
    top_years = yearly_totals.sort("total_papers", descending=True).head(10)
    for row in top_years.iter_rows(named=True):
        logger.info(f"  {int(row['year'])}: {row['total_papers']:,} papers")
    logger.info("")
    
    # Step 8: Save results
    logger.info("Step 8: Saving results...")
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save detailed distribution
    yearly_dist.write_csv(OUTPUT_CSV)
    logger.info(f"  Detailed distribution saved to: {OUTPUT_CSV}")
    
    # Save pivot table
    pivot_df.write_csv(OUTPUT_PIVOT)
    logger.info(f"  Pivot table saved to: {OUTPUT_PIVOT}")
    
    # Generate markdown summary
    generate_summary_report(yearly_dist, pivot_df, company_totals, yearly_totals)
    logger.info(f"  Summary report saved to: {OUTPUT_SUMMARY}")
    logger.info("")
    
    # Step 9: Print sample of pivot table
    logger.info("Step 9: Sample of yearly distribution (first 20 years):")
    logger.info("")
    logger.info(pivot_df.head(20).to_pandas().to_string(index=False))
    logger.info("")
    
    logger.info("=" * 80)
    logger.info("ANALYSIS COMPLETE")
    logger.info("=" * 80)


def generate_summary_report(yearly_dist, pivot_df, company_totals, yearly_totals):
    """Generate a markdown summary report."""
    
    report = f"""# Big Tech Papers Year-over-Year Distribution

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Overview

This report analyzes the distribution of ArXiv papers from big tech companies (Google, Microsoft, Meta, Amazon, Apple) by year.

**Total Unique Papers**: {yearly_dist['unique_papers'].sum():,}

## Total Papers by Company

| Company | Total Papers | First Year | Last Year |
|---------|-------------|------------|-----------|
"""
    
    for row in company_totals.iter_rows(named=True):
        report += f"| {row['bigtech_company']} | {row['total_papers']:,} | {int(row['first_year'])} | {int(row['last_year'])} |\n"
    
    report += f"""
## Yearly Distribution (Pivot Table)

| Year | GOOGLE | MICROSOFT | META | AMAZON | APPLE | TOTAL |
|------|--------|-----------|------|--------|-------|-------|
"""
    
    for row in pivot_df.iter_rows(named=True):
        year = int(row['year'])
        google = row.get('GOOGLE', 0)
        microsoft = row.get('MICROSOFT', 0)
        meta = row.get('META', 0)
        amazon = row.get('AMAZON', 0)
        apple = row.get('APPLE', 0)
        total = row.get('TOTAL', 0)
        report += f"| {year} | {google} | {microsoft} | {meta} | {amazon} | {apple} | {total} |\n"
    
    report += f"""
## Top 10 Years by Total Papers

| Year | Total Papers |
|------|-------------|
"""
    
    top_years = yearly_totals.sort("total_papers", descending=True).head(10)
    for row in top_years.iter_rows(named=True):
        report += f"| {int(row['year'])} | {row['total_papers']:,} |\n"
    
    report += f"""
## Notes

- Counts represent unique papers (deduplicated by arxiv_id)
- A paper may have multiple authors from the same or different big tech companies
- The same paper may appear in multiple rows if it has authors from multiple big tech companies
- Year is extracted from publication_year or published date

## Data Sources

- **Input File**: `{INPUT_FILE}`
- **Detailed Distribution**: `{OUTPUT_CSV}`
- **Pivot Table**: `{OUTPUT_PIVOT}`
"""
    
    with open(OUTPUT_SUMMARY, 'w') as f:
        f.write(report)


def main():
    """Main entry point."""
    try:
        analyze_bigtech_yearly_distribution()
    except Exception as e:
        logger.error(f"Error during analysis: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
