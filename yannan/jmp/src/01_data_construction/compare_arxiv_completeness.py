"""
Compare Merged ArXiv Dataset with Official ArXiv Statistics

Compares the merged_arxiv_complete dataset against official ArXiv monthly
submission statistics to determine completeness per year.
Acceptable error rate: < 1% per year
"""

import pandas as pd
import polars as pl
from pathlib import Path
import logging

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
OUTPUT_DIR = PROJECT_ROOT / "output" / "tables"

ARXIV_OFFICIAL = DATA_RAW / "arxiv_monthly_submissions.csv"
YOUR_DATASET = OUTPUT_DIR / "merged_arxiv_yearly_stats.csv"
OUTPUT_COMPARISON = OUTPUT_DIR / "arxiv_completeness_comparison.csv"
OUTPUT_REPORT = OUTPUT_DIR / "arxiv_completeness_report.md"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "logs" / "compare_arxiv_completeness.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Acceptable error rate (1%)
ACCEPTABLE_ERROR_RATE = 1.0


def aggregate_monthly_to_yearly(monthly_df):
    """Aggregate monthly submission data to yearly totals."""
    # Extract year from month column (format: YYYY-MM)
    monthly_df = monthly_df.copy()
    monthly_df['year'] = monthly_df['month'].str[:4].astype(int)
    
    # Sum submissions by year
    yearly = monthly_df.groupby('year')['submissions'].sum().reset_index()
    yearly.columns = ['year', 'arxiv_official']
    
    return yearly


def compare_datasets():
    """
    Compare merged dataset with official ArXiv statistics.
    """
    logger.info("=" * 80)
    logger.info("COMPARING MERGED ARXIV DATASET WITH OFFICIAL STATISTICS")
    logger.info("=" * 80)
    logger.info("")
    
    # Step 1: Load official ArXiv monthly data
    logger.info("Step 1: Loading official ArXiv monthly submission data...")
    if not ARXIV_OFFICIAL.exists():
        logger.error(f"Official ArXiv data not found: {ARXIV_OFFICIAL}")
        return
    
    monthly_df = pd.read_csv(ARXIV_OFFICIAL)
    logger.info(f"  Loaded {len(monthly_df)} monthly records")
    logger.info(f"  Date range: {monthly_df['month'].min()} to {monthly_df['month'].max()}")
    
    # Aggregate to yearly
    official_yearly = aggregate_monthly_to_yearly(monthly_df)
    logger.info(f"  Aggregated to {len(official_yearly)} years")
    logger.info("")
    
    # Step 2: Load your dataset yearly stats
    logger.info("Step 2: Loading merged dataset yearly statistics...")
    if not YOUR_DATASET.exists():
        logger.error(f"Merged dataset stats not found: {YOUR_DATASET}")
        return
    
    your_yearly = pd.read_csv(YOUR_DATASET)
    logger.info(f"  Loaded {len(your_yearly)} years")
    logger.info("")
    
    # Step 3: Merge and compare
    logger.info("Step 3: Comparing datasets...")
    
    comparison = pd.merge(
        your_yearly,
        official_yearly,
        on='year',
        how='outer',
        suffixes=('_your_dataset', '_arxiv_official')
    )
    
    # Fill missing values with 0 for comparison
    comparison['paper_count'] = comparison['paper_count'].fillna(0)
    comparison['arxiv_official'] = comparison['arxiv_official'].fillna(0)
    
    # Calculate differences
    comparison['difference'] = comparison['paper_count'] - comparison['arxiv_official']
    comparison['difference_abs'] = comparison['difference'].abs()
    
    # Calculate percentage difference
    # Avoid division by zero
    comparison['pct_difference'] = comparison.apply(
        lambda row: (row['difference'] / row['arxiv_official'] * 100) 
        if row['arxiv_official'] > 0 else None,
        axis=1
    )
    comparison['pct_difference_abs'] = comparison['pct_difference'].abs()
    
    # Determine if within acceptable range
    comparison['within_acceptable'] = (
        (comparison['pct_difference_abs'] <= ACCEPTABLE_ERROR_RATE) |
        (comparison['arxiv_official'] == 0)  # No official data available
    )
    
    # Sort by year
    comparison = comparison.sort_values('year').reset_index(drop=True)
    
    logger.info(f"  Compared {len(comparison)} years")
    logger.info("")
    
    # Step 4: Analyze results
    logger.info("Step 4: Analyzing completeness...")
    
    # Years with official data
    years_with_official = comparison[comparison['arxiv_official'] > 0]
    total_years = len(years_with_official)
    
    # Years within acceptable range
    acceptable_years = years_with_official[years_with_official['within_acceptable']]
    num_acceptable = len(acceptable_years)
    
    # Years exceeding acceptable range
    exceeding_years = years_with_official[~years_with_official['within_acceptable']]
    num_exceeding = len(exceeding_years)
    
    logger.info(f"  Total years with official data: {total_years}")
    logger.info(f"  Years within acceptable range (≤{ACCEPTABLE_ERROR_RATE}%): {num_acceptable} ({num_acceptable/total_years*100:.1f}%)")
    logger.info(f"  Years exceeding acceptable range (>1%): {num_exceeding} ({num_exceeding/total_years*100:.1f}%)")
    logger.info("")
    
    # Step 5: Detailed statistics
    logger.info("Step 5: Detailed statistics...")
    
    if len(years_with_official) > 0:
        total_official = years_with_official['arxiv_official'].sum()
        total_your = years_with_official['paper_count'].sum()
        overall_pct = (total_your / total_official * 100) if total_official > 0 else 0
        
        logger.info(f"  Total official submissions: {total_official:,}")
        logger.info(f"  Total in your dataset: {total_your:,}")
        logger.info(f"  Overall coverage: {overall_pct:.2f}%")
        logger.info(f"  Overall difference: {total_your - total_official:,} papers")
        logger.info("")
        
        # Show years exceeding acceptable range
        if num_exceeding > 0:
            logger.info("  Years exceeding 1% threshold:")
            for _, row in exceeding_years.iterrows():
                logger.info(f"    {int(row['year'])}: "
                          f"Your={row['paper_count']:,.0f}, "
                          f"Official={row['arxiv_official']:,.0f}, "
                          f"Diff={row['difference']:+,.0f} "
                          f"({row['pct_difference']:+.2f}%)")
            logger.info("")
    
    # Step 6: Save results
    logger.info("Step 6: Saving comparison results...")
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save CSV
    comparison.to_csv(OUTPUT_COMPARISON, index=False)
    logger.info(f"  Comparison CSV saved to: {OUTPUT_COMPARISON}")
    
    # Generate markdown report
    generate_report(comparison, years_with_official, acceptable_years, exceeding_years)
    logger.info(f"  Report saved to: {OUTPUT_REPORT}")
    logger.info("")
    
    # Step 7: Summary
    logger.info("=" * 80)
    logger.info("COMPARISON SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total years compared: {total_years}")
    logger.info(f"Years within acceptable range (≤{ACCEPTABLE_ERROR_RATE}%): {num_acceptable}/{total_years} ({num_acceptable/total_years*100:.1f}%)")
    logger.info(f"Years exceeding acceptable range: {num_exceeding}/{total_years} ({num_exceeding/total_years*100:.1f}%)")
    
    if len(years_with_official) > 0:
        logger.info(f"Overall coverage: {overall_pct:.2f}%")
    
    if num_exceeding == 0:
        logger.info("")
        logger.info("✓ SUCCESS: All years are within acceptable range!")
    else:
        logger.info("")
        logger.info(f"⚠ WARNING: {num_exceeding} years exceed the acceptable error rate")
    
    logger.info("=" * 80)


def generate_report(comparison, years_with_official, acceptable_years, exceeding_years):
    """Generate a detailed markdown report."""
    
    total_official = years_with_official['arxiv_official'].sum()
    total_your = years_with_official['paper_count'].sum()
    overall_pct = (total_your / total_official * 100) if total_official > 0 else 0
    
    report = f"""# ArXiv Dataset Completeness Comparison Report

**Generated**: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

## Executive Summary

- **Total years compared**: {len(years_with_official)}
- **Years within acceptable range (≤{ACCEPTABLE_ERROR_RATE}%)**: {len(acceptable_years)} ({len(acceptable_years)/len(years_with_official)*100:.1f}%)
- **Years exceeding acceptable range**: {len(exceeding_years)} ({len(exceeding_years)/len(years_with_official)*100:.1f}%)
- **Overall coverage**: {overall_pct:.2f}%
- **Total official submissions**: {total_official:,}
- **Total in merged dataset**: {total_your:,}
- **Overall difference**: {total_your - total_official:+,} papers

## Acceptable Error Rate

The acceptable error rate is set to **{ACCEPTABLE_ERROR_RATE}%** per year. This means for each year, the difference between your dataset and official ArXiv statistics should be less than 1% of the official count.

## Year-by-Year Comparison

| Year | Your Dataset | ArXiv Official | Difference | % Difference | Status |
|------|-------------|----------------|------------|--------------|--------|
"""
    
    for _, row in comparison.iterrows():
        if row['arxiv_official'] > 0:
            status = "✓ Acceptable" if row['within_acceptable'] else "⚠ Exceeds 1%"
            pct = row['pct_difference']
            report += f"| {int(row['year'])} | {row['paper_count']:,.0f} | {row['arxiv_official']:,.0f} | {row['difference']:+,.0f} | {pct:+.2f}% | {status} |\n"
        else:
            report += f"| {int(row['year'])} | {row['paper_count']:,.0f} | N/A | N/A | N/A | No official data |\n"
    
    report += f"""
## Years Exceeding Acceptable Range

"""
    
    if len(exceeding_years) > 0:
        report += f"**{len(exceeding_years)} years exceed the {ACCEPTABLE_ERROR_RATE}% threshold:**\n\n"
        report += "| Year | Your Dataset | ArXiv Official | Difference | % Difference |\n"
        report += "|------|-------------|----------------|------------|--------------|\n"
        for _, row in exceeding_years.iterrows():
            report += f"| {int(row['year'])} | {row['paper_count']:,.0f} | {row['arxiv_official']:,.0f} | {row['difference']:+,.0f} | {row['pct_difference']:+.2f}% |\n"
    else:
        report += "**All years are within the acceptable range!** ✓\n"
    
    report += f"""
## Years Within Acceptable Range

**{len(acceptable_years)} years are within the {ACCEPTABLE_ERROR_RATE}% threshold.**

## Overall Assessment

"""
    
    if len(exceeding_years) == 0:
        report += "**✓ PASS**: All years with official data are within the acceptable error rate of 1%.\n\n"
        report += "Your merged ArXiv dataset appears to be **complete** based on the official ArXiv statistics.\n"
    else:
        report += f"**⚠ PARTIAL**: {len(exceeding_years)} years exceed the acceptable error rate.\n\n"
        report += "Your merged ArXiv dataset may have gaps or inconsistencies in the following years:\n"
        for _, row in exceeding_years.iterrows():
            report += f"- **{int(row['year'])}**: {row['pct_difference']:+.2f}% difference ({row['difference']:+,.0f} papers)\n"
    
    report += f"""
## Notes

- Years with no official data (e.g., 1988-1990) are excluded from the completeness assessment
- The comparison is based on yearly totals aggregated from monthly submission statistics
- Differences may be due to:
  - Data collection timing differences
  - Paper versioning (multiple versions of same paper)
  - Withdrawn papers
  - Data source coverage differences

## Data Sources

- **Official ArXiv Statistics**: `{ARXIV_OFFICIAL}`
- **Merged Dataset Statistics**: `{YOUR_DATASET}`
- **Comparison Results**: `{OUTPUT_COMPARISON}`
"""
    
    with open(OUTPUT_REPORT, 'w') as f:
        f.write(report)


def main():
    """Main entry point."""
    try:
        compare_datasets()
    except Exception as e:
        logger.error(f"Error during comparison: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
