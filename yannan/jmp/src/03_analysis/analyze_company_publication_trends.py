"""
Analyze Company Publication Trends Over Time

Main analysis: Analyzes publication distribution for ALL companies over time,
with focus on detecting post-2020 decline patterns.

Optional subset analysis: Can analyze Big Tech firms specifically (controlled by arguments).

Key analyses:
1. Overall company publication trends by year
2. Detection of post-2020 decline patterns
3. Top companies by publication volume
4. Big Tech subset analysis (optional)
5. Comparison with patent trends (optional)

Date: 2025
"""

import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import argparse
from pathlib import Path
import logging
from datetime import datetime

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
OUTPUT_FIGURES = PROJECT_ROOT / "output" / "figures"
OUTPUT_TABLES = PROJECT_ROOT / "output" / "tables"

# Input files
ARXIV_INSTITUTIONS_PATH = DATA_PROCESSED / "arxiv_paper_institutions.parquet"
PATENTS_PANEL_PATH = DATA_INTERIM / "patents_panel.parquet"

# Ensure output directories exist
OUTPUT_FIGURES.mkdir(parents=True, exist_ok=True)
OUTPUT_TABLES.mkdir(parents=True, exist_ok=True)

# Set plotting style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("Set2")

# Default analysis period
DEFAULT_START_YEAR = 2010
DEFAULT_END_YEAR = 2024
DECLINE_CHECK_YEAR = 2020  # Check for decline after this year

# Big Tech firm matching patterns (for subset analysis)
BIG_TECH_PATTERNS = {
    'GOOGLE': ['google', 'alphabet', 'google research', 'deepmind', 'waymo'],
    'MICROSOFT': ['microsoft', 'microsoft research'],
    'AMAZON': ['amazon', 'amazon web services', 'aws', 'amazon alexa'],
    'APPLE': ['apple', 'apple inc'],
    'META': ['meta platforms', 'facebook', 'facebook ai research', 'meta ai'],
    'OPENAI': ['openai']
}

# Colors for visualization
FIRM_COLORS = {
    'GOOGLE': '#4285F4',
    'MICROSOFT': '#00A4EF',
    'AMAZON': '#FF9900',
    'APPLE': '#A2AAAD',
    'META': '#0081FB',
    'OPENAI': '#10A37F'
}

FIRM_MARKERS = {
    'GOOGLE': 'o',
    'MICROSOFT': 's',
    'AMAZON': '^',
    'APPLE': 'D',
    'META': 'v',
    'OPENAI': 'p'
}

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "logs" / "analyze_company_publication_trends.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# Data Loading and Processing
# ============================================================================

def load_arxiv_data(start_year: int, end_year: int, country_filter: str = None) -> pl.DataFrame:
    """Load and process ArXiv publication data."""
    logger.info("Loading ArXiv publication data...")
    df = pl.read_parquet(ARXIV_INSTITUTIONS_PATH)
    
    # Filter for company institutions
    company_data = df.filter(
        (pl.col('institution_type') == 'company') &
        (pl.col('institution_name').is_not_null()) &
        (pl.col('publication_year').is_not_null()) &
        (pl.col('publication_year') >= start_year) &
        (pl.col('publication_year') <= end_year)
    )
    
    if country_filter:
        company_data = company_data.filter(pl.col('country_code') == country_filter)
    
    logger.info(f"  Loaded {len(company_data):,} company institution records")
    logger.info(f"  Unique companies: {company_data['institution_name'].n_unique():,}")
    logger.info(f"  Unique papers: {company_data['arxiv_id'].n_unique():,}")
    logger.info(f"  Year range: {company_data['publication_year'].min()} to {company_data['publication_year'].max()}")
    
    return company_data


def get_all_company_trends(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate publication trends for ALL companies by year."""
    logger.info("\nCalculating publication trends for all companies...")
    
    # Count unique papers per company per year
    trends = (
        df
        .group_by(['institution_name', 'publication_year'])
        .agg([
            pl.n_unique('arxiv_id').alias('publications')
        ])
        .sort(['institution_name', 'publication_year'])
    )
    
    logger.info(f"  Calculated trends for {trends.select('institution_name').n_unique():,} companies")
    return trends


def get_top_companies(df: pl.DataFrame, top_n: int = 20) -> pl.DataFrame:
    """Get top N companies by total publication count."""
    logger.info(f"\nIdentifying top {top_n} companies by publication volume...")
    
    company_totals = (
        df
        .group_by('institution_name')
        .agg([
            pl.n_unique('arxiv_id').alias('total_papers'),
            pl.col('publication_year').min().alias('first_year'),
            pl.col('publication_year').max().alias('last_year')
        ])
        .sort('total_papers', descending=True)
        .head(top_n)
    )
    
    logger.info("\nTop companies:")
    for row in company_totals.iter_rows(named=True):
        logger.info(f"  {row['institution_name']}: {row['total_papers']:,} papers "
                   f"({int(row['first_year'])}-{int(row['last_year'])})")
    
    return company_totals


def detect_post_2020_decline(trends: pl.DataFrame, decline_year: int = 2020) -> pl.DataFrame:
    """
    Detect companies that show decline in publications after specified year.
    
    Returns companies with:
    - Peak year before or at decline_year
    - Decline in publications after decline_year
    """
    logger.info(f"\nDetecting post-{decline_year} decline patterns...")
    
    # Calculate peak year and post-decline trend for each company
    decline_analysis = []
    
    for company in trends['institution_name'].unique():
        company_trends = trends.filter(pl.col('institution_name') == company).sort('publication_year')
        
        # Find peak year
        peak_row = company_trends.sort('publications', descending=True).head(1)
        if len(peak_row) == 0:
            continue
        
        peak_year = peak_row['publication_year'].item()
        peak_publications = peak_row['publications'].item()
        
        # Get pre-decline average (years before decline_year)
        pre_decline = company_trends.filter(pl.col('publication_year') <= decline_year)
        post_decline = company_trends.filter(pl.col('publication_year') > decline_year)
        
        if len(pre_decline) == 0 or len(post_decline) == 0:
            continue
        
        pre_avg = pre_decline['publications'].mean()
        post_avg = post_decline['publications'].mean()
        
        # Calculate decline percentage
        if pre_avg > 0:
            decline_pct = ((post_avg - pre_avg) / pre_avg) * 100
        else:
            decline_pct = 0.0
        
        # Check if peak was at or before decline_year
        peak_before_decline = peak_year <= decline_year
        
        # Check if there's a decline
        has_decline = post_avg < pre_avg
        
        decline_analysis.append({
            'institution_name': company,
            'total_papers': company_trends['publications'].sum(),
            'peak_year': int(peak_year),
            'peak_publications': peak_publications,
            'pre_decline_avg': round(pre_avg, 2),
            'post_decline_avg': round(post_avg, 2),
            'decline_pct': round(decline_pct, 2),
            'peak_before_decline': peak_before_decline,
            'has_decline': has_decline
        })
    
    decline_df = pl.DataFrame(decline_analysis)
    
    # Filter for companies with decline
    declining_companies = decline_df.filter(
        (pl.col('has_decline') == True) &
        (pl.col('peak_before_decline') == True)
    ).sort('decline_pct')
    
    logger.info(f"\nCompanies with post-{decline_year} decline:")
    logger.info(f"  Total companies analyzed: {len(decline_df):,}")
    logger.info(f"  Companies with decline: {len(declining_companies):,} "
               f"({len(declining_companies)/len(decline_df)*100:.1f}%)")
    
    # Show top decliners
    top_decliners = declining_companies.sort('decline_pct').head(20)
    logger.info("\nTop 20 companies by decline percentage:")
    for row in top_decliners.iter_rows(named=True):
        logger.info(f"  {row['institution_name']}: "
                   f"{row['decline_pct']:.1f}% decline "
                   f"(peak: {row['peak_year']}, {row['peak_publications']} papers)")
    
    return decline_df, declining_companies


def identify_big_tech_companies(df: pl.DataFrame) -> pl.DataFrame:
    """Identify Big Tech firms in ArXiv data (for subset analysis)."""
    logger.info("\nIdentifying Big Tech firms...")
    
    # Create big_tech_firm column
    df = df.with_columns([
        pl.when(
            (pl.col('institution_name').str.to_lowercase().str.contains('google', literal=False)) |
            (pl.col('institution_name').str.to_lowercase().str.contains('alphabet', literal=False)) |
            (pl.col('institution_name').str.to_lowercase().str.contains('deepmind', literal=False)) |
            (pl.col('institution_name').str.to_lowercase().str.contains('waymo', literal=False))
        ).then(pl.lit('GOOGLE'))
        .when(
            (pl.col('institution_name').str.to_lowercase().str.contains('microsoft', literal=False)) &
            (~pl.col('institution_name').str.to_lowercase().str.contains('micromet', literal=False))
        ).then(pl.lit('MICROSOFT'))
        .when(
            (pl.col('institution_name').str.to_lowercase().str.contains('amazon', literal=False)) |
            (pl.col('institution_name').str.to_lowercase().str.contains('amazon web services', literal=False)) |
            (pl.col('institution_name').str.to_lowercase().str.contains(' aws ', literal=False))
        ).then(pl.lit('AMAZON'))
        .when(
            (pl.col('institution_name').str.to_lowercase().str.contains('apple', literal=False)) &
            (~pl.col('institution_name').str.to_lowercase().str.contains('pineapple', literal=False))
        ).then(pl.lit('APPLE'))
        .when(
            (
                (pl.col('institution_name').str.to_lowercase().str.contains('meta platforms', literal=False)) |
                (pl.col('institution_name').str.to_lowercase().str.contains('facebook', literal=False)) |
                (
                    (pl.col('institution_name').str.to_lowercase().str.contains('meta', literal=False)) &
                    (pl.col('institution_name').str.to_lowercase().str.contains('platforms', literal=False))
                )
            ) &
            (~pl.col('institution_name').str.to_lowercase().str.contains('metacomp', literal=False)) &
            (~pl.col('institution_name').str.to_lowercase().str.contains('metametrics', literal=False)) &
            (~pl.col('institution_name').str.to_lowercase().str.contains('metamateria', literal=False))
        ).then(pl.lit('META'))
        .when(
            pl.col('institution_name').str.to_lowercase().str.contains('openai', literal=False)
        ).then(pl.lit('OPENAI'))
        .otherwise(pl.lit(None))
        .alias('big_tech_firm')
    ])
    
    big_tech_data = df.filter(pl.col('big_tech_firm').is_not_null())
    
    logger.info(f"  Found {len(big_tech_data):,} Big Tech institution records")
    logger.info(f"  Unique Big Tech papers: {big_tech_data['arxiv_id'].n_unique():,}")
    
    # Show breakdown
    firm_counts = big_tech_data.group_by('big_tech_firm').agg([
        pl.len().alias('records'),
        pl.n_unique('arxiv_id').alias('unique_papers')
    ]).sort('records', descending=True)
    
    logger.info("\nBig Tech firms:")
    for row in firm_counts.iter_rows(named=True):
        logger.info(f"  {row['big_tech_firm']}: {row['unique_papers']:,} papers")
    
    return big_tech_data


def get_big_tech_trends(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate Big Tech publication trends by firm and year."""
    trends = (
        df
        .group_by(['big_tech_firm', 'publication_year'])
        .agg([
            pl.n_unique('arxiv_id').alias('publications')
        ])
        .sort(['big_tech_firm', 'publication_year'])
    )
    return trends


# ============================================================================
# Visualization
# ============================================================================

def create_all_companies_trend_figure(trends: pl.DataFrame, top_n: int = 20):
    """Create figure showing trends for top N companies."""
    logger.info(f"\nGenerating trends figure for top {top_n} companies...")
    
    # Get top companies
    top_companies = (
        trends
        .group_by('institution_name')
        .agg(pl.col('publications').sum())
        .sort('publications', descending=True)
        .head(top_n)
    )
    
    top_company_names = top_companies['institution_name'].to_list()
    
    # Filter trends for top companies
    top_trends = trends.filter(pl.col('institution_name').is_in(top_company_names))
    
    # Convert to pandas for plotting
    trends_pd = top_trends.to_pandas()
    
    fig, ax = plt.subplots(figsize=(16, 10))
    
    # Plot each company
    for company in top_company_names:
        company_data = trends_pd[trends_pd['institution_name'] == company]
        if len(company_data) > 0:
            ax.plot(company_data['publication_year'], company_data['publications'],
                   marker='o', linewidth=2, markersize=6, label=company, alpha=0.7)
    
    # Add vertical line at 2020
    ax.axvline(x=DECLINE_CHECK_YEAR, color='red', linestyle='--', alpha=0.5, linewidth=2,
               label=f'Post-{DECLINE_CHECK_YEAR} Period')
    
    ax.set_xlabel('Year', fontsize=13, fontweight='bold')
    ax.set_ylabel('ArXiv Publications', fontsize=13, fontweight='bold')
    ax.set_title(f'Top {top_n} Companies: ArXiv Publication Trends Over Time',
                 fontsize=15, fontweight='bold', pad=15)
    ax.legend(loc='best', fontsize=9, framealpha=0.9, ncol=2)
    ax.grid(True, alpha=0.3, linestyle='--')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / f"top_{top_n}_companies_publication_trends.png", 
                dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"  Saved {OUTPUT_FIGURES / f'top_{top_n}_companies_publication_trends.png'}")


def create_decline_analysis_figure(decline_df: pl.DataFrame, declining_companies: pl.DataFrame):
    """Create figure showing decline patterns."""
    logger.info("\nGenerating decline analysis figure...")
    
    # Get top decliners for visualization
    top_decliners = declining_companies.sort('decline_pct').head(15)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))
    
    # Left: Decline percentage distribution
    decline_pcts = declining_companies['decline_pct'].to_list()
    ax1.hist(decline_pcts, bins=30, edgecolor='black', alpha=0.7, color='steelblue')
    ax1.set_xlabel('Decline Percentage (%)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Number of Companies', fontsize=12, fontweight='bold')
    ax1.set_title('Distribution of Post-2020 Decline Percentages', fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3, linestyle='--')
    
    # Right: Top decliners
    top_decliners_pd = top_decliners.to_pandas()
    y_pos = np.arange(len(top_decliners_pd))
    ax2.barh(y_pos, top_decliners_pd['decline_pct'], alpha=0.7, color='crimson')
    ax2.set_yticks(y_pos)
    ax2.set_yticklabels([name[:40] + '...' if len(name) > 40 else name 
                         for name in top_decliners_pd['institution_name']], fontsize=9)
    ax2.set_xlabel('Decline Percentage (%)', fontsize=12, fontweight='bold')
    ax2.set_title('Top 15 Companies by Decline Percentage', fontsize=13, fontweight='bold')
    ax2.grid(True, alpha=0.3, linestyle='--', axis='x')
    
    plt.suptitle('Post-2020 Publication Decline Analysis', fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "post_2020_decline_analysis.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"  Saved {OUTPUT_FIGURES / 'post_2020_decline_analysis.png'}")


def create_big_tech_trends_figure(big_tech_trends: pl.DataFrame):
    """Create figure showing Big Tech publication trends."""
    logger.info("\nGenerating Big Tech trends figure...")
    
    big_tech_pd = big_tech_trends.to_pandas()
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    firms = ['GOOGLE', 'MICROSOFT', 'AMAZON', 'APPLE', 'META', 'OPENAI']
    
    for firm in firms:
        firm_data = big_tech_pd[big_tech_pd['big_tech_firm'] == firm]
        if len(firm_data) > 0:
            ax.plot(firm_data['publication_year'], firm_data['publications'],
                   marker=FIRM_MARKERS.get(firm, 'o'), linewidth=2.5, markersize=8,
                   label=firm, color=FIRM_COLORS.get(firm, 'gray'), alpha=0.8)
    
    # Add vertical line at 2020
    ax.axvline(x=DECLINE_CHECK_YEAR, color='red', linestyle='--', alpha=0.5, linewidth=2,
               label=f'Post-{DECLINE_CHECK_YEAR} Period')
    
    ax.set_xlabel('Year', fontsize=13, fontweight='bold')
    ax.set_ylabel('ArXiv Publications', fontsize=13, fontweight='bold')
    ax.set_title('Big Tech ArXiv Publication Trends',
                 fontsize=15, fontweight='bold', pad=15)
    ax.legend(loc='best', fontsize=11, framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle='--')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "big_tech_publication_trends.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"  Saved {OUTPUT_FIGURES / 'big_tech_publication_trends.png'}")


def create_aggregate_trend_figure(trends: pl.DataFrame):
    """Create figure showing aggregate publication trends (all companies combined)."""
    logger.info("\nGenerating aggregate trends figure...")
    
    # Aggregate all companies by year
    aggregate = (
        trends
        .group_by('publication_year')
        .agg([
            pl.col('publications').sum().alias('total_publications'),
            pl.col('institution_name').n_unique().alias('num_companies')
        ])
        .sort('publication_year')
    )
    
    aggregate_pd = aggregate.to_pandas()
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # Top: Total publications
    ax1.plot(aggregate_pd['publication_year'], aggregate_pd['total_publications'],
            marker='o', linewidth=3, markersize=10, color='steelblue', alpha=0.8)
    ax1.axvline(x=DECLINE_CHECK_YEAR, color='red', linestyle='--', alpha=0.5, linewidth=2)
    ax1.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Total Publications', fontsize=12, fontweight='bold')
    ax1.set_title('Aggregate Company Publications Over Time', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3, linestyle='--')
    
    # Bottom: Number of companies publishing
    ax2.plot(aggregate_pd['publication_year'], aggregate_pd['num_companies'],
            marker='s', linewidth=3, markersize=10, color='darkgreen', alpha=0.8)
    ax2.axvline(x=DECLINE_CHECK_YEAR, color='red', linestyle='--', alpha=0.5, linewidth=2)
    ax2.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Number of Companies', fontsize=12, fontweight='bold')
    ax2.set_title('Number of Companies Publishing Each Year', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3, linestyle='--')
    
    plt.suptitle('Overall Company Publication Trends', fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "aggregate_company_publication_trends.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"  Saved {OUTPUT_FIGURES / 'aggregate_company_publication_trends.png'}")


# ============================================================================
# Main Analysis
# ============================================================================

def main_analysis(start_year: int, end_year: int, country_filter: str = None, 
                  analyze_big_tech: bool = False, top_n_companies: int = 20):
    """
    Main analysis pipeline for all company publications.
    
    Args:
        start_year: Start year for analysis
        end_year: End year for analysis
        country_filter: Optional country code filter (e.g., 'US')
        analyze_big_tech: Whether to include Big Tech subset analysis
        top_n_companies: Number of top companies to visualize
    """
    logger.info("=" * 80)
    logger.info("COMPANY PUBLICATION TRENDS ANALYSIS")
    logger.info("=" * 80)
    logger.info(f"Analysis period: {start_year}-{end_year}")
    logger.info(f"Country filter: {country_filter if country_filter else 'All countries'}")
    logger.info(f"Big Tech analysis: {'Yes' if analyze_big_tech else 'No'}")
    logger.info("")
    
    # Step 1: Load data
    company_data = load_arxiv_data(start_year, end_year, country_filter)
    
    # Step 2: Calculate trends for all companies
    all_trends = get_all_company_trends(company_data)
    
    # Step 3: Get top companies
    top_companies = get_top_companies(company_data, top_n_companies)
    
    # Step 4: Detect post-2020 decline
    decline_df, declining_companies = detect_post_2020_decline(all_trends, DECLINE_CHECK_YEAR)
    
    # Step 5: Create visualizations
    create_aggregate_trend_figure(all_trends)
    create_all_companies_trend_figure(all_trends, top_n_companies)
    create_decline_analysis_figure(decline_df, declining_companies)
    
    # Step 6: Save summary tables
    logger.info("\nSaving summary tables...")
    
    # Top companies
    top_companies.write_csv(OUTPUT_TABLES / "top_companies_by_publications.csv")
    logger.info(f"  Saved {OUTPUT_TABLES / 'top_companies_by_publications.csv'}")
    
    # Decline analysis
    decline_df.write_csv(OUTPUT_TABLES / "company_decline_analysis.csv")
    declining_companies.write_csv(OUTPUT_TABLES / "declining_companies_post_2020.csv")
    logger.info(f"  Saved decline analysis tables")
    
    # All trends (sample of top companies)
    top_company_names = top_companies['institution_name'].head(50).to_list()
    top_trends = all_trends.filter(pl.col('institution_name').is_in(top_company_names))
    top_trends.write_csv(OUTPUT_TABLES / "top_companies_yearly_trends.csv")
    logger.info(f"  Saved {OUTPUT_TABLES / 'top_companies_yearly_trends.csv'}")
    
    # Step 7: Big Tech subset analysis (if requested)
    if analyze_big_tech:
        logger.info("\n" + "=" * 80)
        logger.info("BIG TECH SUBSET ANALYSIS")
        logger.info("=" * 80)
        
        big_tech_data = identify_big_tech_companies(company_data)
        big_tech_trends = get_big_tech_trends(big_tech_data)
        
        # Check Big Tech decline
        big_tech_decline, big_tech_declining = detect_post_2020_decline(
            big_tech_trends.rename({'big_tech_firm': 'institution_name'}),
            DECLINE_CHECK_YEAR
        )
        
        # Create Big Tech visualization
        create_big_tech_trends_figure(big_tech_trends)
        
        # Save Big Tech tables
        big_tech_trends.write_csv(OUTPUT_TABLES / "big_tech_yearly_trends.csv")
        logger.info(f"  Saved {OUTPUT_TABLES / 'big_tech_yearly_trends.csv'}")
    
    # Step 8: Summary statistics
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY STATISTICS")
    logger.info("=" * 80)
    
    total_companies = all_trends['institution_name'].n_unique()
    total_papers = company_data['arxiv_id'].n_unique()
    declining_count = len(declining_companies)
    declining_pct = (declining_count / total_companies * 100) if total_companies > 0 else 0
    
    logger.info(f"Total companies analyzed: {total_companies:,}")
    logger.info(f"Total unique papers: {total_papers:,}")
    logger.info(f"Companies with post-{DECLINE_CHECK_YEAR} decline: {declining_count:,} ({declining_pct:.1f}%)")
    
    # Check if ALL top companies declined
    top_company_names_list = top_companies['institution_name'].to_list()
    top_declining = declining_companies.filter(
        pl.col('institution_name').is_in(top_company_names_list)
    )
    
    logger.info(f"\nTop {top_n_companies} companies:")
    logger.info(f"  Companies with decline: {len(top_declining):,} / {len(top_company_names_list)}")
    
    if len(top_declining) == len(top_company_names_list):
        logger.info(f"\n⚠️  ALL top {top_n_companies} companies show post-{DECLINE_CHECK_YEAR} decline!")
    elif len(top_declining) / len(top_company_names_list) >= 0.8:
        logger.info(f"\n⚠️  {len(top_declining)/len(top_company_names_list)*100:.0f}% of top companies show decline")
    else:
        logger.info(f"\n✓ Not all top companies show decline")
    
    logger.info("\n" + "=" * 80)
    logger.info("ANALYSIS COMPLETE")
    logger.info("=" * 80)


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Analyze company publication trends over time",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze all companies (default)
  python analyze_company_publication_trends.py
  
  # Analyze only US companies
  python analyze_company_publication_trends.py --country US
  
  # Include Big Tech subset analysis
  python analyze_company_publication_trends.py --big-tech
  
  # Custom year range
  python analyze_company_publication_trends.py --start-year 2015 --end-year 2024
        """
    )
    
    parser.add_argument(
        '--start-year',
        type=int,
        default=DEFAULT_START_YEAR,
        help=f'Start year for analysis (default: {DEFAULT_START_YEAR})'
    )
    
    parser.add_argument(
        '--end-year',
        type=int,
        default=DEFAULT_END_YEAR,
        help=f'End year for analysis (default: {DEFAULT_END_YEAR})'
    )
    
    parser.add_argument(
        '--country',
        type=str,
        default=None,
        help='Filter by country code (e.g., US, UK, CN). If not specified, analyzes all countries.'
    )
    
    parser.add_argument(
        '--big-tech',
        action='store_true',
        help='Include Big Tech subset analysis'
    )
    
    parser.add_argument(
        '--top-n',
        type=int,
        default=20,
        help='Number of top companies to visualize (default: 20)'
    )
    
    args = parser.parse_args()
    
    try:
        main_analysis(
            start_year=args.start_year,
            end_year=args.end_year,
            country_filter=args.country,
            analyze_big_tech=args.big_tech,
            top_n_companies=args.top_n
        )
    except Exception as e:
        logger.error(f"Error during analysis: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
