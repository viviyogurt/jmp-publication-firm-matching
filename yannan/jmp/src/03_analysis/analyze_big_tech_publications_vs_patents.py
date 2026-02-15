"""
Compare Big Tech Publication Rates vs Patent Filing Trends

This script analyzes ArXiv publication rates for Big Tech firms (Google, Microsoft,
Amazon, Apple, Meta) and contrasts them with their patent filing trends.

Creates visualizations comparing:
1. Publications per year (ArXiv)
2. Patents per year (USPTO)
3. Combined view showing strategic pivot

Date: 2025
"""

import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

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

# Analysis period
START_YEAR = 2010
END_YEAR = 2024

# Big Tech firm matching patterns (case-insensitive)
# More comprehensive patterns including research divisions
BIG_TECH_PATTERNS = {
    'GOOGLE': ['google', 'alphabet', 'google research', 'deepmind', 'waymo'],
    'MICROSOFT': ['microsoft', 'microsoft research'],
    'AMAZON': ['amazon', 'amazon web services', 'aws', 'amazon alexa'],
    'APPLE': ['apple', 'apple inc'],
    'META': ['meta platforms', 'facebook', 'facebook ai research', 'meta ai'],
    'OPENAI': ['openai']
}

# Colors for each firm (matching patent analysis)
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


# ============================================================================
# Data Loading and Processing
# ============================================================================

def load_arxiv_data() -> pl.DataFrame:
    """Load and process ArXiv publication data."""
    print("Loading ArXiv publication data...")
    df = pl.read_parquet(ARXIV_INSTITUTIONS_PATH)
    
    # Filter for US company institutions
    us_companies = df.filter(
        (pl.col('institution_type') == 'company') &
        (pl.col('country_code') == 'US') &
        (pl.col('institution_name').is_not_null()) &
        (pl.col('publication_year').is_not_null()) &
        (pl.col('publication_year') >= START_YEAR) &
        (pl.col('publication_year') <= END_YEAR)
    )
    
    print(f"  Loaded {len(us_companies):,} US company institution records")
    return us_companies


def identify_big_tech_in_arxiv(df: pl.DataFrame) -> pl.DataFrame:
    """Identify Big Tech firms in ArXiv data using pattern matching."""
    print("\nIdentifying Big Tech firms in ArXiv data...")
    
    # Create big_tech_firm column based on institution name matching
    # Use more precise matching to avoid false positives
    df = df.with_columns([
        pl.when(
            # Google/Alphabet - be careful not to match "googling" or similar
            (pl.col('institution_name').str.to_lowercase().str.contains('google', literal=False)) |
            (pl.col('institution_name').str.to_lowercase().str.contains('alphabet', literal=False)) |
            (pl.col('institution_name').str.to_lowercase().str.contains('deepmind', literal=False)) |
            (pl.col('institution_name').str.to_lowercase().str.contains('waymo', literal=False))
        ).then(pl.lit('GOOGLE'))
        .when(
            # Microsoft - avoid false matches
            (pl.col('institution_name').str.to_lowercase().str.contains('microsoft', literal=False)) &
            (~pl.col('institution_name').str.to_lowercase().str.contains('micromet', literal=False))
        ).then(pl.lit('MICROSOFT'))
        .when(
            # Amazon - be specific to avoid false matches
            (pl.col('institution_name').str.to_lowercase().str.contains('amazon', literal=False)) |
            (pl.col('institution_name').str.to_lowercase().str.contains('amazon web services', literal=False)) |
            (pl.col('institution_name').str.to_lowercase().str.contains(' aws ', literal=False))
        ).then(pl.lit('AMAZON'))
        .when(
            # Apple - be specific
            (pl.col('institution_name').str.to_lowercase().str.contains('apple', literal=False)) &
            (~pl.col('institution_name').str.to_lowercase().str.contains('pineapple', literal=False))
        ).then(pl.lit('APPLE'))
        .when(
            # Meta/Facebook - avoid false positives like "Metacomp", "MetaMetrics"
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
            # OpenAI
            pl.col('institution_name').str.to_lowercase().str.contains('openai', literal=False)
        ).then(pl.lit('OPENAI'))
        .otherwise(pl.lit(None))
        .alias('big_tech_firm')
    ])
    
    # Filter to only Big Tech firms
    big_tech_data = df.filter(pl.col('big_tech_firm').is_not_null())
    
    print(f"  Found {len(big_tech_data):,} Big Tech institution records")
    
    # Show breakdown by firm
    firm_counts = big_tech_data.group_by('big_tech_firm').agg([
        pl.len().alias('records'),
        pl.n_unique('arxiv_id').alias('unique_papers'),
        pl.n_unique('institution_name').alias('unique_institutions')
    ]).sort('records', descending=True)
    
    print("\nBig Tech firms found:")
    print(firm_counts)
    
    # Show institution names for each firm
    print("\nInstitution names by firm:")
    for firm in firm_counts['big_tech_firm'].to_list():
        firm_data = big_tech_data.filter(pl.col('big_tech_firm') == firm)
        inst_names = firm_data.select('institution_name').unique()
        print(f"\n{firm}:")
        for row in inst_names.iter_rows(named=True):
            count = firm_data.filter(pl.col('institution_name') == row['institution_name']).select('arxiv_id').n_unique()
            print(f"  - {row['institution_name']}: {count} papers")
    
    return big_tech_data


def get_arxiv_publication_trends(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate ArXiv publication trends by firm and year."""
    print("\nCalculating ArXiv publication trends...")
    
    # Count unique papers per firm per year
    # (one paper can have multiple institution records, so we need unique papers)
    trends = (
        df
        .group_by(['big_tech_firm', 'publication_year'])
        .agg([
            pl.n_unique('arxiv_id').alias('publications')
        ])
        .sort(['big_tech_firm', 'publication_year'])
    )
    
    print(f"  Calculated trends for {trends.select('big_tech_firm').n_unique()} firms")
    return trends


def load_patent_data() -> pl.DataFrame:
    """Load and process patent data."""
    print("\nLoading patent data...")
    df = pl.read_parquet(PATENTS_PANEL_PATH)
    
    # Filter to analysis period
    df = df.filter(
        (pl.col('year') >= START_YEAR) &
        (pl.col('year') <= END_YEAR)
    )
    
    print(f"  Loaded {len(df):,} patent records ({START_YEAR}-{END_YEAR})")
    return df


def identify_big_tech_in_patents(df: pl.DataFrame) -> pl.DataFrame:
    """Identify Big Tech firms in patent data."""
    print("\nIdentifying Big Tech firms in patent data...")
    
    # Create big_tech_firm column
    df = df.with_columns([
        pl.when(
            pl.col("clean_name").str.contains("GOOGLE", literal=False) |
            pl.col("clean_name").str.contains("ALPHABET", literal=False)
        ).then(pl.lit("GOOGLE"))
        .when(pl.col("clean_name").str.contains("MICROSOFT", literal=False))
        .then(pl.lit("MICROSOFT"))
        .when(pl.col("clean_name").str.contains("AMAZON", literal=False))
        .then(pl.lit("AMAZON"))
        .when(pl.col("clean_name").str.contains("APPLE", literal=False))
        .then(pl.lit("APPLE"))
        .when(
            pl.col("clean_name").str.contains("META", literal=False) |
            pl.col("clean_name").str.contains("FACEBOOK", literal=False)
        ).then(pl.lit("META"))
        .otherwise(pl.lit(None))
        .alias("big_tech_firm")
    ])
    
    # Filter to only Big Tech firms
    big_tech_patents = df.filter(pl.col("big_tech_firm").is_not_null())
    
    print(f"  Found {len(big_tech_patents):,} Big Tech patent records")
    return big_tech_patents


def get_patent_trends(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate patent trends by firm and year."""
    print("\nCalculating patent trends...")
    
    trends = (
        df
        .group_by(['big_tech_firm', 'year'])
        .agg([
            pl.col('total_ai_patents').sum().alias('patents')
        ])
        .sort(['big_tech_firm', 'year'])
    )
    
    return trends


# ============================================================================
# Visualization
# ============================================================================

def create_publication_trends_figure(arxiv_trends: pl.DataFrame):
    """Create figure showing ArXiv publication trends."""
    print("\nGenerating ArXiv publication trends figure...")
    
    arxiv_pd = arxiv_trends.to_pandas()
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    firms = ['GOOGLE', 'MICROSOFT', 'AMAZON', 'APPLE', 'META', 'OPENAI']
    
    for firm in firms:
        firm_data = arxiv_pd[arxiv_pd['big_tech_firm'] == firm]
        if len(firm_data) > 0:
            ax.plot(firm_data['publication_year'], firm_data['publications'],
                   marker=FIRM_MARKERS.get(firm, 'o'), linewidth=2.5, markersize=8,
                   label=firm, color=FIRM_COLORS.get(firm, 'gray'), alpha=0.8)
    
    ax.set_xlabel('Year', fontsize=13, fontweight='bold')
    ax.set_ylabel('ArXiv Publications', fontsize=13, fontweight='bold')
    ax.set_title('Big Tech ArXiv Publication Trends (2010-2024)',
                 fontsize=15, fontweight='bold', pad=15)
    ax.legend(loc='best', fontsize=11, framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_xlim(START_YEAR - 0.5, END_YEAR + 0.5)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "big_tech_arxiv_publications.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  Saved {OUTPUT_FIGURES / 'big_tech_arxiv_publications.png'}")


def create_combined_comparison_figure(arxiv_trends: pl.DataFrame, patent_trends: pl.DataFrame):
    """Create side-by-side comparison of publications vs patents."""
    print("\nGenerating combined comparison figure...")
    
    arxiv_pd = arxiv_trends.to_pandas()
    patent_pd = patent_trends.to_pandas()
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))
    
    firms = ['GOOGLE', 'MICROSOFT', 'AMAZON', 'APPLE', 'META']
    
    # Left panel: Publications
    for firm in firms:
        firm_data = arxiv_pd[arxiv_pd['big_tech_firm'] == firm]
        if len(firm_data) > 0:
            ax1.plot(firm_data['publication_year'], firm_data['publications'],
                    marker=FIRM_MARKERS.get(firm, 'o'), linewidth=2.5, markersize=7,
                    label=firm, color=FIRM_COLORS.get(firm, 'gray'), alpha=0.8)
    
    ax1.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax1.set_ylabel('ArXiv Publications', fontsize=12, fontweight='bold')
    ax1.set_title('(A) ArXiv Publications', fontsize=13, fontweight='bold')
    ax1.legend(loc='best', fontsize=10, framealpha=0.9)
    ax1.grid(True, alpha=0.3, linestyle='--')
    ax1.set_xlim(START_YEAR - 0.5, END_YEAR + 0.5)
    
    # Right panel: Patents
    for firm in firms:
        firm_data = patent_pd[patent_pd['big_tech_firm'] == firm]
        if len(firm_data) > 0:
            ax2.plot(firm_data['year'], firm_data['patents'],
                    marker=FIRM_MARKERS.get(firm, 'o'), linewidth=2.5, markersize=7,
                    label=firm, color=FIRM_COLORS.get(firm, 'gray'), alpha=0.8)
    
    ax2.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax2.set_ylabel('AI Patents Granted', fontsize=12, fontweight='bold')
    ax2.set_title('(B) AI Patents', fontsize=13, fontweight='bold')
    ax2.legend(loc='best', fontsize=10, framealpha=0.9)
    ax2.grid(True, alpha=0.3, linestyle='--')
    ax2.set_xlim(START_YEAR - 0.5, min(END_YEAR, 2022) + 0.5)  # Patents only go to 2022
    
    # Add vertical line at 2018 for patents (potential pivot point)
    ax2.axvline(x=2018, color='red', linestyle='--', alpha=0.5, linewidth=1.5)
    
    plt.suptitle('Big Tech: Publications vs Patents Comparison (2010-2024)',
                 fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "big_tech_publications_vs_patents.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  Saved {OUTPUT_FIGURES / 'big_tech_publications_vs_patents.png'}")


def create_normalized_comparison(arxiv_trends: pl.DataFrame, patent_trends: pl.DataFrame):
    """Create normalized comparison (indexed to 2015 = 100)."""
    print("\nGenerating normalized comparison figure...")
    
    BASE_YEAR = 2015
    
    # Normalize ArXiv data
    arxiv_normalized = []
    for firm in arxiv_trends['big_tech_firm'].unique():
        firm_data = arxiv_trends.filter(pl.col('big_tech_firm') == firm).sort('publication_year')
        base_value = firm_data.filter(pl.col('publication_year') == BASE_YEAR)
        if len(base_value) > 0:
            base = base_value['publications'].item()
            if base > 0:
                normalized = firm_data.with_columns([
                    (pl.col('publications') / base * 100).alias('publications_indexed')
                ])
                arxiv_normalized.append(normalized)
    
    if arxiv_normalized:
        arxiv_norm = pl.concat(arxiv_normalized)
        arxiv_pd = arxiv_norm.to_pandas()
    else:
        arxiv_pd = arxiv_trends.to_pandas()
        arxiv_pd['publications_indexed'] = np.nan
    
    # Normalize patent data
    patent_normalized = []
    for firm in patent_trends['big_tech_firm'].unique():
        firm_data = patent_trends.filter(pl.col('big_tech_firm') == firm).sort('year')
        base_value = firm_data.filter(pl.col('year') == BASE_YEAR)
        if len(base_value) > 0:
            base = base_value['patents'].item()
            if base > 0:
                normalized = firm_data.with_columns([
                    (pl.col('patents') / base * 100).alias('patents_indexed')
                ])
                patent_normalized.append(normalized)
    
    if patent_normalized:
        patent_norm = pl.concat(patent_normalized)
        patent_pd = patent_norm.to_pandas()
    else:
        patent_pd = patent_trends.to_pandas()
        patent_pd['patents_indexed'] = np.nan
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    firms = ['GOOGLE', 'MICROSOFT', 'AMAZON', 'APPLE', 'META']
    
    # Plot publications (solid lines)
    for firm in firms:
        firm_data = arxiv_pd[arxiv_pd['big_tech_firm'] == firm]
        if len(firm_data) > 0 and 'publications_indexed' in firm_data.columns:
            ax.plot(firm_data['publication_year'], firm_data['publications_indexed'],
                   marker=FIRM_MARKERS.get(firm, 'o'), linewidth=2.5, markersize=7,
                   label=f'{firm} (Publications)', color=FIRM_COLORS.get(firm, 'gray'),
                   alpha=0.8, linestyle='-')
    
    # Plot patents (dashed lines)
    for firm in firms:
        firm_data = patent_pd[patent_pd['big_tech_firm'] == firm]
        if len(firm_data) > 0 and 'patents_indexed' in firm_data.columns:
            ax.plot(firm_data['year'], firm_data['patents_indexed'],
                   marker=FIRM_MARKERS.get(firm, 'o'), linewidth=2.5, markersize=7,
                   label=f'{firm} (Patents)', color=FIRM_COLORS.get(firm, 'gray'),
                   alpha=0.6, linestyle='--')
    
    ax.axhline(y=100, color='black', linestyle=':', alpha=0.5, linewidth=1)
    ax.set_xlabel('Year', fontsize=13, fontweight='bold')
    ax.set_ylabel('Index (2015 = 100)', fontsize=13, fontweight='bold')
    ax.set_title('Big Tech: Normalized Publications vs Patents (2015 = 100)',
                 fontsize=15, fontweight='bold', pad=15)
    ax.legend(loc='best', fontsize=9, framealpha=0.9, ncol=2)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_xlim(START_YEAR - 0.5, END_YEAR + 0.5)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "big_tech_normalized_comparison.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  Saved {OUTPUT_FIGURES / 'big_tech_normalized_comparison.png'}")


def save_summary_table(arxiv_trends: pl.DataFrame, patent_trends: pl.DataFrame):
    """Save summary statistics to CSV."""
    print("\nSaving summary tables...")
    
    # ArXiv summary
    arxiv_summary = (
        arxiv_trends
        .group_by('big_tech_firm')
        .agg([
            pl.col('publications').sum().alias('total_publications'),
            pl.col('publications').mean().alias('avg_publications_per_year'),
            pl.col('publications').max().alias('peak_publications'),
            pl.col('publication_year').filter(pl.col('publications') == pl.col('publications').max()).first().alias('peak_year')
        ])
        .sort('total_publications', descending=True)
    )
    
    arxiv_summary.write_csv(OUTPUT_TABLES / "big_tech_arxiv_summary.csv")
    print(f"  Saved {OUTPUT_TABLES / 'big_tech_arxiv_summary.csv'}")
    
    # Patent summary
    patent_summary = (
        patent_trends
        .group_by('big_tech_firm')
        .agg([
            pl.col('patents').sum().alias('total_patents'),
            pl.col('patents').mean().alias('avg_patents_per_year'),
            pl.col('patents').max().alias('peak_patents'),
            pl.col('year').filter(pl.col('patents') == pl.col('patents').max()).first().alias('peak_year')
        ])
        .sort('total_patents', descending=True)
    )
    
    patent_summary.write_csv(OUTPUT_TABLES / "big_tech_patent_summary.csv")
    print(f"  Saved {OUTPUT_TABLES / 'big_tech_patent_summary.csv'}")
    
    # Combined trends
    arxiv_trends.write_csv(OUTPUT_TABLES / "big_tech_arxiv_trends.csv")
    patent_trends.write_csv(OUTPUT_TABLES / "big_tech_patent_trends.csv")
    print(f"  Saved trend tables")


# ============================================================================
# Main Analysis
# ============================================================================

def main():
    """Main analysis pipeline."""
    print("=" * 80)
    print("BIG TECH: PUBLICATIONS VS PATENTS ANALYSIS")
    print("=" * 80)
    
    # Load and process ArXiv data
    arxiv_data = load_arxiv_data()
    arxiv_big_tech = identify_big_tech_in_arxiv(arxiv_data)
    arxiv_trends = get_arxiv_publication_trends(arxiv_big_tech)
    
    # Load and process patent data
    patent_data = load_patent_data()
    patent_big_tech = identify_big_tech_in_patents(patent_data)
    patent_trends = get_patent_trends(patent_big_tech)
    
    # Create visualizations
    create_publication_trends_figure(arxiv_trends)
    create_combined_comparison_figure(arxiv_trends, patent_trends)
    create_normalized_comparison(arxiv_trends, patent_trends)
    
    # Save summary tables
    save_summary_table(arxiv_trends, patent_trends)
    
    print("\n" + "=" * 80)
    print("Analysis complete!")
    print("=" * 80)
    print(f"\nOutput files:")
    print(f"  - {OUTPUT_FIGURES / 'big_tech_arxiv_publications.png'}")
    print(f"  - {OUTPUT_FIGURES / 'big_tech_publications_vs_patents.png'}")
    print(f"  - {OUTPUT_FIGURES / 'big_tech_normalized_comparison.png'}")
    print(f"  - {OUTPUT_TABLES / 'big_tech_arxiv_summary.csv'}")
    print(f"  - {OUTPUT_TABLES / 'big_tech_patent_summary.csv'}")


if __name__ == "__main__":
    main()

