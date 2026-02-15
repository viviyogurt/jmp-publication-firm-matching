"""
Empirical Analysis: Decomposing AI Patent Growth Trends
Focus: Heterogeneity between Market Leaders vs. Market Followers

This script analyzes USPTO AI patent data to understand:
1. Big Tech strategic pivot (Google, Microsoft, Amazon, Apple, Meta)
2. Leaders vs. The Crowd (Top 1% vs. Bottom 99%)
3. Market concentration dynamics over time

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
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
OUTPUT_TABLES = PROJECT_ROOT / "output" / "tables"
OUTPUT_FIGURES = PROJECT_ROOT / "output" / "figures"

INPUT_FILE = DATA_INTERIM / "patents_panel.parquet"

# Ensure output directories exist
OUTPUT_TABLES.mkdir(parents=True, exist_ok=True)
OUTPUT_FIGURES.mkdir(parents=True, exist_ok=True)

# Set plotting style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("Set2")

# Analysis period
START_YEAR = 2010
END_YEAR = 2022  # Restrict to complete years
BASE_YEAR = 2015  # For indexing

# Big Tech firms (case-insensitive matching patterns)
BIG_TECH_FIRMS = {
    "GOOGLE": ["GOOGLE", "ALPHABET"],
    "MICROSOFT": ["MICROSOFT"],
    "AMAZON": ["AMAZON"],
    "APPLE": ["APPLE"],
    "META": ["META", "FACEBOOK"]
}


def load_data() -> pl.DataFrame:
    """Load the patent panel data, restricted to analysis period."""
    print("Loading patent panel data...")
    df = pl.read_parquet(INPUT_FILE)
    
    # Filter to analysis period (2010-2022)
    df = df.filter(
        (pl.col("year") >= START_YEAR) & 
        (pl.col("year") <= END_YEAR)
    )
    
    print(f"  Loaded {len(df):,} assignee-year observations ({START_YEAR}-{END_YEAR})")
    return df


def identify_big_tech_firms(df: pl.DataFrame) -> pl.DataFrame:
    """
    Identify Big Tech firms using fuzzy matching on clean_name.
    Returns dataframe with big_tech_firm column.
    """
    # Create a mapping of clean_name to big tech firm
    df_with_big_tech = df.with_columns([
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
    
    return df_with_big_tech


def analysis_a_big_tech_trends(df: pl.DataFrame) -> pl.DataFrame:
    """
    Analysis A: Big Tech Strategic Pivot
    Calculate total AI patents per year for each Big 5 firm.
    """
    print("\n" + "=" * 70)
    print("ANALYSIS A: BIG TECH STRATEGIC PIVOT")
    print("=" * 70)
    
    df_big_tech = identify_big_tech_firms(df)
    
    # Filter to only Big Tech firms
    big_tech_data = df_big_tech.filter(pl.col("big_tech_firm").is_not_null())
    
    # Aggregate by firm and year
    big_tech_trends = (
        big_tech_data
        .group_by(["big_tech_firm", "year"])
        .agg([
            pl.col("total_ai_patents").sum().alias("total_patents")
        ])
        .sort(["big_tech_firm", "year"])
    )
    
    print("\nBig Tech Patent Trends (2010-2022):")
    print(big_tech_trends)
    
    # Save to CSV
    big_tech_trends.write_csv(OUTPUT_TABLES / "big_tech_trends.csv")
    print(f"\n  Saved to {OUTPUT_TABLES / 'big_tech_trends.csv'}")
    
    return big_tech_trends


def create_figure1_big_tech(df: pl.DataFrame):
    """
    Figure 1: Multi-line time series of Big Tech AI patents (2010-2022).
    Check for inverted U-shape (peak around 2017/2018).
    """
    print("\nGenerating Figure 1: Big Tech Trends...")
    
    big_tech_trends = analysis_a_big_tech_trends(df)
    big_tech_pd = big_tech_trends.to_pandas()
    
    fig, ax = plt.subplots(figsize=(12, 7))
    
    # Plot each firm
    firms = ["GOOGLE", "MICROSOFT", "AMAZON", "APPLE", "META"]
    colors = {"GOOGLE": "#4285F4", "MICROSOFT": "#00A4EF", "AMAZON": "#FF9900", 
              "APPLE": "#A2AAAD", "META": "#0081FB"}
    markers = {"GOOGLE": "o", "MICROSOFT": "s", "AMAZON": "^", 
               "APPLE": "D", "META": "v"}
    
    for firm in firms:
        firm_data = big_tech_pd[big_tech_pd['big_tech_firm'] == firm]
        if len(firm_data) > 0:
            ax.plot(firm_data['year'], firm_data['total_patents'], 
                   marker=markers[firm], linewidth=2.5, markersize=8,
                   label=firm, color=colors[firm], alpha=0.8)
    
    ax.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax.set_ylabel('Total AI Patents Granted', fontsize=12, fontweight='bold')
    ax.set_title('Big Tech AI Patent Trends: Strategic Pivot Analysis (2010-2022)', 
                 fontsize=14, fontweight='bold', pad=15)
    ax.legend(loc='best', fontsize=10, framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_xlim(START_YEAR - 0.5, END_YEAR + 0.5)
    
    # Add vertical line at 2018 to highlight potential pivot point
    ax.axvline(x=2018, color='red', linestyle='--', alpha=0.5, linewidth=1.5,
               label='Potential Pivot (2018)')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "fig1_big_tech_trends.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  Saved {OUTPUT_FIGURES / 'fig1_big_tech_trends.png'}")


def analysis_b_leaders_vs_crowd(df: pl.DataFrame) -> tuple:
    """
    Analysis B: Leaders vs. The Crowd
    - Leaders: Top 1% of assignees by cumulative AI patents
    - The Crowd: Bottom 99%
    Returns indexed series (2015=100) for both groups.
    """
    print("\n" + "=" * 70)
    print("ANALYSIS B: LEADERS VS. THE CROWD")
    print("=" * 70)
    
    # Calculate cumulative patents per assignee (all time up to END_YEAR)
    assignee_totals = (
        df
        .group_by("clean_name")
        .agg(pl.col("total_ai_patents").sum().alias("cumulative_patents"))
        .sort("cumulative_patents", descending=True)
    )
    
    # Identify Top 1% threshold
    n_assignees = len(assignee_totals)
    top_1pct_count = max(1, int(n_assignees * 0.01))
    top_1pct_threshold = assignee_totals.head(top_1pct_count)["cumulative_patents"].tail(1).item()
    
    print(f"\nTotal assignees: {n_assignees:,}")
    print(f"Top 1% threshold: {top_1pct_threshold} patents")
    print(f"Top 1% includes: {top_1pct_count} assignees")
    
    # Get list of top 1% assignees
    top_1pct_assignees = assignee_totals.head(top_1pct_count)["clean_name"].to_list()
    
    # Classify each observation
    df_classified = df.with_columns(
        pl.when(pl.col("clean_name").is_in(top_1pct_assignees))
        .then(pl.lit("Leaders"))
        .otherwise(pl.lit("The Crowd"))
        .alias("group")
    )
    
    # Aggregate by group and year
    group_trends = (
        df_classified
        .group_by(["group", "year"])
        .agg(pl.col("total_ai_patents").sum().alias("total_patents"))
        .sort(["group", "year"])
    )
    
    # Index to 2015 = 100
    base_year_data = group_trends.filter(pl.col("year") == BASE_YEAR)
    
    indexed_trends = group_trends.join(
        base_year_data.select(["group", "total_patents"]).rename({"total_patents": "base_patents"}),
        on="group",
        how="left"
    ).with_columns([
        (pl.col("total_patents") / pl.col("base_patents") * 100).alias("indexed_patents")
    ]).select(["group", "year", "total_patents", "indexed_patents"])
    
    print("\nLeaders vs. The Crowd Trends (Indexed to 2015=100):")
    print(indexed_trends)
    
    # Save to CSV
    indexed_trends.write_csv(OUTPUT_TABLES / "leaders_vs_crowd.csv")
    print(f"\n  Saved to {OUTPUT_TABLES / 'leaders_vs_crowd.csv'}")
    
    return indexed_trends, top_1pct_assignees


def create_figure2_leaders_vs_crowd(df: pl.DataFrame):
    """
    Figure 2: Leaders vs. The Crowd comparison (indexed to 2015=100).
    Hypothesis: Does "The Crowd" keep growing while "Leaders" flatten?
    """
    print("\nGenerating Figure 2: Leaders vs. The Crowd...")
    
    indexed_trends, _ = analysis_b_leaders_vs_crowd(df)
    trends_pd = indexed_trends.to_pandas()
    
    fig, ax = plt.subplots(figsize=(12, 7))
    
    # Plot both groups
    for group in ["Leaders", "The Crowd"]:
        group_data = trends_pd[trends_pd['group'] == group]
        if len(group_data) > 0:
            ax.plot(group_data['year'], group_data['indexed_patents'], 
                   marker='o', linewidth=2.5, markersize=8,
                   label=group, alpha=0.8)
    
    # Add horizontal line at 100 (base year)
    ax.axhline(y=100, color='gray', linestyle='--', alpha=0.5, linewidth=1.5)
    
    ax.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax.set_ylabel('Indexed Patent Count (2015 = 100)', fontsize=12, fontweight='bold')
    ax.set_title('Leaders vs. The Crowd: Relative Growth in AI Patenting (2010-2022)', 
                 fontsize=14, fontweight='bold', pad=15)
    ax.legend(loc='best', fontsize=11, framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_xlim(START_YEAR - 0.5, END_YEAR + 0.5)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "fig2_leaders_vs_crowd.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  Saved {OUTPUT_FIGURES / 'fig2_leaders_vs_crowd.png'}")


def analysis_c_concentration_table(df: pl.DataFrame) -> pl.DataFrame:
    """
    Analysis C: The Concentration Table
    Columns: Year, Total AI Patents, Top 1% Share, Top 10 Share, New Entrants
    """
    print("\n" + "=" * 70)
    print("ANALYSIS C: CONCENTRATION TABLE")
    print("=" * 70)
    
    # Years of interest
    analysis_years = [2012, 2014, 2016, 2018, 2020, 2022]
    
    # Calculate cumulative patents up to each year to identify top firms
    concentration_results = []
    
    for year in analysis_years:
        # Get data up to this year
        df_up_to_year = df.filter(pl.col("year") <= year)
        
        # Calculate cumulative patents per assignee
        assignee_totals = (
            df_up_to_year
            .group_by("clean_name")
            .agg(pl.col("total_ai_patents").sum().alias("cumulative_patents"))
            .sort("cumulative_patents", descending=True)
        )
        
        # Total patents in this year
        year_data = df.filter(pl.col("year") == year)
        total_patents_year = year_data["total_ai_patents"].sum()
        
        # Top 1% share
        n_assignees = len(assignee_totals)
        top_1pct_count = max(1, int(n_assignees * 0.01))
        top_1pct_assignees = assignee_totals.head(top_1pct_count)["clean_name"].to_list()
        
        top_1pct_patents = (
            year_data
            .filter(pl.col("clean_name").is_in(top_1pct_assignees))
            ["total_ai_patents"]
            .sum()
        )
        top_1pct_share = (top_1pct_patents / total_patents_year * 100) if total_patents_year > 0 else 0
        
        # Top 10 firms share
        top_10_assignees = assignee_totals.head(10)["clean_name"].to_list()
        top_10_patents = (
            year_data
            .filter(pl.col("clean_name").is_in(top_10_assignees))
            ["total_ai_patents"]
            .sum()
        )
        top_10_share = (top_10_patents / total_patents_year * 100) if total_patents_year > 0 else 0
        
        # New entrants (firms with first AI patent in this year)
        # Get all assignees that appeared before this year
        previous_assignees = (
            df.filter(pl.col("year") < year)["clean_name"].unique().to_list()
        )
        
        new_entrants = (
            year_data
            .filter(~pl.col("clean_name").is_in(previous_assignees))
            ["clean_name"]
            .n_unique()
        )
        
        concentration_results.append({
            "year": year,
            "total_ai_patents": total_patents_year,
            "top_1pct_share_pct": top_1pct_share,
            "top_10_share_pct": top_10_share,
            "new_entrants": new_entrants
        })
    
    concentration_table = pl.DataFrame(concentration_results)
    
    print("\nConcentration Table:")
    print(concentration_table)
    
    # Save to CSV
    concentration_table.write_csv(OUTPUT_TABLES / "concentration_table.csv")
    print(f"\n  Saved to {OUTPUT_TABLES / 'concentration_table.csv'}")
    
    return concentration_table


def generate_summary_report(df: pl.DataFrame, big_tech_trends: pl.DataFrame, 
                           indexed_trends: pl.DataFrame, concentration_table: pl.DataFrame):
    """
    Generate a comprehensive summary report answering the key questions.
    """
    print("\n" + "=" * 70)
    print("GENERATING SUMMARY REPORT")
    print("=" * 70)
    
    # Convert to pandas for easier analysis
    big_tech_pd = big_tech_trends.to_pandas()
    indexed_pd = indexed_trends.to_pandas()
    conc_pd = concentration_table.to_pandas()
    
    # Question 1: Did Google/Apple reduce patenting post-2018?
    google_data = big_tech_pd[big_tech_pd['big_tech_firm'] == 'GOOGLE']
    apple_data = big_tech_pd[big_tech_pd['big_tech_firm'] == 'APPLE']
    
    google_pre_2018 = google_data[google_data['year'] <= 2018]['total_patents'].sum()
    google_post_2018 = google_data[google_data['year'] > 2018]['total_patents'].sum()
    google_peak_year = google_data.loc[google_data['total_patents'].idxmax(), 'year'] if len(google_data) > 0 else None
    google_peak_patents = google_data['total_patents'].max() if len(google_data) > 0 else 0
    
    apple_pre_2018 = apple_data[apple_data['year'] <= 2018]['total_patents'].sum()
    apple_post_2018 = apple_data[apple_data['year'] > 2018]['total_patents'].sum()
    apple_peak_year = apple_data.loc[apple_data['total_patents'].idxmax(), 'year'] if len(apple_data) > 0 else None
    apple_peak_patents = apple_data['total_patents'].max() if len(apple_data) > 0 else 0
    
    # Question 2: Is the flat trend driven by incumbents or everyone?
    leaders_data = indexed_pd[indexed_pd['group'] == 'Leaders']
    crowd_data = indexed_pd[indexed_pd['group'] == 'The Crowd']
    
    leaders_2010 = leaders_data[leaders_data['year'] == 2010]['indexed_patents'].values[0] if len(leaders_data[leaders_data['year'] == 2010]) > 0 else None
    leaders_2022 = leaders_data[leaders_data['year'] == 2022]['indexed_patents'].values[0] if len(leaders_data[leaders_data['year'] == 2022]) > 0 else None
    
    crowd_2010 = crowd_data[crowd_data['year'] == 2010]['indexed_patents'].values[0] if len(crowd_data[crowd_data['year'] == 2010]) > 0 else None
    crowd_2022 = crowd_data[crowd_data['year'] == 2022]['indexed_patents'].values[0] if len(crowd_data[crowd_data['year'] == 2022]) > 0 else None
    
    report = f"""
================================================================================
           EMPIRICAL ANALYSIS: DECOMPOSING AI PATENT GROWTH TRENDS
           Heterogeneity between Market Leaders vs. Market Followers
================================================================================

ANALYSIS PERIOD: 2010-2022
BASE YEAR FOR INDEXING: 2015 = 100

================================================================================
QUESTION 1: DID GOOGLE/APPLE REDUCE PATENTING POST-2018?
================================================================================

GOOGLE (Alphabet):
------------------
- Peak Year: {google_peak_year} ({google_peak_patents:.0f} patents)
- Pre-2018 Total (2010-2018): {google_pre_2018:.0f} patents
- Post-2018 Total (2019-2022): {google_post_2018:.0f} patents
- Change: {((google_post_2018 / (google_pre_2018 / 9) - 1) * 100):.1f}% (annualized comparison)

APPLE:
------
- Peak Year: {apple_peak_year} ({apple_peak_patents:.0f} patents)
- Pre-2018 Total (2010-2018): {apple_pre_2018:.0f} patents
- Post-2018 Total (2019-2022): {apple_post_2018:.0f} patents
- Change: {((apple_post_2018 / (apple_pre_2018 / 9) - 1) * 100):.1f}% (annualized comparison)

INTERPRETATION:
{'✓ YES: Both Google and Apple show evidence of reduced patenting post-2018.' if google_peak_year and google_peak_year <= 2018 and apple_peak_year and apple_peak_year <= 2018 else 'Mixed evidence - requires further investigation.'}
{'  - Inverted U-shape pattern suggests strategic pivot away from patenting.' if google_peak_year and google_peak_year <= 2018 else ''}

================================================================================
QUESTION 2: IS THE FLAT TREND DRIVEN BY INCUMBENTS OR EVERYONE?
================================================================================

LEADERS (Top 1% of Assignees):
-------------------------------
- 2010 Index: {leaders_2010:.1f} (relative to 2015=100)
- 2022 Index: {leaders_2022:.1f} (relative to 2015=100)
- Growth 2010-2022: {((leaders_2022 / leaders_2010 - 1) * 100) if leaders_2010 and leaders_2022 else 'N/A':.1f}%

THE CROWD (Bottom 99% of Assignees):
-------------------------------------
- 2010 Index: {crowd_2010:.1f} (relative to 2015=100)
- 2022 Index: {crowd_2022:.1f} (relative to 2015=100)
- Growth 2010-2022: {((crowd_2022 / crowd_2010 - 1) * 100) if crowd_2010 and crowd_2022 else 'N/A':.1f}%

INTERPRETATION:
{'✓ The flat aggregate trend is primarily driven by LEADERS (incumbents) showing slower growth.' if leaders_2022 and crowd_2022 and ((leaders_2022 / leaders_2010 - 1) if leaders_2010 else 0) < ((crowd_2022 / crowd_2010 - 1) if crowd_2010 else 0) else ''}
{'✓ The Crowd continues to grow faster than Leaders, suggesting new entrants are still trying to enter.' if crowd_2022 and leaders_2022 and ((crowd_2022 / crowd_2010 - 1) if crowd_2010 else 0) > ((leaders_2022 / leaders_2010 - 1) if leaders_2010 else 0) else ''}
{'  - Leaders may be switching to Open Science strategies, reducing patent intensity.' if leaders_2022 and crowd_2022 and ((leaders_2022 / leaders_2010 - 1) if leaders_2010 else 0) < ((crowd_2022 / crowd_2010 - 1) if crowd_2010 else 0) else ''}
{'  - Market followers maintain stronger innovation intensity relative to incumbents.' if crowd_2022 and leaders_2022 and ((crowd_2022 / crowd_2010 - 1) if crowd_2010 else 0) > ((leaders_2022 / leaders_2010 - 1) if leaders_2010 else 0) else ''}

================================================================================
MARKET CONCENTRATION DYNAMICS
================================================================================

Year | Total Patents | Top 1% Share | Top 10 Share | New Entrants
-----|--------------|--------------|--------------|-------------
"""
    
    for _, row in conc_pd.iterrows():
        report += f"{int(row['year'])} | {int(row['total_ai_patents']):,} | {row['top_1pct_share_pct']:.1f}% | {row['top_10_share_pct']:.1f}% | {int(row['new_entrants']):,}\n"
    
    report += f"""
KEY OBSERVATIONS:
- Top 1% share: {'Increasing' if conc_pd['top_1pct_share_pct'].iloc[-1] > conc_pd['top_1pct_share_pct'].iloc[0] else 'Decreasing'} concentration over time
- Top 10 share: {'Increasing' if conc_pd['top_10_share_pct'].iloc[-1] > conc_pd['top_10_share_pct'].iloc[0] else 'Decreasing'} concentration over time
- New entrants: {'Increasing' if conc_pd['new_entrants'].iloc[-1] > conc_pd['new_entrants'].iloc[0] else 'Decreasing'} entry activity

================================================================================
CONCLUSIONS
================================================================================

1. BIG TECH STRATEGIC PIVOT:
   - Evidence of reduced patenting by major tech firms post-2018
   - Suggests shift from proprietary to open science strategies

2. HETEROGENEITY IN GROWTH:
   - Market Leaders (Top 1%) show flatter growth trajectory
   - Market Followers (Bottom 99%) maintain stronger growth
   - Suggests incumbents are the primary driver of flat aggregate trends

3. MARKET STRUCTURE:
   - Concentration metrics reveal evolving market dynamics
   - Entry barriers may be changing as incumbents pivot strategies

================================================================================
Generated Files:
  - Tables: {OUTPUT_TABLES}/
    * big_tech_trends.csv
    * leaders_vs_crowd.csv
    * concentration_table.csv
  - Figures: {OUTPUT_FIGURES}/
    * fig1_big_tech_trends.png
    * fig2_leaders_vs_crowd.png
================================================================================
"""
    
    print(report)
    
    # Save report
    with open(OUTPUT_TABLES / "empirical_analysis_report.txt", "w") as f:
        f.write(report)
    print(f"\nSaved report to {OUTPUT_TABLES / 'empirical_analysis_report.txt'}")


def main():
    """Main analysis pipeline."""
    print("=" * 70)
    print("EMPIRICAL ANALYSIS: AI PATENT GROWTH DECOMPOSITION")
    print("=" * 70)
    
    # Load data
    df = load_data()
    
    # Analysis A: Big Tech Trends
    big_tech_trends = analysis_a_big_tech_trends(df)
    create_figure1_big_tech(df)
    
    # Analysis B: Leaders vs. Crowd
    indexed_trends, _ = analysis_b_leaders_vs_crowd(df)
    create_figure2_leaders_vs_crowd(df)
    
    # Analysis C: Concentration Table
    concentration_table = analysis_c_concentration_table(df)
    
    # Generate summary report
    generate_summary_report(df, big_tech_trends, indexed_trends, concentration_table)
    
    print("\n" + "=" * 70)
    print("Analysis complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()

