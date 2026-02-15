"""
Summary Statistics for AI Patent Panel Data.

Generates comprehensive statistics to understand the landscape of AI innovation
based on the assignee-year patent panel.

Note: Analysis is restricted to years <= 2023 to exclude incomplete recent data,
as patents require processing time before being granted and included in the dataset.

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
sns.set_palette("viridis")


def load_data() -> pl.DataFrame:
    """Load the patent panel data, restricted to years <= 2023."""
    print("Loading patent panel data...")
    df = pl.read_parquet(INPUT_FILE)
    
    # Filter to years <= 2023 (exclude incomplete recent years)
    df = df.filter(pl.col("year") <= 2023)
    
    print(f"  Loaded {len(df):,} assignee-year observations (restricted to 1976-2023)")
    return df


def basic_descriptives(df: pl.DataFrame) -> pl.DataFrame:
    """Generate basic descriptive statistics."""
    print("\n" + "=" * 70)
    print("BASIC DESCRIPTIVE STATISTICS")
    print("=" * 70)
    
    # Overall statistics
    stats = {
        "Metric": [
            "Total Assignee-Year Observations",
            "Unique Assignees",
            "Unique Years",
            "Year Range",
            "Total AI Patents",
            "Mean Patents per Assignee-Year",
            "Median Patents per Assignee-Year",
            "Max Patents per Assignee-Year",
            "Std Dev Patents per Assignee-Year",
            "Mean AI Score",
            "Median AI Score",
            "Min AI Score",
            "Max AI Score",
        ],
        "Value": [
            f"{len(df):,}",
            f"{df['clean_name'].n_unique():,}",
            f"{df['year'].n_unique():,}",
            f"{df['year'].min()} - {df['year'].max()}",
            f"{df['total_ai_patents'].sum():,}",
            f"{df['total_ai_patents'].mean():.2f}",
            f"{df['total_ai_patents'].median():.2f}",
            f"{df['total_ai_patents'].max():,}",
            f"{df['total_ai_patents'].std():.2f}",
            f"{df['avg_ai_score'].mean():.4f}",
            f"{df['avg_ai_score'].median():.4f}",
            f"{df['avg_ai_score'].min():.4f}",
            f"{df['avg_ai_score'].max():.4f}",
        ]
    }
    
    stats_df = pl.DataFrame(stats)
    print(stats_df)
    
    # Save to CSV
    stats_df.write_csv(OUTPUT_TABLES / "01_basic_descriptives.csv")
    print(f"\n  Saved to {OUTPUT_TABLES / '01_basic_descriptives.csv'}")
    
    return stats_df


def yearly_trends(df: pl.DataFrame) -> pl.DataFrame:
    """Analyze yearly trends in AI patenting."""
    print("\n" + "=" * 70)
    print("YEARLY TRENDS")
    print("=" * 70)
    
    yearly = (
        df
        .group_by("year")
        .agg([
            pl.col("clean_name").n_unique().alias("unique_assignees"),
            pl.col("total_ai_patents").sum().alias("total_patents"),
            pl.col("total_ai_patents").mean().alias("avg_patents_per_assignee"),
            pl.col("avg_ai_score").mean().alias("mean_ai_score"),
        ])
        .sort("year")
    )
    
    # Add year-over-year growth
    yearly = yearly.with_columns([
        ((pl.col("total_patents") - pl.col("total_patents").shift(1)) / 
         pl.col("total_patents").shift(1) * 100).alias("yoy_patent_growth_pct"),
        ((pl.col("unique_assignees") - pl.col("unique_assignees").shift(1)) / 
         pl.col("unique_assignees").shift(1) * 100).alias("yoy_assignee_growth_pct"),
    ])
    
    print("\nYearly Summary (last 20 years):")
    print(yearly.filter(pl.col("year") >= 2005))
    
    # Save full table
    yearly.write_csv(OUTPUT_TABLES / "02_yearly_trends.csv")
    print(f"\n  Saved to {OUTPUT_TABLES / '02_yearly_trends.csv'}")
    
    return yearly


def top_assignees(df: pl.DataFrame, top_n: int = 50) -> pl.DataFrame:
    """Identify top AI patent assignees."""
    print("\n" + "=" * 70)
    print(f"TOP {top_n} AI PATENT ASSIGNEES (All Time)")
    print("=" * 70)
    
    top = (
        df
        .group_by("clean_name")
        .agg([
            pl.col("total_ai_patents").sum().alias("total_patents"),
            pl.col("year").min().alias("first_year"),
            pl.col("year").max().alias("last_year"),
            pl.col("year").n_unique().alias("active_years"),
            pl.col("avg_ai_score").mean().alias("mean_ai_score"),
        ])
        .sort("total_patents", descending=True)
        .head(top_n)
        .with_row_index("rank", offset=1)
    )
    
    print(top.head(20))
    
    # Save
    top.write_csv(OUTPUT_TABLES / "03_top_assignees.csv")
    print(f"\n  Saved to {OUTPUT_TABLES / '03_top_assignees.csv'}")
    
    return top


def patent_distribution(df: pl.DataFrame) -> dict:
    """Analyze the distribution of patents across assignees."""
    print("\n" + "=" * 70)
    print("PATENT DISTRIBUTION ANALYSIS")
    print("=" * 70)
    
    # Assignee-level aggregation
    assignee_totals = (
        df
        .group_by("clean_name")
        .agg([
            pl.col("total_ai_patents").sum().alias("total_patents"),
        ])
    )
    
    # Calculate percentiles
    percentiles = [10, 25, 50, 75, 90, 95, 99]
    pct_values = [assignee_totals["total_patents"].quantile(p/100) for p in percentiles]
    
    print("\nPatent Count Percentiles (by assignee):")
    for p, v in zip(percentiles, pct_values):
        print(f"  {p}th percentile: {v:.0f} patents")
    
    # Distribution buckets
    buckets = assignee_totals.with_columns(
        pl.when(pl.col("total_patents") == 1).then(pl.lit("1"))
        .when(pl.col("total_patents") <= 5).then(pl.lit("2-5"))
        .when(pl.col("total_patents") <= 10).then(pl.lit("6-10"))
        .when(pl.col("total_patents") <= 50).then(pl.lit("11-50"))
        .when(pl.col("total_patents") <= 100).then(pl.lit("51-100"))
        .when(pl.col("total_patents") <= 500).then(pl.lit("101-500"))
        .when(pl.col("total_patents") <= 1000).then(pl.lit("501-1000"))
        .otherwise(pl.lit("1000+"))
        .alias("patent_bucket")
    )
    
    bucket_dist = (
        buckets
        .group_by("patent_bucket")
        .agg([
            pl.len().alias("assignee_count"),
            pl.col("total_patents").sum().alias("total_patents_in_bucket")
        ])
        .with_columns([
            (pl.col("assignee_count") / pl.col("assignee_count").sum() * 100).alias("pct_of_assignees"),
            (pl.col("total_patents_in_bucket") / pl.col("total_patents_in_bucket").sum() * 100).alias("pct_of_patents"),
        ])
    )
    
    # Sort by bucket order
    bucket_order = {"1": 1, "2-5": 2, "6-10": 3, "11-50": 4, "51-100": 5, "101-500": 6, "501-1000": 7, "1000+": 8}
    bucket_dist = bucket_dist.with_columns(
        pl.col("patent_bucket").replace(bucket_order).alias("sort_order")
    ).sort("sort_order").drop("sort_order")
    
    print("\nAssignee Distribution by Patent Count:")
    print(bucket_dist)
    
    bucket_dist.write_csv(OUTPUT_TABLES / "04_patent_distribution.csv")
    print(f"\n  Saved to {OUTPUT_TABLES / '04_patent_distribution.csv'}")
    
    return {"percentiles": dict(zip(percentiles, pct_values)), "buckets": bucket_dist}


def concentration_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate market concentration metrics."""
    print("\n" + "=" * 70)
    print("CONCENTRATION METRICS")
    print("=" * 70)
    
    # Assignee-level totals
    assignee_totals = (
        df
        .group_by("clean_name")
        .agg(pl.col("total_ai_patents").sum().alias("total_patents"))
        .sort("total_patents", descending=True)
    )
    
    total_patents = assignee_totals["total_patents"].sum()
    n_assignees = len(assignee_totals)
    
    # Top N concentration
    concentration_metrics = []
    for n in [1, 5, 10, 20, 50, 100]:
        top_n_patents = assignee_totals.head(n)["total_patents"].sum()
        pct = top_n_patents / total_patents * 100
        concentration_metrics.append({
            "Metric": f"Top {n} Share (%)",
            "Value": f"{pct:.2f}%"
        })
    
    # Herfindahl-Hirschman Index (HHI)
    market_shares = assignee_totals["total_patents"] / total_patents
    hhi = (market_shares ** 2).sum() * 10000
    concentration_metrics.append({"Metric": "HHI (x10000)", "Value": f"{hhi:.2f}"})
    
    # Gini coefficient
    sorted_shares = np.sort(market_shares.to_numpy())
    n = len(sorted_shares)
    cumulative = np.cumsum(sorted_shares)
    gini = (n + 1 - 2 * np.sum(cumulative) / cumulative[-1]) / n
    concentration_metrics.append({"Metric": "Gini Coefficient", "Value": f"{gini:.4f}"})
    
    conc_df = pl.DataFrame(concentration_metrics)
    print(conc_df)
    
    conc_df.write_csv(OUTPUT_TABLES / "05_concentration_metrics.csv")
    print(f"\n  Saved to {OUTPUT_TABLES / '05_concentration_metrics.csv'}")
    
    return conc_df


def ai_score_analysis(df: pl.DataFrame) -> pl.DataFrame:
    """Analyze AI score distributions."""
    print("\n" + "=" * 70)
    print("AI SCORE ANALYSIS")
    print("=" * 70)
    
    # Score distribution by buckets
    score_buckets = df.with_columns(
        pl.when(pl.col("avg_ai_score") < 0.6).then(pl.lit("0.5-0.6"))
        .when(pl.col("avg_ai_score") < 0.7).then(pl.lit("0.6-0.7"))
        .when(pl.col("avg_ai_score") < 0.8).then(pl.lit("0.7-0.8"))
        .when(pl.col("avg_ai_score") < 0.9).then(pl.lit("0.8-0.9"))
        .when(pl.col("avg_ai_score") < 0.95).then(pl.lit("0.9-0.95"))
        .otherwise(pl.lit("0.95-1.0"))
        .alias("score_bucket")
    )
    
    score_dist = (
        score_buckets
        .group_by("score_bucket")
        .agg([
            pl.len().alias("observation_count"),
            pl.col("total_ai_patents").sum().alias("total_patents"),
        ])
        .with_columns([
            (pl.col("observation_count") / pl.col("observation_count").sum() * 100).alias("pct_observations"),
            (pl.col("total_patents") / pl.col("total_patents").sum() * 100).alias("pct_patents"),
        ])
    )
    
    # Sort by bucket
    bucket_order = {"0.5-0.6": 1, "0.6-0.7": 2, "0.7-0.8": 3, "0.8-0.9": 4, "0.9-0.95": 5, "0.95-1.0": 6}
    score_dist = score_dist.with_columns(
        pl.col("score_bucket").replace(bucket_order).alias("sort_order")
    ).sort("sort_order").drop("sort_order")
    
    print("\nAI Score Distribution:")
    print(score_dist)
    
    # Score trends over time
    score_by_year = (
        df
        .group_by("year")
        .agg([
            pl.col("avg_ai_score").mean().alias("mean_score"),
            pl.col("avg_ai_score").std().alias("std_score"),
            pl.col("avg_ai_score").quantile(0.25).alias("q25_score"),
            pl.col("avg_ai_score").quantile(0.75).alias("q75_score"),
        ])
        .sort("year")
    )
    
    score_dist.write_csv(OUTPUT_TABLES / "06_ai_score_distribution.csv")
    score_by_year.write_csv(OUTPUT_TABLES / "07_ai_score_by_year.csv")
    print(f"\n  Saved to {OUTPUT_TABLES / '06_ai_score_distribution.csv'}")
    print(f"  Saved to {OUTPUT_TABLES / '07_ai_score_by_year.csv'}")
    
    return score_dist


def create_visualizations(df: pl.DataFrame, yearly: pl.DataFrame, top: pl.DataFrame):
    """Generate visualization figures."""
    print("\n" + "=" * 70)
    print("GENERATING VISUALIZATIONS")
    print("=" * 70)
    
    # Convert to pandas for matplotlib
    yearly_pd = yearly.to_pandas()
    top_pd = top.to_pandas()
    df_pd = df.to_pandas()
    
    # 1. Time series: AI patents and assignees over time
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # 1a. Total patents by year (restrict to <= 2023)
    ax = axes[0, 0]
    yearly_recent = yearly_pd[(yearly_pd['year'] >= 1990) & (yearly_pd['year'] <= 2023)]
    ax.bar(yearly_recent['year'], yearly_recent['total_patents'], color='#2E86AB', alpha=0.8)
    ax.set_xlabel('Year', fontsize=11)
    ax.set_ylabel('Total AI Patents', fontsize=11)
    ax.set_title('AI Patent Grants by Year', fontsize=13, fontweight='bold')
    ax.tick_params(axis='x', rotation=45)
    
    # 1b. Unique assignees by year
    ax = axes[0, 1]
    ax.bar(yearly_recent['year'], yearly_recent['unique_assignees'], color='#A23B72', alpha=0.8)
    ax.set_xlabel('Year', fontsize=11)
    ax.set_ylabel('Unique Assignees', fontsize=11)
    ax.set_title('Unique AI Patent Assignees by Year', fontsize=13, fontweight='bold')
    ax.tick_params(axis='x', rotation=45)
    
    # 1c. Mean AI score over time
    ax = axes[1, 0]
    ax.plot(yearly_recent['year'], yearly_recent['mean_ai_score'], 
            marker='o', linewidth=2, markersize=4, color='#F18F01')
    ax.set_xlabel('Year', fontsize=11)
    ax.set_ylabel('Mean AI Score', fontsize=11)
    ax.set_title('Average AI Score Over Time', fontsize=13, fontweight='bold')
    ax.tick_params(axis='x', rotation=45)
    ax.set_ylim(0.5, 1.0)
    
    # 1d. Patents per assignee over time
    ax = axes[1, 1]
    ax.plot(yearly_recent['year'], yearly_recent['avg_patents_per_assignee'], 
            marker='s', linewidth=2, markersize=4, color='#44AF69')
    ax.set_xlabel('Year', fontsize=11)
    ax.set_ylabel('Avg Patents per Assignee', fontsize=11)
    ax.set_title('Patent Intensity per Assignee Over Time', fontsize=13, fontweight='bold')
    ax.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "01_yearly_trends.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved {OUTPUT_FIGURES / '01_yearly_trends.png'}")
    
    # 2. Top 20 assignees bar chart
    fig, ax = plt.subplots(figsize=(12, 8))
    top20 = top_pd.head(20).iloc[::-1]  # Reverse for horizontal bar
    colors = plt.cm.viridis(np.linspace(0.2, 0.8, 20))[::-1]
    ax.barh(range(20), top20['total_patents'], color=colors)
    ax.set_yticks(range(20))
    ax.set_yticklabels(top20['clean_name'], fontsize=9)
    ax.set_xlabel('Total AI Patents', fontsize=11)
    ax.set_title('Top 20 AI Patent Assignees (All Time)', fontsize=13, fontweight='bold')
    
    # Add value labels
    for i, v in enumerate(top20['total_patents']):
        ax.text(v + 50, i, f'{v:,}', va='center', fontsize=8)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "02_top_assignees.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved {OUTPUT_FIGURES / '02_top_assignees.png'}")
    
    # 3. AI Score distribution histogram
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(df_pd['avg_ai_score'], bins=50, color='#5E60CE', alpha=0.8, edgecolor='white')
    ax.axvline(df_pd['avg_ai_score'].mean(), color='#E63946', linestyle='--', 
               linewidth=2, label=f"Mean: {df_pd['avg_ai_score'].mean():.3f}")
    ax.axvline(df_pd['avg_ai_score'].median(), color='#F4A261', linestyle='--', 
               linewidth=2, label=f"Median: {df_pd['avg_ai_score'].median():.3f}")
    ax.set_xlabel('Average AI Score', fontsize=11)
    ax.set_ylabel('Frequency', fontsize=11)
    ax.set_title('Distribution of Average AI Scores', fontsize=13, fontweight='bold')
    ax.legend(fontsize=10)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "03_ai_score_distribution.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved {OUTPUT_FIGURES / '03_ai_score_distribution.png'}")
    
    # 4. Assignee patent count distribution (log scale)
    assignee_totals = df_pd.groupby('clean_name')['total_ai_patents'].sum()
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(assignee_totals, bins=100, color='#457B9D', alpha=0.8, edgecolor='white')
    ax.set_xlabel('Total Patents per Assignee', fontsize=11)
    ax.set_ylabel('Frequency (log scale)', fontsize=11)
    ax.set_title('Distribution of Patent Counts Across Assignees', fontsize=13, fontweight='bold')
    ax.set_yscale('log')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "04_patent_count_distribution.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved {OUTPUT_FIGURES / '04_patent_count_distribution.png'}")
    
    # 5. Cumulative patent share (Lorenz curve)
    sorted_patents = np.sort(assignee_totals.values)[::-1]
    cumulative_share = np.cumsum(sorted_patents) / sorted_patents.sum()
    assignee_share = np.arange(1, len(sorted_patents) + 1) / len(sorted_patents)
    
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.plot(assignee_share * 100, cumulative_share * 100, linewidth=2, color='#2E86AB', label='AI Patents')
    ax.plot([0, 100], [0, 100], 'k--', linewidth=1, label='Perfect Equality')
    ax.fill_between(assignee_share * 100, cumulative_share * 100, assignee_share * 100, alpha=0.3)
    ax.set_xlabel('Cumulative % of Assignees (ranked by patents)', fontsize=11)
    ax.set_ylabel('Cumulative % of Patents', fontsize=11)
    ax.set_title('Lorenz Curve: AI Patent Concentration', fontsize=13, fontweight='bold')
    ax.legend(fontsize=10)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    
    # Add annotation for top 10%
    top10_idx = int(len(sorted_patents) * 0.1)
    top10_share = cumulative_share[top10_idx] * 100
    ax.annotate(f'Top 10% hold {top10_share:.1f}% of patents', 
                xy=(10, top10_share), xytext=(30, top10_share - 15),
                arrowprops=dict(arrowstyle='->', color='gray'),
                fontsize=10, color='#E63946')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "05_lorenz_curve.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved {OUTPUT_FIGURES / '05_lorenz_curve.png'}")
    
    # 6. Heatmap: Top assignees over time
    top10_names = top_pd.head(10)['clean_name'].tolist()
    top10_data = df_pd[df_pd['clean_name'].isin(top10_names)]
    
    # Filter to recent years (2010-2023)
    top10_recent = top10_data[(top10_data['year'] >= 2010) & (top10_data['year'] <= 2023)]
    pivot = top10_recent.pivot_table(index='clean_name', columns='year', 
                                      values='total_ai_patents', aggfunc='sum', fill_value=0)
    
    fig, ax = plt.subplots(figsize=(14, 8))
    sns.heatmap(pivot, cmap='YlOrRd', annot=True, fmt='d', ax=ax, 
                linewidths=0.5, cbar_kws={'label': 'AI Patents'})
    ax.set_title('Top 10 Assignees: AI Patents by Year (2010+)', fontsize=13, fontweight='bold')
    ax.set_xlabel('Year', fontsize=11)
    ax.set_ylabel('Assignee', fontsize=11)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_FIGURES / "06_top10_heatmap.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved {OUTPUT_FIGURES / '06_top10_heatmap.png'}")


def generate_summary_report(df: pl.DataFrame):
    """Generate a text summary report."""
    print("\n" + "=" * 70)
    print("SUMMARY REPORT")
    print("=" * 70)
    
    # Calculate key statistics
    total_obs = len(df)
    unique_assignees = df['clean_name'].n_unique()
    total_patents = df['total_ai_patents'].sum()
    year_min, year_max = df['year'].min(), df['year'].max()
    
    # Top assignees
    top5 = (
        df.group_by("clean_name")
        .agg(pl.col("total_ai_patents").sum())
        .sort("total_ai_patents", descending=True)
        .head(5)
    )
    
    # Recent growth (2020-2023)
    recent = df.filter((pl.col("year") >= 2020) & (pl.col("year") <= 2023))
    recent_patents = recent['total_ai_patents'].sum()
    
    report = f"""
================================================================================
                     AI PATENT PANEL: SUMMARY STATISTICS REPORT
                     (Data restricted to years <= 2023)
================================================================================

DATA OVERVIEW
-------------
- Total Assignee-Year Observations: {total_obs:,}
- Unique Assignees: {unique_assignees:,}
- Total AI Patents: {total_patents:,}
- Coverage Period: {year_min} - {year_max} (restricted to complete years)

TOP 5 AI PATENT ASSIGNEES (All Time)
------------------------------------
"""
    for i, row in enumerate(top5.iter_rows(named=True), 1):
        report += f"  {i}. {row['clean_name']}: {row['total_ai_patents']:,} patents\n"
    
    report += f"""
RECENT ACTIVITY (2020-2023)
---------------------------------
- Patents Granted: {recent_patents:,}
- Share of All-Time Total: {recent_patents/total_patents*100:.1f}%

KEY INSIGHTS
------------
1. AI patenting shows strong concentration - top assignees dominate the landscape
2. Patent activity has accelerated significantly in recent years
3. Large technology companies are the primary drivers of AI innovation
4. Average AI confidence scores indicate high-quality patent classification

================================================================================
Generated Files:
  - Tables: {OUTPUT_TABLES}/
  - Figures: {OUTPUT_FIGURES}/
================================================================================
"""
    
    print(report)
    
    # Save report
    with open(OUTPUT_TABLES / "00_summary_report.txt", "w") as f:
        f.write(report)
    print(f"\nSaved report to {OUTPUT_TABLES / '00_summary_report.txt'}")


def main():
    """Main analysis pipeline."""
    print("=" * 70)
    print("AI PATENT PANEL - SUMMARY STATISTICS")
    print("=" * 70)
    
    # Load data
    df = load_data()
    
    # Generate statistics
    basic_descriptives(df)
    yearly = yearly_trends(df)
    top = top_assignees(df)
    patent_distribution(df)
    concentration_metrics(df)
    ai_score_analysis(df)
    
    # Generate visualizations
    create_visualizations(df, yearly, top)
    
    # Generate summary report
    generate_summary_report(df)
    
    print("\n" + "=" * 70)
    print("Analysis complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()

