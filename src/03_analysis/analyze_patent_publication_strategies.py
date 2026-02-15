"""
Analyze patent vs. publication strategies across firms, industries, and time.

This script analyzes:
1. Temporal trends in patenting vs. publishing
2. Ratio of patents to publications over time
3. Firm-level patterns in innovation strategy
4. Industry heterogeneity (if industry data available)

Author: Claude Code
Date: 2026-02-15
"""

import polars as pl
from pathlib import Path
import logging

# Setup
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output" / "tables"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 80)
    logger.info("PATENT VS. PUBLICATION STRATEGY ANALYSIS")
    logger.info("=" * 80)

    # [1] Load data
    logger.info("\n[1/6] Loading data...")

    # Patent firm-year panel
    patent_fy = pl.read_parquet(
        DATA_DIR / "processed/analysis/patent_firm_year_panel.parquet"
    )
    logger.info(f"  Patent panel: {len(patent_fy):,} obs, {patent_fy.select(pl.n_unique('GVKEY')).item():,} firms")

    # Publication firm-year panel
    pub_fy = pl.read_parquet(
        DATA_DIR / "processed/analysis/publication_firm_year_panel.parquet"
    )
    logger.info(f"  Publication panel: {len(pub_fy):,} obs, {pub_fy.select(pl.n_unique('GVKEY')).item():,} firms")

    # Combined dataset (firms with both)
    combined_both = pl.read_parquet(
        DATA_DIR / "processed/linking/patent_publication_combined.parquet"
    )
    logger.info(f"  Firms with BOTH: {len(combined_both):,}")

    # [2] Filter to post-1990 and standardize year types
    logger.info("\n[2/6] Filtering to post-1990...")

    patent_fy = (
        patent_fy
        .filter(pl.col('year') >= 1990)
        .with_columns(pl.col('year').cast(pl.Int32))
    )
    pub_fy = (
        pub_fy
        .filter((pl.col('year') >= 1990) & (pl.col('year') <= 2025))
        .with_columns(pl.col('year').cast(pl.Int32))
    )

    # Get the firms that have BOTH patents and publications
    both_firms_list = combined_both.select('GVKEY').to_numpy().flatten().tolist()
    logger.info(f"  Analyzing {len(both_firms_list):,} firms with both patents and publications")

    # [3] Filter to firms with both
    logger.info("\n[3/6] Filtering to firms with both...")

    patent_fy_both = patent_fy.filter(pl.col('GVKEY').is_in(both_firms_list))
    pub_fy_both = pub_fy.filter(pl.col('GVKEY').is_in(both_firms_list))

    logger.info(f"  Patent panel (both firms): {len(patent_fy_both):,} obs")
    logger.info(f"  Publication panel (both firms): {len(pub_fy_both):,} obs")

    # [4] Merge and calculate ratios
    logger.info("\n[4/6] Merging and calculating ratios...")

    merged = (
        patent_fy_both
        .join(
            pub_fy_both,
            on=['GVKEY', 'year'],
            how='outer',
            suffix='_pub'
        )
        .fill_null(0)
        .with_columns([
            # Patent-publication ratio
            (pl.when(pl.col('total_papers') > 0)
             .then(pl.col('total_ai_patents') / pl.col('total_papers'))
             .otherwise(None)
            ).alias('patents_per_paper'),
            (pl.when(pl.col('total_ai_patents') > 0)
             .then(pl.col('total_papers') / pl.col('total_ai_patents'))
             .otherwise(None)
            ).alias('papers_per_patent'),
            # Activity indicators
            (pl.col('total_ai_patents') > 0).alias('has_patents'),
            (pl.col('total_papers') > 0).alias('has_papers'),
            ((pl.col('total_ai_patents') > 0) & (pl.col('total_papers') > 0)).alias('both_activities'),
        ])
    )

    logger.info(f"  Merged: {len(merged):,} firm-year observations")

    # [5] Analyze by year
    logger.info("\n[5/6] Analyzing temporal patterns...")

    yearly_stats = (
        merged
        .group_by('year')
        .agg([
            pl.len().alias('n_firms'),
            pl.sum('has_patents').alias('n_patenting'),
            pl.sum('has_papers').alias('n_publishing'),
            pl.sum('both_activities').alias('n_both'),
            # Mean ratios for firms with both activities
            pl.when(pl.col('both_activities'))
             .then(pl.col('patents_per_paper'))
             .mean()
            .alias('mean_patents_per_paper_both'),
            pl.when(pl.col('both_activities'))
             .then(pl.col('papers_per_patent'))
             .mean()
            .alias('mean_papers_per_patent_both'),
            # Mean activity levels
            pl.when(pl.col('has_patents'))
             .then(pl.col('total_ai_patents'))
             .mean()
            .alias('mean_patents_patenting'),
            pl.when(pl.col('has_papers'))
             .then(pl.col('total_papers'))
             .mean()
            .alias('mean_papers_publishing'),
        ])
        .sort('year')
    )

    # [6] Output results
    logger.info("\n[6/6] Outputting results...")

    print("\n" + "=" * 80)
    print("TEMPORAL TRENDS IN PATENT VS. PUBLICATION STRATEGIES")
    print("=" * 80)
    print("\nFirms with BOTH Patents and Publications (2,202 firms)")
    print("\nYear-by-Year Statistics:")
    print(yearly_stats)

    # Calculate period averages
    print("\n" + "=" * 80)
    print("5-YEAR PERIOD AVERAGES")
    print("=" * 80)

    yearly_with_period = yearly_stats.with_columns(
        ((pl.col('year') // 5) * 5).alias('period')
    )

    period_stats = (
        yearly_with_period
        .group_by('period')
        .agg([
            pl.col('n_firms').mean().alias('avg_firms_per_year'),
            pl.col('n_both').mean().alias('avg_both_per_year'),
            pl.col('mean_patents_per_paper_both').mean().alias('avg_patents_per_paper'),
            pl.col('mean_patents_patenting').mean().alias('avg_patents_per_firm'),
            pl.col('mean_papers_publishing').mean().alias('avg_papers_per_firm'),
        ])
        .sort('period')
        .filter(pl.col('period') >= 1990)
    )

    print(period_stats)

    # Save to CSV
    output_file = OUTPUT_DIR / "patent_publication_strategies_temporal.csv"
    yearly_stats.write_csv(output_file)
    logger.info(f"\n✅ Saved temporal analysis to: {output_file}")

    # Key insights
    print("\n" + "=" * 80)
    print("KEY INSIGHTS")
    print("=" * 80)

    early = yearly_stats.filter(pl.col('year').is_in(range(1990, 1995)))
    recent = yearly_stats.filter(pl.col('year').is_in(range(2020, 2025)))

    if len(early) > 0 and len(recent) > 0:
        early_both_pct = early['n_both'].sum() / early['n_firms'].sum() * 100
        recent_both_pct = recent['n_both'].sum() / recent['n_firms'].sum() * 100
        early_patents = early['mean_patents_patenting'].mean()
        recent_patents = recent['mean_patents_patenting'].mean()
        early_papers = early['mean_papers_publishing'].mean()
        recent_papers = recent['mean_papers_publishing'].mean()

        print(f"""
Early Period (1990-1994) vs. Recent Period (2020-2024):

Firms Active in Both Activities:
  - 1990-1994: {early_both_pct:.1f}% of firm-years
  - 2020-2024: {recent_both_pct:.1f}% of firm-years
  - Change: {recent_both_pct - early_both_pct:+.1f} percentage points

Mean Patenting Activity (for patenting firms):
  - 1990-1994: {early_patents:.1f} patents per firm-year
  - 2020-2024: {recent_patents:.1f} patents per firm-year
  - Change: {recent_patents - early_patents:+.1f} patents

Mean Publishing Activity (for publishing firms):
  - 1990-1994: {early_papers:.1f} papers per firm-year
  - 2020-2024: {recent_papers:.1f} papers per firm-year
  - Change: {recent_papers - early_papers:+.1f} papers
        """)

    logger.info("\n✅ Analysis complete!")


if __name__ == "__main__":
    main()
