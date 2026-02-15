"""
Analyze patent vs. publication strategies by industry.

Uses business descriptions to classify firms into broad industry categories,
then analyzes strategy heterogeneity across industries.

Author: Claude Code
Date: 2026-02-15
"""

import polars as pl
from pathlib import Path
import logging
import re

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


def classify_industry(busdesc: str, conm: str) -> str:
    """
    Classify firm into broad industry category based on business description
    and company name.

    Categories:
    1. Technology/Software
    2. Pharma/Biotech/Healthcare
    3. Manufacturing/Industrial
    4. Finance/Banking
    5. Energy/Utilities
    6. Retail/Consumer
    7. Other
    """
    if busdesc is None:
        busdesc = ""

    busdesc_lower = busdesc.lower()
    conm_lower = conm.lower()

    # Technology/Software
    tech_keywords = [
        'software', 'semiconducto', 'chip', 'microproces', 'comput',
        'technology', 'internet', 'network', 'data center', 'cloud',
        'artificial intelligence', 'ai ', 'machine learning', 'platform',
        'digital', 'cybersecurity', 'it services', 'information technology'
    ]
    if any(kw in busdesc_lower or kw in conm_lower for kw in tech_keywords):
        return "Technology/Software"

    # Pharma/Biotech/Healthcare
    pharma_keywords = [
        'pharmaceutical', 'drug', 'biotech', 'medical device', 'healthcare',
        'hospital', 'clinic', 'medicine', 'therapeutic', 'clinical',
        'vaccine', 'diagnostic', 'life science', 'biolog', 'genome',
        'patient', 'physician', 'surgical', 'treatment'
    ]
    if any(kw in busdesc_lower or kw in conm_lower for kw in pharma_keywords):
        return "Pharma/Biotech/Healthcare"

    # Finance/Banking
    finance_keywords = [
        'bank', 'financial', 'insurance', 'investment', 'asset management',
        'securities', 'credit', 'lending', 'mortgage', 'reinsurance',
        'broker', 'trading', 'wealth management', 'fund', 'trust'
    ]
    if any(kw in busdesc_lower or kw in conm_lower for kw in finance_keywords):
        return "Finance/Banking"

    # Energy/Utilities
    energy_keywords = [
        'oil', 'gas', 'petroleum', 'energy', 'utilities', 'electric',
        'power', 'renewable', 'solar', 'wind', 'mining', 'drilling',
        'pipeline', 'refining', 'exploration', 'utility'
    ]
    if any(kw in busdesc_lower or kw in conm_lower for kw in energy_keywords):
        return "Energy/Utilities"

    # Retail/Consumer
    retail_keywords = [
        'retail', 'consumer', 'restaurant', 'food & beverage', 'apparel',
        'clothing', 'fashion', 'e-commerce', 'ecommerce', 'wholesale',
        'distribution', 'supermarket', 'grocery', 'hotel', 'gaming'
    ]
    if any(kw in busdesc_lower or kw in conm_lower for kw in retail_keywords):
        return "Retail/Consumer"

    # Manufacturing/Industrial
    manufact_keywords = [
        'manufactur', 'industrial', 'machinery', 'equipment', 'aerospace',
        'automotive', 'chemical', 'material', 'construction', 'engineering',
        'steel', 'metal', 'packaging', 'transportation', 'logistics'
    ]
    if any(kw in busdesc_lower or kw in conm_lower for kw in manufact_keywords):
        return "Manufacturing/Industrial"

    return "Other"


def main():
    logger.info("=" * 80)
    logger.info("INDUSTRY-LEVEL PATENT VS. PUBLICATION STRATEGY ANALYSIS")
    logger.info("=" * 80)

    # [1] Load firm data with business descriptions
    logger.info("\n[1/7] Loading firm data...")

    compustat = pl.read_parquet(DATA_DIR / "interim/compustat_firms_standardized.parquet")
    logger.info(f"  Compustat firms: {len(compustat):,}")

    # Classify industries
    logger.info("\n[2/7] Classifying industries from business descriptions...")

    def apply_classification(df):
        return df.with_columns([
            pl.struct(['busdesc', 'conm'])
            .apply(lambda x: classify_industry(x['busdesc'], x['conm']), return_dtype=pl.String)
            .alias('industry')
        ])

    compustat_with_industry = apply_classification(compustat)

    # Check classification results
    industry_counts = compustat_with_industry.group_by('industry').agg([
        pl.len().alias('n_firms'),
    ]).sort('n_firms', descending=True)

    total_firms = len(compustat_with_industry)
    industry_counts = industry_counts.with_columns([
        (pl.col('n_firms') / total_firms * 100).alias('pct')
    ])

    logger.info(f"\nIndustry classification:")
    logger.info(f"{industry_counts}")

    # [2] Load combined dataset (firms with both patents and publications)
    logger.info("\n[3/7] Loading patent/publication combined data...")

    combined_both = pl.read_parquet(DATA_DIR / "processed/linking/patent_publication_combined.parquet")
    logger.info(f"  Firms with both: {len(combined_both):,}")

    # [4] Merge with industry classification
    logger.info("\n[4/7] Merging with industry classification...")

    combined_with_industry = (
        combined_both
        .join(
            compustat_with_industry.select(['GVKEY', 'industry', 'busdesc']),
            on='GVKEY',
            how='left'
        )
    )

    n_with_industry = combined_with_industry.filter(pl.col('industry').is_not_null()).select(pl.len()).item()
    logger.info(f"  Firms with industry classification: {n_with_industry:,} ({n_with_industry/len(combined_with_industry)*100:.1f}%)")

    # [5] Calculate industry-level statistics
    logger.info("\n[5/7] Calculating industry-level statistics...")

    industry_stats = (
        combined_with_industry
        .filter(pl.col('industry').is_not_null())
        .group_by('industry')
        .agg([
            pl.len().alias('n_firms'),
            # Patent statistics
            pl.col('total_patents').sum().alias('total_patents'),
            pl.col('total_patents').mean().alias('mean_patents'),
            pl.col('total_patents').median().alias('median_patents'),
            # Publication statistics
            pl.col('total_papers').sum().alias('total_papers'),
            pl.col('total_papers').mean().alias('mean_papers'),
            pl.col('total_papers').median().alias('median_papers'),
            # Ratios
            (pl.col('total_papers') / (pl.col('total_patents') + 1)).alias('papers_per_patent'),
            (pl.col('total_patents') / (pl.col('total_papers') + 1)).alias('patents_per_paper'),
        ])
        .sort('total_patents', descending=True)
        .with_columns([
            (pl.col('total_papers') / pl.col('n_firms')).alias('avg_papers_per_firm'),
            (pl.col('total_patents') / pl.col('n_firms')).alias('avg_patents_per_firm'),
        ])
    )

    logger.info(f"\nIndustry statistics:")
    logger.info(f"{industry_stats}")

    # [6] Analyze strategy types by industry
    logger.info("\n[6/7] Analyzing strategy types by industry...")

    # Classify firm strategy
    median_ratio = combined_with_industry.filter(pl.col('industry').is_not_null()).select(
        pl.col('total_papers') / (pl.col('total_patents') + 1)
    ).median().item()

    combined_with_strategy = (
        combined_with_industry
        .filter(pl.col('industry').is_not_null())
        .with_columns([
            (pl.col('total_papers') / (pl.col('total_patents') + 1)).alias('papers_per_patent_ratio'),
            (pl.col('total_patents') / (pl.col('total_papers') + 1)).alias('patents_per_paper_ratio'),
        ])
        .with_columns([
            pl.when(pl.col('patents_per_paper_ratio') > 10)
             .then(pl.lit("Patent-focused"))
             .when(pl.col('papers_per_patent_ratio') > 0.5)
             .then(pl.lit("Publication-focused"))
             .otherwise(pl.lit("Balanced"))
             .alias('strategy_type')
        ])
    )

    # Strategy distribution by industry
    strategy_by_industry = (
        combined_with_strategy
        .group_by(['industry', 'strategy_type'])
        .agg([
            pl.len().alias('n_firms'),
        ])
        .sort(['industry', 'n_firms'], descending=[False, True])
    )

    # Calculate percentages within industry
    industry_totals = strategy_by_industry.group_by('industry').agg([
        pl.col('n_firms').sum().alias('industry_total')
    ])

    strategy_by_industry = strategy_by_industry.join(
        industry_totals,
        on='industry',
        how='left'
    ).with_columns([
        (pl.col('n_firms') / pl.col('industry_total') * 100).alias('pct_within_industry')
    ]).drop('industry_total')

    logger.info(f"\nStrategy distribution by industry:")
    logger.info(f"{strategy_by_industry}")

    # [7] Output results
    logger.info("\n[7/7] Saving results...")

    # Flatten industry stats for CSV export
    industry_stats_flat = (
        industry_stats
        .with_columns([
            pl.col('papers_per_patent').list.first().alias('papers_per_patent'),
            pl.col('patents_per_paper').list.first().alias('patents_per_paper'),
        ])
        .drop(['papers_per_patent', 'patents_per_paper', 'avg_papers_per_firm', 'avg_patents_per_firm'])
    )

    # Save industry stats
    output_file = OUTPUT_DIR / "patent_publication_by_industry.csv"
    industry_stats_flat.write_csv(output_file)
    logger.info(f"  Saved: {output_file}")

    # Save strategy by industry
    strategy_file = OUTPUT_DIR / "patent_publication_strategy_by_industry.csv"
    strategy_by_industry.write_csv(strategy_file)
    logger.info(f"  Saved: {strategy_file}")

    # Print summary
    print("\n" + "=" * 80)
    print("INDUSTRY-LEVEL PATENT VS. PUBLICATION STRATEGIES")
    print("=" * 80)
    print("\nIndustry Summary:")
    print(industry_stats.select([
        'industry', 'n_firms', 'total_patents', 'total_papers',
        'mean_patents', 'mean_papers'
    ]))

    print("\n" + "=" * 80)
    print("STRATEGY DISTRIBUTION BY INDUSTRY")
    print("=" * 80)
    print(strategy_by_industry.filter(pl.col('strategy_type') == "Patent-focused").select([
        'industry', 'strategy_type', 'n_firms', 'pct_within_industry'
    ]))

    print("\n" + "=" * 80)
    print("KEY INSIGHTS")
    print("=" * 80)

    # Calculate industry ratios
    industry_ratios = industry_stats.with_columns([
        (pl.col('total_papers') / pl.col('total_patents')).alias('papers_per_patent_total'),
        (pl.col('total_patents') / pl.col('total_papers')).alias('patents_per_paper_total'),
    ])

    # Find top and bottom industries
    top_pub_row = industry_ratios.sort('papers_per_patent_total', descending=True).row(0, named=True)
    top_patent_row = industry_ratios.sort('patents_per_paper_total', descending=True).row(0, named=True)

    print(f"""
Most Publication-Focused Industry: {top_pub_row['industry']}
  - {top_pub_row['mean_papers']:.1f} mean papers per firm
  - {top_pub_row['mean_patents']:.1f} mean patents per firm
  - Ratio: {top_pub_row['papers_per_patent_total']:.2f} papers per patent

Most Patent-Focused Industry: {top_patent_row['industry']}
  - {top_patent_row['mean_patents']:.1f} mean patents per firm
  - {top_patent_row['mean_papers']:.1f} mean papers per firm
  - Ratio: {top_patent_row['patents_per_paper_total']:.2f} patents per paper

Industry Rankings by Patents per Paper:
1. {industry_ratios.sort('patents_per_paper_total', descending=True).row(0, named=True)['industry']}: {industry_ratios.sort('patents_per_paper_total', descending=True).row(0, named=True)['patents_per_paper_total']:.1f}
2. {industry_ratios.sort('patents_per_paper_total', descending=True).row(1, named=True)['industry']}: {industry_ratios.sort('patents_per_paper_total', descending=True).row(1, named=True)['patents_per_paper_total']:.1f}
3. {industry_ratios.sort('patents_per_paper_total', descending=True).row(2, named=True)['industry']}: {industry_ratios.sort('patents_per_paper_total', descending=True).row(2, named=True)['patents_per_paper_total']:.1f}

Industry Rankings by Papers per Patent:
1. {industry_ratios.sort('papers_per_patent_total', descending=True).row(0, named=True)['industry']}: {industry_ratios.sort('papers_per_patent_total', descending=True).row(0, named=True)['papers_per_patent_total']:.2f}
2. {industry_ratios.sort('papers_per_patent_total', descending=True).row(1, named=True)['industry']}: {industry_ratios.sort('papers_per_patent_total', descending=True).row(1, named=True)['papers_per_patent_total']:.2f}
3. {industry_ratios.sort('papers_per_patent_total', descending=True).row(2, named=True)['industry']}: {industry_ratios.sort('papers_per_patent_total', descending=True).row(2, named=True)['papers_per_patent_total']:.2f}
    """)

    logger.info("\n✅ Analysis complete!")


if __name__ == "__main__":
    main()
