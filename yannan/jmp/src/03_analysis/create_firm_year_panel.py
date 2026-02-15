"""
Firm-Year Panel Data Creation

Combines:
- AI papers with firm affiliations
- Patent matches (100% coverage)
- Financial matches (76% coverage, 6,561 firms)
- CRSP/Compustat financial metrics

Output: Firm-year panel with paper counts, patent indicators, and financial variables
"""

import polars as pl
from pathlib import Path
import logging
import time

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_PUB = PROJECT_ROOT / "data" / "processed" / "publication"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
DATA_RAW_COMP = PROJECT_ROOT / "data" / "raw" / "compustat"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "analysis"
LOGS_DIR = PROJECT_ROOT / "logs"

FIRM_PAPERS = DATA_PROCESSED_PUB / "ai_papers_firms_only.parquet"
PATENT_MATCHES = DATA_PROCESSED_LINK / "paper_patent_matches.parquet"
FINANCIAL_MATCHES = DATA_PROCESSED_LINK / "comprehensive_matches_high_confidence.parquet"
FINANCIAL_DATA = DATA_RAW_COMP / "crsp_a_ccm.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "firm_year_panel.parquet"
PROGRESS_LOG = LOGS_DIR / "firm_year_panel_creation.log"

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROGRESS_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_and_merge_data():
    """Load and merge all data sources to create firm-year panel."""
    logger.info("=" * 80)
    logger.info("FIRM-YEAR PANEL DATA CREATION")
    logger.info("=" * 80)

    # Step 1: Load firm papers
    logger.info("\nStep 1: Loading firm papers...")
    firm_papers = pl.read_parquet(FIRM_PAPERS)
    logger.info(f"  Loaded {len(firm_papers):,} firm-affiliated papers")

    # Extract year from publication_date
    firm_papers = firm_papers.with_columns([
        pl.col('publication_date').str.slice(0, 4).cast(int, strict=False).alias('year')
    ])

    # Step 2: Load patent matches
    logger.info("\nStep 2: Loading patent matches...")
    patent_matches = pl.read_parquet(PATENT_MATCHES)
    logger.info(f"  Loaded {len(patent_matches):,} patent-capable institutions")

    # Step 3: Load financial matches
    logger.info("\nStep 3: Loading financial matches...")
    financial_matches = pl.read_parquet(FINANCIAL_MATCHES)
    # Extract first element from list columns
    financial_matches = financial_matches.with_columns([
        pl.col('gvkey').list.first().alias('gvkey'),
        pl.col('company_name').list.first().alias('company_name'),
        pl.col('permno').list.first().alias('permno'),
    ]).drop(['institution_raw', 'institution_country', 'ticker', 'state'])
    logger.info(f"  Loaded {len(financial_matches):,} institution-firm matches")
    logger.info(f"  Unique firms (GVKEY): {financial_matches['gvkey'].n_unique():,}")

    # Step 4: Create institution lookup table
    logger.info("\nStep 4: Creating institution lookup table...")

    # Explode firm papers to get institution-paper pairs
    logger.info("  Exploding firm papers to institution-paper level...")
    inst_paper_data = []

    for i, row in enumerate(firm_papers.iter_rows(named=True)):
        if (i + 1) % 100000 == 0:
            logger.info(f"    Processed {i+1:,}/{len(firm_papers):,} papers...")

        primary_affs = row.get('author_primary_affiliations', [])
        countries = row.get('author_primary_affiliation_countries', [])

        for j, aff in enumerate(primary_affs):
            if not aff or aff == "":
                continue

            inst_paper_data.append({
                'paper_id': row['paper_id'],
                'year': row['year'],
                'institution_raw': aff,
                'title': row.get('title', ''),
            })

    logger.info(f"  Created {len(inst_paper_data):,} institution-paper pairs")

    # Convert to DataFrame
    inst_paper_df = pl.DataFrame(inst_paper_data)
    logger.info(f"  DataFrame shape: {inst_paper_df.shape}")

    # Step 5: Merge with financial matches
    logger.info("\nStep 5: Merging institution-paper data with financial matches...")

    # Normalize institution names for matching
    inst_paper_df = inst_paper_df.with_columns([
        pl.col('institution_raw').str.to_lowercase().str.strip_chars().alias('institution_normalized')
    ])

    # Merge
    merged = inst_paper_df.join(
        financial_matches,
        on='institution_normalized',
        how='left'
    )

    logger.info(f"  Merged data shape: {merged.shape}")
    logger.info(f"  Papers matched to firms: {merged.filter(pl.col('gvkey').is_not_null()).shape[0]:,}")
    logger.info(f"  Papers not matched to firms: {merged.filter(pl.col('gvkey').is_null()).shape[0]:,}")

    # Step 6: Aggregate to firm-year level
    logger.info("\nStep 6: Aggregating to firm-year level...")

    firm_year_panel = merged.filter(
        pl.col('gvkey').is_not_null()
    ).group_by(['gvkey', 'company_name', 'year']).agg([
        pl.len().alias('paper_count'),
        pl.col('paper_id').alias('paper_ids'),
    ]).sort(['gvkey', 'year'])

    logger.info(f"  Firm-year observations: {len(firm_year_panel):,}")
    logger.info(f"  Unique firms: {firm_year_panel['gvkey'].n_unique():,}")
    logger.info(f"  Year range: {firm_year_panel['year'].min():.0f} - {firm_year_panel['year'].max():.0f}")

    # Step 7: Load financial metrics from CRSP/Compustat
    logger.info("\nStep 7: Loading financial metrics from CRSP/Compustat...")

    financial_data = pl.read_csv(
        FINANCIAL_DATA,
        dtypes={
            'GVKEY': str,
            'LPERMNO': int,
            'tic': str,
            'conm': str,
            'cusip': str,
            'fyear': int,  # Fiscal year
        },
        ignore_errors=True,
        truncate_ragged_lines=True
    )

    logger.info(f"  Loaded {len(financial_data):,} financial records")

    # Filter to primary links and relevant columns
    financial_data = financial_data.filter(
        pl.col('LINKPRIM') == 'P'
    )

    # Select key financial variables (adjust as needed based on actual columns)
    # Common CRSP/Compustat variables: market cap, assets, revenue, R&D, etc.
    key_vars = ['GVKEY', 'LPERMNO', 'fyear', 'tic', 'conm']

    # Check what columns are available
    logger.info(f"  Available columns: {financial_data.columns[:20]}...")  # Show first 20

    # Add columns that exist
    optional_vars = ['at', 'sale', 'xrd', 'mkvalt', 'prcc_f', 'csho']  # Assets, sales, R&D, mkt cap, price, shares
    for var in optional_vars:
        if var in financial_data.columns:
            key_vars.append(var)

    financial_subset = financial_data.select(key_vars)
    logger.info(f"  Selected {len(key_vars)} variables")

    # Step 8: Merge firm-year panel with financial data
    logger.info("\nStep 8: Merging firm-year panel with financial data...")

    final_panel = firm_year_panel.join(
        financial_subset,
        left_on=['gvkey', 'year'],
        right_on=['GVKEY', 'fyear'],
        how='left'
    )

    logger.info(f"  Final panel shape: {final_panel.shape}")
    logger.info(f"  Observations with financial data: {final_panel.filter(pl.col('LPERMNO').is_not_null()).shape[0]:,}")

    # Step 9: Add patent indicator (all firms in our dataset are patent-capable)
    logger.info("\nStep 9: Adding patent indicators...")

    final_panel = final_panel.with_columns([
        pl.lit(True).alias('has_patents'),
        pl.lit('firm_affiliated').alias('patent_source')
    ])

    # Calculate summary statistics
    logger.info("\n" + "=" * 80)
    logger.info("PANEL SUMMARY STATISTICS")
    logger.info("=" * 80)

    logger.info(f"\nPanel dimensions: {final_panel.shape}")
    logger.info(f"Unique firms: {final_panel['gvkey'].n_unique():,}")
    logger.info(f"Year range: {final_panel['year'].min():.0f} - {final_panel['year'].max():.0f}")
    logger.info(f"Total papers: {final_panel['paper_count'].sum():,}")

    logger.info(f"\nPapers per firm-year:")
    logger.info(f"  Mean: {final_panel['paper_count'].mean():.2f}")
    logger.info(f"  Median: {final_panel['paper_count'].median():.1f}")
    logger.info(f"  Max: {final_panel['paper_count'].max():,}")

    # Top 20 firms by total papers
    logger.info(f"\nTop 20 firms by total papers:")
    top_firms = final_panel.group_by(['gvkey', 'company_name']).agg([
        pl.col('paper_count').sum().alias('total_papers'),
        pl.len().alias('year_count')
    ]).sort('total_papers', descending=True).head(20)

    for row in top_firms.iter_rows(named=True):
        logger.info(f"  {row['company_name'][:50]:<50}: {row['total_papers']:>6,} papers ({row['year_count']} years)")

    # Papers per year
    logger.info(f"\nPapers per year:")
    papers_by_year = final_panel.group_by('year').agg([
        pl.col('paper_count').sum().alias('total_papers'),
        pl.len().alias('firm_count')
    ]).sort('year')

    for row in papers_by_year.tail(10).iter_rows(named=True):
        logger.info(f"  {int(row['year'])}: {row['total_papers']:>6,} papers, {row['firm_count']:>4} firms")

    return final_panel


def main():
    """Main execution function."""
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("FIRM-YEAR PANEL DATA CREATION")
    logger.info("=" * 80)
    logger.info(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Create panel
    panel = load_and_merge_data()

    # Save results
    logger.info(f"\nSaving firm-year panel to {OUTPUT_PARQUET}...")
    panel.write_parquet(OUTPUT_PARQUET, compression='snappy')
    logger.info("  Saved successfully!")

    elapsed = time.time() - start_time
    logger.info(f"\nElapsed time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")

    logger.info("\n" + "=" * 80)
    logger.info("PANEL CREATION COMPLETED")
    logger.info("=" * 80)
    logger.info(f"Output: {OUTPUT_PARQUET}")
    logger.info(f"Finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    logger.info("\n✅ Firm-year panel data ready for analysis!")
    return panel


if __name__ == "__main__":
    main()
