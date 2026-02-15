"""
Manual Top Company Mappings

Match major research companies that automated methods miss due to:
- Brand vs corporate name differences (Google → Alphabet)
- Subsidiary structures (Microsoft Research → Microsoft)
- Different domains (google.com ≠ abc.xyz)

These are MANUAL mappings for top 60 institutions by paper count.
Quality: 100% accuracy (manually verified).
Target: +60 firms covering >250,000 papers.
"""

import polars as pl
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

MANUAL_MAPPINGS_FILE = DATA_INTERIM / "manual_top_company_mappings.csv"
INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_manual.parquet"

DATA_PROCESSED_LINK.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "match_manual_top_companies.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Confidence for manual mappings
MANUAL_CONFIDENCE = 0.99  # Very high - manually verified


def load_manual_mappings():
    """Load manual mappings from CSV."""
    logger.info("[1/4] Loading manual mappings...")

    mappings_df = pl.read_csv(MANUAL_MAPPINGS_FILE)
    logger.info(f"  Loaded {len(mappings_df):,} manual mappings")

    # Show sample
    logger.info("\n  Sample mappings:")
    for i, row in enumerate(mappings_df.head(10).iter_rows(named=True), 1):
        logger.info(f"    {i}. {row['institution_display_name'][:50]:<50} → {row['firm_conm'][:30]}")

    return mappings_df


def load_and_enrich_data(mappings_df: pl.DataFrame):
    """Load institutions and firms, enrich mappings with metadata."""
    logger.info("\n[2/4] Loading institutions and firms...")

    institutions = pl.read_parquet(INSTITUTIONS_MASTER)
    firms = pl.read_parquet(COMPUSTAT_STANDARDIZED)

    logger.info(f"  Loaded {len(institutions):,} institutions")
    logger.info(f"  Loaded {len(firms):,} firms")

    # Enrich mappings with institution metadata
    logger.info("\n  Enriching mappings with metadata...")

    # Standardize column names and types
    mappings_df = mappings_df.rename({
        'gvkey': 'GVKEY'
    })
    # GVKEY should be string to match firms dataframe
    mappings_df = mappings_df.with_columns(
        pl.col('GVKEY').cast(pl.String).str.pad_start(6, '0')
    )

    mappings_enriched = mappings_df.join(
        institutions.select(['institution_id', 'paper_count', 'homepage_domain', 'country_code', 'display_name']),
        on='institution_id',
        how='left'
    )

    # Enrich with firm metadata
    mappings_enriched = mappings_enriched.join(
        firms.select(['GVKEY', 'LPERMNO', 'weburl', 'fic', 'conm_clean']),
        on='GVKEY',
        how='left'
    )

    logger.info(f"  Enriched {len(mappings_enriched):,} mappings")

    return mappings_enriched


def create_match_records(mappings_df: pl.DataFrame):
    """Create match records in standard format."""
    logger.info("\n[3/4] Creating match records...")

    match_records = mappings_df.select([
        pl.col('GVKEY'),
        pl.col('LPERMNO'),
        pl.col('firm_conm').alias('firm_conm'),
        pl.col('institution_id'),
        pl.col('institution_display_name'),
        pl.lit('manual_mapping').alias('match_type'),
        pl.lit(MANUAL_CONFIDENCE).alias('match_confidence'),
        pl.lit('manual_top_company').alias('match_method'),
        pl.col('paper_count').alias('institution_paper_count'),
    ])

    logger.info(f"  Created {len(match_records):,} match records")
    logger.info(f"  Unique firms: {match_records['GVKEY'].n_unique():,}")
    logger.info(f"  Total papers: {match_records['institution_paper_count'].sum():,}")

    return match_records


def save_and_summarize(matches_df: pl.DataFrame, mappings_df: pl.DataFrame):
    """Save matches and generate summary."""
    logger.info("\n[4/4] Saving and summarizing...")

    # Save
    matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved to: {OUTPUT_FILE}")

    # Calculate coverage statistics
    total_papers = matches_df['institution_paper_count'].sum()
    unique_firms = matches_df['GVKEY'].n_unique()
    unique_institutions = matches_df['institution_id'].n_unique()

    # Top institutions by paper count
    logger.info("\n" + "=" * 80)
    logger.info("MANUAL MAPPING SUMMARY")
    logger.info("=" * 80)
    logger.info(f"\nTotal matches: {len(matches_df):,}")
    logger.info(f"Unique firms: {unique_firms:,}")
    logger.info(f"Unique institutions: {unique_institutions:,}")
    logger.info(f"Total papers covered: {total_papers:,}")

    logger.info("\nTop 20 institutions by paper count:")
    top_institutions = mappings_df.sort('paper_count', descending=True).head(20)
    for i, row in enumerate(top_institutions.iter_rows(named=True), 1):
        papers = f"{row['paper_count']:>6,}" if row['paper_count'] is not None else "    N/A"
        logger.info(f"  {i}. {row['institution_display_name'][:50]:<50}")
        logger.info(f"     Papers: {papers} → {row['firm_conm'][:40]}")

    # Coverage comparison
    logger.info("\n" + "=" * 80)
    logger.info("COVERAGE IMPACT")
    logger.info("=" * 80)
    logger.info(f"\nThese {unique_firms} firms were previously UNMATCHED")
    logger.info(f"but represent {total_papers:,} publications ({total_papers/1750000*100:.1f}% of all publications)")

    logger.info("\n" + "=" * 80)


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("MANUAL TOP COMPANY MAPPINGS")
    logger.info("=" * 80)

    # Load mappings
    mappings_df = load_manual_mappings()

    # Load and enrich with metadata
    mappings_enriched = load_and_enrich_data(mappings_df)

    # Create match records
    match_records = create_match_records(mappings_enriched)

    # Save and summarize
    save_and_summarize(match_records, mappings_enriched)

    logger.info("\nMANUAL MAPPING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
