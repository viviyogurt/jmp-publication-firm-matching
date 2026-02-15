"""
Stage 5: Create Institution-Firm Master Table

This script creates the master table by enriching matches with institution and firm metadata.

Process:
1. Load final matches from Stage 4
2. Join with institution master table
3. Join with Compustat firm data
4. Calculate statistics (papers per firm, coverage, etc.)
5. Output master table for analysis
"""

import polars as pl
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
FINAL_MATCHES = DATA_PROCESSED_LINK / "publication_firm_matches_final.parquet"
OUTPUT_FILE = DATA_INTERIM / "publication_institution_firm_master.parquet"
PROGRESS_LOG = LOGS_DIR / "create_publication_institution_firm_master.log"

DATA_INTERIM.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROGRESS_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 80)
    logger.info("STAGE 5: CREATE INSTITUTION-FIRM MASTER TABLE")
    logger.info("=" * 80)

    # Step 1: Load data
    logger.info("\n[1/4] Loading data...")

    if not FINAL_MATCHES.exists():
        raise FileNotFoundError(f"Final matches file not found: {FINAL_MATCHES}")
    if not INSTITUTIONS_MASTER.exists():
        raise FileNotFoundError(f"Institutions master file not found: {INSTITUTIONS_MASTER}")
    if not COMPUSTAT_STANDARDIZED.exists():
        raise FileNotFoundError(f"Compustat standardized file not found: {COMPUSTAT_STANDARDIZED}")

    matches = pl.read_parquet(FINAL_MATCHES)
    institutions = pl.read_parquet(INSTITUTIONS_MASTER)
    firms = pl.read_parquet(COMPUSTAT_STANDARDIZED)

    logger.info(f"  Loaded {len(matches):,} matches")
    logger.info(f"  Loaded {len(institutions):,} institutions")
    logger.info(f"  Loaded {len(firms):,} firms")

    # Step 2: Join with institution metadata
    logger.info("\n[2/4] Joining with institution metadata...")

    # Select institution columns to keep
    inst_cols = [
        'institution_id',
        'display_name',
        'normalized_name',
        'alternative_names',
        'acronyms',
        'ror_id',
        'wikidata_id',
        'homepage_url',
        'homepage_domain',
        'country_code',
        'geo_city',
        'geo_region',
        'geo_country',
        'paper_count',
    ]

    institutions_subset = institutions.select(inst_cols)

    master = matches.join(institutions_subset, on='institution_id', how='left')
    logger.info(f"  After institution join: {len(master):,} rows")

    # Step 3: Join with firm metadata
    logger.info("\n[3/4] Joining with firm metadata...")

    # Select firm columns to keep (avoid duplicate column names)
    firm_cols = [
        'GVKEY',
        'LPERMNO',
        'tic',
        'conm',
        'conml',
        'state',
        'city',
        'fic',
        'busdesc',
        'weburl',
        'conm_clean',
        'conml_clean',
    ]

    firms_subset = firms.select(firm_cols)

    # Rename columns to avoid conflicts
    firms_subset = firms_subset.rename({
        'LPERMNO': 'firm_LPERMNO',
        'tic': 'firm_tic',
        'conm': 'firm_conm_full',
        'conml': 'firm_conml',
        'state': 'firm_state',
        'city': 'firm_city',
        'fic': 'firm_fic',
        'busdesc': 'firm_busdesc',
        'weburl': 'firm_weburl',
        'conm_clean': 'firm_conm_clean',
        'conml_clean': 'firm_conml_clean',
    })

    master = master.join(firms_subset, on='GVKEY', how='left')
    logger.info(f"  After firm join: {len(master):,} rows")

    # Step 4: Calculate statistics
    logger.info("\n[4/4] Calculating statistics...")

    # Papers per firm
    papers_per_firm = master.group_by('GVKEY').agg(
        pl.col('paper_count').sum().alias('total_papers_per_firm')
    )
    master = master.join(papers_per_firm, on='GVKEY', how='left')

    # Institution coverage
    unique_institutions = institutions['institution_id'].n_unique()
    matched_institutions = master['institution_id'].n_unique()
    coverage_pct = (matched_institutions / unique_institutions) * 100

    logger.info(f"\nStatistics:")
    logger.info(f"  Total institutions in database: {unique_institutions:,}")
    logger.info(f"  Institutions matched: {matched_institutions:,}")
    logger.info(f"  Coverage: {coverage_pct:.2f}%")
    logger.info(f"  Unique firms matched: {master['GVKEY'].n_unique():,}")

    # Save output
    logger.info("\nSaving master table...")
    master.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved {len(master):,} rows to {OUTPUT_FILE}")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("MASTER TABLE SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total rows (institution-firm pairs): {len(master):,}")
    logger.info(f"Unique institutions: {master['institution_id'].n_unique():,}")
    logger.info(f"Unique firms: {master['GVKEY'].n_unique():,}")
    logger.info(f"Institution coverage: {coverage_pct:.2f}%")

    if 'total_papers_per_firm' in master.columns:
        total_papers = master['total_papers_per_firm'].sum()
        logger.info(f"\nTotal papers covered: {total_papers:,}")

    logger.info("\n" + "=" * 80)
    logger.info("STAGE 5 COMPLETE")
    logger.info("=" * 80)

    return master


if __name__ == "__main__":
    main()
