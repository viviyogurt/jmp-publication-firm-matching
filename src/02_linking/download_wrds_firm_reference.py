"""
Download WRDS Firm Reference Data

This script connects to WRDS and downloads comprehensive firm reference data
including CRSP (permno, company names, historical names), Compustat (gvkey, conm, tic),
and CCM linking table.

Output: data/interim/wrds_firm_reference.parquet

WRDS Credentials:
    Username: sunyanna
    Password: ******

Date: 2025
"""

import wrds
import polars as pl
from pathlib import Path
import logging
import time
from typing import Dict, List

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
LOGS_DIR = PROJECT_ROOT / "logs"

OUTPUT_PARQUET = DATA_INTERIM / "wrds_firm_reference.parquet"
PROGRESS_LOG = LOGS_DIR / "wrds_download_progress.log"

# WRDS Credentials
WRDS_USERNAME = "sunyanna"

# Ensure directories exist
DATA_INTERIM.mkdir(parents=True, exist_ok=True)
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


def download_wrds_data():
    """Download comprehensive firm reference data from WRDS."""
    logger.info("=" * 80)
    logger.info("DOWNLOADING WRDS FIRM REFERENCE DATA")
    logger.info("=" * 80)

    # Connect to WRDS
    logger.info(f"\nConnecting to WRDS (username: {WRDS_USERNAME})...")
    try:
        # Auto-create ~/.wrds/password file with credentials
        import os
        wrds_path = Path.home() / '.wrds'
        wrds_path.mkdir(exist_ok=True)
        password_file = wrds_path / 'password'

        # Write password to file (WRDS standard method)
        with open(password_file, 'w') as f:
            f.write('Emilyyogurt11\n')

        os.chmod(password_file, 0o600)  # Restrict permissions

        # Now connect
        db = wrds.Connection(wrds_username=WRDS_USERNAME)
        logger.info("  Connected successfully!")
    except Exception as e:
        logger.error(f"  Failed to connect: {e}")
        raise

    all_data = {}

    # ============================================================================
    # 1. CRSP Stock Names (Current and Historical)
    # ============================================================================
    logger.info("\n" + "-" * 80)
    logger.info("1. DOWNLOADING CRSP STOCK NAMES")
    logger.info("-" * 80)

    logger.info("  Querying crsp.stocknames...")
    crsp_names_sql = """
        SELECT permno, comnam, ncusip, ticker, namedt, namendt
        FROM crsp.stocknames
        WHERE namedt <= '2024-12-31'
        ORDER BY permno, namedt
    """

    try:
        crsp_names = db.raw_sql(crsp_names_sql, date_cols=['namedt', 'namendt'])
        logger.info(f"  Downloaded {len(crsp_names):,} CRSP name records")
        all_data['crsp_names'] = crsp_names
    except Exception as e:
        logger.error(f"  Error downloading CRSP names: {e}")

    # ============================================================================
    # 2. CRSP Daily Stock File Headers (for additional identifiers)
    # ============================================================================
    logger.info("\n" + "-" * 80)
    logger.info("2. DOWNLOADING CRSP DSENAMES")
    logger.info("-" * 80)

    logger.info("  Querying crsp.dsenames...")
    crsp_dsenames_sql = """
        SELECT permno, comnam, ncusip, ticker, shrcd, exchcd, siccd,
               namedt, namendt, msedt, msendt
        FROM crsp.dsenames
        WHERE namedt <= '2024-12-31'
        ORDER BY permno, namedt
    """

    try:
        crsp_dsenames = db.raw_sql(crsp_dsenames_sql, date_cols=['namedt', 'namendt', 'msedt', 'msendt'])
        logger.info(f"  Downloaded {len(crsp_dsenames):,} CRSP dsename records")
        all_data['crsp_dsenames'] = crsp_dsenames
    except Exception as e:
        logger.error(f"  Error downloading CRSP dsenames: {e}")

    # ============================================================================
    # 3. Compustat Company Names
    # ============================================================================
    logger.info("\n" + "-" * 80)
    logger.info("3. DOWNLOADING COMPUSTAT COMPANY NAMES")
    logger.info("-" * 80)

    logger.info("  Querying comp.names...")
    compustat_names_sql = """
        SELECT gvkey, conml, conm, tic, cusip, sic, naics,
               costat, conml_dt, busdt
        FROM comp.names
        WHERE conml_dt <= '2024-12-31'
        ORDER BY gvkey, conml_dt
    """

    try:
        compustat_names = db.raw_sql(compustat_names_sql, date_cols=['conml_dt', 'busdt'])
        logger.info(f"  Downloaded {len(compustat_names):,} Compustat name records")
        all_data['compustat_names'] = compustat_names
    except Exception as e:
        logger.error(f"  Error downloading Compustat names: {e}")

    # ============================================================================
    # 4. Compustat Company Information
    # ============================================================================
    logger.info("\n" + "-" * 80)
    logger.info("4. DOWNLOADING COMPUSTAT COMPANY INFORMATION")
    logger.info("-" * 80)

    logger.info("  Querying comp.company...")
    compustat_company_sql = """
        SELECT gvkey, conm, tic, cusip, cik, fic, loc, naics, sic,
               gind, gsector, gsubind, zip, state, city, address,
               costat, incorporated, idbflag
        FROM comp.company
    """

    try:
        compustat_company = db.raw_sql(compustat_company_sql)
        logger.info(f"  Downloaded {len(compustat_company):,} Compustat company records")
        all_data['compustat_company'] = compustat_company
    except Exception as e:
        logger.error(f"  Error downloading Compustat company: {e}")

    # ============================================================================
    # 5. CRSP-Compustat Merged (CCM) Linking Table
    # ============================================================================
    logger.info("\n" + "-" * 80)
    logger.info("5. DOWNLOADING CCM LINKING TABLE")
    logger.info("-" * 80)

    logger.info("  Querying crsp.ccmxpf_lnkhist...")
    ccm_link_sql = """
        SELECT gvkey, lpermno AS permno, linktype, linkprim,
               linkdt, enddt
        FROM crsp.ccmxpf_lnkhist
        WHERE linktype IN ('LC', 'LU')  # Use only reliable links
        ORDER BY gvkey, lpermno, linkdt
    """

    try:
        ccm_link = db.raw_sql(ccm_link_sql, date_cols=['linkdt', 'enddt'])
        logger.info(f"  Downloaded {len(ccm_link):,} CCM link records")
        all_data['ccm_link'] = ccm_link
    except Exception as e:
        logger.error(f"  Error downloading CCM links: {e}")

    # ============================================================================
    # 6. CIK to GVKEY Mapping (for SEC-based matching)
    # ============================================================================
    logger.info("\n" + "-" * 80)
    logger.info("6. DOWNLOADING CIK MAPPINGS")
    logger.info("-" * 80)

    logger.info("  Querying comp.company for CIKs...")
    cik_mapping_sql = """
        SELECT gvkey, tic, cik, conm
        FROM comp.company
        WHERE cik IS NOT NULL
    """

    try:
        cik_mapping = db.raw_sql(cik_mapping_sql)
        logger.info(f"  Downloaded {len(cik_mapping):,} CIK mappings")
        all_data['cik_mapping'] = cik_mapping
    except Exception as e:
        logger.error(f"  Error downloading CIK mappings: {e}")

    # Close connection
    db.close()
    logger.info("\n  WRDS connection closed")

    # ============================================================================
    # Merge and Save Data
    # ============================================================================
    logger.info("\n" + "=" * 80)
    logger.info("MERGING AND SAVING DATA")
    logger.info("=" * 80)

    # Convert pandas dataframes to polars
    logger.info("\nConverting to Polars format...")
    polars_data = {}
    for key, df in all_data.items():
        polars_data[key] = pl.from_pandas(df)
        logger.info(f"  {key}: {len(df):,} rows")

    # Create unified firm reference
    logger.info("\nCreating unified firm reference table...")

    # Start with Compustat company as base
    if 'compustat_company' in polars_data:
        base_df = polars_data['compustat_company'].clone()
        logger.info(f"  Base: Compustat company ({len(base_df):,} rows)")
    else:
        logger.error("  No Compustat company data available!")
        return

    # Add CRSP information via CCM links
    if 'ccm_link' in polars_data and 'crsp_dsenames' in polars_data:
        # Link Compustat -> CCM -> CRSP
        ccm = polars_data['ccm_link']
        crsp = polars_data['crsp_dsenames']

        # Get latest CRSP name for each PERMNO
        latest_crsp = crsp.sort('namedt', descending=True).unique(subset=['permno'], keep='first')

        # Join CCM to get GVKEY-PERMNO mapping
        ccm_permno = ccm.filter(pl.col('linkprim') == 'P').select(['gvkey', 'permno']).unique()

        # Merge with base
        base_df = base_df.join(ccm_permno, on='gvkey', how='left')
        logger.info(f"  After CCM join: {len(base_df):,} rows")

    # Save to parquet
    logger.info(f"\nSaving to {OUTPUT_PARQUET}...")
    base_df.write_parquet(OUTPUT_PARQUET, compression='snappy')
    logger.info(f"  Saved {len(base_df):,} firms")

    # Also save individual components for reference
    logger.info("\nSaving individual components...")
    components_dir = DATA_INTERIM / "wrds_components"
    components_dir.mkdir(exist_ok=True)

    for key, df_pl in polars_data.items():
        output_path = components_dir / f"{key}.parquet"
        df_pl.write_parquet(output_path, compression='snappy')
        logger.info(f"  Saved {key} to {output_path.name}")

    # Generate statistics
    logger.info("\n" + "=" * 80)
    logger.info("WRDS FIRM REFERENCE STATISTICS")
    logger.info("=" * 80)

    logger.info(f"\nUnique Compustat firms (GVKEYs): {base_df['gvkey'].n_unique():,}")
    if 'permno' in base_df.columns:
        logger.info(f"Unique CRSP firms (PERMNOs): {base_df['permno'].n_unique():,}")
    logger.info(f"Unique tickers: {base_df['tic'].n_unique():,}")
    logger.info(f"Unique CIKs: {base_df['cik'].n_unique():,}")

    logger.info("\nTop 20 states by firm count:")
    if 'state' in base_df.columns:
        state_dist = base_df.group_by('state').agg(pl.len().alias('count')).sort('count', descending=True)
        for row in state_dist.head(20).iter_rows(named=True):
            logger.info(f"  {row['state']}: {row['count']:,} firms")

    logger.info("\nTop 20 industries (SIC) by firm count:")
    if 'sic' in base_df.columns:
        sic_dist = base_df.group_by('sic').agg(pl.len().alias('count')).sort('count', descending=True)
        for row in sic_dist.head(20).iter_rows(named=True):
            logger.info(f"  SIC {row['sic']}: {row['count']:,} firms")

    return base_df


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("WRDS FIRM REFERENCE DOWNLOAD")
    logger.info("=" * 80)
    logger.info(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    firm_df = download_wrds_data()

    logger.info("\n" + "=" * 80)
    logger.info("DOWNLOAD COMPLETED")
    logger.info("=" * 80)
    logger.info(f"Total firms: {len(firm_df):,}")
    logger.info(f"Output saved to: {OUTPUT_PARQUET}")
    logger.info(f"Finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
