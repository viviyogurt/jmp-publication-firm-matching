"""
Fetch Compustat North America Fundamentals Annual Data from WRDS.

This script connects to WRDS and downloads Compustat Fundamentals Annual data
for the period 2000-2023 with specified filters and variables.

"""

import wrds
import pandas as pd
import numpy as np
from pathlib import Path
import sys
from datetime import datetime
import logging

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "compustat"
OUTPUT_FILE = DATA_RAW / "raw_funda.parquet"

# Ensure output directory exists
DATA_RAW.mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# SQL Query Definition
# ============================================================================
SQL_QUERY = """
SELECT 
    -- Identifiers
    gvkey,
    conm,
    datadate,
    fyear,
    sic,
    naics,
    
    -- Size/Performance
    at,
    sale,
    ni,
    oibdp,
    capx,
    
    -- Innovation/Intangibles
    xrd,
    xsga,
    intan,
    ppent,
    
    -- Valuation
    prcc_f,
    csho,
    dltt,
    dlc,
    pstk,
    ceq,
    txdb,
    
    -- Software/IT (if available)
    xstf
    
FROM comp.funda
WHERE 
    datadate >= '2000-01-01' 
    AND datadate <= '2023-12-31'
    AND indfmt = 'INDL'
    AND datafmt = 'STD'
    AND popsrc = 'D'
    AND consol = 'C'
ORDER BY gvkey, datadate
"""


# ============================================================================
# Helper Functions
# ============================================================================
def connect_to_wrds(wrds_username: str = None) -> wrds.Connection:
    """
    Establish connection to WRDS database.
    
    Parameters:
    -----------
    wrds_username : str, optional
        WRDS username. If not provided, will attempt to use stored credentials
        or prompt for username.
    
    Returns:
    --------
    wrds.Connection
        Active WRDS database connection
    
    Raises:
    -------
    ConnectionError
        If connection to WRDS fails
    """
    logger.info("Connecting to WRDS...")
    
    try:
        if wrds_username:
            db = wrds.Connection(wrds_username=wrds_username)
        else:
            # Try to connect using stored credentials
            db = wrds.Connection()
        logger.info("Successfully connected to WRDS")
        return db
    except Exception as e:
        logger.error(f"Failed to connect to WRDS: {str(e)}")
        raise ConnectionError(f"Could not establish WRDS connection: {str(e)}")


def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize data types for the Compustat dataframe.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Raw dataframe from WRDS query
    
    Returns:
    --------
    pd.DataFrame
        Dataframe with optimized data types
    """
    logger.info("Optimizing data types...")
    
    df = df.copy()
    
    # Convert datadate to datetime
    if 'datadate' in df.columns:
        df['datadate'] = pd.to_datetime(df['datadate'], errors='coerce')
    
    # Convert year to integer if present
    if 'fyear' in df.columns:
        df['fyear'] = pd.to_numeric(df['fyear'], errors='coerce').astype('Int64')
    
    # Convert SIC and NAICS to string (preserve leading zeros)
    if 'sic' in df.columns:
        df['sic'] = df['sic'].astype('Int64').astype(str).replace('<NA>', None)
    if 'naics' in df.columns:
        df['naics'] = df['naics'].astype('Int64').astype(str).replace('<NA>', None)
    
    # Convert gvkey to string (preserve format)
    if 'gvkey' in df.columns:
        df['gvkey'] = df['gvkey'].astype(str)
    
    # Convert company name to string
    if 'conm' in df.columns:
        df['conm'] = df['conm'].astype(str)
    
    # Convert financial variables to float32 for memory efficiency
    # (sufficient precision for financial data)
    financial_vars = [
        'at', 'sale', 'ni', 'oibdp', 'capx',
        'xrd', 'xsga', 'intan', 'ppent',
        'prcc_f', 'csho', 'dltt', 'dlc', 'pstk', 'ceq', 'txdb', 'xstf'
    ]
    
    for var in financial_vars:
        if var in df.columns:
            df[var] = pd.to_numeric(df[var], errors='coerce').astype('float32')
    
    logger.info(f"Data types optimized. Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    return df


def fetch_compustat_data(db: wrds.Connection, query: str) -> pd.DataFrame:
    """
    Execute SQL query and fetch data from WRDS.
    
    Parameters:
    -----------
    db : wrds.Connection
        Active WRDS database connection
    query : str
        SQL query string
    
    Returns:
    --------
    pd.DataFrame
        Query results as pandas DataFrame
    
    Raises:
    -------
    RuntimeError
        If query execution fails
    """
    logger.info("Executing SQL query...")
    logger.info(f"Query filters: 2000-2023, INDL, STD, D, C")
    
    try:
        df = db.raw_sql(query, date_cols=['datadate'])
        logger.info(f"Query executed successfully. Retrieved {len(df):,} observations")
        logger.info(f"Date range: {df['datadate'].min()} to {df['datadate'].max()}")
        logger.info(f"Unique companies (gvkey): {df['gvkey'].nunique():,}")
        return df
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        raise RuntimeError(f"Failed to execute SQL query: {str(e)}")


def save_to_parquet(df: pd.DataFrame, output_path: Path) -> None:
    """
    Save dataframe to parquet file with compression.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Dataframe to save
    output_path : Path
        Output file path
    """
    logger.info(f"Saving data to {output_path}...")
    
    try:
        df.to_parquet(
            output_path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        logger.info(f"Data successfully saved to {output_path}")
        logger.info(f"File size: {output_path.stat().st_size / 1024**2:.2f} MB")
    except Exception as e:
        logger.error(f"Failed to save data: {str(e)}")
        raise RuntimeError(f"Failed to save parquet file: {str(e)}")


# ============================================================================
# Main Execution
# ============================================================================
def main(wrds_username: str = None):
    """
    Main execution function.
    
    Parameters:
    -----------
    wrds_username : str, optional
        WRDS username. If not provided, will use stored credentials.
    """
    logger.info("=" * 60)
    logger.info("Compustat Fundamentals Annual Data Fetch")
    logger.info("=" * 60)
    
    db = None
    try:
        # Step 1: Connect to WRDS
        db = connect_to_wrds(wrds_username)
        
        # Step 2: Execute query
        df = fetch_compustat_data(db, SQL_QUERY)
        
        # Step 3: Optimize data types
        df = optimize_dtypes(df)
        
        # Step 4: Save to parquet
        save_to_parquet(df, OUTPUT_FILE)
        
        # Step 5: Print summary statistics
        logger.info("\n" + "=" * 60)
        logger.info("Data Summary")
        logger.info("=" * 60)
        logger.info(f"Total observations: {len(df):,}")
        logger.info(f"Unique companies (gvkey): {df['gvkey'].nunique():,}")
        logger.info(f"Date range: {df['datadate'].min()} to {df['datadate'].max()}")
        logger.info(f"Columns: {', '.join(df.columns.tolist())}")
        logger.info("\nMissing values per column:")
        missing = df.isnull().sum()
        for col in df.columns:
            if missing[col] > 0:
                pct = 100 * missing[col] / len(df)
                logger.info(f"  {col}: {missing[col]:,} ({pct:.1f}%)")
        
        logger.info("\n" + "=" * 60)
        logger.info("SUCCESS: Data fetch completed successfully!")
        logger.info("=" * 60)
        
    except (ConnectionError, RuntimeError) as e:
        logger.error(f"\n{'=' * 60}")
        logger.error("ERROR: Data fetch failed!")
        logger.error(f"{'=' * 60}")
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n{'=' * 60}")
        logger.error("UNEXPECTED ERROR:")
        logger.error(f"{'=' * 60}")
        logger.error(str(e), exc_info=True)
        sys.exit(1)
    finally:
        if db is not None:
            try:
                db.close()
                logger.info("WRDS connection closed")
            except:
                pass


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fetch Compustat Fundamentals Annual data from WRDS"
    )
    parser.add_argument(
        '--username',
        type=str,
        default=None,
        help='WRDS username (optional, will use stored credentials if not provided)'
    )
    
    args = parser.parse_args()
    main(wrds_username=args.username)

