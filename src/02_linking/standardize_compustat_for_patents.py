"""
Standardize Compustat Firm Names for Patent Matching

This script standardizes CRSP/Compustat firm names to match the normalization
used for patent assignee names, enabling efficient matching.

Following Arora et al. (2021) methodology for name standardization.
"""

import polars as pl
import re
from pathlib import Path
import logging
from typing import List, Optional, Union

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW_COMP = PROJECT_ROOT / "data" / "raw" / "compustat"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
LOGS_DIR = PROJECT_ROOT / "logs"

FINANCIAL_DATA = DATA_RAW_COMP / "crsp_a_ccm.csv"
OUTPUT_FILE = DATA_INTERIM / "compustat_firms_standardized.parquet"
PROGRESS_LOG = LOGS_DIR / "standardize_compustat_for_patents.log"

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


def clean_organization_name(name: Optional[str]) -> Optional[str]:
    """
    Standardize organization names (same as patent assignee normalization):
    - Convert to uppercase
    - Remove common suffixes (INC, LTD, CORP, LLC, CO, etc.)
    - Remove punctuation
    - Strip extra whitespace
    """
    if name is None or name == "":
        return None
    
    # Convert to uppercase
    name = name.upper()
    
    # Remove common corporate suffixes (order matters - longer patterns first)
    suffixes_pattern = r'\b(INCORPORATED|CORPORATION|COMPANY|LIMITED|L\.?L\.?C\.?|INC\.?|LTD\.?|CORP\.?|CO\.?|PLC\.?|S\.?A\.?|A\.?G\.?|GMBH|N\.?V\.?|B\.?V\.?)\b'
    name = re.sub(suffixes_pattern, '', name)
    
    # Remove punctuation (keep alphanumeric and spaces)
    name = re.sub(r'[^\w\s]', '', name)
    
    # Collapse multiple spaces and strip
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name if name else None


def create_name_variants(conm: Optional[str], conml: Optional[str], tic: Optional[str]) -> List[str]:
    """
    Create name variants for a firm to improve matching.
    Includes abbreviations and common variations.
    """
    variants = set()
    
    # Add cleaned company name
    if conm:
        cleaned = clean_organization_name(conm)
        if cleaned:
            variants.add(cleaned)
    
    # Add cleaned legal name
    if conml:
        cleaned = clean_organization_name(conml)
        if cleaned:
            variants.add(cleaned)
    
    # Add ticker if available
    if tic and tic.strip():
        variants.add(tic.strip().upper())
    
    # Common abbreviation patterns
    if conm:
        # Extract first letters of words (e.g., "International Business Machines" -> "IBM")
        words = conm.split()
        if len(words) >= 2:
            abbrev = ''.join([w[0] for w in words if w and len(w) > 0])
            if len(abbrev) >= 2:
                variants.add(abbrev)
    
    return sorted(list(variants))


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("STANDARDIZING COMPUSTAT FIRM NAMES FOR PATENT MATCHING")
    logger.info("=" * 80)
    
    # Step 1: Load CRSP/Compustat data
    logger.info("\n[1/4] Loading CRSP/Compustat data...")
    logger.info(f"Reading from: {FINANCIAL_DATA}")
    
    if not FINANCIAL_DATA.exists():
        raise FileNotFoundError(f"Financial data file not found: {FINANCIAL_DATA}")
    
    crsp_df = pl.read_csv(
        FINANCIAL_DATA,
        dtypes={
            'GVKEY': str,
            'LPERMNO': pl.Int64,
            'tic': str,
            'conm': str,  # Company name
            'conml': str,  # Legal company name
            'cusip': str,
            'cik': str,
            'incorp': str,
            'state': str,
            'city': str,
            'zip': str,
            'sic': pl.Int64,
            'busdesc': str,
            'fyear': pl.Int64,
            'fic': str,
            'weburl': str,
        },
        ignore_errors=True,
        truncate_ragged_lines=True
    )
    
    logger.info(f"  Loaded {len(crsp_df):,} records")
    
    # Step 2: Filter to primary links and get unique firms
    logger.info("\n[2/4] Filtering to primary links and extracting unique firms...")
    
    unique_firms = crsp_df.filter(
        pl.col('LINKPRIM') == 'P'
    ).select([
        'GVKEY', 'LPERMNO', 'tic', 'conm', 'conml',
        'state', 'city', 'incorp', 'fic', 'busdesc', 'weburl'
    ]).unique()
    
    logger.info(f"  Found {len(unique_firms):,} unique firms")
    
    # Step 3: Standardize names
    logger.info("\n[3/4] Standardizing firm names...")
    
    # Apply name cleaning using map_elements
    firms_with_clean = unique_firms.with_columns([
        pl.col('conm').map_elements(clean_organization_name, return_dtype=pl.Utf8).alias('conm_clean'),
        pl.col('conml').map_elements(clean_organization_name, return_dtype=pl.Utf8).alias('conml_clean'),
    ])
    
    # Create name variants for each firm
    logger.info("  Creating name variants...")
    firms_list = []
    for row in firms_with_clean.iter_rows(named=True):
        variants = create_name_variants(
            row.get('conm', ''),
            row.get('conml', ''),
            row.get('tic', '')
        )
        row['name_variants'] = variants
        firms_list.append(row)
    
    # Convert back to DataFrame
    firms_final = pl.DataFrame(firms_list)
    
    # Convert name_variants list to string representation for parquet storage
    # (Polars can handle lists, but we'll keep it as list for now)
    
    logger.info(f"  Standardized {len(firms_final):,} firms")
    logger.info(f"  Firms with conm_clean: {firms_final.filter(pl.col('conm_clean').is_not_null()).shape[0]:,}")
    logger.info(f"  Firms with conml_clean: {firms_final.filter(pl.col('conml_clean').is_not_null()).shape[0]:,}")
    
    # Step 4: Save output
    logger.info("\n[4/4] Saving standardized firm data...")
    logger.info(f"Output: {OUTPUT_FILE}")
    
    firms_final.write_parquet(OUTPUT_FILE, compression='snappy')
    
    logger.info(f"  Saved {len(firms_final):,} standardized firms")
    
    # Summary statistics
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY STATISTICS")
    logger.info("=" * 80)
    logger.info(f"Total unique firms: {len(firms_final):,}")
    logger.info(f"Firms with ticker: {firms_final.filter(pl.col('tic').is_not_null()).shape[0]:,}")
    logger.info(f"Firms with business description: {firms_final.filter(pl.col('busdesc').is_not_null()).shape[0]:,}")
    logger.info(f"Firms with website: {firms_final.filter(pl.col('weburl').is_not_null()).shape[0]:,}")
    
    # Sample output
    logger.info("\nSample of standardized firms:")
    sample = firms_final.head(10)
    for row in sample.iter_rows(named=True):
        logger.info(f"  {row['conm'][:50]:<50} -> {row['conm_clean']}")
    
    logger.info("\n" + "=" * 80)
    logger.info("STANDARDIZATION COMPLETE")
    logger.info("=" * 80)
    
    return firms_final


if __name__ == "__main__":
    main()
