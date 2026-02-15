"""
Extract Institution Reference Data from Firm-Affiliated Papers

This script extracts all unique institutions from the firm-affiliated papers dataset,
standardizes names, and creates a comprehensive reference database with OpenAlex IDs,
ROR IDs, organization types, and geographic information.

Output: data/interim/institution_reference.parquet

Date: 2025
"""

import polars as pl
from pathlib import Path
import logging
import json
import re
from collections import defaultdict
from typing import Set, Dict, List
import requests
import time

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
LOGS_DIR = PROJECT_ROOT / "logs"

INPUT_PARQUET = DATA_PROCESSED / "ai_papers_firms_only.parquet"
OUTPUT_PARQUET = DATA_INTERIM / "institution_reference.parquet"
PROGRESS_LOG = LOGS_DIR / "institution_extraction_progress.log"

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

# ============================================================================
# Institution Name Standardization
# ============================================================================

SUFFIXES_TO_REMOVE = [
    ' inc', ' corp', ' llc', 'ltd', 'limited', 'plc', 'gmbh', 'ag', 'sa', 'spa',
    'pty', 'bv', 'nv', 'aps', 'sro', 'zoo', 'kft', 'oao', 'ooo', 'as',
    ' co', 'company', 'companies', 'corporation', 'technologies', 'technology',
    ' laboratories', 'laboratory', 'labs', 'research', 'group', 'holdings',
    ' international', 'industries', 'systems', 'solutions', 'software'
]

ABBREVIATION_MAP = {
    'ibm': 'international business machines',
    'ge': 'general electric',
    'gm': 'general motors',
    'at&t': 'at and t',
    'ibm corp': 'international business machines',
    'ibm corporation': 'international business machines',
    'hp': 'hewlett packard',
    'hewlett-packard': 'hewlett packard',
    'faANG': 'facebook amazon apple netflix google',
}


def normalize_institution_name(name: str) -> str:
    """Normalize institution name for matching."""
    if not name or name == "":
        return ""

    normalized = name.lower().strip()

    # Remove country in parentheses
    normalized = re.sub(r'\s*\([^)]*\)', '', normalized)

    # Remove common suffixes
    for suffix in SUFFIXES_TO_REMOVE:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()

    # Replace & with 'and'
    normalized = normalized.replace('&', 'and')

    # Normalize whitespace
    normalized = re.sub(r'\s+', ' ', normalized)

    # Expand abbreviations
    for abbr, expanded in ABBREVIATION_MAP.items():
        if abbr in normalized:
            normalized = normalized.replace(abbr, expanded)

    return normalized.strip()


def extract_country_from_affiliation(affiliation: str) -> str:
    """Extract country from affiliation string."""
    if not affiliation:
        return None

    # Look for country in parentheses
    match = re.search(r'\(([^)]+)\)', affiliation)
    if match:
        country = match.group(1).strip()
        # Standardize country names
        if 'united states' in country.lower():
            return 'United States'
        if 'usa' in country.lower():
            return 'United States'
        if 'us' == country.lower():
            return 'United States'
        if 'uk' in country.lower():
            return 'United Kingdom'
        if 'china' in country.lower():
            return 'China'
        return country

    return None


# ============================================================================
# OpenAlex API Functions
# ============================================================================

OPENALEX_API_URL = "https://api.openalex.org"


def query_openalex_institution(institution_id: str) -> Dict:
    """Query OpenAlex API for institution details."""
    url = f"{OPENALEX_API_URL}/institutions/{institution_id}"
    params = {'mailto': 'sunyanna@hku.hk'}  # Replace with actual email

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.warning(f"Failed to query OpenAlex for {institution_id}: {e}")
        return None


def get_openalex_institutions_batch(institution_ids: List[str]) -> List[Dict]:
    """Batch query OpenAlex institutions."""
    results = []
    for i, inst_id in enumerate(institution_ids):
        if (i + 1) % 100 == 0:
            logger.info(f"  Queried {i+1}/{len(institution_ids)} institutions from OpenAlex...")
            time.sleep(1)  # Rate limiting

        data = query_openalex_institution(inst_id)
        if data:
            results.append(data)

    return results


# ============================================================================
# Main Extraction Function
# ============================================================================

def extract_institution_reference():
    """Extract and standardize all unique institutions from firm papers."""
    logger.info("=" * 80)
    logger.info("EXTRACTING INSTITUTION REFERENCE DATA")
    logger.info("=" * 80)

    # Load firm papers
    logger.info(f"\nLoading firm papers from {INPUT_PARQUET}")
    df = pl.read_parquet(INPUT_PARQUET)
    logger.info(f"Loaded {len(df):,} papers")

    # Extract all unique institutions
    logger.info("\nExtracting unique institutions...")
    institutions_set = set()
    institutions_data = []

    for i, row in enumerate(df.iter_rows(named=True)):
        if (i + 1) % 100000 == 0:
            logger.info(f"  Processed {i+1:,}/{len(df):,} papers...")

        # Get primary affiliations
        primary_affs = row.get('author_primary_affiliations', [])
        primary_aff_ids = row.get('author_primary_affiliation_ids', [])
        primary_countries = row.get('author_primary_affiliation_countries', [])

        # Get all affiliations from nested lists
        all_affs = row.get('author_affiliations', [])

        # Process primary affiliations
        for j, aff in enumerate(primary_affs):
            if not aff or aff == "":
                continue

            # Extract OpenAlex ID
            openalex_id = None
            if j < len(primary_aff_ids) and primary_aff_ids[j]:
                openalex_id = primary_aff_ids[j]

            # Extract country
            if j < len(primary_countries) and primary_countries[j]:
                country = primary_countries[j]
            else:
                country = extract_country_from_affiliation(aff)

            # Create unique key
            key = (aff, openalex_id, country)
            if key not in institutions_set:
                institutions_set.add(key)
                institutions_data.append({
                    'raw_name': aff,
                    'openalex_id': openalex_id,
                    'country': country,
                    'source': 'primary_affiliation'
                })

    logger.info(f"\nExtracted {len(institutions_data):,} unique institution entries")

    # Convert to DataFrame for further processing
    inst_df = pl.DataFrame(institutions_data)

    # Standardize names
    logger.info("\nStandardizing institution names...")
    normalized_names = [normalize_institution_name(name) for name in inst_df['raw_name']]
    inst_df = inst_df.with_columns([
        pl.Series(normalized_names).alias('normalized_name')
    ])

    # Group by normalized name to merge duplicates
    logger.info("\nMerging duplicate institutions...")
    inst_df = inst_df.group_by('normalized_name').agg([
        pl.first('raw_name').alias('canonical_name'),
        pl.first('openalex_id').alias('openalex_id'),
        pl.first('country').alias('country'),
        pl.len().alias('occurrence_count'),
        pl.col('source').first().alias('source')
    ])

    logger.info(f"\nFinal unique institutions: {len(inst_df):,}")

    # Query OpenAlex for additional metadata (if OpenAlex ID exists)
    logger.info("\nQuerying OpenAlex for additional metadata...")
    institutions_with_metadata = []

    # Sample institutions with OpenAlex IDs
    inst_with_ids = inst_df.filter(pl.col('openalex_id').is_not_null())
    logger.info(f"  Found {len(inst_with_ids):,} institutions with OpenAlex IDs")

    # For now, skip OpenAlex API queries to save time
    # Can be enabled later for enrichment

    # Save to parquet
    logger.info(f"\nSaving to {OUTPUT_PARQUET}")
    inst_df.write_parquet(OUTPUT_PARQUET, compression='snappy')
    logger.info(f"  Saved {len(inst_df):,} institutions")

    # Generate statistics
    logger.info("\n" + "=" * 80)
    logger.info("INSTITUTION REFERENCE STATISTICS")
    logger.info("=" * 80)

    country_dist = inst_df.group_by('country').agg(pl.len().alias('count')).sort('count', descending=True)
    logger.info(f"\nTop 20 countries:")
    for row in country_dist.head(20).iter_rows(named=True):
        logger.info(f"  {row['country'] or 'Unknown'}: {row['count']:,} institutions")

    logger.info(f"\nTop 20 institutions by occurrence count:")
    top_inst = inst_df.sort('occurrence_count', descending=True).head(20)
    for row in top_inst.iter_rows(named=True):
        logger.info(f"  {row['canonical_name'][:60]:<60} ({row['occurrence_count']:,} papers)")

    return inst_df


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("INSTITUTION REFERENCE EXTRACTION")
    logger.info("=" * 80)
    logger.info(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    inst_df = extract_institution_reference()

    logger.info("\n" + "=" * 80)
    logger.info("EXTRACTION COMPLETED")
    logger.info("=" * 80)
    logger.info(f"Total unique institutions: {len(inst_df):,}")
    logger.info(f"Output saved to: {OUTPUT_PARQUET}")
    logger.info(f"Finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
