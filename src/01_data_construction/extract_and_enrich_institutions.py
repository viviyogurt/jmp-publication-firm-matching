"""
Stage 0: Extract and Enrich Institutions with Rich Data

This script loads the raw institutions_all.jsonl.gz file and extracts ALL rich information:
- Wikipedia/Wikidata/ROR/GRID IDs
- Homepage URLs and domains
- Geographic information (city, region, lat/long)
- Alternative names and acronyms
- Parent-child relationships from associated_institutions
- Institution type classification

Output: publication_institutions_enriched.parquet
"""

import polars as pl
import json
import gzip
from pathlib import Path
import logging
from typing import Dict, List, Set
from urllib.parse import urlparse
import re

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
LOGS_DIR = PROJECT_ROOT / "logs"

INPUT_FILE = DATA_RAW / "institutions_all.jsonl.gz"
OUTPUT_FILE = DATA_INTERIM / "publication_institutions_enriched.parquet"
PROGRESS_LOG = LOGS_DIR / "extract_enrich_institutions.log"

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


def extract_domain_from_url(url: str) -> str:
    """Extract domain from URL."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return None


def extract_institution_data(raw_record: Dict) -> Dict:
    """Extract all relevant fields from raw institution record."""

    # Basic fields
    institution_id = raw_record.get('id', '')
    display_name = raw_record.get('display_name', '')

    # IDs dictionary
    ids = raw_record.get('ids', {})
    wikipedia_url = ids.get('wikipedia') or None
    wikidata_url = ids.get('wikidata') or None
    ror_url = ids.get('ror') or None
    grid_id = ids.get('grid') or None

    # Homepage
    homepage_url = raw_record.get('homepage_url') or None
    homepage_domain = extract_domain_from_url(homepage_url) if homepage_url else None

    # Geographic info
    geo = raw_record.get('geo', {}) or {}
    geo_city = geo.get('city') if geo else None
    geo_region = geo.get('region') if geo else None
    geo_country_code = geo.get('country_code') if geo else None
    geo_latitude = geo.get('latitude') if geo else None
    geo_longitude = geo.get('longitude') if geo else None

    # Alternative names and acronyms
    alternative_names = raw_record.get('display_name_alternatives') or []
    acronyms = raw_record.get('display_name_acronyms') or []

    # Associated institutions (parent/child relationships)
    associated_institutions = raw_record.get('associated_institutions') or []
    parent_ids = []
    child_ids = []
    associated_ids = []

    for assoc in associated_institutions:
        relationship = assoc.get('relationship', '')
        inst_id = assoc.get('id', '')

        if not inst_id:
            continue

        if relationship == 'parent':
            parent_ids.append(inst_id)
        elif relationship == 'child':
            child_ids.append(inst_id)
        else:
            associated_ids.append(inst_id)

    # Institution type
    institution_type = raw_record.get('type') or None
    is_company = 1 if institution_type == 'company' else 0

    # Paper count
    paper_count = raw_record.get('works_count') or 0

    # Return enriched record
    return {
        'institution_id': institution_id,
        'display_name': display_name,
        'normalized_name': display_name.lower().strip(),

        # Rich identifiers
        'wikipedia_url': wikipedia_url,
        'wikidata_url': wikidata_url,
        'ror_url': ror_url,
        'grid_id': grid_id,

        # URL & Domain
        'homepage_url': homepage_url,
        'homepage_domain': homepage_domain,

        # Geography
        'geo_city': geo_city,
        'geo_region': geo_region,
        'geo_country_code': geo_country_code,
        'geo_latitude': geo_latitude,
        'geo_longitude': geo_longitude,

        # Names
        'alternative_names': alternative_names,
        'acronyms': acronyms,

        # Relationships
        'parent_institution_ids': parent_ids,
        'child_institution_ids': child_ids,
        'associated_institution_ids': associated_ids,

        # Classification
        'institution_type': institution_type,
        'is_company': is_company,

        # Metrics
        'paper_count': paper_count,
    }


def main():
    logger.info("=" * 80)
    logger.info("STAGE 0: EXTRACT AND ENRICH INSTITUTIONS WITH RICH DATA")
    logger.info("=" * 80)

    # Check input file exists
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        return

    logger.info(f"\n[1/4] Reading institutions from {INPUT_FILE}...")

    # Read and process records
    enriched_records = []
    total_records = 0

    with gzip.open(INPUT_FILE, 'rt') as f:
        for line in f:
            total_records += 1

            # Progress update every 10000 records
            if total_records % 10000 == 0:
                logger.info(f"  Processed {total_records:,} records...")

            try:
                raw_record = json.loads(line)
                enriched = extract_institution_data(raw_record)
                enriched_records.append(enriched)
            except Exception as e:
                logger.warning(f"  Error processing record {total_records}: {e}")
                continue

    logger.info(f"\n[2/4] Creating DataFrame from {len(enriched_records):,} enriched records...")

    # Convert to Polars DataFrame
    df = pl.DataFrame(enriched_records)

    logger.info(f"\n[3/4] Data summary:")
    logger.info(f"  Total institutions: {len(df):,}")
    logger.info(f"  Companies: {df.filter(pl.col('is_company') == 1).shape[0]:,}")
    logger.info(f"  With Wikipedia URL: {df.filter(pl.col('wikipedia_url').is_not_null()).shape[0]:,}")
    logger.info(f"  With Wikidata URL: {df.filter(pl.col('wikidata_url').is_not_null()).shape[0]:,}")
    logger.info(f"  With homepage: {df.filter(pl.col('homepage_url').is_not_null()).shape[0]:,}")
    logger.info(f"  With parent institutions: {df.filter(pl.col('parent_institution_ids').list.len() > 0).shape[0]:,}")
    logger.info(f"  With geo coordinates: {df.filter(pl.col('geo_latitude').is_not_null()).shape[0]:,}")

    # Save to parquet
    logger.info(f"\n[4/4] Saving enriched institutions to {OUTPUT_FILE}...")
    df.write_parquet(OUTPUT_FILE, compression='snappy')

    logger.info("\n" + "=" * 80)
    logger.info("ENRICHMENT COMPLETE")
    logger.info("=" * 80)
    logger.info(f"\nOutput saved to: {OUTPUT_FILE}")
    logger.info(f"Total institutions enriched: {len(df):,}")


if __name__ == "__main__":
    main()
