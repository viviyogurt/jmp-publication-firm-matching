"""
Extract Structured Company Data from Wikipedia/Wikidata

This script extracts high-quality structured data from Wikipedia pages and Wikidata API:
- Ticker symbols (P249)
- CIK codes (P5585) - SEC Central Index Key for Compustat matching
- Parent companies (P749)
- ISIN codes (P946)
- Stock exchanges (P414)
- Industry classification

Uses efficient batch processing with rate limiting to avoid API blocks.
"""
import polars as pl
from pathlib import Path
import logging
import time
import re
import requests
from typing import Optional, Dict, List, Set
from datetime import datetime
import json

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
LOGS_DIR = PROJECT_ROOT / "logs"

INPUT_FILE = DATA_INTERIM / "publication_institutions_enriched.parquet"
OUTPUT_FILE = DATA_INTERIM / "publication_institutions_wikidata_structured.parquet"
PROGRESS_LOG = LOGS_DIR / "extract_wikipedia_structured.log"

DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
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

# API Endpoints
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"

# Rate limiting
REQUEST_DELAY = 0.1  # 100ms between requests (10 requests/second)
BATCH_SIZE = 50  # Process in batches

# Wikidata property IDs
PROPS = {
    'ticker': 'P249',      # Stock ticker symbol
    'cik': 'P5585',        # SEC Central Index Key
    'parent': 'P749',      # Parent company
    'isin': 'P946',        # ISIN (International Securities Identification Number)
    'exchange': 'P414',    # Stock exchange
    'founded': 'P571',     # Inception date
    'headquarters': 'P159', # Headquarters location
}


# ============================================================================
# API Functions
# ============================================================================

def make_api_request(url: str, params: dict) -> Optional[dict]:
    """Make API request with rate limiting and error handling."""
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        time.sleep(REQUEST_DELAY)  # Rate limiting
        return response.json()
    except Exception as e:
        logger.debug(f"API request failed for {url}: {e}")
        return None


def extract_wikidata_id_from_wikipedia_url(wikipedia_url: str) -> Optional[str]:
    """
    Extract Wikidata entity ID from Wikipedia URL.

    Input: http://en.wikipedia.org/wiki/Google
    Output: Q95 (Wikidata ID for Google)
    """
    if not wikipedia_url or 'wikipedia.org/wiki/' not in wikipedia_url:
        return None

    try:
        # Extract page title from URL
        page_title = wikipedia_url.split('wiki/')[-1].split('#')[0]

        # Query Wikipedia API to get Wikidata ID
        params = {
            'action': 'query',
            'prop': 'pageprops',
            'ppprop': 'wikibase_item',
            'titles': page_title,
            'format': 'json'
        }

        data = make_api_request(WIKIPEDIA_API, params)
        if not data:
            return None

        pages = data.get('query', {}).get('pages', {})
        for page_id, page_data in pages.items():
            if 'wikibase_item' in page_data.get('pageprops', {}):
                return page_data['pageprops']['wikibase_item']

        return None
    except Exception as e:
        logger.debug(f"Failed to extract Wikidata ID from {wikipedia_url}: {e}")
        return None


def get_wikidata_entity(wikidata_id: str) -> Optional[dict]:
    """
    Retrieve Wikidata entity with all claims.

    Input: Q95 (Google)
    Output: Full entity data with claims (tickers, CIK, parent, etc.)
    """
    try:
        params = {
            'action': 'wbgetentities',
            'ids': wikidata_id,
            'format': 'json'
        }

        data = make_api_request(WIKIDATA_API, params)
        if not data:
            return None

        entities = data.get('entities', {})
        return entities.get(wikidata_id)

    except Exception as e:
        logger.debug(f"Failed to get Wikidata entity {wikidata_id}: {e}")
        return None


def extract_property_values(entity: dict, property_id: str) -> List[str]:
    """
    Extract values from a Wikidata property.

    Handles different data types:
    - String values (ticker symbols, CIK)
    - Entity references (parent company)
    - Item references (stock exchange)
    """
    if not entity or 'claims' not in entity:
        return []

    claims = entity['claims'].get(property_id, [])
    values = []

    for claim in claims:
        try:
            mainsnak = claim.get('mainsnak', {})
            datavalue = mainsnak.get('datavalue', {})
            datatype = mainsnak.get('datatype', '')

            if datatype == 'string':
                # String values (ticker, CIK, ISIN)
                value = datavalue.get('value', '')
                if value:
                    values.append(value)

            elif datatype == 'wikibase-item':
                # Entity references (parent company, stock exchange)
                entity_id = datavalue.get('value', {}).get('id', '')
                if entity_id:
                    values.append(entity_id)

            elif datatype == 'external-id':
                # External identifiers
                value = datavalue.get('value', '')
                if value:
                    values.append(value)

            elif datatype == 'time':
                # Dates (founded)
                value = datavalue.get('value', {}).get('time', '')
                if value:
                    # Convert from +YYYY-MM-DD to YYYY
                    year = value[1:5] if value.startswith('+') else value[:4]
                    values.append(year)

        except Exception as e:
            logger.debug(f"Failed to extract property {property_id}: {e}")
            continue

    return values


def extract_company_structured_data(wikipedia_url: str) -> Dict:
    """
    Extract all structured company data from Wikipedia/Wikidata.

    Returns:
        {
            'wikidata_id': 'Q95',
            'tickers': ['GOOGL', 'GOOG'],
            'cik': '0001652044',
            'parent_company_id': 'Q193403',  # Alphabet
            'isin': ['US02079K3059'],
            'exchange': ['Q41778'],
            'founded': '1998',
            'headquarters': 'Q16973'  # Mountain View, CA
        }
    """
    result = {
        'wikidata_id': None,
        'tickers': [],
        'cik': None,
        'parent_company_id': None,
        'isin': [],
        'exchange': [],
        'founded': None,
        'headquarters': None,
        'extraction_timestamp': datetime.now().isoformat()
    }

    try:
        # Step 1: Get Wikidata ID from Wikipedia URL
        wikidata_id = extract_wikidata_id_from_wikipedia_url(wikipedia_url)
        if not wikidata_id:
            return result

        result['wikidata_id'] = wikidata_id

        # Step 2: Get full Wikidata entity
        entity = get_wikidata_entity(wikidata_id)
        if not entity:
            return result

        # Step 3: Extract all properties
        # Ticker symbols (can be multiple)
        tickers = extract_property_values(entity, PROPS['ticker'])
        result['tickers'] = tickers

        # CIK code (single value, take first)
        cik_values = extract_property_values(entity, PROPS['cik'])
        result['cik'] = cik_values[0] if cik_values else None

        # Parent company (single value, take first)
        parent_values = extract_property_values(entity, PROPS['parent'])
        result['parent_company_id'] = parent_values[0] if parent_values else None

        # ISIN codes (can be multiple)
        result['isin'] = extract_property_values(entity, PROPS['isin'])

        # Stock exchanges (can be multiple)
        result['exchange'] = extract_property_values(entity, PROPS['exchange'])

        # Founded year
        founded_values = extract_property_values(entity, PROPS['founded'])
        result['founded'] = founded_values[0] if founded_values else None

        # Headquarters location
        hq_values = extract_property_values(entity, PROPS['headquarters'])
        result['headquarters'] = hq_values[0] if hq_values else None

    except Exception as e:
        logger.debug(f"Failed to extract structured data from {wikipedia_url}: {e}")

    return result


# ============================================================================
# Batch Processing
# ============================================================================

def process_batch(institutions_df: pl.DataFrame, batch_num: int, total_batches: int) -> pl.DataFrame:
    """Process a batch of institutions and extract structured data."""

    results = []

    for i, row in enumerate(institutions_df.iter_rows(named=True), 1):
        institution_id = row.get('institution_id')
        wikipedia_url = row.get('wikipedia_url')

        if not wikipedia_url:
            continue

        # Extract structured data
        structured_data = extract_company_structured_data(wikipedia_url)

        # Combine with original data
        result_row = {
            'institution_id': institution_id,
            'wikipedia_url': wikipedia_url,
            **structured_data
        }

        results.append(result_row)

        # Progress update
        if i % 10 == 0:
            logger.info(f"  Batch {batch_num}/{total_batches}: {i}/{len(institutions_df)} institutions processed")

    return pl.DataFrame(results)


# ============================================================================
# Main Processing
# ============================================================================

def main():
    """Main extraction workflow."""

    logger.info("=" * 80)
    logger.info("EXTRACTING STRUCTURED DATA FROM WIKIPEDIA/WIKIDATA")
    logger.info("=" * 80)
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Input file: {INPUT_FILE}")
    logger.info(f"Output file: {OUTPUT_FILE}")

    # Load data
    logger.info("\n[1/5] Loading institutions with Wikipedia URLs...")
    institutions_df = pl.read_parquet(INPUT_FILE)
    logger.info(f"  Loaded {len(institutions_df):,} institutions")

    # Filter institutions with Wikipedia URLs
    logger.info("\n[2/5] Filtering institutions with Wikipedia URLs...")
    has_wikipedia = institutions_df.filter(
        pl.col('wikipedia_url').is_not_null()
    )
    logger.info(f"  {len(has_wikipedia):,} institutions have Wikipedia URLs")

    # Check if already processed
    if OUTPUT_FILE.exists():
        logger.info(f"\n  Found existing output file: {OUTPUT_FILE}")
        logger.info(f"  Loading previously processed data...")
        existing_df = pl.read_parquet(OUTPUT_FILE)
        processed_ids = set(existing_df['institution_id'].to_list())
        logger.info(f"  {len(processed_ids):,} institutions already processed")

        # Filter out already processed
        has_wikipedia = has_wikipedia.filter(
            ~pl.col('institution_id').is_in(processed_ids)
        )
        logger.info(f"  {len(has_wikipedia):,} institutions remaining to process")
    else:
        processed_ids = set()

    if len(has_wikipedia) == 0:
        logger.info("\n  All institutions already processed!")
        return

    # Split into batches
    total_institutions = len(has_wikipedia)
    num_batches = (total_institutions + BATCH_SIZE - 1) // BATCH_SIZE
    logger.info(f"\n[3/5] Processing in {num_batches} batches of {BATCH_SIZE} institutions each...")

    all_results = []

    for batch_num in range(num_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, total_institutions)

        batch_df = has_wikipedia[start_idx:end_idx]

        logger.info(f"\n  Processing batch {batch_num + 1}/{num_batches} ({len(batch_df)} institutions)...")

        try:
            batch_results = process_batch(batch_df, batch_num + 1, num_batches)
            all_results.append(batch_results)

            # Save intermediate results
            if len(all_results) % 5 == 0:
                combined = pl.concat(all_results)
                combined.write_parquet(OUTPUT_FILE, compression='snappy')
                logger.info(f"  Saved intermediate results: {len(combined):,} institutions")

        except Exception as e:
            logger.error(f"  Error processing batch {batch_num + 1}: {e}")
            continue

    # Combine all results
    logger.info("\n[4/5] Combining all results...")

    if all_results:
        new_results = pl.concat(all_results)

        # Combine with existing if any
        if OUTPUT_FILE.exists():
            existing_df = pl.read_parquet(OUTPUT_FILE)
            final_df = pl.concat([existing_df, new_results])
        else:
            final_df = new_results

        # Save final results
        final_df.write_parquet(OUTPUT_FILE, compression='snappy')

        logger.info(f"  Saved {len(final_df):,} institutions to {OUTPUT_FILE}")

        # Statistics
        logger.info("\n[5/5] EXTRACTION STATISTICS")
        logger.info("=" * 80)

        with_tickers = final_df.filter(pl.col('tickers').list.len() > 0)
        with_cik = final_df.filter(pl.col('cik').is_not_null())
        with_parent = final_df.filter(pl.col('parent_company_id').is_not_null())
        with_isin = final_df.filter(pl.col('isin').list.len() > 0)

        logger.info(f"  Total institutions processed: {len(final_df):,}")
        logger.info(f"  With ticker symbols: {len(with_tickers):,}")
        logger.info(f"  With CIK codes: {len(with_cik):,}")
        logger.info(f"  With parent company: {len(with_parent):,}")
        logger.info(f"  With ISIN codes: {len(with_isin):,}")

        # Sample tickers found
        if len(with_tickers) > 0:
            logger.info("\n  Sample ticker symbols found:")
            samples = with_tickers.select(['institution_id', 'tickers']).head(10)
            for row in samples.iter_rows(named=True):
                tickers_str = ', '.join(row['tickers'][:3])
                logger.info(f"    {row['institution_id']}: {tickers_str}")

        # Sample CIKs found
        if len(with_cik) > 0:
            logger.info("\n  Sample CIK codes found:")
            samples = with_cik.select(['institution_id', 'cik']).head(10)
            for row in samples.iter_rows(named=True):
                logger.info(f"    {row['institution_id']}: {row['cik']}")

    logger.info("\n" + "=" * 80)
    logger.info("WIKIPEDIA/WIKIDATA STRUCTURED DATA EXTRACTION COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
