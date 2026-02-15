"""
Extract Structured Company Data from Wikidata Using SPARQL

This script uses the Wikidata Query Service (SPARQL) to extract all companies
with ticker symbols, CIK codes, parent companies, and other structured data.

SPARQL is much more efficient than REST API calls:
- No rate limiting issues
- Get all data in a single query
- No 403 errors
- Can query by properties directly

Wikidata Query Service: https://query.wikidata.org/
"""
import polars as pl
from pathlib import Path
import logging
import requests
import time
from typing import Optional, Dict, List
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
SPARQL_CACHE_FILE = DATA_INTERIM / "wikidata_companies_cache.json"
PROGRESS_LOG = LOGS_DIR / "extract_wikidata_sparql.log"

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

# Wikidata SPARQL endpoint
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_API_BASE = "https://www.wikidata.org/wiki/Special:EntityData/"

# Property IDs
PROPS = {
    'ticker': 'P249',
    'cik': 'P5585',
    'parent': 'P749',
    'isin': 'P946',
    'exchange': 'P414',
    'founded': 'P571',
}

# ============================================================================
# SPARQL Queries
# ============================================================================

def build_sparql_query(wikipedia_urls: List[str]) -> str:
    """
    Build SPARQL query to fetch structured data for multiple Wikipedia entities.

    Uses VALUES clause to query multiple Wikipedia pages at once.
    """
    # Convert Wikipedia URLs to page titles
    # http://en.wikipedia.org/wiki/Google -> "Google"
    page_titles = []
    for url in wikipedia_urls:
        if 'wikipedia.org/wiki/' in url:
            title = url.split('wiki/')[-1].split('#')[0]
            # Don't URL encode - SPARQL will handle it
            # Just escape quotes
            title = title.replace('\\', '\\\\').replace('"', '\\"')
            page_titles.append(f'"{title}"')

    if not page_titles:
        return None

    # Join page titles with commas (max 50 per query to avoid 400 errors)
    values_clause = ', '.join(page_titles[:50])

    # Simplified SPARQL query - just get essential data
    sparql = f"""
    SELECT ?item ?itemLabel ?wikipediaTitle ?ticker ?cik ?parent ?parentLabel WHERE {{
      VALUES ?wikipediaTitle {{ {values_clause} }}.

      # Find item by Wikipedia English article title
      ?wikipediaUrl schema:about ?item ;
                   schema:isPartOf <https://en.wikipedia.org/> ;
                   schema:name ?wikipediaTitle .

      # OPTIONAL: Get ticker symbol
      OPTIONAL {{ ?item wdt:{PROPS['ticker']} ?ticker. }}

      # OPTIONAL: Get CIK code
      OPTIONAL {{ ?item wdt:{PROPS['cik']} ?cik. }}

      # OPTIONAL: Get parent company
      OPTIONAL {{
        ?item wdt:{PROPS['parent']} ?parent .
        ?parent rdfs:label ?parentLabel .
        FILTER(LANG(?parentLabel) = "en")
      }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """

    return sparql


def execute_sparql_query(sparql: str) -> Optional[List[Dict]]:
    """
    Execute SPARQL query against Wikidata Query Service.

    Returns list of results with all structured data.
    """
    if not sparql:
        return None

    headers = {
        'User-Agent': 'JMPResearch/1.0 (Academic research; contact: research@university.edu)',
        'Accept': 'application/json'
    }

    params = {
        'query': sparql,
        'format': 'json'
    }

    try:
        logger.info("  Executing SPARQL query...")
        response = requests.get(WIKIDATA_SPARQL, params=params, headers=headers, timeout=60)
        response.raise_for_status()

        data = response.json()

        # Parse results
        results = data.get('results', {}).get('bindings', [])

        logger.info(f"  Query returned {len(results)} results")
        return results

    except Exception as e:
        logger.error(f"  SPARQL query failed: {e}")
        return None


def parse_sparql_result(result: Dict) -> Dict:
    """
    Parse a single SPARQL result into structured data format.

    SPARQL returns values with 'value' and 'type' fields.
    """
    parsed = {
        'wikidata_id': None,
        'wikipedia_title': None,
        'tickers': [],
        'cik': None,
        'parent_company_id': None,
        'parent_company_name': None,
        'isin': [],
        'exchange': [],
        'exchange_name': None,
    }

    try:
        # Extract Wikidata ID (item URI)
        item_uri = result.get('item', {}).get('value', '')
        if item_uri:
            # http://www.wikidata.org/entity/Q95 -> Q95
            parsed['wikidata_id'] = item_uri.split('/')[-1]

        # Extract Wikipedia title
        wiki_title = result.get('wikipediaTitle', {}).get('value', '')
        if wiki_title:
            parsed['wikipedia_title'] = wiki_title

        # Extract item label (company name)
        item_label = result.get('itemLabel', {}).get('value', '')
        if item_label:
            parsed['company_name'] = item_label

        # Extract ticker symbol
        ticker = result.get('ticker', {}).get('value', '')
        if ticker:
            parsed['tickers'] = [ticker]

        # Extract CIK
        cik = result.get('cik', {}).get('value', '')
        if cik:
            # Remove leading zeros for standardization
            parsed['cik'] = cik.lstrip('0') or cik

        # Extract parent company
        parent_uri = result.get('parent', {}).get('value', '')
        if parent_uri:
            parsed['parent_company_id'] = parent_uri.split('/')[-1]

        parent_label = result.get('parentLabel', {}).get('value', '')
        if parent_label:
            parsed['parent_company_name'] = parent_label

        # Extract ISIN
        isin = result.get('isin', {}).get('value', '')
        if isin:
            parsed['isin'] = [isin]

        # Extract stock exchange
        exchange_uri = result.get('exchange', {}).get('value', '')
        if exchange_uri:
            parsed['exchange'] = [exchange_uri.split('/')[-1]]

        exchange_label = result.get('exchangeLabel', {}).get('value', '')
        if exchange_label:
            parsed['exchange_name'] = exchange_label

    except Exception as e:
        logger.debug(f"  Failed to parse SPARQL result: {e}")

    return parsed


# ============================================================================
# Main Processing
# ============================================================================

def main():
    """Main extraction workflow using SPARQL."""

    logger.info("=" * 80)
    logger.info("EXTRACTING WIKIDATA STRUCTURED DATA USING SPARQL")
    logger.info("=" * 80)
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load institutions
    logger.info("\n[1/4] Loading institutions with Wikipedia URLs...")
    institutions_df = pl.read_parquet(INPUT_FILE)
    logger.info(f"  Loaded {len(institutions_df):,} institutions")

    # Filter institutions with Wikipedia URLs
    has_wikipedia = institutions_df.filter(
        pl.col('wikipedia_url').is_not_null()
    )
    logger.info(f"  {len(has_wikipedia):,} institutions have Wikipedia URLs")

    # Split into batches of 50 (smaller batches avoid query timeout/400 errors)
    BATCH_SIZE = 50
    total_batches = (len(has_wikipedia) + BATCH_SIZE - 1) // BATCH_SIZE
    logger.info(f"\n[2/4] Processing in {total_batches} batches of {BATCH_SIZE} institutions each...")

    all_results = []

    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(has_wikipedia))

        batch_df = has_wikipedia[start_idx:end_idx]
        wikipedia_urls = batch_df['wikipedia_url'].to_list()

        logger.info(f"\n  Batch {batch_num + 1}/{total_batches} ({len(wikipedia_urls)} URLs)...")

        # Build and execute SPARQL query
        sparql = build_sparql_query(wikipedia_urls)

        if sparql:
            sparql_results = execute_sparql_query(sparql)

            if sparql_results:
                # Create mapping from Wikipedia title to structured data
                wiki_to_structured = {}

                for result in sparql_results:
                    parsed = parse_sparql_result(result)
                    wiki_title = parsed.get('wikipedia_title')

                    if wiki_title:
                        wiki_to_structured[wiki_title] = parsed

                # Match back to institutions
                for inst_row in batch_df.iter_rows(named=True):
                    wiki_url = inst_row['wikipedia_url']

                    # Extract title from URL
                    wiki_title = wiki_url.split('wiki/')[-1].split('#')[0]

                    # Get structured data
                    structured_data = wiki_to_structured.get(wiki_title, {})

                    # Build result row
                    result_row = {
                        'institution_id': inst_row['institution_id'],
                        'wikipedia_url': wiki_url,
                        'wikidata_id': structured_data.get('wikidata_id'),
                        'tickers': structured_data.get('tickers', []),
                        'cik': structured_data.get('cik'),
                        'parent_company_id': structured_data.get('parent_company_id'),
                        'parent_company_name': structured_data.get('parent_company_name'),
                        'isin': structured_data.get('isin', []),
                        'exchange': structured_data.get('exchange', []),
                        'exchange_name': structured_data.get('exchange_name'),
                        'extraction_timestamp': datetime.now().isoformat()
                    }

                    all_results.append(result_row)

                logger.info(f"  Processed {len(sparql_results)} results")

        # Small delay between batches
        time.sleep(1)

    # Save results
    logger.info("\n[3/4] Saving results...")

    if all_results:
        results_df = pl.DataFrame(all_results)
        results_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(results_df):,} institutions to {OUTPUT_FILE}")
    else:
        logger.warning("  No results to save!")
        return

    # Statistics
    logger.info("\n[4/4] EXTRACTION STATISTICS")
    logger.info("=" * 80)

    with_tickers = results_df.filter(pl.col('tickers').list.len() > 0)
    with_cik = results_df.filter(pl.col('cik').is_not_null())
    with_parent = results_df.filter(pl.col('parent_company_id').is_not_null())

    logger.info(f"  Total institutions processed: {len(results_df):,}")
    logger.info(f"  With ticker symbols: {len(with_tickers):,}")
    logger.info(f"  With CIK codes: {len(with_cik):,}")
    logger.info(f"  With parent company: {len(with_parent):,}")

    # Sample tickers found
    if len(with_tickers) > 0:
        logger.info("\n  Sample ticker symbols found:")
        samples = with_tickers.select(['institution_id', 'tickers']).head(10)
        for row in samples.iter_rows(named=True):
            tickers_str = ', '.join(row['tickers'])
            logger.info(f"    {row['institution_id'][:50]:50s} | {tickers_str}")

    # Sample CIKs found
    if len(with_cik) > 0:
        logger.info("\n  Sample CIK codes found:")
        samples = with_cik.select(['institution_id', 'cik']).head(10)
        for row in samples.iter_rows(named=True):
            logger.info(f"    {row['institution_id'][:50]:50s} | {row['cik']}")

    # Sample parent companies
    if len(with_parent) > 0:
        logger.info("\n  Sample parent companies found:")
        samples = with_parent.select(['institution_id', 'parent_company_name']).head(10)
        for row in samples.iter_rows(named=True):
            logger.info(f"    {row['institution_id'][:50]:50s} | Parent: {row['parent_company_name']}")

    logger.info("\n" + "=" * 80)
    logger.info("WIKIDATA SPARQL EXTRACTION COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
