"""
Wikidata Entity Resolution for Company Matching

This script queries Wikidata API for institutions with Wikidata URLs to extract:
- Ticker symbols (P414)
- Previous company names (P1448, P742)
- Owned by / parent organization relationships (P127, P749)
- Stock exchange information

This provides structured entity-level matching with very high confidence.
"""

import polars as pl
import requests
from pathlib import Path
import logging
import time
from typing import Dict, List, Optional
import re

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_ENRICHED = DATA_INTERIM / "publication_institutions_enriched.parquet"
COMPUSTAT_FIRMS = DATA_INTERIM / "compustat_firms_standardized.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_wikidata.parquet"
PROGRESS_LOG = LOGS_DIR / "match_wikidata_companies.log"

DATA_PROCESSED_LINK.mkdir(parents=True, exist_ok=True)
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

# Wikidata API endpoints
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

# Rate limiting
API_DELAY = 0.1  # 100ms between requests


def extract_entity_id_from_url(wikidata_url: str) -> Optional[str]:
    """Extract Wikidata entity ID from URL."""
    if not wikidata_url:
        return None
    match = re.search(r'Q\d+', wikidata_url)
    return match.group(0) if match else None


def query_wikidata_entity(entity_id: str) -> Optional[Dict]:
    """Query Wikidata API for entity information."""
    try:
        params = {
            'action': 'wbgetentities',
            'ids': entity_id,
            'format': 'json',
            'props': 'claims|labels|descriptions'
        }
        response = requests.get(WIKIDATA_API, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        time.sleep(API_DELAY)
        return data.get('entities', {}).get(entity_id)
    except Exception as e:
        logger.warning(f"  Error querying Wikidata for {entity_id}: {e}")
        return None


def extract_ticker_from_claims(entity: Dict) -> List[str]:
    """Extract ticker symbols from Wikidata claims (P414)."""
    tickers = []
    claims = entity.get('claims', {})
    ticker_claims = claims.get('P414', [])  # ticker symbol

    for claim in ticker_claims:
        try:
            mainsnak = claim.get('mainsnak', {})
            datavalue = mainsnak.get('datavalue', {})
            value = datavalue.get('value')
            if value:
                tickers.append(value)
        except:
            pass

    return tickers


def extract_parent_company_from_claims(entity: Dict) -> List[str]:
    """Extract parent company from Wikidata claims (P127, P749)."""
    parents = []
    claims = entity.get('claims', {})

    # P127: owned by
    for claim in claims.get('P127', []):
        try:
            mainsnak = claim.get('mainsnak', {})
            datavalue = mainsnak.get('datavalue', {})
            value = datavalue.get('value', {})
            if value and 'id' in value:
                parents.append(value['id'])
        except:
            pass

    # P749: parent organization
    for claim in claims.get('P749', []):
        try:
            mainsnak = claim.get('mainsnak', {})
            datavalue = mainsnak.get('datavalue', {})
            value = datavalue.get('value', {})
            if value and 'id' in value:
                parents.append(value['id'])
        except:
            pass

    return parents


def extract_alternative_names(entity: Dict) -> List[str]:
    """Extract alternative names from entity."""
    alt_names = []
    claims = entity.get('claims', {})

    # P1448: official name
    for claim in claims.get('P1448', []):
        try:
            mainsnak = claim.get('mainsnak', {})
            datavalue = mainsnak.get('datavalue', {})
            value = datavalue.get('value', {})
            if value and 'text' in value:
                alt_names.append(value['text'])
        except:
            pass

    # P742: stock exchange name
    for claim in claims.get('P742', []):
        try:
            mainsnak = claim.get('mainsnak', {})
            datavalue = mainsnak.get('datavalue', {})
            value = datavalue.get('value', {})
            if value and 'text' in value:
                alt_names.append(value['text'])
        except:
            pass

    return alt_names


def match_institution_via_wikidata(institution_row: pl.DataFrame, firms_df: pl.DataFrame) -> List[Dict]:
    """Match institution to firms using Wikidata data."""
    matches = []

    institution_id = institution_row['institution_id']
    display_name = institution_row['display_name']
    wikidata_url = institution_row['wikidata_url']
    country_code = institution_row['geo_country_code']
    paper_count = institution_row['paper_count']

    if not wikidata_url:
        return matches

    # Extract entity ID
    entity_id = extract_entity_id_from_url(wikidata_url)
    if not entity_id:
        return matches

    # Query Wikidata
    entity = query_wikidata_entity(entity_id)
    if not entity:
        return matches

    # Extract information from Wikidata
    tickers = extract_ticker_from_claims(entity)
    alt_names = extract_alternative_names(entity)
    parent_entities = extract_parent_company_from_claims(entity)

    # Strategy 1: Match by ticker symbol
    for ticker in tickers:
        ticker_upper = ticker.upper()
        firm_matches = firms_df.filter(
            (pl.col('tic').str.to_uppercase() == ticker_upper) |
            (pl.col('name_variants').list.contains(ticker_upper))
        )

        for firm_row in firm_matches.iter_rows(named=True):
            matches.append({
                'GVKEY': firm_row['GVKEY'],
                'LPERMNO': firm_row.get('LPERMNO'),
                'firm_conm': firm_row['conm'],
                'institution_id': institution_id,
                'institution_display_name': display_name,
                'institution_wikidata': wikidata_url,
                'match_type': 'wikidata_ticker',
                'match_confidence': 0.98,
                'match_method': 'wikidata_ticker_match',
                'matched_value': ticker,
                'institution_paper_count': paper_count,
            })

    # Strategy 2: Match by alternative names
    for alt_name in alt_names:
        alt_clean = alt_name.lower().strip()
        firm_matches = firms_df.filter(
            (pl.col('conm_clean').str.to_lowercase() == alt_clean) |
            (pl.col('conml_clean').str.to_lowercase() == alt_clean)
        )

        for firm_row in firm_matches.iter_rows(named=True):
            matches.append({
                'GVKEY': firm_row['GVKEY'],
                'LPERMNO': firm_row.get('LPERMNO'),
                'firm_conm': firm_row['conm'],
                'institution_id': institution_id,
                'institution_display_name': display_name,
                'institution_wikidata': wikidata_url,
                'match_type': 'wikidata_altname',
                'match_confidence': 0.97,
                'match_method': 'wikidata_alternative_name',
                'matched_value': alt_name,
                'institution_paper_count': paper_count,
            })

    # Strategy 3: Match parent company
    for parent_entity_id in parent_entities:
        # Query parent entity
        parent_entity = query_wikidata_entity(parent_entity_id)
        if not parent_entity:
            continue

        parent_label = parent_entity.get('labels', {}).get('en', '')
        parent_tickers = extract_ticker_from_claims(parent_entity)

        # Try to match parent label to firms
        if parent_label:
            parent_clean = parent_label.lower().strip()
            firm_matches = firms_df.filter(
                (pl.col('conm_clean').str.to_lowercase() == parent_clean) |
                (pl.col('conml_clean').str.to_lowercase() == parent_clean)
            )

            for firm_row in firm_matches.iter_rows(named=True):
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row['conm'],
                    'institution_id': institution_id,
                    'institution_display_name': display_name,
                    'institution_wikidata': wikidata_url,
                    'match_type': 'wikidata_parent',
                    'match_confidence': 0.97,
                    'match_method': 'wikidata_parent_cascade',
                    'matched_value': parent_label,
                    'institution_paper_count': paper_count,
                })

        # Try to match parent ticker to firms
        for ticker in parent_tickers:
            ticker_upper = ticker.upper()
            firm_matches = firms_df.filter(
                (pl.col('tic').str.to_uppercase() == ticker_upper) |
                (pl.col('name_variants').list.contains(ticker_upper))
            )

            for firm_row in firm_matches.iter_rows(named=True):
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row['conm'],
                    'institution_id': institution_id,
                    'institution_display_name': display_name,
                    'institution_wikidata': wikidata_url,
                    'match_type': 'wikidata_parent_ticker',
                    'match_confidence': 0.98,
                    'match_method': 'wikidata_parent_ticker',
                    'matched_value': ticker,
                    'institution_paper_count': paper_count,
                })

    return matches


def main():
    logger.info("=" * 80)
    logger.info("WIKIDATA ENTITY RESOLUTION FOR COMPANY MATCHING")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/4] Loading data...")
    institutions = pl.read_parquet(INSTITUTIONS_ENRICHED)
    firms = pl.read_parquet(COMPUSTAT_FIRMS)

    logger.info(f"  Loaded {len(institutions)} institutions")
    logger.info(f"  Loaded {len(firms)} firms")

    # Filter institutions with Wikidata URLs
    logger.info("\n[2/4] Filtering institutions with Wikidata URLs...")
    inst_with_wikidata = institutions.filter(pl.col('wikidata_url').is_not_null())
    logger.info(f"  {len(inst_with_wikidata)} institutions have Wikidata URLs")

    # Run matching
    logger.info("\n[3/4] Running Wikidata matching...")
    logger.info("  This may take time due to API rate limiting...")

    all_matches = []
    total = len(inst_with_wikidata)

    for i, inst_row in enumerate(inst_with_wikidata.iter_rows(named=True)):
        if (i + 1) % 100 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} institutions ({len(all_matches)} matches so far)...")

        matches = match_institution_via_wikidata(inst_row, firms)
        all_matches.extend(matches)

    logger.info(f"  Completed. Found {len(all_matches)} total matches")

    # Save output
    logger.info("\n[4/4] Saving matches...")
    if not all_matches:
        logger.warning("  No matches found!")
    else:
        matches_df = pl.DataFrame(all_matches)
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df)} matches to {OUTPUT_FILE}")

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("WIKIDATA MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df)}")
        logger.info(f"Unique institutions: {matches_df['institution_id'].n_unique()}")

        if 'match_type' in matches_df.columns:
            logger.info("\nMatches by type:")
            for match_type, count in matches_df.group_by('match_type').agg(pl.len().alias('count')).iter_rows(named=True):
                logger.info(f"  {match_type}: {count}")

    logger.info("\n" + "=" * 80)
    logger.info("WIKIDATA MATCHING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
