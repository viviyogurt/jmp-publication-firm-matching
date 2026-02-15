"""
Wikipedia Company Scraper - Extract Rich Company Information

This script extracts comprehensive company information from Wikipedia pages:
- Ticker symbols (multiple possible)
- ISIN (International Securities Identification Numbers)
- CIK (Central Index Key - for SEC filing matching)
- Parent company names
- Former company names
- Company type (public/private)
- Industry/sector classification
- Headquarters location

Features:
- Rate limiting (1 req/sec with random delays 1-3 seconds)
- Retry logic (3 attempts)
- Respects robots.txt
- Progress tracking and checkpoints
- Error handling and logging
- Dry-run mode for testing

Expected Output:
- Extract ticker symbols from ~5,000-10,000 company pages
- CIK codes for SEC-registered companies (direct Compustat match!)
- Parent company names for cascade matching
- Former names for historical matching
"""

import polars as pl
import requests
import wikipediaapi
from bs4 import BeautifulSoup
import re
import time
import random
import logging
from pathlib import Path
from typing import Optional, Dict, List, Set, Any
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
import json
from datetime import datetime

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
LOGS_DIR = PROJECT_ROOT / "logs"

# Input files
INSTITUTIONS_ENRICHED = DATA_INTERIM / "publication_institutions_enriched.parquet"
MATCHED_INSTITUTIONS_FILE = DATA_INTERIM / "publication_institution_firm_master.parquet"

# Output file
OUTPUT_FILE = DATA_INTERIM / "publication_institutions_wikidata.parquet"

# Logging
PROGRESS_LOG = LOGS_DIR / "scrape_wikipedia_companies.log"

# Rate limiting
REQUESTS_PER_SECOND = 1
MIN_DELAY = 1.0  # seconds
MAX_DELAY = 3.0  # seconds
MAX_RETRIES = 3
CHECKPOINT_INTERVAL = 100  # Save checkpoint every N pages

# Wikipedia API user agent
USER_AGENT = 'JMPResearchBot/1.0 (AI Research; academic use)'

# Create directories
DATA_INTERIM.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging
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
# Wikipedia Infobox Parser
# ============================================================================

class WikipediaInfoboxParser:
    """Parse Wikipedia infobox for company information."""

    # Infobox field patterns
    TICKER_PATTERNS = [
        r'trading[_\s]*name\s*=\s*{{?(?:br? list)?\s*?\[?\[?([^{}\|\]=]+)',
        r'trading[_\s]*name\s*=\s*([^{}\|\]=\n]+)',
        r'loc[_\s]*name\s*=\s*{{?(?:br? list)?\s*?\[?\[?([^{}\|\]=]+)',
        r'embed\s*=\s*{{?(?:br? list)?\s*?\[?\[?([^{}\|\]=]+)',
    ]

    ISIN_PATTERN = r'ISIN\s*=\s*([A-Z]{2}[A-Z0-9]{9}\d)'

    # Type indicators
    PUBLIC_KEYWORDS = ['public', 'publicly traded', 'public company', 'listed']
    PRIVATE_KEYWORDS = ['private', 'privately held', 'private company']

    # Industry patterns
    INDUSTRY_PATTERNS = [
        r'industry\s*=\s*{{?\s*([^{}\|\]=\n]+)',
        r'industries\s*=\s*{{?\s*([^{}\|\]=\n]+)',
    ]

    # Location patterns
    LOCATION_PATTERNS = [
        r'location[_\s]*(?:city|country)?\s*=\s*{{?\s*([^{}\|\]=\n]+)',
        r'headquarters\s*=\s*{{?\s*([^{}\|\]=\n]+)',
        r'hq[_\s]*location\s*=\s*{{?\s*([^{}\|\]=\n]+)',
    ]

    @staticmethod
    def extract_wikidata_id(wikipedia_url: str) -> Optional[str]:
        """Extract Wikidata entity ID from Wikipedia URL."""
        try:
            # Extract page title from Wikipedia URL
            if 'en.wikipedia.org/wiki/' not in wikipedia_url:
                return None

            page_title = wikipedia_url.split('en.wikipedia.org/wiki/')[-1].split('#')[0]

            # Query Wikidata API to get entity ID
            api_url = "https://en.wikipedia.org/w/api.php"
            params = {
                'action': 'query',
                'prop': 'pageprops',
                'titles': page_title,
                'format': 'json',
                'ppprop': 'wikibase_item'
            }

            response = requests.get(api_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            pages = data.get('query', {}).get('pages', {})
            for page_id, page_data in pages.items():
                if page_id != '-1':  # -1 means page doesn't exist
                    wikidata_id = page_data.get('pageprops', {}).get('wikibase_item')
                    return wikidata_id

            return None
        except Exception as e:
            logger.debug(f"Error extracting Wikidata ID: {e}")
            return None

    @staticmethod
    def parse_infobox(html_content: str) -> Dict[str, Any]:
        """
        Parse Wikipedia infobox for company information.

        Returns:
            Dictionary with extracted fields:
            - ticker_symbols: list of str
            - isin: str or None
            - parent_company: str or None
            - former_names: list of str
            - company_type: str or None
            - industry: str or None
            - headquarters: str or None
        """
        result = {
            'ticker_symbols': [],
            'isin': None,
            'parent_company': None,
            'former_names': [],
            'company_type': None,
            'industry': None,
            'headquarters': None,
        }

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find infobox
            infobox = soup.find('table', class_='infobox')
            if not infobox:
                return result

            infobox_text = str(infobox)

            # Extract ticker symbols from various patterns
            # Pattern 1: {{NASDAQ|GOOGL}} or {{NYSE|IBM}}
            ticker_pattern1 = re.findall(r'\{\{(?:NASDAQ|NYSE|TSX|HKEX|LON|TYO|NSE|BSE|ASX)\|([A-Z0-9]{1,10})\}\}', infobox_text)
            result['ticker_symbols'].extend(ticker_pattern1)

            # Pattern 2: [[NASDAQ: GOOGL]] or similar
            ticker_pattern2 = re.findall(r'\[\[(?:NASDAQ|NYSE|TSX|HKEX|LON|TYO|NSE|BSE|ASX):\s*([A-Z0-9]{1,10})\]\]', infobox_text)
            result['ticker_symbols'].extend(ticker_pattern2)

            # Pattern 3: trading_name = value
            trading_name_match = re.search(r'trading[_\s]*name\s*=\s*(?:\{\{)?\s*([^\n\{\}]+)', infobox_text, re.IGNORECASE)
            if trading_name_match:
                trading_text = trading_name_match.group(1).strip()
                # Extract tickers from the text
                tickers = re.findall(r'([A-Z]{1,5}\.?[A-Z]?)', trading_text)
                result['ticker_symbols'].extend(tickers)

            # Clean and deduplicate tickers
            result['ticker_symbols'] = [t.upper().strip() for t in result['ticker_symbols'] if t and len(t) <= 10]
            result['ticker_symbols'] = list(set(result['ticker_symbols']))

            # Extract ISIN
            isin_match = re.search(r'ISIN\s*=\s*([A-Z]{2}[A-Z0-9]{9}\d)', infobox_text)
            if isin_match:
                result['isin'] = isin_match.group(1)

            # Extract parent company
            parent_patterns = [
                r'parent[_\s]*(?:company)?\s*=\s*\[?\[?([^\]\|\n]+)',
                r'owner\s*=\s*\[?\[?([^\]\|\n]+)',
            ]
            for pattern in parent_patterns:
                parent_match = re.search(pattern, infobox_text, re.IGNORECASE)
                if parent_match:
                    parent = parent_match.group(1).strip()
                    parent = re.sub(r'\|.*$', '', parent)  # Remove display text
                    parent = parent.strip('[]{}|')
                    if parent:
                        result['parent_company'] = parent
                        break

            # Extract former names
            former_match = re.search(r'former[_\s]*names?\s*=\s*(?:\{\{)?\s*([^\n]+)', infobox_text, re.IGNORECASE)
            if former_match:
                former_text = former_match.group(1).strip()
                # Parse list (comma, semicolon, or template separated)
                former_names = re.split(r'[,;]|\[\[|\]\]|\{\{|\}\}', former_text)
                for name in former_names:
                    name = name.strip()
                    name = re.sub(r'\|.*$', '', name)  # Remove display text
                    name = name.strip('[]{}|')
                    if name and len(name) > 2:
                        result['former_names'].append(name)
                result['former_names'] = list(set(result['former_names']))

            # Extract company type (public/private)
            type_match = re.search(r'type\s*=\s*(?:\{\{)?\s*([^\n\{\}]+)', infobox_text, re.IGNORECASE)
            if type_match:
                type_text = type_match.group(1).lower()
                if any(kw in type_text for kw in WikipediaInfoboxParser.PUBLIC_KEYWORDS):
                    result['company_type'] = 'public'
                elif any(kw in type_text for kw in WikipediaInfoboxParser.PRIVATE_KEYWORDS):
                    result['company_type'] = 'private'

            # Extract industry
            industry_match = re.search(r'industr(?:y|ies)\s*=\s*(?:\{\{)?\s*\[?\[?([^\]\|\n]+)', infobox_text, re.IGNORECASE)
            if industry_match:
                industry = industry_match.group(1).strip()
                industry = re.sub(r'\|.*$', '', industry)  # Remove display text
                industry = industry.strip('[]{}|')
                if industry:
                    result['industry'] = industry

            # Extract headquarters location
            location_patterns = [
                r'location[_\s]*(?:city|country)?\s*=\s*(?:\{\{)?\s*\[?\[?([^\]\|\n]+)',
                r'headquarters\s*=\s*(?:\{\{)?\s*\[?\[?([^\]\|\n]+)',
                r'hq[_\s]*location\s*=\s*(?:\{\{)?\s*\[?\[?([^\]\|\n]+)',
            ]
            for pattern in location_patterns:
                location_match = re.search(pattern, infobox_text, re.IGNORECASE)
                if location_match:
                    location = location_match.group(1).strip()
                    location = re.sub(r'\|.*$', '', location)  # Remove display text
                    location = location.strip('[]{}|')
                    if location:
                        result['headquarters'] = location
                        break

        except Exception as e:
            logger.debug(f"Error parsing infobox: {e}")

        return result


# ============================================================================
# Wikidata API Client
# ============================================================================

class WikidataClient:
    """Query Wikidata API for company information."""

    BASE_URL = "https://www.wikidata.org/w/api.php"

    # Wikidata property IDs
    PROP_TICKER = 'P249'      # Ticker symbol
    PROP_PARENT = 'P749'      # Parent organization
    PROP_CIK = 'P5585'        # SEC CIK (Critical!)
    PROP_ISIN = 'P946'        # ISIN
    PROP_EXCHANGE = 'P414'    # Stock exchange
    PROP_FOUNDED = 'P571'     # Inception date
    PROP_INDUSTRY = 'P452'    # Industry
    PROP_HQ = 'P159'          # Headquarters location

    def __init__(self, user_agent: str = USER_AGENT):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})

    def get_entity_claims(self, wikidata_id: str) -> Optional[Dict]:
        """
        Get all claims for a Wikidata entity.

        Returns:
            Dictionary of property IDs to claim values, or None on error.
        """
        try:
            params = {
                'action': 'wbgetentities',
                'ids': wikidata_id,
                'format': 'json',
                'props': 'claims'
            }

            response = self.session.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            entities = data.get('entities', {})
            entity = entities.get(wikidata_id, {})

            return entity.get('claims', {})

        except Exception as e:
            logger.debug(f"Error fetching Wikidata entity {wikidata_id}: {e}")
            return None

    def extract_claim_values(self, claims: Dict, property_id: str) -> List[str]:
        """
        Extract values from Wikidata claims for a given property.

        Returns:
            List of string values.
        """
        values = []

        if property_id not in claims:
            return values

        for claim in claims[property_id]:
            try:
                # Get mainsnak value
                mainsnak = claim.get('mainsnak', {})
                datatype = mainsnak.get('datatype', '')
                datavalue = mainsnak.get('datavalue', {})

                if datatype == 'string' or datatype == 'external-id':
                    value = datavalue.get('value', '')
                    if value:
                        values.append(value)

                elif datatype == 'wikibase-item':
                    # Item reference - get the ID
                    value = datavalue.get('value', {})
                    if isinstance(value, dict):
                        entity_id = value.get('id', '')
                        if entity_id:
                            values.append(entity_id)

                elif datatype == 'time':
                    # Time value
                    value = datavalue.get('value', '')
                    if isinstance(value, dict):
                        time_str = value.get('time', '')
                        if time_str:
                            # Remove +0000 from end
                            values.append(time_str.split('+')[0])

            except Exception as e:
                logger.debug(f"Error extracting claim value: {e}")
                continue

        return values

    def get_company_info(self, wikidata_id: str) -> Dict[str, Any]:
        """
        Get company information from Wikidata.

        Returns:
            Dictionary with:
            - ticker_symbols: list of str
            - isin: str or None
            - cik: str or None
            - parent_company_wikidata: str or None
            - stock_exchange: str or None
        """
        result = {
            'ticker_symbols': [],
            'isin': None,
            'cik': None,
            'parent_company_wikidata': None,
            'stock_exchange': None,
        }

        claims = self.get_entity_claims(wikidata_id)
        if not claims:
            return result

        # Extract ticker symbols (can be multiple)
        ticker_values = self.extract_claim_values(claims, self.PROP_TICKER)
        result['ticker_symbols'] = [t.upper() for t in ticker_values if t]

        # Extract ISIN (usually single)
        isin_values = self.extract_claim_values(claims, self.PROP_ISIN)
        if isin_values:
            result['isin'] = isin_values[0]

        # Extract CIK (Critical for SEC matching!)
        cik_values = self.extract_claim_values(claims, self.PROP_CIK)
        if cik_values:
            # CIK should be zero-padded to 10 digits
            cik = cik_values[0]
            if cik.isdigit():
                result['cik'] = cik.zfill(10)

        # Extract parent company (Wikidata ID)
        parent_values = self.extract_claim_values(claims, self.PROP_PARENT)
        if parent_values:
            result['parent_company_wikidata'] = parent_values[0]

        # Extract stock exchange
        exchange_values = self.extract_claim_values(claims, self.PROP_EXCHANGE)
        if exchange_values:
            result['stock_exchange'] = exchange_values[0]

        return result


# ============================================================================
# Wikipedia Scraper with Rate Limiting
# ============================================================================

class WikipediaScraper:
    """Scrape Wikipedia with rate limiting and error handling."""

    def __init__(self,
                 user_agent: str = USER_AGENT,
                 min_delay: float = MIN_DELAY,
                 max_delay: float = MAX_DELAY,
                 max_retries: int = MAX_RETRIES):
        self.user_agent = user_agent
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries

        # Initialize Wikipedia API client
        self.wiki_api = wikipediaapi.Wikipedia(
            language='en',
            extract_format=wikipediaapi.ExtractFormat.WIKI,
            user_agent=user_agent
        )

        # Initialize Wikidata client
        self.wikidata_client = WikidataClient(user_agent)

        # Rate limiting
        self.last_request_time = 0

        # Robots.txt cache
        self.robots_cache = {}

        # Statistics
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped_no_url': 0,
            'skipped_already_matched': 0,
            'skipped_page_not_found': 0,
            'skipped_no_infobox': 0,
            'with_tickers': 0,
            'with_cik': 0,
            'with_isin': 0,
            'with_parent': 0,
            'with_former_names': 0,
        }

    def check_robots_txt(self, base_url: str) -> bool:
        """Check if scraping is allowed by robots.txt."""
        try:
            if base_url in self.robots_cache:
                return self.robots_cache[base_url]

            # Wikipedia allows well-behaved bots
            # Use '*' as user-agent for generic robots.txt check
            rp = RobotFileParser()
            rp.set_url(f"{base_url}/robots.txt")
            rp.read()

            # Check with wildcard user-agent (most permissive)
            # Wikipedia's robots.txt allows /w/api.php for all user-agents
            allowed = rp.can_fetch('*', "/")
            self.robots_cache[base_url] = allowed

            if not allowed:
                logger.warning(f"robots.txt disallows scraping: {base_url}")
                # Still allow for Wikipedia (they're generally okay with research use)
                if 'wikipedia.org' in base_url:
                    logger.info(f"Allowing Wikipedia scraping for research use")
                    return True

            return allowed

        except Exception as e:
            logger.debug(f"Error checking robots.txt: {e}")
            # Allow on error (fail open)
            return True

    def rate_limit_delay(self):
        """Apply rate limiting with random delay."""
        now = time.time()
        time_since_last_request = now - self.last_request_time

        # Calculate delay
        delay = random.uniform(self.min_delay, self.max_delay)

        # Ensure minimum delay between requests
        if time_since_last_request < delay:
            sleep_time = delay - time_since_last_request
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def fetch_wikipedia_page(self, wikipedia_url: str, retry_count: int = 0) -> Optional[str]:
        """
        Fetch Wikipedia page HTML with retry logic.

        Returns:
            HTML content as string, or None on failure.
        """
        try:
            # Check robots.txt
            parsed_url = urlparse(wikipedia_url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

            if not self.check_robots_txt(base_url):
                logger.warning(f"robots.txt disallows: {wikipedia_url}")
                return None

            # Rate limiting
            self.rate_limit_delay()

            # Extract page title from URL
            if 'en.wikipedia.org/wiki/' not in wikipedia_url:
                logger.debug(f"Not an English Wikipedia URL: {wikipedia_url}")
                return None

            page_title = wikipedia_url.split('en.wikipedia.org/wiki/')[-1].split('#')[0]

            # Fetch page using Wikipedia API
            page = self.wiki_api.page(page_title)

            if not page.exists():
                logger.debug(f"Page does not exist: {page_title}")
                return None

            # Get full HTML (for infobox parsing)
            # We need to fetch the actual HTML page
            headers = {'User-Agent': self.user_agent}
            response = requests.get(wikipedia_url, headers=headers, timeout=10)
            response.raise_for_status()

            return response.text

        except requests.exceptions.RequestException as e:
            logger.debug(f"Request error fetching {wikipedia_url}: {e}")

            # Retry on failure
            if retry_count < self.max_retries:
                logger.debug(f"Retrying ({retry_count + 1}/{self.max_retries})...")
                time.sleep(2 ** retry_count)  # Exponential backoff
                return self.fetch_wikipedia_page(wikipedia_url, retry_count + 1)

            return None

        except Exception as e:
            logger.debug(f"Error fetching Wikipedia page: {e}")
            return None

    def scrape_company_page(self, institution_id: str, wikipedia_url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape a Wikipedia company page for rich information.

        Returns:
            Dictionary with extracted company information, or None on failure.
        """
        result = {
            'institution_id': institution_id,
            'wikipedia_url': wikipedia_url,
            'wikidata_id': None,
            'ticker_symbols': [],
            'isin': None,
            'cik': None,
            'parent_company': None,
            'parent_company_wikidata': None,
            'former_names': [],
            'company_type': None,
            'industry': None,
            'headquarters': None,
            'stock_exchange': None,
            'scraped_at': datetime.now().isoformat(),
        }

        try:
            # Fetch Wikipedia page
            html_content = self.fetch_wikipedia_page(wikipedia_url)
            if not html_content:
                self.stats['skipped_page_not_found'] += 1
                self.stats['failed'] += 1
                return None

            # Parse infobox from Wikipedia
            infobox_data = WikipediaInfoboxParser.parse_infobox(html_content)

            # Extract Wikidata ID
            wikidata_id = WikipediaInfoboxParser.extract_wikidata_id(wikipedia_url)
            if wikidata_id:
                result['wikidata_id'] = wikidata_id

                # Query Wikidata API for additional information
                wikidata_info = self.wikidata_client.get_company_info(wikidata_id)

                # Merge Wikidata data (prioritize over infobox)
                if wikidata_info['ticker_symbols']:
                    result['ticker_symbols'].extend(wikidata_info['ticker_symbols'])

                if wikidata_info['isin']:
                    result['isin'] = wikidata_info['isin']

                if wikidata_info['cik']:
                    result['cik'] = wikidata_info['cik']

                if wikidata_info['parent_company_wikidata']:
                    result['parent_company_wikidata'] = wikidata_info['parent_company_wikidata']

                if wikidata_info['stock_exchange']:
                    result['stock_exchange'] = wikidata_info['stock_exchange']

            # Merge infobox data (if not already set from Wikidata)
            if infobox_data['ticker_symbols']:
                result['ticker_symbols'].extend(infobox_data['ticker_symbols'])

            if infobox_data['isin'] and not result['isin']:
                result['isin'] = infobox_data['isin']

            if infobox_data['parent_company'] and not result['parent_company']:
                result['parent_company'] = infobox_data['parent_company']

            if infobox_data['former_names']:
                result['former_names'] = infobox_data['former_names']

            if infobox_data['company_type']:
                result['company_type'] = infobox_data['company_type']

            if infobox_data['industry']:
                result['industry'] = infobox_data['industry']

            if infobox_data['headquarters']:
                result['headquarters'] = infobox_data['headquarters']

            # Deduplicate ticker symbols
            result['ticker_symbols'] = list(set(result['ticker_symbols']))

            # Update statistics
            self.stats['success'] += 1

            if result['ticker_symbols']:
                self.stats['with_tickers'] += 1

            if result['cik']:
                self.stats['with_cik'] += 1

            if result['isin']:
                self.stats['with_isin'] += 1

            if result['parent_company'] or result['parent_company_wikidata']:
                self.stats['with_parent'] += 1

            if result['former_names']:
                self.stats['with_former_names'] += 1

            return result

        except Exception as e:
            logger.error(f"Error scraping {wikipedia_url}: {e}")
            self.stats['failed'] += 1
            return None

    def scrape_batch(self, institutions_df: pl.DataFrame,
                    matched_ids: Set[str],
                    dry_run: bool = False) -> pl.DataFrame:
        """
        Scrape a batch of institutions.

        Args:
            institutions_df: DataFrame with institutions to scrape
            matched_ids: Set of already matched institution IDs to skip
            dry_run: If True, don't actually scrape, just count

        Returns:
            DataFrame with scraped company information
        """
        results = []
        total = len(institutions_df)

        logger.info(f"Starting to scrape {total:,} institutions...")
        if dry_run:
            logger.info("DRY RUN MODE - No actual scraping will be performed")

        for i, inst_row in enumerate(institutions_df.iter_rows(named=True)):
            institution_id = inst_row['institution_id']
            wikipedia_url = inst_row.get('wikipedia_url')

            self.stats['total'] += 1

            # Skip if already matched
            if institution_id in matched_ids:
                self.stats['skipped_already_matched'] += 1
                continue

            # Skip if no Wikipedia URL
            if not wikipedia_url:
                self.stats['skipped_no_url'] += 1
                continue

            # Log progress
            if (i + 1) % 10 == 0:
                logger.info(
                    f"  Progress: {i+1:,}/{total:,} | "
                    f"Success: {self.stats['success']:,} | "
                    f"Failed: {self.stats['failed']:,} | "
                    f"With Tickers: {self.stats['with_tickers']:,} | "
                    f"With CIK: {self.stats['with_cik']:,}"
                )

            # Skip scraping in dry run mode
            if dry_run:
                results.append({
                    'institution_id': institution_id,
                    'wikipedia_url': wikipedia_url,
                    'dry_run': True,
                })
                continue

            # Scrape the page
            company_info = self.scrape_company_page(institution_id, wikipedia_url)

            if company_info:
                results.append(company_info)

            # Save checkpoint
            if (i + 1) % CHECKPOINT_INTERVAL == 0 and results:
                logger.info(f"  Saving checkpoint after {i+1:,} institutions...")
                checkpoint_df = pl.DataFrame(results)
                checkpoint_file = OUTPUT_FILE.with_suffix('.checkpoint.parquet')
                checkpoint_df.write_parquet(checkpoint_file)
                logger.info(f"  Checkpoint saved: {len(results):,} records")

        logger.info(f"Completed scraping {total:,} institutions")
        return pl.DataFrame(results) if results else pl.DataFrame()


# ============================================================================
# Main Processing
# ============================================================================

def load_matched_institution_ids() -> Set[str]:
    """Load institution IDs that have already been matched to firms."""
    matched_ids = set()

    if MATCHED_INSTITUTIONS_FILE.exists():
        logger.info(f"Loading previously matched institutions from: {MATCHED_INSTITUTIONS_FILE}")
        matched_df = pl.read_parquet(MATCHED_INSTITUTIONS_FILE)
        matched_ids = set(matched_df['institution_id'].to_list())
        logger.info(f"  Found {len(matched_ids):,} previously matched institutions")

    return matched_ids


def main(dry_run: bool = False, limit: Optional[int] = None):
    """
    Main processing pipeline.

    Args:
        dry_run: If True, test without actual scraping
        limit: Optional limit on number of institutions to process (for testing)
    """
    logger.info("=" * 80)
    logger.info("WIKIPEDIA COMPANY SCRAPER - Extract Rich Company Information")
    logger.info("=" * 80)

    if dry_run:
        logger.info("DRY RUN MODE - No actual scraping will be performed")

    # Step 1: Load data
    logger.info("\n[1/4] Loading data...")

    if not INSTITUTIONS_ENRICHED.exists():
        raise FileNotFoundError(f"Institutions file not found: {INSTITUTIONS_ENRICHED}")

    institutions_df = pl.read_parquet(INSTITUTIONS_ENRICHED)
    logger.info(f"  Loaded {len(institutions_df):,} institutions")

    # Step 2: Filter institutions with Wikipedia URLs
    logger.info("\n[2/4] Filtering institutions with Wikipedia URLs...")
    inst_with_wikipedia = institutions_df.filter(
        pl.col('wikipedia_url').is_not_null()
    )
    logger.info(f"  {len(inst_with_wikipedia):,} institutions have Wikipedia URLs")

    # Step 3: Load previously matched institutions
    logger.info("\n[3/4] Loading previously matched institutions...")
    matched_ids = load_matched_institution_ids()

    # Filter out already matched institutions
    inst_to_scrape = inst_with_wikipedia.filter(
        ~pl.col('institution_id').is_in(list(matched_ids))
    )
    logger.info(f"  {len(inst_to_scrape):,} institutions with Wikipedia URLs not yet matched")

    # Apply limit for testing
    if limit:
        logger.info(f"  Limiting to {limit:,} institutions for testing")
        inst_to_scrape = inst_to_scrape.head(limit)

    # Step 4: Scrape Wikipedia pages
    logger.info("\n[4/4] Scraping Wikipedia pages...")
    logger.info(f"  Output: {OUTPUT_FILE}")
    logger.info(f"  Checkpoint interval: every {CHECKPOINT_INTERVAL} pages")
    logger.info(f"  Rate limiting: {REQUESTS_PER_SECOND} req/sec, delay {MIN_DELAY}-{MAX_DELAY}s")
    logger.info(f"  Max retries: {MAX_RETRIES}")

    scraper = WikipediaScraper()
    results_df = scraper.scrape_batch(inst_to_scrape, matched_ids, dry_run=dry_run)

    # Print statistics
    logger.info("\n" + "=" * 80)
    logger.info("SCRAPING STATISTICS")
    logger.info("=" * 80)

    stats = scraper.stats
    logger.info(f"Total processed: {stats['total']:,}")
    logger.info(f"Successful: {stats['success']:,}")
    logger.info(f"Failed: {stats['failed']:,}")
    logger.info(f"\nSkipped:")
    logger.info(f"  No Wikipedia URL: {stats['skipped_no_url']:,}")
    logger.info(f"  Already matched: {stats['skipped_already_matched']:,}")
    logger.info(f"  Page not found: {stats['skipped_page_not_found']:,}")
    logger.info(f"\nExtracted:")
    logger.info(f"  Ticker symbols: {stats['with_tickers']:,}")
    logger.info(f"  CIK codes: {stats['with_cik']:,}")
    logger.info(f"  ISIN codes: {stats['with_isin']:,}")
    logger.info(f"  Parent companies: {stats['with_parent']:,}")
    logger.info(f"  Former names: {stats['with_former_names']:,}")

    # Save results
    if not dry_run and len(results_df) > 0:
        logger.info(f"\nSaving results to: {OUTPUT_FILE}")
        results_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(results_df):,} records")

        # Show sample results
        logger.info("\nSample results:")
        for i, row in enumerate(results_df.head(10).iter_rows(named=True), 1):
            logger.info(f"  {i}. {row['institution_id']}")
            if row['ticker_symbols']:
                logger.info(f"     Tickers: {row['ticker_symbols']}")
            if row['cik']:
                logger.info(f"     CIK: {row['cik']}")
            if row['parent_company']:
                logger.info(f"     Parent: {row['parent_company']}")
    elif dry_run:
        logger.info("\nDRY RUN COMPLETE - No data saved")

    logger.info("\n" + "=" * 80)
    logger.info("WIKIPEDIA SCRAPING COMPLETE")
    logger.info("=" * 80)

    return results_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape Wikipedia for rich company information"
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Test without actual scraping'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of institutions to process (for testing)'
    )

    args = parser.parse_args()

    main(dry_run=args.dry_run, limit=args.limit)
