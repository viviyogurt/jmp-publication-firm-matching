"""
Smart URL/Domain Matching with Wikipedia Extraction

This script implements intelligent URL/domain matching using:
1. Extract company names from Wikipedia URLs
2. Root domain matching (google.com not www.google.com)
3. Parent institution cascade (via parent_institution_ids)
4. Subdomain handling (research.google.com → google.com)
5. Domain normalization and comparison

Confidence: 0.95-0.99 depending on match strength
"""

import polars as pl
from pathlib import Path
import logging
import re
from urllib.parse import urlparse
from typing import Optional, Set, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_ENRICHED = DATA_INTERIM / "publication_institutions_enriched.parquet"
COMPUSTAT_FIRMS = DATA_INTERIM / "compustat_firms_standardized.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_smart_urls.parquet"
PROGRESS_LOG = LOGS_DIR / "match_smart_urls.log"

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


def extract_domain_from_url(url: str) -> Optional[str]:
    """Extract root domain from URL, handling edge cases."""
    if not url or url == 'None':
        return None

    try:
        # Parse URL
        if not url.startswith(('http://', 'https://')):
            url = 'http://' + url

        parsed = urlparse(url)
        domain = parsed.netloc.lower()

        # Remove port
        domain = domain.split(':')[0]

        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]

        return domain if domain else None
    except:
        return None


def get_root_domain(domain: str) -> str:
    """Extract root domain (e.g., google.com from www.google.com)."""
    if not domain:
        return domain

    parts = domain.split('.')

    # Handle country code TLDs (co.uk, com.au, etc.)
    cc_tlds = ['co.uk', 'com.au', 'co.nz', 'com.sg', 'co.jp', 'co.in',
               'co.kr', 'co.tw', 'co.za', 'co.il', 'co.th', 'com.cn',
               'com.br', 'com.au', 'com.mx', 'com.my', 'com.ph', 'com.vn',
               'co.th', 'co.id', 'co.kr']

    for cc_tld in cc_tlds:
        if domain.endswith(cc_tld):
            idx = domain.find('.' + cc_tld)
            if idx > 0:
                root_parts = domain[:idx].split('.')
                if len(root_parts) >= 1:
                    return root_parts[-1] + '.' + cc_tld

    # Standard case: domain.com or sub.domain.com
    if len(parts) >= 2:
        return '.'.join(parts[-2:])

    return domain


def extract_company_name_from_wikipedia(wikipedia_url: str) -> Optional[str]:
    """Extract company name from Wikipedia URL."""
    if not wikipedia_url:
        return None

    try:
        # Extract from URL path: http://en.wikipedia.org/wiki/Google
        # or: https://en.wikipedia.org/wiki/Google_DeepMind

        # Remove protocol
        url = wikipedia_url.replace('http://', '').replace('https://', '')

        # Remove en.wikipedia.org/wiki/
        if 'en.wikipedia.org/wiki/' in url:
            # Get the page title
            page_title = url.split('en.wikipedia.org/wiki/')[-1]
            # Replace underscores with spaces
            company_name = page_title.replace('_', ' ')
            return company_name

        return None
    except:
        return None


def normalize_company_name(name: str) -> str:
    """Normalize company name for comparison."""
    if not name:
        return ''

    # Convert to lowercase
    name = name.lower().strip()

    # Remove common suffixes
    suffixes = [' inc', ' ltd', ' corp', ' llc', ' plc', ' gmbh', ' ag', ' co', ' corporation',
                  ' company', ' industries', ' international', ' worldwide', ' group',
                  ' holdings', ' limited', ' technologies', ' solutions']

    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()

    return name


def match_institution_by_smart_urls(inst_row: pl.DataFrame,
                                  firms_df: pl.DataFrame,
                                  inst_dict: dict,
                                  matched_ids: set) -> list:
    """Match institution using smart URL logic."""
    matches = []

    institution_id = inst_row['institution_id']
    display_name = inst_row['display_name']
    wikipedia_url = inst_row.get('wikipedia_url')
    homepage_url = inst_row.get('homepage_url')
    homepage_domain = inst_row.get('homepage_domain')
    paper_count = inst_row['paper_count']
    parent_ids = inst_row.get('parent_institution_ids') or []

    # Skip if already matched
    if institution_id in matched_ids:
        return matches

    # Strategy 1: Wikipedia company name match
    if wikipedia_url:
        wiki_company = extract_company_name_from_wikipedia(wikipedia_url)
        if wiki_company:
            wiki_normalized = normalize_company_name(wiki_company)

            # Match to firm names
            firm_matches = firms_df.filter(
                pl.col('conm_clean').str.to_lowercase().str.contains(wiki_normalized) |
                pl.col('conml_clean').str.to_lowercase().str.contains(wiki_normalized)
            )

            for firm_row in firm_matches.iter_rows(named=True):
                confidence = 0.98 if wiki_normalized in firm_row['conm_clean'].lower() else 0.97
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row['conm'],
                    'institution_id': institution_id,
                    'institution_display_name': display_name,
                    'wikipedia_url': wikipedia_url,
                    'match_type': 'wikipedia_name',
                    'match_confidence': confidence,
                    'match_method': 'wikipedia_company_name',
                    'matched_value': wiki_company,
                    'institution_paper_count': paper_count,
                })

            if matches:
                return matches  # Use Wikipedia match if found

    # Strategy 2: Homepage domain exact match
    if homepage_domain:
        # Match exact domains
        firm_domains = firms_df.filter(pl.col('homepage_domain') == homepage_domain)

        if firm_domains.shape[0] > 0:
            for firm_row in firm_domains.iter_rows(named=True):
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row['conm'],
                    'institution_id': institution_id,
                    'institution_display_name': display_name,
                    'homepage_url': homepage_url,
                    'match_type': 'homepage_exact',
                    'match_confidence': 0.98,
                    'match_method': 'homepage_domain_exact',
                    'matched_domain': homepage_domain,
                    'institution_paper_count': paper_count,
                })

            return matches

    # Strategy 3: Root domain match
    root_domain = get_root_domain(homepage_domain) if homepage_domain else None

    if root_domain and root_domain != homepage_domain:
        firm_root_domains = firms_df.with_columns(
            pl.col('homepage_domain').map_elements(
                lambda x: get_root_domain(x) if x else None,
                return_dtype=pl.String
            ).alias('root_domain')
        )

        firm_matches = firm_root_domains.filter(
            pl.col('root_domain') == root_domain
        )

        if firm_matches.shape[0] > 0:
            for firm_row in firm_matches.iter_rows(named=True):
                matches.append({
                    'GVKEY:': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row['conm'],
                    'institution_id': institution_id,
                    'institution_display_name': display_name,
                    'homepage_url': homepage_url,
                    'match_type': 'homepage_root_domain',
                    'match_confidence': 0.96,
                    'match_method': 'homepage_root_domain',
                    'matched_domain': root_domain,
                    'institution_paper_count': paper_count,
                })

            return matches

    # Strategy 4: Parent institution URL cascade
    # If this institution has parents, check if any parent matched to a firm
    if parent_ids:
        for parent_id in parent_ids[:3]:  # Check up to 3 parents
            if parent_id in inst_dict:
                parent_inst = inst_dict[parent_id]
                parent_homepage = parent_inst.get('homepage_url')
                parent_display_name = parent_inst.get('display_name', '')

                if parent_homepage:
                    # Check if parent homepage matches any firm
                    parent_domain = extract_domain_from_url(parent_homepage)
                    if parent_domain:
                        firm_matches = firms_df.filter(
                            pl.col('homepage_domain') == parent_domain
                        )

                        if firm_matches.shape[0] > 0:
                            for firm_row in firm_matches.iter_rows(named=True):
                                matches.append({
                                    'GVKEY': firm_row['GVKEY'],
                                    'LPERMNO': firm_row.get('LPERMNO'),
                                    'firm_conm': firm_row['conm'],
                                    'institution_id': institution_id,
                                    'institution_display_name': display_name,
                                    'homepage_url': homepage_url,
                                    'match_type': 'parent_url_cascade',
                                    'match_confidence': 0.95,
                                    'match_method': 'parent_homepage_match',
                                    'parent_institution': parent_display_name,
                                    'parent_domain': parent_domain,
                                    'institution_paper_count': paper_count,
                                })

                            return matches

    return matches


def main():
    logger.info("=" * 80)
    logger.info("SMART URL/DOMAIN MATCHING WITH WIKIPEDIA EXTRACTION")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/5] Loading data...")
    institutions = pl.read_parquet(INSTITUTIONS_ENRICHED)
    firms = pl.read_parquet(COMPUSTAT_FIRMS)

    logger.info(f"  Loaded {len(institutions)} institutions")
    logger.info(f"  Loaded {len(firms)} firms")

    # Extract firm domains
    firms = firms.with_columns(
        pl.col('weburl').map_elements(
            extract_domain_from_url,
            return_dtype=pl.String
        ).alias('homepage_domain')
    )

    firms_with_domain = firms.filter(pl.col('homepage_domain').is_not_null())
    logger.info(f"  {len(firms_with_domain)} firms have homepage domains")

    # Get institutions with URLs
    inst_with_urls = institutions.filter(
        (pl.col('homepage_url').is_not_null()) |
        (pl.col('wikipedia_url').is_not_null())
    )
    logger.info(f"  {len(inst_with_urls)} institutions have URLs")

    # Build institution lookup
    inst_dict = {row['institution_id']: row for row in institutions.iter_rows(named=True)}

    # Get matched IDs
    matched_ids = set()

    stage1_file = DATA_PROCESSED_LINK / "publication_firm_matches_stage1.parquet"
    if stage1_file.exists():
        stage1 = pl.read_parquet(stage1_file)
        matched_ids.update(stage1['institution_id'].to_list())

    parent_file = DATA_PROCESSED_LINK / "publication_firm_matches_parent_cascade.parquet"
    if parent_file.exists():
        parent_matches = pl.read_parquet(parent_file)
        matched_ids.update(parent_matches['institution_id'].to_list())

    acronym_file = DATA_PROCESSED_LINK / "publication_firm_matches_acronyms.parquet"
    if acronym_file.exists():
        acronym_matches = pl.read_parquet(acronym_file)
        # Don't include acronym matches - too many false positives
        # matched_ids.update(acronym_matches['institution_id'].to_list())
        pass

    logger.info(f"  Institutions already matched: {len(matched_ids)}")

    # Run matching
    logger.info("\n[2/5] Running smart URL matching...")

    all_matches = []
    total = len(inst_with_urls)

    for i, inst_row in enumerate(inst_with_urls.iter_rows(named=True)):
        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} institutions ({len(all_matches)} matches so far)...")

        matches = match_institution_by_smart_urls(inst_row, firms_with_domain, inst_dict, matched_ids)
        all_matches.extend(matches)

    logger.info(f"  Completed. Found {len(all_matches)} total matches")

    # Save output
    logger.info("\n[3/5] Saving matches...")

    if not all_matches:
        logger.warning("  No matches found!")
    else:
        matches_df = pl.DataFrame(all_matches)
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df)} matches to {OUTPUT_FILE}")

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("SMART URL MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df)}")
        logger.info(f"Unique institutions: {matches_df['institution_id'].n_unique()}")
        logger.info(f"Unique firms: {matches_df['GVKEY'].n_unique()}")

        if 'match_type' in matches_df.columns:
            logger.info("\nMatches by type:")
            for match_type, count in matches_df.group_by('match_type').agg(pl.len().alias('count')).iter_rows(named=True):
                logger.info(f"  {match_type}: {count}")

        # Show examples
        logger.info("\nExample matches:")
        for i, row in enumerate(matches_df.head(20).iter_rows(named=True), 1):
            logger.info(f"  {i}. {row['institution_display_name'][:60]}")
            if 'wikipedia_url' in row and row['wikipedia_url']:
                logger.info(f"     Wikipedia: {row['wikipedia_url']}")
            if 'homepage_url' in row and row['homepage_url']:
                logger.info(f"     Homepage: {row['homepage_url'][:70]}")
            logger.info(f"     → {row['firm_conm'][:50]} (Conf: {row['match_confidence']:.2f})")
            if 'match_method' in row:
                logger.info(f"     Method: {row['match_method']}")

    logger.info("\n" + "=" * 80)
    logger.info("SMART URL MATCHING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
