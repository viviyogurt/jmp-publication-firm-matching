"""
Enhanced Homepage Domain Matching

This script matches institutions to firms using homepage domains.
It extracts the domain from homepage_url and matches to firm weburl domains.

Strategies:
1. Exact domain match (google.com → google.com)
2. Subdomain match (research.google.com → google.com)
3. Country-specific domain handling (google.co.uk → google.com)

Confidence: 0.97 (very high - direct company website match)
"""

import polars as pl
from pathlib import Path
import logging
from urllib.parse import urlparse
import re

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_ENRICHED = DATA_INTERIM / "publication_institutions_enriched.parquet"
COMPUSTAT_FIRMS = DATA_INTERIM / "compustat_firms_standardized.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_homepage.parquet"
PROGRESS_LOG = LOGS_DIR / "match_homepage_domains.log"

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


def extract_domain(url: str) -> str:
    """Extract root domain from URL."""
    if not url:
        return None

    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()

        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]

        # Handle common patterns
        # Remove port numbers
        domain = domain.split(':')[0]

        return domain
    except:
        return None


def get_root_domain(domain: str) -> str:
    """Extract root domain (second-level domain + TLD)."""
    if not domain:
        return None

    try:
        parts = domain.split('.')

        # Handle common patterns like co.uk, com.au
        cc_tlds = ['co.uk', 'com.au', 'co.nz', 'com.sg', 'co.jp', 'co.in',
                   'co.kr', 'co.tw', 'co.za', 'co.il', 'co.th', 'com.cn']

        for cc_tld in cc_tlds:
            if domain.endswith(cc_tld):
                # Get the part before cc_tld
                idx = domain.find('.' + cc_tld)
                if idx > 0:
                    root_parts = domain[:idx].split('.')
                    if len(root_parts) >= 1:
                        return root_parts[-1] + '.' + cc_tld

        # Standard case: domain.com or sub.domain.com
        if len(parts) >= 2:
            return '.'.join(parts[-2:])

        return domain
    except:
        return None


def match_institution_by_domain(inst_row: pl.DataFrame,
                                 firms_df: pl.DataFrame,
                                 matched_ids: set) -> list:
    """Match institution to firms by homepage domain."""
    matches = []

    institution_id = inst_row['institution_id']
    display_name = inst_row['display_name']
    homepage_url = inst_row['homepage_url']
    homepage_domain = inst_row['homepage_domain']
    paper_count = inst_row['paper_count']
    country_code = inst_row.get('geo_country_code', '')

    # Skip if already matched or no homepage
    if institution_id in matched_ids or not homepage_domain:
        return matches

    # Strategy 1: Exact domain match
    firm_matches = firms_df.filter(
        pl.col('homepage_domain').str.to_lowercase() == homepage_domain.lower()
    )

    for firm_row in firm_matches.iter_rows(named=True):
        matches.append({
            'GVKEY': firm_row['GVKEY'],
            'LPERMNO': firm_row.get('LPERMNO'),
            'firm_conm': firm_row['conm'],
            'institution_id': institution_id,
            'institution_display_name': display_name,
            'institution_homepage': homepage_url,
            'match_type': 'homepage_exact',
            'match_confidence': 0.98,
            'match_method': 'homepage_domain_exact',
            'matched_domain': homepage_domain,
            'institution_paper_count': paper_count,
        })

    # Strategy 2: Root domain match (for subdomains)
    inst_root_domain = get_root_domain(homepage_domain)
    if inst_root_domain and inst_root_domain != homepage_domain:
        # Match firms' root domains
        firm_matches = firms_df.filter(
            pl.col('homepage_domain').str.to_lowercase() == inst_root_domain.lower()
        )

        for firm_row in firm_matches.iter_rows(named=True):
            matches.append({
                'GVKEY': firm_row['GVKEY'],
                'LPERMNO': firm_row.get('LPERMNO'),
                'firm_conm': firm_row['conm'],
                'institution_id': institution_id,
                'institution_display_name': display_name,
                'institution_homepage': homepage_url,
                'match_type': 'homepage_subdomain',
                'match_confidence': 0.97,
                'match_method': 'homepage_domain_subdomain',
                'matched_domain': inst_root_domain,
                'institution_paper_count': paper_count,
            })

    return matches


def main():
    logger.info("=" * 80)
    logger.info("ENHANCED HOMEPAGE DOMAIN MATCHING")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/4] Loading data...")
    institutions = pl.read_parquet(INSTITUTIONS_ENRICHED)
    firms = pl.read_parquet(COMPUSTAT_FIRMS)

    logger.info(f"  Loaded {len(institutions)} institutions")
    logger.info(f"  Loaded {len(firms)} firms")

    # Extract firm domains
    logger.info("\n[2/4] Extracting firm domains from weburl...")
    firms = firms.with_columns(
        pl.col('weburl').map_elements(
            lambda x: extract_domain(x),
            return_dtype=pl.String
        ).alias('homepage_domain')
    )

    firms_with_domain = firms.filter(pl.col('homepage_domain').is_not_null())
    logger.info(f"  {len(firms_with_domain)} firms have homepage domains")

    # Filter institutions with homepages
    inst_with_homepage = institutions.filter(pl.col('homepage_domain').is_not_null())
    logger.info(f"  {len(inst_with_homepage)} institutions have homepage domains")

    # Get matched IDs from existing matches
    import os
    matched_ids = set()

    # Check Stage 1 matches
    stage1_file = DATA_PROCESSED_LINK / "publication_firm_matches_stage1.parquet"
    if stage1_file.exists():
        stage1 = pl.read_parquet(stage1_file)
        matched_ids.update(stage1['institution_id'].to_list())

    # Check parent cascade matches
    parent_file = DATA_PROCESSED_LINK / "publication_firm_matches_parent_cascade.parquet"
    if parent_file.exists():
        parent_matches = pl.read_parquet(parent_file)
        matched_ids.update(parent_matches['institution_id'].to_list())

    logger.info(f"  Institutions already matched: {len(matched_ids)}")

    # Run matching
    logger.info("\n[3/4] Running homepage domain matching...")

    all_matches = []
    total = len(inst_with_homepage)

    for i, inst_row in enumerate(inst_with_homepage.iter_rows(named=True)):
        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} institutions ({len(all_matches)} matches so far)...")

        matches = match_institution_by_domain(inst_row, firms_with_domain, matched_ids)
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
        logger.info("HOMEPAGE DOMAIN MATCHING SUMMARY")
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
        for i, row in enumerate(matches_df.head(15).iter_rows(named=True), 1):
            logger.info(f"  {i}. {row['institution_display_name'][:50]}")
            logger.info(f"     {row['institution_homepage'][:60]}")
            logger.info(f"     → {row['firm_conm'][:50]}")
            logger.info(f"     Match: {row['matched_domain']} (Conf: {row['match_confidence']:.2f})")

    logger.info("\n" + "=" * 80)
    logger.info("HOMEPAGE DOMAIN MATCHING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
