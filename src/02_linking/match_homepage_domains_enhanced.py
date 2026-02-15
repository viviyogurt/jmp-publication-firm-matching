"""
Method 1.2: Enhanced Homepage Domain Matching

This script enhances the existing homepage domain matching by:
1. Removing 12 identified incorrect matches (from validation)
2. Adding domain-to-firm validation for multi-firm domains
3. Cross-validating with country code (institution.country vs firm.fic)

Incorrect matches to remove (from VALIDATION_500_CLEANED_RESULTS.md):
- AkzoNobel → Courtaulds (GVKEY: various)
- Biogen → Reata Pharmaceuticals
- Getinge → Medtronic
- Jazz Pharmaceuticals → Celator
- Komatsu → Joy Global
- Kuraray → Calgon Carbon
- NeoGenomics → Clarient
- Nokia → Infinera (duplicate, incorrect)
- Vifor Pharma → Relypsa

Reference: src/02_linking/match_homepage_domains.py (existing)
"""

import polars as pl
from pathlib import Path
import logging
from urllib.parse import urlparse
import re
from typing import Dict, List, Optional, Set

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_ENRICHED = DATA_INTERIM / "publication_institutions_enriched.parquet"
COMPUSTAT_FIRMS = DATA_INTERIM / "compustat_firms_standardized.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_homepage_enhanced.parquet"
PROGRESS_LOG = LOGS_DIR / "match_homepage_domains_enhanced.log"

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

# Confidence scores
CONFIDENCE = 0.97

# GVKEYs to exclude (12 incorrect matches from validation)
# Format: {institution_name_pattern: set of GVKEYs to exclude}
EXCLUDED_GVKEYS = {
    # AkzoNobel should not match to Courtaulds
    'AKZONOBEL': {'024706'},  # COURTAULDS PLC

    # Biogen should not match to Reata Pharmaceuticals
    'BIOGEN': {'028377'},  # REATA PHARMACEUTICALS INC

    # Getinge should not match to Medtronic
    'GETINGE': {'066356'},  # MDT CORP

    # Jazz Pharmaceuticals should not match to Celator
    'JAZZ PHARMACEUTICALS': {'036332'},  # CELATOR PHARMACEUTICALS INC

    # Komatsu should not match to Joy Global
    'KOMATSU': {'057528'},  # JOY GLOBAL INC

    # Kuraray should not match to Calgon Carbon
    'KURARAY': {'022100'},  # CALGON CARBON CORP

    # NeoGenomics should not match to Clarient
    'NEOGENOMICS': {'025277'},  # CLARIENT INC

    # Nokia should not match to Infinera (but should match to Nokia Oyj)
    'NOKIA': {'034405'},  # INFINERA CORP

    # Vifor Pharma should not match to Relypsa
    'VIFOR PHARMA': {'034846'},  # RELYPSA INC
}


def extract_domain(url: str) -> Optional[str]:
    """Extract root domain from URL."""
    if not url:
        return None

    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()

        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]

        # Remove port numbers
        domain = domain.split(':')[0]

        return domain
    except:
        return None


def get_root_domain(domain: str) -> Optional[str]:
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


def should_exclude_match(institution_name: str, gvkey: str) -> bool:
    """
    Check if this specific institution-GVKEY match should be excluded.

    Args:
        institution_name: Institution display name (uppercase)
        gvkey: Firm GVKEY (6-digit string)

    Returns:
        True if match should be excluded
    """
    inst_upper = institution_name.upper()

    # Check each exclusion pattern
    for pattern, excluded_gvkeys in EXCLUDED_GVKEYS.items():
        if pattern in inst_upper and gvkey in excluded_gvkeys:
            return True

    return False


def check_country_match(inst_country: Optional[str], firm_fic: Optional[str]) -> bool:
    """Check if institution country matches firm country."""
    if not inst_country or not firm_fic:
        return False

    inst_country = inst_country.upper()[:2]
    firm_country = firm_fic.upper()[:2]

    return inst_country == firm_country


def build_domain_firm_lookup(firms_df: pl.DataFrame) -> Dict[str, List[Dict]]:
    """
    Build a lookup of domains to firms.
    Also tracks domains that map to multiple firms (potential conflicts).

    Args:
        firms_df: Standardized Compustat firms DataFrame

    Returns:
        Dictionary mapping domain → list of firm records
    """
    lookup = {}

    for row in firms_df.iter_rows(named=True):
        gvkey = row['GVKEY']
        lpermno = row.get('LPERMNO')
        conm = row['conm']
        weburl = row.get('weburl')
        fic = row.get('fic')

        if not weburl:
            continue

        domain = extract_domain(weburl)
        if not domain:
            continue

        root_domain = get_root_domain(domain)
        if not root_domain:
            continue

        if root_domain not in lookup:
            lookup[root_domain] = []

        lookup[root_domain].append({
            'GVKEY': gvkey,
            'LPERMNO': lpermno,
            'firm_conm': conm,
            'weburl': weburl,
            'domain': root_domain,
            'fic': fic,
        })

    return lookup


def match_institution_by_domain(inst_row: Dict,
                                domain_lookup: Dict[str, List[Dict]]) -> List[Dict]:
    """
    Match institution to firms by homepage domain with enhanced validation.

    Args:
        inst_row: Institution record
        domain_lookup: Dictionary mapping domain → firm records

    Returns:
        List of match records
    """
    matches = []

    institution_id = inst_row['institution_id']
    display_name = inst_row['display_name']
    homepage_url = inst_row.get('homepage_url')
    homepage_domain = inst_row.get('homepage_domain')
    country_code = inst_row.get('geo_country_code') or inst_row.get('country_code')
    paper_count = inst_row.get('paper_count', 0)

    if not homepage_url and not homepage_domain:
        return matches

    # Extract domain from institution
    inst_domain = None
    if homepage_domain:
        inst_domain = homepage_domain.lower()
    elif homepage_url:
        inst_domain = extract_domain(homepage_url)

    if not inst_domain:
        return matches

    # Get root domain
    inst_root_domain = get_root_domain(inst_domain)
    if not inst_root_domain:
        return matches

    # Look up firms by domain
    if inst_root_domain not in domain_lookup:
        return matches

    firm_candidates = domain_lookup[inst_root_domain]

    # Check each firm candidate
    for firm_record in firm_candidates:
        gvkey = firm_record['GVKEY']
        lpermno = firm_record['LPERMNO']
        firm_conm = firm_record['firm_conm']
        firm_domain = firm_record['domain']
        firm_fic = firm_record['fic']

        # Check if this match should be excluded
        if should_exclude_match(display_name, gvkey):
            logger.debug(f"Excluding match: {display_name} → {firm_conm} (GVKEY: {gvkey})")
            continue

        # Validation: Country match
        country_match = check_country_match(country_code, firm_fic)

        # For domains with multiple firms, require country match
        if len(firm_candidates) > 1 and not country_match:
            logger.debug(f"Skipping multi-firm domain without country match: {display_name} → {firm_conm}")
            continue

        # Valid match
        matches.append({
            'GVKEY': gvkey,
            'LPERMNO': lpermno,
            'firm_conm': firm_conm,
            'institution_id': institution_id,
            'institution_display_name': display_name,
            'match_type': 'homepage_domain_enhanced',
            'match_confidence': CONFIDENCE,
            'match_method': 'homepage_exact_enhanced',
            'institution_domain': inst_domain,
            'firm_domain': firm_domain,
            'country_match': country_match,
            'institution_paper_count': paper_count,
        })

    return matches


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("METHOD 1.2: ENHANCED HOMEPAGE DOMAIN MATCHING")
    logger.info("=" * 80)

    # Step 1: Load data
    logger.info("\n[1/5] Loading data...")

    if not INSTITUTIONS_ENRICHED.exists():
        raise FileNotFoundError(f"Institutions enriched file not found: {INSTITUTIONS_ENRICHED}")
    if not COMPUSTAT_FIRMS.exists():
        raise FileNotFoundError(f"Compustat firms file not found: {COMPUSTAT_FIRMS}")

    institutions = pl.read_parquet(INSTITUTIONS_ENRICHED)
    firms = pl.read_parquet(COMPUSTAT_FIRMS)

    logger.info(f"  Loaded {len(institutions)} institutions")
    logger.info(f"  Loaded {len(firms)} firms")

    # Step 2: Build domain lookup
    logger.info("\n[2/5] Building domain→firm lookup...")

    domain_lookup = build_domain_firm_lookup(firms)

    logger.info(f"  Found {len(domain_lookup):,} unique domains")
    logger.info(f"  Domains with multiple firms: {sum(1 for firms in domain_lookup.values() if len(firms) > 1):,}")

    # Step 3: Run matching
    logger.info("\n[3/5] Running enhanced homepage domain matching...")

    all_matches = []
    total = len(institutions)
    matched_count = 0
    excluded_count = 0

    for i, inst_row in enumerate(institutions.iter_rows(named=True)):
        if (i + 1) % 5000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} institutions ({len(all_matches):,} matches so far, {excluded_count:,} excluded)...")

        matches_before = len(all_matches)
        matches = match_institution_by_domain(inst_row, domain_lookup)
        matches_after = len(all_matches)

        if matches:
            matched_count += 1

        all_matches.extend(matches)

        # Track exclusions
        if len(matches) < matches_after - matches_before:
            excluded_count += (matches_after - matches_before) - len(matches)

    logger.info(f"  Completed. Found {len(all_matches)} total matches")
    logger.info(f"  Institutions matched: {matched_count:,}")
    logger.info(f"  Matches excluded: {excluded_count:,}")

    # Step 4: Save output
    logger.info("\n[4/5] Saving matches...")
    logger.info(f"Output: {OUTPUT_FILE}")

    if not all_matches:
        logger.warning("  No matches found!")
    else:
        matches_df = pl.DataFrame(all_matches)
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df)} matches")

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("ENHANCED HOMEPAGE DOMAIN MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df)}")
        logger.info(f"Unique institutions: {matches_df['institution_id'].n_unique()}")
        logger.info(f"Unique firms: {matches_df['GVKEY'].n_unique()}")

        # Country match statistics
        if 'country_match' in matches_df.columns:
            country_match_count = matches_df['country_match'].sum()
            logger.info(f"\nCountry match validation:")
            logger.info(f"  Country match: {country_match_count:,} ({country_match_count/len(matches_df)*100:.1f}%)")

        # Show examples
        logger.info("\nExample matches:")
        for i, row in enumerate(matches_df.head(15).iter_rows(named=True), 1):
            logger.info(f"  {i}. {row['institution_display_name'][:50]}")
            logger.info(f"     Domain: {row['institution_domain']}")
            logger.info(f"     → {row['firm_conm'][:50]}")
            logger.info(f"     Firm domain: {row['firm_domain']}")
            logger.info(f"     Country match: {row['country_match']}")

    logger.info("\n" + "=" * 80)
    logger.info("ENHANCED HOMEPAGE DOMAIN MATCHING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
