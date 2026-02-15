"""
Method 1.4: Enhanced Acronym-to-Ticker Matching

This script matches institutions to firms using acronyms with strict validation.

Enhancements over original match_acronyms.py:
1. Blocklist generic acronyms (CP, AI, IT, etc.)
2. Require country match for 0.95 confidence
3. Name similarity ≥0.75 between inst and firm
4. If no validation: confidence 0.92 (may filter in Stage 2)

Target: 800-1,200 firms @ 96% accuracy

Reference: src/02_linking/match_acronyms.py (existing)
"""

import polars as pl
from pathlib import Path
import logging
from rapidfuzz.distance import JaroWinkler
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
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_acronyms_enhanced.parquet"
PROGRESS_LOG = LOGS_DIR / "match_publications_acronyms_enhanced.log"

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

# Configuration
CONFIDENCE_WITH_VALIDATION = 0.95
CONFIDENCE_NO_VALIDATION = 0.92
MIN_NAME_SIMILARITY = 0.75

# Generic acronyms to blocklist (too ambiguous)
GENERIC_ACRONYYM_BLOCKLIST = {
    'CP', 'AI', 'IT', 'HR', 'PR', 'R&D', 'RD', 'CEO', 'CFO', 'CTO',
    'COO', 'CSO', 'CIO', 'CMO', 'CLO', 'CKO', 'CDO', 'CVO',
    'USA', 'UK', 'USSR', 'EU', 'UN', 'WHO', 'UNESCO',
    'A&M', 'A&M', 'J&J', 'M&A', 'B2B', 'B2C', 'P&L',
    'VC', 'PE', 'IPO', 'M&A', 'CEO', 'CFO', 'CTO', 'COO',
    'R&D', 'QA', 'QC', 'SOP', 'KPI', 'ROI', 'B2B', 'B2C',
    'FMCG', 'SME', 'MNC', 'JV', 'HQ', 'APAC', 'EMEA', 'LATAM',
    'NA', 'AP', 'EM', 'LA', 'ME', 'NAFTA', 'OECD'
}


def extract_acronyms(acronyms_list: Optional[List]) -> Set[str]:
    """
    Extract and normalize acronyms from institutions data.

    Args:
        acronyms_list: List of acronyms (may be None or empty)

    Returns:
        Set of normalized acronym strings (uppercase, stripped)
    """
    if not acronyms_list:
        return set()

    normalized = set()
    for acronym in acronyms_list:
        if acronym and isinstance(acronym, str):
            acronym_clean = acronym.strip().upper()
            # Skip generic acronyms
            if acronym_clean and acronym_clean not in GENERIC_ACRONYYM_BLOCKLIST:
                normalized.add(acronym_clean)

    return normalized


def jaro_winkler_similarity(str1: Optional[str], str2: Optional[str]) -> float:
    """Calculate Jaro-Winkler similarity between two strings."""
    if not str1 or not str2:
        return 0.0
    return JaroWinkler.similarity(str1, str2)


def check_country_match(institution_country: Optional[str],
                       firm_fic: Optional[str]) -> bool:
    """
    Check if institution country matches firm country.

    Args:
        institution_country: Institution country code
        firm_fic: Firm FIPS country code

    Returns:
        True if countries match
    """
    if not institution_country or not firm_fic:
        return False

    inst_country = institution_country.upper()[:2]
    firm_country = firm_fic.upper()[:2]

    return inst_country == firm_country


def create_ticker_lookup(firms_df: pl.DataFrame) -> Dict[str, List[Dict]]:
    """
    Create ticker→GVKEY lookup dictionary from Compustat firms.

    Args:
        firms_df: Standardized Compustat firms DataFrame

    Returns:
        Dictionary mapping ticker → list of firm records
    """
    lookup = {}

    for row in firms_df.iter_rows(named=True):
        gvkey = row['GVKEY']
        lpermno = row.get('LPERMNO')
        conm = row['conm']
        conm_clean = row.get('conm_clean')
        tic = row['tic']
        fic = row.get('fic')

        # Add ticker
        if tic and tic.strip():
            ticker = tic.strip().upper()
            if ticker not in lookup:
                lookup[ticker] = []
            lookup[ticker].append({
                'GVKEY': gvkey,
                'LPERMNO': lpermno,
                'firm_conm': conm,
                'conm_clean': conm_clean,
                'tic': ticker,
                'fic': fic,
                'source': 'tic'
            })

        # Add name_variants that look like tickers (short, all caps)
        name_variants = row.get('name_variants') or []
        for variant in name_variants:
            if variant and len(variant) <= 6 and variant.isupper():
                if variant not in lookup:
                    lookup[variant] = []
                lookup[variant].append({
                    'GVKEY': gvkey,
                    'LPERMNO': lpermno,
                    'firm_conm': conm,
                    'conm_clean': conm_clean,
                    'tic': variant,
                    'fic': fic,
                    'source': 'name_variant'
                })

    return lookup


def match_institution_by_acronym(institution_row: Dict,
                                 ticker_lookup: Dict[str, List[Dict]]) -> List[Dict]:
    """
    Match institution to firms using acronyms with validation.

    Args:
        institution_row: Institution record
        ticker_lookup: Dictionary mapping ticker → firm records

    Returns:
        List of match records
    """
    matches = []

    institution_id = institution_row['institution_id']
    display_name = institution_row['display_name']
    normalized_name = institution_row.get('normalized_name', '')
    acronyms = institution_row.get('acronyms')
    country_code = institution_row.get('geo_country_code') or institution_row.get('country_code')
    paper_count = institution_row.get('paper_count', 0)

    # Extract and filter acronyms
    acronym_set = extract_acronyms(acronyms)

    if not acronym_set:
        return matches

    # Try each acronym
    for acronym in acronym_set:
        if acronym not in ticker_lookup:
            continue

        # Check all firms with this ticker/acronym
        for firm_record in ticker_lookup[acronym]:
            gvkey = firm_record['GVKEY']
            lpermno = firm_record['LPERMNO']
            firm_conm = firm_record['firm_conm']
            conm_clean = firm_record.get('conm_clean')
            fic = firm_record.get('fic')
            source = firm_record['source']

            # Validation: Country match
            country_match = check_country_match(country_code, fic)

            # Validation: Name similarity
            name_similarity = jaro_winkler_similarity(display_name, conm_clean)

            # Determine confidence based on validation
            if country_match and name_similarity >= MIN_NAME_SIMILARITY:
                confidence = CONFIDENCE_WITH_VALIDATION
            else:
                confidence = CONFIDENCE_NO_VALIDATION

            # Create match record
            matches.append({
                'GVKEY': gvkey,
                'LPERMNO': lpermno,
                'firm_conm': firm_conm,
                'institution_id': institution_id,
                'institution_display_name': display_name,
                'match_type': 'acronym_enhanced',
                'match_confidence': confidence,
                'match_method': f"acronym_to_{source}",
                'matched_acronym': acronym,
                'matched_ticker': firm_record['tic'],
                'country_match': country_match,
                'name_similarity': name_similarity,
                'institution_paper_count': paper_count,
            })

        # Only match first acronym that finds firms
        if len(matches) > 0:
            break

    return matches


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("METHOD 1.4: ENHANCED ACRONYM-TO-TICKER MATCHING")
    logger.info("=" * 80)

    # Step 1: Load data
    logger.info("\n[1/4] Loading data...")

    if not INSTITUTIONS_ENRICHED.exists():
        raise FileNotFoundError(f"Institutions enriched file not found: {INSTITUTIONS_ENRICHED}")
    if not COMPUSTAT_FIRMS.exists():
        raise FileNotFoundError(f"Compustat firms file not found: {COMPUSTAT_FIRMS}")

    institutions = pl.read_parquet(INSTITUTIONS_ENRICHED)
    firms = pl.read_parquet(COMPUSTAT_FIRMS)

    logger.info(f"  Loaded {len(institutions)} institutions")
    logger.info(f"  Loaded {len(firms)} firms")

    # Step 2: Prepare data
    logger.info("\n[2/4] Preparing data...")

    # Get institutions with acronyms
    inst_with_acronyms = institutions.filter(
        pl.col('acronyms').list.len() > 0
    )
    logger.info(f"  {len(inst_with_acronyms)} institutions have acronyms")

    # Build firm ticker lookup
    ticker_lookup = create_ticker_lookup(firms)
    logger.info(f"  Created lookup for {len(ticker_lookup):,} unique tickers")

    # Step 3: Run matching
    logger.info("\n[3/4] Running enhanced acronym matching...")

    all_matches = []
    total = len(inst_with_acronyms)
    matched_count = 0
    blocked_count = 0

    for i, inst_row in enumerate(inst_with_acronyms.iter_rows(named=True)):
        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} institutions ({len(all_matches)} matches so far)...")

        # Check if acronyms are blocked
        acronyms_raw = inst_row.get('acronyms')
        acronym_set = extract_acronyms(acronyms_raw)

        if not acronym_set and acronyms_raw:
            # All acronyms were blocked
            blocked_count += 1

        matches = match_institution_by_acronym(inst_row, ticker_lookup)

        if matches:
            matched_count += 1

        all_matches.extend(matches)

    logger.info(f"  Completed. Found {len(all_matches)} total matches")
    logger.info(f"  Institutions matched: {matched_count:,}")
    logger.info(f"  Institutions with blocked acronyms: {blocked_count:,}")

    # Step 4: Save output
    logger.info("\n[4/4] Saving matches...")
    logger.info(f"Output: {OUTPUT_FILE}")

    if not all_matches:
        logger.warning("  No matches found!")
    else:
        matches_df = pl.DataFrame(all_matches)

        # Deduplicate: keep highest confidence per institution-firm pair
        matches_df = (
            matches_df
            .sort(['institution_id', 'GVKEY', 'match_confidence', 'name_similarity'],
                  descending=[False, False, True, True])
            .unique(subset=['institution_id', 'GVKEY'], keep='first')
        )

        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df)} matches")

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("ENHANCED ACRONYM MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df)}")
        logger.info(f"Unique institutions: {matches_df['institution_id'].n_unique()}")
        logger.info(f"Unique firms: {matches_df['GVKEY'].n_unique()}")

        # Confidence distribution
        logger.info(f"\nConfidence distribution:")
        conf_counts = matches_df.group_by('match_confidence').agg(pl.len().alias('count'))
        for row in conf_counts.iter_rows(named=True):
            logger.info(f"  {row['match_confidence']:.2f}: {row['count']:,} matches")

        # Validation statistics
        country_match_count = matches_df['country_match'].sum()
        high_sim_count = (matches_df['name_similarity'] >= MIN_NAME_SIMILARITY).sum()
        logger.info(f"\nValidation statistics:")
        logger.info(f"  Country match: {country_match_count:,} ({country_match_count/len(matches_df)*100:.1f}%)")
        logger.info(f"  High similarity (≥{MIN_NAME_SIMILARITY}): {high_sim_count:,} ({high_sim_count/len(matches_df)*100:.1f}%)")

        # Name similarity statistics
        logger.info(f"\nName similarity statistics:")
        logger.info(f"  Mean: {matches_df['name_similarity'].mean():.3f}")
        logger.info(f"  Median: {matches_df['name_similarity'].median():.3f}")
        logger.info(f"  Min: {matches_df['name_similarity'].min():.3f}")
        logger.info(f"  Max: {matches_df['name_similarity'].max():.3f}")

        # Show examples
        logger.info("\nExample matches:")
        for i, row in enumerate(matches_df.head(15).iter_rows(named=True), 1):
            logger.info(f"  {i}. {row['institution_display_name'][:50]}")
            logger.info(f"     Acronym: {row['matched_acronym']} → Ticker: {row['matched_ticker']}")
            logger.info(f"     → {row['firm_conm'][:50]}")
            logger.info(f"     Confidence: {row['match_confidence']:.2f} (Country: {row['country_match']}, Sim: {row['name_similarity']:.2f})")

    logger.info("\n" + "=" * 80)
    logger.info("ENHANCED ACRONYM MATCHING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
