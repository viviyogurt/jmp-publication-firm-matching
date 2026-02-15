"""
Method 2.1: Conservative Fuzzy Matching

This script matches institutions to firms using fuzzy matching with strict validation.

Key Differences from Patents:
- Patents: Jaro-Winkler ≥0.85 → 96.7% accuracy (high conf)
- Publications: Jaro-Winkler ≥0.90 → 95-97% expected (more conservative)

Methodology:
1. Fuzzy matching: Jaro-Winkler ≥0.90 (higher than patents)
2. Cross-validation checks (MULTIPLE REQUIRED):
   - Country match (+0.02 boost)
   - Business description keywords (+0.02 boost)
   - Location match (+0.01 boost)
   - URL similarity (+0.01 boost)
3. Confidence scoring:
   - Map JW 0.90-0.99 → 0.94-0.99
   - Add validation boosts (max +0.06)
4. Strict filtering:
   - Accept only confidence ≥0.94
   - For 0.94-0.95: Require ≥2 validation checks
   - For ≥0.96: Require ≥1 validation check
   - Reject if no validation (even high JW score)

Target: +1,500-2,500 firms @ 95-97% accuracy

Reference: src/02_linking/match_patents_to_firms_stage2.py
"""

import polars as pl
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple, Set
from rapidfuzz import fuzz, process

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_fuzzy_conservative.parquet"
PROGRESS_LOG = LOGS_DIR / "match_publications_fuzzy_conservative.log"

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
MIN_JARO_WINKLER = 0.90  # Higher than patents (0.85)
MIN_CONFIDENCE = 0.94
COUNTRY_BOOST = 0.02
BUSINESS_BOOST = 0.02
LOCATION_BOOST = 0.01
URL_BOOST = 0.01
MAX_BOOST = COUNTRY_BOOST + BUSINESS_BOOST + LOCATION_BOOST + URL_BOOST


def calculate_fuzzy_confidence(inst_name: str, firm_name: str) -> Tuple[float, str]:
    """
    Calculate fuzzy matching confidence using multiple methods.

    Args:
        inst_name: Institution normalized name
        firm_name: Firm normalized name

    Returns:
        Tuple of (confidence, method)
    """
    if not inst_name or not firm_name:
        return 0.0, ""

    # Jaro-Winkler similarity (using ratio as proxy)
    jw_score = fuzz.ratio(inst_name, firm_name) / 100.0

    # Partial ratio (for substring matches)
    partial_score = fuzz.partial_ratio(inst_name, firm_name) / 100.0

    # Token sort ratio (order-independent)
    token_score = fuzz.token_sort_ratio(inst_name, firm_name) / 100.0

    # Use the best score
    best_score = max(jw_score, partial_score, token_score)

    # Map to confidence (more conservative than patents)
    if best_score >= 0.98:
        return 0.98, "fuzzy_very_high"
    elif best_score >= 0.95:
        return 0.96, "fuzzy_high"
    elif best_score >= MIN_JARO_WINKLER:
        return 0.94, "fuzzy_medium"
    else:
        return 0.0, ""


def check_country_match(inst_country: Optional[str], firm_fic: Optional[str]) -> bool:
    """Check if institution country matches firm country."""
    if not inst_country or not firm_fic:
        return False

    inst_country = inst_country.upper()[:2]
    firm_country = firm_fic.upper()[:2]

    return inst_country == firm_country


def check_business_description_keywords(inst_name: str, firm_busdesc: Optional[str]) -> bool:
    """
    Check if firm's business description contains institution name keywords.

    Args:
        inst_name: Institution name to extract keywords from
        firm_busdesc: Firm's business description

    Returns:
        True if business description contains relevant keywords
    """
    if not firm_busdesc:
        return False

    busdesc_upper = firm_busdesc.upper()

    # Extract meaningful keywords from institution name (4+ chars)
    keywords = [w for w in inst_name.split() if len(w) >= 4]

    # Check if any keyword appears in business description
    for keyword in keywords:
        if keyword in busdesc_upper:
            return True

    return False


def check_location_match(inst_city: Optional[str], inst_region: Optional[str],
                        firm_city: Optional[str], firm_state: Optional[str]) -> bool:
    """Check if institution location matches firm location."""
    if not inst_city or not firm_city:
        return False

    # Normalize city names
    inst_city_norm = inst_city.upper().strip()
    firm_city_norm = firm_city.upper().strip()

    # Check if cities match
    if inst_city_norm == firm_city_norm:
        return True

    # Check if state/region matches
    if inst_region and firm_state:
        inst_region_norm = inst_region.upper().strip()
        firm_state_norm = firm_state.upper().strip()
        if inst_region_norm == firm_state_norm:
            return True

    return False


def check_url_similarity(inst_domain: Optional[str], firm_weburl: Optional[str]) -> bool:
    """Check if institution domain is similar to firm weburl."""
    if not inst_domain or not firm_weburl:
        return False

    # Extract domain from firm weburl
    firm_domain = firm_weburl.lower()
    if '://' in firm_domain:
        firm_domain = firm_domain.split('://')[1].split('/')[0]

    # Remove www. prefix
    if firm_domain.startswith('www.'):
        firm_domain = firm_domain[4:]

    # Check if domains match or one contains the other
    inst_domain_lower = inst_domain.lower()

    if inst_domain_lower == firm_domain:
        return True

    if inst_domain_lower in firm_domain or firm_domain in inst_domain_lower:
        return True

    return False


def count_validations(confidence: float, country_match: bool, business_match: bool,
                     location_match: bool, url_match: bool) -> int:
    """Count number of validation checks passed."""
    return sum([country_match, business_match, location_match, url_match])


def match_institution_fuzzy(institution_row: Dict,
                           firms_df: pl.DataFrame,
                           matched_ids: Set[str]) -> List[Dict]:
    """
    Match institution to firms using fuzzy matching with validation.

    Args:
        institution_row: Institution record
        firms_df: Standardized Compustat firms DataFrame
        matched_ids: Set of already matched institution IDs

    Returns:
        List of match records
    """
    matches = []

    institution_id = institution_row['institution_id']

    # Skip if already matched
    if institution_id in matched_ids:
        return matches

    display_name = institution_row['display_name']
    normalized_name = institution_row.get('normalized_name') or institution_row.get('name_variants', [None])[0]
    country_code = institution_row.get('country_code') or institution_row.get('geo_country_code')
    city = institution_row.get('geo_city') or institution_row.get('city')
    region = institution_row.get('geo_region') or institution_row.get('region')
    homepage_domain = institution_row.get('homepage_domain')
    paper_count = institution_row.get('paper_count', 0)

    if not normalized_name:
        return matches

    # Find best fuzzy matches using rapidfuzz
    firm_names = firms_df['conm_clean'].to_list()

    # Get top 10 candidates
    results = process.extract(
        normalized_name,
        firm_names,
        scorer=fuzz.ratio,
        limit=10
    )

    # Process candidates
    for firm_name, score, _ in results:
        # Convert score to 0-1 scale
        jw_score = score / 100.0

        # Apply minimum threshold
        if jw_score < MIN_JARO_WINKLER:
            break  # Remaining results will be worse

        # Calculate base confidence
        base_confidence, method = calculate_fuzzy_confidence(normalized_name, firm_name)

        if base_confidence == 0.0:
            continue

        # Find the firm record
        firm_records = firms_df.filter(pl.col('conm_clean') == firm_name)

        if firm_records.is_empty():
            continue

        firm_row = firm_records.row(0, named=True)

        # Validation checks
        country_match = check_country_match(country_code, firm_row.get('fic'))
        business_match = check_business_description_keywords(display_name.upper(), firm_row.get('busdesc'))
        location_match = check_location_match(city, region, firm_row.get('city'), firm_row.get('state'))
        url_match = check_url_similarity(homepage_domain, firm_row.get('weburl'))

        # Count validations
        num_validations = count_validations(base_confidence, country_match, business_match,
                                          location_match, url_match)

        # Apply validation boosts
        confidence_boost = 0.0
        if country_match:
            confidence_boost += COUNTRY_BOOST
        if business_match:
            confidence_boost += BUSINESS_BOOST
        if location_match:
            confidence_boost += LOCATION_BOOST
        if url_match:
            confidence_boost += URL_BOOST

        final_confidence = min(base_confidence + confidence_boost, 0.99)

        # Strict filtering:
        # - Accept only confidence ≥0.94
        # - For 0.94-0.95: Require ≥2 validation checks
        # - For ≥0.96: Require ≥1 validation check
        # - Reject if no validation (even high JW score)

        if final_confidence < MIN_CONFIDENCE:
            continue

        if final_confidence < 0.96 and num_validations < 2:
            continue

        if final_confidence >= 0.96 and num_validations < 1:
            continue

        # Valid match
        matches.append({
            'GVKEY': firm_row['GVKEY'],
            'LPERMNO': firm_row.get('LPERMNO'),
            'firm_conm': firm_row['conm'],
            'institution_id': institution_id,
            'institution_display_name': display_name,
            'match_type': 'fuzzy_conservative',
            'match_confidence': final_confidence,
            'match_method': method,
            'jaro_winkler_score': jw_score,
            'country_match': country_match,
            'business_match': business_match,
            'location_match': location_match,
            'url_match': url_match,
            'num_validations': num_validations,
            'confidence_boost': confidence_boost,
            'institution_paper_count': paper_count,
        })

        # Only keep best match per institution
        break

    return matches


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("METHOD 2.1: CONSERVATIVE FUZZY MATCHING")
    logger.info("=" * 80)

    # Step 1: Load data
    logger.info("\n[1/5] Loading data...")

    if not INSTITUTIONS_MASTER.exists():
        raise FileNotFoundError(f"Institutions master file not found: {INSTITUTIONS_MASTER}")
    if not COMPUSTAT_STANDARDIZED.exists():
        raise FileNotFoundError(f"Compustat standardized file not found: {COMPUSTAT_STANDARDIZED}")

    institutions_df = pl.read_parquet(INSTITUTIONS_MASTER)
    firms_df = pl.read_parquet(COMPUSTAT_STANDARDIZED)

    logger.info(f"  Loaded {len(institutions_df):,} institutions")
    logger.info(f"  Loaded {len(firms_df):,} firms")

    # Step 2: Load existing matches to exclude
    logger.info("\n[2/5] Loading existing matches to exclude...")

    matched_ids = set()

    # Check for other stage match files
    stage_files = [
        'publication_firm_matches_wikidata_tickers.parquet',
        'publication_firm_matches_contained_name.parquet',
        'publication_firm_matches_acronyms_enhanced.parquet',
        'publication_firm_matches_homepage.parquet',
    ]

    for filename in stage_files:
        filepath = DATA_PROCESSED_LINK / filename
        if filepath.exists():
            stage_matches = pl.read_parquet(filepath)
            matched_ids.update(stage_matches['institution_id'].to_list())
            logger.info(f"  Excluding {len(stage_matches):,} from {filename}")

    logger.info(f"  Total already matched: {len(matched_ids):,}")

    # Step 3: Run fuzzy matching
    logger.info("\n[3/5] Running conservative fuzzy matching...")
    logger.info("  This may take several minutes...")

    all_matches = []
    total_institutions = len(institutions_df)
    matched_count = 0
    rejected_no_validation = 0
    rejected_low_confidence = 0

    for i, inst_row in enumerate(institutions_df.iter_rows(named=True)):
        if (i + 1) % 500 == 0:
            logger.info(f"  Processed {i+1:,}/{total_institutions:,} institutions ({len(all_matches):,} matches so far)...")

        matches = match_institution_fuzzy(inst_row, firms_df, matched_ids)

        if matches:
            matched_count += 1
            matched_ids.add(inst_row['institution_id'])
        else:
            # Track rejection reasons
            normalized_name = inst_row.get('normalized_name') or inst_row.get('name_variants', [None])[0]
            if normalized_name:
                # Try a quick check to see if it was rejected due to validation
                firm_names = firms_df['conm_clean'].to_list()
                results = process.extract(normalized_name, firm_names, scorer=fuzz.ratio, limit=1)
                if results and results[0][1] / 100.0 >= MIN_JARO_WINKLER:
                    rejected_no_validation += 1
                else:
                    rejected_low_confidence += 1

        all_matches.extend(matches)

    logger.info(f"  Completed matching.")
    logger.info(f"  Institutions matched: {matched_count:,}")
    logger.info(f"  Rejected (no validation): {rejected_no_validation:,}")
    logger.info(f"  Rejected (low confidence): {rejected_low_confidence:,}")
    logger.info(f"  Total matches: {len(all_matches):,}")

    # Step 4: Deduplicate
    logger.info("\n[4/5] Deduplicating matches...")

    if not all_matches:
        logger.warning("  No matches found!")
        matches_df = pl.DataFrame()
    else:
        matches_df = pl.DataFrame(all_matches)

        # If same institution matched to multiple firms, keep highest confidence
        matches_df = (
            matches_df
            .sort(['institution_id', 'GVKEY', 'match_confidence'], descending=[False, False, True])
            .unique(subset=['institution_id', 'GVKEY'], keep='first')
        )

        logger.info(f"  After deduplication: {len(matches_df):,} unique institution-firm matches")
        logger.info(f"  Unique institutions matched: {matches_df['institution_id'].n_unique():,}")
        logger.info(f"  Unique firms matched: {matches_df['GVKEY'].n_unique():,}")

    # Step 5: Save output
    logger.info("\n[5/5] Saving matches...")
    logger.info(f"Output: {OUTPUT_FILE}")

    if len(matches_df) > 0:
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df):,} matches")

        # Summary statistics
        logger.info("\n" + "=" * 80)
        logger.info("CONSERVATIVE FUZZY MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df):,}")
        logger.info(f"Unique institutions matched: {matches_df['institution_id'].n_unique():,}")
        logger.info(f"Unique firms matched: {matches_df['GVKEY'].n_unique():,}")

        # Confidence distribution
        logger.info(f"\nConfidence statistics:")
        logger.info(f"  Mean: {matches_df['match_confidence'].mean():.3f}")
        logger.info(f"  Median: {matches_df['match_confidence'].median():.3f}")
        logger.info(f"  Min: {matches_df['match_confidence'].min():.3f}")
        logger.info(f"  Max: {matches_df['match_confidence'].max():.3f}")

        # JW score distribution
        logger.info(f"\nJaro-Winkler score statistics:")
        logger.info(f"  Mean: {matches_df['jaro_winkler_score'].mean():.3f}")
        logger.info(f"  Median: {matches_df['jaro_winkler_score'].median():.3f}")
        logger.info(f"  Min: {matches_df['jaro_winkler_score'].min():.3f}")
        logger.info(f"  Max: {matches_df['jaro_winkler_score'].max():.3f}")

        # Validation statistics
        country_count = matches_df['country_match'].sum()
        business_count = matches_df['business_match'].sum()
        location_count = matches_df['location_match'].sum()
        url_count = matches_df['url_match'].sum()
        logger.info(f"\nValidation statistics:")
        logger.info(f"  Country match: {country_count:,} ({country_count/len(matches_df)*100:.1f}%)")
        logger.info(f"  Business match: {business_count:,} ({business_count/len(matches_df)*100:.1f}%)")
        logger.info(f"  Location match: {location_count:,} ({location_count/len(matches_df)*100:.1f}%)")
        logger.info(f"  URL match: {url_count:,} ({url_count/len(matches_df)*100:.1f}%)")

        # Number of validations distribution
        logger.info(f"\nNumber of validations per match:")
        val_counts = matches_df.group_by('num_validations').agg(pl.len().alias('count'))
        for row in val_counts.sort('num_validations').iter_rows(named=True):
            logger.info(f"  {row['num_validations']}: {row['count']:,} matches")

        # Show examples
        logger.info("\nExample matches:")
        for i, row in enumerate(matches_df.head(15).iter_rows(named=True), 1):
            logger.info(f"  {i}. {row['institution_display_name'][:50]}")
            logger.info(f"     → {row['firm_conm'][:50]}")
            logger.info(f"     JW: {row['jaro_winkler_score']:.3f}, Conf: {row['match_confidence']:.2f}")
            logger.info(f"     Validations: {row['num_validations']} (C: {row['country_match']}, B: {row['business_match']}, L: {row['location_match']}, U: {row['url_match']})")

    else:
        logger.warning("  No matches to save!")

    logger.info("\n" + "=" * 80)
    logger.info("CONSERVATIVE FUZZY MATCHING COMPLETE")
    logger.info("=" * 80)

    return matches_df


if __name__ == "__main__":
    main()
