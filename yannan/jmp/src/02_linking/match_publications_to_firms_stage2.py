"""
Stage 2: Fuzzy String Matching for Publication-Firm Linking

This script implements Stage 2 matching for institutions not matched in Stage 1.
Uses Jaro-Winkler similarity with cross-validation checks (country, business, location, URL).

Following the patent matching approach (Arora et al. 2021, Kogan et al. 2017).

Matching Strategy:
1. Fuzzy string matching: Jaro-Winkler similarity ≥0.85
2. Cross-validation checks:
   - Country match: +0.02 confidence
   - Business description match: +0.02
   - Location match: +0.01
   - URL similarity: +0.01
3. Confidence scoring: base 0.85-0.99 maps to 0.90-0.99
4. Filtering: confidence ≥0.90 with additional validation requirements

Target: Additional 2,000-4,000 institutions matched with 85-95% accuracy.
"""

import polars as pl
from pathlib import Path
import logging
import re
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict

try:
    from rapidfuzz import fuzz
    from rapidfuzz.distance import JaroWinkler
except ImportError:
    from fuzzywuzzy import fuzz
    from jellyfish import jaro_winkler as JaroWinkler

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
DATA_PROCESSED_PUB = PROJECT_ROOT / "data" / "processed" / "publication"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
STAGE1_MATCHES = DATA_PROCESSED_LINK / "publication_firm_matches_stage1.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_stage2.parquet"
OUTPUT_UNMATCHED = DATA_PROCESSED_LINK / "publication_firm_matches_stage2_unmatched.parquet"
PROGRESS_LOG = LOGS_DIR / "match_publications_to_firms_stage2.log"

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

# Minimum similarity threshold for fuzzy matching
MIN_SIMILARITY = 0.85

# Confidence thresholds
MIN_CONFIDENCE = 0.90
HIGH_CONFIDENCE = 0.95


# ============================================================================
# Cross-Validation Functions
# ============================================================================

def validate_country_match(institution_row: Dict, firm_row: Dict) -> bool:
    """
    Validate that institution country matches firm country.
    Institution country_code should match firm fic (country of incorporation).
    """
    inst_country = institution_row.get('country_code')
    firm_country = firm_row.get('fic')

    if not inst_country or not firm_country:
        return False

    # Normalize to 2-letter codes
    inst_country = inst_country.upper()
    firm_country = firm_country.upper()

    # Direct match
    if inst_country == firm_country:
        return True

    # Common country code mappings (some variations exist)
    country_equiv = {
        'US': ['USA', 'US'],
        'GB': ['UK', 'GB', 'ENG'],
        'CN': ['CHN', 'CN'],
        'JP': ['JPN', 'JP'],
        'DE': ['DEU', 'DE'],
        'FR': ['FRA', 'FR'],
        'CA': ['CAN', 'CA'],
        'AU': ['AUS', 'AU'],
        'IN': ['IND', 'IN'],
        'KR': ['KOR', 'KR'],
        'NL': ['NLD', 'NL'],
        'CH': ['CHE', 'CH'],
        'SE': ['SWE', 'SE'],
        'SG': ['SGP', 'SG'],
    }

    for equiv_list in country_equiv.values():
        if inst_country in equiv_list and firm_country in equiv_list:
            return True

    return False


def validate_business_description(institution_row: Dict, firm_row: Dict) -> bool:
    """
    Validate that keywords from institution name appear in firm business description.
    """
    inst_name = institution_row.get('display_name', '') or institution_row.get('normalized_name', '')
    busdesc = firm_row.get('busdesc', '')

    if not inst_name or not busdesc:
        return False

    # Extract keywords from institution name (2+ letter words)
    inst_words = set(re.findall(r'\b[A-Za-z]{2,}\b', inst_name.upper()))

    # Check if any institution keywords appear in business description
    busdesc_upper = busdesc.upper()

    # Check for at least 2 keyword matches (or 1 if very specific)
    matches = sum(1 for word in inst_words if word in busdesc_upper)

    # Require at least 2 matches or 1 very specific match (long word)
    if matches >= 2:
        return True

    # Check for specific long keywords
    for word in inst_words:
        if len(word) >= 6 and word in busdesc_upper:
            return True

    return False


def validate_location_match(institution_row: Dict, firm_row: Dict) -> bool:
    """
    Validate geographic proximity (city/state if available).
    """
    inst_city = institution_row.get('geo_city')
    inst_region = institution_row.get('geo_region')
    firm_city = firm_row.get('city')
    firm_state = firm_row.get('state')

    if not inst_city and not inst_region:
        return False

    # City match
    if inst_city and firm_city:
        inst_city_norm = re.sub(r'[^\w\s]', '', inst_city.upper())
        firm_city_norm = re.sub(r'[^\w\s]', '', firm_city.upper())

        if inst_city_norm == firm_city_norm:
            return True
        if inst_city_norm in firm_city_norm or firm_city_norm in inst_city_norm:
            return True

    # State/Region match
    if inst_region and firm_state:
        inst_region_norm = re.sub(r'[^\w\s]', '', inst_region.upper())
        firm_state_norm = re.sub(r'[^\w\s]', '', firm_state.upper())

        if inst_region_norm == firm_state_norm:
            return True
        if inst_region_norm in firm_state_norm or firm_state_norm in inst_region_norm:
            return True

    return False


def validate_url_similarity(institution_row: Dict, firm_row: Dict) -> bool:
    """
    Validate URL/domain similarity if both have URLs.
    """
    inst_domain = institution_row.get('homepage_domain')
    firm_weburl = firm_row.get('weburl')

    if not inst_domain or not firm_weburl:
        return False

    # Normalize firm URL to domain
    try:
        from urllib.parse import urlparse
        parsed = urlparse(firm_weburl if '://' in firm_weburl else f'http://{firm_weburl}')
        firm_domain = parsed.netloc.lower().replace('www.', '')
        inst_domain_norm = inst_domain.lower().replace('www.', '')

        # Check for substantial overlap
        if firm_domain == inst_domain_norm:
            return True

        # Check if one is substring of other (for subdomains)
        if len(firm_domain) > 3 and len(inst_domain_norm) > 3:
            if firm_domain in inst_domain_norm or inst_domain_norm in firm_domain:
                return True

    except Exception:
        pass

    return False


# ============================================================================
# Fuzzy Matching Functions
# ============================================================================

def calculate_similarity(str1: str, str2: str) -> float:
    """
    Calculate Jaro-Winkler similarity between two strings.
    Returns value between 0 and 1.
    """
    if not str1 or not str2:
        return 0.0

    # Use Jaro-Winkler from rapidfuzz.distance
    # JaroWinkler.similarity returns a value between 0 and 1
    similarity = JaroWinkler.similarity(str1, str2)

    return similarity  # Already in 0-1 scale


def match_institution_to_firms_fuzzy(institution_row: Dict,
                                     firms_df: pl.DataFrame,
                                     matched_institution_ids: Set[str]) -> List[Dict]:
    """
    Match a single institution to firms using fuzzy string matching with cross-validation.
    Returns list of matches that pass confidence threshold.
    """
    matches = []

    # Skip if already matched in Stage 1
    if institution_row['institution_id'] in matched_institution_ids:
        return matches

    inst_clean = institution_row.get('normalized_name', '')
    inst_display = institution_row.get('display_name', '')
    inst_alternatives = institution_row.get('alternative_names') or []
    inst_name_variants = institution_row.get('name_variants') or []

    # Combine all name variants
    all_inst_names = [inst_clean, inst_display] + inst_alternatives + inst_name_variants
    all_inst_names = [n for n in all_inst_names if n and n.strip()]

    if not all_inst_names:
        return matches

    # Calculate similarity to each firm
    for firm_row in firms_df.iter_rows(named=True):
        firm_conm_clean = firm_row.get('conm_clean')
        firm_conml_clean = firm_row.get('conml_clean')
        firm_name_variants = firm_row.get('name_variants') or []

        # Combine all firm names
        all_firm_names = [firm_conm_clean, firm_conml_clean] + firm_name_variants
        all_firm_names = [n for n in all_firm_names if n and n.strip()]

        if not all_firm_names:
            continue

        # Calculate maximum similarity across all name pairs
        max_similarity = 0.0
        best_pair = None

        for inst_name in all_inst_names:
            for firm_name in all_firm_names:
                similarity = calculate_similarity(inst_name, firm_name)
                if similarity > max_similarity:
                    max_similarity = similarity
                    best_pair = (inst_name, firm_name)

        # Check if passes minimum similarity threshold
        if max_similarity < MIN_SIMILARITY:
            continue

        # Map similarity to base confidence
        # 0.85 -> 0.90, 0.99 -> 0.99
        base_confidence = 0.90 + (max_similarity - MIN_SIMILARITY) * (0.99 - 0.90) / (0.99 - MIN_SIMILARITY)
        base_confidence = min(base_confidence, 0.99)

        # Cross-validation checks
        validation_flags = []
        confidence_boost = 0.0

        # Country match (+0.02)
        if validate_country_match(institution_row, firm_row):
            validation_flags.append('country')
            confidence_boost += 0.02

        # Business description match (+0.02)
        if validate_business_description(institution_row, firm_row):
            validation_flags.append('business')
            confidence_boost += 0.02

        # Location match (+0.01)
        if validate_location_match(institution_row, firm_row):
            validation_flags.append('location')
            confidence_boost += 0.01

        # URL similarity (+0.01)
        if validate_url_similarity(institution_row, firm_row):
            validation_flags.append('url')
            confidence_boost += 0.01

        # Calculate final confidence
        final_confidence = min(base_confidence + confidence_boost, 0.99)

        # Apply filtering rules
        # For confidence 0.90-0.95, require at least 2 validation checks
        # For confidence ≥0.95, require at least 1 validation check
        if final_confidence < MIN_CONFIDENCE:
            continue

        if final_confidence < HIGH_CONFIDENCE and len(validation_flags) < 2:
            continue

        if final_confidence >= HIGH_CONFIDENCE and len(validation_flags) < 1:
            continue

        matches.append({
            'GVKEY': firm_row['GVKEY'],
            'LPERMNO': firm_row.get('LPERMNO'),
            'firm_conm': firm_row.get('conm'),
            'institution_id': institution_row['institution_id'],
            'institution_display_name': inst_display,
            'institution_clean_name': inst_clean,
            'match_type': 'stage2',
            'match_confidence': round(final_confidence, 3),
            'match_method': 'fuzzy_jaro_winkler',
            'similarity_score': round(max_similarity, 3),
            'validation_flags': validation_flags,
            'institution_paper_count': institution_row.get('paper_count', 0),
        })

    return matches


# ============================================================================
# Main Processing
# ============================================================================

def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("STAGE 2: FUZZY STRING MATCHING FOR PUBLICATION-FIRM LINKING")
    logger.info("=" * 80)

    # Step 1: Load data
    logger.info("\n[1/6] Loading data...")

    if not INSTITUTIONS_MASTER.exists():
        raise FileNotFoundError(f"Institutions master file not found: {INSTITUTIONS_MASTER}")
    if not COMPUSTAT_STANDARDIZED.exists():
        raise FileNotFoundError(f"Compustat standardized file not found: {COMPUSTAT_STANDARDIZED}")
    if not STAGE1_MATCHES.exists():
        raise FileNotFoundError(f"Stage 1 matches file not found: {STAGE1_MATCHES}")

    institutions_df = pl.read_parquet(INSTITUTIONS_MASTER)
    firms_df = pl.read_parquet(COMPUSTAT_STANDARDIZED)
    stage1_matches = pl.read_parquet(STAGE1_MATCHES)

    logger.info(f"  Loaded {len(institutions_df):,} institutions")
    logger.info(f"  Loaded {len(firms_df):,} firms")
    logger.info(f"  Loaded {len(stage1_matches):,} Stage 1 matches")

    # Get IDs of institutions already matched in Stage 1
    matched_institution_ids = set(stage1_matches['institution_id'].to_list())
    logger.info(f"  Institutions already matched: {len(matched_institution_ids):,}")

    # Filter to unmatched institutions
    unmatched_institutions = institutions_df.filter(
        ~pl.col('institution_id').is_in(matched_institution_ids)
    )
    logger.info(f"  Institutions to match in Stage 2: {len(unmatched_institutions):,}")

    # Step 2: Create firm name variants list for efficient matching
    logger.info("\n[2/6] Preparing firm data...")

    # Filter firms to those not already matched (optional - keeps all firms for better matching)
    # For now, keep all firms

    # Step 3: Fuzzy matching
    logger.info("\n[3/6] Running fuzzy string matching with cross-validation...")
    logger.info("  This may take considerable time...")

    all_matches = []
    total_institutions = len(unmatched_institutions)

    for i, institution_row in enumerate(unmatched_institutions.iter_rows(named=True)):
        if (i + 1) % 100 == 0:
            logger.info(f"  Processed {i+1:,}/{total_institutions:,} institutions ({len(all_matches):,} matches so far)...")

        matches = match_institution_to_firms_fuzzy(institution_row, firms_df, matched_institution_ids)
        all_matches.extend(matches)

    logger.info(f"  Completed matching. Found {len(all_matches):,} total matches")

    # Step 4: Deduplicate and filter
    logger.info("\n[4/6] Deduplicating and filtering matches...")

    if not all_matches:
        logger.warning("  No matches found!")
        matches_df = pl.DataFrame()
    else:
        # Convert to DataFrame
        matches_df = pl.DataFrame(all_matches)

        # If same institution-firm matched multiple times, keep highest confidence
        matches_df = (
            matches_df
            .sort(['institution_id', 'GVKEY', 'match_confidence'], descending=[False, False, True])
            .unique(subset=['institution_id', 'GVKEY'], keep='first')
        )

        logger.info(f"  After deduplication: {len(matches_df):,} unique institution-firm matches")
        logger.info(f"  Unique institutions matched: {matches_df['institution_id'].n_unique():,}")
        logger.info(f"  Unique firms matched: {matches_df['GVKEY'].n_unique():,}")

    # Step 5: Save output
    logger.info("\n[5/6] Saving Stage 2 matches...")
    logger.info(f"Output: {OUTPUT_FILE}")

    if len(matches_df) > 0:
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df):,} matches")

        # Summary statistics
        logger.info("\n" + "=" * 80)
        logger.info("STAGE 2 MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df):,}")
        logger.info(f"Unique institutions matched: {matches_df['institution_id'].n_unique():,}")
        logger.info(f"Unique firms matched: {matches_df['GVKEY'].n_unique():,}")

        # Coverage statistics
        coverage_pct = (matches_df['institution_id'].n_unique() / total_institutions) * 100
        logger.info(f"\nCoverage of unmatched institutions: {coverage_pct:.2f}%")

        # Confidence distribution
        logger.info(f"\nConfidence statistics:")
        logger.info(f"  Mean: {matches_df['match_confidence'].mean():.3f}")
        logger.info(f"  Median: {matches_df['match_confidence'].median():.3f}")
        logger.info(f"  Min: {matches_df['match_confidence'].min():.3f}")
        logger.info(f"  Max: {matches_df['match_confidence'].max():.3f}")

        # Validation flag distribution
        if 'validation_flags' in matches_df.columns:
            # Count validation flags
            flag_counts = defaultdict(int)
            for flags in matches_df['validation_flags'].to_list():
                for flag in flags:
                    flag_counts[flag] += 1

            logger.info(f"\nValidation flag distribution:")
            for flag, count in sorted(flag_counts.items(), key=lambda x: -x[1]):
                logger.info(f"  {flag}: {count:,} matches")

        # Paper count statistics
        if 'institution_paper_count' in matches_df.columns:
            total_papers = matches_df['institution_paper_count'].sum()
            logger.info(f"\nTotal papers covered: {total_papers:,}")
    else:
        logger.warning("  No matches to save!")

    # Step 6: Save unmatched institutions
    logger.info("\n[6/6] Saving unmatched institutions...")

    # Get IDs of institutions matched in Stage 2
    if len(matches_df) > 0:
        stage2_matched_ids = set(matches_df['institution_id'].to_list())
    else:
        stage2_matched_ids = set()

    # Combine Stage 1 and Stage 2 matched IDs
    all_matched_ids = matched_institution_ids | stage2_matched_ids

    # Filter to still-unmatched institutions
    still_unmatched = institutions_df.filter(
        ~pl.col('institution_id').is_in(all_matched_ids)
    )

    still_unmatched.write_parquet(OUTPUT_UNMATCHED, compression='snappy')
    logger.info(f"  Saved {len(still_unmatched):,} unmatched institutions to {OUTPUT_UNMATCHED}")

    logger.info(f"\nUnmatched institutions: {len(still_unmatched):,} ({len(still_unmatched)/len(institutions_df)*100:.2f}%)")

    logger.info("\n" + "=" * 80)
    logger.info("STAGE 2 MATCHING COMPLETE")
    logger.info("=" * 80)

    return matches_df


if __name__ == "__main__":
    main()
