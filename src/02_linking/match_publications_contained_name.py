"""
Method 1.3: Contained Name Matching

This script matches institutions to firms using contained name matching.
It checks if a firm's name is a substring of an institution's name.

Methodology:
1. Check if firm normalized_name is substring of institution name
2. Require: firm_name length ≥8 characters (not generic words)
3. Validation checks (boost confidence):
   - Country match: institution.country_code == firm.fic (+0.01)
   - Business description: firm.busdesc contains keywords (+0.01)
4. Base confidence: 0.96, with boosts up to 0.97

Key Difference from Failed Alternative Names:
- ❌ Alternative: Generic "CP" → 5 firms → 89.7% error
- ✅ Contained: "Google DeepMind" contains "Google" → 100% accuracy (patents)

Target: 1,200-1,500 firms @ 97% accuracy

Reference: src/02_linking/match_patents_to_firms_stage1.py (lines 129-152)
"""

import polars as pl
from pathlib import Path
import logging
import re
from rapidfuzz.distance import JaroWinkler
from typing import Dict, List, Optional, Tuple

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_contained_name.parquet"
PROGRESS_LOG = LOGS_DIR / "match_publications_contained_name.log"

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
MIN_FIRM_NAME_LENGTH = 8  # Require firm name to be at least 8 chars
BASE_CONFIDENCE = 0.96
COUNTRY_BOOST = 0.01
BUSINESS_BOOST = 0.01
MAX_CONFIDENCE = 0.97

# Generic words to avoid matching
GENERIC_WORDS = {
    'COMPANY', 'CORPORATION', 'INCORPORATED', 'LIMITED', 'LTD',
    'LLC', 'CO', 'CORP', 'INC', 'GROUP', 'HOLDINGS', 'INTERNATIONAL',
    'NATIONAL', 'ASSOCIATION', 'INSTITUTE', 'LABORATORY', 'UNIVERSITY',
    'COLLEGE', 'SCHOOL', 'SYSTEMS', 'SOLUTIONS', 'TECHNOLOGIES', 'SERVICES'
}


def check_contained_name(institution_name: Optional[str],
                        firm_name: Optional[str]) -> Tuple[bool, str]:
    """
    Check if firm name is contained in institution name.

    Args:
        institution_name: Normalized institution name
        firm_name: Normalized firm name

    Returns:
        Tuple of (is_contained, which_name)
    """
    if not institution_name or not firm_name:
        return False, ""

    # Firm name must be substantial (not generic)
    if len(firm_name) < MIN_FIRM_NAME_LENGTH:
        return False, ""

    # Check if firm name is a substring of institution name
    if firm_name in institution_name:
        # Make sure it's not just a generic word
        if firm_name in GENERIC_WORDS:
            return False, ""

        return True, "contained_in_normalized"

    return False, ""


def check_country_match(institution_country: Optional[str],
                       firm_fic: Optional[str]) -> bool:
    """
    Check if institution country matches firm country.

    Args:
        institution_country: Institution country code (e.g., 'US')
        firm_fic: Firm FIPS country code (e.g., 'USA')

    Returns:
        True if countries match
    """
    if not institution_country or not firm_fic:
        return False

    # Normalize to 2-letter codes
    inst_country = institution_country.upper()[:2]
    firm_country = firm_fic.upper()[:2]

    return inst_country == firm_country


def check_business_description_keywords(institution_name: str,
                                       busdesc: Optional[str]) -> bool:
    """
    Check if firm's business description contains institution name keywords.

    Args:
        institution_name: Institution name to extract keywords from
        busdesc: Firm's business description

    Returns:
        True if business description contains relevant keywords
    """
    if not busdesc:
        return False

    busdesc_upper = busdesc.upper()

    # Extract meaningful keywords from institution name (3+ chars)
    keywords = [w for w in institution_name.split()
                if len(w) >= 3 and w not in GENERIC_WORDS]

    # Check if any keyword appears in business description
    for keyword in keywords:
        # Use word boundary matching
        pattern = rf'\b{re.escape(keyword)}\b'
        if re.search(pattern, busdesc_upper):
            return True

    return False


def match_institution_by_contained_name(institution_row: Dict,
                                        firms_df: pl.DataFrame) -> List[Dict]:
    """
    Match institution to firms using contained name matching.

    Args:
        institution_row: Institution record
        firms_df: Standardized Compustat firms DataFrame

    Returns:
        List of match records
    """
    matches = []

    institution_id = institution_row['institution_id']
    display_name = institution_row['display_name']
    normalized_name = institution_row.get('normalized_name') or institution_row.get('name_variants', [None])[0]
    country_code = institution_row.get('country_code') or institution_row.get('geo_country_code')
    paper_count = institution_row['paper_count']

    if not normalized_name:
        return matches

    # Use display_name for keyword extraction (more descriptive)
    inst_name_upper = display_name.upper()

    # Check each firm
    for firm_row in firms_df.iter_rows(named=True):
        gvkey = firm_row['GVKEY']
        lpermno = firm_row.get('LPERMNO')
        conm = firm_row['conm']
        conm_clean = firm_row.get('conm_clean')
        conml_clean = firm_row.get('conml_clean')
        fic = firm_row.get('fic')
        busdesc = firm_row.get('busdesc')

        # Try both conm_clean and conml_clean
        firm_names_to_check = []
        if conm_clean:
            firm_names_to_check.append(('conm_clean', conm_clean))
        if conml_clean:
            firm_names_to_check.append(('conml_clean', conml_clean))

        for name_type, firm_name in firm_names_to_check:
            # Check if firm name is contained in institution name
            is_contained, which_name = check_contained_name(normalized_name, firm_name)

            if not is_contained:
                continue

            # Calculate confidence with validation boosts
            confidence = BASE_CONFIDENCE

            # Validation: Country match
            country_match = check_country_match(country_code, fic)
            if country_match:
                confidence = min(confidence + COUNTRY_BOOST, MAX_CONFIDENCE)

            # Validation: Business description keywords
            business_match = check_business_description_keywords(inst_name_upper, busdesc)
            if business_match:
                confidence = min(confidence + BUSINESS_BOOST, MAX_CONFIDENCE)

            # Create match record
            matches.append({
                'GVKEY': gvkey,
                'LPERMNO': lpermno,
                'firm_conm': conm,
                'institution_id': institution_id,
                'institution_display_name': display_name,
                'match_type': 'contained_name',
                'match_confidence': confidence,
                'match_method': f'contained_{name_type}',
                'firm_name_contained': firm_name,
                'country_match': country_match,
                'business_match': business_match,
                'institution_paper_count': paper_count,
            })

            # Only match first firm name that matches (avoid duplicates)
            break

    return matches


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("METHOD 1.3: CONTAINED NAME MATCHING")
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

    # Step 2: Create firm lookup for efficiency
    logger.info("\n[2/5] Preparing firm data...")

    # Filter firms that have meaningful cleaned names
    firms_with_names = firms_df.filter(
        (pl.col('conm_clean').is_not_null()) &
        (pl.col('conm_clean').str.len_chars() >= MIN_FIRM_NAME_LENGTH)
    )

    logger.info(f"  {len(firms_with_names):,} firms with names ≥{MIN_FIRM_NAME_LENGTH} chars")

    # Step 3: Match institutions to firms
    logger.info("\n[3/5] Matching institutions to firms (contained name)...")
    logger.info("  This may take several minutes...")

    all_matches = []
    total_institutions = len(institutions_df)
    matched_count = 0

    for i, inst_row in enumerate(institutions_df.iter_rows(named=True)):
        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1:,}/{total_institutions:,} institutions ({len(all_matches):,} matches so far, {matched_count:,} matched)...")

        matches = match_institution_by_contained_name(inst_row, firms_with_names)

        if matches:
            matched_count += 1

        all_matches.extend(matches)

    logger.info(f"  Completed matching.")
    logger.info(f"  Institutions matched: {matched_count:,}")
    logger.info(f"  Total matches: {len(all_matches):,}")

    # Step 4: Deduplicate
    logger.info("\n[4/5] Deduplicating matches...")

    if not all_matches:
        logger.warning("  No matches found!")
        matches_df = pl.DataFrame()
    else:
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
    logger.info("\n[5/5] Saving matches...")
    logger.info(f"Output: {OUTPUT_FILE}")

    if len(matches_df) > 0:
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df):,} matches")

        # Summary statistics
        logger.info("\n" + "=" * 80)
        logger.info("CONTAINED NAME MATCHING SUMMARY")
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

        # Validation boost statistics
        if 'country_match' in matches_df.columns:
            country_match_count = matches_df['country_match'].sum()
            business_match_count = matches_df['business_match'].sum()
            logger.info(f"\nValidation boosts:")
            logger.info(f"  Country match: {country_match_count:,} ({country_match_count/len(matches_df)*100:.1f}%)")
            logger.info(f"  Business match: {business_match_count:,} ({business_match_count/len(matches_df)*100:.1f}%)")

        # Show examples
        logger.info("\nExample matches:")
        for i, row in enumerate(matches_df.head(15).iter_rows(named=True), 1):
            logger.info(f"  {i}. {row['institution_display_name'][:50]}")
            logger.info(f"     Contains: {row['firm_name_contained']}")
            logger.info(f"     → {row['firm_conm'][:50]}")
            logger.info(f"     Confidence: {row['match_confidence']:.2f} (Country: {row['country_match']}, Business: {row['business_match']})")

    else:
        logger.warning("  No matches to save!")

    logger.info("\n" + "=" * 80)
    logger.info("CONTAINED NAME MATCHING COMPLETE")
    logger.info("=" * 80)

    return matches_df


if __name__ == "__main__":
    main()
