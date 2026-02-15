"""
Subsidiary Recognition Matching

Match corporate research units and subsidiaries to parent companies.

Approach:
1. Identify research subsidiaries: "X Research", "X Labs", "X Innovation"
2. Match to parent companies using name containment
3. Require validation: country match, business description keywords
4. Conservative: only match when relationship is obvious

Target: +1,000-1,500 firms @ 90%+ accuracy

Examples:
- "Microsoft Research" → Microsoft Corp
- "Google DeepMind" → Alphabet/Google
- "IBM Research" → IBM
- "Samsung Electronics" → Samsung
"""

import polars as pl
from pathlib import Path
import logging
import re

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
BASELINE_MATCHES = DATA_PROCESSED_LINK / "publication_firm_matches_cleaned.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_subsidiaries.parquet"

DATA_PROCESSED_LINK.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "match_subsidiaries.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIDENCE_BASE = 0.92
COUNTRY_BOOST = 0.03
RESEARCH_KEYWORD_BOOST = 0.02
MAX_CONFIDENCE = 0.97

# Research subsidiary patterns
SUBSIDIARY_PATTERNS = [
    r'(.+)\s+Research$',
    r'(.+)\s+Laborator(?:y|ies)$',
    r'(.+)\s+Labs$',
    r'(.+)\s+Innovation$',
    r'(.+)\s+Technologies?$',
    r'(.+)\s+Solutions?$',
    r'(.+)\s+Advanced\s+Technology$',
    r'(.+)\s+Corporate\s+Research$',
]

# Research keywords in business descriptions
RESEARCH_KEYWORDS = [
    'research', 'development', 'technology', 'innovation',
    'biotechnology', 'pharmaceutical', 'semiconductor',
    'software', 'hardware', 'engineering', 'sciences'
]


def extract_parent_name(institution_name: str) -> str:
    """
    Extract parent company name from research subsidiary.

    Examples:
    - "Microsoft Research (United Kingdom)" → "Microsoft"
    - "IBM Research (United States)" → "IBM"
    - "Google DeepMind" → "Google"
    """
    name = institution_name.strip()

    # Remove common suffixes and qualifiers
    for pattern in SUBSIDIARY_PATTERNS:
        match = re.match(pattern, name, re.IGNORECASE)
        if match:
            parent = match.group(1).strip()
            # Remove trailing words that are clearly not part of company name
            parent = re.sub(r'\s+(United|States|America|North|South|East|West|International|Global|Worldwide|Group|Holdings)\s*$', '', parent, flags=re.IGNORECASE)
            return parent.strip()

    # If no pattern matched, check for direct substring matches
    # Example: "Google DeepMind" contains "Google"
    # This is handled in the main matching logic

    return name


def check_business_description_research(firm_busdesc: str) -> bool:
    """Check if firm business description indicates research activity."""
    if not firm_busdesc:
        return False

    busdesc_lower = firm_busdesc.lower()
    return any(keyword in busdesc_lower for keyword in RESEARCH_KEYWORDS)


def match_institution_as_subsidiary(institution_row: dict,
                                       firms_df: pl.DataFrame,
                                       baseline_gvkeys: set) -> list:
    """
    Match institution to firms using subsidiary recognition.
    """
    matches = []

    institution_id = institution_row['institution_id']
    display_name = institution_row['display_name']
    normalized_name = institution_row.get('normalized_name') or institution_row.get('name_variants', [None])[0]
    country_code = institution_row.get('country_code') or institution_row.get('geo_country_code')
    paper_count = institution_row.get('paper_count', 0)

    if not normalized_name:
        return matches

    # Skip if already matched in baseline
    # (This check will be done at the combination stage)

    # Extract potential parent name
    potential_parent = extract_parent_name(display_name)

    # For each firm, check if it could be the parent
    for firm_row in firms_df.iter_rows(named=True):
        gvkey = firm_row['GVKEY']
        lpermno = firm_row.get('LPERMNO')
        conm = firm_row['conm']
        conm_clean = firm_row.get('conm_clean')
        conml_clean = firm_row.get('conml_clean')
        firm_country = firm_row.get('fic')
        busdesc = firm_row.get('busdesc')

        # Skip if no clean name to match against
        if not conm_clean and not conml_clean:
            continue

        firm_names_to_check = [conm_clean]
        if conml_clean:
            firm_names_to_check.append(conml_clean)

        # Check if firm name is contained in institution name or extracted parent
        for firm_name in firm_names_to_check:
            # Check if firm name is in potential_parent
            if firm_name in potential_parent or potential_parent in firm_name:
                # Check length requirement (not too generic)
                if len(firm_name) < 8:
                    continue

                # Calculate confidence
                confidence = CONFIDENCE_BASE

                # Validation: Country match
                country_match = False
                if country_code and firm_country:
                    inst_country = country_code.upper()[:2]
                    firm_country = firm_country.upper()[:2]
                    if inst_country == firm_country:
                        country_match = True
                        confidence = min(confidence + COUNTRY_BOOST, MAX_CONFIDENCE)

                # Validation: Business description indicates research
                research_match = check_business_description_research(busdesc)
                if research_match:
                    confidence = min(confidence + RESEARCH_KEYWORD_BOOST, MAX_CONFIDENCE)

                # Only match if at least one validation OR high confidence
                if confidence >= 0.94 or country_match or research_match:
                    matches.append({
                        'GVKEY': gvkey,
                        'LPERMNO': lpermno,
                        'firm_conm': conm,
                        'institution_id': institution_id,
                        'institution_display_name': display_name,
                        'match_type': 'subsidiary_recognition',
                        'match_confidence': confidence,
                        'match_method': 'subsidiary_to_parent',
                        'extracted_parent': potential_parent,
                        'firm_name_matched': firm_name,
                        'country_match': country_match,
                        'research_match': research_match,
                        'institution_paper_count': paper_count,
                    })

                # Only match first firm per institution
                break

    return matches


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("SUBSIDIARY RECOGNITION MATCHING")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/5] Loading data...")

    institutions = pl.read_parquet(INSTITUTIONS_MASTER)
    firms = pl.read_parquet(COMPUSTAT_STANDARDIZED)

    logger.info(f"  Loaded {len(institutions):,} institutions")
    logger.info(f"  Loaded {len(firms):,} firms")

    # Load baseline to get matched IDs
    if BASELINE_MATCHES.exists():
        baseline = pl.read_parquet(BASELINE_MATCHES)
        baseline_gvkeys = set(baseline['gvkey'].to_list())
        baseline_inst_ids = set(baseline['institution_id'].to_list())
        logger.info(f"  Baseline: {baseline_gvkeys} firms, {baseline_inst_ids} institutions")
    else:
        baseline_gvkeys = set()
        baseline_inst_ids = set()
        logger.info("  No baseline found")

    # Filter to unmatched institutions
    logger.info("\n[2/5] Filtering to unmatched institutions...")
    unmatched_inst = institutions.filter(
        ~pl.col('institution_id').is_in(baseline_inst_ids)
    )
    logger.info(f"  Unmatched institutions: {len(unmatched_inst):,}")

    # Run matching
    logger.info("\n[3/5] Running subsidiary recognition matching...")
    logger.info("  This may take a few minutes...")

    all_matches = []
    total = len(unmatched_inst)
    matched_count = 0

    for i, inst_row in enumerate(unmatched_inst.iter_rows(named=True)):
        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} ({len(all_matches):,} matches so far)...")

        matches = match_institution_as_subsidiary(inst_row, firms, baseline_gvkeys)

        if matches:
            matched_count += 1

        all_matches.extend(matches)

    logger.info(f"  Completed. Found {len(all_matches)} total matches")
    logger.info(f"  Institutions matched: {matched_count:,}")

    if not all_matches:
        logger.warning("  No matches found!")
        return

    # Deduplicate
    logger.info("\n[4/5] Deduplicating...")

    matches_df = pl.DataFrame(all_matches)
    matches_df = (
        matches_df
        .sort(['institution_id', 'GVKEY', 'match_confidence'], descending=[False, False, True])
        .unique(subset=['institution_id', 'GVKEY'], keep='first')
    )

    logger.info(f"  After deduplication: {len(matches_df):,} unique institution-firm matches")
    logger.info(f"  Unique institutions: {matches_df['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {matches_df['GVKEY'].n_unique():,}")

    # Save
    logger.info("\n[5/5] Saving...")
    matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved to: {OUTPUT_FILE}")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("SUBSIDIARY RECOGNITION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total matches: {len(matches_df)}")
    logger.info(f"Unique institutions: {matches_df['institution_id'].n_unique()}")
    logger.info(f"Unique firms: {matches_df['GVKEY'].n_unique()}")

    # Confidence distribution
    logger.info(f"\nConfidence statistics:")
    logger.info(f"  Mean: {matches_df['match_confidence'].mean():.3f}")
    logger.info(f"  Min: {matches_df['match_confidence'].min():.3f}")
    logger.info(f"  Max: {matches_df['match_confidence'].max():.3f}")

    # Validation stats
    if 'country_match' in matches_df.columns:
        country_count = matches_df['country_match'].sum()
        logger.info(f"\nCountry match: {country_count:,} ({country_count/len(matches_df)*100:.1f}%)")

    if 'research_match' in matches_df.columns:
        research_count = matches_df['research_match'].sum()
        logger.info(f"Research indicator: {research_count:,} ({research_count/len(matches_df)*100:.1f}%)")

    # Show examples
    logger.info("\nTop 20 matches by paper count:")
    top_matches = matches_df.sort('institution_paper_count', descending=True).head(20)
    for i, row in enumerate(top_matches.iter_rows(named=True), 1):
        logger.info(f"  {i}. {row['institution_display_name'][:50]}")
        logger.info(f"     → {row['firm_conm'][:50]}")
        logger.info(f"     Papers: {row['institution_paper_count']:,}, Conf: {row['match_confidence']:.2f}")

    logger.info("\n" + "=" * 80)
    logger.info("SUBSIDIARY RECOGNITION COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
