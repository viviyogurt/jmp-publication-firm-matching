"""
Enhanced Alternative Name Matching for Publications

Handles abbreviations (INTL vs INTERNATIONAL) and uses fuzzy matching
for better coverage. Key to matching IBM, Samsung, etc.

Target: +200-500 additional firms @ 95%+ accuracy
"""

import polars as pl
from pathlib import Path
import logging
import re
from typing import Dict, List
from rapidfuzz import fuzz, process

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
MATCHED_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_with_location_removal.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_alternative_names_enhanced.parquet"

DATA_PROCESSED_LINK.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "match_alternative_names_enhanced.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Common abbreviations mapping
ABBREVIATIONS = {
    'INTL': 'INTERNATIONAL',
    'INT': 'INTERNATIONAL',
    'CORP': 'CORPORATION',
    'INC': 'INCORPORATED',
    'CO': 'COMPANY',
    'LTD': 'LIMITED',
    'MFG': 'MANUFACTURING',
    'MFR': 'MANUFACTURER',
    'TECH': 'TECHNOLOGY',
    'TEL': 'TELEPHONE',
    'TELCO': 'TELECOMMUNICATION',
    'COMM': 'COMMUNICATIONS',
    'SYS': 'SYSTEMS',
    'SERV': 'SERVICES',
    'DEV': 'DEVELOPMENT',
    'RES': 'RESEARCH',
    'ELEC': 'ELECTRIC',
    'ENG': 'ENGINEERING',
    'IND': 'INDUSTRIES',
    'INDUS': 'INDUSTRIAL',
    'PROD': 'PRODUCTS',
    'GRP': 'GROUP',
    'ENT': 'ENTERTAINMENT',
    'ASSOC': 'ASSOCIATES',
    'ASSN': 'ASSOCIATION',
    'NATL': 'NATIONAL',
    'GOV': 'GOVERNMENT',
    'PUB': 'PUBLIC',
    'PRI': 'PRIVATE',
    'CONST': 'CONSTRUCTION',
    'AGR': 'AGRICULTURE',
    'PHARM': 'PHARMACEUTICAL',
    'BIOTECH': 'BIOTECHNOLOGY',
    'INFO': 'INFORMATION',
    'SCI': 'SCIENCE',
    'UNIV': 'UNIVERSITY',
    'INST': 'INSTITUTE',
    'CTR': 'CENTER',
    'CTR': 'CENTRE',
    'LAB': 'LABORATORY',
    'LABS': 'LABORATORIES',
}


def remove_location_qualifiers(name: str) -> str:
    """Remove location qualifiers like ' (United States)'."""
    if not name:
        return name
    name = re.sub(r'\s*\(.*?\)\s*$', '', name)
    return name.strip()


def expand_abbreviations(name: str) -> str:
    """Expand common abbreviations in company names."""
    if not name:
        return name

    words = name.split()
    expanded_words = []

    for word in words:
        # Remove punctuation for matching
        word_clean = re.sub(r'[^\w]', '', word.upper())

        if word_clean in ABBREVIATIONS:
            expanded_words.append(ABBREVIATIONS[word_clean])
        else:
            expanded_words.append(word)

    return ' '.join(expanded_words)


def clean_name(name: str) -> str:
    """Clean name for matching."""
    if not name:
        return name
    name = name.upper()
    # Remove suffixes
    suffixes = [
        r'\s+INCORPORATED$', r'\s+CORPORATION$', r'\s+COMPANY$',
        r'\s+LTD$', r'\s+LIMITED$', r'\s+LLC$', r'\s+AG$',
        r'\s+GMBH$', r'\s+SA$', r'\s+PLC$', r'\s+NV$',
        r'\s+CO$', r'\s+CORP$', r'\s+INC$', r'\s+LTEE$',
    ]
    for suffix in suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def clean_name_keep_suffixes(name: str) -> str:
    """Clean name but keep useful suffixes for matching."""
    if not name:
        return name
    name = name.upper()
    # Remove punctuation
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("ENHANCED ALTERNATIVE NAME MATCHING FOR PUBLICATIONS")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/6] Loading data...")
    institutions = pl.read_parquet(INSTITUTIONS_MASTER)
    firms = pl.read_parquet(COMPUSTAT_STANDARDIZED)
    matched = pl.read_parquet(MATCHED_FILE)

    logger.info(f"  Loaded {len(institutions):,} institutions")
    logger.info(f"  Loaded {len(firms):,} firms")
    logger.info(f"  Loaded {len(matched):,} matched pairs")

    # Get matched IDs
    matched_ids = set(matched['institution_id'].to_list())
    logger.info(f"  Already matched: {len(matched_ids):,} institutions")

    # Filter to unmatched
    logger.info("\n[2/6] Filtering to unmatched institutions...")
    unmatched = institutions.filter(
        ~pl.col('institution_id').is_in(matched_ids)
    )
    logger.info(f"  Unmatched: {len(unmatched):,} institutions")

    # Build firm lookup with multiple versions
    logger.info("\n[3/6] Building firm lookup dictionary...")

    firm_lookup = {}
    firm_names_list = []

    for row in firms.iter_rows(named=True):
        gvkey = row['GVKEY']
        conm = row['conm']
        conm_clean = row.get('conm_clean')

        if not conm_clean:
            continue

        # Store different versions for matching
        versions = [
            conm_clean,  # Standard cleaned
            clean_name_keep_suffixes(conm),  # Keep suffixes
            expand_abbreviations(conm_clean),  # Expanded abbreviations
            clean_name_keep_suffixes(expand_abbreviations(conm)),  # Expanded + keep suffixes
        ]

        for version in versions:
            if version and len(version) >= 3:
                if version not in firm_lookup:
                    firm_lookup[version] = []
                firm_lookup[version].append(row)

        # Also add to list for fuzzy matching
        firm_names_list.append({
            'gvkey': gvkey,
            'conm': conm,
            'conm_clean': conm_clean,
        })

    logger.info(f"  Built lookup with {len(firm_lookup):,} name variants")
    logger.info(f"  Total firms: {len(firm_names_list):,}")

    # Process alternative names
    logger.info("\n[4/6] Matching using alternative names with fuzzy matching...")

    all_matches = []
    processed_count = 0
    matched_count = 0

    # Process top unmatched by paper count
    top_by_papers = unmatched.sort('paper_count', descending=True).head(10000)  # Top 10K

    for i, inst_row in enumerate(top_by_papers.iter_rows(named=True)):
        if (i + 1) % 500 == 0:
            logger.info(f"  Processed {i+1:,}/{len(top_by_papers):,} ({len(all_matches):,} matches so far)...")

        institution_id = inst_row['institution_id']
        display_name = inst_row['display_name']
        paper_count = inst_row['paper_count']
        alt_names = inst_row.get('alternative_names', [])

        processed_count += 1

        if not alt_names:
            continue

        # Try each alternative name with multiple strategies
        best_match = None
        best_score = 0

        for alt_name in alt_names:
            # Remove location from alt name
            alt_no_loc = remove_location_qualifiers(alt_name)

            # Try different cleaning strategies
            alt_versions = [
                clean_name(alt_no_loc),  # Standard cleaning
                clean_name_keep_suffixes(alt_no_loc),  # Keep suffixes
                expand_abbreviations(clean_name(alt_no_loc)),  # Expanded
            ]

            for alt_clean in alt_versions:
                if not alt_clean or len(alt_clean) < 3:
                    continue

                # Strategy 1: Exact match
                if alt_clean in firm_lookup:
                    for firm_row in firm_lookup[alt_clean]:
                        score = 100
                        if score > best_score:
                            best_match = (firm_row, alt_name, 'alternative_name_exact')
                            best_score = score

                # Strategy 2: Fuzzy match for high-value institutions
                if paper_count >= 1000:  # Only for institutions with 1000+ papers
                    # Use fuzzy matching on cleaned names
                    results = process.extract(
                        alt_clean,
                        [f['conm_clean'] for f in firm_names_list],
                        limit=1,
                        scorer=fuzz.WRatio
                    )

                    if results and results[0][1] >= 90:  # 90% similarity threshold
                        firm_name = results[0][0]
                        score = results[0][1]

                        if score > best_score:
                            # Find the firm row
                            for firm_data in firm_names_list:
                                if firm_data['conm_clean'] == firm_name:
                                    # Get full firm row
                                    firm_rows = firms.filter(pl.col('GVKEY') == firm_data['gvkey'])
                                    if len(firm_rows) > 0:
                                        best_match = (firm_rows.row(0, named=True), alt_name, 'alternative_name_fuzzy')
                                        best_score = score
                                    break

        # Add best match if found
        if best_match and best_score >= 90:
            firm_row, alt_name_used, method = best_match
            confidence = 0.98 if method == 'alternative_name_exact' else 0.95

            all_matches.append({
                'GVKEY': firm_row['GVKEY'],
                'LPERMNO': firm_row.get('LPERMNO'),
                'firm_conm': firm_row['conm'],
                'institution_id': institution_id,
                'institution_display_name': display_name,
                'alternative_name_used': alt_name_used,
                'match_type': 'alternative_name',
                'match_confidence': confidence,
                'match_method': method,
                'institution_paper_count': paper_count,
            })
            matched_count += 1

    logger.info(f"\n  Completed. Found {len(all_matches):,} total matches")
    logger.info(f"  Institutions matched: {matched_count:,}")

    if not all_matches:
        logger.warning("  No matches found!")
        return

    # Deduplicate
    logger.info("\n[5/6] Deduplicating...")

    matches_df = pl.DataFrame(all_matches)

    matches_df = (
        matches_df
        .sort(['institution_id', 'GVKEY', 'match_confidence'], descending=[False, False, True])
        .unique(subset=['institution_id', 'GVKEY'], keep='first')
    )

    logger.info(f"  After deduplication: {len(matches_df):,} unique matches")
    logger.info(f"  Unique institutions: {matches_df['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {matches_df['GVKEY'].n_unique():,}")

    # Save
    logger.info("\n[6/6] Saving...")
    matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved to: {OUTPUT_FILE}")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("ENHANCED ALTERNATIVE NAME MATCHING SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total matches: {len(matches_df)}")
    logger.info(f"Unique institutions: {matches_df['institution_id'].n_unique()}")
    logger.info(f"Unique firms: {matches_df['GVKEY'].n_unique()}")

    # Match method distribution
    method_counts = matches_df.group_by('match_method').agg(
        pl.len().alias('count')
    ).sort('count', descending=True)
    logger.info("\nMatch method distribution:")
    for row in method_counts.iter_rows(named=True):
        logger.info(f"  {row['match_method']}: {row['count']:,} matches")

    # Confidence distribution
    logger.info(f"\nConfidence statistics:")
    logger.info(f"  Mean: {matches_df['match_confidence'].mean():.3f}")
    logger.info(f"  Min: {matches_df['match_confidence'].min():.3f}")
    logger.info(f"  Max: {matches_df['match_confidence'].max():.3f}")

    # Top matches by paper count
    logger.info("\nTop 30 matches by paper count:")
    top_matches = matches_df.sort('institution_paper_count', descending=True).head(30)
    for i, row in enumerate(top_matches.iter_rows(named=True), 1):
        logger.info(f"  {i}. {row['institution_display_name'][:55]:<55}")
        logger.info(f"     Papers: {row['institution_paper_count']:>6,} → {row['firm_conm'][:50]}")
        logger.info(f"     Alternative name used: {row['alternative_name_used'][:50]}")
        logger.info(f"     Method: {row['match_method']}, Conf: {row['match_confidence']:.2f}")

    logger.info("\n" + "=" * 80)
    logger.info("ENHANCED ALTERNATIVE NAME MATCHING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
