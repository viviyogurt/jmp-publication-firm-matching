"""
Direct Matching for Top Unmatched Institutions

Creates direct mappings for top institutions by paper count.
Uses location removal and name matching with firm name variants.

Target: +200-500 firms @ 95%+ accuracy
"""

import polars as pl
from pathlib import Path
import logging
import re
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
BASELINE_MATCHES = DATA_PROCESSED_LINK / "publication_firm_matches_cleaned.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_direct.parquet"

DATA_PROCESSED_LINK.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "match_top_institutions_direct.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def remove_location_qualifiers(name: str) -> str:
    """Remove location qualifiers like ' (United States)'."""
    if not name:
        return name
    # Remove anything in parentheses at the end
    name = re.sub(r'\s*\(.*?\)\s*$', '', name)
    return name.strip()


def clean_name_for_matching(name: str) -> str:
    """
    Clean name for matching by removing common suffixes and standardizing.
    """
    if not name:
        return name

    name = name.upper()

    # Remove common suffixes
    suffixes = [
        r'\s+INCORPORATED$', r'\s+CORPORATION$', r'\s+COMPANY$',
        r'\s+LTD$', r'\s+LIMITED$', r'\s+LLC$', r'\s+AG$',
        r'\s+GMBH$', r'\s+SA$', r'\s+PLC$', r'\s+NV$',
        r'\s+CO$', r'\s+CORP$', r'\s+INC$', r'\s+LTEE$',
        r'\s+S$', r'\s+K\.K\.$', r'\s+SPA$'
    ]

    for suffix in suffixes:
        name = re.sub(suffix, '', name)

    # Remove punctuation and extra spaces
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()

    return name


def match_institution_to_firms(institution_name: str,
                                firms_df: pl.DataFrame) -> List[Dict]:
    """
    Match institution to firms using various strategies.
    """
    matches = []

    # Clean institution name
    inst_clean = clean_name_for_matching(institution_name)

    if not inst_clean or len(inst_clean) < 3:
        return matches

    # Strategy 1: Exact match on cleaned names
    exact_matches = firms_df.filter(
        pl.col('conm_clean') == inst_clean
    )

    if len(exact_matches) > 0:
        for row in exact_matches.iter_rows(named=True):
            matches.append({
                'GVKEY': row['GVKEY'],
                'LPERMNO': row.get('LPERMNO'),
                'firm_conm': row['conm'],
                'match_type': 'direct_exact',
                'match_confidence': 0.98,
                'match_method': 'clean_name_exact',
            })
        return matches

    # Strategy 2: Institution name is substring of firm name
    # Example: "Google" in "Alphabet Inc"
    substring_matches = firms_df.filter(
        pl.col('conm_clean').str.contains(inst_clean)
    )

    if len(substring_matches) > 0:
        for row in substring_matches.iter_rows(named=True):
            firm_clean = row['conm_clean']

            # Check if it's a meaningful substring (not too generic)
            if len(inst_clean) >= 5 or inst_clean not in ['TECH', 'SYS', 'SOL']:
                matches.append({
                    'GVKEY': row['GVKEY'],
                    'LPERMNO': row.get('LPERMNO'),
                    'firm_conm': row['conm'],
                    'match_type': 'direct_contained',
                    'match_confidence': 0.95,
                    'match_method': 'institution_in_firm_name',
                })

    # Strategy 3: Firm name is substring of institution name
    # Example: "Microsoft" in "Microsoft Research"
    for firm_row in firms_df.iter_rows(named=True):
        firm_clean = firm_row['conm_clean']

        if firm_clean and len(firm_clean) >= 5 and firm_clean in inst_clean:
            matches.append({
                'GVKEY': firm_row['GVKEY'],
                'LPERMNO': firm_row.get('LPERMNO'),
                'firm_conm': firm_row['conm'],
                'match_type': 'direct_contained',
                'match_confidence': 0.95,
                'match_method': 'firm_in_institution_name',
            })

    return matches


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("DIRECT MATCHING FOR TOP UNMATCHED INSTITUTIONS")
    logger.info("=" * 80)

    # Step 1: Load data
    logger.info("\n[1/5] Loading data...")

    institutions = pl.read_parquet(INSTITUTIONS_MASTER)
    firms = pl.read_parquet(COMPUSTAT_STANDARDIZED)

    # Add cleaned firm names for matching
    firm_names_clean = []
    for name in firms['conm_clean'].to_list():
        firm_names_clean.append(clean_name_for_matching(name))

    firms = firms.with_columns(
        pl.Series('conm_clean_extra', firm_names_clean)
    )

    # Load baseline
    baseline = pl.read_parquet(BASELINE_MATCHES)
    baseline_ids = set(baseline['institution_id'].to_list())

    logger.info(f"  Loaded {len(institutions):,} institutions")
    logger.info(f"  Loaded {len(firms):,} firms")
    logger.info(f"  Baseline matched: {len(baseline_ids):,} institutions")

    # Step 2: Filter to unmatched institutions
    logger.info("\n[2/5] Filtering to unmatched institutions and removing locations...")

    unmatched = institutions.filter(
        ~pl.col('institution_id').is_in(baseline_ids)
    ).sort('paper_count', descending=True)

    # Remove location qualifiers
    unmatched_clean = []
    for row in unmatched.iter_rows(named=True):
        display_name = row['display_name']
        name_no_location = remove_location_qualifiers(display_name)

        new_row = dict(row)
        new_row['display_name_no_location'] = name_no_location
        unmatched_clean.append(new_row)

    unmatched = pl.DataFrame(unmatched_clean)

    logger.info(f"  Unmatched institutions: {len(unmatched):,}")
    location_changed = unmatched.filter(
        pl.col('display_name') != pl.col('display_name_no_location')
    )
    logger.info(f"  Location removed from {len(location_changed):,} institutions")

    # Step 3: Match top institutions
    logger.info("\n[3/5] Matching top institutions...")

    # Focus on top institutions by paper count (e.g., top 1,000)
    TOP_N = 1000
    top_institutions = unmatched.head(TOP_N)

    logger.info(f"  Processing top {TOP_N} institutions by paper count...")

    all_matches = []
    matched_count = 0

    for i, inst_row in enumerate(top_institutions.iter_rows(named=True)):
        if (i + 1) % 100 == 0:
            logger.info(f"  Processed {i+1:,}/{TOP_N:,} ({len(all_matches):,} matches so far)...")

        institution_id = inst_row['institution_id']
        display_name = inst_row['display_name']
        name_no_location = inst_row['display_name_no_location']

        # Try matching with location removed
        matches = match_institution_to_firms(name_no_location, firms)

        if matches:
            matched_count += 1
            for match in matches:
                match['institution_id'] = institution_id
                match['institution_display_name'] = display_name
                match['institution_paper_count'] = inst_row['paper_count']
                all_matches.append(match)

    logger.info(f"\n  Completed. Found {len(all_matches):,} total matches")
    logger.info(f"  Institutions matched: {matched_count:,}")

    if not all_matches:
        logger.warning("  No matches found!")
        return

    # Step 4: Deduplicate
    logger.info("\n[4/5] Deduplicating...")

    matches_df = pl.DataFrame(all_matches)

    # Keep highest confidence match per institution-firm pair
    matches_df = (
        matches_df
        .sort(['institution_id', 'GVKEY', 'match_confidence'], descending=[False, False, True])
        .unique(subset=['institution_id', 'GVKEY'], keep='first')
    )

    logger.info(f"  After deduplication: {len(matches_df):,} unique matches")
    logger.info(f"  Unique institutions: {matches_df['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {matches_df['GVKEY'].n_unique():,}")

    # Step 5: Save
    logger.info("\n[5/5] Saving...")
    matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved to: {OUTPUT_FILE}")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("DIRECT MATCHING SUMMARY")
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
    logger.info("\nTop 20 matches by paper count:")
    top_matches = matches_df.sort('institution_paper_count', descending=True).head(20)
    for i, row in enumerate(top_matches.iter_rows(named=True), 1):
        logger.info(f"  {i}. {row['institution_display_name'][:50]:<50}")
        logger.info(f"     Papers: {row['institution_paper_count']:>6,} → {row['firm_conm'][:50]}")
        logger.info(f"     Method: {row['match_method']}, Conf: {row['match_confidence']:.2f}")

    logger.info("\n" + "=" * 80)
    logger.info("DIRECT MATCHING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
