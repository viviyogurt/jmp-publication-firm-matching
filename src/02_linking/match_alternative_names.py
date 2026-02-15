"""
Alternative Name Matching for Publications

Uses institution alternative_names field to match to firms.
This is the key to matching IBM, Samsung, Toshiba, etc.

Target: +500-1,000 firms @ 95%+ accuracy
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
MATCHED_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_with_location_removal.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_alternative_names.parquet"

DATA_PROCESSED_LINK.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "match_alternative_names.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def remove_location_qualifiers(name: str) -> str:
    """Remove location qualifiers like ' (United States)'."""
    if not name:
        return name
    name = re.sub(r'\s*\(.*?\)\s*$', '', name)
    return name.strip()


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


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("ALTERNATIVE NAME MATCHING FOR PUBLICATIONS")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/5] Loading data...")
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
    logger.info("\n[2/5] Filtering to unmatched institutions...")
    unmatched = institutions.filter(
        ~pl.col('institution_id').is_in(matched_ids)
    )
    logger.info(f"  Unmatched: {len(unmatched):,} institutions")

    # Process alternative names
    logger.info("\n[3/5] Matching using alternative names...")

    all_matches = []
    processed_count = 0
    matched_count = 0

    # Create firm lookup
    firm_lookup = {}
    for row in firms.iter_rows(named=True):
        conm_clean = row.get('conm_clean')
        if conm_clean:
            if conm_clean not in firm_lookup:
                firm_lookup[conm_clean] = []
            firm_lookup[conm_clean].append(row)

    # Process institutions
    total = len(unmatched)
    top_by_papers = unmatched.sort('paper_count', descending=True).head(5000)  # Top 5K

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

        # Try each alternative name
        for alt_name in alt_names:
            # Clean alternative name
            alt_clean = clean_name(alt_name)

            if not alt_clean or len(alt_clean) < 3:
                continue

            # Try exact match
            if alt_clean in firm_lookup:
                for firm_row in firm_lookup[alt_clean]:
                    all_matches.append({
                        'GVKEY': firm_row['GVKEY'],
                        'LPERMNO': firm_row.get('LPERMNO'),
                        'firm_conm': firm_row['conm'],
                        'institution_id': institution_id,
                        'institution_display_name': display_name,
                        'alternative_name_used': alt_name,
                        'match_type': 'alternative_name',
                        'match_confidence': 0.95,
                        'match_method': 'alternative_name_exact',
                        'institution_paper_count': paper_count,
                    })
                break  # Use first matching alternative name

        if len(all_matches) > matched_count:
            matched_count = len(all_matches)

    logger.info(f"\n  Completed. Found {len(all_matches):,} total matches")
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

    logger.info(f"  After deduplication: {len(matches_df):,} unique matches")
    logger.info(f"  Unique institutions: {matches_df['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {matches_df['GVKEY'].n_unique():,}")

    # Save
    logger.info("\n[5/5] Saving...")
    matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved to: {OUTPUT_FILE}")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("ALTERNATIVE NAME MATCHING SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total matches: {len(matches_df)}")
    logger.info(f"Unique institutions: {matches_df['institution_id'].n_unique()}")
    logger.info(f"Unique firms: {matches_df['GVKEY'].n_unique()}")

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

    logger.info("\n" + "=" * 80)
    logger.info("ALTERNATIVE NAME MATCHING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
