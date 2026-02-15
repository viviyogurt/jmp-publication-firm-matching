"""
Fix Separator Normalization Issues

Matches institutions that failed due to separator differences:
- "Alcatel Lucent" vs "ALCATEL-LUCENT"
- "Bank of America" vs "BANKOFAMERICA"
etc.

Target: +50-100 firms @ 98%+ accuracy
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
MATCHED_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_with_alternative_names.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_separator_normalized.parquet"

DATA_PROCESSED_LINK.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "match_separator_normalization.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def ultra_clean_name(name: str) -> str:
    """
    Ultra-clean name that removes ALL separators for comparison.
    "Alcatel Lucent" → "ALCATELLUCENT"
    "ALCATEL-LUCENT" → "ALCATELLUCENT"
    "Bank of America" → "BANKOFAMERICA"
    """
    if not name:
        return name

    name = name.upper()
    # Remove location qualifiers
    name = re.sub(r'\s*\(.*?\)\s*$', '', name)
    # Remove ALL separators (spaces, hyphens, periods, commas)
    name = re.sub(r'[\s\-\.&,]', '', name)
    # Remove common suffixes
    suffixes = [
        r'INCORPORATED$', r'CORPORATION$', r'COMPANY$',
        r'LTD$', r'LIMITED$', r'LLC$', r'AG$',
        r'GMBH$', r'SA$', r'PLC$', r'NV$',
        r'CO$', r'CORP$', r'INC$', r'LTEE$',
    ]
    for suffix in suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    # Remove any remaining non-alphanumeric characters
    name = re.sub(r'[^A-Z0-9]', '', name)
    return name


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("SEPARATOR NORMALIZATION MATCHING")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/5] Loading data...")
    institutions = pl.read_parquet(INSTITUTIONS_MASTER)
    firms = pl.read_parquet(COMPUSTAT_STANDARDIZED)
    matches = pl.read_parquet(MATCHED_FILE)

    logger.info(f"  Loaded {len(institutions):,} institutions")
    logger.info(f"  Loaded {len(firms):,} firms")
    logger.info(f"  Loaded {len(matches):,} matched pairs")

    # Get matched IDs
    matched_ids = set(matches['institution_id'].to_list())
    matched_gvkeys = set(matches['GVKEY'].to_list())

    logger.info(f"  Already matched: {len(matched_ids):,} institutions")
    logger.info(f"  Already matched firms: {len(matched_gvkeys):,}")

    # Filter to unmatched
    logger.info("\n[2/5] Filtering to unmatched institutions...")
    unmatched = institutions.filter(
        ~pl.col('institution_id').is_in(matched_ids)
    )
    logger.info(f"  Unmatched: {len(unmatched):,} institutions")

    # Filter to unmatched firms
    unmatched_firms = firms.filter(
        ~pl.col('GVKEY').is_in(matched_gvkeys)
    )
    logger.info(f"  Unmatched firms: {len(unmatched_firms):,}")

    # Build ultra-clean lookups
    logger.info("\n[3/5] Building ultra-clean lookups...")

    # Ultra-clean firm lookup
    firm_ultra_lookup = {}
    for firm_row in unmatched_firms.iter_rows(named=True):
        conm_ultra_clean = ultra_clean_name(firm_row['conm'])
        if conm_ultra_clean and len(conm_ultra_clean) >= 4:  # Minimum length
            if conm_ultra_clean not in firm_ultra_lookup:
                firm_ultra_lookup[conm_ultra_clean] = []
            firm_ultra_lookup[conm_ultra_clean].append(firm_row)

    logger.info(f"  Built ultra-clean firm lookup: {len(firm_ultra_lookup):,} entries")

    # Match using ultra-clean names
    logger.info("\n[4/5] Matching using ultra-clean names...")

    all_matches = []

    # Process top unmatched by paper count
    top_unmatched = unmatched.sort('paper_count', descending=True).head(5000)

    for i, inst_row in enumerate(top_unmatched.iter_rows(named=True)):
        if (i + 1) % 500 == 0:
            logger.info(f"  Processed {i+1:,}/{len(top_unmatched):,} ({len(all_matches):,} matches)...")

        # Ultra-clean institution name
        inst_ultra_clean = ultra_clean_name(inst_row['display_name'])

        if not inst_ultra_clean or len(inst_ultra_clean) < 4:
            continue

        # Try exact match on ultra-clean names
        if inst_ultra_clean in firm_ultra_lookup:
            for firm_row in firm_ultra_lookup[inst_ultra_clean]:
                all_matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row['conm'],
                    'institution_id': inst_row['institution_id'],
                    'institution_display_name': inst_row['display_name'],
                    'match_type': 'separator_normalized',
                    'match_confidence': 0.98,
                    'match_method': 'ultra_clean_exact',
                    'institution_paper_count': inst_row['paper_count'],
                    'inst_ultra_clean': inst_ultra_clean,
                    'firm_ultra_clean': ultra_clean_name(firm_row['conm']),
                })

        # Also try alternative names
        alt_names = inst_row.get('alternative_names', [])
        for alt_name in alt_names:
            alt_ultra_clean = ultra_clean_name(alt_name)
            if alt_ultra_clean and len(alt_ultra_clean) >= 4 and alt_ultra_clean in firm_ultra_lookup:
                for firm_row in firm_ultra_lookup[alt_ultra_clean]:
                    all_matches.append({
                        'GVKEY': firm_row['GVKEY'],
                        'LPERMNO': firm_row.get('LPERMNO'),
                        'firm_conm': firm_row['conm'],
                        'institution_id': inst_row['institution_id'],
                        'institution_display_name': inst_row['display_name'],
                        'alternative_name_used': alt_name,
                        'match_type': 'separator_normalized',
                        'match_confidence': 0.98,
                        'match_method': 'alt_name_ultra_clean',
                        'institution_paper_count': inst_row['paper_count'],
                        'inst_ultra_clean': alt_ultra_clean,
                        'firm_ultra_clean': ultra_clean_name(firm_row['conm']),
                    })
                break  # Use first matching alternative name

    logger.info(f"\n  Completed. Found {len(all_matches):,} total matches")

    if not all_matches:
        logger.warning("  No matches found!")
        return

    # Deduplicate
    logger.info("\n[5/5] Deduplicating...")

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
    matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"\n  Saved to: {OUTPUT_FILE}")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("SEPARATOR NORMALIZATION MATCHING SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total matches: {len(matches_df)}")
    logger.info(f"Unique institutions: {matches_df['institution_id'].n_unique()}")
    logger.info(f"Unique firms: {matches_df['GVKEY'].n_unique()}")

    # Top matches by paper count
    logger.info("\nTop 30 matches by paper count:")
    top_matches = matches_df.sort('institution_paper_count', descending=True).head(30)
    for i, row in enumerate(top_matches.iter_rows(named=True), 1):
        logger.info(f"  {i}. {row['institution_display_name'][:55]:<55}")
        logger.info(f"     Papers: {row['institution_paper_count']:>6,} → {row['firm_conm'][:50]}")
        logger.info(f"     Inst ultra-clean: {row['inst_ultra_clean'][:50]}")
        logger.info(f"     Firm ultra-clean: {row['firm_ultra_clean'][:50]}")

    logger.info("\n" + "=" * 80)
    logger.info("SEPARATOR NORMALIZATION MATCHING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
