"""
Parent Institution Cascade Matching

This script uses parent_institution_ids to cascade matches from parent to children.
If a parent company matches to a firm, all its child institutions automatically match to the same firm.

This is CRITICAL for multi-national company matching:
- Google (parent) → Alphabet Inc
- Google DeepMind (child) → Alphabet Inc (via parent)
- Google UK (child) → Alphabet Inc (via parent)
"""

import polars as pl
from pathlib import Path
import logging
from typing import Dict, List, Set

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_ENRICHED = DATA_INTERIM / "publication_institutions_enriched.parquet"
STAGE1_MATCHES = DATA_PROCESSED_LINK / "publication_firm_matches_stage1.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_parent_cascade.parquet"
PROGRESS_LOG = LOGS_DIR / "match_parent_institutions.log"

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


def main():
    logger.info("=" * 80)
    logger.info("PARENT INSTITUTION CASCADE MATCHING")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/4] Loading data...")
    institutions = pl.read_parquet(INSTITUTIONS_ENRICHED)

    # Check if Stage 1 matches exist
    stage1_exists = STAGE1_MATCHES.exists()
    if stage1_exists:
        stage1_matches = pl.read_parquet(STAGE1_MATCHES)
        logger.info(f"  Loaded {len(institutions)} institutions")
        logger.info(f"  Loaded {len(stage1_matches)} Stage 1 matches")
    else:
        logger.warning("  No Stage 1 matches found - running standalone")
        stage1_matches = None

    # Build institution lookup by ID
    logger.info("\n[2/4] Building institution lookup...")
    inst_dict = {row['institution_id']: row for row in institutions.iter_rows(named=True)}
    logger.info(f"  Created lookup for {len(inst_dict):,} institutions")

    # Find all institutions with parent IDs
    logger.info("\n[3/4] Finding institutions with parent relationships...")
    has_parent = institutions.filter(pl.col('parent_institution_ids').list.len() > 0)
    logger.info(f"  {len(has_parent)} institutions have parent relationships")

    if stage1_matches is None:
        logger.warning("  No Stage 1 matches to cascade from!")
        logger.info("  Skipping parent cascade matching")
        return

    # Get set of already matched institution IDs from Stage 1
    matched_ids = set(stage1_matches['institution_id'].to_list())
    logger.info(f"  Institutions already matched in Stage 1: {len(matched_ids)}")

    # Build mapping of parent institution -> firms (from Stage 1)
    logger.info("\n[3/4] Building parent-to-firm mapping from Stage 1 matches...")
    parent_to_firms = {}  # parent_id -> list of (GVKEY, firm_conm, confidence)

    for row in stage1_matches.iter_rows(named=True):
        inst_id = row['institution_id']
        # Check if this institution is a parent of others
        if inst_id in inst_dict:
            inst_record = inst_dict[inst_id]
            child_ids = inst_record.get('child_institution_ids') or []
            if len(child_ids) > 0:
                # This is a parent institution, add to mapping
                if inst_id not in parent_to_firms:
                    parent_to_firms[inst_id] = []
                parent_to_firms[inst_id].append({
                    'GVKEY': row['GVKEY'],
                    'LPERMNO': row.get('LPERMNO'),
                    'firm_conm': row['firm_conm'],
                    'confidence': row['match_confidence'],
                    'method': row['match_method'],
                })

    logger.info(f"  Found {len(parent_to_firms)} parent institutions with Stage 1 matches")

    # Cascade matches to children
    logger.info("\n[4/4] Cascading matches to child institutions...")
    all_cascade_matches = []

    for inst_row in has_parent.iter_rows(named=True):
        inst_id = inst_row['institution_id']
        display_name = inst_row['display_name']
        parent_ids = inst_row['parent_institution_ids'] or []
        paper_count = inst_row['paper_count']
        country_code = inst_row.get('geo_country_code', '')

        # Skip if already matched
        if inst_id in matched_ids:
            continue

        # Check each parent
        for parent_id in parent_ids:
            if parent_id in parent_to_firms:
                # Parent matched to firm(s), cascade to this child
                for firm_info in parent_to_firms[parent_id]:
                    all_cascade_matches.append({
                        'GVKEY': firm_info['GVKEY'],
                        'LPERMNO': firm_info['LPERMNO'],
                        'firm_conm': firm_info['firm_conm'],
                        'institution_id': inst_id,
                        'institution_display_name': display_name,
                        'institution_country': country_code,
                        'match_type': 'parent_cascade',
                        'match_confidence': round(firm_info['confidence'] * 0.98, 3),  # Slightly lower than parent
                        'match_method': f"parent_cascade_{firm_info['method']}",
                        'parent_institution_id': parent_id,
                        'institution_paper_count': paper_count,
                    })
                break  # Only use first matched parent

    logger.info(f"  Created {len(all_cascade_matches)} cascade matches")

    # Save results
    if not all_cascade_matches:
        logger.warning("  No cascade matches created!")
        return

    cascade_df = pl.DataFrame(all_cascade_matches)
    cascade_df.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved {len(cascade_df)} matches to {OUTPUT_FILE}")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("PARENT CASCADE MATCHING SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total cascade matches: {len(cascade_df)}")
    logger.info(f"Unique institutions: {cascade_df['institution_id'].n_unique()}")
    logger.info(f"Unique firms: {cascade_df['GVKEY'].n_unique()}")

    # Show examples of cascaded matches
    logger.info("\nExamples of parent-child matches:")
    for i, row in enumerate(cascade_df.head(10).iter_rows(named=True), 1):
        parent_name = inst_dict.get(row['parent_institution_id'], {}).get('display_name', row['parent_institution_id'])
        logger.info(f"  {i}. {row['institution_display_name'][:50]}")
        logger.info(f"     Parent: {parent_name[:50]}")
        logger.info(f"     → {row['firm_conm'][:40]} (Confidence: {row['match_confidence']:.3f})")

    logger.info("\n" + "=" * 80)
    logger.info("PARENT CASCADE MATCHING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
