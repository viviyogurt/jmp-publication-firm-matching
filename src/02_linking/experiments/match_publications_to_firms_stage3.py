"""
Stage 3: Manual Mapping and Edge Cases for Publication-Firm Linking

This script handles known edge cases that automatic matching cannot handle:
1. Name changes (Facebook → Meta)
2. Parent-subsidiary relationships (Google AI → Alphabet)
3. Joint ventures
4. Top firms manual mappings

Target: 100-500 additional high-confidence matches for edge cases.
"""

import polars as pl
from pathlib import Path
import logging
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
STAGE1_MATCHES = DATA_PROCESSED_LINK / "publication_firm_matches_stage1.parquet"
STAGE2_MATCHES = DATA_PROCESSED_LINK / "publication_firm_matches_stage2.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_stage3.parquet"
MANUAL_MAPPINGS_FILE = DATA_INTERIM / "manual_publication_firm_mappings.csv"
PROGRESS_LOG = LOGS_DIR / "match_publications_to_firms_stage3.log"

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

MANUAL_MAPPINGS = {
    "FACEBOOK": "META",
    "META PLATFORMS": "META",
    # Google/Alphabet - match all Google variants to GOOGL ticker
    "GOOGLE": "GOOGL",
    "GOOGLE AI": "GOOGL",
    "GOOGLE RESEARCH": "GOOGL",
    "DEEPMIND": "GOOGL",
    "GOOGLE DEEPMIND": "GOOGL",
    "ALPHABET": "GOOGL",
    "ALPHABET INC": "GOOGL",
    # Other tech firms
    "MICROSOFT RESEARCH": "MSFT",
    "AMAZON WEB SERVICES": "AMZN",
    "AMAZON AI": "AMZN",
    "TESLA MOTORS": "TSLA",
    "NVIDIA RESEARCH": "NVDA",
    "INTEL LABS": "INTC",
    "IBM RESEARCH": "IBM",
    "APPLE INC": "AAPL",
}


def create_firm_lookup(firms_df: pl.DataFrame) -> Dict[str, Dict]:
    lookups = {'tic': {}, 'conm_clean': {}}
    for row in firms_df.iter_rows(named=True):
        if row.get('tic'):
            lookups['tic'][row['tic'].upper()] = row
        if row.get('conm_clean'):
            lookups['conm_clean'][row['conm_clean']] = row
    return lookups


def apply_manual_mapping(institution_row: Dict, firm_lookup: Dict) -> List[Dict]:
    matches = []
    inst_clean = institution_row.get('normalized_name', '')
    inst_display = institution_row.get('display_name', '')

    for key, value in MANUAL_MAPPINGS.items():
        key_upper = key.upper()
        if key_upper in inst_clean or key_upper in inst_display.upper():
            if value in firm_lookup.get('tic', {}):
                firm_row = firm_lookup['tic'][value]
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row.get('conm'),
                    'institution_id': institution_row['institution_id'],
                    'institution_display_name': institution_row.get('display_name'),
                    'institution_clean_name': inst_clean,
                    'match_type': 'stage3',
                    'match_confidence': 0.99,
                    'match_method': 'manual_mapping',
                    'manual_rule': key,
                    'institution_paper_count': institution_row.get('paper_count', 0),
                })
                return matches
            if value in firm_lookup.get('conm_clean', {}):
                firm_row = firm_lookup['conm_clean'][value]
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row.get('conm'),
                    'institution_id': institution_row['institution_id'],
                    'institution_display_name': institution_row.get('display_name'),
                    'institution_clean_name': inst_clean,
                    'match_type': 'stage3',
                    'match_confidence': 0.99,
                    'match_method': 'manual_mapping',
                    'manual_rule': key,
                    'institution_paper_count': institution_row.get('paper_count', 0),
                })
                return matches
    return matches


def main():
    logger.info("=" * 80)
    logger.info("STAGE 3: MANUAL MAPPING AND EDGE CASES")
    logger.info("=" * 80)

    logger.info("\n[1/4] Loading data...")
    institutions_df = pl.read_parquet(INSTITUTIONS_MASTER)
    firms_df = pl.read_parquet(COMPUSTAT_STANDARDIZED)
    logger.info(f"  Loaded {len(institutions_df):,} institutions")
    logger.info(f"  Loaded {len(firms_df):,} firms")

    matched_ids = set()
    if STAGE1_MATCHES.exists():
        stage1 = pl.read_parquet(STAGE1_MATCHES)
        matched_ids.update(stage1['institution_id'].to_list())
        logger.info(f"  Stage 1 matches: {len(stage1):,}")

    if STAGE2_MATCHES.exists():
        stage2 = pl.read_parquet(STAGE2_MATCHES)
        matched_ids.update(stage2['institution_id'].to_list())
        logger.info(f"  Stage 2 matches: {len(stage2):,}")

    unmatched = institutions_df.filter(~pl.col('institution_id').is_in(matched_ids))
    logger.info(f"  Institutions to match in Stage 3: {len(unmatched):,}")

    logger.info("\n[2/4] Creating firm lookups...")
    firm_lookup = create_firm_lookup(firms_df)

    logger.info("\n[3/4] Applying manual mappings...")
    all_matches = []
    for institution_row in unmatched.iter_rows(named=True):
        matches = apply_manual_mapping(institution_row, firm_lookup)
        all_matches.extend(matches)
    logger.info(f"  Found {len(all_matches):,} manual matches")

    logger.info("\n[4/4] Saving Stage 3 matches...")
    if len(all_matches) > 0:
        matches_df = pl.DataFrame(all_matches)
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df):,} matches to {OUTPUT_FILE}")

        logger.info("\n" + "=" * 80)
        logger.info("STAGE 3 MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df):,}")
        logger.info(f"Unique institutions: {matches_df['institution_id'].n_unique():,}")

        if 'institution_paper_count' in matches_df.columns:
            total_papers = matches_df['institution_paper_count'].sum()
            logger.info(f"Total papers covered: {total_papers:,}")
    else:
        logger.warning("  No matches found!")

    mappings_export = pl.DataFrame({
        'rule_key': list(MANUAL_MAPPINGS.keys()),
        'target_firm': list(MANUAL_MAPPINGS.values()),
    })
    mappings_export.write_csv(MANUAL_MAPPINGS_FILE)
    logger.info(f"\nExported {len(MANUAL_MAPPINGS)} mapping rules to {MANUAL_MAPPINGS_FILE}")

    logger.info("\n" + "=" * 80)
    logger.info("STAGE 3 MATCHING COMPLETE")
    logger.info("=" * 80)

    return all_matches


if __name__ == "__main__":
    main()
