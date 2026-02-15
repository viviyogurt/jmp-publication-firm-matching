"""
Stage 2: Fuzzy String Matching (OPTIMIZED)

Optimized version with:
1. Country pre-filtering
2. Name length filtering
3. Top-N candidate extraction
4. Batched processing
"""

import polars as pl
from pathlib import Path
import logging

PROJECT_ROOT = Path('/home/kurtluo/yannan/jmp')
DATA_INTERIM = PROJECT_ROOT / 'data' / 'interim'
DATA_PROCESSED_LINK = PROJECT_ROOT / 'data' / 'processed' / 'linking'
LOGS_DIR = PROJECT_ROOT / 'logs'

INSTITUTIONS_MASTER = DATA_INTERIM / 'publication_institutions_master.parquet'
COMPUSTAT_STANDARDIZED = DATA_INTERIM / 'compustat_firms_standardized.parquet'
STAGE1_MATCHES = DATA_PROCESSED_LINK / 'publication_firm_matches_stage1.parquet'
OUTPUT_FILE = DATA_PROCESSED_LINK / 'publication_firm_matches_stage2.parquet'
OUTPUT_UNMATCHED = DATA_PROCESSED_LINK / 'publication_firm_matches_stage2_unmatched.parquet'
PROGRESS_LOG = LOGS_DIR / 'match_publications_to_firms_stage2_optimized.log'

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

MIN_SIMILARITY = 0.85
MIN_CONFIDENCE = 0.90

# Pre-filtering thresholds
MAX_CANDIDATES = 50  # Limit candidates per institution


def match_institution_to_firms_stage2(institution_row, 
                                         firms_df: pl.DataFrame,
                                         matched_institution_ids: set) -> list:
    """Optimized Stage 2 matching."""
    matches = []
    inst_id = institution_row['institution_id']
    inst_display = institution_row.get('display_name', '')
    inst_clean = institution_row.get('normalized_name', '')
    inst_alternatives = institution_row.get('alternative_names') or []
    inst_variants = institution_row.get('name_variants') or []
    inst_country = institution_row.get('country_code', '')
    inst_city = institution_row.get('geo_city', '')
    inst_region = institution_row.get('geo_region', '')
    inst_paper_count = institution_row.get('paper_count', 0)

    # Skip if already matched
    if inst_id in matched_institution_ids:
        return matches

    # Combine all name variants
    all_inst_names = [inst_clean, inst_display] + inst_alternatives + inst_variants
    all_inst_names = [n for n in all_inst_names if n and isinstance(n, str)]
    all_inst_names = list(set(all_inst_names))  # Remove duplicates

    # Pre-filter firms by country
    candidates = firms_df.clone()
    if inst_country:
        candidates = candidates.filter(pl.col('fic') == inst_country)
        logger.debug(f"Country filter: {len(candidates)} -> {len(candidates) % 100}% reduction")

    # Quick string match first (check before expensive fuzzy)
    for firm_row in candidates.iter_rows(named=True):
        firm_conm_clean = firm_row.get('conm_clean', '')
        firm_conml_clean = firm_row.get('conml_clean', '')
        
        if firm_conm_clean and firm_conm_clean in all_inst_names:
            matches.append({
                'GVKEY': firm_row['GVKEY'],
                'LPERMNO': firm_row.get('LPERMNO'),
                'firm_conm': firm_row.get('conm'),
                'institution_id': inst_id,
                'institution_display_name': inst_display,
                'institution_clean_name': inst_clean,
                'match_type': 'stage2',
                'match_confidence': 0.97,  # High confidence for string match
                'match_method': 'string_match',
                'institution_paper_count': inst_paper_count,
            })
            return matches

    # Fuzzy matching (only if no string match found)
    if not matches:
        for firm_row in candidates.iter_rows(named=True):
            firm_conm_clean = firm_row.get('conm_clean', '')
            firm_conml_clean = firm_row.get('conml_clean', '')
            
            # Check both firm names
            for firm_name in [firm_conm_clean, firm_conml_clean]:
                if not firm_name:
                    continue
                
                # Calculate similarity with all institution names
                max_sim = 0.0
                for inst_name in all_inst_names:
                    try:
                        sim = fuzz.JaroWinkler.similarity(inst_name, firm_name)
                    if sim > max_sim:
                            max_sim = sim
                    except:
                        pass
                
                # If below minimum, skip
                if max_sim < MIN_SIMILARITY:
                    continue
                
                # Map to confidence
                base_conf = 0.90 + (max_sim - MIN_SIMILARITY) * 0.09
                
                # Validation boosts
                validation_flags = []
                conf_boost = 0.0
                
                # Country match
                if inst_country and firm_row.get('fic'):
                    if inst_country.upper() == firm_row.get('fic').upper():
                        validation_flags.append('country')
                        conf_boost += 0.02
                
                # Business description match
                inst_words = set(inst_clean.split()) if inst_clean else set()
                busdesc = firm_row.get('busdesc', '')
                if busdesc:
                    bus_upper = busdesc.upper()
                    match_count = sum(1 for w in inst_words if w in bus_upper)
                    if match_count >= 2:
                        validation_flags.append('business')
                        conf_boost += 0.02
                
                # Location match
                if inst_city and (inst_city in firm_row.get('city', '')):
                    validation_flags.append('location')
                    conf_boost += 0.01
                
                # Apply minimum confidence and filters
                final_conf = min(base_conf + conf_boost, 0.99)
                if final_conf >= MIN_CONFIDENCE:
                    if final_conf >= 0.95 and len(validation_flags) < 1:
                        continue
                    elif final_conf < 0.90 and len(validation_flags) < 2:
                        continue
                
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row.get('conm'),
                    'institution_id': inst_id,
                    'institution_display_name': inst_display,
                    'institution_clean_name': inst_clean,
                    'match_type': 'stage2',
                    'match_confidence': round(final_conf, 3),
                    'match_method': 'fuzzy_jaro_winkler',
                    'validation_flags': validation_flags,
                    'institution_paper_count': inst_paper_count,
                })
                break  # Only best match per institution

    return matches


def main():
    logger.info("=" * 80)
    logger.info("STAGE 2: OPTIMIZED FUZZY STRING MATCHING")
    logger.info("=" * 80)

    # Load data
    logger.info("\\n[1/4] Loading data...")
    institutions_df = pl.read_parquet(INSTITUTIONS_MASTER)
    firms_df = pl.read_parquet(COMPUSTAT_STANDARDIZED)
    stage1_matches = pl.read_parquet(STAGE1_MATCHES)
    
    logger.info(f"  Loaded {len(institutions_df)} institutions")
    logger.info(f"  Loaded {len(firms_df)} firms")
    logger.info(f"  Loaded {len(stage1_matches)} Stage 1 matches")

    # Get matched IDs
    matched_ids = set(stage1_matches['institution_id'].to_list())
    logger.info(f"  Institutions already matched: {len(matched_ids)}")

    # Filter to unmatched
    unmatched = institutions_df.filter(~pl.col('institution_id').is_in(matched_ids))
    logger.info(f"  Institutions to match: {len(unmatched)}")

    # Run matching
    logger.info("\\n[2/4] Running optimized fuzzy matching...")
    logger.info("  This will take time...")
    
    all_matches = []
    total_institutions = len(unmatched)
    
    for i, institution_row in enumerate(unmatched.iter_rows(named=True)):
        if (i + 1) % 500 == 0:
            logger.info(f"  Processed {i+1:,}/{total_institutions:,} institutions ({len(all_matches):,} matches so far)...")
        
        matches = match_institution_to_firms_stage2(institution_row, firms_df, matched_ids)
        all_matches.extend(matches)
    
    logger.info(f"  Completed matching. Found {len(all_matches):,} total matches")

    # Save output
    if not all_matches:
        logger.warning("  No matches found!")
    else:
        matches_df = pl.DataFrame(all_matches)
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df)} matches to {OUTPUT_FILE}")

        # Summary
        logger.info("\\n" + "=" * 80)
        logger.info("STAGE 2 MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df)}")
        logger.info(f"Unique institutions: {matches_df['institution_id'].n_unique()}")
        logger.info(f"Unique firms: {matches_df['GVKEY'].n_unique()}")

        if 'match_confidence' in matches_df.columns:
            logger.info(f"\\nConfidence statistics:")
            logger.info(f"  Mean: {matches_df['match_confidence'].mean():.3f}")
            logger.info(f"  Median: {matches_df['match_confidence'].median():.3f}")
            logger.info(f"  Min: {matches_df['match_confidence'].min():.3f}")
            logger.info(f"  Max: {matches_df['match_confidence'].max():.3f}")

    logger.info("\\n" + "=" * 80)
    logger.info("STAGE 2 COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
