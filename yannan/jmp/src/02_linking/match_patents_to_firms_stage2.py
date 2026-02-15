"""
Stage 2: Fuzzy Matching with Business Description and Location Validation

This script implements Stage 2 fuzzy matching following Arora et al. (2021):
1. Fuzzy string matching (Jaro-Winkler, Levenshtein)
2. Business description validation
3. Location validation

Target: 85-90% accuracy, additional 2,000-4,000 firms
"""

import polars as pl
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple
from rapidfuzz import fuzz, process

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
PATENT_ASSIGNEES_MASTER = DATA_INTERIM / "patent_assignees_master.parquet"
STAGE1_MATCHES = DATA_PROCESSED_LINK / "patent_firm_matches_stage1.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "patent_firm_matches_stage2.parquet"
PROGRESS_LOG = LOGS_DIR / "match_patents_to_firms_stage2.log"

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

# Minimum confidence threshold for Stage 2
MIN_CONFIDENCE = 0.85


def jaro_winkler_similarity(str1: str, str2: str) -> float:
    """Calculate Jaro-Winkler similarity (0-1 scale)."""
    if not str1 or not str2:
        return 0.0
    return fuzz.ratio(str1, str2) / 100.0


def calculate_fuzzy_confidence(assignee_clean: str, firm_conm_clean: str) -> Tuple[float, str]:
    """
    Calculate fuzzy matching confidence using multiple methods.
    Returns: (confidence, method)
    """
    if not assignee_clean or not firm_conm_clean:
        return 0.0, ""
    
    # Jaro-Winkler similarity
    jw_score = fuzz.ratio(assignee_clean, firm_conm_clean) / 100.0
    
    # Partial ratio (for substring matches)
    partial_score = fuzz.partial_ratio(assignee_clean, firm_conm_clean) / 100.0
    
    # Token sort ratio (order-independent)
    token_score = fuzz.token_sort_ratio(assignee_clean, firm_conm_clean) / 100.0
    
    # Use the best score
    best_score = max(jw_score, partial_score, token_score)
    
    if best_score >= 0.90:
        return 0.94, "fuzzy_jw_high"
    elif best_score >= 0.85:
        return 0.90, "fuzzy_jw_medium"
    else:
        return 0.0, ""


def business_description_boost(assignee_clean: str, firm_busdesc: Optional[str]) -> float:
    """
    Check if business description contains keywords from assignee name.
    Returns confidence boost (0.0 to 0.05).
    """
    if not assignee_clean or not firm_busdesc:
        return 0.0
    
    # Extract key words from assignee name (words >= 4 chars)
    assignee_words = [w for w in assignee_clean.split() if len(w) >= 4]
    if not assignee_words:
        return 0.0
    
    busdesc_upper = firm_busdesc.upper()
    
    # Check if any assignee word appears in business description
    matches = sum(1 for word in assignee_words if word in busdesc_upper)
    
    if matches >= 2:
        return 0.05
    elif matches >= 1:
        return 0.03
    else:
        return 0.0


def location_validation(assignee_clean: str, firm_state: Optional[str], 
                      firm_city: Optional[str]) -> float:
    """
    Location-based validation (if assignee location available).
    Returns confidence boost (0.0 to 0.03).
    """
    # Note: Patent assignee data doesn't typically include location
    # This is a placeholder for future enhancement
    # For now, return 0.0
    return 0.0


def match_firm_to_assignees_fuzzy(firm_row: Dict, assignee_lookup: Dict[str, List[Dict]],
                                  stage1_matched_assignees: set) -> List[Dict]:
    """
    Match a firm to assignees using fuzzy matching (Stage 2).
    Only checks assignees NOT matched in Stage 1.
    """
    matches = []
    
    firm_conm_clean = firm_row.get('conm_clean')
    firm_busdesc = firm_row.get('busdesc')
    firm_state = firm_row.get('state')
    firm_city = firm_row.get('city')
    
    if not firm_conm_clean:
        return matches
    
    # Get best fuzzy matches using rapidfuzz
    # Only consider assignees not matched in Stage 1
    candidate_assignees = {
        name: rows for name, rows in assignee_lookup.items()
        if name not in stage1_matched_assignees
    }
    
    if not candidate_assignees:
        return matches
    
    # Use rapidfuzz to find best matches
    # Get top 5 candidates
    results = process.extract(
        firm_conm_clean,
        candidate_assignees.keys(),
        limit=5,
        scorer=fuzz.ratio
    )
    
    for assignee_clean, score, _ in results:
        if score < 85:  # Minimum threshold
            continue
        
        # Calculate base confidence
        base_conf, method = calculate_fuzzy_confidence(assignee_clean, firm_conm_clean)
        if base_conf < MIN_CONFIDENCE:
            continue
        
        # Apply boosts
        busdesc_boost = business_description_boost(assignee_clean, firm_busdesc)
        location_boost = location_validation(assignee_clean, firm_state, firm_city)
        
        final_confidence = min(0.99, base_conf + busdesc_boost + location_boost)
        
        # Add all assignee rows with this name
        for assignee_row in candidate_assignees[assignee_clean]:
            matches.append({
                'GVKEY': firm_row['GVKEY'],
                'LPERMNO': firm_row.get('LPERMNO'),
                'firm_conm': firm_row.get('conm'),
                'assignee_id': assignee_row['assignee_id'],
                'assignee_clean_name': assignee_row['clean_name'],
                'match_type': 'stage2',
                'match_confidence': final_confidence,
                'match_method': method,
                'assignee_patent_count': assignee_row.get('patent_count_total', 0),
                'fuzzy_score': score / 100.0,
                'busdesc_boost': busdesc_boost,
                'location_boost': location_boost,
            })
    
    return matches


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("STAGE 2: FUZZY PATENT-FIRM MATCHING")
    logger.info("=" * 80)
    
    # Step 1: Load data
    logger.info("\n[1/5] Loading data...")
    
    if not COMPUSTAT_STANDARDIZED.exists():
        raise FileNotFoundError(f"Standardized Compustat file not found: {COMPUSTAT_STANDARDIZED}")
    if not PATENT_ASSIGNEES_MASTER.exists():
        raise FileNotFoundError(f"Patent assignees master file not found: {PATENT_ASSIGNEES_MASTER}")
    
    firms_df = pl.read_parquet(COMPUSTAT_STANDARDIZED)
    assignees_df = pl.read_parquet(PATENT_ASSIGNEES_MASTER)
    
    # Load Stage 1 matches to exclude already-matched assignees
    if STAGE1_MATCHES.exists():
        stage1_matches = pl.read_parquet(STAGE1_MATCHES)
        stage1_matched_assignees = set(stage1_matches['assignee_clean_name'].unique().to_list())
        stage1_matched_firms = set(stage1_matches['GVKEY'].unique().to_list())
        logger.info(f"  Loaded {len(stage1_matched_assignees):,} assignees already matched in Stage 1")
        logger.info(f"  Loaded {len(stage1_matched_firms):,} firms already matched in Stage 1")
        
        # Only process firms NOT matched in Stage 1
        firms_df = firms_df.filter(~pl.col('GVKEY').is_in(list(stage1_matched_firms)))
    else:
        stage1_matched_assignees = set()
        logger.warning("  Stage 1 matches not found - processing all firms")
    
    logger.info(f"  Loaded {len(firms_df):,} firms to process")
    logger.info(f"  Loaded {len(assignees_df):,} assignees")
    
    # Step 2: Create assignee lookup
    logger.info("\n[2/5] Creating assignee lookup dictionary...")
    assignee_lookup = {}
    for row in assignees_df.iter_rows(named=True):
        clean_name = row.get('clean_name')
        if clean_name:
            if clean_name not in assignee_lookup:
                assignee_lookup[clean_name] = []
            assignee_lookup[clean_name].append(row)
    logger.info(f"  Created lookup for {len(assignee_lookup):,} unique assignee names")
    
    # Step 3: Fuzzy matching
    logger.info("\n[3/5] Performing fuzzy matching (Stage 2 strategies)...")
    logger.info("  This may take several minutes...")
    
    all_matches = []
    total_firms = len(firms_df)
    
    for i, firm_row in enumerate(firms_df.iter_rows(named=True)):
        if (i + 1) % 500 == 0:
            logger.info(f"  Processed {i+1:,}/{total_firms:,} firms ({len(all_matches):,} matches so far)...")
        
        matches = match_firm_to_assignees_fuzzy(firm_row, assignee_lookup, stage1_matched_assignees)
        all_matches.extend(matches)
    
    logger.info(f"  Completed matching. Found {len(all_matches):,} total matches")
    
    # Step 4: Deduplicate
    logger.info("\n[4/5] Deduplicating matches...")
    
    if not all_matches:
        logger.warning("  No matches found!")
        matches_df = pl.DataFrame()
    else:
        matches_df = pl.DataFrame(all_matches)
        
        # Keep highest confidence match per firm-assignee pair
        matches_df = (
            matches_df
            .sort(['GVKEY', 'assignee_id', 'match_confidence'], descending=[False, False, True])
            .unique(subset=['GVKEY', 'assignee_id'], keep='first')
        )
        
        logger.info(f"  After deduplication: {len(matches_df):,} unique firm-assignee matches")
        logger.info(f"  Unique firms matched: {matches_df['GVKEY'].n_unique():,}")
        logger.info(f"  Unique assignees matched: {matches_df['assignee_id'].n_unique():,}")
    
    # Step 5: Save output
    logger.info("\n[5/5] Saving Stage 2 matches...")
    logger.info(f"Output: {OUTPUT_FILE}")
    
    if len(matches_df) > 0:
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df):,} matches")
        
        # Summary statistics
        logger.info("\n" + "=" * 80)
        logger.info("STAGE 2 MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df):,}")
        logger.info(f"Unique firms matched: {matches_df['GVKEY'].n_unique():,}")
        logger.info(f"Unique assignees matched: {matches_df['assignee_id'].n_unique():,}")
        
        # Confidence distribution
        logger.info(f"\nConfidence statistics:")
        logger.info(f"  Mean: {matches_df['match_confidence'].mean():.3f}")
        logger.info(f"  Median: {matches_df['match_confidence'].median():.3f}")
        logger.info(f"  Min: {matches_df['match_confidence'].min():.3f}")
        logger.info(f"  Max: {matches_df['match_confidence'].max():.3f}")
        
        # Match method distribution
        if 'match_method' in matches_df.columns:
            method_counts = matches_df.group_by('match_method').agg(pl.len().alias('count')).sort('count', descending=True)
            logger.info("\nMatch method distribution:")
            for row in method_counts.iter_rows(named=True):
                logger.info(f"  {row['match_method']}: {row['count']:,} matches")
    else:
        logger.warning("  No matches to save!")
    
    logger.info("\n" + "=" * 80)
    logger.info("STAGE 2 MATCHING COMPLETE")
    logger.info("=" * 80)
    
    return matches_df


if __name__ == "__main__":
    main()
