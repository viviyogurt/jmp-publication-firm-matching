"""
Stage 1: Exact and High-Confidence Patent-Firm Matching

This script implements Stage 1 matching following Arora et al. (2021) methodology:
1. Exact name match
2. Ticker in assignee name
3. Firm name contained in assignee
4. Abbreviation match

Target: >95% accuracy, 3,000-5,000 firms matched
"""

import polars as pl
from pathlib import Path
import logging
import re
from typing import Dict, List, Optional, Tuple

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
PATENT_ASSIGNEES_MASTER = DATA_INTERIM / "patent_assignees_master.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "patent_firm_matches_stage1.parquet"
PROGRESS_LOG = LOGS_DIR / "match_patents_to_firms_stage1.log"

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

# Known abbreviation mappings for top firms
ABBREVIATION_DICT = {
    'IBM': 'INTERNATIONAL BUSINESS MACHINES',
    'AT&T': 'AMERICAN TELEPHONE TELEGRAPH',
    'ATT': 'AMERICAN TELEPHONE TELEGRAPH',
    'GE': 'GENERAL ELECTRIC',
    'GM': 'GENERAL MOTORS',
    'HP': 'HEWLETT PACKARD',
    'HPE': 'HEWLETT PACKARD ENTERPRISE',
    'JPM': 'JPMORGAN CHASE',
    'JPMORGAN': 'JPMORGAN CHASE',
    'BA': 'BOEING',
    'CAT': 'CATERPILLAR',
    'XOM': 'EXXON MOBIL',
    'CVX': 'CHEVRON',
    'WMT': 'WALMART',
    'PG': 'PROCTER GAMBLE',
    'KO': 'COCA COLA',
    'PEP': 'PEPSICO',
    'DIS': 'WALT DISNEY',
    'NFLX': 'NETFLIX',
    'TSLA': 'TESLA',
    'META': 'FACEBOOK',
    'GOOGL': 'GOOGLE',
    'GOOG': 'GOOGLE',
    'AMZN': 'AMAZON',
    'MSFT': 'MICROSOFT',
    'AAPL': 'APPLE',
    'NVDA': 'NVIDIA',
    'INTC': 'INTEL',
    'AMD': 'ADVANCED MICRO DEVICES',
    'QCOM': 'QUALCOMM',
    'TXN': 'TEXAS INSTRUMENTS',
    'AVGO': 'BROADCOM',
    'CSCO': 'CISCO',
    'ORCL': 'ORACLE',
    'CRM': 'SALESFORCE',
    'ADBE': 'ADOBE',
    'NOW': 'SERVICENOW',
    'PANW': 'PALO ALTO NETWORKS',
}


def exact_name_match(assignee_clean: str, firm_conm_clean: Optional[str], 
                    firm_conml_clean: Optional[str]) -> Tuple[bool, float, str]:
    """
    Strategy 1: Exact name match.
    Returns: (is_match, confidence, match_method)
    """
    if not assignee_clean or not firm_conm_clean:
        return False, 0.0, ""
    
    if assignee_clean == firm_conm_clean:
        return True, 0.98, "exact_conm"
    
    if firm_conml_clean and assignee_clean == firm_conml_clean:
        return True, 0.98, "exact_conml"
    
    return False, 0.0, ""


def ticker_in_assignee(assignee_clean: str, firm_tic: Optional[str]) -> Tuple[bool, float, str]:
    """
    Strategy 2: Ticker in assignee name (e.g., "Microsoft (MSFT) Research").
    Returns: (is_match, confidence, match_method)
    """
    if not assignee_clean or not firm_tic or not firm_tic.strip():
        return False, 0.0, ""
    
    ticker = firm_tic.strip().upper()
    
    # Check if ticker appears in parentheses or as standalone word
    patterns = [
        rf'\b\({ticker}\)\b',  # (MSFT)
        rf'\b{ticker}\b',  # MSFT as word
    ]
    
    for pattern in patterns:
        if re.search(pattern, assignee_clean, re.IGNORECASE):
            return True, 0.97, "ticker_in_name"
    
    return False, 0.0, ""


def firm_name_contained(assignee_clean: str, firm_conm_clean: Optional[str],
                       firm_conml_clean: Optional[str]) -> Tuple[bool, float, str]:
    """
    Strategy 3: Firm name contained in assignee (subsidiary/division).
    Example: "Microsoft Research Asia" contains "Microsoft"
    Returns: (is_match, confidence, match_method)
    """
    if not assignee_clean:
        return False, 0.0, ""
    
    # Check if firm name is a substantial substring of assignee
    # Require at least 5 characters to avoid false matches
    if firm_conm_clean and len(firm_conm_clean) >= 5:
        if firm_conm_clean in assignee_clean:
            # Check it's not just a common word
            if len(firm_conm_clean) >= 8 or firm_conm_clean not in ['COMPANY', 'CORPORATION', 'INCORPORATED']:
                return True, 0.96, "firm_contained_conm"
    
    if firm_conml_clean and len(firm_conml_clean) >= 5:
        if firm_conml_clean in assignee_clean:
            if len(firm_conml_clean) >= 8 or firm_conml_clean not in ['COMPANY', 'CORPORATION', 'INCORPORATED']:
                return True, 0.96, "firm_contained_conml"
    
    return False, 0.0, ""


def abbreviation_match(assignee_clean: str, firm_conm_clean: Optional[str],
                      firm_tic: Optional[str]) -> Tuple[bool, float, str]:
    """
    Strategy 4: Abbreviation match.
    Returns: (is_match, confidence, match_method)
    """
    if not assignee_clean or not firm_conm_clean:
        return False, 0.0, ""
    
    # Check if assignee is an abbreviation of firm name
    # Extract first letters of firm name words
    firm_words = firm_conm_clean.split()
    if len(firm_words) >= 2:
        abbrev = ''.join([w[0] for w in firm_words if w and len(w) > 0])
        if len(abbrev) >= 2 and abbrev == assignee_clean:
            return True, 0.95, "abbreviation_generated"
    
    # Check known abbreviation dictionary
    for abbrev, full_name in ABBREVIATION_DICT.items():
        if assignee_clean == abbrev and firm_conm_clean == full_name:
            return True, 0.95, "abbreviation_dict"
        if assignee_clean == full_name and firm_conm_clean == abbrev:
            return True, 0.95, "abbreviation_dict_reverse"
    
    # Check if ticker matches assignee
    if firm_tic and firm_tic.strip().upper() == assignee_clean:
        return True, 0.95, "ticker_match"
    
    return False, 0.0, ""


def create_assignee_lookup(assignees_df: pl.DataFrame) -> Dict[str, List[Dict]]:
    """
    Create lookup dictionaries for efficient matching.
    Returns dict mapping clean_name -> list of assignee rows.
    """
    lookup = {}
    for row in assignees_df.iter_rows(named=True):
        clean_name = row.get('clean_name')
        if clean_name:
            if clean_name not in lookup:
                lookup[clean_name] = []
            lookup[clean_name].append(row)
    return lookup


def match_firm_to_assignees(firm_row: Dict, assignee_lookup: Dict[str, List[Dict]]) -> List[Dict]:
    """
    Match a single firm to assignees using all Stage 1 strategies.
    Uses lookup dictionary for efficiency.
    Returns list of matches (may be empty or have multiple matches).
    """
    matches = []
    
    firm_conm_clean = firm_row.get('conm_clean')
    firm_conml_clean = firm_row.get('conml_clean')
    firm_tic = firm_row.get('tic')
    
    if not firm_conm_clean:
        return matches
    
    # Strategy 1: Exact match on conm_clean
    if firm_conm_clean in assignee_lookup:
        for assignee_row in assignee_lookup[firm_conm_clean]:
            matches.append({
                'GVKEY': firm_row['GVKEY'],
                'LPERMNO': firm_row.get('LPERMNO'),
                'firm_conm': firm_row.get('conm'),
                'assignee_id': assignee_row['assignee_id'],
                'assignee_clean_name': assignee_row['clean_name'],
                'match_type': 'stage1',
                'match_confidence': 0.98,
                'match_method': 'exact_conm',
                'assignee_patent_count': assignee_row.get('patent_count_total', 0),
            })
    
    # Strategy 1b: Exact match on conml_clean
    if firm_conml_clean and firm_conml_clean in assignee_lookup:
        for assignee_row in assignee_lookup[firm_conml_clean]:
            matches.append({
                'GVKEY': firm_row['GVKEY'],
                'LPERMNO': firm_row.get('LPERMNO'),
                'firm_conm': firm_row.get('conm'),
                'assignee_id': assignee_row['assignee_id'],
                'assignee_clean_name': assignee_row['clean_name'],
                'match_type': 'stage1',
                'match_confidence': 0.98,
                'match_method': 'exact_conml',
                'assignee_patent_count': assignee_row.get('patent_count_total', 0),
            })
    
    # Strategy 2, 3, 4: Check other strategies (only if no exact match)
    if not matches:
        ticker = firm_tic.strip().upper() if firm_tic else None
        
        # Strategy 2: Ticker in assignee - only check assignees that might contain ticker
        if ticker and len(ticker) >= 2:
            for assignee_clean, assignee_rows in assignee_lookup.items():
                if ticker in assignee_clean or f'({ticker})' in assignee_clean:
                    is_match, conf, method = ticker_in_assignee(assignee_clean, firm_tic)
                    if is_match:
                        for assignee_row in assignee_rows:
                            matches.append({
                                'GVKEY': firm_row['GVKEY'],
                                'LPERMNO': firm_row.get('LPERMNO'),
                                'firm_conm': firm_row.get('conm'),
                                'assignee_id': assignee_row['assignee_id'],
                                'assignee_clean_name': assignee_row['clean_name'],
                                'match_type': 'stage1',
                                'match_confidence': conf,
                                'match_method': method,
                                'assignee_patent_count': assignee_row.get('patent_count_total', 0),
                            })
        
        # Strategy 3: Firm name contained - only check assignees that contain firm name
        if firm_conm_clean and len(firm_conm_clean) >= 5:
            for assignee_clean, assignee_rows in assignee_lookup.items():
                if firm_conm_clean in assignee_clean:
                    is_match, conf, method = firm_name_contained(assignee_clean, firm_conm_clean, firm_conml_clean)
                    if is_match:
                        for assignee_row in assignee_rows:
                            matches.append({
                                'GVKEY': firm_row['GVKEY'],
                                'LPERMNO': firm_row.get('LPERMNO'),
                                'firm_conm': firm_row.get('conm'),
                                'assignee_id': assignee_row['assignee_id'],
                                'assignee_clean_name': assignee_row['clean_name'],
                                'match_type': 'stage1',
                                'match_confidence': conf,
                                'match_method': method,
                                'assignee_patent_count': assignee_row.get('patent_count_total', 0),
                            })
        
        # Strategy 4: Abbreviation - check if assignee is abbreviation or vice versa
        if firm_conm_clean:
            # Generate abbreviation from firm name
            firm_words = firm_conm_clean.split()
            if len(firm_words) >= 2:
                abbrev = ''.join([w[0] for w in firm_words if w and len(w) > 0])
                if len(abbrev) >= 2 and abbrev in assignee_lookup:
                    for assignee_row in assignee_lookup[abbrev]:
                        matches.append({
                            'GVKEY': firm_row['GVKEY'],
                            'LPERMNO': firm_row.get('LPERMNO'),
                            'firm_conm': firm_row.get('conm'),
                            'assignee_id': assignee_row['assignee_id'],
                            'assignee_clean_name': assignee_row['clean_name'],
                            'match_type': 'stage1',
                            'match_confidence': 0.95,
                            'match_method': 'abbreviation_generated',
                            'assignee_patent_count': assignee_row.get('patent_count_total', 0),
                        })
            
            # Check abbreviation dictionary
            for abbrev, full_name in ABBREVIATION_DICT.items():
                if firm_conm_clean == full_name and abbrev in assignee_lookup:
                    for assignee_row in assignee_lookup[abbrev]:
                        matches.append({
                            'GVKEY': firm_row['GVKEY'],
                            'LPERMNO': firm_row.get('LPERMNO'),
                            'firm_conm': firm_row.get('conm'),
                            'assignee_id': assignee_row['assignee_id'],
                            'assignee_clean_name': assignee_row['clean_name'],
                            'match_type': 'stage1',
                            'match_confidence': 0.95,
                            'match_method': 'abbreviation_dict',
                            'assignee_patent_count': assignee_row.get('patent_count_total', 0),
                        })
                elif firm_conm_clean == abbrev and full_name in assignee_lookup:
                    for assignee_row in assignee_lookup[full_name]:
                        matches.append({
                            'GVKEY': firm_row['GVKEY'],
                            'LPERMNO': firm_row.get('LPERMNO'),
                            'firm_conm': firm_row.get('conm'),
                            'assignee_id': assignee_row['assignee_id'],
                            'assignee_clean_name': assignee_row['clean_name'],
                            'match_type': 'stage1',
                            'match_confidence': 0.95,
                            'match_method': 'abbreviation_dict_reverse',
                            'assignee_patent_count': assignee_row.get('patent_count_total', 0),
                        })
            
            # Check ticker match
            if ticker and ticker in assignee_lookup:
                for assignee_row in assignee_lookup[ticker]:
                    matches.append({
                        'GVKEY': firm_row['GVKEY'],
                        'LPERMNO': firm_row.get('LPERMNO'),
                        'firm_conm': firm_row.get('conm'),
                        'assignee_id': assignee_row['assignee_id'],
                        'assignee_clean_name': assignee_row['clean_name'],
                        'match_type': 'stage1',
                        'match_confidence': 0.95,
                        'match_method': 'ticker_match',
                        'assignee_patent_count': assignee_row.get('patent_count_total', 0),
                    })
    
    return matches


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("STAGE 1: EXACT AND HIGH-CONFIDENCE PATENT-FIRM MATCHING")
    logger.info("=" * 80)
    
    # Step 1: Load standardized data
    logger.info("\n[1/4] Loading standardized data...")
    
    if not COMPUSTAT_STANDARDIZED.exists():
        raise FileNotFoundError(f"Standardized Compustat file not found: {COMPUSTAT_STANDARDIZED}")
    if not PATENT_ASSIGNEES_MASTER.exists():
        raise FileNotFoundError(f"Patent assignees master file not found: {PATENT_ASSIGNEES_MASTER}")
    
    firms_df = pl.read_parquet(COMPUSTAT_STANDARDIZED)
    assignees_df = pl.read_parquet(PATENT_ASSIGNEES_MASTER)
    
    logger.info(f"  Loaded {len(firms_df):,} firms")
    logger.info(f"  Loaded {len(assignees_df):,} assignees")
    
    # Step 2: Create assignee lookup for efficient matching
    logger.info("\n[2/4] Creating assignee lookup dictionary...")
    assignee_lookup = create_assignee_lookup(assignees_df)
    logger.info(f"  Created lookup for {len(assignee_lookup):,} unique assignee names")
    
    # Step 3: Match firms to assignees
    logger.info("\n[3/4] Matching firms to assignees (Stage 1 strategies)...")
    logger.info("  This may take several minutes...")
    
    all_matches = []
    total_firms = len(firms_df)
    
    for i, firm_row in enumerate(firms_df.iter_rows(named=True)):
        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1:,}/{total_firms:,} firms ({len(all_matches):,} matches so far)...")
        
        matches = match_firm_to_assignees(firm_row, assignee_lookup)
        all_matches.extend(matches)
    
    logger.info(f"  Completed matching. Found {len(all_matches):,} total matches")
    
    # Step 4: Deduplicate and select best match per firm-assignee pair
    logger.info("\n[4/5] Deduplicating matches...")
    
    if not all_matches:
        logger.warning("  No matches found!")
        matches_df = pl.DataFrame()
    else:
        matches_df = pl.DataFrame(all_matches)
        
        # If same firm-assignee matched multiple times, keep highest confidence
        matches_df = (
            matches_df
            .sort(['GVKEY', 'assignee_id', 'match_confidence'], descending=[False, False, True])
            .unique(subset=['GVKEY', 'assignee_id'], keep='first')
        )
        
        logger.info(f"  After deduplication: {len(matches_df):,} unique firm-assignee matches")
        logger.info(f"  Unique firms matched: {matches_df['GVKEY'].n_unique():,}")
        logger.info(f"  Unique assignees matched: {matches_df['assignee_id'].n_unique():,}")
    
    # Step 5: Save output
    logger.info("\n[5/5] Saving Stage 1 matches...")
    logger.info(f"Output: {OUTPUT_FILE}")
    
    if len(matches_df) > 0:
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df):,} matches")
        
        # Summary statistics
        logger.info("\n" + "=" * 80)
        logger.info("STAGE 1 MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df):,}")
        logger.info(f"Unique firms matched: {matches_df['GVKEY'].n_unique():,}")
        logger.info(f"Unique assignees matched: {matches_df['assignee_id'].n_unique():,}")
        
        # Match method distribution
        if 'match_method' in matches_df.columns:
            method_counts = matches_df.group_by('match_method').agg(pl.len().alias('count')).sort('count', descending=True)
            logger.info("\nMatch method distribution:")
            for row in method_counts.iter_rows(named=True):
                logger.info(f"  {row['match_method']}: {row['count']:,} matches")
        
        # Confidence distribution
        logger.info(f"\nConfidence statistics:")
        logger.info(f"  Mean: {matches_df['match_confidence'].mean():.3f}")
        logger.info(f"  Median: {matches_df['match_confidence'].median():.3f}")
        logger.info(f"  Min: {matches_df['match_confidence'].min():.3f}")
        logger.info(f"  Max: {matches_df['match_confidence'].max():.3f}")
    else:
        logger.warning("  No matches to save!")
    
    logger.info("\n" + "=" * 80)
    logger.info("STAGE 1 MATCHING COMPLETE")
    logger.info("=" * 80)
    
    return matches_df


if __name__ == "__main__":
    main()
