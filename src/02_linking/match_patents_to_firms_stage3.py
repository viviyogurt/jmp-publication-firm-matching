"""
Stage 3: Manual Mapping & Edge Cases

This script handles manual mappings for known large firms and edge cases:
- Name changes over time (Google → Alphabet, Facebook → Meta)
- Subsidiaries vs. parent companies
- Joint ventures and partnerships
- AT&T and other complex corporate structures

Target: Near 100% accuracy, additional 500-1,000 firms
"""

import polars as pl
from pathlib import Path
import logging
from typing import Dict, List, Optional

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
STAGE2_MATCHES = DATA_PROCESSED_LINK / "patent_firm_matches_stage2.parquet"
MANUAL_MAPPINGS_FILE = DATA_INTERIM / "manual_patent_firm_mappings.csv"
OUTPUT_FILE = DATA_PROCESSED_LINK / "patent_firm_matches_stage3.parquet"
PROGRESS_LOG = LOGS_DIR / "match_patents_to_firms_stage3.log"

DATA_PROCESSED_LINK.mkdir(parents=True, exist_ok=True)
DATA_INTERIM.mkdir(parents=True, exist_ok=True)
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

# Manual mappings for known large firms
# Format: {assignee_clean_name: [list of GVKEYs or firm identifiers]}
MANUAL_MAPPINGS = {
    # Google / Alphabet
    'GOOGLE': ['GOOGL', 'GOOG'],  # Will match to firms with these tickers
    'ALPHABET': ['GOOGL', 'GOOG'],
    'GOOGLE LLC': ['GOOGL', 'GOOG'],
    
    # Facebook / Meta
    'FACEBOOK': ['META'],
    'META': ['META'],
    'FACEBOOK INC': ['META'],
    
    # AT&T (complex - multiple subsidiaries)
    'ATT': ['T'],  # AT&T Inc
    'AT&T': ['T'],
    'AMERICAN TELEPHONE TELEGRAPH': ['T'],
    'ATT INTELLECTUAL PROPERTY': ['T'],
    'ATT INTELLECTUAL PROPERTY I LP': ['T'],
    'ATT MOBILITY': ['T'],
    'ATT SERVICES': ['T'],
    
    # IBM
    'INTERNATIONAL BUSINESS MACHINES': ['IBM'],
    'IBM': ['IBM'],
    'IBM CORP': ['IBM'],
    
    # Microsoft
    'MICROSOFT': ['MSFT'],
    'MICROSOFT CORP': ['MSFT'],
    'MICROSOFT TECHNOLOGY LICENSING': ['MSFT'],
    'MICROSOFT RESEARCH': ['MSFT'],
    
    # Amazon
    'AMAZON': ['AMZN'],
    'AMAZON COM': ['AMZN'],
    'AMAZON TECHNOLOGIES': ['AMZN'],
    'AMAZON WEB SERVICES': ['AMZN'],
    
    # Apple
    'APPLE': ['AAPL'],
    'APPLE INC': ['AAPL'],
    
    # Intel
    'INTEL': ['INTC'],
    'INTEL CORP': ['INTC'],
    
    # Other tech companies
    'ORACLE': ['ORCL'],
    'CISCO': ['CSCO'],
    'QUALCOMM': ['QCOM'],
    'NVIDIA': ['NVDA'],
    'ADOBE': ['ADBE'],
    'SALESFORCE': ['CRM'],
    'NETFLIX': ['NFLX'],
    'TESLA': ['TSLA'],
    'HP': ['HPQ'],
    'HEWLETT PACKARD': ['HPQ'],
    'HEWLETTPACKARD': ['HPQ'],
    'HEWLETTPACKARD DEVELOPMENT LP': ['HPQ'],
}


def create_manual_mappings_file():
    """
    Create a CSV file template for manual mappings that users can edit.
    """
    if MANUAL_MAPPINGS_FILE.exists():
        logger.info(f"Manual mappings file already exists: {MANUAL_MAPPINGS_FILE}")
        return
    
    logger.info("Creating manual mappings template file...")
    
    # Create template with known mappings
    rows = []
    for assignee_name, tickers in MANUAL_MAPPINGS.items():
        for ticker in tickers:
            rows.append({
                'assignee_clean_name': assignee_name,
                'firm_ticker': ticker,
                'firm_gvkey': '',  # To be filled manually
                'notes': '',  # For manual notes
            })
    
    template_df = pl.DataFrame(rows)
    template_df.write_csv(MANUAL_MAPPINGS_FILE)
    logger.info(f"  Created template with {len(rows):,} mappings")
    logger.info(f"  File: {MANUAL_MAPPINGS_FILE}")
    logger.info("  You can edit this file to add more mappings")


def load_manual_mappings() -> Dict[str, List[str]]:
    """
    Load manual mappings from CSV file if it exists, otherwise use defaults.
    """
    if MANUAL_MAPPINGS_FILE.exists():
        logger.info(f"Loading manual mappings from: {MANUAL_MAPPINGS_FILE}")
        try:
            mappings_df = pl.read_csv(MANUAL_MAPPINGS_FILE)
            mappings = {}
            for row in mappings_df.iter_rows(named=True):
                assignee = row.get('assignee_clean_name', '').upper().strip()
                ticker = row.get('firm_ticker', '').strip()
                if assignee and ticker:
                    if assignee not in mappings:
                        mappings[assignee] = []
                    mappings[assignee].append(ticker)
            logger.info(f"  Loaded {len(mappings):,} manual mappings from file")
            return mappings
        except Exception as e:
            logger.warning(f"  Error loading manual mappings file: {e}")
            logger.info("  Using default manual mappings")
            return MANUAL_MAPPINGS
    else:
        logger.info("Manual mappings file not found - using defaults")
        return MANUAL_MAPPINGS


def match_manual_mappings(firms_df: pl.DataFrame, assignees_df: pl.DataFrame,
                         manual_mappings: Dict[str, List[str]],
                         already_matched_firms: set, already_matched_assignees: set) -> List[Dict]:
    """
    Apply manual mappings to match assignees to firms.
    """
    matches = []
    
    # Create ticker to GVKEY lookup
    ticker_to_gvkey = {}
    for row in firms_df.iter_rows(named=True):
        ticker = row.get('tic', '').strip().upper()
        gvkey = row.get('GVKEY')
        if ticker and gvkey:
            if ticker not in ticker_to_gvkey:
                ticker_to_gvkey[ticker] = []
            ticker_to_gvkey[ticker].append(row)
    
    # Create assignee lookup
    assignee_lookup = {}
    for row in assignees_df.iter_rows(named=True):
        clean_name = row.get('clean_name', '').upper().strip()
        if clean_name and clean_name not in already_matched_assignees:
            if clean_name not in assignee_lookup:
                assignee_lookup[clean_name] = []
            assignee_lookup[clean_name].append(row)
    
    # Apply manual mappings
    for assignee_name, tickers in manual_mappings.items():
        assignee_name_upper = assignee_name.upper().strip()
        
        if assignee_name_upper not in assignee_lookup:
            continue
        
        for ticker in tickers:
            ticker_upper = ticker.upper().strip()
            if ticker_upper not in ticker_to_gvkey:
                continue
            
            # Match all assignees with this name to all firms with this ticker
            for assignee_row in assignee_lookup[assignee_name_upper]:
                for firm_row in ticker_to_gvkey[ticker_upper]:
                    gvkey = firm_row['GVKEY']
                    if gvkey not in already_matched_firms:
                        matches.append({
                            'GVKEY': gvkey,
                            'LPERMNO': firm_row.get('LPERMNO'),
                            'firm_conm': firm_row.get('conm'),
                            'assignee_id': assignee_row['assignee_id'],
                            'assignee_clean_name': assignee_row['clean_name'],
                            'match_type': 'stage3',
                            'match_confidence': 0.99,
                            'match_method': 'manual_mapping',
                            'assignee_patent_count': assignee_row.get('patent_count_total', 0),
                        })
    
    return matches


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("STAGE 3: MANUAL MAPPING & EDGE CASES")
    logger.info("=" * 80)
    
    # Step 1: Create manual mappings template if needed
    logger.info("\n[1/4] Setting up manual mappings...")
    create_manual_mappings_file()
    manual_mappings = load_manual_mappings()
    logger.info(f"  Using {len(manual_mappings):,} manual mappings")
    
    # Step 2: Load data
    logger.info("\n[2/4] Loading data...")
    
    if not COMPUSTAT_STANDARDIZED.exists():
        raise FileNotFoundError(f"Standardized Compustat file not found: {COMPUSTAT_STANDARDIZED}")
    if not PATENT_ASSIGNEES_MASTER.exists():
        raise FileNotFoundError(f"Patent assignees master file not found: {PATENT_ASSIGNEES_MASTER}")
    
    firms_df = pl.read_parquet(COMPUSTAT_STANDARDIZED)
    assignees_df = pl.read_parquet(PATENT_ASSIGNEES_MASTER)
    
    # Get already matched firms and assignees
    already_matched_firms = set()
    already_matched_assignees = set()
    
    if STAGE1_MATCHES.exists():
        stage1 = pl.read_parquet(STAGE1_MATCHES)
        already_matched_firms.update(stage1['GVKEY'].unique().to_list())
        already_matched_assignees.update(stage1['assignee_clean_name'].unique().to_list())
    
    if STAGE2_MATCHES.exists():
        stage2 = pl.read_parquet(STAGE2_MATCHES)
        already_matched_firms.update(stage2['GVKEY'].unique().to_list())
        already_matched_assignees.update(stage2['assignee_clean_name'].unique().to_list())
    
    logger.info(f"  Loaded {len(firms_df):,} firms")
    logger.info(f"  Loaded {len(assignees_df):,} assignees")
    logger.info(f"  {len(already_matched_firms):,} firms already matched")
    logger.info(f"  {len(already_matched_assignees):,} assignees already matched")
    
    # Step 3: Apply manual mappings
    logger.info("\n[3/4] Applying manual mappings...")
    
    matches = match_manual_mappings(
        firms_df, assignees_df, manual_mappings,
        already_matched_firms, already_matched_assignees
    )
    
    logger.info(f"  Found {len(matches):,} matches from manual mappings")
    
    # Step 4: Save output
    logger.info("\n[4/4] Saving Stage 3 matches...")
    
    if not matches:
        logger.warning("  No matches found!")
        matches_df = pl.DataFrame()
    else:
        matches_df = pl.DataFrame(matches)
        
        # Deduplicate
        matches_df = (
            matches_df
            .sort(['GVKEY', 'assignee_id', 'match_confidence'], descending=[False, False, True])
            .unique(subset=['GVKEY', 'assignee_id'], keep='first')
        )
        
        logger.info(f"  After deduplication: {len(matches_df):,} unique matches")
        logger.info(f"  Unique firms matched: {matches_df['GVKEY'].n_unique():,}")
        logger.info(f"  Unique assignees matched: {matches_df['assignee_id'].n_unique():,}")
        
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved to: {OUTPUT_FILE}")
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("STAGE 3 MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df):,}")
        logger.info(f"Unique firms matched: {matches_df['GVKEY'].n_unique():,}")
        logger.info(f"Unique assignees matched: {matches_df['assignee_id'].n_unique():,}")
        logger.info(f"Mean confidence: {matches_df['match_confidence'].mean():.3f}")
    
    logger.info("\n" + "=" * 80)
    logger.info("STAGE 3 MATCHING COMPLETE")
    logger.info("=" * 80)
    
    return matches_df


if __name__ == "__main__":
    main()
