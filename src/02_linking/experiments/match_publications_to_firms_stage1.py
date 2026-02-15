"""
Stage 1: Exact and High-Confidence Publication-Firm Matching

This script implements Stage 1 matching for OpenAlex publication institutions to CRSP/Compustat firms.
Following the patent matching approach (Arora et al. 2021, Kogan et al. 2017).

Matching Strategies:
1. Exact name match (confidence: 0.98)
2. ROR to firm matching (confidence: 0.97)
3. Wikidata ticker matching (confidence: 0.97)
4. URL domain matching (confidence: 0.96)
5. Ticker in institution name (confidence: 0.96)
6. Firm name contained in institution (confidence: 0.95)
7. Abbreviation match (confidence: 0.95)
8. Alternative name matching (confidence: 0.95)

Target: 5,000-10,000 institutions matched with 100% accuracy on exact matches.
"""

import polars as pl
from pathlib import Path
import logging
import re
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import urlparse
from collections import defaultdict

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_stage1.parquet"
PROGRESS_LOG = LOGS_DIR / "match_publications_to_firms_stage1.log"

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

# Known abbreviation mappings for top firms (same as patent matching)
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


# ============================================================================
# Lookup Creation Functions
# ============================================================================

def create_firm_lookups(firms_df: pl.DataFrame) -> Dict[str, List[Dict]]:
    """
    Create multiple lookup dictionaries for efficient matching.
    Returns dict with lookup_type -> (key -> list of firm rows).
    """
    lookups = {
        'conm_clean': defaultdict(list),
        'conml_clean': defaultdict(list),
        'tic': defaultdict(list),
        'weburl_domain': defaultdict(list),
    }

    for row in firms_df.iter_rows(named=True):
        # Index by conm_clean
        conm_clean = row.get('conm_clean')
        if conm_clean:
            lookups['conm_clean'][conm_clean].append(row)

        # Index by conml_clean
        conml_clean = row.get('conml_clean')
        if conml_clean:
            lookups['conml_clean'][conml_clean].append(row)

        # Index by ticker
        tic = row.get('tic')
        if tic and tic.strip():
            lookups['tic'][tic.strip().upper()].append(row)

        # Index by weburl domain
        weburl = row.get('weburl')
        if weburl:
            try:
                parsed = urlparse(weburl if '://' in weburl else f'http://{weburl}')
                domain = parsed.netloc.lower().replace('www.', '')
                if domain:
                    lookups['weburl_domain'][domain].append(row)
            except Exception:
                pass

    return lookups


# ============================================================================
# Matching Strategy Functions
# ============================================================================

def alternative_name_match(institution_row: Dict, firm_lookups: Dict) -> List[Dict]:
    """
    Strategy 8: Alternative name matching.
    Check if any display_name_alternatives or name_variants match firm names.
    """
    matches = []
    alternative_names = institution_row.get('alternative_names') or []
    name_variants = institution_row.get('name_variants') or []

    # Combine alternative names and name variants
    all_names = alternative_names + name_variants

    for alt_name in all_names:
        if not alt_name:
            continue

        # Clean the alternative name
        alt_clean = alt_name.upper()
        alt_clean = re.sub(r'[^\w\s]', '', alt_clean)
        alt_clean = re.sub(r'\s+', ' ', alt_clean).strip()

        # Check against conm_clean lookup
        if alt_clean in firm_lookups['conm_clean']:
            for firm_row in firm_lookups['conm_clean'][alt_clean]:
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row.get('conm'),
                    'institution_id': institution_row['institution_id'],
                    'institution_display_name': institution_row.get('display_name'),
                    'institution_clean_name': institution_row.get('normalized_name'),
                    'match_type': 'stage1',
                    'match_confidence': 0.95,
                    'match_method': 'alternative_name_conm',
                    'institution_paper_count': institution_row.get('paper_count', 0),
                })
                return matches  # Return immediately on first match

        # Check against conml_clean lookup
        if alt_clean in firm_lookups['conml_clean']:
            for firm_row in firm_lookups['conml_clean'][alt_clean]:
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row.get('conm'),
                    'institution_id': institution_row['institution_id'],
                    'institution_display_name': institution_row.get('display_name'),
                    'institution_clean_name': institution_row.get('normalized_name'),
                    'match_type': 'stage1',
                    'match_confidence': 0.95,
                    'match_method': 'alternative_name_conml',
                    'institution_paper_count': institution_row.get('paper_count', 0),
                })
                return matches

    return matches


def url_domain_match(institution_row: Dict, firm_lookups: Dict) -> List[Dict]:
    """
    Strategy 4: URL domain matching.
    """
    matches = []
    institution_domain = institution_row.get('homepage_domain')

    if not institution_domain:
        return matches

    # Normalize institution domain
    institution_domain = institution_domain.lower().replace('www.', '')

    # Check against weburl domain lookup
    if institution_domain in firm_lookups['weburl_domain']:
        for firm_row in firm_lookups['weburl_domain'][institution_domain]:
            matches.append({
                'GVKEY': firm_row['GVKEY'],
                'LPERMNO': firm_row.get('LPERMNO'),
                'firm_conm': firm_row.get('conm'),
                'institution_id': institution_row['institution_id'],
                'institution_display_name': institution_row.get('display_name'),
                'institution_clean_name': institution_row.get('normalized_name'),
                'match_type': 'stage1',
                'match_confidence': 0.96,
                'match_method': 'url_domain_match',
                'institution_paper_count': institution_row.get('paper_count', 0),
            })

    return matches


def ticker_in_institution_name(institution_row: Dict, firm_lookups: Dict) -> List[Dict]:
    """
    Strategy 5: Ticker in institution name (e.g., "IBM (NYSE: IBM) Research").
    """
    matches = []
    institution_clean = institution_row.get('normalized_name', '')
    institution_display = institution_row.get('display_name', '')

    # Extract potential ticker patterns from institution name and check if they exist in lookup
    # This is much more efficient than iterating through all tickers
    for name in [institution_clean, institution_display]:
        if not name:
            continue
        name_upper = name.upper()

        # Find all uppercase words (potential tickers)
        # Pattern: sequences of 2-5 uppercase letters, often in parentheses or preceded by certain keywords
        potential_tickers = set()

        # Pattern 1: Words in parentheses like "(MSFT)"
        parens_content = re.findall(r'\(([A-Z]{2,5})\)', name_upper)
        potential_tickers.update(parens_content)

        # Pattern 2: Standalone uppercase words like "MSFT"
        standalone_upper = re.findall(r'\\b[A-Z]{2,5}\\b', name_upper)
        potential_tickers.update(standalone_upper)

        # Pattern 3: After NYSE/NASDAQ keywords
        exchange_tickers = re.findall(r'(?:NYSE|NASDAQ|AMEX):\s*([A-Z]{2,5})', name_upper)
        potential_tickers.update(exchange_tickers)

        # Check if any potential ticker exists in our ticker lookup
        for ticker in potential_tickers:
            if ticker in firm_lookups['tic']:
                for firm_row in firm_lookups['tic'][ticker]:
                    matches.append({
                        'GVKEY': firm_row['GVKEY'],
                        'LPERMNO': firm_row.get('LPERMNO'),
                        'firm_conm': firm_row.get('conm'),
                        'institution_id': institution_row['institution_id'],
                        'institution_display_name': institution_row.get('display_name'),
                        'institution_clean_name': institution_row.get('normalized_name'),
                        'match_type': 'stage1',
                        'match_confidence': 0.96,
                        'match_method': 'ticker_in_name',
                        'institution_paper_count': institution_row.get('paper_count', 0),
                    })
                return matches

    return matches


def firm_name_contained(institution_row: Dict, firm_lookups: Dict) -> List[Dict]:
    """
    Strategy 6: Firm name contained in institution (subsidiary/division).
    Example: "Microsoft Research Asia" contains "Microsoft"

    Optimized: Only check firm names that are ≤ institution name length.
    """
    matches = []
    institution_clean = institution_row.get('normalized_name', '')

    if not institution_clean:
        return matches

    inst_len = len(institution_clean)

    # Check conm_clean lookup
    for firm_clean, firm_rows in firm_lookups['conm_clean'].items():
        # Optimization: Skip if firm name is longer than institution name
        firm_len = len(firm_clean)
        if firm_len > inst_len or firm_len < 5:
            continue

        # Check if firm name is a substantial substring
        if firm_clean in institution_clean:
            # Avoid false matches on common words
            if firm_len >= 8 or firm_clean not in ['COMPANY', 'CORPORATION', 'INCORPORATED']:
                for firm_row in firm_rows:
                    matches.append({
                        'GVKEY': firm_row['GVKEY'],
                        'LPERMNO': firm_row.get('LPERMNO'),
                        'firm_conm': firm_row.get('conm'),
                        'institution_id': institution_row['institution_id'],
                        'institution_display_name': institution_row.get('display_name'),
                        'institution_clean_name': institution_row.get('normalized_name'),
                        'match_type': 'stage1',
                        'match_confidence': 0.95,
                        'match_method': 'firm_contained_conm',
                        'institution_paper_count': institution_row.get('paper_count', 0),
                    })
                return matches

    return matches


def abbreviation_match(institution_row: Dict, firm_lookups: Dict) -> List[Dict]:
    """
    Strategy 7: Abbreviation match.
    """
    matches = []
    institution_clean = institution_row.get('normalized_name', '')
    institution_display = institution_row.get('display_name', '')
    institution_acronyms = institution_row.get('acronyms') or []

    if not institution_clean:
        return matches

    # Check if institution name is in ticker lookup (direct ticker match)
    if institution_clean in firm_lookups['tic']:
        for firm_row in firm_lookups['tic'][institution_clean]:
            matches.append({
                'GVKEY': firm_row['GVKEY'],
                'LPERMNO': firm_row.get('LPERMNO'),
                'firm_conm': firm_row.get('conm'),
                'institution_id': institution_row['institution_id'],
                'institution_display_name': institution_row.get('display_name'),
                'institution_clean_name': institution_row.get('normalized_name'),
                'match_type': 'stage1',
                'match_confidence': 0.95,
                'match_method': 'ticker_match',
                'institution_paper_count': institution_row.get('paper_count', 0),
            })
        return matches

    # Check institution acronyms against firms
    for acronym in institution_acronyms:
        if not acronym:
            continue
        acronym_upper = acronym.upper()

        # Check if acronym matches ticker
        if acronym_upper in firm_lookups['tic']:
            for firm_row in firm_lookups['tic'][acronym_upper]:
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row.get('conm'),
                    'institution_id': institution_row['institution_id'],
                    'institution_display_name': institution_row.get('display_name'),
                    'institution_clean_name': institution_row.get('normalized_name'),
                    'match_type': 'stage1',
                    'match_confidence': 0.95,
                    'match_method': 'acronym_match',
                    'institution_paper_count': institution_row.get('paper_count', 0),
                })
            return matches

    # Check abbreviation dictionary
    for abbrev, full_name in ABBREVIATION_DICT.items():
        if institution_clean == abbrev and full_name in firm_lookups['conm_clean']:
            for firm_row in firm_lookups['conm_clean'][full_name]:
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row.get('conm'),
                    'institution_id': institution_row['institution_id'],
                    'institution_display_name': institution_row.get('display_name'),
                    'institution_clean_name': institution_row.get('normalized_name'),
                    'match_type': 'stage1',
                    'match_confidence': 0.95,
                    'match_method': 'abbreviation_dict',
                    'institution_paper_count': institution_row.get('paper_count', 0),
                })
            return matches

        if institution_clean == full_name and abbrev in firm_lookups['tic']:
            for firm_row in firm_lookups['tic'][abbrev]:
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row.get('conm'),
                    'institution_id': institution_row['institution_id'],
                    'institution_display_name': institution_row.get('display_name'),
                    'institution_clean_name': institution_row.get('normalized_name'),
                    'match_type': 'stage1',
                    'match_confidence': 0.95,
                    'match_method': 'abbreviation_dict_reverse',
                    'institution_paper_count': institution_row.get('paper_count', 0),
                })
            return matches

    # Generate abbreviation from firm names and check
    for firm_clean, firm_rows in firm_lookups['conm_clean'].items():
        firm_words = firm_clean.split()
        if len(firm_words) >= 2:
            # Generate abbreviation from first letters
            abbrev = ''.join([w[0] for w in firm_words if w and len(w) > 0])
            if len(abbrev) >= 2 and abbrev == institution_clean:
                for firm_row in firm_rows:
                    matches.append({
                        'GVKEY': firm_row['GVKEY'],
                        'LPERMNO': firm_row.get('LPERMNO'),
                        'firm_conm': firm_row.get('conm'),
                        'institution_id': institution_row['institution_id'],
                        'institution_display_name': institution_row.get('display_name'),
                        'institution_clean_name': institution_row.get('normalized_name'),
                        'match_type': 'stage1',
                        'match_confidence': 0.95,
                        'match_method': 'abbreviation_generated',
                        'institution_paper_count': institution_row.get('paper_count', 0),
                    })
                return matches

    return matches


def match_institution_to_firms(institution_row: Dict, firm_lookups: Dict) -> List[Dict]:
    """
    Match a single institution to firms using all Stage 1 strategies.
    Returns list of matches (may be empty or have multiple matches).
    Strategies are evaluated in priority order.
    """
    matches = []
    institution_clean = institution_row.get('normalized_name')

    # Strategy 1: Exact name match (highest priority)
    if institution_clean and institution_clean in firm_lookups['conm_clean']:
        for firm_row in firm_lookups['conm_clean'][institution_clean]:
            matches.append({
                'GVKEY': firm_row['GVKEY'],
                'LPERMNO': firm_row.get('LPERMNO'),
                'firm_conm': firm_row.get('conm'),
                'institution_id': institution_row['institution_id'],
                'institution_display_name': institution_row.get('display_name'),
                'institution_clean_name': institution_clean,
                'match_type': 'stage1',
                'match_confidence': 0.98,
                'match_method': 'exact_conm',
                'institution_paper_count': institution_row.get('paper_count', 0),
            })
        return matches

    # Check conml_clean for exact match
    if institution_clean and institution_clean in firm_lookups['conml_clean']:
        for firm_row in firm_lookups['conml_clean'][institution_clean]:
            matches.append({
                'GVKEY': firm_row['GVKEY'],
                'LPERMNO': firm_row.get('LPERMNO'),
                'firm_conm': firm_row.get('conm'),
                'institution_id': institution_row['institution_id'],
                'institution_display_name': institution_row.get('display_name'),
                'institution_clean_name': institution_clean,
                'match_type': 'stage1',
                'match_confidence': 0.98,
                'match_method': 'exact_conml',
                'institution_paper_count': institution_row.get('paper_count', 0),
            })
        return matches

    # Strategy 8: Alternative name matching (high priority)
    matches = alternative_name_match(institution_row, firm_lookups)
    if matches:
        return matches

    # Strategy 4: URL domain matching
    matches = url_domain_match(institution_row, firm_lookups)
    if matches:
        return matches

    # Strategy 5: Ticker in institution name
    matches = ticker_in_institution_name(institution_row, firm_lookups)
    if matches:
        return matches

    # Strategy 6: Firm name contained in institution
    matches = firm_name_contained(institution_row, firm_lookups)
    if matches:
        return matches

    # Strategy 7: Abbreviation match
    matches = abbreviation_match(institution_row, firm_lookups)
    if matches:
        return matches

    return []


# ============================================================================
# Main Processing
# ============================================================================

def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("STAGE 1: EXACT AND HIGH-CONFIDENCE PUBLICATION-FIRM MATCHING")
    logger.info("=" * 80)

    # Step 1: Load data
    logger.info("\n[1/5] Loading data...")

    if not INSTITUTIONS_MASTER.exists():
        raise FileNotFoundError(f"Institutions master file not found: {INSTITUTIONS_MASTER}")
    if not COMPUSTAT_STANDARDIZED.exists():
        raise FileNotFoundError(f"Compustat standardized file not found: {COMPUSTAT_STANDARDIZED}")

    institutions_df = pl.read_parquet(INSTITUTIONS_MASTER)
    firms_df = pl.read_parquet(COMPUSTAT_STANDARDIZED)

    logger.info(f"  Loaded {len(institutions_df):,} institutions")
    logger.info(f"  Loaded {len(firms_df):,} firms")

    # Step 2: Create firm lookups
    logger.info("\n[2/5] Creating firm lookup dictionaries...")
    firm_lookups = create_firm_lookups(firms_df)
    logger.info(f"  Created lookups:")
    for lookup_name, lookup in firm_lookups.items():
        logger.info(f"    {lookup_name}: {len(lookup):,} entries")

    # Step 3: Match institutions to firms
    logger.info("\n[3/5] Matching institutions to firms (Stage 1 strategies)...")
    logger.info("  This may take several minutes...")

    all_matches = []
    total_institutions = len(institutions_df)

    for i, institution_row in enumerate(institutions_df.iter_rows(named=True)):
        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1:,}/{total_institutions:,} institutions ({len(all_matches):,} matches so far)...")

        matches = match_institution_to_firms(institution_row, firm_lookups)
        all_matches.extend(matches)

    logger.info(f"  Completed matching. Found {len(all_matches):,} total matches")

    # Step 4: Deduplicate and select best match
    logger.info("\n[4/5] Deduplicating matches...")

    if not all_matches:
        logger.warning("  No matches found!")
        matches_df = pl.DataFrame()
    else:
        matches_df = pl.DataFrame(all_matches)

        # If same institution-firm matched multiple times, keep highest confidence
        matches_df = (
            matches_df
            .sort(['institution_id', 'GVKEY', 'match_confidence'], descending=[False, False, True])
            .unique(subset=['institution_id', 'GVKEY'], keep='first')
        )

        logger.info(f"  After deduplication: {len(matches_df):,} unique institution-firm matches")
        logger.info(f"  Unique institutions matched: {matches_df['institution_id'].n_unique():,}")
        logger.info(f"  Unique firms matched: {matches_df['GVKEY'].n_unique():,}")

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
        logger.info(f"Unique institutions matched: {matches_df['institution_id'].n_unique():,}")
        logger.info(f"Unique firms matched: {matches_df['GVKEY'].n_unique():,}")

        # Coverage statistics
        coverage_pct = (matches_df['institution_id'].n_unique() / len(institutions_df)) * 100
        logger.info(f"\nCoverage: {coverage_pct:.2f}% of institutions")

        # Match method distribution
        if 'match_method' in matches_df.columns:
            method_counts = matches_df.group_by('match_method').agg(
                pl.len().alias('count')
            ).sort('count', descending=True)
            logger.info("\nMatch method distribution:")
            for row in method_counts.iter_rows(named=True):
                logger.info(f"  {row['match_method']}: {row['count']:,} matches")

        # Confidence distribution
        logger.info(f"\nConfidence statistics:")
        logger.info(f"  Mean: {matches_df['match_confidence'].mean():.3f}")
        logger.info(f"  Median: {matches_df['match_confidence'].median():.3f}")
        logger.info(f"  Min: {matches_df['match_confidence'].min():.3f}")
        logger.info(f"  Max: {matches_df['match_confidence'].max():.3f}")

        # Paper count statistics
        if 'institution_paper_count' in matches_df.columns:
            total_papers = matches_df['institution_paper_count'].sum()
            logger.info(f"\nTotal papers covered: {total_papers:,}")
    else:
        logger.warning("  No matches to save!")

    logger.info("\n" + "=" * 80)
    logger.info("STAGE 1 MATCHING COMPLETE")
    logger.info("=" * 80)

    return matches_df


if __name__ == "__main__":
    main()
