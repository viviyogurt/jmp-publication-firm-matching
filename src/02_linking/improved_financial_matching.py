"""
Improved Firm-Level Financial Matching System

Aggressive matching strategy to maximize firm-level matches.
Target: >95% match rate for institutions to financial firms.

Key improvements:
1. Search ALL firms (not just US)
2. Lower fuzzy threshold (80% instead of 87%)
3. Increase search limit
4. Multiple matching strategies
5. Handle subsidiaries and alternate names
"""

import polars as pl
from pathlib import Path
import logging
import json
import re
from rapidfuzz import fuzz, process
import time

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "compustat"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

FIRM_PAPERS = DATA_PROCESSED / "ai_papers_firms_only.parquet"
FINANCIAL_DATA = DATA_RAW / "crsp_a_ccm.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "institution_to_firm_matches_aggressive.parquet"
PROGRESS_LOG = LOGS_DIR / "improved_financial_matching.log"

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROGRESS_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Name Normalization (Enhanced)
# ============================================================================

SUFFIXES = [
    ' inc', ' corp', ' llc', 'ltd', 'limited', 'plc', 'gmbh', 'ag', 'sa', 'spa',
    'pty', 'bv', 'nv', 'aps', 'co', 'company', 'corporation', 'laboratories',
    ' laboratory', 'research', 'group', 'holdings', 'international', 'industries',
    ' technologies', 'technology', 'systems', 'solutions', 'software', 'services',
    ' incorporated', ' llp', ' partnership', ' enterprises', ' industries'
]

ABBREVIATIONS = {
    'ibm': 'international business machines',
    'ge': 'general electric',
    'gm': 'general motors',
    'hp': 'hewlett packard',
    'at&t': 'at and t',
    '3m': 'three m',
    'faang': 'facebook amazon apple netflix google',
}

# Common subsidiary keywords to remove
SUBSIDIARY_PREFIXES = [
    'subsidiary of', 'division of', 'unit of', 'part of',
    'lab of', 'labs of', 'laboratory of'
]

def normalize_name_aggressive(name: str) -> str:
    """Aggressive name normalization for matching."""
    if not name or name == "":
        return ""

    normalized = name.lower().strip()

    # Remove country in parentheses
    normalized = re.sub(r'\s*\([^)]*\)', '', normalized)

    # Replace abbreviations
    for abbr, expanded in ABBREVIATIONS.items():
        normalized = normalized.replace(abbr, expanded)

    # Remove subsidiary prefixes
    for prefix in SUBSIDIARY_PREFIXES:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):].strip()

    # Remove suffixes
    for suffix in SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()

    # Replace & with 'and'
    normalized = normalized.replace('&', 'and')

    # Remove common words
    common_words = ['the', 'a', 'an', 'corporation', 'incorporated', 'limited']
    words = normalized.split()
    words = [w for w in words if w not in common_words and len(w) > 1]
    normalized = ' '.join(words)

    # Normalize whitespace
    normalized = re.sub(r'\s+', ' ', normalized)

    return normalized.strip()


# ============================================================================
# Load Financial Data
# ============================================================================

def load_financial_data():
    """Load CRSP-CCM data."""
    logger.info("=" * 80)
    logger.info("LOADING FINANCIAL DATA (CRSP-CCM)")
    logger.info("=" * 80)

    logger.info(f"\nLoading {FINANCIAL_DATA}...")

    financial_df = pl.read_csv(
        FINANCIAL_DATA,
        dtypes={
            'GVKEY': str,
            'LPERMNO': int,
            'tic': str,
            'conm': str,
            'cusip': str,
        },
        ignore_errors=True,
        truncate_ragged_lines=True
    )
    logger.info(f"  Loaded {len(financial_df):,} records")

    # Normalize company names
    logger.info("\nNormalizing company names...")
    financial_df = financial_df.with_columns([
        pl.col('conm').str.to_lowercase().str.strip_chars().alias('conm_normalized')
    ])

    # Get unique firms (primary links only) - INCLUDE ALL FIRMS, not just US
    logger.info("\nExtracting unique firms (including international)...")
    unique_firms = financial_df.filter(
        pl.col('LINKPRIM') == 'P'
    ).select([
        'GVKEY', 'LPERMNO', 'tic', 'conm', 'conm_normalized',
        'state', 'city', 'cik'
    ]).unique()

    logger.info(f"  Found {len(unique_firms):,} unique firms (primary links)")

    return unique_firms


# ============================================================================
# Enhanced Matching Strategies
# ============================================================================

def ticker_match(institution_name: str, firms_df: pl.DataFrame) -> dict:
    """Ticker-based matching."""
    # Extract ticker from name
    ticker_match = re.search(r'\(([A-Z]{1,5})\)', institution_name)
    if ticker_match:
        ticker = ticker_match.group(1)

        matches = firms_df.filter(
            pl.col('tic').str.to_uppercase() == ticker
        )

        if len(matches) > 0:
            match = matches[0]
            state_val = match['state'] if 'state' in match.schema else None

            return {
                'gvkey': match['GVKEY'],
                'permno': match['LPERMNO'],
                'ticker': match['tic'],
                'company_name': match['conm'],
                'state': state_val,
                'confidence': 0.95,
                'method': 'ticker'
            }

    return None


def exact_match(institution_normalized: str, firms_df: pl.DataFrame) -> dict:
    """Exact name matching."""
    matches = firms_df.filter(
        pl.col('conm_normalized') == institution_normalized
    )

    if len(matches) > 0:
        match = matches[0]
        state_val = match['state'] if 'state' in match.schema else None

        return {
            'gvkey': match['GVKEY'],
            'permno': match['LPERMNO'],
            'ticker': match['tic'],
            'company_name': match['conm'],
            'state': state_val,
            'confidence': 0.98,
            'method': 'exact'
        }
    return None


def fuzzy_match_aggressive(institution_normalized: str, firms_df: pl.DataFrame) -> dict:
    """Aggressive fuzzy matching - search all firms with lower threshold."""

    # Don't filter by state - search ALL firms
    all_firms = firms_df.unique(subset=['conm_normalized'])

    # Get company names
    company_names = all_firms['conm_normalized'].to_list()

    # Use rapidfuzz with LOWER score cutoff (80% instead of 87%)
    result = process.extract(
        institution_normalized,
        company_names,
        scorer=fuzz.WRatio,
        score_cutoff=80,  # Lower threshold for more matches
        limit=1
    )

    if result and len(result) > 0:
        best_name, score, _ = result[0]
        matches = all_firms.filter(pl.col('conm_normalized') == best_name)

        if len(matches) > 0:
            match = matches[0]
            # Adjust confidence based on score
            confidence = 0.60 + (score - 80) * 0.01
            confidence = min(confidence, 0.85)

            state_val = match['state'] if 'state' in match.schema else None

            return {
                'gvkey': match['GVKEY'],
                'permno': match['LPERMNO'],
                'ticker': match['tic'],
                'company_name': match['conm'],
                'state': state_val,
                'confidence': confidence,
                'method': 'fuzzy_aggressive'
            }

    return None


def contains_match(institution_normalized: str, firms_df: pl.DataFrame) -> dict:
    """Match if institution name contains key words from firm name."""

    # Extract key words from institution name
    words = [w for w in institution_normalized.split() if len(w) > 3]

    if len(words) < 2:
        return None

    # Try to find firms that contain these words
    for word in words[:3]:  # Check first 3 significant words
        matches = firms_df.filter(
            pl.col('conm_normalized').str.contains(word)
        )

        if len(matches) == 1:  # Only match if exactly one firm contains this word
            match = matches[0]
            state_val = match['state'] if 'state' in match.schema else None

            return {
                'gvkey': match['GVKEY'],
                'permno': match['LPERMNO'],
                'ticker': match['tic'],
                'company_name': match['conm'],
                'state': state_val,
                'confidence': 0.65,
                'method': 'contains'
            }

    return None


# ============================================================================
# Main Matching Function
# ============================================================================

def match_institutions_to_firms():
    """Match institutions to financial firms with aggressive strategy."""
    logger.info("=" * 80)
    logger.info("AGGRESSIVE FIRM-LEVEL FINANCIAL MATCHING")
    logger.info("=" * 80)

    # Load data
    logger.info("\nLoading institution data...")
    inst_ref = pl.read_parquet(DATA_PROCESSED / "ai_papers_firms_only.parquet")
    logger.info(f"  Loaded {len(inst_ref):,} firm papers")

    logger.info("\nLoading financial data...")
    firms_df = load_financial_data()

    # Extract unique institutions from papers
    logger.info("\nExtracting unique institutions from papers...")
    institutions_set = {}

    for row in inst_ref.iter_rows(named=True):
        primary_affs = row.get('author_primary_affiliations', [])
        countries = row.get('author_primary_affiliation_countries', [])

        for j, aff in enumerate(primary_affs):
            if not aff or aff == "":
                continue

            country = countries[j] if j < len(countries) else None

            # Create normalized name (aggressive)
            normalized = normalize_name_aggressive(aff)

            # Store canonical name and normalized name
            if aff not in institutions_set:
                institutions_set[aff] = {
                    'normalized_name': normalized,
                    'country': country,
                    'paper_count': 0
                }
            institutions_set[aff]['paper_count'] += 1

    logger.info(f"  Found {len(institutions_set):,} unique institutions")

    # Convert to list for processing
    institutions_list = []
    for raw_name, data in institutions_set.items():
        institutions_list.append({
            'raw_name': raw_name,
            'normalized_name': data['normalized_name'],
            'country': data['country'],
            'paper_count': data['paper_count']
        })

    logger.info(f"  Total institutions: {len(institutions_list):,}")

    # Match institutions to firms
    logger.info("\nMatching institutions to financial firms (aggressive strategy)...")
    matches = []
    unmatched = []

    total = len(institutions_list)
    for i, inst in enumerate(institutions_list):
        if (i + 1) % 5000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} institutions ({len(matches)} matched so far)...")

        inst_normalized = inst['normalized_name']

        # Try matching strategies in order
        match_result = None

        # Strategy 1: Ticker match (highest confidence)
        if not match_result:
            match_result = ticker_match(inst['raw_name'], firms_df)

        # Strategy 2: Exact name match
        if not match_result:
            match_result = exact_match(inst_normalized, firms_df)

        # Strategy 3: Fuzzy match with lower threshold
        if not match_result:
            match_result = fuzzy_match_aggressive(inst_normalized, firms_df)

        # Strategy 4: Contains match (for subsidiaries)
        if not match_result:
            match_result = contains_match(inst_normalized, firms_df)

        if match_result:
            matches.append({
                'institution_raw': inst['raw_name'],
                'institution_normalized': inst_normalized,
                'institution_country': inst['country'],
                'paper_count': inst['paper_count'],
                **match_result
            })
        else:
            unmatched.append(inst)

    logger.info(f"\nMatched: {len(matches):,} institutions")
    logger.info(f"Unmatched: {len(unmatched):,} institutions")

    match_rate = len(matches) / total * 100
    logger.info(f"Match rate: {match_rate:.1f}%")

    # Save matches
    matches_df = pl.DataFrame(matches)
    matches_df.write_parquet(OUTPUT_PARQUET, compression='snappy')
    logger.info(f"\nSaved to {OUTPUT_PARQUET}")

    # Save unmatched for reference
    if len(unmatched) > 0:
        unmatched_df = pl.DataFrame(unmatched)
        unmatched_file = OUTPUT_DIR / "unmatched_institutions_aggressive.parquet"
        unmatched_df.write_parquet(unmatched_file, compression='snappy')
        logger.info(f"Saved {len(unmatched):,} unmatched institutions to {unmatched_file}")

    # Statistics
    logger.info("\n" + "=" * 80)
    logger.info("MATCHING STATISTICS")
    logger.info("=" * 80)

    logger.info(f"\nMethod distribution:")
    if len(matches) > 0:
        method_dist = matches_df.group_by('method').agg(pl.len().alias('count')).sort('count', descending=True)
        for row in method_dist.iter_rows(named=True):
            logger.info(f"  {row['method']}: {row['count']:,} ({row['count']/len(matches)*100:.1f}%)")

        logger.info(f"\nConfidence distribution:")
        logger.info(f"  High (>0.90): {len(matches_df.filter(pl.col('confidence') > 0.90)):,} ({len(matches_df.filter(pl.col('confidence') > 0.90))/len(matches)*100:.1f}%)")
        logger.info(f"  Medium (0.70-0.90): {len(matches_df.filter((pl.col('confidence') >= 0.70) & (pl.col('confidence') <= 0.90))):,} ({len(matches_df.filter((pl.col('confidence') >= 0.70) & (pl.col('confidence') <= 0.90)))/len(matches)*100:.1f}%)")
        logger.info(f"  Low (0.60-0.70): {len(matches_df.filter(pl.col('confidence') < 0.70)):,} ({len(matches_df.filter(pl.col('confidence') < 0.70))/len(matches)*100:.1f}%)")

        logger.info(f"\nUnique financial firms matched: {matches_df['gvkey'].n_unique():,}")
        logger.info(f"Unique PERMNOs: {matches_df['permno'].n_unique():,}")

        logger.info(f"\nTop 20 firms by institution count:")
        top_firms = matches_df.group_by(['gvkey', 'company_name']).agg([
            pl.len().alias('inst_count'),
            pl.col('paper_count').sum().alias('total_papers')
        ]).sort('inst_count', descending=True).head(20)

        for row in top_firms.iter_rows(named=True):
            logger.info(f"  {row['company_name'][:50]:<50}: {row['inst_count']:>4} institutions, {row['total_papers']:>6,} papers")

    return matches_df, match_rate


def main():
    """Main execution function."""
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("AGGRESSIVE FIRM-LEVEL FINANCIAL MATCHING")
    logger.info("=" * 80)
    logger.info(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    matches_df, match_rate = match_institutions_to_firms()

    elapsed = time.time() - start_time
    logger.info(f"\nElapsed time: {elapsed:.1f} seconds")

    logger.info("\n" + "=" * 80)
    logger.info("FINANCIAL MATCHING COMPLETED")
    logger.info("=" * 80)
    logger.info(f"Match rate: {match_rate:.1f}%")
    logger.info(f"Unique firms: {matches_df['gvkey'].n_unique():,}")
    logger.info(f"Output: {OUTPUT_PARQUET}")
    logger.info(f"Finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Check if targets achieved
    logger.info("\n" + "=" * 80)
    logger.info("TARGET VALIDATION")
    logger.info("=" * 80)
    logger.info(f"Match rate: {match_rate:.1f}% (target: >95%)")
    logger.info(f"Unique firms: {matches_df['gvkey'].n_unique():,} (target: >=1000)")

    if match_rate >= 95.0:
        logger.info("✅ Match rate target ACHIEVED")
    else:
        logger.info(f"⚠️  Match rate target NOT achieved (need {95.0 - match_rate:.1f}% more)")

    if matches_df['gvkey'].n_unique() >= 1000:
        logger.info("✅ Firm count target ACHIEVED")
    else:
        logger.info(f"⚠️  Firm count target NOT achieved (need {1000 - matches_df['gvkey'].n_unique():,} more firms)")

    return matches_df, match_rate


if __name__ == "__main__":
    main()
