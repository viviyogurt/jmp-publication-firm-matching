"""
Autonomous Financial Linking Agent

Matches institutions to CRSP-Compustat financial data using multiple strategies:
1. Ticker-based matching
2. Exact name matching
3. Fuzzy matching (Jaro-Winkler)
4. Subsidiary matching
5. CIK-based matching

Target: >95% match rate

Input:
- Institution reference: data/interim/institution_reference.parquet
- Financial data: data/raw/compustat/crsp_a_ccm.csv

Output: data/processed/linking/paper_financial_matches.parquet
"""

import polars as pl
from pathlib import Path
import logging
import json
import re
from rapidfuzz import fuzz, process
from collections import defaultdict
import time

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "compustat"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTION_REF = DATA_INTERIM / "institution_reference.parquet"
FINANCIAL_DATA = DATA_RAW / "crsp_a_ccm.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "paper_financial_matches.parquet"
PROGRESS_LOG = LOGS_DIR / "financial_matching_progress.log"

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
# Subsidiary Database (Auto-generated patterns)
# ============================================================================
SUBSIDIARY_PATTERNS = {
    # Tech subsidiaries
    'google': ['alphabet', 'google deepmind', 'google brain', 'waymo', 'youtube', 'android'],
    'microsoft': ['microsoft research', 'linkedin', 'github', 'skype', 'microsoft azure'],
    'amazon': ['aws', 'amazon web services', 'amazon lab126', 'whole foods', 'zappos'],
    'apple': ['apple research', 'shazam', 'beats'],
    'meta': ['facebook', 'instagram', 'whatsapp', 'oculus', 'meta ai', 'facebook research'],
    'alphabet': ['google', 'waymo', 'deepmind', 'verily'],

    # Chinese tech
    'alibaba': ['alibaba cloud', 'alibaba research', 'aliexpress', 'taobao', 'tmall'],
    'tencent': ['tencent cloud', 'tencent research', 'weixin', 'wechat', 'qq games'],
    'baidu': ['baidu research', 'baidu cloud'],
    'huawei': ['huawei research', 'huawei cloud', 'honor'],
    'xiaomi': ['xiaomi research'],

    # Hardware/Chips
    'intel': ['intel research', 'intel capital', 'mobileye', 'altera'],
    'nvidia': ['nvidia research', 'nvidia capital'],
    'amd': ['amd research'],
    'samsung': ['samsung research', 'samsung electronics', 'samsung sds'],
    'lg': ['lg research', 'lg electronics'],

    # Enterprise
    'ibm': ['ibm research', 'ibm watson', 'red hat', 'the weather company'],
    'oracle': ['oracle research', 'java', 'sun microsystems'],
    'sap': ['sap research'],
    'salesforce': ['salesforce research', 'tableau', 'slack', 'mulesoft'],
    'adobe': ['adobe research', 'adobe cloud', 'magento'],
    'qualcomm': ['qualcomm research', 'qualcomm ventures'],

    # Automotive
    'tesla': ['tesla motors', 'solarcity'],
    'gm': ['general motors', 'chevrolet', 'cadillac', 'buick'],
    'ford': ['ford motor', 'lincoln'],
    'toyota': ['toyota research', 'lexus'],

    # Healthcare
    'pfizer': ['pfizer research'],
    'johnson & johnson': ['janssen', 'jnj'],
    'novartis': ['novartis research'],
    'roche': ['roche research', 'genentech'],
}

# Reverse mapping: subsidiary -> parent
SUBSIDIARY_TO_PARENT = {}
for parent, subsidiaries in SUBSIDIARY_PATTERNS.items():
    for sub in subsidiaries:
        SUBSIDIARY_TO_PARENT[sub] = parent

# ============================================================================
# Name Normalization
# ============================================================================

SUFFIXES = [
    ' inc', ' corp', ' llc', 'ltd', 'limited', 'plc', 'gmbh', 'ag', 'sa', 'spa',
    'pty', 'bv', 'nv', 'aps', 'co', 'company', 'corporation', 'laboratories',
    ' laboratory', 'research', 'group', 'holdings', 'international', 'industries',
    ' technologies', 'technology', 'systems', 'solutions', 'software', 'services'
]

def normalize_name(name: str) -> str:
    """Normalize company name for matching."""
    if not name or name == "":
        return ""

    normalized = name.lower().strip()

    # Remove country in parentheses
    normalized = re.sub(r'\s*\([^)]*\)', '', normalized)

    # Remove suffixes
    for suffix in SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()

    # Replace & with 'and'
    normalized = normalized.replace('&', 'and')

    # Normalize whitespace
    normalized = re.sub(r'\s+', ' ', normalized)

    return normalized.strip()


# ============================================================================
# Matching Strategies
# ============================================================================

def ticker_match(institution_name: str, financial_df: pl.DataFrame) -> dict:
    """Strategy 1: Ticker-based matching (highest confidence)."""
    # Extract potential ticker from institution name (e.g., "IBM (US)" -> "IBM")
    ticker_match = re.search(r'\(([A-Z]{1,5})\)', institution_name)
    if ticker_match:
        ticker = ticker_match.group(1)

        # Search in financial data
        matches = financial_df.filter(
            (pl.col('tic').str.to_upper() == ticker) &
            (pl.col('LINKPRIM') == 'P')  # Primary link only
        )

        if len(matches) > 0:
            return {
                'gvkey': matches['GVKEY'][0],
                'permno': matches['LPERMNO'][0],
                'ticker': ticker,
                'company_name': matches['conm'][0],
                'confidence': 0.98,
                'method': 'ticker'
            }

    return None


def exact_name_match(institution_normalized: str, financial_df: pl.DataFrame) -> dict:
    """Strategy 2: Exact name matching."""
    # Search for exact match in company names
    matches = financial_df.filter(
        pl.col('conm').str.to_lower() == institution_normalized
    )

    if len(matches) > 0:
        # Prefer primary links
        primary = matches.filter(pl.col('LINKPRIM') == 'P')
        if len(primary) > 0:
            match = primary[0]
        else:
            match = matches[0]

        return {
            'gvkey': match['GVKEY'],
            'permno': match['LPERMNO'],
            'ticker': match['tic'],
            'company_name': match['conm'],
            'confidence': 0.95,
            'method': 'exact_name'
        }

    return None


def fuzzy_name_match(institution_normalized: str, financial_df: pl.DataFrame) -> dict:
    """Strategy 3: Fuzzy name matching (Jaro-Winkler)."""
    # Sample company names for matching (for efficiency)
    company_names = financial_df['conm'].unique().to_list()

    # Use rapidfuzz for fast matching
    result = process.extract(
        institution_normalized,
        company_names,
        scorer=fuzz.WRatio,
        score_cutoff=85  # 85% similarity threshold
    )

    if result and len(result) > 0:
        best_match_name, score, _ = result[0]

        # Find the corresponding company record
        matches = financial_df.filter(pl.col('conm') == best_match_name)
        if len(matches) > 0:
            primary = matches.filter(pl.col('LINKPRIM') == 'P')
            if len(primary) > 0:
                match = primary[0]
            else:
                match = matches[0]

            confidence = 0.70 + (score - 85) * 0.01  # Scale 85-100 to 0.70-0.95
            confidence = min(confidence, 0.92)  # Cap at 0.92

            return {
                'gvkey': match['GVKEY'],
                'permno': match['LPERMNO'],
                'ticker': match['tic'],
                'company_name': match['conm'],
                'confidence': confidence,
                'method': 'fuzzy_name'
            }

    return None


def subsidiary_match(institution_normalized: str, financial_df: pl.DataFrame) -> dict:
    """Strategy 4: Subsidiary matching."""
    # Check if institution is a known subsidiary
    for sub, parent in SUBSIDIARY_TO_PARENT.items():
        if sub in institution_normalized:
            # Match parent company
            parent_result = exact_name_match(parent, financial_df)
            if parent_result:
                parent_result['method'] = 'subsidiary'
                parent_result['subsidiary_parent'] = parent
                parent_result['confidence'] = 0.80
                return parent_result

    return None


def cik_match(institution_name: str, financial_df: pl.DataFrame) -> dict:
    """Strategy 5: CIK-based matching."""
    # Extract CIK if present in metadata
    cik_match = re.search(r'CIK[:\s]?(\d{10})', institution_name, re.IGNORECASE)
    if not cik_match:
        return None

    cik = cik_match.group(1).lstrip('0')  # Remove leading zeros

    # Search in financial data
    matches = financial_df.filter(
        pl.col('cik').str.zfill(10, '0').str.contains(cik) |
        pl.col('cik').str.slice(-len(cik)) == cik
    )

    if len(matches) > 0:
        match = matches[0]
        return {
            'gvkey': match['GVKEY'],
            'permno': match['LPERMNO'],
            'ticker': match['tic'],
            'company_name': match['conm'],
            'confidence': 0.90,
            'method': 'cik'
        }

    return None


# ============================================================================
# Main Matching Function
# ============================================================================

def match_institutions_to_financial():
    """Match institutions to financial data using all strategies."""
    logger.info("=" * 80)
    logger.info("AUTONOMOUS FINANCIAL MATCHING AGENT")
    logger.info("=" * 80)

    # Load data
    logger.info("\nLoading data...")
    institutions = pl.read_parquet(INSTITUTION_REF)
    logger.info(f"  Loaded {len(institutions):,} institutions")

    financial_df = pl.read_csv(FINANCIAL_DATA)
    # Normalize company names for faster matching
    financial_df = financial_df.with_columns([
        pl.col('conm').str.to_lower().alias('conm_normalized')
    ])
    logger.info(f"  Loaded {len(financial_df):,} financial records")

    # Get unique firm papers for linking
    firm_papers = pl.read_parquet(DATA_PROCESSED / "ai_papers_firms_only.parquet")
    logger.info(f"  Loaded {len(firm_papers):,} firm papers")

    # Match institutions
    logger.info("\nMatching institutions to financial data...")
    matches = []
    unmatched = []

    total = len(institutions)
    for i, row in enumerate(institutions.iter_rows(named=True)):
        if (i + 1) % 5000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} institutions...")

        inst_name = row['canonical_name']
        inst_normalized = row['normalized_name']
        inst_country = row['country']
        inst_openalex_id = row.get('openalex_id')

        # Try strategies in order
        match_result = None

        # Strategy 1: Ticker match
        if not match_result:
            match_result = ticker_match(inst_name, financial_df)

        # Strategy 2: Exact name match
        if not match_result:
            match_result = exact_name_match(inst_normalized, financial_df)

        # Strategy 3: Subsidiary match
        if not match_result:
            match_result = subsidiary_match(inst_normalized, financial_df)

        # Strategy 4: CIK match
        if not match_result:
            match_result = cik_match(inst_name, financial_df)

        # Strategy 5: Fuzzy match
        if not match_result:
            match_result = fuzzy_name_match(inst_normalized, financial_df)

        if match_result:
            matches.append({
                'openalex_id': inst_openalex_id,
                'institution_name': inst_name,
                'institution_country': inst_country,
                **match_result
            })
        else:
            unmatched.append({
                'openalex_id': inst_openalex_id,
                'institution_name': inst_name,
                'institution_country': inst_country,
                'normalized_name': inst_normalized
            })

    logger.info(f"\nMatched: {len(matches):,} institutions")
    logger.info(f"Unmatched: {len(unmatched):,} institutions")
    match_rate = len(matches) / total * 100
    logger.info(f"Match rate: {match_rate:.1f}%")

    # Save matches
    matches_df = pl.DataFrame(matches)
    matches_df.write_parquet(OUTPUT_PARQUET, compression='snappy')
    logger.info(f"\nSaved to {OUTPUT_PARQUET}")

    # Save unmatched for analysis
    if len(unmatched) > 0:
        unmatched_df = pl.DataFrame(unmatched)
        unmatched_file = OUTPUT_DIR / "unmatched_institutions.parquet"
        unmatched_df.write_parquet(unmatched_file, compression='snappy')
        logger.info(f"Saved {len(unmatched):,} unmatched institutions to {unmatched_file}")

    # Statistics
    logger.info("\n" + "=" * 80)
    logger.info("MATCHING STATISTICS")
    logger.info("=" * 80)

    logger.info(f"\nMethod distribution:")
    method_dist = matches_df.group_by('method').agg(pl.len().alias('count')).sort('count', descending=True)
    for row in method_dist.iter_rows(named=True):
        logger.info(f"  {row['method']}: {row['count']:,} ({row['count']/len(matches)*100:.1f}%)")

    logger.info(f"\nConfidence distribution:")
    logger.info(f"  High (>0.90): {len(matches_df.filter(pl.col('confidence') > 0.90)):,} ({len(matches_df.filter(pl.col('confidence') > 0.90))/len(matches)*100:.1f}%)")
    logger.info(f"  Medium (0.70-0.90): {len(matches_df.filter((pl.col('confidence') >= 0.70) & (pl.col('confidence') <= 0.90))):,} ({len(matches_df.filter((pl.col('confidence') >= 0.70) & (pl.col('confidence') <= 0.90)))/len(matches)*100:.1f}%)")

    logger.info(f"\nTop 20 matched firms by institution count:")
    top_firms = matches_df.group_by('company_name').agg(pl.len().alias('inst_count')).sort('inst_count', descending=True).head(20)
    for row in top_firms.iter_rows(named=True):
        logger.info(f"  {row['company_name'][:60]:<60}: {row['inst_count']:,} institutions")

    return matches_df, match_rate


def main():
    """Main execution function."""
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("AUTONOMOUS FINANCIAL LINKING")
    logger.info("=" * 80)
    logger.info(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    matches_df, match_rate = match_institutions_to_financial()

    elapsed = time.time() - start_time
    logger.info(f"\nElapsed time: {elapsed:.1f} seconds")

    logger.info("\n" + "=" * 80)
    logger.info("FINANCIAL MATCHING COMPLETED")
    logger.info("=" * 80)
    logger.info(f"Match rate: {match_rate:.1f}%")
    logger.info(f"Output: {OUTPUT_PARQUET}")
    logger.info(f"Finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Check if we need optimization
    if match_rate < 95:
        logger.warning(f"\nMatch rate {match_rate:.1f}% < 95% target")
        logger.warning("Will proceed with optimization in next phase")

    return matches_df, match_rate


if __name__ == "__main__":
    main()
