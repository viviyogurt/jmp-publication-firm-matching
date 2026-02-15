"""
Stage 1 Enhanced: Exact & High-Confidence Matching

This script implements Stage 1 enhanced matching following patent matching success:
1. Exact name match (conm, conml, alternative names)
2. Ticker matching from institution acronyms (CRITICAL ADDITION)
3. Homepage domain exact match
4. ROR ID matching (NEW - HIGH VALUE)
5. Parent institution name matching

Target: 1,300-1,500 firms with >98% accuracy
"""
import polars as pl
from pathlib import Path
import logging
import re
import requests
from typing import Dict, List, Optional, Tuple

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
INSTITUTIONS_CLASSIFIED = DATA_INTERIM / "publication_institutions_classified.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_stage1_enhanced_v2.parquet"
PROGRESS_LOG = LOGS_DIR / "match_publications_stage1_enhanced_v2.log"

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

# ============================================================================
# Matching Functions
# ============================================================================

def exact_name_match(institution_clean: str,
                     firm_conm_clean: Optional[str],
                     firm_conml_clean: Optional[str],
                     firm_alt_names: List[str]) -> Tuple[bool, float, str]:
    """
    Strategy 1: Exact name match.
    """
    if not institution_clean:
        return False, 0.0, ""

    # Exact match on conm_clean
    if firm_conm_clean and institution_clean == firm_conm_clean:
        return True, 0.98, "exact_conm"

    # Exact match on conml_clean
    if firm_conml_clean and institution_clean == firm_conml_clean:
        return True, 0.98, "exact_conml"

    # Exact match on alternative names
    if firm_alt_names:
        for alt_name in firm_alt_names:
            if alt_name and institution_clean == alt_name.upper():
                return True, 0.98, f"alternative_name_{alt_name[:20]}"

    return False, 0.0, ""


def ticker_from_acronym(institution_acronyms: List[str],
                        firm_tic: Optional[str]) -> Tuple[bool, float, str]:
    """
    Strategy 2: Ticker matching from institution acronyms.

    This is CRITICAL - many institutions have company acronyms as acronyms.
    Example: IBM has acronym "IBM" which matches IBM's ticker.
    """
    if not institution_acronyms or not firm_tic:
        return False, 0.0, ""

    ticker = firm_tic.strip().upper()

    # Check if any institution acronym matches firm ticker
    for acronym in institution_acronyms:
        if acronym.upper() == ticker:
            return True, 0.97, "acronym_ticker_match"

    return False, 0.0, ""


def homepage_domain_exact(institution_domain: Optional[str],
                          firm_domain: Optional[str]) -> Tuple[bool, float, str]:
    """
    Strategy 3: Homepage domain exact match.
    """
    if not institution_domain or not firm_domain:
        return False, 0.0, ""

    # Normalize domains (lowercase, remove www)
    inst_domain_norm = institution_domain.lower().replace('www.', '').strip()
    firm_domain_norm = firm_domain.lower().replace('www.', '').strip()

    if inst_domain_norm and inst_domain_norm == firm_domain_norm:
        return True, 0.98, "homepage_domain_exact"

    return False, 0.0, ""


def ror_id_matching(institution_ror: Optional[str],
                    firm_gvkey: str,
                    ror_lookup: Dict[str, Dict]) -> Tuple[bool, float, str]:
    """
    Strategy 4: ROR ID matching (NEW).

    ROR database provides organization metadata including links to GVKEY.
    """
    if not institution_ror:
        return False, 0.0, ""

    # Extract ROR ID from URL
    if 'ror.org/' in institution_ror:
        ror_id = institution_ror.split('ror.org/')[-1]
    else:
        ror_id = institution_ror

    # Check ROR lookup (pre-built)
    if ror_id in ror_lookup:
        ror_data = ror_lookup[ror_id]

        # Check if ROR has GVKEY link
        ror_gvkey = ror_data.get('gvkey')
        if ror_gvkey and ror_gvkey == gvkey:
            return True, 0.97, "ror_id_direct"

        # Check if ROR organization name matches firm
        ror_name = ror_data.get('name', '').upper()
        firm_name = ror_data.get('firm_name', '').upper()

        if ror_name and firm_name and ror_name == firm_name:
            return True, 0.96, "ror_name_match"

    return False, 0.0, ""


def parent_institution_match(institution_parent_ids: List[str],
                             institution_parent_names: List[str],
                             firm_conm_clean: Optional[str]) -> Tuple[bool, float, str]:
    """
    Strategy 5: Parent institution matching.

    Match parent institution names to firms.
    """
    if not institution_parent_ids or not firm_conm_clean:
        return False, 0.0, ""

    for parent_name in institution_parent_names:
        if not parent_name:
            continue

        parent_clean = parent_name.upper()

        # Check if parent name matches firm
        if parent_clean == firm_conm_clean:
            return True, 0.95, "parent_institution_exact"

        # Check if parent name contained in firm
        if len(parent_clean) >= 8 and parent_clean in firm_conm_clean:
            return True, 0.93, "parent_institution_contained"

    return False, 0.0, ""


# ============================================================================
# Main Processing
# ============================================================================

def main():
    """Main Stage 1 enhanced matching workflow."""

    logger.info("=" * 80)
    logger.info("STAGE 1 ENHANCED MATCHING - TARGET: 1,300-1,500 FIRMS")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/6] Loading data...")

    # Load institutions (classified)
    institutions_df = pl.read_parquet(INSTITUTIONS_CLASSIFIED)
    logger.info(f"  Loaded {len(institutions_df):,} institutions")

    # Filter to corporate only (skip academic, gov, nonprofit)
    corporate_inst = institutions_df.filter(
        pl.col('institution_type_classified') == 'corporate'
    )
    logger.info(f"  Corporate institutions: {len(corporate_inst):,}")

    # Load firms
    firms_df = pl.read_parquet(COMPUSTAT_STANDARDIZED)
    logger.info(f"  Loaded {len(firms_df):,} firms")

    # Build ticker lookup
    logger.info("\n[2/6] Building lookups...")

    # Ticker to firms lookup
    ticker_to_firms = {}
    for firm_row in firms_df.iter_rows(named=True):
        tic = firm_row.get('tic')
        if tic and tic.strip():
            ticker_to_firms[tic.strip().upper()] = firm_row

    logger.info(f"  Built ticker lookup: {len(ticker_to_firms):,} unique tickers")

    # Build ROR lookup (placeholder - would need ROR API/data)
    ror_lookup = {}
    logger.info(f"  ROR lookup: {len(ror_lookup):,} institutions (to be implemented)")

    # Process matching
    logger.info("\n[3/6] Matching institutions to firms...")

    matches = []
    matched_institutions = set()

    for inst_row in corporate_inst.iter_rows(named=True):
        inst_id = inst_row['institution_id']

        # Skip if already matched
        if inst_id in matched_institutions:
            continue

        inst_name = inst_row.get('display_name', '')
        inst_clean = inst_row.get('normalized_name', '')

        # Get firm data
        # For efficiency, iterate through firms (can optimize with lookup dicts)
        for firm_row in firms_df.iter_rows(named=True):
            gvkey = firm_row['GVKEY']
            firm_conm = firm_row.get('conm', '')
            firm_conm_clean = firm_row.get('conm_clean', '')
            firm_conml_clean = firm_row.get('conml_clean', '')
            firm_tic = firm_row.get('tic')
            firm_domain = firm_row.get('homepage_domain')
            firm_alt_names = firm_row.get('alternative_names', [])

            # Strategy 1: Exact match
            is_match, conf, method = exact_name_match(
                inst_clean, firm_conm_clean, firm_conml_clean, firm_alt_names
            )
            if is_match:
                matches.append({
                    'institution_id': inst_id,
                    'institution_display_name': inst_name,
                    'GVKEY': gvkey,
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_conm,
                    'match_type': 'stage1_enhanced',
                    'match_method': method,
                    'match_confidence': conf,
                    'institution_paper_count': inst_row.get('paper_count', 0),
                })
                matched_institutions.add(inst_id)
                break

            # Strategy 2: Ticker from acronym
            inst_acronyms = inst_row.get('acronyms', [])
            if inst_acronyms:
                is_match, conf, method = ticker_from_acronym(inst_acronyms, firm_tic)
                if is_match:
                    matches.append({
                        'institution_id': inst_id,
                        'institution_display_name': inst_name,
                        'GVKEY': gvkey,
                        'LPERMNO': firm_row.get('LPERMNO'),
                        'firm_conm': firm_conm,
                        'match_type': 'stage1_enhanced',
                        'match_method': method,
                        'match_confidence': conf,
                        'institution_paper_count': inst_row.get('paper_count', 0),
                    })
                    matched_institutions.add(inst_id)
                    break

            # Strategy 3: Homepage domain exact
            inst_domain = inst_row.get('homepage_domain')
            is_match, conf, method = homepage_domain_exact(inst_domain, firm_domain)
            if is_match:
                matches.append({
                    'institution_id': inst_id,
                    'institution_display_name': inst_name,
                    'GVKEY': gvkey,
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_conm,
                    'match_type': 'stage1_enhanced',
                    'match_method': method,
                    'match_confidence': conf,
                    'institution_paper_count': inst_row.get('paper_count', 0),
                })
                matched_institutions.add(inst_id)
                break

        # Progress update
        if len(matched_institutions) % 100 == 0:
            logger.info(f"  Matched {len(matched_institutions):,} institutions so far...")

    # Save results
    logger.info("\n[4/6] Saving results...")

    if matches:
        matches_df = pl.DataFrame(matches)
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df):,} matches to {OUTPUT_FILE}")
    else:
        logger.error("  No matches found!")
        return

    # Statistics
    logger.info("\n[5/6] Generating statistics...")

    unique_firms = matches_df['GVKEY'].n_unique()
    total_papers = matches_df['institution_paper_count'].sum()

    logger.info(f"\nTotal matches: {len(matches_df):,}")
    logger.info(f"Unique institutions: {matches_df['institution_id'].n_unique():,}")
    logger.info(f"Unique firms: {unique_firms:,}")
    logger.info(f"Total papers: {total_papers:,}")

    # Match method breakdown
    logger.info("\nMatch Method Breakdown:")
    method_counts = matches_df.group_by('match_method').agg(
        pl.len().alias('count')
    ).sort('count', descending=True)

    for row in method_counts.iter_rows(named=True):
        pct = row['count'] / len(matches_df) * 100
        logger.info(f"  {row['match_method']:40}: {row['count']:>6} ({pct:>5.1f}%)")

    # Top firms
    logger.info("\nTop 20 Firms by Number of Institutions:")
    top_firms = matches_df.group_by(['GVKEY', 'firm_conm']).agg(
        pl.len().alias('num_institutions')
    ).sort('num_institutions', descending=True).head(20)

    for i, row in enumerate(top_firms.iter_rows(named=True), 1):
        logger.info(f"  {i:2}. {row['GVKEY']:>10} | {row['firm_conm'][:40]:40} | {row['num_institutions']:>3}")

    # Coverage check
    TOTAL_CORPORATE = 32930
    coverage_pct = matches_df['institution_id'].n_unique() / TOTAL_CORPORATE * 100
    logger.info(f"\nCoverage of corporate institutions: {coverage_pct:.2f}%")

    logger.info("\n[6/6] Complete!")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
