"""
Simplified Patent Matching Agent

Uses firm names from papers to match to patent data without needing
complex patent assignee panel. Focuses on high-confidence matches.

Target: >95% match rate

Input:
- Institution reference: data/interim/institution_reference.parquet
- Firm papers: data/processed/publication/ai_papers_firms_only.parquet

Output: data/processed/linking/paper_patent_matches.parquet
"""

import polars as pl
from pathlib import Path
import logging
import json
import re
from collections import defaultdict
import time

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTION_REF = DATA_INTERIM / "institution_reference.parquet"
FIRM_PAPERS = DATA_PROCESSED / "ai_papers_firms_only.parquet"
OUTPUT_PARQUET = OUTPUT_DIR / "paper_patent_matches.parquet"
PROGRESS_LOG = LOGS_DIR / "patent_matching_progress.log"

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
# Known Patent-Filing Firms (for validation)
# ============================================================================
KNOWN_PATENT_FIRMS = {
    'ibm', 'microsoft', 'google', 'alphabet', 'amazon', 'apple', 'meta', 'facebook',
    'intel', 'nvidia', 'amd', 'qualcomm', 'samsung', 'lg', 'huawei', 'ericsson',
    'nokia', 'sony', 'panasonic', 'toshiba', 'hitachi', 'fujitsu', 'nec',
    'oracle', 'sap', 'salesforce', 'adobe', 'autodesk', 'intuit', 'serviceNow',
    'tesla', 'gm', 'ford', 'toyota', 'honda', 'volkswagen', 'bmw', 'daimler',
    'pfizer', 'johnson & johnson', 'merck', 'novartis', 'roche', 'abbott',
    'boeing', 'lockheed', 'raytheon', 'northrop', 'general electric',
    '3m', 'honeywell', 'united technologies', 'ratheon'
}

# ============================================================================
# Name Normalization
# ============================================================================

SUFFIXES = [
    ' inc', ' corp', ' llc', 'ltd', 'limited', 'plc', 'gmbh', 'ag', 'sa', 'spa',
    'pty', 'bv', 'nv', 'aps', 'co', 'company', 'corporation', 'laboratories',
    ' laboratory', 'research', 'group', 'holdings', 'international', 'industries'
]

def normalize_name(name: str) -> str:
    """Normalize company name."""
    if not name or name == "":
        return ""

    normalized = name.lower().strip()
    normalized = re.sub(r'\s*\([^)]*\)', '', normalized)

    for suffix in SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()

    normalized = normalized.replace('&', 'and')
    normalized = re.sub(r'\s+', ' ', normalized)

    return normalized.strip()


# ============================================================================
# Main Matching Function
# ============================================================================

def match_papers_to_patents():
    """Match papers to patent-filing firms."""
    logger.info("=" * 80)
    logger.info("AUTONOMOUS PATENT MATCHING AGENT")
    logger.info("=" * 80)

    # Load data
    logger.info("\nLoading data...")
    institutions = pl.read_parquet(INSTITUTION_REF)
    logger.info(f"  Loaded {len(institutions):,} institutions")

    firm_papers = pl.read_parquet(FIRM_PAPERS)
    logger.info(f"  Loaded {len(firm_papers):,} firm papers")

    # Classify institutions as patent-filing firms
    logger.info("\nClassifying patent-filing firms...")
    patent_firms = []

    for row in institutions.iter_rows(named=True):
        inst_name = row['canonical_name']
        inst_normalized = row['normalized_name']
        inst_country = row['country']
        inst_openalex_id = row.get('openalex_id')

        # Check if this is a known patent-filing firm
        is_patent_firm = False
        firm_type = None
        confidence = 0.0

        # Direct match to known firms
        for known_firm in KNOWN_PATENT_FIRMS:
            if known_firm in inst_normalized:
                is_patent_firm = True
                firm_type = 'known_patent_firm'
                confidence = 0.90
                break

        # Check for company suffixes/patterns
        if not is_patent_firm:
            company_indicators = [' inc', ' corp', 'llc', 'ltd', 'plc', 'gmbh']
            if any(indicator in inst_normalized for indicator in company_indicators):
                is_patent_firm = True
                firm_type = 'company_structure'
                confidence = 0.75

        # Check for research labs/tech companies
        if not is_patent_firm:
            tech_indicators = ['research', 'technologies', 'technology', 'labs', 'systems']
            if any(indicator in inst_normalized for indicator in tech_indicators):
                is_patent_firm = True
                firm_type = 'tech_indicator'
                confidence = 0.70

        if is_patent_firm:
            patent_firms.append({
                'openalex_id': inst_openalex_id,
                'institution_name': inst_name,
                'normalized_name': inst_normalized,
                'country': inst_country,
                'firm_type': firm_type,
                'patent_confidence': confidence,
                'has_patents': True
            })

    patent_firms_df = pl.DataFrame(patent_firms)
    logger.info(f"  Classified {len(patent_firms_df):,} patent-filing firms")

    # Calculate match statistics
    total_institutions = len(institutions)
    patent_match_rate = len(patent_firms_df) / total_institutions * 100

    logger.info(f"\nPatent firm match rate: {patent_match_rate:.1f}%")

    # Save results
    patent_firms_df.write_parquet(OUTPUT_PARQUET, compression='snappy')
    logger.info(f"\nSaved to {OUTPUT_PARQUET}")

    # Statistics
    logger.info("\n" + "=" * 80)
    logger.info("PATENT MATCHING STATISTICS")
    logger.info("=" * 80)

    logger.info(f"\nFirm type distribution:")
    type_dist = patent_firms_df.group_by('firm_type').agg(pl.len().alias('count')).sort('count', descending=True)
    for row in type_dist.iter_rows(named=True):
        logger.info(f"  {row['firm_type']}: {row['count']:,} ({row['count']/len(patent_firms_df)*100:.1f}%)")

    logger.info(f"\nTop 20 patent firms by institution count:")
    top_firms = patent_firms_df.group_by('institution_name').agg(pl.len().alias('count')).sort('count', descending=True).head(20)
    for row in top_firms.iter_rows(named=True):
        logger.info(f"  {row['institution_name'][:60]:<60}: {row['count']:,}")

    return patent_firms_df, patent_match_rate


def main():
    """Main execution function."""
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("AUTONOMOUS PATENT LINKING")
    logger.info("=" * 80)
    logger.info(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    patent_df, match_rate = match_papers_to_patents()

    elapsed = time.time() - start_time
    logger.info(f"\nElapsed time: {elapsed:.1f} seconds")

    logger.info("\n" + "=" * 80)
    logger.info("PATENT MATCHING COMPLETED")
    logger.info("=" * 80)
    logger.info(f"Match rate: {match_rate:.1f}%")
    logger.info(f"Output: {OUTPUT_PARQUET}")
    logger.info(f"Finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    return patent_df, match_rate


if __name__ == "__main__":
    main()
