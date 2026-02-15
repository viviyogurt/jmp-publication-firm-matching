"""
Test Firm Classification Logic on Small Sample

This script tests the firm classification logic on a small sample of the dataset
to verify the filtering approach before running on the full dataset.

Date: 2025
"""

import polars as pl
from pathlib import Path
import logging
import re

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
LOGS_DIR = PROJECT_ROOT / "logs"

# Input file
INPUT_PARQUET = DATA_PROCESSED / "ai_papers_condensed.parquet"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "test_firm_classification.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Affiliation Classification Keywords (copied from main script)
# ============================================================================

# Known company research organizations (exact match priority)
KNOWN_COMPANY_RESEARCH = [
    'sas institute', 'microsoft research', 'google research', 'deepmind',
    'facebook ai research', 'meta ai', 'openai', 'anthropic',
    'ibm research', 'adobe research', 'amazon research', 'aws',
    'apple research', 'nvidia research', 'intel research',
    'alibaba research', 'tencent research', 'baidu research',
    'samsung research', 'qualcomm research', 'atos research',
    'accenture research', 'deloitte', 'mckinsey', 'bcg',
    'pwc', 'ey', 'kpmg', 'capgemini',
    'palantir', 'snowflake', 'databricks', 'salesforce research',
    'oracle research', 'sap research', 'uber research', 'spotify',
    'netflix research', 'airbnb research', 'byte research',
    'xiaomi research', 'huawei research', 'cisco research',
    'vmware research', 'broadcom research', 'amd research',
    'arm research', 'qualcomm technologies', 'bell labs',
    'xerox parc', 'parc', 'yahoo research', 'yandex research',
    'adobe labs', 'google labs', 'amazon labs', 'microsoft labs',
    'facebook research', 'meta research', 'nvidia labs',
    'intel labs', 'ibm thomas', 'ibm watson', 'alibaba labs'
]

UNIVERSITY_KEYWORDS = [
    'university', 'college', 'institute of technology', 'polytechnic',
    'school of', 'academy', 'faculty of', 'department of',
    'lab for', 'laboratory for', 'center for', 'centre for',
    'graduate school', 'medical school', 'law school', 'business school',
    'autonomous university', 'state university', 'national university',
    'technical university', 'technological university',
    'école', 'université', 'universidad', 'universita'
]

FIRM_KEYWORDS = [
    'inc', 'corp', 'corporation', 'llc', 'ltd', 'limited', 'plc', 'gmbh',
    'company', 'co.', 'companies', 'technologies', 'technology',
    'systems', 'solutions', 'software', 'analytics',
    'research labs', 'research laboratory', 'r&d',
    'intelligence', 'robotics', 'machine learning',
    'amazon', 'google', 'microsoft', 'apple', 'meta', 'facebook',
    'openai', 'anthropic', 'deepmind', 'alphabet',
    'nvidia', 'intel', 'amd', 'qualcomm', 'broadcom',
    'ibm', 'oracle', 'sap', 'salesforce', 'adobe',
    'tesla', 'spacex', 'uber', 'lyft', 'airbnb',
    'byte', 'tencent', 'alibaba', 'baidu',
    'samsung', 'lg', 'hyundai', 'toyota',
    'pharmaceutical', 'biotech', 'biosciences',
    'bank', 'financial', 'capital', 'ventures',
    'industries', 'group', 'holdings', 'international',
    'private limited', 'pty ltd', 'proprietary', 'ag', 'sa',
    'consulting', 'consultancy', 'partners', 'enterprises'
]

GOVERNMENT_KEYWORDS = [
    'national laboratory', 'national lab', 'naval research', 'army research',
    'air force', 'defense', 'government', 'federal',
    'national institute of', 'national aeronautics and space',
    'national science foundation', 'department of energy',
    'department of defense', 'ministry of'
]

EDUCATIONAL_INDICATORS = [
    'edu', 'ac.', 'ac.uk', 'edu.', 'univ.', 'college.',
    'graduate', 'undergraduate', 'phd', 'doctoral',
    'professor', 'lecturer', 'faculty'
]

# Company suffixes/prefixes that override university classification
COMPANY_SUFFIXES = [
    'inc', 'corp', 'llc', 'ltd', 'plc', 'gmbh', 'ag', 'sa', 'spa',
    'pty', 'bv', 'nv', 'aps', 'sro', 'zoo', 'kft', 'oao', 'ooo'
]

# ============================================================================
# Classification Functions
# ============================================================================

def classify_affiliation(affiliation: str) -> str:
    """
    Classify a single affiliation using improved logic with priority:
    1. Government
    2. Strong university indicators (university, college, etc.)
    3. Educational indicators
    4. Known company research organizations (with word boundaries)
    5. Company suffixes
    6. Ambiguous keywords (institute, lab, center) with additional checks
    7. Firm keywords
    """
    if not affiliation or affiliation == "":
        return 'unknown'

    normalized = affiliation.lower().strip()

    # Priority 1: Government (highest priority)
    for keyword in GOVERNMENT_KEYWORDS:
        if keyword in normalized:
            return 'government'

    # Priority 2: Strong university indicators (check BEFORE firm indicators)
    if ('university' in normalized or 'college' in normalized or
        'école' in normalized or 'université' in normalized or
        'universidad' in normalized or 'universita' in normalized):
        return 'university'

    # Priority 3: Educational indicators
    for indicator in EDUCATIONAL_INDICATORS:
        if indicator in normalized:
            return 'university'

    # Priority 4: Known company research organizations (with word boundaries)
    for known_org in KNOWN_COMPANY_RESEARCH:
        pattern = r'\b' + re.escape(known_org) + r'\b'
        if re.search(pattern, normalized):
            return 'firm'

    # Priority 5: Company suffixes (overrides "institute")
    for suffix in COMPANY_SUFFIXES:
        if normalized.endswith(suffix) or f' {suffix}' in normalized:
            return 'firm'

    # Priority 6: Ambiguous keywords (institute, lab, center) with additional checks
    for keyword in ['institute', 'lab', 'laboratory', 'center', 'centre']:
        if keyword in normalized:
            # Check if it's a company research institute (with word boundaries)
            for known_org in KNOWN_COMPANY_RESEARCH:
                pattern = r'\b' + re.escape(known_org) + r'\b'
                if re.search(pattern, normalized):
                    return 'firm'
            # Check for firm-specific patterns
            if (normalized.startswith(keyword) or
                f'{keyword} for' in normalized or
                any(fw in normalized for fw in ['technology', 'analytics', 'intelligence', 'systems', 'solutions'])):
                return 'firm'
            # Otherwise classify as university/academic
            return 'university'

    # Priority 7: Firm keywords
    for keyword in FIRM_KEYWORDS:
        if keyword in normalized:
            return 'firm'

    return 'unknown'


# ============================================================================
# Main Test
# ================================================================================

def main():
    """Run test on small sample."""
    logger.info("=" * 80)
    logger.info("TESTING FIRM CLASSIFICATION LOGIC")
    logger.info("=" * 80)

    # Load small sample
    logger.info(f"\nLoading sample of 1000 papers from {INPUT_PARQUET}")
    sample_df = pl.read_parquet(INPUT_PARQUET, n_rows=1000)

    logger.info(f"Sample columns: {sample_df.columns[:20]}...")  # Show first 20 columns
    logger.info(f"Sample shape: {sample_df.shape}")

    # Check affiliation columns
    logger.info(f"\nAffiliation-related columns:")
    for col in sample_df.columns:
        if 'affil' in col.lower():
            logger.info(f"  - {col}")

    # Extract some sample affiliations
    logger.info(f"\nExtracting sample affiliations from first 10 papers...")

    for i in range(min(10, len(sample_df))):
        row = sample_df.row(i, named=True)

        logger.info(f"\n--- Paper {i+1} ---")
        logger.info(f"Title: {row.get('title', 'N/A')[:80]}...")

        # Get primary affiliations
        primary_affs = row.get('author_primary_affiliations', [])
        if primary_affs:
            logger.info(f"Primary affiliations ({len(primary_affs)}):")
            for j, aff in enumerate(primary_affs[:5]):  # Show first 5
                if aff and aff != "":
                    classification = classify_affiliation(aff)
                    logger.info(f"  {j+1}. [{classification}] {aff}")
        else:
            logger.info("No primary affiliations")

        # Get all affiliations (nested)
        all_affs = row.get('author_affiliations', [])
        if all_affs and len(all_affs) > 0:
            logger.info(f"All affiliations (first author, first 5):")
            for j, aff in enumerate(all_affs[0][:5] if all_affs[0] else []):
                if aff and aff != "":
                    classification = classify_affiliation(aff)
                    logger.info(f"  {j+1}. [{classification}] {aff}")

    # Classify all papers in sample
    logger.info(f"\n" + "=" * 80)
    logger.info("CLASSIFYING ALL SAMPLE PAPERS")
    logger.info("=" * 80)

    classification_counts = {'firm': 0, 'university': 0, 'government': 0, 'unknown': 0, 'no_affil': 0}
    papers_with_firm = 0
    papers_with_university = 0
    papers_with_government = 0

    for row in sample_df.iter_rows(named=True):
        primary_affs = row.get('author_primary_affiliations', [])

        if not primary_affs or len(primary_affs) == 0:
            classification_counts['no_affil'] += 1
            continue

        # Classify each paper's primary affiliations
        paper_classifications = []
        for aff in primary_affs:
            if aff and aff != "":
                cls = classify_affiliation(aff)
                paper_classifications.append(cls)

        if not paper_classifications:
            classification_counts['no_affil'] += 1
            continue

        # Count affiliations by type
        firm_count = paper_classifications.count('firm')
        univ_count = paper_classifications.count('university')
        gov_count = paper_classifications.count('government')

        if firm_count > 0:
            papers_with_firm += 1
        if univ_count > 0:
            papers_with_university += 1
        if gov_count > 0:
            papers_with_government += 1

        # Determine primary classification
        if firm_count >= univ_count and firm_count >= gov_count:
            classification_counts['firm'] += 1
        elif univ_count >= firm_count and univ_count >= gov_count:
            classification_counts['university'] += 1
        elif gov_count >= firm_count and gov_count >= univ_count:
            classification_counts['government'] += 1
        else:
            classification_counts['unknown'] += 1

    # Print summary
    logger.info(f"\nClassification Summary (Sample of {len(sample_df)} papers):")
    logger.info(f"  Firm papers: {classification_counts['firm']} ({classification_counts['firm']/len(sample_df)*100:.1f}%)")
    logger.info(f"  University papers: {classification_counts['university']} ({classification_counts['university']/len(sample_df)*100:.1f}%)")
    logger.info(f"  Government papers: {classification_counts['government']} ({classification_counts['government']/len(sample_df)*100:.1f}%)")
    logger.info(f"  No affiliations: {classification_counts['no_affil']} ({classification_counts['no_affil']/len(sample_df)*100:.1f}%)")

    logger.info(f"\nPapers with at least one:")
    logger.info(f"  Firm affiliation: {papers_with_firm} ({papers_with_firm/len(sample_df)*100:.1f}%)")
    logger.info(f"  University affiliation: {papers_with_university} ({papers_with_university/len(sample_df)*100:.1f}%)")
    logger.info(f"  Government affiliation: {papers_with_government} ({papers_with_government/len(sample_df)*100:.1f}%)")

    logger.info("\n" + "=" * 80)
    logger.info("TEST COMPLETED")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
