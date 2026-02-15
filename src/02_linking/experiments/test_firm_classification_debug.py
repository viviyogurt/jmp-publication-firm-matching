"""
Debug firm classification to understand misclassifications
"""

import polars as pl
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
INPUT_PARQUET = DATA_PROCESSED / "ai_papers_condensed.parquet"

# Known company research organizations
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

COMPANY_SUFFIXES = [
    'inc', 'corp', 'llc', 'ltd', 'plc', 'gmbh', 'ag', 'sa', 'spa',
    'pty', 'bv', 'nv', 'aps', 'sro', 'zoo', 'kft', 'oao', 'ooo'
]


import re

def classify_affiliation_debug(affiliation: str) -> str:
    """Classify with debug output."""
    if not affiliation or affiliation == "":
        return 'unknown'

    normalized = affiliation.lower().strip()

    # Priority 1: Known company research organizations (with word boundaries)
    for known_org in KNOWN_COMPANY_RESEARCH:
        # Use regex with word boundaries to avoid substring matches
        pattern = r'\b' + re.escape(known_org) + r'\b'
        if re.search(pattern, normalized):
            return f'firm [matched known company: "{known_org}"]'

    # Priority 2: Government
    for keyword in GOVERNMENT_KEYWORDS:
        if keyword in normalized:
            return f'government [matched: "{keyword}"]'

    # Priority 3: Strong university indicators (check BEFORE known company check)
    if ('university' in normalized or 'college' in normalized or
        'école' in normalized or 'université' in normalized or
        'universidad' in normalized or 'universita' in normalized):
        # Check which keyword matched
        if 'university' in normalized:
            return f'university [matched: "university"]'
        elif 'college' in normalized:
            return f'university [matched: "college"]'
        else:
            return f'university [matched: foreign university term]'

    # Priority 4: Educational indicators
    for indicator in EDUCATIONAL_INDICATORS:
        if indicator in normalized:
            return f'university [matched edu indicator: "{indicator}"]'

    # Priority 5: Company suffixes
    for suffix in COMPANY_SUFFIXES:
        if normalized.endswith(suffix) or f' {suffix}' in normalized:
            return f'firm [matched suffix: "{suffix}"]'

    # Priority 6: Ambiguous keywords
    for keyword in ['institute', 'lab', 'laboratory', 'center', 'centre']:
        if keyword in normalized:
            for known_org in KNOWN_COMPANY_RESEARCH:
                pattern = r'\b' + re.escape(known_org) + r'\b'
                if re.search(pattern, normalized):
                    return f'firm [ambiguous + known company: "{known_org}"]'
            if (normalized.startswith(keyword) or
                f'{keyword} for' in normalized or
                any(fw in normalized for fw in ['technology', 'analytics', 'intelligence', 'systems', 'solutions'])):
                matched_fw = [fw for fw in ['technology', 'analytics', 'intelligence', 'systems', 'solutions'] if fw in normalized]
                return f'firm [ambiguous + firm word: {matched_fw}]'
            return f'university [ambiguous keyword: "{keyword}"]'

    # Priority 7: Firm keywords
    for keyword in FIRM_KEYWORDS:
        if keyword in normalized:
            return f'firm [matched firm keyword: "{keyword}"]'

    return 'unknown'


# Load sample and test specific affiliations
sample_df = pl.read_parquet(INPUT_PARQUET, n_rows=1000)

print("=" * 80)
print("DEBUG CLASSIFICATION FOR SPECIFIC AFFILIATIONS")
print("=" * 80)

# Test specific problematic affiliations
test_affiliations = [
    "University of California, Berkeley",
    "University of California, San Diego",
    "SAS Institute (United States)",
    "Pennsylvania State University",
    "Carnegie Mellon University",
    "United States Naval Research Laboratory",
    "Institute of Mathematical Statistics",
    "National Center for Biotechnology Information"
]

print("\nTesting specific affiliations:")
for aff in test_affiliations:
    result = classify_affiliation_debug(aff)
    print(f"  {aff}")
    print(f"    -> {result}")

# Now check actual affiliations from the dataset
print("\n" + "=" * 80)
print("CHECKING ACTUAL AFFILIATIONS FROM DATASET")
print("=" * 80)

for i in range(min(20, len(sample_df))):
    row = sample_df.row(i, named=True)
    primary_affs = row.get('author_primary_affiliations', [])

    if primary_affs and len(primary_affs) > 0:
        for aff in primary_affs[:2]:  # Check first 2 affiliations
            if aff and aff != "" and "university" in aff.lower():
                result = classify_affiliation_debug(aff)
                if not result.startswith('university'):
                    print(f"\n--- MISCLASSIFICATION DETECTED ---")
                    print(f"Affiliation: {aff}")
                    print(f"Result: {result}")

print("\n" + "=" * 80)
print("DEBUG COMPLETE")
print("=" * 80)
