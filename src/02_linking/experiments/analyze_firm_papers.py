"""
Analyze Firm-Affiliated Papers Dataset

This script analyzes the filtered firm-affiliated papers dataset to extract:
1. Total firm-affiliated papers
2. Number of unique firms
3. Classification by country
4. Papers published by US firms

Date: 2025
"""

import polars as pl
from pathlib import Path
import logging
import json
import re
from collections import Counter

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
OUTPUT_DIR = PROJECT_ROOT / "output" / "tables"
LOGS_DIR = PROJECT_ROOT / "logs"

INPUT_PARQUET = DATA_PROCESSED / "ai_papers_firms_only.parquet"
STATISTICS_FILE = DATA_PROCESSED / "ai_papers_firm_filtering_stats.json"
OUTPUT_FILE = OUTPUT_DIR / "firm_papers_analysis.txt"
OUTPUT_JSON = OUTPUT_DIR / "firm_papers_analysis.json"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Known company research for filtering
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
    'private limited', 'pty ltd', 'proprietary',
    'consulting', 'consultancy', 'partners', 'enterprises'
]


def is_firm_affiliation(affiliation: str) -> bool:
    """Check if an affiliation is a firm (not university/government)."""
    if not affiliation or affiliation == "":
        return False

    normalized = affiliation.lower().strip()

    # Check for government
    for keyword in GOVERNMENT_KEYWORDS:
        if keyword in normalized:
            return False

    # Check for university/college
    if ('university' in normalized or 'college' in normalized or
        'école' in normalized or 'université' in normalized or
        'universidad' in normalized or 'universita' in normalized):
        return False

    # Check for educational indicators
    for indicator in EDUCATIONAL_INDICATORS:
        if indicator in normalized:
            return False

    # Check for known company research
    for known_org in KNOWN_COMPANY_RESEARCH:
        pattern = r'\b' + re.escape(known_org) + r'\b'
        if re.search(pattern, normalized):
            return True

    # Check for company suffixes
    for suffix in COMPANY_SUFFIXES:
        if normalized.endswith(suffix) or f' {suffix}' in normalized:
            return True

    # Check for firm keywords
    for keyword in FIRM_KEYWORDS:
        if keyword in normalized:
            return True

    return False


def extract_country_from_affiliation(affiliation: str) -> str:
    """Extract country from affiliation string (looks for patterns like '(United States)')."""
    if not affiliation:
        return 'Unknown'

    # Look for country in parentheses
    import re
    match = re.search(r'\(([^)]+)\)', affiliation)
    if match:
        country = match.group(1).strip()
        if country:
            return country

    return 'Unknown'


def normalize_firm_name(affiliation: str) -> str:
    """Normalize firm name for counting unique firms."""
    if not affiliation or affiliation == "":
        return ""

    normalized = affiliation.lower().strip()

    # Remove country in parentheses
    normalized = re.sub(r'\s*\([^)]*\)', '', normalized)

    # Remove common suffixes for grouping
    for suffix in [' inc', ' corp', ' llc', ' ltd', ' plc', ' gmbh', ' ag', ' sa', ' co.', ' corp.', ' inc.', ' llc.', ' ltd.']:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()

    # Remove common prefixes
    for prefix in ['the ', 'a ']:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):].strip()

    return normalized


def main():
    """Main analysis function."""
    logger.info("=" * 80)
    logger.info("ANALYZING FIRM-AFFILIATED PAPERS")
    logger.info("=" * 80)

    # Load the filtered dataset
    logger.info(f"\nLoading dataset from {INPUT_PARQUET}")
    df = pl.read_parquet(INPUT_PARQUET)
    logger.info(f"Loaded {len(df):,} firm-affiliated papers")

    # Basic statistics
    logger.info("\n" + "=" * 80)
    logger.info("BASIC STATISTICS")
    logger.info("=" * 80)

    total_papers = len(df)
    logger.info(f"\nTotal firm-affiliated papers: {total_papers:,}")

    # Year distribution
    year_dist = df.group_by('publication_year').agg(pl.len().alias('count')).sort('publication_year')
    logger.info(f"\nYear range: {year_dist['publication_year'].min():.0f} - {year_dist['publication_year'].max():.0f}")

    # Extract all firm affiliations
    logger.info("\n" + "=" * 80)
    logger.info("EXTRACTING FIRM AFFILIATIONS")
    logger.info("=" * 80)

    all_firm_affiliations = []
    all_firm_affiliations_with_country = []

    for i, row in enumerate(df.iter_rows(named=True)):
        if (i + 1) % 100000 == 0:
            logger.info(f"  Processed {i+1:,}/{len(df):,} papers...")

        # Get all affiliations
        primary_affs = row.get('author_primary_affiliations', [])
        all_affs = row.get('author_affiliations', [])

        affiliations_to_check = set()

        # Add primary affiliations
        if primary_affs:
            for aff in primary_affs:
                if aff and aff != "":
                    affiliations_to_check.add(aff)

        # Add all affiliations from nested lists
        if all_affs:
            for aff_list in all_affs:
                if aff_list:
                    for aff in aff_list:
                        if aff and aff != "":
                            affiliations_to_check.add(aff)

        # Filter for firm affiliations only
        for aff in affiliations_to_check:
            if is_firm_affiliation(aff):
                country = extract_country_from_affiliation(aff)
                all_firm_affiliations.append(aff)
                all_firm_affiliations_with_country.append((aff, country))

    logger.info(f"\nExtracted {len(all_firm_affiliations):,} firm affiliations")

    # Count unique firms
    logger.info("\n" + "=" * 80)
    logger.info("UNIQUE FIRMS ANALYSIS")
    logger.info("=" * 80)

    normalized_firms = [normalize_firm_name(aff) for aff in all_firm_affiliations]
    unique_normalized_firms = set(normalized_firms)
    logger.info(f"\nTotal unique firms (normalized): {len(unique_normalized_firms):,}")

    # Top firms by paper count
    firm_counter = Counter(normalized_firms)
    top_firms = firm_counter.most_common(50)

    logger.info(f"\nTop 50 firms by paper count:")
    for i, (firm, count) in enumerate(top_firms, 1):
        logger.info(f"  {i:2d}. {firm}: {count:,} papers")

    # Country distribution
    logger.info("\n" + "=" * 80)
    logger.info("COUNTRY DISTRIBUTION")
    logger.info("=" * 80)

    country_counter = Counter([country for _, country in all_firm_affiliations_with_country])
    total_with_country = sum(country_counter.values())

    logger.info(f"\nAffiliations with country info: {total_with_country:,} / {len(all_firm_affiliations):,} ({total_with_country/len(all_firm_affiliations)*100:.1f}%)")

    logger.info(f"\nTop 50 countries by firm affiliations:")
    country_rank = 1
    for country, count in country_counter.most_common(50):
        pct = count / total_with_country * 100
        logger.info(f"  {country_rank:2d}. {country}: {count:,} affiliations ({pct:.1f}%)")
        country_rank += 1

    # US firms analysis
    logger.info("\n" + "=" * 80)
    logger.info("US FIRMS ANALYSIS")
    logger.info("=" * 80)

    us_affiliations = [aff for aff, country in all_firm_affiliations_with_country if 'united states' in country.lower() or 'usa' in country.lower() or country == 'US']
    us_firms_normalized = [normalize_firm_name(aff) for aff in us_affiliations]
    unique_us_firms = set(us_firms_normalized)

    logger.info(f"\nTotal US firm affiliations: {len(us_affiliations):,}")
    logger.info(f"Unique US firms: {len(unique_us_firms):,}")

    # Count papers with at least one US firm affiliation
    us_papers_count = 0
    for row in df.iter_rows(named=True):
        primary_affs = row.get('author_primary_affiliations', [])
        all_affs = row.get('author_affiliations', [])

        affiliations_to_check = set()
        if primary_affs:
            for aff in primary_affs:
                if aff and aff != "":
                    affiliations_to_check.add(aff)
        if all_affs:
            for aff_list in all_affs:
                if aff_list:
                    for aff in aff_list:
                        if aff and aff != "":
                            affiliations_to_check.add(aff)

        has_us = False
        for aff in affiliations_to_check:
            if is_firm_affiliation(aff):
                country = extract_country_from_affiliation(aff)
                if 'united states' in country.lower() or 'usa' in country.lower() or country == 'US':
                    has_us = True
                    break

        if has_us:
            us_papers_count += 1

    logger.info(f"\nTotal papers with US firm affiliations: {us_papers_count:,} ({us_papers_count/total_papers*100:.1f}%)")

    # Top US firms
    us_firm_counter = Counter(us_firms_normalized)
    top_us_firms = us_firm_counter.most_common(30)

    logger.info(f"\nTop 30 US firms by paper count:")
    for i, (firm, count) in enumerate(top_us_firms, 1):
        logger.info(f"  {i:2d}. {firm}: {count:,} papers")

    # Save results to files
    logger.info("\n" + "=" * 80)
    logger.info("SAVING RESULTS")
    logger.info("=" * 80)

    # Prepare results for JSON
    results = {
        'total_firm_papers': total_papers,
        'unique_firms_count': len(unique_normalized_firms),
        'us_firms_count': len(unique_us_firms),
        'us_papers_count': us_papers_count,
        'top_firms': [{'name': firm, 'papers': count} for firm, count in top_firms],
        'country_distribution': [{'country': country, 'affiliations': count, 'percentage': count/total_with_country*100} for country, count in country_counter.most_common(50)],
        'top_us_firms': [{'name': firm, 'papers': count} for firm, count in top_us_firms]
    }

    # Save JSON
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(results, f, indent=2)
    logger.info(f"  Saved JSON to {OUTPUT_JSON}")

    # Save text report
    with open(OUTPUT_FILE, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("FIRM-AFFILIATED PAPERS ANALYSIS REPORT\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Total firm-affiliated papers: {total_papers:,}\n")
        f.write(f"Unique firms (normalized): {len(unique_normalized_firms):,}\n")
        f.write(f"US firms: {len(unique_us_firms):,}\n")
        f.write(f"Papers with US firm affiliations: {us_papers_count:,} ({us_papers_count/total_papers*100:.1f}%)\n\n")

        f.write("=" * 80 + "\n")
        f.write("TOP 50 FIRMS BY PAPER COUNT\n")
        f.write("=" * 80 + "\n\n")
        for i, (firm, count) in enumerate(top_firms, 1):
            f.write(f"{i:3d}. {firm}: {count:,} papers\n")

        f.write("\n" + "=" * 80 + "\n")
        f.write("TOP 50 COUNTRIES BY FIRM AFFILIATIONS\n")
        f.write("=" * 80 + "\n\n")
        for i, (country, count) in enumerate(country_counter.most_common(50), 1):
            pct = count / total_with_country * 100
            f.write(f"{i:3d}. {country}: {count:,} affiliations ({pct:.1f}%)\n")

        f.write("\n" + "=" * 80 + "\n")
        f.write("TOP 30 US FIRMS BY PAPER COUNT\n")
        f.write("=" * 80 + "\n\n")
        for i, (firm, count) in enumerate(top_us_firms, 1):
            f.write(f"{i:3d}. {firm}: {count:,} papers\n")

    logger.info(f"  Saved report to {OUTPUT_FILE}")

    logger.info("\n" + "=" * 80)
    logger.info("ANALYSIS COMPLETED")
    logger.info("=" * 80)

    # Print summary to console
    print("\n" + "=" * 80)
    print("SUMMARY RESULTS")
    print("=" * 80)
    print(f"\n✓ Total firm-affiliated papers: {total_papers:,}")
    print(f"✓ Unique firms: {len(unique_normalized_firms):,}")
    print(f"✓ Papers with US firm affiliations: {us_papers_count:,} ({us_papers_count/total_papers*100:.1f}%)")
    print(f"✓ Unique US firms: {len(unique_us_firms):,}")

    print(f"\nTop 10 firms by paper count:")
    for i, (firm, count) in enumerate(top_firms[:10], 1):
        print(f"  {i}. {firm}: {count:,} papers")

    print(f"\nTop 10 countries:")
    for i, (country, count) in enumerate(country_counter.most_common(10), 1):
        pct = count / total_with_country * 100
        print(f"  {i}. {country}: {count:,} affiliations ({pct:.1f}%)")


if __name__ == "__main__":
    main()
