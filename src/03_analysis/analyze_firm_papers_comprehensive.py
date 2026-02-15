"""
Comprehensive Analysis of Firm-Affiliated AI Papers

This script performs three analyses:
1. Memory-efficient exact US firm paper counts
2. Big Tech firms subset analysis
3. Time-series analysis by year

Date: 2025
"""

import polars as pl
from pathlib import Path
import logging
import json
from collections import Counter, defaultdict
import re

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
OUTPUT_DIR = PROJECT_ROOT / "output" / "tables"
LOGS_DIR = PROJECT_ROOT / "logs"

INPUT_PARQUET = DATA_PROCESSED / "ai_papers_firms_only.parquet"
OUTPUT_JSON = OUTPUT_DIR / "firm_papers_comprehensive_analysis.json"
OUTPUT_REPORT = OUTPUT_DIR / "firm_papers_comprehensive_report.txt"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "comprehensive_firm_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Big Tech firms list
BIG_TECH_FIRMS = {
    'google', 'alphabet', 'microsoft', 'amazon', 'aws', 'amazon web services',
    'apple', 'meta', 'facebook', 'instagram', 'whatsapp',
    'openai', 'anthropic', 'deepmind',
    'nvidia', 'intel', 'amd', 'qualcomm', 'broadcom',
    'tesla', 'spacex', 'uber', 'lyft', 'airbnb',
    'ibm', 'oracle', 'salesforce', 'adobe', 'sap',
    'samsung', 'lg', 'hyundai', 'xiaomi', 'huawei',
    'tencent', 'alibaba', 'baidu', 'byte', 'bytedance', 'tiktok',
    'netflix', 'spotify', 'paypal', 'stripe'
}

# Known company research organizations
KNOWN_COMPANY_RESEARCH = [
    'google research', 'microsoft research', 'deepmind',
    'facebook ai research', 'meta ai', 'openai', 'anthropic',
    'ibm research', 'adobe research', 'amazon research', 'aws',
    'apple research', 'nvidia research', 'intel research',
    'alibaba research', 'tencent research', 'baidu research',
    'samsung research', 'qualcomm research',
    'salesforce research', 'oracle research', 'uber research', 'spotify',
    'netflix research', 'airbnb research',
    'xiaomi research', 'huawei research', 'cisco research',
    'vmware research', 'broadcom research', 'amd research',
    'adobe labs', 'google labs', 'amazon labs', 'microsoft labs',
    'facebook research', 'meta research', 'nvidia labs', 'intel labs',
    'ibm thomas', 'ibm watson', 'alibaba labs', 'bell labs'
]

# Government keywords to exclude
GOVERNMENT_KEYWORDS = [
    'national laboratory', 'national lab', 'naval research', 'army research',
    'air force', 'defense', 'government', 'federal',
    'national institute of', 'national aeronautics and space',
    'national science foundation', 'department of energy',
    'department of defense', 'ministry of'
]

# Educational indicators to exclude
EDUCATIONAL_INDICATORS = [
    'edu', 'ac.', 'ac.uk', 'edu.', 'univ.', 'college.',
    'graduate', 'undergraduate', 'phd', 'doctoral',
    'professor', 'lecturer', 'faculty'
]


def normalize_firm_name(name: str) -> str:
    """Normalize firm name for counting."""
    if not name or name == "":
        return ""

    normalized = name.lower().strip()

    # Remove country in parentheses
    normalized = re.sub(r'\s*\([^)]*\)', '', normalized)

    # Remove common suffixes
    for suffix in [' inc', ' corp', ' llc', ' ltd', ' plc', ' gmbh', ' ag', ' sa', ' co.', ' corporation', ' technologies']:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()

    # Remove common prefixes
    for prefix in ['the ', 'a ']:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):].strip()

    return normalized


def extract_country(affiliation: str) -> str:
    """Extract country from affiliation string."""
    if not affiliation:
        return None

    # Look for country in parentheses
    match = re.search(r'\(([^)]+)\)', affiliation)
    if match:
        return match.group(1).strip()

    # Check for common country patterns in the string
    if 'united states' in affiliation.lower():
        return 'United States'
    if 'usa' in affiliation.lower():
        return 'USA'
    if 'uk' in affiliation.lower():
        return 'UK'
    if 'china' in affiliation.lower():
        return 'China'
    if 'japan' in affiliation.lower():
        return 'Japan'
    if 'germany' in affiliation.lower():
        return 'Germany'
    if 'france' in affiliation.lower():
        return 'France'
    if 'india' in affiliation.lower():
        return 'India'
    if 'south korea' in affiliation.lower():
        return 'South Korea'

    return None


def is_firm_affiliation(affiliation: str) -> bool:
    """Check if affiliation is a firm (not university/government)."""
    if not affiliation or affiliation == "":
        return False

    normalized = affiliation.lower().strip()

    # Check for government
    for keyword in GOVERNMENT_KEYWORDS:
        if keyword in normalized:
            return False

    # Check for university/college
    if ('university' in normalized or 'college' in normalized):
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
    for suffix in ['inc', 'corp', 'llc', 'ltd', 'plc', 'gmbh']:
        if normalized.endswith(suffix) or f' {suffix}' in normalized:
            return True

    # Check for firm keywords
    firm_keywords = ['company', 'technologies', 'systems', 'solutions', 'analytics',
                     'google', 'microsoft', 'amazon', 'apple', 'meta', 'facebook',
                     'openai', 'nvidia', 'intel', 'samsung', 'alibaba', 'tencent']
    for keyword in firm_keywords:
        if keyword in normalized:
            return True

    return False


def is_big_tech(affiliation: str) -> tuple:
    """Check if affiliation is a Big Tech firm. Returns (is_big_tech, company_name)."""
    if not affiliation:
        return False, None

    normalized = normalize_firm_name(affiliation)

    # Check exact matches first
    for company in BIG_TECH_FIRMS:
        pattern = r'\b' + re.escape(company) + r'\b'
        if re.search(pattern, normalized):
            return True, company

    return False, None


# ============================================================================
# Analysis 1: US Firm Papers (Memory-Efficient)
# ============================================================================

def analyze_us_firms():
    """Analyze US firm-affiliated papers in batches."""
    logger.info("\n" + "=" * 80)
    logger.info("ANALYSIS 1: US FIRM PAPERS")
    logger.info("=" * 80)

    # Load data and process in batches
    df = pl.read_parquet(INPUT_PARQUET)
    total_papers = len(df)
    logger.info(f"Total papers: {total_papers:,}")

    # Process to find US firm papers
    us_firm_papers = 0
    us_firms = set()
    us_firm_affiliations = []

    for i, row in enumerate(df.iter_rows(named=True)):
        if (i + 1) % 100000 == 0:
            logger.info(f"  Processed {i+1:,}/{total_papers:,} papers...")

        # Get affiliation countries
        countries = row.get('author_primary_affiliation_countries', [])

        # Get affiliations
        primary_affs = row.get('author_primary_affiliations', [])

        has_us_firm = False

        for j, aff in enumerate(primary_affs):
            if not aff:
                continue

            country = countries[j] if j < len(countries) else None

            # Check if US and firm
            is_us = country and ('united states' in country.lower() or
                                'usa' in country.lower() or
                                country == 'US' or
                                country == 'USA')

            if is_us and is_firm_affiliation(aff):
                has_us_firm = True
                us_firms.add(normalize_firm_name(aff))
                us_firm_affiliations.append(aff)

        if has_us_firm:
            us_firm_papers += 1

    logger.info(f"\n✓ US firm-affiliated papers: {us_firm_papers:,} ({us_firm_papers/total_papers*100:.1f}%)")
    logger.info(f"✓ Unique US firms: {len(us_firms):,}")
    logger.info(f"✓ Total US firm affiliations: {len(us_firm_affiliations):,}")

    return {
        'us_firm_papers': us_firm_papers,
        'us_firm_percentage': us_firm_papers / total_papers * 100,
        'unique_us_firms': len(us_firms),
        'total_us_affiliations': len(us_firm_affiliations)
    }


# ============================================================================
# Analysis 2: Big Tech Firms
# ============================================================================

def analyze_big_tech():
    """Analyze Big Tech firm papers."""
    logger.info("\n" + "=" * 80)
    logger.info("ANALYSIS 2: BIG TECH FIRMS")
    logger.info("=" * 80)

    df = pl.read_parquet(INPUT_PARQUET)
    total_papers = len(df)

    big_tech_papers = 0
    big_tech_by_company = defaultdict(int)
    big_tech_by_year = defaultdict(lambda: defaultdict(int))

    for i, row in enumerate(df.iter_rows(named=True)):
        if (i + 1) % 100000 == 0:
            logger.info(f"  Processed {i+1:,}/{total_papers:,} papers...")

        year = row.get('publication_year')
        primary_affs = row.get('author_primary_affiliations', [])

        paper_big_tech_companies = set()

        for aff in primary_affs:
            if not aff:
                continue

            is_bt, company = is_big_tech(aff)
            if is_bt:
                paper_big_tech_companies.add(company)
                big_tech_by_company[company] += 1

        if paper_big_tech_companies:
            big_tech_papers += 1
            if year:
                for company in paper_big_tech_companies:
                    big_tech_by_year[year][company] += 1

    logger.info(f"\n✓ Big Tech papers: {big_tech_papers:,} ({big_tech_papers/total_papers*100:.1f}%)")

    logger.info(f"\nTop Big Tech by paper count:")
    top_big_tech = sorted(big_tech_by_company.items(), key=lambda x: x[1], reverse=True)[:20]
    for i, (company, count) in enumerate(top_big_tech, 1):
        logger.info(f"  {i:2d}. {company}: {count:,} papers")

    return {
        'big_tech_papers': big_tech_papers,
        'big_tech_percentage': big_tech_papers / total_papers * 100,
        'by_company': dict(big_tech_by_company),
        'by_year': dict(big_tech_by_year)
    }


# ============================================================================
# Analysis 3: Time-Series by Year
# ============================================================================

def analyze_time_series():
    """Analyze firm publications over time."""
    logger.info("\n" + "=" * 80)
    logger.info("ANALYSIS 3: TIME-SERIES BY YEAR")
    logger.info("=" * 80)

    df = pl.read_parquet(INPUT_PARQUET)

    # Aggregate by year
    yearly_stats = df.group_by('publication_year').agg([
        pl.len().alias('total_papers'),
        pl.col('affiliations_firm_count').sum().alias('total_firm_affiliations'),
        pl.col('affiliations_university_count').sum().alias('total_university_affiliations'),
        pl.col('affiliations_government_count').sum().alias('total_gov_affiliations'),
        pl.col('cited_by_count').mean().alias('avg_citations'),
        pl.col('is_open_access').sum().alias('open_access_count')
    ]).sort('publication_year')

    logger.info(f"\nYearly statistics (last 20 years):")
    logger.info(f"{'Year':<6} {'Papers':>10} {'Firm Affs':>12} {'Avg Citations':>14} {'OA%':>8}")
    logger.info("-" * 60)

    for row in yearly_stats.iter_rows(named=True):
        if row['publication_year'] >= 2005:
            year = row['publication_year']
            papers = row['total_papers']
            firm_affs = row['total_firm_affiliations']
            avg_cit = row['avg_citations']
            oa_pct = (row['open_access_count'] / papers * 100) if papers > 0 else 0

            logger.info(f"{int(year):<6} {papers:>10,} {firm_affs:>12,} {avg_cit:>14.1f} {oa_pct:>7.1f}%")

    return yearly_stats.to_dict(as_series=False)


# ============================================================================
# Main Execution
# ================================================================================

def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("COMPREHENSIVE FIRM PAPERS ANALYSIS")
    logger.info("=" * 80)

    results = {}

    # Analysis 1: US Firms
    results['us_firms'] = analyze_us_firms()

    # Analysis 2: Big Tech
    results['big_tech'] = analyze_big_tech()

    # Analysis 3: Time Series
    results['time_series'] = analyze_time_series()

    # Save results
    logger.info("\n" + "=" * 80)
    logger.info("SAVING RESULTS")
    logger.info("=" * 80)

    with open(OUTPUT_JSON, 'w') as f:
        json.dump(results, f, indent=2)
    logger.info(f"  Saved JSON to {OUTPUT_JSON}")

    # Create text report
    with open(OUTPUT_REPORT, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("COMPREHENSIVE FIRM PAPERS ANALYSIS REPORT\n")
        f.write("=" * 80 + "\n\n")

        f.write("1. US FIRM ANALYSIS\n")
        f.write("-" * 40 + "\n")
        f.write(f"US firm-affiliated papers: {results['us_firms']['us_firm_papers']:,}\n")
        f.write(f"Percentage of all firm papers: {results['us_firms']['us_firm_percentage']:.1f}%\n")
        f.write(f"Unique US firms: {results['us_firms']['unique_us_firms']:,}\n")
        f.write(f"Total US firm affiliations: {results['us_firms']['total_us_affiliations']:,}\n\n")

        f.write("2. BIG TECH ANALYSIS\n")
        f.write("-" * 40 + "\n")
        f.write(f"Big Tech papers: {results['big_tech']['big_tech_papers']:,}\n")
        f.write(f"Percentage of all firm papers: {results['big_tech']['big_tech_percentage']:.1f}%\n\n")

        f.write("Top Big Tech by paper count:\n")
        sorted_companies = sorted(results['big_tech']['by_company'].items(),
                                  key=lambda x: x[1], reverse=True)[:20]
        for i, (company, count) in enumerate(sorted_companies, 1):
            f.write(f"  {i:2d}. {company}: {count:,} papers\n")

    logger.info(f"  Saved report to {OUTPUT_REPORT}")

    logger.info("\n" + "=" * 80)
    logger.info("ANALYSIS COMPLETED")
    logger.info("=" * 80)

    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY RESULTS")
    print("=" * 80)
    print(f"\n1. US FIRM ANALYSIS")
    print(f"   ✓ US firm papers: {results['us_firms']['us_firm_papers']:,}")
    print(f"   ✓ Percentage: {results['us_firms']['us_firm_percentage']:.1f}%")
    print(f"   ✓ Unique US firms: {results['us_firms']['unique_us_firms']:,}")

    print(f"\n2. BIG TECH ANALYSIS")
    print(f"   ✓ Big Tech papers: {results['big_tech']['big_tech_papers']:,}")
    print(f"   ✓ Percentage: {results['big_tech']['big_tech_percentage']:.1f}%")
    print(f"   ✓ Top companies:")
    for i, (company, count) in enumerate(sorted_companies[:10], 1):
        print(f"      {i}. {company}: {count:,}")


if __name__ == "__main__":
    main()
