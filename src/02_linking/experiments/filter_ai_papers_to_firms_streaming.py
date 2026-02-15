"""
Filter AI Papers Dataset to Firm-Affiliated Papers Only (Streaming Version)

This script processes the ai_papers_condensed.parquet dataset in streaming mode,
filtering to only include papers that have firm (company) affiliations.

Date: 2025
"""

import polars as pl
from pathlib import Path
import logging
from datetime import datetime
import json
import re
from typing import List, Tuple
import gc

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "publication"
LOGS_DIR = PROJECT_ROOT / "logs"

INPUT_PARQUET = DATA_PROCESSED / "ai_papers_condensed.parquet"
OUTPUT_PARQUET = OUTPUT_DIR / "ai_papers_firms_only.parquet"
STATISTICS_FILE = OUTPUT_DIR / "ai_papers_firm_filtering_stats.json"
SUMMARY_REPORT = LOGS_DIR / "ai_papers_firm_filtering_report.txt"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "filter_ai_papers_to_firms_streaming.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Processing configuration
BATCH_SIZE = 100000  # Process 100k rows at a time
MIN_FIRM_RATIO = 0.5
REQUIRE_AT_LEAST_ONE_FIRM = True

# ============================================================================
# Affiliation Classification Keywords
# ============================================================================
UNIVERSITY_KEYWORDS = [
    'university', 'college', 'institute of technology', 'polytechnic',
    'school of', 'academy', 'faculty of', 'department of',
    'lab for', 'laboratory for', 'center for', 'centre for',
    'graduate school', 'medical school', 'law school', 'business school',
    'autonomous university', 'state university', 'national university',
    'technical university', 'technological university',
    'école', 'université', 'universidad', 'universita'
]

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

COMPANY_SUFFIXES = [
    'inc', 'corp', 'llc', 'ltd', 'plc', 'gmbh', 'ag', 'sa', 'spa',
    'pty', 'bv', 'nv', 'aps', 'sro', 'zoo', 'kft', 'oao', 'ooo'
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

# ============================================================================
# Classification Functions
# ============================================================================

def normalize_affiliation_name(name: str) -> str:
    if not name or name == "":
        return ""
    return name.lower().strip()


def classify_affiliation(affiliation: str) -> str:
    """Classify a single affiliation."""
    if not affiliation or affiliation == "":
        return 'unknown'

    normalized = normalize_affiliation_name(affiliation)

    # Priority 1: Government
    for keyword in GOVERNMENT_KEYWORDS:
        if keyword in normalized:
            return 'government'

    # Priority 2: Strong university indicators
    if ('university' in normalized or 'college' in normalized or
        'école' in normalized or 'université' in normalized or
        'universidad' in normalized or 'universita' in normalized):
        return 'university'

    # Priority 3: Educational indicators
    for indicator in EDUCATIONAL_INDICATORS:
        if indicator in normalized:
            return 'university'

    # Priority 4: Known company research (with word boundaries)
    for known_org in KNOWN_COMPANY_RESEARCH:
        pattern = r'\b' + re.escape(known_org) + r'\b'
        if re.search(pattern, normalized):
            return 'firm'

    # Priority 5: Company suffixes
    for suffix in COMPANY_SUFFIXES:
        if normalized.endswith(suffix) or f' {suffix}' in normalized:
            return 'firm'

    # Priority 6: Ambiguous keywords
    for keyword in ['institute', 'lab', 'laboratory', 'center', 'centre']:
        if keyword in normalized:
            for known_org in KNOWN_COMPANY_RESEARCH:
                pattern = r'\b' + re.escape(known_org) + r'\b'
                if re.search(pattern, normalized):
                    return 'firm'
            if (normalized.startswith(keyword) or
                f'{keyword} for' in normalized or
                any(fw in normalized for fw in ['technology', 'analytics', 'intelligence', 'systems', 'solutions'])):
                return 'firm'
            return 'university'

    # Priority 7: Firm keywords
    for keyword in FIRM_KEYWORDS:
        if keyword in normalized:
            return 'firm'

    return 'unknown'


def extract_all_affiliations_from_row(row: dict) -> List[str]:
    """Extract all unique affiliations from a single paper row."""
    affiliations = []

    # Get primary affiliations
    primary_affs = row.get('author_primary_affiliations', [])
    if primary_affs:
        for aff in primary_affs:
            if aff and aff != "":
                affiliations.append(aff)

    # Get all affiliations from nested lists
    all_affs = row.get('author_affiliations', [])
    if all_affs:
        for aff_list in all_affs:
            if aff_list:
                for aff in aff_list:
                    if aff and aff != "":
                        affiliations.append(aff)

    # Remove duplicates while preserving order
    seen = set()
    unique_affs = []
    for aff in affiliations:
        if aff not in seen:
            seen.add(aff)
            unique_affs.append(aff)

    return unique_affs


def classify_paper_affiliations(affiliations: List[str]) -> dict:
    """Classify all affiliations for a paper."""
    result = {
        'total_affiliations': len(affiliations),
        'firm_count': 0,
        'university_count': 0,
        'government_count': 0,
        'unknown_count': 0,
        'has_firm': False,
        'has_university': False,
        'has_government': False,
        'firm_ratio': 0.0,
        'primary_classification': 'unknown'
    }

    if len(affiliations) == 0:
        return result

    for aff in affiliations:
        classification = classify_affiliation(aff)
        result[f'{classification}_count'] += 1

        if classification == 'firm':
            result['has_firm'] = True
        elif classification == 'university':
            result['has_university'] = True
        elif classification == 'government':
            result['has_government'] = True

    # Calculate firm ratio
    if result['total_affiliations'] > 0:
        result['firm_ratio'] = result['firm_count'] / result['total_affiliations']

    # Determine primary classification
    counts = {
        'firm': result['firm_count'],
        'university': result['university_count'],
        'government': result['government_count'],
        'unknown': result['unknown_count']
    }
    result['primary_classification'] = max(counts, key=counts.get)

    return result


# ============================================================================
# Main Execution
# ================================================================================

def main():
    """Main execution function with streaming processing."""
    logger.info("=" * 80)
    logger.info("AI PAPERS FIRM FILTERING PIPELINE (STREAMING)")
    logger.info("=" * 80)
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Get total row count
    logger.info(f"\nGetting total row count...")
    total_rows = pl.scan_parquet(INPUT_PARQUET).select(pl.len()).collect().item()
    logger.info(f"Total papers to process: {total_rows:,}")

    # Process in batches using scan_parquet with slicing
    logger.info(f"\nProcessing in batches of {BATCH_SIZE:,} papers...")
    logger.info(f"Filtering criteria:")
    logger.info(f"  - Minimum firm ratio: {MIN_FIRM_RATIO:.0%}")
    logger.info(f"  - Require at least one firm: {REQUIRE_AT_LEAST_ONE_FIRM}")

    all_firm_batches = []
    total_processed = 0
    start_time = datetime.now()

    # Process using scan_parquet with slice for each batch
    for batch_start in range(0, total_rows, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_rows)
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE

        logger.info(f"\nProcessing batch {batch_num}/{total_batches} ({batch_start:,} - {batch_end:,})...")

        # Read this batch using slice
        batch_df = (
            pl.scan_parquet(INPUT_PARQUET)
            .slice(batch_start, batch_end - batch_start)
            .collect()
        )
        logger.info(f"  Loaded {len(batch_df):,} papers")

        # Classify affiliations
        results = []
        for i, row in enumerate(batch_df.iter_rows(named=True)):
            if (i + 1) % 20000 == 0:
                logger.info(f"    Classified {i+1:,}/{len(batch_df):,} papers...")

            affiliations = extract_all_affiliations_from_row(row)
            classification = classify_paper_affiliations(affiliations)
            results.append(classification)

        # Add classification columns
        classified_batch = batch_df.with_columns([
            pl.Series([r['total_affiliations'] for r in results]).alias('affiliations_total'),
            pl.Series([r['firm_count'] for r in results]).alias('affiliations_firm_count'),
            pl.Series([r['university_count'] for r in results]).alias('affiliations_university_count'),
            pl.Series([r['government_count'] for r in results]).alias('affiliations_government_count'),
            pl.Series([r['unknown_count'] for r in results]).alias('affiliations_unknown_count'),
            pl.Series([r['has_firm'] for r in results]).alias('has_firm_affiliation'),
            pl.Series([r['has_university'] for r in results]).alias('has_university_affiliation'),
            pl.Series([r['has_government'] for r in results]).alias('has_government_affiliation'),
            pl.Series([r['firm_ratio'] for r in results]).alias('firm_affiliation_ratio'),
            pl.Series([r['primary_classification'] for r in results]).alias('primary_institution_type')
        ])

        # Filter for firm papers
        firm_batch = classified_batch.filter(
            (pl.col('has_firm_affiliation') == True) &
            (pl.col('firm_affiliation_ratio') >= MIN_FIRM_RATIO)
        )

        logger.info(f"  Batch retained {len(firm_batch):,} firm papers ({len(firm_batch)/len(batch_df)*100:.1f}%)")

        all_firm_batches.append(firm_batch)
        total_processed = batch_end

        # Progress report
        elapsed = (datetime.now() - start_time).total_seconds()
        rate = total_processed / elapsed if elapsed > 0 else 0
        remaining = (total_rows - total_processed) / rate if rate > 0 else 0
        logger.info(f"  Progress: {total_processed/total_rows*100:.1f}% | Rate: {rate:,.0f} papers/sec | ETA: {remaining/60:.1f} min")

        # Clean up memory
        del batch_df, classified_batch, results
        gc.collect()

    # Combine all batches
    logger.info(f"\nCombining all batches...")
    firm_papers_df = pl.concat(all_firm_batches)
    logger.info(f"Combined {len(firm_papers_df):,} firm papers")

    # Save filtered dataset
    logger.info(f"\nSaving filtered dataset to {OUTPUT_PARQUET}")
    firm_papers_df.write_parquet(OUTPUT_PARQUET, compression='snappy')
    logger.info(f"  Saved {len(firm_papers_df):,} firm papers")

    # Calculate statistics
    stats = {
        'total_papers': total_rows,
        'firm_papers': len(firm_papers_df),
        'filtering_rate': len(firm_papers_df) / total_rows,
        'min_firm_ratio_threshold': MIN_FIRM_RATIO,
        'require_at_least_one_firm': REQUIRE_AT_LEAST_ONE_FIRM,
        'processing_time_seconds': (datetime.now() - start_time).total_seconds()
    }

    # Save statistics
    with open(STATISTICS_FILE, 'w') as f:
        json.dump(stats, f, indent=2)
    logger.info(f"  Saved statistics to {STATISTICS_FILE}")

    # Final summary
    logger.info("\n" + "=" * 80)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)
    logger.info(f"\nResults:")
    logger.info(f"  Total papers processed: {stats['total_papers']:,}")
    logger.info(f"  Firm-affiliated papers: {stats['firm_papers']:,}")
    logger.info(f"  Filtering rate: {stats['filtering_rate']:.2%}")
    logger.info(f"  Processing time: {stats['processing_time_seconds']/60:.1f} minutes")

    logger.info(f"\nOutput files:")
    logger.info(f"  - Filtered dataset: {OUTPUT_PARQUET}")
    logger.info(f"  - Statistics JSON: {STATISTICS_FILE}")


if __name__ == "__main__":
    main()
