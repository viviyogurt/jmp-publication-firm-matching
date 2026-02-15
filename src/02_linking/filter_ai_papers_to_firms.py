"""
Filter AI Papers Dataset to Firm-Affiliated Papers Only

This script processes the ai_papers_condensed.parquet dataset and filters it to only
include papers that have firm (company) affiliations, excluding universities and
government institutions.

Key features:
1. Classifies affiliations as firms, universities, or government using keyword matching
2. Handles nested list structure of affiliations
3. Filters papers based on proportion of firm affiliations
4. Generates detailed statistics on filtering results
5. Saves filtered dataset with metadata

Date: 2025
"""

import polars as pl
from pathlib import Path
import logging
from datetime import datetime
import json
import re
from typing import List, Tuple, Optional

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "publication"
LOGS_DIR = PROJECT_ROOT / "logs"

# Ensure output directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Input/Output files
INPUT_PARQUET = DATA_PROCESSED / "ai_papers_condensed.parquet"
OUTPUT_PARQUET = OUTPUT_DIR / "ai_papers_firms_only.parquet"
STATISTICS_FILE = OUTPUT_DIR / "ai_papers_firm_filtering_stats.json"
SUMMARY_REPORT = LOGS_DIR / "ai_papers_firm_filtering_report.txt"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "filter_ai_papers_to_firms.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Affiliation Classification Keywords
# ============================================================================

# Keywords that suggest a university/academic institution
UNIVERSITY_KEYWORDS = [
    'university', 'college', 'institute of technology', 'polytechnic',
    'school of', 'academy', 'faculty of', 'department of',
    'lab for', 'laboratory for', 'center for', 'centre for',
    'graduate school', 'medical school', 'law school', 'business school',
    'autonomous university', 'state university', 'national university',
    'technical university', 'technological university',
    'école', 'université', 'universidad', 'universita'
]

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
    'vmware research', ' Broadcom research', 'amd research',
    'arm research', 'qualcomm technologies', 'bell labs',
    'xerox parc', 'parc', 'yahoo research', 'yandex research',
    'adobe labs', 'google labs', 'amazon labs', 'microsoft labs',
    'facebook research', 'meta research', 'nvidia labs',
    'intel labs', 'ibm thomas', 'ibm watson', 'alibaba labs'
]

# Keywords that suggest a company/firm
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

# Company suffixes/prefixes that override university classification
COMPANY_SUFFIXES = [
    'inc', 'corp', 'llc', 'ltd', 'plc', 'gmbh', 'ag', 'sa', 'spa',
    'pty', 'bv', 'nv', 'aps', 'sro', 'zoo', 'kft', 'oao', 'ooo'
]

# Keywords that suggest government/national labs
GOVERNMENT_KEYWORDS = [
    'national laboratory', 'national lab', 'naval research', 'army research',
    'air force', 'defense', 'government', 'federal',
    'national institute of', 'national aeronautics and space',
    'national science foundation', 'department of energy',
    'department of defense', 'ministry of'
]

# Educational indicators (strong university signal)
EDUCATIONAL_INDICATORS = [
    'edu', 'ac.', 'ac.uk', 'edu.', 'univ.', 'college.',
    'graduate', 'undergraduate', 'phd', 'doctoral',
    'professor', 'lecturer', 'faculty'
]

# ================================================================================
# Affiliation Classification Functions
# ================================================================================

def normalize_affiliation_name(name: str) -> str:
    """
    Normalize affiliation name for classification.
    Convert to lowercase, strip whitespace, remove special characters.
    """
    if not name or name == "":
        return ""
    return name.lower().strip()


def classify_affiliation(affiliation: str) -> str:
    """
    Classify a single affiliation as 'firm', 'university', 'government', or 'unknown'.

    Classification priority:
    1. Government institutions (highest priority for correct classification)
    2. Strong university indicators (university, college, etc.) - check BEFORE firm indicators
    3. Educational indicators (strong university signal)
    4. Known company research organizations (with word boundaries to avoid false matches)
    5. Company suffixes (inc, corp, llc, etc.) - overrides "institute"
    6. Ambiguous keywords (institute, lab, center) with additional checks
    7. Firm keywords

    Parameters:
    -----------
    affiliation : str
        The affiliation string to classify

    Returns:
    --------
    str
        One of: 'firm', 'university', 'government', 'unknown'
    """
    if not affiliation or affiliation == "":
        return 'unknown'

    normalized = normalize_affiliation_name(affiliation)

    # Priority 1: Check for government first (highest priority)
    for keyword in GOVERNMENT_KEYWORDS:
        if keyword in normalized:
            return 'government'

    # Priority 2: Strong university indicators (check BEFORE firm indicators)
    # This prevents universities with "research" from being misclassified
    if ('university' in normalized or 'college' in normalized or
        'école' in normalized or 'université' in normalized or
        'universidad' in normalized or 'universita' in normalized):
        return 'university'

    # Priority 3: Check for educational indicators (strong university signal)
    for indicator in EDUCATIONAL_INDICATORS:
        if indicator in normalized:
            return 'university'

    # Priority 4: Check for known company research organizations (with word boundaries)
    # Using word boundaries avoids false matches like "Berkeley" matching "ey"
    for known_org in KNOWN_COMPANY_RESEARCH:
        pattern = r'\b' + re.escape(known_org) + r'\b'
        if re.search(pattern, normalized):
            return 'firm'

    # Priority 5: Check for company suffixes (overrides "institute" keyword)
    # This handles cases like "SAS Institute Inc"
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
            if (normalized.startswith(keyword) or  # e.g., "Institute for..." at start
                f'{keyword} for' in normalized or
                any(fw in normalized for fw in ['technology', 'analytics', 'intelligence', 'systems', 'solutions'])):
                return 'firm'
            # Otherwise classify as university/academic
            return 'university'

    # Priority 7: Check for firm keywords
    for keyword in FIRM_KEYWORDS:
        if keyword in normalized:
            return 'firm'

    return 'unknown'


def extract_all_affiliations_from_row(row: pl.DataFrame) -> List[str]:
    """
    Extract all unique affiliations from a single paper row.

    Combines both primary affiliations and all affiliations from nested lists.

    Parameters:
    -----------
    row : pl.DataFrame
        Single row from the dataset

    Returns:
    --------
    List[str]
        List of unique affiliation strings
    """
    affiliations = []

    # Get primary affiliations
    primary_affs = row['author_primary_affiliations'][0]
    if primary_affs:
        for aff in primary_affs:
            if aff and aff != "":
                affiliations.append(aff)

    # Get all affiliations from nested lists
    all_affs = row['author_affiliations'][0]
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
    """
    Classify all affiliations for a paper and count by type.

    Parameters:
    -----------
    affiliations : List[str]
        List of affiliation strings

    Returns:
    --------
    dict
        Dictionary with counts and classification results:
        {
            'total_affiliations': int,
            'firm_count': int,
            'university_count': int,
            'government_count': int,
            'unknown_count': int,
            'has_firm': bool,
            'has_university': bool,
            'has_government': bool,
            'firm_ratio': float,
            'primary_classification': str
        }
    """
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

    # Determine primary classification (most common type)
    counts = {
        'firm': result['firm_count'],
        'university': result['university_count'],
        'government': result['government_count'],
        'unknown': result['unknown_count']
    }
    result['primary_classification'] = max(counts, key=counts.get)

    return result


# ================================================================================
# Main Processing Functions
# ================================================================================

def process_batch(batch: pl.DataFrame) -> pl.DataFrame:
    """
    Process a batch of papers to classify affiliations and filter for firms.

    Parameters:
    -----------
    batch : pl.DataFrame
        Batch of papers to process

    Returns:
    --------
    pl.DataFrame
        Processed batch with new classification columns
    """
    logger.info(f"Processing batch of {len(batch):,} papers...")

    # Extract affiliations and classify each paper
    results = []
    for row in batch.iter_rows(named=True):
        affiliations = extract_all_affiliations_from_row(pl.DataFrame([row]))
        classification = classify_paper_affiliations(affiliations)
        results.append(classification)

    # Add classification columns to the dataframe
    result_df = batch.with_columns([
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

    return result_df


def filter_firm_papers(
    df: pl.DataFrame,
    min_firm_ratio: float = 0.5,
    require_at_least_one_firm: bool = True
) -> Tuple[pl.DataFrame, dict]:
    """
    Filter papers to only those with firm affiliations.

    Parameters:
    -----------
    df : pl.DataFrame
        Papers dataset with classification columns
    min_firm_ratio : float
        Minimum ratio of firm affiliations to include (0.0 to 1.0)
    require_at_least_one_firm : bool
        If True, require at least one firm affiliation

    Returns:
    --------
    Tuple[pl.DataFrame, dict]
        Filtered dataframe and statistics dictionary
    """
    logger.info("\n" + "=" * 80)
    logger.info("FILTERING FOR FIRM-AFFILIATED PAPERS")
    logger.info("=" * 80)

    total_papers = len(df)

    # Build filter conditions
    if require_at_least_one_firm:
        filtered_df = df.filter(
            (pl.col('has_firm_affiliation') == True) &
            (pl.col('firm_affiliation_ratio') >= min_firm_ratio)
        )
    else:
        filtered_df = df.filter(
            pl.col('firm_affiliation_ratio') >= min_firm_ratio
        )

    firm_papers = len(filtered_df)

    # Calculate statistics
    stats = {
        'total_papers': total_papers,
        'firm_papers': firm_papers,
        'filtering_rate': firm_papers / total_papers if total_papers > 0 else 0,
        'papers_with_at_least_one_firm': df.filter(pl.col('has_firm_affiliation') == True).shape[0],
        'papers_with_only_universities': df.filter(
            (pl.col('has_university_affiliation') == True) &
            (pl.col('has_firm_affiliation') == False)
        ).shape[0],
        'papers_with_government': df.filter(pl.col('has_government_affiliation') == True).shape[0],
        'papers_no_affiliations': df.filter(pl.col('affiliations_total') == 0).shape[0],
        'min_firm_ratio_threshold': min_firm_ratio,
        'require_at_least_one_firm': require_at_least_one_firm
    }

    logger.info(f"\nFiltering Results:")
    logger.info(f"  Total papers: {stats['total_papers']:,}")
    logger.info(f"  Papers with firm affiliations: {stats['firm_papers']:,}")
    logger.info(f"  Filtering rate: {stats['filtering_rate']:.2%}")
    logger.info(f"  Papers with at least one firm: {stats['papers_with_at_least_one_firm']:,}")
    logger.info(f"  Papers with only universities: {stats['papers_with_only_universities']:,}")
    logger.info(f"  Papers with government: {stats['papers_with_government']:,}")
    logger.info(f"  Papers with no affiliations: {stats['papers_no_affiliations']:,}")

    return filtered_df, stats


def generate_summary_statistics(df: pl.DataFrame) -> dict:
    """
    Generate comprehensive summary statistics for the filtered dataset.

    Parameters:
    -----------
    df : pl.DataFrame
        Filtered firm-affiliated papers dataset

    Returns:
    --------
    dict
        Dictionary of summary statistics
    """
    logger.info("\n" + "=" * 80)
    logger.info("GENERATING SUMMARY STATISTICS")
    logger.write("=" * 80)

    stats = {
        'dataset_info': {
            'total_papers': len(df),
            'total_columns': len(df.columns),
            'file_size_mb': OUTPUT_PARQUET.stat().st_size / (1024 * 1024) if OUTPUT_PARQUET.exists() else 0
        },
        'temporal_coverage': {
            'min_year': int(df['publication_year'].min()),
            'max_year': int(df['publication_year'].max()),
            'year_distribution': df.group_by('publication_year').agg(
                pl.len().alias('count')
            ).sort('publication_year').to_dicts()
        },
        'affiliation_stats': {
            'mean_affiliations_per_paper': float(df['affiliations_total'].mean()),
            'mean_firm_affiliations_per_paper': float(df['affiliations_firm_count'].mean()),
            'mean_university_affiliations_per_paper': float(df['affiliations_university_count'].mean()),
            'mean_firm_ratio': float(df['firm_affiliation_ratio'].mean()),
            'median_firm_ratio': float(df['firm_affiliation_ratio'].median())
        },
        'top_venues': df.group_by('venue_name').agg(
            pl.len().alias('paper_count')
        ).sort('paper_count', descending=True).head(20).to_dicts(),
        'top_publishers': df.group_by('publisher').agg(
            pl.len().alias('paper_count')
        ).sort('paper_count', descending=True).head(20).to_dicts(),
        'ai_classification': {
            'is_computer_vision': int(df['is_computer_vision'].sum()),
            'is_deep_learning': int(df['is_deep_learning'].sum()),
            'is_machine_learning': int(df['is_machine_learning'].sum()),
            'is_nlp': int(df['is_nlp'].sum()),
            'is_llm': int(df['is_llm'].sum()),
            'is_reinforcement_learning': int(df['is_reinforcement_learning'].sum())
        }
    }

    logger.info(f"\nDataset Summary:")
    logger.info(f"  Total papers: {stats['dataset_info']['total_papers']:,}")
    logger.info(f"  Year range: {stats['temporal_coverage']['min_year']} - {stats['temporal_coverage']['max_year']}")
    logger.info(f"  Mean affiliations per paper: {stats['affiliation_stats']['mean_affiliations_per_paper']:.2f}")
    logger.info(f"  Mean firm ratio: {stats['affiliation_stats']['mean_firm_ratio']:.2%}")

    return stats


# ================================================================================
# Main Execution
# ================================================================================

def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("AI PAPERS FIRM FILTERING PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Configuration
    BATCH_SIZE = 50000  # Process 50k papers at a time
    MIN_FIRM_RATIO = 0.5  # Require at least 50% firm affiliations
    REQUIRE_AT_LEAST_ONE_FIRM = True  # Must have at least one firm affiliation

    # Step 1: Load the dataset
    logger.info(f"\nStep 1: Loading dataset from {INPUT_PARQUET}")
    logger.info(f"File size: {INPUT_PARQUET.stat().st_size / (1024**3):.2f} GB")

    # Get total rows without loading full dataset
    total_rows = pl.scan_parquet(INPUT_PARQUET).select(pl.len()).collect().item()
    logger.info(f"Total papers to process: {total_rows:,}")

    # Step 2: Process in batches
    logger.info(f"\nStep 2: Processing in batches of {BATCH_SIZE:,} papers...")
    logger.info(f"Filtering criteria:")
    logger.info(f"  - Minimum firm ratio: {MIN_FIRM_RATIO:.0%}")
    logger.info(f"  - Require at least one firm: {REQUIRE_AT_LEAST_ONE_FIRM}")

    processed_batches = []
    total_processed = 0

    # Use lazy frame with streaming for memory efficiency
    lazy_df = pl.scan_parquet(INPUT_PARQUET)

    # Process in batches
    for i, batch in enumerate(lazy_df.collect().iter_slices(n_rows=BATCH_SIZE)):
        logger.info(f"\nProcessing batch {i+1} ({total_processed:,} - {total_processed + len(batch):,})...")

        # Classify affiliations
        classified_batch = process_batch(batch)

        # Filter for firm papers
        firm_batch, batch_stats = filter_firm_papers(
            classified_batch,
            min_firm_ratio=MIN_FIRM_RATIO,
            require_at_least_one_firm=REQUIRE_AT_LEAST_ONE_FIRM
        )

        processed_batches.append(firm_batch)
        total_processed += len(batch)

        logger.info(f"  Batch {i+1} retained {len(firm_batch):,} firm papers ({batch_stats['filtering_rate']:.1%})")

    # Step 3: Combine all batches
    logger.info(f"\nStep 3: Combining all batches...")
    firm_papers_df = pl.concat(processed_batches)

    # Step 4: Save filtered dataset
    logger.info(f"\nStep 4: Saving filtered dataset to {OUTPUT_PARQUET}")
    firm_papers_df.write_parquet(OUTPUT_PARQUET, compression='snappy')
    logger.info(f"  Saved {len(firm_papers_df):,} firm-affiliated papers")

    # Step 5: Generate summary statistics
    logger.info(f"\nStep 5: Generating summary statistics...")
    summary_stats = generate_summary_statistics(firm_papers_df)

    # Step 6: Save statistics and report
    logger.info(f"\nStep 6: Saving statistics and report...")

    # Save JSON statistics
    stats_to_save = {
        'metadata': {
            'generated_at': datetime.now().isoformat(),
            'input_file': str(INPUT_PARQUET),
            'output_file': str(OUTPUT_PARQUET),
            'filtering_parameters': {
                'min_firm_ratio': MIN_FIRM_RATIO,
                'require_at_least_one_firm': REQUIRE_AT_LEAST_ONE_FIRM
            }
        },
        'statistics': summary_stats
    }

    with open(STATISTICS_FILE, 'w') as f:
        json.dump(stats_to_save, f, indent=2)
    logger.info(f"  Saved statistics to {STATISTICS_FILE}")

    # Save text report
    with open(SUMMARY_REPORT, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("AI PAPERS FIRM FILTERING REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write("FILTERING PARAMETERS\n")
        f.write("-" * 40 + "\n")
        f.write(f"Input file: {INPUT_PARQUET}\n")
        f.write(f"Output file: {OUTPUT_PARQUET}\n")
        f.write(f"Minimum firm ratio: {MIN_FIRM_RATIO:.0%}\n")
        f.write(f"Require at least one firm: {REQUIRE_AT_LEAST_ONE_FIRM}\n\n")

        f.write("RESULTS\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total papers processed: {total_rows:,}\n")
        f.write(f"Firm-affiliated papers: {len(firm_papers_df):,}\n")
        f.write(f"Filtering rate: {len(firm_papers_df) / total_rows:.2%}\n\n")

        f.write("AFFILIATION STATISTICS\n")
        f.write("-" * 40 + "\n")
        f.write(f"Mean affiliations per paper: {summary_stats['affiliation_stats']['mean_affiliations_per_paper']:.2f}\n")
        f.write(f"Mean firm affiliations per paper: {summary_stats['affiliation_stats']['mean_firm_affiliations_per_paper']:.2f}\n")
        f.write(f"Mean firm ratio: {summary_stats['affiliation_stats']['mean_firm_ratio']:.2%}\n\n")

        f.write("TEMPORAL COVERAGE\n")
        f.write("-" * 40 + "\n")
        f.write(f"Year range: {summary_stats['temporal_coverage']['min_year']} - {summary_stats['temporal_coverage']['max_year']}\n\n")

    logger.info(f"  Saved report to {SUMMARY_REPORT}")

    logger.info("\n" + "=" * 80)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)
    logger.info(f"\nOutput files:")
    logger.info(f"  - Filtered dataset: {OUTPUT_PARQUET}")
    logger.info(f"  - Statistics JSON: {STATISTICS_FILE}")
    logger.info(f"  - Summary report: {SUMMARY_REPORT}")
    logger.info(f"\nFinished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
