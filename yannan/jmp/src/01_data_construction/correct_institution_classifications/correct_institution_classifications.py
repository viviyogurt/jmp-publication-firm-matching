"""
Correct Institution Classifications Using OpenAlex API
=========================================================

This script corrects the institution classifications in the AI papers dataset
by using OpenAlex's official institution types instead of keyword matching.

Key Features:
- Processes each institution separately for each paper
- Calculates firm_count and firm_ratio at the paper level
- Uses OpenAlex API to get official institution types
- Handles multiple authors with multiple affiliations per paper

Author: JMP Research Team
Date: 2025-02-08
"""

import requests
import pyarrow.parquet as pq
import pandas as pd
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import time
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# =============================================================================
# CONFIGURATION
# =============================================================================

OPENALEX_API_BASE = "https://api.openalex.org"
DATA_DIR = Path("/home/kurtluo/yannan/jmp/data")
INPUT_PARQUET = DATA_DIR / "processed/publication/ai_papers_condensed.parquet"
OUTPUT_PARQUET = DATA_DIR / "processed/publication/ai_papers_with_correct_classifications.parquet"

# OpenAlex institution type mapping
TYPE_MAPPING = {
    'company': 'FIRM',
    'education': 'UNIVERSITY',
    'government': 'GOVERNMENT',
    'nonprofit': 'NONPROFIT',
    'facility': 'FACILITY',
    'archive': 'ARCHIVE',
    'healthcare': 'HEALTHCARE',
    'other': 'OTHER',
}

# =============================================================================
# OPENALEX API FUNCTIONS
# =============================================================================

def get_institution_from_openalex(openalex_id: str) -> Optional[Dict]:
    """
    Get institution data from OpenAlex API.

    Args:
        openalex_id: OpenAlex institution ID (full URL or just ID part)

    Returns:
        Institution data dict or None if error
    """
    # Extract numeric ID if full URL
    if openalex_id.startswith('http'):
        openalex_id = openalex_id.split('/')[-1]

    url = f"{OPENALEX_API_BASE}/institutions/{openalex_id}"

    try:
        response = requests.get(url, timeout=5)

        if response.status_code == 200:
            data = response.json()
            return {
                'openalex_id': openalex_id,
                'name': data.get('display_name', 'Unknown'),
                'type': data.get('type', 'unknown'),
                'country': data.get('country_code', 'Unknown'),
                'ror': data.get('ror'),
            }
        else:
            print(f"Warning: HTTP {response.status_code} for {openalex_id}")
            return None
    except Exception as e:
        print(f"Warning: Error fetching {openalex_id}: {e}")
        return None


def classify_institution_type(openalex_type: str) -> str:
    """
    Map OpenAlex institution type to our classification.

    Args:
        openalex_type: Type from OpenAlex API

    Returns:
        Our classification string
    """
    return TYPE_MAPPING.get(openalex_type, 'OTHER')


# =============================================================================
# PAPER-LEVEL CLASSIFICATION
# =============================================================================

def classify_paper_affiliations(
    author_affiliation_ids: List[List[str]],
    author_affiliations: List[List[str]]
) -> Dict:
    """
    Classify all affiliations for a paper.

    IMPORTANT: Each paper has multiple authors, each with multiple affiliations.
    We need to process each institution separately and calculate:

    - firm_count: Number of affiliations that are companies
    - total_count: Total number of affiliations
    - firm_ratio: firm_count / total_count
    - has_firm_affiliation: True if firm_count > 0

    Args:
        author_affiliation_ids: List of lists of OpenAlex IDs for each author
        author_affiliations: List of lists of institution names for each author

    Returns:
        Classification results dictionary
    """
    # Initialize counters
    firm_count = 0
    university_count = 0
    government_count = 0
    facility_count = 0
    other_count = 0
    total_count = 0

    # Track classifications per affiliation
    affiliation_classifications = []

    # Process each author's affiliations
    for author_idx, (aff_ids, aff_names) in enumerate(zip(author_affiliation_ids, author_affiliations)):
        for aff_idx, (aff_id, aff_name) in enumerate(zip(aff_ids, aff_names)):
            if not aff_id:  # Skip empty affiliations
                continue

            total_count += 1

            # Get institution data from OpenAlex
            inst_data = get_institution_from_openalex(aff_id)

            if inst_data:
                openalex_type = inst_data['type']
                classification = classify_institution_type(openalex_type)

                # Count by type
                if classification == 'FIRM':
                    firm_count += 1
                elif classification == 'UNIVERSITY':
                    university_count += 1
                elif classification == 'GOVERNMENT':
                    government_count += 1
                elif classification == 'FACILITY':
                    facility_count += 1
                else:
                    other_count += 1

                # Store classification
                affiliation_classifications.append({
                    'author_index': author_idx,
                    'affiliation_index': aff_idx,
                    'openalex_id': aff_id,
                    'institution_name': aff_name,
                    'openalex_type': openalex_type,
                    'classification': classification,
                })

            # Rate limiting
            time.sleep(0.05)  # 20 requests per second

    # Calculate firm ratio
    firm_ratio = firm_count / total_count if total_count > 0 else 0.0

    return {
        'firm_count': firm_count,
        'university_count': university_count,
        'government_count': government_count,
        'facility_count': facility_count,
        'other_count': other_count,
        'total_count': total_count,
        'firm_ratio': firm_ratio,
        'has_firm_affiliation': firm_count > 0,
        'has_university_affiliation': university_count > 0,
        'has_government_affiliation': government_count > 0,
        'affiliation_classifications': affiliation_classifications,
    }


# =============================================================================
# BATCH PROCESSING
# =============================================================================

def process_dataset_sample(
    input_path: str,
    output_path: str,
    num_papers: int = 1000
):
    """
    Process a sample of papers and add corrected classifications.

    Args:
        input_path: Path to input parquet file
        output_path: Path to output parquet file
        num_papers: Number of papers to process (for testing)
    """
    print(f"Loading dataset from {input_path}...")
    pf = pq.ParquetFile(input_path)

    print(f"Total row groups: {pf.num_row_groups}")

    # Read papers
    papers_processed = 0
    results = []

    for rg_idx in range(pf.num_row_groups):
        if papers_processed >= num_papers:
            break

        print(f"\nProcessing row group {rg_idx + 1}/{pf.num_row_groups}...")

        try:
            batch = pf.read_row_group(rg_idx, columns=[
                'paper_id',
                'openalex_id',
                'publication_year',
                'author_affiliation_ids',
                'author_affiliations',
            ])

            # Process each paper in this row group
            for i in range(len(batch['paper_id'])):
                if papers_processed >= num_papers:
                    break

                paper_id = batch['paper_id'][i].as_py()
                openalex_id = batch['openalex_id'][i].as_py()
                pub_year = batch['publication_year'][i].as_py()
                affiliation_ids = batch['author_affiliation_ids'][i].as_py()
                affiliation_names = batch['author_affiliations'][i].as_py()

                # Classify affiliations
                classification = classify_paper_affiliations(affiliation_ids, affiliation_names)

                # Store result
                results.append({
                    'paper_id': paper_id,
                    'openalex_id': openalex_id,
                    'publication_year': pub_year,
                    **classification
                })

                papers_processed += 1

                if papers_processed % 100 == 0:
                    print(f"  Processed {papers_processed}/{num_papers} papers...")

        except Exception as e:
            print(f"Error processing row group {rg_idx}: {e}")
            continue

    # Save results
    print(f"\nSaving {len(results)} papers to {output_path}...")
    df = pd.DataFrame(results)

    # Expand affiliation_classifications if needed
    df.to_parquet(output_path, index=False)

    # Print summary statistics
    print("\n" + "="*70)
    print("SUMMARY STATISTICS")
    print("="*70)
    print(f"Total papers processed: {len(results):,}")
    print(f"Papers with firm affiliations: {df['has_firm_affiliation'].sum():,} ({df['has_firm_affiliation'].sum()/len(df)*100:.1f}%)")
    print(f"Papers with university affiliations: {df['has_university_affiliation'].sum():,} ({df['has_university_affiliation'].sum()/len(df)*100:.1f}%)")
    print(f"Papers with government affiliations: {df['has_government_affiliation'].sum():,} ({df['has_government_affiliation'].sum()/len(df)*100:.1f}%)")

    print(f"\nFirm ratio statistics:")
    print(f"  Mean: {df['firm_ratio'].mean():.3f}")
    print(f"  Median: {df['firm_ratio'].median():.3f}")
    print(f"  Std: {df['firm_ratio'].std():.3f}")

    print(f"\nFirm count statistics:")
    print(f"  Mean: {df['firm_count'].mean():.2f}")
    print(f"  Median: {df['firm_count'].median():.0f}")
    print(f"  Max: {df['firm_count'].max():.0f}")

    return df


# =============================================================================
# DEMONSTRATION
# =============================================================================

def demonstrate_on_sample():
    """Demonstrate the classification on a small sample."""

    print("="*70)
    print("DEMONSTRATION: Correct Classification on Sample Papers")
    print("="*70)

    # Read a few papers
    pf = pq.ParquetFile(str(INPUT_PARQUET))
    batch = pf.read_row_group(0, columns=[
        'paper_id',
        'author_affiliation_ids',
        'author_affiliations',
    ])

    print("\nProcessing first 3 papers:\n")

    for idx in range(min(3, len(batch['paper_id']))):
        paper_id = batch['paper_id'][idx].as_py()
        affiliation_ids = batch['author_affiliation_ids'][idx].as_py()
        affiliation_names = batch['author_affiliations'][idx].as_py()

        print(f"\n{'='*70}")
        print(f"PAPER {idx + 1}: {paper_id}")
        print(f"{'='*70}")

        # Show affiliations
        print(f"\nAffiliations (raw):")
        for i, names in enumerate(affiliation_names[:5]):
            if names and len(names) > 0:
                print(f"  Author {i+1}: {names[0]}")

        # Classify
        print(f"\n{'─'*70}")
        print("CORRECT CLASSIFICATION:")
        print(f"{'─'*70}")

        result = classify_paper_affiliations(affiliation_ids, affiliation_names)

        # Show detailed classifications
        for aff_class in result['affiliation_classifications'][:10]:
            marker = {
                'FIRM': '✅',
                'UNIVERSITY': '❌',
                'GOVERNMENT': '❌',
                'FACILITY': '⚠️',
            }.get(aff_class['classification'], '❓')

            print(f"  {marker} {aff_class['institution_name']}")
            print(f"     OpenAlex type: {aff_class['openalex_type']}")
            print(f"     Classification: {aff_class['classification']}")

        # Summary
        print(f"\nSUMMARY:")
        print(f"  Total affiliations: {result['total_count']}")
        print(f"  Firms: {result['firm_count']}")
        print(f"  Universities: {result['university_count']}")
        print(f"  Government: {result['government_count']}")
        print(f"  Facilities: {result['facility_count']}")
        print(f"  Other: {result['other_count']}")
        print(f"  Firm ratio: {result['firm_ratio']:.2f}")
        print(f"  Has firm affiliation: {result['has_firm_affiliation']}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main entry point."""

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "demo":
            # Run demonstration
            demonstrate_on_sample()

        elif command == "process":
            # Process sample dataset
            num_papers = int(sys.argv[2]) if len(sys.argv) > 2 else 1000

            output_path = sys.argv[3] if len(sys.argv) > 3 else str(OUTPUT_PARQUET)

            process_dataset_sample(
                input_path=str(INPUT_PARQUET),
                output_path=output_path,
                num_papers=num_papers
            )

        else:
            print(f"Unknown command: {command}")
            print("Usage:")
            print("  python correct_institution_classifications.py demo")
            print("  python correct_institution_classifications.py process [num_papers] [output_path]")
    else:
        print("Usage:")
        print("  python correct_institution_classifications.py demo")
        print("  python correct_institution_classifications.py process [num_papers] [output_path]")
        print("\nExamples:")
        print("  python correct_institution_classifications.py demo")
        print("  python correct_institution_classifications.py process 1000")
        print("  python correct_institution_classifications.py process 10000 /path/to/output.parquet")


if __name__ == "__main__":
    main()
