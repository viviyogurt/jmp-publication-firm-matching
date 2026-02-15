"""
Correct Institution Classifications Using Local Institution Data
================================================================

This script uses local institution data instead of API calls for faster
processing. Falls back to API for institutions not in the local file.

Author: JMP Research Team
Date: 2025-02-08
"""

import json
import pyarrow.parquet as pq
import pandas as pd
from typing import Dict, List, Optional
from pathlib import Path
import sys

# =============================================================================
# CONFIGURATION
# =============================================================================

OPENALEX_API_BASE = "https://api.openalex.org"
DATA_DIR = Path("/home/kurtluo/yannan/jmp/data")
INPUT_PARQUET = DATA_DIR / "processed/publication/ai_papers_condensed.parquet"
OUTPUT_PARQUET = DATA_DIR / "processed/publication/ai_papers_with_correct_classifications.parquet"
LOCAL_INSTITUTIONS_FILE = DATA_DIR / "raw/publication/open_alex.json"

# Institution type mapping
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
# LOCAL INSTITUTION LOOKUP
# =============================================================================

def load_local_institutions(file_path: str) -> Dict[str, Dict]:
    """
    Load local institution data into a lookup table.

    Args:
        file_path: Path to open_alex.json file

    Returns:
        Dictionary mapping {openalex_id: institution_data}
    """
    print(f"Loading local institutions from {file_path}...")

    with open(file_path, 'r') as f:
        data = json.load(f)

    # Extract institutions from the API response format
    institutions = data.get('results', data.get('institutions', []))

    print(f"Loaded {len(institutions)} institutions from local file")

    # Create lookup table: openalex_id -> institution_data
    lookup = {}

    for inst in institutions:
        inst_id = inst.get('id', '')
        if inst_id and inst_id.startswith('https://openalex.org/I'):
            # Extract just the ID part for easier matching
            inst_id_only = inst_id.split('/')[-1]

            lookup[inst_id] = {
                'id': inst_id,
                'id_only': inst_id_only,
                'name': inst.get('display_name', 'Unknown'),
                'type': inst.get('type', 'unknown'),
                'country': inst.get('country_code', 'Unknown'),
                'ror': inst.get('ror'),
            }

            # Also store with full URL as key (but only if different)
            if inst_id not in lookup:
                lookup[inst_id] = lookup[inst_id_only]

    print(f"Created lookup table with {len(lookup)} institution entries")

    return lookup


def get_institution_type(
    openalex_id: str,
    local_lookup: Dict[str, Dict],
    use_api_fallback: bool = True
) -> Optional[Dict]:
    """
    Get institution type from local lookup or API (with fallback).

    Args:
        openalex_id: OpenAlex institution ID (full URL or just ID part)
        local_lookup: Local institution lookup table
        use_api_fallback: If True, query API for missing institutions

    Returns:
        Institution data dict or None
    """
    # Extract numeric ID if full URL
    if openalex_id.startswith('http'):
        openalex_id = openalex_id.split('/')[-1]

    # First, try local lookup
    if openalex_id in local_lookup:
        return local_lookup[openalex_id]

    # Try with full URL format
    full_url = f"https://openalex.org/{openalex_id}"
    if full_url in local_lookup:
        return local_lookup[full_url]

    # Not in local data - optionally fetch from API
    if use_api_fallback:
        import requests
        import time

        url = f"{OPENALEX_API_BASE}/institutions/{openalex_id}"

        try:
            response = requests.get(url, timeout=5)

            if response.status_code == 200:
                data = response.json()
                inst_data = {
                    'id': data.get('id'),
                    'id_only': openalex_id,
                    'name': data.get('display_name', 'Unknown'),
                    'type': data.get('type', 'unknown'),
                    'country': data.get('country_code', 'Unknown'),
                    'ror': data.get('ror'),
                    'from_api': True,
                }

                # Add to local lookup for future use
                local_lookup[openalex_id] = inst_data
                local_lookup[full_url] = inst_data

                return inst_data
        except Exception as e:
            pass

    return None


# =============================================================================
# PAPER-LEVEL CLASSIFICATION
# =============================================================================

def classify_paper_affiliations(
    author_affiliation_ids: List[List[str]],
    author_affiliation_names: List[List[str]],
    local_lookup: Dict[str, Dict],
    use_api_fallback: bool = True
) -> Dict:
    """
    Classify all affiliations for a paper using local institution data.

    IMPORTANT: Each paper has multiple authors, each with multiple affiliations.
    We process each institution separately and calculate:

    - firm_count: Number of affiliations that are companies
    - total_count: Total number of affiliations
    - firm_ratio: firm_count / total_count

    Args:
        author_affiliation_ids: List of lists of OpenAlex IDs for each author
        author_affiliation_names: List of lists of institution names for each author
        local_lookup: Pre-loaded institution lookup table
        use_api_fallback: If True, query API for institutions not in local data

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

    # Track unique institutions to avoid redundant API calls
    seen_institutions = set()

    # Process each author's affiliations
    for author_idx, (aff_ids, aff_names) in enumerate(zip(author_affiliation_ids, author_affiliation_names)):
        for aff_idx, (aff_id, aff_name) in enumerate(zip(aff_ids, aff_names)):
            if not aff_id:  # Skip empty affiliations
                continue

            total_count += 1

            # Skip if we've already seen this institution
            if aff_id in seen_institutions:
                continue

            seen_institutions.add(aff_id)

            # Get institution data from local lookup (or API fallback)
            inst_data = get_institution_type(
                aff_id,
                local_lookup,
                use_api_fallback=use_api_fallback
            )

            if inst_data:
                openalex_type = inst_data['type']
                classification = TYPE_MAPPING.get(openalex_type, 'OTHER')

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
                    'institution_name': aff_name[0] if aff_name else inst_data['name'],
                    'openalex_type': openalex_type,
                    'classification': classification,
                    'source': 'local' if not inst_data.get('from_api') else 'api',
                })

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

def process_dataset_fast(
    input_path: str,
    output_path: str,
    num_papers: Optional[int] = None,
    use_api_fallback: bool = False
):
    """
    Process dataset using local institution data (much faster than API-only approach).

    Args:
        input_path: Path to input parquet file
        output_path: Path to output parquet file
        num_papers: Number of papers to process (None = all)
        use_api_fallback: If True, query API for institutions not in local file
    """
    print("="*70)
    print("CORRECTING INSTITUTION CLASSIFICATIONS (Using Local Data)")
    print("="*70)

    # Load local institution lookup table
    local_lookup = load_local_institutions(str(LOCAL_INSTITUTIONS_FILE))

    print(f"\n{'='*70}")
    print(f"Loading dataset from {input_path}...")
    print(f"{'='*70}")

    pf = pq.ParquetFile(input_path)
    total_row_groups = pf.num_row_groups

    if num_papers:
        print(f"Processing limited sample of {num_papers:,} papers")
    else:
        print(f"Processing all papers (17M estimated)")

    print(f"Total row groups: {total_row_groups}")

    # Read papers
    papers_processed = 0
    results = []

    # Statistics
    stats = {
        'from_local': 0,
        'from_api': 0,
        'not_found': 0,
    }

    for rg_idx in range(total_row_groups):
        if num_papers and papers_processed >= num_papers:
            break

        print(f"\nProcessing row group {rg_idx + 1}/{total_row_groups}...")

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
                if num_papers and papers_processed >= num_papers:
                    break

                paper_id = batch['paper_id'][i].as_py()
                openalex_id = batch['openalex_id'][i].as_py()
                pub_year = batch['publication_year'][i].as_py()
                affiliation_ids = batch['author_affiliation_ids'][i].as_py()
                affiliation_names = batch['author_affiliations'][i].as_py()

                # Classify affiliations
                result = classify_paper_affiliations(
                    affiliation_ids,
                    affiliation_names,
                    local_lookup,
                    use_api_fallback
                )

                # Track data source statistics
                for aff in result.get('affiliation_classifications', []):
                    if aff['source'] == 'local':
                        stats['from_local'] += 1
                    elif aff['source'] == 'api':
                        stats['from_api'] += 1
                    else:
                        stats['not_found'] += 1

                # Store result
                results.append({
                    'paper_id': paper_id,
                    'openalex_id': openalex_id,
                    'publication_year': pub_year,
                    **result
                })

                papers_processed += 1

                if papers_processed % 1000 == 0:
                    print(f"  Processed {papers_processed:,} papers...")

        except Exception as e:
            print(f"Error processing row group {rg_idx}: {e}")
            import traceback
            traceback.print_exc()
            continue

    # Save results
    print(f"\n{'='*70}")
    print(f"Saving {len(results):,} papers to {output_path}...")
    print(f"{'='*70}")

    df = pd.DataFrame(results)
    df.to_parquet(output_path, index=False)

    # Print summary statistics
    print(f"\n{'='*70}")
    print("SUMMARY STATISTICS")
    print(f"{'='*70}")
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

    print(f"\nData source statistics:")
    print(f"  From local lookup: {stats['from_local']:,}")
    print(f"  From API calls: {stats['from_api']:,}")
    print(f"  Not found: {stats['not_found']:,}")

    print(f"\n{'='*70}")
    print(f"Output saved to: {output_path}")
    print(f"{'='*70}")

    return df, stats


# =============================================================================
# DEMONSTRATION
# =============================================================================

def demonstrate_on_sample():
    """Demonstrate the classification on a small sample."""

    print("="*70)
    print("DEMONSTRATION: Fast Classification Using Local Data")
    print("="*70)

    # Load local lookup
    local_lookup = load_local_institutions(str(LOCAL_INSTITUTIONS_FILE))

    # Read a few papers
    pf = pq.ParquetFile(str(INPUT_PARQUET))
    batch = pf.read_row_group(0, columns=[
        'paper_id',
        'author_affiliation_ids',
        'author_affiliations',
    ])

    print("\nProcessing first 3 papers (with local data lookup):\n")

    for idx in range(min(3, len(batch['paper_id']))):
        paper_id = batch['paper_id'][idx].as_py()
        affiliation_ids = batch['author_affiliation_ids'][idx].as_py()
        affiliation_names = batch['author_affiliations'][idx].as_py()

        print(f"\n{'='*70}")
        print(f"PAPER {idx + 1}: {paper_id}")
        print(f"{'='*70}")

        # Show affiliations
        print(f"\nAffiliations (raw):")
        unique_affiliations = set()
        for i, names in enumerate(affiliation_names[:5]):
            if names and len(names) > 0:
                name = names[0]
                if name not in unique_affiliations:
                    unique_affiliations.add(name)
                    print(f"  - {name}")

        # Classify with local data (no API)
        print(f"\n{'─'*70}")
        print("CLASSIFICATION (using local data only, no API calls):")
        print(f"{'─'*70}")

        result = classify_paper_affiliations(
            affiliation_ids,
            affiliation_names,
            local_lookup,
            use_api_fallback=False  # No API calls
        )

        # Show classifications
        unique_classifications = {}
        for aff_class in result['affiliation_classifications']:
            inst_name = aff_class['institution_name']
            classification = aff_class['classification']
            source = aff_class['source']

            if inst_name not in unique_classifications:
                unique_classifications[inst_name] = (classification, source)

        for inst_name, (classification, source) in list(unique_classifications.items())[:10]:
            in_local = "✅" if source == 'local' else "❌"
            marker = {
                'FIRM': '✅',
                'UNIVERSITY': '❌',
                'GOVERNMENT': '❌',
                'FACILITY': '⚠️',
                'OTHER': '❓',
            }.get(classification, '❓')

            print(f"  {marker} {inst_name}")
            print(f"     Classification: {classification}")
            print(f"     Source: {in_local} local file" if source == 'local' else f"     Source: API (not in local file)")

        # Summary
        print(f"\nSUMMARY:")
        print(f"  Total unique affiliations: {len(unique_classifications)}")
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
            # Process dataset
            num_papers_arg = sys.argv[2] if len(sys.argv) > 2 else None
            use_api = sys.argv[3].lower() == 'true' if len(sys.argv) > 3 else False

            num_papers = int(num_papers_arg) if num_papers_arg else None

            output_path = sys.argv[4] if len(sys.argv) > 4 else str(OUTPUT_PARQUET)

            process_dataset_fast(
                input_path=str(INPUT_PARQUET),
                output_path=output_path,
                num_papers=num_papers,
                use_api_fallback=use_api
            )

        else:
            print(f"Unknown command: {command}")
            print("Usage:")
            print("  python correct_institution_classifications_fast.py demo")
            print("  python correct_institution_classifications_fast.py process [num_papers] [use_api_fallback] [output_path]")
            print("")
            print("Examples:")
            print("  python correct_institution_classifications_fast.py demo")
            print("  python correct_institution_classifications_fast.py process 1000")
            print("  python correct_institution_classifications_fast.py process 10000 false")
            print("  python correct_institution_classifications_fast.py process 10000 true")
    else:
        print("Usage:")
        print("  python correct_institution_classifications_fast.py demo")
        print("  python correct_institution_classifications_fast.py process [num_papers] [use_api_fallback] [output_path]")
        print("\nExamples:")
        print("  python correct_institution_classifications_fast.py demo")
        print("  python correct_institution_classifications_fast.py process 1000")
        print("  python correct_institution_classifications_fast.py process 10000 false")
        print("  python correct_institution_classifications_fast.py process 10000 true")
        print("\nOptions:")
        print("  demo              - Show demonstration on first 3 papers")
        print("  process           - Process dataset")
        print("  num_papers        - Number of papers to process (default: all)")
        print("  use_api_fallback  - 'true' to query API for missing institutions, 'false' to skip")
        print("  output_path       - Custom output file path (optional)")


if __name__ == "__main__":
    main()
