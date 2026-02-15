"""
Fast Institution Classification Using Local Institutions Database
================================================================

Loads the complete institutions database from institution_openalex.gz
and processes papers to calculate firm_count and firm_ratio.

Much faster than API calls - processes institutions instantly!

Author: JMP Research Team
Date: 2025-02-08
"""

import json
import gzip
import pyarrow.parquet as pq
import pandas as pd
from typing import Dict, List
from pathlib import Path
import sys
from collections import defaultdict

# =============================================================================
# CONFIGURATION
# =============================================================================

DATA_DIR = Path("/home/kurtluo/yannan/jmp/data")
INPUT_PARQUET = DATA_DIR / "processed/publication/ai_papers_condensed.parquet"
# Use the complete institutions database (all 120K+ institutions)
INSTITUTIONS_DB = DATA_DIR / "raw/publication/institutions_all.gz"
OUTPUT_PARQUET = DATA_DIR / "processed/publication/ai_papers_with_correct_classifications_fast.parquet"

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
# LOAD INSTITUTIONS DATABASE
# =============================================================================

def load_institutions_database(file_path: str) -> Dict[str, Dict]:
    """
    Load all institutions from the gzipped JSONL file into a lookup table.

    Args:
        file_path: Path to institution_openalex.gz

    Returns:
        Dictionary mapping {openalex_id_only: institution_data}
    """
    print(f"Loading institutions database from {file_path}...")
    print("-"*70)

    institutions = []

    # Read JSONL file (one JSON per line)
    with gzip.open(file_path, 'rt') as f:
        for line_num, line in enumerate(f, 1):
            try:
                inst = json.loads(line.strip())
                institutions.append(inst)
            except:
                print(f"Warning: Failed to parse line {line_num}")
                continue

    print(f"Loaded {len(institutions):,} institutions")

    # Create lookup table
    lookup = {}

    for inst in institutions:
        inst_id = inst.get('id', '')
        if inst_id and inst_id.startswith('https://openalex.org/I'):
            # Extract just the ID part
            inst_id_only = inst_id.split('/')[-1]

            # Create lookup entry
            lookup[inst_id_only] = {
                'id': inst_id,
                'name': inst.get('display_name', 'Unknown'),
                'type': inst.get('type', 'unknown'),
                'country': inst.get('country_code', 'Unknown'),
            }

    print(f"Created lookup table with {len(lookup):,} institution entries")
    print("-"*70)

    # Show distribution of institution types
    type_counts = defaultdict(int)
    for inst_data in lookup.values():
        type_counts[inst_data['type']] += 1

    print("\nInstitution type distribution:")
    for inst_type, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        classification = TYPE_MAPPING.get(inst_type, inst_type)
        print(f"  {inst_type:20s} {count:6,} institutions ({classification})")

    return lookup


# =============================================================================
# CLASSIFICATION FUNCTIONS
# =============================================================================

def classify_institutions_fast(
    author_affiliation_ids: List[List[str]],
    author_affiliation_names: List[List[str]],
    institutions_lookup: Dict[str, Dict],
    use_api_fallback: bool = False
) -> Dict:
    """
    Classify all affiliations for a paper using local institution database.

    Can optionally use API fallback for institutions not in local database.
    """
    # Initialize counters
    firm_count = 0
    university_count = 0
    government_count = 0
    facility_count = 0
    nonprofit_count = 0
    other_count = 0
    total_count = 0

    # Track unique institutions
    seen_institutions = set()

    # Track classifications for each unique institution
    institution_details = []

    # Process each author's affiliations
    for aff_ids, aff_names in zip(author_affiliation_ids, author_affiliation_names):
        for aff_id, aff_name_list in zip(aff_ids, aff_names):
            if not aff_id:
                continue

            # Extract ID only
            aff_id_only = aff_id.split('/')[-1]

            # Skip if we've already processed this institution
            if aff_id_only in seen_institutions:
                continue

            seen_institutions.add(aff_id_only)
            total_count += 1

            # Look up institution in local database
            inst_data = None
            from_api = False

            if aff_id_only in institutions_lookup:
                inst_data = institutions_lookup[aff_id_only]
            elif use_api_fallback:
                # Try API for institutions not in local database
                import requests
                import time
                url = f"https://api.openalex.org/institutions/{aff_id_only}"
                try:
                    response = requests.get(url, timeout=5)
                    if response.status_code == 200:
                        data = response.json()
                        inst_data = {
                            'id': data.get('id'),
                            'name': data.get('display_name', 'Unknown'),
                            'type': data.get('type', 'unknown'),
                            'country': data.get('country_code', 'Unknown'),
                        }
                        from_api = True
                        # Add to lookup for future use
                        institutions_lookup[aff_id_only] = inst_data
                        time.sleep(0.05)  # Rate limit: 20 req/sec
                except Exception as e:
                    pass

            if inst_data:
                openalex_type = inst_data['type']
                classification = TYPE_MAPPING.get(openalex_type, 'OTHER')

                # Count by classification
                if classification == 'FIRM':
                    firm_count += 1
                elif classification == 'UNIVERSITY':
                    university_count += 1
                elif classification == 'GOVERNMENT':
                    government_count += 1
                elif classification == 'FACILITY':
                    facility_count += 1
                elif classification == 'NONPROFIT':
                    nonprofit_count += 1
                else:
                    other_count += 1

                # Store details
                institution_details.append({
                    'openalex_id': aff_id,
                    'institution_name': aff_name_list[0] if aff_name_list else inst_data['name'],
                    'openalex_type': openalex_type,
                    'classification': classification,
                    'country': inst_data['country'],
                    'found': True,
                    'source': 'api' if from_api else 'local',
                })
            else:
                # Not in database and API fallback failed/not enabled
                other_count += 1
                institution_details.append({
                    'openalex_id': aff_id,
                    'institution_name': aff_name_list[0] if aff_name_list else 'Unknown',
                    'openalex_type': 'unknown',
                    'classification': 'OTHER',
                    'country': 'Unknown',
                    'found': False,
                    'source': 'none',
                })

    # Calculate firm ratio
    firm_ratio = firm_count / total_count if total_count > 0 else 0.0

    return {
        'total_affiliations': total_count,
        'unique_institutions': len(seen_institutions),
        'firm_count': firm_count,
        'university_count': university_count,
        'government_count': government_count,
        'facility_count': facility_count,
        'nonprofit_count': nonprofit_count,
        'other_count': other_count,
        'firm_ratio': firm_ratio,
        'has_firm_affiliation': firm_count > 0,
        'has_university_affiliation': university_count > 0,
        'has_government_affiliation': government_count > 0,
        'institution_details': institution_details,
    }


# =============================================================================
# PROCESS DATASET
# =============================================================================

def process_dataset_fast(
    input_path: str,
    output_path: str,
    num_papers: int = None,
    use_api_fallback: bool = False
):
    """
    Process the papers dataset using local institutions database.

    Can optionally use API fallback for institutions not in local database.
    """
    print("="*70)
    print("FAST CLASSIFICATION USING LOCAL INSTITUTIONS DATABASE")
    print("="*70)

    # Step 1: Load institutions database
    institutions_lookup = load_institutions_database(str(INSTITUTIONS_DB))

    # Step 2: Process papers
    print(f"\nProcessing papers from {input_path}...")
    print("-"*70)

    pf = pq.ParquetFile(input_path)
    total_row_groups = pf.num_row_groups

    if num_papers:
        print(f"Processing sample of {num_papers:,} papers")
    else:
        print(f"Processing all papers (17M estimated)")

    print(f"Total row groups: {total_row_groups}")

    papers_processed = 0
    results = []

    for rg_idx in range(total_row_groups):
        if num_papers and papers_processed >= num_papers:
            print(f"\nReached target of {num_papers:,} papers")
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

            # Process each paper
            for i in range(len(batch['paper_id'])):
                if num_papers and papers_processed >= num_papers:
                    break

                paper_id = batch['paper_id'][i].as_py()
                openalex_id = batch['openalex_id'][i].as_py()
                pub_year = batch['publication_year'][i].as_py()
                affiliation_ids = batch['author_affiliation_ids'][i].as_py()
                affiliation_names = batch['author_affiliations'][i].as_py()

                # Classify
                result = classify_institutions_fast(
                    affiliation_ids,
                    affiliation_names,
                    institutions_lookup,
                    use_api_fallback
                )

                # Store result (flatten institution_details for storage)
                result_flat = {
                    'paper_id': paper_id,
                    'openalex_id': openalex_id,
                    'publication_year': pub_year,
                }
                result_flat.update({k: v for k, v in result.items() if k != 'institution_details'})

                results.append(result_flat)

                papers_processed += 1

                if papers_processed % 10000 == 0:
                    print(f"  Processed {papers_processed:,} papers so far...")

        except Exception as e:
            print(f"Error processing row group {rg_idx}: {e}")
            import traceback
            traceback.print_exc()
            continue

    # Convert to DataFrame and save
    print(f"\n{'='*70}")
    print(f"Converting to DataFrame and saving to {output_path}...")
    print(f"{'='*70}")

    df = pd.DataFrame(results)
    df.to_parquet(output_path, index=False)

    # Print summary
    print(f"\n{'='*70}")
    print("SUMMARY STATISTICS")
    print(f"{'='*70}")
    print(f"Total papers processed: {len(df):,}")
    print(f"\nAffiliation statistics:")
    print(f"  Total affiliations (with institutions): {df['total_affiliations'].sum():,}")
    print(f"  Unique institutions: {df['unique_institutions'].sum():,}")
    print(f"\nFirm affiliation statistics:")
    print(f"  Papers with firm affiliations: {df['has_firm_affiliation'].sum():,} ({df['has_firm_affiliation'].sum()/len(df)*100:.1f}%)")
    print(f"  Papers with university affiliations: {df['has_university_affiliation'].sum():,} ({df['has_university_affiliation'].sum()/len(df)*100:.1f}%)")
    print(f"  Papers with government affiliations: {df['has_government_affiliation'].sum():,} ({df['has_government_affiliation'].sum()/len(df)*100:.1f}%)")

    print(f"\nFirm ratio statistics:")
    print(f"  Mean: {df['firm_ratio'].mean():.3f}")
    print(f"  Median: {df['firm_ratio'].median():.3f}")
    print(f"  Min: {df['firm_ratio'].min():.3f}")
    print(f"  Max: {df['firm_ratio'].max():.3f}")

    print(f"\nFirm count statistics:")
    print(f"  Mean: {df['firm_count'].mean():.2f}")
    print(f"  Median: {df['firm_count'].median():.0f}")
    print(f"  Max: {df['firm_count'].max():.0f}")

    # Distribution of firm counts
    print(f"\nFirm count distribution:")
    for count in range(6):
        num_papers = (df['firm_count'] == count).sum()
        pct = num_papers / len(df) * 100
        bar = '█' * int(pct / 2)
        print(f"  {count}: {num_papers:6,} papers ({pct:5.1f}%) {bar}")

    print(f"\n{'='*70}")
    print(f"Output saved to: {output_path}")
    print(f"{'='*70}")

    return df


# =============================================================================
# DEMONSTRATION
# =============================================================================

def demonstrate():
    """Run a small demonstration."""

    print("="*70)
    print("DEMONSTRATION: Fast Classification with Institutions Database")
    print("="*70)

    # Load institutions
    institutions_lookup = load_institutions_database(str(INSTITUTIONS_DB))

    # Read first few papers
    pf = pq.ParquetFile(str(INPUT_PARQUET))
    batch = pf.read_row_group(0, columns=[
        'paper_id',
        'author_affiliation_ids',
        'author_affiliations',
    ])

    print("\nProcessing first 2 papers:\n")

    for idx in range(min(2, len(batch['paper_id']))):
        paper_id = batch['paper_id'][idx].as_py()
        affiliation_ids = batch['author_affiliation_ids'][idx].as_py()
        affiliation_names = batch['author_affiliations'][idx].as_py()

        print(f"\n{'='*70}")
        print(f"PAPER {idx + 1}: {paper_id}")
        print(f"{'='*70}")

        # Show affiliations
        print(f"\nAffiliations:")
        for i, names in enumerate(affiliation_names[:5]):
            if names and len(names) > 0:
                print(f"  {i+1}. {names[0]}")

        # Classify
        print(f"\n{'─'*70}")
        print("CLASSIFICATION:")
        print(f"{'─'*70}")

        result = classify_institutions_fast(
            affiliation_ids,
            affiliation_names,
            institutions_lookup
        )

        # Show unique institutions found
        unique_insts = {}
        for detail in result['institution_details']:
            inst_name = detail['institution_name']
            classification = detail['classification']
            found = detail['found']

            if inst_name not in unique_insts:
                unique_insts[inst_name] = (classification, found)

        print(f"Total affiliations: {result['total_affiliations']}")
        print(f"Unique institutions: {result['unique_institutions']}")
        print(f"Firms: {result['firm_count']}")
        print(f"Universities: {result['university_count']}")
        print(f"Government: {result['government_count']}")
        print(f"Firm ratio: {result['firm_ratio']:.2f}")
        print(f"Has firm affiliation: {result['has_firm_affiliation']}")

        # Show breakdown by institution
        if unique_insts:
            print(f"\nInstitution breakdown:")
            for inst_name, (classification, found) in unique_insts.items():
                marker = {
                    'FIRM': '✅',
                    'UNIVERSITY': '❌',
                    'GOVERNMENT': '❌',
                    'FACILITY': '⚠️',
                    'NONPROFIT': '❌',
                    'OTHER': '❓',
                }.get(classification, '?')

                status = "✓ in database" if found else "✗ NOT in database"
                print(f"  {marker} {inst_name}")
                print(f"     {classification} - {status}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main entry point."""

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "demo":
            demonstrate()

        elif command == "process":
            num_papers = int(sys.argv[2]) if len(sys.argv) > 2 else None
            use_api = sys.argv[3].lower() == 'true' if len(sys.argv) > 3 else False

            print(f"API Fallback: {'ENABLED' if use_api else 'DISABLED'}")
            if use_api:
                print("Note: API calls will be made for institutions not in local database.")
                print("This will be slower but will provide more complete classifications.\n")

            process_dataset_fast(
                input_path=str(INPUT_PARQUET),
                output_path=str(OUTPUT_PARQUET),
                num_papers=num_papers,
                use_api_fallback=use_api
            )

        else:
            print(f"Unknown command: {command}")
            print("\nUsage:")
            print("  python classify_fast.py demo")
            print("  python classify_fast.py process [num_papers] [use_api_fallback]")
            print("\nExamples:")
            print("  python classify_fast.py demo")
            print("  python classify_fast.py process 10000 false")
            print("  python classify_fast.py process 10000 true")
    else:
        print("\nUsage:")
        print("  python classify_fast.py demo              - Show demonstration")
        print("  python classify_fast.py process [num_papers] [use_api]   - Process dataset")
        print("\nExamples:")
        print("  python classify_fast.py demo")
        print("  python classify_fast.py process 10000 false")
        print("  python classify_fast.py process 10000 true")
        print("\nOptions:")
        print("  num_papers        - Number of papers to process (default: all)")
        print("  use_api           - 'true' to query API for missing institutions, 'false' to skip")
        print("                     (default: false - faster, but only classifies institutions in local DB)")


if __name__ == "__main__":
    main()
