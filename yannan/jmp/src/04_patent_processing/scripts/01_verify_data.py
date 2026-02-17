#!/usr/bin/env python3
"""
Phase 1: Verify Downloaded PatentsView Data

This script checks if the PatentsView data files are properly downloaded and readable.
It validates file formats, counts records, and creates the directory structure for processing.

Expected files in /Data/patent/raw/:
- g_patent.tsv (~2 GB)
- g_patent_abstract.tsv (~1.6 GB)
- g_cpc_current.tsv (~1 GB)
- g_assignee_disambiguated.tsv (~200 MB)
- g_patent_assignee.tsv (~500 MB)
"""

import os
import sys
from pathlib import Path

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl


# Configuration
RAW_DATA_DIR = Path('/Data/patent/raw')
PROCESSED_DATA_DIR = Path('/Data/patent/processed')
OUTPUTS_DIR = Path('/Data/patent/outputs')

# Required files
REQUIRED_FILES = {
    'g_patent.tsv': {
        'description': 'Patent metadata (ID, date, type, title)',
        'min_size_gb': 1.5,
        'columns': ['patent_id', 'patent_date', 'patent_type', 'patent_title'],
    },
    'g_patent_abstract.tsv': {
        'description': 'Patent abstracts (text)',
        'min_size_gb': 1.2,
        'columns': ['patent_id', 'patent_abstract'],
    },
    'g_cpc_current.tsv': {
        'description': 'CPC classifications',
        'min_size_gb': 0.8,
        'columns': ['patent_id', 'cpc_section', 'cpc_subgroup', 'cpc_group'],
    },
    'g_assignee_disambiguated.tsv': {
        'description': 'Assignee names (cleaned)',
        'min_size_gb': 0.15,
        'columns': ['assignee_id', 'assignee_type', 'disambig_assignee_organization'],
    },
    'g_patent_assignee.tsv': {
        'description': 'Patent → assignee links',
        'min_size_gb': 0.4,
        'columns': ['patent_id', 'assignee_id'],
    },
}


def check_file_exists(filepath: Path) -> bool:
    """Check if file exists."""
    return filepath.exists() and filepath.is_file()


def check_file_size(filepath: Path, min_size_gb: float) -> bool:
    """Check if file size is at least minimum expected."""
    size_gb = filepath.stat().st_size / (1024**3)
    return size_gb >= min_size_gb


def check_file_readable(filepath: Path, sep: str = '\t') -> bool:
    """Check if file is readable (valid TSV format)."""
    try:
        # Try to read first 5 rows
        df = pl.read_csv(filepath, separator=sep, n_rows=5)
        return True
    except Exception as e:
        print(f"  ❌ Error reading file: {e}")
        return False


def check_file_columns(filepath: Path, expected_columns: list, sep: str = '\t') -> bool:
    """Check if file has expected columns."""
    try:
        df = pl.read_csv(filepath, separator=sep, n_rows=1)
        actual_columns = df.columns

        # Check if all expected columns are present
        missing_cols = set(expected_columns) - set(actual_columns)

        if missing_cols:
            print(f"  ⚠️  Missing columns: {missing_cols}")
            return False

        return True
    except Exception as e:
        print(f"  ❌ Error checking columns: {e}")
        return False


def count_records(filepath: Path, sep: str = '\t') -> int:
    """Count total records in file."""
    try:
        # Use Polars lazy frame for memory efficiency
        lf = pl.scan_csv(filepath, separator=sep)
        count = lf.select(pl.len()).collect().item()
        return count
    except Exception as e:
        print(f"  ❌ Error counting records: {e}")
        return 0


def create_directory_structure():
    """Create output directories if they don't exist."""
    dirs_to_create = [
        PROCESSED_DATA_DIR,
        OUTPUTS_DIR,
    ]

    for dir_path in dirs_to_create:
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"✅ Created directory: {dir_path}")
        else:
            print(f"✅ Directory exists: {dir_path}")


def main():
    """Main verification function."""
    print("=" * 80)
    print("Phase 1: PatentsView Data Verification")
    print("=" * 80)
    print()

    # Check if raw data directory exists
    if not RAW_DATA_DIR.exists():
        print(f"❌ Raw data directory does not exist: {RAW_DATA_DIR}")
        print("\nPlease download PatentsView data to:")
        print(f"  {RAW_DATA_DIR}")
        print("\nRequired files:")
        for filename, info in REQUIRED_FILES.items():
            print(f"  - {filename}: {info['description']}")
        sys.exit(1)

    print(f"✅ Raw data directory exists: {RAW_DATA_DIR}")
    print()

    # Check each required file
    all_files_ok = True

    for filename, info in REQUIRED_FILES.items():
        filepath = RAW_DATA_DIR / filename

        print(f"Checking {filename}...")
        print(f"  Description: {info['description']}")

        # Check if file exists
        if not check_file_exists(filepath):
            print(f"  ❌ File does not exist")
            all_files_ok = False
            print()
            continue

        # Check file size
        if not check_file_size(filepath, info['min_size_gb']):
            size_gb = filepath.stat().st_size / (1024**3)
            print(f"  ⚠️  File size: {size_gb:.2f} GB (expected > {info['min_size_gb']} GB)")
            print(f"  ⚠️  File may be incomplete or still downloading")
        else:
            size_gb = filepath.stat().st_size / (1024**3)
            print(f"  ✅ File size: {size_gb:.2f} GB")

        # Check if file is readable
        if not check_file_readable(filepath):
            all_files_ok = False
            print()
            continue

        # Check if file has expected columns
        if not check_file_columns(filepath, info['columns']):
            all_files_ok = False
            print()
            continue

        # Count records
        count = count_records(filepath)
        if count > 0:
            print(f"  ✅ Records: {count:,}")
        else:
            print(f"  ❌ Unable to count records")
            all_files_ok = False

        print()

    # Create output directories
    print("Creating directory structure...")
    create_directory_structure()
    print()

    # Final summary
    print("=" * 80)
    if all_files_ok:
        print("✅ All checks passed! Data is ready for processing.")
        print()
        print("Next steps:")
        print("  1. Run Phase 2: Filter AI patents")
        print("     python src/04_patent_processing/scripts/02_filter_ai_patents.py")
    else:
        print("❌ Some checks failed. Please fix the issues above before proceeding.")
        sys.exit(1)

    print("=" * 80)


if __name__ == '__main__':
    main()
