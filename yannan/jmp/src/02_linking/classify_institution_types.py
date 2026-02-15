"""
Classify institutions into types to improve matching quality.

This script reclassifies institutions from OpenAlex categories into a simplified
4-category system (academic, government, nonprofit, corporate) for better filtering
in the firm matching process.
"""

import polars as pl
import re
from pathlib import Path

# Define classification patterns
ACADEMIC_PATTERNS = [
    "university", "college", "institute of technology",
    "school", "polytechnic", "academy", "conservatory"
]

ACADEMIC_DOMAINS = [
    ".ac.uk", ".edu", ".edu.", ".ac.", ".edu.au",
    ".edu.cn", ".ac.cn", ".edu.in", ".ac.in"
]

GOVERNMENT_PATTERNS = [
    "government", "ministry", "agency", "department",
    "bureau", "federal", "national", "state", "municipal",
    "public service", "administration"
]

NONPROFIT_PATTERNS = [
    "foundation", "association", "society", "council",
    "charity", "institute", "trust", "organization",
    "consortium", "alliance", "coalition"
]

NONPROFIT_DOMAINS = [".org", ".ngo", ".foundation"]


def classify_institution(df: pl.DataFrame) -> pl.DataFrame:
    """
    Classify institutions into 4 categories.

    Mapping from OpenAlex types:
    - education, facility → academic
    - government → government
    - nonprofit, funder, archive → nonprofit
    - company → corporate
    - healthcare, other → contextual classification based on name
    """

    def classify_row(row) -> str:
        """Classify a single institution row."""
        # Start with OpenAlex classification
        openalex_type = row.get("institution_type")

        # Direct mappings
        if openalex_type == "company":
            return "corporate"
        elif openalex_type == "government":
            return "government"
        elif openalex_type in ["education", "facility"]:
            return "academic"
        elif openalex_type in ["nonprofit", "funder", "archive"]:
            return "nonprofit"

        # For healthcare, other, and null, do contextual classification
        display_name = str(row.get("display_name", "")).lower() if row.get("display_name") else ""
        homepage_domain = str(row.get("homepage_domain", "")).lower() if row.get("homepage_domain") else ""

        # Check academic patterns
        for pattern in ACADEMIC_PATTERNS:
            if pattern in display_name:
                return "academic"
        for domain in ACADEMIC_DOMAINS:
            if domain in homepage_domain:
                return "academic"

        # Check government patterns
        for pattern in GOVERNMENT_PATTERNS:
            if pattern in display_name:
                return "government"

        # Check nonprofit patterns
        for pattern in NONPROFIT_PATTERNS:
            if pattern in display_name:
                return "nonprofit"
        for domain in NONPROFIT_DOMAINS:
            if domain in homepage_domain:
                return "nonprofit"

        # Check is_company flag
        if row.get("is_company", 0) == 1:
            return "corporate"

        # Check for corporate indicators (banks, hospitals, etc.)
        corporate_indicators = [
            "bank", "financial", "hospital", "clinic", "medical center",
            "pharma", "biotech", "technologies", "industries", "corporation"
        ]
        for indicator in corporate_indicators:
            if indicator in display_name:
                return "corporate"

        # Default to unknown
        return "unknown"

    # Apply classification
    result = df.with_columns(
        pl.struct(["institution_type", "display_name", "homepage_domain", "is_company"])
        .map_elements(classify_row, return_dtype=pl.String)
        .alias("institution_type_classified")
    )

    return result


def main():
    """Main execution function."""
    print("Loading publication institutions data...")

    input_path = Path("/home/kurtluo/yannan/jmp/data/interim/publication_institutions_enriched.parquet")
    output_path = Path("/home/kurtluo/yannan/jmp/data/interim/publication_institutions_classified.parquet")

    # Load data
    df = pl.read_parquet(input_path)
    print(f"Loaded {len(df):,} institutions")

    # Classify institutions
    print("\nClassifying institutions into types...")
    df_classified = classify_institution(df)

    # Show classification distribution
    print("\nClassification distribution:")
    type_counts = (
        df_classified.group_by("institution_type_classified")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
    print(type_counts)

    # Show breakdown by OpenAlex type
    print("\nBreakdown by original OpenAlex type:")
    breakdown = (
        df_classified.group_by(["institution_type", "institution_type_classified"])
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
    print(breakdown)

    # Save classified data
    print(f"\nSaving classified data to {output_path}...")
    df_classified.write_parquet(output_path)
    print("Done!")

    # Print examples of each type
    print("\nExamples of each classification:")
    for inst_type in ["academic", "government", "nonprofit", "corporate", "unknown"]:
        print(f"\n{inst_type.upper()}:")
        examples = (
            df_classified.filter(pl.col("institution_type_classified") == inst_type)
            .select(["display_name", "institution_type", "homepage_domain"])
            .head(5)
        )
        print(examples)


if __name__ == "__main__":
    main()
