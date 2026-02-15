"""
Filtered publication-to-firm matching strategy (OPTIMIZED).

This script implements a filtered matching approach that only matches
corporate institutions to firms, significantly reducing false positives.

Optimized using token-based matching and blocking strategies.
"""

import polars as pl
from pathlib import Path
from typing import Dict, List, Set
import string
from rapidfuzz import fuzz, process

# Configuration
DATA_DIR = Path("/home/kurtluo/yannan/jmp/data")
INSTITUTIONS_PATH = DATA_DIR / "interim/publication_institutions_classified.parquet"
FIRMS_PATH = DATA_DIR / "interim/compustat_firms_standardized.parquet"
OUTPUT_PATH = DATA_DIR / "interim/publication_firm_matches_filtered.parquet"

# Matching parameters
NAME_SIMILARITY_THRESHOLD = 85  # Using rapidfuzz score (0-100)
MIN_NAME_LENGTH = 3


def normalize_name(name: str) -> str:
    """Normalize company/institution name for matching."""
    if not name:
        return ""

    # Convert to lowercase
    name = name.lower()

    # Remove punctuation
    name = name.translate(str.maketrans("", "", string.punctuation))

    # Remove common country/location suffixes (often in parentheses)
    # These get added by OpenAlex but aren't part of the company name
    location_patterns = [
        " united states", " usa", " us ",
        " united kingdom", " uk ",
        " south korea", " north korea",
        " czechia", " czech republic",
        " china", " japan", " germany", " france",
        " canada", " australia", " india",
        " netherlands", " switzerland", " sweden",
        " norway", " denmark", " finland",
        " italy", " spain", " poland",
        " brazil", " argentina", " mexico",
        " singapore", " taiwan", " hong kong"
    ]
    for pattern in location_patterns:
        name = name.replace(pattern, " ")

    # Remove common legal suffixes
    suffixes = [
        " inc", " ltd", " llc", " corp", " corporation",
        " co", " plc", " gmbh", " ag", " sa", " pty",
        " limited", " incorporated", " technologies", " group"
    ]
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()

    # Remove extra whitespace
    name = " ".join(name.split())

    return name


def extract_tokens(name: str) -> Set[str]:
    """Extract significant tokens from a name for blocking."""
    if not name:
        return set()

    # Normalize and split
    normalized = normalize_name(name)
    tokens = set(normalized.split())

    # Remove common stopwords
    stopwords = {
        "the", "and", "of", "in", "for", "with", "at", "on",
        "de", "la", "le", "les", "et", "und", "der", "die", "das"
    }
    tokens = tokens - stopwords

    # Filter out very short tokens
    tokens = {t for t in tokens if len(t) >= 3}

    return tokens


def build_token_index(firms_df: pl.DataFrame) -> Dict[str, List[int]]:
    """Build inverted index from tokens to firm indices."""
    token_index = {}

    for idx, row in enumerate(firms_df.iter_rows(named=True)):
        # Get all name variants
        names = [row["normalized_name"]]
        if row.get("name_variants"):
            names.extend(row["name_variants"])

        # Extract tokens from all names
        all_tokens = set()
        for name in names:
            if name:
                all_tokens.update(extract_tokens(str(name)))

        # Add to index
        for token in all_tokens:
            if token not in token_index:
                token_index[token] = []
            token_index[token].append(idx)

    return token_index


def find_candidate_firms(
    institution_name: str,
    token_index: Dict[str, List[int]],
    firms_df: pl.DataFrame
) -> List[int]:
    """Find candidate firms using token-based blocking."""
    tokens = extract_tokens(institution_name)

    if not tokens:
        return []

    # Get firm indices for each token
    candidate_indices = set()
    for token in tokens:
        if token in token_index:
            candidate_indices.update(token_index[token])

    # Require firms to match at least min(2, num_tokens) tokens
    min_matches = min(2, len(tokens))

    # Count token matches per firm
    firm_token_counts = {}
    for token in tokens:
        if token in token_index:
            for idx in token_index[token]:
                firm_token_counts[idx] = firm_token_counts.get(idx, 0) + 1

    # Keep only firms that match at least min_matches tokens
    candidate_indices = {
        idx for idx, count in firm_token_counts.items()
        if count >= min_matches
    }

    return list(candidate_indices)


def find_best_match(
    institution_name: str,
    candidate_indices: List[int],
    firms_df: pl.DataFrame,
    threshold: int = NAME_SIMILARITY_THRESHOLD
) -> Dict:
    """
    Find best matching firm among candidates using fuzzy matching.

    Returns dict with match info or None if no match found.
    """
    if not candidate_indices:
        return None

    # Normalize institution name
    normalized_inst = normalize_name(institution_name)

    # Get candidate firm names by iterating through candidate indices
    candidate_firms = []
    for idx in candidate_indices:
        # Get the row - convert DataFrame slice to actual row
        row_df = firms_df[idx]
        firm_id = str(row_df[0, 0])
        tic = str(row_df[0, 1])
        conm = str(row_df[0, 2])
        normalized_name = str(row_df[0, 3])

        # Get name_variants - it's a Polars Series
        name_variants_series = row_df[0, 4]

        names_to_check = [normalized_name]

        # Handle name_variants - convert Series to list
        if name_variants_series is not None and len(name_variants_series) > 0:
            variants_list = name_variants_series.to_list()
            if variants_list:
                names_to_check.extend(variants_list)

        for firm_name in names_to_check:
            if firm_name:
                candidate_firms.append({
                    "name": str(firm_name),
                    "firm_id": firm_id,
                    "tic": tic,
                    "conm": conm
                })

    if not candidate_firms:
        return None

    # Use rapidfuzz to find best match
    result = process.extractOne(
        normalized_inst,
        [cf["name"] for cf in candidate_firms],
        scorer=fuzz.WRatio
    )

    if result and result[1] >= threshold:
        best_match_name = result[0]
        best_score = result[1]

        # Find the corresponding firm
        for cf in candidate_firms:
            if cf["name"] == best_match_name:
                return {
                    "firm_id": cf["firm_id"],
                    "tic": cf["tic"],
                    "conm": cf["conm"],
                    "similarity_score": best_score / 100.0  # Convert to 0-1 scale
                }

    return None


def main():
    """Main execution function."""
    print("=" * 80)
    print("FILTERED PUBLICATION-TO-FIRM MATCHING (OPTIMIZED)")
    print("=" * 80)

    # Load institutions
    print("\n1. Loading institutions...")
    institutions = pl.read_parquet(INSTITUTIONS_PATH)

    # Filter to corporate institutions only
    corporate_institutions = institutions.filter(
        pl.col("institution_type_classified") == "corporate"
    )

    print(f"   Total institutions: {len(institutions):,}")
    print(f"   Corporate institutions: {len(corporate_institutions):,}")
    print(f"   Filtered out: {len(institutions) - len(corporate_institutions):,} (non-corporate)")

    # Load firms
    print("\n2. Loading firm reference data...")
    firms = pl.read_parquet(FIRMS_PATH)
    print(f"   Total firms: {len(firms):,}")

    # Prepare firms for matching
    firms_matching = firms.select([
        pl.col("GVKEY").alias("firm_id"),
        "tic",
        "conm",
        pl.col("conm_clean").alias("normalized_name"),
        "name_variants"
    ])

    # Build token index for fast blocking
    print("\n3. Building token index for fast matching...")
    token_index = build_token_index(firms_matching)
    print(f"   Index built with {len(token_index):,} unique tokens")

    # Initialize results storage
    all_matches = []

    # Match each corporate institution
    print("\n4. Matching corporate institutions to firms...")
    print(f"   Using similarity threshold: {NAME_SIMILARITY_THRESHOLD}%")

    total_to_match = len(corporate_institutions)
    matches_found = 0
    no_candidates = 0
    below_threshold = 0

    for i, inst_row in enumerate(corporate_institutions.iter_rows(named=True)):
        if (i + 1) % 1000 == 0:
            print(f"   Progress: {i+1:,}/{total_to_match:,} ({100*(i+1)/total_to_match:.1f}%) | "
                  f"Matches: {matches_found:,}")

        inst_name = inst_row["display_name"]
        inst_id = inst_row["institution_id"]

        # Find candidate firms using token blocking
        candidates = find_candidate_firms(inst_name, token_index, firms_matching)

        if not candidates:
            no_candidates += 1
            continue

        # Find best match among candidates
        match = find_best_match(inst_name, candidates, firms_matching)

        if match:
            matches_found += 1
            all_matches.append({
                "institution_id": inst_id,
                "display_name": inst_name,
                "institution_type": inst_row["institution_type_classified"],
                "firm_id": match["firm_id"],
                "tic": match["tic"],
                "conm": match["conm"],
                "similarity_score": match["similarity_score"]
            })
        else:
            below_threshold += 1

    # Convert to DataFrame
    print("\n5. Creating results DataFrame...")
    matches_df = pl.DataFrame(all_matches)

    # Summary statistics
    print("\n" + "=" * 80)
    print("MATCHING RESULTS SUMMARY")
    print("=" * 80)
    print(f"\nTotal corporate institutions: {total_to_match:,}")
    print(f"Institutions with matches: {matches_found:,} ({100*matches_found/total_to_match:.2f}%)")
    print(f"  - No candidates found: {no_candidates:,} ({100*no_candidates/total_to_match:.2f}%)")
    print(f"  - Candidates below threshold: {below_threshold:,} ({100*below_threshold/total_to_match:.2f}%)")

    if len(matches_df) > 0:
        print(f"\nSimilarity score distribution:")
        print(matches_df.select(
            pl.col("similarity_score").min().alias("min"),
            pl.col("similarity_score").max().alias("max"),
            pl.col("similarity_score").mean().alias("mean"),
            pl.col("similarity_score").median().alias("median")
        ))

        print(f"\nHigh-confidence matches (≥0.95): {len(matches_df.filter(pl.col('similarity_score') >= 0.95)):,}")
        print(f"Medium-confidence matches (0.90-0.95): {len(matches_df.filter((pl.col('similarity_score') >= 0.90) & (pl.col('similarity_score') < 0.95))):,}")
        print(f"Lower-confidence matches (0.85-0.90): {len(matches_df.filter((pl.col('similarity_score') >= 0.85) & (pl.col('similarity_score') < 0.90))):,}")

        # Show sample matches
        print(f"\nSample high-confidence matches:")
        sample = (
            matches_df
            .filter(pl.col("similarity_score") >= 0.95)
            .sort("similarity_score", descending=True)
            .head(10)
        )
        print(sample.select([
            "display_name", "conm", "tic", "similarity_score"
        ]))

        # Check for potential false positives
        print(f"\nChecking for potential false positives (low-score matches):")
        low_score = (
            matches_df
            .filter(pl.col("similarity_score") < 0.90)
            .sort("similarity_score", descending=True)
            .head(10)
        )
        if len(low_score) > 0:
            print(low_score.select([
                "display_name", "conm", "tic", "similarity_score"
            ]))
        else:
            print("   No low-score matches found!")

    # Save results
    print(f"\n6. Saving results to {OUTPUT_PATH}...")
    matches_df.write_parquet(OUTPUT_PATH)
    print("   Done!")

    print("\n" + "=" * 80)
    print("FILTERED MATCHING COMPLETE")
    print("=" * 80)
    print(f"\nKey improvements:")
    print(f"  - Filtered out {len(institutions) - len(corporate_institutions):,} non-corporate institutions")
    print(f"  - Used token-based blocking for fast candidate selection")
    print(f"  - Applied high similarity threshold ({NAME_SIMILARITY_THRESHOLD}%)")
    print(f"  - Achieved {100*matches_found/total_to_match:.1f}% match rate on corporate institutions")
    print("=" * 80)


if __name__ == "__main__":
    main()
