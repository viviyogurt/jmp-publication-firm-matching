"""
Random Sample Validation of Final Matches

Randomly selects 1000 matches from the final filtered dataset for manual validation.
Displays detailed information for each match to assess quality.
"""
import polars as pl
from pathlib import Path
import random

# Set seed for reproducibility
random.seed(42)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_FILE = PROJECT_ROOT / "data" / "processed" / "linking" / "publication_firm_matches_stage1_final.parquet"

# Load data
print("=" * 120)
print("RANDOM SAMPLE VALIDATION - FINAL MATCHES")
print("=" * 120)
print(f"\nLoading data from: {DATA_FILE}")

df = pl.read_parquet(DATA_FILE)
print(f"Total matches: {len(df):,}")
print(f"Unique institutions: {df['institution_id'].n_unique():,}")
print(f"Unique firms: {df['GVKEY'].n_unique():,}")

# Take random sample of 1000
sample_size = min(1000, len(df))
sample_df = df.sample(sample_size, seed=42)

print(f"\n{'=' * 120}")
print(f"RANDOM SAMPLE: {sample_size} MATCHES")
print(f"{'=' * 120}\n")

# Display each match
for i, row in enumerate(sample_df.iter_rows(named=True), 1):
    print(f"[{i:4d}] Institution: {row['institution_display_name'][:70]}")

    # Get institution ID (shorten if needed)
    inst_id = row['institution_id']
    if inst_id.startswith('https://openalex.org/'):
        inst_id_short = inst_id.split('/')[-1][:30]
    else:
        inst_id_short = inst_id[:30]

    print(f"      ID: {inst_id_short}")
    print(f"      Papers: {row['institution_paper_count']:,}")

    # Match details
    print(f"      → Matched Firm: {row['firm_conm']}")
    print(f"         GVKEY: {row['GVKEY']} | LPERMNO: {row.get('LPERMNO', 'N/A')}")
    print(f"         Method: {row['match_method']:40} | Type: {row['match_type']:20}")
    print(f"         Confidence: {row['match_confidence']:.3f}")

    # Additional fields if present
    if 'matched_value' in row and row['matched_value'] is not None:
        print(f"         Matched Value: {row['matched_value']}")

    if 'matched_domain' in row and row['matched_domain'] is not None:
        print(f"         Matched Domain: {row['matched_domain']}")

    if 'matched_ticker' in row and row['matched_ticker'] is not None:
        print(f"         Matched Ticker: {row['matched_ticker']}")

    print()

# Summary statistics
print("=" * 120)
print("SAMPLE STATISTICS")
print("=" * 120)

# Match type breakdown in sample
type_counts = sample_df.group_by('match_type').agg(
    pl.len().alias('count')
).sort('count', descending=True)

print("\nMatch Type Distribution:")
for row in type_counts.iter_rows(named=True):
    pct = row['count'] / sample_size * 100
    print(f"  {row['match_type']:30}: {row['count']:>4} ({pct:>5.1f}%)")

# Confidence distribution in sample
print("\nConfidence Distribution:")
conf_bins = [
    (0.98, 0.981, "0.98"),
    (0.97, 0.98, "0.97"),
    (0.96, 0.97, "0.96"),
    (0.95, 0.96, "0.95"),
]

for min_val, max_val, label in conf_bins:
    count = sample_df.filter(
        (pl.col('match_confidence') >= min_val) &
        (pl.col('match_confidence') < max_val)
    ).shape[0]
    pct = count / sample_size * 100
    print(f"  {label:>5}: {count:>4} ({pct:>5.1f}%)")

# Paper count distribution
print("\nPaper Count Distribution:")
paper_stats = sample_df.select(
    pl.col('institution_paper_count').min().alias('min'),
    pl.col('institution_paper_count').max().alias('max'),
    pl.col('institution_paper_count').mean().alias('mean'),
    pl.col('institution_paper_count').median().alias('median'),
)

for stat in ['min', 'max', 'mean', 'median']:
    val = paper_stats[stat][0]
    print(f"  {stat.capitalize():>8}: {val:,.1f}")

# Top institutions by paper count in sample
print("\nTop 20 Institutions by Paper Count (in sample):")
top_papers = sample_df.sort('institution_paper_count', descending=True).head(20)

for i, row in enumerate(top_papers.iter_rows(named=True), 1):
    print(f"  {i:2}. {row['institution_display_name'][:60]:60} | {row['institution_paper_count']:>8,} papers | {row['firm_conm'][:40]:40}")

print("\n" + "=" * 120)
