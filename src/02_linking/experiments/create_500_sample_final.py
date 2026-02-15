"""
Create 500-Match Sample for MANUAL Validation - Final Check
Different seed from previous samples (seed=2025)
"""
import polars as pl
from pathlib import Path

PROJECT_ROOT = Path("/home/kurtluo/yannan/jmp")
FILTERED_MATCHES = PROJECT_ROOT / "data/processed/linking/publication_firm_matches_filtered.parquet"
OUTPUT_FILE = PROJECT_ROOT / "data/processed/linking/validation_sample_500_final.csv"

# Sample 500 matches with different seed
df = pl.read_parquet(FILTERED_MATCHES)

# Select columns needed for validation
df_sample = df.select([
    "institution_id",
    "display_name",
    "gvkey",
    "conm",
    "match_method",
    "confidence",
    "paper_count"
])

# Sample with seed=2025 (different from 999, 42, and 2024)
df_sample = df_sample.sample(n=500, seed=2025)

# Sort for easier manual review
df_sample = df_sample.sort("display_name")

# Save
df_sample.write_csv(OUTPUT_FILE)

print(f"Created 500-match validation sample: {OUTPUT_FILE}")
print(f"Total matches in filtered population: {len(df):,}")
print(f"Sample size: 500")
print(f"Random seed: 2025 (different from all previous samples)")
print(f"\nFiltered dataset statistics:")
print(f"  Institutions: {df['institution_id'].n_unique():,}")
print(f"  Firms: {df['gvkey'].n_unique():,}")
print(f"  Papers: {df['paper_count'].sum():,}")
