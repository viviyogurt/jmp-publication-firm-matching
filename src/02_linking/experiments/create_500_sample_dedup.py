"""
Create 500-Match Sample for MANUAL Validation - De-Duplicated Dataset
Random seed: 910 (different from all previous samples)

De-duplication strategy: Each institution maps to exactly ONE firm
(but a firm can have multiple institutions)
"""
import polars as pl
from pathlib import Path

PROJECT_ROOT = Path("/home/kurtluo/yannan/jmp")
DEDUP_MATCHES = PROJECT_ROOT / "data/processed/linking/publication_firm_matches_dedup.parquet"
OUTPUT_FILE = PROJECT_ROOT / "data/processed/linking/validation_sample_500_dedup.csv"

# Sample 500 matches with seed=910 (different from 999, 2024, 2025, and 3030)
df = pl.read_parquet(DEDUP_MATCHES)

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

# Sample with seed=910 (different from all previous samples)
df_sample = df_sample.sample(n=500, seed=910)

# Sort for easier manual review
df_sample = df_sample.sort("display_name")

# Save
df_sample.write_csv(OUTPUT_FILE)

print(f"Created 500-match validation sample: {OUTPUT_FILE}")
print(f"Total matches in de-duplicated population: {len(df):,}")
print(f"Sample size: 500")
print(f"Random seed: 910 (different from all previous samples)")
print(f"\nDe-duplicated dataset statistics:")
print(f"  Institutions: {df['institution_id'].n_unique():,} (one firm per institution)")
print(f"  Firms: {df['gvkey'].n_unique():,}")
print(f"  Papers: {df['paper_count'].sum():,}")
print(f"\nExpected accuracy: 97.6% (same as cleaned dataset)")
