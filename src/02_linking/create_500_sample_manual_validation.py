"""
Create 500-Match Sample for MANUAL Validation
Different seed from 200-match sample (seed=999 vs seed=42)
"""
import polars as pl
from pathlib import Path

PROJECT_ROOT = Path("/home/kurtluo/yannan/jmp")
MATCHES_FILE = PROJECT_ROOT / "data/processed/linking/publication_firm_matches_optimized.parquet"
OUTPUT_FILE = PROJECT_ROOT / "data/processed/linking/validation_sample_500_manual.csv"

# Sample 500 matches with different seed
df = pl.read_parquet(MATCHES_FILE)

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

# Sample with seed=999 (different from 200-sample which used seed=42)
df_sample = df_sample.sample(n=500, seed=999)

# Sort for easier manual review
df_sample = df_sample.sort("display_name")

# Save
df_sample.write_csv(OUTPUT_FILE)

print(f"Created 500-match validation sample: {OUTPUT_FILE}")
print(f"Total matches in population: {len(df):,}")
print(f"Sample size: 500")
print(f"Random seed: 999 (different from 200-sample)")
