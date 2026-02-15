"""Quick analysis of ai_papers batch file"""

import polars as pl
from pathlib import Path

batch_file = Path("/home/kurtluo/yannan/jmp/data/raw/publication/ai_papers_batches/batch_000000.parquet")

print("=" * 80)
print("ANALYZING AI PAPERS BATCH FILE")
print("=" * 80)
print(f"File: {batch_file}")
print()

# Read the file
df = pl.read_parquet(batch_file)

# Convert binary columns to string if needed
for col in df.columns:
    if df[col].dtype == pl.Binary:
        df = df.with_columns(pl.col(col).cast(pl.Utf8).alias(col))

print(f"Total rows: {len(df):,}")
print(f"Total columns: {len(df.columns)}")
print()

print("=" * 80)
print("COLUMN NAMES AND TYPES")
print("=" * 80)
for col, dtype in df.schema.items():
    print(f"  {col:30s} : {dtype}")
print()

print("=" * 80)
print("MISSING VALUES PER COLUMN")
print("=" * 80)
for col in df.columns:
    null_count = df[col].null_count()
    null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0
    print(f"  {col:30s} : {null_count:>10,} ({null_pct:>5.1f}%)")
print()

print("=" * 80)
print("SAMPLE DATA (first 3 rows)")
print("=" * 80)
print(df.head(3))
print()

print("=" * 80)
print("DATA SUMMARY")
print("=" * 80)
print(df.describe())
print()

# Analyze JSON column
if 'json' in df.columns:
    print("=" * 80)
    print("JSON COLUMN ANALYSIS")
    print("=" * 80)
    non_null_json = df.filter(pl.col('json').is_not_null())
    print(f"Rows with json: {len(non_null_json):,} ({len(non_null_json)/len(df)*100:.1f}%)")
    if len(non_null_json) > 0:
        # Show first JSON (truncated)
        first_json = non_null_json['json'][0]
        if isinstance(first_json, str):
            print(f"First JSON length: {len(first_json):,} characters")
            print(f"\nFirst 1000 chars of JSON:")
            print(first_json[:1000])
            print("...")
            
            # Try to parse and show structure
            try:
                import json as json_lib
                parsed = json_lib.loads(first_json)
                print(f"\nParsed JSON keys: {list(parsed.keys())}")
                print(f"Top-level structure:")
                for key, value in list(parsed.items())[:10]:
                    if isinstance(value, (dict, list)):
                        print(f"  {key}: {type(value).__name__} (length: {len(value) if hasattr(value, '__len__') else 'N/A'})")
                    else:
                        print(f"  {key}: {type(value).__name__} = {str(value)[:100]}")
            except Exception as e:
                print(f"Could not parse JSON: {e}")
print()

# Check arxiv_id distribution
print("=" * 80)
print("ARXIV_ID DISTRIBUTION")
print("=" * 80)
arxiv_count = df.filter(pl.col('arxiv_id').str.strip() != '').height
print(f"Rows with arxiv_id: {arxiv_count:,} ({arxiv_count/len(df)*100:.1f}%)")
print(f"Rows without arxiv_id: {len(df) - arxiv_count:,} ({(len(df) - arxiv_count)/len(df)*100:.1f}%)")
print()

# Check DOI distribution
print("=" * 80)
print("DOI DISTRIBUTION")
print("=" * 80)
doi_count = df.filter(pl.col('doi').str.strip() != '').height
print(f"Rows with DOI: {doi_count:,} ({doi_count/len(df)*100:.1f}%)")
print(f"Rows without DOI: {len(df) - doi_count:,} ({(len(df) - doi_count)/len(df)*100:.1f}%)")
print()

# Check unique values for categorical columns
print("=" * 80)
print("UNIQUE VALUES FOR KEY COLUMNS")
print("=" * 80)
for col in ['work_type', 'language', 'primary_ai_category', 'is_llm', 'is_machine_learning']:
    if col in df.columns:
        unique_count = df[col].n_unique()
        print(f"\n{col}: {unique_count} unique values")
        if unique_count <= 20:
            value_counts = df[col].value_counts().sort('count', descending=True)
            print(value_counts)
print()
