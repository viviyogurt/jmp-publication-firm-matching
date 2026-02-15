# How to Use: Correct Institution Classifications

## Overview

The script `correct_institution_classifications.py` corrects the institution classifications in your AI papers dataset using OpenAlex's official institution types.

## Location

```
/home/kurtluo/yannan/jmp/src/01_data_construction/correct_institution_classifications/
```

## Key Features

1. **Processes each institution separately** for each paper
2. **Calculates firm_count and firm_ratio** at the paper level
3. **Uses OpenAlex API** to get official institution types
4. **Handles multiple authors** with multiple affiliations per paper

## Firm Ratio Calculation

```
firm_ratio = firm_count / total_affiliations

Where:
- firm_count: Number of affiliations that are companies (OpenAlex type="company")
- total_affiliations: Total number of affiliations across all authors
```

## Usage

### 1. Run Demonstration (First 3 Papers)

```bash
cd /home/kurtluo/yannan/jmp
python src/01_data_construction/correct_institution_classifications/correct_institution_classifications.py demo
```

Output shows:
- Each affiliation with its OpenAlex type
- Classification (FIRM ✅, UNIVERSITY ❌, GOVERNMENT ❌, FACILITY ⚠️)
- Summary statistics for each paper

### 2. Process Sample Dataset

```bash
cd /home/kurtluo/yannan/jmp

# Process 1000 papers
python src/01_data_construction/correct_institution_classifications/correct_institution_classifications.py process 1000

# Process 10000 papers
python src/01_data_construction/correct_institution_classifications/correct_institution_classifications.py process 10000

# Process with custom output path
python src/01_data_construction/correct_institution_classifications/correct_institution_classifications.py process 1000 /path/to/output.parquet
```

Output:
- Saves to: `data/processed/publication/ai_papers_with_correct_classifications.parquet`
- Prints summary statistics

### 3. Process Full Dataset (Not Yet Implemented)

For the full dataset, you'll need to:
1. Modify the script to handle all row groups
2. Add progress tracking
3. Handle rate limiting properly
4. Save intermediate results periodically

## Example Output

### Paper 1: https://openalex.org/W2055043387

```
Affiliations:
  ⚠️ National Center for Biotechnology Information (facility)
  ❌ National Institutes of Health (government)
  ❌ Pennsylvania State University (education)
  ❌ University of Arizona (education)

SUMMARY:
  Total affiliations: 8
  Firms: 0
  Universities: 2
  Government: 3
  Facilities: 3
  Firm ratio: 0.00
  Has firm affiliation: False
```

### Paper with Mixed Affiliations (Hypothetical)

```
Affiliations:
  ✅ IBM Research (company)
  ❌ MIT (education)
  ✅ Google (company)
  ❌ Stanford (education)

SUMMARY:
  Total affiliations: 4
  Firms: 2
  Universities: 2
  Firm ratio: 0.50 (50%)
  Has firm affiliation: True
```

## Output Schema

The script creates a parquet file with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| paper_id | string | OpenAlex paper ID |
| openalex_id | string | OpenAlex work ID |
| publication_year | int | Year of publication |
| firm_count | int | Number of company affiliations |
| university_count | int | Number of university affiliations |
| government_count | int | Number of government affiliations |
| facility_count | int | Number of facility affiliations |
| other_count | int | Number of other affiliations |
| total_count | int | Total number of affiliations |
| firm_ratio | float | firm_count / total_count |
| has_firm_affiliation | boolean | True if firm_count > 0 |
| has_university_affiliation | boolean | True if university_count > 0 |
| has_government_affiliation | boolean | True if government_count > 0 |
| affiliation_classifications | list | Detailed classification per affiliation |

## Filtering for Firm-Only Papers

After processing, filter for papers with firm affiliations:

```python
import pandas as pd

# Load corrected data
df = pd.read_parquet('data/processed/publication/ai_papers_with_correct_classifications.parquet')

# Option 1: Any firm affiliation
firms_any = df[df['has_firm_affiliation'] == True]

# Option 2: Majority firm (firm_ratio > 0.5)
firms_majority = df[df['firm_ratio'] > 0.5]

# Option 3: Pure firm (no university/government)
firms_pure = df[
    (df['has_firm_affiliation'] == True) &
    (df['has_university_affiliation'] == False) &
    (df['has_government_affiliation'] == False)
]

print(f"Total papers: {len(df):,}")
print(f"Papers with any firm affiliation: {len(firms_any):,}")
print(f"Papers with majority firm affiliation: {len(firms_majority):,}")
print(f"Papers with pure firm affiliation: {len(firms_pure):,}")
```

## Expected Results

Based on the validation:

| Metric | Current (Broken) | Corrected |
|--------|------------------|------------|
| Papers in "firms_only" | 1,391,063 | ~200,000-400,000 |
| False positive rate | 60-70% | <5% |
| Firm ratio distribution | Skewed (many 1.0) | Realistic mix |

## Rate Limiting

The script includes rate limiting:
- 0.05 seconds per API call = ~20 requests/second
- Safe for OpenAlex API (polite usage)

For the full dataset (17M papers):
- Estimated time: Depends on total affiliations
- Consider batch processing overnight
- Save intermediate results

## Troubleshooting

### Error: "Field does not exist in table schema"

This occurs when reading nested array columns. The script handles this by using `as_py()` to convert to Python lists.

### Error: "HTTP 429" (Too Many Requests)

Increase the rate limiting:
```python
time.sleep(0.1)  # 10 requests per second instead of 20
```

### Slow Processing

For faster processing:
1. Use parallel processing (multiple workers)
2. Cache OpenAlex API responses
3. Process in batches and save intermediate results

## Next Steps

1. ✅ Run demo to verify correctness
2. ⏳ Process sample (1000-10000 papers)
3. ⏳ Validate results manually
4. ⏳ Process full dataset
5. ⏳ Create new firm-only dataset
6. ⏳ Run your analysis

---

**Status:** Ready to use ✅
**Last Updated:** 2025-02-08
**Location:** `src/01_data_construction/correct_institution_classifications/`
