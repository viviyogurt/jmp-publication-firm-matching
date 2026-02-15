# Institution Classification: Quick Reference

## Problem Summary

**Your current `ai_papers_firms_only.parquet` has 60-70% false positives.**

### Example of the Problem

```
Paper: W2055043387

Your Dataset (WRONG):
  affiliations_firm_count: 1 ❌
  affiliations_university_count: 0 ❌
  has_firm_affiliation: True ❌

Actual Affiliations:
  • National Center for Biotechnology Information → facility (not firm!)
  • NIH → government (not firm!)
  • Penn State → education (not firm!)
  • University of Arizona → education (not firm!)

CORRECT Classification:
  firm_count: 0 ✅
  university_count: 2 ✅
  government_count: 3 ✅
  facility_count: 3 ✅
  firm_ratio: 0.00 ✅
  has_firm_affiliation: False ✅
```

---

## The Solution

**Use OpenAlex API to get official institution types.**

### OpenAlex Institution Types

| Type | Examples | Include as Firm? |
|------|----------|------------------|
| `company` | IBM, Google, Microsoft | ✅ YES |
| `education` | MIT, Stanford, Penn State | ❌ NO |
| `government` | NIH, NASA, Max Planck | ❌ NO |
| `facility` | NCBI, National Labs | ⚠️ REVIEW |
| `nonprofit` | Gates Foundation | ❌ NO |

### Firm Ratio Calculation

```
firm_ratio = firm_count / total_affiliations

Where:
- firm_count: Number of affiliations with type="company"
- total_affiliations: All affiliations across all authors
```

---

## How to Use

### 1. Run Demo

```bash
cd /home/kurtluo/yannan/jmp
python src/01_data_construction/correct_institution_classifications/correct_institution_classifications.py demo
```

### 2. Process Sample

```bash
# Process 1000 papers
python src/01_data_construction/correct_institution_classifications/correct_institution_classifications.py process 1000
```

### 3. Filter for Firms

```python
import pandas as pd

df = pd.read_parquet('ai_papers_with_correct_classifications.parquet')

# Papers with any firm affiliation
firms_only = df[df['has_firm_affiliation'] == True]

# Papers with majority firm affiliations (firm_ratio > 0.5)
firms_majority = df[df['firm_ratio'] > 0.5]

print(f"Total papers: {len(df):,}")
print(f"With any firm affiliation: {len(firms_only):,}")
print(f"With majority firm affiliations: {len(firms_majority):,}")
```

---

## Key Files

| File | Purpose |
|------|---------|
| `src/01_data_construction/correct_institution_classifications/correct_institution_classifications.py` | Main script |
| `src/01_data_construction/correct_institution_classifications/README.md` | Full documentation |
| `docs/firm_dataset_validation_report.md` | Validation of broken dataset |
| `docs/openalex_classification_final_solution.md` | Complete solution guide |

---

## Status

✅ Script created in correct location (`src/01_data_construction/correct_institution_classifications/`)
✅ Demo working correctly
✅ Handles multiple authors and affiliations per paper
✅ Calculates firm_count and firm_ratio correctly

**Next:** Process sample dataset and validate results
