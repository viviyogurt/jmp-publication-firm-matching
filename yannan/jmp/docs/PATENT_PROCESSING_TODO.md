# Patent Processing - Priority Todo List for Code Agent

## 🔥 Critical Fixes (Do First)

### Problem 1: Memory Constraints (Process Killed)
- **Issue:** Full dataset (9.3M patents) → exit code 137
- **Solution:** Filter by CPC FIRST (95% reduction), use Polars lazy loading
- **Script:** `02_filter_ai_patents_fixed.py`
- **Expected:** Memory <8GB

```bash
# Run the fixed script
python src/04_patent_processing/scripts/02_filter_ai_patents_fixed.py
```

### Problem 2: High AI Patent Rate (40.7% - Too High)
- **Issue:** UNION approach (CPC OR text) too permissive
- **Solution:** INTERSECTION approach (CPC AND text)
- **Target:** 5-10% AI patent rate (literature-consistent)

### Problem 3: High Unknown Rate (93.6%)
- **Issue:** Title-only classification insufficient
- **Solution:** Use abstracts with refined keywords
- **Target:** <20% Unknown

### Problem 4: False Positives (Generic Keywords)
- **Issue:** "method", "system", "associated" trigger false classification
- **Solution:** Refined keywords in `keyword_lists_refined.py`
- **Target:** >85% precision

---

## ✅ Implementation Steps

### Step 1: Run Fixed Classification Script
```bash
cd /path/to/jmp-project/yannan/jmp
python src/04_patent_processing/scripts/02_filter_ai_patents_fixed.py
```

**Expected outputs:**
- AI patents: ~100K-200K (5-10% of CPC-matched)
- Unknown rate: <20%
- Classification: Balanced distribution
- Memory: <8GB

### Step 2: Review Validation Samples
Script automatically outputs 5 samples from each category. Check:
- Are Infrastructure samples actually AI hardware?
- Are Algorithm samples actually ML methods?
- Are Application samples actual AI applications?
- Are Unknown samples truly ambiguous?

### Step 3: Manual Validation (2-4 hours)
```bash
# Review validation set
less /Data/patent/processed/validation_set_for_manual_review.csv
```

**Steps:**
1. Add `manual_label` column to CSV
2. Label 250 patents (50 from each category)
3. Compute precision/recall metrics

### Step 4: Compute Metrics
```python
import pandas as pd
from sklearn.metrics import classification_report

validation = pd.read_csv('/Data/patent/processed/validation_set_for_manual_review.csv')
validation['manual_label'] = ...  # Your manual labels

print(classification_report(
    validation['manual_label'],
    validation['ai_category_primary']
))
```

**Targets:**
- Precision: >85%
- Recall: >60%
- Unknown: <20%

### Step 5: Iterate (If Metrics Poor)
If precision <85% or Unknown >20%:
1. Analyze misclassified examples
2. Identify missing keywords
3. Update `keyword_lists_refined.py`
4. Re-run `02_filter_ai_patents_fixed.py`
5. Repeat until metrics acceptable

---

## 📋 Quality Checklist

After running `02_filter_ai_patents_fixed.py`, verify:

- [ ] **Memory:** Process didn't get killed (exit code ≠ 137)
- [ ] **AI rate:** 5-10% (not 40%)
- [ ] **Unknown rate:** <20% (not 93%)
- [ ] **Samples look correct:** Manual inspection of 20 samples
- [ ] **Distribution reasonable:**
  - Infrastructure: 15-20%
  - Algorithm: 40-50%
  - Application: 20-30%
  - Unknown: <20%

---

## 🎯 Success Criteria

### Must Have (Blocking Issues)
- ✅ Memory <8GB (not killed)
- ✅ AI rate 5-10% (literature-consistent)
- ✅ Abstracts used (not titles)
- ✅ INTERSECTION approach (CPC AND text)

### Should Have (Quality Targets)
- ✅ Unknown rate <20%
- ✅ Precision >85%
- ✅ Sample inspection passes manual review

### Nice to Have (Improvements)
- Multi-label classification implemented
- Context-aware keywords (e.g., "dropout" in ML context)
- Validation set created for manual review

---

## 📊 Expected Results

| Metric | Before | After | Target |
|--------|--------|-------|--------|
| **Memory** | Killed (137) | <8GB | <16GB |
| **AI rate** | 40.7% | 5-10% | 5-10% |
| **Unknown** | 93.6% | <20% | <20% |
| **Precision** | ~30% | >85% | >85% |
| **Distribution** | Skewed | Balanced | Reasonable |

---

## 🚀 After Phase 2 Complete

Continue to remaining phases:

### Phase 3: Match Patents to Firms
- Load `g_patent_assignee.tsv` and `g_assignee_disambiguated.tsv`
- Fuzzy match assignee names to Compustat GVKEYs
- Expected: 15K-40K AI patents matched to 1.5K-3K firms

### Phase 4: Create Firm-Year Panel
- Aggregate by GVKEY and year
- Merge with CRSP-CCM financials
- Expected: 5K-15K firm-year observations

### Phase 5: Integration
- Merge with existing publication data
- Ready for translation efficiency analysis

---

## 📞 If Issues Persist

### If still out of memory:
1. Process in yearly chunks (see Expert Guide)
2. Use even more selective CPC filter
3. Rent high-memory server (32-64GB RAM)

### If AI rate still too high (>15%):
1. Make keywords more specific
2. Require multiple keyword matches
3. Add negative keywords to exclude

### If Unknown rate still too high (>30%):
1. Add more keywords to each category
2. Check if abstracts loaded correctly
3. May need LLM-based classification

---

## 📖 Reference Documentation

All details in:
- `docs/PATENT_PROCESSING_EXPERT_GUIDE.md` - Comprehensive analysis
- `docs/CLAUDE_PROMPT_PATENT_PROCESSING.md` - Original prompt
- `src/04_patent_processing/README.md` - Implementation guide

---

## Summary

**Immediate action:** Run `02_filter_ai_patents_fixed.py`

**Expected outcome:**
- Memory-efficient processing
- Literature-consistent AI rate (5-10%)
- Low Unknown rate (<20%)
- High precision (>85%)
- Ready for Phase 3 (firm matching)

**Time estimate:** 2-3 hours for Phase 2, 4-8 hours for validation/iteration
