# Publication Matching Implementation Progress

**Date:** 2026-02-15
**Status:** Phase 1 In Progress
**Target:** 6,500-7,500 firms @ 95.5-96.5% accuracy

---

## Implementation Summary

### Completed Methods

#### Method 1.1: Wikidata Ticker Matching ✅
**File:** `src/02_linking/match_publications_wikidata_tickers.py`
**Status:** Script complete, but data unavailable
**Result:** 0 matches (Wikidata tickers field is empty)

**Issue:** The `publication_institutions_wikidata_structured.parquet` file has empty ticker lists.
The script is ready for when ticker data becomes available.

**Expected if data available:** 1,800-2,200 firms @ 99.5% accuracy

#### Method 1.4: Enhanced Acronym Matching ✅
**File:** `src/02_linking/match_publications_acronyms_enhanced.py`
**Status:** Complete, but low quality as expected

**Results:**
- Total matches: 72,254
- Unique institutions: 12,541
- Unique firms: 12,685
- Mean name similarity: 0.412 (very low)
- 72,252/72,254 matches at 0.92 confidence (no validation)
- Only 2 matches at 0.95 confidence

**Assessment:** Confirms acronym matching is problematic. Most matches lack validation
and have very low name similarity. This method should be used cautiously or excluded
from final dataset.

---

### In Progress

#### Method 1.3: Contained Name Matching 🔄
**File:** `src/02_linking/match_publications_contained_name.py`
**Status:** Running (slow due to nested loop)

**Progress:** ~5,000/16,278 institutions processed in ~4 minutes
**Estimated completion:** ~12 more minutes

**Expected:** 1,200-1,500 firms @ 97% accuracy

**Issue:** Current implementation has O(n×m) complexity (16k institutions × 16k firms).
This is inefficient. Need to optimize with:
- Pre-filtering by first letter
- Using string containment lookup
- Parallel processing

#### Method 2.1: Conservative Fuzzy Matching 📝
**File:** `src/02_linking/match_publications_fuzzy_conservative.py`
**Status:** Script complete, ready to run

**Features:**
- Higher threshold than patents (JW ≥0.90 vs 0.85)
- Multiple validation checks required
- Confidence scoring 0.94-0.99
- Strict filtering rules

**Expected:** +1,500-2,500 firms @ 95-97% accuracy

---

### Not Started

#### Method 1.2: Enhanced Homepage Domain Matching ⏳
**Status:** Existing script exists, needs enhancement
**Requirements:**
- Remove 12 identified incorrect matches
- Add domain-to-firm validation for multi-firm domains
- Cross-validate with country code

**Expected:** 2,000-2,500 firms @ 98.5% accuracy

#### Method 1.5: Alternative Names with Validation ⏳
**Status:** Not started
**Risk:** High (previous attempt had 89.7% error rate)
**Requirements:**
- Strict validation: country + business + URL (ALL required)
- Pilot on 1,000 institutions first
- Validate 500 samples
- Abandon if accuracy <93%

**Expected:** 600-1,000 firms @ 95% accuracy (if successful)

#### Method 3.1: Enhanced Parent Institution Cascade ⏳
**Status:** Not started
**Requirements:**
- Build parent-child graph
- Seed from Stage 1 matches
- Cascade to children
- Validate with country/URL consistency

**Expected:** +800-1,200 firms @ 94-96% accuracy

#### Combination & Deduplication ⏳
**File:** `src/02_linking/combine_publication_matches_final.py`
**Status:** Not started

#### Final Validation ⏳
**File:** `src/02_linking/validate_publication_matches_final.py`
**Status:** Not started

---

## Data Availability Issues

### Issue 1: Empty Wikidata Tickers
**Expected:** 2,500 institutions with ticker data
**Actual:** Tickers column exists but is empty for all rows

**Impact:** Cannot implement Method 1.1 as planned
**Workaround:** Skip this method, rely on other high-confidence methods

### Issue 2: Inefficient Contained Name Matching
**Current runtime:** ~12-15 minutes for 16k institutions
**Issue:** O(n×m) nested loop is too slow

**Solution needed:** Optimize with:
```python
# Pre-filter by first letter
institutions_by_first_letter = {
    letter: insts for letter, insts in group_by(institutions, first_char)
}

# For each firm, only check institutions with matching first letter
for firm in firms:
    first_char = firm.conm_clean[0]
    candidates = institutions_by_first_letter.get(first_char, [])
    # Check containment only in candidates
```

---

## Current Coverage Estimate

Based on completed and running methods:

| Method | Firms | Status |
|--------|-------|--------|
| 1.1 Wikidata ticker | 0 | Data unavailable |
| 1.2 Homepage domain | TBD | Existing, needs enhancement |
| 1.3 Contained name | ~500-800 | Running, slow |
| 1.4 Acronym | 12,685 | Complete, low quality |
| 1.5 Alt names | TBD | Not started, high risk |
| 2.1 Fuzzy | TBD | Script ready |
| 3.1 Parent | TBD | Not started |

**Note:** Acronym matching (1.4) has 12,685 firms but most at 0.92 confidence with
very low name similarity (mean 0.41). These should be filtered out or treated
as low-confidence.

---

## Next Steps (Priority Order)

### Immediate (Today)
1. **Wait for contained name matching to complete** or kill and optimize
2. **Run fuzzy matching** (Method 2.1) - script is ready
3. **Enhance homepage domain matching** (Method 1.2) - remove 12 incorrect matches
4. **Check actual results** from all methods to assess quality

### Short-term (This Week)
5. **Skip Method 1.5** (Alternative names) - too risky given 89.7% error rate
6. **Implement Method 3.1** (Parent cascade) - can build on Method 1.3 results
7. **Combine all methods** - deduplicate and filter by confidence
8. **Sample validation** - validate 500-1,000 matches stratified by method

### Medium-term (Next Week)
9. **Full validation** - 1,000 sample validation, error analysis
10. **Bias testing** - geographic, industry, size coverage
11. **Final report** - accuracy, coverage, error taxonomy

---

## Expected Final Coverage (Revised)

Given the data issues and early results:

**Optimistic Scenario:**
- Method 1.2 (Homepage): 2,000 firms @ 98.5%
- Method 1.3 (Contained): 1,200 firms @ 97%
- Method 2.1 (Fuzzy): 2,000 firms @ 96%
- Method 3.1 (Parent): 800 firms @ 95%
- **Total: ~6,000 firms @ 96% accuracy** (32% of CRSP)

**Conservative Scenario:**
- Method 1.2 (Homepage): 1,500 firms (after removing 12 errors)
- Method 1.3 (Contained): 800 firms
- Method 2.1 (Fuzzy): 1,500 firms
- Method 3.1 (Parent): 600 firms
- **Total: ~4,400 firms @ 96% accuracy** (23.5% of CRSP)

**Note:** Both scenarios fall short of the 6,500-7,500 target but meet the
≥95% accuracy requirement. The lower coverage is acceptable given the
conservative validation approach.

---

## Files Created

1. `src/02_linking/match_publications_wikidata_tickers.py` - ✅ Complete (data unavailable)
2. `src/02_linking/match_publications_contained_name.py` - ✅ Complete (running, slow)
3. `src/02_linking/match_publications_acronyms_enhanced.py` - ✅ Complete (low quality)
4. `src/02_linking/match_publications_fuzzy_conservative.py` - ✅ Complete (ready to run)

---

## Files Still Needed

1. `src/02_linking/match_homepage_domains_enhanced.py` - Enhance existing
2. `src/02_linking/match_publications_alt_names_validated.py` - MAY SKIP (too risky)
3. `src/02_linking/match_parent_institutions_enhanced.py` - Enhance existing
4. `src/02_linking/combine_publication_matches_final.py` - Create
5. `src/02_linking/validate_publication_matches_final.py` - Create

---

## Recommendations

### 1. Skip Method 1.5 (Alternative Names)
**Reason:** Previous attempt had 89.7% error rate. Even with strict validation,
the risk is too high. Time is better spent on:
- Optimizing Method 1.3 (Contained name)
- Improving Method 2.1 (Fuzzy) validation
- Method 3.1 (Parent cascade)

### 2. Optimize Contained Name Matching
**Current:** O(n×m) = 16,278 × 16,274 = 265M comparisons
**Optimized:** Pre-filter by first letter → ~10M comparisons (96% reduction)

### 3. Filter Acronym Matches Aggressively
**Current:** 72,254 matches, most at 0.92 confidence
**Recommendation:** Only keep matches with:
- Confidence ≥0.95, OR
- Country match AND name similarity ≥0.60

This will likely reduce to ~2,000-3,000 high-quality matches.

### 4. Prioritize Validation
**Before combining all methods:**
- Validate 500 samples from Method 1.2 (Homepage)
- Validate 500 samples from Method 1.3 (Contained)
- Validate 500 samples from Method 2.1 (Fuzzy)

**If any method <95% accuracy:** Adjust or abandon that method.

---

## Conclusion

The implementation is progressing but facing challenges:
- **Data issue:** Wikidata tickers unavailable
- **Performance issue:** Contained name matching slow
- **Quality issue:** Acronym matching low quality as expected

**Next critical step:** Run fuzzy matching (Method 2.1) and validate results
from each method before proceeding further.

**Expected outcome:** 4,400-6,000 firms @ 96% accuracy (below 6,500 target but
meets quality requirements).
