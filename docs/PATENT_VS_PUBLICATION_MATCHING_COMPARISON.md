# Patent vs. Publication Matching: Methodology Comparison

**Date:** 2026-02-15
**Purpose:** Understand the successful patent matching approach and apply lessons to publications

---

## 📊 Executive Summary

**Patent Matching Results (SUCCESSFUL):**
- Coverage: 8,436 firms (45.1% of CRSP)
- Accuracy: 95.4% overall
- Methodology: 3-stage progressive matching

**Publication Matching Results (CURRENT):**
- Coverage: 1,951 firms (10.43% of CRSP)
- Accuracy: 91.8%
- Methodology: Baseline + contained name + manual mappings

**Key Insight:** Patent matching achieved 4.3x higher coverage while maintaining high accuracy through a **systematic 3-stage approach**.

---

## 🎯 Stage 1: Exact & High-Confidence Matching

### Patent Matching (Stage 1)

**Target:** >95% accuracy, 3,000-5,000 firms

**Strategies:**

1. **Exact Name Match** (confidence: 0.98)
   - Assignee clean name == Firm clean name
   - Checks both `conm_clean` and `conml_clean`
   - Example: "MICROSOFT CORP" == "MICROSOFT CORP"

2. **Ticker in Assignee Name** (confidence: 0.97)
   - Pattern: `Microsoft (MSFT) Research`
   - Checks for `(TICKER)` or standalone ticker
   - Example: "MSFT" appears in "MICROSOFT (MSFT) RESEARCH"

3. **Firm Name Contained** (confidence: 0.96)
   - Firm name is substring of assignee name
   - Requires ≥5 characters, ≥8 for common words
   - Example: "Microsoft Research Asia" contains "Microsoft"
   - **Validation:** Excludes "COMPANY", "CORPORATION", "INCORPORATED"

4. **Abbreviation Match** (confidence: 0.95)
   - Known abbreviations dictionary (IBM, AT&T, GE, etc.)
   - Generated abbreviations: first letters of each word
   - Ticker matches: MSFT == "MICROSOFT"
   - **85 mappings in ABBREVIATION_DICT**

**Key Implementation Details:**
```python
# Uses lookup dictionary for O(1) matching
assignee_lookup = {
    clean_name: [list of assignee rows]
}

# Exact match strategies first (0.98 confidence)
# Then other strategies if no exact match
# Deduplicate: keep highest confidence per firm-assignee pair
```

### Publication Matching (Current)

**Baseline Methods:**
- ✅ Homepage domain exact match (0.98 confidence)
- ✅ Contained name matching (0.96-0.97 confidence)
- ✅ Manual top company mappings (0.99 confidence)

**Missing Strategies:**
- ❌ No ticker-based matching (Wikidata tickers field empty)
- ❌ No abbreviation dictionary approach
- ❌ No contained-name validation (≥5 chars, ≥8 for common words)

---

## 🔍 Stage 2: Fuzzy Matching with Validation

### Patent Matching (Stage 2)

**Target:** 85-90% accuracy, +2,000-4,000 firms

**Fuzzy Matching Methods:**

1. **Jaro-Winkler Similarity**
   - Uses RapidFuzz `fuzz.ratio()`
   - Thresholds:
     - ≥0.90 → 0.94 confidence (high)
     - ≥0.85 → 0.90 confidence (medium)
     - <0.85 → reject

2. **Additional Metrics:**
   - Partial ratio (substring matches)
   - Token sort ratio (order-independent)

3. **Validation Boosts:**
   - **Business description boost:** +0.03-0.05
     - Check if assignee keywords appear in firm business description
     - ≥2 matching words: +0.05
     - 1 matching word: +0.03
   - **Location boost:** +0.00-0.03 (placeholder)
   - **Maximum confidence:** 0.99

4. **Excludes Stage 1 Matches:**
   - Only processes firms NOT matched in Stage 1
   - Only processes assignees NOT matched in Stage 1

**Key Implementation Details:**
```python
# Use RapidFuzz process.extract() for efficiency
results = process.extract(
    firm_conm_clean,
    candidate_assignees.keys(),
    limit=5,
    scorer=fuzz.ratio
)

# Apply validation boosts
final_confidence = min(0.99, base_conf + busdesc_boost + location_boost)
```

### Publication Matching (Current)

**Fuzzy Matching Attempt:**
- ✅ Conservative fuzzy matching implemented
- ✅ Jaro-Winkler ≥0.90 threshold (higher than patents)
- ✅ Multiple validation checks (country, business, location, URL)
- ⚠️ Only found 11 matches (very low coverage)

**Why It Underperformed:**
- Publication institutions have different naming patterns than patent assignees
- Higher threshold (0.90 vs 0.85) too conservative for publications
- Institution names often include location qualifiers that reduce similarity

---

## 📝 Stage 3: Manual Mappings

### Patent Matching (Stage 3)

**Target:** Near 100% accuracy, +500-1,000 firms

**Manual Mapping Categories:**

1. **Name Changes Over Time**
   - Google → Alphabet (GOOGL, GOOG)
   - Facebook → Meta (META)
   - Uses both old and new names

2. **Subsidiaries & Research Labs**
   - Microsoft Research → MSFT
   - Amazon Web Services → AMZN
   - ATT Intellectual Property → T

3. **Complex Corporate Structures**
   - AT&T and multiple subsidiaries
   - ATT Mobility, ATT Services, etc.
   - All map to parent company ticker

4. **Abbreviated Names**
   - Hewlett Packard → HPQ
   - HewlettPackard Development LP → HPQ

**Implementation:**
```python
MANUAL_MAPPINGS = {
    'GOOGLE': ['GOOGL', 'GOOG'],
    'ALPHABET': ['GOOGL', 'GOOG'],
    'ATT': ['T'],
    'AMERICAN TELEPHONE TELEGRAPH': ['T'],
    'MICROSOFT RESEARCH': ['MSFT'],
    # ... 85+ mappings
}

# Loads from CSV file if exists, otherwise uses defaults
# Maps assignee names → ticker symbols
# Tickers then mapped to GVKEYs
```

### Publication Matching (Current)

**Manual Mappings:**
- ✅ Created manual mappings for 54 institutions
- ✅ Google, Microsoft, IBM, Samsung, Intel, etc.
- ✅ 100% accuracy (manually verified)
- ✅ Maps directly to GVKEY (not via ticker)

**Differences:**
- Publications map institution_id → GVKEY directly
- Patents map assignee_name → ticker → GVKEY
- Publications have 54 manual mappings vs 85+ for patents

---

## 🔑 Key Success Factors: Patent Matching

### 1. **Progressive 3-Stage Approach**

```
Stage 1 (Exact)     → High accuracy (95%+)   → 3,000-5,000 firms
Stage 2 (Fuzzy)     → Medium accuracy (85-90%) → 2,000-4,000 firms
Stage 3 (Manual)    → Perfect accuracy (100%) → 500-1,000 firms
                    ─────────────────────────────────────────
Combined          → 95.4% accuracy           → 8,436 firms (45.1%)
```

**Key Principle:** Each stage targets unmatched entities from previous stages, avoiding redundant work and maintaining quality.

### 2. **Lookup Dictionary Optimization**

```python
# Patent matching uses O(1) lookups
assignee_lookup = {
    clean_name: [assignee_rows]
}

# Exact match: O(1)
# Contained name: O(n) but filtered to candidates
# Fuzzy: Uses rapidfuzz.process.extract() for efficiency
```

### 3. **Confidence Calibration**

| Strategy | Confidence | Justification |
|----------|-----------|---------------|
| Exact match | 0.98 | Perfect name match |
| Ticker in name | 0.97 | Ticker is unique identifier |
| Name contained | 0.96 | Subsidiary relationship |
| Abbreviation | 0.95 | Known or generated abbreviation |
| Fuzzy high (≥0.90) | 0.94 | Very similar names + validation |
| Fuzzy medium (≥0.85) | 0.90 | Similar names + validation |
| Manual mapping | 0.99 | Human verified |

**Principle:** Confidence scores reflect actual accuracy (validated through sampling).

### 4. **Validation & Quality Control**

**Stage 1 Validation:**
- Exact match: No validation needed (perfect match)
- Ticker match: Ticker is unique by definition
- Contained name: Length requirements (≥5, ≥8 for common words)
- Abbreviation: Known dictionary or generated rules

**Stage 2 Validation:**
- Fuzzy score ≥0.85 minimum
- Business description keywords boost confidence
- Location validation (placeholder)
- Only processes unmatched entities

**Stage 3 Validation:**
- Manual verification by researcher
- 100% accuracy assumed

---

## 📈 Performance Comparison

### Coverage by Stage

| Stage | Patent Firms | Patent % | Publication Firms | Publication % |
|-------|-------------|----------|------------------|---------------|
| **Stage 1** | ~5,000 | 26.7% | 1,562 | 8.4% |
| **Stage 2** | ~3,000 | 16.0% | 0 | 0% |
| **Stage 3** | ~500 | 2.7% | 389 | 2.1% |
| **Total** | **8,436** | **45.1%** | **1,951** | **10.4%** |

### Accuracy by Stage

| Stage | Patent Accuracy | Publication Accuracy |
|-------|----------------|---------------------|
| **Stage 1** | ~100% | 97.6% |
| **Stage 2** | 96.7% (high conf) | N/A (only 11 matches) |
| **Stage 3** | 100% | 100% (manual) |
| **Overall** | **95.4%** | **91.8%** |

---

## 💡 Lessons for Publication Matching

### ✅ What Worked Well (Applied to Publications)

1. **Manual Top Company Mappings**
   - ✅ Added Google, Microsoft, IBM (52,618 papers)
   - ✅ 100% accuracy
   - **Lesson:** Manual mappings for high-value targets are essential

2. **Contained Name Matching**
   - ✅ Implemented with validation
   - ✅ 970 firms @ 75% accuracy
   - **Lesson:** Useful but requires strict validation

3. **Confidence Scoring**
   - ✅ Different methods have different confidence scores
   - ✅ Weighted accuracy calculation
   - **Lesson:** Confidence calibration improves interpretability

### ❌ What's Missing (Failed or Not Implemented)

1. **Ticker-Based Matching**
   - ❌ Wikidata tickers field empty (data unavailable)
   - **Impact:** Cannot match "Google" → "GOOGL" automatically
   - **Workaround:** Manual mappings required

2. **Abbreviation Dictionary**
   - ❌ No systematic abbreviation matching
   - **Impact:** Missed "IBM", "AT&T", "GE" patterns
   - **Workaround:** Manual mappings

3. **Effective Fuzzy Matching**
   - ❌ Conservative threshold (0.90) too high for publications
   - ❌ Only 11 matches found
   - **Lesson:** Publication naming patterns differ from patents

4. **Subsidiary Recognition**
   - ⚠️ Only 48 matches, many false positives ("IQ Solutions" problem)
   - **Impact:** Minimal contribution
   - **Issue:** Too conservative + false positives from generic words

### 🔧 Recommended Improvements

#### Priority 1: Lower Fuzzy Threshold for Publications

**Current:** Jaro-Winkler ≥0.90 → 11 matches
**Proposed:** Jaro-Winkler ≥0.80 → target 500-1,000 matches

**Rationale:**
- Institution names include location qualifiers: "Google (United States)" vs "Alphabet Inc"
- Lower threshold with strict validation better than high threshold with no matches

#### Priority 2: Enhanced Abbreviation Matching

**Add publication-specific abbreviations:**
```python
ABBREVIATION_DICT = {
    'IBM': 'INTERNATIONAL BUSINESS MACHINES',
    'AT&T': 'AMERICAN TELEPHONE TELEGRAPH',
    # ... add more
}

# Match institution alternative_names to abbreviations
# Example: "IBM Research" → IBM → INTERNATIONAL BUSINESS MACHINES
```

#### Priority 3: Alternative Names with Validation

**Current Problem:** Alternative name matching had 89.7% error rate
**Proposed Solution:**
```python
# STRICT VALIDATION for alternative names
if alternative_name_match:
    if country_match AND business_keywords_match:
        confidence = 0.94  # Accept
    else:
        confidence = 0.90  # Accept if fuzzy score also high
    else:
        reject  # Require validation
```

#### Priority 4: Improve Subsidiary Recognition

**Current Problem:** "IQ Solutions" matches to any firm with "IQ" in name
**Proposed Solution:**
```python
# Blacklist generic words
GENERIC_WORDS = {'SOLUTIONS', 'SYSTEMS', 'TECHNOLOGIES', 'SERVICES'}

# Require minimum firm name length
if len(firm_name_clean) < 8:
    continue  # Skip generic short names

# Require additional validation
if not (country_match or business_match):
    continue  # Skip if no validation signals
```

---

## 📋 Next Steps for Publication Matching

### Option A: Refine Current Approach (Incremental)

1. **Lower fuzzy threshold** (0.90 → 0.80)
   - Expected: +500-1,000 firms @ 85-90% accuracy
   - Implementation: Edit `match_publications_fuzzy_conservative.py`

2. **Add abbreviation dictionary**
   - Expected: +200-500 firms @ 95% accuracy
   - Implementation: Create ABBREVIATION_DICT for publications

3. **Improve subsidiary recognition**
   - Expected: +100-300 firms @ 90% accuracy
   - Implementation: Add generic word blacklist, stricter validation

**Expected Result:** 2,700-2,800 firms @ 90-92% accuracy (13-15% coverage)

### Option B: Adopt Patent Matching Approach (Aggressive)

1. **Reimplement as true 3-stage pipeline**
   - Stage 1: Exact + contained name + homepage + abbreviations
   - Stage 2: Fuzzy matching (threshold 0.80) + validation
   - Stage 3: Manual mappings for top 100 firms

2. **Add ticker-based matching**
   - Source: Manual ticker table or alternative data source
   - Expected: +1,000-2,000 firms @ 99% accuracy

3. **Comprehensive manual mappings**
   - Expand from 54 to 200+ manual mappings
   - Focus on top firms by publication count

**Expected Result:** 4,000-5,000 firms @ 93-95% accuracy (21-27% coverage)

---

## 🎯 Recommendations

### Short-Term (This Week)

1. **Lower fuzzy threshold** to 0.80
2. **Add 50 manual mappings** for top unmatched institutions
3. **Validate results** on 500-sample

**Target:** 2,500 firms @ 90% accuracy

### Medium-Term (Next 2 Weeks)

1. **Build abbreviation dictionary** for publications
2. **Improve subsidiary recognition** with generic word blacklist
3. **Add alternative name validation** (country + business required)

**Target:** 3,500 firms @ 92% accuracy

### Long-Term (Next Month)

1. **Source ticker data** for institutions (alternative to Wikidata)
2. **Expand manual mappings** to 200+ institutions
3. **Implement full 3-stage pipeline** like patent matching

**Target:** 5,000+ firms @ 94% accuracy

---

## 📚 Key References

### Patent Matching Files
- `src/02_linking/match_patents_to_firms_stage1.py` - Exact matching
- `src/02_linking/match_patents_to_firms_stage2.py` - Fuzzy matching
- `src/02_linking/match_patents_to_firms_stage3.py` - Manual mappings

### Publication Matching Files
- `src/02_linking/explore_baseline_matches.py` - Baseline exploration
- `src/02_linking/match_publications_contained_name.py` - Contained name
- `src/02_linking/match_manual_top_companies.py` - Manual mappings
- `src/02_linking/match_publications_fuzzy_conservative.py` - Fuzzy (conservative)
- `src/02_linking/combine_improved.py` - Combination script

### Validation Results
- `docs/BASELINE_EXPLORATION_ANALYSIS.md` - Baseline analysis
- `docs/IMPROVED_MATCHING_RESULTS.md` - Final results
- `docs/PUBLICATION_MATCHING_VALIDATION_REPORT_FINAL.md` - Validation details

---

## 🔬 Technical Insights

### Why Patent Matching Worked Better

1. **Patent assignee names are cleaner**
   - Fewer location qualifiers
   - More consistent naming conventions
   - Better standardized data

2. **Patent assignees include tickers**
   - "Microsoft (MSFT) Research" pattern common
   - Unique identifier for matching
   - Publications lack this in institution names

3. **Patent matching had ticker data**
   - Firm tickers available for all Compustat firms
   - Could match assignee tickers to firm tickers
   - Publications: Wikidata tickers field empty

4. **Subsidiary recognition works for patents**
   - Patent assignees often: "Microsoft Research", "IBM Research"
   - Clear parent-subsidiary relationships
   - Publications: Same patterns exist but harder to validate

### Why Publication Matching Is Harder

1. **Institution names include locations**
   - "Google (United States)" vs "Alphabet Inc"
   - "Microsoft (United Kingdom)" vs "Microsoft Corp"
   - Reduces string similarity scores

2. **No ticker in institution names**
   - Institutions rarely include "(GOOGL)" in names
   - Cannot use ticker-based matching
   - Must rely on name similarity or manual mappings

3. **Diverse institution types**
   - Universities, government labs, corporate research
   - Different naming conventions
   - Harder to standardize

4. **Data quality issues**
   - Wikidata tickers field empty
   - Inconsistent location data
   - Variable name formatting

---

**Generated:** 2026-02-15
**Status:** Lessons learned, improvements recommended
**Next:** Implement Priority 1 improvements (lower fuzzy threshold, more manual mappings)
