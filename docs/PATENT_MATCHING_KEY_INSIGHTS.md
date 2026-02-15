# Patent Matching Key Insights for Publication Matching

## Executive Summary

After analyzing the patent matching methodology, I've identified **3 critical success factors** that enabled patent matching to achieve **45.1% firm coverage with 95.4% accuracy** (vs. our current 8.4% coverage with 98.7% accuracy for publications).

## The Three Key Success Factors

### 1. **Multiple High-Confidence Methods in Stage 1** (35,203 matches, 100.0% accuracy)

Patent matching didn't rely on just one method. They used **4 different high-accuracy methods**:

**a) Exact Name Match** (0.98 confidence)
- Standardized firm name = standardized assignee name
- Simple but highly effective

**b) Ticker Symbol Match** (0.97 confidence) ⭐ **MOST IMPORTANT**
- Match assignee names containing "(TICKER)" to firm tickers
- Example: "International Business Machines (IBM)" → IBM ticker
- This alone contributed significantly to coverage
- **Why it works:** Tickers are unique identifiers assigned by stock exchanges

**c) Contained Name Match** (0.96 confidence)
- Firm name appears as substring in assignee name
- Indicates subsidiary relationship
- Example: "Google DeepMind" contains "Google" → ALPHABET INC
- **Why it works:** Large tech companies have many subsidiaries with parent name in subsidiary name

**d) Abbreviation Match** (0.95 confidence)
- Dictionary of known firm abbreviations
- Example: "IBM" → INTERNATIONAL BUSINESS MACHINES
- **Why it works:** Many firms are known by abbreviations

**Key insight:** Each method captures different firms. Using 4 methods instead of 1 multiplies coverage.

### 2. **Conservative Fuzzy Matching** (4,331 matches, but only 3,224 with good accuracy)

Patent matching used fuzzy matching, but with a critical twist:

**Stage 2: Fuzzy Matching Overall**
- Total matches: 4,331
- Overall accuracy: 57.0% (TERRIBLE!)

**BUT High-Confidence Subset (≥0.95):**
- Matches: 3,224 (74.4% of Stage 2)
- Accuracy: **96.7%** (EXCELLENT!)

**Key insight:** **Don't use all fuzzy matches!** Only keep high-confidence ones (≥0.95).

**What they did:**
- Used Jaro-Winkler similarity ≥ 0.85
- BUT only kept matches with confidence ≥ 0.95 for final dataset
- Low-confidence matches (0.90-0.95) had 63.0% accuracy (still bad)

**What we should do:**
- Use even higher threshold: ≥ 0.93 (more conservative)
- Add cross-validation: country match, business description keywords, domain similarity
- Only keep matches with confidence ≥ 0.94

### 3. **Confidence Scoring & Stratification**

Every match gets a confidence score based on method:
- 0.98: Exact name match
- 0.97: Ticker match
- 0.96: Contained name match
- 0.95: Abbreviation match
- 0.90-0.94: Fuzzy match (based on similarity)

**Key insight:** Confidence scores enable:
1. Transparent reporting (accuracy by confidence level)
2. Filtering (exclude low-confidence matches)
3. Flexibility (users can choose confidence threshold)

**Validation by confidence:**
- ≥ 0.98: 100.0% accuracy
- 0.95-0.97: 96.7% accuracy
- 0.90-0.94: 63.0% accuracy
- **Conclusion:** Keep only ≥ 0.95 matches

## What We're Doing Wrong (Current Publication Matching)

### Problem 1: Only One Method

**Current:** Homepage domain exact matching only (2,841 matches)

**What we're missing:**
- Ticker matching (we have ticker data in Wikidata!)
- ROR ID matching (we have ROR IDs!)
- CIK code matching (we have CIK codes!)
- Contained name matching (with proper validation)
- Acronym matching (with proper filtering)

**Impact:** We're getting 1,580 firms (8.4%) when we could get 5,500-6,500 firms (29-35%)

### Problem 2: Rejected Alternative Name Matching Too Broadly

**What happened:** Alternative name matching had 89.7% error rate

**Why it failed:**
- Generic acronym collisions (CP → 5 different firms)
- No validation signals (country, domain, business description)
- Matched anything in Compustat's alternative name field

**What we should do instead:**
- Implement contained name matching WITH validation signals
- Check country match
- Check domain similarity
- Check business description keywords
- Use confidence scoring (0.94-0.96 based on validation strength)

**Patent matching's contained name method:** 0.96 confidence, part of Stage 1 (100.0% accuracy)

**The difference:** Patent matching had better validation signals (state, location, business description)

### Problem 3: No Fuzzy Matching

**Current:** No fuzzy matching at all

**Patent matching approach:**
- Fuzzy matching contributed +3,224 matches (96.7% accuracy for high-confidence)
- Used Jaro-Winkler similarity ≥ 0.85
- BUT only kept ≥ 0.95 confidence matches

**What we should do:**
- Implement fuzzy matching with higher threshold (≥ 0.93)
- Add cross-validation (country, domain, business)
- Only keep ≥ 0.94 confidence matches
- Expected: +1,000-1,500 matches with 95-97% accuracy

## What We Have That Patent Matching Didn't

### 1. **Wikidata Structured Data** (2,500 institutions)

**Goldmine of high-quality data:**
- `tickers` - Stock tickers (like patent's ticker matching!)
- `cik` - CIK codes (SEC identifiers)
- `parent_company_id` - For parent matching
- `exchange`, `isin`, etc.

**This is huge:** We can implement ticker matching exactly like patent matching did!

**Estimated coverage:** +1,000-1,200 firms from ticker matching alone

### 2. **ROR IDs** (disambiguated organization identifiers)

**High-quality identifiers:**
- ROR provides disambiguated organization entities
- Similar to patent's assignee IDs
- Can match to firms if we enrich Compustat with ROR IDs

**Estimated coverage:** +500-800 firms from ROR ID matching

### 3. **Homepage Domains** (already using)

**Current method:** 2,841 matches, 98.7% accuracy

**This is excellent!** Keep it as the baseline (highest confidence: 0.98)

## Proposed Multi-Stage Strategy (Learning from Patents)

### Stage 1: High-Confidence Exact Matches

**Goal:** 7,841 matches (5,500 firms), 98-99% accuracy

1. **Homepage domain exact** (current): 2,841 matches, 0.98 confidence
2. **Wikidata ticker match** ⭐: +1,000 matches, 0.97 confidence
3. **ROR ID match**: +700 matches, 0.97 confidence
4. **CIK code match**: +800 matches, 0.97 confidence
5. **Contained name match** (enhanced): +2,000 matches, 0.94-0.96 confidence
6. **Acronym-to-ticker match** (enhanced): +500 matches, 0.93-0.95 confidence

**Stage 1 total:** ~7,841 matches (5,500 firms), 98-99% accuracy

### Stage 2: High-Confidence Fuzzy Matches

**Goal:** +1,600 matches, 95-97% accuracy

1. **Jaro-Winkler ≥ 0.93** (not 0.85!): +1,200 matches, 0.94-0.95 confidence
2. **Name variant matching**: +400 matches, 0.94 confidence

**Stage 2 total:** +1,600 matches, 95-97% accuracy

### Stage 3: Parent Company Matching

**Goal:** +1,200 matches, 94-96% accuracy

1. **Parent cascade**: +700 matches, 0.94-0.95 confidence
2. **Subsidiary detection**: +500 matches, 0.94 confidence

**Stage 3 total:** +1,200 matches, 94-96% accuracy

### Grand Total

**Expected results:**
- **Total matches:** ~10,641 (6,500 firms)
- **Firm coverage:** 29-35% of CRSP (vs. 8.4% current)
- **Overall accuracy:** 96-97% (vs. 98.7% current, but vs. 95.4% for patents)
- **Improvement:** 4.1-5.9x more firms while maintaining high accuracy

## Implementation Priority

### Phase 1: Quick Wins (Highest ROI) ⭐

**Implement these 3 methods first:**

1. **Wikidata Ticker Matching** ⭐ **HIGHEST PRIORITY**
   - We already have ticker data for 2,500 institutions
   - Simple exact match to Compustat tickers
   - Expected: +1,000 firms, 97-99% accuracy
   - Time: 2-3 hours

2. **CIK Code Matching**
   - We have CIK data in Wikidata
   - Need CIK-to-GVKEY mapping (in Compustat)
   - Expected: +800 firms, 97-99% accuracy
   - Time: 3-4 hours

3. **Enhanced Contained Name Matching**
   - More sophisticated than failed "alternative name" method
   - Use multiple validation signals
   - Expected: +2,000 firms, 94-96% accuracy
   - Time: 4-6 hours

**Phase 1 Expected:**
- +3,800 firms
- Total: 5,400 firms (3.4x current)
- Accuracy: ~97%

### Phase 2: Medium-Term (If more coverage needed)

4. ROR ID matching: +700 firms, 97-99% accuracy
5. High-confidence fuzzy matching: +1,200 firms, 95-97% accuracy
6. Enhanced acronym matching: +500 firms, 93-95% accuracy

**Phase 2 Expected:**
- +2,400 additional firms
- Total: 7,800 firms (4.9x current)
- Accuracy: ~96%

### Phase 3: Advanced (If needed)

7. Parent company matching: +1,200 firms, 94-96% accuracy

**Phase 3 Expected:**
- +1,200 additional firms
- Total: 9,000 firms (5.7x current)
- Accuracy: ~95-96%

## Validation Strategy (Learning from Patents)

**What patent matching did:**
- Stratified sample of 1,000 matches by stage and confidence
- Manual validation by research assistant
- Reported accuracy by method and confidence level

**What we should do:**

1. **Validate each method separately:**
   - Sample 500 matches per new method
   - Calculate accuracy for each method
   - Exclude methods with accuracy < 93%

2. **Validate overall dataset:**
   - Stratified sample of 2,000 matches
   - Calculate overall accuracy
   - Target: ≥95% (patent matching standard)

3. **Report by confidence level:**
   - ≥ 0.97: Expected 97-99% accuracy
   - 0.95-0.96: Expected 95-97% accuracy
   - 0.94: Expected 93-95% accuracy
   - Exclude < 0.94

4. **Compare with patent matching:**
   - Our accuracy: 96-97% (expected) vs. 95.4% (patents)
   - Our coverage: 29-35% vs. 45.1% (patents)
   - Trade-off: Higher accuracy, lower coverage (acceptable)

## Key Takeaways

1. **Use multiple high-confidence methods** (not just one)
2. **Ticker matching is huge** (we have the data!)
3. **Conservative fuzzy matching** (high threshold + cross-validation)
4. **Confidence scoring** (enables transparency and filtering)
5. **Validate extensively** (500-1,000 samples per method)
6. **Exclude low-accuracy methods** (drop methods with < 93% accuracy)
7. **Prioritize high-confidence methods** (ticker, CIK, ROR ID first)

## Success Criteria

**Minimum acceptable:**
- Coverage: ≥4,000 firms (2.5x current, 21% of CRSP)
- Accuracy: ≥95% overall (patent matching standard)

**Target:**
- Coverage: ≥5,500 firms (3.5x current, 29% of CRSP)
- Accuracy: ≥96% overall (exceeds patent matching)

**Stretch goal:**
- Coverage: ≥6,500 firms (4.1x current, 35% of CRSP)
- Accuracy: ≥96% overall

---

**Bottom Line:** We can achieve 3.5-5.9x more coverage (5,500-9,000 firms vs. 1,580 current) while maintaining 96-97% accuracy by learning from patent matching's multi-stage approach with multiple high-confidence methods.

**Next Step:** Implement Phase 1 methods (Wikidata ticker, CIK code, enhanced contained name matching) and validate each method with 500 samples.

---

**Created:** February 15, 2026
**Status:** Ready for implementation
**Priority:** HIGH - Current 8.4% coverage is unacceptably low
