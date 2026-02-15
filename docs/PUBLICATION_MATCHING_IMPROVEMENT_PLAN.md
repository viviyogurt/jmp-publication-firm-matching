# Publication Matching Improvement Plan
## Learning from Patent Matching Success

## Current Situation

**Publication Matching:**
- Coverage: 1,580 firms (8.4% of CRSP) - **TOO LOW**
- Accuracy: 98.7% (homepage domain exact only)
- Method: Single-stage (homepage domain exact matching)

**Patent Matching:**
- Coverage: 8,436 firms (45.1% of CRSP) - **5.3x MORE**
- Accuracy: 95.4% overall
- Method: Multi-stage with multiple high-accuracy strategies

## Patent Matching Success Factors

### Stage 1: Multiple High-Confidence Methods (35,203 matches, 100.0% accuracy)

1. **Exact name match** (confidence: 0.98)
   - Standardized firm name = standardized assignee name

2. **TICKER symbol match** (confidence: 0.97) ⭐ **KEY SUCCESS FACTOR**
   - Match assignee names containing "(TICKER)" to firm tickers
   - Example: "International Business Machines (IBM)" → IBM ticker
   - This alone contributed significantly to coverage

3. **Contained name match** (confidence: 0.96)
   - Firm name appears as substring in assignee name
   - Indicates subsidiary relationship
   - Example: "Google DeepMind" contains "Google" → ALPHABET INC

4. **Abbreviation match** (confidence: 0.95)
   - Dictionary of known firm abbreviations
   - Example: "IBM" → INTERNATIONAL BUSINESS MACHINES

### Stage 2: Fuzzy Matching (4,331 matches, overall 57.0% BUT...)

**Critical insight:** High-confidence subset (≥0.95) achieved **96.7% accuracy**!
- High confidence (≥0.95): 3,224 matches, 96.7% accuracy
- Medium confidence (0.90-0.95): 1,107 matches, 63.0% accuracy
- **Key learning:** Restrict fuzzy matching to high-confidence matches only

## What We Have for Publication Matching

### Data Assets

**Publication Institutions (16,278 firms):**
- `institution_id`, `display_name`, `normalized_name`
- `alternative_names`, `acronyms`, `name_variants`
- `ror_id`, `wikidata_id`
- `homepage_url`, `homepage_domain`
- `country_code`, geo fields
- `paper_count`

**Wikidata Structured (2,500 institutions):** ⭐ **GOLDMINE**
- `tickers` - Stock tickers from Wikidata
- `cik` - CIK codes (SEC identifiers)
- `parent_company_id` - For parent-subsidiary matching
- `exchange`, `isin`, etc.

**Compustat Firms (18,709 firms):**
- `GVKEY`, `tic` (ticker), `conm` (company name), `conml` (legal name)
- `weburl` (homepage URL)
- `name_variants`
- `state`, `city`, `busdesc` (business description)

## Proposed Multi-Stage Matching Strategy

### Stage 1: High-Confidence Exact Matches (Target: 3,500-4,000 firms, 100.0% accuracy)

**Method 1.1: Wikidata Ticker Matching** ⭐ **HIGHEST PRIORITY**
- Match institution tickers from Wikidata to Compustat tickers
- Confidence: 0.97 (same as patent matching)
- Estimated coverage: +800-1,200 firms
- Validation: Cross-check with name similarity, country match
- Example: "IBM (United States)" with ticker "IBM" → INTERNATIONAL BUSINESS MACHINES

**Method 1.2: ROR ID Matching**
- Match institution ROR IDs to firms (if we can get ROR IDs for Compustat)
- Confidence: 0.97
- Estimated coverage: +500-800 firms
- ROR provides disambiguated organization identifiers

**Method 1.3: CIK Code Matching**
- Match institution CIK codes (from Wikidata) to Compustat GVKEYs
- Need CIK-to-GVKEY mapping (available in Compustat)
- Confidence: 0.97
- Estimated coverage: +600-1,000 firms

**Method 1.4: Enhanced Homepage Domain Matching** (already implemented)
- Current method: 2,841 matches, 98.7% accuracy
- Confidence: 0.98
- Keep this as the baseline

**Method 1.5: Contained Name Matching (Enhanced)**
- Match where firm name appears in institution name
- BUT with multiple validation signals:
  - Country match
  - Homepage domain similarity
  - Business description keyword match
- Confidence: 0.94-0.96 (based on validation signals)
- Estimated coverage: +1,500-2,500 firms
- Example: "Google DeepMind" contains "Google" → ALPHABET INC (if country=US, domain related)

**Method 1.6: Acronym-to-Ticker Matching (Enhanced)**
- Extract acronyms from institution names
- Match to firm tickers
- BUT only for specific, unique acronyms (not generic like "CP", "CSI")
- Confidence: 0.93-0.95 (depends on acronym uniqueness)
- Estimated coverage: +400-600 firms
- Example: "IBM" acronym → IBM ticker (validated by name similarity)

### Stage 2: High-Confidence Fuzzy Matching (Target: +1,000-1,500 firms, ≥95.0% accuracy)

**Method 2.1: Jaro-Winkler Similarity ≥ 0.93** (not 0.85!)
- Require much higher similarity threshold than patent matching
- Confidence: 0.94-0.95 (based on similarity score)
- Cross-validation required:
  - Country match (country_code = firm country)
  - Business description keyword match
  - Homepage domain similarity (if available)
- Estimated coverage: +1,000-1,500 firms
- Example: "Intl Business Machines" ↔ "INTERNATIONAL BUSINESS MACHINES" (similarity: 0.95)

**Method 2.2: Name Variant Matching**
- Match institution alternative_names to firm name_variants
- Confidence: 0.94
- Only if name variants are highly similar (Jaro-Winkler ≥ 0.90)
- Estimated coverage: +300-500 firms

### Stage 3: Parent Company Matching (Target: +500-800 firms, 95.0%+ accuracy)

**Method 3.1: Parent Company Cascade**
- Use Wikidata parent_company_id
- Navigate parent hierarchy: Institution → Parent → Grandparent → Firm
- Confidence: 0.94-0.95
- Cross-validate with homepage domains
- Estimated coverage: +500-800 firms
- Example: YouTube → Google → ALPHABET INC

**Method 3.2: Subsidiary Detection**
- Detect subsidiary patterns in institution names
- Match to parent firms
- Example: "Microsoft Research" → MICROSOFT CORP
- Confidence: 0.94-0.95
- Estimated coverage: +400-600 firms

### Expected Results

**Stage 1 (High-Confidence Exact):**
- Homepage domain: 2,841 matches (current)
- Wikidata ticker: +1,000 matches
- ROR ID: +700 matches
- CIK code: +800 matches
- Contained name: +2,000 matches
- Acronym-to-ticker: +500 matches
- **Stage 1 Total: ~7,841 matches** (4.9x current coverage)
- **Expected accuracy: 98-99%** (all methods are high-confidence)

**Stage 2 (High-Confidence Fuzzy):**
- Jaro-Winkler ≥ 0.93: +1,200 matches
- Name variant matching: +400 matches
- **Stage 2 Total: +1,600 matches**
- **Expected accuracy: 95-97%** (high threshold + cross-validation)

**Stage 3 (Parent Matching):**
- Parent cascade: +700 matches
- Subsidiary detection: +500 matches
- **Stage 3 Total: +1,200 matches**
- **Expected accuracy: 94-96%** (validated against parent relationships)

**Grand Total: ~10,641 matches** (6.7x current coverage)
- Unique firms: ~5,500-6,500 (vs. 1,580 current)
- Overall accuracy: ~96-97% (weighted average)
- Firm coverage: ~29-35% of CRSP (vs. 8.4% current)

## Implementation Priority

### Phase 1: Quick Wins (Highest ROI)

1. **Wikidata Ticker Matching** ⭐ **HIGHEST PRIORITY**
   - We already have ticker data for 2,500 institutions
   - Simple exact match to Compustat tickers
   - Expected: +800-1,200 firms, 97-99% accuracy
   - Implementation time: 2-3 hours

2. **CIK Code Matching**
   - We have CIK data for institutions
   - Need CIK-to-GVKEY mapping (in Compustat)
   - Expected: +600-1,000 firms, 97-99% accuracy
   - Implementation time: 3-4 hours

3. **Enhanced Contained Name Matching**
   - More sophisticated than current "alternative name" method
   - Use multiple validation signals (country, domain, business description)
   - Expected: +1,500-2,500 firms, 94-96% accuracy
   - Implementation time: 4-6 hours

**Phase 1 Expected Results:**
- +3,000-4,700 additional firms
- Total: ~4,500-6,000 firms (2.8-3.8x current)
- Overall accuracy: ~97% (still excellent)

### Phase 2: Medium-Term Improvements

4. **ROR ID Matching**
   - May need to enrich Compustat with ROR IDs
   - Expected: +500-800 firms, 97-99% accuracy
   - Implementation time: 6-8 hours (includes data enrichment)

5. **High-Confidence Fuzzy Matching**
   - Jaro-Winkler ≥ 0.93 (not 0.85!)
   - Cross-validation with country, business, domain
   - Expected: +1,000-1,500 firms, 95-97% accuracy
   - Implementation time: 6-8 hours

6. **Acronym-to-Ticker Matching (Enhanced)**
   - Extract acronyms, filter out generic ones
   - Match to tickers, validate with name similarity
   - Expected: +400-600 firms, 93-95% accuracy
   - Implementation time: 4-5 hours

**Phase 2 Expected Results:**
- +2,000-3,300 additional firms
- Total: ~6,500-9,300 firms (4.1-5.9x current)
- Overall accuracy: ~96%

### Phase 3: Advanced Matching

7. **Parent Company Matching**
   - Use Wikidata parent_company_id
   - Build parent hierarchy
   - Expected: +700-1,200 firms, 94-96% accuracy
   - Implementation time: 8-10 hours

8. **Subsidiary Detection**
   - Pattern matching for subsidiary indicators
   - Validate against parent firms
   - Expected: +400-600 firms, 94-96% accuracy
   - Implementation time: 6-8 hours

**Phase 3 Expected Results:**
- +1,100-1,800 additional firms
- Total: ~7,600-11,100 firms (4.8-7.0x current)
- Overall accuracy: ~95-96%

## Validation Strategy

### Stage 1 Validation (All methods)

For each new method, validate 500 random matches:

1. **Wikidata Ticker Matching**
   - Sample: 500 matches
   - Expected accuracy: 97-99%
   - Manual validation by research assistant
   - If accuracy < 95%, adjust validation rules

2. **CIK Code Matching**
   - Sample: 500 matches
   - Expected accuracy: 97-99%
   - CIK codes are highly reliable identifiers

3. **Contained Name Matching**
   - Sample: 1,000 matches (more due to higher risk)
   - Expected accuracy: 94-96%
   - If accuracy < 93%, add more validation signals

### Overall Validation

After implementing all methods:

1. **Stratified sample** of 2,000 matches by method and confidence
2. **Calculate accuracy** for each method
3. **Exclude methods** with accuracy < 93% (similar to patent matching's approach)
4. **Report overall accuracy** weighted by match count
5. **Compare with patent matching** (95.4% accuracy target)

## Risk Mitigation

### Risk 1: Alternative Name Matching Failure (89.7% error rate)

**What happened:**
- Generic acronym collisions (CP → 5 different firms)
- Name fragment collisions
- No validation signals

**How we avoid this:**
1. **Multiple validation signals** (country, domain, business description)
2. **Stricter thresholds** (Jaro-Winkler ≥ 0.93, not 0.85)
3. **Confidence scoring** (only keep high-confidence matches)
4. **Extensive validation** (500-1,000 samples per method)
5. **Fallback to homepage exact** (keep current 2,841 matches as baseline)

### Risk 2: Lower Overall Accuracy

**Target:** Maintain ≥95% overall accuracy (patent matching standard)

**How we ensure this:**
1. **Confidence thresholds** - Only keep matches with confidence ≥0.94
2. **Method-by-method validation** - Test each method separately
3. **Exclude low-accuracy methods** - Drop methods with accuracy < 93%
4. **Prioritize high-confidence methods** - Wikidata ticker, CIK, ROR ID first
5. **Transparency** - Report accuracy by method, confidence level

### Risk 3: False Positives in Fuzzy Matching

**What patent matching did:**
- Stage 2 fuzzy: 57.0% accuracy overall
- BUT high-confidence (≥0.95): 96.7% accuracy

**How we avoid this:**
1. **Higher similarity threshold** - ≥0.93 (not ≥0.85)
2. **Multiple cross-validation signals** - Country, business, domain
3. **Only keep high-confidence** - Exclude 0.90-0.94 range
4. **Smaller Stage 2** - Prioritize Stage 1 exact matches

## Comparison with Patent Matching

| Aspect | Patent Matching | Publication Matching (Proposed) |
|--------|----------------|--------------------------------|
| **Stage 1 Methods** | 4 methods | 6 methods |
| **Stage 1 Matches** | 35,203 (6,786 firms) | ~7,841 (estimated, ~5,500 firms) |
| **Stage 1 Accuracy** | 100.0% | 98-99% (expected) |
| **Stage 2 Threshold** | Jaro-Winkler ≥ 0.85 | Jaro-Winkler ≥ 0.93 |
| **Stage 2 Accuracy** | 57.0% overall, 96.7% (≥0.95) | 95-97% (≥0.94, expected) |
| **Stage 3** | Manual mapping (1 match) | Parent matching (+1,200 matches) |
| **Total Matches** | 39,535 (8,436 firms) | ~10,641 (estimated, ~6,500 firms) |
| **Overall Accuracy** | 95.4% | 96-97% (expected) |
| **Firm Coverage** | 45.1% of CRSP | 29-35% of CRSP (expected) |

**Key Insight:** Our proposed publication matching will have:
- **Higher accuracy** (96-97% vs. 95.4%)
- **Lower but reasonable coverage** (29-35% vs. 45.1%)
- **More conservative fuzzy matching** (≥0.93 vs. ≥0.85 threshold)

## Next Steps

1. ✅ **Document plan** (this document)
2. **Implement Phase 1 methods:**
   - [ ] Wikidata ticker matching
   - [ ] CIK code matching
   - [ ] Enhanced contained name matching
3. **Validate Phase 1:**
   - [ ] Sample 500 matches per method
   - [ ] Calculate accuracy
   - [ ] Exclude methods with accuracy < 93%
4. **Report Phase 1 results:**
   - [ ] Update coverage statistics
   - [ ] Update LaTeX documentation
   - [ ] Compare with patent matching
5. **Implement Phase 2 if needed** (based on Phase 1 results)

## Success Criteria

**Minimum Acceptable Results:**
- **Coverage:** ≥4,000 firms (2.5x current, 21% of CRSP)
- **Accuracy:** ≥95% overall (meets patent matching standard)
- **Homepage exact baseline:** Keep all 2,841 current matches (98.7% accuracy)

**Target Results:**
- **Coverage:** ≥5,500 firms (3.5x current, 29% of CRSP)
- **Accuracy:** ≥96% overall (exceeds patent matching standard)
- **High-confidence subset:** ≥95% accuracy for confidence ≥0.95

**Stretch Goal:**
- **Coverage:** ≥6,500 firms (4.1x current, 35% of CRSP)
- **Accuracy:** ≥96% overall
- **Compare favorably with patent matching:** Higher accuracy, reasonable coverage

---

**Created:** February 15, 2026
**Status:** Ready for implementation
**Priority:** HIGH - Current publication coverage (8.4%) is unacceptably low compared to patents (45.1%)
