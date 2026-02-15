# Publication-Firm Matching Plan - Targets: >95% Accuracy, >2,000 Firms

**Date:** 2026-02-15
**Current Status:** 689 institutions (0.60%), 100% accuracy
**Target:** >2,000 firms, >95% accuracy

---

## Analysis: Why Patent Matching Succeeded

### Patent Matching Results (Stage 1 Only):
- **6,786 unique firms** matched (36% of 18,709)
- **35,203 assignee-firm pairs**
- **High accuracy** (validated in separate process)

### Key Success Factors:
1. **Clean assignee names** - Patent assignees are official company names
2. **Rich assignee data** - IDs, patent counts, years
3. **Exact match priority** - Exact matches first, then fuzzy
4. **Ticker matching** - Assignees often include tickers
5. **Abbreviation dictionary** - Known mappings (IBM, AT&T, etc.)
6. **Firm contained** - "Microsoft Research" contains "Microsoft"

---

## Publication Matching Challenges

### Current Issues:
1. **Generic institutions** - Academic, government, non-profit
2. **Institution names** - Less standardized than assignee names
3. **No tickers in names** - Rarely include ticker symbols
4. **Subsidiaries** - "University X" not clearly linked to firm

### Why Current Results Are Low:
- 0.60% coverage = 689/115,138 institutions
- Only corporate institutions (28.6% of total) = 32,930
- 689/32,930 = 2.1% of corporate institutions matched
- **Missed:** Most companies don't have Wikipedia URLs or domains

---

## Comprehensive Plan to Achieve Targets

### **OVERALL STRATEGY: Three-Stage Pipeline with Multiple Data Sources**

---

## Stage 1: Exact & High-Confidence Matching (Target: 1,200-1,500 firms)

### 1.1 Enhanced Exact Matching (Learn from Patent Success)

**Input:** Institutions with cleaned names
**Method:** Exact string matching (case-insensitive)
**Confidence:** 0.98
**Expected:** 800-1,000 firms

```python
# Strategies:
- exact_conm: Institution name == firm name
- exact_conml: Institution name == firm legal name
- alternative_name_conm: Inst alt name == firm name
- alternative_name_conml: Inst alt name == firm legal name
```

### 1.2 Ticker Matching (CRITICAL ADDITION)

**Input:** Institutions with acronyms + Firm tickers
**Method:** Direct acronym-to-ticker matching
**Confidence:** 0.97
**Expected:** 200-300 firms

**Key Innovation:** Check institution acronyms against firm tickers
```python
# From classify_institution_types.py:
institutions have 'acronyms' field

# Match logic:
if institution_acronym in firm_tickers:
    match = True
    confidence = 0.97
```

**Example:**
- Institution: "NASA" (acronym)
- Firm: "Northrop Grumman" (ticker: NOC) ❌
- But better: "IBM" (acronym) → IBM ✓

### 1.3 Homepage Domain Matching (Already Working)

**Input:** Institutions with homepages + Firm weburl
**Method:** Exact domain match
**Confidence:** 0.98
**Expected:** 180-220 firms

**Current Results:** 180 matches, 100% accurate

### 1.4 ROR ID Matching (NEW - HIGH VALUE)

**Input:** Institutions with ROR IDs
**Method:** Query ROR database for organization metadata
**Confidence:** 0.96-0.98
**Expected:** 100-150 firms

**ROR Database:** ror.org provides:
- Organization type (company vs university)
- Parent organizations
- Links to firms (GVKEY, permno)
- Alternative names

**Implementation:**
```python
# Use ROR API
import requests

inst_ror_id = institution.get('ror_id')
response = requests.get(f'https://api.ror.org/organizations/{inst_ror_id}')
ror_data = response.json()

# Check if ROR has firm links
if ror_data.get('external_ids'):
    gvkey = ror_data['external_ids'].get('GVKEY')
    if gvkey:
        match = True
        confidence = 0.97
```

**Stage 1 Expected Total:** 1,280-1,670 firms

---

## Stage 2: Smart Fuzzy Matching (Target: +400-600 firms)

### 2.1 Business Description Validation (From Patent Success)

**Input:** Firm business descriptions (SIC descriptions)
**Method:** Fuzzy match + keyword validation
**Confidence:** 0.90-0.94
**Expected:** 200-300 firms

**Logic:**
```python
# Fuzzy match institution name to firm name
similarity = fuzz.ratio(inst_name, firm_name) / 100

# If similarity >= 0.85, boost with business description
if similarity >= 0.85:
    # Check if inst words in firm business description
    inst_words = extract_keywords(inst_name)
    busdesc = firm.get('busdesc', '').upper()

    matches = sum(1 for w in inst_words if w in busdesc)

    if matches >= 2:
        confidence = 0.92  # Boost
    else:
        confidence = 0.90
```

**Example:**
- Institution: "BioTech Research Center"
- Firm: "AMGEN INC" (busdesc: "biotechnology...")
- Similarity: 0.86, "biotech" in busdesc ✓
- Confidence: 0.92

### 2.2 Location-Aware Matching (New)

**Input:** Institution locations (if available)
**Method:** Fuzzy match + location validation
**Confidence:** 0.90-0.93
**Expected:** 100-200 firms

**Logic:**
```python
# Extract location from institution name
# "Microsoft Research Asia" → Asia
# "Novartis Switzerland" → Switzerland

# Match to firm headquarters
if inst_location in [firm.state, firm.country]:
    boost = 0.03
```

**Example:**
- Institution: "Siemens Germany"
- Firm: "SIEMENS AG" (country: Germany)
- Confidence boost: +0.03

### 2.3 Parent Institution Name Matching

**Input:** Institutions with parent_institution_ids
**Method:** Match parent names to firms
**Confidence:** 0.91-0.95
**Expected:** 50-100 firms

**Logic:**
```python
# From extract_and_enrich_institutions.py:
parent_ids = institution.get('parent_institution_ids', [])

# Match parent institution names to firms
for parent_id in parent_ids:
    parent_inst = institutions_dict.get(parent_id)
    parent_name = parent_inst.get('display_name')

    # Match parent name to firm
    if is_match(parent_name, firm_name):
        # Child institution gets same match
        confidence = 0.93
```

**Example:**
- Institution: "Google DeepMind" (parent: Alphabet)
- Match parent "Alphabet" → Alphabet Inc ✓
- Child "Google DeepMind" → Alphabet Inc ✓

**Stage 2 Expected Total:** +350-600 firms (additional)

---

## Stage 3: Manual & Semi-Automated (Target: +200-300 firms)

### 3.1 Top-100 Firms Manual Curation

**Input:** Top 100 institutions by paper count (unmatched)
**Method:** Manual lookup and verification
**Confidence:** 0.98-1.00 (manual)
**Expected:** 80-100 firms

**Process:**
1. Sort unmatched institutions by paper count
2. Take top 100
3. Manually verify firm affiliation
4. Create manual mappings file

**Example Manual Mappings:**
```python
MANUAL_MAPPINGS = {
    # Institution ID → GVKEY
    "https://openalex.org/I4210139498": "027845",  # Allergan
    "https://openalex.org/I4210118662": "006008",  # Nvidia
    # ... add more
}
```

### 3.2 Company Website Scraping (Targeted)

**Input:** Institutions with Wikipedia URLs
**Method:** Scrape Wikipedia infobox for company links
**Confidence:** 0.95-0.98
**Expected:** 100-150 firms

**Logic:**
```python
# For institutions with Wikipedia URLs
wiki_url = institution['wikipedia_url']

# Parse Wikipedia page for infobox
# Extract: website, ticker, parent_company

# If found, match to firms
if ticker in firm_tickers:
    match = True
    confidence = 0.97
```

**Note:** Only scrape, don't use Wikipedia API (blocked)

### 3.3 Subsidiary Pattern Matching

**Input:** Institution names with subsidiary patterns
**Method:** Identify and match subsidiaries
**Confidence:** 0.90-0.93
**Expected:** 50-80 firms

**Subsidiary Patterns:**
```python
SUBSIDIARY_PATTERNS = [
    r'(.+) Research',  # "Microsoft Research" → Microsoft
    r'(.+) Laboratories',  # "Bell Laboratories" → Bell
    r'(.+) Inc(.+)',  # Subsidiary Inc
    r'(.+) Corp(.+)',  # Subsidiary Corp
]

# Extract parent name and match to firm
```

**Example:**
- "IBM Research" → Extract "IBM" → IBM ✓
- "Google AI" → Extract "Google" → Alphabet ✓

**Stage 3 Expected Total:** +230-330 firms (additional)

---

## Combined Expected Results

| Stage | Firms | Accuracy | Papers |
|-------|-------|----------|--------|
| **Stage 1: Exact** | 1,400 | 98% | 200,000 |
| **Stage 2: Fuzzy** | +500 | 92% | 100,000 |
| **Stage 3: Manual** | +250 | 97% | 150,000 |
| **TOTAL** | **2,150** | **95%+** | **450,000** |

---

## Quality Assurance to Maintain >95% Accuracy

### 1. Confidence Thresholds
- **Keep only:** confidence ≥ 0.90 for Stage 2
- **Keep only:** confidence ≥ 0.95 for final output
- **Remove:** All acronym matching (currently 13% error rate)

### 2. Validation Strategy
- **Sample 200 matches** for manual validation
- **Target:** ≥190 correct (95%)
- **If below:** Raise thresholds and re-run

### 3. Multi-Firm Institution Removal
- Remove institutions matched to multiple firms
- These are ambiguous and reduce accuracy

### 4. Generic Term Filtering
- Remove institutions with "international", "group", etc.
- Unless validated by other signals (URL, ticker, etc.)

---

## Implementation Order (Priority)

### **Phase 1: Quick Wins (1-2 days)**
1. ✓ **Ticker matching** from acronyms (200-300 firms)
2. ✓ **Keep existing** homepage exact (180 firms)
3. ✓ **Add ROR ID matching** (100-150 firms)
4. ✓ **Parent institution cascade** (50-100 firms)

**Expected after Phase 1:** 1,030-1,230 firms

### **Phase 2: Smart Fuzzy (3-4 days)**
5. **Business description validation** (200-300 firms)
6. **Location-aware matching** (100-200 firms)

**Expected after Phase 2:** 1,330-1,730 firms

### **Phase 3: Targeted Manual (2-3 days)**
7. **Top-100 manual curation** (80-100 firms)
8. **Wikipedia scraping** (100-150 firms)
9. **Subsidiary patterns** (50-80 firms)

**Expected after Phase 3:** 1,560-2,060 firms

### **Phase 4: Validation & Refinement (1-2 days)**
10. **Validate 200 matches**
11. **Filter below 95% accuracy**
12. **Final quality check**

---

## Success Metrics

### Minimum Targets:
- ✅ **≥2,000 unique firms** matched
- ✅ **≥95% accuracy** (validated)
- ✅ **≥300,000 papers** covered
- ✅ **Reproducible methodology**

### Stretch Goals:
- **3,000+ firms** (if methods work well)
- **97%+ accuracy**
- **500,000+ papers**

---

## Risk Mitigation

### Risk 1: Low Coverage
**Mitigation:** If Phase 1+2 only reaches 1,500 firms, expand Phase 3 (manual)

### Risk 2: Accuracy Below 95%
**Mitigation:**
- Raise confidence threshold to 0.95
- Remove Stage 2 fuzzy matches
- Keep only Stage 1 exact + manual

### Risk 3: Wikipedia Scraping Blocked
**Mitigation:**
- Use already extracted Wikipedia URLs
- Parse HTML locally (no API calls)
- Fallback to manual curation

---

## File Structure

### New Scripts to Create:
```
src/02_linking/
├── match_publications_stage1_enhanced.py  # All Stage 1 methods
├── match_publications_stage2_fuzzy.py     # Stage 2 fuzzy + validation
├── match_publications_stage3_manual.py     # Stage 3 manual + scraping
├── validate_publication_final.py           # Final validation
└── combine_all_stages.py                  # Combine + deduplicate
```

### Data Files:
```
data/processed/linking/
├── publication_firm_matches_stage1.parquet
├── publication_firm_matches_stage2.parquet
├── publication_firm_matches_stage3.parquet
├── publication_firm_matches_final.parquet  # Combined, >2000 firms
└── validation_results.csv                 # Manual validation
```

---

## Timeline Estimate

- **Phase 1:** 1-2 days (Quick wins)
- **Phase 2:** 3-4 days (Fuzzy matching)
- **Phase 3:** 2-3 days (Manual + scraping)
- **Phase 4:** 1-2 days (Validation)

**Total:** 7-11 days to completion

---

## Conclusion

This plan achieves the targets by:
1. ✅ Learning from patent matching success (6,786 firms)
2. ✅ Adding multiple high-value data sources (tickers, ROR, parents)
3. ✅ Using smart fuzzy matching with validation
4. ✅ Targeted manual curation for high-impact institutions
5. ✅ Maintaining >95% accuracy through confidence thresholds

**Expected Results:** 2,150 firms, 95%+ accuracy, 450,000 papers

---

**Next Step:** Start Phase 1 implementation (ticker + ROR + parent cascade)
