# Publication-Firm Matching: Final Optimized Plan

**Date:** 2026-02-15
**Learned from:** Patent matching (6,786 firms, 95.4% accuracy)
**Target:** >2,000 firms, >95% accuracy

---

## Key Learning from Patent Matching Success

### Patent Matching Results:
- **Stage 1 only:** 6,786 firms (36% of 18,709)
- **35,203 assignee-firm pairs**
- **Accuracy:** 95.4% overall, 100% for Stage 1

### Success Factors:
1. **Lookup dictionaries** - O(1) matching instead of O(N*M)
2. **Multiple strategies** - 8 different exact-match methods
3. **Ticker matching** - Assignees with "(TICKER)" in name
4. **Abbreviation dictionary** - Known mappings (IBM, AT&T, etc.)

---

## Optimized Plan: Achieve >2,000 Firms

### **OVERALL STRATEGY: Use Lookup Dictionaries + Available Metadata**

---

## Phase 1: Enhanced Stage 1 with Lookups (Target: 1,500-2,000 firms)

### Critical Optimization: Create Lookup Dictionaries

```python
# Build lookup dictionaries for O(1) matching
name_to_firms = {}  # normalized_name -> list of firm rows
acronym_to_firms = {}  # acronym -> list of firm rows
domain_to_firms = {}  # domain -> list of firm rows
alt_name_to_firms = {}  # alternative_name -> list of firm rows
```

### Strategy 1: Exact Name Match (Already Works, Keep)
- **Confidence:** 0.98
- **Expected:** 400-600 firms
- **Current issue:** Only exact matches, need to expand

### Strategy 2: Ticker Matching from Acronyms (NEW - CRITICAL)
**Input:** Institutions have `acronyms` field
**Method:** Match institution acronyms to firm tickers
**Confidence:** 0.97
**Expected:** 300-500 firms ⭐

**Implementation:**
```python
# Build ticker lookup
for firm_row in firms_df.iter_rows(named=True):
    ticker = firm_row.get('tic')
    if ticker:
        ticker_to_firms[ticker.upper()] = firm_row

# Match institutions
for inst_row in institutions_df.iter_rows(named=True):
    for acronym in inst_row['acronyms']:
        if acronym in ticker_to_firms:
            match = True
```

**Why This Will Work:**
- Many companies use ticker as acronym: "IBM", "NVDA"
- Already present in OpenAlex data
- No API calls needed
- **This is the highest-value addition**

### Strategy 3: Homepage Domain Exact (Already Works)
- **Confidence:** 0.98
- **Expected:** 180-220 firms
- **100% accurate** (validated)

### Strategy 4: Alternative Names (Already Partially Works)
- **Confidence:** 0.95
- **Expected:** 200-300 firms
- **Method:** Match institution `display_name` to firm `alternative_names`

### Strategy 5: Abbreviation Dictionary (from Patent Success)
- **Confidence:** 0.95
- **Expected:** 50-100 firms
- **Method:** Use known mappings (IBM→INTL BUS MACHINES, etc.)

### Strategy 6: Firm Name Contained (Careful Application)
- **Confidence:** 0.94 (lowered from 0.95)
- **Expected:** 200-300 firms
- **Filter:** Minimum 8 characters, not generic words
- **Exclude:** "international", "group", "innovations", etc.

**Phase 1 Expected Total:** 1,330-2,020 firms ✓

---

## Phase 2: Smart Fuzzy with Business Description (Target: +300-500 firms)

### Key Innovation from Patent Matching:

**Business Description Boost** (No API needed)
```python
# From firm data: busdesc field
firm_busdesc = firm_row.get('busdesc', '')

# Extract keywords from institution name
inst_keywords = extract_keywords(inst_name)

# Check if keywords in business description
matches = sum(1 for kw in inst_keywords if kw in firm_busdesc.upper())

if matches >= 2:
    confidence += 0.03  # Boost
```

**Fuzzy Matching + Validation:**
- Jaro-Winkler similarity ≥ 0.85
- Business description keyword match
- Confidence: 0.90-0.94
- **Expected:** 300-500 additional firms

---

## Phase 3: Parent & Subsidiary (Target: +100-200 firms)

### Parent Institution Matching:
- Use `parent_institution_ids` from institutions
- Match parent names to firms
- Cascade to children
- **Expected:** 100-200 firms

---

## Optimized Implementation (Fixes Timeout Issue)

```python
# WRONG: O(N*M) nested loops (TOO SLOW)
for inst_row in institutions:
    for firm_row in firms:
        # Check match...

# CORRECT: Lookup dictionaries (FAST)
build_lookups(firms_df)
for inst_row in institutions:
    # O(1) lookup
    if inst_name in name_to_firms:
        # Match
```

---

## Expected Results Summary

| Phase | Firms | Accuracy | Papers |
|-------|-------|----------|--------|
| **Stage 1: Exact + Ticker** | 1,670 | 98% | 250,000 |
| **Stage 2: Fuzzy + BusDesc** | +400 | 93% | 80,000 |
| **Stage 3: Parent + Manual** | +150 | 97% | 50,000 |
| **TOTAL** | **2,220** | **96%** | **380,000** |

---

## Implementation Script (Optimized)

**File:** `src/02_linking/match_publications_optimized.py`

**Key Features:**
1. Build 4 lookup dictionaries (O(1) lookup)
2. Process institutions in batches (for progress tracking)
3. Use ticker-to-firm matching (highest value)
4. Filter generic terms and short names
5. No O(N*M) nested loops (fixes timeout)

---

## Success Metrics

### Minimum Targets:
- ✅ **≥2,000 unique firms**
- ✅ **≥95% accuracy**
- ✅ **≥300,000 papers**
- ✅ **Runtime: <10 minutes** (not 3 hours!)

### Stretch Goals:
- **3,000+ firms**
- **97%+ accuracy**
- **500,000+ papers**

---

## Comparison: Original Plan vs Optimized Plan

| Aspect | Original Plan | Optimized Plan |
|--------|--------------|----------------|
| **Target firms** | 10,000-13,000 (60-80%) | 2,000-3,000 (realistic) |
| **ROR/Wikidata APIs** | Required | **Blocked - skip** |
| **Ticker matching** | From Wikidata API | **From institution acronyms** ✓ |
| **Performance** | Not specified | **Optimized with lookups** ✓ |
| **Runtime** | Hours | **Minutes** ✓ |

---

## Next Steps

1. ✅ **Create optimized script** with lookup dictionaries
2. ✅ **Run Stage 1** (expect 1,500-2,000 firms)
3. ✅ **Validate accuracy** (target >95%)
4. ✅ **If below target:** Add Stage 2 fuzzy matching
5. ✅ **Final validation** and quality check

---

**Timeline:** 2-3 hours to complete Stage 1 + validation

**Ready to implement?** The optimized approach should achieve 2,000+ firms with >95% accuracy in under 10 minutes runtime.
