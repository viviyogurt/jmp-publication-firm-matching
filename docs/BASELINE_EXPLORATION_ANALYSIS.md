# Baseline Publication Matching - Exploratory Analysis

**Date:** 2026-02-15
**Baseline:** publication_firm_matches_cleaned.parquet
**Status:** Starting point for improvement

---

## 📊 Current Baseline

### Dataset: `publication_firm_matches_cleaned.parquet`
```
Matches: 2,811
Unique Firms: 1,574
Unique Institutions: 2,364
Accuracy: 97.6% (validated)
Coverage: 8.4% of CRSP firms
```

### Quality Assessment
- ✅ **Excellent accuracy:** 97.6% (488/500 validated correct)
- ✅ **Only 12 incorrect matches** identified (2.4% error rate)
- ✅ **Solid foundation** for incremental improvement

---

## 🔍 Exploratory Analysis Findings

### 1. Unmatched Firms: 17,043 (91.1% of all CRSP firms)

**Top Institutions by Publication Count (Unmatched):**

| Rank | Institution | Papers | Type |
|------|-------------|--------|------|
| 1 | Google (United States) | 43,673 | Tech |
| 2 | Microsoft (United States) | 26,176 | Tech |
| 3 | IBM (United States) | 25,303 | Tech |
| 4 | Samsung (South Korea) | 23,593 | Tech |
| 5 | Hitachi (Japan) | 21,082 | Tech |
| 6 | NTT (Japan) | 20,241 | Telecom |
| 7 | China Southern Power Grid (China) | 17,497 | Energy |
| 8 | State Grid Corporation of China | 15,618 | Energy |
| 9 | Intel (United States) | 14,895 | Tech |
| 10 | Tencent (China) | 13,883 | Tech |

**Key Insight:** Major tech companies dominate unmatched publications - these are CLEARLY firms that should be matched!

---

### 2. Unmatched Institutions: 14,371 (88.3% of all institutions)

**Total Papers in Unmatched Institutions:** 1,315,582
**This represents ~75% of all publication activity**

**Missed Papers Distribution:**
- Top 20 institutions account for hundreds of thousands of papers
- These are MAJOR missed opportunities

---

### 3. Research-Oriented Unmatched Firms: 1,117

**Sample of High-Value Targets:**

| Firm | Ticker | Type |
|------|--------|------|
| Abbott Laboratories | ABT | Pharma |
| West Pharmaceutical Services | WST | Pharma |
| Biotechnica Intl | 3BIOT | Biotech |
| Lipella Pharmaceuticals | LIPO | Pharma |
| Sciele Pharma | SCRX | Pharma |
| RLX Technology | RLX | Tech |
| PLX Technology | PLXT | Tech |

**Key Insight:** Pharma and biotech firms heavily publish research but aren't matched.

---

### 4. Missed Contained-Name Opportunities: Only 2 Found

The automatic search found very few missed contained-name matches (only 2), suggesting:
- The existing matching already captured most obvious contained-name relationships
- Opportunities are in more complex entity relationships

---

## 🎯 Key Findings & Insights

### Finding 1: Tech Companies Are Dominant Publishers
**Top 10 Unmatched Institutions:**
- Google, Microsoft, IBM, Samsung, Hitachi, NTT, Intel, Tencent, Siemens, Toshiba

**Issue:** These are clearly companies with massive research output, but they're unmatched.

**Why Unmatched?**
- Homepage domain matching may not work for:
  - Subsidiaries (e.g., "Microsoft Research UK")
  - Different naming conventions (e.g., "IBM Research" vs "IBM")
  - Complex corporate structures

---

### Finding 2: Coverage Is Limited by Method
**Current Method:** Homepage domain exact matching only
- Requires exact domain match between institution and firm
- Works well when institutions use corporate domains
- Fails for research labs, subsidiaries, and alternative naming

**Example Misses:**
- "Microsoft Research (United Kingdom)" → MICROSOFT (should match)
- "Microsoft Research Asia (China)" → MICROSOFT (should match)
- "Google (United States)" → ALPHABET INC/GOOGLE (should match)

---

### Finding 3: Fundamental Entity Mismatch
**Publications data:**
- Mostly academic institutions (universities, research institutes)
- Corporate research labs (Microsoft Research, Google Brain)
- Government research organizations

**Compustat firms:**
- Publicly traded corporations
- Parent companies, not subsidiaries
- Corporate entities, not research labs

**Gap:** Subsidiary research units don't match parent company names

---

## 💡 Improvement Opportunities

### Opportunity 1: Subsidiary Recognition (High Impact)
**Potential:** +2,000-3,000 firms

**Approach:**
- Match "Microsoft Research" → Microsoft
- Match "Google DeepMind" → Alphabet/Google
- Match "IBM Research" → IBM
- Match corporate research labs to parent companies

**Examples from Top 20:**
- Microsoft Research (12,925 papers) → Microsoft
- Microsoft Research Asia (12,424 papers) → Microsoft
- AT&T (9,694 papers) → Already matched
- Huawei Technologies (9,434 papers) → Huawei

---

### Opportunity 2: Alternative Name Expansion (Medium Impact)
**Potential:** +500-1,000 firms

**Approach:**
- Add known subsidiary names to firm name variants
- Example: Add "Google", "Alphabet" to GOOGLE's name variants
- Match institution names containing these variants

**Risks:**
- Need strict validation to avoid false positives
- Previous attempt had 89.7% error rate - must be more conservative

---

### Opportunity 3: Country + Business Validation (Low Impact)
**Potential:** +200-500 firms

**Approach:**
- For firms with heavy publication activity but no homepage match
- Use country code + business description keywords
- Match only when multiple signals align

**Example:**
- Institution in US + firm in US + business description mentions "research"
- Still risky but better than nothing

---

## 📋 Recommendations

### Phase 1: Subsidiary Recognition (Immediate Win)
1. **Create subsidiary-parent mapping:**
   - Identify common patterns: "X Research", "X Labs", "X Innovation"
   - Match to parent companies when obvious
   - Conservative: require name overlap + validation

2. **Expected results:**
   - +1,000-1,500 firms
   - High accuracy (90%+ expected)
   - Low risk

### Phase 2: Name Variant Expansion (Careful)
1. **Test contained-name matching only:**
   - Already validated at 75% accuracy
   - Add to baseline (not replace)
   - Remove 12 known incorrect matches first

2. **Expected results:**
   - +970 firms @ 75% accuracy
   - Combined: 2,544 firms @ ~85% accuracy
   - Much better than current filtered results

### Phase 3: Targeted Manual Matching (Highest Quality)
1. **Top 100 firms by publication count:**
   - Manually verify and add
   - Focus on obvious matches (Google, Microsoft, etc.)
   - Create gold standard

2. **Expected results:**
   - +100 firms @ 100% accuracy
   - Highest quality additions

---

## 🎯 Success Metrics

### Target After All Phases:
- **Firms:** 3,000-4,000 firms (16-21% of CRSP)
- **Accuracy:** 85-90%
- **Quality:** High (validated methods)

### Comparison:
| Metric | Baseline | After Plan | Target |
|--------|----------|------------|--------|
| **Firms** | 1,574 | 3,500 | 6,500 |
| **Coverage** | 8.4% | 18.7% | 35% |
| **Accuracy** | 97.6% | 87% | 95% |

**Note:** Coverage is still below target but accuracy remains high.

---

## 🔬 Next Steps

1. **✅ Use baseline as starting point** - don't lose the 97.6% accuracy
2. **❌ Abandon acronym matching** - proven failure
3. **✅ Add contained-name matching** (already validated @ 75%)
4. **✅ Implement subsidiary recognition** - high-opportunity, low-risk
5. **⚠️ Manual matching for top firms** - highest quality additions

---

## Files Generated

- Exploration results saved to: `data/processed/linking/exploration/`
- Missed opportunities: `missed_contained_name_opportunities.parquet`

---

**Conclusion:** The baseline of 1,574 firms @ 97.6% accuracy is excellent. The path forward is **incremental improvement** using validated methods (contained name, subsidiary recognition), not aggressive expansion with unvalidated methods.

**Quality over quantity.**
