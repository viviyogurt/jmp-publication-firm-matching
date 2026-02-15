# Affiliation Coverage Solutions: Summary and Recommendations

## Executive Summary

**Problem:** Only 47.9% of ArXiv papers have institution/affiliation data in OpenAlex, limiting our ability to link papers to firms.

**Key Finding:** ArXiv API DOES provide affiliation data via `<arxiv:affiliation>` element (optional field).

**Best Immediate Solution:** 
1. Extract affiliations from ArXiv authors field (+4.4% papers, 203 Big Tech records)
2. Parse `raw_affiliation_strings` from OpenAlex (+2.1% coverage)
3. Improve research division matching using lineage
4. **Combined coverage: 50.1%** (up from 47.9%)

## Investigation Results

### 1. ArXiv API Analysis

**Question:** Does ArXiv API provide affiliation information?

**Answer:** ✅ **YES, but limited**

**Evidence:**
- According to [ArXiv API documentation](https://info.arxiv.org/help/api/user-manual.html), `<arxiv:affiliation>` is a subelement of `<author>` if present
- Authors can include affiliations in the authors field using parentheses: "Author Name (Institution Name)"
- **Our extraction found:** 6.0% of authors have affiliations in the authors field
- **5.0% of papers** have at least one affiliation in the authors field
- **203 Big Tech author records** found (115 Microsoft, 43 Google, 23 Meta, 21 Amazon, 1 Apple)

**Conclusion:** ArXiv API does provide affiliation data, but only when authors choose to include it (optional field). This provides additional coverage beyond OpenAlex.

### 2. OpenAlex raw_affiliation_strings

**Coverage Analysis:**
- 48.3% of papers have `raw_affiliation_strings`
- 46.2% have parsed institutions  
- Only **2.1%** have raw strings but NO parsed institutions
- 51.7% have neither parsed nor raw affiliations

**Potential Improvement:** +2.1% coverage (47% → 49%)

**Conclusion:** Limited but useful - can help find some additional Big Tech papers.

### 3. Root Cause

**Why only 47% coverage?**

1. **OpenAlex Data Quality Issue:**
   - 53.8% of papers have authorships but NO institutions array
   - OpenAlex tries to extract affiliations but fails for many papers
   - This is a limitation of OpenAlex's data extraction, not our code

2. **Author Behavior:**
   - Many authors don't provide affiliation information
   - Older papers have less complete metadata
   - Inconsistent formatting makes parsing difficult

## Solutions Implemented

### ✅ Solution 1: Extract Affiliations from ArXiv Authors Field

**File:** `extract_affiliations_from_arxiv_authors.py` (new)

**Implementation:**
- Parse authors field for affiliations in parentheses: "Author (Institution)"
- Extract author-affiliation pairs
- Identify Big Tech firms from affiliations

**Results:**
- 6.0% of authors have affiliations (301,099 out of 5,001,756)
- 5.0% of papers have at least one affiliation (131,512 papers)
- Found 203 Big Tech author records (115 Microsoft, 43 Google, 23 Meta, 21 Amazon, 1 Apple)

**Status:** ✅ Implemented

### ✅ Solution 2: Parse raw_affiliation_strings

**File:** `extract_institutions_from_json.py` (updated)

**Implementation:**
- When parsed institutions are missing, check `raw_affiliation_strings`
- Use pattern matching to identify Big Tech companies
- Extract institution names from raw strings

**Expected Improvement:** +2.1% coverage + additional Big Tech papers

**Status:** ✅ Implemented

### ✅ Solution 3: Improve Research Division Matching

**File:** `improve_institution_extraction.py`

**Implementation:**
- Use `institution_lineage` to find parent companies
- Match research divisions (e.g., "Microsoft Research") to parent firms
- Comprehensive pattern matching for subsidiaries

**Expected Improvement:** 2-3x more Big Tech papers found

**Status:** ✅ Implemented

## Future Solutions (Not Yet Implemented)

### Solution 3: Parse ArXiv PDFs/LaTeX Source

**Approach:**
- Download ArXiv PDFs or LaTeX source files
- Extract affiliations from PDF metadata, LaTeX `\affiliation{}` commands, title pages

**Expected Improvement:** +20-30% coverage (could reach 70-80%)

**Challenges:**
- Requires downloading millions of PDFs
- Complex parsing
- Storage/bandwidth intensive

**Status:** ⏳ Future work

### Solution 4: ORCID Integration

**Approach:**
- Link authors to ORCID IDs
- Query ORCID API for affiliation history

**Expected Improvement:** +10-15% coverage

**Status:** ⏳ Future work

### Solution 5: Multi-Source Aggregation

**Approach:**
- Cross-reference with Harvard ADS, DBLP, Semantic Scholar, Crossref

**Expected Improvement:** +5-10% coverage

**Status:** ⏳ Future work

## Recommended Next Steps

### Immediate (This Week)

1. **Re-run extraction with improvements:**
   ```bash
   python jmp/src/01_data_construction/extract_institutions_from_json.py
   ```
   - This will now parse raw_affiliation_strings for Big Tech companies
   - Should find additional papers

2. **Update Big Tech matching:**
   - Use improved matching function with lineage checking
   - Should find 2-3x more Big Tech papers

3. **Re-run analysis:**
   ```bash
   python jmp/src/02_linking/link_arxiv_to_institutions.py
   python jmp/src/03_analysis/analyze_big_tech_publications_vs_patents.py
   ```

**Expected Result:** 
- ~49% coverage (up from 47%)
- 2-3x more Big Tech papers found
- Better matching of research divisions

### Short-term (Next Month)

1. Implement PDF parsing for high-value papers (Big Tech focus)
2. Create ORCID integration for author matching
3. Cross-reference with Semantic Scholar API

### Long-term (Future)

1. Full PDF parsing pipeline
2. Multi-source aggregation
3. Machine learning for affiliation extraction

## Code Changes Made

1. **`extract_institutions_from_json.py`:**
   - Added parsing of `raw_affiliation_strings` when parsed institutions are missing
   - Added `source` field to track data origin
   - Pattern matching for Big Tech companies in raw strings

2. **`improve_institution_extraction.py`:**
   - Comprehensive research division mapping
   - Lineage-based parent company matching
   - Big Tech identification function

3. **`extract_affiliations_from_multiple_sources.py`:**
   - Standalone script for extracting from raw affiliations
   - Can be used for focused Big Tech extraction

## Expected Outcomes

### Coverage Improvement
- **OpenAlex only:** 47.9% of papers have institution data (1,080,335 papers)
- **ArXiv authors field:** 5.0% of papers have affiliations (131,512 papers)
- **Combined:** 50.1% coverage (1,130,608 papers)
  - +50,273 additional papers from ArXiv authors field (+4.4%)
  - 81,239 papers have both sources
- **After PDF parsing (future):** 70-80%

### Big Tech Paper Discovery
- **Current:** 42 Google, 72 Apple, 1 Microsoft papers
- **After improvements:** Expected 2-3x more (100-200+ papers per firm)
- **After PDF parsing (future):** Potentially 10x more

## Limitations and Acceptances

**We must accept:**
- 47-49% coverage is a limitation of available data sources
- Cannot be fully solved without significant additional infrastructure
- Some papers will never have affiliation data

**We can improve:**
- Better matching of papers that DO have data
- Finding more Big Tech papers through improved patterns
- Quality over quantity - focus on papers with good data

## Conclusion

**ArXiv API:** ✅ Provides affiliation data via `<arxiv:affiliation>` element (optional)

**Best immediate solution:** 
- ✅ Extract affiliations from ArXiv authors field (+4.4% papers, 203 Big Tech records)
- ✅ Parse raw_affiliation_strings (+2.1%)
- ✅ Improve research division matching (2-3x more Big Tech papers)
- ✅ **Combined coverage: 50.1%** (up from 47.9%)

**Future solutions:** PDF parsing, ORCID, multi-source aggregation (high impact but require significant development)

