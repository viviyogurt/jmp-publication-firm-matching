# Improving Affiliation Coverage: Solutions and Strategies

## Problem Statement

**Current Situation:**
- Only **47% of ArXiv papers** have institution/affiliation data in OpenAlex
- This limits our ability to link papers to firms
- Big Tech firms show very few papers (42 for Google, 72 for Apple) compared to patents (20k+)

## Investigation Results

### 1. ArXiv API Limitations

**Finding:** ArXiv API does NOT provide affiliation data.

**Evidence:**
- ArXiv API `Result` object only has: `authors` (list of Author objects)
- `Author` object only has: `name` (string)
- No affiliation field in ArXiv API response
- ArXiv submission form doesn't require affiliations
- Authors can optionally include affiliations in parentheses in author field, but:
  - Only **2.7%** of papers have this format
  - Format is inconsistent and hard to parse

**Conclusion:** ArXiv API cannot solve the coverage problem.

### 2. OpenAlex raw_affiliation_strings

**Finding:** Limited additional coverage from raw affiliation strings.

**Evidence:**
- 48.3% of papers have `raw_affiliation_strings`
- 46.2% have parsed institutions
- Only **2.1%** have raw strings but NO parsed institutions
- Most papers with raw strings also have parsed institutions

**Potential Improvement:** +2.1% coverage (from 47% to ~49%)

**Conclusion:** Raw affiliation strings provide minimal additional coverage.

### 3. Root Cause Analysis

**Why only 47% coverage?**

1. **OpenAlex Data Quality:**
   - 53.8% of papers have authorships but NO institutions in the institutions array
   - This is a data quality issue in OpenAlex
   - OpenAlex tries to extract affiliations from various sources but fails for many papers

2. **Author Behavior:**
   - Many authors don't provide affiliation information
   - Older papers have less complete metadata
   - Some sources don't include affiliation metadata

3. **Parsing Challenges:**
   - Affiliation formats vary widely
   - Multiple authors per paper
   - Inconsistent naming conventions

## Solutions and Strategies

### Solution 1: Parse raw_affiliation_strings (Immediate - +2.1%)

**Implementation:**
- Extract from `raw_affiliation_strings` when parsed institutions are missing
- Use regex patterns to identify company names
- Parse location information to extract country codes

**Code:** `extract_affiliations_from_multiple_sources.py`

**Expected Improvement:** +2.1% coverage (47% → 49%)

**Status:** ✅ Implemented

### Solution 2: Improve Research Division Matching (Immediate)

**Implementation:**
- Use `institution_lineage` to find parent companies
- Match research divisions (e.g., "Microsoft Research") to parent firms
- Comprehensive pattern matching for subsidiaries

**Expected Improvement:** Better matching of Big Tech papers (may find 2-3x more)

**Status:** ✅ Implemented in `improve_institution_extraction.py`

### Solution 3: Parse ArXiv PDFs/LaTeX Source (Future - High Impact)

**Approach:**
- Download ArXiv PDFs or LaTeX source files
- Extract affiliation information from:
  - PDF metadata
  - LaTeX `\affiliation{}` commands
  - Title page text parsing
  - Acknowledgments section

**Expected Improvement:** +20-30% coverage (could reach 70-80%)

**Challenges:**
- Requires downloading millions of PDFs (storage/bandwidth)
- PDF parsing is complex and error-prone
- LaTeX source not always available
- Processing time intensive

**Status:** ⏳ Not implemented (requires significant infrastructure)

### Solution 4: Use ORCID Profiles (Future - Medium Impact)

**Approach:**
- Link authors to ORCID IDs
- Query ORCID API for affiliation history
- Match ORCID affiliations to papers

**Expected Improvement:** +10-15% coverage

**Challenges:**
- Not all authors have ORCID IDs
- ORCID API rate limits
- Requires author name matching (ambiguous)

**Status:** ⏳ Not implemented

### Solution 5: Cross-reference with Other Databases (Future - Medium Impact)

**Approach:**
- Harvard ADS (Astrophysics Data System)
- DBLP (Computer Science Bibliography)
- Semantic Scholar
- Crossref

**Expected Improvement:** +5-10% coverage

**Challenges:**
- Each database has different coverage
- Requires API access and rate limiting
- Data format inconsistencies

**Status:** ⏳ Not implemented

### Solution 6: Focus Analysis on Papers WITH Data (Immediate)

**Approach:**
- Accept 47% coverage as limitation
- Focus analysis on papers with institution data
- Clearly document coverage limitations
- Use statistical methods to account for selection bias

**Expected Improvement:** Better quality analysis (no quantity increase)

**Status:** ✅ Current approach

## Recommended Implementation Plan

### Phase 1: Immediate Improvements (This Week)
1. ✅ Parse `raw_affiliation_strings` for missing cases (+2.1%)
2. ✅ Improve research division matching using lineage
3. ✅ Update Big Tech matching to use improved patterns

**Expected Result:** 49% coverage + better Big Tech matching

### Phase 2: Short-term (Next Month)
1. ⏳ Implement PDF/LaTeX parsing for high-value papers (Big Tech focus)
2. ⏳ Create ORCID integration for author matching
3. ⏳ Cross-reference with Semantic Scholar API

**Expected Result:** 60-70% coverage

### Phase 3: Long-term (Future)
1. ⏳ Full PDF parsing pipeline for all papers
2. ⏳ Multi-source aggregation (OpenAlex + ORCID + Semantic Scholar + Crossref)
3. ⏳ Machine learning for affiliation extraction

**Expected Result:** 70-80% coverage

## Current Best Practice

**For immediate analysis:**
1. Use existing 47% coverage
2. Apply improved research division matching
3. Parse raw_affiliation_strings for additional 2.1%
4. **Total: ~49% coverage**

**For Big Tech analysis:**
1. Use lineage-based matching
2. Include research divisions (Microsoft Research, Google Brain, etc.)
3. Parse raw affiliations for company names
4. **Expected: 2-3x more Big Tech papers found**

## Code Files

1. `extract_affiliations_from_multiple_sources.py` - Parse raw_affiliation_strings
2. `improve_institution_extraction.py` - Research division matching
3. `extract_institutions_from_json.py` - Existing extraction (update with improvements)

## Summary

**ArXiv API:** ❌ Does not provide affiliation data

**OpenAlex raw_affiliation_strings:** ✅ Can add +2.1% coverage

**Best immediate solution:** 
- Parse raw_affiliation_strings
- Improve research division matching
- Accept ~49% coverage as current limitation

**Future solutions:**
- PDF/LaTeX parsing (high impact, high effort)
- ORCID integration (medium impact, medium effort)
- Multi-source aggregation (high impact, high effort)

