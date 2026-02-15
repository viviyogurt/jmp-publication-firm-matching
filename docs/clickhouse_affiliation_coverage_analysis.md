# ClickHouse Affiliation Coverage Analysis

## Summary

**Key Finding:** ClickHouse contains significantly more papers with affiliation data than we're currently using!

## Current Data We're Using

- **`arxiv_index`**: ~2.7M rows (from `openalex_claude_arxiv_index.parquet`)
- **Coverage**: 47.9% of papers have institutions (1,080,335 papers)

## Additional Data Available in ClickHouse

### 1. `works_raw` Table

**Size:** 270,051,911 rows (huge!)

**Key Findings:**
- **246,983,770 works have affiliations** (91.5% of all works!)
- **3,958,072 works mention 'arxiv'** (much more than our current ~2.7M)
- Contains full OpenAlex work JSON with `authorships` and `institutions`

**Potential Improvement:**
- Could potentially find **1.2M+ additional ArXiv papers** with affiliations
- This would significantly improve our coverage from 47.9% to potentially 60-70%+

### 2. `arxiv_index_enhanced` Table

**Size:** 40,109,739 rows

**Comparison:**
- Current `arxiv_index`: ~2.7M rows
- `arxiv_index_enhanced`: 40.1M rows
- **Difference: +37.4M rows** (14.8x more!)

**Structure:**
- `openalex_id`: OpenAlex work ID
- `arxiv_id`: ArXiv ID
- `doi`: DOI
- `json`: Full OpenAlex work JSON

**Potential Improvement:**
- Much larger dataset - likely includes more ArXiv papers
- May have better/more complete affiliation data

### 3. Other Tables

- **`authors_raw`**: 104,532,760 rows - Full author JSON (may have affiliation history)
- **`institutions_raw`**: 117,061 rows - Full institution JSON
- **`parsed_institutions`**: 117,061 rows - Parsed institution data (id, display_name, country_code, type, etc.)

## Recommendations

### Immediate Action: Use `arxiv_index_enhanced`

**Why:**
1. **14.8x more rows** than current `arxiv_index`
2. Same structure (has `json` field with full OpenAlex work data)
3. Likely includes many more ArXiv papers with affiliations

**Action:**
1. Fetch `arxiv_index_enhanced` from ClickHouse
2. Extract institutions using same method as `extract_institutions_from_json.py`
3. Compare coverage with current data

### Future: Consider `works_raw` for Non-ArXiv Papers

**Why:**
- 246M works with affiliations
- Could expand analysis beyond just ArXiv papers
- For now, focus on ArXiv papers first

## Expected Coverage Improvement

**Current:**
- 47.9% coverage (1,080,335 papers with institutions)

**After using `arxiv_index_enhanced`:**
- Potentially **60-70%+ coverage**
- Could find **500K-1M+ additional papers** with affiliations

**Combined with ArXiv authors field:**
- Current: 50.1% coverage (1,130,608 papers)
- With `arxiv_index_enhanced`: Potentially **65-75% coverage**

## Implementation Plan

### Phase 1: Fetch `arxiv_index_enhanced` (High Priority)

1. Create script to fetch `arxiv_index_enhanced` from ClickHouse
2. Save as parquet: `openalex_claude_arxiv_index_enhanced.parquet`
3. Extract institutions using existing `extract_institutions_from_json.py` logic
4. Compare coverage with current data

### Phase 2: Analyze Coverage Improvement

1. Count papers with institutions in enhanced data
2. Compare with current coverage
3. Identify additional Big Tech papers found

### Phase 3: Merge and Update Pipeline

1. Merge enhanced data with current data
2. Update linking scripts to use enhanced data
3. Re-run Big Tech analysis

## Code Changes Needed

1. **New script:** `fetch_arxiv_index_enhanced.py`
   - Fetch `arxiv_index_enhanced` from ClickHouse
   - Save as parquet

2. **Update:** `extract_institutions_from_json.py`
   - Add option to process enhanced data
   - Or create separate script: `extract_institutions_from_enhanced.py`

3. **Update:** `link_arxiv_to_institutions.py`
   - Use enhanced data if available
   - Merge with current data

## Conclusion

**Yes, we can significantly improve affiliation coverage!**

The `arxiv_index_enhanced` table has **14.8x more rows** than our current data, which could:
- Increase coverage from 47.9% to 60-70%+
- Find 500K-1M+ additional papers with affiliations
- Significantly improve Big Tech paper discovery

**Next Step:** Fetch and analyze `arxiv_index_enhanced` to quantify the improvement.

