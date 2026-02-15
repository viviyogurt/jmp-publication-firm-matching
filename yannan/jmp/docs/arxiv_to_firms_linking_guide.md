# Complete Guide: Linking ArXiv Papers to Public Firms

## Key Findings from Data Analysis (Updated 2025-01-07)

**✅ Critical Discovery:** All institution data needed is already in `arxiv_index.json`!

### Data Files Analyzed:
1. **`openalex_claude_ai_papers.parquet`** (18,868,693 rows)
   - Contains: Paper metadata only (title, year, citations, AI categories)
   - Does NOT contain: Institution or author data
   - Use for: Filtering AI papers, getting citation counts

2. **`openalex_claude_arxiv_index.parquet`** (2,763,564 rows)
   - Contains: `openalex_id`, `arxiv_id`, `doi`, and **`json`** field
   - **Key Finding:** `json` field contains COMPLETE OpenAlex work object
   - **Includes:** Full `authorships[]` array with institution relationships
   - **Structure Verified:** Each authorship has `institutions[]` with:
     - `id`, `display_name`, `type`, `ror`, `country_code`, `lineage`

3. **`claude_arxiv.parquet`** (1,248,157 rows)
   - Contains: ArXiv paper metadata
   - Links to: OpenAlex via `arxiv_index`

### Implementation Path:
- ✅ **No additional table fetches needed** - all data is in `arxiv_index.json`
- ✅ **Ready to implement** - JSON structure is known and verified
- ⏳ **Next step:** Extract institutions from JSON, filter for US companies, match to Compustat

## Executive Summary

**Goal:** Link ArXiv papers to public firms to enable economics/finance research on corporate innovation.

**Current Situation:**
- ✅ Have: 1.2M ArXiv papers, 2.7M ArXiv-OpenAlex mappings, Compustat firm data
- ✅ **NEW:** `arxiv_index.json` contains full OpenAlex work data including author-institution relationships
- ✅ **NEW:** `ai_papers.parquet` (18.8M rows) - paper metadata with AI classifications
- ❌ Missing: Direct paper-institution linkages (linktable is empty)
- ✅ **SOLUTION:** Extract institution data from `arxiv_index.json` field - no need to fetch additional tables!

## Data Flow Architecture

```
ArXiv Papers → OpenAlex Works → Authors → Institutions → Public Firms
     ↓              ↓              ↓            ↓              ↓
  arxiv_id    openalex_id    author_id   institution_id   gvkey
```

## Step-by-Step Strategy

### STEP 1: ArXiv → OpenAlex Works Linkage

**What you have:**
- `claude_arxiv.parquet`: 1,248,157 ArXiv papers
- `openalex_claude_arxiv_index.parquet`: 2,763,564 mappings (arxiv_id → openalex_id)
- Coverage: 100% of arxiv_index entries have openalex_id

**Action:**
1. Join ArXiv papers with `arxiv_index` on `arxiv_id`
2. Get `openalex_id` for each ArXiv paper
3. Note: Some ArXiv papers may have multiple OpenAlex matches (versions)

**Code approach:**
```python
arxiv = pl.read_parquet("data/raw/publication/claude_arxiv.parquet")
arxiv_index = pl.read_parquet("data/raw/publication/openalex_claude_arxiv_index.parquet")

# Join to get openalex_id
arxiv_with_openalex = arxiv.join(
    arxiv_index.select(["arxiv_id", "openalex_id"]),
    on="arxiv_id",
    how="left"
)
```

### STEP 2: OpenAlex Works → Institutions

**✅ SOLUTION IDENTIFIED: Extract from `arxiv_index.json`**

**Data Source:** `openalex_claude_arxiv_index.parquet` (2,763,564 rows)
- **Key Finding:** The `json` field contains the COMPLETE OpenAlex work data
- **Contains:** Full `authorships` array with institution relationships
- **No additional fetch needed:** All institution data is already in the JSON field

**JSON Structure (Verified):**
```json
{
  "id": "https://openalex.org/W100001730",
  "publication_year": 2012,
  "authorships": [
    {
      "author_position": "first",
      "author": {"id": "https://openalex.org/A123456", "display_name": "..."},
      "institutions": [
        {
          "id": "https://openalex.org/I123456",
          "display_name": "Google",
          "ror": "https://ror.org/...",
          "country_code": "US",
          "type": "company",
          "type_id": 1
        }
      ],
      "countries": ["US"],
      "is_corresponding": true,
      "raw_affiliation_strings": ["Google Research, Mountain View, CA"]
    }
  ],
  "institution_assertions": [...],
  "institutions_distinct_count": 1,
  "corresponding_institution_ids": [...]
}
```

**Key Fields to Extract:**
- `authorships[].author.id`: OpenAlex author identifier
- `authorships[].institutions[].id`: OpenAlex institution identifier  
- `authorships[].institutions[].display_name`: Institution name
- `authorships[].institutions[].type`: Filter for 'company'
- `authorships[].institutions[].ror`: ROR ID (useful for matching)
- `authorships[].institutions[].country_code`: Country (filter for US)
- `authorships[].institutions[].lineage`: Parent institution hierarchy

**Implementation Code:**
```python
import pandas as pd
import json
import polars as pl

# Load arxiv_index
arxiv_index = pl.read_parquet("data/raw/publication/openalex_claude_arxiv_index.parquet")

# Parse JSON and extract institution data
def extract_institutions_from_json(json_str):
    """Extract institution data from OpenAlex work JSON"""
    try:
        work = json.loads(json_str) if isinstance(json_str, str) else json_str
        institutions_list = []
        
        if 'authorships' not in work:
            return []
        
        for authorship in work.get('authorships', []):
            author_id = authorship.get('author', {}).get('id', '')
            author_position = authorship.get('author_position', '')
            
            for inst in authorship.get('institutions', []):
                institutions_list.append({
                    'openalex_id': work.get('id', ''),
                    'publication_year': work.get('publication_year'),
                    'author_id': author_id,
                    'author_position': author_position,
                    'institution_id': inst.get('id', ''),
                    'institution_name': inst.get('display_name', ''),
                    'institution_type': inst.get('type', ''),
                    'institution_ror': inst.get('ror', ''),
                    'country_code': inst.get('country_code', ''),
                    'is_corresponding': authorship.get('is_corresponding', False)
                })
        
        return institutions_list
    except Exception as e:
        return []

# Extract institutions (process in chunks for memory efficiency)
institution_records = []
for batch in arxiv_index.iter_slices(n_rows=10000):
    for row in batch:
        json_data = row['json']
        records = extract_institutions_from_json(json_data)
        institution_records.extend(records)

# Convert to DataFrame
paper_institutions = pl.DataFrame(institution_records)
```

**Note on `ai_papers.parquet`:**
- **Does NOT contain institution data** - only paper metadata (title, year, citations, AI categories)
- Use `ai_papers` for filtering AI papers, but extract institutions from `arxiv_index.json`

### STEP 3: Filter for Company Institutions

**Action:**
1. Filter extracted institutions where `institution_type = 'company'`
2. **Focus on US companies:** Filter `country_code = 'US'` for public firm matching
3. Get institution names and metadata
4. Clean and standardize names

**Data Source:**
- From Step 2: `paper_institutions` DataFrame (extracted from `arxiv_index.json`)
- Optional: `openalex_claude.institutions` table (117,062 institutions) for additional metadata
  - Can join on `institution_id` to get full institution details if needed

**Filtering Code:**
```python
# Filter for US company institutions
us_company_institutions = paper_institutions.filter(
    (pl.col('institution_type') == 'company') &
    (pl.col('country_code') == 'US')
)

# Get unique company institutions
unique_companies = us_company_institutions.select([
    'institution_id',
    'institution_name',
    'institution_ror',
    'country_code'
]).unique()

print(f"Found {len(unique_companies):,} unique US company institutions")
```

**Expected Output:**
- List of US company institutions with:
  - `institution_id` (OpenAlex ID)
  - `institution_name` (company name)
  - `institution_ror` (ROR ID if available)
  - `country_code` ('US')
  - Paper-level linkages preserved for panel construction

### STEP 4: Institution → Public Firm Matching

**Data Sources:**
- OpenAlex company institutions (from Step 3)
- Compustat fundamentals (`data/raw/compustat/raw_funda.parquet`)
  - `conm`: Company name
  - `gvkey`: Firm identifier
  - `datadate`: Date for time-varying matching

**Matching Strategy (Multi-layered):**

#### Layer 1: Exact Match (after cleaning)
1. Clean both institution names and Compustat names:
   - Remove suffixes: Inc, Corp, LLC, Ltd, etc.
   - Convert to uppercase
   - Remove punctuation
   - Standardize abbreviations

2. Match on cleaned names
3. High confidence matches

#### Layer 2: Fuzzy String Matching
1. Use fuzzy matching algorithms:
   - Levenshtein distance
   - Jaro-Winkler similarity
   - Token-based matching

2. Set similarity threshold (e.g., >0.85)
3. Review matches manually

#### Layer 3: Known Mappings
Create manual mapping for major tech companies:
```python
KNOWN_MAPPINGS = {
    "Google": "Alphabet Inc",
    "Alphabet": "Alphabet Inc", 
    "Facebook": "Meta Platforms Inc",
    "Microsoft": "Microsoft Corporation",
    "Amazon": "Amazon.com Inc",
    "Apple": "Apple Inc",
    # ... etc
}
```

#### Layer 4: ROR ID Matching (if available)
- Some institutions have ROR IDs
- ROR database may link to company identifiers
- Cross-reference with other databases

#### Layer 5: Subsidiary Mapping
- Map subsidiaries to parent companies
- Use company hierarchy data if available
- Example: "Google Research" → "Alphabet Inc"

**Matching Quality:**
- Assign confidence scores (0-1)
- Flag ambiguous matches for review
- Keep multiple potential matches if uncertain

### STEP 5: Create Paper-Firm Panel

**Final Panel Structure:**
```
paper_id | firm_gvkey | firm_name | paper_year | 
institution_name | match_confidence | match_method | ...
```

**Key Considerations:**

1. **Multiple Authors per Paper:**
   - One paper can have multiple firm affiliations
   - Create one row per paper-firm combination
   - Or aggregate (e.g., count firms per paper)

2. **Time-Varying Affiliations:**
   - Authors may change firms over time
   - Use paper publication year for matching
   - Consider author's firm at time of publication

3. **Match Confidence:**
   - High: Exact match or known mapping
   - Medium: Fuzzy match with high similarity
   - Low: Ambiguous matches (flag for review)

4. **Data Quality Flags:**
   - `has_institution_data`: Paper has institution info
   - `is_company`: Institution is a company
   - `matched_to_firm`: Successfully matched to public firm
   - `match_confidence`: 0-1 score

## Implementation Priority

### Phase 1: Quick Wins (Start Here) - **READY TO IMPLEMENT**
1. ✅ Check `arxiv_index` structure (DONE - has openalex_id and full JSON)
2. ✅ Verify JSON contains `authorships` with institutions (DONE)
3. ✅ Confirm `ai_papers` structure (DONE - metadata only, no institutions)
4. ⏳ **NEXT:** Extract author-institution relationships from `arxiv_index.json`
5. ⏳ Filter for US company-type institutions

### Phase 2: Core Matching
1. ⏳ Implement name cleaning functions
2. ⏳ Build exact match pipeline
3. ⏳ Add fuzzy matching
4. ⏳ Create manual mappings for top firms
5. ⏳ Handle subsidiary mappings (e.g., "Google Research" → "Alphabet Inc")

### Phase 3: Panel Construction
1. ⏳ Build paper-firm-year panel
2. ⏳ Add match quality indicators
3. ⏳ Validate against known datasets
4. ⏳ Create research-ready output

**Status Update:**
- ✅ Data discovery complete - all needed data is in `arxiv_index.json`
- ✅ No additional table fetches required
- ⏳ Ready to implement extraction and matching pipeline

## Where to Get Institution Data

### ✅ PRIMARY SOURCE: `arxiv_index.json` Field (RECOMMENDED)

**Location:** `openalex_claude_arxiv_index.parquet` (2,763,564 rows)
- **Status:** Already downloaded and available locally
- **Contains:** Complete OpenAlex work JSON including `authorships` array
- **Advantage:** No additional database queries needed
- **Coverage:** All ArXiv papers with OpenAlex matches

**Data Structure:**
- Each row has a `json` field containing the full OpenAlex work object
- `authorships[]` array contains author-institution relationships
- Each institution has: `id`, `display_name`, `type`, `ror`, `country_code`, `lineage`

**Extraction Method:**
```python
# Parse JSON field to extract institutions
# See STEP 2 code example above
```

### Alternative: ClickHouse `institutions` Table (Optional)
**Location:** ClickHouse `openalex_claude.institutions` (117,062 institutions)
- **Use Case:** Get additional metadata for institutions (if needed)
- **Not Required:** Basic matching can use data from `arxiv_index.json`
- **When Useful:** If you need full institution details, ROR data, or want to validate institution types

### Alternative: OpenAlex API (Not Recommended)
- More complete but much slower (2.7M API calls)
- Query: `https://api.openalex.org/works/{openalex_id}`
- Extract `authorships[].institutions[]` field
- **Only use if:** JSON parsing fails or data is incomplete

### Note on `ai_papers.parquet`
- **Does NOT contain institution data** - verified structure
- Contains: `openalex_id`, `title`, `publication_year`, `cited_by_count`, AI category flags
- **Use for:** Filtering AI papers, getting citation counts, but NOT for institution extraction

## Matching Institution Names to Firms

### Data Sources for Matching:

1. **Compustat** (You have this):
   - `conm`: Company name field
   - `gvkey`: Unique firm identifier
   - Time-varying (can match by year)

2. **Additional Matching Aids:**
   - **Company websites:** Cross-reference
   - **SEC filings:** Company legal names
   - **Patent data:** You already have patent-firm linkages (can cross-reference)

### Matching Algorithm:

```python
def match_institution_to_firm(institution_name, compustat_df, year):
    """
    Multi-step matching:
    1. Clean names
    2. Exact match
    3. Fuzzy match (if no exact)
    4. Known mappings
    5. Return best match with confidence score
    """
    # Implementation details...
```

## Expected Challenges & Solutions

### Challenge 1: Incomplete Institution Data
**Problem:** Not all papers have institution affiliations
**Solution:** 
- Use what's available
- Flag papers without institution data
- Consider alternative sources (patents, author profiles)

### Challenge 2: Name Variations
**Problem:** "Google" vs "Google Inc" vs "Alphabet Inc"
**Solution:**
- Aggressive name cleaning
- Multiple matching strategies
- Manual review for large firms

### Challenge 3: Subsidiaries
**Problem:** "Google Research" vs "Alphabet Inc"
**Solution:**
- Build subsidiary mapping table
- Map to parent company
- Use company hierarchy data

### Challenge 4: Private vs Public Firms
**Problem:** Many companies are private (not in Compustat)
**Solution:**
- Focus on public firms only
- Flag private companies separately
- May need additional data sources for private firms

## Next Immediate Steps

1. ✅ **Data Structure Analysis (COMPLETE):**
   - ✅ Verified `arxiv_index.json` contains full OpenAlex work data
   - ✅ Confirmed `authorships` array structure with institutions
   - ✅ Verified `ai_papers` does NOT contain institution data (metadata only)

2. ⏳ **Implement JSON Extraction (NEXT STEP):**
   - Create script to parse `arxiv_index.json` field
   - Extract `authorships[].institutions[]` data
   - Create paper-institution linkage table
   - Filter for US companies (`type='company'` AND `country_code='US'`)

3. ⏳ **Build Matching Pipeline:**
   - Implement name cleaning functions
   - Create exact match logic
   - Add fuzzy matching for near-matches
   - Build manual mappings for major tech firms

4. ⏳ **Test on Sample:**
   - Extract institutions from 10k papers
   - Test matching against Compustat
   - Validate match quality
   - Refine before full 2.7M row run

5. ⏳ **Full Pipeline:**
   - Process all 2.7M arxiv_index rows
   - Match to Compustat firms
   - Create final paper-firm-year panel

## Recommended Code Structure

```
src/02_data_linking/
├── 01_extract_institutions.py     # Step 2: Parse arxiv_index.json to extract institutions
├── 02_filter_companies.py          # Step 3: Filter for US company institutions
├── 03_match_to_compustat.py       # Step 4: Match institutions to Compustat firms
├── 04_build_paper_firm_panel.py   # Step 5: Create final paper-firm-year panel
└── utils/
    ├── json_parser.py              # Parse OpenAlex JSON structure
    ├── name_cleaning.py            # Name standardization
    ├── fuzzy_matching.py           # Fuzzy matching algorithms
    ├── known_mappings.py           # Manual firm mappings (Google→Alphabet, etc.)
    └── subsidiary_mapping.py       # Map subsidiaries to parent companies
```

**Implementation Order:**
1. `01_extract_institutions.py` - Parse JSON, extract all institution linkages
2. `02_filter_companies.py` - Filter for US companies
3. `03_match_to_compustat.py` - Match company names to Compustat
4. `04_build_paper_firm_panel.py` - Join with ArXiv papers, create panel

## Success Metrics

- **Coverage:** % of ArXiv papers with institution data (from `arxiv_index.json`)
- **US Company Coverage:** % of papers with US company affiliations
- **Company Match Rate:** % of US company institutions matched to Compustat firms
- **Match Quality:** Distribution of confidence scores (exact, fuzzy, manual)
- **Panel Completeness:** % of ArXiv papers successfully linked to public firms
- **Validation:** Compare with known firm-publication datasets (e.g., patent-firm linkages)

## Data File Summary

**Available Files:**
- `claude_arxiv.parquet`: 1,248,157 ArXiv papers
- `openalex_claude_arxiv_index.parquet`: 2,763,564 mappings with full JSON
- `openalex_claude_ai_papers.parquet`: 18,868,693 AI papers (metadata only)
- `openalex_claude_arxiv_index.parquet`: Contains `json` field with `authorships` ✅

**Key Finding:**
- All institution data needed is in `arxiv_index.json` - no additional fetches required
- `ai_papers` is useful for filtering AI papers but does NOT contain institution data
- Ready to implement extraction and matching pipeline

