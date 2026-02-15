# Strategy: Linking ArXiv Papers to Public Firms

## Goal
Link ArXiv papers to public firms to enable economics/finance research on corporate innovation.

## Current Data Assets

### Available Data:
1. **ArXiv Papers** (`claude_arxiv.parquet`)
   - 1,248,157 papers
   - Columns: arxiv_id, title, authors, categories, abstract, published, updated, etc.
   - **Issue:** No direct institution/affiliation information in ArXiv metadata

2. **OpenAlex Data** (from ClickHouse)
   - `institutions` table: 117,062 institutions (includes companies)
   - `works` table: Publications with author-institution linkages
   - `ai_papers` table: AI-related papers subset
   - `arxiv_index` table: Links ArXiv IDs to OpenAlex works

3. **Compustat Data** (`data/raw/compustat/raw_funda.parquet`)
   - Public firm financial data
   - Company names (`conm` field)
   - GVKEY identifiers

### Missing/Empty Data:
- `arxiv_openalex_linktable`: **EMPTY (0 rows)** - This was supposed to link ArXiv to institutions
- Need to build this linkage ourselves

## Linking Strategy

### Step 1: ArXiv → OpenAlex Works
**Goal:** Match ArXiv papers to OpenAlex works to get author-institution data

**Data Sources:**
- `claude_arxiv.parquet` (ArXiv papers)
- `openalex_claude_arxiv_index.parquet` (ArXiv to OpenAlex mapping)
- `openalex_claude_works.parquet` (OpenAlex works with author data)

**Approach:**
1. Use `arxiv_index` table to match `arxiv_id` → `openalex_id`
2. Join with `works` table to get work details
3. Extract author-institution relationships from works

**Challenges:**
- Need to check if `arxiv_index` has good coverage
- Works table may need to be fetched (currently not in local data)

### Step 2: OpenAlex Works → Institutions
**Goal:** Get institution affiliations for each paper

**Data Sources:**
- `openalex_claude_works.parquet` (when fetched)
- `openalex_claude_institutions.parquet` (when fetched)
- Or query OpenAlex API/database directly

**Approach:**
1. Extract author-institution relationships from works
2. Filter for institutions with `type = 'company'`
3. Get institution names and metadata

**Key Fields:**
- `institution_id` (OpenAlex ID)
- `institution_name` / `display_name`
- `institution_type` (filter for 'company')
- `institution_country`
- `institution_ror` (ROR ID - useful for matching)

### Step 3: Institutions → Public Firms
**Goal:** Match OpenAlex company institutions to Compustat public firms

**Data Sources:**
- OpenAlex institutions (type='company')
- Compustat fundamentals (`raw_funda.parquet`)

**Matching Approaches (in order of preference):**

#### 3a. Name-Based Fuzzy Matching
- Clean institution names (remove suffixes: Inc, Corp, LLC, etc.)
- Clean Compustat company names (`conm` field)
- Use fuzzy string matching (Levenshtein, Jaro-Winkler)
- Match on:
  - Exact match (after cleaning)
  - High similarity score (>0.85)
  - Consider country code for disambiguation

#### 3b. ROR ID Matching (if available)
- Some institutions have ROR IDs
- ROR database may have company identifiers
- Can cross-reference with other databases

#### 3c. Manual Mapping for Large Firms
- Create manual mappings for known tech companies
- Google → Alphabet (GOOGL)
- Facebook → Meta (META)
- Microsoft, Amazon, Apple, etc.

#### 3d. Alternative Data Sources
- **Crunchbase:** Links companies to publications
- **LinkedIn:** Author profiles with company affiliations
- **Patent data:** Already have patent-to-firm linkages (can cross-reference)

### Step 4: Create Final Panel
**Goal:** Build paper-firm-year panel for analysis

**Structure:**
```
paper_id | firm_gvkey | firm_name | paper_year | paper_title | 
institution_name | match_confidence | ...
```

**Key Considerations:**
- Handle multiple authors per paper (multiple institutions)
- Handle multiple institutions per author (affiliation changes)
- Time-varying affiliations (author may change firms)
- Match confidence scores for quality control

## Implementation Plan

### Phase 1: Data Preparation
1. **Fetch missing OpenAlex tables:**
   - `works` table (with author-institution relationships)
   - `institutions` table (full version)
   - `authors_raw` (if needed for author details)

2. **Build ArXiv-OpenAlex linkage:**
   - Use `arxiv_index` to create mapping
   - Validate coverage and quality

### Phase 2: Institution Extraction
1. **Extract author-institution pairs from works:**
   - Parse author affiliations from works data
   - Filter for company-type institutions
   - Create paper-institution mapping

2. **Clean and standardize institution names:**
   - Remove suffixes, normalize formats
   - Handle variations (e.g., "Google" vs "Google Inc" vs "Alphabet")

### Phase 3: Firm Matching
1. **Prepare Compustat data:**
   - Clean company names
   - Create name variations
   - Extract relevant time periods

2. **Implement matching algorithm:**
   - Fuzzy string matching
   - Country-based filtering
   - Manual overrides for known firms

3. **Quality control:**
   - Review high-confidence matches
   - Flag ambiguous matches
   - Create match confidence scores

### Phase 4: Panel Construction
1. **Create paper-firm-year panel:**
   - Aggregate by paper, firm, year
   - Handle multiple firms per paper
   - Handle multiple papers per firm

2. **Add metadata:**
   - Paper characteristics (AI-related, categories)
   - Firm characteristics (from Compustat)
   - Match quality indicators

## Data Quality Considerations

### Coverage Issues:
- **ArXiv → OpenAlex:** Need to check `arxiv_index` coverage
- **Works → Institutions:** Not all papers may have institution data
- **Institutions → Firms:** Not all companies are public firms

### Matching Challenges:
- **Name variations:** "Google" vs "Google Inc" vs "Alphabet Inc"
- **Subsidiaries:** Need to map subsidiaries to parent companies
- **Name changes:** Companies may have changed names over time
- **International firms:** Different naming conventions

### Solutions:
- Use multiple matching strategies
- Manual review for top firms
- Confidence scoring
- Validation against known datasets

## Alternative Approaches

### If OpenAlex Linkage is Insufficient:

1. **Use Patent Data:**
   - You already have patent-to-firm linkages
   - Cross-reference authors who appear in both patents and papers
   - Infer firm affiliations from patent data

2. **Author Name Matching:**
   - Match author names between ArXiv and patent data
   - Use patent assignee as proxy for paper affiliation
   - Requires author name disambiguation

3. **External APIs:**
   - **OpenAlex API:** Query directly for paper-institution data
   - **Semantic Scholar API:** May have affiliation data
   - **ORCID:** Author profiles with affiliations

## Recommended Next Steps

1. **Immediate:**
   - Check `arxiv_index` table coverage and structure
   - Fetch `works` and `institutions` tables from ClickHouse
   - Examine data quality and completeness

2. **Short-term:**
   - Build ArXiv → OpenAlex → Institutions pipeline
   - Implement basic name-based firm matching
   - Create initial paper-firm linkages

3. **Medium-term:**
   - Refine matching algorithm
   - Add manual mappings for large firms
   - Validate against known datasets

4. **Long-term:**
   - Build comprehensive panel
   - Add time-varying affiliations
   - Create research-ready dataset

## Tools/Libraries Needed

- **Fuzzy Matching:** `fuzzywuzzy`, `rapidfuzz`, or `thefuzz`
- **Name Cleaning:** Custom functions (similar to patent name cleaning)
- **Data Processing:** Polars (already in use)
- **Validation:** Manual review tools, confidence scoring

## Expected Output

**Final Dataset Structure:**
- Paper-level: arxiv_id, title, year, categories, etc.
- Firm-level: gvkey, company_name, industry, etc.
- Linkage: paper_id, firm_gvkey, match_confidence, match_method
- Panel: paper-firm-year observations for analysis

