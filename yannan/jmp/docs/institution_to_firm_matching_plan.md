# Matching Research Affiliations to US Public Firms
## Comprehensive Strategy for AI Publications Dataset

**Last Updated:** 2025-02-08
**Dataset:** /home/kurtluo/yannan/jmp/data/processed/publication/ai_papers_firms_only.parquet
**Goal:** Match OpenAlex institution affiliations to CRSP/Compustat public firms with minimal false negatives

---

## Executive Summary

This document outlines a multi-stage methodology for matching research institution affiliations from AI publications to US public firms. The approach combines automated matching techniques, semi-automated validation, and targeted hand-matching, following established practices in financial economics literature.

---

## Literature Review: How Researchers Match Institutions to Firms

### 1. Green, Huang, Wen, Zhou (2019) - Glassdoor Matching
**Source:** [Crowdsourced Employer Reviews and Stock Returns](https://faculty.georgetown.edu/qw50/Green,Huang,Wen,Zhou_EmpRatings.pdf)

**Key Methodology:**
- **Hand-matching** of company names to CRSP PERMNO identifiers
- Used Glassdoor identifiers together with company names
- **Validation criteria:**
  - Company headquarters location match
  - CEO name verification
- Coverage: 1,238 unique firms (65% of CRSP-Compustat universe, 81% by market cap)

### 2. Kogan, Papanikolaou, Seru, Stoffman (2017) - Patent Matching
**Source:** [Technological Innovation, Resource Allocation, and Growth](https://www.nber.org/system/files/working_papers/w17769/w17769.pdf)

**Key Methodology:**
- Matched patent assignee names to public firms in CRSP
- Created comprehensive database: 1.9M+ matched patents
- Patent coverage: 27% in NBER dataset matched to Compustat
- Limited to post-1926 period (CRSP coverage)

### 3. OpenAlex/ROR Integration
**Sources:**
- [OpenAlex Institution API](https://docs.openalex.org/api-entities/institutions/institution-object)
- [ROR Matching Documentation](https://ror.readme.io/docs/matching)

**Key Features:**
- **ROR IDs** (successor to GRID) as globally unique identifiers
- All OpenAlex institutions have ROR IDs
- Sophisticated string matching for raw affiliations
- ROR includes organization type (company, university, government)
- Links to WikiData (may contain stock ticker information)

### 4. CRSP/Compustat Merged (CCM) Database
**Source:** [CCM Database Guide](https://www.crsp.org/wp-content/uploads/guides/CRSP_Compustat_Merged_Database_Guide.pdf)

**Key Identifiers:**
- **PERMNO** (CRSP permanent security identifier)
- **GVKEY** (Compustat global company key)
- **CUSIP** (6-digit and 9-digit)
- **CIK** (SEC EDGAR identifier)
- One-to-one mapping validation per time period
- Historical identifier tracking critical

### 5. WRDS Database Linking Matrix
**Source:** [WRDS Linking Matrix](https://wrds-www.wharton.upenn.edu/pages/wrds-research/database-linking-matrix/)

**Systematic Approach:**
- Standardized cross-database mapping
- Historical identifier tables
- Time-series aware linking

---

## Dataset Overview

### AI Papers Dataset Structure
**File:** `data/processed/publication/ai_papers_condensed.parquet`
**Total Papers:** 17,135,917

**Affiliation Columns:**
- `author_affiliations` - List of institution names for each author
- `author_affiliation_ids` - OpenAlex institution IDs (e.g., `https://openalex.org/I4210109390`)
- `author_affiliation_countries` - Country for each affiliation
- `author_primary_affiliations` - Primary institution only
- `author_primary_affiliation_ids` - Primary OpenAlex IDs
- `author_primary_affiliation_countries` - Primary country

**Sample Record:**
```
Paper ID: https://openalex.org/W2055043387
Affiliations: [
  ['National Center for Biotechnology Information', 'National Institutes of Health'],
  ['Pennsylvania State University'],
  ['University of Arizona']
]
Affiliation IDs: [
  ['https://openalex.org/I4210109390', 'https://openalex.org/I1299303238'],
  ['https://openalex.org/I130769515'],
  ['https://openalex.org/I138006243']
]
```

---

## Comprehensive Matching Strategy

### Phase 1: Data Preparation & Exploration

#### Step 1.1: Extract Unique Institutions
```python
# Extract all unique OpenAlex institution IDs
unique_institutions = df['author_affiliation_ids'].explode().unique()

# Create mapping: OpenAlex ID → Institution Name → Country
institution_map = {
    'openalex_id': str,
    'institution_name': str,
    'country': str,
    'organization_type': str,  # from ROR
    'wiki_data_id': str,       # from ROR
}
```

#### Step 1.2: Filter to US Institutions
- Use `author_affiliation_countries` to filter
- Focus on US-based institutions for CRSP matching
- Flag international institutions for separate analysis

#### Step 1.3: Link OpenAlex IDs to ROR
- Extract ROR ID from OpenAlex API responses
- Use [ROR API](https://ror.readme.io/docs/matching) for organization details
- Get organization type (company vs. university vs. government)

#### Step 1.4: Prepare Firm Reference Data (from WRDS)
```
CRSP/Compustat Merged Data:
- PERMNO, GVKEY, company names (current and historical)
- CUSIPs, CIKs
- Firm headquarters (state, city)
- Name change history
- SIC/NAICS codes
```

---

### Phase 2: Multi-Stage Matching Pipeline

**Goal:** Minimize false negatives while maintaining accuracy

#### Stage 1: Direct/High-Confidence Matching (Automated)

##### 2.1.1 ROR Company Type Matching
```python
# Query ROR for organization type
# If type == "Company" → high priority for matching
# If type == "Education" → flag for research collaboration analysis
```

##### 2.1.2 Exact Name Matching
- Direct string match between institution name and CRSP company name
- Include historical name variants
- Case-insensitive, punctuation-normalized

##### 2.1.3 Fuzzy Name Matching
**Algorithms:**
- Jaro-Winkler similarity (good for names)
- Levenshtein distance
- TF-IDF/cosine similarity (for longer names)

**Python Tools:**
- `rapidfuzz` / `thefuzz`
- `textdistance`
- `scikit-learn` (TF-IDF)

**Thresholds:**
- **> 0.95**: High confidence (auto-accept)
- **0.85-0.95**: Medium confidence (flag for review)
- **< 0.85**: Low confidence (manual verification)

##### 2.1.4 Location-Based Filtering
```
Match criteria:
- Institution headquarters state == Firm headquarters state
- Institution city == Firm city (when available)
- US states only (international handled separately)
```

##### 2.1.5 Subsidiary/Division Matching
**Create subsidiary-to-parent mapping:**
```
Examples:
- "Google DeepMind" → "Alphabet Inc." (GOOGL)
- "Meta AI Research (FAIR)" → "Meta Platforms Inc." (META)
- "Microsoft Research" → "Microsoft Corporation" (MSFT)
```

**Data sources:**
- SEC 10-K filings (subsidiary lists)
- EDGAR CIK relationships
- Company websites

#### Stage 2: Medium-Confidence Matching (Semi-Automated)

##### 2.2.1 WikiData/Wikipedia Integration
```
Query WikiData using ROR IDs:
- Stock ticker information
- Parent company
- Industry classification
- Founded date
```

**Example SPARQL query structure:**
```python
# Get stock ticker from WikiData
wd_id = ror_record.get('wikidata_id')
ticker = query_wikidata(wd_id, property='P249')  # stock ticker property
```

##### 2.2.2 Patent Assignee Cross-Reference
- Cross-reference with USPTO patent assignee names
- Use existing Kogan et al. patent-to-CRSP matching as reference
- Patent assignees often use company legal names

##### 2.2.3 Website/Domain Matching
```
1. Extract website domains from ROR/OpenAlex records
2. Match to company domains in SEC filings
3. Useful for subsidiaries with distinct names but shared domains

Example:
- "openai.com" → "OpenAI, LP"
- "anthropic.com" → "Anthropic PBC"
```

#### Stage 3: Hand-Matching (Following Green et al. Methodology)

**Target cases:**
- Medium-confidence matches from Stage 2
- Institutions with ambiguous names
- Recent M&A/acquisition targets
- Spin-offs from universities
- Joint ventures and research consortia

**Validation Criteria:**
1. **Company name similarity** (subjective assessment)
2. **Headquarters location match** (state + ideally city)
3. **CEO/Director names cross-check** (for high-value firms)
4. **Website verification**
5. **Industry alignment** (SIC/NAICS codes)

**Documentation:**
Create match log with:
- Match decision (accept/reject)
- Reasoning
- Confidence level
- Date of verification
- Verifier initials

---

### Phase 3: Quality Assurance & Validation

#### 3.1 False Positive Control
**Methods:**
1. **Random sample validation**: 5-10% of matches reviewed manually
2. **Cross-source verification**:
   - LinkedIn company pages
   - Crunchbase profiles
   - SEC EDGAR filings
   - Company websites
3. **Ticker verification**: Ensure ticker symbols are still active
4. **Temporal consistency**: Check if firm existed at paper publication date

#### 3.2 False Negative Minimization
**Strategies:**
1. **Common misspellings dictionary**:
   ```
   "Alphabet" vs "Alaphabet"
   "NVIDIA" vs "Nvidia"
   "Qualcomm" vs "Qualcom"
   ```

2. **Alias/abbreviation expansion**:
   ```
   "IBM" → "International Business Machines"
   "GE" → "General Electric"
   "HP" → "Hewlett-Packard"
   ```

3. **Frequent re-matching**: As new institutions appear in dataset

4. **Name change tracking**:
   ```
   Example: "Facebook, Inc." → "Meta Platforms, Inc."
   Need historical awareness for papers pre-2021
   ```

#### 3.3 Temporal Consistency Checks
```
For each match:
- Paper publication year >= Firm founding year
- Paper publication year within firm's CRSP coverage period
- Handle name changes over time
- Account for mergers, acquisitions, spin-offs
```

---

### Phase 4: Output & Linkages

#### Final Match Table Schema
```sql
CREATE TABLE paper_institution_firm_matches (
    paper_id STRING,                    -- OpenAlex paper ID
    author_id STRING,                   -- OpenAlex author ID
    openalex_institution_id STRING,     -- OpenAlex institution ID
    ror_id STRING,                      -- ROR identifier
    institution_name STRING,            -- Standardized name
    institution_country STRING,         -- ISO country code
    matched_permno INT,                 -- CRSP PERMNO
    matched_gvkey INT,                  -- Compustat GVKEY
    company_name STRING,                -- Matched company name
    ticker STRING,                      -- Stock ticker
    exchange STRING,                    -- NYSE, NASDAQ, etc.
    match_confidence FLOAT,             -- 0.0 to 1.0
    match_method STRING,                -- 'exact', 'fuzzy', 'subsidiary', 'manual'
    location_match BOOLEAN,            -- HQ state/city match
    subsidiary_parent STRING,           -- Parent company if subsidiary
    validation_status STRING,           -- 'auto', 'reviewed', 'pending'
   matched_at TIMESTAMP,               -- When match was created
   matched_by STRING                   -- 'automatic' or verifier ID
);
```

#### Paper-Level Aggregations
```sql
-- Papers with company affiliations
CREATE TABLE papers_with_firm_matches AS
SELECT
    paper_id,
    publication_year,
    COUNT(DISTINCT matched_permno) as num_firms,
    ARRAY_AGG(DISTINCT ticker) as tickers,
    ARRAY_AGG(DISTINCT company_name) as company_names,
    MAX(match_confidence) as max_confidence
FROM paper_institution_firm_matches
WHERE validation_status = 'approved'
GROUP BY paper_id, publication_year;
```

---

## Implementation Roadmap

### Phase 1: Setup (Week 1)
- [ ] Extract unique institutions from dataset
- [ ] Set up OpenAlex/ROR API access
- [ ] Obtain CRSP/Compustat data from WRDS
- [ ] Create development database schema

### Phase 2: Automated Matching (Week 2-3)
- [ ] Implement exact name matching
- [ ] Implement fuzzy name matching with location filtering
- [ ] Build subsidiary mapping database
- [ ] Integrate ROR organization type classification
- [ ] Query WikiData for ticker information

### Phase 3: Validation (Week 4)
- [ ] Create review interface for medium-confidence matches
- [ ] Hand-match top 100 institutions by publication count
- [ ] Implement random sample validation
- [ ] Cross-reference with patent assignee data

### Phase 4: Quality Assurance (Week 5)
- [ ] Run false positive tests
- [ ] Implement name change tracking
- [ ] Add temporal consistency checks
- [ ] Create matching documentation

### Phase 5: Production (Week 6)
- [ ] Run full pipeline on 17M papers
- [ ] Generate final match tables
- [ ] Create aggregate statistics
- [ ] Document match rates and coverage

---

## Expected Coverage & Limitations

### Expected Match Rates
Based on literature:
- **Direct matches**: 40-50% of company-affiliated papers
- **Subsidiary matches**: 20-30% additional
- **After manual review**: 70-80% total coverage of company-affiliated research

### Limitations
1. **Private companies**: No CRSP/Compustat coverage
2. **International firms**: Limited CRSP coverage
3. **Spin-offs**: Complex ownership structures
4. **Name changes**: Historical tracking required
5. **Joint ventures**: Multiple parent companies
6. **University research centers**: May be funded by companies but not directly affiliated

---

## Tools & Resources

### Python Libraries
```python
# Fuzzy matching
pip install rapidfuzz thefuzz textdistance

# Data processing
pip install pandas pyarrow polars

# API access
pip install requests urllib3

# WRDS access
pip install wrds
```

### APIs & Data Sources
- **OpenAlex API**: https://api.openalex.org
- **ROR API**: https://ror.org/api-docs/
- **WRDS**: https://wrds-www.wharton.upenn.edu/
- **SEC EDGAR**: https://www.sec.gov/edgar/
- **WikiData Query Service**: https://query.wikidata.org/

### Reference Implementations
- Kogan et al. patent matching code (if available)
- CRSP/Compustat linking examples from WRDS

---

## Next Steps

1. **Pilot Study**: Match top 100 institutions by publication count
2. **Validate Approach**: Test match accuracy on known companies
3. **Refine Thresholds**: Adjust fuzzy matching parameters based on pilot
4. **Scale Up**: Run full pipeline on complete dataset
5. **Ongoing Maintenance**: Set up periodic re-matching for new papers

---

## References

1. Green, T. C., Huang, R., Wen, Q., & Zhou, D. (2019). Crowdsourced Employer Reviews and Stock Returns. *Journal of Financial Economics*. [Link](https://faculty.georgetown.edu/qw50/Green,Huang,Wen,Zhou_EmpRatings.pdf)

2. Kogan, L., Papanikolaou, D., Seru, A., & Stoffman, N. (2017). Technological Innovation, Resource Allocation, and Growth. *NBER Working Paper*. [Link](https://www.nber.org/system/files/working_papers/w17769/w17769.pdf)

3. CRSP/Compustat Merged Database Guide. [Link](https://www.crsp.org/wp-content/uploads/guides/CRSP_Compustat_Merged_Database_Guide.pdf)

4. OpenAlex Institution API Documentation. [Link](https://docs.openalex.org/api-entities/institutions/institution-object)

5. ROR Matching Documentation. [Link](https://ror.readme.io/docs/matching)

6. WRDS Database Linking Matrix. [Link](https://wrds-www.wharton.upenn.edu/pages/wrds-research/database-linking-matrix/)

---

**Document Status:** Draft v1.0
**Author:** Generated based on literature review and dataset analysis
**Contact:** [Your email]
