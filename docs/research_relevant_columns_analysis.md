# Research-Relevant Columns Analysis for Finance/Economics Research

## Executive Summary

From 13,083 total columns discovered, here are the **most important columns** for finance/economics research on:
- AI/ML applications
- Finance & Financial Markets
- Economics & Policy
- Firms & Corporate Governance
- Innovation & R&D
- Entrepreneurship
- Labor Markets

---

## 1. CORE IDENTIFIERS & METADATA (100% coverage)

### Essential Identifiers
- `id` - OpenAlex work ID
- `doi` - Digital Object Identifier
- `ids.doi` - DOI from IDs object
- `ids.openalex` - OpenAlex ID
- `title` - Paper title
- `display_name` - Display name of the work

### Publication Metadata
- `publication_year` - Year of publication (100%)
- `publication_date` - Full publication date (100%)
- `type` - Work type (article, book, etc.)
- `type_id` - Work type ID
- `type_crossref` - Crossref type
- `language` - Language code
- `language_id` - Language ID

---

## 2. AUTHORS & AFFILIATIONS (100% coverage)

### Author Information
- `authorships` - **CRITICAL**: Full authorship data (JSON array with author details)
- `authors_count` - Number of authors
- `corresponding_author_ids` - IDs of corresponding authors

### Institution Information
- `corresponding_institution_ids` - **CRITICAL**: Institution IDs for corresponding authors
- `institution_assertions` - Institution assertions data
- `institutions_distinct_count` - Count of distinct institutions

**Note**: The `authorships` column contains nested JSON with:
  - Author IDs, names, positions
  - Institution affiliations per author
  - Country information
  - Whether author is corresponding

---

## 3. GEOGRAPHIC & LOCATION DATA (100% coverage)

### Location Information
- `locations` - All publication locations (JSON array)
- `locations_count` - Number of locations
- `primary_location.*` - Primary publication location details
  - `primary_location.source.host_organization` - Host organization
  - `primary_location.source.host_organization_name` - Organization name
  - `primary_location.source.host_organization_lineage` - Organization hierarchy
  - `primary_location.country_code` - Country code (if available in nested data)

**Note**: Geographic data is often nested in `authorships` and `locations` JSON structures.

---

## 4. CITATIONS & IMPACT METRICS (100% coverage)

### Citation Counts
- `cited_by_count` - Total citations received
- `cited_by_api_url` - API URL for citation data
- `summary_stats.cited_by_count` - Summary citation count
- `summary_stats.2yr_cited_by_count` - 2-year citation count

### Citation Percentiles
- `cited_by_percentile_year.max` - Maximum percentile year
- `cited_by_percentile_year.min` - Minimum percentile year
- `citation_normalized_percentile.value` - Normalized percentile (93.5% coverage)
- `citation_normalized_percentile.is_in_top_1_percent` - Top 1% indicator
- `citation_normalized_percentile.is_in_top_10_percent` - Top 10% indicator

### References
- `referenced_works` - Works referenced (JSON array)
- `referenced_works_count` - Count of references

---

## 5. TOPICS & CONCEPTS (100% coverage)

### Topic Classification
- `topics` - **CRITICAL**: All topics assigned (JSON array)
- `topics_count` - Number of topics
- `primary_topic.*` - Primary topic details (93.9% coverage)
  - `primary_topic.id` - Topic ID
  - `primary_topic.display_name` - Topic name
  - `primary_topic.score` - Relevance score
  - `primary_topic.domain.*` - Domain classification
  - `primary_topic.field.*` - Field classification
  - `primary_topic.subfield.*` - Subfield classification

### Concepts
- `concepts` - **CRITICAL**: Concepts associated (JSON array)
- `concepts_count` - Number of concepts
- `keywords` - Keywords (if available)

**Note**: Topics and concepts are JSON arrays that need to be parsed to extract specific research domains.

---

## 6. FUNDING INFORMATION (100% coverage)

- `grants` - **CRITICAL**: Grant/funding information (JSON array)
  - Contains funder IDs, names, award IDs, etc.

---

## 7. PUBLICATION VENUE (100% coverage)

### Source/Journal Information
- `primary_location.source.*` - Primary publication source
  - `primary_location.source.display_name` - Journal/venue name
  - `primary_location.source.id` - Source ID
  - `primary_location.source.issn` - ISSN
  - `primary_location.source.issn_l` - Linking ISSN
  - `primary_location.source.publisher` - Publisher name
  - `primary_location.source.type` - Source type
  - `primary_location.source.is_core` - Core collection indicator
  - `primary_location.source.is_indexed_in_scopus` - Scopus indexing (99.9%)

---

## 8. OPEN ACCESS INFORMATION (100% coverage)

- `open_access.is_oa` - Is open access
- `open_access.oa_status` - OA status (green, gold, etc.)
- `open_access.oa_url` - OA URL
- `open_access.any_repository_has_fulltext` - Fulltext available
- `primary_location.is_oa` - Primary location OA status
- `best_oa_location` - Best OA location (JSON)

---

## 9. CONTENT & ABSTRACT (100% coverage)

- `has_fulltext` - Fulltext availability indicator
- `abstract_inverted_index.*` - **12,986 columns**: Word-level abstract indexing
  - These are individual word positions in abstracts
  - Useful for text analysis but creates column explosion
  - Most common words: "will", "to", "the", "and", "of", "in", etc.

**Note**: The abstract inverted index creates most of the 13K columns. For research purposes, you may want to:
1. Extract the full abstract text (if available in raw JSON)
2. Use only specific keywords from abstract_inverted_index
3. Filter to research-relevant terms

---

## 10. RESEARCH DOMAIN-SPECIFIC COLUMNS

### Finance-Related (from abstract_inverted_index)
- `abstract_inverted_index.financial` (4.2% of rows)
- `abstract_inverted_index.market` (4.2%)
- `abstract_inverted_index.risk` (4.9%)
- `abstract_inverted_index.risks` (4.9%)
- `abstract_inverted_index.bank` (1.1%)
- `abstract_inverted_index.banks` (1.1%)
- `abstract_inverted_index.trading` (1.1%)
- `abstract_inverted_index.return` (1.9%)
- `abstract_inverted_index.returns` (0.9%)
- `abstract_inverted_index.credit` (0.8%)
- `abstract_inverted_index.assets` (0.7%)

### Economics-Related
- `abstract_inverted_index.economic` (16.7%)
- `abstract_inverted_index.economy` (3.9%)
- `abstract_inverted_index.policy` (8.5%)
- `abstract_inverted_index.trade` (5.8%)
- `abstract_inverted_index.growth` (5.7%)
- `abstract_inverted_index.inflation` (3.7%)
- `abstract_inverted_index.fiscal` (3.1%)
- `abstract_inverted_index.GDP` (2.2%)
- `abstract_inverted_index.monetary` (1.2%)

### Firms/Corporate
- `abstract_inverted_index.firms` (3.5%)
- `abstract_inverted_index.business` (1.8%)
- `abstract_inverted_index.governance` (1.2%)
- `abstract_inverted_index.company` (1.0%)
- `abstract_inverted_index.corporate` (0.9%)
- `abstract_inverted_index.executive` (0.9%)

### Innovation/R&D
- `abstract_inverted_index.development` (3.4%)
- `abstract_inverted_index.technology` (1.5%)
- `abstract_inverted_index.innovation` (0.8%)
- `abstract_inverted_index.research` (0.4%)

### Entrepreneurship
- `abstract_inverted_index.venture` (0.2%)
- `abstract_inverted_index.ventures` (0.1%)

### Labor Market
- `abstract_inverted_index.workers` (1.3%)
- `abstract_inverted_index.job` (1.1%)
- `abstract_inverted_index.jobs` (1.1%)
- `abstract_inverted_index.employment` (0.7%)
- `abstract_inverted_index.wages` (0.7%)
- `abstract_inverted_index.wage` (0.5%)
- `abstract_inverted_index.skills` (0.6%)
- `abstract_inverted_index.workforce` (0.5%)

---

## RECOMMENDED COLUMN SUBSET FOR RESEARCH

### Essential Columns (High Priority)
1. **Identifiers**: `id`, `doi`, `title`
2. **Authors**: `authorships` (parse for author/institution/country)
3. **Citations**: `cited_by_count`, `citation_normalized_percentile.*`
4. **Topics**: `topics`, `primary_topic.*`
5. **Concepts**: `concepts`
6. **Dates**: `publication_year`, `publication_date`
7. **Venue**: `primary_location.source.*`
8. **Funding**: `grants`
9. **Geographic**: Extract from `authorships` and `locations` JSON

### Optional Columns (Medium Priority)
- `concepts` - For concept-based filtering
- `keywords` - If available
- `open_access.*` - For OA analysis
- `referenced_works` - For citation network analysis

### Low Priority (Can Skip)
- `abstract_inverted_index.*` - 12,986 columns, mostly noise
  - Only extract if you need specific keyword matching
  - Better to use full abstract text if available

---

## DATA EXTRACTION STRATEGY

### Option 1: Extract All Columns (Current Approach)
- **Pros**: Complete data, no information loss
- **Cons**: 13K columns, slow processing, many sparse columns
- **Use Case**: Exploratory analysis, unknown research questions

### Option 2: Extract Core + Parse Nested JSON (Recommended)
- **Pros**: Faster, focused on research-relevant data
- **Cons**: Need to parse nested JSON structures
- **Use Case**: Specific research questions, known data needs

**Recommended Core Columns** (~50-100 columns):
- All identifier/metadata columns (~20)
- All citation/impact columns (~10)
- All topic/concept columns (~5)
- All venue/publication columns (~15)
- All open access columns (~5)
- Parse nested JSON for:
  - `authorships` → author_id, author_name, institution_id, country_code
  - `topics` → topic_id, topic_name, score
  - `concepts` → concept_id, concept_name, score
  - `grants` → funder_id, funder_name, award_id
  - `locations` → country_code, source_id

### Option 3: Extract Only Research Domain Keywords
- Filter `abstract_inverted_index.*` to only finance/economics keywords
- Reduces from 12,986 to ~100-200 columns
- **Use Case**: Text analysis focused on specific terms

---

## NEXT STEPS

1. **Review this analysis** and identify which columns you need
2. **Modify extraction script** to:
   - Use predefined schema (faster)
   - Optionally filter to only needed columns
   - Parse nested JSON structures (authorships, topics, etc.)
3. **Test extraction** on a sample batch
4. **Verify data quality** and column coverage

---

## STATISTICS

- **Total columns discovered**: 13,083
- **Core research columns** (>=50% coverage): 87
- **High-value column groups**: 
  - Authorships: 1 column (100%)
  - Institutions: 11 columns
  - Citations: 11 columns
  - Topics: 14 columns
  - Concepts: 4 columns
  - Funding: 30 columns
  - Geographic: 68 columns
  - Publication Info: 43 columns

- **Abstract inverted index**: 12,986 columns (creates most of the column explosion)
