# Data Combination Plan: ArXiv + OpenAlex

## Overview
This document outlines the comprehensive plan to combine ArXiv and OpenAlex datasets to create a unified paper dataset where each paper has:
- Abstract (required)
- Full author affiliation information
- Citation data
- Other important metadata

## Data Sources Summary

### ArXiv Datasets (Same Schema, Different Coverage)
1. `arxiv_complete_kaggle.parquet` - 3.6M records, 1988-2025 (PRIMARY)
2. `arxiv_kaggle.parquet` - 2.6M records, 1988-2025
3. `claude_arxiv_complete.parquet` - 1.8M records, 1990-2025
4. `arxiv_2021_2025.parquet` - 807K records, 2021-2025
5. `claude_arxiv.parquet` - 1M records, 1990-2021

### OpenAlex Datasets
1. `openalex_claude_ai_papers.parquet` - 17M records, AI-focused papers
2. `openalex_claude_arxiv_index.parquet` - 2.7M records, linking table with full JSON
3. Flattened batches - 2006 files with complete OpenAlex data (1462 columns each)

### Institution/Affiliation Datasets
1. `arxiv_paper_institutions.parquet` - 2.2M records (has both arxiv_id and openalex_id)
2. `arxiv_affiliations_from_authors.parquet` - 2.6M records (arxiv-only)
3. `paper_institutions.parquet` - 2.7M records (OpenAlex-only)

### Linking/Match Datasets
1. `openalex_claude_arxiv_index.parquet` - 2.7M records (PRIMARY linking source)
2. `claude_arxiv_openalex_matches.parquet` - 37 records (small sample)
3. `claude_arxiv_openalex_linktable.parquet` - Check if not empty

---

## Phase 1: Data Cleanup and Validation

### 1.1 Delete Empty Files
- **Action**: Scan all parquet files in the directories
- **Check**: Files with 0 rows or 0 columns
- **Action**: Delete empty files immediately
- **Log**: Record all deleted files for reference

### 1.2 Validate File Integrity
- **Check**: Each file can be read successfully
- **Verify**: Required linking columns exist (`arxiv_id`, `openalex_id`, `doi`)
- **Identify**: Corrupted or unreadable files

---

## Phase 2: Consolidate Same-Source Data

### 2.1 Merge ArXiv Datasets

**Priority Order** (use largest/most complete first):
1. `arxiv_complete_kaggle.parquet` (3.6M records, 1988-2025) - **PRIMARY**
2. `arxiv_kaggle.parquet` (2.6M records, 1988-2025)
3. `claude_arxiv_complete.parquet` (1.8M records, 1990-2025)
4. `arxiv_2021_2025.parquet` (807K records, 2021-2025)
5. `claude_arxiv.parquet` (1M records, 1990-2021)

**Merge Strategy**:
- Use `arxiv_id` as primary key
- Keep most recent `harvest_date` for duplicates
- Fill missing values from other sources when available
- Standardize `arxiv_id` format (handle versioned vs non-versioned)
- **Output**: `merged_arxiv_complete.parquet`

### 2.2 Consolidate OpenAlex Data

**Sources**:
1. `openalex_claude_ai_papers.parquet` (17M records, AI-focused)
2. Flattened batches (2006 files, full OpenAlex data with 1462 columns)

**Strategy**:
- Process all 2006 flattened batch files
- Extract key columns: `openalex_id`, `doi`, `title`, `abstract`, `authorships_*`, `cited_by_count`, `publication_date`, etc.
- Merge with `openalex_claude_ai_papers.parquet` using `openalex_id`
- Handle duplicates (keep most complete record)
- **Output**: `merged_openalex_complete.parquet`

### 2.3 Consolidate Institution/Affiliation Data

**Sources**:
1. `arxiv_paper_institutions.parquet` (2.2M records, has both `arxiv_id` and `openalex_id`)
2. `arxiv_affiliations_from_authors.parquet` (2.6M records, arxiv-only)
3. `paper_institutions.parquet` (2.7M records, OpenAlex-only)

**Strategy**:
- Use `arxiv_paper_institutions.parquet` as base (has both IDs)
- Supplement with `arxiv_affiliations_from_authors.parquet` for papers missing in base
- Supplement with `paper_institutions.parquet` for OpenAlex-only papers
- **Output**: `merged_institutions_complete.parquet`

---

## Phase 3: Link ArXiv and OpenAlex

### 3.1 Build Linking Table

**Primary Linking Sources** (priority order):
1. `openalex_claude_arxiv_index.parquet` (2.7M records, has `arxiv_id`, `openalex_id`, `doi`)
2. `arxiv_paper_institutions.parquet` (2.2M records, has both IDs)
3. `claude_arxiv_openalex_matches.parquet` (37 records, small sample)
4. `claude_arxiv_openalex_linktable.parquet` (check if not empty)

**Linking Strategy**:
- **Primary**: `arxiv_id` → `openalex_id` (from `openalex_claude_arxiv_index.parquet`)
- **Secondary**: `doi` matching (normalize DOI format first)
- **Tertiary**: Title fuzzy matching (if needed)
- Handle one-to-many relationships (multiple OpenAlex works per ArXiv paper)
- **Output**: `arxiv_openalex_linking.parquet` with columns: `arxiv_id`, `openalex_id`, `doi`, `link_method`, `link_confidence`

### 3.2 Extract Abstract from OpenAlex

- **Source**: `openalex_claude_arxiv_index.parquet` has `json` field with full OpenAlex work data
- **Action**: Extract `abstract` from JSON field
- **Also**: Extract from flattened batches if available
- **Output**: `openalex_abstracts.parquet` with `openalex_id`, `abstract`

### 3.3 Extract Author Affiliations from OpenAlex

- **Source**: Flattened batches have `authorships_*` columns (up to 50+ authors)
- **Extract**: `author_display_name`, `author_id`, `institution_id`, `institution_name`, `institution_ror`, `is_corresponding`, `author_position`
- **Also**: Extract from `openalex_claude_arxiv_index.parquet` JSON field
- **Output**: `openalex_author_affiliations.parquet` with one row per author-paper-institution

---

## Phase 4: Create Unified Paper Dataset

### 4.1 Start with ArXiv Base
- Use `merged_arxiv_complete.parquet` as base
- Ensure all papers have `arxiv_id`, `title`, `abstract` (from ArXiv)
- Filter out papers without abstract

### 4.2 Enrich with OpenAlex Data
- Left join with `arxiv_openalex_linking.parquet` to get `openalex_id`
- Left join with `merged_openalex_complete.parquet` to get:
  - OpenAlex abstract (if ArXiv abstract missing)
  - Citation counts (`cited_by_count`)
  - Additional metadata
- **Priority**: Keep ArXiv abstract if available, use OpenAlex as fallback

### 4.3 Add Author Affiliations
- Join with `merged_institutions_complete.parquet` on `arxiv_id` or `openalex_id`
- For each paper, create:
  - `authors_list`: Array/list of all author names
  - `affiliations_list`: Array/list of all institution names
  - `corresponding_author`: Corresponding author name
  - `corresponding_institution`: Corresponding institution name
  - `author_affiliation_details`: Structured data with author-institution relationships

### 4.4 Add Citation Data
- From OpenAlex: `cited_by_count`, `referenced_works_count`
- From ArXiv: None (ArXiv doesn't have citations)
- Create citation metrics columns

### 4.5 Final Quality Checks
- Ensure each paper has:
  - ✅ Abstract (from ArXiv or OpenAlex)
  - ✅ At least one author
  - ✅ At least one affiliation (if available)
  - ✅ Publication date
  - ✅ Title
- Filter out papers missing required fields
- **Output**: `unified_papers_with_affiliations.parquet`

---

## Phase 5: Handle Flattened Batch Files

### 5.1 Process All 2006 Batches
- **Scan**: Directory `data/processed/publication/arxiv_flattened_batches/`
- **Process**: Each `batch_XXXXX_flatten.parquet` file
- **Extract Key Columns**:
  - Paper metadata: `openalex_id`, `arxiv_id`, `doi`, `title`, `abstract`, `publication_date`
  - Citations: `cited_by_count`, `referenced_works_count`
  - Authors: All `authorships_*` columns
  - Institutions: Extract from `authorships_*_institutions` columns

### 5.2 Merge Batch Data
- Combine all batch files into single dataset
- Deduplicate by `openalex_id`
- Merge with main unified dataset

---

## Phase 6: Final Output Structure

### Target Schema for `unified_papers_with_affiliations.parquet`

#### Core Paper Fields
- `paper_id` (unique identifier, prefer `arxiv_id`, fallback to `openalex_id`)
- `arxiv_id`
- `openalex_id`
- `doi`
- `title`
- `abstract` (required)
- `publication_date`
- `publication_year`

#### Author & Affiliation Fields
- `num_authors` (count)
- `authors_list` (array of author names)
- `author_ids_list` (array of OpenAlex author IDs)
- `affiliations_list` (array of institution names)
- `institution_ids_list` (array of OpenAlex institution IDs)
- `corresponding_author`
- `corresponding_institution`
- `author_affiliation_details` (structured JSON/array with full relationships)

#### Citation Fields
- `cited_by_count`
- `referenced_works_count`

#### Metadata Fields
- `source` (arxiv, openalex, or both)
- `has_abstract` (boolean)
- `has_affiliations` (boolean)
- `link_method` (how arxiv/openalex were linked)

---

## Implementation Order

1. **Phase 1**: Cleanup (delete empty files)
2. **Phase 2.1**: Merge ArXiv datasets
3. **Phase 3.1**: Build linking table
4. **Phase 3.2-3.3**: Extract abstract and affiliations from OpenAlex
5. **Phase 2.2**: Consolidate OpenAlex data (including batch processing)
6. **Phase 2.3**: Consolidate institution data
7. **Phase 4**: Create unified dataset
8. **Phase 5**: Process and merge batch files
9. **Phase 6**: Final quality checks and output

---

## Key Considerations

### Data Quality
- **ArXiv ID format**: Standardize (remove versions, handle old vs new format)
- **DOI normalization**: Remove `https://doi.org/` prefix, handle case sensitivity
- **One-to-many relationships**: Handle multiple OpenAlex works per ArXiv paper
- **Deduplication**: Use most complete record when duplicates exist
- **Missing data**: Fill from best available source, mark data quality flags

### Technical Considerations
- **Memory management**: Process large files in chunks
- **Performance**: Use efficient join strategies, consider indexing
- **Error handling**: Robust error handling for file reading/writing
- **Logging**: Comprehensive logging for each phase
- **Progress tracking**: Monitor progress for batch processing (2006 files)

### Data Validation
- Verify linking quality (check match rates)
- Validate abstract coverage (percentage with abstracts)
- Validate affiliation coverage (percentage with affiliations)
- Check for data inconsistencies

---

## Expected Outputs

### Intermediate Files
1. `merged_arxiv_complete.parquet` - Consolidated ArXiv data
2. `merged_openalex_complete.parquet` - Consolidated OpenAlex data
3. `merged_institutions_complete.parquet` - Consolidated institution data
4. `arxiv_openalex_linking.parquet` - Linking table
5. `openalex_abstracts.parquet` - Extracted abstracts
6. `openalex_author_affiliations.parquet` - Extracted author affiliations

### Final Output
- `unified_papers_with_affiliations.parquet` - Complete unified dataset

---

## Success Criteria

- ✅ All papers have abstracts (from ArXiv or OpenAlex)
- ✅ All papers have at least one author
- ✅ Maximum coverage of papers with affiliation information
- ✅ All papers have citation data (where available)
- ✅ Clean, deduplicated dataset
- ✅ Proper linking between ArXiv and OpenAlex records

---

## Notes

- The flattened batch files contain the most complete OpenAlex data (1462 columns)
- The `openalex_claude_arxiv_index.parquet` JSON field contains full work data that can be extracted
- Some files may be empty and should be deleted during Phase 1
- The linking process is critical - use multiple methods to maximize coverage
- Consider processing batches in parallel for efficiency
