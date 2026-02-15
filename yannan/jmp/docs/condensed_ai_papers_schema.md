# Condensed AI Papers Dataset Schema

## Overview

The condensed dataset reduces the sparse, 1000+ column flattened files to a manageable **~60 columns** suitable for empirical analysis.

**File**: `data/processed/publication/ai_papers_condensed.parquet`

## Schema (60 columns)

### 1. Core Identifiers (4 columns)
- `paper_id` (str) - Composite: prefer arxiv_id, fallback to openalex_id
- `openalex_id` (str) - OpenAlex work ID (primary identifier)
- `arxiv_id` (str, nullable) - ArXiv identifier if available (may be 100% null if not linked)
- `doi` (str, nullable) - Digital Object Identifier

### 2. Basic Metadata (10 columns)
- `title` (str) - Paper title
- `abstract` (str, nullable) - Abstract text (~27% null)
- `publication_date` (str) - Full publication date
- `publication_year` (int) - Year of publication
- `type` (str) - Work type
- `type_crossref` (str, nullable) - Crossref type
- `type_id` (str) - Type ID
- `work_type` (str) - Work type classification
- `url` (str, nullable) - Paper URL
- `language` (str, nullable) - Language code

### 3. Counts (7 columns)
- `authors_count` (int) - Number of authors
- `topics_count` (int) - Number of topics
- `concepts_count` (int) - Number of concepts
- `keywords_count` (int) - Number of keywords
- `locations_count` (int) - Number of locations
- `referenced_works_count` (int) - Number of references
- `cited_by_count` (int) - Number of citations

### 4. AI Category Flags (6 columns)
- `is_computer_vision` (bool)
- `is_deep_learning` (bool)
- `is_llm` (bool)
- `is_machine_learning` (bool)
- `is_nlp` (bool)
- `is_reinforcement_learning` (bool)

### 5. Venue Information (5 columns)
- `venue_name` (str, nullable) - Journal/venue name (~23% null)
- `venue_id` (str, nullable) - Venue ID
- `venue_type` (str, nullable) - Venue type
- `publisher` (str, nullable) - Publisher name (~37% null)
- `is_open_access` (bool) - Open access indicator

### 6. Authors & Affiliations (11 columns)

**Simple lists (one value per author):**
- `author_names` (list[str]) - Ordered list of author names
- `author_ids` (list[str]) - Ordered list of author OpenAlex IDs
- `author_positions` (list[int]) - Author position (1st, 2nd, etc.)
- `is_corresponding_author` (list[bool]) - Which authors are corresponding
- `author_primary_affiliations` (list[str]) - First institution per author
- `author_primary_affiliation_ids` (list[str]) - First institution ID per author
- `author_primary_affiliation_countries` (list[str]) - First institution country per author

**Nested lists (multiple institutions per author):**
- `author_affiliations` (list[list[str]]) - Nested: [author_0_institutions, author_1_institutions, ...]
- `author_affiliation_ids` (list[list[str]]) - Nested institution IDs
- `author_affiliation_countries` (list[list[str]]) - Nested country codes

### 7. Topics (4 columns)
- `topic_names` (list[str]) - All topic display names
- `topic_ids` (list[str]) - Corresponding topic IDs
- `primary_topic_name` (str, nullable) - Primary topic name (~2% null)
- `primary_topic_id` (str, nullable) - Primary topic ID

### 8. Concepts (2 columns)
- `concept_names` (list[str]) - All concept display names
- `concept_ids` (list[str]) - Corresponding concept IDs

### 9. Keywords (2 columns)
- `keyword_names` (list[str]) - All keyword display names
- `keyword_ids` (list[str], nullable) - Keyword IDs (~20% null)

### 10. Locations & SDGs (4 columns)
- `location_countries` (list[str]) - Country codes from all locations
- `location_sources` (list[str], nullable) - Source names (~23% null)
- `sdg_names` (list[str], nullable) - SDG names (~49% null)
- `sdg_ids` (list[str], nullable) - SDG IDs

### 11. Additional Metrics (5 columns)
- `citation_percentile` (float, nullable) - Normalized citation percentile (~2% null)
- `is_top_1_percent` (bool) - Top 1% citation indicator
- `is_top_10_percent` (bool) - Top 10% citation indicator
- `institutions_distinct_count` (int) - Number of unique institutions
- `countries_distinct_count` (int) - Number of unique countries
- `has_abstract` (bool) - Whether abstract is available

## Data Quality Notes

1. **Deduplication**: Papers are deduplicated by `paper_id` (keeps first occurrence)
2. **Filtering**: Papers missing `title` or both `publication_date` and `publication_year` are filtered out
3. **Null Rates**: 
   - `arxiv_id`: 100% null (not in OpenAlex AI papers dataset, requires separate linking)
   - `abstract`: ~27% null
   - `sdg_names`: ~49% null (not all papers have SDG assignments)
   - `venue_name`: ~23% null
4. **List Columns**: All list columns are empty lists `[]` rather than null when no data

## Usage Example

```python
import polars as pl

# Load condensed dataset
df = pl.read_parquet("data/processed/publication/ai_papers_condensed.parquet")

# Filter papers with abstracts
df_with_abstracts = df.filter(pl.col("has_abstract") == True)

# Filter by AI category
ml_papers = df.filter(pl.col("is_machine_learning") == True)

# Access author affiliations (nested lists)
# author_affiliations[i] = list of institutions for author i
first_author_institutions = df["author_affiliations"].list.first()

# Access primary affiliations (simpler)
first_author_primary_affiliation = df["author_primary_affiliations"].list.first()
```

## File Size

- **Sample (2 batch files, 153K papers)**: ~0.15 GB
- **Full dataset (189 batch files, ~18.9M papers)**: Estimated ~18-20 GB (much smaller than original 1000+ column files)

## Generation

Run the condensation script:
```bash
python src/01_data_construction/condense_ai_papers_dataset.py
```

Options:
- `--sample-size N`: Test on N batch files
- `--chunk-size N`: Process N files per chunk (default: 10)
- `--output-file PATH`: Custom output path
