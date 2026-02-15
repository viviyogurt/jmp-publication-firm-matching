# Publication-Firm Matching Plan (Redesigned with Rich Data Utilization)

## Overview

This plan leverages ALL rich information from `institutions_all.jsonl.gz` to significantly improve matching accuracy and coverage.

## Rich Data Available

### Key Fields in Raw Institution Data:
1. **ids Dictionary**:
   - `wikipedia` - Wikipedia article URL
   - `wikidata` - Wikidata entity URL (structured data!)
   - `ror` - Research Organization Registry
   - `grid` - GRID identifier

2. **homepage_url** - Institution website

3. **geo**:
   - `city` - City name
   - `region` - State/province
   - `country_code` - ISO country code
   - `latitude`, `longitude` - Precise coordinates

4. **associated_institutions** - Parent/child/sibling relationships (CRITICAL!)

5. **display_name_alternatives** - Alternative names

6. **display_name_acronyms** - Known acronyms

7. **type** - Institution type (company, education, healthcare, etc.)

8. **roles** - Role the institution plays

---

## Stage 0: Data Preparation & Enrichment

**Goal**: Create an enriched institution table with all rich fields extracted.

### Tasks:
1. Load `institutions_all.jsonl.gz`
2. Extract and flatten rich fields:
   - Extract Wikipedia/Wikidata/ROR/GRID from `ids` dict
   - Extract city/region/lat/long from `geo`
   - Flatten `associated_institutions` relationships
   - Collect all alternative names and acronyms
3. Add to existing `publication_institutions_master.parquet`

### Output:
- `publication_institutions_enriched.parquet` with columns:
  - institution_id, display_name, normalized_name
  - wikipedia_url, wikidata_url, ror_url, grid_id
  - homepage_url, homepage_domain
  - geo_city, geo_region, geo_country_code
  - geo_latitude, geo_longitude
  - alternative_names (list)
  - acronyms (list)
  - parent_institutions (list)
  - child_institutions (list)
  - institution_type
  - paper_count

---

## Stage 1: Exact & High-Confidence Matching (Enhanced)

**Goal**: Match institutions with 100% accuracy using exact matching and structured data.

### Matching Strategies (Priority Order):

#### 1.1 Wikipedia/Wikidata Company Match (NEW!)
**Confidence**: 0.98-0.99
- Extract company name from Wikipedia/Wikidata URL
- Parse Wikipedia page for company information
- Match to firm ticker/name
- **Example**: `wikipedia.org/wiki/Google` → Match to GOOGL

#### 1.2 Homepage Domain Match (Enhanced)
**Confidence**: 0.97
- Extract domain from `homepage_url`
- Match to firm `weburl` domain
- **Example**: `google.com` → Match to GOOGL

#### 1.3 Parent Institution Match (NEW!)
**Confidence**: 0.97
- Use `associated_institutions` to find parent companies
- Match parent institution to firms, then cascade to children
- **Example**: `Google DeepMind` → Parent `Google` → Match to GOOGL

#### 1.4 ROR-ID Cross-Reference (NEW!)
**Confidence**: 0.96
- Cross-reference ROR database with firm data
- Match ROR organization IDs to firm identifiers

#### 1.5 Alternative Names & Acronyms (Enhanced)
**Confidence**: 0.95
- Use `display_name_alternatives` (not just in normalized name)
- Use `display_name_acronyms` to match firm tickers
- **Example**: `IBM` acronym → Match to IBM

#### 1.6 Exact Name Match
**Confidence**: 0.98
- Exact match on cleaned name (existing)

#### 1.7 Subsidiary Name Matching (Enhanced)
**Confidence**: 0.94-0.96
- Match patterns: "Company Name [Country]" → "Company Name"
- Use geo information to validate country when needed
- **Example**: `Microsoft Research Asia` → Microsoft

#### 1.8 GRID Cross-Reference (NEW!)
**Confidence**: 0.95
- Match GRID IDs to known firm-institution mappings

### Target:
- 8,000-12,000 institutions matched
- 100% accuracy on exact matches

---

## Stage 2: Intelligent Matching (Redesigned)

**Goal**: Match remaining institutions using intelligent cross-validation with rich data.

**REMOVED**: Old fuzzy string matching (too many false positives)

### New Strategy: Multi-Factor Scoring System

#### 2.1 Geographic Proximity Matching (NEW!)
**Weight**: 15%
- Use `latitude`, `longitude` for precise location matching
- Match institutions to firms within 50km
- **Validation**: Institutions and firms in same metro area

#### 2.2 Name Similarity + Context (Enhanced)
**Weight**: 25%
- Jaro-Winkler similarity on multiple name variants
- **NEW**: Only match if institution_type = 'company'
- **NEW**: Boost score if both are companies (not education/healthcare)
- **Threshold**: 0.90 minimum similarity

#### 2.3 URL Domain Similarity (Enhanced)
**Weight**: 20%
- Extract domains from both `homepage_url` and firm `weburl`
- Check for:
  - Exact domain match
  - Subdomain relationship (e.g., `research.google.com` → `google.com`)
  - Same organization (check WHOIS/domain ownership if possible)

#### 2.4 Wikipedia/Wikidata Entity Resolution (NEW!)
**Weight**: 25%
- Query Wikidata API for institution and firm
- Check if they resolve to same entity
- Check for "owned by" / "subsidiary of" relationships
- **Example**: Wikidata shows `Google DeepMind` owned by `Alphabet Inc`

#### 2.5 Parent-Child Cascade (Enhanced)
**Weight**: 15%
- If institution has parent company in OpenAlex
- Check if parent matches to firm
- Cascade match to all children
- **Example**: Match `Amazon Web Services` via parent `Amazon`

### Confidence Scoring:
```
Base Score = Sum(weighted factors) / Sum(weights)
Final Confidence = Base Score × Validation_Multiplier

Validation_Multipliers:
- Type match (both companies): ×1.05
- Country match: ×1.02
- Wikipedia entity match: ×1.10
- Homepage domain match: ×1.08
- Parent relationship confirmed: ×1.15

Minimum Confidence: 0.94 (raised from 0.90)
```

### Target:
- 3,000-5,000 additional institutions matched
- 90%+ accuracy (significant improvement from old Stage 2)

---

## Stage 3: Manual & Knowledge-Based Matching (Enhanced)

**Goal**: Handle edge cases and add domain knowledge.

### 3.1 Manual Mapping Rules (Enhanced)
- Add more comprehensive mappings for:
  - Tech giants (Google, Microsoft, Amazon, Meta, Apple)
  - Conglomerates with many subsidiaries
  - Company rebrands (Facebook → Meta)
  - M&A integration (acquired companies)

### 3.2 Wikipedia-Derived Mappings (NEW!)
- Parse Wikipedia "List of [company] subsidiaries" pages
- Extract subsidiary-parent relationships
- **Example**: List of Google subsidiaries → Match all to GOOGL

### 3.3 Wikidata Relationship Extraction (NEW!)
- Query Wikidata API for:
  - `owned by` (P127)
  - `subsidiary` (P355)
  - `parent organization` (P749)
- Build firm hierarchy graph
- Match all nodes in graph to firm

### 3.4 Industry-Specific Rules
- Healthcare: Match hospitals/clinics to healthcare firms
- Finance: Match banks to financial firms
- Tech: Match research labs to tech firms
- Manufacturing: Match factories to industrial firms

### Target:
- 500-1,000 additional high-quality matches

---

## Stage 4: Quality Validation & Filtering (NEW!)

**Goal**: Systematic validation to remove false positives.

### 4.1 Cross-Validation Checks
- **Type consistency**: Company institutions → Company firms
- **Size validation**: Institution paper count vs firm size
- **Geographic plausibility**: Distance between institution and firm HQ
- **Industry alignment**: Institution topics vs firm industry (if available)

### 4.2 Reverse Validation
- For each match, check if firm name appears in institution papers
- Check if firm ticker appears in institution acknowledgments

### 4.3 Manual Review Sample
- Sample 500 low-confidence matches (0.94-0.95)
- Sample 100 high-confidence matches (0.98+)
- Manual verification to estimate precision

### 4.4 Outlier Detection
- Flag matches with:
  - High confidence but low paper count (<10)
  - Country distance >5000km
  - Type mismatch (company institution → education firm)
  - Unusual similarity patterns (short acronyms, common words)

---

## Stage 5: Deduplication & Ranking

**Goal**: Create final unique matches with best match per institution.

### 5.1 Duplicate Resolution
- One institution → Multiple firms: Keep highest confidence
- Same confidence: Use tiebreakers:
  1. Wikipedia/Wikidata match
  2. Homepage domain match
  3. Parent relationship
  4. Geographic proximity
  5. Name similarity

### 5.2 Match Ranking
- Rank all matches by:
  1. Confidence score
  2. Match stage priority (1 > 2 > 3)
  3. Validation strength

---

## Expected Results (Improved)

### Coverage:
- **Stage 1**: 10,000-12,000 institutions (60-65%)
- **Stage 2**: 3,000-5,000 institutions (18-25%)
- **Stage 3**: 500-1,000 institutions (3-5%)
- **Total**: 14,000-16,000 institutions (80-90% coverage)

### Accuracy:
- **Stage 1**: ~100% (exact matching)
- **Stage 2**: ~90-95% (intelligent matching)
- **Stage 3**: ~99% (knowledge-based)

### Improvement from Previous:
- **Coverage**: 55% → 85% (+30 percentage points)
- **Accuracy**: Stage 2 false positives dramatically reduced
- **Multi-national matching**: Properly handled via parent relationships
- **Rich data utilization**: Full use of Wikipedia, Wikidata, URLs, geo

---

## Implementation Priority

### Phase 1 (Immediate):
1. ✅ Stage 0: Data enrichment (extract rich fields)
2. ✅ Stage 1: Enhanced exact matching (use Wikipedia, URLs, parents)
3. ✅ Stage 3: Manual mappings + Wikipedia extraction

### Phase 2 (After Phase 1):
4. ✅ Stage 2: Intelligent matching (multi-factor scoring)
5. ✅ Stage 4: Quality validation
6. ✅ Stage 5: Deduplication & final output

---

## Key Technical Changes

### 1. Wikipedia/Wikidata Integration
- Use Wikipedia API to extract company information
- Use Wikidata Query Service (SPARQL) for relationship queries
- Cache results to avoid rate limits

### 2. Homepage URL Processing
- Extract domain using proper URL parsing
- Handle subdomains and country-specific domains
- WHOIS lookup for domain ownership verification

### 3. Parent-Child Relationship Graph
- Build directed graph from `associated_institutions`
- Traverse graph to find firm connections
- Handle multi-level hierarchies

### 4. Geographic Distance Calculation
- Use Haversine formula for distance between lat/long points
- Implement fast spatial indexing (KD-tree) for proximity queries

### 5. Type-Based Filtering
- Only match company-type institutions to firms
- Filter out education, healthcare, government institutions
- Reduce false positives by focusing on relevant types

---

## Files to Create/Modify

### New Files:
- `src/01_data_construction/extract_and_enrich_institutions.py` - Extract rich fields
- `src/01_data_construction/build_parent_child_graph.py` - Build relationship graph
- `src/02_linking/match_wikipedia_companies.py` - Wikipedia matching
- `src/02_linking/match_parent_institutions.py` - Parent cascade matching
- `src/02_linking/match_geographic_proximity.py` - Geo proximity matching
- `src/02_linking/match_wikidata_entities.py` - Wikidata entity resolution
- `src/02_linking/validate_matches_quality.py` - Quality validation

### Modified Files:
- All Stage 1, 2, 3 scripts to use enriched data
- Combine script to handle new schema
- Validation script to check quality metrics

---

## Data Schema Changes

### Enriched Institutions Table:
```python
{
    "institution_id": str,
    "display_name": str,
    "normalized_name": str,

    # Rich identifiers
    "wikipedia_url": str,
    "wikidata_url": str,
    "ror_url": str,
    "grid_id": str,

    # URL & Domain
    "homepage_url": str,
    "homepage_domain": str,

    # Geography
    "geo_city": str,
    "geo_region": str,
    "geo_country_code": str,
    "geo_latitude": float,
    "geo_longitude": float,

    # Names
    "alternative_names": List[str],
    "acronyms": List[str],

    # Relationships
    "parent_institution_ids": List[str],
    "child_institution_ids": List[str],
    "associated_institution_ids": List[str],

    # Classification
    "institution_type": str,
    "is_company": bool,

    # Metrics
    "paper_count": int,
}
```

---

## Success Metrics

### Quantitative:
- Total institutions matched: >14,000 (85%+)
- Estimated precision: >95%
- Multi-national company coverage: >95%

### Qualitative:
- Google/Alphabet: All 6+ subsidiaries matched correctly
- Microsoft: All 18+ international subsidiaries matched
- Amazon: All research labs matched
- Meta: All international offices matched
- Apple: All international subsidiaries matched

### Validation:
- Manual review sample: >98% precision on Stage 1, >90% on Stage 2
- False positive rate: <5%
- Country restrictions: ZERO (all removed)
