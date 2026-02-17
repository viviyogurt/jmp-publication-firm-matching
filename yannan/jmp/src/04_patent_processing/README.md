# Patent Processing Implementation Guide

## Directory Structure

```
src/04_patent_processing/
├── README.md                    # This file
├── scripts/
│   ├── 01_verify_data.py       # Phase 1: Verify downloaded data
│   ├── 02_filter_ai_patents.py # Phase 2: Two-stage classification
│   ├── 03_match_firms.py       # Phase 3: Patent → Firm matching
│   ├── 04_create_panel.py      # Phase 4: Firm-year panel creation
│   ├── 05_translation_efficiency.py # Phase 5: Text similarity
│   ├── 06_innovation_index.py  # Phase 6: Innovation index
│   └── 07_validation.py        # Phase 7: Quality checks
├── utils/
│   ├── __init__.py
│   ├── classification.py       # Two-stage classification functions
│   ├── keyword_lists.py        # AI and strategic category keywords
│   ├── fuzzy_match.py          # Firm matching utilities
│   └── validation.py           # Data quality checks
└── requirements.txt            # Python dependencies
```

## Quick Start

### 1. Setup Environment

```bash
# Navigate to project directory
cd /path/to/jmp

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r src/04_patent_processing/requirements.txt
```

### 2. Verify Data

```bash
# Check if PatentsView data is downloaded
ls -lh /Data/patent/raw/

# Expected files:
# - g_patent.tsv (~2 GB)
# - g_patent_abstract.tsv (~1.6 GB)
# - g_cpc_current.tsv (~1 GB)
# - g_assignee_disambiguated.tsv (~200 MB)
# - g_patent_assignee.tsv (~500 MB)
```

### 3. Run Processing Pipeline

```bash
# Phase 1: Verify data
python src/04_patent_processing/scripts/01_verify_data.py

# Phase 2: Filter AI patents (two-stage classification)
python src/04_patent_processing/scripts/02_filter_ai_patents.py

# Phase 3: Match patents to firms
python src/04_patent_processing/scripts/03_match_firms.py

# Phase 4: Create firm-year panel
python src/04_patent_processing/scripts/04_create_panel.py

# Phase 5: Translation efficiency analysis
python src/04_patent_processing/scripts/05_translation_efficiency.py

# Phase 6: Innovation index construction
python src/04_patent_processing/scripts/06_innovation_index.py

# Phase 7: Validation
python src/04_patent_processing/scripts/07_validation.py
```

## Implementation Details

### Phase 1: Verify Data

**Script:** `01_verify_data.py`

**Actions:**
- Check files exist and are readable
- Validate file formats (TSV, UTF-8)
- Count records
- Create directory structure

**Output:** Verification report

### Phase 2: Filter AI Patents

**Script:** `02_filter_ai_patents.py`

**Actions:**
- Load patent metadata (2010-2024)
- Load patent abstracts
- **Stage 1:** Identify AI patents (CPC + text union)
- **Stage 2:** Strategic classification (Infrastructure/Algorithm/Application)
- Filter to utility patents
- Save processed dataset

**Output:** `/Data/patent/processed/ai_patents_2010_2024.parquet`

### Phase 3: Match Firms

**Script:** `03_match_firms.py`

**Actions:**
- Load assignee data (disambiguated)
- Load Compustat firm data
- Fuzzy matching (assignee → GVKEY)
- Create patent → firm mapping
- Merge with AI patents

**Output:** `/Data/patent/processed/firm_patent_panel.parquet`

### Phase 4: Create Panel

**Script:** `04_create_panel.py`

**Actions:**
- Create firm-year aggregates
- Extract patent grant dates
- Merge with Compustat (market cap, firm values)
- Create event study dataset

**Outputs:**
- `/Data/patent/processed/firm_year_panel.parquet`
- `/Data/patent/processed/patent_event_dates.parquet`
- `/Data/patent/outputs/event_study_dates.csv`

### Phase 5: Translation Efficiency

**Script:** `05_translation_efficiency.py`

**Actions:**
- Load publication data (OpenAlex)
- Match publications to firms
- Compute embeddings (SciBERT/PatBERT)
- Calculate cosine similarity
- Compute translation efficiency metrics

**Output:** `/Data/patent/outputs/translation_metrics.parquet`

### Phase 6: Innovation Index

**Script:** `06_innovation_index.py`

**Actions:**
- Load market cap data
- Calculate value weights
- Construct AI innovation index
- Compare to KPSS general innovation index

**Output:** `/Data/patent/outputs/ai_innovation_index.csv`

### Phase 7: Validation

**Script:** `07_validation.py`

**Actions:**
- Sample validation (AI patent ID)
- Strategic classification validation
- Temporal distribution check
- Firm matching quality check
- Event study validation
- Cross-validation with publication data
- Data integrity checks

**Output:** `/Data/patent/outputs/validation_report.md`

## Utility Functions

### classification.py

```python
def classify_ai_patent_cpc(cpc_codes):
    """Check if patent is AI-related based on CPC codes."""

def classify_ai_patent_text(abstract_text):
    """Check if patent is AI-related based on text."""

def classify_strategic_category(abstract_text):
    """Classify AI patent into Infrastructure/Algorithm/Application."""
```

### keyword_lists.py

```python
AI_KEYWORDS = [...]  # AI identification keywords
INFRA_KEYWORDS = [...]  # Infrastructure classification
ALGO_KEYWORDS = [...]  # Algorithm classification
APP_KEYWORDS = [...]  # Application classification
```

### fuzzy_match.py

```python
def match_assignee_to_firm(assignee_name, firm_names, threshold=85):
    """Fuzzy match assignee name to Compustat firm name."""
```

### validation.py

```python
def validate_ai_patents(sample_df):
    """Manual validation of AI patent identification."""

def validate_classification(sample_df):
    """Manual validation of strategic classification."""
```

## Expected Outputs

### AI Patent Identification
- **Count:** 30,000-60,000 AI utility patents (2010-2024)
- **Abstract coverage:** >90%
- **True positive rate:** >90%

### Strategic Classification
- **Infrastructure:** 10-20%
- **Algorithm:** 40-50%
- **Application:** 20-30%
- **Unknown:** 10-20%
- **Accuracy:** >80%

### Firm Matching
- **Matched patents:** 15,000-40,000
- **Unique firms:** 1,500-3,000
- **Matching accuracy:** >85%

### Panel Creation
- **Firm-year observations:** 5,000-15,000
- **Year range:** 2010-2024

## Troubleshooting

### Out of Memory Errors

**Problem:** RAM insufficient for large TSV files

**Solutions:**
```python
# Use Polars lazy evaluation
import polars as pl
patents = pl.scan_csv('/Data/patent/raw/g_patent.tsv', separator='\tab')

# Process in yearly chunks
for year in range(2010, 2025):
    year_data = patents.filter(pl.col('patent_date').dt.year() == year).collect()
```

### Slow Fuzzy Matching

**Problem:** Fuzzy matching is too slow

**Solutions:**
```python
# Use exact matching where possible
exact_matches = patent_orgs.join(firms, left_on='assignee_name', right_on='conm')

# Parallel processing
from multiprocessing import Pool
with Pool(4) as p:
    results = p.map(match_function, assignee_list)
```

### Low Classification Accuracy

**Problem:** Strategic classification accuracy <80%

**Solutions:**
1. Refine keyword lists based on manual review
2. Use CPC-based classification as fallback
3. Consider LLM-based classification (GPT-4 API) for higher accuracy

## Notes

- **CRITICAL:** The new PatentsView format separates abstracts (g_patent_abstract.tsv) from metadata
- **KPSS alignment:** Follow KPSS methodology for data processing, NOT for analysis
- **Time period:** 2010-2024 (AI-focused, different from KPSS 1977-2014)
- **Your research:** Translation efficiency, strategic choice, complementarity analysis
