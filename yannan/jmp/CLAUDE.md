# JMP Publication Project - Claude Agent Guidelines

## Project Overview
This is an academic JMP (Job Market Paper) project studying firm innovation through publications and patents. All work must meet academic research standards with rigorous methodology and validation.

## Folder Structure Convention

```
jmp/
├── src/                          # Source code (organized by workflow stage)
│   ├── 01_data_construction/    # Data fetching, extraction, cleaning
│   ├── 02_linking/              # Entity-to-firm matching/linking
│   └── 03_analysis/             # Panel creation and analysis
├── data/
│   ├── raw/                     # Original downloaded data (NEVER modify)
│   │   ├── publication/         # OpenAlex, arXiv raw data
│   │   ├── compustat/          # CRSP/Compustat raw files
│   │   └── patents/            # USPTO patent raw data
│   ├── interim/                 # Intermediate processed data
│   │   ├── *master.parquet     # Master entity tables
│   │   ├── *standardized.parquet # Standardized firm data
│   │   └── manual_*.csv        # Manual mapping files
│   └── processed/               # Final outputs for analysis
│       ├── publication/         # Processed publication datasets
│       ├── linking/             # Matched entity-firm pairs
│       └── analysis/            # Firm-year panels, final datasets
├── logs/                        # ALL script logs (one per script)
├── output/
│   ├── tables/                  # CSV/TEX tables for paper
│   └── figures/                 # PNG/PDF figures for paper
└── docs/                        # Project documentation
```

## Strict Rules

### 1. File Placement Rules
- **NEW scripts in `src/`**: Must go into appropriate subfolder:
  - Data fetching/extraction → `01_data_construction/`
  - Entity matching/linking → `02_linking/`
  - Panel/analysis creation → `03_analysis/`
- **NEW outputs in `data/`**:
  - Intermediate data → `data/interim/`
  - Final matched data → `data/processed/linking/`
  - Final analysis data → `data/processed/analysis/`
- **ALL scripts MUST log** → `logs/{script_name}.log`
- **Tables/figures for paper** → `output/tables/` or `output/figures/`

### 2. Naming Conventions
- Scripts: `snake_case.py` with descriptive action verbs (e.g., `match_publications_to_firms_stage1.py`)
- Data files: `snake_case.parquet` or `snake_case.csv`
- Log files: Same as script name with `.log` extension

### 3. Code Standards

#### 3.1 Script Structure
```python
"""
Module Purpose: One-line description
Detailed description of methodology, following academic literature.
References: Author (Year) - Paper Title
"""

import polars as pl
from pathlib import Path
import logging

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
# Define paths using PROJECT_ROOT
# Setup logging

# ============================================================================
# Functions
# ============================================================================
# All functions must have docstrings

# ============================================================================
# Main
# ============================================================================
def main():
    # Implementation

if __name__ == "__main__":
    main()
```

#### 3.2 Required Elements
- **Logging**: Every script must log to `logs/{script_name}.log`
- **Progress indicators**: Log major steps with `[1/N]` prefixes
- **Error handling**: Validate input files exist before processing
- **Type hints**: Use on all function signatures

#### 3.3 Data Processing Standards
- Use **Polars** (not pandas) for all data processing
- Use **RapidFuzz** for fuzzy string matching (not fuzzywuzzy)
- All string matching must be **case-insensitive** after normalization
- Clean organization names using `standardize_compustat_for_patents.py` function
- Never modify files in `data/raw/`

### 4. Documentation Standards

#### 4.1 Script Documentation
Every script must start with a docstring containing:
- Purpose
- Input files with paths
- Output files with paths
- Methodology with academic citations
- Expected outputs/metrics

#### 4.2 Process Documentation
Create/update docs in `docs/` for:
- Major methodology decisions
- Validation results
- Data quality issues
- Matching strategy evolution

### 5. Output Standards

#### 5.1 Log Files
- Log file = script basename + `.log`
- Include timestamp, level, message
- Log progress: `[1/5] Loading data...`
- Log summary statistics at end

#### 5.2 Data Outputs
- Use **Parquet format** with `compression='snappy'`
- Include metadata in separate `.json` or `.md` files if needed
- Document schema in `docs/` if complex

#### 5.3 Tables and Figures
- Tables: CSV for data, TEX for paper
- Figures: PNG (600 DPI minimum)
- Include descriptive filenames (e.g., `fig1_big_tech_trends.png`)

### 6. Version Control and Automatic Commits

#### 6.1 Automatic Git Commits on Code Changes
**IMPORTANT:** Every time you edit or create code files, you MUST commit the changes to git.

**Using the `/commit` skill:**
```
/commit "message describing the change"
```

**Examples:**
- After editing a script: `/commit "Fix bug in ticker matching logic"`
- After creating new file: `/commit "Add optimized publication matching script"`
- After fixing issues: `/commit "Resolve timeout issue in Stage 2 matching"`

**When to commit:**
- ✅ After ANY code edit (Edit tool usage)
- ✅ After ANY file creation (Write tool usage)
- ✅ After fixing bugs or errors
- ✅ After completing features
- ❌ NOT needed for just reading files

**Commit message format:**
```
Short description (50 chars or less)

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

**Manual git commands (if /commit not available):**
```bash
git add path/to/file.py
git commit -m "Description of changes"
```

**Verification:**
```bash
git log --oneline -5  # Check recent commits
git status            # Check uncommitted changes
```

### 7. Research Rigor Requirements

#### 7.1 Validation
- **ALL matching requires validation**:
  - Stage 1 (exact): 100% accuracy expected
  - Stage 2 (fuzzy): Validate minimum 1,000 random samples
  - Stage 3 (manual): Document all manual mappings
- Create validation samples in `data/processed/linking/validation_sample_*.csv`
- Calculate and report accuracy metrics

#### 6.2 Replicability
- Every script must be **deterministic** (set seeds if using randomization)
- Document all parameters and thresholds
- Use version control for all code

#### 6.3 No Fluff / Academic Standards
- **No placeholders**: Every function must work
- **No mock data**: Use real data or explicitly label test data
- **No "TODO" comments in committed code**
- All claims must be **supported by data**
- Cite relevant literature in docstrings

#### 6.4 Performance
- Use lookup dictionaries for O(1) matching (not O(n) loops)
- Process in batches when memory constrained
- Log progress for operations >5 seconds

### 7. Specific to Publication-Firm Matching

Follow the methodology in `docs/paper_match_financials.md`:

```
Stage 0: Prepare institutions (EXISTS - prepare_publication_institutions.py)
Stage 1: Exact matches (TODO - match_publications_to_firms_stage1.py)
  - Exact name match (0.98)
  - ROR to firm (0.97)
  - Wikidata ticker (0.97)
  - URL domain (0.96)
  - Ticker in name (0.96)
  - Firm contained (0.95)
  - Abbreviation (0.95)
  - Alternative names (0.95)

Stage 2: Fuzzy matching (TODO - match_publications_to_firms_stage2.py)
  - Jaro-Winkler similarity ≥0.85
  - Cross-validation: country, business, location, URL
  - Confidence scoring with validation boosts
  - Filter: confidence ≥0.90

Stage 3: Manual mapping (TODO - match_publications_to_firms_stage3.py)
  - Name changes, subsidiaries, JVs
  - Manual mapping file for top 100 firms

Stage 4: Combine (TODO - combine_publication_matches.py)
  - Deduplicate keeping highest confidence
  - Stage priority: 1 > 2 > 3

Stage 5: Master table (TODO - create_publication_institution_firm_master.py)
  - Enrich with metadata
  - Calculate statistics

Stage 6: Firm-year panel (TODO - create_publication_firm_year_panel.py)
  - Aggregate to GVKEY-year level
  - Citation metrics per firm-year

Stage 7: Validation (TODO - validate_publication_matches.py)
  - 1,000 random sample validation
  - Accuracy report by stage
```

### 8. Quality Checklist Before Committing

- [ ] **Git commit created** (use `/commit` or `git commit` command)
- [ ] Script runs end-to-end without errors
- [ ] Log file created in `logs/`
- [ ] Output file in correct directory (`data/interim/` or `data/processed/`)
- [ ] Progress indicators logged
- [ ] Summary statistics logged
- [ ] Docstring complete with methodology
- [ ] No hardcoded paths (use `PROJECT_ROOT`)
- [ ] Type hints on all functions
- [ ] Validation performed (if matching script)
- [ ] Documentation updated in `docs/`

**IMPORTANT:** Git commit is MANDATORY after any code changes! Use:
```
/commit "Brief description of changes"
```

### 9. Existing Reference Code

Use these as templates (same patterns expected for publication matching):

**Patent Matching (COMPLETE):**
- `src/02_linking/match_patents_to_firms_stage1.py` - Exact matches
- `src/02_linking/match_patents_to_firms_stage2.py` - Fuzzy matches
- `src/02_linking/match_patents_to_firms_stage3.py` - Manual mappings
- `src/02_linking/validate_patent_matches.py` - Validation

**Data Preparation (COMPLETE):**
- `src/02_linking/prepare_publication_institutions.py` - Stage 0 done
- `src/02_linking/standardize_compustat_for_patents.py` - Firm standardization

**Analysis (REFERENCE):**
- `src/03_analysis/create_patent_firm_year_panel.py` - Panel creation pattern

## Common Patterns

### Reading/Writing Parquet
```python
# Always use PROJECT_ROOT
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"

# Read
df = pl.read_parquet(DATA_INTERIM / "filename.parquet")

# Write with compression
df.write_parquet(output_path, compression='snappy')
```

### Logging Setup
```python
LOGS_DIR = PROJECT_ROOT / "logs"
PROGRESS_LOG = LOGS_DIR / "script_name.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROGRESS_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
```

### Progress Logging
```python
logger.info("=" * 80)
logger.info("STAGE 1: DESCRIPTION")
logger.info("=" * 80)
logger.info("\n[1/5] Loading data...")
logger.info(f"  Loaded {len(df):,} rows")
```

### String Normalization (CRITICAL)
```python
# Use this function from standardize_compustat_for_patents.py
def clean_organization_name(name: str | None) -> str | None:
    if name is None or name == "":
        return None
    name = name.upper()
    suffixes_pattern = r'\b(INCORPORATED|CORPORATION|...)\b'
    name = re.sub(suffixes_pattern, '', name)
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name if name else None
```

## Final Reminders

1. **Academic rigor first**: Every claim needs validation
2. **No shortcuts**: Implement full methodology, not simplified versions
3. **Document everything**: If you make a decision, document why
4. **Test on subsets**: Before running on full dataset
5. **Log everything**: Replicability depends on complete logs
6. **Follow patent matching pattern**: It worked well (95.4% accuracy)
7. **Ask before breaking patterns**: If you need to deviate from established patterns, explain why
