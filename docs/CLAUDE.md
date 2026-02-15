# JMP Publication Project - Claude Agent Guidelines

## Project Overview
This is an academic JMP (Job Market Paper) project studying firm innovation through publications and patents. All work must meet academic research standards with rigorous methodology and validation.

## Folder Structure Convention

```
jmp/
├── README.md                    # Project overview (KEEP IN ROOT)
├── requirements.txt             # Python dependencies (KEEP IN ROOT)
├── commit.sh                    # Auto-commit tool (KEEP IN ROOT)
│
├── src/                         # Python source code (organized by workflow stage)
│   ├── 01_data_construction/    # Data fetching, extraction, cleaning
│   ├── 02_linking/              # Entity-to-firm matching/linking
│   └── 03_analysis/             # Panel creation and analysis
│
├── scripts/                     # Shell scripts (ALL .sh files except commit.sh)
│   ├── monitor_*.sh            # Progress monitoring scripts
│   ├── setup_*.sh              # Setup and installation scripts
│   └── [utility scripts]
│
├── docs/                        # Project documentation
│   ├── latex/                  # LaTeX paper files
│   │   ├── jmp_main.tex
│   │   ├── jmp_*_section.tex
│   │   └── references.bib
│   ├── *.md                    # All markdown documentation
│   └── [guides, reports, etc.]
│
├── data/
│   ├── raw/                     # Original downloaded data (NEVER modify)
│   │   ├── publication/         # OpenAlex, arXiv raw data
│   │   ├── compustat/          # CRSP/Compustat raw files
│   │   └── patents/            # USPTO patent raw data
│   ├── interim/                 # Intermediate processed data
│   │   ├── *master.parquet     # Master entity tables
│   │   ├── *standardized.parquet # Standardized firm data
│   │   ├── manual_*.csv        # Manual mapping files
│   │   └── [analysis results]
│   └── processed/               # Final outputs for analysis
│       ├── publication/         # Processed publication datasets
│       ├── linking/             # Matched entity-firm pairs
│       └── analysis/            # Firm-year panels, final datasets
│
├── logs/                        # ALL script logs (one per script)
├── output/                      # Generated output files
│   ├── tables/                  # CSV/TEX tables for paper
│   ├── figures/                 # PNG/PDF figures for paper
│   └── latex/                  # Generated LaTeX tables
```

**CRITICAL: ROOT DIRECTORY MUST STAY CLEAN**
- ONLY 4 files allowed in root: `README.md`, `commit.sh`, `requirements.txt`, `PROJECT_STRUCTURE.md`
- ALL other files MUST go into appropriate subdirectories
- NEVER create `.md`, `.txt`, `.py`, `.sh`, `.tex` files in root directory

## Strict Rules

### 1. File Placement Rules (STRICTLY ENFORCED)

#### Python Scripts (.py)
- **ALL** `.py` files → `src/{appropriate_subfolder}/`
  - Data fetching/extraction → `src/01_data_construction/`
  - Entity matching/linking → `src/02_linking/`
  - Panel/analysis creation → `src/03_analysis/`
- **NEVER** create `.py` files in root directory

#### Shell Scripts (.sh)
- **ALL** `.sh` files → `scripts/` (except `commit.sh` which stays in root)
- Monitor scripts → `scripts/monitor_*.sh`
- Setup scripts → `scripts/setup_*.sh`
- Utility scripts → `scripts/{name}.sh`
- **NEVER** create `.sh` files in root directory (except commit.sh)

#### Documentation Files
- **Markdown (.md)** → `docs/`
  - Guides → `docs/*_GUIDE.md`
  - Reports → `docs/*_REPORT.md`
  - Summaries → `docs/*_SUMMARY.md`
  - **NEVER** create `.md` files in root (except README.md)

- **LaTeX (.tex, .bib)** → `docs/latex/`
  - Main paper → `docs/latex/jmp_main.tex`
  - Sections → `docs/latex/jmp_*_section.tex`
  - Bibliography → `docs/latex/references.bib`
  - **NEVER** create `.tex` or `.bib` files in root

- **Text files (.txt)** → `docs/` (if documentation) or `data/` (if data)

#### Data Files
- **Raw data** → `data/raw/{source}/`
- **Intermediate data** → `data/interim/`
- **Processed data** → `data/processed/{category}/`
- **Manual mappings** → `data/interim/manual_*.csv`
- **Analysis results** → `data/interim/` (e.g., parquet_analysis_results.csv)

#### Log Files
- **ALL script logs** → `logs/{script_name}.log`
- Automatic: Configure scripts to log to `PROJECT_ROOT / "logs" / f"{script_name}.log"`

#### Output Files
- **Tables for paper** → `output/tables/`
- **Figures for paper** → `output/figures/`
- **Generated LaTeX** → `output/latex/`

#### Configuration Files
- **requirements.txt** → Root directory (KEEP HERE)
- **.gitignore** → Root directory (KEEP HERE)
- **setup/config scripts** → `scripts/`

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

#### 6.2 Backup and Recovery Strategy
**Daily workflow:**
1. **Start of day:** `git pull` (get latest changes)
2. **During work:** Commit after each change
3. **Before risky operations:** Create checkpoint
   ```bash
   git tag -a checkpoint-before-run -m "Checkpoint before running matching"
   ```
4. **End of day:** Push to GitHub
   ```bash
   git push
   git push origin --tags  # Push tags too
   ```

**Recovery commands:**
```bash
# Undo last commit (keep changes)
git reset --soft HEAD~1

# Restore file from previous version
git checkout HEAD~1 -- path/to/file.py

# See history of all operations
git reflog

# Restore from specific commit
git checkout abc1234
```

**For complete git workflow guide:** See `docs/git_workflow_best_practices.md`

---
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

## File Placement Checklist

**BEFORE creating ANY file, ask yourself:**

### For Python Scripts (.py)
- [ ] Does this go in `src/01_data_construction/` (data fetching/cleaning)?
- [ ] Does this go in `src/02_linking/` (entity matching)?
- [ ] Does this go in `src/03_analysis/` (panel creation/analysis)?
- [ ] **NEVER** create `.py` in root directory

### For Shell Scripts (.sh)
- [ ] Does this go in `scripts/`?
- [ ] Is this named descriptively (e.g., `monitor_*.sh`, `setup_*.sh`)?
- [ ] **NEVER** create `.sh` in root (except commit.sh)

### For Documentation (.md)
- [ ] Does this go in `docs/`?
- [ ] **NEVER** create `.md` in root (except README.md)

### For LaTeX Files (.tex, .bib)
- [ ] Does this go in `docs/latex/`?
- [ ] **NEVER** create `.tex` or `.bib` in root

### For Data Files
- [ ] Is this raw data? → `data/raw/{source}/`
- [ ] Is this intermediate? → `data/interim/`
- [ ] Is this processed? → `data/processed/{category}/`

### Quick Reference
```
File Type  | Location
-----------|------------------------------------------------------------
.py        | src/{01_data_construction, 02_linking, 03_analysis}/
.sh        | scripts/ (except commit.sh in root)
.md        | docs/ (except README.md in root)
.tex       | docs/latex/
.bib       | docs/latex/
.csv       | data/{raw, interim, processed}/
.parquet   | data/{raw, interim, processed}/
.log       | logs/
.png/.pdf  | output/figures/
.tex (out) | output/latex/
```

## Final Reminders

1. **Academic rigor first**: Every claim needs validation
2. **No shortcuts**: Implement full methodology, not simplified versions
3. **Document everything**: If you make a decision, document why
4. **Test on subsets**: Before running on full dataset
5. **Log everything**: Replicability depends on complete logs
6. **Follow patent matching pattern**: It worked well (95.4% accuracy)
7. **Ask before breaking patterns**: If you need to deviate from established patterns, explain why
8. **KEEP ROOT CLEAN**: Only 4 files allowed in root directory
9. **Check file placement**: Use the checklist above for every new file
