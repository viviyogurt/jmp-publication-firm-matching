# JMP Job Market Paper: Firm Innovation Through Publications and Patents

This repository contains code and documentation for a Job Market Paper (JMP) studying firm innovation through publication and patent analysis. The project matches OpenAlex publication institutions to CRSP/Compustat financial firms to analyze research productivity and innovation patterns.

## Project Overview

**Research Question:** How do firms innovate through publications and patents?

**Key Components:**
- **Publication matching:** Link OpenAlex research institutions to financial firms
- **Patent matching:** Link USPTO patent assignees to financial firms
- **Innovation metrics:** Analyze publication and patent output at firm-year level
- **Quality validation:** >95% accuracy through multi-stage matching pipeline

## Project Structure

```
jmp/
├── src/
│   ├── 01_data_construction/    # Data fetching, extraction, cleaning
│   ├── 02_linking/              # Entity-to-firm matching/linking
│   └── 03_analysis/             # Panel creation and analysis
├── data/
│   ├── raw/                     # Original downloaded data (NEVER modify)
│   ├── interim/                 # Intermediate processed data
│   └── processed/               # Final outputs for analysis
├── logs/                        # Script execution logs
├── docs/                        # Project documentation and methodology
├── output/                      # Tables and figures for paper
└── README.md                    # This file
```

## Key Features

### Multi-Stage Matching Pipeline

**Stage 1: Exact & High-Confidence Matching**
- Exact name matching (conm, conml, alternative names)
- Ticker matching from institution acronyms
- Homepage domain exact matching
- ROR ID matching (when available)

**Stage 2: Smart Fuzzy Matching**
- Jaro-Winkler similarity (≥0.85 threshold)
- Business description validation
- Location-aware matching
- Parent institution cascade

**Stage 3: Manual & Semi-Automated**
- Top firm manual curation
- Wikipedia company scraping
- Subsidiary pattern matching

### Validation Framework

- **Random sampling:** 1,000 matches validated
- **Accuracy target:** >95% overall
- **Confidence scoring:** All matches scored 0.90-0.98
- **Quality filters:** Remove false positives aggressively

## Results

### Patent Matching (Complete)
- **6,786 unique firms** matched (36% of 18,709)
- **35,203 assignee-firm pairs**
- **Accuracy:** 95.4% overall, 100% for Stage 1
- **Papers:** 1.2M+ patents covered

### Publication Matching (In Progress)
- **Target:** 2,000+ firms, >95% accuracy
- **Current:** Optimized pipeline using lookup dictionaries
- **Methods:** Ticker matching from acronyms (highest value)
- **Expected:** 300K+ papers covered

## Technology Stack

- **Python 3.10+**
- **Polars** - Efficient DataFrame operations
- **RapidFuzz** - Fuzzy string matching
- **OpenAlex API** - Publication and institution data
- **WRDS** - CRSP/Compustat financial data
- **USPTO** - Patent data

## Installation

```bash
# Clone repository
git clone <repository-url>
cd jmp

# Install dependencies
pip install -r requirements.txt
```

## Key Scripts

### Data Construction
```bash
# Fetch publication data
python src/01_data_construction/fetch_publication_data.py

# Extract and enrich institutions
python src/01_data_construction/extract_and_enrich_institutions.py

# Classify institution types
python src/01_data_construction/correct_institution_classifications/classify_fast.py
```

### Linking (Matching)
```bash
# Patent matching (complete)
python src/02_linking/match_patents_to_firms_stage1.py
python src/02_linking/match_patents_to_firms_stage2.py
python src/02_linking/match_patents_to_firms_stage3.py

# Publication matching (optimized)
python src/02_linking/match_publications_optimized.py

# Validate matches
python src/02_linking/validate_patent_matches.py
python src/02_linking/validate_publication_matches.py
```

### Analysis
```bash
# Create firm-year panels
python src/03_analysis/create_patent_firm_year_panel.py
python src/03_analysis/create_publication_firm_year_panel.py

# Analyze trends
python src/03_analysis/analyze_big_tech_publications_vs_patents.py
```

## Documentation

- **`CLAUDE.md`** - Complete project guidelines and coding standards
- **`docs/paper_match_financials.md`** - Original comprehensive matching plan
- **`docs/publication_matching_plan_final.md`** - Optimized matching strategy
- **`docs/publication_firm_matching_final.md`** - Final validation results

## Data Sources

- **OpenAlex:** https://openalex.org/ - Global research publication database
- **WRDS CRSP/Compustat:** Financial firm data
- **USPTO:** Patent assignee data

## Quality Standards

All code adheres to strict academic research standards:
- ✅ Reproducible (deterministic with seeds)
- ✅ Validated (manual accuracy checks)
- ✅ Documented (comprehensive docstrings)
- ✅ Logged (complete execution traces)
- ✅ No placeholders (all functions work)

## Citation

If you use this code or methodology, please cite:

```bibtex
@working_paper{jmp2024,
  title={Firm Innovation Through Publications and Patents},
  author={[Author Name]},
  year={2024},
  institution={[University]}
}
```

## License

This project is for academic research purposes. Please contact the author for any commercial use inquiries.

## Contact

- **Project:** JMP Job Market Paper
- **Last Updated:** February 2026
- **Status:** Active development

## Acknowledgments

This research uses data from OpenAlex, WRDS (CRSP/Compustat), and USPTO. Matching methodology builds on patent-financial linking best practices from academic literature.

---

**Note:** This repository contains source code and documentation only. Data files are excluded for size and privacy reasons. To reproduce results, obtain data from the sources listed above.
