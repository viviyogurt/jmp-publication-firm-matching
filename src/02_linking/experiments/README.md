# Experimental and Archived Scripts

This directory contains experimental, exploratory, and deprecated scripts used during the development of the patent and publication matching pipelines.

## Categories

### Exploratory Scripts
- `explore_*.py` - Scripts for exploring unmatched institutions and firms
- `analyze_*.py` - Analysis and diagnostic scripts
- `show_*.py` - Scripts for visualizing match results

### Experimental Matching Approaches
- `match_publications_*.py` - Various experimental publication matching approaches
  - `match_publications_contained_name.py` - Contained name matching (superseded by integrated approach)
  - `match_publications_acronyms_enhanced.py` - Acronym matching (high error rate, not used in final)
  - `match_publications_fuzzy_conservative.py` - Fuzzy matching experiments
  - `match_publications_stage*.py` - Early stage-based approaches

### Stage-based Approaches (Deprecated)
- `stage_*.py` - Early stage-based matching approaches
- `combine_stage*.py` - Combination scripts for stage-based approaches
- `filter_stage*.py` - Filtering scripts for stage-based approaches

### Test Scripts
- `test_*.py` - Unit tests and validation experiments

### Sample Creation Scripts
- `create_500_sample*.py` - Scripts for creating validation samples
- `create_deduplicated_dataset.py` - Deduplication experiments
- `create_filtered_dataset.py` - Filtering experiments

### Validation Experiments
- `validate_publication_matches*.py` - Various validation approaches
- `validate_optimized_matching.py` - Optimization experiments
- `validate_stage1_enhanced.py` - Stage 1 validation experiments

## Final Production Scripts

The final production scripts are in the parent directory (`../`):
- **Publication Matching:**
  - `match_homepage_domains.py` - Homepage domain exact matching
  - `match_alternative_names_enhanced.py` - Enhanced alternative names with abbreviation expansion
  - `match_separator_normalization.py` - Separator normalization
  - `match_top_institutions_direct.py` - Direct matching with location removal
  - `combine_with_alternative_names.py` - Final combination of all methods

- **Patent Matching:**
  - `match_patents_to_firms_stage1.py` - Stage 1: Homepage domain matching
  - `match_patents_to_firms_stage2.py` - Stage 2: Name-based matching
  - `match_patents_to_firms_stage3.py` - Stage 3: Subsidiary matching

- **Validation:**
  - `validate_random_sample.py` - Random sample validation
  - `validate_patent_matches.py` - Patent match validation
  - `automated_validate_sample.py` - Automated validation

## Notes

- These scripts are kept for reference and reproducibility
- Many represent approaches that were tested but not used in the final production pipeline
- Some scripts may have dependencies on data files that are no longer available
- Use `../` directory scripts for production matching workflows
