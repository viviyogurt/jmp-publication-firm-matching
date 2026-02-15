#!/usr/bin/env python3
"""
Condense AI papers flattened batch files into a manageable, production-ready dataset.

This script:
1. Reads flattened batch files in chunks
2. Extracts and consolidates columns according to improved schema
3. Handles author-institution relationships (nested lists)
4. Validates and cleans data
5. Outputs a single condensed Parquet file

Schema: ~57 columns instead of 1000+
"""

import polars as pl
from pathlib import Path
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import argparse
from datetime import datetime
import gc
import time

# Configuration
DEFAULT_INPUT_DIR = Path(__file__).parent.parent.parent / "data" / "raw" / "publication" / "ai_papers_batches_noduplication_flattened"
DEFAULT_OUTPUT_DIR = Path(__file__).parent.parent.parent / "data" / "processed" / "publication"
DEFAULT_CHUNK_SIZE = 10  # Process 10 batch files at a time
LOG_DIR = Path(__file__).parent.parent.parent / "logs"
PROGRESS_FILE = LOG_DIR / "condense_ai_papers_progress.json"

# Setup logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "condense_ai_papers.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)




def extract_author_data(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract author and affiliation data from a row.
    Handles nested structure: one author can have multiple institutions.
    
    Returns:
        Dictionary with author lists and nested affiliation lists
    """
    author_names = []
    author_ids = []
    author_positions = []
    is_corresponding = []
    
    # Nested lists: author_affiliations[i] = list of institutions for author i
    author_affiliations = []  # list[list[str]]
    author_affiliation_ids = []  # list[list[str]]
    author_affiliation_countries = []  # list[list[str]]
    author_affiliation_types = []  # list[list[str]]
    
    # Primary (first) institution per author (simpler alternative)
    author_primary_affiliations = []
    author_primary_affiliation_ids = []
    author_primary_affiliation_countries = []
    
    # Find all authorship columns
    max_authors = 50  # Reasonable limit
    for author_idx in range(max_authors):
        author_name_col = f'authorships_{author_idx}_author_display_name'
        author_id_col = f'authorships_{author_idx}_author_id'
        author_pos_col = f'authorships_{author_idx}_author_position'
        is_corr_col = f'authorships_{author_idx}_is_corresponding'
        
        if author_name_col not in row_dict:
            break
        
        author_name = row_dict.get(author_name_col)
        if author_name is None or str(author_name).strip() == '' or str(author_name) == 'None':
            continue
        
        author_names.append(str(author_name).strip())
        author_ids.append(str(row_dict.get(author_id_col, '')) if row_dict.get(author_id_col) else '')
        
        pos = row_dict.get(author_pos_col)
        author_positions.append(int(pos) if pos is not None and str(pos).isdigit() else len(author_names))
        
        is_corr = row_dict.get(is_corr_col)
        is_corresponding.append(
            str(is_corr).lower() in ('true', '1', 'yes') if is_corr is not None else False
        )
        
        # Extract institutions for this author
        author_inst_names = []
        author_inst_ids = []
        author_inst_countries = []
        author_inst_types = []
        
        max_institutions = 10  # Reasonable limit per author
        for inst_idx in range(max_institutions):
            inst_name_col = f'authorships_{author_idx}_institutions_{inst_idx}_display_name'
            inst_id_col = f'authorships_{author_idx}_institutions_{inst_idx}_id'
            inst_country_col = f'authorships_{author_idx}_institutions_{inst_idx}_country_code'
            inst_type_col = f'authorships_{author_idx}_institutions_{inst_idx}_type'
            
            if inst_name_col not in row_dict:
                break
            
            inst_name = row_dict.get(inst_name_col)
            if inst_name is None or str(inst_name).strip() == '' or str(inst_name) == 'None':
                continue
            
            author_inst_names.append(str(inst_name).strip())
            author_inst_ids.append(
                str(row_dict.get(inst_id_col, '')) if row_dict.get(inst_id_col) else ''
            )
            
            country = row_dict.get(inst_country_col)
            author_inst_countries.append(str(country).strip() if country else '')
            
            inst_type = row_dict.get(inst_type_col)
            author_inst_types.append(str(inst_type).strip() if inst_type else '')
        
        author_affiliations.append(author_inst_names)
        author_affiliation_ids.append(author_inst_ids)
        author_affiliation_countries.append(author_inst_countries)
        author_affiliation_types.append(author_inst_types)
        
        # Primary (first) institution
        if author_inst_names:
            author_primary_affiliations.append(author_inst_names[0])
            author_primary_affiliation_ids.append(author_inst_ids[0] if author_inst_ids else '')
            author_primary_affiliation_countries.append(
                author_inst_countries[0] if author_inst_countries else ''
            )
        else:
            author_primary_affiliations.append('')
            author_primary_affiliation_ids.append('')
            author_primary_affiliation_countries.append('')
    
    return {
        'author_names': author_names,
        'author_ids': author_ids,
        'author_positions': author_positions,
        'is_corresponding_author': is_corresponding,
        'author_affiliations': author_affiliations,  # Nested
        'author_affiliation_ids': author_affiliation_ids,  # Nested
        'author_affiliation_countries': author_affiliation_countries,  # Nested
        'author_affiliation_types': author_affiliation_types,  # Nested
        'author_primary_affiliations': author_primary_affiliations,
        'author_primary_affiliation_ids': author_primary_affiliation_ids,
        'author_primary_affiliation_countries': author_primary_affiliation_countries,
    }


def extract_topics_data(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Extract topic data from a row."""
    topic_names = []
    topic_ids = []
    topic_scores = []
    
    max_topics = 20
    for idx in range(max_topics):
        name_col = f'topics_{idx}_display_name'
        id_col = f'topics_{idx}_id'
        score_col = f'topics_{idx}_score'
        
        if name_col not in row_dict:
            break
        
        name = row_dict.get(name_col)
        if name is None or str(name).strip() == '' or str(name) == 'None':
            continue
        
        topic_names.append(str(name).strip())
        
        topic_id = row_dict.get(id_col)
        topic_ids.append(str(topic_id).strip() if topic_id else '')
        
        score = row_dict.get(score_col)
        try:
            topic_scores.append(float(score) if score is not None and str(score) != 'None' else None)
        except:
            topic_scores.append(None)
    
    # Primary topic
    primary_topic_name = row_dict.get('primary_topic_display_name')
    primary_topic_id = row_dict.get('primary_topic_id')
    
    return {
        'topic_names': topic_names,
        'topic_ids': topic_ids,
        'topic_scores': topic_scores if any(s is not None for s in topic_scores) else None,
        'primary_topic_name': str(primary_topic_name).strip() if primary_topic_name else None,
        'primary_topic_id': str(primary_topic_id).strip() if primary_topic_id else None,
    }


def extract_concepts_data(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Extract concept data from a row."""
    concept_names = []
    concept_ids = []
    concept_scores = []
    
    max_concepts = 40
    for idx in range(max_concepts):
        name_col = f'concepts_{idx}_display_name'
        id_col = f'concepts_{idx}_id'
        score_col = f'concepts_{idx}_score'
        
        if name_col not in row_dict:
            break
        
        name = row_dict.get(name_col)
        if name is None or str(name).strip() == '' or str(name) == 'None':
            continue
        
        concept_names.append(str(name).strip())
        
        concept_id = row_dict.get(id_col)
        concept_ids.append(str(concept_id).strip() if concept_id else '')
        
        score = row_dict.get(score_col)
        try:
            concept_scores.append(float(score) if score is not None and str(score) != 'None' else None)
        except:
            concept_scores.append(None)
    
    return {
        'concept_names': concept_names,
        'concept_ids': concept_ids,
        'concept_scores': concept_scores if any(s is not None for s in concept_scores) else None,
    }


def extract_keywords_data(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Extract keyword data from a row."""
    keyword_names = []
    keyword_ids = []
    
    max_keywords = 20
    for idx in range(max_keywords):
        name_col = f'keywords_{idx}_display_name'
        id_col = f'keywords_{idx}_id'
        
        if name_col not in row_dict:
            break
        
        name = row_dict.get(name_col)
        if name is None or str(name).strip() == '' or str(name) == 'None':
            continue
        
        keyword_names.append(str(name).strip())
        
        keyword_id = row_dict.get(id_col)
        keyword_ids.append(str(keyword_id).strip() if keyword_id else '')
    
    return {
        'keyword_names': keyword_names,
        'keyword_ids': keyword_ids if any(kid for kid in keyword_ids) else None,
    }


def extract_locations_data(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Extract location data from a row."""
    location_countries = []
    location_sources = []
    
    # Try primary location first
    primary_country = row_dict.get('primary_location_country_code')
    if primary_country:
        location_countries.append(str(primary_country).strip())
    
    primary_source = row_dict.get('primary_location_source_display_name')
    if primary_source:
        location_sources.append(str(primary_source).strip())
    
    # Extract from authorships country codes
    max_authors = 50
    for author_idx in range(max_authors):
        country_col = f'authorships_{author_idx}_countries'
        if country_col not in row_dict:
            break
        
        countries = row_dict.get(country_col)
        if countries:
            # Could be a string representation of a list
            if isinstance(countries, str):
                try:
                    countries_list = json.loads(countries) if countries.startswith('[') else [countries]
                except:
                    countries_list = [countries]
            elif isinstance(countries, list):
                countries_list = countries
            else:
                countries_list = [str(countries)]
            
            for country in countries_list:
                if country and str(country).strip() and str(country) != 'None':
                    country_str = str(country).strip()
                    if country_str not in location_countries:
                        location_countries.append(country_str)
    
    return {
        'location_countries': location_countries,
        'location_sources': location_sources if location_sources else None,
    }


def extract_sdg_data(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Extract Sustainable Development Goal data from a row."""
    sdg_names = []
    sdg_ids = []
    sdg_scores = []
    
    max_sdgs = 17
    for idx in range(max_sdgs):
        name_col = f'sustainable_development_goals_{idx}_display_name'
        id_col = f'sustainable_development_goals_{idx}_id'
        score_col = f'sustainable_development_goals_{idx}_score'
        
        if name_col not in row_dict:
            break
        
        name = row_dict.get(name_col)
        if name is None or str(name).strip() == '' or str(name) == 'None':
            continue
        
        sdg_names.append(str(name).strip())
        
        sdg_id = row_dict.get(id_col)
        sdg_ids.append(str(sdg_id).strip() if sdg_id else '')
        
        score = row_dict.get(score_col)
        try:
            sdg_scores.append(float(score) if score is not None and str(score) != 'None' else None)
        except:
            sdg_scores.append(None)
    
    return {
        'sdg_names': sdg_names if sdg_names else None,
        'sdg_ids': sdg_ids if sdg_ids else None,
        'sdg_scores': sdg_scores if any(s is not None for s in sdg_scores) else None,
    }


def condense_row(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Condense a single row from flattened format to condensed schema.
    
    Args:
        row_dict: Dictionary representing one row from flattened DataFrame
    
    Returns:
        Dictionary with condensed schema
    """
    # Core identifiers
    openalex_id = str(row_dict.get('id', '')) or str(row_dict.get('openalex_id', ''))
    
    # Try multiple possible arxiv_id column names
    arxiv_id = None
    arxiv_id_candidates = ['arxiv_id', 'ids_arxiv', 'arxiv']
    for candidate in arxiv_id_candidates:
        arxiv_val = row_dict.get(candidate)
        if arxiv_val and str(arxiv_val).strip() and str(arxiv_val) != 'None':
            arxiv_id = str(arxiv_val).strip()
            break
    
    # Try multiple possible doi column names
    doi = None
    doi_candidates = ['doi', 'ids_doi']
    for candidate in doi_candidates:
        doi_val = row_dict.get(candidate)
        if doi_val and str(doi_val).strip() and str(doi_val) != 'None':
            doi = str(doi_val).strip()
            break
    
    # Create composite paper_id
    paper_id = arxiv_id if arxiv_id and arxiv_id != 'None' else openalex_id
    
    # Basic metadata
    title = str(row_dict.get('title', '') or row_dict.get('display_name', '')).strip()
    publication_date = row_dict.get('publication_date')
    publication_year = row_dict.get('publication_year')
    
    # Extract year from date if needed
    if not publication_year and publication_date:
        try:
            if isinstance(publication_date, str):
                publication_year = int(publication_date[:4]) if len(publication_date) >= 4 else None
        except:
            pass
    
    # Extract abstract - try multiple sources
    abstract = None
    abstract_sources = ['abstract', 'abstract_inverted_index']
    for source in abstract_sources:
        abstract_val = row_dict.get(source)
        if abstract_val:
            if isinstance(abstract_val, str) and abstract_val.strip() and abstract_val != 'None':
                abstract = abstract_val.strip()
                break
            # If it's a dict (abstract_inverted_index), we can't easily reconstruct, skip
            elif isinstance(abstract_val, dict):
                continue
    
    # Extract all data
    author_data = extract_author_data(row_dict)
    topics_data = extract_topics_data(row_dict)
    concepts_data = extract_concepts_data(row_dict)
    keywords_data = extract_keywords_data(row_dict)
    locations_data = extract_locations_data(row_dict)
    sdg_data = extract_sdg_data(row_dict)
    
    # Build condensed row
    condensed = {
        # Identifiers
        'paper_id': paper_id,
        'openalex_id': openalex_id,
        'arxiv_id': arxiv_id,
        'doi': doi,
        
        # Basic metadata
        'title': title,
        'abstract': abstract,
        'publication_date': str(publication_date) if publication_date else None,
        'publication_year': int(publication_year) if publication_year else None,
        'type': str(row_dict.get('type', '')),
        'type_crossref': str(row_dict.get('type_crossref', '')) if row_dict.get('type_crossref') else None,
        'type_id': str(row_dict.get('type_id', '')),
        'work_type': str(row_dict.get('work_type', '')),
        'url': str(row_dict.get('url', '')) if row_dict.get('url') else None,
        'language': str(row_dict.get('language', '')) if row_dict.get('language') else None,
        
        # Counts
        'authors_count': int(row_dict.get('authors_count', 0)) if row_dict.get('authors_count') else 0,
        'topics_count': int(row_dict.get('topics_count', 0)) if row_dict.get('topics_count') else 0,
        'concepts_count': int(row_dict.get('concepts_count', 0)) if row_dict.get('concepts_count') else 0,
        'keywords_count': len(keywords_data['keyword_names']),
        'locations_count': int(row_dict.get('locations_count', 0)) if row_dict.get('locations_count') else 0,
        'referenced_works_count': int(row_dict.get('referenced_works_count', 0)) if row_dict.get('referenced_works_count') else 0,
        'cited_by_count': int(row_dict.get('cited_by_count', 0)) if row_dict.get('cited_by_count') else 0,
        
        # AI category flags
        'is_computer_vision': bool(row_dict.get('is_computer_vision', False)),
        'is_deep_learning': bool(row_dict.get('is_deep_learning', False)),
        'is_llm': bool(row_dict.get('is_llm', False)),
        'is_machine_learning': bool(row_dict.get('is_machine_learning', False)),
        'is_nlp': bool(row_dict.get('is_nlp', False)),
        'is_reinforcement_learning': bool(row_dict.get('is_reinforcement_learning', False)),
        
        # Venue information
        'venue_name': str(row_dict.get('primary_location_source_display_name', '')).strip() if row_dict.get('primary_location_source_display_name') else None,
        'venue_id': str(row_dict.get('primary_location_source_id', '')) if row_dict.get('primary_location_source_id') else None,
        'venue_type': str(row_dict.get('primary_location_source_type', '')) if row_dict.get('primary_location_source_type') else None,
        'publisher': str(row_dict.get('primary_location_source_publisher', '')) if row_dict.get('primary_location_source_publisher') else None,
        'is_open_access': bool(row_dict.get('open_access_is_oa', False)) or bool(row_dict.get('primary_location_is_oa', False)),
        
        # Authors (from extracted data)
        'author_names': author_data['author_names'],
        'author_ids': author_data['author_ids'],
        'author_positions': author_data['author_positions'],
        'is_corresponding_author': author_data['is_corresponding_author'],
        'author_primary_affiliations': author_data['author_primary_affiliations'],
        'author_primary_affiliation_ids': author_data['author_primary_affiliation_ids'],
        'author_primary_affiliation_countries': author_data['author_primary_affiliation_countries'],
        'author_affiliations': author_data['author_affiliations'],  # Nested
        'author_affiliation_ids': author_data['author_affiliation_ids'],  # Nested
        'author_affiliation_countries': author_data['author_affiliation_countries'],  # Nested
        
        # Topics
        'topic_names': topics_data['topic_names'],
        'topic_ids': topics_data['topic_ids'],
        'primary_topic_name': topics_data['primary_topic_name'],
        'primary_topic_id': topics_data['primary_topic_id'],
        
        # Concepts
        'concept_names': concepts_data['concept_names'],
        'concept_ids': concepts_data['concept_ids'],
        
        # Keywords
        'keyword_names': keywords_data['keyword_names'],
        'keyword_ids': keywords_data['keyword_ids'] if keywords_data['keyword_ids'] else None,
        
        # Locations & SDGs
        'location_countries': locations_data['location_countries'],
        'location_sources': locations_data['location_sources'],
        'sdg_names': sdg_data['sdg_names'],
        'sdg_ids': sdg_data['sdg_ids'],
        
        # Additional metrics
        'citation_percentile': float(row_dict.get('citation_normalized_percentile_value', 0)) if row_dict.get('citation_normalized_percentile_value') else None,
        'is_top_1_percent': bool(row_dict.get('citation_normalized_percentile_is_in_top_1_percent', False)),
        'is_top_10_percent': bool(row_dict.get('citation_normalized_percentile_is_in_top_10_percent', False)),
        'institutions_distinct_count': int(row_dict.get('institutions_distinct_count', 0)) if row_dict.get('institutions_distinct_count') else len(set(author_data['author_primary_affiliation_ids'])),
        'countries_distinct_count': len(set(locations_data['location_countries'])) if locations_data['location_countries'] else 0,
        'has_abstract': abstract is not None and len(abstract) > 0,
    }
    
    return condensed


def process_batch_chunk(batch_files: List[Path], chunk_num: int) -> pl.DataFrame:
    """
    Process a chunk of batch files and return condensed DataFrame.
    
    Args:
        batch_files: List of batch file paths
        chunk_num: Chunk number for logging
    
    Returns:
        Condensed Polars DataFrame
    """
    logger.info(f"Processing chunk {chunk_num}: {len(batch_files)} files...")
    
    all_condensed_rows = []
    
    for batch_file in batch_files:
        try:
            df = pl.read_parquet(batch_file)
            logger.debug(f"  Loaded {batch_file.name}: {len(df):,} rows, {len(df.columns)} columns")
            
            # Process each row
            for row in df.iter_rows(named=True):
                try:
                    condensed_row = condense_row(row)
                    all_condensed_rows.append(condensed_row)
                except Exception as e:
                    logger.warning(f"  Error condensing row in {batch_file.name}: {e}")
                    continue
            
            del df
            gc.collect()
            
        except Exception as e:
            logger.error(f"  Error processing {batch_file.name}: {e}")
            continue
    
    if not all_condensed_rows:
        logger.warning(f"  No rows processed in chunk {chunk_num}")
        return None
    
    # Create DataFrame from condensed rows
    try:
        # Handle nested lists properly - Polars needs explicit schema for list of lists
        condensed_df = pl.DataFrame(all_condensed_rows)
        
        # Convert nested lists to proper Polars list types
        # For columns that are list of lists, we need to ensure they're properly typed
        list_of_list_cols = ['author_affiliations', 'author_affiliation_ids', 
                            'author_affiliation_countries', 'author_affiliation_types']
        
        for col in list_of_list_cols:
            if col in condensed_df.columns:
                # Ensure it's a list of lists
                try:
                    # Polars should handle this automatically, but ensure consistency
                    pass  # Polars handles nested lists automatically
                except:
                    logger.warning(f"  Could not process nested list column {col}")
        
        logger.info(f"  Chunk {chunk_num}: Condensed to {len(condensed_df):,} rows, {len(condensed_df.columns)} columns")
        return condensed_df
    except Exception as e:
        logger.error(f"  Error creating DataFrame from chunk {chunk_num}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def load_progress(progress_file: Path) -> Dict[str, Any]:
    """Load progress from previous run."""
    if progress_file.exists():
        try:
            with open(progress_file, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_progress(progress_file: Path, progress: Dict[str, Any]):
    """Save progress to file."""
    try:
        with open(progress_file, 'w') as f:
            json.dump(progress, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to save progress: {e}")


def main():
    parser = argparse.ArgumentParser(description='Condense AI papers flattened batch files')
    parser.add_argument('--input-dir', type=str, default=str(DEFAULT_INPUT_DIR),
                       help='Directory containing flattened batch files')
    parser.add_argument('--output-file', type=str, default=None,
                       help='Output condensed Parquet file path')
    parser.add_argument('--chunk-size', type=int, default=DEFAULT_CHUNK_SIZE,
                       help='Number of batch files to process per chunk')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Process only this many batch files (for testing)')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from previous run using progress file')
    
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise ValueError(f"Input directory does not exist: {input_dir}")
    
    # Get batch files
    batch_files = sorted(input_dir.glob("*_flatten.parquet"))
    
    if not batch_files:
        raise ValueError(f"No flattened batch files found in {input_dir}")
    
    if args.sample_size:
        batch_files = batch_files[:args.sample_size]
        logger.info(f"Processing sample of {len(batch_files)} files...")
    else:
        logger.info(f"Processing all {len(batch_files)} batch files...")
    
    # Setup output
    if args.output_file:
        output_file = Path(args.output_file)
    else:
        output_file = DEFAULT_OUTPUT_DIR / "ai_papers_condensed.parquet"
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Output file: {output_file}")
    logger.info(f"Chunk size: {args.chunk_size} files per chunk")
    
    # Load progress if resuming
    progress = load_progress(PROGRESS_FILE) if args.resume else {}
    processed_files = set(progress.get('processed_files', []))
    all_dfs = []
    start_time = time.time()  # Initialize start time early
    
    # If resuming and output file exists, load it
    if args.resume and output_file.exists():
        try:
            logger.info(f"Resuming: Loading existing output file with {len(pl.read_parquet(output_file)):,} papers...")
            existing_df = pl.read_parquet(output_file)
            all_dfs.append(existing_df)
            logger.info(f"  Loaded {len(existing_df):,} papers from previous run")
        except Exception as e:
            logger.warning(f"Could not load existing output file: {e}. Starting fresh.")
            processed_files = set()
            all_dfs = []
    
    # Filter out already processed files
    if processed_files:
        batch_files = [f for f in batch_files if f.name not in processed_files]
        logger.info(f"Resuming: {len(processed_files)} files already processed, {len(batch_files)} remaining")
    
    # Check if temp file exists and is more recent/complete than output (resume from temp file)
    temp_file = output_file.parent / f"{output_file.stem}_temp.parquet"
    if temp_file.exists():
        temp_size = temp_file.stat().st_size
        output_size = output_file.stat().st_size if output_file.exists() else 0
        # If temp file is significantly larger or all files are processed, use temp file
        if temp_size > output_size * 10 or (not batch_files and temp_size > 100 * 1024 * 1024):  # 100MB threshold
            logger.info(f"Temp file exists ({temp_size / (1024**3):.2f} GB). Checking if we should resume from it...")
            try:
                temp_df = pl.read_parquet(temp_file)
                temp_row_count = len(temp_df)
                logger.info(f"  Temp file has {temp_row_count:,} papers")
                # If temp file has substantial data and we're close to completion, use it
                if temp_row_count > 10_000_000 or (not batch_files and temp_row_count > 1_000_000):
                    logger.info("  Resuming from temp file (contains combined data from all processed chunks)...")
                    final_df = temp_df
                    all_dfs = []  # Skip chunk processing, go directly to final steps
                    batch_files = []  # Mark as no files to process
                else:
                    logger.info("  Temp file seems incomplete, will continue processing...")
                    del temp_df  # Free memory
            except Exception as e:
                logger.warning(f"Could not read temp file: {e}. Will continue processing...")
    
    if not batch_files and 'final_df' in locals():
        # We loaded from temp file, skip to final steps
        pass
    elif not batch_files:
        logger.info("All files already processed and no temp file found. Checking if final output exists...")
        if output_file.exists():
            logger.info("Final output file already exists. Nothing to do.")
            return
        else:
            logger.warning("All files processed but no output found. This should not happen.")
            return
    
    # Process in chunks (only if we have files to process)
    if batch_files:
        total_chunks = (len(batch_files) + args.chunk_size - 1) // args.chunk_size
    
    for chunk_idx in range(0, len(batch_files), args.chunk_size):
        chunk_files = batch_files[chunk_idx:chunk_idx + args.chunk_size]
        chunk_num = (chunk_idx // args.chunk_size) + 1
        
        logger.info(f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk_files)} files)...")
        chunk_start_time = time.time()
        
        condensed_df = process_batch_chunk(chunk_files, chunk_num)
        
        if condensed_df is not None and len(condensed_df) > 0:
            all_dfs.append(condensed_df)
            
            # Update progress
            for f in chunk_files:
                processed_files.add(f.name)
            progress['processed_files'] = list(processed_files)
            progress['last_processed'] = chunk_files[-1].name
            progress['last_update'] = datetime.now().isoformat()
            save_progress(PROGRESS_FILE, progress)
            
            chunk_elapsed = time.time() - chunk_start_time
            logger.info(f"  Chunk {chunk_num} completed in {chunk_elapsed:.1f}s")
        
        # Periodic memory cleanup and intermediate save
        if chunk_num % 5 == 0:
            gc.collect()
            elapsed = time.time() - start_time
            logger.info(f"  Memory cleanup after chunk {chunk_num}/{total_chunks} (elapsed: {elapsed/60:.1f} min)")
            
            # Save intermediate result
            if all_dfs:
                try:
                    logger.info("  Saving intermediate result...")
                    temp_output = output_file.parent / f"{output_file.stem}_temp.parquet"
                    combined = pl.concat(all_dfs, how='diagonal')
                    combined.write_parquet(temp_output, compression='snappy', use_pyarrow=True)
                    logger.info(f"  Intermediate save complete: {len(combined):,} papers")
                except Exception as e:
                    logger.warning(f"  Failed to save intermediate result: {e}")
    
    # If we loaded from temp file, skip chunk processing
    if 'final_df' not in locals():
        if not all_dfs:
            raise ValueError("No data was successfully processed")
        
        # Combine all chunks
        logger.info(f"Combining {len(all_dfs)} chunks...")
        final_df = pl.concat(all_dfs, how='diagonal')
    
    # Remove duplicates by paper_id
    logger.info("Removing duplicates...")
    initial_count = len(final_df)
    final_df = final_df.unique(subset=['paper_id'], keep='first')
    duplicates_removed = initial_count - len(final_df)
    if duplicates_removed > 0:
        logger.info(f"  Removed {duplicates_removed:,} duplicate papers")
    
    # Filter out papers missing critical fields
    logger.info("Filtering invalid papers...")
    before_filter = len(final_df)
    final_df = final_df.filter(
        (pl.col('title').is_not_null()) & 
        (pl.col('title') != '') &
        (pl.col('publication_date').is_not_null() | pl.col('publication_year').is_not_null())
    )
    filtered_out = before_filter - len(final_df)
    if filtered_out > 0:
        logger.info(f"  Filtered out {filtered_out:,} papers missing critical fields")
    
    # Sort by publication_year, then by cited_by_count
    logger.info("Sorting data...")
    final_df = final_df.sort(['publication_year', 'cited_by_count'], descending=[False, True], nulls_last=True)
    
    # Write output
    logger.info(f"Writing condensed dataset to {output_file}...")
    final_df.write_parquet(
        output_file,
        compression='snappy',
        use_pyarrow=True
    )
    
    # Clean up progress file and temp file on success
    if PROGRESS_FILE.exists():
        try:
            PROGRESS_FILE.unlink()
            logger.info("Progress file cleaned up")
        except:
            pass
    
    temp_file = output_file.parent / f"{output_file.stem}_temp.parquet"
    if temp_file.exists():
        try:
            temp_file.unlink()
            logger.info("Temporary file cleaned up")
        except:
            pass
    
    total_elapsed = time.time() - start_time
    logger.info("=" * 80)
    logger.info("Condensation complete!")
    logger.info("=" * 80)
    logger.info(f"Final dataset: {len(final_df):,} papers")
    logger.info(f"Columns: {len(final_df.columns)}")
    logger.info(f"File size: {output_file.stat().st_size / (1024**3):.2f} GB")
    logger.info(f"Total processing time: {total_elapsed/3600:.2f} hours")
    logger.info(f"Output file: {output_file}")
    
    # Print column summary
    logger.info("\nColumn summary:")
    for col in final_df.columns:
        null_count = final_df[col].null_count()
        null_pct = (null_count / len(final_df)) * 100
        logger.info(f"  {col}: {null_pct:.1f}% null")


if __name__ == "__main__":
    main()
