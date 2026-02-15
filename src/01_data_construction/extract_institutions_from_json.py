"""
Extract Institution Data from OpenAlex JSON

This script parses the JSON field in arxiv_index.parquet to extract
author-institution relationships from OpenAlex work data.

The JSON field contains complete OpenAlex work objects with authorships
arrays that include institution affiliations for each author.

Date: 2025
"""

import polars as pl
import json
from pathlib import Path
import logging
from typing import List, Dict, Any, Optional
import sys

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"

# Input files
ARXIV_INDEX_PATH = DATA_RAW / "openalex_claude_arxiv_index.parquet"

# Output files
INSTITUTIONS_OUTPUT = DATA_PROCESSED / "paper_institutions.parquet"

# Ensure output directory exists
DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Processing configuration
CHUNK_SIZE = 50000  # Process in chunks for memory efficiency


# ============================================================================
# Helper Functions
# ============================================================================

def extract_institutions_from_json(json_str: str) -> List[Dict[str, Any]]:
    """
    Extract institution data from OpenAlex work JSON.
    
    Parameters:
    -----------
    json_str : str
        JSON string containing OpenAlex work data
        
    Returns:
    --------
    List[Dict[str, Any]]
        List of institution records, one per author-institution pair
    """
    try:
        # Parse JSON
        if isinstance(json_str, str):
            work = json.loads(json_str)
        else:
            work = json_str
        
        institutions_list = []
        
        # Check if authorships exist
        if 'authorships' not in work or not work.get('authorships'):
            return []
        
        openalex_id = work.get('id', '')
        publication_year = work.get('publication_year')
        publication_date = work.get('publication_date')
        
        # Extract institution data from each authorship
        for authorship in work.get('authorships', []):
            author = authorship.get('author', {})
            author_id = author.get('id', '')
            author_display_name = author.get('display_name', '')
            author_position = authorship.get('author_position', '')
            is_corresponding = authorship.get('is_corresponding', False)
            
            # Get institutions for this author
            institutions = authorship.get('institutions', [])
            
            # If no institutions, still record the author (for coverage analysis)
            if not institutions:
                institutions_list.append({
                    'openalex_id': openalex_id,
                    'publication_year': publication_year,
                    'publication_date': publication_date,
                    'author_id': author_id,
                    'author_display_name': author_display_name,
                    'author_position': author_position,
                    'is_corresponding': is_corresponding,
                    'institution_id': None,
                    'institution_name': None,
                    'institution_type': None,
                    'institution_ror': None,
                    'country_code': None,
                    'institution_lineage': None,
                    'has_institution': False
                })
            else:
                # Create one record per institution
                for inst in institutions:
                    institutions_list.append({
                        'openalex_id': openalex_id,
                        'publication_year': publication_year,
                        'publication_date': publication_date,
                        'author_id': author_id,
                        'author_display_name': author_display_name,
                        'author_position': author_position,
                        'is_corresponding': is_corresponding,
                        'institution_id': inst.get('id', ''),
                        'institution_name': inst.get('display_name', ''),
                        'institution_type': inst.get('type', ''),
                        'institution_ror': inst.get('ror', ''),
                        'country_code': inst.get('country_code', ''),
                        'institution_lineage': json.dumps(inst.get('lineage', [])) if inst.get('lineage') else None,
                        'has_institution': True,
                        'source': 'parsed_institution'  # Mark source for tracking
                    })
            
            # If no institutions but have raw_affiliation_strings, try to extract
            if not institutions:
                raw_affs = authorship.get('raw_affiliation_strings', [])
                if raw_affs:
                    # Try to extract from raw affiliations (basic parsing)
                    for raw_aff in raw_affs:
                        # Simple pattern matching for company names
                        raw_aff_lower = raw_aff.lower()
                        company_keywords = ['google', 'microsoft', 'amazon', 'apple', 'meta', 'facebook', 'openai']
                        
                        if any(keyword in raw_aff_lower for keyword in company_keywords):
                            # Extract potential institution name (first part before comma)
                            institution_name = raw_aff.split(',')[0].strip() if ',' in raw_aff else raw_aff.strip()
                            
                            institutions_list.append({
                                'openalex_id': openalex_id,
                                'publication_year': publication_year,
                                'publication_date': publication_date,
                                'author_id': author_id,
                                'author_display_name': author_display_name,
                                'author_position': author_position,
                                'is_corresponding': is_corresponding,
                                'institution_id': None,  # Not available from raw string
                                'institution_name': institution_name,
                                'institution_type': 'company',  # Likely a company if matches keywords
                                'institution_ror': None,
                                'country_code': None,  # Could parse from raw string but skip for now
                                'institution_lineage': None,
                                'has_institution': True,
                                'source': 'raw_affiliation_string'  # Mark source
                            })
        
        return institutions_list
        
    except json.JSONDecodeError as e:
        logger.warning(f"JSON decode error: {str(e)[:100]}")
        return []
    except Exception as e:
        logger.warning(f"Error extracting institutions: {str(e)[:100]}")
        return []


def process_chunk(df_chunk: pl.DataFrame) -> pl.DataFrame:
    """
    Process a chunk of arxiv_index data to extract institutions.
    
    Parameters:
    -----------
    df_chunk : pl.DataFrame
        Chunk of arxiv_index data
        
    Returns:
    --------
    pl.DataFrame
        DataFrame with extracted institution data
    """
    all_records = []
    
    for row in df_chunk.iter_rows(named=True):
        json_str = row.get('json', '')
        if json_str:
            records = extract_institutions_from_json(json_str)
            all_records.extend(records)
    
    if not all_records:
        return pl.DataFrame()
    
    # Convert to DataFrame with explicit schema to avoid inference issues
    schema = {
        'openalex_id': pl.Utf8,
        'publication_year': pl.Int16,
        'publication_date': pl.Utf8,
        'author_id': pl.Utf8,
        'author_display_name': pl.Utf8,
        'author_position': pl.Utf8,
        'is_corresponding': pl.Boolean,
        'institution_id': pl.Utf8,
        'institution_name': pl.Utf8,
        'institution_type': pl.Utf8,
        'institution_ror': pl.Utf8,
        'country_code': pl.Utf8,
        'institution_lineage': pl.Utf8,
        'has_institution': pl.Boolean,
        'source': pl.Utf8  # Track data source: 'parsed_institution' or 'raw_affiliation_string'
    }
    return pl.DataFrame(all_records, schema=schema)


# ============================================================================
# Main Processing
# ============================================================================

def extract_all_institutions():
    """
    Extract all institution data from arxiv_index JSON field.
    """
    logger.info("=" * 80)
    logger.info("Extracting Institution Data from OpenAlex JSON")
    logger.info("=" * 80)
    logger.info(f"Input: {ARXIV_INDEX_PATH}")
    logger.info(f"Output: {INSTITUTIONS_OUTPUT}")
    logger.info("")
    
    # Check if input file exists
    if not ARXIV_INDEX_PATH.exists():
        logger.error(f"Input file not found: {ARXIV_INDEX_PATH}")
        sys.exit(1)
    
    # Load arxiv_index
    logger.info("Loading arxiv_index data...")
    arxiv_index = pl.read_parquet(ARXIV_INDEX_PATH)
    total_rows = len(arxiv_index)
    logger.info(f"  Loaded {total_rows:,} rows")
    logger.info("")
    
    # Process in chunks
    logger.info(f"Processing in chunks of {CHUNK_SIZE:,} rows...")
    all_results = []
    processed = 0
    
    for i in range(0, total_rows, CHUNK_SIZE):
        chunk_end = min(i + CHUNK_SIZE, total_rows)
        df_chunk = arxiv_index[i:chunk_end]
        
        logger.info(f"  Processing rows {i:,} to {chunk_end:,}...")
        chunk_results = process_chunk(df_chunk)
        
        if len(chunk_results) > 0:
            all_results.append(chunk_results)
        
        processed = chunk_end
        progress_pct = (processed / total_rows * 100) if total_rows > 0 else 0
        logger.info(f"    Extracted {len(chunk_results):,} institution records ({progress_pct:.1f}% complete)")
    
    logger.info("")
    logger.info("Combining results...")
    
    if not all_results:
        logger.warning("No institution data extracted!")
        sys.exit(1)
    
    # Combine all chunks
    paper_institutions = pl.concat(all_results)
    
    logger.info(f"  Total institution records: {len(paper_institutions):,}")
    logger.info("")
    
    # Save results
    logger.info(f"Saving to {INSTITUTIONS_OUTPUT}...")
    paper_institutions.write_parquet(INSTITUTIONS_OUTPUT, compression='snappy')
    
    file_size_mb = INSTITUTIONS_OUTPUT.stat().st_size / (1024 * 1024)
    logger.info(f"  Saved successfully ({file_size_mb:.2f} MB)")
    logger.info("")
    
    # Summary statistics
    logger.info("=" * 80)
    logger.info("SUMMARY STATISTICS")
    logger.info("=" * 80)
    logger.info(f"Total institution records: {len(paper_institutions):,}")
    
    # Count unique papers
    unique_papers = paper_institutions.select('openalex_id').n_unique()
    logger.info(f"Unique papers (openalex_id): {unique_papers:,}")
    
    # Count papers with institutions
    papers_with_inst = paper_institutions.filter(pl.col('has_institution') == True).select('openalex_id').n_unique()
    logger.info(f"Papers with institutions: {papers_with_inst:,}")
    
    # Count unique institutions
    unique_institutions = paper_institutions.filter(
        pl.col('institution_id').is_not_null()
    ).select('institution_id').n_unique()
    logger.info(f"Unique institutions: {unique_institutions:,}")
    
    # Institution types
    inst_types = paper_institutions.filter(
        pl.col('institution_type').is_not_null()
    ).group_by('institution_type').agg(
        pl.count().alias('count')
    ).sort('count', descending=True)
    
    logger.info("\nInstitution types:")
    for row in inst_types.iter_rows(named=True):
        logger.info(f"  {row['institution_type']:20s} {row['count']:>10,}")
    
    # Countries
    countries = paper_institutions.filter(
        pl.col('country_code').is_not_null()
    ).group_by('country_code').agg(
        pl.count().alias('count')
    ).sort('count', descending=True).head(10)
    
    logger.info("\nTop 10 countries:")
    for row in countries.iter_rows(named=True):
        logger.info(f"  {row['country_code']:5s} {row['count']:>10,}")
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("Extraction completed successfully!")
    logger.info("=" * 80)


def main():
    """Main execution function"""
    try:
        extract_all_institutions()
        logger.info("\n✓ Institution extraction completed successfully!")
    except KeyboardInterrupt:
        logger.warning("\n✗ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

