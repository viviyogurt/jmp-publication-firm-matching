"""
Stage 0: Prepare Publication Institutions for Matching

This script extracts company institutions from papers and enriches them with
metadata from the institutions_all.jsonl.gz file.

Following the patent matching approach, we create a master table of institutions
with all name variants and metadata needed for matching.
"""

import polars as pl
import json
import gzip
import re
from pathlib import Path
import logging
from typing import Dict, List, Optional, Set
from collections import defaultdict

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
LOGS_DIR = PROJECT_ROOT / "logs"

PAPERS_FILE = DATA_PROCESSED / "ai_papers_firm_affiliated.parquet"
INSTITUTIONS_FILE = DATA_RAW / "institutions_all.jsonl.gz"
OUTPUT_FILE = DATA_INTERIM / "publication_institutions_master.parquet"
PROGRESS_LOG = LOGS_DIR / "prepare_publication_institutions.log"

DATA_INTERIM.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROGRESS_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def clean_organization_name(name: Optional[str]) -> Optional[str]:
    """
    Standardize organization names (same as patent matching):
    - Convert to uppercase
    - Remove common suffixes (INC, LTD, CORP, LLC, CO, etc.)
    - Remove punctuation
    - Strip extra whitespace
    """
    if name is None or name == "":
        return None
    
    # Convert to uppercase
    name = name.upper()
    
    # Remove common corporate suffixes (order matters - longer patterns first)
    suffixes_pattern = r'\b(INCORPORATED|CORPORATION|COMPANY|LIMITED|L\.?L\.?C\.?|INC\.?|LTD\.?|CORP\.?|CO\.?|PLC\.?|S\.?A\.?|A\.?G\.?|GMBH|N\.?V\.?|B\.?V\.?)\b'
    name = re.sub(suffixes_pattern, '', name)
    
    # Remove punctuation (keep alphanumeric and spaces)
    name = re.sub(r'[^\w\s]', '', name)
    
    # Collapse multiple spaces and strip
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name if name else None


def extract_domain(url: Optional[str]) -> Optional[str]:
    """Extract normalized domain from URL."""
    if not url:
        return None
    
    try:
        # Remove protocol
        url = url.replace('http://', '').replace('https://', '')
        # Remove www.
        url = url.replace('www.', '')
        # Remove path and trailing slash
        url = url.split('/')[0]
        # Remove trailing dot
        url = url.rstrip('.')
        return url.lower() if url else None
    except:
        return None


def create_name_variants(display_name: str, alternative_names: List[str], 
                         acronyms: List[str]) -> List[str]:
    """Create all name variants for an institution."""
    variants = set()
    
    # Add cleaned display name
    cleaned = clean_organization_name(display_name)
    if cleaned:
        variants.add(cleaned)
    
    # Add cleaned alternative names
    for alt_name in alternative_names:
        cleaned = clean_organization_name(alt_name)
        if cleaned:
            variants.add(cleaned)
    
    # Add acronyms (uppercase)
    for acronym in acronyms:
        if acronym:
            variants.add(acronym.upper())
    
    # Remove country suffixes from display name (e.g., "Company (United States)")
    if '(' in display_name and ')' in display_name:
        name_without_country = re.sub(r'\s*\([^)]+\)\s*', '', display_name)
        cleaned = clean_organization_name(name_without_country)
        if cleaned:
            variants.add(cleaned)
    
    return sorted([v for v in variants if v and len(v) >= 2])


def extract_institution_ids_from_papers(papers_df: pl.DataFrame) -> Dict[str, int]:
    """
    Extract unique institution IDs from papers and count papers per institution.
    Returns: dict mapping institution_id to paper_count
    """
    logger.info("Extracting institution IDs from papers...")
    
    institution_counts = defaultdict(int)
    total_papers = len(papers_df)
    
    for i, row in enumerate(papers_df.iter_rows(named=True)):
        if (i + 1) % 100000 == 0:
            logger.info(f"  Processed {i+1:,}/{total_papers:,} papers...")
        
        # Get all affiliation IDs (nested list structure)
        aff_ids = row.get('author_affiliation_ids', [])
        
        # Flatten nested list
        for author_affs in aff_ids:
            if isinstance(author_affs, list):
                for inst_id in author_affs:
                    if inst_id and isinstance(inst_id, str):
                        institution_counts[inst_id] += 1
    
    logger.info(f"  Found {len(institution_counts):,} unique institution IDs")
    return dict(institution_counts)


def load_institution_metadata(institution_ids: Set[str]) -> Dict[str, Dict]:
    """
    Load institution metadata from institutions_all.jsonl.gz.
    Only loads institutions that appear in papers and are of type 'company'.
    """
    logger.info(f"Loading institution metadata for {len(institution_ids):,} institutions...")
    
    institutions = {}
    companies_found = 0
    total_scanned = 0
    
    if not INSTITUTIONS_FILE.exists():
        raise FileNotFoundError(f"Institutions file not found: {INSTITUTIONS_FILE}")
    
    with gzip.open(INSTITUTIONS_FILE, 'rt') as f:
        for line in f:
            total_scanned += 1
            if total_scanned % 100000 == 0:
                logger.info(f"  Scanned {total_scanned:,} institutions, found {companies_found:,} companies...")
            
            try:
                inst = json.loads(line)
                inst_id = inst.get('id')
                
                # Only process if it's in our list and is a company
                if inst_id in institution_ids and inst.get('type') == 'company':
                    companies_found += 1
                    
                    # Extract metadata
                    institutions[inst_id] = {
                        'institution_id': inst_id,
                        'display_name': inst.get('display_name', ''),
                        'ror_id': inst.get('ror'),
                        'wikidata_id': inst.get('ids', {}).get('wikidata'),
                        'homepage_url': inst.get('homepage_url'),
                        'country_code': inst.get('country_code'),
                        'alternative_names': inst.get('display_name_alternatives', []),
                        'acronyms': inst.get('display_name_acronyms', []),
                        'geo_city': inst.get('geo', {}).get('city'),
                        'geo_region': inst.get('geo', {}).get('region'),
                        'geo_country': inst.get('geo', {}).get('country'),
                    }
                    
                    # Stop if we've found all companies
                    if companies_found >= len(institution_ids):
                        logger.info(f"  Found all {companies_found:,} companies!")
                        break
            except json.JSONDecodeError:
                continue
            except Exception as e:
                logger.warning(f"  Error processing institution: {e}")
                continue
    
    logger.info(f"  Loaded metadata for {len(institutions):,} company institutions")
    return institutions


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("STAGE 0: PREPARE PUBLICATION INSTITUTIONS")
    logger.info("=" * 80)
    
    # Step 1: Load papers
    logger.info("\n[1/4] Loading papers...")
    if not PAPERS_FILE.exists():
        raise FileNotFoundError(f"Papers file not found: {PAPERS_FILE}")
    
    papers_df = pl.read_parquet(PAPERS_FILE)
    logger.info(f"  Loaded {len(papers_df):,} papers")
    
    # Step 2: Extract institution IDs and count papers
    logger.info("\n[2/4] Extracting institution IDs from papers...")
    institution_counts = extract_institution_ids_from_papers(papers_df)
    logger.info(f"  Found {len(institution_counts):,} unique institutions")
    
    # Step 3: Load institution metadata
    logger.info("\n[3/4] Loading institution metadata...")
    institution_metadata = load_institution_metadata(set(institution_counts.keys()))
    
    # Add paper counts to metadata
    for inst_id, metadata in institution_metadata.items():
        metadata['paper_count'] = institution_counts.get(inst_id, 0)
    
    logger.info(f"  Enriched {len(institution_metadata):,} institutions with metadata")
    
    # Step 4: Create master table with name variants
    logger.info("\n[4/4] Creating master table with name variants...")
    
    master_records = []
    for inst_id, metadata in institution_metadata.items():
        display_name = metadata.get('display_name', '')
        alternative_names = metadata.get('alternative_names', [])
        acronyms = metadata.get('acronyms', [])
        
        # Create name variants
        name_variants = create_name_variants(display_name, alternative_names, acronyms)
        
        # Normalize display name
        normalized_name = clean_organization_name(display_name)
        
        # Extract domain from homepage
        domain = extract_domain(metadata.get('homepage_url'))
        
        master_records.append({
            'institution_id': inst_id,
            'display_name': display_name,
            'normalized_name': normalized_name,
            'alternative_names': alternative_names,
            'acronyms': acronyms,
            'name_variants': name_variants,
            'ror_id': metadata.get('ror_id'),
            'wikidata_id': metadata.get('wikidata_id'),
            'homepage_url': metadata.get('homepage_url'),
            'homepage_domain': domain,
            'country_code': metadata.get('country_code'),
            'geo_city': metadata.get('geo_city'),
            'geo_region': metadata.get('geo_region'),
            'geo_country': metadata.get('geo_country'),
            'paper_count': metadata.get('paper_count', 0),
        })
    
    # Create DataFrame
    master_df = pl.DataFrame(master_records)
    
    # Sort by paper count (descending)
    master_df = master_df.sort('paper_count', descending=True)
    
    logger.info(f"\nMaster table created with {len(master_df):,} institutions")
    logger.info(f"  Total papers: {master_df['paper_count'].sum():,}")
    logger.info(f"  Institutions with ROR: {master_df.filter(pl.col('ror_id').is_not_null()).height:,}")
    logger.info(f"  Institutions with Wikidata: {master_df.filter(pl.col('wikidata_id').is_not_null()).height:,}")
    logger.info(f"  Institutions with homepage: {master_df.filter(pl.col('homepage_url').is_not_null()).height:,}")
    
    # Save
    logger.info(f"\nSaving to: {OUTPUT_FILE}")
    master_df.write_parquet(OUTPUT_FILE)
    
    logger.info("\n" + "=" * 80)
    logger.info("STAGE 0 COMPLETE")
    logger.info("=" * 80)
    
    return master_df


if __name__ == "__main__":
    main()
