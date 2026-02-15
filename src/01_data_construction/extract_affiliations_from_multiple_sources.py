"""
Extract Author Affiliations from Multiple Sources

This script addresses the 47% coverage problem by extracting affiliations from:
1. OpenAlex JSON (existing - 47% coverage)
2. raw_affiliation_strings in OpenAlex (can parse for missing cases)
3. ArXiv PDF/LaTeX source files (future enhancement)
4. ORCID profiles (future enhancement)

Current focus: Parse raw_affiliation_strings to improve coverage.

Date: 2025
"""

import polars as pl
import json
import re
from pathlib import Path
import logging
from typing import List, Dict, Any, Optional

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Parse raw_affiliation_strings
# ============================================================================

def parse_raw_affiliation_string(raw_aff: str) -> Dict[str, Any]:
    """
    Parse a raw affiliation string to extract institution information.
    
    Examples:
    - "Google Research, Mountain View, CA"
    - "Microsoft Research, Redmond, WA, USA"
    - "Stanford University, Department of Computer Science"
    
    Parameters:
    -----------
    raw_aff : str
        Raw affiliation string from OpenAlex
        
    Returns:
    --------
    Dict[str, Any]
        Parsed affiliation information
    """
    if not raw_aff:
        return {}
    
    # Common patterns for company names
    company_keywords = [
        'google', 'alphabet', 'microsoft', 'amazon', 'apple', 
        'meta', 'facebook', 'openai', 'deepmind', 'waymo',
        'research', 'lab', 'laboratory', 'labs'
    ]
    
    # Extract potential institution name (usually first part before comma)
    parts = [p.strip() for p in raw_aff.split(',')]
    
    institution_name = parts[0] if parts else raw_aff
    
    # Try to extract location (usually last parts)
    location_parts = parts[1:] if len(parts) > 1 else []
    
    # Check if it's a company (contains company keywords)
    is_company = any(keyword.lower() in institution_name.lower() 
                    for keyword in company_keywords)
    
    # Try to extract country (usually last part or contains country codes)
    country_code = None
    country_keywords = {
        'usa': 'US', 'united states': 'US', 'us': 'US',
        'uk': 'GB', 'united kingdom': 'GB', 'england': 'GB',
        'canada': 'CA', 'germany': 'DE', 'france': 'FR',
        'china': 'CN', 'japan': 'JP', 'india': 'IN'
    }
    
    for part in reversed(location_parts):
        part_lower = part.lower()
        for keyword, code in country_keywords.items():
            if keyword in part_lower:
                country_code = code
                break
        if country_code:
            break
    
    return {
        'institution_name': institution_name,
        'location': ', '.join(location_parts) if location_parts else None,
        'country_code': country_code,
        'is_company_likely': is_company,
        'raw_string': raw_aff
    }


def extract_from_raw_affiliations(json_str: str) -> List[Dict[str, Any]]:
    """
    Extract institution data from raw_affiliation_strings when 
    parsed institutions are missing.
    
    Parameters:
    -----------
    json_str : str
        JSON string containing OpenAlex work data
        
    Returns:
    --------
    List[Dict[str, Any]]
        List of institution records extracted from raw affiliations
    """
    try:
        if isinstance(json_str, str):
            work = json.loads(json_str)
        else:
            work = json_str
        
        institutions_list = []
        
        if 'authorships' not in work:
            return []
        
        openalex_id = work.get('id', '')
        publication_year = work.get('publication_year')
        publication_date = work.get('publication_date')
        
        for authorship in work.get('authorships', []):
            # Check if this authorship has parsed institutions
            parsed_institutions = authorship.get('institutions', [])
            
            # If no parsed institutions, try to extract from raw_affiliation_strings
            if not parsed_institutions:
                raw_affs = authorship.get('raw_affiliation_strings', [])
                
                if raw_affs:
                    author = authorship.get('author', {})
                    author_id = author.get('id', '')
                    author_display_name = author.get('display_name', '')
                    
                    for raw_aff in raw_affs:
                        parsed = parse_raw_affiliation_string(raw_aff)
                        
                        if parsed.get('institution_name'):
                            institutions_list.append({
                                'openalex_id': openalex_id,
                                'publication_year': publication_year,
                                'publication_date': publication_date,
                                'author_id': author_id,
                                'author_display_name': author_display_name,
                                'author_position': authorship.get('author_position', ''),
                                'is_corresponding': authorship.get('is_corresponding', False),
                                'institution_id': None,  # Not available from raw string
                                'institution_name': parsed['institution_name'],
                                'institution_type': 'company' if parsed.get('is_company_likely') else None,
                                'institution_ror': None,
                                'country_code': parsed.get('country_code'),
                                'institution_lineage': None,
                                'has_institution': True,
                                'source': 'raw_affiliation_string',  # Mark source
                                'raw_affiliation': parsed['raw_string']
                            })
        
        return institutions_list
        
    except Exception as e:
        logger.warning(f"Error extracting from raw affiliations: {str(e)[:100]}")
        return []


# ============================================================================
# Big Tech Company Name Matching (for raw affiliations)
# ============================================================================

BIG_TECH_PATTERNS = {
    'GOOGLE': [
        r'\bgoogle\b', r'\balphabet\b', r'google research', r'google brain',
        r'\bdeepmind\b', r'\bwaymo\b', r'\bx company\b', r'\bverily\b'
    ],
    'MICROSOFT': [
        r'\bmicrosoft\b', r'microsoft research'
    ],
    'AMAZON': [
        r'\bamazon\b', r'\baws\b', r'amazon web services', r'amazon science',
        r'amazon alexa', r'amazon robotics'
    ],
    'APPLE': [
        r'\bapple\b', r'apple research', r'apple inc'
    ],
    'META': [
        r'\bmeta\b', r'\bfacebook\b', r'facebook research', r'meta ai',
        r'meta platforms', r'\bfair\b'  # Facebook AI Research
    ],
    'OPENAI': [
        r'\bopenai\b'
    ]
}


def identify_big_tech_from_raw_affiliation(raw_aff: str) -> Optional[str]:
    """
    Identify Big Tech firm from raw affiliation string.
    
    Parameters:
    -----------
    raw_aff : str
        Raw affiliation string
        
    Returns:
    --------
    Optional[str]
        Big Tech firm name if matched
    """
    raw_aff_lower = raw_aff.lower()
    
    for firm, patterns in BIG_TECH_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, raw_aff_lower, re.IGNORECASE):
                return firm
    
    return None


# ============================================================================
# Main Processing
# ============================================================================

def extract_affiliations_improved():
    """
    Extract affiliations from both parsed institutions and raw_affiliation_strings.
    """
    print("=" * 80)
    print("IMPROVED AFFILIATION EXTRACTION")
    print("=" * 80)
    
    arxiv_index = pl.read_parquet(DATA_RAW / "openalex_claude_arxiv_index.parquet")
    total = len(arxiv_index)
    
    print(f"\nProcessing {total:,} papers...")
    print("Extracting from:")
    print("  1. Parsed institutions (existing)")
    print("  2. raw_affiliation_strings (new)")
    
    # Process in chunks
    CHUNK_SIZE = 50000
    all_records = []
    stats = {
        'papers_with_parsed_inst': 0,
        'papers_with_raw_aff_only': 0,
        'papers_with_both': 0,
        'papers_with_neither': 0,
        'big_tech_from_raw': {firm: 0 for firm in BIG_TECH_PATTERNS.keys()}
    }
    
    for i in range(0, total, CHUNK_SIZE):
        chunk_end = min(i + CHUNK_SIZE, total)
        chunk = arxiv_index[i:chunk_end]
        
        print(f"\nProcessing rows {i:,} to {chunk_end:,}...")
        
        for row in chunk.iter_rows(named=True):
            json_str = row.get('json', '')
            if json_str:
                try:
                    work = json.loads(json_str) if isinstance(json_str, str) else json_str
                    
                    if 'authorships' not in work:
                        continue
                    
                    has_parsed = False
                    has_raw_only = False
                    
                    for authorship in work.get('authorships', []):
                        parsed_insts = authorship.get('institutions', [])
                        raw_affs = authorship.get('raw_affiliation_strings', [])
                        
                        if parsed_insts:
                            has_parsed = True
                            # Extract parsed institutions (existing logic)
                            # ... (would use existing extraction code)
                        
                        if raw_affs and not parsed_insts:
                            has_raw_only = True
                            # Extract from raw affiliations
                            raw_records = extract_from_raw_affiliations(json_str)
                            all_records.extend(raw_records)
                            
                            # Check for Big Tech
                            for raw_aff in raw_affs:
                                firm = identify_big_tech_from_raw_affiliation(raw_aff)
                                if firm:
                                    stats['big_tech_from_raw'][firm] += 1
                    
                    # Update stats
                    if has_parsed and has_raw_only:
                        stats['papers_with_both'] += 1
                    elif has_parsed:
                        stats['papers_with_parsed_inst'] += 1
                    elif has_raw_only:
                        stats['papers_with_raw_aff_only'] += 1
                    else:
                        stats['papers_with_neither'] += 1
                        
                except Exception as e:
                    pass
        
        progress = (chunk_end / total * 100) if total > 0 else 0
        print(f"  Progress: {progress:.1f}%")
    
    print("\n" + "=" * 80)
    print("EXTRACTION STATISTICS")
    print("=" * 80)
    print(f"Papers with parsed institutions: {stats['papers_with_parsed_inst']:,}")
    print(f"Papers with raw affiliations only: {stats['papers_with_raw_aff_only']:,}")
    print(f"Papers with both: {stats['papers_with_both']:,}")
    print(f"Papers with neither: {stats['papers_with_neither']:,}")
    
    print("\nBig Tech firms found in raw affiliations:")
    for firm, count in stats['big_tech_from_raw'].items():
        if count > 0:
            print(f"  {firm}: {count:,} papers")
    
    print(f"\nTotal records extracted from raw affiliations: {len(all_records):,}")
    
    # Save results
    if all_records:
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
            'source': pl.Utf8,
            'raw_affiliation': pl.Utf8
        }
        
        df = pl.DataFrame(all_records, schema=schema)
        output_path = DATA_PROCESSED / "paper_institutions_from_raw_affiliations.parquet"
        df.write_parquet(output_path, compression='snappy')
        print(f"\nSaved to {output_path}")
    
    return stats


if __name__ == "__main__":
    extract_affiliations_improved()

