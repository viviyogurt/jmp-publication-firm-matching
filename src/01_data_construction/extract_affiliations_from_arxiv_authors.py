"""
Extract Affiliations from ArXiv Authors Field

According to ArXiv API documentation, authors can include affiliations in the
authors field using parentheses: "Author Name (Institution Name)".

This script parses the authors field in our ArXiv data to extract affiliations
that are embedded in the author names.

Reference: https://info.arxiv.org/help/api/user-manual.html
The <arxiv:affiliation> element is a subelement of <author> if present.

Date: 2025
"""

import polars as pl
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

# Input files
ARXIV_KAGGLE_PATH = DATA_RAW / "arxiv_kaggle.parquet"
ARXIV_2021_2025_PATH = DATA_RAW / "arxiv_2021_2025.parquet"

# Output files
ARXIV_AFFILIATIONS_OUTPUT = DATA_PROCESSED / "arxiv_affiliations_from_authors.parquet"

# Ensure output directory exists
DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Parsing Functions
# ============================================================================

def parse_author_with_affiliation(author_str: str) -> Dict[str, Any]:
    """
    Parse author string that may contain affiliation in parentheses.
    
    Examples:
    - "John Doe (Google Research)" -> name="John Doe", affiliation="Google Research"
    - "Jane Smith" -> name="Jane Smith", affiliation=None
    - "Bob Lee (MIT) and Alice Chen (Stanford)" -> multiple authors
    
    Parameters:
    -----------
    author_str : str
        Author string that may contain affiliations
        
    Returns:
    --------
    Dict[str, Any]
        Parsed author information
    """
    if not author_str:
        return {'name': None, 'affiliation': None}
    
    # Pattern: "Name (Affiliation)" or "Name, Affiliation" or just "Name"
    # Try to extract name and affiliation
    affiliation = None
    name = author_str.strip()
    
    # Check for parentheses pattern: "Name (Affiliation)"
    paren_match = re.search(r'^(.+?)\s*\(([^)]+)\)\s*$', author_str)
    if paren_match:
        name = paren_match.group(1).strip()
        affiliation = paren_match.group(2).strip()
    else:
        # Check for comma pattern: "Name, Affiliation" (less common)
        # But be careful - names can have commas too
        # Only use if it looks like an institution (contains keywords)
        parts = [p.strip() for p in author_str.split(',')]
        if len(parts) >= 2:
            # Check if last part looks like an institution
            last_part = parts[-1].lower()
            institution_keywords = ['university', 'institute', 'lab', 'laboratory', 
                                  'college', 'school', 'research', 'company', 'corp', 'inc']
            if any(kw in last_part for kw in institution_keywords):
                name = ', '.join(parts[:-1])
                affiliation = parts[-1]
    
    return {
        'name': name if name else None,
        'affiliation': affiliation if affiliation else None
    }


def extract_affiliations_from_authors_field(authors_str: str, arxiv_id: str) -> List[Dict[str, Any]]:
    """
    Extract author-affiliation pairs from the authors field.
    
    The authors field can contain multiple authors separated by:
    - "and" (e.g., "John Doe (MIT) and Jane Smith (Stanford)")
    - Commas (e.g., "John Doe (MIT), Jane Smith (Stanford)")
    - Semicolons
    
    Parameters:
    -----------
    authors_str : str
        Authors field string
    arxiv_id : str
        ArXiv ID for this paper
        
    Returns:
    --------
    List[Dict[str, Any]]
        List of author-affiliation records
    """
    if not authors_str:
        return []
    
    records = []
    
    # Split by common separators: "and", ", and", ";"
    # Handle patterns like "Author1 (Aff1) and Author2 (Aff2)"
    # or "Author1, Author2 (Shared Affiliation)"
    
    # First, try to split by " and " (most common)
    if ' and ' in authors_str.lower():
        author_parts = re.split(r'\s+and\s+', authors_str, flags=re.IGNORECASE)
    elif ', and ' in authors_str:
        author_parts = re.split(r',\s+and\s+', authors_str, flags=re.IGNORECASE)
    else:
        # Try comma separation (but be careful - names can have commas)
        # Only split if there are parentheses (indicating affiliations)
        if '(' in authors_str:
            # Split by comma, but keep parentheses together
            author_parts = re.split(r',\s*(?=[^()]*(?:\(|$))', authors_str)
        else:
            author_parts = [authors_str]
    
    for author_part in author_parts:
        author_part = author_part.strip()
        if not author_part:
            continue
        
        parsed = parse_author_with_affiliation(author_part)
        
        if parsed['name']:
            records.append({
                'arxiv_id': arxiv_id,
                'author_name': parsed['name'],
                'affiliation': parsed['affiliation'],
                'has_affiliation': parsed['affiliation'] is not None,
                'source': 'arxiv_authors_field'
            })
    
    return records


def identify_big_tech_from_affiliation(affiliation: str) -> Optional[str]:
    """
    Identify Big Tech firm from affiliation string.
    
    Parameters:
    -----------
    affiliation : str
        Affiliation string
        
    Returns:
    --------
    Optional[str]
        Big Tech firm name if matched
    """
    if not affiliation:
        return None
    
    affiliation_lower = affiliation.lower()
    
    big_tech_patterns = {
        'GOOGLE': [
            r'\bgoogle\b', r'\balphabet\b', r'google research', r'google brain',
            r'\bdeepmind\b', r'\bwaymo\b'
        ],
        'MICROSOFT': [
            r'\bmicrosoft\b', r'microsoft research'
        ],
        'AMAZON': [
            r'\bamazon\b', r'\baws\b', r'amazon web services', r'amazon science'
        ],
        'APPLE': [
            r'\bapple\b', r'apple research', r'apple inc'
        ],
        'META': [
            r'\bmeta\b', r'\bfacebook\b', r'facebook research', r'meta ai',
            r'meta platforms'
        ],
        'OPENAI': [
            r'\bopenai\b'
        ]
    }
    
    for firm, patterns in big_tech_patterns.items():
        for pattern in patterns:
            if re.search(pattern, affiliation_lower, re.IGNORECASE):
                return firm
    
    return None


# ============================================================================
# Main Processing
# ============================================================================

def extract_affiliations_from_arxiv_data():
    """
    Extract affiliations from ArXiv authors field.
    """
    print("=" * 80)
    print("EXTRACTING AFFILIATIONS FROM ARXIV AUTHORS FIELD")
    print("=" * 80)
    
    all_records = []
    files_to_process = []
    
    # Check which files exist
    if ARXIV_KAGGLE_PATH.exists():
        files_to_process.append(('arxiv_kaggle', ARXIV_KAGGLE_PATH))
    if ARXIV_2021_2025_PATH.exists():
        files_to_process.append(('arxiv_2021_2025', ARXIV_2021_2025_PATH))
    
    if not files_to_process:
        logger.error("No ArXiv data files found!")
        return
    
    for file_name, file_path in files_to_process:
        print(f"\nProcessing {file_name}...")
        df = pl.read_parquet(file_path)
        print(f"  Loaded {len(df):,} papers")
        
        # Process in chunks
        CHUNK_SIZE = 50000
        total = len(df)
        
        for i in range(0, total, CHUNK_SIZE):
            chunk_end = min(i + CHUNK_SIZE, total)
            chunk = df[i:chunk_end]
            
            print(f"  Processing rows {i:,} to {chunk_end:,}...")
            
            for row in chunk.iter_rows(named=True):
                arxiv_id = row.get('arxiv_id', '')
                authors = row.get('authors', '')
                
                if authors:
                    records = extract_affiliations_from_authors_field(authors, arxiv_id)
                    all_records.extend(records)
            
            progress = (chunk_end / total * 100) if total > 0 else 0
            print(f"    Progress: {progress:.1f}%")
    
    if not all_records:
        logger.warning("No affiliation records extracted!")
        return
    
    # Convert to DataFrame
    print(f"\nTotal records extracted: {len(all_records):,}")
    
    # Add Big Tech identification
    print("Identifying Big Tech firms...")
    for record in all_records:
        if record['affiliation']:
            firm = identify_big_tech_from_affiliation(record['affiliation'])
            record['big_tech_firm'] = firm
    
    # Create DataFrame
    schema = {
        'arxiv_id': pl.Utf8,
        'author_name': pl.Utf8,
        'affiliation': pl.Utf8,
        'has_affiliation': pl.Boolean,
        'source': pl.Utf8,
        'big_tech_firm': pl.Utf8
    }
    
    df_affiliations = pl.DataFrame(all_records, schema=schema)
    
    # Statistics
    print("\n" + "=" * 80)
    print("EXTRACTION STATISTICS")
    print("=" * 80)
    
    total_authors = len(df_affiliations)
    authors_with_aff = df_affiliations.filter(pl.col('has_affiliation') == True).height
    unique_papers = df_affiliations.select('arxiv_id').n_unique()
    papers_with_aff = df_affiliations.filter(pl.col('has_affiliation') == True).select('arxiv_id').n_unique()
    
    print(f"Total author records: {total_authors:,}")
    print(f"Authors with affiliations: {authors_with_aff:,} ({authors_with_aff/total_authors*100:.1f}%)")
    print(f"Unique papers: {unique_papers:,}")
    print(f"Papers with at least one affiliation: {papers_with_aff:,} ({papers_with_aff/unique_papers*100:.1f}%)")
    
    # Big Tech statistics
    big_tech = df_affiliations.filter(pl.col('big_tech_firm').is_not_null())
    if len(big_tech) > 0:
        print(f"\nBig Tech firms found: {len(big_tech):,} author records")
        big_tech_summary = big_tech.group_by('big_tech_firm').agg([
            pl.len().alias('author_count'),
            pl.n_unique('arxiv_id').alias('paper_count')
        ]).sort('author_count', descending=True)
        print("\nBig Tech breakdown:")
        print(big_tech_summary)
    
    # Save results
    print(f"\nSaving to {ARXIV_AFFILIATIONS_OUTPUT}...")
    df_affiliations.write_parquet(ARXIV_AFFILIATIONS_OUTPUT, compression='snappy')
    
    file_size_mb = ARXIV_AFFILIATIONS_OUTPUT.stat().st_size / (1024 * 1024)
    print(f"  Saved successfully ({file_size_mb:.2f} MB)")
    
    print("\n" + "=" * 80)
    print("Extraction completed!")
    print("=" * 80)
    
    return df_affiliations


if __name__ == "__main__":
    extract_affiliations_from_arxiv_data()

