"""
Improve Institution Extraction and Matching

This script addresses two problems:
1. Investigate why only 47% of papers have institution data
2. Improve matching to capture research divisions and subsidiaries

Date: 2025
"""

import polars as pl
import json
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
# Problem 1: Investigate Missing Institution Data
# ============================================================================

def investigate_missing_institutions():
    """
    Investigate why only 47% of papers have institution data.
    Check various reasons for missing data.
    """
    print("=" * 80)
    print("PROBLEM 1: INVESTIGATING MISSING INSTITUTION DATA")
    print("=" * 80)
    
    arxiv_index = pl.read_parquet(DATA_RAW / "openalex_claude_arxiv_index.parquet")
    total_papers = len(arxiv_index)
    
    print(f"\nTotal papers in arxiv_index: {total_papers:,}")
    
    # Analyze a sample
    sample_size = 10000
    sample = arxiv_index.head(sample_size)
    
    stats = {
        'has_json': 0,
        'no_json': 0,
        'has_authorships': 0,
        'no_authorships': 0,
        'authorships_empty': 0,
        'authorships_with_institutions': 0,
        'authorships_without_institutions': 0,
        'total_authorships': 0,
        'authorships_with_inst_count': 0
    }
    
    for row in sample.iter_rows(named=True):
        json_str = row.get('json', '')
        if not json_str:
            stats['no_json'] += 1
            continue
        
        stats['has_json'] += 1
        
        try:
            work = json.loads(json_str) if isinstance(json_str, str) else json_str
            
            if 'authorships' not in work:
                stats['no_authorships'] += 1
                continue
            
            authorships = work.get('authorships', [])
            if not authorships:
                stats['authorships_empty'] += 1
                continue
            
            stats['has_authorships'] += 1
            stats['total_authorships'] += len(authorships)
            
            has_any_institution = False
            for authorship in authorships:
                institutions = authorship.get('institutions', [])
                if institutions and len(institutions) > 0:
                    has_any_institution = True
                    stats['authorships_with_inst_count'] += len(institutions)
            
            if has_any_institution:
                stats['authorships_with_institutions'] += 1
            else:
                stats['authorships_without_institutions'] += 1
                
        except Exception as e:
            logger.warning(f"Error processing JSON: {str(e)[:50]}")
    
    print(f"\nAnalysis of {sample_size:,} sample papers:")
    print(f"  Has JSON: {stats['has_json']:,} ({stats['has_json']/sample_size*100:.1f}%)")
    print(f"  No JSON: {stats['no_json']:,}")
    print(f"  Has authorships: {stats['has_authorships']:,} ({stats['has_authorships']/sample_size*100:.1f}%)")
    print(f"  No authorships field: {stats['no_authorships']:,}")
    print(f"  Empty authorships array: {stats['authorships_empty']:,}")
    print(f"  Papers with institutions: {stats['authorships_with_institutions']:,} ({stats['authorships_with_institutions']/sample_size*100:.1f}%)")
    print(f"  Papers without institutions: {stats['authorships_without_institutions']:,} ({stats['authorships_without_institutions']/sample_size*100:.1f}%)")
    print(f"  Total authorships: {stats['total_authorships']:,}")
    print(f"  Total institution records: {stats['authorships_with_inst_count']:,}")
    
    print("\n" + "=" * 80)
    print("CONCLUSION:")
    print("=" * 80)
    if stats['authorships_without_institutions'] > stats['authorships_with_institutions']:
        print("Most papers have authorships but NO institutions in the institutions array.")
        print("This is a data quality issue in OpenAlex - institution data is missing.")
        print("\nPossible reasons:")
        print("  1. Authors didn't provide affiliation information")
        print("  2. OpenAlex couldn't extract/parse affiliation data")
        print("  3. Papers are too old (affiliation data less available)")
        print("  4. Papers from certain sources don't have affiliation metadata")
    else:
        print("Most papers have institution data available.")
    
    return stats


# ============================================================================
# Problem 2: Improved Research Division Matching
# ============================================================================

# Comprehensive mapping of research divisions to parent companies
RESEARCH_DIVISION_MAPPING = {
    # Google/Alphabet
    'GOOGLE': [
        'google', 'alphabet', 'google inc', 'alphabet inc',
        'google research', 'google brain', 'deepmind', 'waymo',
        'x company', 'x development', 'verily', 'calico',
        'google ai', 'google cloud', 'google llc'
    ],
    # Microsoft
    'MICROSOFT': [
        'microsoft', 'microsoft corporation', 'microsoft corp',
        'microsoft research', 'microsoft research new england',
        'microsoft research asia', 'microsoft research india',
        'microsoft azure', 'microsoft ai'
    ],
    # Amazon
    'AMAZON': [
        'amazon', 'amazon.com', 'amazon.com inc',
        'amazon web services', 'aws', 'amazon science',
        'amazon alexa', 'amazon robotics', 'amazon lab126'
    ],
    # Apple
    'APPLE': [
        'apple', 'apple inc', 'apple computer',
        'apple research', 'apple ai/ml research'
    ],
    # Meta/Facebook
    'META': [
        'meta platforms', 'meta platforms inc', 'facebook',
        'facebook inc', 'facebook research', 'meta ai',
        'meta research', 'facebook ai research', 'fair'
    ],
    # OpenAI
    'OPENAI': [
        'openai', 'openai inc', 'openai lp'
    ]
}


def check_lineage_for_parent(inst: Dict, target_firm: str) -> bool:
    """
    Check if institution lineage contains parent company.
    
    Parameters:
    -----------
    inst : Dict
        Institution dictionary
    target_firm : str
        Target firm name to match
        
    Returns:
    --------
    bool
        True if lineage contains target firm
    """
    lineage = inst.get('lineage', [])
    if not lineage:
        return False
    
    target_patterns = RESEARCH_DIVISION_MAPPING.get(target_firm, [])
    
    for lineage_item in lineage:
        if isinstance(lineage_item, dict):
            lineage_name = lineage_item.get('display_name', '').lower()
            for pattern in target_patterns:
                if pattern in lineage_name:
                    return True
        elif isinstance(lineage_item, str):
            lineage_name = lineage_item.lower()
            for pattern in target_patterns:
                if pattern in lineage_name:
                    return True
    
    return False


def identify_big_tech_with_lineage(inst_name: str, inst: Dict) -> Optional[str]:
    """
    Identify Big Tech firm using institution name and lineage.
    
    Parameters:
    -----------
    inst_name : str
        Institution display name
    inst : Dict
        Full institution dictionary with lineage
        
    Returns:
    --------
    Optional[str]
        Big Tech firm name if matched, None otherwise
    """
    inst_name_lower = inst_name.lower()
    
    # Check each firm's patterns
    for firm, patterns in RESEARCH_DIVISION_MAPPING.items():
        for pattern in patterns:
            # Direct name match
            if pattern in inst_name_lower:
                return firm
            
            # Check lineage for parent company
            if check_lineage_for_parent(inst, firm):
                return firm
    
    return None


def find_research_divisions_in_data():
    """
    Find all research divisions and subsidiaries in the data.
    """
    print("\n" + "=" * 80)
    print("PROBLEM 2: FINDING RESEARCH DIVISIONS")
    print("=" * 80)
    
    arxiv_index = pl.read_parquet(DATA_RAW / "openalex_claude_arxiv_index.parquet")
    
    # Process a larger sample
    sample_size = 100000
    sample = arxiv_index.head(sample_size)
    
    found_institutions = {firm: {} for firm in RESEARCH_DIVISION_MAPPING.keys()}
    
    print(f"\nProcessing {sample_size:,} papers to find research divisions...")
    
    for row in sample.iter_rows(named=True):
        json_str = row.get('json', '')
        if json_str:
            try:
                work = json.loads(json_str) if isinstance(json_str, str) else json_str
                if 'authorships' in work:
                    for authorship in work.get('authorships', []):
                        for inst in authorship.get('institutions', []):
                            inst_name = inst.get('display_name', '')
                            if inst_name:
                                # Use improved matching
                                matched_firm = identify_big_tech_with_lineage(inst_name, inst)
                                if matched_firm:
                                    if inst_name not in found_institutions[matched_firm]:
                                        found_institutions[matched_firm][inst_name] = {
                                            'type': inst.get('type', ''),
                                            'country': inst.get('country_code', ''),
                                            'lineage': inst.get('lineage', [])
                                        }
            except Exception as e:
                pass
    
    print(f"\nFound research divisions and subsidiaries:\n")
    for firm, institutions in found_institutions.items():
        if institutions:
            print(f"{firm} ({len(institutions)} unique institutions):")
            for inst_name, details in sorted(institutions.items())[:20]:
                lineage_str = ""
                if details['lineage']:
                    lineage_names = [item.get('display_name', '') if isinstance(item, dict) else str(item) 
                                   for item in details['lineage']]
                    lineage_str = f" (lineage: {', '.join(lineage_names[:2])})"
                print(f"  - {inst_name} [{details['type']}, {details['country']}]{lineage_str}")
            if len(institutions) > 20:
                print(f"  ... and {len(institutions) - 20} more")
            print()
    
    return found_institutions


def create_improved_matching_function():
    """
    Create an improved matching function that uses lineage.
    """
    print("\n" + "=" * 80)
    print("CREATING IMPROVED MATCHING FUNCTION")
    print("=" * 80)
    
    code = '''
def identify_big_tech_improved(institution_name: str, institution_dict: Dict) -> Optional[str]:
    """
    Improved Big Tech identification using name and lineage.
    
    Parameters:
    -----------
    institution_name : str
        Institution display name
    institution_dict : Dict
        Full institution dictionary with lineage
        
    Returns:
    --------
    Optional[str]
        Big Tech firm name if matched, None otherwise
    """
    inst_name_lower = institution_name.lower()
    
    # Research division mapping
    RESEARCH_DIVISION_MAPPING = {
        'GOOGLE': [
            'google', 'alphabet', 'google inc', 'alphabet inc',
            'google research', 'google brain', 'deepmind', 'waymo',
            'x company', 'x development', 'verily', 'calico',
            'google ai', 'google cloud', 'google llc'
        ],
        'MICROSOFT': [
            'microsoft', 'microsoft corporation', 'microsoft corp',
            'microsoft research', 'microsoft research new england',
            'microsoft research asia', 'microsoft research india',
            'microsoft azure', 'microsoft ai'
        ],
        'AMAZON': [
            'amazon', 'amazon.com', 'amazon.com inc',
            'amazon web services', 'aws', 'amazon science',
            'amazon alexa', 'amazon robotics', 'amazon lab126'
        ],
        'APPLE': [
            'apple', 'apple inc', 'apple computer',
            'apple research', 'apple ai/ml research'
        ],
        'META': [
            'meta platforms', 'meta platforms inc', 'facebook',
            'facebook inc', 'facebook research', 'meta ai',
            'meta research', 'facebook ai research', 'fair'
        ],
        'OPENAI': [
            'openai', 'openai inc', 'openai lp'
        ]
    }
    
    # Check direct name match
    for firm, patterns in RESEARCH_DIVISION_MAPPING.items():
        for pattern in patterns:
            if pattern in inst_name_lower:
                return firm
    
    # Check lineage for parent company
    lineage = institution_dict.get('lineage', [])
    if lineage:
        for lineage_item in lineage:
            if isinstance(lineage_item, dict):
                lineage_name = lineage_item.get('display_name', '').lower()
            elif isinstance(lineage_item, str):
                lineage_name = lineage_item.lower()
            else:
                continue
                
            for firm, patterns in RESEARCH_DIVISION_MAPPING.items():
                for pattern in patterns:
                    if pattern in lineage_name:
                        return firm
    
    return None
'''
    
    print("\nImproved matching function code:")
    print(code)
    print("\nThis function:")
    print("  1. Checks institution name against all research division patterns")
    print("  2. Checks lineage/parent relationships")
    print("  3. Returns the matched Big Tech firm name")
    
    return code


# ============================================================================
# Main
# ============================================================================

def main():
    """Run both investigations."""
    # Problem 1
    stats = investigate_missing_institutions()
    
    # Problem 2
    found_institutions = find_research_divisions_in_data()
    
    # Create improved matching
    matching_code = create_improved_matching_function()
    
    print("\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print("\n1. For missing institution data (47% coverage):")
    print("   - This is a limitation of OpenAlex data quality")
    print("   - Cannot be fixed without additional data sources")
    print("   - Consider using author profiles or other metadata")
    print("   - Focus analysis on papers WITH institution data")
    
    print("\n2. For research division matching:")
    print("   - Use the improved matching function with lineage checking")
    print("   - Update extract_institutions_from_json.py to use lineage")
    print("   - This should capture more papers from Big Tech firms")
    print("   - Found", sum(len(v) for v in found_institutions.values()), "unique institutions across all Big Tech firms")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()

