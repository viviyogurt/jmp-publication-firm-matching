"""
Compare works_raw Coverage with Current arxiv_index

Check if works_raw has more papers with affiliation data than what we're
currently using from arxiv_index.

Date: 2025
"""

import clickhouse_connect
import polars as pl
from pathlib import Path
import json

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"

# ClickHouse connection
CLICKHOUSE_HOST = "chenlin04.fbe.hku.hk"
CLICKHOUSE_USER = "yannan"
CLICKHOUSE_PASSWORD = "alaniscoolerthanluoye"
CLICKHOUSE_DB = "openalex_claude"


def connect_clickhouse():
    """Connect to ClickHouse"""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        database=CLICKHOUSE_DB,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )


def check_works_raw_coverage(client):
    """Check works_raw for papers with affiliations"""
    print("="*80)
    print("CHECKING works_raw FOR AFFILIATION COVERAGE")
    print("="*80)
    
    # Sample query to check if works_raw has affiliation data
    # We'll check a sample to see if it has authorships with institutions
    
    print("\n1. Sampling works_raw to check for affiliation data...")
    
    # Get a sample of works_raw JSON
    query = """
    SELECT json
    FROM works_raw
    WHERE json LIKE '%authorships%'
      AND json LIKE '%institutions%'
    LIMIT 10
    """
    
    try:
        result = client.query(query)
        samples = result.result_rows
        
        print(f"   Found {len(samples)} samples with authorships and institutions")
        
        if samples:
            # Parse first sample
            sample_json = json.loads(samples[0][0])
            if 'authorships' in sample_json:
                print("\n   Sample work structure:")
                print(f"     ID: {sample_json.get('id', 'N/A')}")
                print(f"     Title: {sample_json.get('title', 'N/A')[:60]}...")
                
                authorships = sample_json.get('authorships', [])
                print(f"     Authorships: {len(authorships)}")
                
                # Count authorships with institutions
                with_inst = sum(1 for a in authorships if a.get('institutions'))
                print(f"     Authorships with institutions: {with_inst}/{len(authorships)}")
                
                if with_inst > 0:
                    # Show example
                    for a in authorships[:2]:
                        if a.get('institutions'):
                            insts = a.get('institutions', [])
                            print(f"       Author: {a.get('author', {}).get('display_name', 'N/A')}")
                            print(f"       Institutions: {[i.get('display_name') for i in insts[:2]]}")
        
    except Exception as e:
        print(f"   Error: {e}")
    
    # Count total works with affiliations
    print("\n2. Counting works with affiliations in works_raw...")
    
    # This is approximate - we'll count works that have "institutions" in the JSON
    query = """
    SELECT COUNT(*) as count
    FROM works_raw
    WHERE json LIKE '%"institutions"%'
      AND json LIKE '%authorships%'
    """
    
    try:
        result = client.query(query)
        count = result.result_rows[0][0]
        print(f"   Works with affiliations: {count:,}")
    except Exception as e:
        print(f"   Error: {e}")
    
    # Compare with arxiv_index
    print("\n3. Comparing with current arxiv_index...")
    
    arxiv_index_path = DATA_RAW / "publication" / "openalex_claude_arxiv_index.parquet"
    if arxiv_index_path.exists():
        arxiv_index = pl.read_parquet(arxiv_index_path)
        print(f"   Current arxiv_index: {len(arxiv_index):,} rows")
        
        # Count papers with institutions in our current data
        # (We'd need to parse JSON, but for now just show row count)
        print(f"   (Note: We extract institutions from arxiv_index.json)")
    else:
        print(f"   arxiv_index file not found at {arxiv_index_path}")
    
    # Check if works_raw has ArXiv papers
    print("\n4. Checking if works_raw contains ArXiv papers...")
    
    query = """
    SELECT COUNT(*) as count
    FROM works_raw
    WHERE json LIKE '%arxiv%'
       OR json LIKE '%arxiv.org%'
    """
    
    try:
        result = client.query(query)
        count = result.result_rows[0][0]
        print(f"   Works mentioning 'arxiv' in JSON: {count:,}")
    except Exception as e:
        print(f"   Error: {e}")


def check_arxiv_index_enhanced(client):
    """Check arxiv_index_enhanced - might have more data"""
    print("\n" + "="*80)
    print("CHECKING arxiv_index_enhanced")
    print("="*80)
    
    # Count rows
    query = "SELECT COUNT(*) as count FROM arxiv_index_enhanced"
    result = client.query(query)
    count = result.result_rows[0][0]
    print(f"\nTotal rows: {count:,}")
    
    # Check if it has more papers than arxiv_index
    arxiv_index_path = DATA_RAW / "publication" / "openalex_claude_arxiv_index.parquet"
    if arxiv_index_path.exists():
        arxiv_index = pl.read_parquet(arxiv_index_path)
        print(f"Current arxiv_index: {len(arxiv_index):,} rows")
        print(f"arxiv_index_enhanced: {count:,} rows")
        print(f"Difference: +{count - len(arxiv_index):,} rows")
    
    # Sample to see structure
    query = """
    SELECT openalex_id, arxiv_id, 
           length(json) as json_length
    FROM arxiv_index_enhanced
    WHERE arxiv_id != ''
    LIMIT 5
    """
    
    try:
        result = client.query(query)
        print("\nSample rows:")
        for row in result.result_rows:
            print(f"  OpenAlex ID: {row[0]}")
            print(f"  ArXiv ID: {row[1]}")
            print(f"  JSON length: {row[2]:,} bytes")
    except Exception as e:
        print(f"Error: {e}")


def main():
    """Main function"""
    print("="*80)
    print("COMPARING CLICKHOUSE TABLES FOR AFFILIATION COVERAGE")
    print("="*80)
    
    client = connect_clickhouse()
    
    # Check works_raw
    check_works_raw_coverage(client)
    
    # Check arxiv_index_enhanced
    check_arxiv_index_enhanced(client)
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)
    print("\nRecommendations:")
    print("1. If works_raw has more papers with affiliations, consider fetching it")
    print("2. If arxiv_index_enhanced has more rows, it might have better coverage")
    print("3. Compare JSON structure to see if there's additional affiliation data")


if __name__ == "__main__":
    main()

