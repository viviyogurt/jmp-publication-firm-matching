"""
Check ClickHouse Tables for Additional Affiliation Data

This script queries ClickHouse to see if there are additional tables with
affiliation data that we're not currently using:
- authors_raw
- institutions_raw
- parsed_institutions
- works_raw
- arxiv_index_enhanced

Date: 2025
"""

import clickhouse_connect
from pathlib import Path
import json

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOGS_DIR = PROJECT_ROOT / "logs"
OUTPUT_FILE = LOGS_DIR / "clickhouse_affiliation_analysis.txt"

LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ClickHouse connection parameters
CLICKHOUSE_HOST = "chenlin04.fbe.hku.hk"
CLICKHOUSE_USER = "yannan"
CLICKHOUSE_PASSWORD = "alaniscoolerthanluoye"
CLICKHOUSE_DB = "openalex_claude"

# Tables to investigate
TABLES_TO_CHECK = [
    "authors_raw",
    "institutions_raw",
    "parsed_institutions",
    "works_raw",
    "arxiv_index_enhanced"
]


def connect_clickhouse():
    """Connect to ClickHouse database"""
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            database=CLICKHOUSE_DB,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        return client
    except Exception as e:
        print(f"Error connecting to ClickHouse: {str(e)}")
        raise


def get_table_structure(client, table_name):
    """Get table structure"""
    query = f"DESCRIBE TABLE {table_name}"
    result = client.query(query)
    # DESCRIBE returns: name, type, default_type, default_expression, comment, codec_expression, ttl_expression
    return [(row[0], row[1]) for row in result.result_rows]  # Just name and type


def get_table_sample(client, table_name, limit=1):
    """Get sample rows from table"""
    query = f"SELECT * FROM {table_name} LIMIT {limit}"
    try:
        result = client.query(query)
        # Get column names
        columns = [col[0] for col in result.column_names]
        rows = result.result_rows
        if rows:
            # Return as dict
            return dict(zip(columns, rows[0]))
        return None
    except Exception as e:
        return f"Error: {str(e)}"


def get_table_row_count(client, table_name):
    """Get row count for table"""
    query = f"SELECT COUNT(*) as count FROM {table_name}"
    result = client.query(query)
    return result.result_rows[0][0] if result.result_rows else 0


def check_affiliation_columns(client, table_name):
    """Check if table has affiliation-related columns"""
    query = f"""
    SELECT name, type 
    FROM system.columns 
    WHERE database = '{CLICKHOUSE_DB}' 
      AND table = '{table_name}' 
      AND (
        name LIKE '%affiliation%' OR 
        name LIKE '%institution%' OR 
        name LIKE '%org%' OR 
        name LIKE '%company%' OR
        name LIKE '%author%' OR
        name LIKE '%json%'
      )
    ORDER BY name
    """
    result = client.query(query)
    return result.result_rows


def analyze_table(client, table_name, f):
    """Analyze a table for affiliation data"""
    print(f"\n{'='*80}")
    print(f"Analyzing: {table_name}")
    print(f"{'='*80}")
    f.write(f"\n{'='*80}\n")
    f.write(f"Table: {table_name}\n")
    f.write(f"{'='*80}\n")
    
    # Check if table exists
    try:
        row_count = get_table_row_count(client, table_name)
        print(f"Row count: {row_count:,}")
        f.write(f"Row count: {row_count:,}\n")
    except Exception as e:
        print(f"Error getting row count: {e}")
        f.write(f"Error: {e}\n")
        return
    
    # Get table structure
    print("\nTable structure:")
    f.write("\nTable structure:\n")
    try:
        structure = get_table_structure(client, table_name)
        for col_name, col_type in structure[:20]:  # Limit to first 20 columns
            print(f"  {col_name}: {col_type}")
            f.write(f"  {col_name}: {col_type}\n")
        if len(structure) > 20:
            print(f"  ... and {len(structure) - 20} more columns")
            f.write(f"  ... and {len(structure) - 20} more columns\n")
    except Exception as e:
        print(f"Error getting structure: {e}")
        f.write(f"Error: {e}\n")
    
    # Check for affiliation-related columns
    print("\nAffiliation-related columns:")
    f.write("\nAffiliation-related columns:\n")
    try:
        aff_columns = check_affiliation_columns(client, table_name)
        if aff_columns:
            for col_name, col_type in aff_columns:
                print(f"  {col_name}: {col_type}")
                f.write(f"  {col_name}: {col_type}\n")
        else:
            print("  None found")
            f.write("  None found\n")
    except Exception as e:
        print(f"Error checking columns: {e}")
        f.write(f"Error: {e}\n")
    
    # Get sample data
    print("\nSample data (first row, key columns only):")
    f.write("\nSample data (first row, key columns only):\n")
    try:
        sample = get_table_sample(client, table_name, limit=1)
        if isinstance(sample, str):
            print(f"  {sample}")
            f.write(f"  {sample}\n")
        elif sample:
            # Show key columns
            key_cols = ['id', 'arxiv_id', 'openalex_id', 'json', 'raw_json']
            shown = {}
            for col in key_cols:
                if col in sample:
                    val = sample[col]
                    if col == 'json' or col == 'raw_json':
                        # Show JSON structure, not full content
                        if isinstance(val, str):
                            try:
                                json_obj = json.loads(val)
                                if isinstance(json_obj, dict):
                                    shown[col] = f"JSON with keys: {list(json_obj.keys())[:10]}"
                                else:
                                    shown[col] = f"JSON (length: {len(val)})"
                            except:
                                shown[col] = f"String (length: {len(val)})"
                        else:
                            shown[col] = str(val)[:100]
                    else:
                        shown[col] = str(val)[:200]
            
            if shown:
                for col, val in shown.items():
                    print(f"  {col}: {val}")
                    f.write(f"  {col}: {val}\n")
            else:
                # Show all columns (limited)
                for col, val in list(sample.items())[:5]:
                    print(f"  {col}: {str(val)[:100]}")
                    f.write(f"  {col}: {str(val)[:200]}\n")
        else:
            print("  No data")
            f.write("  No data\n")
    except Exception as e:
        print(f"Error getting sample: {e}")
        f.write(f"Error: {e}\n")


def main():
    """Main analysis function"""
    print("="*80)
    print("CLICKHOUSE AFFILIATION DATA ANALYSIS")
    print("="*80)
    print(f"Database: {CLICKHOUSE_DB}")
    print(f"Host: {CLICKHOUSE_HOST}")
    print(f"Tables to check: {', '.join(TABLES_TO_CHECK)}")
    
    # Connect to ClickHouse
    print("\nConnecting to ClickHouse...")
    try:
        client = connect_clickhouse()
        print("Connected successfully!")
    except Exception as e:
        print(f"Failed to connect: {e}")
        return
    
    # Analyze each table
    with open(OUTPUT_FILE, 'w') as f:
        f.write("="*80 + "\n")
        f.write("CLICKHOUSE AFFILIATION DATA ANALYSIS\n")
        f.write("="*80 + "\n")
        f.write(f"Database: {CLICKHOUSE_DB}\n")
        f.write(f"Host: {CLICKHOUSE_HOST}\n")
        f.write(f"Generated: {Path(__file__).stat().st_mtime}\n")
        
        for table_name in TABLES_TO_CHECK:
            try:
                analyze_table(client, table_name, f)
            except Exception as e:
                print(f"\nError analyzing {table_name}: {e}")
                f.write(f"\nError analyzing {table_name}: {e}\n")
    
    print(f"\n{'='*80}")
    print(f"Analysis complete! Results saved to: {OUTPUT_FILE}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()

