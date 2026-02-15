"""
Comprehensive ClickHouse Database Audit for ArXiv and OpenAlex Data

This script conducts a thorough audit of the available ArXiv and OpenAlex data
to prepare for panel construction. It examines table volumetrics, bridge table
quality, AI definition overlaps, institution quality, and coverage gaps.

Date: 2025
"""

import clickhouse_connect
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOGS_DIR = PROJECT_ROOT / "logs"
OUTPUT_FILE = LOGS_DIR / "data_audit_report.txt"

# Ensure logs directory exists
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ClickHouse connection parameters
CLICKHOUSE_HOST = "chenlin04.fbe.hku.hk"
CLICKHOUSE_USER = "yannan"
CLICKHOUSE_PASSWORD = "alaniscoolerthanluoye"
CLICKHOUSE_DB1 = "claude"
CLICKHOUSE_DB2 = "openalex_claude"

# Target tables
CLAUDE_TABLES = ["arxiv", "arxiv_openalex_linktable", "arxiv_openalex_matches"]
OPENALEX_TABLES = [
    "ai_papers", "works", "institutions", "concepts", 
    "sources", "authors_raw", "funders"
]

# ============================================================================
# Helper Functions
# ============================================================================

def connect_clickhouse(database):
    """Connect to ClickHouse database"""
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            database=database,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        return client
    except Exception as e:
        print(f"Error connecting to {database}: {str(e)}")
        raise


def execute_query(client, query, return_df=True):
    """Execute query and return results as DataFrame or raw result"""
    try:
        if return_df:
            return client.query_df(query)
        else:
            return client.query(query)
    except Exception as e:
        print(f"Query execution error: {str(e)}")
        print(f"Query: {query[:200]}...")
        raise


def write_section(f, title, level=1):
    """Write a formatted section header to file"""
    if level == 1:
        f.write("\n" + "=" * 80 + "\n")
        f.write(f"{title}\n")
        f.write("=" * 80 + "\n\n")
    elif level == 2:
        f.write("\n" + "-" * 80 + "\n")
        f.write(f"{title}\n")
        f.write("-" * 80 + "\n\n")
    elif level == 3:
        f.write(f"\n### {title}\n\n")


def write_dataframe(f, df, max_rows=100):
    """Write DataFrame to file in readable format"""
    if df is None or df.empty:
        f.write("  [No data]\n\n")
        return
    
    # Limit rows for display
    display_df = df.head(max_rows)
    f.write(display_df.to_string(index=False))
    if len(df) > max_rows:
        f.write(f"\n  ... (showing first {max_rows} of {len(df)} rows)\n")
    f.write("\n\n")


# ============================================================================
# Audit Functions
# ============================================================================

def audit_table_volumetrics(f, client_claude, client_openalex):
    """1. Table Volumetrics"""
    write_section(f, "1. TABLE VOLUMETRICS", level=1)
    
    # Claude database tables
    write_section(f, "1.1 Claude Database Tables", level=2)
    for table in CLAUDE_TABLES:
        try:
            query = f"SELECT COUNT(*) as total_rows FROM {CLICKHOUSE_DB1}.{table}"
            result = execute_query(client_claude, query)
            count = result.iloc[0]['total_rows']
            f.write(f"  {table}: {count:,} rows\n")
        except Exception as e:
            f.write(f"  {table}: ERROR - {str(e)}\n")
    
    # OpenAlex database tables
    write_section(f, "1.2 OpenAlex Database Tables", level=2)
    for table in OPENALEX_TABLES:
        try:
            query = f"SELECT COUNT(*) as total_rows FROM {CLICKHOUSE_DB2}.{table}"
            result = execute_query(client_openalex, query)
            count = result.iloc[0]['total_rows']
            f.write(f"  {table}: {count:,} rows\n")
        except Exception as e:
            f.write(f"  {table}: ERROR - {str(e)}\n")
    
    # Publication year distribution for ai_papers
    write_section(f, "1.3 Publication Year Distribution - ai_papers", level=2)
    try:
        query = f"""
        SELECT 
            MIN(publication_year) as min_year,
            MAX(publication_year) as max_year,
            COUNT(*) as total_papers,
            COUNT(DISTINCT publication_year) as distinct_years
        FROM {CLICKHOUSE_DB2}.ai_papers
        WHERE publication_year > 0
        """
        result = execute_query(client_openalex, query)
        f.write("  Summary:\n")
        write_dataframe(f, result, max_rows=10)
        
        # Year-by-year distribution
        query = f"""
        SELECT 
            publication_year,
            COUNT(*) as paper_count
        FROM {CLICKHOUSE_DB2}.ai_papers
        WHERE publication_year > 0
        GROUP BY publication_year
        ORDER BY publication_year DESC
        LIMIT 50
        """
        result = execute_query(client_openalex, query)
        f.write("  Recent Years (last 50):\n")
        write_dataframe(f, result, max_rows=50)
    except Exception as e:
        f.write(f"  ERROR: {str(e)}\n")
    
    # Publication year distribution for works
    write_section(f, "1.4 Publication Year Distribution - works", level=2)
    try:
        query = f"""
        SELECT 
            MIN(publication_year) as min_year,
            MAX(publication_year) as max_year,
            COUNT(*) as total_works,
            COUNT(DISTINCT publication_year) as distinct_years
        FROM {CLICKHOUSE_DB2}.works
        WHERE publication_year > 0
        """
        result = execute_query(client_openalex, query)
        f.write("  Summary:\n")
        write_dataframe(f, result, max_rows=10)
        
        # Year-by-year distribution
        query = f"""
        SELECT 
            publication_year,
            COUNT(*) as work_count
        FROM {CLICKHOUSE_DB2}.works
        WHERE publication_year > 0
        GROUP BY publication_year
        ORDER BY publication_year DESC
        LIMIT 50
        """
        result = execute_query(client_openalex, query)
        f.write("  Recent Years (last 50):\n")
        write_dataframe(f, result, max_rows=50)
    except Exception as e:
        f.write(f"  ERROR: {str(e)}\n")


def audit_bridge_analysis(f, client_claude):
    """2. The Bridge (Linktable) Analysis"""
    write_section(f, "2. THE BRIDGE (LINKTABLE) ANALYSIS", level=1)
    
    # Check if table has data
    try:
        query = f"SELECT COUNT(*) as total_rows FROM {CLICKHOUSE_DB1}.arxiv_openalex_linktable"
        result = execute_query(client_claude, query)
        total_rows = result.iloc[0]['total_rows']
        f.write(f"Total rows in linktable: {total_rows:,}\n\n")
        
        if total_rows == 0:
            f.write("  WARNING: Linktable is EMPTY. Cannot perform further bridge analysis.\n\n")
            return
    except Exception as e:
        f.write(f"  ERROR checking linktable: {str(e)}\n\n")
        return
    
    # Distinct institution types
    write_section(f, "2.1 Distinct Institution Types", level=2)
    try:
        query = f"""
        SELECT 
            institution_type,
            COUNT(*) as count,
            COUNT(DISTINCT institution_name) as distinct_names
        FROM {CLICKHOUSE_DB1}.arxiv_openalex_linktable
        GROUP BY institution_type
        ORDER BY count DESC
        """
        result = execute_query(client_claude, query)
        f.write("All unique institution types:\n")
        write_dataframe(f, result, max_rows=100)
    except Exception as e:
        f.write(f"  ERROR: {str(e)}\n")
    
    # Check for company-related types
    write_section(f, "2.2 Company-Related Institution Types", level=2)
    try:
        query = f"""
        SELECT DISTINCT institution_type
        FROM {CLICKHOUSE_DB1}.arxiv_openalex_linktable
        WHERE LOWER(institution_type) LIKE '%compan%'
           OR LOWER(institution_type) LIKE '%commer%'
           OR LOWER(institution_type) LIKE '%enterpr%'
           OR LOWER(institution_type) LIKE '%corp%'
           OR LOWER(institution_type) LIKE '%business%'
        ORDER BY institution_type
        """
        result = execute_query(client_claude, query)
        f.write("Company-related types found:\n")
        write_dataframe(f, result, max_rows=50)
    except Exception as e:
        f.write(f"  ERROR: {str(e)}\n")
    
    # Check grain/duplicates
    write_section(f, "2.3 Table Grain Analysis (Duplicate Check)", level=2)
    try:
        # Check if unique by arxiv_id + author_name + institution_id
        query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT arxiv_id) as distinct_arxiv_ids,
            COUNT(DISTINCT author_name) as distinct_authors,
            COUNT(DISTINCT institution_id) as distinct_institutions,
            COUNT(DISTINCT tuple(arxiv_id, author_name, institution_id)) as distinct_combinations
        FROM {CLICKHOUSE_DB1}.arxiv_openalex_linktable
        """
        result = execute_query(client_claude, query)
        f.write("Grain analysis:\n")
        write_dataframe(f, result, max_rows=10)
        
        # Check for duplicates
        total_rows = result.iloc[0]['total_rows']
        distinct_combos = result.iloc[0]['distinct_combinations']
        if total_rows > distinct_combos:
            duplicates = total_rows - distinct_combos
            f.write(f"  WARNING: {duplicates:,} duplicate rows found!\n")
            f.write(f"  (Total rows: {total_rows:,}, Unique combinations: {distinct_combos:,})\n\n")
        else:
            f.write(f"  ✓ Table appears to be unique by (arxiv_id, author_name, institution_id)\n\n")
    except Exception as e:
        f.write(f"  ERROR: {str(e)}\n")
    
    # Sample company rows
    write_section(f, "2.4 Sample Rows - Company Institution Type", level=2)
    try:
        query = f"""
        SELECT 
            arxiv_id,
            author_name,
            institution_id,
            institution_name,
            institution_type,
            institution_ror,
            institution_country
        FROM {CLICKHOUSE_DB1}.arxiv_openalex_linktable
        WHERE institution_type = 'company'
        LIMIT 10
        """
        result = execute_query(client_claude, query)
        f.write("Sample rows where institution_type = 'company':\n")
        write_dataframe(f, result, max_rows=10)
    except Exception as e:
        f.write(f"  ERROR: {str(e)}\n")
        # Try case-insensitive
        try:
            query = f"""
            SELECT 
                arxiv_id,
                author_name,
                institution_id,
                institution_name,
                institution_type,
                institution_ror,
                institution_country
            FROM {CLICKHOUSE_DB1}.arxiv_openalex_linktable
            WHERE LOWER(institution_type) LIKE '%compan%'
            LIMIT 10
            """
            result = execute_query(client_claude, query)
            f.write("Sample rows (case-insensitive 'company' search):\n")
            write_dataframe(f, result, max_rows=10)
        except Exception as e2:
            f.write(f"  ERROR (case-insensitive): {str(e2)}\n")


def audit_ai_definition(f, client_openalex):
    """3. The AI Definition Analysis"""
    write_section(f, "3. THE AI DEFINITION ANALYSIS", level=1)
    
    # Correlation/overlap between flags
    write_section(f, "3.1 AI Flag Overlaps and Correlations", level=2)
    try:
        query = f"""
        SELECT 
            COUNT(*) as total_papers,
            SUM(is_deep_learning) as dl_count,
            SUM(is_llm) as llm_count,
            SUM(is_nlp) as nlp_count,
            SUM(CASE WHEN is_deep_learning = 1 AND is_llm = 1 THEN 1 ELSE 0 END) as dl_and_llm,
            SUM(CASE WHEN is_deep_learning = 1 AND is_nlp = 1 THEN 1 ELSE 0 END) as dl_and_nlp,
            SUM(CASE WHEN is_llm = 1 AND is_nlp = 1 THEN 1 ELSE 0 END) as llm_and_nlp,
            SUM(CASE WHEN is_deep_learning = 1 AND is_llm = 1 AND is_nlp = 1 THEN 1 ELSE 0 END) as all_three,
            SUM(CASE WHEN is_deep_learning = 1 OR is_llm = 1 OR is_nlp = 1 THEN 1 ELSE 0 END) as any_flag
        FROM {CLICKHOUSE_DB2}.ai_papers
        """
        result = execute_query(client_openalex, query)
        f.write("Flag overlap analysis:\n")
        write_dataframe(f, result, max_rows=10)
        
        # Calculate percentages
        row = result.iloc[0]
        total = row['total_papers']
        f.write(f"\nPercentages:\n")
        f.write(f"  Deep Learning: {100*row['dl_count']/total:.2f}%\n")
        f.write(f"  LLM: {100*row['llm_count']/total:.2f}%\n")
        f.write(f"  NLP: {100*row['nlp_count']/total:.2f}%\n")
        f.write(f"  DL AND LLM: {100*row['dl_and_llm']/total:.2f}%\n")
        f.write(f"  DL AND NLP: {100*row['dl_and_nlp']/total:.2f}%\n")
        f.write(f"  LLM AND NLP: {100*row['llm_and_nlp']/total:.2f}%\n")
        f.write(f"  All Three: {100*row['all_three']/total:.2f}%\n")
        f.write(f"  Any Flag: {100*row['any_flag']/total:.2f}%\n\n")
    except Exception as e:
        f.write(f"  ERROR: {str(e)}\n")
    
    # Check if ai_papers is subset of works
    write_section(f, "3.2 ai_papers vs works Relationship", level=2)
    try:
        # Check overlap
        query = f"""
        SELECT 
            (SELECT COUNT(DISTINCT openalex_id) FROM {CLICKHOUSE_DB2}.ai_papers) as ai_papers_count,
            (SELECT COUNT(DISTINCT id) FROM {CLICKHOUSE_DB2}.works) as works_count,
            (SELECT COUNT(DISTINCT a.openalex_id) 
             FROM {CLICKHOUSE_DB2}.ai_papers a
             INNER JOIN {CLICKHOUSE_DB2}.works w ON a.openalex_id = w.id) as overlap_count
        """
        result = execute_query(client_openalex, query)
        f.write("Relationship analysis:\n")
        write_dataframe(f, result, max_rows=10)
        
        row = result.iloc[0]
        ai_count = row['ai_papers_count']
        works_count = row['works_count']
        overlap = row['overlap_count']
        
        f.write(f"\nInterpretation:\n")
        f.write(f"  ai_papers has {ai_count:,} distinct openalex_ids\n")
        f.write(f"  works has {works_count:,} distinct ids\n")
        f.write(f"  Overlap: {overlap:,} papers\n")
        
        if overlap == ai_count:
            f.write(f"  ✓ ai_papers appears to be a SUBSET of works\n")
        elif overlap < ai_count:
            missing = ai_count - overlap
            f.write(f"  WARNING: {missing:,} papers in ai_papers are NOT in works\n")
        f.write("\n")
    except Exception as e:
        f.write(f"  ERROR: {str(e)}\n")


def audit_institution_quality(f, client_claude, client_openalex):
    """4. Institution Quality Check"""
    write_section(f, "4. INSTITUTION QUALITY CHECK", level=1)
    
    # Compare display_name quality
    write_section(f, "4.1 Institution Name Quality Comparison", level=2)
    try:
        # Sample from institutions table
        query = f"""
        SELECT 
            id,
            display_name,
            type,
            country_code
        FROM {CLICKHOUSE_DB2}.institutions
        WHERE type = 'company'
        LIMIT 10
        """
        result = execute_query(client_openalex, query)
        f.write("Sample from openalex_claude.institutions (type='company'):\n")
        write_dataframe(f, result, max_rows=10)
        
        # Sample from linktable
        query = f"""
        SELECT 
            institution_id,
            institution_name,
            institution_type,
            institution_country
        FROM {CLICKHOUSE_DB1}.arxiv_openalex_linktable
        WHERE institution_type = 'company'
        LIMIT 10
        """
        result = execute_query(client_claude, query)
        f.write("Sample from claude.arxiv_openalex_linktable (type='company'):\n")
        write_dataframe(f, result, max_rows=10)
    except Exception as e:
        f.write(f"  ERROR: {str(e)}\n")
    
    # Check ROR ID population
    write_section(f, "4.2 ROR ID Population Check", level=2)
    try:
        query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(institution_ror) as ror_populated,
            COUNT(*) - COUNT(institution_ror) as ror_missing,
            100.0 * COUNT(institution_ror) / COUNT(*) as ror_percentage
        FROM {CLICKHOUSE_DB1}.arxiv_openalex_linktable
        """
        result = execute_query(client_claude, query)
        f.write("ROR ID population:\n")
        write_dataframe(f, result, max_rows=10)
    except Exception as e:
        f.write(f"  ERROR: {str(e)}\n")
    
    # Compare institutions vs parsed_institutions
    write_section(f, "4.3 institutions vs parsed_institutions", level=2)
    try:
        query = f"""
        SELECT 
            (SELECT COUNT(*) FROM {CLICKHOUSE_DB2}.institutions) as institutions_count,
            (SELECT COUNT(*) FROM {CLICKHOUSE_DB2}.parsed_institutions) as parsed_count,
            (SELECT COUNT(DISTINCT id) FROM {CLICKHOUSE_DB2}.institutions) as institutions_distinct,
            (SELECT COUNT(DISTINCT id) FROM {CLICKHOUSE_DB2}.parsed_institutions) as parsed_distinct
        """
        result = execute_query(client_openalex, query)
        f.write("Table comparison:\n")
        write_dataframe(f, result, max_rows=10)
    except Exception as e:
        f.write(f"  ERROR: {str(e)}\n")


def audit_coverage_gaps(f, client_claude):
    """5. ArXiv vs OpenAlex Coverage"""
    write_section(f, "5. ARXIV VS OPENALEX COVERAGE", level=1)
    
    # Count unmatched ArXiv papers
    write_section(f, "5.1 Unmatched ArXiv Papers", level=2)
    try:
        # First check if tables have data
        query = f"SELECT COUNT(*) as count FROM {CLICKHOUSE_DB1}.arxiv"
        result = execute_query(client_claude, query)
        arxiv_total = result.iloc[0]['count']
        f.write(f"Total ArXiv papers: {arxiv_total:,}\n\n")
        
        if arxiv_total == 0:
            f.write("  WARNING: ArXiv table is EMPTY.\n\n")
            return
        
        # Count matched vs unmatched
        query = f"""
        SELECT 
            (SELECT COUNT(DISTINCT arxiv_id) FROM {CLICKHOUSE_DB1}.arxiv_openalex_linktable) as matched_count,
            (SELECT COUNT(*) FROM {CLICKHOUSE_DB1}.arxiv) as total_arxiv,
            (SELECT COUNT(*) FROM {CLICKHOUSE_DB1}.arxiv) - 
            (SELECT COUNT(DISTINCT arxiv_id) FROM {CLICKHOUSE_DB1}.arxiv_openalex_linktable) as unmatched_count
        """
        result = execute_query(client_claude, query)
        f.write("Coverage analysis:\n")
        write_dataframe(f, result, max_rows=10)
        
        row = result.iloc[0]
        matched = row['matched_count']
        total = row['total_arxiv']
        unmatched = row['unmatched_count']
        
        if total > 0:
            coverage_pct = 100.0 * matched / total
            f.write(f"\nCoverage:\n")
            f.write(f"  Matched: {matched:,} ({coverage_pct:.2f}%)\n")
            f.write(f"  Unmatched: {unmatched:,} ({100-coverage_pct:.2f}%)\n")
            f.write(f"  Total: {total:,}\n\n")
            
            if unmatched > 0:
                f.write(f"  WARNING: {unmatched:,} ArXiv papers have NO OpenAlex match.\n")
                f.write(f"  These may include corporate papers not in OpenAlex.\n\n")
    except Exception as e:
        f.write(f"  ERROR: {str(e)}\n")


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 80)
    print("ClickHouse Database Audit")
    print("=" * 80)
    print(f"Output file: {OUTPUT_FILE}")
    print()
    
    # Connect to databases
    print("Connecting to ClickHouse databases...")
    try:
        client_claude = connect_clickhouse(CLICKHOUSE_DB1)
        client_openalex = connect_clickhouse(CLICKHOUSE_DB2)
        print("  ✓ Connected successfully\n")
    except Exception as e:
        print(f"  ✗ Connection failed: {str(e)}")
        sys.exit(1)
    
    # Open output file
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        # Write header
        f.write("=" * 80 + "\n")
        f.write("CLICKHOUSE DATABASE AUDIT REPORT\n")
        f.write("ArXiv and OpenAlex Data Assessment\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Database 1: {CLICKHOUSE_DB1}\n")
        f.write(f"Database 2: {CLICKHOUSE_DB2}\n")
        f.write(f"Host: {CLICKHOUSE_HOST}\n")
        f.write("=" * 80 + "\n")
        
        # Run audits
        print("Running audits...")
        print("  1. Table volumetrics...")
        audit_table_volumetrics(f, client_claude, client_openalex)
        
        print("  2. Bridge analysis...")
        audit_bridge_analysis(f, client_claude)
        
        print("  3. AI definition analysis...")
        audit_ai_definition(f, client_openalex)
        
        print("  4. Institution quality check...")
        audit_institution_quality(f, client_claude, client_openalex)
        
        print("  5. Coverage gaps...")
        audit_coverage_gaps(f, client_claude)
        
        # Write footer
        f.write("\n" + "=" * 80 + "\n")
        f.write("END OF AUDIT REPORT\n")
        f.write("=" * 80 + "\n")
    
    print(f"\n✓ Audit complete! Report saved to: {OUTPUT_FILE}")
    print(f"  File size: {OUTPUT_FILE.stat().st_size / 1024:.2f} KB")


if __name__ == "__main__":
    main()

