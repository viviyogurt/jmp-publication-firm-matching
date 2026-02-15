"""
Fetch All Publication Data from ClickHouse Databases

This script retrieves all tables from the ClickHouse databases (claude and openalex_claude)
and saves each table as a Parquet file in the project data folder.

The script dynamically discovers all tables using SHOW TABLES and handles large datasets
efficiently using streaming and chunked processing. It includes retry logic with exponential
backoff and automatically skips already-fetched tables.

Date: 2025
"""

import clickhouse_connect
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
import sys
import logging
import time
import argparse
import json
from typing import List, Tuple, Optional

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"

# Ensure output directory exists
DATA_RAW.mkdir(parents=True, exist_ok=True)

# ClickHouse connection parameters
CLICKHOUSE_HOST = "chenlin04.fbe.hku.hk"
CLICKHOUSE_USER = "yannan"
CLICKHOUSE_PASSWORD = "alaniscoolerthanluoye"
CLICKHOUSE_DB1 = "claude"
CLICKHOUSE_DB2 = "openalex_claude"

# Chunk size for large table processing (rows per chunk)
CHUNK_SIZE = 500000  # 500k rows per chunk for memory efficiency
CHUNK_SIZE_SMALL = 25000  # 25k rows for problematic tables like ai_papers
CHUNK_SIZE_VERY_LARGE = 100000  # 100k rows for very large tables like arxiv_index_enhanced
CHUNK_SIZE_EXTRA_SMALL = 10000  # 10k rows for tables with very large JSON columns

# Retry configuration
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 1  # seconds
MAX_RETRY_DELAY = 60  # seconds
RETRY_BACKOFF_MULTIPLIER = 2

# Global columns to exclude from all tables
GLOBAL_EXCLUDE_COLUMNS = ["full_json"]

# Tables that need special handling (beyond global excludes)
SPECIAL_TABLES = {
    "openalex_claude.ai_papers": {
        "exclude_columns": [],  # Will be merged with global excludes
        "chunk_size": CHUNK_SIZE_SMALL
    },
    "openalex_claude.arxiv_index_enhanced": {
        "exclude_columns": [],  # Will be merged with global excludes
        "chunk_size": 25000,  # 25k rows - middle ground: avoids memory errors while keeping connection count reasonable
        "chunk_delay": 20.0,  # 20s delay between chunks - longer to prevent connection exhaustion with more chunks
        "connection_check_interval": 10,  # Check connection every 10 chunks
        # "max_memory_usage": "53687091200"  # Limit memory per query to 50GiB (in bytes) to avoid server memory limits
    }
}

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress clickhouse_connect library's internal warnings
# These are just noise from the library catching and re-raising HTTP exceptions
# We handle errors properly in our code, so we don't need the library's warnings
logging.getLogger('clickhouse_connect').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)


# ============================================================================
# Helper Functions
# ============================================================================

def connect_clickhouse(database: str, retry_count: int = 0):
    """
    Connect to ClickHouse database with retry logic.
    
    Parameters:
    -----------
    database : str
        Database name
    retry_count : int
        Current retry attempt number
        
    Returns:
    --------
    clickhouse_connect client
        Active ClickHouse client connection
        
    Raises:
    -------
    ConnectionError
        If connection fails after retries
    """
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            database=database,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        return client
    except Exception as e:
        if retry_count < MAX_RETRIES:
            delay = min(INITIAL_RETRY_DELAY * (RETRY_BACKOFF_MULTIPLIER ** retry_count), MAX_RETRY_DELAY)
            logger.warning(f"Connection attempt {retry_count + 1} failed for {database}: {str(e)}")
            logger.info(f"  Retrying in {delay} seconds...")
            time.sleep(delay)
            return connect_clickhouse(database, retry_count + 1)
        else:
            logger.error(f"Error connecting to {database} after {MAX_RETRIES} attempts: {str(e)}")
            raise ConnectionError(f"Could not connect to {database}: {str(e)}")


def ensure_connection(client, database: str):
    """
    Ensure ClickHouse connection is still alive, reconnect if needed.
    
    Parameters:
    -----------
    client : clickhouse_connect client
        Current client connection
    database : str
        Database name
        
    Returns:
    --------
    clickhouse_connect client
        Active client connection (may be new if reconnected)
    """
    try:
        # Try a simple query to test connection
        client.query("SELECT 1")
        return client
    except Exception as e:
        logger.warning(f"Connection lost for {database}, reconnecting...")
        try:
            client.close()
        except:
            pass
        return connect_clickhouse(database)


def retry_with_backoff(func, *args, max_retries: int = MAX_RETRIES, **kwargs):
    """
    Execute a function with exponential backoff retry logic.
    
    Parameters:
    -----------
    func : callable
        Function to execute
    *args : tuple
        Positional arguments for function
    max_retries : int
        Maximum number of retry attempts
    **kwargs : dict
        Keyword arguments for function
        
    Returns:
    --------
    Any
        Return value of the function
        
    Raises:
    -------
    Exception
        If function fails after all retries
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            error_str = str(e)
            
            # Check if it's a connection error
            is_connection_error = any(keyword in error_str.lower() for keyword in [
                'connection refused', 'connection reset', 'connection closed',
                'connection error', 'connection pool', 'failed to establish'
            ])
            
            # Check if it's a memory error (don't retry)
            is_memory_error = 'memory limit exceeded' in error_str.lower() or '241' in error_str
            
            if is_memory_error:
                logger.error(f"Memory limit error - not retrying: {error_str[:200]}")
                raise
            
            if attempt < max_retries:
                delay = min(INITIAL_RETRY_DELAY * (RETRY_BACKOFF_MULTIPLIER ** attempt), MAX_RETRY_DELAY)
                logger.warning(f"Attempt {attempt + 1}/{max_retries + 1} failed: {error_str[:200]}")
                if is_connection_error:
                    logger.info(f"  Connection error detected, waiting {delay} seconds before retry...")
                else:
                    logger.info(f"  Waiting {delay} seconds before retry...")
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries + 1} attempts failed")
                raise
    
    raise last_exception


def get_all_tables(client, database: str) -> List[str]:
    """
    Get list of all tables in a database using SHOW TABLES.
    
    Parameters:
    -----------
    client : clickhouse_connect client
        ClickHouse client connection
    database : str
        Database name
        
    Returns:
    --------
    List[str]
        List of table names
    """
    def _get_tables():
        query = f"SHOW TABLES FROM {database}"
        result = client.query(query)
        return [row[0] for row in result.result_rows]
    
    try:
        tables = retry_with_backoff(_get_tables)
        logger.info(f"  Found {len(tables)} tables in {database}")
        return tables
    except Exception as e:
        logger.error(f"Error getting tables from {database}: {str(e)}")
        raise


def get_table_count(client, database: str, table: str) -> int:
    """
    Get row count for a table.
    
    Parameters:
    -----------
    client : clickhouse_connect client
        ClickHouse client connection
    database : str
        Database name
    table : str
        Table name
        
    Returns:
    --------
    int
        Number of rows in the table
    """
    def _get_count():
        query = f"SELECT COUNT(*) as total_rows FROM {database}.{table}"
        result = client.query_df(query)
        return int(result.iloc[0]['total_rows'])
    
    try:
        return retry_with_backoff(_get_count)
    except Exception as e:
        logger.warning(f"Could not get count for {database}.{table}: {str(e)}")
        return 0


def get_table_columns(client, database: str, table: str) -> List[str]:
    """
    Get list of all columns in a table.
    
    Parameters:
    -----------
    client : clickhouse_connect client
        ClickHouse client connection
    database : str
        Database name
    table : str
        Table name
        
    Returns:
    --------
    List[str]
        List of column names
    """
    def _get_columns():
        query = f"DESCRIBE TABLE {database}.{table}"
        schema_df = client.query_df(query)
        return schema_df['name'].tolist()
    
    try:
        return retry_with_backoff(_get_columns)
    except Exception as e:
        logger.warning(f"Could not get columns for {database}.{table}: {str(e)}")
        return []


def get_table_schema(client, database: str, table: str) -> pd.DataFrame:
    """
    Get table schema information.
    
    Parameters:
    -----------
    client : clickhouse_connect client
        ClickHouse client connection
    database : str
        Database name
    table : str
        Table name
        
    Returns:
    --------
    pd.DataFrame
        DataFrame with column information
    """
    def _get_schema():
        query = f"DESCRIBE TABLE {database}.{table}"
        return client.query_df(query)
    
    try:
        return retry_with_backoff(_get_schema)
    except Exception as e:
        logger.warning(f"Could not get schema for {database}.{table}: {str(e)}")
        return pd.DataFrame()


def build_select_query(database: str, table: str, exclude_columns: Optional[List[str]] = None) -> str:
    """
    Build SELECT query, optionally excluding specified columns.
    
    Parameters:
    -----------
    database : str
        Database name
    table : str
        Table name
    exclude_columns : List[str], optional
        List of column names to exclude from SELECT
        
    Returns:
    --------
    str
        SELECT query string
    """
    if exclude_columns:
        # Get all columns and exclude specified ones
        # We'll use a subquery approach or get columns first
        # For now, use SELECT * and filter in pandas, or get columns dynamically
        # Actually, better to get columns and build explicit SELECT
        return f"SELECT * FROM {database}.{table}"  # Will be handled in fetch functions
    else:
        return f"SELECT * FROM {database}.{table}"


def get_table_special_config(database: str, table: str) -> dict:
    """
    Get special configuration for a table if it exists.
    Merges global exclude columns with table-specific excludes.
    
    Parameters:
    -----------
    database : str
        Database name
    table : str
        Table name
        
    Returns:
    --------
    dict
        Special configuration dict with merged exclude_columns
    """
    key = f"{database}.{table}"
    config = SPECIAL_TABLES.get(key, {}).copy()
    
    # Merge global exclude columns with table-specific excludes
    table_excludes = config.get("exclude_columns", [])
    all_excludes = list(set(GLOBAL_EXCLUDE_COLUMNS + table_excludes))
    config["exclude_columns"] = all_excludes
    
    return config


def file_exists_and_valid(output_path: Path) -> bool:
    """
    Check if output file exists and is valid (non-empty).
    
    Parameters:
    -----------
    output_path : Path
        Path to output file
        
    Returns:
    --------
    bool
        True if file exists and is valid, False otherwise
    """
    if not output_path.exists():
        return False
    
    # Check if file is non-empty (at least has schema)
    try:
        file_size = output_path.stat().st_size
        if file_size == 0:
            return False
        # Try to read parquet metadata to ensure it's valid
        parquet_file = pq.ParquetFile(output_path)
        if parquet_file.metadata.num_rows == 0:
            # Empty but valid schema - consider it as successfully fetched
            return True
        return True
    except Exception as e:
        logger.warning(f"File {output_path} exists but may be corrupted: {str(e)}")
        return False


def fetch_table_chunked_streaming(
    client, 
    database: str, 
    table: str, 
    output_path: Path,
    chunk_size: int = CHUNK_SIZE,
    exclude_columns: Optional[List[str]] = None
) -> int:
    """
    Fetch large table in chunks using streaming and save to Parquet.
    This method is memory-efficient and handles very large tables.
    
    Parameters:
    -----------
    client : clickhouse_connect client
        ClickHouse client connection
    database : str
        Database name
    table : str
        Table name
    output_path : Path
        Output Parquet file path
    chunk_size : int
        Number of rows per chunk
    exclude_columns : List[str], optional
        List of columns to exclude from fetch
        
    Returns:
    --------
    int
        Total number of rows fetched
    """
    logger.info(f"  Using chunked streaming fetch (chunk_size={chunk_size:,})...")
    if exclude_columns:
        logger.info(f"  Excluding columns: {', '.join(exclude_columns)}")
    
    def _get_count():
        return get_table_count(client, database, table)
    
    total_count = retry_with_backoff(_get_count)
    
    if total_count == 0:
        logger.warning(f"  Table is empty")
        # Create empty parquet file with schema
        try:
            query = build_select_query(database, table, exclude_columns)
            df_empty = retry_with_backoff(lambda: client.query_df(query))
            df_empty.to_parquet(
                output_path,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            logger.info(f"  ✓ Saved empty table with schema")
        except Exception as e:
            logger.error(f"  ✗ Error creating empty table: {str(e)}")
        return 0
    
    logger.info(f"  Table has {total_count:,} rows")
    
    # Get schema and columns
    schema_df = get_table_schema(client, database, table)
    all_columns = schema_df['name'].tolist() if not schema_df.empty else []
    
    # Filter out excluded columns
    if exclude_columns:
        # Only exclude columns that actually exist in the table
        existing_excludes = [col for col in exclude_columns if col in all_columns]
        select_columns = [col for col in all_columns if col not in existing_excludes]
        if not select_columns:
            raise ValueError(f"All columns would be excluded for {database}.{table}")
        if existing_excludes:
            logger.info(f"  Selecting {len(select_columns)} columns (excluding {len(existing_excludes)}: {', '.join(existing_excludes)})")
        else:
            select_columns = None  # No columns to exclude, use SELECT *
    else:
        select_columns = None  # Use SELECT *
    
    # Use ClickHouse's streaming query capability
    parquet_writer = None
    total_rows_fetched = 0
    chunk_num = 0
    
    try:
        # Try to identify a suitable column for cursor-based pagination
        cursor_column = None
        if not schema_df.empty:
            # Look for an indexed or primary key column
            for col_name in schema_df['name']:
                col_lower = col_name.lower()
                if any(keyword in col_lower for keyword in ['id', 'key', 'uuid', 'index']):
                    cursor_column = col_name
                    break
            
            # If no ID column found, use first column
            if cursor_column is None and len(schema_df) > 0:
                cursor_column = schema_df.iloc[0]['name']
        
        # Get special config for chunk delay and connection checks
        special_config = get_table_special_config(database, table)
        chunk_delay = special_config.get("chunk_delay", 0.0)
        connection_check_interval = special_config.get("connection_check_interval", 0)
        max_memory_usage = special_config.get("max_memory_usage")
        
        if cursor_column:
            logger.info(f"  Using cursor-based pagination with column: {cursor_column}")
            if chunk_delay > 0:
                logger.info(f"  Using {chunk_delay}s delay between chunks")
            if connection_check_interval > 0:
                logger.info(f"  Connection health check every {connection_check_interval} chunks")
            if max_memory_usage:
                logger.info(f"  Query memory limit: {max_memory_usage}")
            total_rows_fetched = fetch_with_cursor(
                client, database, table, output_path, cursor_column, 
                chunk_size, total_count, select_columns,
                chunk_delay=chunk_delay,
                connection_check_interval=connection_check_interval,
                max_memory_usage=max_memory_usage
            )
        else:
            logger.info(f"  Using OFFSET-based pagination")
            if chunk_delay > 0:
                logger.info(f"  Using {chunk_delay}s delay between chunks")
            if connection_check_interval > 0:
                logger.info(f"  Connection health check every {connection_check_interval} chunks")
            if max_memory_usage:
                logger.info(f"  Query memory limit: {max_memory_usage}")
            total_rows_fetched = fetch_with_offset(
                client, database, table, output_path, chunk_size, 
                total_count, select_columns,
                chunk_delay=chunk_delay,
                connection_check_interval=connection_check_interval,
                max_memory_usage=max_memory_usage
            )
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"  ✓ Saved successfully ({file_size_mb:.2f} MB, {total_rows_fetched:,} rows)")
        
        return total_rows_fetched
        
    except Exception as e:
        logger.error(f"  ✗ Error in chunked streaming fetch: {str(e)}")
        # Clean up partial file
        if output_path.exists():
            output_path.unlink()
        raise


def fetch_with_cursor(
    client, 
    database: str, 
    table: str, 
    output_path: Path,
    cursor_column: str,
    chunk_size: int,
    total_count: int,
    select_columns: Optional[List[str]] = None,
    chunk_delay: float = 0.0,
    connection_check_interval: int = 0,
    max_memory_usage: Optional[str] = None
) -> int:
    """
    Fetch table using cursor-based pagination (more efficient for large tables).
    
    Parameters:
    -----------
    chunk_delay : float
        Delay in seconds between chunks (for very large tables)
    connection_check_interval : int
        Check connection health every N chunks (0 = disabled)
    """
    parquet_writer = None
    total_rows = 0
    chunk_num = 0
    last_value = None
    
    # Build column list for SELECT
    # Ensure cursor_column is included if we're selecting specific columns
    if select_columns:
        if cursor_column not in select_columns:
            select_columns = [cursor_column] + select_columns
        columns_str = ", ".join(select_columns)
        select_clause = f"SELECT {columns_str}"
    else:
        select_clause = "SELECT *"
    
    try:
        # Set memory limit if specified
        if max_memory_usage:
            try:
                # ClickHouse expects memory in bytes or with quotes for human-readable format
                # Try with quotes first (e.g., '50Gi'), fallback to direct value
                if any(c.isalpha() for c in max_memory_usage):
                    # Contains letters (e.g., '50GB', '50Gi'), need quotes
                    client.command(f"SET max_memory_usage = '{max_memory_usage}'")
                else:
                    # Pure number, use directly
                    client.command(f"SET max_memory_usage = {max_memory_usage}")
                logger.info(f"    Set query memory limit to {max_memory_usage}")
            except Exception as e:
                logger.warning(f"    Could not set memory limit: {e}")
                # Continue without memory limit - query might still work
        
        while True:
            # Build query with cursor
            if last_value is None:
                # First chunk - order by cursor column
                query = f"""
                    {select_clause} FROM {database}.{table}
                    ORDER BY {cursor_column}
                    LIMIT {chunk_size}
                """
            else:
                # Subsequent chunks - use WHERE clause with cursor
                # Determine if cursor is numeric or string
                type_check_query = f"SELECT {cursor_column} FROM {database}.{table} WHERE {cursor_column} IS NOT NULL LIMIT 1"
                try:
                    sample = retry_with_backoff(lambda: client.query_df(type_check_query))
                    if len(sample) > 0:
                        sample_val = sample.iloc[0][cursor_column]
                        if isinstance(sample_val, (int, float)):
                            query = f"""
                                {select_clause} FROM {database}.{table}
                                WHERE {cursor_column} > {last_value}
                                ORDER BY {cursor_column}
                                LIMIT {chunk_size}
                            """
                        else:
                            query = f"""
                                {select_clause} FROM {database}.{table}
                                WHERE {cursor_column} > '{last_value}'
                                ORDER BY {cursor_column}
                                LIMIT {chunk_size}
                            """
                    else:
                        break
                except:
                    break
            
            # Periodic connection health check
            if connection_check_interval > 0 and chunk_num > 0 and chunk_num % connection_check_interval == 0:
                logger.info(f"    Performing connection health check...")
                client = ensure_connection(client, database)
            
            # Fetch chunk with retry
            def _fetch_chunk():
                # Ensure connection is alive
                nonlocal client
                client = ensure_connection(client, database)
                return client.query_df(query)
            
            df_chunk = retry_with_backoff(_fetch_chunk)
            
            if len(df_chunk) == 0:
                break
            
            # Write chunk to parquet
            if parquet_writer is None:
                # First chunk - create parquet writer
                table_schema = pa.Table.from_pandas(df_chunk).schema
                parquet_writer = pq.ParquetWriter(output_path, table_schema, compression='snappy')
            
            # Convert to PyArrow table and write
            table_chunk = pa.Table.from_pandas(df_chunk)
            parquet_writer.write_table(table_chunk)
            
            total_rows += len(df_chunk)
            chunk_num += 1
            
            # Update cursor value
            last_value = df_chunk.iloc[-1][cursor_column]
            
            progress_pct = (total_rows / total_count * 100) if total_count > 0 else 0
            logger.info(f"    Chunk {chunk_num}: {total_rows:,}/{total_count:,} rows ({progress_pct:.1f}%)")
            
            # If we got fewer rows than chunk_size, we're done
            if len(df_chunk) < chunk_size:
                break
            
            # Add delay between chunks for very large tables
            if chunk_delay > 0:
                time.sleep(chunk_delay)
        
        # Close parquet writer
        if parquet_writer is not None:
            parquet_writer.close()
        
        return total_rows
        
    except Exception as e:
        if parquet_writer is not None:
            try:
                parquet_writer.close()
            except:
                pass
        raise


def fetch_with_offset(
    client, 
    database: str, 
    table: str, 
    output_path: Path,
    chunk_size: int,
    total_count: int,
    select_columns: Optional[List[str]] = None,
    chunk_delay: float = 0.0,
    connection_check_interval: int = 0,
    max_memory_usage: Optional[str] = None
) -> int:
    """
    Fetch table using OFFSET-based pagination (fallback method).
    
    Parameters:
    -----------
    chunk_delay : float
        Delay in seconds between chunks (for very large tables)
    connection_check_interval : int
        Check connection health every N chunks (0 = disabled)
    """
    parquet_writer = None
    total_rows = 0
    chunk_num = 0
    offset = 0
    
    # Build column list for SELECT
    if select_columns:
        columns_str = ", ".join(select_columns)
        select_clause = f"SELECT {columns_str}"
    else:
        select_clause = "SELECT *"
    
    try:
        # Set memory limit if specified
        if max_memory_usage:
            try:
                # ClickHouse expects memory in bytes or with quotes for human-readable format
                # Try with quotes first (e.g., '50Gi'), fallback to direct value
                if any(c.isalpha() for c in max_memory_usage):
                    # Contains letters (e.g., '50GB', '50Gi'), need quotes
                    client.command(f"SET max_memory_usage = '{max_memory_usage}'")
                else:
                    # Pure number, use directly
                    client.command(f"SET max_memory_usage = {max_memory_usage}")
                logger.info(f"    Set query memory limit to {max_memory_usage}")
            except Exception as e:
                logger.warning(f"    Could not set memory limit: {e}")
                # Continue without memory limit - query might still work
        
        while offset < total_count:
            query = f"""
                {select_clause} FROM {database}.{table}
                LIMIT {chunk_size} OFFSET {offset}
            """
            
            # Periodic connection health check
            if connection_check_interval > 0 and chunk_num > 0 and chunk_num % connection_check_interval == 0:
                logger.info(f"    Performing connection health check...")
                client = ensure_connection(client, database)
            
            # Fetch chunk with retry
            def _fetch_chunk():
                # Ensure connection is alive
                nonlocal client
                client = ensure_connection(client, database)
                return client.query_df(query)
            
            df_chunk = retry_with_backoff(_fetch_chunk)
            
            if len(df_chunk) == 0:
                break
            
            # Write chunk to parquet
            if parquet_writer is None:
                # First chunk - create parquet writer
                table_schema = pa.Table.from_pandas(df_chunk).schema
                parquet_writer = pq.ParquetWriter(output_path, table_schema, compression='snappy')
            
            # Convert to PyArrow table and write
            table_chunk = pa.Table.from_pandas(df_chunk)
            parquet_writer.write_table(table_chunk)
            
            total_rows += len(df_chunk)
            chunk_num += 1
            offset += chunk_size
            
            progress_pct = (total_rows / total_count * 100) if total_count > 0 else 0
            logger.info(f"    Chunk {chunk_num}: {total_rows:,}/{total_count:,} rows ({progress_pct:.1f}%)")
            
            # If we got fewer rows than chunk_size, we're done
            if len(df_chunk) < chunk_size:
                break
            
            # Add delay between chunks for very large tables
            if chunk_delay > 0:
                time.sleep(chunk_delay)
        
        # Close parquet writer
        if parquet_writer is not None:
            parquet_writer.close()
        
        return total_rows
        
    except Exception as e:
        if parquet_writer is not None:
            try:
                parquet_writer.close()
            except:
                pass
        raise


def fetch_table_to_parquet(
    client, 
    database: str, 
    table: str, 
    output_path: Path,
    use_chunked: bool = True
) -> int:
    """
    Fetch all data from a ClickHouse table and save to Parquet.
    Automatically chooses between single fetch and chunked streaming based on table size.
    
    Parameters:
    -----------
    client : clickhouse_connect client
        ClickHouse client connection
    database : str
        Database name
    table : str
        Table name
    output_path : Path
        Output Parquet file path
    use_chunked : bool
        Whether to use chunked streaming (recommended for large tables)
        
    Returns:
    --------
    int
        Number of rows fetched
    """
    logger.info(f"Fetching {database}.{table}...")
    
    # Check if file already exists and is valid
    if file_exists_and_valid(output_path):
        logger.info(f"  ✓ File already exists and is valid, skipping: {output_path.name}")
        try:
            # Get row count from existing file
            parquet_file = pq.ParquetFile(output_path)
            existing_rows = parquet_file.metadata.num_rows
            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"    Existing file: {existing_rows:,} rows, {file_size_mb:.2f} MB")
            return existing_rows
        except:
            # If we can't read it, treat as invalid and re-fetch
            logger.warning(f"  File exists but cannot be read, will re-fetch")
    
    try:
        # Get special configuration for this table
        special_config = get_table_special_config(database, table)
        exclude_columns = special_config.get("exclude_columns")
        table_chunk_size = special_config.get("chunk_size", CHUNK_SIZE)
        
        # Get table size
        def _get_count():
            return get_table_count(client, database, table)
        
        row_count = retry_with_backoff(_get_count)
        logger.info(f"  Table has {row_count:,} rows")
        
        # Decide on fetch strategy
        if use_chunked and row_count > 1000000:
            logger.info(f"  Using chunked streaming (table is large)")
            return fetch_table_chunked_streaming(
                client, database, table, output_path, 
                chunk_size=table_chunk_size,
                exclude_columns=exclude_columns
            )
        elif use_chunked and row_count > 0:
            logger.info(f"  Using chunked streaming (recommended)")
            return fetch_table_chunked_streaming(
                client, database, table, output_path,
                chunk_size=table_chunk_size,
                exclude_columns=exclude_columns
            )
        else:
            # For small or empty tables, fetch all at once
            logger.info(f"  Fetching all data at once...")
            
            # Build query
            if exclude_columns:
                # Get columns and exclude specified ones
                all_columns = get_table_columns(client, database, table)
                # Only exclude columns that actually exist
                existing_excludes = [col for col in exclude_columns if col in all_columns]
                select_columns = [col for col in all_columns if col not in existing_excludes]
                if select_columns:
                    columns_str = ", ".join(select_columns)
                    query = f"SELECT {columns_str} FROM {database}.{table}"
                else:
                    query = f"SELECT * FROM {database}.{table}"
            else:
                query = f"SELECT * FROM {database}.{table}"
            
            def _fetch_all():
                return client.query_df(query)
            
            df = retry_with_backoff(_fetch_all)
            
            if len(df) == 0:
                logger.warning(f"  Table is empty (0 rows)")
                df.to_parquet(
                    output_path,
                    engine='pyarrow',
                    compression='snappy',
                    index=False
                )
                logger.info(f"  ✓ Saved empty table with schema")
                return 0
            
            logger.info(f"  Retrieved {len(df):,} rows, {len(df.columns)} columns")
            
            # Save to Parquet
            logger.info(f"  Saving to {output_path}...")
            df.to_parquet(
                output_path,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"  ✓ Saved successfully ({file_size_mb:.2f} MB)")
            
            return len(df)
        
    except Exception as e:
        logger.error(f"  ✗ Error fetching {database}.{table}: {str(e)}")
        # Clean up partial file
        if output_path.exists():
            try:
                output_path.unlink()
            except:
                pass
        raise


def fetch_single_table(database: str, table: str):
    """Fetch a single table from ClickHouse"""
    logger.info("=" * 80)
    logger.info(f"Fetching {database}.{table} from ClickHouse")
    logger.info("=" * 80)
    logger.info(f"Output directory: {DATA_RAW}")
    logger.info("")
    
    # Connect to database
    logger.info(f"Connecting to {database} database...")
    try:
        client = connect_clickhouse(database)
        logger.info("  ✓ Connected successfully\n")
    except Exception as e:
        logger.error(f"  ✗ Connection failed: {str(e)}")
        sys.exit(1)
    
    try:
        output_file = DATA_RAW / f"{database}_{table}.parquet"
        
        # Check if file already exists
        if file_exists_and_valid(output_file):
            logger.info(f"  ✓ File already exists and is valid, skipping: {output_file.name}")
            try:
                parquet_file = pq.ParquetFile(output_file)
                existing_rows = parquet_file.metadata.num_rows
                file_size_mb = output_file.stat().st_size / (1024 * 1024)
                logger.info(f"    Existing file: {existing_rows:,} rows, {file_size_mb:.2f} MB")
                return
            except:
                logger.warning(f"  File exists but cannot be read, will re-fetch")
        
        # Ensure connection is alive before fetching
        client = ensure_connection(client, database)
        
        rows = fetch_table_to_parquet(client, database, table, output_file)
        logger.info("")
        
        # Add delay after very large table fetches to allow server recovery
        if rows > 10000000:  # 10M+ rows
            logger.info("  Adding extended delay after very large table fetch...")
            time.sleep(10)
        elif rows > 1000000:  # 1M+ rows
            logger.info("  Adding delay after large table fetch...")
            time.sleep(5)
        
    except Exception as e:
        logger.error(f"  ✗ Failed to fetch {database}.{table}: {str(e)}")
        raise
    finally:
        # Close connection
        try:
            client.close()
            logger.info("Connection closed")
        except:
            pass
    
    # Summary
    logger.info("=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Successfully fetched: {database}.{table}")
    logger.info(f"Output file: {output_file}")
    logger.info("=" * 80)


def fetch_all_tables():
    """Fetch all tables from both databases"""
    logger.info("=" * 80)
    logger.info("Fetching Publication Data from ClickHouse")
    logger.info("=" * 80)
    logger.info(f"Output directory: {DATA_RAW}")
    logger.info("")
    
    # Connect to databases
    logger.info("Connecting to ClickHouse databases...")
    try:
        client_claude = connect_clickhouse(CLICKHOUSE_DB1)
        client_openalex = connect_clickhouse(CLICKHOUSE_DB2)
        logger.info("  ✓ Connected successfully\n")
    except Exception as e:
        logger.error(f"  ✗ Connection failed: {str(e)}")
        sys.exit(1)
    
    all_tables = []
    tables_processed = 0
    tables_skipped = 0
    tables_failed = []
    
    try:
        # Get all tables from Claude database
        logger.info(f"Discovering tables in {CLICKHOUSE_DB1} database...")
        claude_tables = get_all_tables(client_claude, CLICKHOUSE_DB1)
        all_tables.extend([(CLICKHOUSE_DB1, table, client_claude) for table in claude_tables])
        logger.info("")
        
        # Get all tables from OpenAlex database
        logger.info(f"Discovering tables in {CLICKHOUSE_DB2} database...")
        openalex_tables = get_all_tables(client_openalex, CLICKHOUSE_DB2)
        all_tables.extend([(CLICKHOUSE_DB2, table, client_openalex) for table in openalex_tables])
        logger.info("")
        
        total_tables = len(all_tables)
        logger.info(f"Total tables discovered: {total_tables}\n")
        
        # Fetch each table
        logger.info("=" * 80)
        logger.info("Fetching Tables")
        logger.info("=" * 80)
        
        for database, table, client in all_tables:
            try:
                output_file = DATA_RAW / f"{database}_{table}.parquet"
                
                # Check if file already exists
                if file_exists_and_valid(output_file):
                    tables_skipped += 1
                    continue
                
                # Ensure connection is alive before fetching
                if database == CLICKHOUSE_DB1:
                    client = ensure_connection(client, CLICKHOUSE_DB1)
                else:
                    client = ensure_connection(client, CLICKHOUSE_DB2)
                
                rows = fetch_table_to_parquet(client, database, table, output_file)
                tables_processed += 1
                logger.info("")
                
                # Add delay after large table fetches to allow server recovery
                if rows > 1000000:
                    logger.info("  Adding delay after large table fetch...")
                    time.sleep(5)
                
            except Exception as e:
                logger.error(f"  Failed to fetch {database}.{table}: {str(e)}\n")
                tables_failed.append(f"{database}.{table}")
        
    finally:
        # Close connections
        try:
            client_claude.close()
            client_openalex.close()
            logger.info("Connections closed")
        except:
            pass
    
    # Summary
    logger.info("=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total tables discovered: {len(all_tables)}")
    logger.info(f"Successfully processed: {tables_processed}")
    logger.info(f"Skipped (already exist): {tables_skipped}")
    logger.info(f"Failed: {len(tables_failed)}")
    
    if tables_failed:
        logger.warning("Failed tables:")
        for table in tables_failed:
            logger.warning(f"  - {table}")
    
    logger.info("")
    logger.info(f"Output directory: {DATA_RAW}")
    logger.info("=" * 80)


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description="Fetch publication data from ClickHouse databases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch all tables from both databases
  python fetch_publication_data.py
  
  # Fetch only arxiv_index_enhanced table
  python fetch_publication_data.py --only-arxiv-index-enhanced
  
  # Fetch a specific table
  python fetch_publication_data.py --table openalex_claude.arxiv_index_enhanced
        """
    )
    
    parser.add_argument(
        "--only-arxiv-index-enhanced",
        action="store_true",
        help="Fetch only the arxiv_index_enhanced table (optimized for very large tables)"
    )
    
    parser.add_argument(
        "--table",
        type=str,
        help="Fetch a specific table in format 'database.table_name' (e.g., 'openalex_claude.arxiv_index_enhanced')"
    )
    
    args = parser.parse_args()
    
    try:
        if args.only_arxiv_index_enhanced:
            # Fetch only arxiv_index_enhanced
            fetch_single_table(CLICKHOUSE_DB2, "arxiv_index_enhanced")
            logger.info("\n✓ Data fetch completed successfully!")
        elif args.table:
            # Fetch specific table
            if "." in args.table:
                database, table = args.table.split(".", 1)
                fetch_single_table(database, table)
                logger.info("\n✓ Data fetch completed successfully!")
            else:
                logger.error(f"Invalid table format: {args.table}. Expected format: 'database.table_name'")
                sys.exit(1)
        else:
            # Fetch all tables
            fetch_all_tables()
            logger.info("\n✓ Data fetch completed successfully!")
    except KeyboardInterrupt:
        logger.warning("\n✗ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
