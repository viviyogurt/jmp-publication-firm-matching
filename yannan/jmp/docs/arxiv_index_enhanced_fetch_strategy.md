# ArXiv Index Enhanced Fetch Strategy

## Problem Analysis

### Issues with HTTP-based approach:
1. **Connection Exhaustion**: Too many small chunks (10k rows) = 4,011 connections → server refuses connections
2. **Memory Errors**: Large chunks (50k+ rows) with JSON column → server memory limit exceeded (226GB limit)
3. **Inefficiency**: HTTP protocol overhead, connection setup/teardown for each chunk
4. **Server Overload**: Many small queries overwhelm the server

### Root Cause:
- The `json` column is extremely large (contains full OpenAlex work data)
- Server has 226GB memory limit
- HTTP protocol is inefficient for large data transfers
- Chunked approach creates too many connections

## Better Solutions

### Option 1: ClickHouse Native Client (RECOMMENDED)
**Advantages:**
- Uses native binary protocol (port 9000) - much more efficient than HTTP
- Single connection, streaming export
- Better memory management
- Can export directly to Parquet format

**Implementation:**
```bash
clickhouse-client \
    --host chenlin04.fbe.hku.hk \
    --port 9000 \
    --user yannan \
    --password alaniscoolerthanluoye \
    --database openalex_claude \
    --query "SELECT * FROM arxiv_index_enhanced FORMAT Parquet" \
    > output.parquet
```

**Script:** `fetch_arxiv_index_enhanced_native.sh`

### Option 2: Export to TSV/CSV, then Convert
**Advantages:**
- Native client can handle large exports better
- Convert to Parquet locally (more control)

**Implementation:**
```bash
# Export to TSV
clickhouse-client --query "SELECT * FROM arxiv_index_enhanced FORMAT TSV" > data.tsv

# Convert to Parquet using Python
python convert_tsv_to_parquet.py data.tsv output.parquet
```

### Option 3: Server-Side Export to Object Storage
**Advantages:**
- Most efficient for very large datasets
- Server handles the export
- Can use S3, HDFS, or local filesystem

**Implementation:**
```sql
INSERT INTO FUNCTION file('output.parquet', 'Parquet')
SELECT * FROM arxiv_index_enhanced
```

### Option 4: Use ClickHouse's Native Protocol in Python
**Advantages:**
- Better than HTTP, but still Python-based
- Can use `clickhouse-driver` (native protocol) instead of `clickhouse-connect` (HTTP)

**Implementation:**
```python
from clickhouse_driver import Client

client = Client(
    host='chenlin04.fbe.hku.hk',
    port=9000,  # Native protocol
    user='yannan',
    password='alaniscoolerthanluoye',
    database='openalex_claude'
)

# Stream data
for row in client.execute_iter('SELECT * FROM arxiv_index_enhanced', with_column_types=True):
    # Process row
    pass
```

## Recommended Approach

**Use ClickHouse Native Client (Option 1)**

1. **Install clickhouse-client** if not available:
   ```bash
   # Ubuntu/Debian
   sudo apt-get install clickhouse-client
   
   # Or download from ClickHouse website
   ```

2. **Run the native export script:**
   ```bash
   bash jmp/src/01_data_construction/fetch_arxiv_index_enhanced_native.sh
   ```

3. **Monitor progress:**
   ```bash
   tail -f jmp/logs/fetch_arxiv_index_enhanced_native.log
   ls -lh jmp/data/raw/publication/openalex_claude_arxiv_index_enhanced.parquet
   ```

## Why Native Client is Better

1. **Binary Protocol**: More efficient than HTTP text protocol
2. **Single Connection**: One long-lived connection vs thousands of HTTP requests
3. **Streaming**: Data streams directly, no chunking needed
4. **Better Memory Management**: Server handles memory more efficiently
5. **Native Format Support**: Can export directly to Parquet

## Fallback Options

If native client export still has memory issues:

1. **Exclude JSON column temporarily:**
   ```sql
   SELECT openalex_id, arxiv_id, doi FROM arxiv_index_enhanced
   ```
   Then fetch JSON separately in smaller batches

2. **Use server-side filtering:**
   ```sql
   SELECT * FROM arxiv_index_enhanced 
   WHERE toYear(toDate(publication_date)) >= 2020
   ```
   Fetch by year ranges

3. **Request server admin to:**
   - Increase memory limits temporarily
   - Export the table directly on the server
   - Provide access to server filesystem

## Next Steps

1. Try native client export (Option 1)
2. If that fails, try TSV export + conversion (Option 2)
3. If still issues, contact server admin for direct export
