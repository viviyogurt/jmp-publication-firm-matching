#!/bin/bash
# Fetch arxiv_index_enhanced using ClickHouse native client
# This is much more efficient than HTTP connections

set -e

# Configuration
CLICKHOUSE_HOST="chenlin04.fbe.hku.hk"
CLICKHOUSE_USER="yannan"
CLICKHOUSE_PASSWORD="alaniscoolerthanluoye"
CLICKHOUSE_DB="openalex_claude"
CLICKHOUSE_TABLE="arxiv_index_enhanced"

PROJECT_ROOT="/home/kurtluo/yannan/jmp"
DATA_RAW="${PROJECT_ROOT}/data/raw/publication"
OUTPUT_FILE="${DATA_RAW}/openalex_claude_arxiv_index_enhanced.parquet"
LOG_FILE="${PROJECT_ROOT}/logs/fetch_arxiv_index_enhanced_native.log"

# Create directories
mkdir -p "$DATA_RAW"
mkdir -p "$(dirname "$LOG_FILE")"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "================================================================================"
log "Fetching arxiv_index_enhanced using ClickHouse native client"
log "================================================================================"
log "Output file: $OUTPUT_FILE"
log ""

# Check if clickhouse-client is available
if ! command -v clickhouse-client &> /dev/null; then
    log "ERROR: clickhouse-client not found. Please install ClickHouse client."
    log "On Ubuntu/Debian: sudo apt-get install clickhouse-client"
    log "On CentOS/RHEL: sudo yum install clickhouse-client"
    exit 1
fi

log "Using ClickHouse native client (binary protocol - much more efficient than HTTP)"
log ""

# Method 1: Export directly to Parquet using ClickHouse's native Parquet export
# This is the most efficient method
log "Attempting Method 1: Direct Parquet export via ClickHouse..."
log ""

# ClickHouse can export to Parquet using INSERT INTO FUNCTION
# We'll use a query that exports to a temporary location, then move it
TEMP_DIR=$(mktemp -d)
TEMP_FILE="${TEMP_DIR}/arxiv_index_enhanced.parquet"

# Export query - ClickHouse can write Parquet directly
EXPORT_QUERY="
SELECT 
    openalex_id,
    arxiv_id,
    doi,
    json
FROM ${CLICKHOUSE_DB}.${CLICKHOUSE_TABLE}
SETTINGS 
    max_memory_usage = 50000000000,
    max_execution_time = 0,
    send_timeout = 0,
    receive_timeout = 0
FORMAT Parquet
"

log "Executing export query..."
log "This may take several hours for 40M rows..."
log ""

# Use clickhouse-client with native protocol (port 9000, not 8123)
# Native protocol is much more efficient than HTTP
if clickhouse-client \
    --host "$CLICKHOUSE_HOST" \
    --port 9000 \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --database "$CLICKHOUSE_DB" \
    --query "$EXPORT_QUERY" \
    --format Parquet \
    --max_memory_usage 50000000000 \
    --max_execution_time 0 \
    > "$OUTPUT_FILE" 2>>"$LOG_FILE"; then
    
    log ""
    log "✓ Export completed successfully!"
    
    # Verify file
    if [ -f "$OUTPUT_FILE" ]; then
        FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
        log "  File size: $FILE_SIZE"
        
        # Try to get row count using Python
        if command -v python3 &> /dev/null; then
            ROW_COUNT=$(python3 -c "
import pyarrow.parquet as pq
try:
    pf = pq.ParquetFile('$OUTPUT_FILE')
    print(pf.metadata.num_rows)
except Exception as e:
    print('Error:', e)
" 2>&1)
            log "  Row count: $ROW_COUNT"
        fi
    fi
    
    log ""
    log "================================================================================"
    log "SUCCESS: Export completed"
    log "================================================================================"
    exit 0
else
    log ""
    log "✗ Export failed. Check log file: $LOG_FILE"
    log ""
    log "If this fails, we can try alternative methods:"
    log "  1. Export to TSV/CSV first, then convert to Parquet"
    log "  2. Use chunked export with native client"
    log "  3. Export to S3/object storage if available"
    exit 1
fi
