#!/bin/bash
# Fetch ai_papers using ClickHouse native client with batch processing
# This script fetches data in batches and saves incrementally to avoid connection issues

set -e

# Configuration
CLICKHOUSE_HOST="chenlin04.fbe.hku.hk"
CLICKHOUSE_USER="yannan"
CLICKHOUSE_PASSWORD="alaniscoolerthanluoye"
CLICKHOUSE_DB="openalex_claude"
CLICKHOUSE_TABLE="ai_papers"

PROJECT_ROOT="/home/kurtluo/yannan/jmp"
DATA_PROCESSED="${PROJECT_ROOT}/data/processed/publication"
BATCH_DIR="${DATA_PROCESSED}/ai_papers_batches"
LOG_FILE="${PROJECT_ROOT}/logs/fetch_ai_papers_batch.log"

# Batch configuration
BATCH_SIZE=100000  # Number of rows per batch
BATCH_DELAY=2      # Seconds to wait between batches (to avoid overwhelming connection)
MAX_RETRIES=3      # Maximum retries for failed batches

# Create directories
mkdir -p "$DATA_PROCESSED"
mkdir -p "$BATCH_DIR"
mkdir -p "$(dirname "$LOG_FILE")"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "================================================================================"
log "Fetching ai_papers using ClickHouse native client (BATCH MODE)"
log "================================================================================"
log "Batch directory: $BATCH_DIR"
log "Batch size: $BATCH_SIZE rows"
log "Batch delay: ${BATCH_DELAY}s"
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

# Step 1: Get total row count
log "Step 1: Getting total row count..."
TOTAL_COUNT_QUERY="SELECT count(*) FROM ${CLICKHOUSE_DB}.${CLICKHOUSE_TABLE}"

TOTAL_ROWS=$(clickhouse-client \
    --host "$CLICKHOUSE_HOST" \
    --port 9000 \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --database "$CLICKHOUSE_DB" \
    --query "$TOTAL_COUNT_QUERY" 2>>"$LOG_FILE")

if [ -z "$TOTAL_ROWS" ] || [ "$TOTAL_ROWS" -eq 0 ]; then
    log "ERROR: Could not get row count or table is empty"
    exit 1
fi

log "  Total rows to fetch: $TOTAL_ROWS"
log "  Estimated batches: $(( (TOTAL_ROWS + BATCH_SIZE - 1) / BATCH_SIZE ))"
log ""

# Step 2: Check for existing batches
log "Step 2: Checking for existing batches..."
EXISTING_BATCHES=$(find "$BATCH_DIR" -name "batch_*.parquet" 2>/dev/null | wc -l)
if [ "$EXISTING_BATCHES" -gt 0 ]; then
    log "  Found $EXISTING_BATCHES existing batch files"
    log "  Will skip already processed batches"
    log ""
    
    # Calculate starting offset based on existing batches
    LAST_BATCH_NUM=$(find "$BATCH_DIR" -name "batch_*.parquet" -printf '%f\n' 2>/dev/null | \
        sed 's/batch_\([0-9]*\)\.parquet/\1/' | sort -n | tail -1)
    START_OFFSET=$((LAST_BATCH_NUM * BATCH_SIZE + BATCH_SIZE))
    
    if [ "$START_OFFSET" -ge "$TOTAL_ROWS" ]; then
        log "  All batches already fetched!"
        log "  Use a merge script to combine batches into a single file"
        exit 0
    fi
    
    log "  Resuming from offset: $START_OFFSET"
else
    START_OFFSET=0
    log "  No existing batches found, starting from beginning"
fi
log ""

# Step 3: Fetch batches
log "Step 3: Starting batch fetching..."
log ""

OFFSET=$START_OFFSET
BATCH_NUM=$((START_OFFSET / BATCH_SIZE))
TOTAL_BATCHES=$(( (TOTAL_ROWS + BATCH_SIZE - 1) / BATCH_SIZE ))
START_TIME=$(date +%s)

# Function to fetch a single batch
fetch_batch() {
    local batch_offset=$1
    local batch_number=$2
    local retry_count=0
    
    while [ $retry_count -lt $MAX_RETRIES ]; do
        local batch_file="${BATCH_DIR}/batch_$(printf "%06d" $batch_number).parquet"
        
        # Skip if batch already exists
        if [ -f "$batch_file" ]; then
            log "  Batch $batch_number: Already exists, skipping..."
            return 0
        fi
        
        # Build query - select all columns including full_json
        # Note: Adjust column names based on actual table schema
        EXPORT_QUERY="
        SELECT *
        FROM ${CLICKHOUSE_DB}.${CLICKHOUSE_TABLE}
        ORDER BY openalex_id
        LIMIT ${BATCH_SIZE} OFFSET ${batch_offset}
        SETTINGS 
            max_memory_usage = 10000000000,
            max_execution_time = 0,
            send_timeout = 0,
            receive_timeout = 0
        FORMAT Parquet
        "
        
        log "  Batch $batch_number: Fetching rows $batch_offset to $((batch_offset + BATCH_SIZE - 1))..."
        
        # Fetch batch with timeout protection
        if timeout 3600 clickhouse-client \
            --host "$CLICKHOUSE_HOST" \
            --port 9000 \
            --user "$CLICKHOUSE_USER" \
            --password "$CLICKHOUSE_PASSWORD" \
            --database "$CLICKHOUSE_DB" \
            --query "$EXPORT_QUERY" \
            --format Parquet \
            --max_memory_usage 10000000000 \
            --max_execution_time 0 \
            > "$batch_file.tmp" 2>>"$LOG_FILE"; then
            
            # Verify batch file is valid
            if [ -f "$batch_file.tmp" ] && [ -s "$batch_file.tmp" ]; then
                # Try to verify it's a valid parquet file
                if command -v python3 &> /dev/null; then
                    ROW_COUNT=$(python3 -c "
import pyarrow.parquet as pq
try:
    pf = pq.ParquetFile('$batch_file.tmp')
    print(pf.metadata.num_rows)
except Exception as e:
    print('0')
" 2>&1)
                    
                    if [ "$ROW_COUNT" -gt 0 ]; then
                        mv "$batch_file.tmp" "$batch_file"
                        FILE_SIZE=$(du -h "$batch_file" | cut -f1)
                        log "    ✓ Batch $batch_number saved: $ROW_COUNT rows, $FILE_SIZE"
                        return 0
                    else
                        log "    ✗ Batch $batch_number: Invalid parquet file, retrying..."
                        rm -f "$batch_file.tmp"
                    fi
                else
                    # No Python, just check file size
                    if [ $(stat -f%z "$batch_file.tmp" 2>/dev/null || stat -c%s "$batch_file.tmp" 2>/dev/null) -gt 0 ]; then
                        mv "$batch_file.tmp" "$batch_file"
                        FILE_SIZE=$(du -h "$batch_file" | cut -f1)
                        log "    ✓ Batch $batch_number saved: $FILE_SIZE (row count verification skipped)"
                        return 0
                    fi
                fi
            else
                log "    ✗ Batch $batch_number: Empty or missing file, retrying..."
                rm -f "$batch_file.tmp"
            fi
        else
            log "    ✗ Batch $batch_number: Fetch failed, retrying..."
            rm -f "$batch_file.tmp"
        fi
        
        retry_count=$((retry_count + 1))
        if [ $retry_count -lt $MAX_RETRIES ]; then
            log "    Retry $retry_count/$MAX_RETRIES in 10 seconds..."
            sleep 10
        fi
    done
    
    log "    ✗✗ Batch $batch_number: Failed after $MAX_RETRIES retries"
    return 1
}

# Main batch fetching loop
FAILED_BATCHES=0
SUCCESSFUL_BATCHES=0

while [ $OFFSET -lt $TOTAL_ROWS ]; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    
    if [ $ELAPSED -gt 0 ]; then
        RATE=$((OFFSET / ELAPSED))
        REMAINING=$((TOTAL_ROWS - OFFSET))
        if [ $RATE -gt 0 ]; then
            ETA=$((REMAINING / RATE))
        else
            ETA=0
        fi
        ETA_HOURS=$((ETA / 3600))
        ETA_MINS=$(((ETA % 3600) / 60))
    else
        RATE=0
        ETA_HOURS=0
        ETA_MINS=0
    fi
    
    PROGRESS_PCT=$((OFFSET * 100 / TOTAL_ROWS))
    
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log "Progress: $PROGRESS_PCT% | Batch: $BATCH_NUM/$TOTAL_BATCHES | Offset: $OFFSET/$TOTAL_ROWS"
    log "Rate: ~$RATE rows/sec | ETA: ${ETA_HOURS}h ${ETA_MINS}m"
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if fetch_batch $OFFSET $BATCH_NUM; then
        SUCCESSFUL_BATCHES=$((SUCCESSFUL_BATCHES + 1))
        OFFSET=$((OFFSET + BATCH_SIZE))
        BATCH_NUM=$((BATCH_NUM + 1))
        
        # Delay between batches to avoid overwhelming connection
        if [ $BATCH_DELAY -gt 0 ] && [ $OFFSET -lt $TOTAL_ROWS ]; then
            sleep $BATCH_DELAY
        fi
    else
        FAILED_BATCHES=$((FAILED_BATCHES + 1))
        log ""
        log "⚠️  Batch $BATCH_NUM failed. Continuing with next batch..."
        log "   You can manually retry failed batches later"
        log ""
        
        # Still increment to avoid infinite loop
        OFFSET=$((OFFSET + BATCH_SIZE))
        BATCH_NUM=$((BATCH_NUM + 1))
        
        # Longer delay after failure
        sleep 10
    fi
done

# Final summary
log ""
log "================================================================================"
log "BATCH FETCHING COMPLETE"
log "================================================================================"
log "Total rows: $TOTAL_ROWS"
log "Successful batches: $SUCCESSFUL_BATCHES"
log "Failed batches: $FAILED_BATCHES"
log "Batch directory: $BATCH_DIR"
log ""

# Count actual batch files
ACTUAL_BATCHES=$(find "$BATCH_DIR" -name "batch_*.parquet" 2>/dev/null | wc -l)
log "Total batch files created: $ACTUAL_BATCHES"

if [ $FAILED_BATCHES -gt 0 ]; then
    log ""
    log "⚠️  WARNING: $FAILED_BATCHES batches failed to fetch"
    log "   Check the log file for details: $LOG_FILE"
    log "   You may need to re-run the script to fetch failed batches"
    log ""
fi

# Calculate total size
if [ $ACTUAL_BATCHES -gt 0 ]; then
    TOTAL_SIZE=$(du -sh "$BATCH_DIR" 2>/dev/null | cut -f1)
    log "Total data size: $TOTAL_SIZE"
    log ""
    log "Next steps:"
    log "  1. Verify all batches were fetched successfully"
    log "  2. Use a merge script to combine batches into a single parquet file"
    log "  3. Example merge command (Python):"
    log "     python -c \"import polars as pl; pl.concat([pl.read_parquet(f) for f in sorted(glob.glob('$BATCH_DIR/batch_*.parquet'))]).write_parquet('${DATA_PROCESSED}/ai_papers_complete.parquet')\""
fi

log ""
log "================================================================================"
log "SUCCESS"
log "================================================================================"
