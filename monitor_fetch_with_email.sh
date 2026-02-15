#!/bin/bash
# Monitor progress of fetch_publication_data.py and send email when finished

LOG_FILE="jmp/logs/fetch_arxiv_index_enhanced.log"
OUTPUT_DIR="jmp/data/raw/publication"
OUTPUT_FILE="${OUTPUT_DIR}/openalex_claude_arxiv_index_enhanced.parquet"
SCRIPT_PATH="jmp/src/01_data_construction/fetch_publication_data.py"
CHECK_INTERVAL=60  # Check every 60 seconds

# Email configuration (adjust as needed)
EMAIL_TO="${USER}@hku.hk"  # Default to user@hku.hk, can be overridden
EMAIL_SUBJECT="ArXiv Index Enhanced Fetch Complete"

echo "=========================================="
echo "Starting ArXiv Index Enhanced Fetch Monitor"
echo "=========================================="
echo "Log file: $LOG_FILE"
echo "Output file: $OUTPUT_FILE"
echo "Check interval: ${CHECK_INTERVAL}s"
echo ""

# Function to send email
send_email() {
    local subject="$1"
    local body="$2"
    
    # Try to use mail command if available
    if command -v mail &> /dev/null; then
        echo "$body" | mail -s "$subject" "$EMAIL_TO"
        echo "Email sent to $EMAIL_TO"
    # Try sendmail as fallback
    elif command -v sendmail &> /dev/null; then
        {
            echo "To: $EMAIL_TO"
            echo "Subject: $subject"
            echo ""
            echo "$body"
        } | sendmail "$EMAIL_TO"
        echo "Email sent to $EMAIL_TO via sendmail"
    else
        echo "WARNING: No email client found (mail or sendmail). Email not sent."
        echo "Subject: $subject"
        echo "Body: $body"
    fi
}

# Start the fetch process in background
echo "Starting fetch process..."
cd /home/kurtluo/yannan
nohup python3 "$SCRIPT_PATH" --only-arxiv-index-enhanced > "$LOG_FILE" 2>&1 &
FETCH_PID=$!
echo "Fetch process started with PID: $FETCH_PID"
echo ""

# Monitor loop
MONITORING=true
LAST_SIZE=0
STALL_COUNT=0
MAX_STALL_COUNT=10  # If file size doesn't change for 10 checks, consider stalled

while $MONITORING; do
    # Check if process is still running
    if ! ps -p $FETCH_PID > /dev/null 2>&1; then
        echo "Process has finished. Checking results..."
        
        # Check if output file exists and is valid
        if [ -f "$OUTPUT_FILE" ]; then
            FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
            FILE_SIZE_BYTES=$(stat -f%z "$OUTPUT_FILE" 2>/dev/null || stat -c%s "$OUTPUT_FILE" 2>/dev/null)
            
            # Try to check if it's a valid parquet file
            if python3 -c "import pyarrow.parquet as pq; pq.ParquetFile('$OUTPUT_FILE')" 2>/dev/null; then
                ROW_COUNT=$(python3 -c "import pyarrow.parquet as pq; pf = pq.ParquetFile('$OUTPUT_FILE'); print(pf.metadata.num_rows)" 2>/dev/null)
                
                # Success!
                BODY="ArXiv Index Enhanced table fetch completed successfully!

Output file: $OUTPUT_FILE
File size: $FILE_SIZE
Row count: ${ROW_COUNT:-Unknown}

Check the log file for details: $LOG_FILE"
                
                send_email "$EMAIL_SUBJECT - SUCCESS" "$BODY"
                echo "✓ Fetch completed successfully!"
                echo "  File: $OUTPUT_FILE"
                echo "  Size: $FILE_SIZE"
                echo "  Rows: ${ROW_COUNT:-Unknown}"
            else
                # File exists but may be corrupted
                BODY="ArXiv Index Enhanced table fetch completed, but output file may be corrupted.

Output file: $OUTPUT_FILE
File size: $FILE_SIZE

Please check the log file: $LOG_FILE"
                
                send_email "$EMAIL_SUBJECT - WARNING" "$BODY"
                echo "⚠ Fetch completed but file may be corrupted"
            fi
        else
            # Process finished but no output file
            BODY="ArXiv Index Enhanced table fetch process finished, but no output file was created.

Please check the log file for errors: $LOG_FILE"
            
            send_email "$EMAIL_SUBJECT - ERROR" "$BODY"
            echo "✗ Process finished but no output file found"
        fi
        
        # Show last 30 lines of log
        echo ""
        echo "--- Last 30 lines of log ---"
        tail -30 "$LOG_FILE" 2>/dev/null || echo "Log file not found"
        
        MONITORING=false
        break
    fi
    
    # Check file size progress
    if [ -f "$OUTPUT_FILE" ]; then
        CURRENT_SIZE=$(stat -f%z "$OUTPUT_FILE" 2>/dev/null || stat -c%s "$OUTPUT_FILE" 2>/dev/null)
        CURRENT_SIZE_MB=$((CURRENT_SIZE / 1024 / 1024))
        
        if [ "$CURRENT_SIZE" -eq "$LAST_SIZE" ]; then
            STALL_COUNT=$((STALL_COUNT + 1))
        else
            STALL_COUNT=0
        fi
        
        LAST_SIZE=$CURRENT_SIZE
        
        # Show progress
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Process running... File size: ${CURRENT_SIZE_MB} MB"
        
        # Check for stall
        if [ $STALL_COUNT -ge $MAX_STALL_COUNT ]; then
            echo "WARNING: File size hasn't changed for $MAX_STALL_COUNT checks. Process may be stalled."
            STALL_COUNT=0  # Reset counter
        fi
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Process running... Output file not created yet"
    fi
    
    # Wait before next check
    sleep $CHECK_INTERVAL
done

echo ""
echo "=========================================="
echo "Monitoring complete"
echo "=========================================="
