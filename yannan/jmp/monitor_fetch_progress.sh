#!/bin/bash
# Monitor progress of fetch_publication_data.py

LOG_FILE="logs/fetch_publication.log"
OUTPUT_DIR="data/raw/publication"

echo "=========================================="
echo "Monitoring fetch_publication_data.py"
echo "=========================================="
echo ""

# Check if process is running
if pgrep -f "fetch_publication_data.py" > /dev/null; then
    echo "✓ Process is running"
    PID=$(pgrep -f "fetch_publication_data.py" | head -1)
    echo "  PID: $PID"
else
    echo "✗ Process is not running"
fi

echo ""
echo "--- Recent Log Output (last 20 lines) ---"
if [ -f "$LOG_FILE" ]; then
    tail -20 "$LOG_FILE"
else
    echo "Log file not found yet"
fi

echo ""
echo "--- Output Files Created ---"
if [ -d "$OUTPUT_DIR" ]; then
    FILE_COUNT=$(ls -1 "$OUTPUT_DIR"/*.parquet 2>/dev/null | wc -l)
    echo "Total Parquet files: $FILE_COUNT"
    if [ "$FILE_COUNT" -gt 0 ]; then
        echo ""
        echo "Recent files (last 5):"
        ls -lht "$OUTPUT_DIR"/*.parquet 2>/dev/null | head -5 | awk '{print $9, "(" $5 ")"}'
        echo ""
        echo "Total size:"
        du -sh "$OUTPUT_DIR" 2>/dev/null
    fi
else
    echo "Output directory not found"
fi

echo ""
echo "=========================================="

