#!/bin/bash
echo "=== Flatten Progress Monitor ==="
echo ""

# Check if process is running
if ps aux | grep -v grep | grep "flatten_ai_papers_batches.py" > /dev/null; then
    echo "✓ Process is running"
    ps aux | grep -v grep | grep "flatten_ai_papers_batches.py" | head -1 | awk '{print "  PID: " $2 " | CPU: " $3 "% | MEM: " $4 "%"}'
else
    echo "✗ Process not running"
fi

echo ""

# Count files
INPUT_DIR="data/raw/publication/ai_papers_batches_noduplication"
OUTPUT_DIR="data/raw/publication/ai_papers_batches_noduplication_flattened"

TOTAL_BATCHES=$(find "$INPUT_DIR" -name "batch_*.parquet" 2>/dev/null | wc -l)
FLATTENED=$(find "$OUTPUT_DIR" -name "*_flatten.parquet" 2>/dev/null | wc -l)

echo "Progress:"
echo "  Total batches: $TOTAL_BATCHES"
echo "  Flattened: $FLATTENED"
if [ $TOTAL_BATCHES -gt 0 ]; then
    PCT=$(echo "scale=1; $FLATTENED * 100 / $TOTAL_BATCHES" | bc)
    echo "  Progress: $PCT%"
fi

echo ""

# Show recent log
echo "Recent log entries:"
tail -10 logs/flatten_ai_papers_batches_noduplication.log 2>/dev/null | grep -E "(INFO|WARNING|ERROR)" | tail -5
