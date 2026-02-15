#!/bin/bash
# Monitor classification progress

echo "======================================================================"
echo "Institution Classification Progress Monitor"
echo "======================================================================"
echo ""

# Check if process is running
PID=$(pgrep -f "correct_institution_classifications.py" | head -1)

if [ -n "$PID" ]; then
    echo "✅ Process is running (PID: $PID)"
    echo ""

    # Show process info
    ps -p $PID -o pid,etime,%mem,vsz,comm
    echo ""

    # Show recent log output
    if [ -f /tmp/classify_progress.log ]; then
        echo "Recent log output:"
        echo "────────────────────────────────────────────────────────────"
        tail -20 /tmp/classify_progress.log
        echo "────────────────────────────────────────────────────────────"
    fi
else
    echo "❌ Process is not running"
fi

echo ""
echo "======================================================================"
echo "Commands:"
echo "  Check progress: tail -f /tmp/classify_progress.log"
echo "  Stop process: kill $PID"
echo "  Check output: ls -lh data/processed/publication/ai_papers_with_correct_classifications.parquet"
echo "======================================================================"
