#!/bin/bash
echo "=== Condense Progress Monitor ==="
echo ""

# Check if process is running
if ps aux | grep -v grep | grep "condense_ai_papers_dataset.py" > /dev/null; then
    echo "✓ Process is running"
    ps aux | grep -v grep | grep "condense_ai_papers_dataset.py" | head -1 | awk '{print "  PID: " $2 " | CPU: " $3 "% | MEM: " $4 "%"}'
else
    echo "✗ Process not running"
fi

echo ""

# Check progress file
if [ -f logs/condense_ai_papers_progress.json ]; then
    python3 -c "
import json
from pathlib import Path

progress_file = Path('logs/condense_ai_papers_progress.json')
with open(progress_file) as f:
    progress = json.load(f)

processed = len(progress.get('processed_files', []))
total = 3478
pct = processed / total * 100 if total > 0 else 0

print(f'Progress: {processed:,} / {total:,} files ({pct:.1f}%)')
print(f'Last processed: {progress.get(\"last_processed\", \"N/A\")}')
"
else
    echo "Progress file not created yet"
fi

echo ""

# Show recent log
echo "Recent log entries:"
tail -10 logs/condense_ai_papers_noduplication.log 2>/dev/null | grep -E "(Processing chunk|Chunk.*completed|Total papers|unique|deduplicated)" | tail -5
