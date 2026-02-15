#!/bin/bash
# Quick script to check download progress

echo "=== Download Progress ==="
if [ -f data/raw/publication/institutions_all.jsonl.gz ]; then
    SIZE=$(ls -lh data/raw/publication/institutions_all.jsonl.gz | awk '{print $5}')
    echo "File size: $SIZE"
    
    COUNT=$(python3 -c "import gzip, json; print(sum(1 for line in gzip.open('data/raw/publication/institutions_all.jsonl.gz', 'rt') if line.strip()))" 2>/dev/null || echo "Error reading")
    echo "Institution count: $COUNT"
    echo "Target: 120,658"
    if [ "$COUNT" != "Error reading" ] && [ "$COUNT" -gt 0 ]; then
        PCT=$(python3 -c "print(f'{($COUNT / 120658 * 100):.1f}%')")
        echo "Coverage: $PCT"
    fi
else
    echo "File not found yet"
fi

echo ""
echo "=== Recent Log Activity ==="
tail -5 logs/institutions_download.log 2>/dev/null | grep -E "(Page|PHASE|Filter|Completed)" || echo "No recent activity"

echo ""
echo "=== Process Status ==="
ps aux | grep download_openalex_institutions | grep -v grep && echo "Running" || echo "Not running"
