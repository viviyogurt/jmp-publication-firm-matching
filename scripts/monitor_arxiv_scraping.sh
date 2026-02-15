#!/bin/bash
# Monitor ArXiv scraping progress
# Displays recent log output, checkpoint status, and file statistics

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${PROJECT_ROOT}/logs/arxiv_scraping_improved.log"
CHECKPOINT_FILE="${PROJECT_ROOT}/data/checkpoints/arxiv_scraping/arxiv_scraping_checkpoint.json"
COMPLETENESS_TRACKER="${PROJECT_ROOT}/data/checkpoints/arxiv_scraping/completeness_tracker.json"
OUTPUT_FILE="${PROJECT_ROOT}/data/raw/publication/arxiv_2021_2025.parquet"

echo "=========================================="
echo "ArXiv Scraping Monitor"
echo "=========================================="
echo ""

# Check if process is running
if pgrep -f "fetch_arxiv_missing_data.py" > /dev/null; then
    echo "Status: RUNNING"
    PID=$(pgrep -f "fetch_arxiv_missing_data.py" | head -1)
    echo "PID: $PID"
else
    echo "Status: NOT RUNNING"
fi
echo ""

# Show checkpoint status
if [ -f "$CHECKPOINT_FILE" ]; then
    echo "--- Checkpoint Status ---"
    python3 << EOF
import json
from datetime import datetime

try:
    with open("$CHECKPOINT_FILE", 'r') as f:
        cp = json.load(f)
    
    print(f"Last processed date: {cp.get('last_processed_date', 'N/A')}")
    print(f"Total papers fetched: {cp.get('total_papers_fetched', 0):,}")
    print(f"Status: {cp.get('status', 'unknown')}")
    if 'last_checkpoint_time' in cp:
        print(f"Last checkpoint: {cp['last_checkpoint_time']}")
    if 'error' in cp:
        print(f"Error: {cp['error']}")
except Exception as e:
    print(f"Error reading checkpoint: {e}")
EOF
else
    echo "No checkpoint file found"
fi
echo ""

# Show completeness tracker status
if [ -f "$COMPLETENESS_TRACKER" ]; then
    echo "--- Completeness Tracker Status ---"
    python3 << EOF
import json

try:
    with open("$COMPLETENESS_TRACKER", 'r') as f:
        tracker = json.load(f)
    
    complete_ranges = sum(1 for v in tracker.values() if v.get("complete", False))
    total_ranges = len(tracker)
    print(f"Complete ranges: {complete_ranges}/{total_ranges}")
    
    if total_ranges > 0:
        # Show some recent complete ranges
        complete_items = [v for v in tracker.values() if v.get("complete", False)]
        if complete_items:
            print(f"\nRecent complete ranges (last 3):")
            for item in sorted(complete_items, key=lambda x: x.get("timestamp", ""), reverse=True)[:3]:
                print(f"  {item.get('start_date')} to {item.get('end_date')}: {item.get('paper_count', 0):,} papers ({item.get('completeness_ratio', 0)*100:.1f}%)")
except Exception as e:
    print(f"Error reading tracker: {e}")
EOF
else
    echo "Completeness tracker not yet created"
fi
echo ""

# Show output file stats
if [ -f "$OUTPUT_FILE" ]; then
    echo "--- Output File Statistics ---"
    python3 << EOF
import polars as pl
from pathlib import Path

try:
    df = pl.read_parquet("$OUTPUT_FILE")
    print(f"Total papers in file: {len(df):,}")
    
    if 'published' in df.columns:
        date_range = df.select([
            pl.col("published").min().alias("min_date"),
            pl.col("published").max().alias("max_date")
        ])
        min_date = date_range['min_date'][0]
        max_date = date_range['max_date'][0]
        print(f"Date range: {min_date} to {max_date}")
    
    # File size
    import os
    size_mb = os.path.getsize("$OUTPUT_FILE") / (1024 * 1024)
    print(f"File size: {size_mb:.2f} MB")
except Exception as e:
    print(f"Error reading output file: {e}")
EOF
else
    echo "Output file not yet created"
fi
echo ""

# Show recent log entries
if [ -f "$LOG_FILE" ]; then
    echo "--- Recent Log Entries (last 20 lines) ---"
    tail -20 "$LOG_FILE"
else
    echo "Log file not found"
fi
echo ""

echo "=========================================="
echo "Monitor complete. Refresh to see updates."
echo "=========================================="

