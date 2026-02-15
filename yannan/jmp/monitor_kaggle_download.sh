#!/bin/bash
# Monitor Kaggle ArXiv download progress
# Displays recent log output, file statistics, and process status

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${PROJECT_ROOT}/logs/arxiv_kaggle_download.log"
OUTPUT_FILE="${PROJECT_ROOT}/data/raw/publication/arxiv_kaggle.parquet"
MERGED_OUTPUT="${PROJECT_ROOT}/data/raw/publication/arxiv_complete_kaggle.parquet"

echo "=========================================="
echo "Kaggle ArXiv Download Monitor"
echo "=========================================="
echo ""

# Check if process is running
if pgrep -f "fetch_arxiv_from_kaggle.py" > /dev/null; then
    echo "Status: RUNNING"
    PID=$(pgrep -f "fetch_arxiv_from_kaggle.py" | head -1)
    echo "PID: $PID"
    
    # Show process stats
    if command -v ps > /dev/null; then
        ps -p $PID -o pid,pcpu,pmem,etime,cmd --no-headers 2>/dev/null | awk '{print "  CPU: " $2 "%, MEM: " $3 "%, Runtime: " $4}'
    fi
else
    echo "Status: NOT RUNNING"
fi
echo ""

# Show output file stats
if [ -f "$OUTPUT_FILE" ]; then
    echo "--- Output File Statistics ---"
    python3 << EOF
import polars as pl
from pathlib import Path
import os

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
        
        # Year distribution
        year_dist = df.select(pl.col("published").dt.year().alias("year")).group_by("year").agg(pl.len().alias("count")).sort("year")
        print(f"\nYear distribution:")
        for row in year_dist.iter_rows():
            print(f"  {row[0]}: {row[1]:,} papers")
    
    # File size
    size_mb = os.path.getsize("$OUTPUT_FILE") / (1024 * 1024)
    print(f"\nFile size: {size_mb:.2f} MB")
except Exception as e:
    print(f"Error reading output file: {e}")
EOF
else
    echo "Output file not yet created"
fi
echo ""

# Show merged file stats if exists
if [ -f "$MERGED_OUTPUT" ]; then
    echo "--- Merged File Statistics ---"
    python3 << EOF
import polars as pl
import os

try:
    df = pl.read_parquet("$MERGED_OUTPUT")
    print(f"Total papers in merged file: {len(df):,}")
    
    size_mb = os.path.getsize("$MERGED_OUTPUT") / (1024 * 1024)
    print(f"File size: {size_mb:.2f} MB")
except Exception as e:
    print(f"Error reading merged file: {e}")
EOF
    echo ""
fi

# Show recent log entries
if [ -f "$LOG_FILE" ]; then
    echo "--- Recent Log Entries (last 25 lines) ---"
    tail -25 "$LOG_FILE"
else
    echo "Log file not found"
fi
echo ""

echo "=========================================="
echo "Monitor complete. Refresh to see updates."
echo "=========================================="

