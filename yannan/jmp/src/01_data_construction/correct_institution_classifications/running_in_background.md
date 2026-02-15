# Full Dataset Classification: Running in Background

## Current Status

**Process ID:** 3118750
**Status:** ✅ Running
**Started:** Just now
**Memory Usage:** ~5.9 GB
**CPU Time:** 40+ seconds

## What's Happening

The script is processing the full dataset (~17M papers) and:
1. Reading each paper's affiliations
2. Querying OpenAlex API for each unique institution
3. Classifying based on OpenAlex institution types
4. Calculating firm_count and firm_ratio for each paper

**Estimated Time:** Several hours to days (depends on number of unique institutions)

## How to Monitor Progress

### Option 1: Check Process Status
```bash
# See if it's still running
ps aux | grep 3118750 | grep -v grep

# See resource usage
top -p 3118750
```

### Option 2: Monitor Log File
```bash
# Follow the log (output may be buffered)
tail -f /tmp/classify_progress.log

# Or check periodically
tail -20 /tmp/classify_progress.log
```

### Option 3: Use the Monitor Script
```bash
bash src/01_data_construction/correct_institution_classifications/monitor_progress.sh
```

### Option 4: Check for Output File
The script will save results to:
```
data/processed/publication/ai_papers_with_correct_classifications.parquet
```

Check if it exists and is growing:
```bash
# Check file size (if exists)
ls -lh data/processed/publication/ai_papers_with_correct_classifications.parquet

# Monitor if growing
watch -n 10 'ls -lh data/processed/publication/ai_papers_with_correct_classifications.parquet'
```

## Expected Behavior

The script will:
1. Read papers row group by row group
2. For each paper, query OpenAlex API for each institution
3. Rate limit: ~20 requests/second (0.05s delay)
4. Save results when done

**Note:** With 17M papers and potentially millions of unique institutions, this will take a long time.

## If You Want to Stop It

```bash
# Graceful stop
kill 3118750

# Force kill (if needed)
kill -9 3118750
```

## Alternative: Process Smaller Sample First

If you want to test with a smaller sample first:
```bash
# Stop current process
kill 3118750

# Process 10,000 papers instead
python src/01_data_construction/correct_institution_classifications/correct_institution_classifications.py process 10000
```

This will complete faster and let you validate results before processing the full dataset.

## Why Is It Taking Long?

For each unique institution, the script:
1. Makes an HTTP request to OpenAlex API
2. Waits for response (network latency)
3. Rate limits to 20 requests/second

If you have 100K unique institutions:
- 100,000 institutions × 0.05 seconds = 5,000 seconds ≈ 1.4 hours

Plus processing time for the papers themselves.

