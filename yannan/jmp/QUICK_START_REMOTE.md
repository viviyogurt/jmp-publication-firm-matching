# Quick Start - Remote Server (One Page)

## For Claude Code on Remote Server - Copy This Short Version

---

### Step 1: Pull Updates (1 min)

```bash
cd /path/to/jmp-project/yannan/jmp
git pull origin lightweight-master
```

### Step 2: Run Fixed Script (30-60 min)

```bash
python3 src/04_patent_processing/scripts/02_filter_ai_patents_fixed.py
```

### Step 3: Check Results (5 min)

After script completes, verify:

```bash
# Check output file exists
ls -lh /Data/patent/processed/ai_patents_2010_2024.parquet

# Quick stats
python3 -c "
import polars as pl
df = pl.read_parquet('/Data/patent/processed/ai_patents_2010_2024.parquet')
print(f'Total AI patents: {df.height:,}')
print(df.groupby('ai_category_primary').agg(pl.count().alias('n')).sort('n', descending=True))
"
```

### Expected Results

✅ **AI Patent Rate:** 5-10% (NOT 40%)
✅ **Unknown Rate:** <20% (NOT 93%)
✅ **Memory:** <8 GB (NOT killed)
✅ **Runtime:** 30-60 minutes

✅ **Classification Distribution:**
- Infrastructure: 15-20%
- Algorithm: 40-50%
- Application: 20-30%
- Unknown: <20%

### If Success → Continue

If metrics look good, say:
```
Continue to Phase 3: Match patents to firms using fuzzy matching
```

### If Problems → Report

If any issues:
```
Results summary:
- AI rate: ?%
- Unknown: ?%
- Sample quality: [good/bad]
- Issues: [describe]
```

### Need Details?

Read full guide:
```bash
cat docs/PROMPT_FOR_REMOTE_AGENT_STEP_BY_STEP.md
```

---

**That's it! Pull, run, check results.**
