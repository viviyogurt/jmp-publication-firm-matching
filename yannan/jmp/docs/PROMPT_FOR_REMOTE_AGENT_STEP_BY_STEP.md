# Claude Code Prompt - For Remote Server Agent

**Copy and paste this ENTIRE prompt to Claude Code on your remote server**

---

## Mission: Process AI Patent Data (Fixed Implementation)

I need you to help me process PatentsView data for AI innovation research using the FIXED implementation that solves critical memory and quality issues.

### Context

**Research Project:** JMP dissertation on AI knowledge disclosure strategies
**Core Question:** When do firms choose to publish vs. patent AI innovations?

**Previous Attempt Issues:**
1. ❌ Memory constraints (process killed, exit code 137)
2. ❌ AI patent rate 40.7% (too high - should be 5-10%)
3. ❌ Unknown classification rate 93.6% (unacceptable - should be <20%)
4. ❌ High false positives from generic keywords

**Solution:** Updated code on GitHub with expert fixes based on AI patent literature (Babina 2024, KPSS 2017, Cockburn 2019)

---

## Step 1: Pull Latest Updates from GitHub

First, navigate to the project directory and pull the latest code:

```bash
# Go to project directory (adjust path as needed)
cd /path/to/jmp-project/yannan/jmp

# Pull latest changes with fixes
git pull origin lightweight-master
```

**Please execute this and confirm the pull was successful.**

---

## Step 2: Review the Documentation

After pulling, please read these files to understand the fixes:

### Start Here (Quick Overview):
```bash
cat docs/PATENT_FIXES_SUMMARY.md
```

### Then Read (Detailed Analysis):
```bash
cat docs/PATENT_PROCESSING_EXPERT_GUIDE.md
```

### Action Checklist:
```bash
cat docs/PATENT_PROCESSING_TODO.md
```

**These documents explain:**
- Why the previous approach failed
- What was fixed (memory, AI rate, unknown rate, precision)
- How the new approach works (INTERSECTION, abstract-based, refined keywords)
- Expected results and quality metrics
- Literature benchmarks

**Please read these files and summarize the key improvements.**

---

## Step 3: Verify Data Exists

Check that PatentsView data is available:

```bash
ls -lh /Data/patent/raw/

# Expected files:
# - g_patent.tsv (~2 GB)
# - g_patent_abstract.tsv (~1.6 GB)
# - g_cpc_current.tsv (~1 GB)
# - g_assignee_disambiguated.tsv (~200 MB)
# - g_patent_assignee.tsv (~500 MB)

# Count records
wc -l /Data/patent/raw/g_patent.tsv
wc -l /Data/patent/raw/g_patent_abstract.tsv
wc -l /Data/patent/raw/g_cpc_current.tsv
```

**Please confirm these files exist and report their sizes.**

---

## Step 4: Install Dependencies (if needed)

Check if required packages are installed:

```bash
# Activate virtual environment if using one
# source venv/bin/activate

# Check/install polars
python3 -c "import polars; print(polars.__version__)"

# If not installed:
pip install polars pyarrow tqdm
```

---

## Step 5: Run the Fixed Patent Processing Script

Execute the fixed implementation:

```bash
python3 src/04_patent_processing/scripts/02_filter_ai_patents_fixed.py
```

**This script will:**

1. **Load CPC data first** - Identify AI patents by CPC code (reduces dataset 95%)
2. **Filter patents early** - Only load CPC-matched patents (memory-efficient)
3. **Load abstracts selectively** - Only for CPC-matched patents
4. **Apply INTERSECTION classification** - Patent must match BOTH (CPC AND text)
5. **Classify strategic categories** - Using abstracts (not titles) with refined keywords
6. **Multi-label classification** - Allow patents in multiple categories
7. **Create validation set** - For manual quality check
8. **Save results** - To `/Data/patent/processed/ai_patents_2010_2024.parquet`

**Expected runtime:** 30-60 minutes
**Expected memory:** <8 GB RAM (should NOT get killed)

---

## Step 6: Monitor Progress and Validate Results

While the script runs, it will output progress. Please report:

### A. Progress Updates
The script will print:
- Step 1: Loading CPC data → Expected: ~400K AI patents by CPC
- Step 2: Loading patent metadata → Expected: ~400K patents (CPC-filtered)
- Step 3: Loading abstracts → Expected: ~400K abstracts
- Step 4: INTERSECTION classification → Expected: ~100K-200K AI patents (5-10%)
- Step 5: Strategic classification → Expected: Distribution shown
- Validation samples → 5 from each category for review

### B. Key Metrics to Check

**After script completes, verify:**

1. **AI Patent Rate:**
   ```
   ✅ AI patent rate: 5-10%
   ```
   - If >15%: Still too many false positives
   - If <2%: Too restrictive

2. **Unknown Rate:**
   ```
   Unknown: <20%
   ```
   - If >30%: Classification needs improvement

3. **Classification Distribution:**
   ```
   Infrastructure: 15-20%
   Algorithm: 40-50%
   Application: 20-30%
   Unknown: <20%
   ```
   - Should be roughly balanced

4. **Sample Quality:**
   The script will output 5 samples from each category.
   **Please manually review these samples and report:**
   - Are Infrastructure samples actually AI hardware?
   - Are Algorithm samples actually ML methods?
   - Are Application samples actual AI applications?
   - Any obvious false positives?

---

## Step 7: Review Output Files

After script completes, check the outputs:

```bash
# Main dataset
ls -lh /Data/patent/processed/ai_patents_2010_2024.parquet

# Validation set for manual review
ls -lh /Data/patent/processed/validation_set_for_manual_review.csv

# Preview main dataset
python3 -c "
import polars as pl
df = pl.read_parquet('/Data/patent/processed/ai_patents_2010_2024.parquet')
print(f'Total AI patents: {df.height:,}')
print(df.head())
print(df.groupby('ai_category_primary').agg(
    pl.count().alias('n')
).sort('n', descending=True))
"
```

---

## Step 8: Quality Validation

### A. Automatic Checks

The script should report:

```
✅ Memory: Process didn't get killed
✅ AI rate: 5-10% (within target)
✅ Unknown rate: <20% (within target)
✅ Classification distribution: Balanced
```

### B. Manual Validation (Important!)

Please open the validation set:

```bash
# Open in spreadsheet or text editor
less /Data/patent/processed/validation_set_for_manual_review.csv

# Or export for manual labeling
# Add a 'manual_label' column and label 50-100 samples
```

**For 20-30 samples, manually verify:**
- Read the title and abstract
- Decide: Is this actually AI? (Yes/No)
- Decide: Which category? (Infrastructure/Algorithm/Application/Unknown)
- Compare to predicted label
- Report accuracy

**Target precision: >85%** (at least 17 out of 20 should be correct)

---

## Step 9: Report Results

Please provide a comprehensive report with:

### 1. Execution Status
- Did the script complete successfully?
- Was it killed (exit code 137)?
- How long did it take?
- Memory usage?

### 2. Key Metrics
- Total patents processed: ?
- AI patents found: ? (percentage: ?%)
- Classification distribution:
  - Infrastructure: ? (?%)
  - Algorithm: ? (?%)
  - Application: ? (?%)
  - Unknown: ? (?%)

### 3. Sample Quality Review
From the 5 samples shown for each category:
- Infrastructure: Are these actually AI hardware? (any false positives?)
- Algorithm: Are these actually ML methods? (any false positives?)
- Application: Are these actual AI applications? (any false positives?)
- Unknown: Are these truly ambiguous, or should they be classified?

### 4. Issues Encountered
Any errors, warnings, or unexpected behavior?

### 5. Validation Assessment
Based on manual review of samples:
- Estimated precision: ?%
- Estimated recall: ?%
- Need to refine keywords? (Yes/No)
- Ready for Phase 3 (firm matching)? (Yes/No)

---

## Step 10: Next Steps (After Phase 2 Complete)

If results are good (AI rate 5-10%, Unknown <20%, precision >85%):

### Option A: Continue to Phase 3 (Firm Matching)
```
I want to continue to Phase 3: Match patents to firms.

Please:
1. Load g_patent_assignee.tsv and g_assignee_disambiguated.tsv
2. Fuzzy match assignee names to Compustat GVKEYs
3. Merge with AI patents
4. Create firm-patent panel

Expected: 15K-40K AI patents matched to 1.5K-3K firms
```

### Option B: Iterate on Classification (If Quality Poor)
```
The classification quality is poor. Please:

1. Analyze the misclassified examples
2. Identify missing keywords causing errors
3. Update keyword_lists_refined.py
4. Re-run 02_filter_ai_patents_fixed.py
5. Report new metrics

Issues found:
- [Describe specific problems]
```

---

## Reference Documentation Summary

**Quick Reference:**
- `docs/PATENT_FIXES_SUMMARY.md` - Executive summary
- `docs/PATENT_PROCESSING_TODO.md` - Action checklist

**Detailed Analysis:**
- `docs/PATENT_PROCESSING_EXPERT_GUIDE.md` - Expert answers to 15 questions

**Original Plan:**
- `docs/patents/PATENT_PROCESSING_PLAN.md` - Full implementation plan

**Code:**
- `src/04_patent_processing/scripts/02_filter_ai_patents_fixed.py` - Run this
- `src/04_patent_processing/utils/keyword_lists_refined.py` - Refined keywords

---

## Success Criteria

**Phase 2 is successful if:**

✅ Script completes without being killed
✅ AI patent rate: 5-10% (not 40%)
✅ Unknown rate: <20% (not 93%)
✅ Sample inspection shows good quality (>85% precision)
✅ Classification distribution is reasonable
✅ Output files created

**If any criteria fail:** Review the issues and we'll iterate on the approach.

---

## Expected Timeline

- Pull updates: 1 minute
- Review documentation: 5-10 minutes
- Run script: 30-60 minutes
- Validate results: 10-15 minutes
- **Total: ~1-1.5 hours**

---

## Please Start Now

**Begin with:**
```bash
cd /path/to/jmp-project/yannan/jmp
git pull origin lightweight-master
```

**Then read:**
```bash
cat docs/PATENT_FIXES_SUMMARY.md
```

**Then run:**
```bash
python3 src/04_patent_processing/scripts/02_filter_ai_patents_fixed.py
```

**Report back with all the information requested in Step 9.**

Let me know if you have any questions or encounter issues!

---

**End of Prompt**
