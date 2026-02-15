# Quick File Placement Reference

**For detailed guidelines, see [docs/CLAUDE.md](docs/CLAUDE.md)**

## Root Directory (KEEP CLEAN!)
Only these files allowed in root:
- `README.md` - Project overview
- `commit.sh` - Auto-commit tool
- `requirements.txt` - Python dependencies
- `PROJECT_STRUCTURE.md` - Structure documentation

## File Placement Rules

### Creating New Files? Follow This:

```
File Type | Location
----------|-----------------------------------------------
.py       | src/{01_data_construction, 02_linking, 03_analysis}/
.sh       | scripts/ (except commit.sh stays in root)
.md       | docs/ (except README.md stays in root)
.tex      | docs/latex/
.bib      | docs/latex/
.csv      | data/{raw, interim, processed}/
.parquet  | data/{raw, interim, processed}/
.log      | logs/
.png/.pdf | output/figures/
```

### Quick Examples

**Creating a Python script:**
```bash
# ✅ CORRECT
touch src/02_linking/match_publications_to_firms.py

# ❌ WRONG
touch match_publications_to_firms.py
```

**Creating a shell script:**
```bash
# ✅ CORRECT
touch scripts/monitor_progress.sh

# ❌ WRONG
touch monitor_progress.sh
```

**Creating documentation:**
```bash
# ✅ CORRECT
touch docs/new_feature_guide.md

# ❌ WRONG
touch new_feature_guide.md
```

**Creating LaTeX files:**
```bash
# ✅ CORRECT
touch docs/latex/jmp_results_section.tex

# ❌ WRONG
touch jmp_results_section.tex
```

## Before Creating ANY File:

1. **Identify file type** (.py, .sh, .md, .tex, etc.)
2. **Find correct location** from table above
3. **Create file in correct location**
4. **Never** create files in root directory (except the 4 allowed files)

## Need More Details?

See complete guidelines in **[docs/CLAUDE.md](docs/CLAUDE.md)**

---

**Remember: Clean directory = Happy coding!** 🧹✨
