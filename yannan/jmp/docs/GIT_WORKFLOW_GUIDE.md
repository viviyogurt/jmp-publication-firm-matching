# Git Workflow Guide - JMP Project

## Repository Structure

**GitHub Repository:** https://github.com/viviyogurt/jmp-publication-firm-matching.git

**Current Branch:** `lightweight-master`

**Directory Locations:**
- **Laptop:** `/Users/yogurtemily/Downloads/jmp-project-github/yannan/jmp/`
- **Remote Server:** (Set up on server)
- **GitHub:** https://github.com/viviyogurt/jmp-publication-firm-matching.git

## Setup Instructions

### On Remote Server (Primary Work Location)

```bash
# 1. Navigate to your working directory on the server
cd /path/to/your/work/directory

# 2. Clone the repository
git clone https://github.com/viviyogurt/jmp-publication-firm-matching.git jmp-project
cd jmp-project/yannan/jmp

# 3. Create necessary data directories (gitignored)
mkdir -p /Data/patent/{raw,processed,outputs}
mkdir -p /Data/openscience
mkdir -p /Data/financials

# 4. Set up Python environment
python3 -m venv venv
source venv/bin/activate

# 5. Install dependencies
pip install -r src/04_patent_processing/requirements.txt
pip install -r requirements.txt  # General project dependencies
```

### On Laptop (Secondary/Planning Location)

You already have the cloned repo at:
```
/Users/yogurtemily/Downloads/jmp-project-github/yannan/jmp/
```

## Workflow: Code Only (No Data Files)

### Key Principle
**Push/fetch code ONLY. Data files stay on remote server.**

The `.gitignore` is configured to exclude:
- `data/`, `logs/`, `output/` directories
- Large analysis files
- Temporary files

### Daily Workflow

#### **On Laptop (Planning/Documentation)**

```bash
cd /Users/yogurtemily/Downloads/jmp-project-github/yannan/jmp

# 1. Pull latest changes from GitHub
git pull origin lightweight-master

# 2. Make changes to code/documentation
# Example: Edit scripts, update plans, modify documentation

# 3. Commit and push changes
git add .
git commit -m "Describe your changes"
git push origin lightweight-master
```

#### **On Remote Server (Execution/Processing)**

```bash
cd /path/to/jmp-project/yannan/jmp

# 1. Pull latest changes from GitHub
git pull origin lightweight-master

# 2. Run processing scripts
# Example: Process patent data (heavy computation)
python src/04_patent_processing/scripts/01_verify_data.py
python src/04_patent_processing/scripts/02_filter_ai_patents.py

# 3. Results are saved to /Data/ (gitignored, not pushed)

# 4. If you modify code, commit and push
git add .
git commit -m "Update processing scripts based on results"
git push origin lightweight-master
```

## Typical Scenarios

### Scenario 1: Develop Code on Laptop, Run on Server

**Laptop:**
```bash
# 1. Edit code
vim src/04_patent_processing/scripts/02_filter_ai_patents.py

# 2. Test logic (small sample)
# (Use small test data locally)

# 3. Push to GitHub
git add .
git commit -m "Add optimization to AI patent filtering"
git push origin lightweight-master
```

**Server:**
```bash
# 1. Pull latest code
git pull origin lightweight-master

# 2. Run on full dataset
python src/04_patent_processing/scripts/02_filter_ai_patents.py
```

### Scenario 2: Analyze Results on Server, Update Plans

**Server:**
```bash
# 1. Run analysis
python src/04_patent_processing/scripts/02_filter_ai_patents.py

# 2. Review results
ls -lh /Data/patent/processed/

# 3. Update documentation based on findings
vim docs/patents/PROCESSING_UPDATE.md

# 4. Push documentation
git add .
git commit -m "Document processing results"
git push origin lightweight-master
```

**Laptop:**
```bash
# Pull updated documentation
git pull origin lightweight-master
```

### Scenario 3: Both Locations Working Simultaneously

**Problem:** You make changes on laptop AND server. How to merge?

**Solution:**

**On Laptop:**
```bash
# 1. Commit your changes
git add .
git commit -m "Add visualization scripts"

# 2. Before pushing, pull latest from GitHub
git pull origin lightweight-master --rebase

# 3. If conflicts occur, resolve them
# (Git will mark conflict files)

# 4. After resolving, commit and push
git add .
git rebase --continue
git push origin lightweight-master
```

**On Server:**
```bash
# Similar process
git add .
git commit -m "Update processing parameters"
git pull origin lightweight-master --rebase
# Resolve conflicts if any
git push origin lightweight-master
```

## Current Project Status

### ✅ Completed

1. **Publication Data Processing** (remote server)
   - OpenAlex data collection
   - Firm-publication matching
   - Firm-year panel creation

2. **Financial Data Collection**
   - CRSP-CCM data downloaded
   - Firm fundamentals available

3. **Patent Processing Plan** (just added to GitHub)
   - Comprehensive documentation
   - Two-stage classification framework
   - Utility functions for AI patent ID

### ⏳ In Progress

**Patent Data Processing** (ready to start on remote server)

**Next steps on remote server:**
```bash
# 1. Pull latest code with patent processing pipeline
git pull origin lightweight-master

# 2. Download PatentsView data to /Data/patent/raw/
# (Manual download from https://patentsview.org/download/download-tables/)
# Required files:
# - g_patent.tsv
# - g_patent_abstract.tsv
# - g_cpc_current.tsv
# - g_assignee_disambiguated.tsv
# - g_patent_assignee.tsv

# 3. Verify data
python src/04_patent_processing/scripts/01_verify_data.py

# 4. Process AI patents (Phase 2-7)
python src/04_patent_processing/scripts/02_filter_ai_patents.py
# ... etc
```

## File Organization

### Code (Version Controlled)
```
yannan/jmp/
├── src/                    # Processing scripts
│   ├── 01_data_construction/
│   ├── 03_analysis/
│   └── 04_patent_processing/  # NEW: Patent processing pipeline
├── docs/                   # Documentation
│   └── patents/            # NEW: Patent processing docs
├── requirements.txt        # Python dependencies
└── README.md              # Project overview
```

### Data (NOT Version Controlled)
```
/Data/
├── patent/
│   ├── raw/              # Downloaded PatentsView data
│   ├── processed/        # Processed datasets
│   └── outputs/          # Analysis results
├── openscience/          # OpenAlex publications
└── financials/           # CRSP-CCM data
```

## Best Practices

### ✅ DO

1. **Commit frequently** with descriptive messages
   ```bash
   git commit -m "Add Stage 2 strategic classification for AI patents"
   ```

2. **Pull before pushing** to avoid conflicts
   ```bash
   git pull origin lightweight-master
   git push origin lightweight-master
   ```

3. **Use branches for experimental features**
   ```bash
   git checkout -b feature/experiment-1
   # ... make changes ...
   git checkout lightweight-master
   git merge feature/experiment-1
   ```

4. **Review `.gitignore`** before adding data files
   ```bash
   cat .gitignore
   # Should include: data/, logs/, output/, etc.
   ```

### ❌ DON'T

1. **DON'T commit large data files**
   - Parquet files, CSV files with millions of rows
   - Model checkpoints, embeddings
   - These should stay in `/Data/` (gitignored)

2. **DON'T push broken code** without testing
   - Test logic on small samples first
   - Use try-except for robustness

3. **DON'T ignore merge conflicts**
   - Resolve conflicts promptly
   - Communicate with collaborators if needed

## Troubleshooting

### Issue: "Push rejected" (remote has changes you don't have)

**Solution:**
```bash
git pull origin lightweight-master --rebase
# Resolve conflicts if any
git push origin lightweight-master
```

### Issue: "Data file accidentally committed"

**Solution:**
```bash
# Remove from git tracking
git rm --cached /path/to/data/file.parquet

# Add to .gitignore
echo "/Data/*.parquet" >> .gitignore

# Commit the removal
git add .gitignore
git commit -m "Remove data files from version control"
git push origin lightweight-master
```

### Issue: "Conflict in merge"

**Solution:**
```bash
# 1. Open conflicted file
vim conflicted_file.py

# 2. Look for conflict markers
# <<<<<<< HEAD
# Your changes
# =======
# Their changes
# >>>>>>> origin/lightweight-master

# 3. Edit file to keep desired changes

# 4. Mark as resolved
git add conflicted_file.py

# 5. Complete merge/rebase
git rebase --continue  # or git commit for merge
```

## Quick Reference

### Common Commands

```bash
# Pull latest changes
git pull origin lightweight-master

# Push your changes
git push origin lightweight-master

# Check status
git status

# View commit history
git log --oneline --graph --all

# Create new branch
git checkout -b feature/my-feature

# Switch branches
git checkout lightweight-master

# View differences
git diff
git diff origin/lightweight-master

# Stash changes temporarily
git stash
git stash pop

# Undo last commit (keep changes)
git reset --soft HEAD~1

# Undo last commit (discard changes)
git reset --hard HEAD~1
```

## Summary

- **Laptop:** Planning, documentation, code development
- **Remote Server:** Heavy computation, data processing, analysis
- **GitHub:** Code synchronization only (no data)
- **Workflow:** Pull → Edit → Commit → Push (repeat)

**Current Status:** ✅ Patent processing pipeline pushed to GitHub, ready to run on remote server!
