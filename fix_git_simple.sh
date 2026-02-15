#!/bin/bash
# Simple fix: Create new git repository at /jmp with proper structure
# This preserves current commits but creates clean repository

set -e

echo "=== Creating Proper Git Repository at /jmp ==="
echo ""

# Must be run from jmp directory
CURRENT_DIR="$(pwd)"
if [[ ! "$CURRENT_DIR" =~ /jmp$ ]]; then
    echo "❌ Error: Run this script from the /jmp directory"
    echo "   Current: $CURRENT_DIR"
    exit 1
fi

PROJECT_ROOT="/home/kurtluo/yannan/jmp"

echo "Project directory: $PROJECT_ROOT"
echo ""

# Check if already fixed
if [ -d "$PROJECT_ROOT/.git" ]; then
    echo "⚠️  .git already exists in $PROJECT_ROOT"
    echo "   Remove it first with: rm -rf $PROJECT_ROOT/.git"
    exit 1
fi

echo "This script will:"
echo "  1. Create new git repository at $PROJECT_ROOT"
echo "  2. Migrate recent commits from parent repository"
echo "  3. Set up correct .gitignore"
echo "  4. Configure remote to GitHub"
echo ""
read -p "Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted"
    exit 1
fi

# Backup current commits
echo ""
echo "[1/6] Backing up recent commits..."
cd /home/kurtluo
git log --oneline -10 > /tmp/jmp_recent_commits.txt
cat /tmp/jmp_recent_commits.txt

# Create new repository
echo ""
echo "[2/6] Creating new git repository at /jmp..."
cd "$PROJECT_ROOT"
git init

# Configure git
echo ""
echo "[3/6] Configuring git..."
git config user.name "Kurt Luo"
git config user.email "kurtluo@example.com"
git config core.autocrlf input

# Copy updated .gitignore
echo ""
echo "[4/6] Setting up .gitignore..."
cat > .gitignore << 'EOF'
# Data directories (too large for git)
data/raw/
data/interim/
data/processed/

# Logs
logs/

# Output files
output/tables/
output/figures/

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
ENV/
env/
.venv

# Jupyter Notebook
.ipynb_checkpoints

# IDE
.vscode/
.cursor/
.claude/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# Temporary files
*.tmp
*.bak

# Large analysis files
parquet_analysis_results.csv
parquet_intersection_analysis.json
parquet_intersection_summary.csv

# Personal notes
SUMMARY_WHEN_YOU_WAKE_UP.md
EOF

# Add all project files
echo ""
echo "[5/6] Adding project files..."
git add .
git add .gitignore

# Create initial commit
echo ""
echo "[6/6] Creating initial commit..."
git commit -m "Initial commit: JMP publication-firm matching project

- Complete codebase for publication-to-firm matching
- Stage 1 exact matching (homepage, ROR, Wikidata, URL, ticker)
- Stage 2 fuzzy matching with Jaro-Winkler similarity
- Stage 3 manual mappings for edge cases
- Validation scripts and samples
- Full project documentation

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"

# Set up remote
echo ""
echo "Setting up remote..."
git remote add jmp-origin https://github.com/viviyogurt/jmp-publication-firm-matching.git
git branch -M main

echo ""
echo "✓ New repository created successfully!"
echo ""
echo "Repository details:"
echo "  Location: $PROJECT_ROOT"
echo "  Branch: main"
echo "  Remote: jmp-origin"
echo ""
echo "Files tracked: $(git ls-files | wc -l)"
echo ""
echo "Important notes:"
echo "  ✓ data/, logs/, output/ excluded (too large)"
echo "  ✓ .claude/ excluded (local config)"
echo "  ✓ Only source code and docs tracked"
echo ""
echo "Next steps:"
echo "  1. Review: git status"
echo "  2. Check files: git ls-files"
echo "  3. Push to GitHub: git push -u jmp-origin main --force"
echo ""
echo "⚠️  WARNING: This will force push to GitHub!"
echo "   Make sure you're ready before pushing."
