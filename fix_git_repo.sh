#!/bin/bash
# Fix git repository location - move from /home/kurtluo to /home/kurtluo/yannan/jmp

set -e

echo "=== Fixing Git Repository Location ==="
echo ""

# Check current location
CURRENT_DIR="$(pwd)"
if [[ ! "$CURRENT_DIR" =~ /jmp$ ]]; then
    echo "❌ Error: Run this script from the /jmp directory"
    echo "   Current: $CURRENT_DIR"
    exit 1
fi

PROJECT_ROOT="$(git rev-parse --show-toplevel)"
echo "Current git root: $PROJECT_ROOT"

if [[ "$PROJECT_ROOT" =~ /jmp$ ]]; then
    echo "✓ Git repository already correctly located at /jmp"
    exit 0
fi

echo ""
echo "This will:"
echo "  1. Export current git history"
echo "  2. Create new repository at /jmp"
echo "  3. Import all jmp files with history"
echo ""
read -p "Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted"
    exit 1
fi

# Create export of current repository
echo ""
echo "[1/5] Exporting current repository..."
cd /home/kurtluo
git fast-export --all --signed-tags=strip > /tmp/jmp-export.fi

# Move to jmp directory and create new repo
echo "[2/5] Creating new repository at /jmp..."
cd /home/kurtluo/yannan/jmp
rm -rf .git
git init

# Import the exported data, filtering for yannan/jmp path
echo "[3/5] Importing jmp files with history..."
git fast-import --force < /tmp/jmp-export.fi

# Clean up - remove references to files outside jmp
echo "[4/5] Cleaning up repository..."
git filter-branch --force --index-filter \
    'git ls-files -s | sed "s-\t\"*-&yannan/jmp/-" |
     GIT_INDEX_FILE=$GIT_INDEX_FILE.new \
     git update-index --index-info &&
     mv "$GIT_INDEX_FILE.new" "$GIT_INDEX_FILE"' HEAD

# Remove references to old history
rm -rf .git/refs/original/
git reflog expire --expire=now --all
git gc --prune=now --aggressive

# Set up remote
echo "[5/5] Setting up remote..."
git remote add jmp-origin https://github.com/viviyogurt/jmp-publication-firm-matching.git
git branch -M lightweight-master

echo ""
echo "✓ Git repository moved successfully!"
echo ""
echo "New git root: $(git rev-parse --show-toplevel)"
echo "Remote: jmp-origin"
echo "Branch: lightweight-master"
echo ""
echo "Next steps:"
echo "  1. Verify: git status"
echo "  2. Push: git push -u jmp-origin lightweight-master --force"
