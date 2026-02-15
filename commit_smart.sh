#!/bin/bash
# Smart commit script - analyzes git diff to generate intelligent commit messages
# Usage: ./commit_smart.sh

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

cd "$(git rev-parse --show-toplevel)"

# Check for changes
if [ -z "$(git status --porcelain)" ]; then
    echo -e "${YELLOW}No changes to commit${NC}"
    exit 0
fi

# Analyze git diff to understand what changed
analyze_changes() {
    local changes=$(git diff --cached --stat 2>/dev/null || git diff --stat)
    local files=$(git status --short)

    # Extract patterns from diff to determine change type
    local change_type=""
    local impact=""
    local details=""

    # Check for different types of changes
    if echo "$changes" | grep -qi "def \|class "; then
        change_type="Update function/class"
    elif echo "$changes" | grep -qi "import "; then
        change_type="Update dependencies"
    elif echo "$changes" | grep -qi "# \|\"\"\""; then
        change_type="Update documentation"
    elif echo "$changes" | grep -qi "fix\|bug\|error"; then
        change_type="Fix bug"
    elif echo "$changes" | grep -qi "optimiz\|speed\|performance"; then
        change_type="Optimize performance"
    else
        change_type="Update code"
    fi

    # Determine which module was changed
    local module=""
    if echo "$files" | grep -q "01_data_construction"; then
        module="data pipeline"
    elif echo "$files" | grep -q "02_linking"; then
        module="entity linking"
    elif echo "$files" | grep -q "03_analysis"; then
        module="analysis"
    elif echo "$files" | grep -q "docs/"; then
        module="documentation"
    else
        module="project files"
    fi

    # Count file types
    local py_count=$(echo "$files" | grep -c '\.py$' || true)
    local md_count=$(echo "$files" | grep -c '\.md$' || true)

    # Build message
    if [ "$py_count" -gt 0 ]; then
        details="($py_count Python file" && [ "$py_count" -gt 1 ] && details="${details}s"
        details="$details)"
    fi

    echo "$change_type in $module $details" | sed 's/  */ /g'
}

# Stage changes
echo -e "${BLUE}=== Analyzing Changes ===${NC}"
git status --short
echo ""

# Generate message
MSG=$(analyze_changes)

echo -e "${YELLOW}Generated commit message:${NC}"
echo -e "${GREEN}$MSG${NC}"
echo ""

# Interactive prompt
read -p "Accept? (Y/n/edit) " -n 1 -r response
echo ""

case "$response" in
    [nN]*)
        read -p "Enter custom message: " custom
        MSG="$custom"
        ;;
    [eE]*)
        tmpfile=$(mktemp)
        echo "$MSG" > "$tmpfile"
        ${EDITOR:-vi} "$tmpfile"
        MSG=$(cat "$tmpfile")
        rm "$tmpfile"
        ;;
esac

# Commit
git add -A
git commit -m "${MSG}"$'\n\n'"Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"

echo ""
echo -e "${GREEN}✓ Done!${NC}"
git log --oneline -1
