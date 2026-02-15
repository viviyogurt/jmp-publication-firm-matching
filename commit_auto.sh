#!/bin/bash
# Enhanced commit script with auto-generated commit messages
# Usage:
#   ./commit.sh                    # Auto-generate commit message
#   ./commit.sh "custom message"   # Use custom message

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Change to project root
cd "$(git rev-parse --show-toplevel)"

# Check if there are changes
if [ -z "$(git status --porcelain)" ]; then
    echo -e "${YELLOW}No changes to commit${NC}"
    exit 0
fi

# Function to analyze changes and generate commit message
generate_commit_message() {
    local changes=$(git status --short)
    local staged=$(git diff --cached --name-only 2>/dev/null || echo "")

    # If files are staged, analyze staged changes
    if [ -n "$staged" ]; then
        changes=$(echo "$staged" | sed 's/^/M /')
    fi

    # Categorize changes
    local python_files=$(echo "$changes" | grep -c '\.py$' || true)
    local md_files=$(echo "$changes" | grep -c '\.md$' || true)
    local sh_files=$(echo "$changes" | grep -c '\.sh$' || true)
    local tex_files=$(echo "$changes" | grep -c '\.tex$' || true)

    # Count operation types
    local added=$(echo "$changes" | grep -c '^A' || true)
    local modified=$(echo "$changes" | grep -c '^M' || true)
    local deleted=$(echo "$changes" | grep -c '^D' || true)

    # Analyze modified files to understand changes
    local changed_dirs=$(echo "$changes" | awk '{print $2}' | sed 's#/.*##' | sort -u)
    local main_dir=$(echo "$changed_dirs" | head -1)

    # Determine change type
    local change_type=""
    local scope=""

    case "$main_dir" in
        src)
            if [[ "$changed_dirs" =~ src/01_data_construction ]]; then
                scope="data construction"
                change_type="Update data pipeline"
            elif [[ "$changed_dirs" =~ src/02_linking ]]; then
                scope="linking"
                change_type="Update entity linking"
            elif [[ "$changed_dirs" =~ src/03_analysis ]]; then
                scope="analysis"
                change_type="Update analysis scripts"
            else
                scope="source code"
                change_type="Update codebase"
            fi
            ;;
        docs)
            scope="documentation"
            change_type="Update documentation"
            ;;
        data)
            scope="data"
            change_type="Update data files"
            ;;
        *)
            scope="project"
            change_type="Update project"
            ;;
    esac

    # Refine message based on operation types
    if [ "$added" -gt 0 ] && [ "$modified" -eq 0 ] && [ "$deleted" -eq 0 ]; then
        change_type="Add $scope files"
    elif [ "$deleted" -gt 0 ] && [ "$modified" -eq 0 ] && [ "$added" -eq 0 ]; then
        change_type="Remove $scope files"
    elif [ "$added" -gt 0 ] && [ "$modified" -eq 0 ]; then
        change_type="Add new $scope"
    fi

    # Build detailed message
    local details=""
    local file_list=$(echo "$changes" | awk '{print $2}' | head -5)

    if [ "$python_files" -gt 0 ]; then
        details="$details($python_files Python files)"
    fi
    if [ "$md_files" -gt 0 ]; then
        details="$details($md_files docs)"
    fi
    if [ "$sh_files" -gt 0 ]; then
        details="$details($sh_files scripts)"
    fi

    # Get specific file changes for more context
    local specific_files=$(echo "$changes" | awk '{print $2}' | sed 's|.*/||' | tr '\n' ' ' | head -c 100)

    # Generate final message
    local message="$change_type"
    if [ -n "$details" ]; then
        message="$message $details"
    fi

    echo "$message"
}

# Function to get AI-generated commit message (optional enhancement)
generate_ai_message() {
    local changes=$(git diff --cached --stat 2>/dev/null || git diff --stat)

    # Check if we can use Claude API (if configured)
    if command -v anthropic &> /dev/null; then
        echo "Using AI for commit message generation..."
        # This would require API setup - placeholder for now
        # anthropic --prompt "Generate a git commit message for these changes: $changes"
    fi

    # Fallback to heuristic
    generate_commit_message
}

# Main script logic
if [ -n "$1" ]; then
    # Custom message provided
    COMMIT_MSG="$*"
else
    # Auto-generate message
    echo -e "${BLUE}=== Analyzing Changes ===${NC}"
    git status --short
    echo ""

    COMMIT_MSG=$(generate_commit_message)

    echo -e "${YELLOW}Generated commit message:${NC}"
    echo -e "${GREEN}$COMMIT_MSG${NC}"
    echo ""

    # Ask for confirmation
    read -p "Use this message? (Y/n/edit) " -n 1 -r response
    echo ""

    case "$response" in
        [nN]*)
            read -p "Enter custom message: " custom_msg
            COMMIT_MSG="$custom_msg"
            ;;
        [eE]*)
            ${EDITOR:-vi} <<< "$COMMIT_MSG"
            ;;
        *)
            # Use generated message
            ;;
    esac
fi

# Add all changes
echo -e "${BLUE}=== Staging Changes ===${NC}"
git add -A

# Create commit
FULL_MSG="${COMMIT_MSG}"$'\n\n'"Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"

echo ""
echo -e "${BLUE}=== Creating Commit ===${NC}"
git commit -m "$FULL_MSG"

# Show result
if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ Committed successfully!${NC}"
    echo ""
    echo "Recent commits:"
    git log --oneline -3
else
    echo ""
    echo -e "${RED}✗ Commit failed${NC}"
    exit 1
fi
