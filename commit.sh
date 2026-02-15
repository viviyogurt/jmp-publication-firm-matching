#!/bin/bash
# Smart Auto-Commit Script for JMP Project
# Automatically generates intelligent commit messages based on codebase changes
#
# Usage:
#   ./commit.sh              # Auto-generate message from changes
#   ./commit.sh "custom"     # Use custom message
#   ./commit.sh --ai         # Generate AI-powered message (if API available)
#

set -e

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT="$(git rev-parse --show-toplevel)"
cd "$PROJECT_ROOT"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

# ============================================================================
# Functions
# ============================================================================

print_header() {
    echo -e "${BLUE}=== Smart Auto-Commit ===${NC}"
    echo ""
}

check_changes() {
    if [ -z "$(git status --porcelain)" ]; then
        echo -e "${YELLOW}No changes to commit${NC}"
        exit 0
    fi
}

# Analyze which directory/module was changed
detect_module() {
    local files=$1
    local module="general"

    if echo "$files" | grep -q "01_data_construction"; then
        module="data construction"
    elif echo "$files" | grep -q "02_linking"; then
        module="entity linking"
    elif echo "$files" | grep -q "03_analysis"; then
        module="analysis"
    elif echo "$files" | grep -q "^docs"; then
        module="documentation"
    elif echo "$files" | grep -q "\.md$"; then
        module="documentation"
    elif echo "$files" | grep -q "\.sh$"; then
        module="scripts"
    fi

    echo "$module"
}

# Analyze git diff to detect change type
detect_change_type() {
    local diff=$1
    local change_type="Update"

    # Check for function/class changes
    if echo "$diff" | grep -q "^+def \|^+class "; then
        change_type="Add function/class"
    elif echo "$diff" | grep -q "^-def \|^-class "; then
        change_type="Remove function/class"
    elif echo "$diff" | grep -q "def \|class "; then
        change_type="Modify function/class"
    fi

    # Check for imports
    if echo "$diff" | grep -q "^+import \|^+from "; then
        change_type="Add dependency"
    fi

    # Check for documentation
    if echo "$diff" | grep -q "^+.*#\|^+.*\"\"\""; then
        change_type="Update documentation"
    fi

    # Check for fixes
    if echo "$diff" | grep -qi "fix\|bug\|error"; then
        change_type="Fix bug"
    fi

    # Check for optimizations
    if echo "$diff" | grep -qi "optimiz\|speed\|performance\|fast"; then
        change_type="Optimize"
    fi

    # Check for refactoring
    if echo "$diff" | grep -qi "refactor\|clean\|reorganize"; then
        change_type="Refactor"
    fi

    # Check for new files
    local added=$(git status --short | grep -c "^A" || true)
    if [ "$added" -gt 0 ]; then
        change_type="Add"
    fi

    echo "$change_type"
}

# Generate intelligent commit message
generate_message() {
    local status=$(git status --short)
    local diff=$(git diff 2>/dev/null || echo "")

    # Detect module and change type
    local module=$(detect_module "$status")
    local change_type=$(detect_change_type "$diff")

    # Count file types
    local py_files=$(echo "$status" | grep -c '\.py$' || true)
    local md_files=$(echo "$status" | grep -c '\.md$' || true)
    local sh_files=$(echo "$status" | grep -c '\.sh$' || true)

    # Build base message
    local message="$change_type $module"

    # Add file count details
    local details=""
    if [ "$py_files" -gt 0 ]; then
        details="$details$py_files Python file"
        [ "$py_files" -gt 1 ] && details="${details}s"
    fi
    if [ "$md_files" -gt 0 ]; then
        [ -n "$details" ] && details="$details, "
        details="$details$md_files doc"
        [ "$md_files" -gt 1 ] && details="${details}s"
    fi
    if [ "$sh_files" -gt 0 ]; then
        [ -n "$details" ] && details="$details, "
        details="$details$sh_files script"
        [ "$sh_files" -gt 1 ] && details="${details}s"
    fi

    if [ -n "$details" ]; then
        message="$message ($details)"
    fi

    echo "$message"
}

# Interactive prompt for message confirmation
prompt_message() {
    local suggested=$1

    # Display suggestion (with colors)
    echo -e "${CYAN}Suggested commit message:${NC}"
    echo -e "${GREEN}$suggested${NC}"
    echo ""

    read -p "Use this message? (Y/n/edit/q) " -n 1 -r response
    echo ""

    case "$response" in
        [nN]*)
            read -p "Enter custom message: " custom
            echo "$custom"
            ;;
        [eE]*)
            local tmpfile=$(mktemp)
            echo "$suggested" > "$tmpfile"
            ${EDITOR:-vi} "$tmpfile" 2>/dev/null || nano "$tmpfile"
            local edited=$(cat "$tmpfile")
            rm "$tmpfile"
            echo "$edited"
            ;;
        [qQ]*)
            echo -e "${YELLOW}Commit cancelled${NC}"
            exit 0
            ;;
        *)
            # Return the clean suggestion without color codes
            echo "$suggested"
            ;;
    esac
}

# Perform the commit
do_commit() {
    local message=$1

    echo -e "${BLUE}=== Staging Changes ===${NC}"
    git add -A

    echo ""
    echo -e "${BLUE}=== Creating Commit ===${NC}"

    local full_msg="${message}"$'\n\n'"Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"

    if git commit -m "$full_msg"; then
        echo ""
        echo -e "${GREEN}✓ Committed successfully!${NC}"
        echo ""
        echo -e "${CYAN}Recent commits:${NC}"
        git log --oneline -3 | while read line; do
            echo "  $line"
        done
    else
        echo ""
        echo -e "${RED}✗ Commit failed${NC}"
        exit 1
    fi
}

# ============================================================================
# Main
# ============================================================================

main() {
    print_header
    check_changes

    # Show what changed
    echo -e "${CYAN}Changed files:${NC}"
    git status --short | while read status file; do
        case "$status" in
            M) echo "  modified: $file" ;;
            A) echo "  added:    $file" ;;
            D) echo "  deleted:  $file" ;;
            *) echo "  $status $file" ;;
        esac
    done
    echo ""

    # Determine message
    local commit_msg=""

    if [ -n "$1" ]; then
        # Custom message provided
        commit_msg="$*"
        echo -e "${CYAN}Using custom message:${NC} ${GREEN}$commit_msg${NC}"
        echo ""
    else
        # Auto-generate message
        commit_msg=$(generate_message)
        commit_msg=$(prompt_message "$commit_msg")
    fi

    # Perform commit
    do_commit "$commit_msg"
}

main "$@"
