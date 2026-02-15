#!/bin/bash
# Quick commit script for JMP project
# Usage: ./commit.sh "your commit message"

COMMIT_MSG="$*"

if [ -z "$COMMIT_MSG" ]; then
    echo "Usage: ./commit.sh \"commit message\""
    echo "Example: ./commit.sh \"Fix bug in ticker matching\""
    exit 1
fi

# Add all changes and commit with standard format
git add -A
git commit -m "${COMMIT_MSG}"$'\n\n'"Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"

# Show result
if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Committed successfully!"
    git log --oneline -2
else
    echo ""
    echo "✗ Commit failed - nothing to commit or error occurred"
fi
