# Git Commit Skill for JMP Project

## Quick Start

Two ways to commit changes with standard format:

### Option 1: Using the commit.sh script (Recommended)
```bash
./commit.sh "your commit message"
```

### Option 2: Using git directly
```bash
git add <files>
git commit -m "your commit message" -m "" -m "Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

## What the skill does

1. **Stages all changes** (`git add -A`)
2. **Creates commit** with standardized format:
   - Your message
   - Blank line
   - Co-author line: "Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
3. **Shows confirmation** with recent commits

## Examples

```bash
# Fix a bug
./commit.sh "Fix bug in ticker matching logic"

# Add new feature
./commit.sh "Add optimized publication matching script"

# Document changes
./commit.sh "Update validation results in README"
```

## Commit Message Format

Following academic research standards, all commits include:
- **Short description** (50 chars or less)
- **Co-author attribution** for Claude Code assistance

Example commit message:
```
Fix bug in ticker matching logic

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

## Files

- `commit.sh` - Main commit script (executable)
- `.claude/skills/commit` - Claude Code skill integration

## Verification

Check recent commits:
```bash
git log --oneline -5
```

## Troubleshooting

If commit fails with "lock file exists":
```bash
rm -f ~/.git/index.lock
```

For selective commits (not all files):
```bash
# Add specific files only
git add path/to/file1.py path/to/file2.py
./commit.sh "message"
```
