# Smart Auto-Commit Feature

## Overview

The `/commit` script now **automatically generates intelligent commit messages** based on your codebase changes. No manual typing required!

## How It Works

The script analyzes:
1. **Which files changed** - Detects module (data construction, linking, analysis, docs)
2. **What type of change** - Detects additions, fixes, optimizations, refactoring
3. **File types** - Counts Python, Markdown, and script files
4. **Git diff** - Analyzes actual code changes to understand context

## Usage

### Auto-Generate Message (Default)
```bash
./commit.sh
```

The script will:
1. Show you what changed
2. Generate an intelligent commit message
3. Ask for confirmation (Y/n/edit/q)
4. Create commit with standard format

### Use Custom Message
```bash
./commit.sh "your custom message"
```

## Interactive Options

When you run `./commit.sh` without arguments, you'll see:

```
=== Smart Auto-Commit ===

Changed files:
  modified: src/02_linking/match_publications_stage1.py
  added:    docs/new_matching_guide.md

Suggested commit message:
Update entity linking (2 Python files, 1 doc)

Use this message? (Y/n/edit/q)
```

**Options:**
- **Y** (Enter) - Accept the suggested message
- **n** - Enter a custom message
- **e** - Edit the message in your text editor
- **q** - Cancel the commit

## Message Examples

The auto-generator creates messages like:

```
Add data construction (3 Python files)
Fix bug in entity linking (1 Python file)
Optimize analysis (2 Python files)
Update documentation (3 docs)
Refactor scripts (2 scripts)
```

## Features

✅ **Smart Detection** - Understands what you changed
✅ **No Typing** - Just press Enter to accept
✅ **Interactive** - Edit or customize if needed
✅ **Fast** - Analyzes changes in seconds
✅ **Consistent** - Always includes co-author attribution

## Examples

### Example 1: Bug Fix
```bash
# You fix a bug in ticker matching
./commit.sh

# Output:
# Suggested commit message:
# Fix bug in entity linking (1 Python file)
# Use this message? (Y/n/edit/q) Y
# ✓ Committed successfully!
```

### Example 2: New Feature
```bash
# You add a new matching stage
./commit.sh

# Output:
# Suggested commit message:
# Add function/class in entity linking (2 Python files)
# Use this message? (Y/n/edit/q) Y
# ✓ Committed successfully!
```

### Example 3: Documentation
```bash
# You update README and add guide
./commit.sh

# Output:
# Suggested commit message:
# Update documentation (2 docs)
# Use this message? (Y/n/edit/q) Y
# ✓ Committed successfully!
```

## Advanced Features

### Edit Message
If you want to tweak the suggested message:
```bash
./commit.sh
# Press 'e' to edit in your text editor
```

### Custom Message
If you want complete control:
```bash
./commit.sh "Implement ROR-based matching with validation"
```

### Cancel Commit
If you change your mind:
```bash
./commit.sh
# Press 'q' to quit without committing
```

## Technical Details

The script uses:
- **Git status** - To detect which files changed
- **Git diff** - To analyze what changed in the code
- **Pattern matching** - To categorize change types
- **Heuristics** - To determine the module affected

## Comparison

### Before (Manual)
```bash
git add .
git commit -m "Update linking scripts and documentation" -m "" -m "Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

### After (Auto)
```bash
./commit.sh
# [Press Enter]
```

Much faster and more consistent!

## Tips

1. **Review the suggestion** - The AI is usually right, but double-check
2. **Use 'e' to edit** - If you want to add more detail
3. **Use custom messages** - For important or complex changes
4. **Trust the defaults** - For routine commits, the suggestions are excellent

## Troubleshooting

**Q: The suggested message is wrong**
A: Press 'n' to enter a custom message, or 'e' to edit the suggestion

**Q: I want to cancel**
A: Press 'q' to quit without committing

**Q: Can I skip the prompt?**
A: Yes, use a custom message: `./commit.sh "message"`

**Q: What if there are no changes?**
A: The script will detect this and exit gracefully

---

**Start using it now:** Just run `./commit.sh` after your next code change!
