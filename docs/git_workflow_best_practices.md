# Git Workflow Best Practices for Claude Code + GitHub

**Date:** 2026-02-15
**Purpose:** Ensure complete version tracking and easy restoration

---

## 1. Branching Strategy

### Main Branch Structure
```
main (or lightweight-master)
├── feature/experiment-1
├── feature/optimized-matching
├── bugfix/timeout-issue
└── docs/update-readme
```

### Recommended Workflow
1. **Keep `main` stable** - Only merge working, tested code
2. **Feature branches** - One branch per task/feature
3. **Short-lived branches** - Delete after merging
4. **Descriptive names** - `feature/`, `bugfix/`, `docs/` prefixes

---

## 2. Commit Best Practices

### Commit Frequency
- ✅ **Commit after every meaningful change** (edits, new files, bug fixes)
- ✅ **Commit before risky operations** (large data processing, deletions)
- ✅ **Commit at natural breakpoints** (after completing a function, section)
- ❌ **Don't commit too frequently** (not after every single line edit)

### Commit Message Format
```bash
# Short, descriptive title (50 chars or less)
# Optional: Detailed explanation

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

**Examples:**
```bash
# Good:
"Add ticker matching from acronyms"
"Fix timeout issue in Stage 2 matching"
"Update CLAUDE.md with git guidelines"

# Bad:
"update" (too vague)
"fix stuff" (uninformative)
"changes" (no context)
```

### Commit Workflow with Claude Code
```bash
# After Claude edits code:
/commit "Add new feature"

# Or manual:
git add .
git commit -m "Description"
```

---

## 3. Tagging Important Milestones

### When to Use Tags
- ✅ **Major releases** (v1.0, v2.0)
- ✅ **Successful experiments** (experiment-ticker-matching-v1)
- ✅ **Paper milestones** (jmp-draft-2024-02, jmp-submission-2024-05)
- ✅ **Validation checkpoints** (validation-95-accuracy)

### Creating Tags
```bash
# Create annotated tag
git tag -a v1.0 -m "Stage 1 matching complete: 1,948 firms, 95% accuracy"

# Push tags to GitHub
git push origin v1.0

# Push all tags
git push origin --tags
```

### Listing and Restoring Tags
```bash
# List all tags
git tag

# Show tag details
git show v1.0

# Restore to tag
git checkout v1.0
```

---

## 4. GitHub Integration

### Remote Repository Setup
```bash
# Add remote
git remote add origin https://github.com/username/repo.git

# Push main branch
git push -u origin main

# Push all branches
git push --all origin
```

### Regular Pushing
```bash
# Push commits frequently (backup)
git push

# Push specific branch
git push origin feature-branch
```

### GitHub Features to Use
- ✅ **Releases** - For paper milestones
- ✅ **Issues** - Track bugs, features, experiments
- ✅ **Projects** - Kanban board for tasks
- ✅ **Actions** - CI/CD for testing (optional)
- ✅ **Wiki** - Extended documentation

---

## 5. Backup and Recovery Strategies

### Multiple Remote Backups
```bash
# Add GitHub remote
git remote add github https://github.com/username/jmp.git

# Add backup remote (e.g., GitLab, Bitbucket)
git remote add backup https://gitlab.com/username/jmp-backup.git

# Push to both
git push github main
git push backup main
```

### Recovery Commands
```bash
# Restore deleted file
git checkout HEAD~1 -- path/to/file.py

# Restore from specific commit
git checkout abc1234 -- path/to/file.py

# Undo last commit (keep changes)
git reset --soft HEAD~1

# Undo last commit (discard changes)
git reset --hard HEAD~1

# Revert to previous state (new commit)
git revert HEAD

# Find lost commit
git reflog
git checkout abc1234
```

---

## 6. .gitignore Best Practices

### Always Exclude
```
# Data files
data/
logs/
output/

# Python
__pycache__/
*.pyc
*.pyo
.venv/
venv/

# IDE
.vscode/
.cursor/
.idea/

# OS
.DS_Store
Thumbs.db

# Temporary files
*.tmp
*.bak
*.log
```

### Never Exclude
```
✅ Source code (src/)
✅ Documentation (docs/)
✅ Configuration (requirements.txt, .gitignore)
✅ Scripts (*.sh, *.py)
✅ README files
```

---

## 7. Claude Code Specific Workflow

### Recommended Claude Code Settings

In `.claude/settings.local.json`:
```json
{
  "permissions": {
    "allow": [
      "Bash(git add:*)",
      "Bash(git commit:*)",
      "Bash(git push:*)",
      "Bash(git tag:*)"
    ]
  }
}
```

### Automatic Commit Hook

Add to `CLAUDE.md` (already done!):
```
### 6. Version Control and Automatic Commits
- Commit after ANY code edit
- Use /commit skill
- Include Co-Authored-By attribution
```

### Before Risky Operations
```bash
# 1. Create checkpoint
git tag -a checkpoint-before-<operation> -m "Checkpoint before <operation>"

# 2. Create backup branch
git branch backup-before-<operation>

# 3. Commit current state
git add .
git commit -m "Checkpoint before <operation>"
```

---

## 8. Daily Workflow Example

### Start of Day
```bash
# Pull latest changes
git pull

# Create feature branch if needed
git checkout -b feature/new-experiment
```

### During Work
```bash
# After each meaningful change
/commit "Add function X"

# Before running long scripts
git tag -a checkpoint-before-run
/commit "Checkpoint before running Stage 2 matching"

# Run script
python src/02_linking/match_publications_stage2.py
```

### End of Day
```bash
# Commit all changes
git add .
git commit -m "Daily progress: completed Stage 2 matching"

# Push to GitHub
git push

# Push tags
git push origin --tags
```

---

## 9. Disaster Recovery

### If You Mess Up Badly
```bash
# 1. Don't panic!
# 2. Check reflog (history of all operations)
git reflog

# 3. Find good commit
git reflog | grep "before-mess-up"

# 4. Restore to that commit
git checkout abc1234
git branch recover-main
git checkout main
git reset --hard recover-main

# 5. Push recovered version
git push --force origin main  # CAREFUL! Only if sure
```

### If GitHub is Corrupted
```bash
# Create new repository on GitHub

# Add new remote
git remote set-url origin https://github.com/username/new-repo.git

# Push everything
git push -u origin --all
git push origin --tags
```

---

## 10. Validation and Testing

### Pre-Commit Checklist
- [ ] Code runs without errors
- [ ] Tests pass (if available)
- [ ] Documentation updated
- [ ] Git commit created

### Pre-Push Checklist
- [ ] All commits pushed
- [ ] Tags pushed
- [ ] Branches pushed if needed
- [ ] GitHub repository updated

---

## 11. Recommended Git Aliases

Add to `~/.gitconfig`:
```bash
[alias]
    st = status
    co = checkout
    br = branch
    ci = commit
    unstage = reset HEAD --
    last = log -1 HEAD
    visual = log --graph --oneline --all --decorate
```

---

## 12. Summary Checklist

### Daily Practices
- ✅ Commit after every meaningful change
- ✅ Push to GitHub at least daily
- ✅ Create tags for milestones
- ✅ Use branches for experiments
- ✅ Keep main branch stable

### Weekly Practices
- ✅ Review git history (`git log --oneline`)
- ✅ Clean up old branches (`git branch -d`)
- ✅ Backup to multiple remotes
- ✅ Verify tags pushed (`git push origin --tags`)

### Monthly Practices
- ✅ Create release milestone
- ✅ Archive old branches
- ✅ Update documentation
- ✅ Review .gitignore effectiveness

---

## Quick Reference Commands

```bash
# Status
git status
git log --oneline -10
git branch -a

# Commit
git add .
git commit -m "Message"
git push

# Tags
git tag -a v1.0 -m "Description"
git push origin v1.0

# Branches
git checkout -b feature/new
git branch -d feature/old
git push origin feature/new

# Recovery
git reflog
git checkout abc1234
git reset --hard HEAD~1
```

---

**Remember:** The goal is to never lose work. Commit frequently, push regularly, and tag milestones.
