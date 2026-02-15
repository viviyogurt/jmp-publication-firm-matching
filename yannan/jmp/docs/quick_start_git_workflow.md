# Quick Start: Git Workflow for JMP Project

## 🚀 Setup Checklist

### 1. Verify Git Configuration
```bash
git config user.name
git config user.email
```

If not set:
```bash
git config user.name "Your Name"
git config user.email "your.email@example.com"
```

### 2. Verify Remote Repository
```bash
git remote -v
```

Should show:
```
origin  https://github.com/username/repo.git (fetch)
origin  https://github.com/username/repo.git (push)
```

If not set:
```bash
git remote add origin https://github.com/username/jmp.git
```

### 3. Test Git Workflow Helper
```bash
./git_workflow_helper.sh status
```

Should show current git status and recent commits.

---

## 📅 Daily Workflow

### Morning: Start Work
```bash
# 1. Pull latest changes
git pull

# 2. Create feature branch (if needed)
git checkout -b feature/my-work
```

### During Work: After Each Change
```bash
# Option 1: Using Claude Code
/commit "Brief description"

# Option 2: Manual
git add .
git commit -m "Description"
```

### Before Risky Operations
```bash
# Create checkpoint
./git_workflow_helper.sh checkpoint

# Or manually:
git tag -a checkpoint-before-run -m "Checkpoint"
```

### End of Day: Push Work
```bash
# Push all work
./git_workflow_helper.sh daily-push

# Or manually:
git add .
git commit -m "Daily progress: $(date +%Y-%m-%d)"
git push
git push origin --tags
```

---

## 🎯 Common Commands

### Check Status
```bash
./git_workflow_helper.sh status
# or
git status
```

### Create Milestone
```bash
./git_workflow_helper.sh milestone "stage1-complete" "Stage 1 matching complete"
# or
git tag -a stage1-complete -m "Stage 1 matching complete"
git push origin stage1-complete
```

### Recover from Mistakes
```bash
# See options
./git_workflow_helper.sh recover

# Undo last commit (keep changes)
git reset --soft HEAD~1

# Restore file from previous version
git checkout HEAD~1 -- path/to/file.py

# Restore from checkpoint
git checkout checkpoint-XYZ
```

### List Branches
```bash
./git_workflow_helper.sh branches
# or
git branch -a
```

### Clean Up Merged Branches
```bash
./git_workflow_helper.sh clean-branches
```

---

## 🏷️ Important Milestones to Tag

Create tags for:
- ✅ **Data acquisition complete** - `data-fetch-complete`
- ✅ **Stage 1 matching done** - `matching-stage1-complete`
- ✅ **Validation >95%** - `validation-95-accuracy`
- ✅ **First draft** - `jmp-draft-v1`
- ✅ **Submission ready** - `jmp-submission-final`

Example:
```bash
git tag -a validation-95-accuracy -m "Achieved 95% accuracy on validation"
git push origin validation-95-accuracy
```

---

## 🔄 Backup Strategy

### Daily (Automatic)
- Push to GitHub at end of day
- Script helper does this automatically

### Weekly (Manual)
```bash
# Backup to multiple remotes
./git_workflow_helper.sh backup
```

### Monthly (Manual)
```bash
# Create monthly milestone
./git_workflow_helper.sh milestone "$(date +%Y-%m)" "End of month checkpoint"

# Review and archive
git branch -a | grep "feature/" | xargs -r git branch -d
```

---

## 🚨 Emergency Recovery

If Something Goes Wrong:

1. **Don't panic!**
2. Check what happened:
   ```bash
   ./git_workflow_helper.sh recover
   ```
3. Find the good commit in reflog
4. Restore it:
   ```bash
   git checkout abc1234
   git branch recover-main
   git checkout main
   git reset --hard recover-main
   ```

---

## ✅ Daily Checklist

- [ ] Pulled latest changes at start
- [ ] Created feature branch for new work
- [ ] Committed after each meaningful change
- [ ] Created checkpoint before risky operations
- [ ] Pushed to GitHub at end of day
- [ ] Tags pushed with commits

---

## 📊 Monitoring

### Check Progress
```bash
# Recent commits
git log --oneline -10

# All tags
git tag

# Branch status
git status
```

### Statistics
```bash
# Total commits
git rev-list --count HEAD

# Commits this week
git rev-list --count --since="1 week ago" HEAD

# Contributors
git shortlog -sn
```

---

## 🎓 Best Practices

1. **Commit frequently** - After every meaningful change
2. **Push daily** - At minimum, push once per day
3. **Tag milestones** - Mark important achievements
4. **Use branches** - Keep main stable
5. **Write good messages** - Descriptive commit messages
6. **Test before pushing** - Make sure code works
7. **Review regularly** - Check git history weekly

---

## 📚 Reference

- Complete guide: `docs/git_workflow_best_practices.md`
- Project guidelines: `CLAUDE.md` (Section 6)
- Git documentation: https://git-scm.com/doc

---

**Remember:** The goal is to never lose work. Commit frequently, push regularly, tag milestones.
