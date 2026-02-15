#!/bin/bash
# GitHub Repository Setup Script for JMP Project

echo "==================================="
echo "JMP GitHub Repository Setup"
echo "==================================="
echo ""

# Option 1: Authenticate with GitHub CLI
echo "Option 1: Authenticate and create repo automatically"
echo "------------------------------------------------------"
echo "Run these commands:"
echo "  gh auth login"
echo "  gh repo create jmp-publication-firm-matching --public --description 'JMP Job Market Paper: Publication-Firm Matching' --source=/home/kurtluo/yannan --remote=jmp-origin --push"
echo ""

# Option 2: Manual GitHub creation
echo "Option 2: Manual repository creation"
echo "-------------------------------------"
echo "1. Go to https://github.com/new"
echo "2. Repository name: jmp-publication-firm-matching"
echo "3. Description: JMP Job Market Paper: Publication-Firm Matching for Innovation Research"
echo "4. Make it PUBLIC"
echo "5. DO NOT initialize with README"
echo "6. Click 'Create repository'"
echo ""

# After creation, push commands
echo "After creating the repository, run:"
echo "==================================="
echo "cd /home/kurtluo/yannan"
echo "git remote add jmp-origin https://github.com/YOUR_USERNAME/jmp-publication-firm-matching.git"
echo "git push -u jmp-origin lightweight-master"
echo "git push jmp-origin --tags"
echo ""

# Current status
echo "Current repository status:"
echo "-------------------------"
cd /home/kurtluo/yannan
echo "Branch: $(git branch --show-current)"
echo "Commits: $(git rev-list --count HEAD)"
echo "Files tracked: $(git ls-files | wc -l)"
echo "Tags: $(git tag | wc -l)"
echo ""

echo "Latest commits:"
git log --oneline -5
echo ""

echo "Ready to push! ✅"
