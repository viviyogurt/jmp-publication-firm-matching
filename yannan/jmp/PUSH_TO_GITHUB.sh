#!/bin/bash
# GitHub Repository Setup - Step by Step

echo "=========================================="
echo "GitHub Repository Setup for JMP Project"
echo "=========================================="
echo ""

echo "STEP 1: Create GitHub Repository"
echo "----------------------------------"
echo "1. Go to: https://github.com/new"
echo "2. Repository name: jmp-publication-firm-matching"
echo "3. Description: JMP Job Market Paper: Publication-Firm Matching for Innovation Research"
echo "4. Set to PUBLIC"
echo "5. DO NOT initialize with README"
echo "6. Click 'Create repository'"
echo ""

echo "STEP 2: Push to GitHub"
echo "------------------------"
echo "After creating the repository, run:"
echo ""
echo "cd /home/kurtluo/yannan"
echo "git remote set-url jmp-origin https://github.com/YOUR_USERNAME/jmp-publication-firm-matching.git"
echo "git push -u jmp-origin lightweight-master"
echo "git push jmp-origin --tags"
echo ""

echo "Current Repository Status:"
echo "--------------------------"
cd /home/kurtluo/yannan
echo "Branch: $(git branch --show-current)"
echo "Commits: $(git rev-list --count HEAD)"
echo "Files: $(git ls-files | wc -l)"
echo "Tags: $(git tag | wc -l)"
echo ""

echo "Latest 5 commits:"
git log --oneline -5
echo ""

echo "Tags:"
git tag
echo ""

echo "Ready to push! ✅"
