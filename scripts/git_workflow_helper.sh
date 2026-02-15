#!/bin/bash
# Git Workflow Helper Script for JMP Project
# Provides easy commands for common git operations

set -e  # Exit on error

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Show usage
show_usage() {
    cat << EOF
Git Workflow Helper - JMP Project

Usage: ./git_workflow_helper.sh [command]

Commands:
  status          Show git status
  checkpoint      Create checkpoint before risky operation
  daily-push      Push daily work to GitHub
  milestone       Create milestone tag
  recover         Show recovery options
  branches        List all branches
  clean-branches  Delete merged branches
  backup          Backup to all remotes
  help            Show this help message

Examples:
  ./git_workflow_helper.sh checkpoint
  ./git_workflow_helper.sh daily-push
  ./git_workflow_helper.sh milestone "Stage 1 complete"

EOF
}

# Show git status
show_status() {
    print_info "Git Status:"
    echo ""
    git status -s
    echo ""

    print_info "Recent Commits:"
    git log --oneline -5
    echo ""

    print_info "Uncommitted Changes:"
    if git diff-index --quiet HEAD --; then
        print_success "No uncommitted changes"
    else
        print_warning "You have uncommitted changes!"
        git status
    fi
}

# Create checkpoint
create_checkpoint() {
    print_info "Creating checkpoint..."
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    CHECKPOINT_NAME="checkpoint-$TIMESTAMP"

    # Commit any changes
    if ! git diff-index --quiet HEAD --; then
        print_warning "You have uncommitted changes"
        read -p "Commit them now? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            git add .
            git commit -m "Checkpoint: $CHECKPOINT_NAME"
        fi
    fi

    # Create tag
    git tag -a "$CHECKPOINT_NAME" -m "Checkpoint created: $TIMESTAMP"
    print_success "Checkpoint created: $CHECKPOINT_NAME"

    # Ask to push
    read -p "Push to GitHub now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git push
        git push origin "$CHECKPOINT_NAME"
        print_success "Checkpoint pushed to GitHub"
    fi
}

# Daily push
daily_push() {
    print_info "Daily Git Push Workflow"
    echo ""

    # Check for uncommitted changes
    if ! git diff-index --quiet HEAD --; then
        print_warning "You have uncommitted changes"
        read -p "Commit them now? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            read -p "Enter commit message: " commit_msg
            if [ -z "$commit_msg" ]; then
                commit_msg="Daily progress: $(date +%Y-%m-%d)"
            fi
            git add .
            git commit -m "$commit_msg"
            print_success "Changes committed"
        fi
    fi

    # Pull first to avoid conflicts
    print_info "Pulling latest changes from remote..."
    git pull --rebase

    # Push commits
    print_info "Pushing commits to GitHub..."
    git push
    print_success "Commits pushed"

    # Push tags
    print_info "Pushing tags to GitHub..."
    git push origin --tags
    print_success "Tags pushed"

    echo ""
    print_success "Daily push complete!"
}

# Create milestone
create_milestone() {
    MILESTONE_NAME=${1:-"milestone-$(date +%Y%m%d)"}
    MILESTONE_MESSAGE=${2:-"Milestone: $MILESTONE_NAME"}

    print_info "Creating milestone: $MILESTONE_NAME"

    # Check for uncommitted changes
    if ! git diff-index --quiet HEAD --; then
        print_error "You have uncommitted changes. Please commit first."
        exit 1
    fi

    # Create tag
    git tag -a "$MILESTONE_NAME" -m "$MILESTONE_MESSAGE"
    print_success "Milestone created: $MILESTONE_NAME"

    # Push tag
    git push origin "$MILESTONE_NAME"
    print_success "Milestone pushed to GitHub"
}

# Show recovery options
show_recovery() {
    print_info "Recovery Options"
    echo ""

    print_info "Recent reflog entries (last 20):"
    git reflog -20 | head -20
    echo ""

    print_info "Recent commits:"
    git log --oneline -10
    echo ""

    print_info "Available checkpoints:"
    git tag | grep checkpoint || echo "No checkpoints found"
    echo ""

    print_info "Available milestones:"
    git tag | grep milestone || echo "No milestones found"
    echo ""

    echo "Recovery commands:"
    echo "  git reflog                    # Show all operations"
    echo "  git checkout abc1234          # Restore to commit abc1234"
    echo "  git reset --soft HEAD~1       # Undo last commit (keep changes)"
    echo "  git reset --hard HEAD~1       # Undo last commit (discard changes)"
    echo "  git checkout checkpoint-XYZ   # Restore to checkpoint"
}

# List branches
list_branches() {
    print_info "All Branches:"
    echo ""
    git branch -a
    echo ""

    print_info "Current branch:"
    git branch --show-current
}

# Clean merged branches
clean_branches() {
    print_info "Cleaning merged branches..."

    # Show merged branches
    print_info "Merged branches:"
    git branch --merged | grep -v "^\*" | grep -v "main" | grep -v "master" | grep -v "lightweight-master"

    read -p "Delete these branches? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git branch --merged | grep -v "^\*" | grep -v "main" | grep -v "master" | grep -v "lightweight-master" | xargs -r git branch -d
        print_success "Merged branches deleted"
    fi
}

# Backup to all remotes
backup_all() {
    print_info "Backing up to all remotes..."

    # Get all remotes
    REMOTES=$(git remote)

    if [ -z "$REMOTES" ]; then
        print_error "No remotes found"
        exit 1
    fi

    for remote in $REMOTES; do
        print_info "Pushing to $remote..."
        git push "$remote" --all
        git push "$remote" --tags
        print_success "Backup to $remote complete"
    done
}

# Main command dispatcher
case "${1:-help}" in
    status)
        show_status
        ;;
    checkpoint)
        create_checkpoint
        ;;
    daily-push)
        daily_push
        ;;
    milestone)
        create_milestone "$2" "$3"
        ;;
    recover)
        show_recovery
        ;;
    branches)
        list_branches
        ;;
    clean-branches)
        clean_branches
        ;;
    backup)
        backup_all
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        print_error "Unknown command: $1"
        show_usage
        exit 1
        ;;
esac
