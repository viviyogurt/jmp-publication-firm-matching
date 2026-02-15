#!/bin/bash
# Demo: Test auto-commit feature with different scenarios

echo "=== Auto-Commit Feature Demo ==="
echo ""

# Scenario 1: Bug fix
echo "Scenario 1: Simulating a bug fix in linking code"
cat > /tmp/test_fix.py << 'EOF'
# Fix: Correct ticker matching logic
def match_ticker(inst_name, firm_ticker):
    """Fixed version - properly handles edge cases"""
    if not inst_name or not firm_ticker:
        return False
    return firm_ticker.upper() in inst_name.upper()
EOF
echo "Created bug fix script"
echo ""

# Scenario 2: New feature
echo "Scenario 2: Adding new documentation"
cat > /tmp/NEW_FEATURE_GUIDE.md << 'EOF'
# New Matching Feature Guide

This guide describes the new matching feature.
EOF
echo "Created new documentation"
echo ""

# Scenario 3: Optimization
echo "Scenario 3: Performance optimization note"
cat > /tmp/OPTIMIZATION_NOTES.md << 'EOF'
# Performance Optimization

Optimized the matching algorithm for faster processing.
Reduced runtime by 50% through vectorization.
EOF
echo "Created optimization notes"
echo ""

echo "=== Demo Complete ==="
echo "These would trigger different commit messages:"
echo "  1. Fix bug in entity linking"
echo "  2. Add documentation (1 doc)"
echo "  3. Optimize general"
echo ""
echo "Cleanup demo files..."
rm -f /tmp/test_fix.py /tmp/NEW_FEATURE_GUIDE.md /tmp/OPTIMIZATION_NOTES.md
echo "Done!"
