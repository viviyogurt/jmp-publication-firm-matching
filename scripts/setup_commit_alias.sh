#!/bin/bash
# Setup commit command shortcuts for JMP project

echo "=== Setting up commit command shortcuts ==="
echo ""

# Detect shell configuration file
SHELL_CONFIG=""
if [ -n "$ZSH_VERSION" ]; then
    SHELL_CONFIG="$HOME/.zshrc"
elif [ -n "$BASH_VERSION" ]; then
    SHELL_CONFIG="$HOME/.bashrc"
else
    SHELL_CONFIG="$HOME/.profile"
fi

echo "Detected shell config: $SHELL_CONFIG"
echo ""

# Add alias to shell config
if ! grep -q "commit.*jmp.*commit\.sh" "$SHELL_CONFIG" 2>/dev/null; then
    echo "" >> "$SHELL_CONFIG"
    echo "# JMP project commit command" >> "$SHELL_CONFIG"
    echo "alias commit='cd /home/kurtluo/yannan/jmp && ./commit.sh'" >> "$SHELL_CONFIG"
    echo "" >> "$SHELL_CONFIG"

    echo "✓ Added 'commit' alias to $SHELL_CONFIG"
    echo ""
    echo "To use immediately, run:"
    echo "  source $SHELL_CONFIG"
    echo ""
    echo "Or restart your terminal."
    echo ""
    echo "Then you can use:"
    echo "  commit              # Auto-generate message"
    echo "  commit \"custom msg\" # Use custom message"
else
    echo "✓ 'commit' alias already exists in $SHELL_CONFIG"
fi

# Also create /commit wrapper script option
echo ""
echo "=== Creating /commit wrapper ==="
cat > /home/kurtluo/yannan/jmp/commit-wrapper.sh << 'EOF'
#!/bin/bash
# Wrapper script for commit command
# Usage: /commit "message" or /commit

cd /home/kurtluo/yannan/jmp
exec ./commit.sh "$@"
EOF

chmod +x /home/kurtluo/yannan/jmp/commit-wrapper.sh

echo "✓ Created commit-wrapper.sh"
echo ""
echo "You can also create a symlink to use '/commit':"
echo "  sudo ln -sf /home/kurtluo/yannan/jmp/commit-wrapper.sh /usr/local/bin/commit"
echo ""

echo "=== Summary ==="
echo ""
echo "After restarting terminal or sourcing .bashrc, you can use:"
echo ""
echo "  Option 1 (alias - RECOMMENDED):"
echo "    commit              # Auto-generate message"
echo "    commit \"custom\"     # Custom message"
echo ""
echo "  Option 2 (direct):"
echo "    ./commit.sh         # From jmp directory"
echo ""
echo "  Option 3 (symlink):"
echo "    /commit             # If you create the symlink"
echo ""
