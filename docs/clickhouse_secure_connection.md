# Secure ClickHouse Connection Methods

## Problem
Putting passwords directly in command line is insecure (visible in process list, shell history).

## Solutions

### Method 1: Interactive Password Prompt (Recommended for Ad-Hoc Use)

**Command:**
```bash
clickhouse-client --host chenlin04.fbe.hku.hk --user yannan --database claude --password
```

**How it works:**
- The `--password` flag without a value will prompt you to enter the password interactively
- Password is not visible on screen and not stored in history

**Usage:**
```bash
clickhouse-client --host chenlin04.fbe.hku.hk --user yannan --database claude --password
# Will prompt: Enter password:
# Type password (hidden): alaniscoolerthanluoye
```

### Method 2: Environment Variable

**Set environment variable:**
```bash
export CLICKHOUSE_PASSWORD="alaniscoolerthanluoye"
```

**Use in command:**
```bash
clickhouse-client --host chenlin04.fbe.hku.hk --user yannan --database claude --password="$CLICKHOUSE_PASSWORD"
```

**Or ClickHouse automatically reads:**
```bash
export CLICKHOUSE_PASSWORD="alaniscoolerthanluoye"
clickhouse-client --host chenlin04.fbe.hku.hk --user yannan --database claude
# ClickHouse client automatically uses CLICKHOUSE_PASSWORD env var
```

**Make it persistent (add to ~/.bashrc or ~/.zshrc):**
```bash
echo 'export CLICKHOUSE_PASSWORD="alaniscoolerthanluoye"' >> ~/.bashrc
source ~/.bashrc
```

### Method 3: Config File (Recommended for Regular Use)

**Create config file:**
```bash
mkdir -p ~/.clickhouse-client
cat > ~/.clickhouse-client/config.xml << 'EOF'
<config>
    <user>yannan</user>
    <password>alaniscoolerthanluoye</password>
    <host>chenlin04.fbe.hku.hk</host>
    <port>9000</port>
    <secure>false</secure>
</config>
EOF

# Set secure permissions
chmod 600 ~/.clickhouse-client/config.xml
```

**Then use simple command:**
```bash
clickhouse-client --database claude
```

**Or use separate config file:**
```bash
clickhouse-client --config-file ~/.clickhouse-client/config.xml --database claude
```

### Method 4: Credentials File (Most Secure)

**Create credentials file:**
```bash
cat > ~/.clickhouse-client/credentials << 'EOF'
chenlin04.fbe.hku.hk:9000:yannan:alaniscoolerthanluoye
EOF

# Set secure permissions (IMPORTANT!)
chmod 600 ~/.clickhouse-client/credentials
```

**Use with:**
```bash
clickhouse-client --host chenlin04.fbe.hku.hk --user yannan --database claude --password-file ~/.clickhouse-client/credentials
```

### Method 5: Alias with Interactive Prompt

**Add to ~/.bashrc or ~/.zshrc:**
```bash
alias clickhouse-claude='clickhouse-client --host chenlin04.fbe.hku.hk --user yannan --database claude --password'
```

**Usage:**
```bash
clickhouse-claude
# Will prompt for password interactively
```

## Security Best Practices

1. **Never commit passwords to git**
   - Add credential files to `.gitignore`
   - Use environment variables or config files outside repo

2. **Set proper file permissions**
   ```bash
   chmod 600 ~/.clickhouse-client/config.xml
   chmod 600 ~/.clickhouse-client/credentials
   ```

3. **Use interactive prompt for one-time use**
   - Most secure for ad-hoc queries
   - No password stored anywhere

4. **Use environment variables for scripts**
   - Can be set per-session
   - Not visible in process list (if set before script runs)

5. **Use config files for regular use**
   - Most convenient
   - Keep file permissions restricted (600)

## Recommended Approach

**For ad-hoc queries:**
```bash
clickhouse-client --host chenlin04.fbe.hku.hk --user yannan --database claude --password
```

**For scripts/automation:**
```bash
# Set in script or environment
export CLICKHOUSE_PASSWORD="alaniscoolerthanluoye"
clickhouse-client --host chenlin04.fbe.hku.hk --user yannan --database claude
```

**For regular use:**
- Create config file with proper permissions
- Use simple command: `clickhouse-client --database claude`

