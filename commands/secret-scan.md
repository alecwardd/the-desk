---
name: secret-scan
description: Scan source files for likely API key leaks and secret patterns. USE WHEN before commit, before opening a PR, or after config/auth changes.
---

# /secret-scan

Scan the repository for common secret patterns.

## Steps

1. **Run scan with ripgrep (matches pre-commit secret convention):**
   ```bash
   rg -n -i "(sk-ant-[A-Za-z0-9_-]{20,}|sk_live_[A-Za-z0-9_-]{20,}|anthropic_api_key\\s*=\\s*['\"][A-Za-z0-9_-]{20,})" src src-tauri AGENT.md CLAUDE.md .cursorrules .cursor/commands .cursor/rules commands
   ```

2. **If no matches are found, report:**
   - Secret scan status: clean

3. **If matches are found, report:**
   - File paths and line references
   - Whether each match appears to be real secret material or a false positive
   - Required remediation steps before commit

## Notes

- Never commit real credentials or tokens.
- Use environment variables or local config excluded from version control.
