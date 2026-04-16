---
name: secret-scan
description: Scan staged and working tree files for accidentally committed API keys. USE WHEN reviewing security or before releases.
---

# /secret-scan

Search the repository for common secret patterns.

## Steps

1. Run ripgrep from the repo root:
   ```bash
   rg -n -i "(sk-ant-[A-Za-z0-9_-]{20,}|sk_live_[A-Za-z0-9_-]{20,}|anthropic_api_key\\s*=\\s*['\"][A-Za-z0-9_-]{20,})" src AGENT.md CLAUDE.md .cursorrules .cursor/commands .cursor/rules commands
   ```

2. Report any matches with file paths and line numbers.

3. If matches are false positives, document why before dismissing.
