---
name: quick-check
description: Fast verification — Rust build and tests. USE WHEN iterating on backend changes.
---

# /quick-check

Run a quick compile and test pass.

## Steps

1. **Rust build and tests:**
   ```bash
   cargo build && cargo test
   ```

2. **Secret scan** (optional but recommended):
   ```bash
   rg -n -i "(sk-ant-[A-Za-z0-9_-]{20,}|sk_live_[A-Za-z0-9_-]{20,}|anthropic_api_key\\s*=\\s*['\"][A-Za-z0-9_-]{20,})" src AGENT.md CLAUDE.md .cursorrules .cursor/commands .cursor/rules commands
   ```

3. Report:
   - Build: success/failure
   - Tests: pass count / failures
   - Secret scan: clean or violations
