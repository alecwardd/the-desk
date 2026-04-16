---
name: build-check
description: Full build verification — Rust format, clippy, compile, tests. USE WHEN finishing a feature, before committing, or when CI fails.
---

# /build-check

Run the complete build and test pipeline.

## Steps

1. **Rust build and lint:**
   ```bash
   cargo fmt --check && cargo clippy -- -D warnings && cargo build && cargo test
   ```

2. **Secret scan** (check for accidentally committed API keys):
   ```bash
   rg -n -i "(sk-ant-[A-Za-z0-9_-]{20,}|sk_live_[A-Za-z0-9_-]{20,}|anthropic_api_key\\s*=\\s*['\"][A-Za-z0-9_-]{20,})" src agents commands skills AGENT.md CLAUDE.md .cursorrules
   ```

3. Report:
   - Rust: compile status, clippy warnings, test results (pass/fail count)
   - Secret scan: clean or violations found
   - Overall: PASS or FAIL with specific issues listed

## Pre-Commit Usage

This command should be run (or its steps incorporated into git hooks) before every commit. Failing any step means the commit should not proceed.
