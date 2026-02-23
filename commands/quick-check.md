---
name: quick-check
description: Fast pre-commit validation (Rust build/tests + TS type/lint + secret scan). USE WHEN iterating quickly before running full build-check.
---

# /quick-check

Run a faster subset of `/build-check` for rapid feedback.

## Steps

1. **Rust compile + tests (skip clippy for speed):**
   ```bash
   cd src-tauri && cargo build && cargo test
   ```

2. **TypeScript type-check + lint:**
   ```bash
   npx tsc --noEmit && npx eslint src/ --max-warnings 0
   ```

3. **Secret scan:**
   ```bash
   rg -n -i "(sk-ant-[A-Za-z0-9_-]{20,}|sk_live_[A-Za-z0-9_-]{20,}|anthropic_api_key\\s*=\\s*['\"][A-Za-z0-9_-]{20,})" src src-tauri AGENT.md CLAUDE.md .cursorrules .cursor/commands .cursor/rules commands
   ```

4. **Report:**
   - Rust build/test status
   - Type-check/lint status
   - Secret scan status
   - Overall PASS/FAIL

## Notes

- Run `/build-check` before merging or release work.
- If quick-check fails, fix issues first, then rerun.
