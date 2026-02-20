---
name: build-check
description: Full build verification — Rust compile, TypeScript type-check, all tests. USE WHEN finishing a feature, before committing, or when CI fails.
---

# /build-check

Run the complete build and test pipeline.

## Steps

1. **Rust build and lint:**
   ```bash
   cd src-tauri && cargo fmt --check && cargo clippy -- -D warnings && cargo build && cargo test
   ```

2. **TypeScript type-check and lint:**
   ```bash
   npx tsc --noEmit && npx eslint src/ --max-warnings 0
   ```

3. **Frontend tests:**
   ```bash
   npm test
   ```

4. **Tauri build check** (compile the full app without packaging):
   ```bash
   cd src-tauri && cargo build
   ```

5. **Secret scan** (check for accidentally committed API keys):
   ```bash
   grep -r "sk-ant-\|sk_live\|ANTHROPIC_API_KEY\s*=" --include="*.ts" --include="*.rs" --include="*.toml" --include="*.json" src/ src-tauri/ || echo "No secrets found"
   ```

6. Report:
   - Rust: compile status, clippy warnings, test results (pass/fail count)
   - TypeScript: type errors, lint warnings, test results
   - Secret scan: clean or violations found
   - Overall: PASS or FAIL with specific issues listed

## Pre-Commit Usage

This command should be run (or its steps incorporated into git hooks) before every commit. Failing any step means the commit should not proceed.
