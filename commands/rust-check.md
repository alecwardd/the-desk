---
name: rust-check
description: Run Rust formatting, lint, build, and tests at repo root. USE WHEN changing Rust code or debugging backend/engine failures.
---

# /rust-check

Run the full Rust verification pipeline for The Desk backend.

## Steps

1. **Format check, lint, build, test:**
   ```bash
   cargo fmt --check && cargo clippy -- -D warnings && cargo build && cargo test
   ```

2. **If failures occur, report:**
   - Which stage failed (fmt, clippy, build, or test)
   - The first actionable error and file path
   - Test summary (pass/fail count) when tests run

3. **If all pass, report:**
   - Rust verification status: PASS
   - Clippy warning status: clean (`-D warnings`)
   - Test result count

## Notes

- Keep Layer 1/2 deterministic (no network calls in pipelines/rules engine).
- For pipeline or rules changes, confirm tests cover realistic NQ samples.
