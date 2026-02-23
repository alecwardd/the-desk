---
name: frontend-check
description: Run TypeScript type-check, lint, and frontend tests. USE WHEN changing React/TypeScript code or debugging UI regressions.
---

# /frontend-check

Run frontend verification for The Desk UI layer.

## Steps

1. **Type-check and lint:**
   ```bash
   npx tsc --noEmit && npx eslint src/ --max-warnings 0
   ```

2. **Run frontend tests:**
   ```bash
   npm test
   ```

3. **Report:**
   - TypeScript status (pass/fail with top errors)
   - ESLint status (clean or violations)
   - Test result summary (pass/fail count)

## Notes

- Keep business logic out of components; route IPC through `src/lib/tauri-bridge.ts`.
- If tests fail, include failing component/hook names and likely root cause.
