---
name: pipeline-test
description: Run all market structure pipeline tests with sample NQ data. USE WHEN you've modified any pipeline (VWAP, TPO, Delta, Levels) and need to verify correctness.
---

# /pipeline-test

Run all pipeline unit tests and report results.

## Steps

1. Run Rust pipeline tests:
   ```bash
   cargo test --lib pipelines -- --nocapture
   ```

2. If tests pass, report: which pipelines were tested, number of assertions, all passing.

3. If tests fail, show:
   - Which pipeline failed
   - Expected vs. actual values
   - The specific test case that failed
   - Reference the relevant section of `skills/trading-domain/SKILL.md` for the correct calculation method

## Known Test Data

When writing new pipeline tests, use these verified NQ data points:

**VWAP Test:** If three trades occur at (21400.00, vol=10), (21401.00, vol=5), (21399.00, vol=15), then:
- VWAP = (21400*10 + 21401*5 + 21399*15) / (10+5+15) = 21399.83̄

**Delta Test:** If trade at ask price (buy) vol=10 and trade at bid price (sell) vol=7, then:
- Cumulative delta = +10 - 7 = +3

**TPO Test:** NQ tick size is 0.25. Price levels must be discretized to 0.25 increments.
