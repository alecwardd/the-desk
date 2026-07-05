# IDEA-011: One-Sided IB Extension Acceptance

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Backtesting-ready
**Source:** Local 2025-11-28 through 2026-03-06 database study
**Complements:** IB Extension Play (tpl_ib_extension), OR5 Mid Retest (tpl_or5_mid_retest)

**Concept:** The useful signal is not "IB extension happened." It is whether extension stayed one-sided or became two-sided. Two-sided extension usually means migration / auction, not trend acceptance.

**Local Statistics:**
- `up_only`: 12 sessions, 75.0% closed up
- `down_only`: 8 sessions, 62.5% closed down
- `both_sides`: 43 sessions, mixed / noisy
- `none`: 18 sessions

**Setup — One-Sided Acceptance Continuation:**
- Context:
  - First valid IB extension is one-sided
  - Opposite-side extension does not print
  - RVOL >= Elevated
  - Price remains accepted above VWAP + DNP for longs, below for shorts
- Entry:
  - First pullback to the extension origin, VWAP, OR5 mid, or developing value edge
- Stop:
  - Back inside IB or through the acceptance level
- Target:
  - 0.5x / 1.0x / 1.5x IB extensions
  - Late-session trend continuation only if opposite extension still absent

**Setup — Extension Failure Reclassification:**
- If the opposite-side extension prints:
  - Cancel continuation bias
  - Reclassify the day as migration / double-distribution until proven otherwise
  - Switch to responsive setups (DNVA, VWAP-band, inventory-clear, failed-break)

**Implementation Notes:**
- Add a session-level `ib_extension_state` enum:
  - `None`
  - `UpOnly`
  - `DownOnly`
  - `BothSides`
- Store the first extension timestamp and direction
- Use it as a hard filter for IB continuation and OR5 continuation logic

**Implementation status (2026-05-04):**
- `session_summaries` now stores `ib_extension_state`, `first_ib_extension_direction`, and `first_ib_extension_timestamp_ms` for RTH sessions.
- Backfill and live RTH close both derive the state from the 0.5x IB extension contract and enrich first direction/timestamp from `ib_extension_hit` event metadata when available.
- Poor-high / poor-low instrumentation remains intentionally deferred. IDEA-011 does not depend on those flags; revisit them in the TPO definition pass before using them for regime slicing.

**Backtesting Hypothesis:**
> When the first IB extension remains one-sided for at least 30 minutes and RVOL >= Elevated, what is the R-distribution of trading the first pullback in extension direction?

**Next verified backtest steps (post signal-outcome repair):**
1. Run `validate_signal_outcome_integrity({ source: "backtest" })` to capture the pre-rerun baseline and confirm old rows are mostly `legacyUnverified`.
2. Add or verify `ib_extension_state = None | UpOnly | DownOnly | BothSides` plus first extension timestamp/direction in the session or event surface used by the backtest.
3. Register a numerically backtestable IDEA-011 hypothesis/setup with explicit `direction`, fixed/named-level target logic, numeric stop logic, and finite positive `risk_points`.
4. Run a fresh backtest with a new `job_id` against the 2025-11-28 through 2026-03-06 RTH window, scoped to `source="backtest"`.
5. Inspect `backtest_runs.metrics.signalOutcomeIntegrity`; proceed only if `status="ok"` and the relevant setup rows are `verified`.
6. Query `query_signal_outcome_distribution`, `query_signal_outcome_conditional`, and `query_signal_outcome_excursions` with `jobId=<fresh job>`, `source="backtest"`, and `includeUnverified=false`.
7. Record the verified expectancy, sample size, R distribution, MFE/MAE, and regime split here before building the broader regime selector.
