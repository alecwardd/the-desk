# Backtest Runbook — IDEA-000 (Regime Gate) & IDEA-012 (Absorption Failure)

After PR1 (`regime`, `ib_extension_state`) and PR2 (`absorption_invalidated`), both ideas are
backtestable through the standard hypothesis loop. Run these from a Cursor agent (or any MCP client)
against the live `~/.the-desk/data.db`. The rules engine is at `RULES_ENGINE_SCHEMA_VERSION = 3`, so
fresh backtests are current and will pass the engine-version freshness gate.

**Window:** `2025-11-28` → `2026-03-06` (the 81-session validated RTH sample). Regime states are rare
(`up_only` was ~12 sessions in this window), so watch the `dryRun` projection — if `feasibleForN30`
is false, widen the window before registering.

---

## Sequence (per hypothesis)

1. **Baseline integrity** — `validate_signal_outcome_integrity({ "source": "backtest" })`
2. **Dry-run feasibility** — `register_hypothesis({ ..., "dryRun": true })`; check `feasibleForN30`,
   `projectedSampleSize`, and `warnings`.
3. **Register** — `register_hypothesis({ ..., "dryRun": false })`.
4. **Backtest** — `run_backtest({ "startDate": "2025-11-28", "endDate": "2026-03-06",
   "setupIds": ["<id>"], "waitForCompletion": true })`; capture the returned `jobId`.
5. **Integrity of the run** — `validate_signal_outcome_integrity({ "source": "backtest",
   "jobId": "<jobId>", "setupId": "<id>" })`; proceed only if `status = "ok"`.
6. **Read stats** — `query_signal_outcome_distribution` / `_conditional` / `_excursions` with
   `{ "setupId": "<id>", "jobId": "<jobId>", "source": "backtest", "includeUnverified": false }`.
7. **Gate** — `propose_draft_setup({ "setupId": "<id>", "jobId": "<jobId>" })` (hypothesis→draft on pass).
8. **Activate** (only after you review the stats) —
   `activate_draft_setup({ "setupId": "<id>", "traderConfirmation": "<your note>" })`.

For **IDEA-000**, the question is *gated vs ungated*: register both the gated and baseline variants
below, backtest both, and compare distributions at step 6. Only promote the gate if it improves
expectancy / win rate over the ungated baseline.

---

## IDEA-000 — Regime-Gated One-Sided Acceptance (Long)

### Gated variant

```json
{
  "metadata": {
    "hypothesisId": "IDEA-000-gate-long",
    "version": 1,
    "docReference": "IDEA-000",
    "proseSummary": "Continuation long gated on computed regime=OneSidedAcceptance with up-only IB extension.",
    "owner": "user",
    "sessionScope": ["rth"]
  },
  "setupDefinition": {
    "id": "hyp_IDEA-000_gate_long_v1",
    "name": "IDEA-000 One-Sided Acceptance Gate (Long)",
    "description": "Fires when regime=OneSidedAcceptance, ib_extension_state=UpOnly, and price accepted above VWAP.",
    "active": false,
    "conditions": [
      "{\"id\":\"c1\",\"field\":\"regime\",\"operator\":\"equals\",\"value\":\"OneSidedAcceptance\"}",
      "{\"id\":\"c2\",\"field\":\"ib_extension_state\",\"operator\":\"equals\",\"value\":\"UpOnly\"}",
      "{\"id\":\"c3\",\"field\":\"price_vs_vwap\",\"operator\":\"above\"}"
    ],
    "stopLogic": { "mode": "fixed_points", "direction": "long", "points": 12 },
    "targets": [ { "mode": "fixed_points", "direction": "long", "points": 18, "label": "1.5R fixed target" } ],
    "positionSizing": { "r_points": 12 },
    "templateSource": "hypothesis:IDEA-000:v1"
  },
  "dryRun": true
}
```

### Ungated baseline (drop the `regime` condition)

Same as above with `id` = `hyp_IDEA-000_baseline_long_v1`, name "(Baseline, Ungated)", and
`conditions` reduced to `c2` (ib_extension_state=UpOnly) + `c3` (price_vs_vwap above). This is the
control: if the gated variant does not beat it, the regime gate is not earning its place.

> Short mirrors: swap `UpOnly`→`DownOnly`, `price_vs_vwap above`→`below`, and `direction long`→`short`.

---

## IDEA-012 — Absorption Failure / Liquidity Vacuum (Short)

```json
{
  "metadata": {
    "hypothesisId": "IDEA-012-vacuum-short",
    "version": 1,
    "docReference": "IDEA-012",
    "proseSummary": "Failed absorption below VWAP resolves into a downside liquidity vacuum.",
    "owner": "user",
    "sessionScope": ["rth"]
  },
  "setupDefinition": {
    "id": "hyp_IDEA-012_vacuum_short_v1",
    "name": "IDEA-012 Absorption Failure / Vacuum (Short)",
    "description": "Fires when a recent absorption invalidates while price is below VWAP — trade through the failed zone, not at it.",
    "active": false,
    "conditions": [
      "{\"id\":\"c1\",\"field\":\"absorption_invalidated\",\"operator\":\"equals\",\"value\":true}",
      "{\"id\":\"c2\",\"field\":\"price_vs_vwap\",\"operator\":\"below\"}"
    ],
    "stopLogic": { "mode": "fixed_points", "direction": "short", "points": 10 },
    "targets": [ { "mode": "fixed_points", "direction": "short", "points": 20, "label": "2R fixed target" } ],
    "positionSizing": { "r_points": 10 },
    "templateSource": "hypothesis:IDEA-012:v1"
  },
  "dryRun": true
}
```

> Long mirror: swap `price_vs_vwap below`→`above` and `direction short`→`long`. The local sample hint
> (failed down-absorption flipped to opposite-direction closes 58.8% of the time) suggests the
> long-from-failed-low side is worth testing too — let the verified distribution decide.

---

## Caveats

- `absorption_invalidated` direction is not yet a condition field, so these scope direction via
  `price_vs_vwap`. If the backtest shows edge but is direction-noisy, the next refinement is a
  dedicated `absorption_invalidation_direction` field.
- Keep every setup `active: false` at registration; activation is gated and explicit.
- `run_backtest` needs SCID / raw-tick coverage for the window; if results are empty, confirm ingest
  with the raw-tick gap tools first.
