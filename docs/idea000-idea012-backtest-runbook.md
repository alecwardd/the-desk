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

**v2 (refined 2026-06-23):** the v1 run had *no entry trigger* — it fired anywhere price was above
VWAP, i.e. chasing extended price. Both gated and ungated were negative because the entry, not just
the gate, lacked edge. v2 adds a **pullback-proximity trigger**: enter long only when price has pulled
back to *within 8 pts above* VWAP (`price_vs_vwap within 8` AND `price_vs_vwap above`), and raises
suppression so each pullback episode is one entry. This re-asks the real question — does the regime
gate help once the entry is a disciplined pullback rather than a chase? No code change; uses the
`within` operator already in the engine.

### Gated variant

```json
{
  "metadata": {
    "hypothesisId": "IDEA-000-gate-long",
    "version": 2,
    "docReference": "IDEA-000",
    "proseSummary": "Continuation long on a pullback to VWAP, gated on regime=OneSidedAcceptance with up-only IB extension.",
    "owner": "user",
    "sessionScope": ["rth"]
  },
  "setupDefinition": {
    "id": "hyp_IDEA-000_gate_long_v2",
    "name": "IDEA-000 One-Sided Acceptance Gate (Long) v2",
    "description": "Fires when regime=OneSidedAcceptance, ib_extension_state=UpOnly, and price has pulled back to within 8 pts above VWAP.",
    "active": false,
    "duplicateSuppressionMs": 600000,
    "conditions": [
      "{\"id\":\"c1\",\"field\":\"regime\",\"operator\":\"equals\",\"value\":\"OneSidedAcceptance\"}",
      "{\"id\":\"c2\",\"field\":\"ib_extension_state\",\"operator\":\"equals\",\"value\":\"UpOnly\"}",
      "{\"id\":\"c3\",\"field\":\"price_vs_vwap\",\"operator\":\"above\"}",
      "{\"id\":\"c4\",\"field\":\"price_vs_vwap\",\"operator\":\"within\",\"value\":8.0}"
    ],
    "stopLogic": { "mode": "fixed_points", "direction": "long", "points": 12 },
    "targets": [ { "mode": "fixed_points", "direction": "long", "points": 18, "label": "1.5R fixed target" } ],
    "positionSizing": { "r_points": 12 },
    "templateSource": "hypothesis:IDEA-000:v2"
  },
  "dryRun": true
}
```

### Ungated baseline (drop the `regime` condition)

Same as above with `id` = `hyp_IDEA-000_baseline_long_v2`, name "(Baseline, Ungated) v2", and `c1`
removed (keep `c2`/`c3`/`c4`). This is the control: if the gated variant does not beat it, the regime
gate is not earning its place — even with the better entry.

> Short mirrors: swap `UpOnly`→`DownOnly` and `price_vs_vwap above`→`below` (the `within 8` pullback
> condition stays; combined with `below` it means within 8 pts *under* VWAP), and `direction long`→`short`.
>
> The 8-pt pullback band and 12-pt stop are starting points — tune from the MFE/MAE distribution once
> v2 produces verified outcomes.

---

## IDEA-012 — Absorption Failure / Liquidity Vacuum (Short) — v2 (refined 2026-06-23)

The v1 run rejected because it fired ~20×/RTH session: it omitted the doc's required **pace-expansion**
filter and used the 2s default suppression against a 45s state flag, so one failure counted ~20 times.
v2 tightens with three changes: scope direction via the new `absorption_invalidation_direction` field
(a failed **up**-defense → downside vacuum → short), require **pace expansion** (`tape_pace_percentile`),
and raise `duplicateSuppressionMs` so a single failure is one signal.

```json
{
  "metadata": {
    "hypothesisId": "IDEA-012-vacuum-short",
    "version": 2,
    "docReference": "IDEA-012",
    "proseSummary": "Failed up-defense + pace expansion resolves into a downside liquidity vacuum.",
    "owner": "user",
    "sessionScope": ["rth"]
  },
  "setupDefinition": {
    "id": "hyp_IDEA-012_vacuum_short_v2",
    "name": "IDEA-012 Absorption Failure / Vacuum (Short) v2",
    "description": "A recent up-rejection absorption fails while pace expands — trade the downside vacuum through the failed zone, not at it.",
    "active": false,
    "duplicateSuppressionMs": 300000,
    "conditions": [
      "{\"id\":\"c1\",\"field\":\"absorption_invalidated\",\"operator\":\"equals\",\"value\":true}",
      "{\"id\":\"c2\",\"field\":\"absorption_invalidation_direction\",\"operator\":\"equals\",\"value\":\"up\"}",
      "{\"id\":\"c3\",\"field\":\"tape_pace_percentile\",\"operator\":\"above\",\"value\":0.7}"
    ],
    "stopLogic": { "mode": "fixed_points", "direction": "short", "points": 10 },
    "targets": [ { "mode": "fixed_points", "direction": "short", "points": 20, "label": "2R fixed target" } ],
    "positionSizing": { "r_points": 10 },
    "templateSource": "hypothesis:IDEA-012:v2"
  },
  "dryRun": true
}
```

> **Direction semantics:** `absorption_invalidation_direction` is the *original* (pre-failure)
> rejection direction, and the trade is the **opposite** — a failed `up`-defense (buyers couldn't hold)
> resolves *down* → short; a failed `down`-defense resolves *up* → long.
>
> Long mirror (`hyp_IDEA-012_vacuum_long_v2`): set `absorption_invalidation_direction = "down"` and
> `direction long`. The local sample hint (failed down-absorption flipped to opposite-direction closes
> 58.8% of the time) makes the long-from-failed-low side the one to watch.

If v2 still fires too often or shows no edge, the next lever is requiring key-level proximity at the
failed zone (per the IDEA-012 "Critical Rule": failed defense **+** pace expansion **+** liquidity pull).
The over-firing is now auto-flagged: `summarize_hypothesis_run` reports `signalsPerActiveSession` and a
`chatty` boolean, and emits an `over_firing` warning above ~5 signals per active session — so you no
longer have to eyeball N to catch a state-flag setup re-firing.

---

## Caveats

- **Direction is now a field** (`absorption_invalidation_direction`, added 2026-06-23); v2 uses it
  instead of the looser `price_vs_vwap` scoping.
- After this field addition the rules engine is at `RULES_ENGINE_SCHEMA_VERSION = 4` — rebuild
  `target/release/the-desk-mcp.exe` and restart the Cursor MCP server before registering, or it will
  reject the new field (see `docs/setup-ideas-and-backtesting.md` infra findings).
- Pass the contract that was front during the window (`NQH6.CME` for 2025-11-28…2026-03-06) with
  `force: true`. **Once the contract-routing change is deployed** (rebuild + restart), pass it directly
  to `run_backtest` as `{ "contract": "NQH6.CME" }` — this replays that contract's `.scid` without
  touching `active_symbol_override`, so you no longer flip global config and live trading stays
  isolated on the current front month. Until then, use the temporary `active_symbol_override` flip.
  Either way, a coverage mismatch now surfaces a `scid_window_mismatch_warning` and
  `integrity_status:"warning"` instead of a silent zero.
- Keep every setup `active: false` at registration; activation is gated and explicit.
- **Run heavy backtests against an isolated DB** so they never contend with the live server's single
  writer (and never bloat the ~600 GB live `data.db`). Build one with
  `the-desk-mcp --seed-backtest-db --to <dest.db> [--from <live data.db>]`: it copies only the small
  *reference* tables (`session_summaries` for RVOL curves, `prior_day_levels`, `risk_config`,
  `setups`, `research_hypotheses`) — **not** `raw_ticks` (the replay reads ticks from `.scid`). This
  gives the replay valid historical inputs *and* meaningful dry-run projections, unlike a blank
  fresh DB. Point the runner / backtest at `<dest.db>`.
