# Research Notes — Snapshots, Backtest Findings & Sources

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

## March 2026 Research Snapshot

Grounding for the additions below. This pass combined:
- Local sample from `~/.the-desk/data.db`: 3.53M raw ticks, 191,819 `market_events`, 222 `session_summaries`
- Valid RTH sample: 81 usable RTH sessions from 2025-11-28 through 2026-03-06
- Current-market research as of 2026-03-09 on 0DTE, dealer gamma, CME liquidity, and around-the-clock NQ flow

### Style Inference From Existing Playbook

The current system clearly encodes a discretionary NQ/MNQ style built around:
- Market Profile / auction context first
- Levels as locations, not entries
- Delta, liquidity, and inventory confirmation before execution
- OR5 / IB / DNVA / DNP / VWAP / rebid-reoffer / session inventory / pinch concepts
- London and RTH handoff awareness

### Local Findings That Matter

These are the highest-signal observations from the local history sample:
- **Double Distribution dominates.** 52 of 81 valid RTH sessions were classified `DoubleDistribution`. Only 7 of 81 were `Trend`.
- **London did not carry cleanly into RTH.** London and RTH closed in the same direction only 41.5% of the time; reversal happened 58.5% of the time.
- **One-sided IB extension was cleaner than generic IB extension.**
  - `up_only`: 12 sessions, 75.0% closed up
  - `down_only`: 8 sessions, 62.5% closed down
  - `both_sides`: 43 sessions, noisy / mixed
- **Raw pinch was not compelling as a standalone directional edge.** Higher-severity pinch events did not show strong session-close alignment in the current sample.
- **Absorption failure looked more actionable than absorption itself.**
  - RTH `absorption_confirmed` with `direction=down` aligned with down closes only 38.9%
  - RTH `absorption_invalidated` with `direction=down` flipped to opposite-direction close behavior 58.8%

### Instrumentation Caveats

Do not use these fields for serious strategy selection until they are repaired or rerun under verified instrumentation:
- `signal_outcomes` instrumentation is repaired as of 2026-05-04, but older rows remain `legacyUnverified` unless a fresh backtest job produces `verified` rows under the current outcome engine
- `single_prints_direction` in `session_summaries` is currently not useful for statistical slicing
- `poor_high` / `poor_low` flags are sparse or incomplete in the current stored sample

**Implementation note (2026-05-04):** signal outcome generation now has a verified fire-time contract, auditable schema fields, source/job/quality filters, read-time R recomputation, and `validate_signal_outcome_integrity`. Treat this as an instrumentation repair, not as evidence that old `signal_outcomes` rows are trustworthy. The next evidence-producing step is to rerun target backtests with a fresh `job_id`, confirm `signalOutcomeIntegrity.status` is `ok`, then use only the verified run for setup statistics.

### Regime-First Conclusion

The strongest conclusion from this pass is not "add more standalone setups." It is:

> Add regime overlays first, then decide which existing setups are even allowed to fire.

Current local evidence suggests:
- Use **initiative / continuation logic** only when the day is proving one-sided and accepting away from balance
- Use **inventory-clear / mean-reversion / repair logic** when the session is behaving like a double-distribution migration or London-to-RTH unwind
- Treat **pinch**, **OR5**, and **raw absorption** as context-dependent, not standalone edge

**Implementation note (2026-06-22):** Template-library coverage was expanded from 9 to 13 in
`src/rules/setup_templates.rs`. Added short-side mirrors (OR5 Mid Retest, Single Print
Continuation, IB Extension, VWAP Band Zone) so continuation/responsive families are no longer
long-only, and tagged every template with a `regime` field (`continuation` | `responsive` |
`transition`) in `marketContext`. A non-destructive seeder (`seed_templates`, exposed via
`the-desk-mcp --seed-templates [--activate]`) idempotently loads these doctrine templates into the
playbook DB — closing the gap where `all_templates()` was never seeded. What is **not** yet done,
and still requires new `ConditionField` variants plus pipeline detection before it can fire live:

- **Regime gate (IDEA-000):** *Partially landed (2026-06-22).* `MarketState` now carries a computed
  `regime` (`OneSidedAcceptance`/`Migration`/`Transition`/`Unclear`) plus a live `ib_extension_state`,
  derived in `pipelines/regime.rs` from IB extension + day type + VWAP/DNP acceptance + participation.
  Both are addressable as rules-engine condition fields (`regime`, `ib_extension_state`), and
  `RULES_ENGINE_SCHEMA_VERSION` was bumped 1→2 (re-backtest hypotheses under v2). Still pending: the
  automatic *eligibility gate* that disables continuation families on `Migration`/`Transition` days
  before condition evaluation, plus a backtest of gated-vs-ungated expectancy. Classifier thresholds
  (`REGIME_ELEVATED_RVOL`, `REGIME_ELEVATED_PACE`) are deliberately provisional pending that backtest.
- **Reversal / trap family (IDEA-002, IDEA-003):** failed-breakout-state and naked-VPOC-proximity
  still have no condition fields today (`delta_confirmation_at_level` / `rebid_zone_held` currently
  always evaluate false). These must go through the `register_hypothesis` → `run_backtest` →
  `propose_draft_setup` → `activate_draft_setup` loop once the detection fields exist.
- **Absorption failure (IDEA-012):** *Landed (2026-06-22).* The absorption pipeline already ran a
  full detected→confirmed→invalidated state machine; PR2 surfaced the invalidation as
  `has_recent_invalidated_absorption` (+ price/direction/age/distance) on `MarketState` and as the
  `absorption_invalidated` condition field, mapped to the existing `absorption_invalidated` market
  event for sample-size projection. `RULES_ENGINE_SCHEMA_VERSION` bumped 2→3. Ready to register and
  backtest a failed-absorption / liquidity-vacuum setup; not yet wired into a template or activated.

### Backtest Results (2026-06-23) — all four hypotheses REJECTED

Window `2025-11-28 → 2026-03-06`, job `091f54ef-3f3d-453b-a38e-0859e157c6ab`, contract `NQH6.CME`
(`force: true`), all integrity `ok`, all left inactive (no activation). **No setup earned a template.**

| Hypothesis | N | Win | Expectancy (R) | Verdict |
|---|---|---|---|---|
| IDEA-000 gated long (`hyp_idea-000-gate-long_v1`) | 90 | 30.0% | **−0.23** | Reject — loses to baseline |
| IDEA-000 baseline long (`hyp_idea-000-baseline-long_v1`) | 19 | 36.8% | −0.04 | Reject — N<30, still negative |
| IDEA-012 vacuum short (`hyp_idea-012-vacuum-short_v1`) | 1,720 | 35.2% | +0.06 | Reject — over-trading noise |
| IDEA-012 vacuum long (`hyp_idea-012-vacuum-long_v1`) | 1,646 | 32.6% | −0.02 | Reject — flat-negative |

**Interpretation:**
- **IDEA-000 gate adds samples but hurts** (gated −0.23R vs ungated −0.04R; 30% vs 37% win). The
  `regime=OneSidedAcceptance` filter is currently *admitting* worse trades, not selecting better ones.
  Both variants are negative because the underlying entry is a fixed-point continuation long fired on
  a static condition (no pullback trigger), tested in a quarter that was ~52/81 double-distribution
  and only 7/81 trend. The entry mechanics — not just the gate — lack edge. Do not activate; revisit
  the entry trigger and the classifier thresholds (`REGIME_ELEVATED_RVOL` / `REGIME_ELEVATED_PACE`)
  before re-testing the gate. *Refined (2026-06-23):* runbook v2 adds a pullback-proximity entry
  (`price_vs_vwap within 8` AND `above`) plus a 10-min suppression so the entry is a disciplined
  pullback, not a chase — no code change (uses the existing `within` operator). Awaiting re-backtest;
  if v2 is still negative, the regime/continuation track is likely dead in this market and the next
  move is a different idea, not more tuning.
- **IDEA-012 fires ~20×/RTH session** because `absorption_invalidated` is a 45s *state flag* that the
  rules engine re-evaluates every analysis pass, and the v1 spec used the 2s default suppression and
  omitted the doc's required pace-expansion filter. The +0.06R on N=1,720 is over-trading noise.
  *Refined (2026-06-23):* added the `absorption_invalidation_direction` condition field
  (`RULES_ENGINE_SCHEMA_VERSION` 3→4) and a v2 spec in the runbook — direction scoping +
  `tape_pace_percentile > 0.7` + `duplicateSuppressionMs = 300000` so one failure is one signal.
  Awaiting re-backtest under v4. *Tooling (2026-06-23):* `summarize_hypothesis_run` now reports
  `signalsPerActiveSession` + a `chatty` flag and emits an `over_firing` warning above ~5 signals per
  active session, so this class of over-trading auto-flags instead of needing manual N inspection.

**Infrastructure findings from this run (must fix before the next pass):**
1. **Stale MCP server rejected the new condition fields** until `target/release/the-desk-mcp.exe` was
   rebuilt. After any `ConditionField` change, rebuild the release binary and restart the Cursor MCP
   server before registering hypotheses.
2. **Silent zero-out on contract mismatch.** `config.toml` lived at `NQU6.CME` (Sept 2026+); this
   window needed `NQH6.CME` with `force: true`. A mismatched live contract makes the backtest return
   0 sessions / 0 signals **silently** — indistinguishable from "no setups fired."
   *Fixed (2026-06-23):* `run_backfill_job_with_options` now reads the SCID file's timestamp bounds
   and, when they do not overlap the requested window, pushes a `scid_window_mismatch_warning` into
   the job result (which flips `integrity_status` to `"warning"`), naming the configured contract,
   the file's actual coverage, and the requested window. Partial-coverage runs are unaffected.
   *Follow-up (2026-06-23):* `run_backtest` / `run_backfill` now accept an optional `contract`
   parameter (`resolve_contract_metadata_for_symbol` → per-job `ScidReader` + `contract_metadata`),
   so a backtest can pin the window's front contract **without** mutating global `active_symbol_override`.
   This removes the live/backtest config conflict — live trading can stay on the current front month
   while a backtest replays a prior contract concurrently. Deploy requires rebuild + MCP restart.
3. **Backtest ran the full snapshot + rules on *every* RTH tick** — far slower than live *and* less
   faithful (live coalesces via `analysis_min_interval_ms` / `analysis_max_ticks`, so the per-tick
   backtest found fire-points live would never check). *Fixed (2026-06-23):* the replay now coalesces
   the expensive full snapshot + rules generation onto the live cadence, while event detection and
   per-tick MFE/MAE outcome tracking stay per-tick. The job result now reports `analysisPasses`,
   `ticksPerAnalysisAvg`, `analysisMinIntervalMs`, and `analysisMaxTicks` so each run is auditable.
   This is faster *and* higher-fidelity; post-coalescing numbers are the valid ones (not comparable
   to pre-fix runs). Remaining speed levers (isolated DB copy, two-phase cache, parallel sessions)
   are tracked separately.

---

## Research Sources

| Source | Topics | Confidence |
|--------|--------|-----------|
| Lopez de Prado, "Advances in Financial Machine Learning" (2018) | Volume clock, imbalance bars, regime detection | Very High |
| Dalton, "Markets in Profile" | Opening types, day types, AMT | Very High |
| Dim, Eraker, Vilkov (2024) — SSRN 4692190 | 0DTE gamma effects | High |
| Garmash (2025) — SSRN 5329719 | 0DTE gamma hedging | High |
| Park & Kownatzki (2024) — SSRN 4872960 | Microstructure regimes, volatility scaling | High |
| CBOE Research | 0DTE market impact | High |
| Adams, Fontaine, Ornthanalai (2024) — Bank of Canada | 0DTE market dynamics | High |
| Cboe volume report (2026-03-04) | SPX 0DTE share of volume | High |
| CME around-the-clock liquidity note (2025) | NQ after-hours volume and earnings response | High |
| CME liquidity beyond order-book depth (2025) | Liquidity vacuum / fill-rate framing | High |
| Božović (2025) — SSRN 5223127 | Intraday jump clustering around open / close | High |
| Hawkes process forecasting — arxiv 2408.03594 | Order flow clustering | Medium-High |
| ICT/SMC practitioner community | FVG, SMT divergence, session sweeps | Medium |
| SpotGamma | GEX levels, gamma regime | Medium-High |

---

*Last updated: 2026-06-30*
