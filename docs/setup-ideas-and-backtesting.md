# The Desk — Setup Ideas & Backtesting Research

Living document for trade setup ideas, backtesting hypotheses, research findings, and cross-cutting infrastructure work (pipelines, MCP server surface, multi-instrument support). Each idea is tracked from concept through validation.

> **Per-idea detail:** [`setup-ideas/index.md`](setup-ideas/index.md). This hub keeps cross-cutting material (snapshot, backtest results, roadmap, queue) plus a short stub per IDEA (status, source, framing, detail link). Idea bodies live in `docs/setup-ideas/IDEA-NNN-*.md`.

### Companion specs

Standalone deep-dive specs referenced by ideas in this document:

- **Multi-instrument flow architecture (NQ / MNQ / ES / MES)** — [`docs/multi-instrument-flow-architecture.md`](multi-instrument-flow-architecture.md) (tracked as IDEA-021): share structure / separate flow, mini-vs-micro flow-agreement → conviction & sizing, cross-asset NQ↔ES.
- **IDEA-000 / IDEA-012 backtest runbook** — [`docs/idea000-idea012-backtest-runbook.md`](idea000-idea012-backtest-runbook.md): copy-pasteable register → backtest → gate → activate sequence.
- **Social intelligence & continual learning (X/Twitter)** — [`docs/social-intelligence-roadmap.md`](social-intelligence-roadmap.md) (master feature track), [`docs/social-confluence-design.md`](social-confluence-design.md) (Phase A v1 spec), [`decision-log.md`](decision-log.md) ADR-020 (Pending), **IDEA-023** below: curated watchlist confluence, external hypothesis queue, subagent-scoped memory/research learning — never a playbook alert source. Access mode + cost still undecided.
- **Market-maker pressure inference** — [`docs/setup-ideas/IDEA-024-market-maker-pressure-inference.md`](setup-ideas/IDEA-024-market-maker-pressure-inference.md): Avellaneda-Stoikov-inspired, DOM/tape-grounded taxonomy for inferring passive defense, retreat, replenishment, and adverse-selection pressure without claiming hidden participant intent.
- **SPX/VIX RTH context feed for agents** — [`docs/setup-ideas/IDEA-028-spx-vix-rth-context-feed.md`](setup-ideas/IDEA-028-spx-vix-rth-context-feed.md): proposal for a derived RTH-only broad-market context packet that pairs Sierra SPX/VIX chart context with options/GEX maps and source notes, never as a standalone signal.
- **Sierra execution chart study context and exports** — [`docs/setup-ideas/IDEA-029-sierra-execution-chart-study-context.md`](setup-ideas/IDEA-029-sierra-execution-chart-study-context.md): docs-only promotion for adaptive volume-bar sizing, Tape Reader / Delta Dynamics review, leg-to-leg volume/delta profiles, and spreadsheet-study exports as context or offline research, never live chart automation.
- **NQ balance-zone taxonomy** — [`docs/setup-ideas/IDEA-030-nq-balance-zone-taxonomy.md`](setup-ideas/IDEA-030-nq-balance-zone-taxonomy.md): research queue for defining balance across TPO, volume, delta, multi-session value overlap, and external options context before testing any composite state.
- **Session range compression and expansion** — [`docs/setup-ideas/IDEA-031-session-range-compression-expansion.md`](setup-ideas/IDEA-031-session-range-compression-expansion.md): research queue for session-scoped range/realized-volatility transitions across RTH, Asia, London, and consecutive like sessions.
- **HMM lecture-notes repo-fit (docs-only)** — [`docs/setup-ideas/IDEA-032-hmm-lecture-notes-repo-fit.md`](setup-ideas/IDEA-032-hmm-lecture-notes-repo-fit.md): Miller 2016 HMM notes assessed as **ADAPT** reference for a future offline regime-research design; not a live signal and not an implement-now task.
- **Expected-range ATR / RV / IV research plan (docs-only)** — [`docs/setup-ideas/IDEA-033-expected-range-atr-rv-iv-research-plan.md`](setup-ideas/IDEA-033-expected-range-atr-rv-iv-research-plan.md): staged offline plan comparing ATR, realized vol, and provenance-gated IV (or labeled proxy) for session sizing vs runner decisions; no backtest executed.
- **Time-of-day liquidity-event calendar (offline research)** — [`docs/setup-ideas/IDEA-034-time-of-day-liquidity-events.md`](setup-ideas/IDEA-034-time-of-day-liquidity-events.md): bucket-stats evidence accepted after clean-`b63e83a` verification; calendar/event-rate extraction has not run, Stage 1 has not passed, and continuation/reversal remains gated.
- **Leg-to-leg volume/delta profile engine (docs-only)** — [`docs/setup-ideas/IDEA-035-leg-to-leg-profile-engine.md`](setup-ideas/IDEA-035-leg-to-leg-profile-engine.md): locked quantitative definitions for swing-anchored legs (k×ATR confirmation, delta-percentile provisional trigger), per-leg HVN/LVN/shelf/taper metrics, and a boundary-stability backtest plan plus a Sierra translation path; no backtest executed.
- **L2L pullback-join setup (docs-only)** — [`docs/setup-ideas/IDEA-036-l2l-pullback-join.md`](setup-ideas/IDEA-036-l2l-pullback-join.md): first tradeable setup on the IDEA-035 engine — second-touch pullback into the prior leg's LVN/shelf zone with taper + delta-realignment confirmation, structural "two LVNs away" invalidation, event-study outcome measurement; gated behind the engine's Stage 1 pass.

---

## How to Use This Document

| Status | Meaning |
|--------|---------|
| **Idea** | Concept identified, not yet researched or coded |
| **Researched** | Supporting evidence gathered, mechanics understood |
| **Prototyped** | Pipeline or detection logic implemented |
| **Backtesting-ready** | Instrumentation and setup mechanics are ready for a verified backtest rerun |
| **Backtesting** | Running through historical .scid data |
| **Validated** | Backtest results confirm edge; ready for template |
| **In Playbook** | Added to setup_templates.rs and active |
| **Rejected** | Tested and found no reliable edge |

---

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
  - `up_only`: 12 sessions, 75.0% closed up — **Insufficient** (`N=12` < 20); directional context only
  - `down_only`: 8 sessions, 62.5% closed down — **Insufficient** (`N=8` < 20); directional context only
  - `both_sides`: 43 sessions, noisy / mixed — Reportable (`N=43`) as mixed, not as a continuation edge
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

## Codebase Audit & Opinion

External codebase review synthesized into this document for traceability alongside research findings and the idea backlog. Paths are relative to the repository root unless noted.

### Overall verdict

This is a **serious, well-architected system** — not a hobby repo. ~36K LOC of Rust with a clean three-layer separation, incremental pipeline math, typed error boundaries, and 80+ unit tests. The domain correctness is the thing that impresses most: DNVA uses `|delta|` not signed delta, value area expands outward from POC (not "middle 70%"), OR/IB are correctly scoped by minute-of-session, single prints are tracked per period. These are the exact places bad trading software gets the math wrong, and this codebase does not. The research layer on top (81 RTH sessions yielding "Double Distribution dominates, London→RTH continues only 41.5%, absorption-failure > absorption") is genuinely the basis of a professional edge, not vibes.

That said, the project is in the zone where the next order of improvement is not more pipelines — it is **hardening the edges, tightening the agent surface, and closing the research→playbook loop**.

### Strengths to build on

1. **Three-layer discipline is holding.** No LLM calls in Rust, no raw ticks to Claude, no rules bypass. That architectural spine is what will let this scale to multi-instrument and multi-account without becoming spaghetti.
2. **Incremental math everywhere.** Every pipeline accumulates; nothing recomputes from scratch. This is the right ceiling for sub-ms tick latency and the reason 100-pt volatile opens do not melt the system.
3. **Terminology precision.** [CLAUDE.md](../CLAUDE.md) enforces it and the code reflects it. That is a moat — most trading tooling (retail and vendor) gets TPO/delta/value-area wrong.
4. **Research infrastructure exists.** [src/research/mod.rs](../src/research/mod.rs) plus [src/backfill.rs](../src/backfill.rs) plus the event detector means you can actually ask "given X, what is P(Y)?" against real history. Most traders never get there.
5. **Observability primitives are in place.** `McpFeedRuntimeState` in [src/bin/the-desk-mcp.rs](../src/bin/the-desk-mcp.rs) exposes tick freshness, lock contention, poll latency, SCID offsets, and now non-monotonic SCID counters via tools. Combined with `scan_scid_timestamp_anomalies`, this is a good foundation for feed diagnostics.
6. **This document (`setup-ideas-and-backtesting.md`) is gold.** It is the kind of living artifact that makes the rest of the system valuable. Keep investing here.

### Weakest points that need addressing

#### 8. End-to-end session replay golden test

Addressed with `tests/session_replay_golden.rs`: a deterministic two-session synthetic `.scid` replay now runs through the real historical backfill path and compares canonical session/event output against `tests/fixtures/session_replay/v1/expected_core.json`. The same test target also includes an ignored private-regression mode for real Sierra files via `THE_DESK_GOLDEN_SCID_DIR` / `THE_DESK_GOLDEN_EXPECTED_DIR`.

Follow-up hardening added a rules-enabled golden (`expected_rules.json`), a non-monotonic timestamp golden (`expected_non_monotonic.json`), explicit comparator tolerances, hermetic prior-day reference seeding, and CI coverage. Future-scoped replay work still worth tracking:

- Depth-aware golden replay for `.depth` / MarketDepthData once depth-derived behavior needs drift protection.
- Adversarial calendar fixtures: DST transition, holiday-shortened RTH, empty Globex, and early-close sessions.
- Private real-data provenance: sort or group by first SCID timestamp, or require sortable date-prefixed filenames.
- Golden failure artifacts under `target/` so reviewers can diff actual vs expected JSON outside the test runner.
- A small `xtask` or PowerShell helper for blessing goldens without hand-written environment commands.
- Fixture provenance metadata such as the commit SHA used when a golden was blessed.

### MCP server construction — specific read

[src/bin/the-desk-mcp.rs](../src/bin/the-desk-mcp.rs) at ~9K LOC with 50+ tools is approaching the point where **it should be split**. Right now it is a single file handling snapshots, profiles, microstructure, options, research, risk, memory, backfill, and ingest. Recommendations:

- **Module-split by domain:** `mcp/snapshots.rs`, `mcp/research.rs`, `mcp/risk.rs`, `mcp/memory.rs`, `mcp/backfill.rs`. Keeps each file <1K LOC and makes tool inventory reviewable.
- **Tool description quality is currently good-to-very-good** but uneven. For an agentic caller, descriptions should be written to answer "when should I call this vs. the adjacent tool?" — lean into disambiguation. E.g., `get_market_snapshot` vs `get_session_context` vs `get_snapshot_at(t)` — a 1-line "call this when…" clause dramatically improves agent tool selection.
- **Some overlap worth pruning:** the DOM tool family (`get_dom_snapshot_at`, `get_dom_window`, `get_dom_tape_context_at`, `explain_book_reaction`) is dense. Either consolidate or document the decision tree so an agent knows which one to reach for first.
- **Missing for "trading partner" use case:**
  - `compare_to_similar_sessions(criteria)` — "find N most similar historical sessions and show how they played out from here." This is the single highest-leverage tool you could add. The raw capability exists; it needs packaging.
  - `explain_current_setup_state()` — agent-friendly explanation of *why* a setup is at "Approaching" vs "Confirmed", citing which conditions are met/missing. Makes the black box legible.
  - `what_changed_since(t)` — diff of structure (new levels, POC shift, day-type reclassification, VA break). Perfect for coaching "hey, since 10:15 things changed…"
  - `risk_check_before_entry(setup_id, size)` — combines Kelly, current R used, consecutive-loss state, and day-type stats into a single "green/yellow/red" response.

### How to make this a higher-level agentic thinking system

3. **Session-relative context for the agent**  
   An agent that says "VWAP is at 21450, price is 21468" is info-dense but not *wise*. Wisdom comes from framing: "price 18 pts above VWAP, 1.2σ band, in a Double Distribution day where that condition closed back to VWAP 68% of the time this quarter." Build a **context-framing layer** between pipelines and the MCP tool response — same raw numbers, but every snapshot carries its historical interpretation. This is where the research DB earns its keep.

   **Implementation note (2026-05-01):** `get_context_frame` now provides the v1 version of this layer: stable buckets, weighted analogs, optional setup outcomes, indexed `pipeline_snapshots`, cache warming, and reliability caveats. Future work should focus on two production refinements before expanding the envelope: materialized per-bucket forward-outcome summaries for very large histories, and golden replay snapshots of the JSON envelope after a few live sessions confirm the agent phrasing is stable.

4. **A memory that knows *you***
   [agents/](../agents/) has role agents (orchestrator, levels-analyst, risk-coach, etc.) but there is no persistent model of **the trader**: best/worst day-types, consecutive-loss behavior, actual hit rate by setup and by time-of-day, typical R deviation. The implementation direction is a typed `get_trader_context_fit` envelope over existing SQLite memory: execution memory comes from `behavioral_patterns` generated from recorded trades, setup opportunity remains separate from `signal_outcomes` / `get_context_frame`, and coaching reminders come from insights/follow-ups. This source separation is how the system becomes a trading *partner* rather than a market-structure oracle or a second inconsistent aggregation engine.

   **Implementation note (2026-05-04):** Phase 0-2 of the trader memory layer are now implemented and committed. `get_trader_context_fit` is the primary structured memory surface: it separates executed-trade memory, setup opportunity context, coaching reminders, live risk/post-loss state, reliability, provenance, and deterministic opportunity-vs-execution conflict detection. Next step is real-session use, not more speculative infrastructure. Track concrete misses where compact `contextFrameAnalog` is not enough (for example, needing full analog session lists inline or event replay after a matched context); only then revisit Phase 4. Markdown capsules remain cancelled/deferred unless structured memory proves hard for agents to consume in practice.

5. **Regime detection as a first-class concept**  
   "Double Distribution dominated 52 of 81 sessions" is a regime observation. Make regime (trending / balanced / double-dist / non-trend volatile) a **computed pipeline field** on every session, queryable historically, and used by the rules engine to gate which setups are even eligible. Most playbook failures are regime mismatches, not condition failures.

---

## Priority 0 — Regime Overlay

<a id="idea-000"></a>
### IDEA-000: Regime-Gated Setup Selector

**Status:** REJECTED as a standalone setup (2026-06-23) — concept folded into IDEA-020.
**Source:** Local 2025-11-28 through 2026-03-06 database study; 0DTE / dealer gamma literature; CME liquidity research
**Complements:** All existing setup templates
**Detail:** [setup-ideas/IDEA-000-regime-gated-selector.md](setup-ideas/IDEA-000-regime-gated-selector.md)

**Framing:** Stop treating every setup as always-on. Add a top-level regime selector that determines which setup families are valid. Rejected as a standalone entry; retained as a context gate reconstructed in IDEA-020.

## Priority 1 — Implementable with Existing Pipelines

<a id="idea-001"></a>
### IDEA-001: Opening Drive Classification

**Status:** Researched
**Source:** Dalton AMT framework, IB/ORB statistics
**Complements:** OR5 Mid Retest (tpl_or5_mid_retest), IB Extension Play (tpl_ib_extension)
**Detail:** [setup-ideas/IDEA-001-opening-drive-classification.md](setup-ideas/IDEA-001-opening-drive-classification.md)

**Framing:** Classify the opening type within the first 15-30 minutes of RTH to predict the day's character *before* IB completes. Use the classification to filter which setups are active for the rest of the session.

<a id="idea-011"></a>
### IDEA-011: One-Sided IB Extension Acceptance

**Status:** Backtesting-ready
**Source:** Local 2025-11-28 through 2026-03-06 database study
**Complements:** IB Extension Play (tpl_ib_extension), OR5 Mid Retest (tpl_or5_mid_retest)
**Detail:** [setup-ideas/IDEA-011-one-sided-ib-extension-acceptance.md](setup-ideas/IDEA-011-one-sided-ib-extension-acceptance.md)

**Framing:** The useful signal is not "IB extension happened." It is whether extension stayed one-sided or became two-sided. Two-sided extension usually means migration / auction, not trend acceptance.

<a id="idea-002"></a>
### IDEA-002: Trapped Trader Reversal

**Status:** Researched
**Source:** Footprint analysis, microstructure theory
**Complements:** Rebid/Reoffer (tpl_rebid_support, tpl_reoffer_resistance), Absorption pipeline
**Detail:** [setup-ideas/IDEA-002-trapped-trader-reversal.md](setup-ideas/IDEA-002-trapped-trader-reversal.md)

**Framing:** When traders chase a breakout that fails, their forced liquidation accelerates the reversal. The existing absorption pipeline already detects passive orders absorbing aggressive flow — this wraps it into a failed breakout framework with explicit entry/stop/target logic.

<a id="idea-012"></a>
### IDEA-012: Absorption Failure / Liquidity Vacuum

**Status:** REJECTED as a standalone setup (2026-06-23) — concept folded into IDEA-020.
**Source:** Local 2025-11-28 through 2026-03-06 database study; CME liquidity research
**Complements:** IDEA-002 Trapped Trader Reversal, Rebid/Reoffer, Absorption pipeline
**Detail:** [setup-ideas/IDEA-012-absorption-failure.md](setup-ideas/IDEA-012-absorption-failure.md)

**Framing:** The better signal may be the *failure* of a defended level, not the original absorption itself. A failed defense plus liquidity pull creates a vacuum move that can travel faster than the original defense setup.

<a id="idea-003"></a>
### IDEA-003: Naked VPOC Magnet Trade

**Status:** Researched
**Source:** Auction Market Theory, volume profile analysis
**Complements:** Single Print Continuation (tpl_single_print_continuation), Session Inventory (tpl_session_inventory_clear)
**Detail:** [setup-ideas/IDEA-003-naked-vpoc-magnet.md](setup-ideas/IDEA-003-naked-vpoc-magnet.md)

**Framing:** Track POCs from prior sessions that price has not revisited ("naked" VPOCs). These act as price magnets — the market tends to gravitate toward unreconciled fair value.

<a id="idea-004"></a>
### IDEA-004: Multi-Timeframe CVD Divergence

**Status:** Researched
**Source:** Order flow analysis, extends delta pinch concept
**Complements:** Delta Pinch Reversal (tpl_delta_pinch_reversal), DNVA Retest (tpl_dnva_retest)
**Detail:** [setup-ideas/IDEA-004-mtf-cvd-divergence.md](setup-ideas/IDEA-004-mtf-cvd-divergence.md)

**Framing:** While delta pinch catches *sudden* inventory shifts, CVD divergence catches *gradual* exhaustion — price making new extremes while cumulative delta weakens. Adding multi-timeframe and level-specific delta divergence creates higher-conviction signals.

<a id="idea-005"></a>
### IDEA-005: Session Transition Sweep Patterns

**Status:** Researched
**Source:** Multi-session analysis, institutional flow patterns
**Complements:** Session Inventory Clear (tpl_session_inventory_clear)
**Detail:** [setup-ideas/IDEA-005-session-transition-sweep.md](setup-ideas/IDEA-005-session-transition-sweep.md)

**Framing:** Session transitions (Asia→London, London→RTH) create predictable liquidity sweep patterns. London almost always sweeps one side of the Asian range. The direction of RTH relative to the London sweep is a strong directional signal.

<a id="idea-020"></a>
### IDEA-020: Footprint Rebid/Reoffer Zone Lifecycle

**Status:** Stage 1 landed (2026-06-23); Stage 2 deferred. **Now the primary track** — the framework into which the rejected IDEA-000 (regime) and IDEA-012 (absorption-failure) concepts were folded.
**Source:** Trader doctrine session 2026-06-23 (see memory `rebid-reoffer-zone-doctrine`)
**Complements / absorbs:** Rebid/Reoffer templates, Absorption pipeline; **supersedes IDEA-000** (regime becomes a Stage-2 read derived from zone outcomes, not a standalone entry) and **IDEA-012** (a failed defense / vacuum is a `Failed` zone in this lifecycle, anchored to a real level — see those entries' 2026-06-23 verdicts).
**Detail:** [setup-ideas/IDEA-020-footprint-rebid-reoffer-lifecycle.md](setup-ideas/IDEA-020-footprint-rebid-reoffer-lifecycle.md)

**Framing:** Redefine acceleration zones around footprint stacked one-sided delta, and treat zone lifecycle outcomes (Forming / Retested / Held / Failed / Abandoned) as the signals. Stage 1 landed; Stage 2 (zone-derived regime) is deferred.

<a id="idea-022"></a>
### IDEA-022: Rally Offer Replenishment / Touch Offer Exhaustion

**Status:** Idea (2026-06-29)
**Source:** Live London Globex DOM observation session 2026-06-29; trader doctrine — *price only rises when buyers lift willing sellers at the offer; rallies often end when offers stop replenishing after being consumed ("no one left to sell to the buyers")*
**Complements:** IDEA-020 (DOM corroboration on zone lifecycle), IDEA-012 (liquidity vacuum after failed defense — different trigger, similar air-pocket mechanics), absorption/exhaustion pipelines
**Detail:** [setup-ideas/IDEA-022-rally-offer-replenishment.md](setup-ideas/IDEA-022-rally-offer-replenishment.md)

**Framing:** During an initiative rally, sellers on the ask are fuel. Rallies often stall when displayed offers stop replenishing after being consumed.

<a id="idea-021"></a>
### IDEA-021: Multi-Instrument Flow Architecture (NQ / MNQ / ES / MES)

**Status:** Spec drafted (2026-06-23); Stage A buildable
**Source:** Trader architecture session 2026-06-23 (memory `multi-instrument-flow-architecture`)
**Complements:** IDEA-018 (multi-instrument tracking), IDEA-009 (NQ/ES SMT), IDEA-020 (zones as flow)
**Full spec:** [`docs/multi-instrument-flow-architecture.md`](multi-instrument-flow-architecture.md)
**Detail:** [setup-ideas/IDEA-021-multi-instrument-flow-architecture.md](setup-ideas/IDEA-021-multi-instrument-flow-architecture.md)

**Framing:** Run all four CME equity-index contracts; share price structure once per underlying (from the mini) and run order flow per contract. Mini↔micro flow-agreement is a conviction/sizing signal, not a trigger.

## Priority 2 — Infrastructure Upgrades

<a id="idea-006"></a>
### IDEA-006: Volume Imbalance Bars (Lopez de Prado)

**Status:** Researched
**Source:** Lopez de Prado, "Advances in Financial Machine Learning" Ch. 2-3
**Complements:** All existing setups (infrastructure improvement)
**Detail:** [setup-ideas/IDEA-006-volume-imbalance-bars.md](setup-ideas/IDEA-006-volume-imbalance-bars.md)

**Framing:** Replace or supplement time-based bars with volume/tick/dollar bars that normalize information arrival. The "3–8 bars earlier" figure is Lopez de Prado (2018), not a Desk sample.

<a id="idea-019"></a>
### IDEA-019: Adaptive Session-Pace Volume Bars (Sierra Chart ACSIL Study)

**Status:** Idea
**Source:** Sierra Chart ACSIL custom chart bar docs; Relative Volume / cumulative volume ratio docs; April 2026 research pass
**Complements:** IDEA-006; discretionary execution chart design; session-awareness work
**Detail:** [setup-ideas/IDEA-019-adaptive-session-pace-volume-bars.md](setup-ideas/IDEA-019-adaptive-session-pace-volume-bars.md)

**Framing:** Build a Sierra Chart ACSIL custom chart bar study that adapts `contracts_per_bar` through the session instead of using a fixed N-volume threshold. The bar size should be smaller during quiet periods (for example Asia / slow Globex), then scale up automatically as expected participation rises into London, premarket, and RTH.

<a id="idea-007"></a>
### IDEA-007: Microstructure Regime Detection

**Status:** Researched
**Source:** HMM literature, Park & Kownatzki 2024, Lopez de Prado 2018
**Complements:** All setups (meta-filter)
**Detail:** [setup-ideas/IDEA-007-microstructure-regime-detection.md](setup-ideas/IDEA-007-microstructure-regime-detection.md)

**Framing:** Classify the current microstructure regime in real-time and use it as a meta-filter for all playbook setups. Run momentum setups in trending regimes, mean-reversion setups in rotational regimes, reduce size in transition regimes.

<a id="idea-016"></a>
### IDEA-016: VWAP Pipeline Enhancements (Dual Session + Anchored)

**Status:** Idea
**Source:** QA review of `vwap.rs` pipeline, March 2026
**Complements:** VWAP Band Zone Entry (tpl_vwap_band_zone), all VWAP-referencing setups
**Detail:** [setup-ideas/IDEA-016-vwap-pipeline-enhancements.md](setup-ideas/IDEA-016-vwap-pipeline-enhancements.md)

**Framing:** The current VWAP pipeline is mathematically correct and incremental, but it only supports a single session-anchored VWAP at a time. Dual-session and event-anchored VWAPs would increase its value as a trading reference.

<a id="idea-017"></a>
### IDEA-017: MCP Product Hardening — Playbook & Guidance as First-Class Data

**Status:** Idea
**Source:** Product review — MCP exposes market intelligence well; playbook and trading philosophy remain primarily in repository markdown
**Complements:** All Cursor agents; orchestrator and specialist prompts that should cite canonical definitions
**Detail:** [setup-ideas/IDEA-017-mcp-product-hardening.md](setup-ideas/IDEA-017-mcp-product-hardening.md)

**Framing:** MCP already exposes market, risk, and setup-evaluation state. Playbook rules, templates, and trader guidance still live in markdown; agents should be able to query those as first-class tool data.

<a id="idea-018"></a>
### IDEA-018: Multi-Instrument Concurrent Tracking (NQ, MNQ, ES, MES)

**Status:** Idea
**Source:** Roadmap — full product vision once the MCP surface and single-symbol path are “done enough”
**Complements:** Correlation and SMT-style ideas (e.g. IDEA-009); session and regime context across equity index futures
**Detail:** [setup-ideas/IDEA-018-multi-instrument-concurrent-tracking.md](setup-ideas/IDEA-018-multi-instrument-concurrent-tracking.md)

**Framing:** Run **four liquid CME equity index micro/mini roots** in parallel: **NQ**, **MNQ**, **ES**, and **MES** — each with its own pipeline state, session scoping, and tool addressing — so agents can reason about alignment, divergence, and relative strength without manually switching symbols or restarting the server.

<a id="idea-023"></a>
### IDEA-023: Social Intelligence & Continual Learning (X / Trusted Accounts)

**Status:** Idea (exploration documented; Phase A build blocked on ADR-020 trader decisions)
**Source:** Trader vision — trusted X accounts for live confluence, backtest hypothesis discovery, and subagent prompts from external edge situations
**Complements:** All setup IDEAs (hypothesis source), orchestrator + specialists, trader memory layer, research query engine
**Requires:** X Developer API access (pay-per-use; see cost model in spec), curated watchlist
**Detail:** [setup-ideas/IDEA-023-social-intelligence-continual-learning.md](setup-ideas/IDEA-023-social-intelligence-continual-learning.md)

**Framing:** A **platform feature track**, not a single setup. Trusted accounts contribute in different ways: real-time confluence, regime framing, level callouts, backtest hypotheses, and edge-case prompts. The Desk compares external reads to **deterministic structure + the trader's playbook**; third-party ideas enter a **trader-gated queue** before any backtest or template work.

<a id="idea-024"></a>
### IDEA-024: Market-Maker Pressure Inference

**Status:** Idea (spec documented; no code implemented)
**Source:** Trader request after reviewing Ruuj's Avellaneda-Stoikov article on X; existing DOM/tape tooling in The Desk
**Complements:** IDEA-007, IDEA-012, IDEA-020, IDEA-022, DOM MCP tools, orderflow-analyst
**Detail:** [setup-ideas/IDEA-024-market-maker-pressure-inference.md](setup-ideas/IDEA-024-market-maker-pressure-inference.md)

**Framing:** A future deterministic inference layer that helps agents say when observable book/tape behavior is **consistent with** passive defense, liquidity retreat, replenishment, exhaustion, adverse-selection pressure, or liquidity vacuum. It must not claim to know named market-maker inventory or hidden intent.

**First slice:** Level-based passive defense vs retreat around key levels using DOM pull/stack, same-window footprint, absorption/invalidation, and post-test acceptance.

---

<a id="idea-025"></a>
### IDEA-025: NQStats Statistical Setup Library

**Status:** Researched (source-capture only; not a Desk-verified edge)
**Source:** NQStats pages captured 2026-07-05
**Complements:** IDEA-005, IDEA-011, IDEA-014
**Detail:** [setup-ideas/IDEA-025-nqstats-stat-library-setups.md](setup-ideas/IDEA-025-nqstats-stat-library-setups.md)

**Framing:** External NQStats concepts are hypothesis generators, not imported win rates. Split source setups into child hypotheses and run Desk backtests before treating any statistic as verified.

---

<a id="idea-026"></a>
### IDEA-026: VolSignals VS3D Vendor Evaluation

**Status:** Researched (vendor triage; no trial or purchase)
**Source:** VolSignals VS3D evaluation captured 2026-07-08
**Complements:** IDEA-008, IDEA-013, IDEA-024
**Detail:** [setup-ideas/IDEA-026-volsignals-vs3d-vendor-eval.md](setup-ideas/IDEA-026-volsignals-vs3d-vendor-eval.md)

**Framing:** Point-in-time vendor evaluation for possible SPX/VIX dealer-positioning intake. Not a Desk edge and not a purchase decision.

---

<a id="idea-027"></a>
### IDEA-027: Options-Data Vendor Comparison

**Status:** Researched (interim-bridge survey; no trial or purchase)
**Source:** Options-data vendor survey captured 2026-07-08
**Complements:** IDEA-008, IDEA-013, IDEA-024, IDEA-026
**Detail:** [setup-ideas/IDEA-027-options-data-vendor-comparison.md](setup-ideas/IDEA-027-options-data-vendor-comparison.md)

**Framing:** Triage of API-accessible SPX dealer-flow vendors as a possible interim intake bridge, not a validated setup.

---

<a id="idea-028"></a>
### IDEA-028: SPX/VIX RTH Context Feed for Agents

**Status:** Researched (proposal only; no code implemented)
**Source:** Vault dispatch after Alec added SPX and VIX charts in Sierra; source pointer says @convexvalue-style SPX flowcharts are desired agent-visible context, not signals
**Complements:** IDEA-008, IDEA-013, IDEA-023, IDEA-024, IDEA-027, options MCP tools
**Detail:** [setup-ideas/IDEA-028-spx-vix-rth-context-feed.md](setup-ideas/IDEA-028-spx-vix-rth-context-feed.md)

**Framing:** A future derived RTH-only snapshot of SPX price context and VIX volatility context for agent narration. It should pair with SPX options/GEX maps and human source notes, but it must not fire alerts, alter risk, or bypass NQ structure/flow/playbook gates.

**First slice:** Explicitly configured SPX/VIX `.scid` context files -> read-only RTH scan -> compact MCP context card with freshness/staleness and "context only" caveats.

---

<a id="idea-029"></a>
### IDEA-029: Sierra Execution Chart Study Context and Exports

**Status:** Researched (docs-only; no code or chart changes)
**Source:** Vault dispatch promoting 2026-07-08 Sierra execution-chart notes plus 2026-07-09 spreadsheet-study export follow-up
**Complements:** IDEA-006, IDEA-019, IDEA-020, IDEA-022, IDEA-024, IDEA-028, order-flow MCP tools
**Detail:** [setup-ideas/IDEA-029-sierra-execution-chart-study-context.md](setup-ideas/IDEA-029-sierra-execution-chart-study-context.md)

**Framing:** Adaptive volume bars, Tape Reader / Delta Dynamics settings, leg-to-leg volume/delta profiles, and Sierra spreadsheet-study exports are candidate context/research inputs. They should stay advisory or offline until deterministic repo-native fields and dated backtests show incremental value over existing RVOL, tape pace, delta, footprint, imbalance, and absorption tools.

**First slice:** Preserve current fixed chart-size context (NQ 250 / ES 500) as human-supplied baseline; design offline replays and export/import checks before any MCP tool or Sierra chart setting changes.

---

<a id="idea-030"></a>
### IDEA-030: NQ Balance-Zone Taxonomy

**Status:** Idea (research queue; no code or signal claim)
**Source:** Trader-authored capture, 2026-07-10
**Complements:** IDEA-000, IDEA-007, IDEA-013, IDEA-029
**Detail:** [setup-ideas/IDEA-030-nq-balance-zone-taxonomy.md](setup-ideas/IDEA-030-nq-balance-zone-taxonomy.md)

**Framing:** Define and separately test TPO/volume balance, delta balance,
multi-session value overlap, and external options neutrality before considering
whether a composite "balance zone" adds information over existing day type,
profile shape, and balance-state fields.

---

<a id="idea-031"></a>
### IDEA-031: Session Range Compression and Expansion

**Status:** Idea (research queue; no code or signal claim)
**Source:** Trader-authored capture, 2026-07-10
**Complements:** IDEA-000, IDEA-005, IDEA-007, IDEA-014
**Detail:** [setup-ideas/IDEA-031-session-range-compression-expansion.md](setup-ideas/IDEA-031-session-range-compression-expansion.md)

**Framing:** Test session-specific compression/expansion transitions with
separate baselines for RTH, Asia, London, and consecutive like sessions. The
captured expectation that expansion tends to be followed by contraction is a
hypothesis to measure, not a current edge.

---

<a id="idea-032"></a>
### IDEA-032: Hidden Markov Models lecture notes — docs-only repo-fit

**Status:** Researched (docs-only; verdict ADAPT — no code or signal claim)
**Source:** Jeffrey W. Miller (2016) HMM lecture notes; trader capture 2026-07-13; assessed 2026-07-22
**Complements:** IDEA-000, IDEA-007, IDEA-030, IDEA-031
**Detail:** [setup-ideas/IDEA-032-hmm-lecture-notes-repo-fit.md](setup-ideas/IDEA-032-hmm-lecture-notes-repo-fit.md)

**Framing:** Treat the PDF as methodology education for a possible later
offline latent-regime experiment. Prefer existing deterministic `Regime` and
IDEA-007’s simpler RV-ratio path before any HMM implementation or agent
exposure.

---

<a id="idea-033"></a>
### IDEA-033: Expected-range ATR vs RV vs IV (session sizing + runners)

**Status:** Researched (docs-only study design; pre-backtest Adapt; no code or signal claim)
**Source:** second-brain queue ready-for-agent (2026-07-22); assessed 2026-07-22
**Complements:** IDEA-007, IDEA-027, IDEA-028, IDEA-031, IDEA-032
**Detail:** [setup-ideas/IDEA-033-expected-range-atr-rv-iv-research-plan.md](setup-ideas/IDEA-033-expected-range-atr-rv-iv-research-plan.md)

**Framing:** Offline expected-range plan with separate session-sizing and
runner-decision tracks. ATR/RV are derivable from session summaries / `.scid`;
IV is live-only via ConvexValue unless a provenance-complete history or
explicitly labeled proxy (e.g. VIX) is added. No backtest executed in this pass.

<a id="idea-034"></a>
### IDEA-034: Time-of-day liquidity-event calendar (participation anomalies + continuation/reversal)

**Status:** Backtesting (bucket-stats evidence accepted 2026-08-11; calendar extraction not run; no signal claim)
**Source:** second-brain queue ready-for-agent (2026-07-13) ← inbox note "large moves in the market based on time"; assessed 2026-07-24
**Complements:** IDEA-007, IDEA-031, IDEA-033
**Detail:** [setup-ideas/IDEA-034-time-of-day-liquidity-events.md](setup-ideas/IDEA-034-time-of-day-liquidity-events.md)

**Framing:** Staged offline study testing whether large participation (executed
volume / tape speed / signed delta, trade-side proxy — DOM parked) clusters at
predictable clock times across the full segmented day (Globex 18:00 ET, London
02:00 ET, RTH 09:30–16:15). The clean-`b63e83a` verification accepted the
bucket-stats artifact and its reportable coverage; this is not a Stage 1 pass.
Calendar/event-rate extraction remains a separate approval gate. Stage 2
(gated) tests continuation/reversal after events vs matched same-bucket
controls. Deliverable is a research calendar + gated verdicts only — no live
integration. See [the verification record](backtests/2026-08-11-idea-034-bucket-stats-verification.md).

<a id="idea-035"></a>
### IDEA-035: Leg-to-leg volume/delta profile engine (swing-anchored per-leg profiles)

**Status:** Researched (docs-only spec; pre-backtest Adapt; no code or signal claim)
**Source:** second-brain inbox "Stacked Leg-To-Leg Volume Profiles" (2026-07-16) + "delta rotation calculator in the-desk" (2026-07-09); interviewed and locked 2026-07-24
**Complements:** IDEA-029 (Track C), IDEA-020, IDEA-004, IDEA-033
**Detail:** [setup-ideas/IDEA-035-leg-to-leg-profile-engine.md](setup-ideas/IDEA-035-leg-to-leg-profile-engine.md)

**Framing:** Quantifies the trader's leg-to-leg read into repo-native
contracts: provisional anchors from 1-min delta-percentile spikes + opposite
displacement, confirmed anchors on k×ATR retracement, per-leg volume/delta
profiles (4-tick bins) with local-shape HVN/LVN/shelf definitions and a
rate+expansion building/tapering metric. Stage 1 is an offline boundary
stability study over recorded `.scid` (RTH, NQ); a gated Stage 2 prototypes
`get_leg_profile`. Sierra translation is config-assembly only (same math,
approximate visuals, no ACSIL).

<a id="idea-036"></a>
### IDEA-036: L2L pullback-join (stacked leg profiles)

**Status:** Researched (docs-only setup spec; gated behind IDEA-035 Stage 1; no signal claim)
**Source:** same 2026-07-16 inbox note + 2026-07-24 interview
**Complements:** IDEA-035 (hard dependency), IDEA-020, IDEA-012
**Detail:** [setup-ideas/IDEA-036-l2l-pullback-join.md](setup-ideas/IDEA-036-l2l-pullback-join.md)

**Framing:** First tradeable setup on the leg engine: counter-leg B pulls back
into leg A's LVN/shelf zone, event fires on the **second touch** with B
tapering + delta realigning to A's direction; invalidation is "two LVNs away"
(structural R), target is the far side of A's value area. Outcome measurement
is event-study MFE/MAE in points and R vs matched controls — no fill model.
Continuation-through and leg-failure variants are future tracks on the same
engine.

---

## Priority 3 — Requires External Data

<a id="idea-008"></a>
### IDEA-008: 0DTE Gamma Regime Trading

**Status:** Researched
**Source:** Dim/Eraker/Vilkov 2024, SpotGamma framework, CBOE research
**Complements:** Delta Pinch (regime context), VWAP Bands (positive gamma = mean reversion)
**Requires:** External GEX data feed (SpotGamma, Databento options chain, or manual levels)
**Detail:** [setup-ideas/IDEA-008-0dte-gamma-regime.md](setup-ideas/IDEA-008-0dte-gamma-regime.md)

**Framing:** 0DTE options create structural dealer hedging flows that shape NQ intraday behavior. Positive gamma = mean reversion (dealers sell rallies, buy dips). Negative gamma = momentum (dealers amplify moves).

<a id="idea-013"></a>
### IDEA-013: Gamma-Gated Setup Overlay

**Status:** Researched
**Source:** Local 2025-11-28 through 2026-03-06 database study; Cboe March 2026 volume data; Dim/Eraker/Vilkov; Adams/Fontaine/Ornthanalai
**Complements:** IDEA-000 Regime Selector, IDEA-008 0DTE Gamma Regime Trading
**Requires:** External gamma / wall / flip data
**Detail:** [setup-ideas/IDEA-013-gamma-gated-setup-overlay.md](setup-ideas/IDEA-013-gamma-gated-setup-overlay.md)

**Framing:** Gamma should not be treated as a standalone setup. It should be used as a selector for which of *your existing setups* are appropriate.

<a id="idea-009"></a>
### IDEA-009: NQ/ES SMT Divergence

**Status:** Researched
**Source:** ICT methodology, cross-asset analysis
**Complements:** All directional setups (confirmation layer)
**Requires:** ES .scid data feed from Sierra Chart
**Detail:** [setup-ideas/IDEA-009-nq-es-smt-divergence.md](setup-ideas/IDEA-009-nq-es-smt-divergence.md)

**Framing:** When ES and NQ diverge at structural levels, the lagging market provides a cleaner, higher-probability trade.

## Priority 4 — New Detection Logic Required

<a id="idea-010"></a>
### IDEA-010: Fair Value Gap with Order Flow Confirmation

**Status:** Researched
**Source:** ICT/SMC methodology combined with order flow
**Complements:** Rebid/Reoffer zones (similar concept — gaps as zones)
**Detail:** [setup-ideas/IDEA-010-fvg-orderflow-confirmation.md](setup-ideas/IDEA-010-fvg-orderflow-confirmation.md)

**Framing:** FVGs represent genuine institutional imbalances. Combining with order flow confirmation (footprint, delta, absorption) filters out low-quality gaps. 70-80% of FVGs eventually fill.

<a id="idea-014"></a>
### IDEA-014: London Inventory Unwind Into RTH

**Status:** Researched
**Source:** Local 2025-11-28 through 2026-03-06 database study
**Complements:** Session Inventory Clear (tpl_session_inventory_clear), DNVA Retest (tpl_dnva_retest), VWAP Band Zone Entry (tpl_vwap_band_zone)
**Detail:** [setup-ideas/IDEA-014-london-inventory-unwind.md](setup-ideas/IDEA-014-london-inventory-unwind.md)

**Framing:** In the current local sample, London direction was more likely to unwind than continue into RTH. This suggests a dedicated handoff setup: trade the unwind only when RTH opens back into value and inventory begins clearing.

<a id="idea-015"></a>
### IDEA-015: Post-Macro / Post-Earnings Jump Repair-or-Go

**Status:** Researched
**Source:** CME around-the-clock liquidity research; jump-risk literature; local style fit
**Complements:** IDEA-000 Regime Selector, Session Inventory Clear, OR5 Mid Retest, DNVA Retest
**Requires:** External event calendar for clean automation; otherwise usable as a discretionary overlay
**Detail:** [setup-ideas/IDEA-015-post-macro-jump-repair-or-go.md](setup-ideas/IDEA-015-post-macro-jump-repair-or-go.md)

**Framing:** NQ is unusually exposed to post-earnings and macro jump risk. The useful setup is not "trade the news." It is classify the jump day into: - **acceptance / continuation** - **repair / re-entry into value**

## Scratchpad — Chartbook MGI, Level Verification, and Microstructure Review

**Status:** Idea (working notes — not a spec for immediate implementation)

This section captures prior chartbook / strategy framing and a checklist of **Market Generated Information (MGI)** the trader wants anchored in the product and agents over time. It also flags definitions and tooling that need a deliberate pass so language in prompts, MCP summaries, and `session_summaries` stays aligned with how *you* trade.

### Weekly MGI (Dalton-style weekly context)

Anchor: **weekly open each Sunday evening** (Globex week start for NQ — exact timestamp rule TBD vs exchange session calendar).

**Weekly Initial Balance (WIB)** — first balance window from that open (duration to confirm vs your chartbook; often first RTH-equivalent slice or first N hours of the week — document when locked in):

- WIB High, Mid, Low
- **50% extensions** up and down from WIB range
- **100% extensions** up and down
- **150% extensions** up and down
- **200% extensions** up and down

**Weekly VWAP:** VWAP **anchored from the weekly open** (distinct from session RTH VWAP).

**Other weekly / prior-week references:**

- Prior week high and low
- Prior week **close** (noted as “CI” in your notes — confirm symbol: close / settlement / last print)
- Weekly open level (current week)
- Current week: value area high, low, POC (TPO- or volume-based — align with pipeline default)
- Prior week: VAH, VAL, POC
- Prior week’s open
- **Current weekly mid-price** (define: midpoint of week range so far, mid of WIB, or other — lock when implementing)

**Verification note:** Cross-check each of the above against `levels` / TPO / VWAP pipelines and MCP tool payloads; flag any field that is missing, uses a different anchor (e.g. calendar week vs RTH week), or duplicates under another name.

### Daily MGI (RTH + Globex decomposition)

Much of this already exists in pipelines or session summaries; this list is the **coverage checklist** for documentation and agent narration.

**Volume / profile (RTH-scoped where noted):**

- Relative volume (RVOL) — session context
- **RVAH, RVAL, RPOC** — prior **RTH** session value area references (naming aligned to your chartbook)

**Prior / overnight structure:**

- Prior day high, prior day low
- **GVAH, GVAL, GPOC** — Globex (overnight) value area references for the relevant session
- **OVNH, OVNL** — overnight high / low (always tracked)

**RTH open and opening structures:**

- RTH open
- RTH opening range: high, low, mid
- RTH IB: high, low, mid
- RTH IB **100% extensions** (both directions)
- RTH IB **200% extensions** (both directions)
- **RTH VWAP**
- **RTH TWAP**

**Asia / London / combined Globex:**

- For **Asia** and **London** (and **combined Globex overnight** where applicable):
  - Opening range: high, low, mid
  - Extensions of each session’s OR (same extension ladder as IB or OR-only — specify when implementing)
  - IB (or equivalent first-balance window per session): high, low, mid
  - IB extensions per session if your chartbook uses them separately from OR

**Verification note:** Confirm session boundaries in code match Sierra/CME definitions you use visually; mismatches here break agent trust.

### TPO — poor highs and poor lows (definition pass)

We already surface **poor high** / **poor low** in places, but the doc and agents should **not** assume a single industry definition.

**Action:** Schedule a revisit to **write down the exact rule** used in The Desk (e.g. unfinished auction at extremes, single-print poor structure, minimum TPO count, multi-day context) and align:

- Pipeline / `session_summaries` field semantics
- Agent phrasing (“poor high” vs “weak high” vs “excess”)

Cross-reference: *Instrumentation Caveats* above (sparse / incomplete poor flags in stored samples) — improving definitions may drive better instrumentation.

### Single prints

**Action:** Explicit review pass — how single prints are detected, stored, and narrated (including direction / context). Ensure setup ideas and `single_prints_direction` (or successor fields) are useful for research, not just display.

### RTH-only gaps

Track **gaps in price for RTH-only** continuity (open vs prior RTH close, prior RTH high/low, etc. — exact gap definition to match your chartbook).

**Use:** Regime context, gap-fill vs gap-and-go narratives, backtest hypotheses later.

### Absorption and initiation — event definitions and rules

Some of this likely exists in pipeline / agent text already; goal is **one canonical definition** for:

- **Absorption events** — what confirms absorption vs noise; invalidation; relationship to pace and delta
- **Initiation events** — initiative vs responsive framing; how initiation is distinguished from absorption failure or liquidity pull

**Action:** Draft explicit rules (even if discretionary) so the rules engine, events, and coaching agents use the **same vocabulary**.

### Iceberg-style behavior and stop runs

**Iceberg / hidden liquidity proxies:** Explore measurable signatures (repeated fills at same price, refresh of displayed size, footprint patterns) — may be partial / probabilistic only on tick data.

**Stop runs / stop-loss sweeps:** Define observable criteria (e.g. liquidity grab beyond level + immediate rejection, delta flip, pace spike) and separate from generic “spike” noise.

**Status:** Research / prototype — no claim yet that full iceberg detection is available; document intent for future tooling.

### Buy zones and sell zones

**Action:** Clarify **logic and inputs** for buy/sell zones (which levels, which flow confirmations, session scope). Review agent prompts so they don’t contradict pipeline math or each other.

### Average rotations, swing highs, swing lows

**Ideas to explore:**

- **Average rotation** — mean/median swing size in ticks or points over a lookback (session- or regime-scoped)
- **Swing high / swing low** — definition of pivot length, session vs multi-day, and how agents should cite them vs key levels / TPO structure

**Use:** Context for extension targets, mean reversion vs trend, and backtesting once definitions are stable.

---

## Backtesting Queue

Ordered by expected information value × implementation ease:

| # | Hypothesis | Setup | Data Needed | Priority |
|---|-----------|-------|-------------|----------|
| 1 | One-sided vs both-sided IB extension: first pullback expectancy | IDEA-011 | session_summaries, IB extension events | High |
| 2 | London trends, RTH opens back in value, DNP/VWAP reclaim → unwind probability | IDEA-014 | multi-session summaries, delta, VWAP | High |
| 3 | Absorption invalidation + pace expansion at key level → 15/30 min follow-through | IDEA-012 | absorption events, pace, key levels | High |
| 4 | Open Drive + RVOL ≥ Elevated → pullback to VWAP win rate | IDEA-001 | session_summaries, events | High |
| 5 | Regime selector improves OR5 / IB / DNVA / VWAP family expectancy | IDEA-000 | session_summaries, events, setup outcomes | High |
| 6 | Naked VPOC fill rate within 1/3/5/10 sessions | IDEA-003 | session_summaries POC + ticks | Medium |
| 7 | CVD divergence at VA boundary → reversal within 30 min | IDEA-004 | delta pipeline, events | Medium |
| 8 | London sweep of Asia range → RTH direction prediction | IDEA-005 | Globex session data | Medium |
| 9 | Volume bars vs time bars: R-distribution comparison for existing setups | IDEA-006 | .scid tick data | Medium |
| 10 | Positive-gamma gating vs negative-gamma gating on existing setup families | IDEA-013 | options / gamma data + setup outcomes | Medium |
| 11 | Stacked imbalances (≥3, ≥4:1) fail → reversal probability | IDEA-002 | footprint data | Medium |
| 12 | Narrow IB (<0.7x avg) → breakout continuation rate | IDEA-001 | session_summaries IB range | Low |
| 13 | Three-session alignment → range extension beyond IB | IDEA-005 | multi-session data | Low |
| 14 | Prior Globex VWAP as S/R in first 60 min of RTH on unwind days | IDEA-016 | session VWAP snapshots, ticks | Low |
| 15 | Anchored VWAP from IB break: band respect vs session VWAP bands | IDEA-016 | IB break events, ticks | Low |
| 16 | Zone establishment age vs clearance velocity → follow-through / regime | IDEA-020 | zone lifecycle events, pace | Medium |

---

## Verified Backtesting Runbook

Use this sequence for any setup study that depends on `signal_outcomes`:

1. **Preflight integrity:** call `validate_signal_outcome_integrity` with the intended `source`, `jobId` if available, and `setupId` if narrowed. `failed` means stop; `warning` means inspect legacy ratios before using the result.
2. **Use fresh job IDs:** never mix old and new outcome engines in the same statistic. Fresh deterministic backtests should produce a new `job_id` and should store their integrity report in `backtest_runs.metrics.signalOutcomeIntegrity`.
3. **Prefer verified rows:** while the transition is active, research tools default `includeUnverified=true` for backwards compatibility. For new studies, pass `includeUnverified=false`.
4. **Pin provenance in notes:** every published result should cite `source`, `job_id`, setup id, date/session scope, outcome engine version, rules schema version, and whether `qualityCounts.verified` covers the full sample.
5. **Flip defaults later:** after verified reruns exist for the immediate research windows, change the research-tool default from `includeUnverified=true` to verified-only and keep legacy inclusion as an explicit opt-in.

Immediate next target: rerun IDEA-011 under this runbook and promote the verified result into the research snapshot above.

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

*Last updated: 2026-08-21*
