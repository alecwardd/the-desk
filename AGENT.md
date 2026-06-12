# The Desk — Agent Instructions

Universal instructions for any LLM coding agent (Claude Code, Cursor, Codex) working in this repository.

---

## Project Context

The Desk is a backend intelligence platform for discretionary NQ futures traders. It reads Sierra Chart `.scid` tick data, computes market structure and microstructure analytics in Rust, stores everything in SQLite, and exposes the intelligence layer via MCP (Model Context Protocol).

Read these documents in order:

1. **CLAUDE.md** — Project rules, architecture, conventions (READ FIRST)
2. **README.md** — Architecture overview, project structure, data flow
3. **docs/trader-memory/identity.md** — durable trader identity, doctrine, and guardrails for agent partnership
4. **Relevant skill** from `skills/` — Domain knowledge for your task

---

## Architecture Summary

```
Sierra Chart (.scid) → Rust Pipeline Engine → SQLite → MCP Server → Cursor Agents
       ↑                      ↑                  ↑           ↑            ↑
   Data source          Layer 1+2 (fast)     Persistence   Layer 3      Interface
   External file       Deterministic math     Local DB     Exposure     LLM-powered
```

**Key principles:**
- Layers 1 (pipelines) and 2 (rules engine) are pure Rust, no network calls, sub-millisecond
- Layer 3 (MCP + LLM coaching) can tolerate 1-5s latency
- **Every layer must be independently testable.** Never skip layers.

---

## Agent Scope (Default Focus)

**Primary:** You work in this repo to support the agentic trading partner. Your outputs appear in Cursor, Claude Code, Codex, or similar platforms. Focus on:
- Rust backend (pipelines, rules, feed, db, research)
- MCP server and tools
- Agent definitions and prompts
- SQLite, backfill, research queries

---

## Subagent Patterns

When you need specialized help, spawn subagents for these tasks.

> **Path note:** Agent definitions live in `agents/` at the project root. Cursor also discovers them at `.cursor/agents/` (symlinked). Both paths resolve to the same files.
>
> **Tool capability:** See **MCP Tools Reference** below for live vs historical tool mapping and agent-to-capability matrix.

### Orchestrator (Primary Entry Point)
**When:** The trader interacts with The Desk for any market question, setup evaluation, trade recording, or session management. This is the default agent.
**How:** Use `orchestrator` (defined in `agents/orchestrator.md`). The orchestrator routes to all specialist agents and ensures risk-coach context is present on every interaction. It calls the same MCP tools the specialists use, with its own synthesis logic and a mandatory risk footer on every response.
**Definition:** `agents/orchestrator.md`

### Sierra data feed (.scid / `.depth`)
**When:** Working on live ingestion, SCID tailing, symbol resolution, or `MarketDepthData` parsing
**How:** Read `skills/trading-domain/SKILL.md` for session semantics; inspect `src/feed/scid_reader.rs` and `src/depth/` for formats. Live paths are **Sierra `.scid` + optional `.depth` files only** (no socket DTC client in-tree).

### Pipeline Verification
**When:** After implementing or modifying a market structure pipeline
**How:** Delegate to `pipeline-verifier` (defined in `agents/pipeline-verifier.md`)

### Prompt Quality Evaluation
**When:** After writing or modifying LLM coaching prompts
**How:** Delegate to `prompt-quality-evaluator` (defined in `agents/prompt-quality-evaluator.md`)

### Options API Research
**When:** Working on Phase 2 options data integration
**How:** Delegate to `options-api-researcher` (defined in `agents/options-api-researcher.md`)

### Market Structure / Orderflow Analysis
**When:** Investigating market behavior or validating pipeline outputs against theory
**How:** Use `market-structure-analyst` or `orderflow-analyst`

### Levels Analysis
**When:** Investigating key level behavior, IB extensions, proximity dynamics
**How:** Delegate to `levels-analyst` (defined in `agents/levels-analyst.md`)

### Performance Analysis
**When:** Evaluating trading performance, setup efficacy, signal outcomes
**How:** Delegate to `performance-analyst` (defined in `agents/performance-analyst.md`)

### Backtesting / Historical Research
**When:** Running historical queries, backfilling data, analyzing event frequencies
**How:** Delegate to `backtest-analyst` (defined in `agents/backtest-analyst.md`)

### Data Integrity Validation
**When:** After ingestion changes or before analysis
**How:** Delegate to `data-integrity-validator` (defined in `agents/data-integrity-validator.md`)

### Risk Coach
**When:** Always — included on every interaction via orchestrator. Also invoked directly for session start, trade recording, position sizing, circuit breakers, and any decision involving risk.
**How:** Via orchestrator (automatic) or directly as `risk-coach` (defined in `agents/risk-coach.md`).
**Capabilities:** Session-start balance/position confirmation, dynamic R derivation (compounding), 1/4 Kelly sizing with confidence scaling, consecutive-loss circuit breaker (3 losses = hard stop), drawdown-based size scaling (2R = half size, 3R = stopped), heat tracking (aggregate open exposure), day-type and time-of-day risk awareness, trade result recording via `record_trade_result` MCP tool.

---

## Implementation Workflow

When implementing a feature:

1. **Read the relevant skill** from `skills/` for domain knowledge
2. **Write the Rust code** in the appropriate module (`pipelines/`, `rules/`, `feed/`, `db/`)
3. **Write tests** alongside the code — every pipeline must have unit tests
4. **Integrate with `PipelineEngine`** if adding a new pipeline (update `mod.rs`, `MarketState`, `snapshot()`)
5. **Add `ConditionField` variants** if the rules engine needs to evaluate the new data
6. **Add MCP tool** in `src/bin/the-desk-mcp.rs` if agents need access
7. **Run `cargo test`** before declaring done

---

## Decision Framework

| Question | Guidance |
|----------|----------|
| Should this be in Rust or somewhere else? | Market data, rules, persistence, and MCP → Rust. |
| Should I add an MCP tool for this? | If an agent would benefit from querying this data → yes. Keep tools focused. |
| Should I use the LLM for this? | If it can be computed deterministically → no LLM. If it requires synthesis → LLM. |
| Should I add a new dependency? | Prefer existing deps. Check `Cargo.toml` first. |
| Should I create a new file? | Prefer editing existing files. Only create new files for genuinely new modules. |

---

## Common Mistakes to Avoid

1. **Using `f32` for prices.** Always `f64` — precision matters for financial data.
2. **Forgetting incremental updates.** Pipelines MUST update incrementally, not recalculate.
3. **Blocking the main thread.** All I/O and computation in background tokio tasks.
4. **Mixing RTH and Globex data.** Always scope calculations to the correct session.
5. **Using advisory language in prompts.** "Your rules say..." not "You should..."
6. **Skipping the rules engine.** Layer 2 MUST evaluate before any LLM is called.
7. **Calling the Claude API from Rust.** LLM orchestration is a downstream consumer, not part of pipelines.
8. **Putting market data math in TypeScript.** All pipeline calculations belong in Rust.

---

## Lucid Direct Context

Use this as the canonical source for shared Lucid Direct account facts referenced by agent definitions.

### Account framing

- **Account stage:** Lucid Direct
- **Typical account size:** $50,000
- **Working daily loss limit:** $1,200 unless the trader updates it
- **Drawdown model:** End-of-day; LucidScale references 60% of peak end-of-day balance
- **Payout gates:** 20% consistency and at least 5 profitable trading days

### Risk framing

- Protect the end-of-day balance. Late-session giveback matters more than headline intraday P&L because LucidScale references peak EOD balance.
- Preserve payout eligibility. Avoid oversized outlier days that break consistency.
- Do not use evaluation pass-target framing for Direct accounts.
- If payout-cycle metrics are not available from tools, ask the trader to confirm them. Do not invent payout progress or eligibility.

### Dynamic R calculation

R is derived from current Lucid parameters and must never be hard-coded:

```text
R_dollars = lucid_daily_loss_dollars / max_daily_loss_r
R_points  = R_dollars / 5.00
```

For NQ/MNQ, use $5.00 per point per MNQ contract when converting dollars to points.

At $50,000 balance with a $1,200 daily loss limit and 3R max daily loss:
- `R_dollars = 1200 / 3 = 400`
- `R_points = 400 / 5 = 80`

As Lucid parameters change, agents must recalculate and report the new R.

---

## Research Sample Size Policy

Use this as the canonical policy whenever any agent cites historical, backtest, setup-performance, or conditional statistics.

| Sample size | Reliability label | Allowed framing |
|-------------|-------------------|-----------------|
| `N < 20` | Insufficient | Only mention with explicit "insufficient sample" language. Treat as directional context at most, not a reliable conclusion. |
| `20 <= N < 30` | Directional | May report with caveats, but do not use high-confidence wording. |
| `N >= 30` | Reportable | Safe to report as a meaningful statistic, while still including `N` and any relevant caveats. |

Rules:
- Always include `N` with any statistic.
- Confidence intervals, standard deviation, percentiles, or similar uncertainty measures are additive. They do not replace the reliability tier.
- If the sample is mixed across incompatible scopes (for example RTH and Globex combined without explicit labeling), downgrade confidence and state the scope limitation.
- If the question asks for strong edge claims, comparisons, or sizing implications, prefer `N >= 30` even when smaller samples can still be discussed directionally.

---

## MCP Tools Reference

The MCP server (`src/bin/the-desk-mcp.rs`) exposes 120 MCP tools across 13 categories.

### Live vs Historical — Quick Reference

**Live tools** read from the in-memory pipeline (current session only). They answer "what's happening now?" and require an active feed or startup backfill. Use for: market reads, setup checks, levels, flow, risk, DOM.

**Historical tools** read from SQLite (session_summaries, market_events, signal_outcomes, raw_ticks). They answer "what happened in the past?" and require `backfill_history` to have been run. Use for: event frequency, conditional probability, session comparison, setup performance, backtests.

| Context | Primary tools |
|---------|---------------|
| **Live (current session)** | `get_market_snapshot`, `get_context_frame`, `get_session_context`, `get_tpo_profile`, `get_delta_profile`, `get_key_levels`, `get_tape_pace`, `get_footprint`, `get_or5_status`, `get_rvol`, `get_day_type`, `get_rebid_reoffer_zones`, `get_pinch_events`, `get_session_inventory`, `evaluate_playbook`, `get_setup_context`, `check_delta_confirmation`, `get_proximity_report`, `get_imbalances`, `get_absorption_events`, `get_trade_size_profile`, DOM tools |
| **Historical (backfill data)** | `get_context_frame(timestampMs)`, `get_snapshot_at`, `get_footprint_window`, `query_ticks`, `get_session_history`, `get_research_summary`, `query_event_frequency`, `query_conditional`, `query_distribution`, `compare_sessions`, `get_setup_performance_matrix`, `query_signal_outcome_*`, `get_signal_performance`, `backfill_history`, `run_backtest`, `get_backfill_status`, `get_backtest_results`, `compare_backtests`, hypothesis promotion tools |

**Data dependency:** Historical tools return empty or minimal data until `backfill_history` has populated the database. Call `get_research_summary` first to check session count; if low, run backfill before deep analysis.

### Full Tool List by Category

| Category | Tools | Description |
|----------|-------|-------------|
| **Snapshot** | `get_market_snapshot` | Current price, VWAP, session state |
| | `get_session_context` | Session type (RTH/Globex), segment (Asia/London), trading day, data freshness |
| | `get_session_summary` | Total tick count, latest tick timestamp, latest pipeline snapshot (health check) |
| | `get_feed_health` | SCID path status, file metadata, ingest lag, freshness diagnostics |
| | `get_runtime_events` | Recent structured MCP runtime diagnostics for post-mortems |
| | `get_contract_rollover_status` | Pre-session contract roll validation: active contract, prior reference contract, and whether carry-forward levels are authoritative |
| | `validate_contract_rollover` | Validation alias for `get_contract_rollover_status` for pre-session safety gates |
| | `get_snapshot_at` | Historical pipeline snapshot nearest to a given timestamp |
| | `get_context_frame` | Session-relative framing with stable buckets, historical analogs, reliability caveats, and optional setup outcomes |
| **Structure** | `get_tpo_profile` | POC, value area, opening range, initial balance |
| | `get_tpo_detail` | Per-price TPO letter detail (which brackets printed where, single prints) |
| | `get_delta_profile` | Session delta, DNVA, DNP |
| | `get_delta_at_price` | Delta at a specific price level + top N prices by absolute delta |
| | `get_key_levels` | Prior day H/L/C, prior VA/POC, overnight H/L, Globex OR30, London OR60, IB |
| **Microstructure** | `get_tape_pace` | Rolling ticks/sec, volume/sec, acceleration, pace percentile, dwell time |
| | `get_footprint` | Volume-at-price for current session (bid/ask/delta per level) |
| | `get_footprint_window` | Time-windowed footprint for a specific time range |
| | `get_imbalances` | Stacked and diagonal imbalance detection from footprint |
| | `get_absorption_events` | Absorption/exhaustion events with severity scores |
| | `get_trade_size_profile` | Trade size distribution (1-lot, 2-5, 6-20, 21+), institutional clustering |
| **PTT Indicators** | `get_or5_status` | 5-min Opening Range: levels, break direction, mid retest, extension targets |
| | `get_rvol` | Relative volume vs N-day average at same time-of-day |
| | `get_day_type` | Day type (Normal/Trend/etc.), profile shape, balance state, single prints |
| | `get_rebid_reoffer_zones` | Active acceleration zones with status (Fresh/Retested/Held/Failed) |
| | `get_pinch_events` | Delta momentum reversals across 1m/5m/15m/30m timeframes |
| | `get_session_inventory` | Cross-session delta inventory (Building/Clearing/Neutral), trend count |
| **Rules** | `evaluate_playbook` | All active setups vs current market state (met/approaching/notActive) |
| | `get_setup_context` | Full context for a named setup (OR5, delta, RVOL, day type, zones, risk) |
| | `check_delta_confirmation` | Session + price-level delta confirmation for a trade direction |
| | `get_setup_state_history` | Durable setup state/progress transitions for restart and lifecycle review |
| | `acknowledge_setup_prompt` | Mark discretionary confirmation as acknowledged for a setup lifecycle |
| | `mark_setup_in_trade` | Mark a setup lifecycle as in-trade |
| | `close_setup_state` | Close a setup lifecycle state |
| **Attention** | `get_attention_inbox` | Ranked proactive inbox. First call for "what deserves attention now?" |
| | `get_signal_detail` | Full evidence, priority breakdown, linked setup/risk context, and suggested next tools for one signal |
| | `acknowledge_attention_signal` | Mark an attention signal reviewed by trader or agent |
| | `what_changed_since` | Cursor-based catch-up feed for structure/setup/risk changes since a prior cursor |
| | `get_attention_changelog` | Replay signal lifecycle deltas: created, priority changed, acknowledged, expired, invalidated, notified |
| | `get_active_trade_ideas` | Current idea-card overlays derived from playbook setup lifecycle and attention signals |
| | `mark_trade_idea_confirmed` | Mark a trade idea confirmed with evidence |
| | `mark_trade_idea_invalidated` | Mark a trade idea invalidated with a reason |
| | `mark_trade_idea_in_trade` | Mark a trade idea as in-trade, optionally linked to a signal outcome |
| | `mark_trade_idea_resolved` | Mark a trade idea resolved with an outcome note |
| **Risk** | `get_risk_state` | Daily P&L in R, trade count, streaks, drawdown, at-limit status |
| | `get_risk_config` | R-value, max daily loss, circuit breaker, trade limits |
| | `save_risk_config` | Persist risk configuration (partial updates supported) |
| | `init_risk_state` | Initialize/reset risk state for new session |
| | `get_account_state` | Last balance, open positions, Lucid params, profit goals |
| | `save_account_state` | Persist account state (partial updates supported) |
| | `get_kelly_position_size` | 1/4 Kelly sizing with confidence scaling |
| | `get_signal_performance` | Win rate, avg R, resolved/pending, target/stop/time-exit counts |
| | `record_trade_result` | Record closed trade, update risk state |
| **Data** | `query_ticks` | Raw tick data queries |
| | `get_proximity_report` | Which key levels is price near (sorted by distance) |
| **Integrity** | `validate_data_integrity` | Tick count, freshness, pipeline consistency invariants |
| **Research** | `query_event_frequency` | How often does event X occur across sessions? |
| | `query_conditional` | When X happens N+ times, how often does Y occur? |
| | `query_distribution` | Distribution stats for a numeric metric (mean, median, percentiles) |
| | `get_setup_performance_matrix` | Per-setup performance table (win rate, avg R, counts) |
| | `query_signal_outcome_distribution` | R-result distribution for a setup's signal outcomes |
| | `query_signal_outcome_conditional` | Conditional win rate for signals filtered by session attributes |
| | `query_signal_outcome_excursions` | MFE/MAE/time-to-outcome diagnostics for signal outcomes |
| | `compare_sessions` | Multi-dimensional similarity matching against historical sessions |
| | `get_session_history` | Query past session summaries with optional filters |
| | `get_research_summary` | Pre-session statistical briefing (session count, IB dist, day types) |
| **Hypotheses** | `register_hypothesis` | Validate or register an inactive per-version hypothesis setup from a typed `SetupDefinition` plus metadata |
| | `list_hypotheses` | Query registered hypotheses by lifecycle so agents do not repeat failed/rejected ideas |
| | `summarize_hypothesis_run` | Summarize one completed hypothesis backtest by explicit `setupId` and `jobId` |
| | `propose_draft_setup` | Apply the strict promotion gate and transition passing hypotheses to inactive drafts |
| | `activate_draft_setup` | Activate a draft setup after human confirmation and engine-version freshness check |
| | `set_hypothesis_lifecycle` | Mark a hypothesis/draft as `rejectedByHuman` or `retired` with a reason |
| **Backfill** | `backfill_history` | Queue historical backfill job (all 14 pipelines + event detection) |
| | `run_backtest` | Queue backtest replay job (rules engine over historical data) |
| | `get_backfill_status` | Poll progress for backfill/backtest jobs |
| | `cancel_backfill` | Cancel in-flight backfill/backtest job |
| | `get_backtest_results` | Retrieve stored backtest runs with metrics |
| | `compare_backtests` | Compare two or more backtest runs side-by-side |
| **Memory** | `get_memory_brief` | Ranked carry-forward memory by intent (session_start, setup_check, trade_review, weekly_review). Read-only — call `refresh_memory_state` first when `memoryMaintenance.refreshSuggested` is true or after memory-affecting writes (trades, reviews, imports) in the same flow. |
| | `get_pre_session_briefing` | Memory brief + account + risk state for session start. When maintenance is dirty, performs one bounded `refresh_memory_state` (patterns + insight lifecycle) before building the brief unless `skipMemoryRefreshIfDirty` is true. Response includes `memoryAutoRefreshed`. |
| | `get_trader_context_fit` | Typed trader memory envelope by intent. Separates executed-trade memory, setup opportunity, coaching reminders, live post-loss/ordinal state, reliability, and provenance. Memory reports context only and must not adjust sizing by itself. |
| | `refresh_memory_state` | Explicit refresh of behavioral patterns and/or insight lifecycle; clears dirty flags. Use before `get_memory_brief` when ranked memory must reflect recent journal or trade changes. |
| | `save_agent_insight` | Persist LLM-authored insight (candidate/validated lifecycle) |
| | `recall_agent_insights` | Query insights by category, setup, status |
| | `acknowledge_agent_insight` | Mark insight surfaced/helpful/irrelevant/wrong/pin |
| | `create_memory_followup` | Open follow-up for next session |
| | `resolve_memory_followup` | Close follow-up with optional note |
| | `detect_behavioral_patterns` | Run deterministic pattern detection |
| | `get_behavioral_patterns` | Query detected patterns |
| **Storage** | `archive_status` | Hot/warm/cold tier sizes, session count, last archive date |

### Agent-to-Capability Mapping

| Agent | Primary context | Key tools |
|-------|------------------|-----------|
| **orchestrator** | Both — routes by intent | All; first call `get_attention_inbox` / `what_changed_since` when the trader asks what changed or what deserves attention. Routes `historical_research` to backtest-analyst. Memory: use `get_pre_session_briefing`, `get_trader_context_fit`, `refresh_memory_state`, `get_memory_brief`, `save_agent_insight`, `recall_agent_insights`, `acknowledge_agent_insight`, `create_memory_followup`, `resolve_memory_followup`, `detect_behavioral_patterns`, `get_behavioral_patterns` |
| **market-structure-analyst** | Live + historical | Live: `get_tpo_profile`, `get_key_levels`, `get_day_type`, `get_rvol`, `get_delta_profile`. Historical: `query_event_frequency`, `query_conditional`, `query_distribution`, `compare_sessions`, `get_session_history`, `get_research_summary` |
| **orderflow-analyst** | Live + historical | Live: `get_delta_profile`, `get_tape_pace`, `get_footprint`, `get_imbalances`, `get_absorption_events`, DOM tools. Historical: same research tools as market-structure |
| **levels-analyst** | Live + historical | Live: `get_key_levels`, `get_proximity_report`, `get_or5_status`. Historical: `query_event_frequency`, `query_conditional`, `compare_sessions`, `get_session_history` |
| **playbook-evaluator** | Live only | `evaluate_playbook`, `get_setup_context`, `get_setup_state_history`, `acknowledge_setup_prompt`, `mark_setup_in_trade`, `close_setup_state`, `get_market_snapshot`, `get_key_levels`, `get_proximity_report` |
| **backtest-analyst** | Historical only | `backfill_history`, `run_backtest`, `get_backfill_status`, `get_backtest_results`, `compare_backtests`, `compare_sessions`, `get_session_history`, `get_research_summary`, all `query_*` research tools, `register_hypothesis`, `list_hypotheses`, `summarize_hypothesis_run`, `propose_draft_setup` |
| **performance-analyst** | Historical only | `get_trader_context_fit`, `get_setup_performance_matrix`, `get_signal_performance`, `query_signal_outcome_*`, `query_distribution`, `query_conditional`, `get_session_history`, `get_research_summary` |
| **risk-coach** | Live | `get_risk_state`, `get_risk_config`, `get_account_state`, `get_kelly_position_size`, `record_trade_result`, `save_account_state`, `init_risk_state`, `get_pre_session_briefing`, `refresh_memory_state`, `get_memory_brief`, `get_session_review_context`, `review_trade_entry` |
| **data-integrity-validator** | Both | `validate_data_integrity`, `get_feed_health`, `get_session_summary` |

### Event Types for `query_event_frequency`

Structural: `ib_formed`, `or_formed`, `ib_mid_test`, `ib_extension_hit`, `ib_ext_0.5x_high`, `ib_ext_1.0x_high`, `ib_ext_1.5x_high`, `new_session_high`, `new_session_low`, `dnp_cross`, `day_type_change`, `poor_high_detected`, `poor_low_detected`, `excess_high_detected`, `excess_low_detected`, `or5_mid_retest`. Flow: `absorption_detected`, `pinch_detected`, `acceleration_zone_created`, `acceleration_zone_held`, `large_trade_cluster`, `rvol_spike`.

---

## Testing with Mock / Historical Data

For development without a live Sierra Chart connection, use `.scid` files from Sierra Chart's data directory. The feed system supports both live tail-reading and bulk historical backfill.

```bash
# Run all tests
cargo test

# Windows recovery when an interrupted test leaves the default target exe locked
$env:CARGO_TARGET_DIR='target_verify'; cargo test

# Run specific pipeline tests
cargo test pipelines::tpo
cargo test pipelines::delta
cargo test pipelines::pinch
cargo test pipelines::event_detector

# Run research / backfill tests
cargo test backfill
cargo test research

# Run end-to-end golden replay drift protection
cargo test --test session_replay_golden

# Bless reviewed golden replay changes after intentional pipeline behavior changes
$env:THE_DESK_BLESS_GOLDENS='1'; cargo test --test session_replay_golden
```

### Golden Replay Verification

`tests/session_replay_golden.rs` generates a deterministic two-session synthetic `.scid`
fixture, replays it through `run_backfill_job_with_options`, and compares canonical
outputs for core session/events, rules-enabled signals/outcomes, and non-monotonic
timestamp handling against `tests/fixtures/session_replay/v1/*.json`.

Use the ignored private regression test for real Sierra files that must not be committed:

```bash
$env:THE_DESK_GOLDEN_SCID_DIR='D:\private\scid-goldens'
$env:THE_DESK_GOLDEN_EXPECTED_DIR='D:\private\the-desk-goldens'
$env:THE_DESK_GOLDEN_START_DATE='2026-03-02'      # optional
$env:THE_DESK_GOLDEN_END_DATE='2026-03-06'        # optional
$env:THE_DESK_GOLDEN_PRICE_SCALE='100'            # optional, use for scaled Rithmic files
cargo test --test session_replay_golden -- --ignored
```

Only bless golden changes after reviewing whether the drift is expected domain behavior
or an accidental regression.

### MCP logging config

Structured MCP diagnostics default to JSON on stderr so stdout remains reserved for MCP protocol messages. Optional `~/.the-desk/config.toml` block:

```toml
[logging]
level = "info"
format = "json"                 # json | compact
destination = "stderr"          # stderr | file | both | none
file_path = "C:\\Users\\you\\.the-desk\\logs\\the-desk-mcp.jsonl"
file_retention_days = 14
runtime_event_buffer = 1000
runtime_event_suppression_window_ms = 1000
runtime_event_suppression_heartbeat_ms = 60000
persist_runtime_events = true
runtime_event_retention_days = 7
runtime_event_max_rows = 50000
```

### Stable runtime event names

These are operational diagnostics exposed by `get_runtime_events` and JSON logs. They are intentionally low-volume and never include raw tick streams.

| Event name | Meaning |
|------------|---------|
| `mcp.startup` | MCP server initialized and runtime config loaded. |
| `rollover.status_evaluated` | Contract rollover status was not OK during validation. |
| `rollover.prior_levels_cleared` | Prior levels were cleared because no authoritative reference was available. |
| `rollover.startup_prior_levels_cleared` | Startup cleared prior levels before serving tools. |
| `historical_job.started` | Backfill/backtest worker started. |
| `historical_job.completed` | Backfill/backtest worker completed successfully. |
| `historical_job.cancelled` | Backfill/backtest worker was cancelled. |
| `historical_job.failed` | Backfill/backtest worker failed. |
| `raw_tick_ingest.started` | Background raw tick ingest started. |
| `raw_tick_ingest.finished` | Background raw tick ingest completed. |
| `raw_tick_ingest.failed` | Background raw tick ingest failed. |
| `scid.file_missing` | Configured `.scid` file was not found. |
| `scid.warm_replay.started` | Startup warm replay began. |
| `scid.warm_replay.completed` | Startup warm replay completed. |
| `scid.warm_replay.failed` | Startup warm replay failed. |
| `scid.warm_replay.empty` | Startup warm replay found no ticks. |
| `scid.warm_replay.truncated` | Warm replay stopped before requested cutover offset. |
| `scid.warm_replay.skipped_all` | Warm replay skipped all candidate ticks as non-monotonic. |
| `scid.startup_cutover` | Live tail cutover offset was selected. |
| `scid.tail_reset` | `.scid` file shrank and tail offset was reset. |
| `scid.tail_realign` | Tail offset was not record-aligned and was realigned. |
| `scid.poll_failed` | Live `.scid` poll failed. |
| `scid.non_monotonic_skip_summary` | Warm replay or live tail skipped non-monotonic ticks. |
| `session.boundary` | Live session boundary crossed. |
| `session.segment_boundary` | Live delta segment boundary crossed. |
| `session.rth_close_finalized` | RTH close persisted atomically. |
| `session.rth_close_finalize_failed` | RTH close finalization failed and needs retry/attention. |
| `session.rth_close_reconcile_started` | Startup replay reconciled a missing RTH close. |
| `setup.transition` | Setup lifecycle transition persisted. |
| `depth.poll_failed` | Depth poll task failed. |
| `depth.worker_failed` | Depth worker failed while processing/persisting depth state. |
| `context_frame.cache_warm_failed` | Context-frame cache pre-warm failed; live tools still work but the next agent read may pay the query cost. |
| `attention.signal_emitted` | Attention signal was emitted or refreshed from deterministic market/setup/risk context. |
| `attention.signal_priority_changed` | Existing attention signal changed priority bucket or score. |
| `attention.signal_acknowledged` | Trader or agent acknowledged an attention signal. |
| `attention.signal_expired` | Attention signal expired from the active inbox. |
| `attention.signal_invalidated` | Attention signal or linked idea was invalidated. |
| `attention.notifier_dispatched` | Configured notifier sink dispatched an attention alert. |
| `attention.notifier_failed` | Reserved for future external notifier sinks (webhook/toast) when dispatch fails. |
| `hypothesis.registered` | A typed hypothesis setup was registered. |
| `hypothesis.run_summarized` | A completed hypothesis backtest was summarized. |
| `hypothesis.gate_passed` | A hypothesis run passed the promotion gate. |
| `hypothesis.gate_failed` | A hypothesis run failed the promotion gate. |
| `hypothesis.promoted_to_draft` | A passing hypothesis was promoted to an inactive draft setup. |
| `hypothesis.activated` | A draft setup was activated after trader confirmation. |
| `hypothesis.rejected` | A hypothesis or draft was rejected by the trader. |
| `hypothesis.retired` | A hypothesis or draft was retired. |
| `hypothesis.engine_version_drift` | Cached hypothesis metrics were stale relative to the current engine version. |
