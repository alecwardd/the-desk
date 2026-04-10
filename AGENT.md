# The Desk — Agent Instructions

Universal instructions for any LLM coding agent (Claude Code, Cursor, Codex) working in this repository.

---

## Project Context

The Desk is a backend intelligence platform for discretionary NQ futures traders. It reads Sierra Chart `.scid` tick data, computes market structure and microstructure analytics in Rust, stores everything in SQLite, and exposes the intelligence layer via MCP (Model Context Protocol).

Read these documents in order:

1. **CLAUDE.md** — Project rules, architecture, conventions (READ FIRST)
2. **README.md** — Architecture overview, project structure, data flow
3. **Relevant skill** from `skills/` — Domain knowledge for your task
4. **`docs/dom-replay.md`** — Required reading for DOM visualizer and historical ladder replay tasks

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
- The Tauri desktop frame exists but is an optional visualization layer — MCP is the primary interface
- **Every layer must be independently testable.** Never skip layers.

---

## Agent Scope (Default Focus)

**Primary:** You work in this repo to support the agentic trading partner. Your outputs appear in Cursor, Claude Code, Codex, or similar platforms. Focus on:
- Rust backend (pipelines, rules, feed, db, research)
- MCP server and tools
- Agent definitions and prompts
- SQLite, backfill, research queries

**Secondary (only when explicitly asked):** The Tauri desktop app and React frontend (`src/`, `src-tauri` Tauri-specific code) are optional. Do NOT modify Tauri/React unless the user explicitly requests work on the desktop app, DOM replay, or UI.

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
**How:** Read `skills/trading-domain/SKILL.md` for session semantics; inspect `src-tauri/src/feed/scid_reader.rs` and `src-tauri/src/depth/` for formats. Live paths are **Sierra `.scid` + optional `.depth` files only** (no socket DTC client in-tree).

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

**Scope check:** Is this task about the Tauri app, DOM replay, or React UI? If no, focus on Rust/MCP. If yes, read `docs/dom-replay.md` (for DOM) or `skills/tauri-bridge/SKILL.md` (for IPC) first.

1. **Read the relevant skill** from `skills/` for domain knowledge
2. **Write the Rust code** in the appropriate module (`pipelines/`, `rules/`, `feed/`, `db/`)
3. **Write tests** alongside the code — every pipeline must have unit tests
4. **Integrate with `PipelineEngine`** if adding a new pipeline (update `mod.rs`, `MarketState`, `snapshot()`)
5. **Add `ConditionField` variants** if the rules engine needs to evaluate the new data
6. **Add MCP tool** in `src/bin/the-desk-mcp.rs` if agents need access
7. **Run `cargo test`** before declaring done

### DOM Replay / Tauri UI Tasks (Only When Explicitly Asked)

If the task touches the DOM visualizer, historical ladder playback, or replay UI:

1. Read `docs/dom-replay.md` before making changes
2. Treat `src-tauri/src/dom_replay.rs` as the backend source of truth
3. Keep DOM replay separate from `.desk` session replay in `src-tauri/src/recording/mod.rs`
4. Preserve the current behavioral contract:
   - SQLite first, Sierra file fallback second
   - timestamp-based seek
   - session-to-cursor volume profile
   - no fake DOM when depth is unavailable

---

## Decision Framework

| Question | Guidance |
|----------|----------|
| Should this be in Rust or TypeScript? | If it processes market data or evaluates rules → Rust. If it's UI-only → TypeScript. |
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

The MCP server (`src/bin/the-desk-mcp.rs`) exposes 54 tools across 11 categories.

### Live vs Historical — Quick Reference

**Live tools** read from the in-memory pipeline (current session only). They answer "what's happening now?" and require an active feed or startup backfill. Use for: market reads, setup checks, levels, flow, risk, DOM.

**Historical tools** read from SQLite (session_summaries, market_events, signal_outcomes, raw_ticks). They answer "what happened in the past?" and require `backfill_history` to have been run. Use for: event frequency, conditional probability, session comparison, setup performance, backtests.

| Context | Primary tools |
|---------|---------------|
| **Live (current session)** | `get_market_snapshot`, `get_session_context`, `get_tpo_profile`, `get_delta_profile`, `get_key_levels`, `get_tape_pace`, `get_footprint`, `get_or5_status`, `get_rvol`, `get_day_type`, `get_rebid_reoffer_zones`, `get_pinch_events`, `get_session_inventory`, `evaluate_playbook`, `get_setup_context`, `check_delta_confirmation`, `get_proximity_report`, `get_imbalances`, `get_absorption_events`, `get_trade_size_profile`, DOM tools |
| **Historical (backfill data)** | `get_snapshot_at`, `get_footprint_window`, `query_ticks`, `get_session_history`, `get_research_summary`, `query_event_frequency`, `query_conditional`, `query_distribution`, `compare_sessions`, `get_setup_performance_matrix`, `query_signal_outcome_*`, `get_signal_performance`, `backfill_history`, `run_backtest`, `get_backfill_status`, `get_backtest_results`, `compare_backtests` |

**Data dependency:** Historical tools return empty or minimal data until `backfill_history` has populated the database. Call `get_research_summary` first to check session count; if low, run backfill before deep analysis.

### Full Tool List by Category

| Category | Tools | Description |
|----------|-------|-------------|
| **Snapshot** | `get_market_snapshot` | Current price, VWAP, session state |
| | `get_session_context` | Session type (RTH/Globex), segment (Asia/London), trading day, data freshness |
| | `get_session_summary` | Total tick count, latest tick timestamp, latest pipeline snapshot (health check) |
| | `get_feed_health` | SCID path status, file metadata, ingest lag, freshness diagnostics |
| | `get_snapshot_at` | Historical pipeline snapshot nearest to a given timestamp |
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
| **Backfill** | `backfill_history` | Queue historical backfill job (all 14 pipelines + event detection) |
| | `run_backtest` | Queue backtest replay job (rules engine over historical data) |
| | `get_backfill_status` | Poll progress for backfill/backtest jobs |
| | `cancel_backfill` | Cancel in-flight backfill/backtest job |
| | `get_backtest_results` | Retrieve stored backtest runs with metrics |
| | `compare_backtests` | Compare two or more backtest runs side-by-side |
| **Memory** | `get_memory_brief` | Ranked carry-forward memory by intent (session_start, setup_check, trade_review, weekly_review) |
| | `get_pre_session_briefing` | Memory brief + account + risk state for session start |
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
| **orchestrator** | Both — routes by intent | All; routes `historical_research` to backtest-analyst. Memory: `get_pre_session_briefing`, `get_memory_brief`, `save_agent_insight`, `recall_agent_insights`, `acknowledge_agent_insight`, `create_memory_followup`, `resolve_memory_followup`, `detect_behavioral_patterns`, `get_behavioral_patterns` |
| **market-structure-analyst** | Live + historical | Live: `get_tpo_profile`, `get_key_levels`, `get_day_type`, `get_rvol`, `get_delta_profile`. Historical: `query_event_frequency`, `query_conditional`, `query_distribution`, `compare_sessions`, `get_session_history`, `get_research_summary` |
| **orderflow-analyst** | Live + historical | Live: `get_delta_profile`, `get_tape_pace`, `get_footprint`, `get_imbalances`, `get_absorption_events`, DOM tools. Historical: same research tools as market-structure |
| **levels-analyst** | Live + historical | Live: `get_key_levels`, `get_proximity_report`, `get_or5_status`. Historical: `query_event_frequency`, `query_conditional`, `compare_sessions`, `get_session_history` |
| **playbook-evaluator** | Live only | `evaluate_playbook`, `get_setup_context`, `get_market_snapshot`, `get_key_levels`, `get_proximity_report` |
| **backtest-analyst** | Historical only | `backfill_history`, `run_backtest`, `get_backfill_status`, `get_backtest_results`, `compare_backtests`, `compare_sessions`, `get_session_history`, `get_research_summary`, all `query_*` research tools |
| **performance-analyst** | Historical only | `get_setup_performance_matrix`, `get_signal_performance`, `query_signal_outcome_*`, `query_distribution`, `query_conditional`, `get_session_history`, `get_research_summary` |
| **risk-coach** | Live | `get_risk_state`, `get_risk_config`, `get_account_state`, `get_kelly_position_size`, `record_trade_result`, `save_account_state`, `init_risk_state` |
| **data-integrity-validator** | Both | `validate_data_integrity`, `get_feed_health`, `get_session_summary` |

### Event Types for `query_event_frequency`

Structural: `ib_formed`, `or_formed`, `ib_mid_test`, `ib_extension_hit`, `ib_ext_0.5x_high`, `ib_ext_1.0x_high`, `ib_ext_1.5x_high`, `new_session_high`, `new_session_low`, `dnp_cross`, `day_type_change`, `poor_high_detected`, `poor_low_detected`, `excess_high_detected`, `excess_low_detected`, `or5_mid_retest`. Flow: `absorption_detected`, `pinch_detected`, `acceleration_zone_created`, `acceleration_zone_held`, `large_trade_cluster`, `rvol_spike`.

---

## Testing with Mock / Historical Data

For development without a live Sierra Chart connection, use `.scid` files from Sierra Chart's data directory. The feed system supports both live tail-reading and bulk historical backfill.

```bash
# Run all tests
cd src-tauri && cargo test

# Run specific pipeline tests
cd src-tauri && cargo test pipelines::tpo
cd src-tauri && cargo test pipelines::delta
cd src-tauri && cargo test pipelines::pinch
cd src-tauri && cargo test pipelines::event_detector

# Run research / backfill tests
cd src-tauri && cargo test backfill
cd src-tauri && cargo test research
```
