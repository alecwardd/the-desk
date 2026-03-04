# The Desk — Agent Instructions

Universal instructions for any LLM coding agent (Claude Code, Cursor, Codex) working in this repository.

---

## Project Context

The Desk is a backend intelligence platform for discretionary NQ futures traders. It reads Sierra Chart `.scid` tick data, computes market structure and microstructure analytics in Rust, stores everything in SQLite, and exposes the intelligence layer via MCP (Model Context Protocol).

Read these documents in order:

1. **CLAUDE.md** — Project rules, architecture, conventions (READ FIRST)
2. **README.md** — Architecture overview, project structure, data flow
3. **Relevant skill** from `skills/` — Domain knowledge for your task

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

## Subagent Patterns

When you need specialized help, spawn subagents for these tasks.

> **Path note:** Agent definitions live in `agents/` at the project root. Cursor also discovers them at `.cursor/agents/` (symlinked). Both paths resolve to the same files.

### Orchestrator (Primary Entry Point)
**When:** The trader interacts with The Desk for any market question, setup evaluation, trade recording, or session management. This is the default agent.
**How:** Use `orchestrator` (defined in `agents/orchestrator.md`). The orchestrator routes to all specialist agents and ensures risk-coach context is present on every interaction. It calls the same MCP tools the specialists use, with its own synthesis logic and a mandatory risk footer on every response.
**Definition:** `agents/orchestrator.md`

### DTC Protocol Research
**When:** Working on the DTC client or .scid data parsing
**How:** Delegate to `dtc-protocol-researcher` (defined in `agents/dtc-protocol-researcher.md`)

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

## MCP Tools Reference

The MCP server (`src/bin/the-desk-mcp.rs`) exposes 35 tools. Key categories:

| Category | Tools |
|----------|-------|
| **Snapshot** | `get_market_snapshot` (includes VWAP) |
| **Structure** | `get_tpo_profile`, `get_delta_profile`, `get_key_levels` (includes prior day levels) |
| **Microstructure** | `get_tape_pace`, `get_footprint`, `get_absorption_events`, `get_trade_size_profile` |
| **PTT** | `get_or5_status`, `get_rvol`, `get_day_type`, `get_rebid_reoffer_zones`, `get_pinch_events`, `get_session_inventory` |
| **Rules** | `evaluate_playbook`, `get_setup_context`, `check_delta_confirmation` |
| **Risk** | `get_risk_state`, `get_risk_config`, `save_risk_config`, `init_risk_state`, `get_account_state`, `save_account_state`, `get_kelly_position_size`, `get_signal_performance`, `record_trade_result` |
| **Data** | `query_ticks`, `get_proximity_report` |
| **Integrity** | `validate_data_integrity` |
| **Research** | `query_event_frequency`, `query_conditional`, `query_distribution`, `query_signal_outcome_distribution`, `query_signal_outcome_conditional`, `compare_sessions`, `get_session_history`, `get_research_summary` |
| **Backfill** | `backfill_history`, `run_backtest`, `get_backfill_status`, `cancel_backfill`, `get_backtest_results`, `compare_backtests` |
| **Storage** | `archive_status` |

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
