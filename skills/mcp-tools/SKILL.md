---
name: McpTools
description: MCP tool routing for The Desk. USE WHEN any agent needs to decide which MCP tool to call — market reads, setup checks, trade ideas, risk, journaling, memory, research/backtests, or data diagnostics. Maps trader scenarios to the right tools and call order. The exhaustive catalog lives in docs/mcp/tool-reference.md (generated, never stale).
---

# MCP Tool Routing

The Desk MCP server exposes **121 tools in 9 domains**. This skill tells you *which* tool to call *when*. For the full catalog with every description, read [docs/mcp/tool-reference.md](../../docs/mcp/tool-reference.md) — it is generated from the compiled server and guarded by a test, so it is never stale.

**Source layout:** each domain is a module in `src/bin/the-desk-mcp/tools/` (market, dom, options, playbook, risk, journal, memory, research, admin).

---

## The Two Questions to Ask First

**1. Live or historical?**

- **Live tools** read the in-memory pipeline for the *current session*. They answer "what is happening now?" and need an active feed (or startup warm replay). Domains: Market, DOM, Options, Playbook, Risk.
- **Historical tools** read SQLite. They answer "what happened before / how often?" and need `backfill_history` to have populated the database. Domains: Research, plus the timestamped variants (`get_snapshot_at`, `get_footprint_window`, `get_context_frame(timestampMs)`, `query_ticks`).

Before deep historical analysis, call `get_research_summary` once — if session count is low, queue `backfill_history` first and poll `get_backfill_status`.

**2. Raw values or interpretation?**

- `get_market_snapshot` → raw numbers + data freshness.
- `get_context_frame` → bucketed session-relative framing, historical analogs, reliability tiers, caveats. Use it when you are about to *interpret* rather than *report*. Cite `N`/`effectiveSampleSize` and `reliabilityTier`; never present an `insufficient` frame as an edge.

---

## Scenario Routing

### Session start (every conversation)

1. `get_session_context` — session type/segment, trading day, freshness, rollover status. Check `rolloverStatus` before trusting any carry-forward level.
2. `get_market_snapshot` — price, VWAP bands, VA/POC, DNVA/DNP, key levels.
3. `get_risk_state` + `get_risk_config` + `get_account_state` (parallel) — hard-stop checks before any analysis.
4. `get_pre_session_briefing` — memory brief + account + risk in one call (auto-refreshes dirty memory).

### "What's the market doing?" / market read

- `get_market_snapshot` → `get_context_frame` for framing.
- Structure: `get_tpo_profile`, `get_tpo_detail`, `get_key_levels`, `get_day_type`.
- Flow: `get_delta_profile`, `get_tape_pace`, `get_footprint`, `get_imbalances`, `get_absorption_events`, `get_trade_size_profile`.
- PTT indicators: `get_or5_status`, `get_rvol`, `get_rebid_reoffer_zones`, `get_pinch_events`, `get_session_inventory`.

### "What deserves attention right now?"

- `get_attention_inbox` — always the first call for this question.
- `get_signal_detail` — evidence and suggested next tools for one signal.
- `what_changed_since` / `get_attention_changelog` — cursor-based catch-up after time away.
- `acknowledge_attention_signal` — after the trader has seen it.

### Price approaching a level

- `get_proximity_report` — which levels are near, sorted by distance.
- `get_delta_at_price` + `check_delta_confirmation` — is delta supporting the trade direction at that level? (Required before entry per playbook doctrine.)
- DOM behavior at the level: `get_liquidity_behavior_at_level`, `get_pull_stack_activity`, `explain_book_reaction`, `get_dom_window`, `get_dom_regime_summary`.
- Historical: `query_dom_reaction_at_levels`, `query_event_frequency` (e.g. `ib_extension_hit`), `query_conditional`.

### Setup and trade-idea lifecycle (potential trades)

This is the canonical "potential trade" flow — keep state in the system, not in chat:

1. `evaluate_playbook` — all active setups vs current state (met / approaching / notActive).
2. `get_setup_context` — full context for one named setup.
3. `get_active_trade_ideas` — current idea cards derived from setups + attention signals.
4. `mark_trade_idea_confirmed` (with evidence) → `mark_trade_idea_in_trade` (optionally linked to a signal outcome) → `mark_trade_idea_resolved` or `mark_trade_idea_invalidated` (with reason).
5. Setup lifecycle mirrors: `acknowledge_setup_prompt`, `mark_setup_in_trade`, `close_setup_state`, `get_setup_state_history`.

### Entering / sizing / closing an actual trade

- Size: `get_kelly_position_size` (1/4 Kelly, confidence-scaled). Risk gates: `get_risk_state`, `get_risk_config`.
- Record: `upsert_trade_entry` (plan + entry) → `close_trade_entry` → `record_trade_result` (updates risk state).
- Import fills from the platform: `import_trade_fills`.
- Session bookends: `start_trading_session` / `end_trading_session`; reset with `init_risk_state` only when explicitly asked.

### Post-session review and journaling

- `get_session_review_context` — one call assembling the review picture.
- `review_trade_entry`, `get_trade_entry`, `list_trade_entries`.
- `save_journal_entry`, `get_session_journal`, `get_recent_journal_notes`, `query_journal_patterns`.
- For each reviewed trade: `get_context_frame(timestampMs=entryTimestampMs)` so the review reflects context *at entry*, not now.

### Memory (durable trader context)

- Read: `get_memory_brief` (by intent: session_start, setup_check, trade_review, weekly_review), `get_trader_context_fit` (typed envelope when the answer depends on trader memory), `recall_agent_insights`, `get_behavioral_patterns`.
- Write: `save_agent_insight` (candidate/validated lifecycle), `create_memory_followup` / `resolve_memory_followup`, `acknowledge_agent_insight`.
- Maintenance: `refresh_memory_state` when `memoryMaintenance.refreshSuggested` is true or after memory-affecting writes in the same flow; `detect_behavioral_patterns` for explicit pattern sweeps.
- Memory reports context only — it must never adjust sizing by itself.

### Research questions ("how often…", "what happens after…")

- `query_event_frequency` — how often does event X occur? (Event types listed in AGENT.md.)
- `query_conditional` — when X happens N+ times, how often does Y follow?
- `query_distribution` — distribution stats for a numeric metric.
- `compare_sessions` — analog sessions by multi-dimensional similarity.
- `get_session_history`, `get_research_summary` — past sessions, statistical briefing.
- Outcomes: `query_signal_outcome_distribution` / `_conditional` / `_excursions`, `get_signal_performance`, `get_setup_performance_matrix`, `validate_signal_outcome_integrity`.
- Respect the Research Sample Size Policy in AGENT.md — always report N.

### Backtests and hypothesis promotion

1. `register_hypothesis` (typed SetupDefinition + metadata; use `dryRun` to validate).
2. `run_backtest` → poll `get_backfill_status` → `summarize_hypothesis_run`.
3. `get_backtest_results`, `compare_backtests` for stored runs.
4. Promotion gate: `propose_draft_setup` → human confirmation → `activate_draft_setup`.
5. `list_hypotheses` first so you never re-test a rejected idea; `set_hypothesis_lifecycle` to retire/reject.
6. `cancel_backfill` for runaway jobs.

### Options context

- `get_gamma_levels`, `get_options_context`; `refresh_options_snapshot` when stale. Requires `[options].enabled = true` in `~/.the-desk/config.toml`.

### Something looks wrong with the data

- `get_feed_health` — SCID path, ingest lag, freshness. First call for "is the feed alive?"
- `validate_data_integrity` — pipeline invariants (POC in VA, delta sums, monotonicity).
- `get_session_summary` — tick counts and latest snapshot sanity check.
- `get_contract_rollover_status` / `validate_contract_rollover` — before trusting carry-forward levels near roll dates.
- `get_runtime_events` — structured diagnostics for post-mortems.
- Gaps and repair: `get_raw_tick_ingest_gaps`, `ingest_raw_ticks_from_scid`, `scan_scid_timestamp_anomalies`, `backfill_history`, `archive_status`.

### Protecting the data (backups)

- `create_database_backup` — verified `VACUUM INTO` snapshot of the whole SQLite store (trades, journal, signal outcomes, memory). Call it before risky operations: large imports, schema migrations, or any bulk edit the trader wants a known-good restore point for.
- The server also takes an automatic verified snapshot on startup (bounded by `[backup].min_interval_hours`) and prunes old snapshots by age and count — no tool call needed for routine protection. Snapshots live in `~/.the-desk/backups`.

---

## Anti-Patterns

- **Don't** call `query_ticks` to summarize market state — use snapshot/profile tools; raw ticks are for targeted forensics.
- **Don't** interpret without a frame — pair `get_market_snapshot` with `get_context_frame` when giving a read.
- **Don't** run research tools before checking `get_research_summary` for sample coverage.
- **Don't** track potential trades in conversation text — use trade idea cards so state survives the session.
- **Don't** skip `check_delta_confirmation` before entry discussion — playbook doctrine requires it.
- **Don't** trust prior-day levels near contract roll without `get_contract_rollover_status`.
- **Don't** let memory tools influence position sizing — memory is context, the trader decides.

## Adding or Changing Tools

When the tool surface changes, regenerate the catalog and keep guards green:

```
cargo run --bin the-desk-mcp -- --write-tool-docs
cargo test --bin the-desk-mcp
```

See [docs/mcp/README.md](../../docs/mcp/README.md) for the server architecture and the add-a-tool checklist.
