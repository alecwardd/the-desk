# Codebase Audit & Opinion

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

External codebase review synthesized into this document for traceability alongside research findings and the idea backlog. Paths are relative to the repository root unless noted.

### Overall verdict

This is a **serious, well-architected system** — not a hobby repo. ~36K LOC of Rust with a clean three-layer separation, incremental pipeline math, typed error boundaries, and 80+ unit tests. The domain correctness is the thing that impresses most: DNVA uses `|delta|` not signed delta, value area expands outward from POC (not "middle 70%"), OR/IB are correctly scoped by minute-of-session, single prints are tracked per period. These are the exact places bad trading software gets the math wrong, and this codebase does not. The research layer on top (81 RTH sessions yielding "Double Distribution dominates, London→RTH continues only 41.5%, absorption-failure > absorption") is genuinely the basis of a professional edge, not vibes.

That said, the project is in the zone where the next order of improvement is not more pipelines — it is **hardening the edges, tightening the agent surface, and closing the research→playbook loop**.

### Strengths to build on

1. **Three-layer discipline is holding.** No LLM calls in Rust, no raw ticks to Claude, no rules bypass. That architectural spine is what will let this scale to multi-instrument and multi-account without becoming spaghetti.
2. **Incremental math everywhere.** Every pipeline accumulates; nothing recomputes from scratch. This is the right ceiling for sub-ms tick latency and the reason 100-pt volatile opens do not melt the system.
3. **Terminology precision.** [CLAUDE.md](../../CLAUDE.md) enforces it and the code reflects it. That is a moat — most trading tooling (retail and vendor) gets TPO/delta/value-area wrong.
4. **Research infrastructure exists.** [src/research/mod.rs](../../src/research/mod.rs) plus [src/backfill.rs](../../src/backfill.rs) plus the event detector means you can actually ask "given X, what is P(Y)?" against real history. Most traders never get there.
5. **Observability primitives are in place.** `McpFeedRuntimeState` in [src/bin/the-desk-mcp.rs](../../src/bin/the-desk-mcp.rs) exposes tick freshness, lock contention, poll latency, SCID offsets, and now non-monotonic SCID counters via tools. Combined with `scan_scid_timestamp_anomalies`, this is a good foundation for feed diagnostics.
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

[src/bin/the-desk-mcp.rs](../../src/bin/the-desk-mcp.rs) at ~9K LOC with 50+ tools is approaching the point where **it should be split**. Right now it is a single file handling snapshots, profiles, microstructure, options, research, risk, memory, backfill, and ingest. Recommendations:

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
   [agents/](../../agents/) has role agents (orchestrator, levels-analyst, risk-coach, etc.) but there is no persistent model of **the trader**: best/worst day-types, consecutive-loss behavior, actual hit rate by setup and by time-of-day, typical R deviation. The implementation direction is a typed `get_trader_context_fit` envelope over existing SQLite memory: execution memory comes from `behavioral_patterns` generated from recorded trades, setup opportunity remains separate from `signal_outcomes` / `get_context_frame`, and coaching reminders come from insights/follow-ups. This source separation is how the system becomes a trading *partner* rather than a market-structure oracle or a second inconsistent aggregation engine.

   **Implementation note (2026-05-04):** Phase 0-2 of the trader memory layer are now implemented and committed. `get_trader_context_fit` is the primary structured memory surface: it separates executed-trade memory, setup opportunity context, coaching reminders, live risk/post-loss state, reliability, provenance, and deterministic opportunity-vs-execution conflict detection. Next step is real-session use, not more speculative infrastructure. Track concrete misses where compact `contextFrameAnalog` is not enough (for example, needing full analog session lists inline or event replay after a matched context); only then revisit Phase 4. Markdown capsules remain cancelled/deferred unless structured memory proves hard for agents to consume in practice.

5. **Regime detection as a first-class concept**  
   "Double Distribution dominated 52 of 81 sessions" is a regime observation. Make regime (trending / balanced / double-dist / non-trend volatile) a **computed pipeline field** on every session, queryable historically, and used by the rules engine to gate which setups are even eligible. Most playbook failures are regime mismatches, not condition failures.
