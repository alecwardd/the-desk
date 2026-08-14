# The Desk — Decision Log

Architectural Decision Records (ADRs) for key decisions made during planning and development. Each entry records what was decided, why, and what alternatives were considered.

---

## Format

Each decision follows this structure:
- **ID:** Sequential (ADR-NNN)
- **Date:** When the decision was made
- **Status:** Decided | Pending | Superseded
- **Context:** Why the decision was needed
- **Decision:** What was decided
- **Alternatives considered:** What else was evaluated
- **Consequences:** Tradeoffs accepted

---

## Decided

### ADR-001: Import backtests, don't build an engine

**Date:** 2026-02-20
**Status:** Decided
**Source:** CLAUDE.md Never Do list, the-desk-vision.md

**Context:** The V1 vision document included language about "providing tools to test your strategies." Building a backtesting engine is a large, complex feature that distracts from the core value proposition (real-time coaching).

**Decision:** The Desk imports backtest results from external tools (Sierra Chart, NinjaTrader, TradingView, custom scripts). It does not execute backtests.

**Alternatives considered:**
- Build a basic backtest engine with replay data — rejected (scope creep, competes with specialized tools)
- Partner with a backtest provider API — rejected (adds dependency, latency, cost)

**Consequences:** Traders must perform their own backtesting externally. The import flow must support multiple formats (Phase 1: Sierra Chart CSV; Phase 2: NinjaTrader, TradingView, generic CSV, JSON).

---

### ADR-002: LLM context assembled in TypeScript, not Rust

**Date:** 2026-02-25
**Status:** Decided
**Source:** tech-plan.md Section 1

**Context:** The LLM prompt needs data from multiple sources (setup rules, risk state, journal notes). This context could be assembled in Rust (single IPC call with all data) or in TypeScript (multiple IPC calls to fetch components).

**Decision:** TypeScript assembles the context. Rust emits a minimal `SetupAlert` (setup_id, state_transition, conditions, price). TypeScript makes 3 sequential Tauri command calls to fetch setup, risk state, and journal notes, then builds the prompt.

**Alternatives considered:**
- Rust assembles full prompt context and sends it in the alert event — rejected (couples Rust to LLM context requirements, harder to iterate on prompt engineering)
- Hybrid: Rust sends a richer event with setup + risk embedded — rejected (partial coupling, still need journal query)

**Consequences:** 3 extra IPC round-trips add ~5-15ms before the Claude call. This is negligible against the 1-2s LLM latency budget. Prompt engineering stays fully in TypeScript, making iteration faster.

---

### ADR-003: Pipeline snapshots every 30 seconds for recording scrub

**Date:** 2026-02-25
**Status:** Decided
**Source:** tech-plan.md Section 1

**Context:** Tape replay needs to scrub to any timestamp. Without snapshots, scrubbing requires replaying all ticks from session start. With frequent snapshots, scrubbing loads the nearest snapshot and replays only a short window.

**Decision:** Pipeline state (`MarketState`) is snapshotted every 30 seconds into the recording file. On scrub, the engine loads the nearest snapshot before the target timestamp and replays ticks from that point.

**Alternatives considered:**
- 5-second snapshots — rejected (larger file size, marginal scrub improvement)
- 60-second snapshots — rejected (up to 60s of recomputation on scrub, noticeable delay)
- No snapshots, always replay from start — rejected (unusable for long sessions)

**Consequences:** At most 30 seconds of recomputation on any scrub operation. Recording file size increases by ~2KB per snapshot (every 30s = ~780 snapshots for a 6.5-hour RTH session = ~1.5MB overhead).

---

### ADR-004: Rules engine implements 6-state machine

**Date:** 2026-02-25
**Status:** Decided
**Source:** tech-plan.md Section 3

**Decision:** Each setup tracks through 6 states: `not_active` -> `approaching` -> `conditions_met` -> `confirmed` -> `in_trade` -> `closed`. Only `conditions_met` triggers a Claude API call. `confirmed` is set after the coaching prompt is generated and emitted.

**Alternatives considered:**
- Simple binary (conditions met / not met) — rejected (no approaching notification, no trade tracking)
- 4-state without approaching — rejected (traders want advance notice when a setup is developing)

**Consequences:** More complex state management in the rules engine, but richer UX (watching notifications, trade lifecycle tracking, post-trade summary prompts).

---

### ADR-005: 4Hz UI throttle for market state updates

**Date:** 2026-02-25
**Status:** Decided
**Source:** tech-plan.md Section 1

**Context:** Pipelines process at data-feed speed (100-500 messages/second during active markets). Updating the UI at this rate would overwhelm React rendering.

**Decision:** The pipeline aggregator emits `MarketState` snapshots to the UI at 4Hz (every 250ms). Coaching prompts are emitted immediately when generated (not throttled).

**Alternatives considered:**
- 1Hz — rejected (too slow, trader sees stale numbers)
- 10Hz — rejected (diminishing returns, higher CPU)
- Event-driven (only on change) — rejected (during active markets, this would be effectively tick-by-tick)

**Consequences:** UI values may be up to 250ms stale. This is acceptable for the sidebar display. The coaching feed is not throttled — prompts appear immediately.

---

### ADR-006: RTH end time = 4:15 PM ET for NQ futures

**Date:** 2026-02-25
**Status:** Decided
**Source:** CLAUDE.md, corrected in phase-1-prd.md

**Context:** NQ futures on CME have a cash settlement reference at 4:00 PM ET, but futures trading continues until 4:15 PM ET.

**Decision:** RTH end time is 4:15 PM ET for all pipeline calculations, session boundaries, and recording stop times.

**Alternatives considered:**
- 4:00 PM ET (cash settlement) — rejected (misses 15 minutes of actual trading)
- Configurable per session — considered for future, but default must be correct

**Consequences:** All documents use 4:15 PM ET consistently. The 4:00 PM settlement time is noted where relevant (e.g., VWAP settlement calculation).

---

### ADR-007: Coaching-only, never trade execution

**Date:** 2026-02-20
**Status:** Decided
**Source:** CLAUDE.md, the-desk-vision.md, epic-brief.md

**Decision:** The Desk never places, modifies, or cancels orders. It never connects to a trading API for execution purposes. It is a coaching and discipline tool.

**Consequences:** Simplifies architecture (no order management), eliminates liability from execution errors, maintains clear regulatory positioning as a coaching tool rather than an investment advisory service.

---

### ADR-008: Pivot from Tauri GUI to Backend Intelligence Platform with MCP

**Date:** 2026-02-26
**Status:** Decided
**Supersedes:** ADR-002 (TypeScript context assembly), ADR-005 (4Hz UI throttle)

**Context:** Sierra Chart intentionally blocks CME Group market data from being served over the DTC protocol to third-party clients. This made the original architecture (DTC → Rust → React UI) unviable for NQ futures. Separately, the emergence of MCP (Model Context Protocol) in Cursor IDE created a more powerful interaction pattern than a dedicated GUI — agents with full context can serve as the trading partner interface.

**Decision:** Pivot from a Tauri desktop GUI app with DTC connectivity to a backend intelligence platform that:
1. Reads Sierra Chart's `.scid` binary tick data files directly (no DTC dependency)
2. Computes all market structure and microstructure analytics in Rust
3. Stores raw ticks and computed state in SQLite
4. Exposes intelligence via MCP server (24 tools) callable by any Cursor agent
5. Retains Tauri as an optional visualization layer, not the primary interface

**Alternatives considered:**
- Alternative data providers (Databento, Polygon, CME direct) — viable but adds subscription cost; `.scid` reading is free with existing Sierra Chart license
- Keep building the Tauri GUI with a different data source — rejected (MCP interface is strictly more capable than a custom GUI for AI-assisted workflows)
- WebSocket server instead of MCP — rejected (MCP is natively supported by Cursor, no custom client needed)

**Consequences:**
- Data latency is 1-5 seconds (Sierra Chart file flush interval) — acceptable for directional trading (15-min to 1-hr holds)
- Not suitable for HFT or sub-second scalping strategies
- AI agents become first-class consumers of market data, not just a coaching afterthought
- Prior planning docs (vision, PRDs, design spec, core flows, tech plan) are archived to `docs/archive/v0-tauri-gui/`
- The project is significantly simpler: no custom DTC client needed for data, no mandatory React UI

---

### ADR-009: .scid file format as canonical data source

**Date:** 2026-02-26
**Status:** Decided

**Context:** With DTC blocked for CME data, we needed an alternative ingestion path. Sierra Chart stores all intraday data as `.scid` binary files (56-byte header + sequential 40-byte records) on the local filesystem as part of normal operation.

**Decision:** Read `.scid` files directly from Sierra Chart's data directory. Each record contains timestamp, open, high, low, close, volume, bid volume, ask volume — everything needed for all pipeline calculations.

**Alternatives considered:**
- Sierra Chart DTC server for non-CME data — works but irrelevant for NQ
- Sierra Chart spreadsheet export — rejected (manual, not real-time)
- Sierra Chart ACSIL plugin to push data — rejected (requires C++ plugin development and maintenance)

**Consequences:** Zero additional cost. Depends on Sierra Chart being open and writing data. Latency equals Sierra Chart's flush interval (configurable, typically ~1s). The .scid reader must handle partial writes at EOF gracefully.

---

### ADR-010: Trade direction from .scid bid/ask volumes

**Date:** 2026-02-26
**Status:** Decided
**Resolves:** ADR-P02 (Trade direction classification)

**Context:** Each `.scid` record includes `BidVolume` and `AskVolume` fields. When `AskVolume > 0`, the trade was at the ask (buyer-initiated). When `BidVolume > 0`, the trade was at the bid (seller-initiated).

**Decision:** Use Sierra Chart's native bid/ask volume classification directly from `.scid` records. No secondary classification needed.

**Consequences:** Delta calculations are as accurate as Sierra Chart's own classification, which uses the exchange-provided aggressor flag where available.

---

### ADR-011: Deterministic research infrastructure replaces external backtesting

**Date:** 2026-02-26
**Status:** Decided
**Supersedes:** ADR-001 (Import backtests, don't build an engine)

**Context:** The original ADR-001 rejected an in-repo backtesting engine to avoid scope creep. As the MCP-based architecture matured, it became clear that agents need historical statistical context to provide useful coaching — questions like "how often is IB-mid tested?" or "if price breaks above IB 3 times, how often does it close above?" require structured historical data that external tools cannot efficiently provide in the conversational flow.

**Decision:** Build a deterministic research infrastructure within the repo:
1. **EventDetector** — logs ~30 structured market events during live pipeline processing and historical backfill
2. **Session summaries** — 35+ field end-of-session snapshots for cross-session comparison
3. **Signal outcomes** — MFE/MAE/R-result tracking per playbook signal
4. **Research query engine** — frequency, conditional probability, distribution, and session comparison queries
5. **Backfill pipeline** — process historical .scid files through all pipelines to populate the research database
6. **9 MCP research tools** — expose all research capabilities to specialized subagents

This is NOT a backtesting engine in the traditional sense — it does not simulate order fills, model slippage, or calculate equity curves. It answers structural and statistical questions about market behavior deterministically.

**Alternatives considered:**
- Keep importing from external tools (ADR-001) — rejected (too slow for conversational flow, agents can't ask ad-hoc questions)
- Build a full backtesting engine with order simulation — rejected (out of scope, unnecessary for coaching use case)

**Consequences:**
- Agents can answer statistical questions in-conversation without manual data preparation
- Requires historical .scid data to be backfilled (one-time operation per symbol)
- All research queries are deterministic — same data always produces same answers
- MCP tool count increased from 24 to 33
- Four specialized subagents (levels-analyst, performance-analyst, backtest-analyst, plus updated market-structure-analyst and orderflow-analyst) leverage the research tools

---

### ADR-012: Embed Dalton AMT and Smashelito frameworks in market-structure-analyst

**Date:** 2026-02-26
**Status:** Decided

**Context:** The market-structure-analyst subagent was a bare tool list with no embedded domain knowledge, no analytical workflow, and no output format. Compared to stronger agents like `pipeline-verifier.md` (which has "Always do this first" checklists, working methods, and output templates), the market-structure-analyst had no structure to guide its reasoning. Additionally, 5 research MCP tools existed in the Rust binary but lacked JSON descriptor files, making them invisible to the Cursor tool discovery layer.

**Decision:** Rewrite `agents/market-structure-analyst.md` to embed:
1. **Jim Dalton's Auction Market Theory** — a 6-step decision tree (Timeframe → Balance/Imbalance → Initiative/Responsive → Day Type → Structural References → Profile Shape) applied on every market structure read
2. **Smashelito's analytical patterns** — three-timeframe state tracking (OTFU/OTFD/BALANCE with duration and invalidation levels), acceptance/rejection framing for conditional scenarios, profile shape reads as positioning (not forecasts), and "unfinished business" tracking
3. **Structural improvements** — "Always do this first" checklist, explicit `skills/trading-domain/SKILL.md` reference, `dataAgeMs` staleness threshold (30s), working method, output format template, compliance framing rules, and "When uncertain" guidance
4. **MCP descriptor JSONs** — added 5 missing `.json` files for research tools (`query_event_frequency`, `query_conditional`, `query_distribution`, `get_session_history`, `get_research_summary`) and updated `compare_sessions.json` from "reserved for analytics phase" to its actual functionality

**Alternatives considered:**
- Keep the agent minimal and rely on the model's implicit knowledge of Market Profile — rejected (inconsistent quality, no guaranteed workflow, no output standardization)
- Create a separate "Dalton knowledge base" document and reference it — rejected (slower to load, adds indirection; embedding directly in the agent definition is more reliable)
- Add only structural improvements without domain knowledge — rejected (the analytical framework is the highest-value improvement; structure without substance doesn't improve the read quality)

**Consequences:**
- Agent file grew from 36 lines to 99 lines — larger context window cost per invocation, but well within limits
- The agent now has an opinionated analytical framework that may not match all Market Profile practitioners' approaches — mitigated by grounding in Dalton (the foundational source) and using compliance framing that prevents overcommitment
- Research tools are now discoverable via MCP descriptor files
- Future: `query_conditional` outcome fields should be expanded to include `profile_shape`, `balance_state`, `poor_high`/`poor_low`, `excess_high`/`excess_low` to fully support the agent's analytical needs

---

### ADR-P06: NQ contract rollover handling

**Date:** 2026-03-19
**Status:** Decided

**Context:** Sierra Chart stores quarterly contracts as distinct `.scid` and `.depth` files, while The Desk previously dropped contract identity after ingestion. That made live roll week behavior ambiguous and allowed prior-day references to leak across contracts.

**Decision:** The Desk now uses a hybrid rollover model:
- `base_symbol` defines the instrument family (for example `NQ`)
- `symbol_mode` controls how the active contract is resolved: `manual`, `auto`, or `hybrid`
- `active_symbol_override` pins a contract when the trader wants explicit control
- resolved contract metadata is propagated through live snapshots, feed health, historical session summaries, raw ticks, and signal outcomes
- prior-day carry-forward levels are stored by `(date, root_symbol, contract_symbol)` and only loaded into deterministic pipelines when same-contract references are authoritative
- `get_contract_rollover_status` / `validate_contract_rollover` expose whether prior references are authoritative, legacy-only, or unavailable before session start
- research can filter by `contractSymbol` or `rootSymbol`, while `get_session_history` also surfaces rollover boundaries

**Consequences:**
- roll week is safer because MCP tools now expose the active contract and warning state directly
- historical storage keeps per-contract truth for newly ingested data and V22 migrates prior-day references away from a date-only key
- operators can verify the resolver state with `get_feed_health`, `get_contract_rollover_status`, or `validate_contract_rollover` before trusting prior-session references
- rules and pipeline consumers do not receive non-authoritative prior-day levels after a roll
- research continuity across contracts is now explicit instead of silently implicit

### ADR-013: Databento as preferred Phase 2 options data provider

**Date:** 2026-03-05
**Status:** Decided
**Resolves:** ADR-P08 (Options data provider selection)

**Context:** Phase 2 requires options/gamma data for NQ trading: GEX by strike, dealer positioning, charm/vanna flow. Multiple providers were evaluated (Gexbot, Unusual Whales, CBOE, OptionData.io, ConvexValue).

**Decision:** Databento is the preferred options data provider. We will compute all Greeks (delta, gamma, charm, vanna) and GEX ourselves from raw options chains. Databento provides:
- **OPRA** for NDX, SPX, SPY, QQQ (1.6M+ equity options)
- **CME Globex** for NQ futures options (650k+ symbols)
- Raw tick data, order book, OI, reference data — no pre-computed IV or Greeks
- Official Rust client library, strong docs, self-service
- Usage-based historical ($0.04/GB OPRA, $0.50/GB CME) or subscription (~$199/mo unlimited live)

**Alternatives considered:**
- Unusual Whales — pre-computed GEX; faster path but less control over model
- CBOE raw — NDX only; no NQ futures options
- Gexbot — API underdocumented; chart-first; NQ/NDX not clearly primary
- OptionData.io — real-time WebSocket; higher cost (~$599/mo)
- ConvexValue — pre-computed gamma, gxoi, gxvolm; evaluate if Databento build proves too heavy

**Consequences:** We build a GEX/Greeks pipeline in Rust (Black-76 or similar for index/futures options). More engineering upfront, but full control over model assumptions and robustness. See `docs/phase-2-options-databento-memo.md` for architecture sketch.

---

### ADR-014: MCP tools use try_lock for pipeline access to avoid stalls

**Date:** 2026-03-05
**Status:** Decided

**Context:** Several MCP tools (`get_tape_pace`, `get_rebid_reoffer_zones`, `get_pinch_events`, `get_rvol`, and others) were stalling and returning "Aborted" when called. Investigation showed that tools that access the pipeline engine use `pipelines.lock()`, which blocks when the lock is held by:
1. **Startup backfill** — processes millions of ticks from 2 Globex opens ago while holding the lock
2. **Live poll loop** — processes new .scid ticks in batches; holds lock per tick
3. **Depth worker** — persists DOM snapshots

When a tool blocks on `lock()`, the MCP server cannot process other requests (stdio transport, single-threaded handler). The client times out and aborts all pending calls.

**Decision:** Tools that have a DB fallback use `try_lock()` instead of `lock()`. If the pipeline is busy, they immediately fall through to the persisted snapshot in the database. Affected tools: `get_tape_pace`, `get_rebid_reoffer_zones`, `get_pinch_events`, `get_footprint`, `get_footprint_window`, `get_tpo_detail`, `get_imbalances`, `get_absorption_events`, `get_trade_size_profile`, `get_session_inventory`, `get_delta_at_price`, `check_delta_confirmation`, `live_snapshot`, `evaluate_playbook`.

**Alternatives considered:**
- Release pipeline lock more frequently during backfill — rejected (complex, risks inconsistent state)
- Run tool handlers on a thread pool — rejected (stdio MCP processes requests sequentially; would require transport changes)
- Increase client timeout — rejected (masks the problem; tools could still block for minutes during heavy backfill)

**Consequences:** When the pipeline is busy, tools return DB-backed data (slightly staler, may lack live-only fields like dwell time, zone details). This is acceptable; the alternative was indefinite stalls. `validate_data_integrity` still uses `lock()` since it requires live pipeline state and is rarely called.

---

### ADR-P08: Options data provider selection (resolved)

**Impact:** Phase 2 options pipeline (gamma, charm, dealer positioning)
**Status:** Resolved by ADR-013

---

### ADR-P09: Multi-session day handling

**Impact:** Risk tracking, session boundaries
**Owner:** _TBD_

**Question:** The trader often trades the London open, takes a break, then trades RTH. How should sessions and risk tracking handle this?

**Candidates:**
- Each sit-down is a separate session with independent risk tracking
- Each sit-down is a separate session but risk carries across sessions for the same calendar day
- One continuous session with pauses

---

### ADR-015: Local storage tiers and maintenance command

**Date:** 2026-04-26
**Status:** Superseded by ADR-021
**Context:** The Desk's runtime SQLite database grew large enough to pressure the primary `C:` drive. Source code and build artifacts were not the main issue; the dominant storage was `~/.the-desk/data.db` and its WAL, while Sierra Chart `.scid` data already lived on the trading/data drive.

**Decision:** Keep all data local, but separate runtime state, cold archives, build cache, and maintenance temp space on the larger local trading/data drive. The recommended Windows layout is:

```text
T:\TheDesk\
  state\        # data.db, config.toml, WAL/SHM files
  archive\      # zstd-compressed cold raw-tick archives
  backups\      # database snapshots
  build-cache\  # optional Cargo target dir
  temp\         # SQLite temp files during maintenance
```

The existing `~/.the-desk` path may be preserved with a Windows directory junction pointing to `T:\TheDesk\state`, so binaries and MCP config do not need a database-path migration. Storage configuration lives in `~/.the-desk/config.toml`:

```toml
[storage]
warm_retention_days = 30
cold_archive_dir = "T:\\TheDesk\\archive"
auto_archive = true
```

Add `the-desk-storage` as the operator-facing maintenance binary for local storage:

- `--status` reports raw tick coverage, archive cutoff, warm/cold config, and SQLite page usage.
- `--archive` streams old raw ticks into compressed `.csv.zst` archive files and deletes only after each archive is written and row-count checked.
- `--vacuum` attempts physical SQLite compaction after archival and forces SQLite temp files onto the data drive.

**Consequences:**
- C: is protected from runtime database growth.
- Old raw ticks can be moved out of SQLite while preserving session summaries, market events, signal outcomes, journal/risk records, and research metadata.
- Full SQLite compaction remains an explicit outside-market-hours operation because large `VACUUM` runs can take hours and temporarily require substantial free space.
- The maintenance command is local-only and does not change the core architecture: Sierra `.scid` remains the canonical raw market-data source, deterministic Rust pipelines remain Layer 1, and MCP tools continue to expose structured data only.

> **Supersession note:** Production later moved cold archives and backups to `X:\TheDesk\` (USB archive drive) and added depth/snapshot retention plus scheduled-task orchestration. See ADR-021.
---

### ADR-016: MCP runtime observability is structured, bounded, and queryable

**Date:** 2026-04-30
**Status:** Decided

**Context:** The MCP server had ad-hoc stderr diagnostics for SCID tailing, startup replay, session boundaries, historical jobs, depth polling, and setup lifecycle changes. Those messages were hard to filter during post-mortems and not directly available to agents.

**Decision:** Runtime observability uses three coordinated surfaces:

1. Structured JSON runtime events emitted to stderr and/or daily log files, with stdout reserved exclusively for MCP protocol traffic.
2. A bounded in-memory runtime event buffer with per-event-name suppression to keep flapping errors from evicting the original cause too quickly.
3. A persisted `runtime_events` SQLite table exposed through `get_runtime_events` for agent-readable post-mortems.

Runtime event persistence is insert-only at emit sites. Retention pruning runs at startup and on a periodic background timer, not during hot feed processing. File logging uses daily rotation and startup-time retention cleanup. Logging initialization is non-fatal: if file logging cannot be initialized, the server falls back to stderr or disables tracing while continuing to serve MCP.

**Alternatives considered:**
- Continue using `eprintln!` strings — rejected because agents and post-mortems need stable event names and fields.
- Persist every runtime event and prune per insert — rejected because it adds redundant SQLite deletes near live processing.
- Send logs to stdout — rejected because MCP stdio owns stdout and non-protocol bytes can corrupt the client connection.

**Consequences:** Operators can query recent runtime issues with `get_runtime_events` and filter by `level`, `minLevel`, `category`, or `eventName`. JSON log payloads expose flattened fields for tools like `jq`, Loki, or Datadog. Event emission must remain low-noise and must not log raw tick streams.

---

### ADR-017: Context frames use stable buckets and weighted analogs

**Date:** 2026-05-01
**Status:** Decided

**Context:** Raw MCP snapshots are precise but not always decision-useful for an agent. A statement like "price is 18 points above VWAP" needs session-relative interpretation, historical sample-size caveats, and rollover-safe scope before it can become useful coaching context.

**Decision:** Add a deterministic context-framing layer in Rust research infrastructure, exposed by `get_context_frame`. The v1 envelope includes `live`, `buckets`, `intradayForwardStats`, `historicalAnalogs`, optional setup-linked `setupOutcomes`, `caveats`, and `meta`. Bucket definitions are versioned as `context-v1`, blessed on 2026-05-01, and include VWAP-sigma, RVOL, time-of-day, IB state, value-area location, DNVA location, day type, profile shape, balance state, and session scope. Historical matching defaults to weighted analogs, not strict bucket equality, with strict matching reserved for diagnostics.

Initial similarity weights are day type 0.30, profile shape 0.20, VWAP-sigma bucket 0.15, RVOL bucket 0.15, IB state/range bucket 0.10, and single-prints direction 0.10. Weighted analog matching uses a 0.35 distance threshold, then falls back to the nearest 30 analogs when the threshold set is below the reportable sample threshold (`N >= 30`). Rollover-sensitive historical comparisons use same-contract scope when available and suppress or caveat level-derived context when symbol scope is ambiguous. Intraday forward-path stats rely on `pipeline_snapshots` plus end-of-session summaries; snapshots are persisted at a bounded 60-second cadence during live ingest and historical backfill, plus session-final snapshots. Pipeline snapshots denormalize context bucket columns at insert time and use indexed SQL narrowing before JSON payload materialization. The v1 research scan caps are 100,000 session summaries and 200,000 intraday snapshots; these are MVP guardrails and may still be replaced by materialized per-bucket outcome summaries if historical scale grows.

**Alternatives considered:**
- Strict exact-bucket matching — rejected because VWAP/RVOL/time/day-type buckets create too many sparse cells for the available history.
- LLM-generated interpretation inside Rust — rejected because Layers 1/2/2.5 must stay deterministic and network-free.
- Fold context directly into every snapshot only — rejected for v1 so raw tools remain lean and agents can opt into richer framing.

**Consequences:** Agents get prompt-ready context with explicit reliability tiers, sample sizes, bucket provenance, cache status, and caveats. Bucket changes must bump `bucketDefinitionVersion` and record a new decision-log note. Context frames are coaching context only: agents must phrase them as playbook/statistical framing, not advice or trade instructions. Pipeline/DOM snapshot retention is configured under `[storage]` (`pipeline_snapshot_retention_days`, `dom_snapshot_retention_days`, `dom_feature_snapshot_retention_days`) and applied by `the-desk-storage --maintain` (see ADR-021).

---

### ADR-018: IDEA-011 uses first-class IB extension state, not poor-high/low

**Date:** 2026-05-04
**Status:** Decided

**Context:** The next regime/backtest path is IDEA-011, which tests one-sided IB extension acceptance. Poor-high and poor-low flags are known instrumentation caveats, but they are not required to classify IDEA-011 and would expand the scope into a separate TPO definition pass.

**Decision:** Add deterministic session-level IB extension fields to `session_summaries`: `ib_extension_state` (`None`, `UpOnly`, `DownOnly`, `BothSides`), `first_ib_extension_direction`, and `first_ib_extension_timestamp_ms`. The state uses the existing 0.5x IB extension contract and is enriched from `ib_extension_hit` event metadata (`extensionDirection: "up" | "down"`) in both historical backfill and live RTH close finalization.

**Alternatives considered:**
- Repair poor-high/poor-low before IDEA-011 — rejected because it is not on the immediate backtest dependency path.
- Infer one-sided extension only from event counts — rejected because live/legacy summaries benefit from a range-derived fallback when event rows are missing.

**Consequences:** IDEA-011 can filter sessions directly by queryable regime fields without depending on sparse poor-high/poor-low flags. Poor-high/poor-low remain explicitly deferred until a dedicated TPO semantics pass defines and validates their exact rule.

### ADR-019: Live SCID ingest splits into a deterministic hot path and a coalesced analysis pass

**Date:** 2026-06-22
**Status:** Decided

**Context:** At the 09:30 ET Globex→RTH open, SCID-derived pipeline state (VWAP, OR5, delta, structure) froze while DOM stayed live and the `.scid` file kept growing — a silent hot-path backlog, not a Sierra outage. Root cause: the live poll loop did all per-tick work on one thread via `process_tick` — pipeline update, event detection, rules/setup evaluation, an `outcome_tracker::on_tick` SQLite query *every tick*, attention persistence, and occasional historical `warm_context_frame_cache` reads. During the open burst the processing rate fell below the arrival rate. Two amplifiers: `read_bulk_from_offset` drained tail→EOF uncapped, and the loop slept unconditionally at the top of every iteration even when behind, so lag could not self-correct. DOM stayed live only because depth polling is a separate task. `prepare_for_new_session` added two more hot-path SQLite reads at the boundary tick.

**Decision:** Split the live tick path into an ingest-only hot path (`ingest_tick`) and a throttled analysis pass (`run_analysis_pass`):
- `ingest_tick` performs deterministic state only (pipeline, event detection, per-tick outcome excursion apply) with no SQLite work.
- `run_analysis_pass` runs the rules engine, outcome DB flush, and attention persistence on `spawn_blocking`, coalesced to at most once per `analysis_min_interval_ms` (250 ms) or `analysis_max_ticks` (500), and always forced at batch end and on session boundaries.
- Outcome MFE/MAE and chronological target/stop resolution are preserved exactly via an in-memory `PendingOutcomeSet` (per-tick CPU apply, DB writes once per pass) rather than carrying only high/low.
- The live reader is capped (`read_bulk_from_offset_capped`, `max_ticks_per_poll`=5000) to bound one poll iteration; the loop yields instead of sleeping while behind so lag self-corrects.
- Boundary SQLite reads move off the hot path: an in-memory reset runs inline, prior-day/inventory references are served from a pre-warmed `BoundarySessionCache` (refreshed by a 60 s background task), with a cold-cache fallback that logs `session.boundary_cache_cold` and reads inline.
- New observability: distinct read-vs-processed offsets, worker-phase labels, batch tick count/process time, analysis lag, and a stall watchdog that warns when the file grows but the processed offset does not advance.

New `FeedConfig` fields (all serde-default): `max_ticks_per_poll` (5000), `analysis_min_interval_ms` (250), `analysis_max_ticks` (500).

**Alternatives considered:**
- Keep the single-threaded `process_tick` and only cap the reader — rejected because the per-tick SQLite query remained on the hot path and would still backlog at the open.
- Carry only running high/low for outcome resolution instead of a pending set — rejected because it cannot reproduce the exact first-crossing target/stop semantics the DB tracker guarantees.

**Consequences:** Rule and setup *firing* is now sampled (≤250 ms / 500 ticks) rather than evaluated on every tick — an accepted 100–250 ms alert-coalescing tradeoff for discretionary coaching. Outcome excursion accuracy stays per-tick exact. Parity tests assert capped == uncapped final pipeline state and coalesced == per-tick outcome extremes. `process_tick` is retained for tests and replay utilities but is no longer on the live path.

---

## Pending

### ADR-020: Social intelligence as an isolated Layer-3 feature track

**Date:** 2026-06-30
**Status:** Pending
**Related:** [social-intelligence-roadmap.md](social-intelligence-roadmap.md), [social-confluence-design.md](social-confluence-design.md) (Phase A spec), [setup-ideas-and-backtesting.md](setup-ideas-and-backtesting.md) IDEA-023, https://docs.x.com/tools/mcp

**Context:** The trader wants trusted X accounts to inform live confluence checks, surface backtesting hypotheses, provide real-time context from voices they respect, and prompt subagents with externally sourced edge situations — while The Desk stays data-based and the deterministic core stays clean. X now exposes a hosted MCP server with read-only post/timeline/search access. Broader vision (continual learning via memory + research, external idea queue, subagent-scoped calibration) is documented in the roadmap; v1 is account confluence only. Two questions must be answered before building: (1) how this fits The Desk's strict layer separation, and (2) the X API access mode and cost, given X moved to pay-per-use (~$0.005/read, 2M/mo cap, then Enterprise $42k+/mo) with legacy Basic/Pro closed to new signups.

**Decision (proposed, not yet committed):**

1. **Phase A (v1):** Build **account confluence** as a new isolated `src/social/` module operating at Layer 3 only. Rust fetches + caches posts in a background task into a `social_posts` table; the agent synthesizes the lean (no Claude API from Rust); a read-only `get_account_confluence` MCP tool returns structured data. It never fires a playbook alert and never touches `pipelines/` or `rules/`. Feature-flagged with graceful degradation.

2. **Phases B–D (follow-on, same ADR track):** Event logging when confluence is checked; research conditionals (`social_alignment` × structure × outcomes); memory promotion (`social_confluence`, `account_calibration`, `external_hypothesis` insight categories). Subagent "learning" is **system learning** (SQLite memory + research), not neural weight updates.

3. **External idea queue:** Third-party setup ideas enter a trader-gated `external_ideas` queue → promoted to IDEA entries → backtested like internal hypotheses. Subagents prompt exploration; market data validates edge.

4. **Deferred:** Open-firehose sentiment indicator; if pursued later, compute only over the curated watchlist (reusing cache), not the open platform.

**Open items blocking "Decided":**
- Access mode: read-only Bearer token vs OAuth 2.0 — **trader undecided**; cost ceiling TBD.
- Watchlist contents and poll cadence (RTH-only vs 24h).
- Whether a curated-list sentiment score ships in v1 or confluence-context only.
- Idea extraction cadence (on poll vs on-demand) and which agent owns the hypothesis queue.

**Alternatives considered:**
- Put social data through the rules engine as a condition field — rejected (violates Rule #3: alerts must trace to the trader's own playbook).
- RL / fine-tuning subagents on Twitter data — rejected for v1 (subagents are prompt frameworks; learning belongs in memory/research layers; compliance risk).
- Open-platform full-archive sentiment index in v1 — rejected for now (read-cap/Enterprise cost, low signal quality: bots, sarcasm, sampling bias).
- Separate repo/service — deferred; co-locating lets the agent pull market structure + social context in one conversation, provided isolation is strict.

**Consequences:** A new optional network dependency enters the codebase, quarantined to Layer 3 behind a feature flag. Until ADR-020 is marked Decided, no live-credential wiring lands. [social-confluence-design.md](social-confluence-design.md) is the Phase A build spec; [social-intelligence-roadmap.md](social-intelligence-roadmap.md) is the working feature track for weeks/months ahead.

---

### ADR-021: T:/X: storage layout, retention, task orchestration, and backup safety

**Date:** 2026-07-11
**Status:** Decided
**Supersedes:** ADR-015 (layout portion)

**Context:** After the one-time depth reclaim, production outgrew the all-on-`T:` layout in ADR-015. Cold raw-tick archives and full-DB backups must not compete with Sierra recording headroom on `T:`. Weekend Task Scheduler automation was never registered under `\TheDesk\`, so archival never ran. Snapshot tables (`pipeline_snapshots`, `dom_snapshots`, `dom_feature_snapshots`) had no retention. Existing `desk-*.db` backups on `X:` were found with **zeroed page 0** (SQLite magic missing) despite non-zero later pages — unusable as restore points. MCP-deferred weekly archive scripts previously exited `0`, making skips indistinguishable from success.

**Decision:**

Canonical production layout:

```text
T:\TheDesk\state\data.db     # hot SQLite (junction from %USERPROFILE%\.the-desk)
T:\SierraChart\Data          # authoritative .scid / MarketDepthData .depth
X:\TheDesk\archive           # cold raw_ticks *.csv.zst
X:\TheDesk\backups           # verified VACUUM INTO snapshots
X:\TheDesk\temp              # SQLite temp during maintenance
X:\TheDesk\logs              # ops logs + maintenance manifests
```

Retention (config `[storage]`):
- `warm_retention_days = 30` for `raw_ticks` (archive then delete)
- `depth_retention_days = 7` for `depth_events` (`.depth` remains durable source)
- `pipeline_snapshot_retention_days = 14`
- `dom_snapshot_retention_days = 7`
- `dom_feature_snapshot_retention_days = 7`
- `auto_archive` remains vestigial; **scheduled tasks** run `the-desk-storage --maintain`

Weekend cadence (machine-local wall-clock; this workstation is Central Time):
1. Friday — Sierra close, then Data Readiness (gap/status manifest; catch-up is operator-gated)
2. Saturday — Storage Maintenance (SYSTEM; hourly retries; exit **2** when MCP writer active)
3. Sunday — Pre-Open Readiness + Sierra open
4. Ongoing — disk/health checks

Backup safety:
- Verify SQLite header magic **and** `PRAGMA quick_check` **and** durable-table presence
- Open snapshots read-only / immutable (never write-open a backup)
- Never prune the newest header-valid backup
- Default `min_interval_hours = 24`; backup directory must be an absolute path on `X:` (Rust does not expand `~`)

Backtest isolation:
- Heavy `run_backtest` requires `--database-mode backtest` (isolated DB) or explicit `allowLiveDatabase`
- `--seed-backtest-db` refuses overwrite unless `--force` and records provenance

**Consequences:**
- Operators must register `\TheDesk\` tasks elevated and verify with `-Verify`
- Deferred maintenance is observable (exit 2 + JSON markers)
- Corrupt zero-header backups must be replaced with a freshly verified snapshot before destructive prune/compact
- Snapshot prune reduces DOM research hot window to configured days; `.depth` / `.scid` remain rebuild sources for market data
- Durable trader state (setups, hypotheses, journal, risk, memory, account) still requires verified full-DB backups — not recoverable from Sierra files alone

> **Implementation note (2026-07-22, commit `f412f3d`):** verified full backups and reclaim-swap
> compaction are now distinct CLI operations. `the-desk-storage --backup` runs the standard
> verifier (header magic + `quick_check` + durable tables via `backup::perform_backup`) and
> **retains unarchived history** — extra old rows in a restore point are the safe direction.
> `--compact-into` keeps the reclaim-only cutoff assertion (`verify_reclaim_retention`): a
> reclaim copy still holding pre-cutoff `raw_ticks` fails exit 4 because the archive step was
> skipped. The split resolved a same-day incident where a fully verified backup exited 4 solely
> because weekly archival had never run.

---

### Decision note: SIL-M0 specialty market tool freeze + orientation telemetry

**Date:** 2026-08-11
**Status:** Decided (implements [alecwardd/the-desk#3](https://github.com/alecwardd/the-desk/issues/3); Part of #2)

**Context:** Before Catalog v0 and the read-kernel shims land, the specialty market tool surface must stop growing, and later deprecation decisions need a measurable orientation-chain baseline rather than estimates.

**Decision:**

1. **Flat freeze:** no new specialty market tools (`tools/market.rs` / `market_router`) until Desk Catalog v0 exists. After Catalog v0, the rule becomes **no catalog entry → no new market tool**. Existing tools are not deleted under this policy. Enforced by `specialty_market_tools_are_frozen_until_catalog_v0`.
2. **Telemetry:** every MCP `tools/call` records per-tool call counts and approximate response token cost (`ceil(bytes/4)`), plus orientation-chain cost for the documented session-start sequence. Structural baseline: `docs/mcp/sil-m0-tool-telemetry-baseline.json`. Runtime counters flush to `~/.the-desk/telemetry/tool-call-snapshot.json`.

This note does not raise the Trust Ceiling and does not introduce Catalog v0.

**Consequences:** M1b shim work can compare before/after orientation cost against the SIL-M0 baseline. Adding a market tool without Catalog v0 fails CI.

---

### Decision note: SIL-M1b get_state / get_events + orientation shims

**Date:** 2026-08-12
**Status:** Decided (implements [alecwardd/the-desk#5](https://github.com/alecwardd/the-desk/issues/5); Part of #2)

**Context:** Catalog v0 and the M0 telemetry baseline are in place. Agents still orient via specialty getters; the read kernel needs provenance-carrying envelopes before engine extract / Journal Frames.

**Decision:**

1. **Read kernel behind `[sil].catalog_discovery`:** `get_state` (R0|R1 only) returns a **StateEnvelope** with per-domain `provenance` and `degraded` maps; absence of provenance is a failure. Degraded domains set their flag and remain in provenance (never silently omitted). `get_events` returns identity rows (`eventType`, `timestampMs`, severity placeholder, `identityId`) without formalizing lifecycle (#9).
2. **Trust Level L0:** kernel read/query operators are tagged Trust Level L0 and cannot carry mutation or order authority (router/capability tests). Trust Ceiling stays L3.
3. **Orientation shims:** when the SIL flag is on, specialty orientation getters `get_session_context` and `get_market_snapshot` still answer and include `deprecated: true` + `suggestedReplacementOperator: "get_state"`. Flag off → pre-M1b responses unchanged. Opinionated bundles (`get_context_frame`, `get_attention_inbox`, `evaluate_playbook`) remain.
4. **M0 baseline immutable:** do not re-bless `docs/mcp/sil-m0-tool-telemetry-baseline.json`; shim deltas are attributable against that frozen before-figure.

**Consequences:** Agents can migrate orientation to `get_state` from inside shim responses. Positioning remains a fail-closed stub domain inside every envelope until a provider lands.

---

### Decision note: SIL-M2a engine extract + embedded fallback + SourceProvider stub

**Date:** 2026-08-12
**Status:** Decided (implements [alecwardd/the-desk#6](https://github.com/alecwardd/the-desk/issues/6); Part of #2)

**Context:** After M1b, intelligence still dies when the MCP/agent session ends because ingest, pipelines, and event detection live inside the stdio-bound MCP process. Contended pipeline mutexes remain a known stdio-stall failure mode (ADR-014). ACSIL is later and must not block the file-spine path.

**Decision:**

1. **`the-desk-engine` host:** headless binary owns ingest, pipelines, and event detection. Lifecycle expectation is Task Scheduler on Sierra hours with **Globex overnight coverage** — the engine runs whenever Sierra records. Closing an MCP/agent session must not stop ingest.
2. **Embedded-engine fallback:** `[sil].engine_mode = "embedded"` (default) keeps today's MCP-owns-ingest topology as a true rollback. `"external"` makes MCP a thin adapter over a read-only localhost state socket (`[sil].engine_bind`, default `127.0.0.1:17843`).
3. **Published state:** engine publishes coaching snapshots behind a lock-free swap (`arc_swap`); socket readers never take the pipeline mutex. Kill-the-engine must degrade cleanly (explicit degraded published state + adapter error) and recover on reconnect — never silent stale success.
4. **`SourceProvider` seam:** `FileProvider` covers `.scid`/`.depth`; `SierraProvider` is stubbed for ACSIL (#23). MarketRouter NQ+ES (#7), Journal Frames / Capsules, and Feature-IR stay out of scope.
5. **Observability:** ingest gaps (`get_raw_tick_ingest_gaps`), feed health (`get_feed_health` + engine stall/behind fields), and runtime events (`engine.adapter_degraded` / `engine.adapter_recovered`) remain the ops/agent surface.
6. **Trust Ceiling stays L3:** no path from this work reaches order placement; read/query kernel operators remain Trust Level L0.

**Consequences:** Continuity becomes a property of the engine process. Embedded mode preserves the live coaching path without requiring an external engine. Socket/embedded coaching-path parity and kill/recover are enforced by process + in-crate tests.

---

### Decision note: SIL-M2b MarketRouter v0 — concurrent NQ + ES

**Date:** 2026-08-13
**Status:** Decided (implements [alecwardd/the-desk#7](https://github.com/alecwardd/the-desk/issues/7); Part of #2)

**Context:** After M2a the engine host is single-symbol. Cross-market predicates that are not co-recorded from the first row are historically unanswerable. Journal Frames (#8) must inherit an aligned NQ+ES clock.

**Decision:**

1. **MarketRouter v0** hosts exactly two roots — **NQ** and **ES** — each with an isolated `EngineHost` / `PipelineEngine`. MES/MNQ and other roots are rejected at the `RouterRoot` parse boundary.
2. **One clock:** ticks from both FileProviders are merge-sorted by `(timestamp_ms, root)` (NQ before ES on a tie) and applied in that order. `clockMs` is the max applied market timestamp across lanes. Session classification (RTH / Globex) is per-tick on the owning lane — NQ Globex never contaminates ES RTH (and vice versa).
3. **StateEnvelope:** `get_state(symbols=["NQ","ES"])` returns both roots in one envelope. Values are keyed `{ROOT}.{catalogFieldId}`; symbol-scoped provenance/degraded are keyed `{ROOT}.{domain}`. Positioning / events / meta stay unprefixed. Single-symbol reads keep the M1b unprefixed shape. Trust Level stays **L0**; Trust Ceiling stays **L3**.
4. **Coaching-path parity:** published `market_state` remains the primary root (configured `base_symbol`, default NQ) so embedded-engine fallback and external-engine live coaching (`get_market_snapshot`, `evaluate_playbook`) do not regress SIL-M2a. A missing ES SCID degrades only the ES slice.
5. **Embedded-engine fallback:** MCP-alone still runs MarketRouter. NQ shares the live coaching pipelines; ES is an isolated FileProvider lane. External `the-desk-engine` uses MarketRouter for both roots.

This note does not introduce Journal Frames, Capsules, Feature-IR, Positioning providers, or ACSIL.

**Consequences:** Cross-market live `get_state` is one call. Later Journal Frames can persist both symbols on the same clock without a backfill gap for NQ↔ES conjunctions.

---

### Decision note: SIL-M3a Market State Journal — 1 Hz Journal Frames + event rows

**Date:** 2026-08-13
**Status:** Decided (implements [alecwardd/the-desk#8](https://github.com/alecwardd/the-desk/issues/8); Part of #2)

**Context:** After MarketRouter v0, NQ and ES share one clock. `get_state(as_of=…)` still read from `pipeline_snapshots` (research/context cadence) with a shim note that Journal Frames had not shipped. The ~250 ms analysis pass (ADR-019) must keep publishing for live coaching, but must not persist 4 Hz forever. MFE/MAE / R-result stay tick-driven on `PendingOutcomeSet`.

**Decision:**

1. **1 Hz Journal Frames** persist for **NQ** and **ES** on the shared **MarketRouter** clock. Each root is keyed by `floor(lane_market_time / 1000)` so a later print on the other root cannot copy last-known state onto a second that root did not print. Roots that print in the same second share the first pinned `clock_ms` of that second. Duplicate `(frame_second, root_symbol)` rows are ignored — a 250 ms publish loop cannot store 4 Hz frames. Capsules / 250 ms dumps are out of scope (#10).
2. **Transition event rows** are written when detectors fire and join to frames on `(journal_frame_second, root_symbol)` where `journal_frame_second = floor(event.timestamp_ms / 1000)` (the printing root's second, not the max clock). `get_events` remains identity rows (no open/updated/resolved — #9).
3. **`get_state(as_of=…)`** is served **only** from Journal Frames (provenance source = **Journal**). Live `get_state` without `as_of` stays the published/live path. Multi-symbol envelope shape from SIL-M2b is preserved. Per-domain provenance remains mandatory. `pipeline_snapshots` is not a silent dual source for `as_of`.
4. **MFE/MAE / R-result** remain tick-driven (ADR-019). The journal writer persists already-computed state and does not sample outcome extremes from frames.
5. **Embedded-engine fallback** still writes Journal Frames for both symbols (NQ shared coaching pipelines + ES lane). External `the-desk-engine` writes the same tables on the same clock. Journal snapshot must not hold the pending-frame mutex across pipeline locks (embedded NQ ingest and ES poll are concurrent). Trust Ceiling stays **L3**; read/query stays Trust Level **L0**.

This note does not introduce Capsules, the research query kernel, DuckDB, Positioning providers, Feature-IR, or ACSIL.

**Consequences:** Historical `as_of` reads are co-recorded NQ↔ES frames. Rebuild from `.scid`/`.depth` through MarketRouter reproduces frames within the existing golden strict/derived tolerance model.

---

### Decision note: SIL-M4 event lifecycle + attention view + DOM-family taxonomy

**Date:** 2026-08-13
**Status:** Decided (implements [alecwardd/the-desk#9](https://github.com/alecwardd/the-desk/issues/9); Part of #2)

**Context:** After Journal Frames, `get_events` still returned identity placeholders (`lifecycleFormalized=false`, severity `"unspecified"`). Attention (`SignalComposer` / `get_attention_inbox`) could disagree with event identity. Overnight completeness required events + attention to persist in the engine + SQLite while MCP/agent is disconnected. Capsules (#10) need an explicit DOM-family taxonomy to key off — without emitting Capsules here.

**Decision:**

1. **Lifecycle on the existing stream:** detector rows are formalized, not rebuilt. Canonical lifecycle is `open → updated → resolved|expired`. Each distinct occurrence is appended in SQLite (research frequency still counts rows). Repeats of the same **dedup identity** stamp the next lifecycle on the new row; `get_events` / `get_attention_inbox` collapse to latest-per-dedup so a persistent condition is not a stream of new Events. `_invalidated` types resolve; TTL on the MarketRouter clock expires any still-live (`open`/`updated`) occurrence whose event time is older than TTL (a fresh latest row is left live). Occurrence `identityId` is per row; **dedup identity** is the canonical condition key. Severity is always present (`low|normal|high|urgent|unspecified`). Schema v34 expires pre-lifecycle rows that v33 defaulted to `open`.
2. **`frame_ref`:** every `get_events` row carries `{ journalFrameSecond, rootSymbol }` using the SIL-M3a join (`floor(event.timestamp_ms / 1000)`, printing root). Keys are never silently omitted.
3. **Attention inbox is a ranked view** over that event stream (`viewOf: eventStream`). Event-linked signals inherit lifecycle from `get_events`; missing live events are synthesized into the view so SignalComposer cannot be a silent second source of truth. The inbox materializes those event-stream rows into SQLite so `get_signal_detail` / `acknowledge_attention_signal` can address the same ids. `acknowledge_attention_signal` remains a typed workflow mutation (not a Trust Ceiling change, not order authority). Setup/risk/absence overlays may follow event-stream rows. Status and minPriority filters apply after the ranked merge. A stale pagination cursor returns an empty page instead of replaying page 1.
4. **Overnight completeness:** `the-desk-engine` persist_journal writes lifecycle-stamped events and event-stream attention to SQLite with no MCP/agent attached. No push vendor and no standing LLM session. Cheap-model invocation is **event-triggered only** — the periodic absence pulse is deterministic, not a narrator.
5. **DOM-family taxonomy** (Capsule-mandatory later, not emitted here): `stop_run`, `iceberg_reload`, `pull_intent`, `book_velocity_regime_shift`. Reads stay on `get_events` / `get_attention_inbox` — no new specialty event getters. Trust Level **L0** on `get_events`; Trust Ceiling stays **L3**.

This note does not introduce Capsules / 250 ms dumps, Episode Query, DuckDB, Vs3dProvider, Feature-IR, or ACSIL.

**Consequences:** Agents can distinguish a new condition from one already discussed. Capsule policy (#10) keys off the named DOM-family types. Embedded-engine fallback and external engine remain behavior-parity for the live coaching path.

---

### Decision note: SIL-M3b Capsules mandatory for DOM-family events

**Date:** 2026-08-13
**Status:** Decided (implements [alecwardd/the-desk#10](https://github.com/alecwardd/the-desk/issues/10); Part of #2)

**Context:** After Journal Frames (1 Hz) and M4 lifecycle + DOM-family taxonomy, forensic sub-second DOM behavior had no persist path. A permanent 250 ms frame store would densify the forever journal. M4 named `stop_run`, `iceberg_reload`, `pull_intent`, `book_velocity_regime_shift` and set `requires_capsule` but did not emit Capsules. Detectors still do not emit those types (#22).

**Decision:**

1. **In-memory ~250 ms ring** per MarketRouter root (session scope per-lane). Depth covers 30 s lookback plus a small margin. Ring samples are never a SQLite/Parquet table. Compute/publish may stay ~250 ms (ADR-019); only 1 Hz Journal Frames stay in the forever journal.
2. **Capsule window** is ~30 s before → ~60 s after the triggering Event on the MarketRouter clock (defaults 30_000 / 60_000 / 250 ms). Lookback is copied from the ring at trigger. The after-window fills until `clock_ms >= event.timestamp_ms + 60s`. If the session/feed ends earlier, persist what exists as `incomplete` + `degraded` — never silently truncated as a full window. RTH and Globex are not mixed in one Capsule.
3. **Trigger policy:** open one Capsule per triggering occurrence (`lifecycle=open`) when `is_dom_family_event_type`. Repeats of the same dedup identity do not spawn unbounded Capsules. Non-DOM types (pinch, absorption, ib_extension, …) are out of scope here.
4. **Persistence / joins:** schema v35 `capsules` table keyed to `trigger_identity_id`, joinable to Journal Frames via `[start_frame_second, end_frame_second] × root_symbol`. `the-desk-engine` `persist_journal` writes Capsules with no MCP/agent attached. `get_events` (Trust Level L0) carries `capsuleRef` on every DOM-family row (nulls OK while pending). No `get_capsule` specialty tool. Trust Ceiling stays **L3**.
5. **Embedded-engine fallback** and external engine remain coaching-path parity. MFE/MAE stay tick-driven. Trader-memory markdown capsules are unrelated and unchanged.

This note does not introduce DOM cluster Base Detectors, Episode Query, DuckDB, cold Parquet, Vs3dProvider, Feature-IR, ACSIL, or a 250 ms forever-store.

**Consequences:** Sub-second DOM dumps exist around injected/future DOM-family Events without densifying the 1 Hz journal. Coaching reads stay on `get_events`.

---

### Decision note: SIL-M3c Research query kernel

**Date:** 2026-08-14
**Status:** Decided (implements [alecwardd/the-desk#11](https://github.com/alecwardd/the-desk/issues/11); Part of #2)

**Context:** Journal Frames (M3a / #8) persist 1 Hz NQ+ES state so historical conjunctions are answerable. Capsules (#10) are joinable forensic dumps around DOM-family Events. Hypothesis / research evidence still could not cite journal-backed windows, and the flagship Episode Query (ES near a Positioning level ∧ extreme seller aggression ∧ poor auction efficiency ∧ replenishing bids ∧ NQ non-confirmation → tick-driven MFE/MAE) was not expressible on the MCP surface.

**Decision:**

1. **SIL read-kernel operators** `query_series`, `query_episodes`, `query_raw`, and `run_job` register behind `[sil].catalog_discovery` (same gate as `get_events`). Default-off keeps the 122-tool surface unchanged. Trust Level **L0**: structurally no mutation of workflow-verb state and no order authority (`run_job` is not classified as a `run_*` mutation). Trust Ceiling stays **L3**.
2. **`query_episodes`** is conjunctive over Desk Catalog fields (and optional event types) across co-recorded NQ+ES Journal Frames. The flagship five-predicate query is expressible end-to-end. Missing detector math (`domSummary.bidReplenishing`) and missing vendor Positioning fail closed with provenance — no invented detectors, no pretend live grid. Positioning today is **Levels-Only Records** via `positioning_entry`. Forward returns reuse tick-driven MFE/MAE (`outcomes::signed_excursion` / `raw_ticks`) — not a fill simulator. Capsules are joinable and are **not** required for the flagship query to be expressible.
3. **`query_series`** (R2) and **`query_raw`** (R3) require `startMs` and `endMs`. Unbounded, inverted, non-finite, and oversized windows are rejected (named caps; not `config.toml` knobs). Mixed RTH+Globex without explicit `sessionType` is rejected. Every result includes sample-size `N` and `reliabilityTier` (AGENT.md Research Sample Size Policy). Truncation / unavailability sets `degraded` — a full window is never advertised as clean when it was capped.
4. **`run_job`** persists a job id (schema v37 `research_query_jobs`) and returns an artifact handle (columnar CSV path + summary JSON), never the full row set as tokens. The MCP tool inserts `queued` on the writer, computes on the read pool via `execute_research_job` (SQLite reads + filesystem write, no upsert), then persists `completed`/`error`. The MCP call still **awaits** so the returned handle is populated. A separate poll operator is out of scope for M3c (would be a tenth kernel operator). Hypothesis evidence joins look up at most 50 exact `(root, frame_second)` keys (`list_journal_frames_at_seconds`) — they do not range-scan Journal Frames.
5. **Hypothesis evidence** (`summarize_hypothesis_run`) cites journal-backed windows via `frame_ref`.

This note does not introduce DuckDB / Episode Query benchmarks (#13), cold session-partitioned Parquet (#12), stopping `depth_events` as hot store (#14), DOM cluster Base Detectors (#22), Feature-IR (#18–#21), Vs3dProvider (#16), ACSIL (#23), `get_capsule`, a 250 ms forever-store, or raising the Trust Ceiling.

**Consequences:** Agents can express the flagship Episode Query against Journal Frames and cite `N` + reliability. Bulk research rides artifact handles. Live market tools still return "no data" in cloud without a Sierra `.scid` feed.

---

### Decision note: SIL-P-VS-a Levels-Only Record path

**Date:** 2026-08-13
**Status:** Decided (implements [alecwardd/the-desk#15](https://github.com/alecwardd/the-desk/issues/15); Part of #2)

**Context:** Catalog v0 named four Positioning record kinds, including Levels-Only Record, but `get_state` Positioning was a fail-closed stub (`source=provider`, always degraded, no values). Teaching exemplars (#17) pointed at a durable write verb that did not exist. Vs3dProvider / ToS capture is later (#16). Manual entry is the ToS-denial steady state and the historical backlog path — not a degraded mode.

**Decision:**

1. **Typed write verb `positioning_entry`:** accepts **Levels-Only Records** into Positioning using the same schema a later capture adapter will write (`recordKind`, `completeness`, `capturedAt`, `asOf`, `dataTime`, `derivedLevels`, provenance). Slice / grid / by-strike writes are rejected here. Vendor stamps (`dataTime`, VolSignals) are rejected so a manual card is never presented as live vendor data.
2. **First-class completeness:** `completeness: levels_only` is a Catalog field and a StateEnvelope value. It is not a fallback, partial, or second-class kind. A fresh same-day Levels-Only Record is `degraded=false`.
3. **Reads ride `get_state`:** no specialty Positioning getter tools. Positioning stays **unprefixed** in multi-symbol envelopes. Provenance source is **`manual`**; explicit as-of lives in `positioning.asOf`. `provenance.dataTime` stays null on this path (that wire name is vendor data time). Missing or mismatched trading-day freshness (live *or* `get_state(as_of=…)` vs the requested day) sets `freshnessOk=false` and `degraded=true` without omitting the domain and without `vendor` / `provider` pretence. A supplied `tradingDay` must match `asOf`'s trading day.
4. **Persistence:** SQLite `positioning_records` (schema v32), written regardless of engine mode (embedded fallback and external engine keep coaching-path parity). Trust Ceiling stays **L3**. `get_state` / discovery stay Trust Level **L0**. `positioning_entry` mutates Positioning records only — not order authority.

This note does not introduce Vs3dProvider, Capsules, Feature-IR, ACSIL, Journal Frame writer changes, or event lifecycle.

**Consequences:** Agents can hand-enter dealer maps with no scrape and no ToS. Later capture writes the same rows. Coaching copy stays "your annotated sessions / your methodology say…".
