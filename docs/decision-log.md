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
**Status:** Decided — amended by ADR-021 (2026-07-05)
**Source:** CLAUDE.md, the-desk-vision.md, epic-brief.md

**Decision:** The Desk never places, modifies, or cancels orders. It never connects to a trading API for execution purposes. It is a coaching and discipline tool.

**Consequences:** Simplifies architecture (no order management), eliminates liability from execution errors, maintains clear regulatory positioning as a coaching tool rather than an investment advisory service.

**Amendment (ADR-021):** The execution ban stands unchanged. The "coaching-only / regulatory positioning" framing is superseded — The Desk is a private single-trader tool and its agents follow the Grounded Partnership doctrine (proactive, grounded trade proposals).

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
**Status:** Decided

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

**Consequences:** Agents get prompt-ready context with explicit reliability tiers, sample sizes, bucket provenance, cache status, and caveats. Bucket changes must bump `bucketDefinitionVersion` and record a new decision-log note. Context frames are coaching context only: agents must phrase them as playbook/statistical framing, not advice or trade instructions. Pipeline snapshot retention remains a follow-up storage policy decision; until then, snapshot growth is bounded by cadence but not automatically pruned.

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

### ADR-021: Grounded Partnership replaces non-advisory framing

**Date:** 2026-07-05
**Status:** Decided
**Amends:** ADR-007 (execution ban retained; compliance framing dropped)

**Context:** The agent surface enforced a public-product "non-advisory / coaching-only" boundary — forbidden-phrase lists ("never say you should buy/sell"), a compliance-research skill, orchestrator "never recommend" rules. The trader confirmed The Desk will never be a public tool and wants the opposite behavior: an agent that proactively proposes trade ideas with entries, stops, and targets, grounded in analyzed and backtested data. The map contradicted both CLAUDE.md rule #4 (opinions encouraged) and the owner's actual use.

**Decision:** Replace phrasing-level compliance policing with a grounding doctrine, canonically in `AGENT.md` "Grounded Partnership": (1) proposals cite evidence — playbook rules, structure/flow, or backtest stats; (2) every statistic carries `N` + reliability tier, full conviction only at `N >= 30` verified; (3) conflicts reported before any lean; (4) risk rules outrank ideas — hard stops binary; (5) data quality gates conviction; (6) the trader executes, and Layer 2 alerts fire only from trader-owned rules (agent ideas route through the hypothesis → backtest → draft-setup lifecycle). `skills/compliance-research/` archived to `docs/archive/`; `prompt-quality-evaluator` re-missioned as the grounding evaluator (policing both ungrounded conviction *and* over-hedged grounded reads); `commands/coaching-test.md` rewritten to test grounding.

**Alternatives considered:**
- Keep the non-advisory framing — rejected (contradicts the owner's intent and produces systematically hedged output).
- Edit only the orchestrator — rejected (evaluator, compliance skill, and coaching-test would still enforce the old doctrine, giving agents contradictory instructions).
- Delete prompt-quality-evaluator — rejected (grounding still needs a dedicated quality gate; the failure mode moved, it didn't disappear).

**Consequences:** Agents give conviction with evidence instead of hedged commentary. The risk spine (footers, circuit breakers, sample-size policy) is unchanged. No Rust or MCP tool behavior changed. If productization is ever revisited, this ADR and the archived skill are the starting point for re-drawing the boundary.

---

## Pending

### ADR-020: Social intelligence as an isolated Layer-3 feature track

**Date:** 2026-06-30
**Status:** Pending
**Related:** [social-intelligence-roadmap.md](social-intelligence-roadmap.md), [social-confluence-design.md](social-confluence-design.md) (Phase A spec), [setup-ideas/idea-023-social-intelligence.md](setup-ideas/idea-023-social-intelligence.md) (IDEA-023), https://docs.x.com/tools/mcp

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
