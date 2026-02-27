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

## Pending Decisions

### ADR-P06: NQ contract rollover handling

**Impact:** Symbol naming in `.scid` file paths, continuous data across quarters
**Owner:** _TBD_
**Deadline:** Before first live deployment near a rollover date

---

### ADR-P08: Options data provider selection

**Impact:** Phase 2 options pipeline (gamma, charm, dealer positioning)
**Owner:** _TBD_
**Deadline:** Before Phase 2 implementation begins
**Action needed:** Use `options-api-researcher` subagent to evaluate providers

---

### ADR-P09: Multi-session day handling

**Impact:** Risk tracking, session boundaries
**Owner:** _TBD_

**Question:** The trader often trades the London open, takes a break, then trades RTH. How should sessions and risk tracking handle this?

**Candidates:**
- Each sit-down is a separate session with independent risk tracking
- Each sit-down is a separate session but risk carries across sessions for the same calendar day
- One continuous session with pauses
