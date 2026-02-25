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

## Pending Decisions

These decisions need to be resolved before or during Phase 1 implementation.

### ADR-P01: DTC message flow quirks in Sierra Chart

**Source:** phase-1-prd.md Section 9, Question 1
**Impact:** DTC client development
**Owner:** _TBD_
**Deadline:** Before DTC client implementation

---

### ADR-P02: Trade direction classification between bid and ask

**Source:** phase-1-prd.md Section 9, Question 2
**Impact:** Delta calculation accuracy
**Owner:** _TBD_
**Deadline:** Before delta pipeline implementation

**Candidates:**
- Proximity-based: classify by nearest of bid/ask
- Last-trade-direction: inherit direction of previous trade
- Split: count as 0.5 buy + 0.5 sell

---

### ADR-P03: Sierra Chart CSV trade log format

**Source:** phase-1-prd.md Section 9, Question 3
**Impact:** Trade import feature (LOG-03)
**Owner:** _TBD_
**Deadline:** Before trade import implementation
**Action needed:** Obtain a sample CSV file from a real Sierra Chart trade log

---

### ADR-P04: Claude API latency benchmarking

**Source:** phase-1-prd.md Section 9, Question 4
**Impact:** LLM-05 (coaching prompt <2s)
**Owner:** _TBD_
**Deadline:** Early Phase 1 prototyping

---

### ADR-P05: Tauri 2.x Windows maturity assessment

**Source:** phase-1-prd.md Section 9, Question 5
**Impact:** Framework choice
**Owner:** _TBD_
**Deadline:** Before full Phase 1 implementation

---

### ADR-P06: NQ contract rollover handling

**Source:** phase-1-prd.md Section 9, Question 6
**Impact:** Continuous operation
**Owner:** _TBD_
**Deadline:** Before first live deployment near a rollover date

---

### ADR-P07: Replay library licensing

**Source:** phase-1-prd.md Section 9, Question 7
**Impact:** RPL-08 (curated session library)
**Owner:** _TBD_
**Deadline:** Before shipping curated replays

---

### ADR-P08: Options data provider selection

**Source:** phase-1-prd.md Section 9, Question 8; phase-2-prd.md Section 3.1
**Impact:** Phase 2 options pipeline
**Owner:** _TBD_
**Deadline:** Before Phase 2 implementation begins
**Action needed:** Use `options-api-researcher` subagent to evaluate providers

---

### ADR-P09: Multi-session day handling

**Impact:** Risk tracking, session boundaries, recording
**Owner:** _TBD_
**Deadline:** Phase 1 implementation

**Question:** Some traders trade the London open, take a break, then trade RTH. How should sessions, risk tracking, and recordings handle this?

**Candidates:**
- Each sit-down is a separate session with independent risk tracking
- Each sit-down is a separate session but risk carries across sessions for the same calendar day
- One continuous session with pauses
