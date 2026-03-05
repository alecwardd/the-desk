# The Desk — Product Requirements Document

**Version:** 1.1
**Date:** 2026-02-25
**Status:** Draft

**Companion specs:**
- [tech-plan.md](tech-plan.md) — Architecture, data model, recording format, component design
- [core-flows.md](core-flows.md) — User flows with wireframes and interaction principles
- [design-spec.md](design-spec.md) — UI/UX design specification
- [prompt-spec.md](prompt-spec.md) — LLM prompt engineering specification

---

## 1. Product Overview

### 1.1 Summary

The Desk is a real-time AI trading co-pilot for discretionary NQ futures traders. It runs as a Tauri desktop application alongside Sierra Chart, connecting to live market data via the DTC protocol, and provides contextual coaching based on the trader's own playbook, rules, and backtested setups.

### 1.2 Problem Statement

Discretionary traders consistently fail to execute the strategies they've developed and tested. The gap between knowing what to do and doing it under live market conditions is the primary source of lost edge. No existing tool provides real-time, personalized coaching that connects a trader's own rules to live market data during a session.

### 1.3 Product Vision

Close the knowledge-execution gap for discretionary traders by providing a disciplined co-pilot that knows their plan, watches the market alongside them, and helps them execute their own process in real time.

### 1.4 Target User

**Primary persona:** Discretionary prop-firm or independent NQ futures trader.

- Has 1-5+ years of trading experience
- Has specific, quantifiable setups (can describe entry/stop/target rules)
- Has experienced some success but struggles with consistency
- Trades NQ (Nasdaq 100 E-mini futures) as primary instrument
- Uses Sierra Chart on Rithmic as primary platform
- Takes short-term discretionary trades (10 minutes to 2 hours)
- Uses market structure (TPO, value areas, delta profiles) for context
- Uses DOM/order flow for entry timing and confirmation
- Often trading under prop firm constraints (daily loss limits, max position size)
- Trades alone and wants a partner in the process

### 1.5 Success Criteria

| Metric | Target |
|--------|--------|
| Trader completes first live co-pilot session | Within 1 hour of installation |
| Trader reports improved plan adherence | After 1 week of use |
| Daily active usage during RTH | >80% of trading days |
| Setup definition completion rate | >90% of started setups fully defined |
| Session recording reliability | 100% of sessions captured without data loss |

---

## 2. Phased Scope

### 2.1 Phase 1: The Live Co-Pilot (This PRD)

The minimum product that delivers the core value proposition: real-time coaching during a live NQ session based on the trader's own playbook.

### 2.2 Phase 2: Intelligence Expansion (Future PRD)

Options/gamma integration, order flow analysis, behavioral pattern recognition, structured post-session review.

### 2.3 Phase 3: Maturity (Future PRD)

Adaptive coaching, multi-instrument support, advanced analytics, playbook versioning.

---

## 3. Phase 1 Feature Requirements

### 3.1 DTC Protocol Client

**Description:** Connect to Sierra Chart's DTC server to receive real-time NQ market data.

**Requirements:**

| ID | Requirement | Priority |
|----|-------------|----------|
| DTC-01 | Establish TCP connection to Sierra Chart DTC server (configurable IP:port) | P0 |
| DTC-02 | Negotiate binary encoding via ENCODING_REQUEST/RESPONSE handshake | P0 |
| DTC-03 | Authenticate via LOGON_REQUEST with heartbeat interval negotiation | P0 |
| DTC-04 | Subscribe to NQ symbol via MARKET_DATA_REQUEST with unique SymbolID | P0 |
| DTC-05 | Receive and parse MARKET_DATA_UPDATE_TRADE messages (price, volume, timestamp) | P0 |
| DTC-06 | Receive and parse MARKET_DATA_UPDATE_BID_ASK messages (bid/ask price and size) | P0 |
| DTC-07 | Subscribe to market depth via MARKET_DEPTH_REQUEST | P1 |
| DTC-08 | Receive and parse MARKET_DEPTH_UPDATE_LEVEL messages (10 levels each side) | P1 |
| DTC-09 | Handle connection drops with automatic reconnection and state recovery | P0 |
| DTC-10 | Support configurable symbol (default NQ front month, but allow any DTC symbol) | P1 |
| DTC-11 | Log all connection events and errors for debugging | P0 |
| DTC-12 | Maintain heartbeat exchange per protocol spec | P0 |

**Technical notes:**
- Sierra Chart DTC documentation: sierrachart.com/index.php?page=doc/DTCMessageDocumentation.php
- Initial feed: Rithmic via Sierra Chart DTC server
- Each DTC message has 2-byte size header; client must handle message buffering for incomplete TCP reads
- Prices are 64-bit double; apply DisplayPriceMultiplier from security definition if non-zero
- Symbol format: use continuous contract symbol (e.g., "NQ" or specific month "NQH26")

**Acceptance criteria:**
- The Desk successfully connects to Sierra Chart's DTC server
- Real-time tick data appears in The Desk within the same second it appears on Sierra Chart
- Connection survives Sierra Chart restart (auto-reconnect)
- DOM data (when subscribed) matches what Sierra Chart displays

### 3.2 Market Structure Pipelines

**Description:** Compute real-time market structure from raw DTC data. All computations are deterministic (no LLM involvement).

#### 3.2.1 VWAP Pipeline

| ID | Requirement | Priority |
|----|-------------|----------|
| VWAP-01 | Calculate session VWAP from trade data (price * volume cumulative) | P0 |
| VWAP-02 | Calculate standard deviation bands (1σ, 2σ, 3σ) | P0 |
| VWAP-03 | Reset at configurable session boundary (default: RTH open 9:30 ET) | P0 |
| VWAP-04 | Expose current VWAP and bands as structured data to rules engine | P0 |

#### 3.2.2 TPO / Market Profile Pipeline

| ID | Requirement | Priority |
|----|-------------|----------|
| TPO-01 | Build TPO profile from trade data using configurable period (default: 30-minute brackets) | P0 |
| TPO-02 | Calculate value area (70% of TPOs) and POC (highest TPO count) | P0 |
| TPO-03 | Identify single print zones (single TPO letter at a price level) | P0 |
| TPO-04 | Track opening range (first 30 min high/low) and initial balance (first 60 min high/low) | P0 |
| TPO-05 | Identify single prints that occurred during OR or IB specifically | P0 |
| TPO-06 | Compute prior session TPO profile from stored data (for overnight context) | P0 |
| TPO-07 | Expose all TPO data as structured output to rules engine | P0 |

#### 3.2.3 Delta Neutral Pipeline

| ID | Requirement | Priority |
|----|-------------|----------|
| DN-01 | Calculate delta at each price level (buy volume minus sell volume, classified by trade at bid vs. ask) | P0 |
| DN-02 | Build delta profile (analogous to volume profile but using delta) | P0 |
| DN-03 | Calculate delta neutral value area (70% of delta, analogous to volume VA) | P0 |
| DN-04 | Calculate delta neutral pivot (price level where cumulative delta crosses zero, analogous to POC) | P0 |
| DN-05 | Track cumulative session delta | P0 |
| DN-06 | Detect delta divergences (price making new highs while delta declining, or vice versa) | P1 |
| DN-07 | Expose all delta data as structured output to rules engine | P0 |

**Technical notes:**
- Trade direction classification: trades at or above ask = buy (positive delta), trades at or below bid = sell (negative delta). Trades between bid and ask classified by proximity.
- The DNVA may be wider, narrower, above, or below the volume-based value area. Both should be computed and available.

#### 3.2.4 Key Levels Pipeline

| ID | Requirement | Priority |
|----|-------------|----------|
| KL-01 | Track prior day high, low, close (from stored session data) | P0 |
| KL-02 | Track overnight (Globex) high, low | P0 |
| KL-03 | Track prior session value area high, value area low, POC | P0 |
| KL-04 | Allow trader-defined custom levels (horizontal lines with labels) | P1 |
| KL-05 | Track round numbers at configurable intervals (default: every 100 NQ points) | P1 |
| KL-06 | Expose all levels as structured data to rules engine | P0 |

#### 3.2.5 Risk Tracker

| ID | Requirement | Priority |
|----|-------------|----------|
| RISK-01 | Track daily P&L in R-multiples (trader defines what 1R equals in points/dollars) | P0 |
| RISK-02 | Track number of trades taken today | P0 |
| RISK-03 | Track consecutive wins and losses in current session | P0 |
| RISK-04 | Track current drawdown from session high-water mark | P0 |
| RISK-05 | Compare current state against trader's risk rules (max daily loss, max trades, etc.) | P0 |
| RISK-06 | Fire risk alert when approaching or breaching risk limits | P0 |
| RISK-07 | Accept manual trade entry (price, size, direction) or import from session log | P0 |

### 3.3 Playbook Builder

**Description:** A guided interface for traders to define their setups in structured, machine-evaluable format.

| ID | Requirement | Priority |
|----|-------------|----------|
| PB-01 | Create new setup with required fields: name, description | P0 |
| PB-02 | Define entry conditions as structured rules (field + operator + value) | P0 |
| PB-03 | Supported condition fields: price relative to VWAP, price relative to VA/POC, price relative to DNVA/DNP, price relative to key levels, session delta (positive/negative/threshold), time of day, day of week, TPO structure (single prints present, value area width, etc.) | P0 |
| PB-04 | Define confirmation type: deterministic (auto-evaluated) or discretionary (DOM observation, trader confirms manually) | P0 |
| PB-05 | Define stop logic: fixed points, ATR-based, structural (below level), or custom | P0 |
| PB-06 | Define targets: up to 3 take-profit levels with position management rules per level | P0 |
| PB-07 | Define position sizing rules: fixed contracts, R-based, or account-percentage | P0 |
| PB-08 | Define market context requirements (trend day only, range day only, any, custom) | P1 |
| PB-09 | Define invalidation criteria (conditions that cancel the setup before entry) | P0 |
| PB-10 | Attach backtest results: win rate, avg R, sample size, profit factor, max drawdown | P0 |
| PB-11 | Attach context-specific backtest results (by day of week, market regime, time of day) | P1 |
| PB-12 | Import backtest results from CSV (Sierra Chart trade log format) | P1 |
| PB-13 | LLM-assisted setup definition: trader describes setup in natural language, LLM helps structure it into fields | P0 |
| PB-14 | LLM sparring: ask clarifying questions about ambiguous rules during definition | P0 |
| PB-15 | Save/edit/delete/duplicate setups | P0 |
| PB-16 | Toggle setups active/inactive (inactive setups not evaluated during live session) | P0 |
| PB-17 | Pre-loaded setup templates: VWAP pullback, OR breakout/failure, DNVA reversion, single print retest | P0 |
| PB-18 | Each setup displays its backtest metrics prominently whenever referenced | P0 |

**Acceptance criteria:**
- A trader can define the "DNVA Reversion" and "Single Print Retest" setups (from the vision doc examples) entirely through the builder
- All structured conditions are automatically evaluated against live data
- Discretionary conditions (e.g., "strong initiation on DOM") are flagged for manual confirmation
- Setup templates provide a useful starting point that traders customize

### 3.4 Rules Engine

**Description:** Deterministic evaluation of playbook conditions against live market data. No LLM involvement — pure conditional logic.

| ID | Requirement | Priority |
|----|-------------|----------|
| RE-01 | Continuously evaluate all active setup conditions against latest pipeline data | P0 |
| RE-02 | Fire setup alert when all deterministic conditions are met | P0 |
| RE-03 | For setups with discretionary confirmation, fire "watching" alert when deterministic conditions are met, prompting trader to watch for confirmation | P0 |
| RE-04 | Include all relevant data in alert payload: setup name, entry level, stop, targets, backtest metrics, current risk state | P0 |
| RE-05 | Evaluate conditions at data-feed speed (sub-second from condition becoming true to alert firing) | P0 |
| RE-06 | Track setup state: not active → conditions approaching → conditions met → confirmed → in trade → closed | P1 |
| RE-07 | Suppress duplicate alerts (don't re-fire the same setup alert within configurable window) | P0 |
| RE-08 | Evaluate risk rules alongside setup rules (don't alert for a setup if daily loss limit is reached) | P0 |
| RE-09 | Log all rule evaluations for debugging and replay verification | P1 |

### 3.5 LLM Coaching Layer

**Description:** The conversational AI layer that synthesizes structured signals into human-readable coaching prompts.

| ID | Requirement | Priority |
|----|-------------|----------|
| LLM-01 | Generate contextual coaching prompt when rules engine fires a setup alert | P0 |
| LLM-02 | Prompt includes: setup name, entry level, stop, all targets with management rules, backtest metrics, current risk state, relevant journal notes | P0 |
| LLM-03 | Generate risk warnings when trader approaches or breaches limits | P0 |
| LLM-04 | Generate trade management prompts when in-trade targets are hit or conditions change | P0 |
| LLM-05 | Response latency: <2 seconds from rules engine alert to coaching prompt displayed | P0 |
| LLM-06 | Use Claude API (Sonnet for real-time coaching, Opus for session review/analysis) | P0 |
| LLM-07 | Configurable voice/personality: direct, analytical, minimal, motivational | P1 |
| LLM-08 | Configurable behavioral rules: quiet periods, reminder triggers, inactivity check-ins | P1 |
| LLM-09 | Pre-session briefing: analyze overnight data, identify setups in play, surface relevant journal notes | P0 |
| LLM-10 | Prompt engineering: all prompts trace to specific playbook rules and data — never speculative | P0 |
| LLM-11 | Fallback for connectivity: if Claude API is unreachable, display raw rules engine alerts without LLM synthesis | P0 |
| LLM-12 | Token-efficient context: send structured data summaries, not raw market data | P0 |
| LLM-13 | When a live position is logged (via "Took it"), management prompts reference actual position state: entry price, size, targets hit, current P&L in R | P0 |
| LLM-14 | When no live position is logged, management prompts are condition-based (e.g., "Price at T1 level — your rules say consider trimming here if you're in this trade") | P0 |
| LLM-15 | The LLM Orchestrator checks for an open `Trade` record before assembling the management prompt context. If a trade is open, include position state; if not, use condition-based framing | P0 |

**Acceptance criteria (LLM-13..15):**
- Management prompts reference actual P&L when a trade is logged
- Management prompts use conditional framing when no trade is logged
- Both paths produce compliant, non-advisory language

**Technical notes:**
- Claude Sonnet for real-time coaching (fast, good enough quality for structured prompts)
- Claude Opus for pre/post session analysis (slower, higher reasoning quality)
- API cost estimate: ~20-50 calls per session, mostly Sonnet = <$1 per session
- Context window strategy: maintain a rolling session context with playbook rules, current market state summary, recent alerts, and risk state. Rebuild context each call rather than maintaining a long conversation.

### 3.6 Session Recording & Tape Replay

**Description:** Record every live session and replay with full coaching simulation.

#### Recording

| ID | Requirement | Priority |
|----|-------------|----------|
| REC-01 | Record all tick data (timestamp, price, volume, side) during live sessions | P0 |
| REC-02 | Record DOM snapshots at configurable interval (default: 100ms) | P1 |
| REC-03 | Record all pipeline outputs (VWAP, TPO state, delta state, key levels) at regular intervals | P0 |
| REC-04 | Record all rules engine events (alerts fired, conditions evaluated) | P0 |
| REC-05 | Record all coaching prompts displayed to trader | P0 |
| REC-06 | Record trader actions (trade entries/exits, if tracked) | P0 |
| REC-07 | Compress recordings with zstd — target: <1MB per RTH session for tick data | P0 |
| REC-08 | Automatic recording start at session open, stop at close (configurable) | P0 |
| REC-09 | Zero-impact on live session performance (recording runs in background thread) | P0 |

#### Replay

| ID | Requirement | Priority |
|----|-------------|----------|
| RPL-01 | Load recorded session and replay tick data at original timestamps | P0 |
| RPL-02 | Adjustable playback speed: 1x, 2x, 4x, 8x | P0 |
| RPL-03 | Pause, resume, and scrub to specific timestamp | P0 |
| RPL-04 | Full pipeline processing during replay (VWAP, TPO, delta computed from replayed data) | P0 |
| RPL-05 | Rules engine evaluates during replay — same alerts fire as would in live | P0 |
| RPL-06 | LLM coaching prompts generated during replay (same as live, with replay context) | P0 |
| RPL-07 | Trader can "take" simulated trades during replay for practice | P1 |
| RPL-08 | Ship with curated library of notable NQ sessions (trend days, chop, FOMC, gap fills) | P1 |
| RPL-09 | Traders can save and organize their own recorded sessions | P0 |
| RPL-10 | After replay ends (or trader stops it), display an optional post-replay summary: total prompts fired, trader responses (took it / watching / passed), and simulated trade results if any trades were taken | P1 |
| RPL-11 | Post-replay summary is dismissible. It does not auto-save to session history (replay sessions are separate from live sessions) | P1 |

### 3.7 Pre-Session Briefing

| ID | Requirement | Priority |
|----|-------------|----------|
| BRIEF-01 | Generate briefing from overnight/Globex data before RTH open | P0 |
| BRIEF-02 | Identify which playbook setups are most likely in play given current context | P0 |
| BRIEF-03 | Display key levels: prior day H/L/C, overnight H/L, prior VA/POC, DNVA/DNP | P0 |
| BRIEF-04 | Surface relevant journal notes from similar past contexts | P1 |
| BRIEF-05 | Display current risk state (daily P&L if multi-session day) | P0 |
| BRIEF-06 | Flag scheduled news/events during today's session (manual input or calendar integration) | P1 |
| BRIEF-07 | Configurable briefing time (default: 15 min before RTH open) | P1 |
| BRIEF-08 | Display a free-text "Your Note for Today" input field at the bottom of the pre-session briefing | P0 |
| BRIEF-09 | Save the pre-session note with the session record. Pre-populate the post-session journal entry field with this note as a starting point | P0 |

### 3.8 Basic Session Log & Trade Import

| ID | Requirement | Priority |
|----|-------------|----------|
| LOG-01 | Chronological log of all coaching prompts, alerts, and risk warnings during session | P0 |
| LOG-02 | Log trader's manual notes/annotations during session (quick text input) | P0 |
| LOG-03 | Import trades from Sierra Chart trade activity log (CSV format) | P0 |
| LOG-04 | Match imported trades against playbook setups (was this a planned trade?) | P1 |
| LOG-05 | Tag each trade: planned/unplanned, rules followed/deviated, emotional state (trader input) | P1 |
| LOG-06 | Post-session summary: trades taken, plan adherence, P&L, key moments | P0 |
| LOG-07 | Searchable session history | P1 |
| LOG-08 | Display three response options on every coaching prompt card: **Took it**, **Watching**, **Passed** | P0 |
| LOG-09 | Store each prompt response as a `SessionEvent` with `event_type = "prompt_response"`. Data payload: `{ prompt_event_id, response: "took_it"\|"watching"\|"passed", note: string\|null }` | P0 |
| LOG-10 | When trader selects "Passed", display an optional quick note field: "Why did you pass?" Dismissible without entering a note | P0 |
| LOG-11 | When trader selects "Took it", open a quick trade entry form pre-filled with alert-inferred direction (long/short). Trader enters size and entry price. Creates a live `Trade` record and activates in-trade management prompts | P0 |
| LOG-12 | Calculate two adherence metrics per session: **Prompt Adherence** (% of setup prompts where response = "took_it") and **Rules Adherence** (% of "took_it" trades marked `rules_followed = true` during review). Display both in post-session summary | P0 |
| LOG-13 | "Watching" response keeps the prompt active in the feed. Conditions continue to be monitored. Trader can respond again if conditions persist | P0 |
| LOG-14 | When importing trades from Sierra Chart CSV, auto-match each trade to the nearest coaching prompt by time proximity (within 30 seconds) and price proximity (within 5 NQ points). Display match confidence for trader confirmation | P1 |
| LOG-15 | Allow trader to skip trade import entirely. Review is saved with coaching log only (no trade data). Prompt Adherence is still calculated; Rules Adherence shown as N/A until trade review is completed | P0 |
| LOG-16 | Post-session summary card displays: total prompts sent, breakdown by response type, Prompt Adherence, Rules Adherence (if available), and session P&L (if trades are logged) | P0 |

**Acceptance criteria (LOG-08..16):**
- Every coaching prompt card shows all three response buttons
- Responses are stored and queryable by session
- Prompt Adherence appears in the post-session summary (always); Rules Adherence appears when trade review is completed
- "Took it" flow opens trade entry without requiring mouse interaction
- Auto-matching surfaces the most likely prompt-trade pair for each imported trade
- "Skip import" path saves a complete session record without trade data
- Summary card is visible immediately after session ends

### 3.9 User Interface

**Description:** The Desk's UI, designed for a dedicated second monitor alongside Sierra Chart.

| ID | Requirement | Priority |
|----|-------------|----------|
| UI-01 | Single-window application designed for dedicated monitor (1920x1080 minimum) | P0 |
| UI-02 | Dark theme default (matching trading environment, reducing eye strain) | P0 |
| UI-03 | Main coaching panel: displays current alerts, prompts, and messages in chronological feed | P0 |
| UI-04 | Sidebar: current market state summary (VWAP, VA, DNVA, delta, key levels — text/numbers, not charts) | P0 |
| UI-05 | Risk dashboard: daily P&L, drawdown, trade count, limit proximity (always visible) | P0 |
| UI-06 | Playbook status: which setups are active, which conditions are currently met | P0 |
| UI-07 | Connection status: DTC feed health, API connectivity | P0 |
| UI-08 | Quick note input: trader can type brief notes during session without leaving The Desk | P0 |
| UI-09 | Playbook builder: multi-step form with LLM chat assist | P0 |
| UI-10 | Session review view: post-session trade list with tagging and journaling | P0 |
| UI-11 | Tape replay view: playback controls, coaching feed, simulated trade entry | P1 |
| UI-12 | Settings: DTC connection, API keys, risk parameters, personality/behavior config | P0 |
| UI-13 | Built with React + TypeScript + shadcn/ui component library | P0 |
| UI-14 | Keyboard-first interaction (traders don't want to grab the mouse during a session) | P0 |
| UI-15 | Notification sounds for high-priority alerts (setup triggered, risk limit warning) | P1 |
| UI-16 | Font size and information density configurable | P1 |
| UI-17 | `N` key opens the quick note input during a live session. `Enter` saves the note. `Escape` dismisses without saving | P0 |
| UI-18 | `Ctrl+E` triggers "End Session" with a confirmation dialog. `Enter` confirms, `Escape` cancels | P0 |
| UI-19 | Prompt response buttons are keyboard-accessible: `1` = Took it, `2` = Watching, `3` = Passed, when a prompt card is focused | P1 |
| UI-20 | Display a keyboard shortcut reference accessible via `?` key or a help icon | P1 |

### 3.10 Onboarding

**Description:** First-time setup experience guiding the trader through DTC connection, risk configuration, and optional playbook definition.

| ID | Requirement | Priority |
|----|-------------|----------|
| ONBOARD-01 | During onboarding Step 4 (Playbook Setup), provide a "Skip for now — I'll add setups later" option that bypasses setup definition and proceeds directly to the main dashboard | P0 |
| ONBOARD-02 | When no setups are active, display a persistent banner on the main dashboard: "No active setups — The Desk is watching but won't alert on setups. Add a setup to unlock coaching." Banner includes a direct link to the playbook builder | P0 |
| ONBOARD-03 | The app is fully functional with no active setups: DTC connection, market state display, risk tracking, and quick notes all work. Only setup alerts and coaching prompts are suppressed | P0 |

**Acceptance criteria:**
- Trader can complete onboarding in under 10 minutes without defining a setup
- Banner is visible and persistent until at least one setup is activated
- All non-coaching features work without an active setup

---

## 4. Data Model

### 4.1 Core Entities

**Setup (Playbook Entry)**
```
id: UUID
name: string
description: string
active: boolean
conditions: SetupCondition[]          // Deterministic conditions
discretionary_conditions: string[]    // Natural language (for LLM interpretation)
entry_logic: EntryLogic
stop_logic: StopLogic
targets: Target[]                     // Up to 3
position_sizing: PositionSizing
market_context: MarketContext         // Required conditions (trend, range, etc.)
invalidation: InvalidationRule[]
backtest_results: BacktestResults
context_backtest_results: ContextBacktest[]  // By day, regime, time, etc.
notes: string
created_at: timestamp
updated_at: timestamp
```

**SetupCondition**
```
id: UUID
field: enum (price_vs_vwap, price_vs_va_high, price_vs_dnva, session_delta_sign,
             time_of_day, tpo_single_prints_present, price_vs_key_level, ...)
operator: enum (above, below, crosses_above, crosses_below, within, outside, equals)
value: number | string | reference    // Static value or reference to computed level
```

**BacktestResults**
```
setup_id: UUID
period: string                        // "2024-01-01 to 2024-12-31"
samples: integer
win_rate: float
avg_winner_r: float
avg_loser_r: float
profit_factor: float
max_consecutive_losses: integer
max_drawdown_r: float
expectancy_r: float                   // (WR * avg_win) - ((1-WR) * avg_loss)
source: string                        // "Sierra Chart backtest" / "manual" / etc.
imported_at: timestamp
```

**Session**
```
id: UUID
date: date
session_type: enum (rth, globex, replay)
start_time: timestamp
end_time: timestamp
recording_path: string                // Path to compressed binary recording
```

**SessionEvent**
```
id: UUID
session_id: UUID
timestamp: timestamp
event_type: enum (alert_fired, coaching_prompt, risk_warning, trader_note,
                  trade_entry, trade_exit, setup_condition_met, prompt_response)
setup_id: UUID | null
data: JSON                            // Event-specific payload
```

**Trade**
```
id: UUID
session_id: UUID
setup_id: UUID | null                 // null = unplanned trade
entry_time: timestamp
entry_price: float
exit_time: timestamp | null
exit_price: float | null
direction: enum (long, short)
size: integer                         // Number of contracts
stop_price: float
target_prices: float[]
result_r: float | null
planned: boolean
rules_followed: boolean | null        // Trader self-assessment
emotional_state: string | null        // Trader self-tag
notes: string
source: enum (manual, imported_csv)
```

**RiskConfig**
```
r_value_points: float                 // How many NQ points = 1R
r_value_dollars: float                // Dollar value of 1R
max_daily_loss_r: float               // e.g., 3.0
max_consecutive_losses: integer       // e.g., 3
max_trades_per_session: integer | null
no_trade_zones: TimeRange[]           // e.g., first 5 min, around news
```

**JournalEntry**
```
id: UUID
session_id: UUID | null
date: date
content: string
tags: string[]
setup_references: UUID[]              // Links to specific setups discussed
trade_references: UUID[]              // Links to specific trades discussed
created_at: timestamp
```

### 4.2 Storage

All data stored in local SQLite database at a configurable path (default: `~/.the-desk/data.db`).

Session recordings stored as compressed binary files at `~/.the-desk/recordings/`.

Configuration stored at `~/.the-desk/config.toml`.

---

## 5. Integration Requirements

### 5.1 Sierra Chart DTC Protocol

| Aspect | Detail |
|--------|--------|
| Protocol | DTC (Data and Trading Communication) over TCP |
| Documentation | sierrachart.com DTC Message Documentation |
| Encoding | Binary (fixed-length strings) — negotiate via ENCODING_REQUEST |
| Authentication | LOGON_REQUEST with heartbeat interval |
| Data subscribed | Market data (trades, bid/ask) + market depth (DOM levels) |
| Connection | Configurable IP:port (default: localhost:11099) |
| Reconnection | Automatic with exponential backoff |

### 5.2 Claude API

| Aspect | Detail |
|--------|--------|
| Provider | Anthropic |
| Models | Claude Sonnet 4.6 (real-time coaching), Claude Opus 4.6 (analysis) |
| Authentication | API key stored locally in config |
| Usage pattern | Stateless calls with rebuilt context per request |
| Estimated volume | 20-50 calls per trading session |
| Fallback | Raw alert display if API unreachable |

### 5.3 Options Data (Phase 2, design now)

| Provider | Type | Data | Priority |
|----------|------|------|----------|
| **Databento** | REST/Historical | Raw OPRA + CME chains; we compute Greeks/GEX | P2-first (preferred, ADR-013) |
| Unusual Whales | REST API | Pre-computed GEX, gamma/delta/charm by strike | P2-alternative |
| ConvexValue | API | Pre-computed gamma, gxoi, gxvolm, charm, vanna | P2-alternative |
| CBOE | Data download | Raw options chains for SPX/NDX, compute GEX locally | P2 |
| OptionData.io | WebSocket | Real-time options trades with Greeks ($599/mo) | P2-optional |

---

## 6. Non-Functional Requirements

### 6.1 Performance

| Requirement | Target |
|-------------|--------|
| DTC data processing latency | <50ms from receipt to pipeline output |
| Rules engine evaluation | <100ms from pipeline output to alert |
| LLM coaching prompt | <2 seconds from alert to displayed prompt |
| UI rendering | 60fps, no jank during data updates |
| Memory usage | <500MB total (running alongside Sierra Chart) |
| Tape replay startup | <3 seconds to load a session |
| Application startup | <5 seconds to ready state |

### 6.2 Reliability

| Requirement | Target |
|-------------|--------|
| DTC connection uptime | 99.9% during trading session (auto-reconnect) |
| Session recording | Zero data loss — no missed ticks during recording |
| Data integrity | SQLite WAL mode, no data corruption on crash |
| Graceful degradation | App functional without LLM API (alerts without coaching prose) |

### 6.3 Security & Privacy

| Requirement | Detail |
|-------------|--------|
| Data storage | All data local, never uploaded without explicit consent |
| API key storage | Encrypted at rest in local config |
| LLM context | Only structured summaries sent to Claude API, never raw market data or PII |
| No telemetry | No usage tracking, analytics, or phone-home without opt-in |

### 6.4 Compatibility

| Requirement | Detail |
|-------------|--------|
| OS | Windows 10/11 (primary), macOS (future) |
| Display | Minimum 1920x1080, optimized for dedicated monitor |
| Sierra Chart | Version 2600+ with DTC server enabled |
| Data feeds | Rithmic (primary), Denali (secondary) |

---

## 7. Technical Architecture Summary

### 7.1 Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Desktop framework | Tauri 2.x | Window management, system integration, IPC |
| Backend | Rust | DTC client, pipelines, rules engine, recording, SQLite |
| Frontend | React 19 + TypeScript | UI components, state management, user interaction |
| UI library | shadcn/ui + Tailwind CSS | Component library, dark theme, keyboard accessibility |
| Database | SQLite (via rusqlite) | Local data persistence |
| LLM | Claude API (Anthropic SDK) | Coaching prompts, session analysis, NL interpretation |
| Compression | zstd | Session recording compression |

### 7.2 Project Structure

```
the-desk/
├── docs/
│   ├── the-desk-vision.md           # Product vision and philosophy
│   ├── phase-1-prd.md               # This document (Phase 1 requirements)
│   ├── phase-2-prd.md               # Phase 2 requirements (Intelligence Expansion)
│   ├── phase-3-prd.md               # Phase 3 requirements (Maturity)
│   ├── epic-brief.md                # Epic brief — problem, scope, constraints
│   ├── tech-plan.md                 # Architecture, data model, component design
│   ├── core-flows.md                # User flows with wireframes
│   ├── design-spec.md               # UI/UX design specification
│   ├── prompt-spec.md               # LLM prompt engineering specification
│   ├── decision-log.md              # ADR-style key decisions and rationale
│   └── roadmap.md                   # Phase sequencing, traceability, entry/exit criteria
├── src-tauri/                        # Rust backend
│   ├── src/
│   │   ├── main.rs
│   │   ├── dtc/                     # DTC protocol client
│   │   ├── pipelines/               # Market structure computations
│   │   │   ├── vwap.rs
│   │   │   ├── tpo.rs
│   │   │   ├── delta.rs
│   │   │   └── levels.rs
│   │   ├── rules/                   # Playbook rules engine
│   │   ├── recording/               # Tape recording & replay
│   │   ├── risk/                    # Risk tracking
│   │   └── db/                      # SQLite operations
│   ├── Cargo.toml
│   └── tauri.conf.json
├── src/                              # React frontend
│   ├── components/
│   │   ├── coaching/                # Coaching panel
│   │   ├── playbook/                # Playbook builder
│   │   ├── dashboard/               # Market state display
│   │   ├── risk/                    # Risk dashboard
│   │   ├── replay/                  # Tape replay controls
│   │   ├── review/                  # Session review
│   │   └── settings/                # Configuration
│   ├── hooks/                        # Tauri event listener hooks
│   ├── lib/
│   │   ├── claude.ts                # Claude API integration
│   │   ├── tauri-bridge.ts          # Rust ↔ React IPC
│   │   └── types.ts                 # Shared TypeScript types
│   ├── context/                      # React context providers
│   ├── App.tsx
│   └── main.tsx
├── agents/                           # Subagent definitions
├── skills/                           # Domain knowledge
├── commands/                         # Slash commands
├── .cursorrules                      # Cursor AI rules
├── CLAUDE.md                         # Project rules (read first)
├── AGENT.md                          # LLM coding agent instructions
├── package.json
└── README.md
```

### 7.3 Development Approach

**Spec-driven development with LLM coding agents.**

1. **Planning documents first** — Vision, PRD, epic brief, tech plan, core flows, design spec, prompt spec
2. **AGENT.md and project rules** — Clear instructions for LLM coding agents (Cursor, Claude Code, Codex)
3. **Phase-based implementation** — Each phase broken into discrete, testable chunks
4. **AI-assisted coding** — LLMs handle bulk implementation from detailed specs
5. **Human review** — All generated code reviewed for correctness, security, and architecture alignment

---

## 8. Regulatory Positioning

The Desk is a **coaching and discipline tool**, not an investment advisory service.

**Key distinctions:**
- The Desk never generates trade recommendations from its own models
- Every alert traces directly to the trader's own playbook rules and backtest data
- The system reflects the trader's own plan back to them — it does not create the plan
- Analogous to: a workout tracking app (not practicing medicine), a checklist app for pilots (not flying the plane), a writing coach that checks your outline (not writing for you)

**Recommended action:** Engage a securities attorney to review product positioning, marketing language, and UI copy before public launch. The coaching framing is strong but should be legally validated.

---

## 9. Open Questions

| # | Question | Impact | Status |
|---|----------|--------|--------|
| 1 | Exact DTC message flow for Sierra Chart's specific implementation — any quirks beyond the spec? | DTC client development | To investigate |
| 2 | How to classify trade direction from DTC trade messages when trade price is between bid and ask? | Delta calculation accuracy | Need to define heuristic |
| 3 | What Sierra Chart CSV trade log format looks like exactly — column names, date format, etc.? | Trade import feature | Need sample file |
| 4 | Claude API latency under load — can we consistently hit <2s including network round-trip? | Coaching prompt timing | Need to benchmark |
| 5 | Tauri 2.x maturity on Windows — any known issues with multi-window, system tray, or high-DPI? | Framework choice | Need to prototype |
| 6 | How to handle NQ contract rollovers (symbol changes quarterly) in DTC subscriptions? | Continuous operation | Need to define approach |
| 7 | Licensing: can we redistribute curated NQ session recordings for the replay library? | Replay feature | Legal review needed |
| 8 | Options data provider selection — Unusual Whales API reliability and rate limits? | Phase 2 planning | Need to test |

---

## 10. Glossary

| Term | Definition |
|------|------------|
| **DTC** | Data and Trading Communication — protocol for market data and trading |
| **DNVA** | Delta Neutral Value Area — value area calculated from delta (buy vs. sell volume) rather than total volume |
| **DNP** | Delta Neutral Pivot — the zero-crossing point in cumulative delta profile (analogous to POC) |
| **DOM** | Depth of Market — the order book showing resting buy and sell orders at each price level |
| **GEX** | Gamma Exposure — aggregate gamma positioning of options market makers, indicating where hedging flows may accelerate or dampen price movement |
| **IB** | Initial Balance — the price range established in the first hour of regular trading |
| **NQ** | E-mini Nasdaq 100 futures contract |
| **OR** | Opening Range — the price range established in the first 30 minutes of regular trading |
| **POC** | Point of Control — the price level with the highest volume (or TPO count) in a profile |
| **R** | Risk unit — the amount risked on a single trade (e.g., if stop is 8 NQ points with 1 contract, 1R = 8 points × $5/point = $40) |
| **RTH** | Regular Trading Hours — 9:30 AM to 4:15 PM ET for NQ futures (4:00 PM ET is the cash settlement time; NQ futures continue trading until 4:15 PM ET) |
| **TPO** | Time Price Opportunity — a unit of time spent at a price level in Market Profile analysis |
| **VA** | Value Area — the price range encompassing 70% of trading activity (by volume or TPO) |

---

*The Desk — Where serious traders do serious work.*
