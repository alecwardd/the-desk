---
name: Risk Manager Integration
overview: "Integrate the risk manager across all decision flows: session-start balance/position confirmation, proactive risk warnings, pre-entry gates, Lucid parameters, 1/4 Kelly position sizing with confidence scaling, and agent-driven account state tracking."
todos: []
isProject: false
---

# Risk Manager Integration Plan (Extended)

## Scope Overview

1. **Session-start flow**: Agent asks for current balance and confirms positions not discussed
2. **Account state persistence**: Last known balance, open positions, Lucid params, goals
3. **Lucid integration**: Daily loss limits ($750), account size, profit/withdrawal goals
4. **1/4 Kelly position sizing** with confidence-based scaling
5. **Proactive risk warnings** and pre-entry gates (from original plan)

---

## Data Model: Account and Risk Context

### New/Extended Tables and Types

**account_state** (new table or extend risk_config):


| Field                      | Type | Purpose                                                 |
| -------------------------- | ---- | ------------------------------------------------------- |
| last_balance_dollars       | f64  | Last confirmed account balance                          |
| last_balance_updated_at_ms | i64  | When it was updated                                     |
| open_positions_json        | TEXT | `[{direction, size, entryPrice, instrument, setupId?}]` |
| lucid_daily_loss_dollars   | f64  | e.g. 750                                                |
| lucid_account_size_dollars | f64  | e.g. 50000                                              |
| profit_target_per_cycle    | f64  | e.g. 2000                                               |
| position_sizing_method     | TEXT | "quarter_kelly"                                         |
| kelly_fraction             | f64  | 0.25                                                    |


**Signal performance extension** for Kelly inputs:

- Extend `get_signal_performance` (or add `get_kelly_inputs`) to return:
  - `avgWinnerR`, `avgLoserR` (or win/loss ratio `b`)
  - `winRate` (already exists as `p`)
- Kelly: `f* = (b*p - q) / b` where `b = avgWinnerR/|avgLoserR|`, `q = 1-p`
- 1/4 Kelly: `0.25 * f`* = fraction of capital to risk per trade

---

## Session-Start Flow

### Agent-Driven (risk-coach / Cursor)

When the user indicates they are starting a session (e.g. "Starting my session" or invokes risk-coach at session open):

1. **risk-coach** calls `get_account_state` → receives last balance, open positions, Lucid params
2. Agent says: "Last time your balance was $X,XXX. What is your current account balance? Do you have any open positions that weren't discussed in this chat?"
3. User replies with balance and/or positions
4. Agent (or user) triggers `save_account_state` with the new values
5. Agent then provides risk context: daily limit remaining, goals, suggested position size from 1/4 Kelly if signal performance is available

### Tauri Pre-Session Briefing

Enhance [PreSessionBriefing](src/components/briefing/pre-session-briefing.tsx) and [BriefingContext](src/lib/claude.ts):

- Add "Account Check" section before/during briefing:
  - Display: "Last balance: $X (updated DATE)"
  - Input: Current balance (required before Start Session)
  - Optional: "Open positions not from this chat" (direction, size, entry)
- On Start Session: save account state via new Tauri command, then proceed
- Pass `lastBalance`, `openPositions` into `generateBriefingSynthesis` so the narrative can reference them (e.g. "Confirming balance $X before we start...")

---

## risk-coach Agent Enhancements

Update [agents/risk-coach.md](agents/risk-coach.md):

**New primary tools:**

- `get_account_state` — last balance, open positions, Lucid params, goals
- `save_account_state` — persist balance and positions (user-confirmed)
- `get_risk_config` — R framework, max daily loss
- `get_signal_performance` — for Kelly inputs (win rate, avg R)
- `get_risk_state` — current session P&L, at_limit
- `get_setup_context` / `get_market_snapshot` — trade context

**Session-start responsibility:**

- At the start of a trading session, call get_account_state first. Report last known balance. Ask the trader to confirm current balance and any open positions not discussed. Once confirmed, save via save_account_state.

**Position sizing:**

- When discussing a proposed trade, use get_signal_performance to compute 1/4 Kelly if sufficient data exists. Fractional Kelly = 0.25 * (b*p - q) / b. Report suggested risk as fraction of capital. Scale down for low-confidence setups (e.g. 0.125 Kelly), scale up for high confidence (e.g. 0.5 Kelly) — always within the trader's stated limits.

**Lucid alignment:**

- Reference Lucid daily loss limit ($750), how proposed trades fit within remaining daily risk, and cycle profit goals. Frame as "your configured limits indicate..." not advice.

**Position confirmation:**

- If the trader logs a position that wasn't discussed in chat (e.g. via TradeEntryForm with a note), the agent should acknowledge and ask for confirmation on next session start: "You had an open X MNQ long from $Y — was this discussed? If not, please confirm."

---

## MCP Tools to Add


| Tool                      | Purpose                                                                                                             |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `get_account_state`       | Last balance, open positions, Lucid params, goals, Kelly fraction                                                   |
| `save_account_state`      | Persist balance, positions, optional Lucid updates                                                                  |
| `get_kelly_position_size` | Inputs: setup_id (optional), balance, confidence_multiplier. Returns: suggested R to risk, fractional Kelly, raw f* |


---

## Position Sizing: 1/4 Kelly with Confidence

**Formula:**

- `b` = avg winner R / |avg loser R| (from signal_outcomes)
- `p` = win rate
- `q` = 1 - p
- Full Kelly: `f* = (b*p - q) / b`
- 1/4 Kelly: `f_quarter = 0.25 * f`*
- Confidence scaling: `f_final = f_quarter * confidence_multiplier` where:
  - High confidence: 1.0–1.5 (up to half Kelly)
  - Normal: 1.0
  - Low confidence: 0.5 (1/8 Kelly)

**Output:** "Risk f_final of your balance this trade" → convert to R given rValueDollars and balance.

---

## File Change Summary


| File                                                                                                 | Changes                                                                                         |
| ---------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| [src-tauri/src/db/mod.rs](src-tauri/src/db/mod.rs)                                                   | account_state table, CRUD, extend signal_performance for avgWinnerR/avgLoserR                   |
| [src-tauri/src/bin/the-desk-mcp.rs](src-tauri/src/bin/the-desk-mcp.rs)                               | get_account_state, save_account_state, get_kelly_position_size                                  |
| [src-tauri/src/commands.rs](src-tauri/src/commands.rs)                                               | get_account_state, save_account_state Tauri commands                                            |
| [src/lib/tauri-bridge.ts](src/lib/tauri-bridge.ts)                                                   | accountBridge                                                                                   |
| [src/lib/types.ts](src/lib/types.ts)                                                                 | AccountState, OpenPosition types                                                                |
| [src/components/briefing/pre-session-briefing.tsx](src/components/briefing/pre-session-briefing.tsx) | Account Check UI, balance/position inputs                                                       |
| [src/lib/claude.ts](src/lib/claude.ts)                                                               | BriefingContext + lastBalance, openPositions; generateBriefingSynthesis includes account prompt |
| [agents/risk-coach.md](agents/risk-coach.md)                                                         | Session-start flow, Kelly sizing, Lucid params, position confirmation                           |
| [src/App.tsx](src/App.tsx)                                                                           | Proactive risk warnings, pass riskState to CoachingFeed                                         |
| [src/components/coaching/coaching-feed.tsx](src/components/coaching/coaching-feed.tsx)               | Pre-entry risk gate                                                                             |
| [src/components/coaching/trade-entry-form.tsx](src/components/coaching/trade-entry-form.tsx)         | Risk state, at_limit block                                                                      |


---

## Implementation Order

1. **DB + MCP**: account_state schema, get/save_account_state, extend signal_performance for Kelly
2. **MCP**: get_kelly_position_size tool
3. **Tauri commands**: account state IPC
4. **risk-coach agent**: Full rewrite with session-start, Kelly, Lucid
5. **Pre-session briefing**: Account Check UI + BriefingContext
6. **Original plan items**: Proactive warnings, pre-entry gates, RiskBar dollars

