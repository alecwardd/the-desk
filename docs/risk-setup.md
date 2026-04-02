# Risk State Setup

The Desk's risk coach requires three pieces of state to be initialized before it can provide full risk tracking:

1. **Account state** — balance, Lucid params, open positions  
2. **Risk config** — R-value, max daily loss, circuit breaker, trade limits  
3. **Risk state** — session P&L, trade count, streaks (created at session start)

## Quick Start (MCP / Cursor)

When the orchestrator reports "Risk state not initialized", run this sequence via MCP tools:

### 1. Save account state

Call `save_account_state` with your current values:

```json
{
  "lastBalanceDollars": 50000,
  "lucidDailyLossDollars": 1200,
  "lucidAccountSizeDollars": 50000,
  "openPositions": [
    {
      "direction": "short",
      "size": 1,
      "entryPrice": 24846.25,
      "instrument": "MNQ"
    }
  ]
}
```

- **lastBalanceDollars** — current account balance  
- **lucidDailyLossDollars** — Lucid daily loss limit (e.g. $1,200)  
- **lucidAccountSizeDollars** — account size for R derivation  
- **openPositions** — any open positions not yet closed  

### 2. Save risk config (optional)

Call `save_risk_config` to persist custom limits. Omit fields to keep defaults:

```json
{
  "rValuePoints": 80,
  "rValueDollars": 400,
  "maxDailyLossR": 3,
  "maxConsecutiveLosses": 3,
  "maxTradesPerSession": 8,
  "maxDailyLossDollars": 1200
}
```

Defaults: R = 80 pts / $400, max 3R daily loss ($1,200), 3-loss circuit breaker, 8 trades/session.

### 3. Initialize risk state

Call `init_risk_state` to create the session risk row (0 P&L, 0 trades, no streaks). Do this at session start.

No parameters required.

**Alternative:** Run the one-shot binary to initialize both risk config and risk state:

```bash
cargo run --bin the-desk-init-risk
```

This writes default risk config (R=80pts/$400, max 3R daily = $1,200, 3-loss circuit breaker) and creates the initial risk state row. Uses the same database as MCP and Tauri (`~/.the-desk/data.db`).

## R Derivation

R is derived from Lucid params:

```
R_dollars = lucid_daily_loss_dollars / max_daily_loss_r
R_points  = R_dollars / 5.00   (NQ: $5 per point per MNQ contract)
```

Example: $1,200 daily loss, 3R max → R = $400 = 80 NQ points.

## Lucid Direct Payout Rules

The current account-specific payout rules are:

- 20% consistency before payout
- At least 5 profitable trading days before payout
- End-of-day drawdown framing via LucidScale

Those payout-cycle metrics are currently prompt-managed for the risk coach rather than persisted in the SQLite risk tables, so confirm them manually when payout eligibility matters.

## Tauri App

If using the Tauri desktop app, risk config and account state can also be set via:

- `riskBridge.saveConfig(config)` — persist risk config  
- `accountBridge.save(input)` — persist balance and positions  

The MCP server shares the same SQLite database, so changes in either interface are visible to both.
