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
  "lucidDailyLossDollars": 750,
  "lucidAccountSizeDollars": 50000,
  "profitTargetPerCycle": 2000,
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
- **lucidDailyLossDollars** — Lucid daily loss limit (e.g. $750)  
- **lucidAccountSizeDollars** — account size for R derivation  
- **profitTargetPerCycle** — Lucid profit target per cycle (e.g. $2,000)  
- **openPositions** — any open positions not yet closed  

### 2. Save risk config (optional)

Call `save_risk_config` to persist custom limits. Omit fields to keep defaults:

```json
{
  "rValuePoints": 50,
  "rValueDollars": 250,
  "maxDailyLossR": 3,
  "maxConsecutiveLosses": 3,
  "maxTradesPerSession": 8,
  "maxDailyLossDollars": 750
}
```

Defaults: R = 50 pts / $250, max 3R daily loss, 3-loss circuit breaker, 8 trades/session.

### 3. Initialize risk state

Call `init_risk_state` to create the session risk row (0 P&L, 0 trades, no streaks). Do this at session start.

No parameters required.

**Alternative:** Run the one-shot binary to initialize both risk config and risk state:

```bash
cargo run --bin the-desk-init-risk
```

This writes default risk config (R=50pts/$250, max 3R daily, 3-loss circuit breaker) and creates the initial risk state row. Uses the same database as MCP and Tauri (`~/.the-desk/data.db`).

## R Derivation

R is derived from Lucid params:

```
R_dollars = lucid_daily_loss_dollars / max_daily_loss_r
R_points  = R_dollars / 5.00   (NQ: $5 per point per MNQ contract)
```

Example: $750 daily loss, 3R max → R = $250 = 50 NQ points.

## Tauri App

If using the Tauri desktop app, risk config and account state can also be set via:

- `riskBridge.saveConfig(config)` — persist risk config  
- `accountBridge.save(input)` — persist balance and positions  

The MCP server shares the same SQLite database, so changes in either interface are visible to both.
