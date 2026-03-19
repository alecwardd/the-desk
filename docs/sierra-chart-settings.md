# Sierra Chart Settings Reference

Settings for Sierra Chart integration with The Desk. These are the recommended/current values for NQ futures trading via Rithmic.

**Last verified:** 2026-02-26

---

## Data/Trade Service Settings

| Setting | Value | Notes |
|---------|-------|-------|
| **Intraday Data Storage Time Unit** | 1 Tick | Required. Gives individual trades with bid/ask volume for delta, footprint, and tape pace pipelines. |
| **1-Tick Historical Data Days** | 186 days | ~6 months of tick history. Supports RVOL 20-day lookback and backtesting. |
| **Intraday File Flush Time in Milliseconds** | 1000 (or 0 default) | Primary setting controlling how buffered intraday data is flushed to `.scid` on disk. Lower values reduce latency but increase I/O overhead. |

## General Settings

| Setting | Value | Notes |
|---------|-------|-------|
| **Chart Update Interval (ms)** | 600 | UI/chart refresh cadence. Helpful for display responsiveness, but disk flush behavior for `.scid` is primarily governed by Intraday File Flush Time. |
| **Number of Stored Time & Sales Records** | 4000 | SC display setting only — does not affect `.scid` file storage or The Desk. |
| **Maximum Time & Sales Depth Levels** | 0 | DOM depth recording. Set to 0 (disabled) since The Desk pipelines use tick-level data, not order book depth. Increase to 10 if DOM imbalance analysis is added later. |

## Logging

| Setting | Value | Notes |
|---------|-------|-------|
| **FIX Logging** | On | Logs raw protocol messages between SC and Rithmic. Useful for debugging data feed issues. Not read by The Desk directly. |

## Data Feed

| Setting | Value | Notes |
|---------|-------|-------|
| **Data Feed** | Rithmic Direct - DTC | Primary trading feed via prop firm. |
| **Data Directory** | `T:\SierraChart\Data` | Separate drive from OS for I/O performance and data integrity. |

---

## The Desk Config

These Sierra Chart settings correspond to the following `~/.the-desk/config.toml` values:

```toml
[feed]
sierra_data_dir = "T:\\SierraChart\\Data"
base_symbol = "NQ"
symbol_mode = "hybrid"       # manual | auto | hybrid
symbol = "NQH6.CME"          # Legacy fallback; still honored
active_symbol_override = "NQH6.CME" # Set only when you want to pin a contract
flush_poll_ms = 1000          # How often The Desk checks for new .scid data
price_scale = 100.0           # Rithmic NQ prices are raw * 100
```

---

## Latency Budget

```
Sierra Chart flush:     ~600-1000ms (Intraday File Flush Time)
The Desk poll:          ~1000ms  (flush_poll_ms in config.toml)
Pipeline compute:       ~5ms     (14 pipelines, incremental)
────────────────────────────────────────────────────────
Data available via MCP: ~1.6s behind reality (worst case)
                        ~0.8s average
```

For runtime verification, use MCP tools `get_feed_health` and `validate_data_integrity`.

---

## Contract Rollover Checklist

When NQ rolls to a new quarterly contract:

1. Note the new symbol in Sierra Chart (e.g., `NQH6.CME` → `NQM6.CME`)
2. If you want to pin the new front month immediately, update `active_symbol_override` in `~/.the-desk/config.toml`
3. Restart the backend/MCP process so the resolved contract metadata refreshes
4. Call `get_feed_health` and confirm `contractSymbol`, `symbolResolutionSource`, and `warnings`
5. Run a historical backfill for the new contract if you want fresh per-contract research coverage

NQ quarterly months: H (Mar), M (Jun), U (Sep), Z (Dec).
