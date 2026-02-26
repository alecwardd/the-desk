# Sierra Chart Settings Reference

Settings for Sierra Chart integration with The Desk. These are the recommended/current values for NQ futures trading via Rithmic.

**Last verified:** 2026-02-26

---

## Data/Trade Service Settings

| Setting | Value | Notes |
|---------|-------|-------|
| **Intraday Data Storage Time Unit** | 1 Tick | Required. Gives individual trades with bid/ask volume for delta, footprint, and tape pace pipelines. |
| **1-Tick Historical Data Days** | 186 days | ~6 months of tick history. Supports RVOL 20-day lookback and backtesting. |

## General Settings

| Setting | Value | Notes |
|---------|-------|-------|
| **Chart Update Interval (ms)** | 600 | Controls how often SC flushes data to `.scid` files. Combined with The Desk's 1000ms poll interval, gives ~1.6s worst-case data latency. Fine for directional trading (15-min to 1-hr holds). Could lower to 300-400ms for faster updates; below 200ms wastes CPU. |
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
symbol = "NQH6.CME"          # Update on contract rollover (quarterly)
flush_poll_ms = 1000          # How often The Desk checks for new .scid data
price_scale = 100.0           # Rithmic NQ prices are raw * 100
```

---

## Latency Budget

```
Sierra Chart flush:     ~600ms   (Chart Update Interval)
The Desk poll:          ~1000ms  (flush_poll_ms in config.toml)
Pipeline compute:       ~5ms     (14 pipelines, incremental)
────────────────────────────────────────────────────────
Data available via MCP: ~1.6s behind reality (worst case)
                        ~0.8s average
```

---

## Contract Rollover Checklist

When NQ rolls to a new quarterly contract:

1. Note the new symbol in Sierra Chart (e.g., `NQH6.CME` → `NQM6.CME`)
2. Update `symbol` in `~/.the-desk/config.toml`
3. Rebuild the MCP server: `cd src-tauri && cargo build --release --bin the-desk-mcp`
4. Restart Cursor or reload the MCP server

NQ quarterly months: H (Mar), M (Jun), U (Sep), Z (Dec).
