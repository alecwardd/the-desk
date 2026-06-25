# Sierra Chart Settings Reference

Settings for Sierra Chart integration with The Desk. These are the recommended/current values for NQ futures trading via Rithmic.

**Last verified:** 2026-02-26 · **Updated 2026-06-25:** added multi-contract recording + live-recording fidelity guidance.

> **Multi-contract recording (since 2026-06-23):** The Desk now records four symbols — **NQ, MNQ, ES, MES** — for cross-instrument flow agreement (see `docs/multi-instrument-flow-architecture.md`). Every fidelity setting below (especially **Intraday Data Storage Time Unit = 1 Tick**) must be in effect for **all four charts**, not just NQ. The Data/Trade Service settings are global in Sierra Chart, so setting them once normally covers every symbol — but confirm each new chart is actually storing 1-tick data (open the chart → Chart Settings, or verify the `.scid` is growing during RTH).

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
| **Chartbook to open on startup** | `LightweightChartBook2026.Cht` | Belt-and-suspenders for the watchdog: Sierra should reopen the live recording chartbook even after an abnormal exit. |

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
symbol = "NQU6.CME"          # Legacy fallback; still honored
active_symbol_override = "NQU6.CME" # Set only when you want to pin a contract
flush_poll_ms = 100           # How often The Desk checks for new .scid data
price_scale = 100.0           # Rithmic NQ prices are raw * 100

[storage]
warm_retention_days = 30
cold_archive_dir = "X:\\TheDesk\\archive"
auto_archive = true           # Vestigial; scheduled task performs archival
```

The actual storage automation is handled by Windows Task Scheduler through `scripts\ops\Run-Weekly-Archive.ps1`; `auto_archive` is retained for config compatibility but is not acted on by the runtime. See `docs/ops/automation-and-storage.md`.

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

## Live Recording & Data Fidelity (weekly cadence)

**How recording actually works — the short version: the `.scid` is the source of truth, and it is written *continuously while Sierra Chart runs*, not "saved on shutdown."** Every trade tick is appended to the symbol's `.scid` file as it arrives (buffered, then flushed to disk per **Intraday File Flush Time in Milliseconds**, ~1000 ms). So at any moment, everything up to ~1 second ago is already durably on disk in `T:\SierraChart\Data`. There is no "save" step you can forget. Shutting Sierra Chart down does not write the data — it's already written.

**Your routine (Globex open → weekend) is correct.** Running continuously is exactly what produces the highest-fidelity, gap-free tape. Gaps come from only two things, neither of which is "forgetting to save":

1. **Sierra Chart not running** — any minute SC is closed is a minute of no recording (a true hole in the `.scid`). Keeping it up all week avoids this.
2. **Feed disconnect while running** — if Rithmic drops (outage, network blip, nightly maintenance ~5–6 PM ET), SC keeps running but receives nothing, leaving a gap for that span. Watch for these; **FIX Logging = On** captures the disconnect/reconnect in the logs. These feed-side gaps are the same class of holes that made the historical NQH6 backfill a poor backtest judge — which is exactly why clean *forward* recording matters.

**To maximize fidelity, in priority order:**

1. **Confirm 1-Tick storage on all four charts (NQ, MNQ, ES, MES).** `Intraday Data Storage Time Unit = 1 Tick` is the single most important setting — anything coarser (1 second) permanently discards individual-trade granularity and breaks delta/footprint/tape-pace. This is the one to never get wrong.
2. **Keep Sierra Chart running continuously** Globex open → weekend (you already do this).
3. **Configure Sierra startup chartbook** to open `LightweightChartBook2026.Cht`. The watchdog launches Sierra, and Sierra should deterministically reopen the live recording chartbook.
4. **Clean shutdown on the weekend** (File → Exit / Disconnect, not a hard kill / Task Manager / power loss). A clean exit lets the final buffer flush and avoids a torn last record. Data already flushed is safe regardless, but a clean exit is tidier.
5. **Watch for feed disconnects** during the week. After any reconnect, optionally run `get_raw_tick_ingest_gaps` / `scan_scid_timestamp_anomalies` to see if a hole formed, and `get_feed_health` to confirm the feed is live.
6. **Leave the OS/data drive (`T:`) with headroom.** `.scid` files grow continuously; a full disk stops recording. (Archival of The Desk's *own* SQLite copy is automated by the weekly archive task; it does not touch your Sierra `.scid` files.)

**The Desk's role in all this: read-only over the `.scid`.** The MCP backend *tails* the `.scid` files (poll every `flush_poll_ms`) and keeps its own SQLite copy of the ticks (`raw_ticks`) for research/backtests. It never writes to or alters the Sierra `.scid` — so The Desk being up or down has **zero** effect on your recording fidelity. If The Desk is off for a stretch, the `.scid` still captures everything and a later `backfill_history` / `ingest_raw_ticks_from_scid` can replay it into the DB. Sierra is the recorder; The Desk is the reader.

**Weekend sanity check (optional, 30 seconds):** before shutting down Friday, confirm each of the four `.scid` files in `T:\SierraChart\Data` has a recent modified-time and a sensible size for a full week. That's the fastest visual confirmation that all four symbols recorded all week.

---

## Contract Rollover Checklist

When NQ rolls to a new quarterly contract:

1. Note the new symbol in Sierra Chart (e.g., `NQH6.CME` → `NQM6.CME`)
2. If you want to pin the new front month immediately, update `active_symbol_override` in `~/.the-desk/config.toml`
3. Restart the backend/MCP process so the resolved contract metadata refreshes
4. Call `get_feed_health` and confirm `contractSymbol`, `symbolResolutionSource`, and `warnings`
5. Run a historical backfill for the new contract if you want fresh per-contract research coverage

NQ quarterly months: H (Mar), M (Jun), U (Sep), Z (Dec).
