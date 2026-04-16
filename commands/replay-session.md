---
name: replay-session
description: Work with recorded sessions and historical data for testing. USE WHEN validating pipelines, rules, or research against stored ticks.
---

# /replay-session

Session recordings are compressed files under `~/.the-desk/recordings/`. There is **no** in-repo GUI replay.

## Steps

1. List available recordings:
   ```bash
   ls -la ~/.the-desk/recordings/*.zst
   ```

2. Use **historical backfill** and **research queries** (`backfill_history`, `run_backtest`, `query_*` tools) to validate pipelines against `.scid` history in the configured Sierra data directory.

3. During analysis, monitor pipeline outputs, rules alerts, and DB/session integrity (`validate_data_integrity`, `get_feed_health`).

4. Report summary: session coverage, alerts, anomalies.

## Notes

- The `recording` Rust module and DB `recording_path` fields remain for file-based session artifacts.
- For ladder-specific review, use Sierra Chart or external tools; MCP still exposes DOM-oriented **summaries** from SQLite / `.depth` where implemented.
