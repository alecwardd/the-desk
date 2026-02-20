---
name: replay-session
description: Load and replay a recorded session for testing. USE WHEN testing pipelines, rules engine, or coaching prompts against real recorded market data.
---

# /replay-session

Load a recorded NQ session and replay it through the full system.

## Usage

```
/replay-session                    # List available recordings
/replay-session [filename]         # Replay specific session
/replay-session [filename] --speed 4x   # Replay at 4x speed
```

## Steps

1. List available recordings from `~/.the-desk/recordings/`:
   ```bash
   ls -la ~/.the-desk/recordings/*.zst
   ```

2. If a specific session is requested, load it:
   ```bash
   # Via Tauri command — the Rust replay engine handles decompression and playback
   ```

3. During replay, monitor:
   - Pipeline outputs (VWAP, TPO, delta building correctly)
   - Rules engine alerts (firing at expected times)
   - Coaching prompts (appropriate content and timing)
   - Performance (can we replay at 8x without dropping data?)

4. Report replay summary:
   - Session date and duration
   - Number of trades processed
   - Alerts fired and coaching prompts generated
   - Any errors or anomalies

## For Development

If no recordings exist yet, use the mock DTC server to generate synthetic data:
```bash
cargo run --bin mock-dtc-server -- --record synthetic-session.zst --duration 3600
```
