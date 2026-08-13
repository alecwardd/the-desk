# Engine lifecycle (SIL-M2a / MarketRouter v0)

`the-desk-engine` is the headless **Engine host**: it owns ingest, pipelines, and
event detection so market intelligence survives MCP/agent disconnect.

**MarketRouter** v0 runs **NQ** and **ES** concurrently on one clock (isolated
session scopes per root). Trust Ceiling stays **L3**. This binary never places
or manages orders.

## Topology

| `[sil].engine_mode` | Who owns ingest | MCP role |
| --- | --- | --- |
| `embedded` (default) | `the-desk-mcp` | Today's topology — **embedded-engine fallback** / true rollback |
| `external` | `the-desk-engine` | Thin adapter over the read-only state socket |

```toml
# ~/.the-desk/config.toml
[sil]
catalog_discovery = true
engine_mode = "external"          # or "embedded"
engine_bind = "127.0.0.1:17843"   # optional; default shown
```

## Globex overnight coverage

The engine must run whenever Sierra Chart is recording — including Globex
overnight. Pair this with the existing Sierra Watchdog / Sunday Open tasks in
`Register-DeskTasks.ps1`:

1. Sierra is up (watchdog) and writing `.scid` / `.depth`.
2. `the-desk-engine` is running (Engine Watchdog task) and publishing state.
3. Cursor/`the-desk-mcp` may connect and disconnect freely; ingest continues.

## Launch

```powershell
# Foreground (debug)
cargo run --release --bin the-desk-engine

# Or with explicit bind / fixture SCIDs (NQ primary; optional ES)
the-desk-engine --bind 127.0.0.1:17843 --scid T:\SierraChart\Data\NQU26.scid --scid-es T:\SierraChart\Data\ESU26.scid
```

## Observability (ops / agents)

- `get_feed_health` — includes `engineMode`, `engineAlive`, `engineStallState`
  (`ok` / `behind` / `stalled` / `unavailable`), backlog bytes, and source health.
- `get_raw_tick_ingest_gaps` — historical SCID↔DB gap analysis (unchanged).
- `get_runtime_events` — look for `engine.adapter_degraded` /
  `engine.adapter_recovered` after kill/restart.

When the engine is absent in `external` mode, MCP marks the live coaching path
**degraded** (never silent stale reads). Your playbook / your rules evaluation
should treat live structure as incomplete until the engine returns.

## Kill / recover

1. Stop `the-desk-engine` → MCP adapter sets degraded + records
   `engine.adapter_degraded`.
2. Restart `the-desk-engine` → adapter reconnects, records
   `engine.adapter_recovered`, coaching path resumes.

## SourceProvider

- **FileProvider** — real adapter for `.scid` / `.depth` (default).
- **SierraProvider** — stubbed until ACSIL (#23).
