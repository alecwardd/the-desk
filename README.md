# The Desk

**Backend Intelligence Platform for Discretionary NQ Futures Traders**

The Desk reads Sierra Chart's `.scid` tick data files, computes market structure and microstructure analytics in real-time, stores everything in SQLite, and exposes the full intelligence layer via MCP (Model Context Protocol) — making any AI agent in Cursor your trading partner.

## How It Works

```
Sierra Chart (.scid files) → Rust Pipeline Engine → SQLite → MCP Server → Cursor Agents
```

1. **Sierra Chart** writes tick data to `.scid` files as part of normal operation
2. **The Desk** tail-reads those files, parsing 40-byte binary records (price, bid, ask, volume, aggressor side)
3. **14 pipeline modules** compute market structure incrementally on every tick
4. **EventDetector** logs ~30 structured market events (level tests, IB extensions, day type changes, etc.)
5. **SQLite** stores raw ticks, computed state, session summaries, market events, signal outcomes, and playbook signals
6. **Research query engine** answers frequency, conditional probability, and distribution questions over historical data
7. **MCP server** exposes 119 MCP tools that any Cursor agent can call for market context, feed diagnostics, setup lifecycle state, and historical research
8. **Specialized subagents** (market structure, order flow, levels, performance) access domain-specific tools and report to the orchestrator
9. **You chat with agents** in Cursor who reference live (1-5s delayed) market data and historical statistics

The primary interaction is via Cursor agents and MCP tools (stdio). There is no desktop or web UI in this repository.

## Ingestion Modes

The Desk intentionally runs three ingestion paths:

1. **Startup warm-backfill (MCP startup):** reads recent `.scid` history to seed in-memory pipeline state quickly.
2. **Historical research backfill (`backfill_history`):** queues a background historical job that streams `.scid` data into `session_summaries` + `market_events` without blocking the MCP server (`get_backfill_status` polls progress, `cancel_backfill` cancels long runs).
3. **Live tail persistence:** polls `.scid` for new records, updates pipelines incrementally, and batch-writes `raw_ticks`.

Use `get_feed_health` and `validate_data_integrity` to confirm feed freshness and integrity before relying on outputs.

## What It Computes

### Market Structure (Layer 1)
- **VWAP** with 1/2/3 standard deviation bands
- **TPO Profile** — value area, POC, single prints, poor highs/lows, excess
- **Delta Profile** — delta neutral value area (DNVA), delta neutral pivot (DNP)
- **Key Levels** — prior day H/L/C, prior VA/POC, overnight range, IB extensions

### Microstructure (Layer 2)
- **Tape Pace** — rolling ticks/sec in 5s/30s/5m windows, pace percentile, dwell time
- **Footprint** — bid/ask volume at price, stacked + diagonal imbalances
- **Absorption / Exhaustion** — high-volume defense, declining-volume moves, delta divergence
- **Trade Size Distribution** — institutional vs retail flow, size-at-price

### PTT Methodology (Layer 3)
- **5-Min Opening Range** — Leo's OR5 high/low/mid, break detection, mid retest tracking
- **Relative Volume (RVOL)** — current vs 20-day average, Low/Normal/Elevated/High classification
- **Day Type Classifier** — Normal, NormalVariation, Neutral, Trend, DoubleDistribution (Dalton)
- **Rebid / Reoffer Zones** — acceleration zone detection, retest tracking, delta confirmation
- **Delta Pinch Detector** — multi-timeframe momentum reversals with severity scoring
- **Session Inventory** — cross-session delta positioning (Building/Clearing/Neutral)

### Rules Engine
- Typed conditions evaluated against `MarketState` on every tick
- 40+ condition field variants covering all pipeline outputs
- Setup state machine: NotActive → Approaching → ConditionsMet → Confirmed → InTrade → Closed
- 9 pre-built setup templates (OR5 Mid Retest, Rebid at Support, Delta Pinch Reversal, etc.)

### Research Infrastructure
- **EventDetector** — logs ~30 structured event types (level tests, IB extensions, day type changes, new session highs/lows, poor highs/lows, excess, RVOL spikes, DNP crosses)
- **Session Summaries** — end-of-session snapshots with 35+ fields (OHLC, IB range, day type, delta, close vs key levels)
- **Signal Outcomes** — tracks MFE/MAE/R-result after playbook signals fire
- **Query Engine** — frequency, conditional probability, distribution, and session comparison queries
- **Backfill Pipeline** — process historical .scid data through all pipelines to build the research database

## What It Does NOT Do

- Place or manage trades
- Generate proprietary signals
- Give financial advice ("your playbook says..." not "you should buy/sell...")
- Require any additional data subscriptions beyond what Sierra Chart already provides

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Pipeline engine | Rust (incremental, sub-millisecond per tick) |
| Database | SQLite (rusqlite) |
| MCP server | `rmcp` crate, stdio transport |
| Data source | Sierra Chart `.scid` files (binary, 40-byte records) |
| Compression | zstd (cold storage archival) |

## Project Structure

```
the-desk/
├── Cargo.toml                  # Rust package (default-run: the-desk-mcp)
├── src/
│   ├── bin/the-desk-mcp.rs     # MCP server binary
│   ├── lib.rs                  # Module exports
│   ├── backfill.rs             # Historical .scid backfill engine
│   ├── research/mod.rs         # Query engine (frequency, conditional, distribution)
│   ├── pipelines/              # 14 pipeline modules + event detector
│   │   ├── mod.rs              # PipelineEngine, MarketState
│   │   ├── event_detector.rs   # Structured event detection (~30 event types)
│   │   ├── vwap.rs             # VWAP + std dev bands
│   │   ├── tpo.rs              # TPO profile, VA, POC, single prints
│   │   ├── delta.rs            # Delta profile, DNVA, DNP
│   │   ├── levels.rs           # Key levels, IB extensions, proximity
│   │   ├── tape_pace.rs        # Tape speed, pace percentile, dwell
│   │   ├── footprint.rs        # Volume at price, imbalances
│   │   ├── absorption.rs       # Absorption, exhaustion, divergence
│   │   ├── trade_size.rs       # Trade size distribution
│   │   ├── opening_range_5min.rs  # Leo's 5-min OR
│   │   ├── rvol.rs             # Relative volume
│   │   ├── day_type.rs         # Profile shape / day type
│   │   ├── rebid_reoffer.rs    # Acceleration zones
│   │   ├── pinch.rs            # Delta momentum reversals
│   │   └── session_inventory.rs   # Cross-session positioning
│   ├── rules/
│   │   ├── mod.rs              # Rules engine + condition evaluator
│   │   └── setup_templates.rs  # 9 pre-built playbook templates
│   ├── feed/
│   │   ├── mod.rs              # FeedEvent, FeedConfig, StorageConfig
│   │   └── scid_reader.rs      # .scid binary file parser
│   ├── db/mod.rs               # SQLite schema + operations
│   ├── risk/mod.rs             # Risk state tracking
│   ├── recording/mod.rs        # Session recording + replay
├── skills/                     # Domain knowledge for agents
│   ├── trading-domain/SKILL.md # TPO, delta, PTT methodology
│   └── compliance-research/    # Coaching vs advisory positioning
├── docs/dom-replay.md          # Note on removed DOM visualizer (MCP depth tools remain)
├── agents/                     # Cursor agent definitions
├── CLAUDE.md                   # Project rules for all agents
└── AGENT.md                    # Agent workflow instructions
```

## Getting Started

### Prerequisites

- **Sierra Chart** with an active data feed (Rithmic, CQG, etc.)
- **Rust** toolchain (rustup)
- **Cursor IDE** with MCP support

### Configuration

Create `~/.the-desk/config.toml`:

```toml
[feed]
sierra_data_dir = "D:\\SierraChart\\Data"
base_symbol = "NQ"
symbol_mode = "hybrid"
symbol = "NQH26.CME"
active_symbol_override = "NQH26.CME"
flush_poll_ms = 1000

[storage]
warm_retention_days = 30
cold_archive_dir = "T:\\TheDesk\\archive"
auto_archive = true
```

### MCP Server

The MCP server is configured in `.cursor/mcp.json`.
`target_alt` is an alternative build output directory (`CARGO_TARGET_DIR`) to avoid cargo lock conflicts when the MCP server binary is running while a separate `cargo build` is in progress.

```json
{
    "mcpServers": {
      "the-desk": {
      "command": "c:\\the-desk\\target_alt\\release\\the-desk-mcp.exe",
      "args": []
    }
  }
}
```

Once running, any Cursor agent can call tools like `get_market_snapshot`, `get_day_type`, `get_pinch_events`, `check_delta_confirmation`, `get_proximity_report`, etc.

### Development

```bash
# From repository root — run all tests (pipelines, rules, db, research, MCP helpers, etc.)
cargo test

# Check compilation
cargo check

# Run end-to-end golden replay verification
cargo test --test session_replay_golden

# Build MCP server (release)
cargo build --release --bin the-desk-mcp

# Queue a historical backfill via MCP, then poll `get_backfill_status`

# Run backfill from CLI (no MCP needed — useful for weekend prep)
cargo run --bin the-desk-backfill -- --start 2026-03-02 --end 2026-03-06 --run-rules
# Or load all available: cargo run --bin the-desk-backfill -- --run-rules
```

Historical jobs are asynchronous:

1. Call `backfill_history` or `run_backtest`
2. Poll `get_backfill_status(jobId)`
3. Inspect the final `result` when status is `completed`
4. Call `cancel_backfill(jobId)` to stop a long-running replay safely

Golden replay verification lives in `tests/session_replay_golden.rs`. It generates a
small synthetic SCID fixture, runs the real historical backfill path, and compares
canonical core session/event output, rules-enabled signals/outcomes, and non-monotonic
timestamp behavior to `tests/fixtures/session_replay/v1/*.json`.
Use `THE_DESK_BLESS_GOLDENS=1` only after intentional pipeline changes have been
reviewed. Private real-data regressions can be run with `THE_DESK_GOLDEN_SCID_DIR`,
`THE_DESK_GOLDEN_EXPECTED_DIR`, and `cargo test --test session_replay_golden -- --ignored`.

## Data Flow & Latency

```
Sierra Chart flush:     ~1000ms  (Intraday File Flush Time in SC settings)
Rust poll + parse:      ~500ms
Pipeline compute:       ~5ms     (14 pipelines, incremental)
MCP tool response:      ~50ms
Agent reasoning:        ~3-8s    (model-dependent)
────────────────────────────────────────────────────
Data available via MCP: ~1.5s behind reality
Full prompt-to-answer:  ~5-10s
```

Designed for directional trading with 15-minute to 1-hour holds — not HFT.

## Storage

- **Hot (current session):** all ticks in SQLite, full pipeline state in memory, live event detection
- **Warm (past 30 days):** ticks + snapshots in SQLite, fully queryable, session summaries + events
- **Cold (30+ days):** zstd-compressed monthly archives, session summaries retained in SQLite

Runtime state lives at `~/.the-desk` by default. On Windows, this can be moved to a larger local drive with a directory junction, for example:

```powershell
C:\Users\<user>\.the-desk -> T:\TheDesk\state
```

Recommended local layout for data-heavy installs:

```text
T:\TheDesk\
  state\        # data.db, config.toml, WAL/SHM files
  archive\      # compressed cold raw-tick archives
  backups\      # manual/automated database snapshots
  build-cache\  # optional Cargo target dir
  temp\         # SQLite temp files during maintenance
```

Use the storage maintenance binary outside market hours:

```powershell
# Keep build artifacts off C: during maintenance runs
$env:CARGO_TARGET_DIR = "T:\TheDesk\build-cache"

# Inspect current warm/cold status
cargo run --bin the-desk-storage -- --status

# Archive raw ticks older than the configured warm window
cargo run --bin the-desk-storage -- --archive

# Attempt physical SQLite compaction after archiving.
# This can take hours on large DBs and needs substantial free space on T:.
cargo run --bin the-desk-storage -- --vacuum
```

Archiving removes old `raw_ticks` rows from SQLite after writing compressed `.csv.zst` files to `cold_archive_dir`. Session summaries, market events, signal outcomes, journal/risk records, and research metadata remain in SQLite. `VACUUM` is optional physical compaction; stop it if the target drive approaches the free-space safety floor.

### Research Database (SQLite)

| Table | Purpose | Populated by |
|-------|---------|-------------|
| `market_events` | ~30 event types with timestamp, price, metadata | Live EventDetector + backfill |
| `session_summaries` | End-of-session snapshots plus contract metadata / rollover safety flags | Live processing + backfill |
| `signal_outcomes` | MFE/MAE/R-result per playbook signal | Rules engine + manual resolution + replay jobs |
| `historical_job_runs` | Durable ledger for queued/running/completed historical jobs | `backfill_history` / `run_backtest` |

### Contract Rollover

The Desk now resolves the active futures contract with a hybrid manual-plus-auto policy. `get_feed_health` exposes the resolved `contractSymbol`, whether it came from a manual override or auto-detection, and any rollover warnings. Live snapshots and key-level tools also expose `carryForwardLevelsValid` so prior-day references from an old contract are obvious instead of silent.

## License

Private repository. All rights reserved.
