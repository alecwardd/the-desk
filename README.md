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
7. **MCP server** exposes 33 tools that any Cursor agent can call for market context and historical research
8. **Specialized subagents** (market structure, order flow, levels, performance) access domain-specific tools and report to the orchestrator
9. **You chat with agents** in Cursor who reference live (1-5s delayed) market data and historical statistics

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
| Desktop frame | Tauri 2.x (optional visualization layer) |
| Frontend | React 19 + TypeScript + shadcn/ui (optional) |

## Project Structure

```
the-desk/
├── src-tauri/src/
│   ├── bin/the-desk-mcp.rs     # MCP server binary (33 tools)
│   ├── main.rs                 # Tauri app entry + processing loop
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
│   ├── db/mod.rs               # SQLite schema (V4) + operations
│   ├── risk/mod.rs             # Risk state tracking
│   ├── recording/mod.rs        # Session recording + replay
│   └── dtc/                    # DTC protocol client (legacy)
├── skills/                     # Domain knowledge for agents
│   ├── trading-domain/SKILL.md # TPO, delta, PTT methodology
│   ├── dtc-protocol/           # DTC protocol reference
│   ├── compliance-research/    # Coaching vs advisory positioning
│   └── tauri-bridge/           # IPC patterns
├── agents/                     # Cursor agent definitions (10 agents)
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
symbol = "NQ 03-26"
flush_poll_ms = 1000
```

### MCP Server

The MCP server is configured in `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "the-desk": {
      "command": "cargo",
      "args": ["run", "--release", "--bin", "the-desk-mcp"],
      "cwd": "src-tauri"
    }
  }
}
```

Once running, any Cursor agent can call tools like `get_market_snapshot`, `get_day_type`, `get_pinch_events`, `check_delta_confirmation`, `get_proximity_report`, etc.

### Development

```bash
# Run all tests (89 tests across pipelines, event detector, rules, db, research, dtc, recording)
cd src-tauri && cargo test

# Check compilation
cd src-tauri && cargo check

# Build MCP server (release)
cd src-tauri && cargo build --release --bin the-desk-mcp

# Backfill historical data (run via MCP tool `backfill_history`)
```

## Data Flow & Latency

```
Sierra Chart flush:     ~1000ms  (configurable via SC settings)
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

~250K ticks/day for NQ. ~1.5-2 GB total after a year including warm + cold tiers.

### Research Database (SQLite)

| Table | Purpose | Populated by |
|-------|---------|-------------|
| `market_events` | ~30 event types with timestamp, price, metadata | Live EventDetector + backfill |
| `session_summaries` | End-of-session snapshots (35+ fields) | Live processing + backfill |
| `signal_outcomes` | MFE/MAE/R-result per playbook signal | Rules engine + manual resolution |

## License

Private repository. All rights reserved.
