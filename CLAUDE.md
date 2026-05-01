---
description: 
alwaysApply: true
---

# The Desk — Project Rules

These rules apply to ALL LLM coding agents working in this repository. Read fully before writing any code.

---

## What This Project Is

The Desk is a backend intelligence platform for discretionary NQ futures traders. It reads Sierra Chart's `.scid` tick data files, computes market structure and microstructure analytics in Rust, stores everything in SQLite, and exposes the intelligence layer via MCP (Model Context Protocol) — making any Cursor agent a trading partner.

**It does NOT place or execute trades.** It is a trading partner — grounded in the trader's playbook and live market structure data. It can share opinions, flag concerns, and offer its read on the market, but the trader always makes the final call.

**Primary interface:** AI agents in Cursor (and Claude Code, Codex). This repository is backend-only: Rust, SQLite, and MCP.

---

## Architecture (Mandatory — Never Violate)

The system has three layers. All code must respect this separation:

```
LAYER 1: Deterministic Pipelines (Rust)
  - Reads .scid tick data and computes structured market intelligence
  - 14 pipeline modules: VWAP, TPO, Delta, Levels, Tape Pace, Footprint,
    Absorption, Trade Size, OR5, RVOL, Day Type, Rebid/Reoffer, Pinch, Session Inventory
  - Pure math. No LLM calls. No network requests. Sub-millisecond.

LAYER 2: Rules Engine (Rust)
  - Evaluates playbook conditions against Layer 1 signals
  - 40+ typed condition fields. Deterministic boolean logic. No LLM calls.
  - Fires typed alerts when conditions are met.
  - 9 pre-built setup templates from PTT methodology.

LAYER 2.5: Research Infrastructure (Rust)
  - EventDetector: detects structured market events (level tests, extensions, day type changes)
  - Backfill pipeline: processes historical .scid data through all pipelines
  - Query engine: frequency, conditional probability, distribution analysis
  - Signal outcomes: tracks MFE/MAE/R-result after signals fire
  - Pure math over historical data. No LLM calls.

LAYER 3: MCP Server + LLM Orchestration
 - 119 MCP tools expose pipeline state, rules evaluation, setup lifecycle state, research queries, and data queries
  - Cursor agents call tools for market context during conversation
  - Claude API synthesizes coaching from structured data (1-5s latency acceptable)
```

**Rules:**
- Never call the Claude API from Rust code (Layer 1 or 2)
- Never put market data processing outside the Rust crates (belongs in Rust)
- The rules engine must work without any network connectivity
- Coaching prompts should reference playbook rules and live data — opinions are welcome but must be grounded
- MCP tools return structured data only — never raw tick streams

---

## Technology Stack

| Component | Technology | Notes |
|-----------|-----------|-------|
| Pipeline engine | Rust | 14 incremental pipeline modules, sub-ms per tick |
| Rules engine | Rust | Typed conditions, setup state machine |
| MCP server | `rmcp` crate | 119 MCP tools via stdio transport |
| Data source | Sierra Chart `.scid` | Binary tick data, 40-byte records |
| Database | SQLite (rusqlite) | Raw ticks, computed state, session history |
| Compression | zstd | Cold storage archival |
| LLM | Claude API | Coaching prompts via Cursor agents |

---

## Trading Terminology (Must Be Correct)

These terms have precise meanings. Using them incorrectly will produce a broken product.

| Term | Meaning | Common Mistake |
|------|---------|----------------|
| **TPO** | Time Price Opportunity — time spent at a price level | Confusing with volume profile |
| **Value Area** | 70% of TPOs (or volume), calculated outward from POC | Calculating as "middle 70% of range" |
| **POC** | Point of Control — highest TPO (or volume) price level | N/A |
| **DNVA** | Delta Neutral Value Area — 70% of absolute delta | Calculating from raw (signed) delta |
| **DNP** | Delta Neutral Pivot — midpoint of DNVA high and low | Confusing with POC or delta zero-crossing |
| **Delta** | Buy volume minus sell volume at a price level | Forgetting to classify trade direction |
| **Single Prints** | TPO levels with exactly one letter — initiative activity | Confusing with low-volume levels |
| **IB** | Initial Balance — first 60 minutes of RTH range | Confusing with Opening Range (30 min) |
| **OR** | Opening Range — first 30 minutes of RTH range | Confusing with IB |
| **R** | Risk unit — trader-defined amount risked per trade | Using fixed point value |
| **RTH** | Regular Trading Hours — 9:30 AM to 4:15 PM ET | Using wrong times |
| **NQ tick** | 0.25 points = $5.00 per contract | Using 0.01 or 1.0 |

**Reference skill:** `skills/trading-domain/SKILL.md` — read this before implementing any pipeline.

---

## Code Conventions

### Rust

- Use `tokio` for async runtime
- Use `serde` with `#[serde(rename_all = "camelCase")]` for types that cross the IPC boundary
- All pipeline calculations must be incremental (add new data, don't recalculate from scratch)
- All public functions must have doc comments
- Error handling: use `thiserror` for typed errors, convert to `String` at MCP tool / CLI boundaries only
- Tests: every pipeline must have unit tests with known NQ data samples

### Shared

- File names: snake_case for Rust (standard conventions)
- No hardcoded values — configuration goes in `~/.the-desk/config.toml`
- No secrets in code — API keys go in environment or config, never committed
- Every feature must work without the Claude API (graceful degradation to raw alerts)

---

## Never Do List

1. **The Desk includes a deterministic market structure research module.** It logs structured events during pipeline processing, tracks signal outcomes, and answers historical queries (frequencies, conditional probabilities, distributions). It does NOT simulate order fills with slippage models -- it reports what actually happened in the market relative to computed levels.
2. **Never place or manage trades.** The Desk is coaching only.
3. **Never generate proprietary trading signals.** Every alert traces to the trader's own playbook rules.
4. **Ground opinions in data and playbook rules.** Sharing a market read or saying "I like this" / "I'd be cautious here" is encouraged — but always tie it back to what the structure and the trader's rules show. The trader makes the final call.
5. **Never send raw market data to the Claude API.** Send structured summaries only.
6. **Never store API keys in code or config files that get committed.** Use `.env` or system keychain.
7. **Never block the main thread.** Long operations (feed I/O, LLM API, file I/O) run in background tasks.
8. **Never recalculate entire profiles from scratch on each tick.** All pipeline math is incremental.
9. **Never mix RTH and Globex data in the same calculation** without explicit scoping.
10. **Never skip the rules engine and go directly from pipeline to LLM.** The deterministic layer must always evaluate first.

---

## Skills Reference

Read these before working on related components:

| Skill | When to Read | Path |
|-------|-------------|------|
| Trading Domain | Before implementing any pipeline or playbook logic | `skills/trading-domain/SKILL.md` |
| Sierra SCID / feed | Before working on `.scid` tailing, symbol resolution, or `.depth` | `skills/trading-domain/SKILL.md` + `src/feed/` |
| Compliance | Before writing prompts or marketing text | `skills/compliance-research/SKILL.md` |

---

## File Structure

```
the-desk/
├── Cargo.toml                        # Rust package manifest (default-run: the-desk-mcp)
├── src/                              # Library + binaries
│   ├── lib.rs                        # Crate root (`the_desk_backend`)
│   ├── bin/the-desk-mcp.rs           # MCP server binary (113 tools)
│   ├── backfill.rs                   # Historical .scid backfill engine
│   ├── research/mod.rs               # Query engine (frequency, conditional, distribution)
│   ├── pipelines/                    # 14 pipeline modules + event detector
│   │   ├── mod.rs                    # PipelineEngine, MarketState
│   │   ├── event_detector.rs         # Structured event detection layer
│   │   ├── vwap.rs                   # VWAP + std dev bands
│   │   ├── tpo.rs                    # TPO profile, VA, POC, single prints
│   │   ├── delta.rs                  # Delta profile, DNVA, DNP
│   │   ├── levels.rs                 # Key levels, IB extensions, proximity
│   │   ├── tape_pace.rs             # Tape speed, percentile, dwell
│   │   ├── footprint.rs             # Volume at price, imbalances
│   │   ├── absorption.rs            # Absorption, exhaustion, divergence
│   │   ├── trade_size.rs            # Trade size distribution
│   │   ├── opening_range_5min.rs    # Leo's 5-min Opening Range
│   │   ├── rvol.rs                  # Relative volume
│   │   ├── day_type.rs              # Day type classifier
│   │   ├── rebid_reoffer.rs         # Acceleration zones
│   │   ├── pinch.rs                 # Delta momentum reversals
│   │   └── session_inventory.rs     # Cross-session positioning
│   ├── rules/                        # Playbook rules engine
│   │   ├── mod.rs                    # Condition evaluator (40+ fields)
│   │   └── setup_templates.rs        # 9 pre-built PTT setup templates
│   ├── feed/                         # Data ingestion
│   │   ├── mod.rs                    # FeedEvent, FeedConfig
│   │   └── scid_reader.rs           # .scid binary file parser
│   ├── db/mod.rs                     # SQLite schema + operations
│   ├── risk/mod.rs                   # Risk state tracking
│   └── recording/mod.rs             # Session recording + replay
├── docs/
│   ├── decision-log.md               # ADR-style decisions (living)
│   └── archive/v0-tauri-gui/         # Pre-pivot planning docs (reference)
├── agents/                           # Cursor agent definitions
├── skills/                           # Domain knowledge for agents
│   ├── trading-domain/SKILL.md       # TPO, delta, PTT methodology
│   └── compliance-research/          # Coaching vs advisory
├── .cursor/                          # Cursor IDE integration
│   ├── mcp.json                      # MCP server config
│   ├── agents/ → ../agents/
│   └── skills/ → ../skills/
├── CLAUDE.md                         # This file (project rules)
├── AGENT.md                          # Agent workflow instructions
├── .cursorrules                      # Cursor-specific quick reference
└── README.md                         # Project overview
```

> **Symlink convention:** `agents/` and `skills/` at root are the single source of truth. `.cursor/` contains symlinks so Cursor reads the same files. Edit files in root.

---

## Testing Requirements

- **Pipelines:** Unit tests with known NQ data. Compare VWAP, TPO, delta calculations against manually verified values.
- **Rules engine:** Unit tests for each condition type. Test compound conditions. Test edge cases (no data, session boundary).
- **MCP server:** Tool response format validation, database access under concurrent calls.
- **Feed:** .scid parser tests with known binary data, session boundary detection.
- **Data integrity:** Cross-pipeline invariant checks (POC in VA, VA = 70% of TPOs, delta sum, DNVA within range).

Run `cargo test` before every commit.
