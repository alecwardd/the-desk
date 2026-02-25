---
description: 
alwaysApply: true
---

# The Desk — Project Rules

These rules apply to ALL LLM coding agents working in this repository. Read fully before writing any code.

---

## What This Project Is

The Desk is a real-time AI trading co-pilot for discretionary NQ futures traders. It runs alongside Sierra Chart on a second monitor, connected via the DTC protocol, and provides contextual coaching based on the trader's own playbook.

**It does NOT place trades, generate signals, or give financial advice.** It reflects the trader's own rules back to them.

---

## Architecture (Mandatory — Never Violate)

The system has three layers. All code must respect this separation:

```
LAYER 1: Deterministic Pipelines (Rust)
  - Processes raw DTC market data into structured signals
  - VWAP, TPO/Market Profile, Delta Neutral VA/Pivot, Key Levels, Risk Tracking
  - Pure math. No LLM calls. No network requests. Sub-second.

LAYER 2: Rules Engine (Rust)
  - Evaluates playbook conditions against Layer 1 signals
  - Deterministic boolean logic. No LLM calls.
  - Fires typed alerts when conditions are met.

LAYER 3: LLM Orchestrator (TypeScript → Claude API)
  - Receives structured alerts from Layer 2
  - Synthesizes context (alert + playbook + risk state + journal history)
  - Produces human-readable coaching prompts
  - 1-2 second latency acceptable here
```

**Rules:**
- Never call the Claude API from Rust code (Layer 1 or 2)
- Never put market data processing in TypeScript (belongs in Rust)
- Never put business logic in UI components (delegate to tauri-bridge.ts)
- The rules engine must work without any network connectivity
- Every coaching prompt must trace to a specific playbook rule — never speculative

---

## Technology Stack

| Layer | Technology | Notes |
|-------|-----------|-------|
| Desktop framework | Tauri 2.x | IPC between Rust and React |
| Backend | Rust | All pipelines, rules engine, DTC client, SQLite, recording |
| Frontend | React 19 + TypeScript | UI only |
| UI components | shadcn/ui + Tailwind CSS | Dark theme, keyboard-first |
| Database | SQLite (rusqlite) | All local data storage |
| LLM | Claude API (Anthropic SDK) | Coaching prompts and analysis |
| Compression | zstd | Session recordings |

---

## Trading Terminology (Must Be Correct)

These terms have precise meanings. Using them incorrectly will produce a broken product.

| Term | Meaning | Common Mistake |
|------|---------|----------------|
| **TPO** | Time Price Opportunity — time spent at a price level | Confusing with volume profile |
| **Value Area** | 70% of TPOs (or volume), calculated outward from POC | Calculating as "middle 70% of range" |
| **POC** | Point of Control — highest TPO (or volume) price level | N/A |
| **DNVA** | Delta Neutral Value Area — 70% of absolute delta | Calculating from raw (signed) delta |
| **DNP** | Delta Neutral Pivot — where cumulative delta crosses zero | Confusing with POC |
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
- Error handling: use `thiserror` for typed errors, convert to `String` at the Tauri command boundary only
- Tests: every pipeline must have unit tests with known NQ data samples

### TypeScript / React

- Functional components with hooks only (no class components)
- All Tauri IPC calls go through `src/lib/tauri-bridge.ts` — never call `invoke()` directly from components
- All Tauri event listeners go in custom hooks under `src/hooks/`
- Use TypeScript strict mode
- State management: React Context for global state, local state for component-specific UI
- No `any` types — define proper interfaces in `src/lib/types.ts`

### Shared

- File names: kebab-case for TypeScript, snake_case for Rust (standard conventions)
- No hardcoded values — configuration goes in `~/.the-desk/config.toml`
- No secrets in code — API keys go in environment or config, never committed
- Every feature must work without the Claude API (graceful degradation to raw alerts)

---

## Never Do List

1. **Never build a backtesting engine.** We import backtest results from external tools.
2. **Never place or manage trades.** The Desk is coaching only.
3. **Never generate proprietary trading signals.** Every alert traces to the trader's own playbook rules.
4. **Never use language like "you should buy/sell" or "this is a good trade."** Always frame as "your rules say..." or "your playbook indicates..."
5. **Never send raw market data to the Claude API.** Send structured summaries only.
6. **Never store API keys in code or config files that get committed.** Use `.env` or system keychain.
7. **Never block the main thread.** Long operations (DTC, LLM API, file I/O) run in background tasks.
8. **Never recalculate entire profiles from scratch on each tick.** All pipeline math is incremental.
9. **Never mix RTH and Globex data in the same calculation** without explicit scoping.
10. **Never skip the rules engine and go directly from pipeline to LLM.** The deterministic layer must always evaluate first.

---

## Skills Reference

Read these before working on related components:

| Skill | When to Read | Path |
|-------|-------------|------|
| Trading Domain | Before implementing any pipeline or playbook logic | `skills/trading-domain/SKILL.md` |
| DTC Protocol | Before working on the data feed client | `skills/dtc-protocol/SKILL.md` |
| Compliance | Before writing UI copy, prompts, or marketing text | `skills/compliance-research/SKILL.md` |
| Tauri Bridge | Before implementing IPC between Rust and React | `skills/tauri-bridge/SKILL.md` |

---

## File Structure

```
the-desk/
├── docs/                           # Planning documentation
│   ├── the-desk-vision.md          # Product vision and philosophy
│   ├── phase-1-prd.md              # Phase 1 requirements (Live Co-Pilot)
│   ├── phase-2-prd.md              # Phase 2 requirements (Intelligence Expansion)
│   ├── phase-3-prd.md              # Phase 3 requirements (Maturity)
│   ├── epic-brief.md               # Epic brief — problem, scope, constraints
│   ├── tech-plan.md                # Architecture, data model, component design
│   ├── core-flows.md               # User flows with wireframes
│   ├── design-spec.md              # UI/UX design specification
│   ├── prompt-spec.md              # LLM prompt engineering specification
│   ├── decision-log.md             # ADR-style key decisions and rationale
│   └── roadmap.md                  # Phase sequencing, traceability, entry/exit criteria
├── agents/                         # Subagent definitions (source of truth)
│   ├── dtc-protocol-researcher.md
│   ├── pipeline-verifier.md
│   ├── prompt-quality-evaluator.md
│   └── options-api-researcher.md
├── commands/                       # Slash commands (source of truth)
├── skills/                         # Domain knowledge (source of truth)
│   ├── trading-domain/SKILL.md
│   ├── dtc-protocol/
│   │   ├── SKILL.md
│   │   └── reference.md            # Wire-level protocol details
│   ├── compliance-research/SKILL.md
│   └── tauri-bridge/
│       ├── SKILL.md
│       ├── examples.md              # Copy-paste IPC patterns
│       └── anti-patterns.md         # Common mistakes with fixes
├── .cursor/                        # Cursor IDE integration
│   ├── agents/   → symlink to ../agents/
│   ├── commands/ → symlink to ../commands/
│   ├── skills/   → symlink to ../skills/
│   └── rules/                      # Cursor-specific rules
├── src-tauri/                      # Rust backend
│   ├── src/
│   │   ├── main.rs
│   │   ├── dtc/                    # DTC protocol client
│   │   ├── pipelines/              # Market structure calculations
│   │   ├── rules/                  # Playbook rules engine
│   │   ├── recording/              # Session recording & replay
│   │   ├── risk/                   # Risk tracking
│   │   └── db/                     # SQLite operations
│   └── Cargo.toml
├── src/                            # React frontend
│   ├── components/                 # UI components (by domain)
│   ├── hooks/                      # Tauri event listeners
│   ├── lib/                        # Shared utilities and types
│   ├── context/                    # React context providers
│   └── App.tsx
├── .githooks/                      # Git hooks (pre-commit checks)
├── CLAUDE.md                       # This file (Claude Code rules)
├── .cursorrules                    # Cursor rules (condensed reference)
├── AGENT.md                        # Universal agent instructions
└── README.md
```

> **Symlink convention:** `agents/`, `commands/`, and `skills/` at root are the single source of truth. `.cursor/` contains symlinks pointing to these root directories so both Claude Code and Cursor read the same files. Edit files in root — changes appear in both tools automatically.

---

## Testing Requirements

- **Pipelines:** Unit tests with known NQ data. Compare VWAP, TPO, delta calculations against manually verified values.
- **Rules engine:** Unit tests for each condition type. Test compound conditions. Test edge cases (no data, session boundary).
- **DTC client:** Integration tests with mock DTC server.
- **LLM coaching:** Snapshot tests for prompt templates. Test graceful degradation when API is down.
- **UI:** Component tests for critical interactions (playbook builder form, session controls).

Run `cargo test` for Rust, `npm test` for TypeScript before every commit.
