# The Desk

**The Trading Partner You've Been Missing**

A real-time AI trading co-pilot for discretionary NQ futures traders. Runs alongside Sierra Chart on a second monitor, connects to live market data via the DTC protocol, and provides contextual coaching based on your own playbook, rules, and backtested setups.

## What It Does

- Connects to Sierra Chart via DTC protocol for real-time NQ market data
- Computes market structure in real-time: TPO profiles, delta neutral value areas, VWAP, key levels
- Matches live conditions against your playbook setups (deterministic rules engine)
- Provides contextual coaching prompts via Claude API when your setups trigger
- Tracks your risk state and holds you accountable to your own rules
- Records every session for tape replay and practice
- Helps you review sessions and identify behavioral patterns over time

## What It Doesn't Do

- Place orders or execute trades
- Generate its own trading signals
- Tell you what to trade
- Replace your charting platform

## Tech Stack

- **Tauri 2.x** -- Desktop app framework (Rust backend + web frontend)
- **Rust** -- DTC client, market structure pipelines, rules engine, session recording
- **React + TypeScript + shadcn/ui** -- User interface
- **SQLite** -- Local data storage
- **Claude API** -- LLM coaching layer

## Project Status

**Pre-development** -- Planning and specification phase complete. Ready for implementation.

## Documentation

### Planning Documents

| Document | Description | Status |
|----------|-------------|--------|
| [Product Vision](docs/the-desk-vision.md) | Product vision, philosophy, and target user | Stable |
| [Epic Brief](docs/epic-brief.md) | Problem statement, scope, constraints, success criteria | Stable |
| [Roadmap](docs/roadmap.md) | Phase sequencing, entry/exit criteria, requirements traceability | Stable |

### Phase Requirements

| Document | Description | Status |
|----------|-------------|--------|
| [Phase 1 PRD](docs/phase-1-prd.md) | Live Co-Pilot -- full requirements with IDs and acceptance criteria | Stable |
| [Phase 2 PRD](docs/phase-2-prd.md) | Intelligence Expansion -- structured outline with requirement IDs | Outline |
| [Phase 3 PRD](docs/phase-3-prd.md) | Maturity -- structured outline with requirement IDs | Outline |

### Technical Specifications

| Document | Description | Status |
|----------|-------------|--------|
| [Tech Plan](docs/tech-plan.md) | Architecture, data model, recording format, component design, config schema, testing strategy | Stable |
| [Core Flows](docs/core-flows.md) | 5 user flows with HTML wireframes and interaction principles | Stable |
| [Design Spec](docs/design-spec.md) | UI/UX: layout regions, color system, typography, keyboard shortcuts, accessibility | Stable |
| [Prompt Spec](docs/prompt-spec.md) | LLM prompt templates, context construction, token budgets, quality criteria | Stable |

### Governance

| Document | Description | Status |
|----------|-------------|--------|
| [Decision Log](docs/decision-log.md) | ADR-style record of key architectural and product decisions | Living |
| [CLAUDE.md](CLAUDE.md) | Project rules for all LLM coding agents | Stable |
| [AGENT.md](AGENT.md) | Universal agent instructions and subagent patterns | Stable |

### Domain Knowledge

| Document | Description |
|----------|-------------|
| [Trading Domain](skills/trading-domain/SKILL.md) | TPO theory, delta neutral calculations, order flow, NQ microstructure |
| [DTC Protocol](skills/dtc-protocol/SKILL.md) | Data and Trading Communication protocol reference |
| [Tauri Bridge](skills/tauri-bridge/SKILL.md) | IPC patterns between Rust and React |
| [Compliance](skills/compliance-research/SKILL.md) | Coaching-vs-advisory regulatory positioning |

## Development

This project uses spec-driven development with LLM coding agents. See `AGENT.md` for agent instructions and `CLAUDE.md` for project rules.

```bash
# Coming soon -- project not yet scaffolded
```

## License

Private repository. All rights reserved.
