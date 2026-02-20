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

- **Tauri 2.x** — Desktop app framework (Rust backend + web frontend)
- **Rust** — DTC client, market structure pipelines, rules engine, session recording
- **React + TypeScript + shadcn/ui** — User interface
- **SQLite** — Local data storage
- **Claude API** — LLM coaching layer

## Project Status

**Pre-development** — Vision and planning phase. See `/docs` for documentation.

## Documentation

| Document | Description |
|----------|-------------|
| [Vision](docs/the-desk-vision.md) | Product vision and philosophy |
| [Phase 1 PRD](docs/phase-1-prd.md) | Detailed requirements for the Live Co-Pilot |
| [Phase 2 PRD](docs/phase-2-prd.md) | Placeholder — Intelligence Expansion |
| [Phase 3 PRD](docs/phase-3-prd.md) | Placeholder — Maturity |

## Development

This project uses spec-driven development with LLM coding agents. See `AGENT.md` for agent instructions.

```bash
# Coming soon — project not yet scaffolded
```

## License

Private repository. All rights reserved.
