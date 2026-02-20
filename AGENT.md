# The Desk — Agent Instructions

Universal instructions for any LLM coding agent (Claude Code, Cursor, Codex, Traycer AI) working in this repository.

---

## Project Context

The Desk is a real-time AI trading co-pilot. Read these documents in order:

1. **CLAUDE.md** — Project rules, architecture, conventions (READ FIRST)
2. **docs/the-desk-vision.md** — Product vision and philosophy
3. **docs/phase-1-prd.md** — Phase 1 detailed requirements
4. **Relevant skill** from `skills/` — Domain knowledge for your task

---

## Architecture Summary

```
Sierra Chart (DTC Server) → Rust Pipelines → Rust Rules Engine → Claude API → React UI
         ↑                        ↑                  ↑                ↑           ↑
     Layer 0               Layer 1 (fast)      Layer 2 (fast)   Layer 3 (1-2s)  Layer 4
     External              Deterministic       Deterministic    LLM inference    Display
```

**Every layer must be independently testable.** Never skip layers.

---

## Subagent Patterns

When you need specialized help, spawn subagents for these tasks:

### DTC Protocol Research
**When:** Working on the DTC client, encountering protocol questions
**How:** Spawn an Explore agent with context from `skills/dtc-protocol/SKILL.md`
**Instruction:** "Read `skills/dtc-protocol/SKILL.md` then investigate [specific DTC question]. Check the Sierra Chart DTC documentation at sierrachart.com if needed."

### Pipeline Verification
**When:** After implementing or modifying a market structure pipeline
**How:** Spawn a test agent that validates calculations
**Instruction:** "Read `skills/trading-domain/SKILL.md` for domain knowledge. Verify that the [VWAP/TPO/Delta] pipeline produces correct output for this test data: [provide known input/output pairs]."

### Prompt Quality Evaluation
**When:** After writing or modifying LLM coaching prompts
**How:** Spawn an evaluation agent
**Instruction:** "Read `skills/compliance-research/SKILL.md`. Evaluate these coaching prompts for: (1) compliance with language rules (never 'you should buy'), (2) traceability to playbook rules, (3) clarity and usefulness. Test prompts: [provide sample prompts]."

### Options API Research
**When:** Working on Phase 2 options data integration
**How:** Spawn a research agent with web search
**Instruction:** "Research the [Unusual Whales / CBOE / OptionData.io] API for: available endpoints, authentication, rate limits, data format, and relevance to gamma exposure / dealer positioning calculation."

---

## Implementation Workflow

When implementing a feature:

1. **Find the requirement** in `docs/phase-1-prd.md` (look for the requirement ID, e.g., TPO-03)
2. **Read the relevant skill** for domain knowledge
3. **Write the Rust code** (if it involves data processing or rules)
4. **Write tests** alongside the code
5. **Wire up IPC** using patterns from `skills/tauri-bridge/SKILL.md`
6. **Build the UI component** using shadcn/ui
7. **Test end-to-end** with mock DTC data
8. **Run `cargo test && npm test`** before declaring done

---

## Decision Framework

When you're unsure about an implementation choice:

| Question | Guidance |
|----------|----------|
| Should this be in Rust or TypeScript? | If it processes market data or evaluates rules → Rust. If it's UI or LLM interaction → TypeScript. |
| Should this be a command or an event? | User-triggered or request/response → command. Streaming or Rust-initiated → event. |
| Should I use the LLM for this? | If it can be computed deterministically → no LLM. If it requires natural language or contextual synthesis → LLM. |
| Should I add a new dependency? | Prefer existing deps. Check Cargo.toml and package.json first. |
| Should I create a new file? | Prefer editing existing files. Only create new files for genuinely new modules. |

---

## Common Mistakes to Avoid

1. **Implementing a backtesting engine.** We import results. Never build an engine.
2. **Using `f32` for prices.** Always `f64` — precision matters for financial data.
3. **Forgetting incremental updates.** Pipelines MUST update incrementally, not recalculate.
4. **Emitting events per tick.** Throttle UI updates to ≤4 Hz.
5. **Blocking the main thread.** All I/O and computation in background tokio tasks.
6. **Mixing RTH and Globex data.** Always scope calculations to the correct session.
7. **Using advisory language in prompts.** "Your rules say..." not "You should..."
8. **Skipping Layer 2.** The rules engine MUST evaluate before the LLM is called.

---

## Testing with Mock Data

For development without a live Sierra Chart connection, use the mock DTC server:

```
# Start mock server (generates synthetic NQ trades)
cargo run --bin mock-dtc-server

# The mock server:
# - Listens on localhost:11099
# - Responds to ENCODING_REQUEST and LOGON_REQUEST
# - Streams synthetic NQ trades at realistic rates
# - Supports MARKET_DEPTH_REQUEST with synthetic DOM
# - Replays recorded sessions if provided a recording file
```

Mock data should simulate realistic NQ behavior:
- Price range: ±200 points from a configurable center
- Trade rate: 1-3 trades/second (quiet) to 10-20/second (active)
- Tick size: 0.25 points
- Volume: 1-50 contracts per trade
- Include bid/ask spread of 0.25-0.50 points
