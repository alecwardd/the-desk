# IDEA-023: Social Intelligence & Continual Learning (X / Trusted Accounts)

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Idea (exploration documented; Phase A build blocked on ADR-020 trader decisions)
**Source:** Trader vision — trusted X accounts for live confluence, backtest hypothesis discovery, and subagent prompts from external edge situations
**Complements:** All setup IDEAs (hypothesis source), orchestrator + specialists, trader memory layer, research query engine
**Requires:** X Developer API access (pay-per-use; see cost model in spec), curated watchlist

**Framing:** A **platform feature track**, not a single setup. Trusted accounts contribute in different ways: real-time confluence, regime framing, level callouts, backtest hypotheses, and edge-case prompts. The Desk compares external reads to **deterministic structure + the trader's playbook**; third-party ideas enter a **trader-gated queue** before any backtest or template work.

**Architecture (non-negotiable):**
- Layer 3 only (`src/social/`); pipelines and rules engine unchanged
- Social data never fires playbook alerts (Rule #3)
- Subagent "learning" = SQLite memory + research conditionals, not neural RL
- Compliance: third-party attribution; hypotheses for *your* validation

**Phased delivery:**

| Phase | Deliverable | Doc |
|-------|-------------|-----|
| A | Watchlist cache + `get_account_confluence` MCP tool | [social-confluence-design.md](../social-confluence-design.md) |
| B | Confluence event logging | [social-intelligence-roadmap.md](../social-intelligence-roadmap.md) |
| C | Research conditionals (`social_alignment` × outcomes) | roadmap |
| D | Memory categories + per-account calibration | roadmap + [trader-memory/architecture.md](../trader-memory/architecture.md) |
| E+ | RAG over post history; optional model training | roadmap (defer) |

**Success criteria (Phase A):** During a setup check, the orchestrator can report watchlist lean vs structure vs playbook with explicit confluence/divergence typing, without any social-derived alert.

**Success criteria (full track):** Externally sourced hypotheses flow into IDEA entries and backtests; longitudinal stats show when alignment with specific accounts correlated with the trader's setup outcomes (sample-size gated).

**Open decisions:** Watchlist, API access mode, budget ceiling, poll cadence, idea extraction cadence — see [roadmap open questions](../social-intelligence-roadmap.md#open-questions-trader-decisions).
