# IDEA-018: Multi-Instrument Concurrent Tracking (NQ, MNQ, ES, MES)

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Idea
**Source:** Roadmap — full product vision once the MCP surface and single-symbol path are “done enough”
**Complements:** Correlation and SMT-style ideas (e.g. IDEA-009); session and regime context across equity index futures

**Concept:** Run **four liquid CME equity index micro/mini roots** in parallel: **NQ**, **MNQ**, **ES**, and **MES** — each with its own pipeline state, session scoping, and tool addressing — so agents can reason about alignment, divergence, and relative strength without manually switching symbols or restarting the server.

**Why it is non-trivial:** Today the architecture is optimized around a **primary** symbol stream (Sierra `.scid` tail + SQLite + `MarketState`). Multi-symbol implies duplicate or partitioned pipeline engines, feed scheduling, database keys or separate tables per instrument, MCP tool parameters (or namespaces) for “which symbol,” and clear rules for **never mixing RTH/Globex across symbols** in a single calculation by accident.

**Sequencing:** Treat this as **Phase B** after IDEA-017 (and related MCP hardening): stabilize the agent contract first, then expand capacity so the same contract applies per symbol without ambiguity.
