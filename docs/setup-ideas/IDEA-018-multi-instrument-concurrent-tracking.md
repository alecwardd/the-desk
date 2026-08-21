---
id: IDEA-018
title: "Multi-Instrument Concurrent Tracking (NQ, MNQ, ES, MES)"
status: Idea
regime: [any]
related: [IDEA-009]
companionSpecs: []
mcpPointers: []
hypothesisAnchor: false
---

# IDEA-018 — Multi-Instrument Concurrent Tracking (NQ, MNQ, ES, MES)

> Per-idea detail file. The hub ([setup-ideas-and-backtesting.md](../setup-ideas-and-backtesting.md)) keeps a short stub (status, source, framing, detail link) pointing here.

**Status:** Idea
**Source:** Roadmap — full product vision once the MCP surface and single-symbol path are “done enough”
**Complements:** Correlation and SMT-style ideas (e.g. IDEA-009); session and regime context across equity index futures

**Concept:** Run **four liquid CME equity index micro/mini roots** in parallel: **NQ**, **MNQ**, **ES**, and **MES** — each with its own pipeline state, session scoping, and tool addressing — so agents can reason about alignment, divergence, and relative strength without manually switching symbols or restarting the server.

**Why it is non-trivial:** Today the architecture is optimized around a **primary** symbol stream (Sierra `.scid` tail + SQLite + `MarketState`). Multi-symbol implies duplicate or partitioned pipeline engines, feed scheduling, database keys or separate tables per instrument, MCP tool parameters (or namespaces) for “which symbol,” and clear rules for **never mixing RTH/Globex across symbols** in a single calculation by accident.

**Sequencing:** Treat this as **Phase B** after IDEA-017 (and related MCP hardening): stabilize the agent contract first, then expand capacity so the same contract applies per symbol without ambiguity.

---

## See also

- Hub stub: [setup-ideas-and-backtesting.md#idea-018](../setup-ideas-and-backtesting.md#idea-018)
- Setup index: [index.md](index.md)
