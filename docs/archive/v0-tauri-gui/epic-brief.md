# Epic Brief — The Desk

## Summary

The Desk is a real-time AI trading co-pilot for discretionary NQ futures traders. It runs as a Tauri desktop application on a dedicated second monitor alongside Sierra Chart, connected to live market data via the DTC protocol. It does not place orders, generate signals, or give financial advice — it helps traders execute the process they already defined: their rules, their setups, their backtest data, reflected back to them in real time. The core value proposition is closing the **knowledge-execution gap**: the well-documented failure mode where traders know what to do but don't do it under live market pressure. Phase 1 delivers the minimum viable co-pilot — the product that makes a trader say *"this changed how I traded today"* after their first live session.

---

## Context & Problem

### Who Is Affected

**Primary persona:** Discretionary prop-firm or independent NQ futures trader with 1–5+ years of experience. They have specific, quantifiable setups. They've backtested their ideas. They know their rules. But they struggle to follow them consistently when the market is live and the pressure is real — chasing entries, moving stops, skipping A+ setups after a loss, overtrading on slow days. Many operate under prop firm constraints (daily loss limits, max drawdown rules) where a single impulse trade can blow an evaluation.

These traders work alone. They have no partner, no coach on the sideline, no second opinion during the session.

### The Gap No Tool Fills

| Existing Tool | What It Does | What It Misses |
|---|---|---|
| Trading journals (TraderSync, Edgewonk) | Post-session reflection | No real-time coaching |
| AI signal tools (Trade Ideas, TrendSpider) | Their models, their signals | Not the trader's own rules |
| Psychology coaching | Mindset work | No connection to live data |
| Prop firm dashboards (Topstep Coach T) | Evaluation tracking | Ecosystem-locked, not personalized |
| Options analytics (SpotGamma, ConvexValue) | Gamma data | No integration with trading process |

**The gap:** No tool takes a trader's own backtested rules, connects them to live market data, and provides real-time contextual coaching during the session — with a full lifecycle from preparation through post-session evolution.

### Where This Lives

This is a **greenfield Tauri desktop application**. There is no existing codebase. The full system — Rust backend, React frontend, DTC client, market structure pipelines, rules engine, LLM coaching layer, SQLite storage — is being built from scratch. The architecture is defined in [CLAUDE.md](../CLAUDE.md) and [AGENT.md](../AGENT.md). Detailed Phase 1 requirements live in [phase-1-prd.md](phase-1-prd.md).

---

## Scope

### Phase 1 — Live Co-Pilot (This Epic)
Full implementation-ready specs covering: DTC data feed, market structure pipelines (VWAP, TPO, Delta Neutral, Key Levels), playbook builder, deterministic rules engine, LLM coaching layer (Claude API), session recording & tape replay, pre-session briefing, basic session log & trade import, and the second-monitor UI.

### Phase 2 — Intelligence Expansion (Placeholder)
Options/gamma data pipeline, advanced order flow analysis, structured post-session review, behavioral pattern recognition. Depends on Phase 1 stable and in daily use.

### Phase 3 — Maturity (Placeholder)
Adaptive coaching, multi-instrument support, advanced analytics, playbook versioning. Depends on Phase 2 complete.

---

## Constraints & Non-Negotiables

- **Never place orders.** The Desk is coaching-only. No trade execution, ever.
- **Never generate proprietary signals.** Every alert traces to a rule in the trader's own playbook.
- **Never use advisory language.** All prompts use "your rules say…" framing, never "you should buy/sell."
- **Local-first data.** All playbook, journal, and session data stays on the trader's machine.
- **Graceful degradation.** The app must function without Claude API connectivity (raw alerts, no coaching prose).
- **Performance.** Must run alongside Sierra Chart with <500MB memory. Pipeline latency <50ms. LLM prompt <2s.
- **Regulatory positioning.** The Desk is a productivity/coaching tool, not an investment adviser. See [skills/compliance-research/SKILL.md](../skills/compliance-research/SKILL.md).

---

## Success Criteria (Phase 1)

| Metric | Target |
|---|---|
| First live co-pilot session | Within 1 hour of installation |
| Session recording reliability | 100% — zero missed ticks |
| Setup definition completion rate | >90% of started setups fully defined |
| Daily active usage during RTH | >80% of trading days after first week |
| Qualitative signal | Trader says "this changed how I traded today" after first live session |