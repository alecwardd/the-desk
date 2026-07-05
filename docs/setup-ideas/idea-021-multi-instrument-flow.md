# IDEA-021: Multi-Instrument Flow Architecture (NQ / MNQ / ES / MES)

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Spec drafted (2026-06-23); Stage A buildable
**Source:** Trader architecture session 2026-06-23 (memory `multi-instrument-flow-architecture`)
**Complements:** IDEA-018 (multi-instrument tracking), IDEA-009 (NQ/ES SMT), IDEA-020 (zones as flow)
**Full spec:** [`docs/multi-instrument-flow-architecture.md`](../multi-instrument-flow-architecture.md)

**Concept:** Run all four CME equity-index contracts; treat the **mini↔micro relationship**
(institutional vs retail flow) as a conviction/sizing signal. Core principle: **share price structure
once per underlying (from the mini), run order flow per contract.** Three tiers — contract flow →
instrument complex (with a mini-vs-micro flow-agreement metric) → cross-asset NQ↔ES. Conviction feeds
the risk module (tiered sizing to start) and the subagent narrates it; detection stays deterministic.
Build stages A (NQ-complex flow + agreement) → B (conviction→size) → C (ES-complex) → D (cross-asset).
All four contracts recording `.scid` since 2026-06-23; agreement backtests are forward-only until
micro history accrues. See the full spec for the data model, metric definition, and acceptance criteria.
