# Multi-Instrument Flow Architecture (NQ / MNQ / ES / MES)

**Status:** Spec — Stage A buildable (2026-06-23)
**Tracking:** IDEA-021 in `setup-ideas-and-backtesting.md`
**Doctrine:** memory `multi-instrument-flow-architecture`, `rebid-reoffer-zone-doctrine`
**Owner:** trader

This document specifies how The Desk scales from a single NQ stream to all four CME equity-index
contracts — the two minis (NQ, ES) and their micros (MNQ, MES) — and how the relationship *between*
contracts becomes a first-class trading signal (conviction and position sizing) rather than just more
data.

---

## 1. Goals & non-goals

**Goals**
- Run NQ, MNQ, ES, and MES concurrently without restarting the server or mixing their state.
- Treat the **mini↔micro relationship** (institutional vs retail flow) as a conviction signal.
- Lay the groundwork for **cross-asset** (NQ↔ES) confirmation (SMT / correlation, IDEA-009).
- Keep the existing three-layer discipline: deterministic detection (Rust), LLM interpretation only.
- Be incremental: each stage ships and proves out before the next is built.

**Non-goals (for now)**
- Order execution, smart routing, or auto-sizing (The Desk advises; the trader sizes/executes).
- Cross-asset analytics before the single-instrument read is validated.
- A generic N-asset framework. Scope is the four equity-index contracts, which share one session
  calendar and (within an underlying) one price grid.

---

## 2. First principles

1. **Share structure, separate flow.** NQ and MNQ are the *same auction* — identical price grid,
   value area, levels, and regime — but *different participants*. Compute price structure **once per
   underlying from the mini** (the liquid reference); compute **flow per contract**. This is both
   cheaper and conceptually correct.
2. **Deterministic detection, LLM interpretation.** All agreement/divergence math is Layer 1/2 Rust.
   The subagent reads structured output and narrates conviction; it never computes the signal.
3. **Validate before expanding.** A flow-agreement overlay must sit on top of signals already
   validated on the mini alone. Confluence cannot rescue an unvalidated base setup.
4. **The comparator generalizes.** Mini-vs-micro (Tier 2) and NQ-vs-ES (Tier 3) are the same pattern
   at different scopes. Build it once.
5. **Data is the long-lead item.** `.scid` cannot be backfilled. All four contracts must record
   continuously (done 2026-06-23); agreement backtests are forward-only until micro history accrues.

---

## 3. Domain model

| Term | Meaning |
|------|---------|
| **Underlying** | The index being traded: `NQ` (Nasdaq-100) or `ES` (S&P 500). |
| **Contract** | A tradeable stream: mini (`NQ`, `ES`) or micro (`MNQ`, `MES`). |
| **Instrument complex** | One underlying + its mini and micro contracts, sharing price structure. |
| **Flow** | Order-flow signals derived from a single contract's ticks: footprint, delta, absorption, zones. |
| **Structure** | Price-derived context shared across a complex: TPO/VA/POC, VWAP, levels, day-type, regime. |

Notional: MNQ = 0.1 × NQ; MES = 0.1 × ES. Mini and micro of an underlying track the **same price to
the tick**, so their footprints align on an identical price grid with zero alignment risk.

---

## 4. Architecture — three tiers

```
Tier 1  Contract flow stream        per contract (NQ, MNQ, ES, MES)
        footprint / delta / absorption / rebid-reoffer zones from that contract's ticks

Tier 2  Instrument complex          per underlying (NQ-cx, ES-cx)
        shared price structure (from the mini) + both contract flows
        + intra-instrument FLOW-AGREEMENT metric (mini vs micro)

Tier 3  Cross-asset layer           NQ-cx vs ES-cx
        SMT / correlation / relative strength (read-only over both complexes)
```

### 4.1 Component mapping to the current codebase

- **`PipelineEngine`** today bundles structure + flow for one symbol. Split conceptually into a
  `StructureEngine` (TPO, VWAP, levels, day-type, regime — runs on the mini) and a `FlowEngine`
  (footprint, delta, absorption, rebid-reoffer — runs per contract). Stage A can keep one
  `PipelineEngine` per contract and simply *not* read the micro's structure outputs, deferring the
  formal split.
- **`InstrumentComplex`** (new): owns one `StructureEngine` (mini) + one `FlowEngine` per contract +
  the agreement metric. Exposes a `ComplexState` (extends today's `MarketState`).
- **`MarketRouter`** (new): `root_symbol → InstrumentComplex`; runs each complex's feed loop; routes
  MCP tool calls by a `symbol` parameter (default `NQ` for back-compat).
- **`CrossAssetAnalyzer`** (new, Tier 3): read-only over two `ComplexState`s. Owns no pipelines.

### 4.2 Feed & ingest

- One `.scid` tail loop **per contract** (4 total), each tagging ticks with its `contract_symbol` and
  `root_symbol`. `raw_ticks` already carries both columns.
- Each contract's ticks feed its `FlowEngine`; each mini's ticks also feed its complex's
  `StructureEngine`.
- **Session scoping is uniform** — all four are CME equity index on one RTH/Globex calendar, so the
  existing session logic applies per contract without cross-asset calendar mixing.
- Reuse the hardening already in place: monotonic tick guard, RTH-open ingest/analysis split, and the
  `scid_window_mismatch_warning` contract guard.

---

## 5. The flow-agreement metric (Tier 2)

Computed deterministically each analysis pass, per complex, comparing the mini flow to the micro flow.
All comparisons are **notional-normalized or sign/shape based** — never raw contract counts.

| Component | Definition | Read |
|-----------|------------|------|
| **Delta agreement** | Sign + normalized-magnitude correlation of recent/session delta, mini vs micro | Same sign = participants aligned |
| **Zone agreement** | Is a rebid/reoffer zone present in *both* footprints at the same band? | Both = high conviction; mini-only = institutional; micro-only = retail |
| **Absorption agreement** | Absorption / invalidation events aligning in time and direction | Confirmation of defense or failure |
| **Leader** | Which contract moved first / is more extreme (lead/lag of delta & price) | Mini-led = institutional initiative; micro-led/-only = retail (fade tell) |

**Output (`FlowAgreement`):**
- `agreement_score: f64` in [-1.0, +1.0] (aggregate directional alignment),
- `leader: Mini | Micro | None`,
- `conviction: High | Medium | Low | Conflict`,
- per-component booleans/scores for transparency and backtesting.

**Interpretation contract for the subagent:** the agent receives `FlowAgreement` and frames conviction
("rebid zone confirmed by micro flow, mini leading → high conviction" vs "micro buying into mini
selling → retail trapped, reduce/stand aside"). It must cite the components; it must not recompute them.

---

## 6. Conviction → position sizing

- The conviction feeds the existing **risk module** as a size modulator, not an order.
- **Starting policy (revisable): tiered**, not continuous — `High → full`, `Medium → reduced`,
  `Conflict → stand aside / smallest`. Continuous modulation (e.g., 0.5×–1.5×) is a later refinement
  once the tiers are validated.
- **Starting policy: micro flow is confirm-only** — it can confirm or deny a mini-originated setup but
  cannot *trigger* one. Revisit once micro-only signals have a verified edge.

---

## 7. MCP / agent surface

- Tools gain an optional `symbol` (underlying) parameter, default `NQ` — exact precedent: the
  `contract` parameter added to `run_backtest`.
- New read tools: `get_flow_agreement(symbol)` (Tier 2) and `get_cross_asset(symbolA, symbolB)`
  (Tier 3). `get_market_snapshot`, `evaluate_playbook`, etc. become symbol-scoped.
- Subagents receive the structured agreement/cross-asset state and produce conviction-framed coaching.

---

## 8. Backtesting

- Per-contract `signal_outcomes` already keyed by `root_symbol`/`contract_symbol`; extend research
  queries to scope by underlying and to join mini+micro for agreement studies.
- **Agreement and cross-asset backtests are forward-only** until enough micro/ES history accrues.
  Single-contract methodology continues to backtest on existing mini history.
- Candidate hypotheses: "Does mini+micro delta agreement improve zone-retest expectancy vs mini
  alone?"; "Do micro-led moves into a mini level fail more often (fade edge)?"

---

## 9. Build staging & acceptance criteria

| Stage | Scope | Done when |
|-------|-------|-----------|
| **A** | NQ-complex: wire MNQ as a second flow stream; build `FlowAgreement` metric + `get_flow_agreement(NQ)` | MNQ ticks ingest tagged; agreement metric computes live; unit-tested on synthetic mini/micro flows |
| **B** | Conviction → risk-module size tiers + subagent interpretation | Conviction tier surfaces in risk output; agent narrates it citing components |
| **C** | ES-complex: mirror of A (config-add, no refactor) | ES+MES ingest; `get_flow_agreement(ES)` live |
| **D** | Cross-asset NQ↔ES comparator (SMT / ratio / relative strength) | `get_cross_asset(NQ, ES)` live; first cross-asset hypothesis registered |

Defer C/D until the NQ complex (A/B) is validated against forward data.

---

## 10. Risks & nuances

- **Normalization:** always compare notional-normalized or sign/shape; never raw counts (10:1).
- **Micro thresholds:** micros are thinner — the stacked-delta zone detector needs micro-specific
  thresholds (a "5 stacked levels" band is fewer contracts). Do not blind-share mini params.
- **Feed timing:** mini and micro print at slightly different times; lead/lag detection must tolerate
  sub-second skew and not over-read noise as "leadership."
- **Performance:** four tail loops + per-contract flow pipelines. Flow pipelines are incremental and
  micros are lower-volume, but profile the analysis-pass budget before enabling all four live.
- **DB growth:** the store is already large; per-contract tagging is fine, but revisit partitioning
  (per-root tables or attached DBs) if query latency degrades.
- **Live/backtest contract config:** keep the per-job `contract` parameter (never global flips); the
  same discipline now extends to four contracts.

---

## 11. Open decisions

1. Continuous size modulation vs. tiered gating (starting: **tiered**).
2. Micro-only triggering vs. confirm-only (starting: **confirm-only**).
3. Whether to formally split `PipelineEngine` into `StructureEngine` + `FlowEngine` in Stage A or
   defer the refactor (starting: **defer**; keep one engine per contract, ignore micro structure).
