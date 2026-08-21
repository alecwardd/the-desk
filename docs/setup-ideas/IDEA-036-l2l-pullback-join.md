---
id: IDEA-036
title: L2L Pullback-Join — stacked leg-to-leg profile pullback entry
status: Researched
regime: [any]
related: [IDEA-035, IDEA-020, IDEA-012]
companionSpecs:
  - docs/setup-ideas/IDEA-035-leg-to-leg-profile-engine.md
  - docs/data-and-backtesting-guide.md
mcpPointers:
  - tool: get_research_summary
    note: Call first before any historical claim; do not cache N in this file
  - tool: query_signal_outcome_distribution
    setupId: IDEA-036
    note: Future backtest outcome query; do not quote stats from this file
  - tool: query_signal_outcome_excursions
    setupId: IDEA-036
    note: MFE/MAE in points is the primary outcome read for this setup
hypothesisAnchor: false
---

# IDEA-036 — L2L Pullback-Join (stacked leg profiles)

> Point-in-time **docs-only** setup spec captured **2026-07-24**. Origin:
> second-brain inbox note "Stacked Leg-To-Leg Volume Profiles" (2026-07-16) and
> a 2026-07-24 trader interview locking the definitions. Depends on the
> IDEA-035 leg engine for every structural term below. This note does **not**
> run a backtest, register a hypothesis, or make any signal claim.

<!-- stats: point-in-time -->

## Thesis

Treat each auction leg as its own auction (IDEA-035's fractal premise): its
volume nodes are value and interest on that timeframe. When a **counter-leg B**
pulls back into the **LVN/shelf zone of the prior leg A**, and B's
participation **tapers** while delta turns back in A's direction, the pullback
is failing to find acceptance — the likely resolution is a rotation back
toward (and through) A's value area. Stacked leg profiles supply the whole
trade geometry: leg A's direction is the trend read, the LVN/shelf zone is the
entry location, and A's own node structure beyond the entry is the stop.

Why an edge should exist: LVNs/shelves mark prices where the prior auction did
**not** build two-sided trade — either skipped (unfinished business, fast
rejection likely again) or absorbed (passive defense). A tapering counter-leg
arriving at such a zone with delta realigning is the footprint of the larger
leg's participants re-engaging. The trader reports getting rebids/reoffers —
more than one chance to enter — which is why the **second touch** is the
preferred event.

## Mechanics

Location → trigger → confirmation → invalidation → risk state.

- **Location:** Confirmed leg A (say, up). Confirmed counter-leg B retraces
  into the **zone**: leg A's nearest LVN or shelf band below current price
  (rows identified per IDEA-035 definitions, 4-tick bins).
- **Trigger:** **Second touch** of the zone. First touch = first trade into
  the zone after B begins. Price must then **exit** the zone (close of a 1-min
  bar beyond the zone edge by ≥ 2 ticks — hysteresis so overlapping ticks
  don't mint touches) and re-enter: that re-entry is the second touch.
- **Confirmation (at second touch):**
  1. Leg B is **tapering** per the IDEA-035 rate+expansion definition, and
  2. **Delta realignment**: net delta of the trailing 3 one-minute bars is in
     leg A's direction (sign flip back), or B's bar-delta percentile rank has
     collapsed below the 50th percentile of its own trailing window.
- **Invalidation (structural stop):** two LVN boundaries beyond the entry zone
  in the against-A direction, i.e. price has traded through the entry LVN and
  the next one, into the next HVN of leg A's structure. ("Two low volume nodes
  away.") R = distance from zone edge to that invalidation price.
- **Target (for outcome labeling):** the far side of leg A's value area (VAH
  for a long). Extension outcome (new leg-A extreme) recorded separately.

## Event definition (for backtest extraction)

One event per (leg A, leg B, zone) triple at the second touch with both
confirmations true. Controls: second touches of the same zone **without** the
taper + delta conditions, matched by session segment and leg-A size quintile.
All reads respect IDEA-035's causality rule (only anchors/profiles known at
event time; developing legs labeled as such).

## Outcome measurement

Per the locked decision: **structural R + fixed horizons, event-study only.**

- From each event record MFE/MAE in **points and R** until the first of:
  (a) far side of leg A's value area reached, (b) structural invalidation
  reached, (c) RTH session end.
- Fixed snapshots at +5 / +15 / +30 / +60 min for cross-setup comparability.
- Distributions, never bare hit rates. No fill simulation, no slippage model —
  per the repo Never-Do list this is market movement relative to computed
  levels, not simulated P&L.
- Secondary read: does price respect LVN/shelf rows at all — reaction rate at
  zone rows vs matched non-node rows inside leg A (tests whether the *level
  selection* carries information independent of the trade).

## Backtest plan (staged)

1. **Gate:** IDEA-035 Stage 1 pass (stable leg engine). No engine, no events.
2. **Stage A — event census:** offline replay extracts all pullback-join
   events + controls over the campaign window (RTH only, NQ, explicit contract
   per window). Report coverage: events/day distribution, zone types (LVN vs
   shelf), leg-size quintiles.
3. **Stage B — outcome separation:** event cohort vs matched controls on
   MFE/MAE (points and R) at each horizon and at structural exits. Sensitivity
   over the IDEA-035 parameter cell and the confirmation thresholds.
4. **Verdict per AGENT.md sample-size policy:** N ≥ 30 per compared bucket for
   reportable results; 20–29 directional only; < 20 insufficient.

**Adopt:** event cohort separates from controls with stable sign across folds
and the clean 2026-06-23+ subset → candidate for `register_hypothesis` +
`run_backtest` promotion per the data guide §4 loop, then draft setup.
**Adapt:** separation only in specific regimes/day types → keep as labeled
context. **Skip:** no separation after sensitivity.

## Falsifiable hypotheses

- **H1 (level selection):** Price reacts at leg-A LVN/shelf rows more often
  than at matched non-node rows. *Falsify if* reaction rates are
  indistinguishable — the fractal-value premise fails and the setup is moot.
- **H2 (confirmation conditions add edge):** Second touches with taper +
  delta realignment outperform second touches without them (MFE/MAE and R
  distributions separate). *Falsify if* the conditions only reduce frequency
  without improving outcomes.
- **H3 (structural stop placement):** "Two LVNs away" invalidation sits beyond
  the MAE distribution's body for winning events — i.e. the trader's stop
  geometry matches where adverse moves actually terminate. *Falsify if* the
  median winner's MAE routinely exceeds the structural stop distance.

No hypothesis authorizes live signals, alerts, sizing, or playbook changes.

## Design controls

| Control | Requirement |
|---------|-------------|
| **Dependency** | All structural terms come from IDEA-035 locked definitions; no parallel leg math in this track |
| **Causality** | Events use only confirmed/known state at event time (IDEA-035 rule) |
| **Controls** | Matched second touches without confirmations; no control, no claim |
| **Costs** | Event-study market movement only; no fill model unless separately approved |
| **Sessions / instrument** | RTH only, NQ primary, explicit contract per window; no silent pooling |
| **Sensitivity** | Across leg-engine parameter cell, zone definitions, and confirmation thresholds; table mandatory |
| **Sample** | N ≥ 30 per bucket; `get_research_summary` first |

## Relationship to existing ideas

| Idea | Relation |
|------|----------|
| [IDEA-035](IDEA-035-leg-to-leg-profile-engine.md) | Hard dependency — leg engine, anchors, LVN/shelf, taper definitions |
| [IDEA-020](IDEA-020-footprint-rebid-reoffer-lifecycle.md) | Rebid/reoffer zones are the micro-scale version of the same "second chance at a level" behavior; potential confirmation feature later |
| [IDEA-012](IDEA-012-absorption-failure.md) | Absorption at shelves is one of the two LVN interpretations; failure-of-absorption is the invalidation side |
| Future IDEA-037+ | Continuation-through (leg slices prior LVNs = in control) and leg-failure reversal setups plug into the same engine after this track |

## Explicit non-goals (this pass)

- No backtest execution, no `register_hypothesis` / `run_backtest`.
- No live signals, alerts, or playbook activation.
- No fill/slippage simulation or P&L claims.
- No continuation-through or failure setups (separate future tracks).
- No screenshots or vault raw material copied into this file.

## Recommended next action

1. Wait for IDEA-035 Stage 1 pass.
2. Run the Stage A event census on the campaign window; check events/day is
   viable for N ≥ 30 buckets before building Stage B.
3. Only then run Stage B outcome separation and draft a verdict.

## See also

- Hub stub: [setup-ideas-and-backtesting.md#idea-036](../setup-ideas-and-backtesting.md#idea-036)
- Setup index: [index.md](index.md)
- Leg engine spec: [IDEA-035-leg-to-leg-profile-engine.md](IDEA-035-leg-to-leg-profile-engine.md)
- Data workflow: [docs/data-and-backtesting-guide.md](../data-and-backtesting-guide.md)
