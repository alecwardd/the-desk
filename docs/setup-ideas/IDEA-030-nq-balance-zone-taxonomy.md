---
id: IDEA-030
title: NQ Balance-Zone Taxonomy
status: Idea
regime: [rotation, balance, trend-transition]
related: [IDEA-000, IDEA-007, IDEA-013, IDEA-029]
companionSpecs:
  - docs/data-and-backtesting-guide.md
  - docs/mcp/tool-reference.md
hypothesisAnchor: false
---

# IDEA-030 - NQ Balance-Zone Taxonomy

> Research-only promotion of a trader-authored capture from 2026-07-10. No
> screenshots, chartbooks, private settings, live data, or backtest output were
> copied. Nothing here is a setup or trade signal.

## Thesis

"Balance" should be a typed set of market states rather than one overloaded
label. The useful research question is whether separately defined balance
dimensions—and eventually their overlap—improve regime classification or the
eligibility of existing setups.

The captured composite idea is plausible but unverified: when TPO/volume value,
delta value, longer-horizon balance, and external options context overlap, price
may be in a broader balance zone; movement outside every independently defined
zone may represent price exploration. That must be tested against simpler
baselines before it appears in agent context.

## Candidate Balance Dimensions

### TPO and volume structure

- Current-session TPO value area, POC, profile shape, and balance state.
- Overlap of consecutive RTH value areas; separately test overlap percentage,
  boundary containment, and POC migration.
- HVN/LVN structure and whether multiple distributions represent one balance,
  migration between balances, or a trend transition.
- Single prints, excess, poor highs/lows, and acceptance/rejection at value
  boundaries. Keep their definitions separate from the balance label.
- Weekly and monthly value structures only after their session anchors and
  calculations are explicitly defined.

### Delta structure

- DNVA overlap, DNP migration, net-delta concentration, and whether aggressive
  inventory is building, clearing, or neutral.
- TPO/volume balance and delta balance may disagree; preserve that disagreement
  as information instead of forcing one composite score.

### External options context

- Neutral gamma/charm/vega positioning is external context, not repo-native NQ
  structure and never a trigger by itself.
- Use only source-labeled, freshness-gated data after the options-data boundary
  is implemented. Do not infer current positioning from this note.

## Existing Repo Surface to Verify

- `get_day_type` and the day-type pipeline already expose profile shape and a
  balanced/imbalanced state.
- `get_tpo_profile`, `get_delta_profile`, `get_key_levels`, and historical
  research tools provide much of the raw context needed for a first study.
- The setup hub already calls for exact definition passes on single prints,
  poor highs/lows, weekly MGI, and swing/rotation boundaries.

Before building anything, verify the stored historical coverage and exact field
semantics. Do not create a second balance calculation when an existing field can
be extended or queried.

## Definition Questions

1. What minimum value-area overlap constitutes balance: raw points, percentage
   of the narrower area, or percentage of the union?
2. How many consecutive sessions are required, and how should gaps or double
   distributions reset the state?
3. Is POC/DNP stability required, or is overlapping value sufficient?
4. How are RTH, Globex, weekly, and monthly structures kept separate?
5. When dimensions disagree, should the output remain a vector of states rather
   than a lossy composite score?
6. What event proves exit from balance: time/volume acceptance outside, value
   migration, range extension, or a combination?

## Research Queue

1. Audit current day-type/balance fields and stored session coverage.
2. Define each balance dimension as a deterministic, session-scoped predicate.
3. Build a baseline using only existing TPO/volume fields.
4. Add delta dimensions one at a time and measure incremental information.
5. Add external options context only after its provenance/freshness boundary is
   available.
6. Compare any composite state with simpler day-type/profile-shape baselines.

Every reported statistic must include `N` and follow the repo sample-size
policy. Until then, agents should describe these as research hypotheses only.

## See Also

- Hub stub: [setup-ideas-and-backtesting.md#idea-030](../setup-ideas-and-backtesting.md#idea-030)
- Setup index: [index.md](index.md)
