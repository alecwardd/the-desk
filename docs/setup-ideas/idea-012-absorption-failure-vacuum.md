# IDEA-012: Absorption Failure / Liquidity Vacuum

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** REJECTED as a standalone setup (2026-06-23) — concept folded into IDEA-020.
**Source:** Local 2025-11-28 through 2026-03-06 database study; CME liquidity research
**Complements:** IDEA-002 Trapped Trader Reversal, Rebid/Reoffer, Absorption pipeline

> **Verdict (2026-06-23): rejected as specified; the *concept* is a "Failed zone" — reconstructed in IDEA-020.**
> v2 backtest (`absorption_invalidated` + `absorption_invalidation_direction` + `tape_pace_percentile`):
> short N=58 / −0.07R (reject); long N=58 / +0.09R coalesced (was +0.25R under every-tick over-sampling,
> so the honest number is ~+0.09R — marginal at best). Root cause: a generic `absorption_invalidated` flag
> fires on *any* failed absorption anywhere, with fixed-point stops/targets and **no level context**.
> Reconstruction: a failed defense → vacuum **is** a `Failed` rebid/reoffer zone in IDEA-020 (price breaks
> through the band with acceptance = the trader's "failed zone = trend change"). Express it there, anchored
> to the zone: stop *inside* the failed zone, target the next zone/level. Do not re-test the free-floating
> flag version.

**Concept:** The better signal may be the *failure* of a defended level, not the original absorption itself. A failed defense plus liquidity pull creates a vacuum move that can travel faster than the original defense setup.

**Local Statistics:**
- RTH `absorption_confirmed`, `direction=down`: aligned with down closes only 38.9%
- RTH `absorption_invalidated`, `direction=down`: flipped to opposite-direction close behavior 58.8%

This is not enough to call it validated, but it is enough to promote failure-of-defense into a first-class research track.

**Setup — Failed Absorption Reversal / Vacuum:**
- Context:
  - Absorption detected at a key level
  - Price does not reject cleanly
  - Absorption invalidates or times out
  - DOM shows pulling through the defended level
  - Pace expands into the break
- Entry:
  - Through the failed zone, not at the original defense price
- Stop:
  - Back inside the defended absorption zone
- Target 1:
  - Next nearby key level
- Target 2:
  - Opposite value edge if the move becomes inventory-clearing

**Critical Rule:**
- Do not treat visible resting size as sufficient evidence.
- Require:
  - failed defense
  - pace expansion
  - liquidity pull / inability to refill

**Implementation Notes:**
- Extend absorption tracking with:
  - `absorption_state = detected | confirmed | invalidated`
  - `time_to_invalidation_ms`
  - `liquidity_pull_rate`
  - `pace_at_failure`
- Tie invalidation to level context:
  - IB high / low
  - prior day high / low
  - VAH / VAL
  - DNVA boundary

**Backtesting Hypothesis:**
> When absorption at a key level invalidates within X minutes and pace percentile expands above Y, what is the directional follow-through over the next 15 and 30 minutes?
