---
id: IDEA-005
title: Session Transition Sweep Patterns
status: Researched
regime: [any]
related: []
companionSpecs: []
mcpPointers: []
hypothesisAnchor: false
---

# IDEA-005 — Session Transition Sweep Patterns

> Per-idea detail file. The hub ([setup-ideas-and-backtesting.md](../setup-ideas-and-backtesting.md)) keeps a short stub (status, source, framing, detail link) pointing here.

<!-- stats: point-in-time -->
**Status:** Researched
**Source:** Multi-session analysis, institutional flow patterns
**Complements:** Session Inventory Clear (tpl_session_inventory_clear)

**Concept:** Session transitions (Asia→London, London→RTH) create predictable liquidity sweep patterns. London almost always sweeps one side of the Asian range. The direction of RTH relative to the London sweep is a strong directional signal.

**Setup — Asia Range Sweep → London Continuation:**
- Pre-market: Mark Asia session high/low (6 PM - 2 AM ET)
- London open (3 AM ET): Watch for sweep of Asia high or low
- Entry: After London sweeps Asia range and shows displacement in opposite direction, enter on first pullback
- Stop: Beyond the Asia range extreme that was swept
- Target 1: Opposite side of Asia range
- Target 2: Full London session range extension (50-100+ NQ points)

**Setup — Three-Session Alignment:**
- Context: Asia, London, and RTH all move in same direction
- Entry: Any pullback to intraday VWAP or developing POC
- Stop: Below developing VAL (longs) / above developing VAH (shorts)
- Target: Extended range — highest conviction for trend days

**Setup — London-RTH Gap Direction:**
- Tuesday gaps fill ~70%
- Monday gap-ups fill only 53%
- If gap is in London's direction → continuation; if opposed → gap fill probable

**Implementation Notes:**
- Already have `globex_or30_high/low`, `london_or60_high/low` in MarketState
- Add `asia_session_high/low` tracking to `LevelsPipeline`
- Add sweep detection: price exceeds Asia high/low then reverses → "sweep" event
- Add three-session alignment flag to MarketState

**Backtesting Hypotheses:**
> When London sweeps the Asia high and then reverses, what is the probability RTH continues in the reversal direction?

> On three-session alignment days, what is the average range extension beyond IB?

---

## See also

- Hub stub: [setup-ideas-and-backtesting.md#idea-005](../setup-ideas-and-backtesting.md#idea-005)
- Setup index: [index.md](index.md)
