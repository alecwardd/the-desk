# IDEA-014: London Inventory Unwind Into RTH

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Researched
**Source:** Local 2025-11-28 through 2026-03-06 database study
**Complements:** Session Inventory Clear (tpl_session_inventory_clear), DNVA Retest (tpl_dnva_retest), VWAP Band Zone Entry (tpl_vwap_band_zone)

**Concept:** In the current local sample, London direction was more likely to unwind than continue into RTH. This suggests a dedicated handoff setup: trade the unwind only when RTH opens back into value and inventory begins clearing.

**Local Statistics:**
- London and RTH closed same direction only 41.5%
- Reversal happened 58.5%
- Reverse handoff days were mostly high-RVOL and often `DoubleDistribution`

**Setup — London Inventory Unwind:**
- Context:
  - London trended materially
  - RTH opens back inside prior value, overnight value, or current developing value
  - DNP / VWAP reclaim confirms clearing
  - No clean one-sided acceptance away from value
- Entry:
  - First pullback after reclaim of DNP / VWAP / value edge
- Stop:
  - Back through the reclaim level or back outside accepted value
- Target 1:
  - Developing POC or prior close
- Target 2:
  - Opposite side of current value if unwind becomes full migration

**Do Not Use When:**
- London delta and price both remain one-sided through the RTH open
- RTH immediately shows one-sided IB extension acceptance
- Gamma / event regime strongly favors continuation instead of repair

**Implementation Notes:**
- Add a `london_rth_handoff_state`:
  - `Continuation`
  - `Unwind`
  - `Unclear`
- Inputs:
  - London open/close direction
  - RTH open relative to prior / overnight value
  - DNP / VWAP acceptance
  - early RTH delta sign

**Backtesting Hypothesis:**
> When London trends but RTH opens back inside value and reclaims DNP/VWAP, what is the probability of a move to POC or opposite value edge before IB completes?
