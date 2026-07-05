# IDEA-016: VWAP Pipeline Enhancements (Dual Session + Anchored)

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Idea
**Source:** QA review of `vwap.rs` pipeline, March 2026
**Complements:** VWAP Band Zone Entry (tpl_vwap_band_zone), all VWAP-referencing setups

**Concept:** The current VWAP pipeline is mathematically correct and incremental, but it only supports a single session-anchored VWAP at a time. Two enhancements would increase its value as a trading reference:

**Enhancement 1 — Dual VWAP (Globex + Developing RTH):**

Currently VWAP resets fully at each session boundary (6 PM ET for Globex, 9:30 AM ET for RTH). This means:
- During Globex, there is one VWAP covering Asia + London (correct — London does not reset it)
- At RTH open, the Globex VWAP is discarded and a fresh RTH VWAP begins

The problem: Globex VWAP is a meaningful reference level during the first 30-60 minutes of RTH, especially on London-to-RTH handoff and gap days. Losing it at 9:30 removes context the trader needs.

- Add a second `VwapPipeline` accumulator to `PipelineEngine` (e.g., `vwap_prior_session`)
- At RTH open, snapshot the Globex VWAP + bands into `prior_globex_vwap`, `prior_globex_vwap_1sd_upper/lower`
- Expose in MarketState for the first 60-90 minutes of RTH, then let it age out
- Zero additional per-tick cost (just a snapshot at boundary)

**Enhancement 2 — Anchored VWAP:**

Allow VWAP to be anchored from a user-specified event or time, not just the session open. Common anchors:
- Previous day's high/low (naked VPOC equivalent for VWAP)
- Significant absorption event
- IB high/low break
- OR5 break

- Add a small `AnchoredVwap` struct (same `sum_pv / sum_v` math, separate accumulator)
- Allow 1-3 active anchored VWAPs at a time via MCP tool (e.g., `anchor_vwap { from_timestamp_ms }`)
- Each anchored VWAP accumulates independently and can be queried or cleared
- Useful for playbook rules that reference "VWAP from the break" or "VWAP from the session low"

**Implementation Notes:**
- Enhancement 1 is trivial — one extra `VwapPipeline` instance + snapshot at boundary
- Enhancement 2 requires MCP tool integration and a small vec of active anchors
- Both are O(1) per tick, no recalculation
- Add `prior_globex_vwap`, `prior_globex_vwap_1sd_upper`, `prior_globex_vwap_1sd_lower` to MarketState
- Add `anchored_vwaps: Vec<AnchoredVwapState>` (capped at 3) with MCP create/clear tools

**Backtesting Hypotheses:**
> On London-to-RTH unwind days (IDEA-014), does prior Globex VWAP act as support/resistance during the first 60 minutes of RTH?

> When VWAP is anchored from the IB break point, does price respect the anchored VWAP ±1SD bands more reliably than session VWAP bands for continuation entries?
