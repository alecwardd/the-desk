# IDEA-000: Regime-Gated Setup Selector

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

<!-- hypothesis-anchor: IDEA-000 -->

**Status:** REJECTED as a standalone setup (2026-06-23) — concept folded into IDEA-020.
**Source:** Local 2025-11-28 through 2026-03-06 database study; 0DTE / dealer gamma literature; CME liquidity research

> **Verdict (2026-06-23): rejected as a tradeable setup; retained as a context *gate*, reconstructed in IDEA-020.**
> v2 backtest (regime gate + VWAP-pullback entry) on 2025-11-28…2026-03-06: gated N=2, baseline N=3, both
> 0% win / −1.0R — untestable. Root cause is a *design contradiction*, not "no edge":
> `regime=OneSidedAcceptance` means price is accepted **away** from VWAP, while `price_vs_vwap within 8`
> requires price **at** VWAP — they almost never co-occur, so the entry never fired. The regime *gate*
> idea is still good, but as a **context layer, not an entry**. Reconstruction: derive regime from **zone
> outcomes** (IDEA-020 Stage 2 — many zones forming+held → trend; many failing → change), which is tighter
> than the IB-extension/day-type classifier that proved too loose, and use it to gate which zone family
> may fire. Do not re-test this as a standalone continuation entry.
**Complements:** All existing setup templates

**Concept:** Stop treating every setup as always-on. Add a top-level regime selector that determines which setup families are valid:
- **Initiative / continuation**
- **Responsive / mean reversion**
- **Transition / stand aside**

The regime layer should drive which existing templates are active, not just how they are narrated.

**Local Rationale:**
- Most valid RTH sessions in the current sample were `DoubleDistribution`, not clean trend days
- London-RTH reversal was more common than London-RTH continuation
- One-sided IB extension had meaning; generic IB extension did not
- Raw pinch did not show enough standalone value to justify unrestricted firing

**Primary Regime Buckets:**
1. **One-Sided Acceptance**
   - High RVOL
   - One-sided IB extension
   - Price accepted above/below VWAP and DNP
   - No meaningful opposite-side extension
   - Allowed setup families:
     - OR5 continuation
     - IB Extension Play
     - Single Print Continuation
     - Rebid / Reoffer hold
2. **Migration / Inventory Clear**
   - Double-distribution behavior
   - Both-side extension or London unwind into RTH
   - Acceptance back into prior value or current value
   - Allowed setup families:
     - DNVA retest
     - VWAP band repair
     - Session inventory clear
     - London inventory unwind
3. **Transition / Liquidity Failure**
   - Mixed direction
   - Failed absorption
   - Liquidity pulling / pace expanding through defended level
   - Allowed setup families:
     - Absorption failure / liquidity vacuum
     - Failed-breakout trap
   - Reduce or disable:
     - Blind continuation entries

**Implementation Notes:**
- Add a top-level `regime_selector` or `setup_family_gate` to `MarketState`
- Inputs can be built from existing pipelines:
  - `day_type`
  - `balance_state`
  - IB extension state
  - London and overnight session direction
  - VWAP / DNP acceptance
  - absorption confirmed vs invalidated
  - pace percentile / RVOL
- Rules engine should check regime before evaluating setup conditions

**Backtesting Hypotheses:**
> Does gating OR5, IB Extension, and Single Print Continuation to one-sided acceptance regimes improve win rate versus ungated firing?

> Does gating DNVA, VWAP band, and session inventory setups to migration / inventory-clear regimes improve expectancy?

**Typed hypothesis spec example:**
```json
{
  "metadata": {
    "hypothesisId": "IDEA-000",
    "version": 1,
    "docReference": "IDEA-000",
    "proseSummary": "One-sided acceptance regime gate for continuation-style setups during RTH.",
    "owner": "user",
    "sessionScope": ["rth"]
  },
  "setupDefinition": {
    "id": "hyp_IDEA-000_v1",
    "name": "IDEA-000 One-Sided Acceptance Gate",
    "description": "Prototype gate: continuation setups are only valid when RTH structure shows one-sided acceptance through value and DNP with elevated participation.",
    "active": false,
    "conditions": [
      "{\"id\":\"c1\",\"field\":\"day_type\",\"operator\":\"equals\",\"value\":\"Trend\",\"label\":\"Trend day context\"}",
      "{\"id\":\"c2\",\"field\":\"rvol_classification\",\"operator\":\"equals\",\"value\":\"High\",\"label\":\"High RVOL participation\"}",
      "{\"id\":\"c3\",\"field\":\"price_vs_vwap\",\"operator\":\"above\",\"label\":\"Price accepted above VWAP\"}",
      "{\"id\":\"c4\",\"field\":\"price_vs_dnp\",\"operator\":\"above\",\"label\":\"Price accepted above DNP\"}"
    ],
    "stopLogic": {
      "mode": "fixed_points",
      "direction": "long",
      "points": 12
    },
    "targets": [
      {
        "mode": "fixed_points",
        "direction": "long",
        "points": 18,
        "label": "1.5R fixed target"
      }
    ],
    "positionSizing": {
      "r_points": 12
    },
    "templateSource": "hypothesis:IDEA-000:v1"
  }
}
```
