# IDEA-013: Gamma-Gated Setup Overlay

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Researched
**Source:** Local 2025-11-28 through 2026-03-06 database study; Cboe March 2026 volume data; Dim/Eraker/Vilkov; Adams/Fontaine/Ornthanalai
**Complements:** IDEA-000 Regime Selector, IDEA-008 0DTE Gamma Regime Trading
**Requires:** External gamma / wall / flip data

**Concept:** Gamma should not be treated as a standalone setup. It should be used as a selector for which of *your existing setups* are appropriate.

**Current-Market Motivation (as of 2026-03-09):**
- Cboe reported SPX 0DTE volume hit a record 63% of SPX trading in February 2026
- NQ already has Monday-Friday weekly expiries on CME
- Recent literature suggests regime dependence matters more than blanket "0DTE causes volatility" claims:
  - Positive dealer gamma tends to strengthen reversal behavior
  - Negative dealer gamma tends to strengthen momentum behavior
  - Broad market impact can be modest on average, so the useful application is *filtering*, not narrative overreach

**Overlay Rules:**
- **Positive gamma / inside major wall**
  - Favor:
    - DNVA retest
    - VWAP band repair
    - failed-breakout traps
    - session inventory clear
  - De-emphasize:
    - blind breakout continuation
- **Negative gamma / outside major wall**
  - Favor:
    - OR5 continuation
    - one-sided IB extension acceptance
    - single-print continuation
    - acceleration-zone hold
  - De-emphasize:
    - passive mean reversion

**Implementation Notes:**
- Use the same gamma data feed planned in IDEA-008
- Add:
  - `gamma_regime`
  - `inside_major_gamma_wall`
  - `distance_to_call_wall`
  - `distance_to_put_wall`
- Feed those fields into the regime selector first, then the setup templates

**Backtesting Hypothesis:**
> Does positive-gamma gating improve DNVA / VWAP-band expectancy, and does negative-gamma gating improve OR5 / IB-extension expectancy, versus ungated baseline?
