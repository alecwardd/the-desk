# IDEA-007: Microstructure Regime Detection

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Researched
**Source:** HMM literature, Park & Kownatzki 2024, Lopez de Prado 2018
**Complements:** All setups (meta-filter)

**Concept:** Classify the current microstructure regime in real-time and use it as a meta-filter for all playbook setups. Run momentum setups in trending regimes, mean-reversion setups in rotational regimes, reduce size in transition regimes.

**3-State Model:**
1. **Trend** — High directional autocorrelation, expanding range, persistent order flow imbalance
2. **Rotation** — Low autocorrelation, contracting range, balanced order flow
3. **Transition/High-Vol** — Elevated realized vol, regime uncertainty

**Simpler Volatility Regime Detector (start here):**
- Compute 5-min realized volatility using log returns
- Compare to 20-day rolling average at same time-of-day
- RV ratio > 1.5: Trending → momentum setups
- RV ratio 0.7-1.3: Normal → full playbook
- RV ratio < 0.7: Compressed → breakout imminent, reduce reversion setups

**Advanced: Hidden Markov Model:**
- 3-state HMM on returns + volatility at 1-min frequency
- Academic Sharpe > 2.0 pre-cost on e-mini S&P500
- Requires: state estimation library in Rust or pre-computed in Python/exported

**Implementation Notes:**
- Start with the volatility ratio approach (simple, no ML dependency)
- Add `regime: MicrostructureRegime` to MarketState
- Rules engine checks regime before evaluating setups
- Later: implement HMM in Rust using `nalgebra` for matrix ops

**Backtesting Hypothesis:**
> What is the win rate improvement when filtering DNVA reversion and VWAP band setups to only fire in Rotation regime (RV ratio 0.7-1.3)?
