# Phase 2 Options Data — Databento Memo

**Date:** 2026-03-05  
**Status:** Preferred provider (ADR-013)  
**Purpose:** Don't forget. Sketch of what using Databento would look like.

---

## Summary

Databento is the preferred options data provider for Phase 2. We ingest raw options chains (OPRA for NDX/SPX, CME for NQ futures options) and **compute all Greeks and GEX ourselves** in Rust. This gives us a robust, fully controlled model — no black-box vendor Greeks.

---

## Why Databento

| Criterion | Databento |
|-----------|-----------|
| **NQ/NDX coverage** | OPRA (NDX, SPX, SPY, QQQ) + CME Globex (NQ futures options). Single source for both. |
| **Data model** | Raw — prices, OI, reference data. No pre-computed IV or Greeks. We build the model. |
| **Rust** | Official `databento` crate. |
| **Docs** | Strong. Examples for options chains, IV estimation (Black-76), Greeks. |
| **Pricing** | Historical: ~$0.04/GB (OPRA), ~$0.50/GB (CME). Live: subscription (~$199/mo) or usage-based. $125 free credits for new users. |

---

## Architecture Sketch

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  The Desk — Phase 2 Options Pipeline                                         │
└─────────────────────────────────────────────────────────────────────────────┘

  Databento API (REST / Historical / Live)
  ├── OPRA.PILLAR    → NDX, SPX, SPY, QQQ options (equity/index)
  └── GLBX.MDP3      → NQ futures options (CME)

         │
         │  databento crate (Rust)
         │  - Timeseries API (get range)
         │  - Symbology (parent → chain, strikes, expirations)
         │  - Instrument definitions (OI, strike, expiry)
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  New: options/ module (Rust)                                                 │
│                                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐   │
│  │ DatabentoClient │───▶│ OptionsIngest   │───▶│ GreeksEngine           │   │
│  │ (config: API    │    │ - Fetch chains  │    │ - Black-76 (futures)    │   │
│  │  key, symbols,  │    │ - Parse OI,     │    │ - Black-Scholes (index) │   │
│  │  refresh_interval)│  │   strike, expiry│    │ - Delta, gamma, charm,  │   │
│  └─────────────────┘    └─────────────────┘    │   vanna                  │   │
│                                              └───────────┬───────────────┘   │
│                                                          │                   │
│                                                          ▼                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ GEXAggregator                                                        │   │
│  │ - GEX by strike = gamma × OI × contract_mult × sign(dealer)         │   │
│  │ - Sum across strikes/expirations                                    │   │
│  │ - Key levels: zero gamma, max gamma, GEX flip                        │   │
│  │ - Dealer positioning: long gamma vs short gamma regions             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                          │                   │
│                                                          ▼                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ OptionsState (pipeline output)                                       │   │
│  │ - gex_by_strike: Vec<(strike, gex)>                                  │   │
│  │ - key_levels: zero_gamma, max_gamma, gex_flip                       │   │
│  │ - dealer_positioning: long_gamma_above, short_gamma_below, etc.     │   │
│  │ - charm_vanna_flow: optional P1                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
         │
         │  OptionsState merged into MarketState
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Rules Engine (ConditionField)                                               │
│  - gex_level, gamma_exposure_sign, dealer_positioning                        │
└─────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  MCP tools, LLM coaching prompts                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow (5-min refresh during RTH)

1. **Config**: `~/.the-desk/config.toml` — `databento_api_key`, `options_symbols` (e.g. `NQ`, `NDX`), `options_refresh_minutes` (default 5).
2. **Background task**: Every N minutes during RTH, call Databento Historical API for current options chains (or subscribe to live if on subscription).
3. **Ingest**: Resolve parent symbols → full chain (strikes, expirations). Fetch OI from instrument definitions / statistics.
4. **Greeks**: For each contract, compute delta, gamma, charm, vanna via Black-76 (futures) or Black-Scholes (index). Need: underlying price (from .scid or Databento US Equities), risk-free rate, IV (solve from mid or use prior).
5. **GEX**: `GEX_strike = gamma × OI × multiplier × sign`. Aggregate. Find zero gamma, max gamma, GEX flip.
6. **OptionsState**: Emit structured snapshot. Merge into MarketState. Persist to SQLite for research.

---

## Implementation Checklist (when Phase 2 starts)

- [ ] Add `databento` crate to `Cargo.toml`
- [ ] Create `src-tauri/src/options/` module
- [ ] Implement `DatabentoClient` (auth, timeseries, symbology)
- [ ] Implement `GreeksEngine` (Black-76, Black-Scholes; delta, gamma, charm, vanna)
- [ ] Implement `GEXAggregator` (GEX by strike, key levels, dealer positioning)
- [ ] Add `OptionsState` to `MarketState`, pipeline snapshot
- [ ] Add `ConditionField` variants for GEX, gamma sign, dealer
- [ ] Background task: configurable refresh interval
- [ ] Graceful degradation: all features work when options disabled or API unavailable
- [ ] Unit tests: known options chain → expected Greeks/GEX

---

## Alternatives (if Databento build proves too heavy)

- **ConvexValue** — Pre-computed gamma, gxoi (gamma × OI), gxvolm (gamma × volume). API at convexvalue.com. Has charm, vanna. Evaluate for faster path.
- **Unusual Whales** — Pre-computed GEX by strike, charm, vanna. JSON API. Fastest path if we abandon own model.

---

## References

- [Databento Options](https://databento.com/options)
- [Databento OPRA dataset](https://databento.com/datasets/OPRA.PILLAR)
- [Databento CME Globex](https://databento.com/catalog/cme/GLBX.MDP3)
- [Estimating IV with Black-76 (Databento docs)](https://databento.com/docs/examples/options/estimating-implied-volatility)
- [ConvexValue API params](https://convexvalue.com/docs/params) — gamma, gxoi, gxvolm, charm, vanna
