---
id: IDEA-024
title: Market-Maker Pressure Inference
status: Idea
regime: [any]
related: [IDEA-007, IDEA-012, IDEA-020, IDEA-022]
companionSpecs:
  - docs/data-and-backtesting-guide.md
  - docs/dom-replay.md
mcpPointers:
  - tool: get_dom_tape_context_at
    note: Live or historical DOM+tape evidence around a target timestamp or level
  - tool: get_dom_regime_summary
    note: Time-aware DOM persistence, flip, pull, and stack context
  - tool: query_dom_behavior_frequency
    note: Historical frequency of candidate DOM pressure states
  - tool: query_dom_reaction_at_levels
    note: Historical DOM behavior near structural event types and levels
  - tool: get_footprint
    note: Current volume-at-price and delta footprint by zone
  - tool: get_imbalances
    note: Stacked and diagonal footprint imbalance context
  - tool: get_absorption_events
    note: Confirmed and invalidated passive defense evidence
  - tool: get_pinch_events
    note: Rapid inventory transfer / delta-collapse evidence
  - tool: get_session_inventory
    note: Cross-session delta inventory state and DNP migration
hypothesisAnchor: false
---

# IDEA-024 — Market-Maker Pressure Inference

> Design-only spec for a future deterministic inference layer. This is **not** a claim that The Desk can identify a specific market maker, firm, or hidden intent.

## Thesis

The Desk should be able to help agents reason about where liquidity providers may be defending, retreating, skewing, or getting run over in NQ by translating observable book and tape behavior into a structured **market-maker pressure read**.

The useful target is not "market makers are long here" or "dealers are doing X." That is overconfident and usually unknowable from Sierra `.scid` / `.depth` alone. The useful target is:

> "The visible book, executed tape, and structural context are consistent with passive liquidity defending this level, passive liquidity retreating, or aggressive flow adversely selecting resting liquidity."

This belongs as a context and interpretation layer for agents, not as a direct trade trigger.

## Source Ideas

This idea was prompted by the X article:

- **Title:** "How To Implement The Avellaneda-Stoikov Model The Way Serious Market Maker Does (Complete Framework)"
- **Author:** Ruuj / `@RuujSs`
- **Article URL:** `https://x.com/RuujSs/article/2064024603161436390`
- **Original post URL:** `https://x.com/RuujSs/status/2064024603161436390`
- **Access note:** Full text was readable only through a logged-in Chrome session during the research turn.

Core extracted concepts:

- A market maker's actionable fair value is inventory-adjusted, not just the public mid-price.
- Quote placement is a continuous function of mid-price, inventory, volatility, time horizon, and fill sensitivity.
- Wider spreads and thinner displayed depth are rational responses to volatility and adverse-selection risk.
- Persistent skew can reflect inventory pressure or risk appetite, but it is not proof of hidden inventory.
- Failure to replenish after being hit is often more informative than one large resting order.
- Circuit-breaker behavior matters: when jump risk rises, liquidity providers may stop providing normal quotes.

## Non-Goals

- Do not infer named firms, individual participants, or exact market-maker positions.
- Do not claim "MMs are long/short" without explicit caveat language.
- Do not place, modify, or recommend trades.
- Do not fire a playbook alert solely from this layer.
- Do not send raw tick or DOM streams to an LLM.
- Do not build this as a price-prediction model.
- Do not make the LLM discover these states from raw data; deterministic Rust should compute the evidence.

## Conceptual Model

The future output should be a compact state object, not prose-only commentary:

```json
{
  "timestampMs": 1780000000000,
  "level": 21500.0,
  "state": "passiveBidDefense",
  "side": "bid",
  "confidence": "directional",
  "score": 0.64,
  "evidence": [
    "bid depth persisted near touch while seller delta hit the level",
    "net delta negative but price failed to break lower",
    "bid pull rate below session median",
    "absorption confirmed within 4 ticks"
  ],
  "caveats": [
    "DOM is delayed reconstructed depth, not colocated live book",
    "no hidden liquidity or iceberg inference included"
  ]
}
```

The agent-facing language should stay conditional:

> "This looks consistent with passive bid defense at 21,500. Sellers traded into the level, but bid-side liquidity persisted and price failed to break. Treat it as supportive context, not a standalone setup."

## Proposed State Taxonomy

| State | Meaning | Observable Pattern |
|-------|---------|--------------------|
| `passiveBidDefense` | Resting bid liquidity appears to absorb selling | Seller delta into level, bid depth persists or replenishes, price does not break lower |
| `passiveOfferDefense` | Resting offer liquidity appears to absorb buying | Buyer delta into level, ask depth persists or replenishes, price does not break higher |
| `bidRetreat` | Bid-side liquidity is being withdrawn | Bid pull rate rises, near-touch bid depth thins, seller delta or price pressure persists |
| `offerRetreat` | Offer-side liquidity is being withdrawn | Ask pull rate rises, near-touch ask depth thins, buyer delta or price pressure persists |
| `bidReplenishment` | Bids refill after being hit | Same/near price bid depth refreshes after trades consume it |
| `offerReplenishment` | Offers refill after being lifted | Same/near price ask depth refreshes after trades consume it |
| `bidExhaustion` | Bids stop refilling after defense or pullback | Prior bid support disappears, price accepts below the level |
| `offerExhaustion` | Offers stop refilling after rally | Prior ask supply disappears or fails to replenish, price accepts above |
| `adverseSelectionUp` | Passive offers appear to be run over by informed/urgent buying | Strong buy delta, offers pull or fail to replenish, price continues higher |
| `adverseSelectionDown` | Passive bids appear to be run over by informed/urgent selling | Strong sell delta, bids pull or fail to replenish, price continues lower |
| `inventorySkewUp` | Book/tape behavior is consistent with liquidity favoring upside release | Bid support persists, offers retreat, price accepts upward |
| `inventorySkewDown` | Book/tape behavior is consistent with liquidity favoring downside release | Offer resistance persists, bids retreat, price accepts downward |
| `liquidityVacuum` | Both visible liquidity and opposing defense degrade after a level breaks | Pulling accelerates, spread/depth quality worsens, price moves quickly through nearby levels |
| `unclear` | Evidence is mixed or data quality is weak | Conflicting DOM/tape signals, insufficient sample, stale depth, or no relevant level |

The exact enum names can change if implemented. The important contract is that each state must be tied to measurable evidence and caveats.

## Observable Inputs

The first pass should reuse existing data surfaces before adding new persistence:

| Concept | Existing Source |
|---------|-----------------|
| Pull/stack behavior | `get_pull_stack_activity`, `get_liquidity_behavior_at_level`, `get_dom_window` |
| DOM persistence / flips | `get_dom_regime_summary`, persisted `dom_feature_snapshots` |
| Tape pressure | `get_tape_pace`, `get_dom_tape_context_at`, raw tick windows |
| Aggressive side | `.scid` bid/ask volume, footprint, delta profile |
| Passive defense | absorption pipeline, DOM support/resistance, failed movement |
| Failed defense | invalidated absorption, failed rebid/reoffer zone, liquidity vacuum |
| Replenishment / exhaustion | DOM top stack/pull levels, touch depth changes, IDEA-022 mechanics |
| Structural location | key levels, VAH/VAL, POC, DNP/DNVA, VWAP bands, IB/OR/OR5 |
| Participation regime | RVOL, tape pace, day type, regime classifier |
| Inventory context | session delta, DNP migration, `get_session_inventory` |
| Options/dealer overlay | future Phase 2 gamma/charm context, not required for v1 |

## Derived Feature Candidates

These are feature ideas, not implementation commitments.

### Book-Side Pressure

- `near_touch_depth_ratio`: bid depth / ask depth near touch.
- `pull_stack_bias`: existing DOM summary direction and magnitude.
- `bid_pull_rate`, `ask_pull_rate`: side-specific withdrawal pressure.
- `depth_persistence_ms`: how long one side stays dominant.
- `flip_count`: number of liquidity-bias flips in a rolling window.
- `spread_ticks`: widening as liquidity providers demand more edge or step away.

### Replenishment

- `same_price_refill_rate`: displayed size returns after consumption.
- `refill_latency_ms`: time from trade consumption to visible replenishment.
- `replenishment_decay`: refills shrink after repeated tests.
- `failed_replenishment`: price revisits but size no longer reloads.

### Tape / Adverse Selection

- `aggressive_delta_ratio`: net delta / total volume in the window.
- `price_displacement_per_delta`: how far price moves per unit of net delta.
- `absorption_efficiency`: high volume at level with low displacement.
- `runover_score`: aggressive delta plus failed replenishment plus continuation.
- `pinch_severity`: rapid inventory transfer already available from pinch pipeline.

### Structural Context

- `level_type`: prior high/low, VAH/VAL, POC, DNP, VWAP band, IB extension, OR5 mid.
- `distance_to_level_ticks`: exact proximity.
- `acceptance_after_test_ms`: whether price holds beyond the level after the test.
- `failed_acceptance`: break through level, then return back inside.

### Data Quality

- `depth_source`: `.depth` reconstructed, persisted snapshot, unavailable.
- `depth_lag_ms`: expected/reported depth staleness.
- `sample_count`: number of DOM summaries or ticks used.
- `coverage`: whether enough rows exist for the requested window.
- `confidence_floor`: cap confidence if depth/tape coverage is weak.

## Confidence Contract

The output should never be binary. Suggested confidence tiers:

| Tier | Use |
|------|-----|
| `unavailable` | Missing or stale DOM/tape data |
| `weak` | One evidence family supports the read |
| `directional` | Multiple evidence families agree but sample is small |
| `strong` | DOM, tape, structure, and historical analogs agree |
| `validated` | Historical outcome research meets sample-size policy |

Rules:

- `validated` requires backtest/research support and must include sample size through MCP, not cached prose.
- Confidence must be capped at `directional` when DOM is reconstructed with material lag or sparse coverage.
- Confidence must be capped at `weak` when only tape or only DOM supports the read.
- If evidence conflicts, return `unclear` with the conflict called out.

## Agent Language Contract

Allowed:

- "This is consistent with passive bid defense."
- "Visible liquidity retreated from the bid side."
- "The book/tape combination suggests adverse-selection risk for passive bids."
- "Offers stopped replenishing after lifts, which weakens the rally-fuel read."
- "Treat this as context, not a standalone setup."

Avoid:

- "Market makers are long."
- "Market makers will defend 21,500."
- "They are trapping buyers."
- "This level must hold."
- "Take the trade because MMs are supporting it."

Preferred output shape for agents:

```text
MM pressure read: passive offer defense, directional confidence.
Evidence: buyers lifted into 21,500, offers replenished twice, price failed to accept above.
Caveat: DOM is delayed reconstructed depth; no hidden-liquidity inference.
Playbook relevance: location only; still require your setup confirmation.
```

## Candidate Architecture

```text
Sierra .scid + .depth
  -> existing pipelines and DOM feature snapshots
  -> market_maker_pressure classifier (future Rust module)
  -> optional pressure_events table
  -> MCP tools expose compact state + evidence
  -> agents synthesize conditional read with caveats
```

Layer placement:

- Layer 1 / 2.5 deterministic Rust can compute state labels and evidence scores.
- Layer 3 agents can explain the state in trader-facing language.
- Rules engine should not consume this as a setup trigger until a future trader-approved playbook rule exists.

Potential modules if implemented:

- `src/pipelines/market_maker_pressure.rs` or `src/research/mm_pressure.rs`
- `MarketMakerPressureState`
- `MarketMakerPressureEvidence`
- `MarketMakerPressureConfidence`
- MCP tools under DOM or Market domain:
  - `get_mm_pressure_at_level`
  - `get_mm_pressure_window`
  - `query_mm_pressure_frequency`
  - `query_mm_pressure_conditional`

Do not add these names blindly. Re-check module boundaries before implementation.

## Historical Research Questions

First validation pass should ask structural questions before any setup integration:

1. When `passiveBidDefense` appears at prior VAL / DNP / VWAP lower band, how often does price revisit DNP, VWAP, or prior value within 15/30/60 minutes?
2. When `passiveOfferDefense` appears at prior VAH / VWAP upper band / IB extension, how often does price fail acceptance above within 15/30/60 minutes?
3. When `bidRetreat` appears after a defended level breaks, how often does price continue at least 0.5x IB or one VWAP sigma?
4. Does `offerReplenishment` during rallies improve continuation odds or mark eventual exhaustion?
5. Does failed replenishment after repeated lifts predict reversal better than ordinary absorption events?
6. Does pressure-state alignment improve IDEA-020 rebid/reoffer zone outcomes?
7. Does pressure-state alignment improve IDEA-022 rally offer replenishment reads?
8. Are pressure states useful in Globex, or only during RTH with better participation?
9. Does RVOL/tape pace gate the reliability of each pressure state?
10. Does the state add information beyond existing absorption, pinch, and rebid/reoffer signals?

No statistic should be written into this file unless marked as point-in-time. Pull fresh numbers from MCP/SQLite when researching.

## Backtesting / Research Guardrails

- Use `docs/data-and-backtesting-guide.md` before running any historical work.
- Respect the Research Sample Size Policy from `AGENT.md`.
- Separate RTH and Globex.
- Keep contract rollover scope explicit.
- Treat DOM research as dependent on `dom_feature_snapshots` / `.depth` coverage.
- Compare candidate states against simpler baselines:
  - existing absorption only
  - existing rebid/reoffer only
  - existing DOM liquidity bias only
  - existing tape pace / RVOL only
- Reject states that fire too often or merely restate existing signals.

## Build Phases

### Phase 0 — Documentation and doctrine

- Keep this spec as the conceptual contract.
- Add an agent-facing glossary only after the taxonomy is reviewed.
- Do not implement until we choose the first narrow state family.

### Phase 1 — Read-only research notebooks / MCP usage

- Use existing tools to manually inspect 20-50 candidate examples.
- Save examples as screenshots or notes outside hot-path code.
- Refine the taxonomy before adding new fields.

### Phase 2 — Deterministic prototype

- Implement one narrow classifier first:
  - `passiveBidDefense`
  - `passiveOfferDefense`
  - `bidRetreat`
  - `offerRetreat`
- Emit confidence and evidence arrays.
- Do not connect to playbook setups.

### Phase 3 — Historical event logging

- Persist pressure-state events with timestamp, level, side, state, confidence, evidence hash, and data-quality fields.
- Backfill from `.scid` + `.depth` where feasible.
- Add research queries only after event schema stabilizes.

### Phase 4 — Agent integration

- Add an orchestrator/orderflow-analyst routing note:
  - use pressure read when trader asks what liquidity providers may be doing
  - use pressure read around active setup levels
  - always include caveats
- Still no direct alerting.

### Phase 5 — Playbook review

- If historical results are reportable and useful, decide whether specific pressure states become optional confirmation conditions for existing setups.
- Any rules-engine integration must be trader-approved and remain deterministic.

## Relationship To Existing Ideas

- **IDEA-007 Microstructure Regime Detection:** This can become a sub-regime input, but should be more local and level-specific.
- **IDEA-012 Absorption Failure / Liquidity Vacuum:** This provides the failed-defense family; IDEA-024 generalizes it into a broader pressure taxonomy.
- **IDEA-020 Rebid/Reoffer Zone Lifecycle:** Pressure states can corroborate whether a zone is holding, failing, or becoming vacuum-like.
- **IDEA-022 Rally Offer Replenishment:** This is a specific mechanics slice of the broader replenishment/exhaustion family.
- **Phase 2 Options / Gamma:** Dealer hedging context can become an overlay later, but v1 should work without options data.

## Open Questions

1. Should this live as a DOM-domain tool or a market-domain tool?
2. Should the first implementation be event-based, window-based, or level-based?
3. What is the minimum DOM coverage needed for a reportable pressure read?
4. Should pressure states be persisted live, computed on demand, or both?
5. What is the first narrow state family worth validating manually?
6. Should options/gamma context be explicitly excluded from v1 to keep scope tight?
7. How should agents phrase uncertainty when the state is plausible but historically unvalidated?

## Suggested First Slice

The first build candidate should be:

> Level-based passive defense vs retreat around key levels.

Inputs:

- target level and radius
- 30-90 second DOM window
- same-window footprint / net delta
- absorption confirmation or invalidation
- post-test acceptance over 1-5 minutes

Outputs:

- `passiveBidDefense`
- `passiveOfferDefense`
- `bidRetreat`
- `offerRetreat`
- `unclear`

This is narrow enough to validate visually and broad enough to matter for the trader's actual DOM/order-flow workflow.

## See also

- Hub stub: [setup-ideas-and-backtesting.md#idea-024](../setup-ideas-and-backtesting.md#idea-024)
- Setup idea index: [index.md](index.md)
- Data guide: [data-and-backtesting-guide.md](../data-and-backtesting-guide.md)
- DOM replay: [dom-replay.md](../dom-replay.md)
- Trading domain skill: [skills/trading-domain/SKILL.md](../../skills/trading-domain/SKILL.md)
