---
name: TradingDomain
description: Trading domain knowledge for The Desk. USE WHEN implementing market structure pipelines, playbook builder, rules engine, or any code that processes trading data. Contains TPO/Market Profile theory, delta neutral calculations, order flow concepts, and NQ-specific microstructure.
---

# Trading Domain Knowledge

Reference material for LLM coding agents working on The Desk. This skill ensures domain-correct implementations of trading concepts.

---

## Market Profile / TPO (Time Price Opportunity)

### Core Concepts

**TPO** — A single letter printed at a price level for each time period (typically 30-minute brackets labeled A, B, C, etc.). Each letter represents that price was visited during that time bracket.

**Value Area (VA)** — The price range containing 70% of TPOs. Represents the range where the market spent most of its time. Calculated by starting at the POC and alternating between adding the next highest TPO count above and below until 70% is reached.

**Point of Control (POC)** — The price level with the highest TPO count (most time spent). The "fairest price" by time.

**Value Area High (VAH)** — Upper boundary of the 70% value area.

**Value Area Low (VAL)** — Lower boundary of the 70% value area.

**Single Prints** — Price levels with only one TPO letter. Represent initiative activity where price moved quickly through a level without revisiting. Single prints are "unfinished business" — the market often returns to fill them.

**Initial Balance (IB)** — The price range established in the first 60 minutes of RTH (Regular Trading Hours, 9:30-10:30 ET). Brackets A and B.

**Opening Range (OR)** — The price range established in the first 30 minutes of RTH (9:30-10:00 ET). Bracket A only.

**Excess** — TPO tails at the extremes of a profile. Long tails (3+ single TPOs) at the top or bottom indicate strong rejection of those prices.

### Day Types

| Type | IB Characteristic | Behavior |
|------|-------------------|----------|
| **Trend Day** | Small IB, then range extension in one direction | Price breaks IB and never returns. Strong directional conviction. |
| **Normal Day** | Wide IB contains most of the day's range | 85% of activity within IB. Low directional conviction. |
| **Normal Variation** | Moderate IB with range extension | IB broken but price returns to value. |
| **Neutral Day** | IB broken on both sides | No directional bias. Rotational. |
| **Double Distribution** | Two distinct value areas | Market transitions from one balance to another. |

### Implementation Notes

- TPO brackets are typically 30 minutes. The first bracket (A) starts at RTH open (9:30 ET).
- Price must be discretized to the instrument's tick size. For NQ, minimum tick = 0.25 points.
- A "TPO count" at a price level = number of distinct bracket letters that printed at that level.
- Value area calculation: Start at POC. Look at the two price levels immediately above and below. Whichever pair has more TPOs, add those to the value area. Continue alternating until ≥70% of total TPOs are included.
- Single prints are identified per-bracket: if a price level has exactly 1 TPO letter during a bracket where adjacent levels have 0, it's a single print.

---

## Delta Neutral Value Area (DNVA)

### Core Concepts

**Delta** — The difference between buying volume and selling volume at a price level. Buy volume (trades at or above the ask) minus sell volume (trades at or below the bid).

**Delta Profile** — Just like a volume profile shows volume at each price, a delta profile shows net delta at each price level. Positive delta = more buying; negative delta = more selling.

**Delta Neutral Value Area (DNVA)** — Analogous to the volume-based value area, but calculated using delta instead of volume. The price range containing 70% of the absolute delta. This shows where the most directional conviction occurred.

**Delta Neutral Pivot (DNP)** — The price level where cumulative delta crosses zero when accumulating from one end of the profile. Analogous to the POC but for delta. This is the "balance point" between buyers and sellers.

### How DNVA Differs From Volume VA

| Aspect | Volume VA | Delta VA |
|--------|-----------|----------|
| Measures | Total activity (buys + sells) | Net directional activity (buys - sells) |
| Width | Where the most trading happened | Where the most directional conviction existed |
| Relationship | Can be wide on a balanced day | Can be narrow if both sides equally aggressive |

**Critical insight:** The DNVA may be wider, narrower, above, or below the volume-based VA. When they diverge, it reveals where passive and aggressive participants disagree.

### Key Setups

**DNVA Reversion** — When price is outside the DNVA and re-enters:
1. Price moves outside the DNVA boundary
2. Price returns and re-enters the DNVA
3. Look for a rebid (long) or reoffer (short) at the boundary level
4. Target 1: DNP (take off a contract)
5. Target 2: Opposite DNVA boundary (hold runner)
6. Edge improves on the "retest" version — price breaches, returns, and holds

### Implementation Notes

- Trade direction classification: trades at ask or above = buy (positive delta). Trades at bid or below = sell (negative delta). Trades between bid and ask: classify by proximity to nearest side.
- Delta at a price level = sum of (buy volume - sell volume) for all trades at that price.
- DNVA calculation mirrors VA calculation but uses absolute delta values. Start at the price with the highest absolute delta, expand outward until 70% of total absolute delta is captured.
- DNP: accumulate delta from the lowest price upward. The price where the running sum crosses zero is the pivot. If delta never crosses zero, the pivot is at the extreme (entirely one-sided session).
- Update calculations incrementally as new trades arrive — don't recalculate the entire profile each tick.

---

## Order Flow Concepts

### Delta Divergence

Price makes a new high/low but cumulative delta does not confirm:
- **Bearish divergence:** Price makes higher high, but cumulative delta makes lower high → buyers are losing momentum
- **Bullish divergence:** Price makes lower low, but cumulative delta makes higher low → sellers are losing momentum

### Volume Imbalance

Stacked buy or sell imbalance on the DOM across multiple price levels:
- **Bid imbalance:** Significantly more volume on bid than ask across 3+ consecutive price levels
- **Ask imbalance:** Significantly more volume on ask than bid across 3+ consecutive price levels
- Threshold: typically 2:1 or 3:1 ratio to qualify as "imbalance"

### Absorption

Large resting orders being filled without moving price:
- Price approaches a level with large resting orders (visible on DOM)
- Volume transacts at that level (high volume at price) but price doesn't break through
- Indicates a large participant defending that level
- If absorption eventually fails (level breaks), the move is often strong

### Initiative vs. Responsive Activity

- **Initiative:** Activity that moves price into new territory (breaking out of value area, extending range). Aggressive.
- **Responsive:** Activity that pushes price back toward value (mean reversion at extremes). Defensive.
- Single prints in TPO are evidence of initiative activity.
- Excess tails in TPO are evidence of responsive activity.

---

## NQ-Specific Microstructure

### Contract Specifications

| Attribute | Value |
|-----------|-------|
| Symbol | NQ (E-mini Nasdaq 100) |
| Exchange | CME Globex |
| Tick size | 0.25 points |
| Tick value | $5.00 per tick ($20.00 per point) |
| RTH | 9:30 AM - 4:15 PM ET |
| Globex | 6:00 PM - 5:00 PM ET (next day), Sunday-Friday |
| Contract months | H (Mar), M (Jun), U (Sep), Z (Dec) |
| Roll | Typically 2nd Thursday of expiration month |

### Key Sessions

| Session | Time (ET) | Significance |
|---------|-----------|-------------|
| Globex open | 6:00 PM (prior day) | Overnight session begins |
| London open | 3:00 AM | European participation begins, often establishes overnight range |
| Pre-market | 8:00-9:30 AM | US pre-market, often sets the opening tone |
| RTH open | 9:30 AM | Primary session begins, highest volume period |
| Opening range | 9:30-10:00 AM | First 30 minutes, establishes OR |
| Initial balance | 9:30-10:30 AM | First hour, establishes IB |
| Lunch | 12:00-1:30 PM | Lower volume, rotational |
| MOC | 3:45-4:00 PM | Market-on-close orders, volume spike |
| RTH close | 4:15 PM | Primary session ends |

### NQ Behavioral Characteristics

- NQ is tech-heavy and tends to be more volatile than ES
- Strongly correlated with ES but can diverge on tech-specific catalysts
- Overnight ranges tend to be wider than ES relative to daily range
- Responds strongly to FOMC, CPI, PPI, employment data
- Options gamma on NDX/QQQ/SPX affects NQ price behavior (dealer hedging)
- Round numbers (every 100 points: 21,400, 21,500, etc.) act as psychological levels

---

## Options / Gamma Context (Phase 2 Reference)

### Gamma Exposure (GEX)

**What it is:** The aggregate gamma positioning of options market makers at each strike price. Indicates where hedging flows will accelerate or dampen price movement.

**Positive gamma (above the "gamma flip" level):** Market makers are long gamma → they hedge by selling rallies and buying dips → dampens volatility, creates mean-reversion.

**Negative gamma (below the "gamma flip" level):** Market makers are short gamma → they hedge by buying rallies and selling dips → amplifies volatility, creates momentum.

**Why it matters for NQ:** SPX options dominate the gamma landscape. Large dealer positions at specific strikes create "walls" (resistance) and "floors" (support). The gamma flip level often acts as a regime change point.

### Charm (Delta Decay)

Options delta changes as time passes (theta). Charm measures this: how delta changes with time. As options approach expiration, their delta accelerates toward 0 or 1, which forces market makers to adjust hedges. This creates directional flows, especially on 0DTE expiration days.

### Implementation Notes

- GEX data comes from external providers (Unusual Whales, CBOE, etc.) — not computed from The Desk's market data
- SPX gamma is the primary driver even for NQ trades (since SPX options are far more liquid)
- Refresh GEX data every 5-15 minutes during RTH (not real-time — options data updates are slower)
- Display key levels: gamma flip, major positive/negative gamma walls, 0DTE gamma contribution

---

## Common Implementation Mistakes to Avoid

1. **Confusing volume profile with TPO profile.** Volume profile uses volume at price; TPO uses time at price. They often look similar but can diverge significantly.

2. **Wrong delta classification.** Trades between bid and ask must be classified — don't ignore them. Classify by proximity: closer to ask = buy, closer to bid = sell, at midpoint = split or ignore.

3. **Recalculating entire profiles each tick.** All profile calculations (TPO, volume, delta) should be incremental. Add the new data point; don't rebuild from scratch.

4. **Wrong tick size for NQ.** NQ tick size is 0.25 points, not 0.01. All price levels in profiles should be discretized to 0.25 increments.

5. **Confusing R (risk unit) with points.** R is trader-defined. If a trader's stop is 8 NQ points with 1 contract, 1R = 8 points = $160. Always let the trader define their R.

6. **Treating VWAP as a support/resistance level.** VWAP is a dynamic level that moves with each trade. It's a mean, not a wall. Price can slice through it easily. Its value is as a reference point for relative positioning.

7. **Calculating value area from the wrong direction.** Value area starts at POC and expands outward symmetrically, comparing TPO counts on each side. Don't calculate it as "middle 70% of the range."

8. **Ignoring the difference between RTH and Globex data.** TPO profiles, VWAP, and delta calculations should be clearly scoped to the correct session. Mixing RTH and Globex data produces meaningless results.
