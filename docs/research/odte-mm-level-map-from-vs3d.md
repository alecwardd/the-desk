# ODTE Market-Maker Level Map → Positioning Interpretation Layer

**Status:** Living methodology (SIL-P-VS-c / #17)  
**Authority:** Catalog v0 Positioning record kinds (`docs/mcp/desk-catalog-v0.md`), SIL SPEC P-VS interpretation layer (Levels-Only Record is first-class — Catalog / SIL ADR-025 note), SIL-P-VS-c / #17  
**Trust Ceiling:** L3 — drafts context from the trader's annotated Positioning reads; the trader executes  
**Not blocked on:** VolSignals ToS or `Vs3dProvider` (#16)

This document extends the ODTE market-maker level-map methodology from static **positions** reads to gradient **Slices** and Desk-derived levels. Agents use it when recalling co-annotated sessions from the memory system (`recall_agent_insights` / `get_memory_brief`).

Vocabulary (exact): **Positioning**, **Slice**, **Levels-Only Record**. Do not say "options domain", "surface snapshot", or "degraded/fallback record."

---

## 1. What a Positioning read is

Positioning is dealer/MM context for discretionary NQ work, usually SPX/VIX backdrop correlated to NQ. A read is never a proprietary signal. Agent language must stay:

> "Your annotated sessions / your methodology say…"

Never: "you should buy/sell" or "this is a good trade."

### Record kinds (Catalog v0)

| Kind | Catalog id | Role |
|------|------------|------|
| grid | `position_grid` | Primary capture panel (schema v1) when a provider exists |
| by-strike | `positions_by_strike` | Desk-aggregated from grid when available |
| **Slice** | `slice` | Price-indexed greek values at one moment + Desk-derived levels at ingest |
| **Levels-Only Record** | `levels_only` | First-class record carrying only derived levels — manual entry and historical backlog path (Catalog / SIL ADR-025 note) |

Vendor forward projections are live context only; they are never durable facts inside a Slice or Levels-Only Record.

### Desk-derived levels (carried by Slice and Levels-Only Record)

| Level | Meaning in this methodology |
|-------|-----------------------------|
| **flip** | Dealer gamma sign change / zero-gamma hinge the trader marks from the panel |
| **walls** | Concentrated gamma / positioning strikes that acted as magnets or caps in the trader's read |
| **BALANCE** | Spot/regime zone where hedging flow is two-sided / range-compatible |
| **UPSIDE TEST** | Upside probe level where short-gamma or charm flow is expected to be tested |
| **DOWNSIDE TEST** | Downside probe level symmetric to UPSIDE TEST |

These names are the trader's annotated map, not vendor product labels frozen into code.

---

## 2. Baseline: positions → level map

Before gradient Slices, the methodology mapped a morning **positions** panel into a compact level card:

1. Mark **flip** from the gamma (or net) sign change nearest spot.
2. Mark **walls** at the densest strike clusters above/below flip.
3. Classify the open regime as **BALANCE** when spot sits between walls with two-sided hedging pressure, else note which side is already in **TEST**.
4. Write the card as a **Levels-Only Record** when no Slice is available — still first-class Positioning.

That baseline remains valid for backlog days and ToS-denial steady state.

---

## 3. Extension: gradient Slices / derived-level dynamics

A **Slice** adds a price-indexed greek curve (gamma / charm / vanna; delta + delta-change when present) at `capturedAt` / `dataTime`, plus the same Desk-derived levels computed or confirmed at ingest. Interpretation work is about **how those levels move during the session**, not about treating one morning screenshot as eternal.

### 3.1 Flip and wall migration

- **Flip migration through spot:** When spot walks through the prior flip and the next Slice (or mid-day eyeball re-mark) places flip on the other side of price, your annotated sessions treat the prior BALANCE card as stale until re-annotated.
- **Wall migration:** Walls that held in the morning Slice but thin or shift a full strike cluster by midday are re-marked; do not coach off the morning wall after the trader's re-read moved it.
- **Stable wall + migrating flip:** Common on trend days — walls trail the move; flip ratchets. Your methodology says re-rank setups against the *current* flip, not the open print.

### 3.2 Charm drift into expiration

On ODTE / short-dated panels, charm (and related time decay of hedging) drifts levels toward spot into the cash close:

- Afternoon Slices often pull **TEST** levels inward versus the morning card.
- Pin risk near a sticky flip/wall into the final hour is a *context* note from your annotations — not a trade instruction.
- If only a Levels-Only Record exists for the day, charm drift is inferred from the trader's mid-day re-marks of flip/walls/TEST, not from inventing a surface.

### 3.3 BALANCE → TEST transitions

| Transition | What your annotations look for |
|------------|--------------------------------|
| **BALANCE → UPSIDE TEST** | Spot accepts above BALANCE / through upside wall; hedging skew flips one-sided; next Slice or re-mark promotes UPSIDE TEST as the active probe |
| **BALANCE → DOWNSIDE TEST** | Symmetric downside acceptance |
| **TEST → BALANCE** | Failed probe, replenished two-sided hedging, flip re-centers — morning TEST card is superseded |
| **TEST → opposite TEST** | Trend day that never re-balanced; walls/flip migrate with the move |

Agents recalling corpus exemplars should name the transition the trader annotated, then tie playbook readiness to *that* context ("your playbook indicates… given this Positioning transition").

### 3.4 Spot/vol co-movement archetypes

Use archetype tags on corpus insights (not as signals):

| Archetype tag | What your annotations look for |
|---------------|--------------------------------|
| `trend_day_up` / `trend_day_down` | One-sided acceptance with flip/wall migration |
| `spot_up_vol_up` / `spot_down_vol_up` | Vol expansion with the spot move (TEST often expands) |
| `spot_up_vol_down` / `range_balance` | Compress / two-sided BALANCE persistence |
| `balance_to_upside_test` / `balance_to_downside_test` | Explicit BALANCE → TEST transition cards (corpus teaching tags) |
| `charm_drift_into_expiry` | Afternoon inward drift of TEST/flip |
| `wall_migration` | Strike-cluster relocation without full trend day |
| `levels_only_backlog` | Historical day entered without a Slice scrape |
---

## 4. Levels-Only Records for historical backlog

Historical backlog days **enter as Levels-Only Records**. That path is first-class (Catalog / SIL ADR-025 note), not a second-class mode:

- No scrape and no `Vs3dProvider` required.
- Completeness is `levels_only`; provenance is manual / as-of the trader's annotation time.
- Same derived-level fields (`flip`, `walls`, `balance`, `upsideTest`, `downsideTest`) as a Slice-backed card.
- Durable Positioning write verb productization is owned by #15 (`positioning_entry`); until that lands, backlog exemplars live as pinned `agent_insights` with Levels-Only evidence so `recall_agent_insights` / memory brief can surface them.

Never describe a Levels-Only Record as fallback, partial, or second-class.

---

## 5. Same-day annotation habit (live mid-day re-reads)

Calendar-bound habit — do this even when provider work is gated:

### Cadence

| When (ET) | Action |
|-----------|--------|
| Pre-open / first 30 min RTH | Capture or hand-enter morning card (Slice if available, else Levels-Only Record) |
| ~10:30–11:00 | Mid-morning re-read: check flip vs spot, wall integrity, BALANCE vs TEST |
| Post-lunch / ~13:00–14:00 | Charm-sensitive re-read on ODTE days; mark migrations |
| Final hour | Optional pin-risk / expiry note; supersede stale morning insight if levels moved |

### How to store (memory system)

1. `save_agent_insight` with category `positioning_annotation`.
2. Tags always include `positioning`, plus `slice` or `levels_only`, plus archetype tag(s).
3. Evidence JSON carries `recordKind`, `completeness`, `derivedLevels`, optional `sliceGreek` summary, `transitions`, and `midDayReads[]`.
4. When a mid-day re-read invalidates the morning card, save a new insight and `supersede_agent_insight` the prior id — do not silently edit history.
5. Seed exemplars: `the-desk-mcp --seed-positioning-corpus` (idempotent; pins teaching sessions).

### Agent recall

- `recall_agent_insights` with `category=positioning_annotation` and/or `tag=positioning`.
- `get_memory_brief` surfaces pinned/validated corpus entries by salience when maintenance is fresh.
- Frame coaching: "Your annotated sessions show a BALANCE → UPSIDE TEST transition like [exemplar]…"

---

## 6. Compliance boundary

- Trust Ceiling stays **L3**.
- Every Positioning comment traces to the trader's methodology and annotated sessions — not to a Desk-invented signal.
- No order placement, no proprietary "buy/sell" language, no treating vendor projections as durable truth.

---

## 7. Related artifacts

- Corpus fixture: `docs/trader-memory/fixtures/positioning-exemplar-corpus.json`
- Seed module: `src/memory/positioning_corpus.rs`
- Catalog Positioning stub: `docs/mcp/desk-catalog-v0.md` § Positioning record kinds
- Manual Levels-Only write verb (later): #15
- Vs3dProvider scrape path (ToS-gated, later): #16
