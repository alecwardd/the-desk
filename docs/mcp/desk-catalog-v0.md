# Desk Catalog v0

> **Generated file — do not edit by hand.**
> Regenerate with `cargo run --bin the-desk-mcp -- --write-catalog-docs`.
> The test `desk_catalog_docs_are_current` fails when this file is stale.

- **catalogVersion:** `0.1.0`
- **Trust Ceiling:** L3 (ADR-022)
- **domains:** 10
- **fields:** 146
- **Positioning provider:** none (no Vs3dProvider). Levels-Only Records are first-class via `positioning_entry` (manual/as-of).

## Domains

### `cross_market` — Cross-market

Cross-session inventory and multi-session trend framing.

| Field id | Unit | Session scope | Freshness | Cost |
|---|---|---|---|---|
| `market.cross_market.inventoryDirection` | EnumLabel | CrossSession | SessionScoped | R1 |
| `market.cross_market.inventoryState` | EnumLabel | CrossSession | SessionScoped | R1 |
| `market.cross_market.sessionsInTrend` | Count | CrossSession | SessionScoped | R1 |

### `events` — Events

Formalized event stream: lifecycle (open → updated → resolved|expired), severity, dedup identity, frameRef to the producing Journal Frame, and capsuleRef on DOM-family rows (stop_run, iceberg_reload, pull_intent, book_velocity_regime_shift). Reads ride get_events; the attention inbox is a ranked view over this stream.

| Field id | Unit | Session scope | Freshness | Cost |
|---|---|---|---|---|

### `flow` — Flow

Participation and aggression: delta, tape pace, absorption, pinch, trade size.

| Field id | Unit | Session scope | Freshness | Cost |
|---|---|---|---|---|
| `market.flow.absorptionEventCount` | Count | Session | LiveTickAnchored | R1 |
| `market.flow.avgTradeSize` | Contracts | Session | LiveTickAnchored | R1 |
| `market.flow.confirmedAbsorptionEventCount` | Count | Session | LiveTickAnchored | R1 |
| `market.flow.confirmedDeltaDivergenceEventCount` | Count | Session | LiveTickAnchored | R1 |
| `market.flow.confirmedExhaustionEventCount` | Count | Session | LiveTickAnchored | R1 |
| `market.flow.cumulativeDelta` | Contracts | CrossSession | LiveTickAnchored | R1 |
| `market.flow.globexDelta` | Contracts | Globex | LiveTickAnchored | R1 |
| `market.flow.hasRecentConfirmedAbsorption` | Bool | Session | LiveTickAnchored | R1 |
| `market.flow.hasRecentConfirmedExhaustion` | Bool | Session | LiveTickAnchored | R1 |
| `market.flow.hasRecentInvalidatedAbsorption` | Bool | Session | LiveTickAnchored | R1 |
| `market.flow.imbalanceCount` | Count | Session | LiveTickAnchored | R1 |
| `market.flow.pacePercentile` | Ratio | Session | LiveTickAnchored | R1 |
| `market.flow.pinchEventCount` | Count | Session | LiveTickAnchored | R1 |
| `market.flow.recentConfirmedAbsorptionAgeMs` | Milliseconds | Session | LiveTickAnchored | R1 |
| `market.flow.recentConfirmedAbsorptionDirection` | EnumLabel | Session | LiveTickAnchored | R1 |
| `market.flow.recentConfirmedAbsorptionDistanceTicks` | Ticks | Session | LiveTickAnchored | R1 |
| `market.flow.recentConfirmedAbsorptionPrice` | PricePoints | Session | LiveTickAnchored | R1 |
| `market.flow.recentConfirmedExhaustionAgeMs` | Milliseconds | Session | LiveTickAnchored | R1 |
| `market.flow.recentConfirmedExhaustionDirection` | EnumLabel | Session | LiveTickAnchored | R1 |
| `market.flow.recentConfirmedExhaustionPrice` | PricePoints | Session | LiveTickAnchored | R1 |
| `market.flow.recentInvalidatedAbsorptionAgeMs` | Milliseconds | Session | LiveTickAnchored | R1 |
| `market.flow.recentInvalidatedAbsorptionDirection` | EnumLabel | Session | LiveTickAnchored | R1 |
| `market.flow.recentInvalidatedAbsorptionDistanceTicks` | Ticks | Session | LiveTickAnchored | R1 |
| `market.flow.recentInvalidatedAbsorptionPrice` | PricePoints | Session | LiveTickAnchored | R1 |
| `market.flow.sessionDelta` | Contracts | Segment | LiveTickAnchored | R1 |
| `market.flow.tapeAcceleration` | Ratio | Session | LiveTickAnchored | R1 |
| `market.flow.tapeCoverage30S` | Ratio | Session | LiveTickAnchored | R1 |
| `market.flow.tapeCoverage5M` | Ratio | Session | LiveTickAnchored | R1 |
| `market.flow.tapeCoverage5S` | Ratio | Session | LiveTickAnchored | R1 |
| `market.flow.tapeDwellAtCurrentPriceMs` | Milliseconds | Session | LiveTickAnchored | R1 |
| `market.flow.tapeEventTimeLagMs` | Milliseconds | Session | LiveTickAnchored | R1 |
| `market.flow.tapeLastTradeTimestampMs` | Milliseconds | Session | LiveTickAnchored | R1 |
| `market.flow.tapePace30S` | TicksPerSec | Session | LiveTickAnchored | R1 |
| `market.flow.tapePace5M` | TicksPerSec | Session | LiveTickAnchored | R1 |
| `market.flow.tapePace5S` | TicksPerSec | Session | LiveTickAnchored | R1 |
| `market.flow.tapeRawAcceleration` | Ratio | Session | LiveTickAnchored | R1 |
| `market.flow.tapeRegimeTicksPerSec30MEma` | TicksPerSec | Session | LiveTickAnchored | R1 |
| `market.flow.tapeRegimeVolumePerSec30MEma` | ContractsPerSec | Session | LiveTickAnchored | R1 |
| `market.flow.tapeRollingPercentile` | Ratio | Session | LiveTickAnchored | R1 |
| `market.flow.tapeValid30S` | Bool | Session | LiveTickAnchored | R1 |
| `market.flow.tapeValid5M` | Bool | Session | LiveTickAnchored | R1 |
| `market.flow.tapeValid5S` | Bool | Session | LiveTickAnchored | R1 |
| `market.flow.tapeVolumePerSec30S` | ContractsPerSec | Session | LiveTickAnchored | R1 |
| `market.flow.tapeVolumePerSec5M` | ContractsPerSec | Session | LiveTickAnchored | R1 |
| `market.flow.tapeVolumePerSec5S` | ContractsPerSec | Session | LiveTickAnchored | R1 |
| `market.flow.tapeWindowAnchorTimestampMs` | Milliseconds | Session | LiveTickAnchored | R1 |

### `identity` — Identity

Instrument identity, session labels, and contract resolution metadata.

| Field id | Unit | Session scope | Freshness | Cost |
|---|---|---|---|---|
| `market.identity.carryForwardLevelsValid` | Bool | CrossSession | SessionScoped | R0 |
| `market.identity.contractMonth` | Text | CrossSession | SessionScoped | R0 |
| `market.identity.contractSymbol` | Text | CrossSession | SessionScoped | R0 |
| `market.identity.priorDayContractSymbol` | Text | CrossSession | PriorSessionCarry | R0 |
| `market.identity.rolloverWarning` | Text | CrossSession | SessionScoped | R0 |
| `market.identity.rootSymbol` | Text | CrossSession | SessionScoped | R0 |
| `market.identity.sessionSegment` | EnumLabel | Session | SessionScoped | R0 |
| `market.identity.sessionType` | EnumLabel | Session | SessionScoped | R0 |
| `market.identity.symbolResolutionMode` | EnumLabel | CrossSession | SessionScoped | R0 |
| `market.identity.symbolResolutionSource` | EnumLabel | CrossSession | SessionScoped | R0 |
| `market.identity.tradingDay` | Text | CrossSession | SessionScoped | R0 |

### `liquidity` — Liquidity

Order-book / DOM liquidity summaries when depth context is available.

| Field id | Unit | Session scope | Freshness | Cost |
|---|---|---|---|---|
| `market.liquidity.domSummary` | StructuredBlob | Session | DelayedDepthOptional | R1 |

### `location_structure` — Location / structure

Price location and auction structure: VWAP, TPO VA/POC, DNVA/DNP, IB/OR/OR5, day type.

| Field id | Unit | Session scope | Freshness | Cost |
|---|---|---|---|---|
| `market.location_structure.ask` | PricePoints | Session | LiveTickAnchored | R0 |
| `market.location_structure.balanceState` | EnumLabel | Rth | SessionScoped | R0 |
| `market.location_structure.bid` | PricePoints | Session | LiveTickAnchored | R0 |
| `market.location_structure.dayType` | EnumLabel | Rth | SessionScoped | R0 |
| `market.location_structure.dnp` | PricePoints | Rth | LiveTickAnchored | R0 |
| `market.location_structure.dnvaHigh` | PricePoints | Rth | LiveTickAnchored | R1 |
| `market.location_structure.dnvaLow` | PricePoints | Rth | LiveTickAnchored | R1 |
| `market.location_structure.excessHigh` | Bool | Session | LiveTickAnchored | R1 |
| `market.location_structure.excessLow` | Bool | Session | LiveTickAnchored | R1 |
| `market.location_structure.globexOr30High` | PricePoints | Globex | LiveTickAnchored | R1 |
| `market.location_structure.globexOr30Low` | PricePoints | Globex | LiveTickAnchored | R1 |
| `market.location_structure.ibExtensionState` | EnumLabel | Rth | SessionScoped | R0 |
| `market.location_structure.ibHigh` | PricePoints | Rth | LiveTickAnchored | R0 |
| `market.location_structure.ibLow` | PricePoints | Rth | LiveTickAnchored | R0 |
| `market.location_structure.lastPrice` | PricePoints | Session | LiveTickAnchored | R0 |
| `market.location_structure.londonOr60High` | PricePoints | Globex | LiveTickAnchored | R1 |
| `market.location_structure.londonOr60Low` | PricePoints | Globex | LiveTickAnchored | R1 |
| `market.location_structure.or5BreakDirection` | EnumLabel | Rth | LiveTickAnchored | R1 |
| `market.location_structure.or5High` | PricePoints | Rth | LiveTickAnchored | R0 |
| `market.location_structure.or5Locked` | Bool | Rth | LiveTickAnchored | R1 |
| `market.location_structure.or5Low` | PricePoints | Rth | LiveTickAnchored | R0 |
| `market.location_structure.or5Mid` | PricePoints | Rth | LiveTickAnchored | R0 |
| `market.location_structure.or5MidRetested` | Bool | Rth | LiveTickAnchored | R1 |
| `market.location_structure.orHigh` | PricePoints | Rth | LiveTickAnchored | R0 |
| `market.location_structure.orLow` | PricePoints | Rth | LiveTickAnchored | R0 |
| `market.location_structure.overnightHigh` | PricePoints | CrossSession | LiveTickAnchored | R1 |
| `market.location_structure.overnightLow` | PricePoints | CrossSession | LiveTickAnchored | R1 |
| `market.location_structure.poc` | PricePoints | Rth | LiveTickAnchored | R0 |
| `market.location_structure.poorHigh` | Bool | Session | LiveTickAnchored | R1 |
| `market.location_structure.poorLow` | Bool | Session | LiveTickAnchored | R1 |
| `market.location_structure.priorDayClose` | PricePoints | CrossSession | PriorSessionCarry | R0 |
| `market.location_structure.priorDayHigh` | PricePoints | CrossSession | PriorSessionCarry | R0 |
| `market.location_structure.priorDayLow` | PricePoints | CrossSession | PriorSessionCarry | R0 |
| `market.location_structure.priorDnp` | PricePoints | CrossSession | PriorSessionCarry | R1 |
| `market.location_structure.priorDnvaHigh` | PricePoints | CrossSession | PriorSessionCarry | R1 |
| `market.location_structure.priorDnvaLow` | PricePoints | CrossSession | PriorSessionCarry | R1 |
| `market.location_structure.priorPoc` | PricePoints | CrossSession | PriorSessionCarry | R1 |
| `market.location_structure.priorVaHigh` | PricePoints | CrossSession | PriorSessionCarry | R1 |
| `market.location_structure.priorVaLow` | PricePoints | CrossSession | PriorSessionCarry | R1 |
| `market.location_structure.profileShape` | EnumLabel | Rth | SessionScoped | R0 |
| `market.location_structure.regime` | EnumLabel | Rth | SessionScoped | R0 |
| `market.location_structure.rthClosePrice` | PricePoints | Rth | LiveTickAnchored | R1 |
| `market.location_structure.sessionHigh` | PricePoints | Rth | LiveTickAnchored | R1 |
| `market.location_structure.sessionLow` | PricePoints | Rth | LiveTickAnchored | R1 |
| `market.location_structure.singlePrintsDirection` | EnumLabel | Rth | SessionScoped | R0 |
| `market.location_structure.vaHigh` | PricePoints | Rth | LiveTickAnchored | R0 |
| `market.location_structure.vaLow` | PricePoints | Rth | LiveTickAnchored | R0 |
| `market.location_structure.vwap` | PricePoints | Session | LiveTickAnchored | R0 |
| `market.location_structure.vwap1SdLower` | PricePoints | Session | LiveTickAnchored | R1 |
| `market.location_structure.vwap1SdUpper` | PricePoints | Session | LiveTickAnchored | R1 |
| `market.location_structure.vwap2SdLower` | PricePoints | Session | LiveTickAnchored | R1 |
| `market.location_structure.vwap2SdUpper` | PricePoints | Session | LiveTickAnchored | R1 |
| `market.location_structure.vwap3SdLower` | PricePoints | Session | LiveTickAnchored | R1 |
| `market.location_structure.vwap3SdUpper` | PricePoints | Session | LiveTickAnchored | R1 |

### `meta` — Meta

Catalog and envelope metadata (version pins, cost bands).

| Field id | Unit | Session scope | Freshness | Cost |
|---|---|---|---|---|

### `positioning` — Positioning

Dealer/options Positioning — grid aggregations, by-strike positions, greek Slices, and first-class Levels-Only Records. Manual write via positioning_entry; no live Vs3dProvider.

Record kinds: position_grid, positions_by_strike, slice, levels_only

| Field id | Unit | Session scope | Freshness | Cost |
|---|---|---|---|---|
| `positioning.asOf` | Milliseconds | Session | ManualAsOfFailClosed | R1 |
| `positioning.capturedAt` | Milliseconds | Session | ManualAsOfFailClosed | R1 |
| `positioning.completeness` | EnumLabel | Session | ManualAsOfFailClosed | R0 |
| `positioning.dataTime` | Milliseconds | Session | VendorTimestampFailClosed | R1 |
| `positioning.derivedLevels` | StructuredBlob | Session | ManualAsOfFailClosed | R1 |
| `positioning.freshnessOk` | Bool | Session | ManualAsOfFailClosed | R0 |
| `positioning.recordKind` | EnumLabel | Session | ManualAsOfFailClosed | R1 |

### `response` — Response

Market response structures such as rebid/reoffer acceleration zones.

| Field id | Unit | Session scope | Freshness | Cost |
|---|---|---|---|---|
| `market.response.activeZoneCount` | Count | Session | LiveTickAnchored | R1 |
| `market.response.nearestZoneDirection` | EnumLabel | Session | LiveTickAnchored | R1 |
| `market.response.nearestZoneDistanceTicks` | Ticks | Session | LiveTickAnchored | R1 |
| `market.response.nearestZoneStatus` | EnumLabel | Session | LiveTickAnchored | R1 |
| `market.response.rebidZoneHeld` | Bool | Session | LiveTickAnchored | R1 |
| `market.response.rebidZoneHigh` | PricePoints | Session | LiveTickAnchored | R1 |
| `market.response.rebidZoneLow` | PricePoints | Session | LiveTickAnchored | R1 |
| `market.response.rebidZoneNear` | Bool | Session | LiveTickAnchored | R1 |
| `market.response.rebidZoneRetested` | Bool | Session | LiveTickAnchored | R1 |
| `market.response.reofferZoneHeld` | Bool | Session | LiveTickAnchored | R1 |
| `market.response.reofferZoneHigh` | PricePoints | Session | LiveTickAnchored | R1 |
| `market.response.reofferZoneLow` | PricePoints | Session | LiveTickAnchored | R1 |
| `market.response.reofferZoneNear` | Bool | Session | LiveTickAnchored | R1 |
| `market.response.reofferZoneRetested` | Bool | Session | LiveTickAnchored | R1 |

### `volatility` — Volatility

Relative volume and related session volatility framing.

| Field id | Unit | Session scope | Freshness | Cost |
|---|---|---|---|---|
| `market.volatility.rvolAcceleration` | Ratio | Rth | LiveTickAnchored | R1 |
| `market.volatility.rvolBaselineStatus` | EnumLabel | Rth | LiveTickAnchored | R1 |
| `market.volatility.rvolBucketIndex` | Count | Rth | LiveTickAnchored | R1 |
| `market.volatility.rvolCaveat` | Text | Rth | LiveTickAnchored | R1 |
| `market.volatility.rvolClassification` | EnumLabel | Rth | LiveTickAnchored | R1 |
| `market.volatility.rvolExpectedVolume` | Contracts | Rth | LiveTickAnchored | R1 |
| `market.volatility.rvolLookbackDaysActual` | Count | Rth | LiveTickAnchored | R1 |
| `market.volatility.rvolPercentile` | Percent | Rth | LiveTickAnchored | R1 |
| `market.volatility.rvolRatio` | Ratio | Rth | LiveTickAnchored | R1 |
| `market.volatility.rvolVelocity` | Ratio | Rth | LiveTickAnchored | R1 |

## Positioning record kinds

- **grid** (`position_grid`): Primary capture panel — grid aggregations of dealer positioning (schema v1).
- **by-strike** (`positions_by_strike`): By-strike positions; Desk may aggregate from the position grid rather than capture separately.
- **Slice** (`slice`): Price-indexed greek surface values at one moment (capturedAt/dataTime) plus Desk-derived levels at ingest. Vendor forward projections are never part of a Slice.
- **Levels-Only Record** (`levels_only`): First-class Positioning record carrying only derived levels (flip, walls, BALANCE / UPSIDE / DOWNSIDE TEST) — manual-entry unit, ToS-denial steady state, and historical backlog path (ADR-025). Written via positioning_entry.

## Specialty market tools (allowlist)

Catalog v0 enforces **no catalog entry → no new market tool**. The allowlist below matches the SIL-M0 freeze set:

- `check_delta_confirmation`
- `get_absorption_events`
- `get_context_frame`
- `get_day_type`
- `get_delta_at_price`
- `get_delta_profile`
- `get_footprint`
- `get_footprint_window`
- `get_imbalances`
- `get_key_levels`
- `get_market_snapshot`
- `get_or5_status`
- `get_pinch_events`
- `get_proximity_report`
- `get_rebid_reoffer_zones`
- `get_rvol`
- `get_session_context`
- `get_session_inventory`
- `get_session_summary`
- `get_snapshot_at`
- `get_tape_pace`
- `get_tpo_detail`
- `get_tpo_profile`
- `get_trade_size_profile`

## Feature Registry (shipped Base Detectors)

Governance waist for **Base Detectors** and **Derived Features**: schema, provenance, and promotion (`candidate` → `shadow` → `active`, human-gated). This generated snapshot lists shipped Base Detectors only. Overlay Derived Features live in SQLite and are discovered via `search_catalog` when `[sil].catalog_discovery` is on. Derived Features declare a Feature-IR program over catalog fields using exactly five funded Operator Families (Cross-symbol references, Session-distribution percentiles, Dwell / time-since-predicate, Event sequences, Historical baselines). Unfunded families (including surface lookup / interpolation) are rejected at declaration time. A new Operator Family requires a registry change proposal. Feature-IR evaluation is declaration-and-test-only in M5b. Discovery rides `search_catalog` / catalog descriptors — no specialty getter. The write verb is `feature_registry`. Tier 1 Base Detector math stays reviewed Rust; this catalog does not implement codegen emitters (SIL-M5c).

| Id | Domain | Promotion | Builtin | Event types |
|---|---|---|---|---|
| `detector.absorption` | `flow` | active | true | absorption_detected, absorption_confirmed, absorption_invalidated |
| `detector.pinch` | `flow` | active | true | pinch_detected |
| `detector.rebid_reoffer` | `response` | active | true | acceleration_zone_created, acceleration_zone_held |
| `detector.structure` | `location_structure` | active | true | day_type_change, dnp_cross, dnp_test, dnva_high_test, dnva_low_test, excess_high_detected, excess_low_detected, ib_extension_hit, ib_formed, ib_high_test, ib_low_test, ib_mid_test, ib_reentry, ib_reentry_full_traverse, ib_reentry_hit_mid, new_session_high, new_session_low, or5_mid_retest, or_formed, overnight_high_test, overnight_low_test, poc_test, poor_high_detected, poor_low_detected, prior_close_test, prior_day_high_test, prior_day_low_test, prior_poc_test, prior_vah_test, prior_val_test, rvol_at_ib_close, rvol_spike, vah_test, val_test, vwap_1sd_lower_test, vwap_1sd_upper_test, vwap_2sd_lower_test, vwap_2sd_upper_test, vwap_test |
| `detector.trade_size` | `flow` | active | true | large_trade_cluster |

