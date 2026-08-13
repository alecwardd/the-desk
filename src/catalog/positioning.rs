//! Positioning domain catalog — four first-class record kinds.
//!
//! Levels-Only Records are written via `positioning_entry` (manual/as-of).
//! No live Vs3dProvider in this ticket (`positioning_provider` stays `None`).

use super::types::*;

pub(crate) struct PositioningStub {
    pub domain: DomainDescriptor,
    pub record_kinds: Vec<PositioningRecordKind>,
    pub fields: Vec<FieldDescriptor>,
}

/// Build the Positioning domain (grid / by-strike / Slice / Levels-Only Record).
pub(crate) fn positioning_domain() -> PositioningStub {
    let record_kinds = vec![
        PositioningRecordKind {
            id: "position_grid".into(),
            name: "grid".into(),
            summary: "Primary capture panel — grid aggregations of dealer positioning (schema v1).".into(),
        },
        PositioningRecordKind {
            id: "positions_by_strike".into(),
            name: "by-strike".into(),
            summary: "By-strike positions; Desk may aggregate from the position grid rather than capture separately.".into(),
        },
        PositioningRecordKind {
            id: "slice".into(),
            name: "Slice".into(),
            summary: "Price-indexed greek surface values at one moment (capturedAt/dataTime) plus Desk-derived levels at ingest. Vendor forward projections are never part of a Slice.".into(),
        },
        PositioningRecordKind {
            id: "levels_only".into(),
            name: "Levels-Only Record".into(),
            summary: "First-class Positioning record carrying only derived levels (flip, walls, BALANCE / UPSIDE / DOWNSIDE TEST) — manual-entry unit, ToS-denial steady state, and historical backlog path (ADR-025). Written via positioning_entry.".into(),
        },
    ];

    let fields = vec![
        FieldDescriptor {
            id: "positioning.recordKind".into(),
            name: "recordKind".into(),
            domain_id: "positioning".into(),
            description: "Which Positioning record kind is present: position_grid, positions_by_strike, slice, or levels_only.".into(),
            rust_field: String::new(),
            unit: Unit::EnumLabel,
            session_scope: SessionScope::Session,
            freshness: FreshnessSemantics::ManualAsOfFailClosed,
            cost_hint: CostHint::R1,
        },
        FieldDescriptor {
            id: "positioning.completeness".into(),
            name: "completeness".into(),
            domain_id: "positioning".into(),
            description: "First-class completeness of the current Positioning record. levels_only is a first-class kind — the ToS-denial steady state and historical backlog path.".into(),
            rust_field: String::new(),
            unit: Unit::EnumLabel,
            session_scope: SessionScope::Session,
            freshness: FreshnessSemantics::ManualAsOfFailClosed,
            cost_hint: CostHint::R0,
        },
        FieldDescriptor {
            id: "positioning.capturedAt".into(),
            name: "capturedAt".into(),
            domain_id: "positioning".into(),
            description: "Desk capture / annotation timestamp for a Positioning record (manual as-of on the Levels-Only path).".into(),
            rust_field: String::new(),
            unit: Unit::Milliseconds,
            session_scope: SessionScope::Session,
            freshness: FreshnessSemantics::ManualAsOfFailClosed,
            cost_hint: CostHint::R1,
        },
        FieldDescriptor {
            id: "positioning.asOf".into(),
            name: "asOf".into(),
            domain_id: "positioning".into(),
            description: "Explicit manual/as-of timestamp for a Positioning record. Missing/stale as-of fails closed and is never live vendor data.".into(),
            rust_field: String::new(),
            unit: Unit::Milliseconds,
            session_scope: SessionScope::Session,
            freshness: FreshnessSemantics::ManualAsOfFailClosed,
            cost_hint: CostHint::R1,
        },
        FieldDescriptor {
            id: "positioning.dataTime".into(),
            name: "dataTime".into(),
            domain_id: "positioning".into(),
            description: "Vendor data time when a provider exists; null on the Levels-Only Record path. Fail-closed — never silent live vendor.".into(),
            rust_field: String::new(),
            unit: Unit::Milliseconds,
            session_scope: SessionScope::Session,
            freshness: FreshnessSemantics::VendorTimestampFailClosed,
            cost_hint: CostHint::R1,
        },
        FieldDescriptor {
            id: "positioning.freshnessOk".into(),
            name: "freshnessOk".into(),
            domain_id: "positioning".into(),
            description: "Fail-closed freshness gate for Positioning. False when capturedAt/as-of is missing or the live card is from a prior trading day. Never presents as live vendor data.".into(),
            rust_field: String::new(),
            unit: Unit::Bool,
            session_scope: SessionScope::Session,
            freshness: FreshnessSemantics::ManualAsOfFailClosed,
            cost_hint: CostHint::R0,
        },
        FieldDescriptor {
            id: "positioning.derivedLevels".into(),
            name: "derivedLevels".into(),
            domain_id: "positioning".into(),
            description: "Desk-derived MM levels (flip, walls, BALANCE / UPSIDE / DOWNSIDE TEST) carried by Slice and Levels-Only Record kinds.".into(),
            rust_field: String::new(),
            unit: Unit::StructuredBlob,
            session_scope: SessionScope::Session,
            freshness: FreshnessSemantics::ManualAsOfFailClosed,
            cost_hint: CostHint::R1,
        },
    ];

    let domain = DomainDescriptor {
        id: "positioning".into(),
        name: "Positioning".into(),
        summary: "Dealer/options Positioning — grid aggregations, by-strike positions, greek Slices, and first-class Levels-Only Records. Manual write via positioning_entry; no live Vs3dProvider.".into(),
        field_ids: fields.iter().map(|f| f.id.clone()).collect(),
        record_kinds: record_kinds.iter().map(|k| k.id.clone()).collect(),
    };

    PositioningStub {
        domain,
        record_kinds,
        fields,
    }
}
