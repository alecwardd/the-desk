//! Positioning domain schema stub — record kinds only; no live provider.

use super::types::*;

pub(crate) struct PositioningStub {
    pub domain: DomainDescriptor,
    pub record_kinds: Vec<PositioningRecordKind>,
    pub fields: Vec<FieldDescriptor>,
}

/// Build the Positioning domain stub (grid / by-strike / Slice / Levels-Only Record).
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
            summary: "First-class positioning record carrying only derived levels (flip, walls, BALANCE / UPSIDE / DOWNSIDE TEST) — manual-entry unit and ToS-denial steady state (ADR-025).".into(),
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
            freshness: FreshnessSemantics::StubUnavailable,
            cost_hint: CostHint::R1,
        },
        FieldDescriptor {
            id: "positioning.capturedAt".into(),
            name: "capturedAt".into(),
            domain_id: "positioning".into(),
            description: "Desk capture timestamp for a Positioning record (schema stub; no live provider).".into(),
            rust_field: String::new(),
            unit: Unit::Milliseconds,
            session_scope: SessionScope::Session,
            freshness: FreshnessSemantics::StubUnavailable,
            cost_hint: CostHint::R1,
        },
        FieldDescriptor {
            id: "positioning.dataTime".into(),
            name: "dataTime".into(),
            domain_id: "positioning".into(),
            description: "Vendor data time for a Positioning record when a provider exists; fail-closed freshness later.".into(),
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
            description: "Vendor freshness gate for Positioning; stubbed unavailable until a provider lands.".into(),
            rust_field: String::new(),
            unit: Unit::Bool,
            session_scope: SessionScope::Session,
            freshness: FreshnessSemantics::StubUnavailable,
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
            freshness: FreshnessSemantics::StubUnavailable,
            cost_hint: CostHint::R1,
        },
    ];

    let domain = DomainDescriptor {
        id: "positioning".into(),
        name: "Positioning".into(),
        summary: "Dealer/options Positioning — grid aggregations, by-strike positions, greek Slices, and Levels-Only Records. Schema stub only in Catalog v0 (no live provider).".into(),
        field_ids: fields.iter().map(|f| f.id.clone()).collect(),
        record_kinds: record_kinds.iter().map(|k| k.id.clone()).collect(),
    };

    PositioningStub {
        domain,
        record_kinds,
        fields,
    }
}
