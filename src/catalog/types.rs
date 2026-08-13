//! Catalog descriptor types — metadata only, never live values.

use serde::{Deserialize, Serialize};

/// Pinned Desk Catalog version string served in every discovery envelope.
pub const CATALOG_VERSION: &str = "0.1.0";

/// Trust Ceiling mirrored into catalog environment metadata (ADR-022).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrustCeiling {
    L3,
}

/// Pull-band cost hint for a catalog field (resolution model R0–R3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CostHint {
    /// Orientation-class field (cheap, expected in R0 reads).
    R0,
    /// State-class field (R1).
    R1,
    /// Evidence-class field (R2).
    R2,
    /// Raw/expensive field (R3; hard caps apply later).
    R3,
}

/// Unit of measure for a catalog field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Unit {
    PricePoints,
    Contracts,
    ContractsPerSec,
    TicksPerSec,
    Ticks,
    Ratio,
    Percent,
    Count,
    Milliseconds,
    Bool,
    EnumLabel,
    Text,
    StructuredBlob,
}

/// Session scope for a catalog field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SessionScope {
    /// Current session (RTH or Globex as active).
    Session,
    /// RTH-only semantics.
    Rth,
    /// Globex / overnight semantics.
    Globex,
    /// Delta-reset segment (Asia / London / RTH).
    Segment,
    /// Spans sessions or prior-day carry.
    CrossSession,
}

/// Freshness semantics — how a consumer should interpret staleness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FreshnessSemantics {
    /// Anchored to the latest processed tick.
    LiveTickAnchored,
    /// Stable for the session once computed (e.g. day type labels).
    SessionScoped,
    /// Carried from a prior session reference.
    PriorSessionCarry,
    /// Optional delayed DOM/depth summary when available.
    DelayedDepthOptional,
    /// Schema present; live provider not wired.
    ///
    /// Reserved for future unwired provider domains (Slice / grid until
    /// Vs3dProvider). Positioning Levels-Only uses [`Self::ManualAsOfFailClosed`].
    StubUnavailable,
    /// Vendor timestamp must fail closed when present (future provider).
    VendorTimestampFailClosed,
    /// Manual / as-of Positioning stamp; missing or stale freshness fails closed
    /// and must never present as live vendor data.
    ManualAsOfFailClosed,
}

/// Static annotation used to build [`FieldDescriptor`]s.
#[derive(Debug, Clone, Copy)]
pub struct FieldSpec {
    pub id: &'static str,
    pub rust_field: &'static str,
    pub name: &'static str,
    pub domain_id: &'static str,
    pub description: &'static str,
    pub unit: Unit,
    pub session_scope: SessionScope,
    pub freshness: FreshnessSemantics,
    pub cost_hint: CostHint,
}

/// Serializable field descriptor served by discovery operators.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FieldDescriptor {
    pub id: String,
    pub name: String,
    pub domain_id: String,
    pub description: String,
    pub rust_field: String,
    pub unit: Unit,
    pub session_scope: SessionScope,
    pub freshness: FreshnessSemantics,
    pub cost_hint: CostHint,
}

impl FieldDescriptor {
    /// Lift a static [`FieldSpec`] into an owned catalog descriptor.
    pub fn from_spec(spec: FieldSpec) -> Self {
        Self {
            id: spec.id.to_string(),
            name: spec.name.to_string(),
            domain_id: spec.domain_id.to_string(),
            description: spec.description.to_string(),
            rust_field: spec.rust_field.to_string(),
            unit: spec.unit,
            session_scope: spec.session_scope,
            freshness: spec.freshness,
            cost_hint: spec.cost_hint,
        }
    }
}

/// Named record kind inside the Positioning domain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PositioningRecordKind {
    pub id: String,
    pub name: String,
    pub summary: String,
}

/// Domain descriptor in the catalog ontology.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DomainDescriptor {
    pub id: String,
    pub name: String,
    pub summary: String,
    pub field_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub record_kinds: Vec<String>,
}

/// Versioned Desk Catalog artifact.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeskCatalog {
    pub catalog_version: String,
    pub trust_ceiling: TrustCeiling,
    pub specialty_market_tools: Vec<String>,
    pub domains: Vec<DomainDescriptor>,
    pub fields: Vec<FieldDescriptor>,
    pub positioning_record_kinds: Vec<PositioningRecordKind>,
    /// Always `None` in Catalog v0 — no live Positioning provider.
    pub positioning_provider: Option<String>,
}
