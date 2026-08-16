//! Catalog descriptor types — metadata only, never live values.

use serde::{Deserialize, Serialize};

use super::feature_registry::FeatureDescriptor;

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

impl CostHint {
    /// Wire label (`R0`…`R3`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::R0 => "R0",
            Self::R1 => "R1",
            Self::R2 => "R2",
            Self::R3 => "R3",
        }
    }

    /// Parse a wire label. Unknown values are rejected (not coerced).
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "R0" => Some(Self::R0),
            "R1" => Some(Self::R1),
            "R2" => Some(Self::R2),
            "R3" => Some(Self::R3),
            _ => None,
        }
    }
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

impl Unit {
    /// Catalog JSON / Feature Registry wire label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PricePoints => "pricePoints",
            Self::Contracts => "contracts",
            Self::ContractsPerSec => "contractsPerSec",
            Self::TicksPerSec => "ticksPerSec",
            Self::Ticks => "ticks",
            Self::Ratio => "ratio",
            Self::Percent => "percent",
            Self::Count => "count",
            Self::Milliseconds => "milliseconds",
            Self::Bool => "bool",
            Self::EnumLabel => "enumLabel",
            Self::Text => "text",
            Self::StructuredBlob => "structuredBlob",
        }
    }

    /// Parse a catalog unit label. Unknown values are rejected.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "pricePoints" | "price_points" | "PricePoints" => Some(Self::PricePoints),
            "contracts" | "Contracts" => Some(Self::Contracts),
            "contractsPerSec" | "contracts_per_sec" | "ContractsPerSec" => {
                Some(Self::ContractsPerSec)
            }
            "ticksPerSec" | "ticks_per_sec" | "TicksPerSec" => Some(Self::TicksPerSec),
            "ticks" | "Ticks" => Some(Self::Ticks),
            "ratio" | "Ratio" => Some(Self::Ratio),
            "percent" | "Percent" => Some(Self::Percent),
            "count" | "Count" => Some(Self::Count),
            "milliseconds" | "Milliseconds" => Some(Self::Milliseconds),
            "bool" | "Bool" => Some(Self::Bool),
            "enumLabel" | "enum_label" | "EnumLabel" => Some(Self::EnumLabel),
            "text" | "Text" => Some(Self::Text),
            "structuredBlob" | "structured_blob" | "StructuredBlob" => Some(Self::StructuredBlob),
            _ => None,
        }
    }
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

impl SessionScope {
    /// Catalog JSON / Feature Registry wire label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Rth => "rth",
            Self::Globex => "globex",
            Self::Segment => "segment",
            Self::CrossSession => "crossSession",
        }
    }

    /// Parse a catalog session-scope label. Unknown values are rejected.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "session" | "Session" => Some(Self::Session),
            "rth" | "RTH" | "Rth" => Some(Self::Rth),
            "globex" | "Globex" | "GLOBEX" => Some(Self::Globex),
            "segment" | "Segment" => Some(Self::Segment),
            "crossSession" | "cross_session" | "CrossSession" => Some(Self::CrossSession),
            _ => None,
        }
    }
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

impl FreshnessSemantics {
    /// Catalog JSON / Feature Registry wire label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LiveTickAnchored => "liveTickAnchored",
            Self::SessionScoped => "sessionScoped",
            Self::PriorSessionCarry => "priorSessionCarry",
            Self::DelayedDepthOptional => "delayedDepthOptional",
            Self::StubUnavailable => "stubUnavailable",
            Self::VendorTimestampFailClosed => "vendorTimestampFailClosed",
            Self::ManualAsOfFailClosed => "manualAsOfFailClosed",
        }
    }

    /// Parse a catalog freshness label. Unknown values are rejected.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "liveTickAnchored" | "live_tick_anchored" | "LiveTickAnchored" => {
                Some(Self::LiveTickAnchored)
            }
            "sessionScoped" | "session_scoped" | "SessionScoped" => Some(Self::SessionScoped),
            "priorSessionCarry" | "prior_session_carry" | "PriorSessionCarry" => {
                Some(Self::PriorSessionCarry)
            }
            "delayedDepthOptional" | "delayed_depth_optional" | "DelayedDepthOptional" => {
                Some(Self::DelayedDepthOptional)
            }
            "stubUnavailable" | "stub_unavailable" | "StubUnavailable" => {
                Some(Self::StubUnavailable)
            }
            "vendorTimestampFailClosed"
            | "vendor_timestamp_fail_closed"
            | "VendorTimestampFailClosed" => Some(Self::VendorTimestampFailClosed),
            "manualAsOfFailClosed" | "manual_as_of_fail_closed" | "ManualAsOfFailClosed" => {
                Some(Self::ManualAsOfFailClosed)
            }
            _ => None,
        }
    }
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
    /// Feature Registry Base Detectors (schema + provenance + promotion).
    #[serde(default)]
    pub base_detectors: Vec<FeatureDescriptor>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_enum_parse_round_trips_wire_labels() {
        assert_eq!(Unit::parse("ticks"), Some(Unit::Ticks));
        assert_eq!(Unit::parse("enumLabel"), Some(Unit::EnumLabel));
        assert_eq!(Unit::parse("nope"), None);
        assert_eq!(SessionScope::parse("rth"), Some(SessionScope::Rth));
        assert_eq!(
            SessionScope::parse("cross_session"),
            Some(SessionScope::CrossSession)
        );
        assert_eq!(
            FreshnessSemantics::parse("sessionScoped"),
            Some(FreshnessSemantics::SessionScoped)
        );
        assert_eq!(CostHint::parse("r2"), Some(CostHint::R2));
        assert_eq!(CostHint::parse("R9"), None);
        assert_eq!(Unit::Count.as_str(), "count");
        assert_eq!(CostHint::R1.as_str(), "R1");
    }
}
