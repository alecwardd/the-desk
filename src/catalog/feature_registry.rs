//! Feature Registry — governance waist for Base Detectors (SIL-M5a).
//!
//! Registers schema, provenance, and promotion state (`candidate` → `shadow`
//! → `active`). Promotion is **human-gated**. Tier 1 math stays reviewed Rust
//! in the existing pipeline modules — this ticket does not implement Feature-IR,
//! codegen, the leg engine, or DOM cluster detectors.
//!
//! Existing shipped detectors (absorption, pinch, and other already-emitting
//! detectors) are registered as `active` with `behaviorChange=false`. Discovery
//! rides the Desk Catalog (`search_catalog` / catalog descriptors). The typed
//! write verb is `feature_registry`; there is no specialty getter.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::types::{CostHint, DeskCatalog, FreshnessSemantics, SessionScope, Unit};

/// MCP / catalog write-verb name for Feature Registry lifecycle.
pub const FEATURE_REGISTRY_WRITE_VERB: &str = "feature_registry";

/// Wire labels for the promotion ladder (candidate → shadow → active).
pub const PROMOTION_STATES: &[&str] = &["candidate", "shadow", "active"];

/// Provenance source for reviewed Rust pipeline math.
pub const RUST_PIPELINE_SOURCE: &str = "rust_pipeline";

/// Math-tier stamp: Base Detector math stays reviewed Rust (not Feature-IR).
pub const TIER1_REVIEWED_RUST: &str = "tier1_reviewed_rust";

/// Specialty market tools that expose a shipped Base Detector concept.
///
/// A new specialty market tool for a detector concept is rejected unless that
/// concept has a Feature Registry (and/or catalog) entry — **no catalog/registry
/// entry → no new tool**.
pub const DETECTOR_SPECIALTY_TOOLS: &[(&str, &str)] = &[
    ("get_absorption_events", "detector.absorption"),
    ("get_pinch_events", "detector.pinch"),
    ("get_rebid_reoffer_zones", "detector.rebid_reoffer"),
    ("get_trade_size_profile", "detector.trade_size"),
];

/// Feature kind. M5a accepts Base Detectors only; Derived Features are later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FeatureKind {
    BaseDetector,
    DerivedFeature,
}

impl FeatureKind {
    /// Wire label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BaseDetector => "baseDetector",
            Self::DerivedFeature => "derivedFeature",
        }
    }

    /// Parse a wire label. Unknown values are rejected (not coerced).
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "baseDetector" | "base_detector" | "BaseDetector" => Some(Self::BaseDetector),
            "derivedFeature" | "derived_feature" | "DerivedFeature" => Some(Self::DerivedFeature),
            _ => None,
        }
    }
}

impl std::fmt::Display for FeatureKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Promotion ladder. Forward-only; human-gated at each step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PromotionState {
    Candidate,
    Shadow,
    Active,
}

impl PromotionState {
    /// Wire label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Shadow => "shadow",
            Self::Active => "active",
        }
    }

    /// Parse a wire label. Unknown values are rejected.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "candidate" => Some(Self::Candidate),
            "shadow" => Some(Self::Shadow),
            "active" => Some(Self::Active),
            _ => None,
        }
    }
}

impl std::fmt::Display for PromotionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Provenance for a registry descriptor (where the math lives, if any).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureProvenance {
    /// `rust_pipeline` for shipped detectors.
    pub source: String,
    /// Rust module path (e.g. `pipelines::absorption`).
    pub rust_module: String,
    /// Always [`TIER1_REVIEWED_RUST`] for Base Detectors in M5a.
    pub math_tier: String,
    /// Must stay false for shipped registrations — this ticket does not change math.
    pub behavior_change: bool,
}

/// Schema waist for a Base Detector (catalog fields + event types + semantics).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DetectorSchema {
    pub catalog_field_ids: Vec<String>,
    pub event_types: Vec<String>,
    pub unit: Unit,
    pub session_scope: SessionScope,
    pub freshness: FreshnessSemantics,
    pub cost_hint: CostHint,
}

/// Catalog / registry descriptor for one feature.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureDescriptor {
    pub id: String,
    pub name: String,
    pub kind: FeatureKind,
    pub description: String,
    pub domain_id: String,
    pub schema: DetectorSchema,
    pub provenance: FeatureProvenance,
    pub promotion_state: PromotionState,
    pub builtin: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_gate_note: Option<String>,
}

/// Input accepted when registering a new Base Detector (always starts `candidate`).
#[derive(Debug, Clone, PartialEq)]
pub struct BaseDetectorRegistration {
    pub id: String,
    pub name: String,
    pub description: String,
    pub domain_id: String,
    pub schema: DetectorSchema,
    pub provenance: FeatureProvenance,
}

/// Human confirmation required to move candidate → shadow → active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HumanGate<'a> {
    pub trader_confirmation: &'a str,
}

impl<'a> HumanGate<'a> {
    /// Parse a trader confirmation. Empty / whitespace-only is not a gate.
    pub fn parse(raw: &'a str) -> Result<Self, FeatureRegistryError> {
        if raw.trim().is_empty() {
            return Err(FeatureRegistryError::HumanGateRequired);
        }
        Ok(Self {
            trader_confirmation: raw,
        })
    }
}

/// Feature Registry errors (typed until the MCP / CLI boundary).
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FeatureRegistryError {
    #[error("feature registry requires a Base Detector descriptor (kind=baseDetector); Derived Features are later (SIL-M5b)")]
    DerivedFeatureNotAccepted,
    #[error("feature id `{0}` is invalid (expected detector.<snake_id>)")]
    InvalidId(String),
    #[error("feature `{0}` is already registered")]
    DuplicateId(String),
    #[error("feature `{0}` is a shipped builtin and cannot be overwritten or re-promoted")]
    BuiltinImmutable(String),
    #[error("feature `{0}` is not registered")]
    NotFound(String),
    #[error("descriptor is missing schema (catalogFieldIds or eventTypes)")]
    MissingSchema,
    #[error("descriptor is missing provenance (source and rustModule)")]
    MissingProvenance,
    #[error("human gate required to move candidate → shadow → active (traderConfirmation must not be empty)")]
    HumanGateRequired,
    #[error("illegal promotion {from} → {to} (allowed: candidate → shadow, shadow → active)")]
    IllegalPromotion {
        from: PromotionState,
        to: PromotionState,
    },
    #[error("feature is already {0}")]
    AlreadyInState(PromotionState),
    #[error("name, description, and domainId are required")]
    MissingIdentity,
}

/// In-memory Feature Registry (builtins + optional SQLite overlay).
#[derive(Debug, Clone, PartialEq)]
pub struct FeatureRegistry {
    descriptors: BTreeMap<String, FeatureDescriptor>,
}

impl FeatureRegistry {
    /// Shipped Base Detectors only (all `active`, no behavior change).
    pub fn builtins() -> Self {
        Self::from_descriptors(builtin_base_detectors())
    }

    /// Merge SQLite overlay rows. Overlay cannot replace or demote builtins.
    pub fn with_overlay(overlay: Vec<FeatureDescriptor>) -> Self {
        let mut descriptors: BTreeMap<String, FeatureDescriptor> = builtin_base_detectors()
            .into_iter()
            .map(|d| (d.id.clone(), d))
            .collect();
        for row in overlay {
            if descriptors
                .get(&row.id)
                .is_some_and(|existing| existing.builtin)
            {
                continue;
            }
            descriptors.insert(row.id.clone(), row);
        }
        Self { descriptors }
    }

    fn from_descriptors(rows: Vec<FeatureDescriptor>) -> Self {
        Self {
            descriptors: rows.into_iter().map(|d| (d.id.clone(), d)).collect(),
        }
    }

    /// Lookup by id.
    pub fn get(&self, id: &str) -> Option<&FeatureDescriptor> {
        self.descriptors.get(id)
    }

    /// All descriptors, sorted by id.
    pub fn list(&self) -> Vec<&FeatureDescriptor> {
        self.descriptors.values().collect()
    }

    /// Owned list (catalog / SQLite / MCP).
    pub fn into_descriptors(self) -> Vec<FeatureDescriptor> {
        self.descriptors.into_values().collect()
    }

    /// Register a Base Detector at `candidate`. Does not change pipeline math.
    pub fn register(
        &mut self,
        input: BaseDetectorRegistration,
    ) -> Result<FeatureDescriptor, FeatureRegistryError> {
        let desc = validate_registration(input)?;
        if let Some(existing) = self.descriptors.get(&desc.id) {
            if existing.builtin {
                return Err(FeatureRegistryError::BuiltinImmutable(desc.id));
            }
            return Err(FeatureRegistryError::DuplicateId(desc.id));
        }
        self.descriptors.insert(desc.id.clone(), desc.clone());
        Ok(desc)
    }

    /// Human-gated promotion: candidate → shadow, or shadow → active.
    pub fn promote(
        &mut self,
        id: &str,
        target: PromotionState,
        gate: HumanGate<'_>,
    ) -> Result<FeatureDescriptor, FeatureRegistryError> {
        let current = self
            .descriptors
            .get(id)
            .ok_or_else(|| FeatureRegistryError::NotFound(id.to_string()))?;
        if current.builtin {
            return Err(FeatureRegistryError::BuiltinImmutable(id.to_string()));
        }
        let next = apply_promotion(current.promotion_state, target, gate)?;
        let entry = self.descriptors.get_mut(id).expect("checked");
        entry.promotion_state = next;
        entry.last_gate_note = Some(gate.trader_confirmation.trim().to_string());
        Ok(entry.clone())
    }
}

/// Apply a human-gated promotion step. Skip-state and demotion are rejected.
pub fn apply_promotion(
    from: PromotionState,
    to: PromotionState,
    gate: HumanGate<'_>,
) -> Result<PromotionState, FeatureRegistryError> {
    let _ = HumanGate::parse(gate.trader_confirmation)?;
    if from == to {
        return Err(FeatureRegistryError::AlreadyInState(from));
    }
    let legal = matches!(
        (from, to),
        (PromotionState::Candidate, PromotionState::Shadow)
            | (PromotionState::Shadow, PromotionState::Active)
    );
    if legal {
        Ok(to)
    } else {
        Err(FeatureRegistryError::IllegalPromotion { from, to })
    }
}

/// Validate and normalize a Base Detector registration (always `candidate`).
pub fn validate_registration(
    input: BaseDetectorRegistration,
) -> Result<FeatureDescriptor, FeatureRegistryError> {
    let id = input.id.trim().to_string();
    if !valid_detector_id(&id) {
        return Err(FeatureRegistryError::InvalidId(id));
    }
    if input.name.trim().is_empty()
        || input.description.trim().is_empty()
        || input.domain_id.trim().is_empty()
    {
        return Err(FeatureRegistryError::MissingIdentity);
    }
    if input.schema.catalog_field_ids.is_empty() && input.schema.event_types.is_empty() {
        return Err(FeatureRegistryError::MissingSchema);
    }
    if input.provenance.source.trim().is_empty() || input.provenance.rust_module.trim().is_empty() {
        return Err(FeatureRegistryError::MissingProvenance);
    }
    Ok(FeatureDescriptor {
        id,
        name: input.name.trim().to_string(),
        kind: FeatureKind::BaseDetector,
        description: input.description.trim().to_string(),
        domain_id: input.domain_id.trim().to_string(),
        schema: DetectorSchema {
            catalog_field_ids: unique_sorted(input.schema.catalog_field_ids),
            event_types: unique_sorted(input.schema.event_types),
            unit: input.schema.unit,
            session_scope: input.schema.session_scope,
            freshness: input.schema.freshness,
            cost_hint: input.schema.cost_hint,
        },
        provenance: FeatureProvenance {
            source: input.provenance.source.trim().to_string(),
            rust_module: input.provenance.rust_module.trim().to_string(),
            math_tier: if input.provenance.math_tier.trim().is_empty() {
                TIER1_REVIEWED_RUST.to_string()
            } else {
                input.provenance.math_tier.trim().to_string()
            },
            behavior_change: false,
        },
        promotion_state: PromotionState::Candidate,
        builtin: false,
        last_gate_note: None,
    })
}

/// True when `concept_id` has a catalog field or a Feature Registry entry.
///
/// New specialty market tools for a concept are rejected unless this is true
/// (and the SIL-M0 allowlist is updated together with the catalog).
pub fn concept_has_catalog_or_registry_entry(catalog: &DeskCatalog, concept_id: &str) -> bool {
    let id = concept_id.trim();
    if id.is_empty() {
        return false;
    }
    catalog.fields.iter().any(|f| f.id == id) || catalog.base_detectors.iter().any(|d| d.id == id)
}

/// Registry id a detector specialty tool must cite, if any.
pub fn specialty_tool_registry_id(tool_name: &str) -> Option<&'static str> {
    DETECTOR_SPECIALTY_TOOLS
        .iter()
        .find(|(tool, _)| *tool == tool_name)
        .map(|(_, id)| *id)
}

/// Case-insensitive search over Base Detector descriptors.
pub fn search_features(catalog: &DeskCatalog, query: &str) -> Vec<FeatureDescriptor> {
    let q = query.trim().to_ascii_lowercase();
    if q.is_empty() {
        return Vec::new();
    }
    catalog
        .base_detectors
        .iter()
        .filter(|d| feature_matches(d, &q))
        .cloned()
        .collect()
}

fn feature_matches(d: &FeatureDescriptor, q: &str) -> bool {
    d.id.to_ascii_lowercase().contains(q)
        || d.name.to_ascii_lowercase().contains(q)
        || d.description.to_ascii_lowercase().contains(q)
        || d.domain_id.to_ascii_lowercase().contains(q)
        || d.kind.as_str().to_ascii_lowercase().contains(q)
        || d.promotion_state.as_str().contains(q)
        || d.provenance.rust_module.to_ascii_lowercase().contains(q)
        || d.provenance.source.to_ascii_lowercase().contains(q)
        || d.schema
            .event_types
            .iter()
            .any(|e| e.to_ascii_lowercase().contains(q))
        || d.schema
            .catalog_field_ids
            .iter()
            .any(|e| e.to_ascii_lowercase().contains(q))
        || (q == "base detector" || q == "basedetector" || q == "detector")
}

fn valid_detector_id(id: &str) -> bool {
    let Some(rest) = id.strip_prefix("detector.") else {
        return false;
    };
    let mut chars = rest.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    first.is_ascii_lowercase()
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn unique_sorted(mut items: Vec<String>) -> Vec<String> {
    items.retain(|s| !s.trim().is_empty());
    for item in &mut items {
        *item = item.trim().to_string();
    }
    items.sort();
    items.dedup();
    items
}

fn rust_provenance(module: &str) -> FeatureProvenance {
    FeatureProvenance {
        source: RUST_PIPELINE_SOURCE.into(),
        rust_module: module.into(),
        math_tier: TIER1_REVIEWED_RUST.into(),
        behavior_change: false,
    }
}

fn shipped_schema(
    catalog_field_ids: &[&str],
    event_types: &[&str],
    unit: Unit,
    session_scope: SessionScope,
    freshness: FreshnessSemantics,
    cost_hint: CostHint,
) -> DetectorSchema {
    DetectorSchema {
        catalog_field_ids: catalog_field_ids.iter().map(|s| (*s).to_string()).collect(),
        event_types: event_types.iter().map(|s| (*s).to_string()).collect(),
        unit,
        session_scope,
        freshness,
        cost_hint,
    }
}

fn shipped_detector(
    id: &str,
    name: &str,
    domain_id: &str,
    description: &str,
    rust_module: &str,
    schema: DetectorSchema,
) -> FeatureDescriptor {
    FeatureDescriptor {
        id: id.into(),
        name: name.into(),
        kind: FeatureKind::BaseDetector,
        description: description.into(),
        domain_id: domain_id.into(),
        schema,
        provenance: rust_provenance(rust_module),
        promotion_state: PromotionState::Active,
        builtin: true,
        last_gate_note: None,
    }
}

/// Shipped Base Detectors — registered `active` without changing pipeline math.
pub fn builtin_base_detectors() -> Vec<FeatureDescriptor> {
    vec![
        shipped_detector(
            "detector.absorption",
            "absorption",
            "flow",
            "Base Detector: absorption / exhaustion / delta-divergence (reviewed Rust). Schema, provenance, and promotion are registry-governed. Math lives in pipelines::absorption — this registration does not change behavior.",
            "pipelines::absorption",
            shipped_schema(
                &[
                    "market.flow.absorptionEventCount",
                    "market.flow.confirmedAbsorptionEventCount",
                    "market.flow.confirmedExhaustionEventCount",
                    "market.flow.confirmedDeltaDivergenceEventCount",
                    "market.flow.hasRecentConfirmedAbsorption",
                    "market.flow.hasRecentInvalidatedAbsorption",
                    "market.flow.hasRecentConfirmedExhaustion",
                    "market.flow.recentConfirmedAbsorptionPrice",
                    "market.flow.recentConfirmedAbsorptionDirection",
                    "market.flow.recentConfirmedAbsorptionAgeMs",
                    "market.flow.recentConfirmedAbsorptionDistanceTicks",
                    "market.flow.recentInvalidatedAbsorptionPrice",
                    "market.flow.recentInvalidatedAbsorptionDirection",
                    "market.flow.recentInvalidatedAbsorptionAgeMs",
                    "market.flow.recentInvalidatedAbsorptionDistanceTicks",
                    "market.flow.recentConfirmedExhaustionPrice",
                    "market.flow.recentConfirmedExhaustionDirection",
                    "market.flow.recentConfirmedExhaustionAgeMs",
                ],
                &[
                    "absorption_detected",
                    "absorption_confirmed",
                    "absorption_invalidated",
                ],
                Unit::Count,
                SessionScope::Session,
                FreshnessSemantics::LiveTickAnchored,
                CostHint::R1,
            ),
        ),
        shipped_detector(
            "detector.pinch",
            "pinch",
            "flow",
            "Base Detector: delta-momentum pinch (reviewed Rust). Schema, provenance, and promotion are registry-governed. Math lives in pipelines::pinch — this registration does not change behavior.",
            "pipelines::pinch",
            shipped_schema(
                &["market.flow.pinchEventCount"],
                &["pinch_detected"],
                Unit::Count,
                SessionScope::Session,
                FreshnessSemantics::LiveTickAnchored,
                CostHint::R1,
            ),
        ),
        shipped_detector(
            "detector.rebid_reoffer",
            "rebid_reoffer",
            "response",
            "Base Detector: rebid/reoffer acceleration zones (reviewed Rust). Schema, provenance, and promotion are registry-governed. Math lives in pipelines::rebid_reoffer — this registration does not change behavior.",
            "pipelines::rebid_reoffer",
            shipped_schema(
                &[
                    "market.response.activeZoneCount",
                    "market.response.rebidZoneNear",
                    "market.response.reofferZoneNear",
                    "market.response.rebidZoneRetested",
                    "market.response.reofferZoneRetested",
                    "market.response.rebidZoneHeld",
                    "market.response.reofferZoneHeld",
                    "market.response.nearestZoneDirection",
                    "market.response.nearestZoneStatus",
                    "market.response.nearestZoneDistanceTicks",
                    "market.response.rebidZoneLow",
                    "market.response.rebidZoneHigh",
                    "market.response.reofferZoneLow",
                    "market.response.reofferZoneHigh",
                ],
                &["acceleration_zone_created", "acceleration_zone_held"],
                Unit::Count,
                SessionScope::Session,
                FreshnessSemantics::LiveTickAnchored,
                CostHint::R1,
            ),
        ),
        shipped_detector(
            "detector.structure",
            "structure",
            "location_structure",
            "Base Detector: structural EventDetector (IB/OR/OR5, day type, poor/excess, level tests, rvol_spike). Schema, provenance, and promotion are registry-governed. Math lives in pipelines::event_detector — this registration does not change behavior.",
            "pipelines::event_detector",
            shipped_schema(
                &[
                    "market.location_structure.dayType",
                    "market.location_structure.ibHigh",
                    "market.location_structure.ibLow",
                    "market.location_structure.orHigh",
                    "market.location_structure.orLow",
                    "market.location_structure.or5High",
                    "market.location_structure.or5Low",
                    "market.location_structure.poorHigh",
                    "market.location_structure.poorLow",
                    "market.location_structure.excessHigh",
                    "market.location_structure.excessLow",
                    "market.volatility.rvolRatio",
                ],
                &[
                    "ib_formed",
                    "or_formed",
                    "ib_mid_test",
                    "ib_extension_hit",
                    "new_session_high",
                    "new_session_low",
                    "dnp_cross",
                    "day_type_change",
                    "poor_high_detected",
                    "poor_low_detected",
                    "excess_high_detected",
                    "excess_low_detected",
                    "or5_mid_retest",
                    "rvol_spike",
                ],
                Unit::EnumLabel,
                SessionScope::Rth,
                FreshnessSemantics::SessionScoped,
                CostHint::R1,
            ),
        ),
        shipped_detector(
            "detector.trade_size",
            "trade_size",
            "flow",
            "Base Detector: trade-size distribution / large-trade cluster (reviewed Rust). Schema, provenance, and promotion are registry-governed. Math lives in pipelines::trade_size — this registration does not change behavior.",
            "pipelines::trade_size",
            shipped_schema(
                &["market.flow.avgTradeSize"],
                &["large_trade_cluster"],
                Unit::Contracts,
                SessionScope::Session,
                FreshnessSemantics::LiveTickAnchored,
                CostHint::R1,
            ),
        ),
    ]
}

/// Environment metadata block for `describe_environment`.
pub fn feature_registry_environment(catalog: &DeskCatalog) -> serde_json::Value {
    serde_json::json!({
        "writeVerb": FEATURE_REGISTRY_WRITE_VERB,
        "promotion": PROMOTION_STATES,
        "humanGated": true,
        "baseDetectorCount": catalog.base_detectors.len(),
        "kinds": ["baseDetector"],
        "codegen": false,
        "featureIr": false,
        "newSpecialtyToolPolicy": "no_catalog_or_registry_entry_no_new_market_tool",
        "readOperator": "search_catalog",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::build_catalog;
    use crate::catalog::event_lifecycle::DOM_FAMILY_EVENT_TYPES;
    use std::collections::BTreeSet;

    fn example_registration() -> BaseDetectorRegistration {
        BaseDetectorRegistration {
            id: "detector.example_candidate".into(),
            name: "example_candidate".into(),
            description: "Schema-only Base Detector candidate. No new detector math.".into(),
            domain_id: "flow".into(),
            schema: DetectorSchema {
                catalog_field_ids: vec!["market.flow.pinchEventCount".into()],
                event_types: vec!["example_candidate_detected".into()],
                unit: Unit::Count,
                session_scope: SessionScope::Session,
                freshness: FreshnessSemantics::LiveTickAnchored,
                cost_hint: CostHint::R1,
            },
            provenance: FeatureProvenance {
                source: RUST_PIPELINE_SOURCE.into(),
                rust_module: "unwired".into(),
                math_tier: TIER1_REVIEWED_RUST.into(),
                behavior_change: true, // forced false on register
            },
        }
    }

    #[test]
    fn builtins_include_absorption_and_pinch_as_active_without_behavior_change() {
        let registry = FeatureRegistry::builtins();
        for id in ["detector.absorption", "detector.pinch"] {
            let d = registry.get(id).unwrap_or_else(|| panic!("{id}"));
            assert_eq!(d.kind, FeatureKind::BaseDetector);
            assert_eq!(d.promotion_state, PromotionState::Active);
            assert!(d.builtin);
            assert!(!d.provenance.behavior_change);
            assert_eq!(d.provenance.source, RUST_PIPELINE_SOURCE);
            assert_eq!(d.provenance.math_tier, TIER1_REVIEWED_RUST);
            assert!(!d.schema.event_types.is_empty());
            assert!(!d.schema.catalog_field_ids.is_empty());
        }
        assert!(registry.get("detector.rebid_reoffer").is_some());
        assert!(registry.get("detector.trade_size").is_some());
        assert!(registry.get("detector.structure").is_some());
    }

    #[test]
    fn builtin_ids_are_unique_and_sorted_contract() {
        let rows = builtin_base_detectors();
        let ids: Vec<_> = rows.iter().map(|d| d.id.as_str()).collect();
        let unique: BTreeSet<_> = ids.iter().copied().collect();
        assert_eq!(unique.len(), ids.len());
        assert!(ids.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn builtin_catalog_fields_resolve() {
        let catalog = build_catalog();
        let field_ids: BTreeSet<_> = catalog.fields.iter().map(|f| f.id.as_str()).collect();
        for detector in builtin_base_detectors() {
            for field in &detector.schema.catalog_field_ids {
                assert!(
                    field_ids.contains(field.as_str()),
                    "{} cites unknown catalog field {field}",
                    detector.id
                );
            }
        }
    }

    #[test]
    fn dom_cluster_detectors_are_not_registered_in_m5a() {
        let registry = FeatureRegistry::builtins();
        for ty in DOM_FAMILY_EVENT_TYPES {
            let id = format!("detector.{ty}");
            assert!(
                registry.get(&id).is_none(),
                "{id} is SIL-M5e and must not be a shipped Base Detector here"
            );
        }
        let catalog = build_catalog();
        assert!(!concept_has_catalog_or_registry_entry(
            &catalog,
            "detector.iceberg_reload"
        ));
        assert!(!concept_has_catalog_or_registry_entry(
            &catalog,
            "detector.stop_run"
        ));
    }

    #[test]
    fn register_accepts_base_detector_at_candidate() {
        let mut registry = FeatureRegistry::builtins();
        let desc = registry.register(example_registration()).expect("register");
        assert_eq!(desc.promotion_state, PromotionState::Candidate);
        assert!(!desc.builtin);
        assert!(!desc.provenance.behavior_change);
        assert_eq!(desc.kind, FeatureKind::BaseDetector);
    }

    #[test]
    fn register_rejects_duplicate_and_builtin_ids() {
        let mut registry = FeatureRegistry::builtins();
        let mut dup = example_registration();
        dup.id = "detector.absorption".into();
        assert!(matches!(
            registry.register(dup),
            Err(FeatureRegistryError::BuiltinImmutable(_))
        ));
        registry.register(example_registration()).expect("first");
        assert!(matches!(
            registry.register(example_registration()),
            Err(FeatureRegistryError::DuplicateId(_))
        ));
    }

    #[test]
    fn register_rejects_missing_schema_and_bad_ids() {
        let mut bad = example_registration();
        bad.schema.catalog_field_ids.clear();
        bad.schema.event_types.clear();
        assert_eq!(
            validate_registration(bad).unwrap_err(),
            FeatureRegistryError::MissingSchema
        );
        let mut bad_id = example_registration();
        bad_id.id = "absorption".into();
        assert!(matches!(
            validate_registration(bad_id),
            Err(FeatureRegistryError::InvalidId(_))
        ));
    }

    #[test]
    fn human_gate_required_for_each_promotion_step() {
        assert_eq!(
            HumanGate::parse("").unwrap_err(),
            FeatureRegistryError::HumanGateRequired
        );
        assert_eq!(
            HumanGate::parse("   ").unwrap_err(),
            FeatureRegistryError::HumanGateRequired
        );
        let gate = HumanGate::parse("trader confirms shadow eval").expect("gate");
        assert_eq!(
            apply_promotion(PromotionState::Candidate, PromotionState::Active, gate).unwrap_err(),
            FeatureRegistryError::IllegalPromotion {
                from: PromotionState::Candidate,
                to: PromotionState::Active,
            }
        );
        assert_eq!(
            apply_promotion(PromotionState::Candidate, PromotionState::Shadow, gate).expect("step"),
            PromotionState::Shadow
        );
        assert_eq!(
            apply_promotion(PromotionState::Shadow, PromotionState::Active, gate).expect("step"),
            PromotionState::Active
        );
    }

    #[test]
    fn promote_walks_candidate_shadow_active_and_rejects_builtin() {
        let mut registry = FeatureRegistry::builtins();
        registry.register(example_registration()).expect("register");
        let gate = HumanGate::parse("your rules say promote to shadow").expect("gate");
        let shadow = registry
            .promote("detector.example_candidate", PromotionState::Shadow, gate)
            .expect("shadow");
        assert_eq!(shadow.promotion_state, PromotionState::Shadow);
        let active = registry
            .promote("detector.example_candidate", PromotionState::Active, gate)
            .expect("active");
        assert_eq!(active.promotion_state, PromotionState::Active);
        assert!(matches!(
            registry.promote("detector.absorption", PromotionState::Shadow, gate),
            Err(FeatureRegistryError::BuiltinImmutable(_))
        ));
    }

    #[test]
    fn overlay_cannot_clobber_builtins() {
        let mut fake = builtin_base_detectors()
            .into_iter()
            .find(|d| d.id == "detector.pinch")
            .expect("pinch");
        fake.promotion_state = PromotionState::Candidate;
        fake.builtin = false;
        let merged = FeatureRegistry::with_overlay(vec![fake]);
        let pinch = merged.get("detector.pinch").expect("pinch");
        assert_eq!(pinch.promotion_state, PromotionState::Active);
        assert!(pinch.builtin);
    }

    #[test]
    fn detector_specialty_tools_require_active_registry_entry() {
        let catalog = build_catalog();
        for (tool, id) in DETECTOR_SPECIALTY_TOOLS {
            assert!(
                catalog.specialty_market_tools.iter().any(|t| t == tool),
                "{tool} must stay on the specialty allowlist"
            );
            let detector = catalog
                .base_detectors
                .iter()
                .find(|d| d.id == *id)
                .unwrap_or_else(|| panic!("{id}"));
            assert_eq!(detector.promotion_state, PromotionState::Active);
            assert_eq!(specialty_tool_registry_id(tool), Some(*id));
            assert!(concept_has_catalog_or_registry_entry(&catalog, id));
        }
    }

    #[test]
    fn unregistered_concept_cannot_add_a_specialty_tool() {
        let catalog = build_catalog();
        assert!(!concept_has_catalog_or_registry_entry(
            &catalog,
            "detector.iceberg_reload"
        ));
        assert!(concept_has_catalog_or_registry_entry(
            &catalog,
            "detector.absorption"
        ));
        assert!(concept_has_catalog_or_registry_entry(
            &catalog,
            "market.location_structure.poc"
        ));
    }

    #[test]
    fn sqlite_overlay_round_trips_without_changing_builtins() {
        let db = crate::db::Database::open(":memory:").expect("db");
        let mut registry = FeatureRegistry::builtins();
        let registered = registry.register(example_registration()).expect("register");
        db.upsert_feature_registry(&registered, 1_700_000_000_000.0)
            .expect("upsert");
        let overlay = db.list_feature_registry().expect("list");
        assert_eq!(overlay.len(), 1);
        assert_eq!(overlay[0].id, "detector.example_candidate");
        assert_eq!(overlay[0].promotion_state, PromotionState::Candidate);
        let merged = FeatureRegistry::with_overlay(overlay);
        assert_eq!(
            merged
                .get("detector.absorption")
                .expect("builtin")
                .promotion_state,
            PromotionState::Active
        );
        assert_eq!(
            merged
                .get("detector.example_candidate")
                .expect("overlay")
                .promotion_state,
            PromotionState::Candidate
        );
    }
}
