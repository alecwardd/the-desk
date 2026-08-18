//! Desk Catalog v0 — schema waist over annotated runtime state.
//!
//! Field *identity* is bound to `MarketState` (CI fails if a rust field is
//! missing or orphaned). Descriptions are sourced from `MarketState` doc-
//! comments (CI fails if `///` text drifts from the catalog table). Unit,
//! session scope, freshness, and cost hint are curated annotations beside
//! those fields. Discovery operators serve this metadata only — never live
//! market data. Read-kernel operators (`get_state` / `get_events`) return
//! provenance-carrying envelopes at Trust Level L0. See ADR-022 (Trust
//! Ceiling L3), SIL-M1a (#4), and SIL-M1b (#5).

mod codegen;
mod config;
mod envelope;
mod event_lifecycle;
mod events_kernel;
mod feature_ir;
mod feature_registry;
mod market_state_fields;
mod positioning;
mod positioning_record;
mod render;
mod search;
mod trust;
mod types;

pub use codegen::{
    apply_codegen_fields, catalog_field_values_from_eval, emit_agent_schema, emit_all,
    emit_query_dimension, emit_rule_binding, emit_runtime_field, emit_storage_column,
    evaluate_accepted_feature, is_accepted_derived, is_codegen_field, read_catalog_field_values,
    read_storage_value, stamp_derived_feature_payload, AgentSchemaArtifact, CodegenArtifacts,
    CodegenError, QueryDimensionArtifact, RuleBindingArtifact, RuntimeFieldArtifact,
    StorageColumnArtifact, CATALOG_FIELD_CONDITION, DERIVED_FEATURE_PAYLOAD_OBJECT,
    EMIT_AGENT_SCHEMA, EMIT_QUERY_DIMENSION, EMIT_RULE_BINDING, EMIT_RUNTIME_FIELD,
    EMIT_STORAGE_COLUMN, FEATURE_REGISTRY_CODEGEN, JOURNAL_FRAME_STORAGE_TARGET,
    QUERY_DIMENSION_OPERATORS,
};
pub use config::{load_sil_config, EngineMode, SilConfig};
pub use envelope::{
    apply_token_budget, build_state_envelope, merge_symbol_envelopes, state_envelope_json,
    DomainProvenance, EnvelopeError, ProvenanceSource, StateEnvelope, StateReadRequest,
    StateResolution, TrustLevel,
};
pub use event_lifecycle::{
    apply_lifecycle_transition, cheap_model_may_invoke, classify_event_family,
    detection_kind_for_event_type, event_dedup_identity_id, event_dedup_identity_key,
    event_family_key, is_dom_family_event_type, is_invalidation_event_type,
    next_lifecycle_for_detection, requires_capsule, resolve_event_severity, DetectionKind,
    EventFamily, EventLifecycle, EventSeverity, FrameRef, LifecycleError, CAPSULE_AFTER_MS,
    CAPSULE_LOOKBACK_MS, DOM_FAMILY_EVENT_TYPES, EVENT_LIFECYCLE_STATES, EVENT_LIFECYCLE_TTL_MS,
    SEVERITY_UNSPECIFIED,
};
pub use events_kernel::{
    attach_capsule_refs, coaching_kernel_events_from_db_rows, collapse_events_latest_per_dedup,
    kernel_event_envelope_fields_present, kernel_event_from_db_row, kernel_event_from_market_event,
    kernel_event_from_market_event_scoped, kernel_event_from_persisted, CapsuleRef, EventsEnvelope,
    KernelEvent, COACHING_EVENT_FETCH_CAP, SEVERITY_PLACEHOLDER,
};
pub use feature_ir::{
    declare_program, evaluate, evaluate_historical, evaluate_live_shadow, merge_eval_frames,
    BaselineAggregator, DwellMode, EventSelector, FeatureIrError, FeatureIrEvalPath,
    FeatureIrEvent, FeatureIrFrame, FeatureIrProgram, FeatureIrStore, FeatureIrValue,
    FieldPredicate, MergedEvalWindow, OperatorFamily, PercentileOutput, PredicateOp,
    DERIVED_FEATURE_MATH_TIER, FEATURE_IR_EVAL_MAX_FRAMES, FEATURE_IR_MODULE, FEATURE_IR_SOURCE,
    FUNDED_OPERATOR_FAMILY_GLOSSARY, FUNDED_OPERATOR_FAMILY_LABELS, NEW_OPERATOR_FAMILY_GATE,
};
pub use feature_registry::{
    apply_promotion, builtin_base_detectors, concept_has_catalog_or_registry_entry,
    feature_registry_environment, search_features, specialty_tool_registry_id,
    validate_derived_feature, validate_registration, BaseDetectorRegistration,
    DerivedFeatureRegistration, DetectorSchema, FeatureDescriptor, FeatureKind, FeatureProvenance,
    FeatureRegistry, FeatureRegistryError, HumanGate, PromotionState, DETECTOR_SPECIALTY_TOOLS,
    FEATURE_REGISTRY_WRITE_VERB, PROMOTION_STATES, RUST_PIPELINE_SOURCE, TIER1_REVIEWED_RUST,
};
pub use positioning_record::{
    accept_levels_only_entry, apply_positioning_slice, empty_positioning_slice, evaluate_freshness,
    positioning_state_slice, DerivedLevels, MidDayRead, PositioningEntryInput, PositioningError,
    PositioningRecord, PositioningRecordProvenance, PositioningStateSlice, PositioningWall,
    LEVELS_ONLY_RECORD_KIND, MANUAL_PROVENANCE_SOURCE,
};
pub use render::{
    catalog_json_path, catalog_markdown_path, render_catalog_json, render_catalog_markdown,
    write_catalog_docs,
};
pub use search::search_catalog;
pub use trust::{
    is_kernel_read_query_tool, kernel_read_query_capabilities, tool_name_implies_mutation,
    ToolCapability, KERNEL_READ_QUERY_TOOLS,
};
pub use types::{
    CostHint, DeskCatalog, DomainDescriptor, FieldDescriptor, FieldSpec, FreshnessSemantics,
    PositioningRecordKind, SessionScope, TrustCeiling, Unit, CATALOG_VERSION,
};

use market_state_fields::market_state_field_specs;
use positioning::positioning_domain;

/// Build the versioned Desk Catalog from annotated runtime specs + stubs.
pub fn build_catalog() -> DeskCatalog {
    let mut fields: Vec<FieldDescriptor> = market_state_field_specs()
        .into_iter()
        .map(FieldDescriptor::from_spec)
        .collect();

    let positioning = positioning_domain();
    fields.extend(positioning.fields.clone());

    let mut domains = base_domain_shells();
    // Attach field ids per domain.
    for field in &fields {
        if let Some(domain) = domains.iter_mut().find(|d| d.id == field.domain_id) {
            domain.field_ids.push(field.id.clone());
        }
    }
    // Positioning domain carries record-kind metadata.
    if let Some(domain) = domains.iter_mut().find(|d| d.id == "positioning") {
        *domain = positioning.domain;
        domain.field_ids = fields
            .iter()
            .filter(|f| f.domain_id == "positioning")
            .map(|f| f.id.clone())
            .collect();
    }

    domains.sort_by(|a, b| a.id.cmp(&b.id));
    fields.sort_by(|a, b| a.id.cmp(&b.id));

    let mut base_detectors = builtin_base_detectors();
    base_detectors.sort_by(|a, b| a.id.cmp(&b.id));

    DeskCatalog {
        catalog_version: CATALOG_VERSION.to_string(),
        trust_ceiling: TrustCeiling::L3,
        specialty_market_tools: specialty_market_tools_allowlist(),
        domains,
        fields,
        positioning_record_kinds: positioning.record_kinds,
        positioning_provider: None,
        base_detectors,
        derived_features: Vec::new(),
    }
}

/// Catalog plus SQLite Feature Registry overlay (registered candidates / promotions).
///
/// Overlay rows cannot replace shipped builtins. Used by discovery operators so
/// newly registered Base Detectors are searchable without a specialty getter.
pub fn build_catalog_with_overlay(overlay: Vec<FeatureDescriptor>) -> DeskCatalog {
    let mut catalog = build_catalog();
    let registry = FeatureRegistry::with_overlay(overlay);
    catalog.base_detectors = registry.base_detectors();
    catalog.derived_features = registry.derived_features();
    apply_codegen_fields(&mut catalog);
    catalog
}

/// Environment metadata for `describe_environment` (no live values).
pub fn describe_environment(catalog: &DeskCatalog, discovery_enabled: bool) -> serde_json::Value {
    let mut feature_registry = feature_registry_environment(catalog);
    if let Some(obj) = feature_registry.as_object_mut() {
        obj.insert(
            "discoveryEnabled".into(),
            serde_json::json!(discovery_enabled),
        );
    }
    serde_json::json!({
        "catalogVersion": catalog.catalog_version,
        "trustCeiling": catalog.trust_ceiling,
        "trustCeilingNote": "L3 drafts proposals; the human executes (ADR-022). Raising the Trust Ceiling requires a new ADR.",
        "discoveryEnabled": discovery_enabled,
        "domainCount": catalog.domains.len(),
        "fieldCount": catalog.fields.len(),
        "domains": catalog.domains.iter().map(|d| {
            serde_json::json!({
                "id": d.id,
                "name": d.name,
                "summary": d.summary,
                "fieldCount": d.field_ids.len(),
            })
        }).collect::<Vec<_>>(),
        "positioning": {
            "provider": catalog.positioning_provider,
            "recordKinds": catalog.positioning_record_kinds,
            "writeVerb": "positioning_entry",
            "levelsOnlyFirstClass": true,
        },
        "specialtyMarketToolsPolicy": "no_catalog_or_registry_entry_no_new_market_tool",
        "specialtyMarketToolCount": catalog.specialty_market_tools.len(),
        "featureRegistry": feature_registry,
        "marketRouter": {
            "roots": ["NQ", "ES"],
            "oneClock": true,
            "microsInScope": false,
        },
        "eventKernel": {
            "lifecycleFormalized": true,
            "operator": "get_events",
            "attentionView": "get_attention_inbox",
            "lifecycle": crate::catalog::EVENT_LIFECYCLE_STATES,
            "domFamilyEventTypes": crate::catalog::DOM_FAMILY_EVENT_TYPES,
            "capsules": "later",
            "cheapModel": "event_triggered_only",
        },
        "metadataOnly": true,
    })
}

/// Domain metadata for `describe_domain` (no live values).
pub fn describe_domain(catalog: &DeskCatalog, domain_id: &str) -> Option<serde_json::Value> {
    let domain = catalog.domains.iter().find(|d| d.id == domain_id)?;
    let fields: Vec<&FieldDescriptor> = catalog
        .fields
        .iter()
        .filter(|f| f.domain_id == domain_id)
        .collect();
    let mut out = serde_json::json!({
        "catalogVersion": catalog.catalog_version,
        "domain": domain,
        "fields": fields,
        "metadataOnly": true,
    });
    if domain_id == "positioning" {
        if let Some(obj) = out.as_object_mut() {
            if let Ok(kinds) = serde_json::to_value(&catalog.positioning_record_kinds) {
                obj.insert("recordKinds".to_string(), kinds);
            }
            obj.insert("provider".to_string(), serde_json::Value::Null);
            obj.insert(
                "writeVerb".to_string(),
                serde_json::Value::String("positioning_entry".into()),
            );
            obj.insert(
                "levelsOnlyFirstClass".to_string(),
                serde_json::Value::Bool(true),
            );
        }
    }
    if domain_id == "events" {
        if let Some(obj) = out.as_object_mut() {
            obj.insert("lifecycleFormalized".into(), serde_json::Value::Bool(true));
            obj.insert(
                "lifecycle".into(),
                serde_json::json!(crate::catalog::EVENT_LIFECYCLE_STATES),
            );
            obj.insert(
                "readOperator".into(),
                serde_json::Value::String("get_events".into()),
            );
            obj.insert(
                "attentionView".into(),
                serde_json::Value::String("get_attention_inbox".into()),
            );
            obj.insert(
                "domFamilyEventTypes".into(),
                serde_json::json!(crate::catalog::DOM_FAMILY_EVENT_TYPES),
            );
            obj.insert(
                "requiresCapsule".into(),
                serde_json::Value::String(
                    "later — DOM-family types will require Capsules; this milestone names them only"
                        .into(),
                ),
            );
            obj.insert(
                "cheapModel".into(),
                serde_json::Value::String("event_triggered_only".into()),
            );
            obj.insert("trustLevel".into(), serde_json::json!(TrustLevel::L0));
        }
    }
    let detectors: Vec<&FeatureDescriptor> = catalog
        .base_detectors
        .iter()
        .filter(|d| d.domain_id == domain_id)
        .collect();
    if !detectors.is_empty() {
        if let Some(obj) = out.as_object_mut() {
            if let Ok(value) = serde_json::to_value(&detectors) {
                obj.insert("baseDetectors".into(), value);
            }
        }
    }
    let derived: Vec<&FeatureDescriptor> = catalog
        .derived_features
        .iter()
        .filter(|d| d.domain_id == domain_id)
        .collect();
    if !derived.is_empty() {
        if let Some(obj) = out.as_object_mut() {
            if let Ok(value) = serde_json::to_value(&derived) {
                obj.insert("derivedFeatures".into(), value);
            }
        }
    }
    Some(out)
}

fn base_domain_shells() -> Vec<DomainDescriptor> {
    vec![
        DomainDescriptor {
            id: "identity".into(),
            name: "Identity".into(),
            summary: "Instrument identity, session labels, and contract resolution metadata.".into(),
            field_ids: vec![],
            record_kinds: vec![],
        },
        DomainDescriptor {
            id: "location_structure".into(),
            name: "Location / structure".into(),
            summary: "Price location and auction structure: VWAP, TPO VA/POC, DNVA/DNP, IB/OR/OR5, day type.".into(),
            field_ids: vec![],
            record_kinds: vec![],
        },
        DomainDescriptor {
            id: "flow".into(),
            name: "Flow".into(),
            summary: "Participation and aggression: delta, tape pace, absorption, pinch, trade size.".into(),
            field_ids: vec![],
            record_kinds: vec![],
        },
        DomainDescriptor {
            id: "liquidity".into(),
            name: "Liquidity".into(),
            summary: "Order-book / DOM liquidity summaries when depth context is available.".into(),
            field_ids: vec![],
            record_kinds: vec![],
        },
        DomainDescriptor {
            id: "response".into(),
            name: "Response".into(),
            summary: "Market response structures such as rebid/reoffer acceleration zones.".into(),
            field_ids: vec![],
            record_kinds: vec![],
        },
        DomainDescriptor {
            id: "volatility".into(),
            name: "Volatility".into(),
            summary: "Relative volume and related session volatility framing.".into(),
            field_ids: vec![],
            record_kinds: vec![],
        },
        DomainDescriptor {
            id: "positioning".into(),
            name: "Positioning".into(),
            summary: "Dealer/options Positioning — first-class Levels-Only Records via positioning_entry; no live Vs3dProvider.".into(),
            field_ids: vec![],
            record_kinds: vec![],
        },
        DomainDescriptor {
            id: "cross_market".into(),
            name: "Cross-market".into(),
            summary: "Cross-session inventory and multi-session trend framing.".into(),
            field_ids: vec![],
            record_kinds: vec![],
        },
        DomainDescriptor {
            id: "events".into(),
            name: "Events".into(),
            summary: "Formalized event stream: lifecycle (open → updated → resolved|expired), severity, dedup identity, frameRef to the producing Journal Frame, and capsuleRef on DOM-family rows (stop_run, iceberg_reload, pull_intent, book_velocity_regime_shift). Reads ride get_events; the attention inbox is a ranked view over this stream.".into(),
            field_ids: vec![],
            record_kinds: vec![],
        },
        DomainDescriptor {
            id: "meta".into(),
            name: "Meta".into(),
            summary: "Catalog and envelope metadata (version pins, cost bands).".into(),
            field_ids: vec![],
            record_kinds: vec![],
        },
    ]
}

/// Specialty market tools allowlisted by Catalog v0 (matches SIL-M0 freeze set).
/// Expanding this list requires a catalog entry for the concept the tool exposes.
fn specialty_market_tools_allowlist() -> Vec<String> {
    [
        "check_delta_confirmation",
        "get_absorption_events",
        "get_context_frame",
        "get_day_type",
        "get_delta_at_price",
        "get_delta_profile",
        "get_footprint",
        "get_footprint_window",
        "get_imbalances",
        "get_key_levels",
        "get_market_snapshot",
        "get_or5_status",
        "get_pinch_events",
        "get_proximity_report",
        "get_rebid_reoffer_zones",
        "get_rvol",
        "get_session_context",
        "get_session_inventory",
        "get_session_summary",
        "get_snapshot_at",
        "get_tape_pace",
        "get_tpo_detail",
        "get_tpo_profile",
        "get_trade_size_profile",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn catalog_version_is_pinned() {
        let cat = build_catalog();
        assert_eq!(cat.catalog_version, CATALOG_VERSION);
        assert_eq!(cat.trust_ceiling, TrustCeiling::L3);
    }

    #[test]
    fn every_field_domain_id_resolves_to_a_domain_shell() {
        let cat = build_catalog();
        let domain_ids: BTreeSet<_> = cat.domains.iter().map(|d| d.id.as_str()).collect();
        for field in &cat.fields {
            assert!(
                domain_ids.contains(field.domain_id.as_str()),
                "field `{}` references unknown domain_id `{}`",
                field.id,
                field.domain_id
            );
        }
        for domain in &cat.domains {
            for field_id in &domain.field_ids {
                assert!(
                    cat.fields.iter().any(|f| f.id == *field_id),
                    "domain `{}` lists unknown field_id `{}`",
                    domain.id,
                    field_id
                );
            }
        }
    }

    #[test]
    fn trust_ceiling_and_cost_hints_serialize_documented_labels() {
        let cat = build_catalog();
        let json = serde_json::to_value(&cat).expect("serialize");
        assert_eq!(json["trustCeiling"], "L3");
        let cost = json["fields"][0]["costHint"].as_str().expect("cost");
        assert!(
            matches!(cost, "R0" | "R1" | "R2" | "R3"),
            "costHint wire label must be R0-R3, got {cost}"
        );
    }

    #[test]
    fn every_market_state_field_has_catalog_entry() {
        let source = include_str!("../pipelines/mod.rs");
        let start = source
            .find("pub struct MarketState {")
            .expect("MarketState struct");
        let body = &source[start..];
        let end = body.find("\n}").expect("MarketState end");
        let struct_body = &body[..end];
        let mut src_fields = BTreeSet::new();
        let mut src_docs: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        let mut cur_doc: Vec<String> = Vec::new();
        for line in struct_body.lines() {
            let trimmed = line.trim();
            if let Some(doc) = trimmed.strip_prefix("///") {
                cur_doc.push(doc.trim().to_string());
                continue;
            }
            if trimmed.starts_with("//") {
                cur_doc.clear();
                continue;
            }
            // Match `pub field_name:` only — skip `pub struct …`.
            if let Some(rest) = trimmed.strip_prefix("pub ") {
                if let Some((name, _)) = rest.split_once(':') {
                    let name = name.trim();
                    if name.starts_with(|c: char| c.is_ascii_lowercase())
                        && name
                            .chars()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                    {
                        src_fields.insert(name.to_string());
                        if !cur_doc.is_empty() {
                            src_docs.insert(name.to_string(), cur_doc.join(" "));
                        }
                    }
                }
            }
            if trimmed.starts_with("pub ") {
                cur_doc.clear();
            }
        }

        let specs = market_state_field_specs();
        let catalog_fields: BTreeSet<_> = specs.iter().map(|s| s.rust_field.to_string()).collect();
        assert_eq!(
            src_fields, catalog_fields,
            "MarketState fields and catalog rust_field set must match exactly"
        );

        // Descriptions are annotations sourced from MarketState doc-comments —
        // CI fails if a field's /// text drifts from the catalog without regen.
        for spec in &specs {
            let empty = String::new();
            let expected = src_docs.get(spec.rust_field).unwrap_or(&empty);
            assert_eq!(
                spec.description,
                expected.as_str(),
                "catalog description for `{}` must match MarketState /// doc-comment \
                 (regen: python scripts/gen_catalog_fields.py)",
                spec.rust_field
            );
        }
    }

    #[test]
    fn descriptors_carry_required_semantics() {
        let cat = build_catalog();
        assert!(!cat.fields.is_empty());
        for field in &cat.fields {
            assert!(!field.id.is_empty());
            assert!(!field.name.is_empty());
            assert!(!field.domain_id.is_empty());
            assert!(!field.description.is_empty());
            // unit / session_scope / freshness / cost_hint are enums — presence is type-enforced
            let _ = (
                &field.unit,
                &field.session_scope,
                &field.freshness,
                &field.cost_hint,
            );
        }
    }

    #[test]
    fn positioning_domain_names_four_record_kinds_including_first_class_levels_only() {
        let cat = build_catalog();
        assert!(cat.positioning_provider.is_none());
        assert!(cat
            .fields
            .iter()
            .any(|f| f.id == "positioning.completeness"));
        assert!(cat.fields.iter().any(|f| f.id == "positioning.asOf"));
        let completeness = cat
            .fields
            .iter()
            .find(|f| f.id == "positioning.completeness")
            .expect("completeness");
        assert_eq!(completeness.cost_hint, CostHint::R0);
        assert!(!completeness
            .description
            .to_lowercase()
            .contains("second-class"));
        assert!(!completeness.description.to_lowercase().contains("degraded"));
        let kind_ids: BTreeSet<_> = cat
            .positioning_record_kinds
            .iter()
            .map(|k| k.id.as_str())
            .collect();
        assert_eq!(
            kind_ids,
            BTreeSet::from([
                "position_grid",
                "positions_by_strike",
                "slice",
                "levels_only",
            ])
        );
        let names: BTreeSet<_> = cat
            .positioning_record_kinds
            .iter()
            .map(|k| k.name.as_str())
            .collect();
        assert!(names.contains("grid"));
        assert!(names.contains("by-strike"));
        assert!(names.contains("Slice"));
        assert!(names.contains("Levels-Only Record"));
        assert!(cat.domains.iter().any(|d| d.id == "positioning"));
    }

    #[test]
    fn describe_apis_return_metadata_only() {
        let cat = build_catalog();
        let env = describe_environment(&cat, false);
        let env_str = env.to_string();
        assert!(env_str.contains("catalogVersion"));
        assert!(!env_str.contains("\"lastPrice\""));
        assert!(!env_str.contains("\"vwap\""));
        assert_eq!(env["metadataOnly"], true);

        let domain = describe_domain(&cat, "location_structure").expect("domain");
        let domain_str = domain.to_string();
        assert!(domain_str.contains("catalogVersion"));
        assert_eq!(domain["metadataOnly"], true);
        // Field descriptors may mention vwap as a field *name*, but must not
        // carry live numeric market values.
        assert!(
            domain_str.contains("\"name\":\"vwap\"")
                || domain_str.contains("\"id\":\"market.location_structure.vwap\"")
        );
        assert!(!domain_str.contains("\"lastPrice\":"));
    }

    #[test]
    fn search_catalog_finds_poc_by_text() {
        let cat = build_catalog();
        let hits = search_catalog(&cat, "poc");
        assert!(hits.iter().any(|h| h.id.contains("poc") || h.name == "poc"));
    }

    #[test]
    fn specialty_market_tools_allowlist_is_sorted_and_nonempty() {
        let cat = build_catalog();
        assert_eq!(cat.specialty_market_tools.len(), 24);
        assert!(cat.specialty_market_tools.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn catalog_includes_shipped_base_detectors() {
        let cat = build_catalog();
        assert!(cat.base_detectors.len() >= 2);
        assert!(cat
            .base_detectors
            .iter()
            .any(|d| d.id == "detector.absorption"
                && d.promotion_state == crate::catalog::PromotionState::Active));
        assert!(cat.base_detectors.iter().any(|d| d.id == "detector.pinch"
            && d.promotion_state == crate::catalog::PromotionState::Active));
        let env = describe_environment(&cat, true);
        assert_eq!(env["featureRegistry"]["humanGated"], true);
        assert_eq!(env["featureRegistry"]["writeVerb"], "feature_registry");
        assert_eq!(env["featureRegistry"]["discoveryEnabled"], true);
        assert_eq!(env["featureRegistry"]["featureIr"], true);
        assert_eq!(env["featureRegistry"]["codegen"], true);
        assert_eq!(
            env["featureRegistry"]["newOperatorFamilyGate"],
            "registry_change_proposal"
        );
        assert_eq!(env["featureRegistry"]["readRequiresCatalogDiscovery"], true);
        let env_off = describe_environment(&cat, false);
        assert_eq!(env_off["featureRegistry"]["discoveryEnabled"], false);
        assert_eq!(
            env["specialtyMarketToolsPolicy"],
            "no_catalog_or_registry_entry_no_new_market_tool"
        );
        let flow = describe_domain(&cat, "flow").expect("flow");
        let detectors = flow["baseDetectors"].as_array().expect("baseDetectors");
        assert!(detectors.iter().any(|d| d["id"] == "detector.absorption"));
        assert!(detectors.iter().any(|d| d["id"] == "detector.pinch"));
    }

    #[test]
    fn search_features_finds_pinch_without_a_specialty_getter() {
        let cat = build_catalog();
        let hits = crate::catalog::search_features(&cat, "pinch");
        assert!(hits.iter().any(|h| h.id == "detector.pinch"));
        let hits = crate::catalog::search_features(&cat, "base detector");
        assert!(hits.len() >= 2);
        assert!(hits
            .iter()
            .all(|h| h.kind == crate::catalog::FeatureKind::BaseDetector));
        let reentry = crate::catalog::search_features(&cat, "ib_reentry");
        assert!(reentry.iter().any(|h| h.id == "detector.structure"));
        let poc_test = crate::catalog::search_features(&cat, "poc_test");
        assert!(poc_test.iter().any(|h| h.id == "detector.structure"));
    }
}
