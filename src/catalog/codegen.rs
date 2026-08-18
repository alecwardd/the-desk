//! SIL-M5c Feature Registry codegen — one accepted descriptor → five emitters.
//!
//! This is the cure for five-way write amplification: runtime field, Journal
//! Frame payload key, query dimension, rules binding, and agent schema are
//! generated from a Feature Registry Derived Feature. Hand-written write sites
//! for `feature.*` ids are rejected by drift tests.
//!
//! Storage stays `journal_frames.payload` (named key). DuckDB / Parquet are
//! not a dependency. Reads stay on the existing kernel (`search_catalog`,
//! `get_state`, `query_series` / `query_episodes`) — no specialty MCP tool.
//!
//! Base Detector math stays reviewed Rust and is not re-expressed here.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::feature_ir::{
    evaluate, FeatureIrError, FeatureIrEvalPath, FeatureIrFrame, FeatureIrStore,
};
use super::feature_registry::{
    FeatureDescriptor, FeatureKind, PromotionState, FEATURE_REGISTRY_WRITE_VERB,
};
use super::types::{DeskCatalog, FieldDescriptor};

/// `describe_environment` `featureRegistry.codegen` — SIL-M5c is on.
pub const FEATURE_REGISTRY_CODEGEN: bool = true;

/// Payload object that holds generated Derived Feature scalars.
pub const DERIVED_FEATURE_PAYLOAD_OBJECT: &str = "derivedFeatures";

/// Journal Frame storage target (existing schema — not a DuckDB table).
pub const JOURNAL_FRAME_STORAGE_TARGET: &str = "journal_frames.payload";

/// `get_state` / catalog runtime field emitter name (reserved by SIL-M5b).
pub const EMIT_RUNTIME_FIELD: &str = "emit_runtime_field";
/// Journal Frame payload-key emitter name.
pub const EMIT_STORAGE_COLUMN: &str = "emit_storage_column";
/// `query_series` / `query_episodes` dimension emitter name.
pub const EMIT_QUERY_DIMENSION: &str = "emit_query_dimension";
/// Rules-engine catalog-field binding emitter name.
pub const EMIT_RULE_BINDING: &str = "emit_rule_binding";
/// Desk Catalog / `search_catalog` / `describe_domain` emitter name.
pub const EMIT_AGENT_SCHEMA: &str = "emit_agent_schema";

/// Wire label for the generic catalog-field rules condition (not a specialty tool).
pub const CATALOG_FIELD_CONDITION: &str = "catalog_field";

/// Query kernel operators that address a generated dimension.
pub const QUERY_DIMENSION_OPERATORS: &[&str] = &["query_series", "query_episodes"];

/// Codegen errors (typed until MCP / CLI).
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CodegenError {
    #[error(
        "codegen requires a Derived Feature with a Feature-IR program (ids feature.<snake_id>)"
    )]
    NotDerivedFeature,
    #[error("Derived Feature `{0}` is missing a Feature-IR program")]
    MissingProgram(String),
    #[error("{0}")]
    FeatureIr(#[from] FeatureIrError),
}

/// Runtime field so `get_state` can serve the Generated value (R0/R1 when cost allows).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeFieldArtifact {
    pub field: FieldDescriptor,
    pub served_by: String,
}

/// Named key on `journal_frames.payload` (existing schema).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageColumnArtifact {
    pub payload_key: String,
    pub payload_object: String,
    pub table: String,
}

/// Query dimension so `query_series` / `query_episodes` can address the field id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryDimensionArtifact {
    pub field_id: String,
    pub operators: Vec<String>,
}

/// Rules-engine binding (catalog-field condition — not a new specialty variant).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleBindingArtifact {
    pub condition_field: String,
    pub catalog_field_id: String,
    pub write_verb: String,
}

/// Agent-facing catalog schema (`search_catalog` / `describe_domain`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSchemaArtifact {
    pub field: FieldDescriptor,
    pub search_catalog: bool,
    pub describe_domain: bool,
}

/// Five artifacts emitted from one descriptor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodegenArtifacts {
    pub id: String,
    pub runtime_field: RuntimeFieldArtifact,
    pub storage_column: StorageColumnArtifact,
    pub query_dimension: QueryDimensionArtifact,
    pub rule_binding: RuleBindingArtifact,
    pub agent_schema: AgentSchemaArtifact,
}

/// True when a Derived Feature is accepted (human-gated `active` with a program).
pub fn is_accepted_derived(desc: &FeatureDescriptor) -> bool {
    desc.kind == FeatureKind::DerivedFeature
        && desc.promotion_state == PromotionState::Active
        && desc.program.is_some()
}

/// True when a catalog field was generated from a Derived Feature descriptor.
pub fn is_codegen_field(field: &FieldDescriptor) -> bool {
    field.id.starts_with("feature.") || field.rust_field == "feature_ir"
}

/// Named payload key for a Derived Feature (`derivedFeatures.<id>`).
pub fn storage_payload_key(feature_id: &str) -> String {
    format!("{DERIVED_FEATURE_PAYLOAD_OBJECT}.{feature_id}")
}

/// Catalog / runtime [`FieldDescriptor`] for one Derived Feature.
pub fn emit_runtime_field(desc: &FeatureDescriptor) -> Result<RuntimeFieldArtifact, CodegenError> {
    Ok(RuntimeFieldArtifact {
        field: derived_field_descriptor(desc)?,
        served_by: "get_state".into(),
    })
}

/// Journal Frame storage column (payload key on the existing schema).
pub fn emit_storage_column(
    desc: &FeatureDescriptor,
) -> Result<StorageColumnArtifact, CodegenError> {
    require_derived(desc)?;
    Ok(StorageColumnArtifact {
        payload_key: storage_payload_key(&desc.id),
        payload_object: DERIVED_FEATURE_PAYLOAD_OBJECT.into(),
        table: JOURNAL_FRAME_STORAGE_TARGET.into(),
    })
}

/// Query dimension so series / episode operators can address the field id.
pub fn emit_query_dimension(
    desc: &FeatureDescriptor,
) -> Result<QueryDimensionArtifact, CodegenError> {
    require_derived(desc)?;
    Ok(QueryDimensionArtifact {
        field_id: desc.id.clone(),
        operators: QUERY_DIMENSION_OPERATORS
            .iter()
            .map(|s| (*s).to_string())
            .collect(),
    })
}

/// Rules-engine catalog-field binding (not a new specialty `ConditionField` variant).
pub fn emit_rule_binding(desc: &FeatureDescriptor) -> Result<RuleBindingArtifact, CodegenError> {
    require_derived(desc)?;
    Ok(RuleBindingArtifact {
        condition_field: CATALOG_FIELD_CONDITION.into(),
        catalog_field_id: desc.id.clone(),
        write_verb: FEATURE_REGISTRY_WRITE_VERB.into(),
    })
}

/// Desk Catalog / `search_catalog` / `describe_domain` schema fragment.
pub fn emit_agent_schema(desc: &FeatureDescriptor) -> Result<AgentSchemaArtifact, CodegenError> {
    Ok(AgentSchemaArtifact {
        field: derived_field_descriptor(desc)?,
        search_catalog: true,
        describe_domain: true,
    })
}

/// Emit all five artifacts from one Derived Feature descriptor.
pub fn emit_all(desc: &FeatureDescriptor) -> Result<CodegenArtifacts, CodegenError> {
    Ok(CodegenArtifacts {
        id: desc.id.clone(),
        runtime_field: emit_runtime_field(desc)?,
        storage_column: emit_storage_column(desc)?,
        query_dimension: emit_query_dimension(desc)?,
        rule_binding: emit_rule_binding(desc)?,
        agent_schema: emit_agent_schema(desc)?,
    })
}

/// Merge generated fields for **accepted** Derived Features into the catalog waist.
///
/// Candidate / shadow descriptors stay discoverable via `featureHits` but do not
/// grow `get_state` / query / rules write sites until `active`.
pub fn apply_codegen_fields(catalog: &mut DeskCatalog) {
    let derived = catalog.derived_features.clone();
    for desc in derived.iter().filter(|d| is_accepted_derived(d)) {
        let Ok(artifacts) = emit_all(desc) else {
            continue;
        };
        let field = artifacts.runtime_field.field;
        if !catalog.fields.iter().any(|f| f.id == field.id) {
            catalog.fields.push(field.clone());
        }
        if let Some(domain) = catalog.domains.iter_mut().find(|d| d.id == desc.domain_id) {
            if !domain.field_ids.contains(&field.id) {
                domain.field_ids.push(field.id.clone());
            }
            domain.field_ids.sort();
        }
    }
    catalog.fields.sort_by(|a, b| a.id.cmp(&b.id));
}

/// Stamp generated scalars into a Journal Frame / snapshot payload object.
///
/// Unavailable evaluations omit the key (fail closed — never invent a value).
pub fn stamp_derived_feature_payload(
    payload: &mut Value,
    store: FeatureIrStore<'_>,
    as_of_ms: f64,
    path: FeatureIrEvalPath,
) {
    let Some(obj) = payload.as_object_mut() else {
        return;
    };
    let accepted: Vec<&FeatureDescriptor> = store
        .catalog
        .derived_features
        .iter()
        .filter(|d| is_accepted_derived(d))
        .collect();
    if accepted.is_empty() {
        return;
    }
    let mut derived = obj
        .get(DERIVED_FEATURE_PAYLOAD_OBJECT)
        .cloned()
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let Some(derived_obj) = derived.as_object_mut() else {
        return;
    };
    for desc in accepted {
        let Some(program) = desc.program.as_ref() else {
            continue;
        };
        let Ok(_column) = emit_storage_column(desc) else {
            continue;
        };
        let Ok(value) = evaluate(program, store, as_of_ms, path) else {
            continue;
        };
        if value.available && value.value.is_finite() {
            derived_obj.insert(desc.id.clone(), Value::from(value.value));
            obj.insert(desc.id.clone(), Value::from(value.value));
        }
    }
    if let Some(map) = derived.as_object() {
        if !map.is_empty() {
            obj.insert(DERIVED_FEATURE_PAYLOAD_OBJECT.into(), derived);
        }
    }
}

/// Read a stamped Derived Feature scalar from a Journal Frame payload.
pub fn read_storage_value(payload: &Value, feature_id: &str) -> Option<f64> {
    payload
        .get(DERIVED_FEATURE_PAYLOAD_OBJECT)
        .and_then(|obj| obj.get(feature_id))
        .and_then(json_f64)
        .or_else(|| payload.get(feature_id).and_then(json_f64))
}

/// Stamped scalars for accepted Derived Features (rules-engine catalog binding).
pub fn read_catalog_field_values(catalog: &DeskCatalog, payload: &Value) -> HashMap<String, f64> {
    catalog
        .derived_features
        .iter()
        .filter(|desc| is_accepted_derived(desc))
        .filter_map(|desc| {
            read_storage_value(payload, &desc.id).map(|value| (desc.id.clone(), value))
        })
        .collect()
}

/// Stamp `payload` then read accepted Derived Feature scalars for the rules engine.
pub fn catalog_field_values_from_eval(
    catalog: &DeskCatalog,
    mut payload: Value,
    frames: &[FeatureIrFrame],
    truncated: bool,
    eval_root: &str,
    as_of_ms: f64,
    path: FeatureIrEvalPath,
) -> HashMap<String, f64> {
    if !catalog.derived_features.iter().any(is_accepted_derived) {
        return HashMap::new();
    }
    stamp_derived_feature_payload(
        &mut payload,
        FeatureIrStore {
            catalog,
            frames,
            events: &[],
            eval_root,
            window_truncated: truncated,
        },
        as_of_ms,
        path,
    );
    read_catalog_field_values(catalog, &payload)
}

/// Evaluate one accepted Derived Feature at `as_of` (same evaluator, path is a label).
pub fn evaluate_accepted_feature(
    desc: &FeatureDescriptor,
    store: FeatureIrStore<'_>,
    as_of_ms: f64,
    path: FeatureIrEvalPath,
) -> Result<Option<f64>, CodegenError> {
    if !is_accepted_derived(desc) {
        return Ok(None);
    }
    let program = desc
        .program
        .as_ref()
        .ok_or_else(|| CodegenError::MissingProgram(desc.id.clone()))?;
    let value = evaluate(program, store, as_of_ms, path)?;
    if value.available && value.value.is_finite() {
        Ok(Some(value.value))
    } else {
        Ok(None)
    }
}

fn derived_field_descriptor(desc: &FeatureDescriptor) -> Result<FieldDescriptor, CodegenError> {
    require_derived(desc)?;
    Ok(FieldDescriptor {
        id: desc.id.clone(),
        name: desc.id.clone(),
        domain_id: desc.domain_id.clone(),
        description: desc.description.clone(),
        rust_field: "feature_ir".into(),
        unit: desc.schema.unit,
        session_scope: desc.schema.session_scope,
        freshness: desc.schema.freshness,
        cost_hint: desc.schema.cost_hint,
    })
}

fn require_derived(desc: &FeatureDescriptor) -> Result<(), CodegenError> {
    if desc.kind != FeatureKind::DerivedFeature || !desc.id.starts_with("feature.") {
        return Err(CodegenError::NotDerivedFeature);
    }
    if desc.program.is_none() {
        return Err(CodegenError::MissingProgram(desc.id.clone()));
    }
    Ok(())
}

fn json_f64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_i64().map(|i| i as f64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::feature_ir::{FeatureIrFrame, FeatureIrProgram};
    use crate::catalog::feature_registry::{
        validate_derived_feature, DerivedFeatureRegistration, DetectorSchema, FeatureRegistry,
        HumanGate,
    };
    use crate::catalog::types::{CostHint, FreshnessSemantics, SessionScope, Unit};
    use crate::catalog::{build_catalog, build_catalog_with_overlay};
    use serde_json::json;

    fn last_price_percentile() -> FeatureDescriptor {
        let catalog = build_catalog();
        let input = DerivedFeatureRegistration {
            id: "feature.session_last_price_percentile".into(),
            name: "session_last_price_percentile".into(),
            description: "Session-distribution percentiles of lastPrice (Feature-IR).".into(),
            domain_id: "location_structure".into(),
            schema: DetectorSchema {
                catalog_field_ids: vec!["market.location_structure.lastPrice".into()],
                event_types: vec![],
                unit: Unit::Percent,
                session_scope: SessionScope::Session,
                freshness: FreshnessSemantics::LiveTickAnchored,
                cost_hint: CostHint::R1,
            },
            program: FeatureIrProgram::parse_value(&json!({
                "family": "sessionDistributionPercentiles",
                "field": "market.location_structure.lastPrice"
            }))
            .expect("program"),
        };
        let mut desc = validate_derived_feature(input, &catalog).expect("validate");
        desc.promotion_state = PromotionState::Active;
        desc.last_gate_note = Some("Your rules say this descriptor may be active.".into());
        desc
    }

    #[test]
    fn five_emitters_are_named_and_present() {
        let src = include_str!("codegen.rs");
        for name in [
            EMIT_RUNTIME_FIELD,
            EMIT_STORAGE_COLUMN,
            EMIT_QUERY_DIMENSION,
            EMIT_RULE_BINDING,
            EMIT_AGENT_SCHEMA,
        ] {
            assert!(
                src.contains(&format!("pub fn {name}")),
                "{name} must be a public emitter"
            );
        }
    }

    #[test]
    fn one_accepted_descriptor_emits_all_five_artifacts() {
        let desc = last_price_percentile();
        let artifacts = emit_all(&desc).expect("emit");
        assert_eq!(artifacts.id, "feature.session_last_price_percentile");
        assert_eq!(
            artifacts.runtime_field.field.id,
            "feature.session_last_price_percentile"
        );
        assert_eq!(artifacts.runtime_field.served_by, "get_state");
        assert_eq!(
            artifacts.storage_column.payload_key,
            "derivedFeatures.feature.session_last_price_percentile"
        );
        assert_eq!(artifacts.storage_column.table, JOURNAL_FRAME_STORAGE_TARGET);
        assert_eq!(
            artifacts.query_dimension.field_id,
            "feature.session_last_price_percentile"
        );
        assert_eq!(
            artifacts.query_dimension.operators,
            vec!["query_series".to_string(), "query_episodes".to_string()]
        );
        assert_eq!(artifacts.rule_binding.condition_field, "catalog_field");
        assert_eq!(
            artifacts.rule_binding.catalog_field_id,
            "feature.session_last_price_percentile"
        );
        assert!(artifacts.agent_schema.search_catalog);
        assert!(artifacts.agent_schema.describe_domain);
        assert_eq!(
            artifacts.runtime_field.field.id,
            artifacts.agent_schema.field.id
        );
    }

    #[test]
    fn static_catalog_has_no_handwritten_feature_fields() {
        let cat = build_catalog();
        assert!(
            cat.fields.iter().all(|f| !f.id.starts_with("feature.")),
            "feature.* write sites must come from codegen, not build_catalog()"
        );
        assert!(cat.derived_features.is_empty());
    }

    #[test]
    fn candidate_does_not_grow_catalog_fields_until_active() {
        let catalog = build_catalog();
        let mut registry = FeatureRegistry::builtins();
        let desc = last_price_percentile();
        let input = DerivedFeatureRegistration {
            id: desc.id.clone(),
            name: desc.name.clone(),
            description: desc.description.clone(),
            domain_id: desc.domain_id.clone(),
            schema: desc.schema.clone(),
            program: desc.program.clone().expect("program"),
        };
        let candidate = registry
            .register_derived(input, &catalog)
            .expect("register");
        assert_eq!(candidate.promotion_state, PromotionState::Candidate);
        let overlaid = build_catalog_with_overlay(vec![candidate]);
        assert!(overlaid
            .fields
            .iter()
            .all(|f| f.id != "feature.session_last_price_percentile"));

        let mut registry = FeatureRegistry::with_overlay(overlaid.derived_features.clone());
        let gate = HumanGate {
            trader_confirmation: "Your playbook indicates shadow is allowed.",
        };
        registry
            .promote(
                "feature.session_last_price_percentile",
                PromotionState::Shadow,
                gate,
            )
            .expect("shadow");
        let shadow_cat = build_catalog_with_overlay(registry.derived_features());
        assert!(shadow_cat
            .fields
            .iter()
            .all(|f| f.id != "feature.session_last_price_percentile"));

        let gate = HumanGate {
            trader_confirmation: "Your rules say this descriptor may be active.",
        };
        let active = registry
            .promote(
                "feature.session_last_price_percentile",
                PromotionState::Active,
                gate,
            )
            .expect("active");
        let active_cat = build_catalog_with_overlay(vec![active]);
        assert!(active_cat
            .fields
            .iter()
            .any(|f| f.id == "feature.session_last_price_percentile"));
        let location = active_cat
            .domains
            .iter()
            .find(|d| d.id == "location_structure")
            .expect("domain");
        assert!(location
            .field_ids
            .iter()
            .any(|id| id == "feature.session_last_price_percentile"));
    }

    #[test]
    fn stamp_and_read_round_trip_session_percentile() {
        let desc = last_price_percentile();
        let mut catalog = build_catalog_with_overlay(vec![desc.clone()]);
        apply_codegen_fields(&mut catalog);
        let t0 = 1_700_000_000_000.0;
        let frames: Vec<FeatureIrFrame> = (0..5)
            .map(|i| FeatureIrFrame {
                clock_ms: t0 + i as f64 * 1000.0,
                frame_second: ((t0 + i as f64 * 1000.0) / 1000.0).floor() as i64,
                root_symbol: "NQ".into(),
                session_type: "RTH".into(),
                trading_day: "2026-03-03".into(),
                payload: json!({ "lastPrice": 21_000.0 + i as f64 }),
            })
            .collect();
        let mut payload = frames[4].payload.clone();
        let store = FeatureIrStore {
            catalog: &catalog,
            frames: &frames,
            events: &[],
            eval_root: "NQ",
            window_truncated: false,
        };
        stamp_derived_feature_payload(
            &mut payload,
            store,
            t0 + 4000.0,
            FeatureIrEvalPath::Historical,
        );
        let stored = read_storage_value(&payload, &desc.id).expect("stamped");
        let evaluated =
            evaluate_accepted_feature(&desc, store, t0 + 4000.0, FeatureIrEvalPath::LiveShadow)
                .expect("eval")
                .expect("available");
        assert!((stored - evaluated).abs() < 1e-9);
        assert!((stored - 90.0).abs() < 1e-9);
    }

    #[test]
    fn base_detector_cannot_emit() {
        let pinch = FeatureRegistry::builtins()
            .get("detector.pinch")
            .cloned()
            .expect("pinch");
        assert!(matches!(
            emit_runtime_field(&pinch),
            Err(CodegenError::NotDerivedFeature)
        ));
    }

    #[test]
    fn handwritten_feature_ids_are_not_in_kernel_write_sites() {
        for (rel, src) in [
            ("envelope.rs", include_str!("envelope.rs")),
            (
                "market_state_fields.rs",
                include_str!("market_state_fields.rs"),
            ),
            (
                "query_kernel.rs",
                include_str!("../research/query_kernel.rs"),
            ),
            ("rules/mod.rs", include_str!("../rules/mod.rs")),
        ] {
            assert!(
                !src.contains("feature.session_last_price_percentile"),
                "{rel} must not hardcode a Derived Feature write site"
            );
        }
        let rules = include_str!("../rules/mod.rs");
        assert!(
            rules.contains("CatalogField"),
            "rules engine binds codegen fields via CatalogField, not a specialty variant"
        );
        assert!(
            !rules.contains("SessionLastPricePercentile"),
            "do not add a per-feature ConditionField variant by hand"
        );
    }
}
