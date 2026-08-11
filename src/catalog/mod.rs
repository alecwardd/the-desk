//! Desk Catalog v0 — schema waist over annotated runtime state.
//!
//! Field *identity* is bound to `MarketState` (CI fails if a rust field is
//! missing or orphaned). Descriptions are sourced from `MarketState` doc-
//! comments (CI fails if `///` text drifts from the catalog table). Unit,
//! session scope, freshness, and cost hint are curated annotations beside
//! those fields. Discovery operators serve this metadata only — never live
//! market data. See ADR-022 (Trust Ceiling L3) and SIL SPEC M1a (#4).

mod config;
mod market_state_fields;
mod positioning;
mod render;
mod search;
mod types;

pub use config::{load_sil_config, SilConfig};
pub use render::{
    catalog_json_path, catalog_markdown_path, render_catalog_json, render_catalog_markdown,
    write_catalog_docs,
};
pub use search::search_catalog;
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

    DeskCatalog {
        catalog_version: CATALOG_VERSION.to_string(),
        trust_ceiling: TrustCeiling::L3,
        specialty_market_tools: specialty_market_tools_allowlist(),
        domains,
        fields,
        positioning_record_kinds: positioning.record_kinds,
        positioning_provider: None,
    }
}

/// Environment metadata for `describe_environment` (no live values).
pub fn describe_environment(catalog: &DeskCatalog, discovery_enabled: bool) -> serde_json::Value {
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
        },
        "specialtyMarketToolsPolicy": "no_catalog_entry_no_new_market_tool",
        "specialtyMarketToolCount": catalog.specialty_market_tools.len(),
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
            summary: "Dealer/options Positioning domain (schema stub; no live provider in v0).".into(),
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
            summary: "Reserved for event-taxonomy descriptors (populated as event kernel lands).".into(),
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
    fn positioning_domain_stub_names_four_record_kinds() {
        let cat = build_catalog();
        assert!(cat.positioning_provider.is_none());
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
}
