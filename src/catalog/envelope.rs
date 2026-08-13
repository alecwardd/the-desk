//! StateEnvelope builder for the SIL read kernel (`get_state`).
//!
//! Provenance is mandatory per domain in every envelope — absence is a failure.
//! Degraded domains set `degraded[domain]=true` and still appear in provenance;
//! stale/failed sources are never silently omitted.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::types::{CostHint, DeskCatalog, FieldDescriptor, TrustCeiling, CATALOG_VERSION};

/// Pull-band resolution accepted by `get_state` (R0 orientation / R1 state).
///
/// R2 evidence and R3 raw stay on later query operators (`query_series` /
/// `query_episodes` / `query_raw`) — never widen the state read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum StateResolution {
    R0,
    R1,
}

impl StateResolution {
    /// Parse wire labels `R0` / `R1` (case-insensitive). Rejects R2/R3.
    pub fn parse(raw: &str) -> Result<Self, EnvelopeError> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "R0" => Ok(Self::R0),
            "R1" => Ok(Self::R1),
            "R2" | "R3" => Err(EnvelopeError::ResolutionOutOfBand(raw.to_string())),
            other => Err(EnvelopeError::InvalidResolution(other.to_string())),
        }
    }

    pub(crate) fn allows(self, cost: CostHint) -> bool {
        match self {
            Self::R0 => matches!(cost, CostHint::R0),
            Self::R1 => matches!(cost, CostHint::R0 | CostHint::R1),
        }
    }
}

/// Provenance source for a domain slice inside a StateEnvelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProvenanceSource {
    /// Live pipeline / in-memory MarketState.
    Live,
    /// Historical Journal Frame / persisted feature snapshot (`as_of` path).
    Journal,
    /// External provider record (e.g. later Vs3dProvider) with optional vendor stamp.
    Provider,
    /// Manual / as-of Positioning path (Levels-Only Record). Never live vendor data.
    Manual,
}

/// Per-domain provenance — required for every domain present in an envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomainProvenance {
    pub source: ProvenanceSource,
    /// Epoch-ms data time when known; `null` when the source failed/unavailable
    /// (still present — never silently omitted).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_time: Option<f64>,
    /// Vendor stamp when source is [`ProvenanceSource::Provider`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vendor: Option<String>,
    /// Optional human-readable degradation detail (never advisory coaching).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Provenance-carrying state read returned by `get_state`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateEnvelope {
    pub values: BTreeMap<String, Value>,
    pub provenance: BTreeMap<String, DomainProvenance>,
    pub degraded: BTreeMap<String, bool>,
    pub catalog_version: String,
    pub trust_ceiling: TrustCeiling,
    /// Trust Level this operator runs at (read/query kernel = L0).
    pub trust_level: TrustLevel,
    pub resolution: StateResolution,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbols: Option<Vec<String>>,
    /// Aligned MarketRouter clock (max applied market timestamp) when multi-symbol.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clock_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub as_of: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
}

/// Trust Level capability tag for kernel / tool surface (ADR-022 ceiling stays L3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrustLevel {
    /// Read / query — structurally incapable of mutation or order authority.
    L0,
}

/// Inputs for building a StateEnvelope from a market snapshot JSON blob.
#[derive(Debug, Clone)]
pub struct StateReadRequest<'a> {
    pub symbols: Option<Vec<String>>,
    pub domains: Option<Vec<String>>,
    pub fields: Option<Vec<String>>,
    pub resolution: StateResolution,
    pub as_of: Option<f64>,
    pub budget_tokens: Option<u64>,
    /// Snapshot JSON (`MarketState` camelCase) when available.
    pub snapshot: Option<&'a Value>,
    pub snapshot_source: ProvenanceSource,
    pub data_time: Option<f64>,
    /// When true, every market domain from the live/journal source is degraded.
    pub source_degraded: bool,
    pub source_degraded_note: Option<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EnvelopeError {
    #[error("get_state resolution must be R0 or R1; got `{0}` (R2/R3 use query operators)")]
    ResolutionOutOfBand(String),
    #[error("invalid get_state resolution `{0}` (expected R0 or R1)")]
    InvalidResolution(String),
    #[error("StateEnvelope missing provenance for domain `{0}`")]
    MissingProvenance(String),
    #[error("unknown catalog domain `{0}`")]
    UnknownDomain(String),
}

/// Build a StateEnvelope. Every domain in the response carries provenance;
/// missing provenance is returned as [`EnvelopeError::MissingProvenance`].
pub fn build_state_envelope(
    catalog: &DeskCatalog,
    req: StateReadRequest<'_>,
) -> Result<StateEnvelope, EnvelopeError> {
    let selected_domains = select_domains(catalog, req.domains.as_ref())?;
    let field_filter: Option<std::collections::BTreeSet<&str>> = req
        .fields
        .as_ref()
        .map(|f| f.iter().map(|s| s.as_str()).collect());

    let mut values: BTreeMap<String, Value> = BTreeMap::new();
    let mut provenance: BTreeMap<String, DomainProvenance> = BTreeMap::new();
    let mut degraded: BTreeMap<String, bool> = BTreeMap::new();

    for domain_id in &selected_domains {
        let (prov, is_degraded) = domain_provenance(catalog, domain_id, &req);
        provenance.insert(domain_id.clone(), prov);
        degraded.insert(domain_id.clone(), is_degraded);

        if is_degraded && req.snapshot.is_none() && domain_id.as_str() != "meta" {
            // Still include domain in envelope; values may be empty.
            continue;
        }

        for field in catalog.fields.iter().filter(|f| f.domain_id == *domain_id) {
            if !req.resolution.allows(field.cost_hint) {
                continue;
            }
            if let Some(ref allow) = field_filter {
                if !allow.contains(field.id.as_str()) && !allow.contains(field.name.as_str()) {
                    continue;
                }
            }
            if let Some(val) = extract_field_value(req.snapshot, field) {
                values.insert(field.id.clone(), val);
            }
        }
    }

    // Meta always carries catalog pin even when empty of market fields.
    if selected_domains.iter().any(|d| d == "meta") {
        values
            .entry("meta.catalogVersion".into())
            .or_insert_with(|| Value::String(catalog.catalog_version.clone()));
    }

    let mut envelope = StateEnvelope {
        values,
        provenance,
        degraded,
        catalog_version: catalog.catalog_version.clone(),
        trust_ceiling: catalog.trust_ceiling,
        trust_level: TrustLevel::L0,
        resolution: req.resolution,
        symbols: req.symbols,
        clock_ms: None,
        as_of: req.as_of,
        budget_tokens: req.budget_tokens,
        truncated: None,
    };

    if let Some(budget) = req.budget_tokens {
        apply_token_budget(&mut envelope, budget);
    }

    validate_provenance_complete(&envelope)?;
    Ok(envelope)
}

fn select_domains(
    catalog: &DeskCatalog,
    requested: Option<&Vec<String>>,
) -> Result<Vec<String>, EnvelopeError> {
    match requested {
        None => Ok(catalog.domains.iter().map(|d| d.id.clone()).collect()),
        Some(list) if list.is_empty() => Ok(catalog.domains.iter().map(|d| d.id.clone()).collect()),
        Some(list) => {
            let known: std::collections::BTreeSet<_> =
                catalog.domains.iter().map(|d| d.id.as_str()).collect();
            let mut out = Vec::new();
            for id in list {
                let id = id.trim();
                if id.is_empty() {
                    continue;
                }
                if !known.contains(id) {
                    return Err(EnvelopeError::UnknownDomain(id.to_string()));
                }
                out.push(id.to_string());
            }
            if out.is_empty() {
                Ok(catalog.domains.iter().map(|d| d.id.clone()).collect())
            } else {
                out.sort();
                out.dedup();
                Ok(out)
            }
        }
    }
}

fn domain_provenance(
    catalog: &DeskCatalog,
    domain_id: &str,
    req: &StateReadRequest<'_>,
) -> (DomainProvenance, bool) {
    if domain_id == "positioning" {
        // Fail closed until `apply_positioning_slice` overlays a durable record.
        // Manual/as-of — never presented as live vendor / Provider data.
        let _ = catalog;
        return (
            DomainProvenance {
                source: ProvenanceSource::Manual,
                data_time: None,
                vendor: None,
                note: Some(
                    "No Positioning record. Levels-Only Record path is first-class — write via \
                     positioning_entry (manual/as-of). Not live vendor data."
                        .into(),
                ),
            },
            true,
        );
    }

    if domain_id == "events" {
        // Event taxonomy is reserved; identity for get_events lands separately.
        return (
            DomainProvenance {
                source: req.snapshot_source,
                data_time: req.data_time,
                vendor: None,
                note: Some(
                    "events domain descriptors reserved; use get_events for event rows".into(),
                ),
            },
            true,
        );
    }

    if domain_id == "meta" {
        return (
            DomainProvenance {
                source: ProvenanceSource::Live,
                data_time: req.data_time,
                vendor: None,
                note: Some(format!("catalogVersion={CATALOG_VERSION}")),
            },
            false,
        );
    }

    let mut degraded = req.source_degraded || req.snapshot.is_none();
    let mut note = req.source_degraded_note.clone();
    if req.snapshot.is_none() && note.is_none() {
        degraded = true;
        note = Some("market snapshot unavailable".into());
    }
    (
        DomainProvenance {
            source: req.snapshot_source,
            data_time: req.data_time,
            vendor: None,
            note,
        },
        degraded,
    )
}

fn extract_field_value(snapshot: Option<&Value>, field: &FieldDescriptor) -> Option<Value> {
    let snap = snapshot?;
    // MarketState serializes camelCase via `name` (catalog field name).
    snap.get(&field.name).cloned().or_else(|| {
        // Also accept catalog id tail after last '.' for resilient callers.
        field
            .id
            .rsplit('.')
            .next()
            .and_then(|tail| snap.get(tail).cloned())
    })
}

fn apply_token_budget(envelope: &mut StateEnvelope, budget_tokens: u64) {
    // Approximate tokens as ceil(bytes/4), matching tool telemetry.
    let approx = |v: &StateEnvelope| -> u64 {
        let bytes = serde_json::to_vec(v).map(|b| b.len()).unwrap_or(0) as u64;
        bytes.div_ceil(4)
    };
    if approx(envelope) <= budget_tokens {
        envelope.truncated = Some(false);
        return;
    }
    // Drop highest-cost (R1) values first, then trim alphabetically.
    // Multi-symbol envelopes prefix `{ROOT}.…`; drop non-primary (ES) before NQ
    // so the coaching-path primary is retained under budget pressure.
    let mut ids: Vec<String> = envelope.values.keys().cloned().collect();
    ids.sort_by(|a, b| {
        let a_nq = a.starts_with("NQ.");
        let b_nq = b.starts_with("NQ.");
        match (a_nq, b_nq) {
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
            _ => b.cmp(a),
        }
    });
    for id in ids {
        if approx(envelope) <= budget_tokens {
            break;
        }
        envelope.values.remove(&id);
    }
    envelope.truncated = Some(true);
}

fn validate_provenance_complete(envelope: &StateEnvelope) -> Result<(), EnvelopeError> {
    for domain in envelope.degraded.keys() {
        if !envelope.provenance.contains_key(domain) {
            return Err(EnvelopeError::MissingProvenance(domain.clone()));
        }
    }
    for domain in envelope.provenance.keys() {
        if !envelope.degraded.contains_key(domain) {
            // degraded map must cover every provenance domain (false is fine).
            return Err(EnvelopeError::MissingProvenance(format!(
                "{domain} (degraded flag absent)"
            )));
        }
    }
    if envelope.provenance.is_empty() {
        return Err(EnvelopeError::MissingProvenance("<empty>".into()));
    }
    Ok(())
}

/// Serialize a StateEnvelope to JSON, failing closed if provenance is incomplete.
pub fn state_envelope_json(envelope: &StateEnvelope) -> Result<Value, EnvelopeError> {
    validate_provenance_complete(envelope)?;
    Ok(serde_json::to_value(envelope).unwrap_or(Value::Null))
}

/// Domains whose values are per-symbol (MarketRouter lanes).
fn is_symbol_scoped_domain(domain_id: &str) -> bool {
    !matches!(domain_id, "positioning" | "events" | "meta")
}

/// Catalog field id → domain id (`market.location_structure.lastPrice` → `location_structure`).
fn field_domain_id(field_id: &str) -> Option<&str> {
    if let Some(rest) = field_id.strip_prefix("market.") {
        return rest.split('.').next();
    }
    field_id.split('.').next()
}

fn field_is_symbol_scoped(field_id: &str) -> bool {
    field_domain_id(field_id).is_none_or(is_symbol_scoped_domain)
}

/// Merge per-root StateEnvelopes into one multi-symbol envelope.
///
/// Values and symbol-scoped provenance/degraded keys are prefixed
/// `{ROOT}.{catalogFieldId}` / `{ROOT}.{domain}` so NQ and ES coexist.
/// Positioning / events / meta stay unprefixed (not symbol-scoped).
/// Trust Level remains L0; Trust Ceiling remains L3.
pub fn merge_symbol_envelopes(
    by_root: BTreeMap<String, StateEnvelope>,
    clock_ms: Option<f64>,
) -> Result<StateEnvelope, EnvelopeError> {
    if by_root.len() <= 1 {
        let mut env = by_root
            .into_iter()
            .next()
            .map(|(_, e)| e)
            .ok_or_else(|| EnvelopeError::MissingProvenance("<empty>".into()))?;
        env.clock_ms = clock_ms;
        validate_provenance_complete(&env)?;
        return Ok(env);
    }

    let roots: Vec<String> = by_root.keys().cloned().collect();
    let mut values = BTreeMap::new();
    let mut provenance = BTreeMap::new();
    let mut degraded = BTreeMap::new();
    let mut catalog_version = String::new();
    let mut trust_ceiling = TrustCeiling::L3;
    let mut resolution = StateResolution::R0;
    let mut as_of = None;
    let mut budget_tokens = None;
    let mut truncated = None;

    for (root, env) in &by_root {
        catalog_version = env.catalog_version.clone();
        trust_ceiling = env.trust_ceiling;
        resolution = env.resolution;
        as_of = env.as_of.or(as_of);
        budget_tokens = env.budget_tokens.or(budget_tokens);
        if env.truncated == Some(true) {
            truncated = Some(true);
        }
        for (field_id, value) in &env.values {
            let key = if field_is_symbol_scoped(field_id) {
                format!("{root}.{field_id}")
            } else {
                field_id.clone()
            };
            values.entry(key).or_insert_with(|| value.clone());
        }
        for (domain, prov) in &env.provenance {
            if is_symbol_scoped_domain(domain) {
                provenance.insert(format!("{root}.{domain}"), prov.clone());
            } else {
                provenance
                    .entry(domain.clone())
                    .or_insert_with(|| prov.clone());
            }
        }
        for (domain, flag) in &env.degraded {
            if is_symbol_scoped_domain(domain) {
                degraded.insert(format!("{root}.{domain}"), *flag);
            } else {
                let entry = degraded.entry(domain.clone()).or_insert(false);
                *entry = *entry || *flag;
            }
        }
    }

    if truncated.is_none() && by_root.values().any(|e| e.truncated == Some(false)) {
        truncated = Some(false);
    }

    let mut envelope = StateEnvelope {
        values,
        provenance,
        degraded,
        catalog_version,
        trust_ceiling,
        trust_level: TrustLevel::L0,
        resolution,
        symbols: Some(roots),
        clock_ms,
        as_of,
        budget_tokens,
        truncated,
    };
    if let Some(budget) = budget_tokens {
        apply_token_budget(&mut envelope, budget);
    }
    validate_provenance_complete(&envelope)?;
    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::build_catalog;

    fn sample_snapshot() -> Value {
        serde_json::json!({
            "lastPrice": 21000.25,
            "vwap": 20990.0,
            "poc": 20995.0,
            "sessionType": "RTH",
            "tradingDay": "2026-08-11",
            "rootSymbol": "NQ",
            "contractSymbol": "NQU26.CME",
            "sessionDelta": 120.0,
        })
    }

    #[test]
    fn provenance_required_for_every_domain() {
        let catalog = build_catalog();
        let snap = sample_snapshot();
        let env = build_state_envelope(
            &catalog,
            StateReadRequest {
                symbols: Some(vec!["NQ".into()]),
                domains: None,
                fields: None,
                resolution: StateResolution::R0,
                as_of: None,
                budget_tokens: None,
                snapshot: Some(&snap),
                snapshot_source: ProvenanceSource::Live,
                data_time: Some(1_700_000_000_000.0),
                source_degraded: false,
                source_degraded_note: None,
            },
        )
        .expect("envelope");
        assert!(!env.provenance.is_empty());
        for domain in &catalog.domains {
            assert!(
                env.provenance.contains_key(&domain.id),
                "missing provenance for {}",
                domain.id
            );
            assert!(
                env.degraded.contains_key(&domain.id),
                "missing degraded flag for {}",
                domain.id
            );
        }
        assert_eq!(env.trust_level, TrustLevel::L0);
        assert_eq!(env.trust_ceiling, TrustCeiling::L3);
        assert_eq!(env.catalog_version, CATALOG_VERSION);
    }

    #[test]
    fn degraded_domain_returns_flag_not_whole_call_failure() {
        let catalog = build_catalog();
        let env = build_state_envelope(
            &catalog,
            StateReadRequest {
                symbols: None,
                domains: Some(vec!["positioning".into(), "location_structure".into()]),
                fields: None,
                resolution: StateResolution::R1,
                as_of: None,
                budget_tokens: None,
                snapshot: None,
                snapshot_source: ProvenanceSource::Live,
                data_time: None,
                source_degraded: true,
                source_degraded_note: Some("pipeline_lock_contended".into()),
            },
        )
        .expect("partial envelope must succeed");
        assert_eq!(env.degraded.get("positioning"), Some(&true));
        assert_eq!(env.degraded.get("location_structure"), Some(&true));
        assert!(env.provenance.contains_key("positioning"));
        assert!(env.provenance.contains_key("location_structure"));
        // Stale/failed source must not be omitted from provenance.
        assert!(env.provenance["positioning"].data_time.is_none());
        assert_eq!(
            env.provenance["positioning"].source,
            ProvenanceSource::Manual
        );
        assert!(env.provenance["positioning"].vendor.is_none());
    }

    #[test]
    fn missing_provenance_is_a_failure() {
        let mut env = StateEnvelope {
            values: BTreeMap::new(),
            provenance: BTreeMap::new(),
            degraded: BTreeMap::from([("flow".into(), false)]),
            catalog_version: CATALOG_VERSION.into(),
            trust_ceiling: TrustCeiling::L3,
            trust_level: TrustLevel::L0,
            resolution: StateResolution::R0,
            symbols: None,
            clock_ms: None,
            as_of: None,
            budget_tokens: None,
            truncated: None,
        };
        assert!(matches!(
            validate_provenance_complete(&env),
            Err(EnvelopeError::MissingProvenance(_))
        ));
        env.provenance.insert(
            "flow".into(),
            DomainProvenance {
                source: ProvenanceSource::Live,
                data_time: Some(1.0),
                vendor: None,
                note: None,
            },
        );
        assert!(validate_provenance_complete(&env).is_ok());
    }

    #[test]
    fn rejects_r2_r3_resolution() {
        assert!(matches!(
            StateResolution::parse("R2"),
            Err(EnvelopeError::ResolutionOutOfBand(_))
        ));
        assert!(matches!(
            StateResolution::parse("R3"),
            Err(EnvelopeError::ResolutionOutOfBand(_))
        ));
        assert_eq!(StateResolution::parse("r0").unwrap(), StateResolution::R0);
    }

    #[test]
    fn r0_excludes_r1_cost_fields() {
        let catalog = build_catalog();
        let snap = sample_snapshot();
        // vwap1SdUpper is R1 in catalog — must not appear in R0.
        let env = build_state_envelope(
            &catalog,
            StateReadRequest {
                symbols: None,
                domains: Some(vec!["location_structure".into()]),
                fields: None,
                resolution: StateResolution::R0,
                as_of: None,
                budget_tokens: None,
                snapshot: Some(&snap),
                snapshot_source: ProvenanceSource::Live,
                data_time: Some(1.0),
                source_degraded: false,
                source_degraded_note: None,
            },
        )
        .unwrap();
        assert!(env
            .values
            .contains_key("market.location_structure.lastPrice"));
        assert!(!env
            .values
            .keys()
            .any(|k| k.contains("vwap1Sd") || k.contains("Vwap1Sd")));
    }

    #[test]
    fn journal_as_of_uses_journal_provenance() {
        let catalog = build_catalog();
        let snap = sample_snapshot();
        let env = build_state_envelope(
            &catalog,
            StateReadRequest {
                symbols: None,
                domains: Some(vec!["identity".into()]),
                fields: None,
                resolution: StateResolution::R0,
                as_of: Some(1_700_000_000_000.0),
                budget_tokens: None,
                snapshot: Some(&snap),
                snapshot_source: ProvenanceSource::Journal,
                data_time: Some(1_700_000_000_000.0),
                source_degraded: false,
                source_degraded_note: None,
            },
        )
        .unwrap();
        assert_eq!(env.provenance["identity"].source, ProvenanceSource::Journal);
        assert_eq!(env.as_of, Some(1_700_000_000_000.0));
    }

    #[test]
    fn multi_symbol_envelope_prefixes_values_and_keeps_l0() {
        let catalog = build_catalog();
        let nq = serde_json::json!({
            "lastPrice": 21000.25,
            "rootSymbol": "NQ",
            "sessionType": "RTH",
        });
        let es = serde_json::json!({
            "lastPrice": 5000.0,
            "rootSymbol": "ES",
            "sessionType": "Globex",
        });
        let nq_env = build_state_envelope(
            &catalog,
            StateReadRequest {
                symbols: Some(vec!["NQ".into()]),
                domains: Some(vec!["identity".into(), "location_structure".into()]),
                fields: None,
                resolution: StateResolution::R0,
                as_of: None,
                budget_tokens: None,
                snapshot: Some(&nq),
                snapshot_source: ProvenanceSource::Live,
                data_time: Some(1.0),
                source_degraded: false,
                source_degraded_note: None,
            },
        )
        .unwrap();
        let es_env = build_state_envelope(
            &catalog,
            StateReadRequest {
                symbols: Some(vec!["ES".into()]),
                domains: Some(vec!["identity".into(), "location_structure".into()]),
                fields: None,
                resolution: StateResolution::R0,
                as_of: None,
                budget_tokens: None,
                snapshot: Some(&es),
                snapshot_source: ProvenanceSource::Live,
                data_time: Some(2.0),
                source_degraded: false,
                source_degraded_note: None,
            },
        )
        .unwrap();
        let mut by_root = BTreeMap::new();
        by_root.insert("ES".into(), es_env);
        by_root.insert("NQ".into(), nq_env);
        let merged = merge_symbol_envelopes(by_root, Some(2.0)).unwrap();
        assert_eq!(merged.trust_level, TrustLevel::L0);
        assert_eq!(merged.trust_ceiling, TrustCeiling::L3);
        assert_eq!(merged.clock_ms, Some(2.0));
        assert_eq!(
            merged.symbols.as_deref(),
            Some(&["ES".into(), "NQ".into()][..])
        );
        assert_eq!(
            merged.values.get("NQ.market.location_structure.lastPrice"),
            Some(&serde_json::json!(21000.25))
        );
        assert_eq!(
            merged.values.get("ES.market.location_structure.lastPrice"),
            Some(&serde_json::json!(5000.0))
        );
        assert!(merged.provenance.contains_key("NQ.identity"));
        assert!(merged.provenance.contains_key("ES.identity"));
        assert!(merged.degraded.contains_key("NQ.location_structure"));
        assert!(merged.degraded.contains_key("ES.location_structure"));
    }

    #[test]
    fn multi_symbol_keeps_meta_and_positioning_unprefixed() {
        let catalog = build_catalog();
        let nq = serde_json::json!({
            "lastPrice": 21000.25,
            "rootSymbol": "NQ",
            "sessionType": "RTH",
        });
        let es = serde_json::json!({
            "lastPrice": 5000.0,
            "rootSymbol": "ES",
            "sessionType": "Globex",
        });
        let domains = Some(vec![
            "identity".into(),
            "location_structure".into(),
            "meta".into(),
            "positioning".into(),
        ]);
        let nq_env = build_state_envelope(
            &catalog,
            StateReadRequest {
                symbols: Some(vec!["NQ".into()]),
                domains: domains.clone(),
                fields: None,
                resolution: StateResolution::R0,
                as_of: None,
                budget_tokens: None,
                snapshot: Some(&nq),
                snapshot_source: ProvenanceSource::Live,
                data_time: Some(1.0),
                source_degraded: false,
                source_degraded_note: None,
            },
        )
        .unwrap();
        let es_env = build_state_envelope(
            &catalog,
            StateReadRequest {
                symbols: Some(vec!["ES".into()]),
                domains: domains.clone(),
                fields: None,
                resolution: StateResolution::R0,
                as_of: None,
                budget_tokens: None,
                snapshot: Some(&es),
                snapshot_source: ProvenanceSource::Live,
                data_time: Some(2.0),
                source_degraded: false,
                source_degraded_note: None,
            },
        )
        .unwrap();
        let mut by_root = BTreeMap::new();
        by_root.insert("ES".into(), es_env);
        by_root.insert("NQ".into(), nq_env);
        let merged = merge_symbol_envelopes(by_root, Some(2.0)).unwrap();
        assert!(merged.values.contains_key("meta.catalogVersion"));
        assert!(merged.provenance.contains_key("meta"));
        assert!(merged.provenance.contains_key("positioning"));
        assert!(merged.degraded.contains_key("positioning"));
        assert_eq!(merged.degraded.get("positioning"), Some(&true));
        assert!(!merged
            .values
            .keys()
            .any(|k| k.starts_with("NQ.meta.") || k.starts_with("ES.meta.")));
        assert!(!merged.provenance.contains_key("NQ.meta"));
        assert!(!merged.provenance.contains_key("ES.positioning"));
        assert!(merged.provenance.contains_key("NQ.identity"));
        assert!(merged.provenance.contains_key("ES.identity"));
    }
}
