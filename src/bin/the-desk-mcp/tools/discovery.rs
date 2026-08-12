//! SIL discovery + read-kernel operators.
//!
//! Registered on the MCP router only when `[sil].catalog_discovery = true`.
//! Not specialty market tools; do not add these to `tools/market.rs`.
//!
//! Trust Level: all operators here are L0 (read/query) — structurally
//! incapable of mutation or order authority.

use rmcp::{
    handler::server::wrapper::Parameters, model::*, tool, tool_router, ErrorData as McpError,
};
use the_desk_backend::catalog::{
    build_catalog, build_state_envelope, describe_domain, describe_environment,
    kernel_event_from_db_row, search_catalog, state_envelope_json, EventsEnvelope, ProvenanceSource,
    StateReadRequest, StateResolution, TrustLevel, KERNEL_READ_QUERY_TOOLS,
};

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

#[tool_router(router = discovery_router, vis = "pub(crate)")]
impl TheDeskMcp {
    #[tool(
        description = "Describe the Desk Catalog environment: catalogVersion, Trust Ceiling (L3), domain list, Positioning stub status, and specialty-market-tool policy. Returns catalog metadata only — never live market data. Enable via [sil].catalog_discovery in config.toml. Trust Level L0 (read/query)."
    )]
    pub(crate) async fn describe_environment(&self) -> Result<CallToolResult, McpError> {
        let catalog = build_catalog();
        let mut out = describe_environment(&catalog, self.sil_config.catalog_discovery);
        if let Some(obj) = out.as_object_mut() {
            obj.insert("trustLevel".into(), serde_json::json!(TrustLevel::L0));
            obj.insert(
                "mutationAuthority".into(),
                serde_json::json!(false),
            );
            obj.insert("orderAuthority".into(), serde_json::json!(false));
            obj.insert(
                "kernelOperators".into(),
                serde_json::json!(KERNEL_READ_QUERY_TOOLS),
            );
        }
        Ok(text_result(out))
    }

    #[tool(
        description = "Describe one catalog domain by id (identity, location_structure, flow, liquidity, response, volatility, positioning, cross_market, events, meta). Returns field descriptors (unit, session scope, freshness, cost hint) — metadata only, never live market data. Trust Level L0 (read/query)."
    )]
    pub(crate) async fn describe_domain(
        &self,
        Parameters(params): Parameters<DescribeDomainParams>,
    ) -> Result<CallToolResult, McpError> {
        let domain_id = params
            .domain
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                invalid_params_error("describe_domain requires `domain` (catalog domain id)")
            })?;
        let catalog = build_catalog();
        match describe_domain(&catalog, domain_id) {
            Some(mut out) => {
                if let Some(obj) = out.as_object_mut() {
                    obj.insert("trustLevel".into(), serde_json::json!(TrustLevel::L0));
                    obj.insert("mutationAuthority".into(), serde_json::json!(false));
                    obj.insert("orderAuthority".into(), serde_json::json!(false));
                }
                Ok(text_result(out))
            }
            None => Ok(text_result(serde_json::json!({
                "error": "unknown_domain",
                "domain": domain_id,
                "knownDomains": catalog.domains.iter().map(|d| &d.id).collect::<Vec<_>>(),
                "metadataOnly": true,
                "catalogVersion": catalog.catalog_version,
                "trustLevel": TrustLevel::L0,
                "mutationAuthority": false,
                "orderAuthority": false,
            }))),
        }
    }

    #[tool(
        description = "Search the Desk Catalog by text across field ids, names, descriptions, and domains. Returns matching field descriptors with unit, session scope, freshness, and cost hint — metadata only, never live market data. Trust Level L0 (read/query)."
    )]
    pub(crate) async fn search_catalog(
        &self,
        Parameters(params): Parameters<SearchCatalogParams>,
    ) -> Result<CallToolResult, McpError> {
        let query = params.query.unwrap_or_default();
        let catalog = build_catalog();
        let hits = search_catalog(&catalog, &query);
        Ok(text_result(serde_json::json!({
            "catalogVersion": catalog.catalog_version,
            "query": query,
            "hitCount": hits.len(),
            "hits": hits,
            "metadataOnly": true,
            "trustLevel": TrustLevel::L0,
            "mutationAuthority": false,
            "orderAuthority": false,
        })))
    }

    #[tool(
        description = "SIL read kernel: parameterized state read returning a StateEnvelope with per-domain provenance and degraded flags. Params: symbols?, domains?, fields?, resolution (R0|R1 required), as_of?, budget_tokens?. Absence of provenance is a failure; a degraded domain sets its flag rather than failing the whole call. Trust Level L0 — read/query only, no mutation or order authority. Enable via [sil].catalog_discovery."
    )]
    pub(crate) async fn get_state(
        &self,
        Parameters(params): Parameters<GetStateParams>,
    ) -> Result<CallToolResult, McpError> {
        let resolution_raw = params
            .resolution
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                invalid_params_error("get_state requires `resolution` of R0 or R1")
            })?;
        let resolution = StateResolution::parse(resolution_raw)
            .map_err(|e| invalid_params_error(e.to_string()))?;

        let catalog = build_catalog();
        let (snapshot_owned, snapshot_source, data_time, source_degraded, source_note, as_of) =
            self.resolve_state_snapshot(params.as_of).await?;

        let req = StateReadRequest {
            symbols: params.symbols,
            domains: params.domains,
            fields: params.fields,
            resolution,
            as_of,
            budget_tokens: params.budget_tokens,
            snapshot: snapshot_owned.as_ref(),
            snapshot_source,
            data_time,
            source_degraded,
            source_degraded_note: source_note,
        };
        let envelope = build_state_envelope(&catalog, req)
            .map_err(|e| invalid_params_error(e.to_string()))?;
        let mut out = state_envelope_json(&envelope)
            .map_err(|e| McpError::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?;
        if let Some(obj) = out.as_object_mut() {
            obj.insert("mutationAuthority".into(), serde_json::json!(false));
            obj.insert("orderAuthority".into(), serde_json::json!(false));
        }
        Ok(text_result(out))
    }

    #[tool(
        description = "SIL read kernel: recent market events with identity for later lifecycle formalization (type, time, severity placeholder, identityId). Full lifecycle (open/updated/resolved) is a later ticket — this returns identity rows only. Trust Level L0 (read/query). Enable via [sil].catalog_discovery."
    )]
    pub(crate) async fn get_events(
        &self,
        Parameters(params): Parameters<GetEventsParams>,
    ) -> Result<CallToolResult, McpError> {
        let limit = params.limit.unwrap_or(50).clamp(1, 500) as usize;
        let event_type = params
            .event_type
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let since_ms = params.since_ms.filter(|t| t.is_finite() && *t > 0.0);
        let _symbols = params.symbols; // reserved for MarketRouter multi-symbol

        let rows = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.list_recent_market_events(limit, since_ms, event_type)
                .map_err(db_error)?
        };
        let events = rows.iter().map(kernel_event_from_db_row).collect();
        let envelope = EventsEnvelope::from_events(events);
        let mut out = serde_json::to_value(&envelope).unwrap_or_else(|_| serde_json::json!({}));
        if let Some(obj) = out.as_object_mut() {
            obj.insert("mutationAuthority".into(), serde_json::json!(false));
            obj.insert("orderAuthority".into(), serde_json::json!(false));
        }
        Ok(text_result(out))
    }
}

impl TheDeskMcp {
    /// Resolve a market snapshot for `get_state` (live, journal/as_of, or degraded gap).
    async fn resolve_state_snapshot(
        &self,
        as_of: Option<f64>,
    ) -> Result<
        (
            Option<serde_json::Value>,
            ProvenanceSource,
            Option<f64>,
            bool,
            Option<String>,
            Option<f64>,
        ),
        McpError,
    > {
        if let Some(ts) = as_of {
            if !ts.is_finite() || ts <= 0.0 {
                return Err(invalid_params_error(
                    "asOf must be a positive finite epoch-milliseconds value",
                ));
            }
            let db = self.db.lock().map_err(|_| lock_error())?;
            match db.get_snapshot_near(ts).map_err(db_error)? {
                Some((snapshot_ts, payload)) => Ok((
                    Some(payload),
                    ProvenanceSource::Journal,
                    Some(snapshot_ts),
                    false,
                    Some(
                        "as_of served from persisted pipeline snapshot (Journal Frames not yet shipped)"
                            .into(),
                    ),
                    Some(ts),
                )),
                None => Ok((
                    None,
                    ProvenanceSource::Journal,
                    None,
                    true,
                    Some("as_of snapshot unavailable; domains degraded".into()),
                    Some(ts),
                )),
            }
        } else if let Some(r) = self.resolve_live_market_view() {
            let degraded = r.degradation_reason.is_some() || r.snapshot.is_null();
            let note = r.degradation_reason.clone();
            let snap = if r.snapshot.is_null() {
                None
            } else {
                Some(r.snapshot)
            };
            Ok((
                snap,
                ProvenanceSource::Live,
                Some(r.as_of_timestamp_ms),
                degraded,
                note,
                None,
            ))
        } else if let Some(r) = self.resolve_market_snapshot_contention_gap() {
            Ok((
                None,
                ProvenanceSource::Live,
                Some(r.as_of_timestamp_ms),
                true,
                r.degradation_reason,
                None,
            ))
        } else {
            Ok((
                None,
                ProvenanceSource::Live,
                None,
                true,
                Some("no live or persisted market snapshot available".into()),
                None,
            ))
        }
    }
}
