//! SIL discovery operators — catalog metadata only (never live market data).
//!
//! Registered on the MCP router only when `[sil].catalog_discovery = true`.
//! Not specialty market tools; do not add these to `tools/market.rs`.

use rmcp::{
    handler::server::wrapper::Parameters, model::*, tool, tool_router, ErrorData as McpError,
};
use the_desk_backend::catalog::{
    build_catalog, describe_domain, describe_environment, search_catalog,
};

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

#[tool_router(router = discovery_router, vis = "pub(crate)")]
impl TheDeskMcp {
    #[tool(
        description = "Describe the Desk Catalog environment: catalogVersion, Trust Ceiling (L3), domain list, Positioning stub status, and specialty-market-tool policy. Returns catalog metadata only — never live market data. Enable via [sil].catalog_discovery in config.toml."
    )]
    pub(crate) async fn describe_environment(&self) -> Result<CallToolResult, McpError> {
        let catalog = build_catalog();
        let out = describe_environment(&catalog, self.sil_config.catalog_discovery);
        Ok(text_result(out))
    }

    #[tool(
        description = "Describe one catalog domain by id (identity, location_structure, flow, liquidity, response, volatility, positioning, cross_market, events, meta). Returns field descriptors (unit, session scope, freshness, cost hint) — metadata only, never live market data."
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
            Some(out) => Ok(text_result(out)),
            None => Ok(text_result(serde_json::json!({
                "error": "unknown_domain",
                "domain": domain_id,
                "knownDomains": catalog.domains.iter().map(|d| &d.id).collect::<Vec<_>>(),
                "metadataOnly": true,
                "catalogVersion": catalog.catalog_version,
            }))),
        }
    }

    #[tool(
        description = "Search the Desk Catalog by text across field ids, names, descriptions, and domains. Returns matching field descriptors with unit, session scope, freshness, and cost hint — metadata only, never live market data."
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
        })))
    }
}
