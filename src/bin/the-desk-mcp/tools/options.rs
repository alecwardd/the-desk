//! Options integration: gamma levels and dealer-positioning context.

use rmcp::{
    handler::server::wrapper::Parameters, model::*, tool, tool_router, ErrorData as McpError,
};

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

#[tool_router(router = options_router, vis = "pub(crate)")]
impl TheDeskMcp {
    #[tool(
        description = "Top SPX/options gamma concentration strikes from ConvexValue, with call/put breakdown, open interest, OI change, volume bias, vomma, recent 5m volume, avg spread, expiration coverage, and cache metadata. Use for pre-session context like 'where are the likely gamma walls?' or 'where is new positioning opening today?'"
    )]
    pub(crate) async fn get_gamma_levels(
        &self,
        Parameters(params): Parameters<GammaLevelsParams>,
    ) -> Result<CallToolResult, McpError> {
        let top_n = params.top.unwrap_or(12).clamp(1, 50) as usize;
        let (snapshot, refreshed) = self
            .get_or_refresh_options_snapshot(
                params.root.as_deref(),
                params.exps.clone(),
                params.range,
                params.force_refresh.unwrap_or(false),
            )
            .await?;
        let mut report = snapshot.gamma_levels.clone();
        report
            .top_gamma_concentration_levels
            .truncate(top_n.min(report.top_gamma_concentration_levels.len()));
        let out = serde_json::json!({
            "root": snapshot.root,
            "requestedExpirations": snapshot.requested_exps,
            "requestedRange": snapshot.requested_range,
            "report": report,
            "optionsContextSummary": {
                "aggregateGxoi": snapshot.context.aggregate_gxoi,
                "aggregateDxoi": snapshot.context.aggregate_dxoi,
                "callGxoi": snapshot.context.call_gxoi,
                "putGxoi": snapshot.context.put_gxoi,
                "putCallRatio": snapshot.context.put_call_ratio,
                "flowDirection": snapshot.context.flow_direction,
                "volTermSpread": snapshot.context.vol_term_spread,
            },
            "cache": options_cache_metadata(&snapshot, refreshed),
        });
        Ok(text_result(out))
    }

    #[tool(
        description = "Aggregate ConvexValue options regime context: underlying price/change, aggregate gxoi/dxoi, call/put gxoi/dxoi splits, put-call ratio, flow decomposition (flowratio, call/put value/volume bias), vol surface (front/back IV, term spread), premium flow (value bought/sold), vanna/charm regime, and cache metadata. Use when an agent needs broad options positioning context rather than per-strike detail."
    )]
    pub(crate) async fn get_options_context(
        &self,
        Parameters(params): Parameters<OptionsContextParams>,
    ) -> Result<CallToolResult, McpError> {
        let (snapshot, refreshed) = self
            .get_or_refresh_options_snapshot(
                params.root.as_deref(),
                params.exps.clone(),
                params.range,
                params.force_refresh.unwrap_or(false),
            )
            .await?;
        let out = serde_json::json!({
            "root": snapshot.root,
            "requestedExpirations": snapshot.requested_exps,
            "requestedRange": snapshot.requested_range,
            "context": snapshot.context,
            "topGammaStrikes": snapshot
                .gamma_levels
                .top_gamma_concentration_levels
                .iter()
                .take(3)
                .cloned()
                .collect::<Vec<_>>(),
            "cache": options_cache_metadata(&snapshot, refreshed),
        });
        Ok(text_result(out))
    }

    #[tool(
        description = "Force-refresh the cached ConvexValue snapshot used by get_gamma_levels and get_options_context, then return the fresh options context plus a gamma-level preview."
    )]
    pub(crate) async fn refresh_options_snapshot(
        &self,
        Parameters(params): Parameters<OptionsSnapshotParams>,
    ) -> Result<CallToolResult, McpError> {
        let (snapshot, refreshed) = self
            .get_or_refresh_options_snapshot(
                params.root.as_deref(),
                params.exps,
                params.range,
                true,
            )
            .await?;
        let out = serde_json::json!({
            "root": snapshot.root,
            "requestedExpirations": snapshot.requested_exps,
            "requestedRange": snapshot.requested_range,
            "context": snapshot.context,
            "gammaLevelsPreview": snapshot
                .gamma_levels
                .top_gamma_concentration_levels
                .iter()
                .take(5)
                .cloned()
                .collect::<Vec<_>>(),
            "cache": options_cache_metadata(&snapshot, refreshed),
        });
        Ok(text_result(out))
    }
}
