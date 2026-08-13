//! rmcp `ServerHandler` implementation (server info + instrumented tool dispatch).

use rmcp::handler::server::tool::ToolCallContext;
use rmcp::service::RequestContext;
use rmcp::{model::*, ErrorData as McpError, RoleServer, ServerHandler};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex as AsyncMutex;

#[allow(unused_imports)]
use crate::state::*;

/// Persist a runtime telemetry snapshot every N MCP tool calls (bounded I/O).
const TELEMETRY_PERSIST_EVERY_N: u64 = 25;

static TOOL_CALLS_SINCE_PERSIST: AtomicU64 = AtomicU64::new(0);
static TELEMETRY_PERSIST_LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();

impl ServerHandler for TheDeskMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "The Desk - local-first MCP intelligence backend for NQ futures market analytics. \
                 Live data: Sierra Chart `.scid` ticks plus optional `MarketDepthData` `.depth` files only. \
                 122 MCP tools in 9 domains: market (live structure: VWAP, TPO, delta, levels, tape, footprint), \
                 dom (order book behavior), options (gamma context), \
                 playbook (setups, attention signals, trade idea lifecycle), \
                 risk (limits, sizing, session bookends), journal (trades, fills, reviews), \
                 memory (insights, patterns, briefings), \
                 research (hypotheses, backtests, frequency/conditional/distribution queries), \
                 admin (feed health, ingestion, integrity). \
                 Routing guide: skills/mcp-tools/SKILL.md; full catalog: docs/mcp/tool-reference.md. \
                 Session start: get_session_context + get_market_snapshot, then risk state. \
                 'What deserves attention?': get_attention_inbox. \
                 Historical tools need backfill_history; check get_research_summary first. \
                 All coaching frames as 'your playbook says...' -- never advisory."
                    .into(),
            ),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let tool_name = request.name.to_string();
        let tcc = ToolCallContext::new(self, request, context);
        let result = self.tool_router.call(tcc).await;
        self.tool_telemetry.record(&tool_name, &result);
        maybe_persist_tool_telemetry(self);
        result
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult {
            tools: self.tool_router.list_all(),
            meta: None,
            next_cursor: None,
        })
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tool_router.get(name).cloned()
    }
}

fn maybe_persist_tool_telemetry(server: &TheDeskMcp) {
    let n = TOOL_CALLS_SINCE_PERSIST.fetch_add(1, Ordering::Relaxed) + 1;
    if !n.is_multiple_of(TELEMETRY_PERSIST_EVERY_N) {
        return;
    }
    let telemetry = Arc::clone(&server.tool_telemetry);
    let path = crate::tool_telemetry::runtime_snapshot_path();
    let surface = server.tool_router.list_all().len();
    // File I/O stays off the MCP request path (CLAUDE.md / AGENT.md: no blocking I/O).
    // Persist tasks are serialized so a delayed older write cannot overwrite a newer one;
    // each write takes a fresh snapshot under the lock.
    tokio::spawn(async move {
        let lock = TELEMETRY_PERSIST_LOCK.get_or_init(|| AsyncMutex::new(()));
        let _guard = lock.lock().await;
        let write =
            tokio::task::spawn_blocking(move || telemetry.write_snapshot_file(&path, surface))
                .await;
        match write {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "tool telemetry snapshot write failed");
            }
            Err(join_err) => {
                tracing::warn!(error = %join_err, "tool telemetry snapshot task failed");
            }
        }
    });
}
