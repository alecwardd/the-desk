//! rmcp `ServerHandler` implementation (server info + instructions).

use rmcp::{model::*, tool_handler, ServerHandler};

#[allow(unused_imports)]
use crate::state::*;

#[tool_handler]
impl ServerHandler for TheDeskMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "The Desk - local-first MCP intelligence backend for NQ futures market analytics. \
                 Live data: Sierra Chart `.scid` ticks plus optional `MarketDepthData` `.depth` files only. \
                 121 MCP tools in 9 domains: market (live structure: VWAP, TPO, delta, levels, tape, footprint), \
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
}
