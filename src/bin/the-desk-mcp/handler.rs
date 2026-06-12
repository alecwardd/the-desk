//! rmcp `ServerHandler` implementation (server info + instructions).

use rmcp::{model::*, tool_handler, ServerHandler};

#[allow(unused_imports)]
use crate::state::*;

#[tool_handler]
impl ServerHandler for TheDeskMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "The Desk - AI trading co-pilot backend for NQ futures. \
                 Live data: Sierra Chart `.scid` ticks plus optional `MarketDepthData` `.depth` files only. \
                 Provides real-time market structure (VWAP, TPO, Delta), \
                 microstructure analytics (tape pace, footprint, absorption), \
                 and playbook evaluation. \
                 All coaching frames as 'your playbook says...' -- never advisory."
                    .into(),
            ),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}
