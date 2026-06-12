//! MCP tool implementations grouped by domain. Each module contributes a named
//! router; `TheDeskMcp::tool_router()` in `service.rs` combines them.

pub(crate) mod admin;
pub(crate) mod dom;
pub(crate) mod journal;
pub(crate) mod market;
pub(crate) mod memory;
pub(crate) mod options;
pub(crate) mod playbook;
pub(crate) mod research;
pub(crate) mod risk;
