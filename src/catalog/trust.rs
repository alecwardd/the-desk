//! Trust Level capability tags for SIL kernel / tool surface.
//!
//! Trust Ceiling stays **L3** (human executes). Read/query kernel operators
//! run at Trust Level **L0** and are structurally incapable of mutation or
//! order authority — enforced here and asserted in router tests.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::envelope::TrustLevel;

/// Capability record for one MCP tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCapability {
    pub trust_level: TrustLevel,
    /// When true, the tool may mutate workflow-verb state (risk, playbook,
    /// journal, memory, hypothesis, feature registry, positioning entry, trade idea, …).
    pub mutation_authority: bool,
    /// When true, the tool may reach order placement / modification state.
    /// Must remain false at every Trust Level while Trust Ceiling is L3.
    pub order_authority: bool,
}

impl ToolCapability {
    /// Read/query kernel capability (Trust Level L0).
    pub const fn read_query_l0() -> Self {
        Self {
            trust_level: TrustLevel::L0,
            mutation_authority: false,
            order_authority: false,
        }
    }
}

/// SIL kernel operators registered behind `[sil].catalog_discovery`.
pub const KERNEL_READ_QUERY_TOOLS: &[&str] = &[
    "describe_environment",
    "describe_domain",
    "search_catalog",
    "get_state",
    "get_events",
    "query_series",
    "query_episodes",
    "query_raw",
    "run_job",
];

/// Build the capability map for kernel read/query tools (all L0, no mutation).
pub fn kernel_read_query_capabilities() -> BTreeMap<&'static str, ToolCapability> {
    KERNEL_READ_QUERY_TOOLS
        .iter()
        .map(|name| (*name, ToolCapability::read_query_l0()))
        .collect()
}

/// Returns true when a tool name is a known SIL read/query kernel operator.
pub fn is_kernel_read_query_tool(name: &str) -> bool {
    KERNEL_READ_QUERY_TOOLS.contains(&name)
}

/// Heuristic: tool names that carry mutation authority on the existing surface.
/// Used by router tests to assert L0 tools never overlap this set.
pub fn tool_name_implies_mutation(name: &str) -> bool {
    const MUTATION_PREFIXES: &[&str] = &[
        "save_",
        "upsert_",
        "mark_",
        "acknowledge_",
        "close_",
        "create_",
        "register_",
        "set_",
        "transition_",
        "import_",
        "init_",
        "start_",
        "end_",
        "detect_",
        "propose_",
        "activate_",
        "cancel_",
        "ingest_",
        "archive_",
        "backfill_",
        "run_",
        "refresh_memory",
        "refresh_options",
        "seed_",
    ];
    // Explicit mutation verbs that don't match prefixes cleanly.
    const MUTATION_EXACT: &[&str] = &[
        "record_trade_result",
        "update_attention_signal_status",
        "positioning_entry",
        "feature_registry",
    ];
    if MUTATION_EXACT.contains(&name) {
        return true;
    }
    // SIL-M3c `run_job` is a Trust Level L0 query operator: it writes research
    // artifacts only and must not be classified as a workflow-verb mutation.
    if name == "run_job" {
        return false;
    }
    MUTATION_PREFIXES.iter().any(|p| name.starts_with(p))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_tools_are_trust_level_l0_without_mutation_or_order_authority() {
        let caps = kernel_read_query_capabilities();
        assert_eq!(caps.len(), KERNEL_READ_QUERY_TOOLS.len());
        for name in KERNEL_READ_QUERY_TOOLS {
            let cap = caps.get(name).expect(name);
            assert_eq!(cap.trust_level, TrustLevel::L0);
            assert!(!cap.mutation_authority);
            assert!(!cap.order_authority);
            assert!(
                !tool_name_implies_mutation(name),
                "kernel tool `{name}` must not look like a mutation verb"
            );
        }
    }

    #[test]
    fn mutation_heuristic_flags_known_write_verbs() {
        assert!(tool_name_implies_mutation("save_journal_entry"));
        assert!(tool_name_implies_mutation("mark_trade_idea_confirmed"));
        assert!(tool_name_implies_mutation("backfill_history"));
        assert!(tool_name_implies_mutation("positioning_entry"));
        assert!(tool_name_implies_mutation("feature_registry"));
        assert!(!tool_name_implies_mutation("get_state"));
        assert!(!tool_name_implies_mutation("get_events"));
        assert!(!tool_name_implies_mutation("describe_environment"));
        assert!(!tool_name_implies_mutation("query_series"));
        assert!(!tool_name_implies_mutation("query_episodes"));
        assert!(!tool_name_implies_mutation("query_raw"));
        assert!(!tool_name_implies_mutation("run_job"));
        assert!(tool_name_implies_mutation("run_backtest"));
        assert!(!tool_name_implies_mutation("evaluate_playbook"));
        assert!(!tool_name_implies_mutation("get_market_snapshot"));
    }
}
