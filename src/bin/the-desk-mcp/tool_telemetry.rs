//! Per-tool call telemetry for SIL-M0 baseline evidence.
//!
//! Captures call counts and approximate response token cost so later shim /
//! deprecation work (M1b) can attribute deltas to a durable baseline rather
//! than estimates. Orientation-chain shape is fixed here to match the
//! documented session-start coaching path.

#[cfg(test)]
use rmcp::model::Content;
use rmcp::model::{CallToolResult, RawContent};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Bytes-per-token estimate for approximate response cost (UTF-8 text / 4).
pub(crate) const APPROX_BYTES_PER_TOKEN: usize = 4;

/// SIL-M0 baseline schema version — bump when the snapshot shape changes.
pub(crate) const BASELINE_SCHEMA_VERSION: u32 = 1;

/// Documented five-call orientation chain (session start) used for cost shape.
///
/// Matches `skills/mcp-tools/SKILL.md` / server instructions: session context,
/// market snapshot, then the three risk/account reads agents run in parallel.
pub(crate) const ORIENTATION_CHAIN: &[&str] = &[
    "get_session_context",
    "get_market_snapshot",
    "get_risk_state",
    "get_risk_config",
    "get_account_state",
];

/// Frozen specialty market tool names (SIL-M0). Sorted; enforced by drift test.
///
/// Until Desk Catalog v0 lands, no new specialty market tools may be added.
/// After Catalog v0, the rule becomes: no catalog entry → no new market tool.
pub(crate) const FROZEN_MARKET_TOOLS: &[&str] = &[
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
];

/// Per-tool counters accumulated over the process lifetime.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolCallStats {
    pub call_count: u64,
    pub total_response_bytes: u64,
    pub total_approx_tokens: u64,
    pub error_count: u64,
}

/// Durable / in-memory snapshot of tool-call telemetry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolTelemetrySnapshot {
    pub schema_version: u32,
    pub captured_at_unix_ms: u64,
    pub policy: String,
    pub orientation_chain: Vec<String>,
    pub frozen_market_tools: Vec<String>,
    pub tool_surface_count: usize,
    pub per_tool: BTreeMap<String, ToolCallStats>,
    pub orientation_chain_cost: OrientationChainCost,
}

/// Aggregated cost for the documented orientation chain.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OrientationChainCost {
    pub tools: Vec<String>,
    pub call_count: u64,
    pub total_response_bytes: u64,
    pub total_approx_tokens: u64,
    /// True when every chain tool has been observed at least once.
    pub fully_observed: bool,
}

/// In-process telemetry store; cheap, lock-scoped, no SQLite on the hot path.
#[derive(Debug, Default)]
pub(crate) struct ToolTelemetry {
    inner: Mutex<BTreeMap<String, ToolCallStats>>,
}

impl ToolTelemetry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record one tool invocation from its MCP result.
    pub(crate) fn record(&self, tool_name: &str, result: &Result<CallToolResult, rmcp::ErrorData>) {
        let (bytes, is_error) = match result {
            Ok(call) => (call_tool_result_bytes(call), call.is_error.unwrap_or(false)),
            Err(_) => (0, true),
        };
        self.record_raw(tool_name, bytes, is_error);
    }

    /// Record with an explicit response size (for tests / offline accounting).
    pub(crate) fn record_raw(&self, tool_name: &str, response_bytes: usize, is_error: bool) {
        let approx_tokens = approx_tokens_from_bytes(response_bytes);
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let entry = guard.entry(tool_name.to_string()).or_default();
        entry.call_count = entry.call_count.saturating_add(1);
        entry.total_response_bytes = entry
            .total_response_bytes
            .saturating_add(response_bytes as u64);
        entry.total_approx_tokens = entry
            .total_approx_tokens
            .saturating_add(approx_tokens as u64);
        if is_error {
            entry.error_count = entry.error_count.saturating_add(1);
        }
    }

    pub(crate) fn snapshot(&self, tool_surface_count: usize) -> ToolTelemetrySnapshot {
        let per_tool = self.inner.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let orientation_chain_cost = orientation_chain_cost_from(&per_tool);
        snapshot_shell(tool_surface_count, per_tool, orientation_chain_cost)
    }

    /// Persist a snapshot JSON for M1b before/after comparison.
    pub(crate) fn write_snapshot_file(
        &self,
        path: &Path,
        tool_surface_count: usize,
    ) -> Result<(), String> {
        let snapshot = self.snapshot(tool_surface_count);
        write_snapshot_file(path, &snapshot)
    }
}

const POLICY_FROZEN_UNTIL_CATALOG_V0: &str = "specialty_market_tools_frozen_until_catalog_v0";

fn snapshot_shell(
    tool_surface_count: usize,
    per_tool: BTreeMap<String, ToolCallStats>,
    orientation_chain_cost: OrientationChainCost,
) -> ToolTelemetrySnapshot {
    ToolTelemetrySnapshot {
        schema_version: BASELINE_SCHEMA_VERSION,
        captured_at_unix_ms: unix_ms_now(),
        policy: POLICY_FROZEN_UNTIL_CATALOG_V0.to_string(),
        orientation_chain: ORIENTATION_CHAIN.iter().map(|s| (*s).to_string()).collect(),
        frozen_market_tools: FROZEN_MARKET_TOOLS
            .iter()
            .map(|s| (*s).to_string())
            .collect(),
        tool_surface_count,
        per_tool,
        orientation_chain_cost,
    }
}

/// Structural baseline (policy + chain + freeze set) with empty counters.
#[allow(dead_code)]
pub(crate) fn structural_baseline(tool_surface_count: usize) -> ToolTelemetrySnapshot {
    snapshot_shell(
        tool_surface_count,
        BTreeMap::new(),
        OrientationChainCost {
            tools: ORIENTATION_CHAIN.iter().map(|s| (*s).to_string()).collect(),
            ..OrientationChainCost::default()
        },
    )
}

/// Baseline that includes one cold orientation-chain probe (empty/in-memory server).
///
/// Records the five-call coaching path once so M1b can compare call count and
/// approximate token cost against a durable numeric before-figure.
pub(crate) fn baseline_with_orientation_probe(
    tool_surface_count: usize,
    probe: &ToolTelemetry,
) -> ToolTelemetrySnapshot {
    let mut snap = probe.snapshot(tool_surface_count);
    // Keep only orientation-chain tools in the checked-in probe so the artifact
    // stays focused on the coaching path M1b will shrink.
    snap.per_tool
        .retain(|name, _| ORIENTATION_CHAIN.contains(&name.as_str()));
    snap.orientation_chain_cost = orientation_chain_cost_from(&snap.per_tool);
    snap.policy = POLICY_FROZEN_UNTIL_CATALOG_V0.to_string();
    snap
}

pub(crate) fn write_snapshot_file(
    path: &Path,
    snapshot: &ToolTelemetrySnapshot,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create telemetry dir: {e}"))?;
    }
    let body = serde_json::to_string_pretty(snapshot)
        .map_err(|e| format!("serialize telemetry snapshot: {e}"))?;
    let body = format!("{body}\n");
    // Atomic replace: write a sibling temp file, then rename into place so readers
    // never observe a truncated JSON payload.
    let file_name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
        format!(
            "telemetry snapshot path missing file name: {}",
            path.display()
        )
    })?;
    let tmp_path = path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()));
    fs::write(&tmp_path, &body).map_err(|e| format!("write telemetry snapshot temp: {e}"))?;
    fs::rename(&tmp_path, path).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        format!("replace telemetry snapshot: {e}")
    })
}

pub(crate) fn read_snapshot_file(path: &Path) -> Result<ToolTelemetrySnapshot, String> {
    let raw = fs::read_to_string(path).map_err(|e| format!("read telemetry snapshot: {e}"))?;
    serde_json::from_str(&raw).map_err(|e| format!("parse telemetry snapshot: {e}"))
}

/// Checked-in SIL-M0 baseline path (repo-relative from crate root).
pub(crate) fn checked_in_baseline_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docs/mcp/sil-m0-tool-telemetry-baseline.json")
}

/// Runtime durable path under `~/.the-desk/telemetry/`.
pub(crate) fn runtime_snapshot_path() -> PathBuf {
    crate::lifecycle::data_dir()
        .join("telemetry")
        .join("tool-call-snapshot.json")
}

pub(crate) fn approx_tokens_from_bytes(bytes: usize) -> usize {
    bytes.div_ceil(APPROX_BYTES_PER_TOKEN)
}

pub(crate) fn call_tool_result_bytes(result: &CallToolResult) -> usize {
    result
        .content
        .iter()
        .map(|c| match &c.raw {
            RawContent::Text(text) => text.text.len(),
            RawContent::Image(image) => image.data.len(),
            RawContent::Resource(resource) => format!("{resource:?}").len(),
            RawContent::Audio(audio) => audio.data.len(),
            _ => 0,
        })
        .sum()
}

fn orientation_chain_cost_from(per_tool: &BTreeMap<String, ToolCallStats>) -> OrientationChainCost {
    let mut cost = OrientationChainCost {
        tools: ORIENTATION_CHAIN.iter().map(|s| (*s).to_string()).collect(),
        ..OrientationChainCost::default()
    };
    let mut fully = true;
    for name in ORIENTATION_CHAIN {
        match per_tool.get(*name) {
            Some(stats) if stats.call_count > 0 => {
                cost.call_count = cost.call_count.saturating_add(stats.call_count);
                cost.total_response_bytes = cost
                    .total_response_bytes
                    .saturating_add(stats.total_response_bytes);
                cost.total_approx_tokens = cost
                    .total_approx_tokens
                    .saturating_add(stats.total_approx_tokens);
            }
            _ => fully = false,
        }
    }
    cost.fully_observed = fully;
    cost
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Content helper used only when building synthetic CallToolResult in tests.
#[cfg(test)]
pub(crate) fn text_contents(text: impl Into<String>) -> Vec<Content> {
    vec![Content::text(text.into())]
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::CallToolResult;

    #[test]
    fn approx_tokens_rounds_up() {
        assert_eq!(approx_tokens_from_bytes(0), 0);
        assert_eq!(approx_tokens_from_bytes(1), 1);
        assert_eq!(approx_tokens_from_bytes(4), 1);
        assert_eq!(approx_tokens_from_bytes(5), 2);
    }

    #[test]
    fn record_accumulates_counts_and_tokens() {
        let telemetry = ToolTelemetry::new();
        let ok = Ok(CallToolResult::success(text_contents("abcd"))); // 4 bytes → 1 token
        telemetry.record("get_market_snapshot", &ok);
        telemetry.record_raw("get_market_snapshot", 8, false);

        let snap = telemetry.snapshot(121);
        let stats = snap.per_tool.get("get_market_snapshot").expect("stats");
        assert_eq!(stats.call_count, 2);
        assert_eq!(stats.total_response_bytes, 12);
        assert_eq!(stats.total_approx_tokens, 3);
        assert_eq!(stats.error_count, 0);
        assert_eq!(snap.orientation_chain.len(), 5);
        assert_eq!(snap.frozen_market_tools.len(), FROZEN_MARKET_TOOLS.len());
    }

    #[test]
    fn orientation_chain_cost_tracks_documented_tools_only() {
        let telemetry = ToolTelemetry::new();
        telemetry.record_raw("get_session_context", 100, false);
        telemetry.record_raw("get_market_snapshot", 200, false);
        telemetry.record_raw("get_tpo_profile", 999, false); // not in chain

        let cost = telemetry.snapshot(121).orientation_chain_cost;
        assert!(!cost.fully_observed);
        assert_eq!(cost.call_count, 2);
        assert_eq!(cost.total_response_bytes, 300);
        assert_eq!(
            cost.total_approx_tokens,
            approx_tokens_from_bytes(100) as u64 + approx_tokens_from_bytes(200) as u64
        );
    }

    #[test]
    fn snapshot_roundtrips_through_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("snap.json");
        let telemetry = ToolTelemetry::new();
        telemetry.record_raw("get_risk_state", 40, false);
        telemetry.write_snapshot_file(&path, 121).expect("write");
        let loaded = read_snapshot_file(&path).expect("read");
        assert_eq!(loaded.schema_version, BASELINE_SCHEMA_VERSION);
        assert_eq!(loaded.per_tool["get_risk_state"].call_count, 1);
        assert_eq!(
            loaded.policy,
            "specialty_market_tools_frozen_until_catalog_v0"
        );
    }

    #[test]
    fn snapshot_write_is_atomic_replace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("snap.json");
        let telemetry = ToolTelemetry::new();
        telemetry.record_raw("get_session_context", 12, false);
        telemetry.write_snapshot_file(&path, 121).expect("write");
        assert!(path.is_file());
        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .expect("read_dir")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .filter(|n| n.to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp snapshot files must be renamed away: {leftovers:?}"
        );
    }
}
