use crate::db::PriorDayReference;
use crate::feed::ContractMetadata;
use serde::{Deserialize, Serialize};

/// Contract-roll validation status for carry-forward reference levels.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ContractRolloverStatusKind {
    Ok,
    RolloverDetected,
    MissingCurrentContractHistory,
    ResolverMismatch,
    InstrumentMismatch,
}

/// Recommended deterministic action for an agent or rules caller.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ContractRolloverAgentAction {
    UsePriorLevels,
    ClearPriorLevels,
    LegacyContextOnly,
    RunBackfill,
    PinManualOverride,
    RestartMcpServer,
}

/// Compact prior-reference row used in rollover diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContractReferenceSummary {
    pub date: String,
    pub root_symbol: Option<String>,
    pub contract_symbol: Option<String>,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub va_high: Option<f64>,
    pub va_low: Option<f64>,
    pub poc: Option<f64>,
    pub dnva_high: Option<f64>,
    pub dnva_low: Option<f64>,
    pub dnp: Option<f64>,
}

impl From<PriorDayReference> for ContractReferenceSummary {
    fn from(value: PriorDayReference) -> Self {
        Self {
            date: value.date,
            root_symbol: value.root_symbol,
            contract_symbol: value.contract_symbol,
            high: value.high,
            low: value.low,
            close: value.close,
            va_high: value.va_high,
            va_low: value.va_low,
            poc: value.poc,
            dnva_high: value.dnva_high,
            dnva_low: value.dnva_low,
            dnp: value.dnp,
        }
    }
}

/// Full rollover diagnostic returned by MCP tools.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContractRolloverStatus {
    pub status: ContractRolloverStatusKind,
    pub agent_action: ContractRolloverAgentAction,
    pub active_root_symbol: String,
    pub active_contract_symbol: String,
    pub server_root_symbol: Option<String>,
    pub server_contract_symbol: Option<String>,
    pub server_contract_matches_resolver: bool,
    pub prior_day_reference: Option<ContractReferenceSummary>,
    pub same_root_latest_reference: Option<ContractReferenceSummary>,
    pub legacy_contract_reference: Option<ContractReferenceSummary>,
    pub current_contract_history_available: bool,
    pub same_root_history_available: bool,
    pub prior_references_authoritative: bool,
    pub should_clear_prior_levels: bool,
    pub scid_file_exists: bool,
    pub depth_file_count: usize,
    pub resolver_warnings: Vec<String>,
    pub data_age_ms: Option<f64>,
    pub data_freshness_status: String,
    pub notes: Vec<String>,
}

/// Build a deterministic rollover status from resolved feed metadata and
/// SQLite prior-reference lookups. This does not mutate pipeline state.
pub fn build_contract_rollover_status(
    active: &ContractMetadata,
    server: Option<&ContractMetadata>,
    active_contract_reference: Option<PriorDayReference>,
    same_root_reference: Option<PriorDayReference>,
    data_age_ms: Option<f64>,
    freshness_threshold_ms: f64,
) -> ContractRolloverStatus {
    let active_root = normalize_symbol(&active.root_symbol);
    let active_contract = normalize_symbol(&active.contract_symbol);
    let server_root = server.map(|m| normalize_symbol(&m.root_symbol));
    let server_contract = server.map(|m| normalize_symbol(&m.contract_symbol));
    let server_contract_matches_resolver = match (&server_root, &server_contract) {
        (Some(root), Some(contract)) => root == &active_root && contract == &active_contract,
        _ => true,
    };

    let active_ref = active_contract_reference.map(ContractReferenceSummary::from);
    let same_root_ref = same_root_reference.map(ContractReferenceSummary::from);
    let legacy_ref = same_root_ref
        .as_ref()
        .filter(|reference| {
            reference
                .contract_symbol
                .as_deref()
                .map(normalize_symbol)
                .is_some_and(|contract| contract != active_contract)
        })
        .cloned();

    let mut notes = Vec::new();
    if !server_contract_matches_resolver {
        notes.push("The MCP server pipeline contract differs from the freshly resolved feed contract; restart or reload before trusting live carry-forward state.".to_string());
    }
    if !active.scid_file_exists {
        notes.push("The resolved SCID file is missing for the active contract.".to_string());
    }
    if active.depth_file_count == 0 {
        notes.push("No MarketDepthData files were found for the active contract.".to_string());
    }
    if !active.warnings.is_empty() {
        notes.extend(active.warnings.iter().cloned());
    }

    let data_freshness_status = match data_age_ms {
        Some(age) if age.is_finite() && age <= freshness_threshold_ms => "ok",
        Some(age) if age.is_finite() => "stale",
        _ => "unknown",
    }
    .to_string();
    if data_freshness_status == "stale" {
        notes.push(
            "Live feed data is stale; validate the feed before using session-start levels."
                .to_string(),
        );
    }

    let current_contract_history_available = active_ref.is_some();
    let same_root_history_available = same_root_ref.is_some();

    let (status, agent_action, prior_references_authoritative, should_clear_prior_levels) =
        if !server_contract_matches_resolver {
            (
                ContractRolloverStatusKind::ResolverMismatch,
                ContractRolloverAgentAction::RestartMcpServer,
                false,
                true,
            )
        } else if same_root_ref
            .as_ref()
            .and_then(|reference| reference.root_symbol.as_deref())
            .map(normalize_symbol)
            .is_some_and(|root| root != active_root)
        {
            (
                ContractRolloverStatusKind::InstrumentMismatch,
                ContractRolloverAgentAction::ClearPriorLevels,
                false,
                true,
            )
        } else if current_contract_history_available {
            (
                ContractRolloverStatusKind::Ok,
                ContractRolloverAgentAction::UsePriorLevels,
                true,
                false,
            )
        } else if legacy_ref.is_some() {
            (
                ContractRolloverStatusKind::RolloverDetected,
                ContractRolloverAgentAction::LegacyContextOnly,
                false,
                true,
            )
        } else {
            (
                ContractRolloverStatusKind::MissingCurrentContractHistory,
                ContractRolloverAgentAction::RunBackfill,
                false,
                true,
            )
        };

    ContractRolloverStatus {
        status,
        agent_action,
        active_root_symbol: active_root,
        active_contract_symbol: active_contract,
        server_root_symbol: server_root,
        server_contract_symbol: server_contract,
        server_contract_matches_resolver,
        prior_day_reference: active_ref,
        same_root_latest_reference: same_root_ref,
        legacy_contract_reference: legacy_ref,
        current_contract_history_available,
        same_root_history_available,
        prior_references_authoritative,
        should_clear_prior_levels,
        scid_file_exists: active.scid_file_exists,
        depth_file_count: active.depth_file_count,
        resolver_warnings: active.warnings.clone(),
        data_age_ms,
        data_freshness_status,
        notes,
    }
}

fn normalize_symbol(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(contract: &str) -> ContractMetadata {
        ContractMetadata {
            root_symbol: "NQ".to_string(),
            contract_symbol: contract.to_string(),
            scid_file_exists: true,
            depth_file_count: 1,
            ..Default::default()
        }
    }

    fn reference(date: &str, contract: &str) -> PriorDayReference {
        PriorDayReference {
            date: date.to_string(),
            high: 21_100.0,
            low: 20_900.0,
            close: 21_000.0,
            va_high: Some(21_050.0),
            va_low: Some(20_950.0),
            poc: Some(21_000.0),
            dnva_high: Some(21_025.0),
            dnva_low: Some(20_975.0),
            dnp: Some(21_000.0),
            root_symbol: Some("NQ".to_string()),
            contract_symbol: Some(contract.to_string()),
        }
    }

    #[test]
    fn same_contract_reference_is_authoritative() {
        let status = build_contract_rollover_status(
            &metadata("NQH26"),
            Some(&metadata("NQH26")),
            Some(reference("2026-03-04", "NQH26")),
            Some(reference("2026-03-04", "NQH26")),
            Some(1_000.0),
            15_000.0,
        );

        assert_eq!(status.status, ContractRolloverStatusKind::Ok);
        assert_eq!(
            status.agent_action,
            ContractRolloverAgentAction::UsePriorLevels
        );
        assert!(status.prior_references_authoritative);
        assert!(!status.should_clear_prior_levels);
    }

    #[test]
    fn previous_contract_reference_is_legacy_context_only() {
        let status = build_contract_rollover_status(
            &metadata("NQM26"),
            Some(&metadata("NQM26")),
            None,
            Some(reference("2026-03-04", "NQH26")),
            Some(1_000.0),
            15_000.0,
        );

        assert_eq!(status.status, ContractRolloverStatusKind::RolloverDetected);
        assert_eq!(
            status.agent_action,
            ContractRolloverAgentAction::LegacyContextOnly
        );
        assert!(status.legacy_contract_reference.is_some());
        assert!(!status.prior_references_authoritative);
        assert!(status.should_clear_prior_levels);
    }

    #[test]
    fn resolver_mismatch_requires_restart() {
        let status = build_contract_rollover_status(
            &metadata("NQM26"),
            Some(&metadata("NQH26")),
            Some(reference("2026-03-04", "NQM26")),
            Some(reference("2026-03-04", "NQM26")),
            Some(1_000.0),
            15_000.0,
        );

        assert_eq!(status.status, ContractRolloverStatusKind::ResolverMismatch);
        assert_eq!(
            status.agent_action,
            ContractRolloverAgentAction::RestartMcpServer
        );
        assert!(!status.server_contract_matches_resolver);
    }

    #[test]
    fn missing_current_contract_history_requests_backfill() {
        let status = build_contract_rollover_status(
            &metadata("NQM26"),
            Some(&metadata("NQM26")),
            None,
            None,
            Some(1_000.0),
            15_000.0,
        );

        assert_eq!(
            status.status,
            ContractRolloverStatusKind::MissingCurrentContractHistory
        );
        assert_eq!(
            status.agent_action,
            ContractRolloverAgentAction::RunBackfill
        );
        assert!(!status.current_contract_history_available);
        assert!(status.should_clear_prior_levels);
    }

    #[test]
    fn root_mismatch_clears_prior_references() {
        let mut wrong_root = reference("2026-03-04", "ESH26");
        wrong_root.root_symbol = Some("ES".to_string());
        let status = build_contract_rollover_status(
            &metadata("NQH26"),
            Some(&metadata("NQH26")),
            None,
            Some(wrong_root),
            Some(1_000.0),
            15_000.0,
        );

        assert_eq!(
            status.status,
            ContractRolloverStatusKind::InstrumentMismatch
        );
        assert_eq!(
            status.agent_action,
            ContractRolloverAgentAction::ClearPriorLevels
        );
        assert!(!status.prior_references_authoritative);
    }
}
