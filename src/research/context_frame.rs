use crate::db::{
    ContextSnapshotBuckets, ContextSnapshotQuery, Database, SessionScopeFilter, SessionSummary,
};
use crate::tick_time_context_from_timestamp_ms;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{reliability_tier, ReliabilityTier};

/// Version identifier for the first stable context-bucket contract.
pub const BUCKET_DEFINITION_VERSION: &str = "context-v1";
/// Date the v1 bucket contract was accepted into the decision log.
pub const BUCKET_DEFINITION_BLESSED_AT: &str = "2026-05-01";
/// Decision-log entry that documents the context-framing contract.
pub const DECISION_LOG_REF: &str = "ADR-017";

const NQ_TICK_SIZE: f64 = 0.25;
const ANALOG_DISTANCE_THRESHOLD: f64 = 0.35;
const REPORTABLE_SAMPLE_SIZE: usize = 30;
const TOP_K_FALLBACK: usize = 30;
const HISTORICAL_ANALOG_LIMIT: usize = 100_000;
const INTRADAY_SNAPSHOT_LIMIT: usize = 200_000;

/// Matching mode for historical context-frame sections.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum MatchingMode {
    #[default]
    WeightedAnalog,
    StrictBucket,
}

impl MatchingMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::WeightedAnalog => "weightedAnalog",
            Self::StrictBucket => "strictBucket",
        }
    }
}

/// Input options for building a context frame.
#[derive(Debug, Clone, Default)]
pub struct ContextFrameOptions {
    pub mode: ContextFrameMode,
    pub snapshot_timestamp_ms: Option<f64>,
    pub requested_timestamp_ms: Option<f64>,
    pub snapshot_distance_ms: Option<f64>,
    pub setup_id: Option<String>,
    pub include_historical: bool,
    pub matching_mode: MatchingMode,
}

/// Whether a frame is based on the live pipeline or a historical snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ContextFrameMode {
    #[default]
    Live,
    Historical,
}

/// The full context-frame response consumed by MCP agents.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextFrame {
    pub frame_kind: ContextFrameMode,
    pub live: LiveContext,
    pub buckets: ContextBuckets,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intraday_forward_stats: Option<HistoricalAnalogContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub historical_analogs: Option<HistoricalAnalogContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub setup_outcomes: Option<SetupOutcomeContext>,
    pub caveats: Vec<String>,
    pub meta: ContextFrameMeta,
}

/// Live/session-relative measurements derived from a market snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveContext {
    pub mode: ContextFrameMode,
    pub last_price: f64,
    pub vwap: f64,
    pub vwap_distance_points: f64,
    pub vwap_distance_ticks: f64,
    pub vwap_sigma: Option<f64>,
    pub vwap_relation: String,
    pub value_area_location: String,
    pub dnva_location: String,
    pub ib_state: String,
    pub rvol_ratio: f64,
    pub day_type: String,
    pub profile_shape: String,
    pub balance_state: String,
    pub session_type: String,
    pub session_segment: String,
    pub trading_day: String,
    pub root_symbol: Option<String>,
    pub contract_symbol: Option<String>,
    pub carry_forward_levels_valid: bool,
    pub rollover_warning: Option<String>,
    pub requested_timestamp_ms: Option<f64>,
    pub snapshot_timestamp_ms: Option<f64>,
    pub snapshot_distance_ms: Option<f64>,
}

/// Stable bucket tuple used for cache keys and historical analog matching.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContextBuckets {
    pub bucket_definition_version: String,
    pub vwap_sigma: String,
    pub rvol: String,
    pub time_of_day: String,
    pub ib_state: String,
    pub value_area_location: String,
    pub dnva_location: String,
    pub day_type: String,
    pub profile_shape: String,
    pub balance_state: String,
    pub session_type: String,
    pub session_segment: String,
    pub single_prints_direction: String,
}

/// Metadata that makes the frame auditable by agents.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextFrameMeta {
    pub bucket_definition_version: String,
    pub bucket_definition_blessed_at: String,
    pub decision_log_ref: String,
    pub matching_mode: String,
    pub weighted_distance_threshold: f64,
    pub top_k_fallback_size: usize,
    pub cache_key: String,
    pub cache_status: String,
    pub notes: Vec<String>,
}

/// Historical analog section derived from session summaries or intraday snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalAnalogContext {
    pub source: String,
    pub close_back_to_vwap: OutcomeProbability,
    pub close_vs_vwap_counts: OutcomeCounts,
    pub close_vs_poc_counts: OutcomeCounts,
    pub close_vs_ib_mid_counts: OutcomeCounts,
    pub analogs: Vec<AnalogMatch>,
    pub meta: AnalogMeta,
}

/// Counts for categorical session-summary outcomes.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeCounts {
    pub above: usize,
    pub below: usize,
    pub at: usize,
    pub other: usize,
}

/// Probability-style outcome with research reliability labels.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeProbability {
    pub outcome: String,
    pub favorable_count: usize,
    pub sample_size: usize,
    pub probability_pct: f64,
    pub reliability_tier: ReliabilityTier,
    pub caveat: String,
}

/// One historical analog row shown to the agent for explainability.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalogMatch {
    pub session_date: String,
    pub session_type: String,
    pub weighted_distance: f64,
    pub day_type: String,
    pub profile_shape: String,
    pub rvol_ratio: f64,
    pub close_vs_vwap: String,
    pub close_vs_poc: String,
    pub close_vs_ib_mid: String,
}

/// Matching metadata for historical analog sections.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalogMeta {
    pub matching_mode: String,
    pub weighted_distance_threshold: f64,
    pub top_k_fallback_used: bool,
    pub raw_match_count: usize,
    pub sample_size: usize,
    pub effective_sample_size: usize,
    pub reliability_tier: ReliabilityTier,
    pub rows_scanned: usize,
    pub notes: Vec<String>,
}

/// Optional setup-linked outcome context.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupOutcomeContext {
    pub setup_id: String,
    pub sample_size: usize,
    pub reliability_tier: ReliabilityTier,
    pub win_rate_pct: f64,
    pub avg_r: Option<f64>,
    pub median_r: Option<f64>,
    pub avg_mfe: Option<f64>,
    pub median_mfe: Option<f64>,
    pub avg_mae: Option<f64>,
    pub median_mae: Option<f64>,
    pub caveat: String,
}

#[derive(Debug, Clone)]
struct Candidate {
    distance: f64,
    summary: SessionSummary,
}

/// Build a context frame from a serialized market snapshot and SQLite research data.
pub fn build_context_frame(
    db: &Database,
    snapshot: &Value,
    options: ContextFrameOptions,
) -> Result<ContextFrame, String> {
    let live = live_context_from_snapshot(snapshot, &options);
    let buckets = buckets_from_snapshot(snapshot, &live, options.snapshot_timestamp_ms);
    let mut caveats = Vec::new();
    let mut notes = Vec::new();

    if !live.carry_forward_levels_valid {
        caveats.push(
            "carry-forward prior references are not authoritative for this contract; level-derived historical comparisons are same-contract only".to_string(),
        );
    }
    if live.contract_symbol.is_none() && live.root_symbol.is_none() {
        caveats.push(
            "snapshot has no symbol scope; historical analogs are suppressed to avoid cross-contract comparisons".to_string(),
        );
    }

    let scope = scope_from_live(&live);
    let allow_history = options.include_historical
        && (scope.contract_symbol.is_some() || scope.root_symbol.is_some());

    let historical_analogs = if allow_history {
        match build_session_analogs(db, &buckets, &live, &scope, &options.matching_mode) {
            Ok(Some(section)) => Some(section),
            Ok(None) => {
                caveats.push(
                    "no same-scope session summaries are available for historical analog framing"
                        .to_string(),
                );
                None
            }
            Err(err) => {
                caveats.push(format!("historical analogs unavailable: {err}"));
                None
            }
        }
    } else {
        None
    };

    let intraday_forward_stats = if allow_history {
        match build_intraday_forward_stats(db, &buckets, &live, &scope, &options.matching_mode) {
            Ok(Some(section)) => Some(section),
            Ok(None) => {
                notes.push("no intraday pipeline_snapshots matched the frame scope".to_string());
                None
            }
            Err(err) => {
                caveats.push(format!("intraday forward stats unavailable: {err}"));
                None
            }
        }
    } else {
        None
    };

    let setup_outcomes = if allow_history {
        options
            .setup_id
            .as_deref()
            .and_then(|setup_id| match build_setup_outcomes(db, setup_id, &scope) {
                Ok(Some(section)) => Some(section),
                Ok(None) => {
                    caveats.push(format!(
                        "no resolved signal outcomes are available for setup `{setup_id}` in this scope"
                    ));
                    None
                }
                Err(err) => {
                    caveats.push(format!("setup outcomes unavailable for `{setup_id}`: {err}"));
                    None
                }
            })
    } else {
        None
    };

    if historical_analogs.is_none() && intraday_forward_stats.is_none() {
        caveats.push(
            "historical framing is unavailable or insufficient; use the live buckets as context, not as an edge claim".to_string(),
        );
    }

    let cache_key = cache_key(
        &live,
        &buckets,
        options.setup_id.as_deref(),
        &options.matching_mode,
    );
    Ok(ContextFrame {
        frame_kind: live.mode.clone(),
        live,
        buckets,
        intraday_forward_stats,
        historical_analogs,
        setup_outcomes,
        caveats,
        meta: ContextFrameMeta {
            bucket_definition_version: BUCKET_DEFINITION_VERSION.to_string(),
            bucket_definition_blessed_at: BUCKET_DEFINITION_BLESSED_AT.to_string(),
            decision_log_ref: DECISION_LOG_REF.to_string(),
            matching_mode: options.matching_mode.as_str().to_string(),
            weighted_distance_threshold: ANALOG_DISTANCE_THRESHOLD,
            top_k_fallback_size: TOP_K_FALLBACK,
            cache_key,
            cache_status: "bypassed".to_string(),
            notes,
        },
    })
}

/// Build the cache key for a frame without running historical database queries.
pub fn cache_key_for_snapshot(snapshot: &Value, options: &ContextFrameOptions) -> String {
    let live = live_context_from_snapshot(snapshot, options);
    let buckets = buckets_from_snapshot(snapshot, &live, options.snapshot_timestamp_ms);
    cache_key(
        &live,
        &buckets,
        options.setup_id.as_deref(),
        &options.matching_mode,
    )
}

/// Derive denormalized bucket metadata for indexed persistence of a pipeline snapshot.
pub fn snapshot_context_buckets(snapshot: &Value, timestamp_ms: f64) -> ContextSnapshotBuckets {
    let options = ContextFrameOptions {
        mode: ContextFrameMode::Historical,
        snapshot_timestamp_ms: Some(timestamp_ms),
        include_historical: false,
        ..Default::default()
    };
    let live = live_context_from_snapshot(snapshot, &options);
    let buckets = buckets_from_snapshot(snapshot, &live, Some(timestamp_ms));
    ContextSnapshotBuckets {
        bucket_definition_version: BUCKET_DEFINITION_VERSION.to_string(),
        trading_day: live.trading_day,
        session_type: buckets.session_type,
        session_segment: buckets.session_segment,
        root_symbol: live.root_symbol,
        contract_symbol: live.contract_symbol,
        vwap_sigma: buckets.vwap_sigma,
        rvol: buckets.rvol,
        time_of_day: buckets.time_of_day,
        ib_state: buckets.ib_state,
        value_area_location: buckets.value_area_location,
        dnva_location: buckets.dnva_location,
        day_type: buckets.day_type,
        profile_shape: buckets.profile_shape,
        balance_state: buckets.balance_state,
        single_prints_direction: buckets.single_prints_direction,
    }
}

fn live_context_from_snapshot(snapshot: &Value, options: &ContextFrameOptions) -> LiveContext {
    let last_price = json_f64(snapshot, "lastPrice");
    let vwap = json_f64(snapshot, "vwap");
    let vwap_distance_points = if vwap > 0.0 { last_price - vwap } else { 0.0 };
    let vwap_distance_ticks = vwap_distance_points / NQ_TICK_SIZE;
    let vwap_sigma = vwap_sigma(snapshot, last_price, vwap);
    let vwap_relation = relation_to_level(last_price, vwap);
    let value_area_location = location_in_range(
        last_price,
        json_f64(snapshot, "vaLow"),
        json_f64(snapshot, "vaHigh"),
    );
    let dnva_location = location_in_range(
        last_price,
        json_f64(snapshot, "dnvaLow"),
        json_f64(snapshot, "dnvaHigh"),
    );
    let ib_state = ib_state(snapshot, last_price);

    LiveContext {
        mode: options.mode.clone(),
        last_price,
        vwap,
        vwap_distance_points,
        vwap_distance_ticks,
        vwap_sigma,
        vwap_relation,
        value_area_location,
        dnva_location,
        ib_state,
        rvol_ratio: json_f64(snapshot, "rvolRatio"),
        day_type: json_string(snapshot, "dayType"),
        profile_shape: json_string(snapshot, "profileShape"),
        balance_state: json_string(snapshot, "balanceState"),
        session_type: non_empty_or(json_string(snapshot, "sessionType"), "Unknown"),
        session_segment: non_empty_or(json_string(snapshot, "sessionSegment"), "None"),
        trading_day: json_string(snapshot, "tradingDay"),
        root_symbol: optional_string(snapshot, "rootSymbol"),
        contract_symbol: optional_string(snapshot, "contractSymbol"),
        carry_forward_levels_valid: snapshot
            .get("carryForwardLevelsValid")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        rollover_warning: optional_string(snapshot, "rolloverWarning"),
        requested_timestamp_ms: options.requested_timestamp_ms,
        snapshot_timestamp_ms: options.snapshot_timestamp_ms,
        snapshot_distance_ms: options.snapshot_distance_ms,
    }
}

fn buckets_from_snapshot(
    snapshot: &Value,
    live: &LiveContext,
    timestamp_ms: Option<f64>,
) -> ContextBuckets {
    ContextBuckets {
        bucket_definition_version: BUCKET_DEFINITION_VERSION.to_string(),
        vwap_sigma: vwap_sigma_bucket(live.vwap_sigma),
        rvol: rvol_bucket(live.rvol_ratio),
        time_of_day: time_of_day_bucket(timestamp_ms, &live.session_type, &live.session_segment),
        ib_state: live.ib_state.clone(),
        value_area_location: live.value_area_location.clone(),
        dnva_location: live.dnva_location.clone(),
        day_type: non_empty_or(live.day_type.clone(), "unknown"),
        profile_shape: non_empty_or(live.profile_shape.clone(), "unknown"),
        balance_state: non_empty_or(live.balance_state.clone(), "unknown"),
        session_type: live.session_type.clone(),
        session_segment: live.session_segment.clone(),
        single_prints_direction: non_empty_or(
            json_string(snapshot, "singlePrintsDirection"),
            "none",
        ),
    }
}

fn scope_from_live(live: &LiveContext) -> SessionScopeFilter {
    SessionScopeFilter {
        session_type: if live.session_type == "Unknown" {
            None
        } else {
            Some(live.session_type.clone())
        },
        session_segment: if live.session_segment == "None" {
            None
        } else {
            Some(live.session_segment.clone())
        },
        trading_day_start: None,
        trading_day_end: None,
        root_symbol: live.root_symbol.clone(),
        contract_symbol: live.contract_symbol.clone(),
        include_rollover_sessions: false,
        continuous_mode: false,
    }
}

fn build_session_analogs(
    db: &Database,
    current: &ContextBuckets,
    live: &LiveContext,
    scope: &SessionScopeFilter,
    matching_mode: &MatchingMode,
) -> Result<Option<HistoricalAnalogContext>, String> {
    let summaries = db
        .list_session_summaries_scoped(
            None,
            None,
            None,
            scope.session_type.as_deref(),
            HISTORICAL_ANALOG_LIMIT,
            Some(scope),
        )
        .map_err(|e| e.to_string())?;
    if summaries.is_empty() {
        return Ok(None);
    }
    let candidates = score_summaries(summaries, current);
    Ok(Some(analog_context_from_candidates(
        "sessionSummaries",
        candidates,
        live,
        matching_mode,
    )))
}

fn build_intraday_forward_stats(
    db: &Database,
    current: &ContextBuckets,
    live: &LiveContext,
    scope: &SessionScopeFilter,
    matching_mode: &MatchingMode,
) -> Result<Option<HistoricalAnalogContext>, String> {
    let query = snapshot_query(current, scope, matching_mode);
    let mut snapshots = db
        .list_pipeline_snapshots_for_context(&query, INTRADAY_SNAPSHOT_LIMIT)
        .map_err(|e| e.to_string())?;
    if snapshots.is_empty() && matches!(matching_mode, MatchingMode::WeightedAnalog) {
        let broad_query = broad_snapshot_query(scope);
        snapshots = db
            .list_pipeline_snapshots_for_context(&broad_query, INTRADAY_SNAPSHOT_LIMIT)
            .map_err(|e| e.to_string())?;
    }
    if snapshots.is_empty() {
        return Ok(None);
    }
    let summaries = db
        .list_session_summaries_scoped(
            None,
            None,
            None,
            scope.session_type.as_deref(),
            HISTORICAL_ANALOG_LIMIT,
            Some(scope),
        )
        .map_err(|e| e.to_string())?;
    if summaries.is_empty() {
        return Ok(None);
    }
    let mut summary_by_key = std::collections::HashMap::new();
    for summary in summaries {
        summary_by_key.insert(
            (summary.session_date.clone(), summary.session_type.clone()),
            summary,
        );
    }

    let mut candidates = Vec::new();
    for (timestamp_ms, payload) in snapshots {
        let snapshot_live = live_context_from_snapshot(
            &payload,
            &ContextFrameOptions {
                mode: ContextFrameMode::Historical,
                snapshot_timestamp_ms: Some(timestamp_ms),
                ..Default::default()
            },
        );
        if !snapshot_matches_scope(&snapshot_live, scope) {
            continue;
        }
        let key = (
            snapshot_live.trading_day.clone(),
            snapshot_live.session_type.clone(),
        );
        let Some(summary) = summary_by_key.get(&key) else {
            continue;
        };
        let buckets = buckets_from_snapshot(&payload, &snapshot_live, Some(timestamp_ms));
        candidates.push(Candidate {
            distance: analog_distance(current, &buckets),
            summary: summary.clone(),
        });
    }
    if candidates.is_empty() {
        return Ok(None);
    }
    Ok(Some(analog_context_from_candidates(
        "pipelineSnapshots",
        candidates,
        live,
        matching_mode,
    )))
}

fn snapshot_query(
    buckets: &ContextBuckets,
    scope: &SessionScopeFilter,
    matching_mode: &MatchingMode,
) -> ContextSnapshotQuery {
    let strict = matches!(matching_mode, MatchingMode::StrictBucket);
    ContextSnapshotQuery {
        bucket_definition_version: BUCKET_DEFINITION_VERSION.to_string(),
        session_type: scope.session_type.clone(),
        session_segment: scope.session_segment.clone(),
        root_symbol: scope.root_symbol.clone(),
        contract_symbol: scope.contract_symbol.clone(),
        day_type: Some(buckets.day_type.clone()),
        profile_shape: Some(buckets.profile_shape.clone()),
        vwap_sigma: strict.then(|| buckets.vwap_sigma.clone()),
        rvol: strict.then(|| buckets.rvol.clone()),
        time_of_day: strict.then(|| buckets.time_of_day.clone()),
        ib_state: strict.then(|| buckets.ib_state.clone()),
        value_area_location: strict.then(|| buckets.value_area_location.clone()),
        dnva_location: strict.then(|| buckets.dnva_location.clone()),
        balance_state: strict.then(|| buckets.balance_state.clone()),
        single_prints_direction: strict.then(|| buckets.single_prints_direction.clone()),
    }
}

fn broad_snapshot_query(scope: &SessionScopeFilter) -> ContextSnapshotQuery {
    ContextSnapshotQuery {
        bucket_definition_version: BUCKET_DEFINITION_VERSION.to_string(),
        session_type: scope.session_type.clone(),
        session_segment: scope.session_segment.clone(),
        root_symbol: scope.root_symbol.clone(),
        contract_symbol: scope.contract_symbol.clone(),
        ..Default::default()
    }
}

fn build_setup_outcomes(
    db: &Database,
    setup_id: &str,
    scope: &SessionScopeFilter,
) -> Result<Option<SetupOutcomeContext>, String> {
    let outcomes = db
        .list_signal_outcomes_for_research(Some(setup_id), None, None, Some(scope))
        .map_err(|e| e.to_string())?;
    if outcomes.is_empty() {
        return Ok(None);
    }
    let r_values: Vec<f64> = outcomes.iter().filter_map(|(_, _, r, _)| *r).collect();
    let wins = r_values.iter().filter(|r| **r > 0.0).count();
    let excursions = db
        .list_signal_outcomes_for_excursions_filtered(Some(setup_id), None, None, Some(scope))
        .map_err(|e| e.to_string())?;
    let mfe_values: Vec<f64> = excursions
        .iter()
        .filter_map(|row| row.max_favorable_excursion)
        .collect();
    let mae_values: Vec<f64> = excursions
        .iter()
        .filter_map(|row| row.max_adverse_excursion)
        .collect();
    let sample_size = outcomes.len();
    let tier = tier(sample_size);
    Ok(Some(SetupOutcomeContext {
        setup_id: setup_id.to_string(),
        sample_size,
        reliability_tier: tier.clone(),
        win_rate_pct: pct(wins, sample_size),
        avg_r: mean(&r_values),
        median_r: median(&r_values),
        avg_mfe: mean(&mfe_values),
        median_mfe: median(&mfe_values),
        avg_mae: mean(&mae_values),
        median_mae: median(&mae_values),
        caveat: reliability_caveat(sample_size, &tier),
    }))
}

fn score_summaries(summaries: Vec<SessionSummary>, current: &ContextBuckets) -> Vec<Candidate> {
    summaries
        .into_iter()
        .map(|summary| {
            let buckets = buckets_from_summary(&summary);
            Candidate {
                distance: analog_distance(current, &buckets),
                summary,
            }
        })
        .collect()
}

fn analog_context_from_candidates(
    source: &str,
    mut candidates: Vec<Candidate>,
    live: &LiveContext,
    matching_mode: &MatchingMode,
) -> HistoricalAnalogContext {
    candidates.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let raw_match_count = match matching_mode {
        MatchingMode::WeightedAnalog => candidates
            .iter()
            .filter(|c| c.distance <= ANALOG_DISTANCE_THRESHOLD)
            .count(),
        MatchingMode::StrictBucket => candidates.iter().filter(|c| c.distance == 0.0).count(),
    };
    let mut selected: Vec<&Candidate> = match matching_mode {
        MatchingMode::WeightedAnalog => candidates
            .iter()
            .filter(|c| c.distance <= ANALOG_DISTANCE_THRESHOLD)
            .collect(),
        MatchingMode::StrictBucket => candidates.iter().filter(|c| c.distance == 0.0).collect(),
    };
    let top_k_fallback_used = matches!(matching_mode, MatchingMode::WeightedAnalog)
        && selected.len() < REPORTABLE_SAMPLE_SIZE
        && candidates.len() > selected.len();
    if top_k_fallback_used {
        selected = candidates
            .iter()
            .take(TOP_K_FALLBACK.min(candidates.len()))
            .collect();
    }

    let close_vs_vwap_counts = counts_for(&selected, |s| &s.close_vs_vwap);
    let close_vs_poc_counts = counts_for(&selected, |s| &s.close_vs_poc);
    let close_vs_ib_mid_counts = counts_for(&selected, |s| &s.close_vs_ib_mid);
    let favorable = selected
        .iter()
        .filter(|candidate| {
            close_back_to_vwap_is_favorable(&live.vwap_relation, &candidate.summary.close_vs_vwap)
        })
        .count();
    let sample_size = selected.len();
    let tier = tier(sample_size);
    let analogs = selected
        .iter()
        .take(5)
        .map(|candidate| AnalogMatch {
            session_date: candidate.summary.session_date.clone(),
            session_type: candidate.summary.session_type.clone(),
            weighted_distance: round4(candidate.distance),
            day_type: candidate.summary.day_type.clone(),
            profile_shape: candidate.summary.profile_shape.clone(),
            rvol_ratio: candidate.summary.rvol_ratio,
            close_vs_vwap: candidate.summary.close_vs_vwap.clone(),
            close_vs_poc: candidate.summary.close_vs_poc.clone(),
            close_vs_ib_mid: candidate.summary.close_vs_ib_mid.clone(),
        })
        .collect();

    HistoricalAnalogContext {
        source: source.to_string(),
        close_back_to_vwap: OutcomeProbability {
            outcome: "closeBackToVwapOrThroughBySessionClose".to_string(),
            favorable_count: favorable,
            sample_size,
            probability_pct: pct(favorable, sample_size),
            reliability_tier: tier.clone(),
            caveat: reliability_caveat(sample_size, &tier),
        },
        close_vs_vwap_counts,
        close_vs_poc_counts,
        close_vs_ib_mid_counts,
        analogs,
        meta: AnalogMeta {
            matching_mode: matching_mode.as_str().to_string(),
            weighted_distance_threshold: ANALOG_DISTANCE_THRESHOLD,
            top_k_fallback_used,
            raw_match_count,
            sample_size,
            effective_sample_size: sample_size,
            reliability_tier: tier,
            rows_scanned: candidates.len(),
            notes: if top_k_fallback_used {
                vec!["threshold matches were below reportable sample size; top-K fallback used and reliability tier reflects the effective sample".to_string()]
            } else {
                Vec::new()
            },
        },
    }
}

fn buckets_from_summary(summary: &SessionSummary) -> ContextBuckets {
    if let Some(snapshot) = summary
        .snapshot_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
    {
        let live = live_context_from_snapshot(
            &snapshot,
            &ContextFrameOptions {
                mode: ContextFrameMode::Historical,
                ..Default::default()
            },
        );
        return buckets_from_snapshot(&snapshot, &live, None);
    }
    ContextBuckets {
        bucket_definition_version: BUCKET_DEFINITION_VERSION.to_string(),
        vwap_sigma: "unknown".to_string(),
        rvol: rvol_bucket(summary.rvol_ratio),
        time_of_day: "sessionClose".to_string(),
        ib_state: ib_state_from_summary(summary),
        value_area_location: location_in_range(summary.close, summary.val, summary.vah),
        dnva_location: location_in_range(summary.close, summary.dnva_low, summary.dnva_high),
        day_type: non_empty_or(summary.day_type.clone(), "unknown"),
        profile_shape: non_empty_or(summary.profile_shape.clone(), "unknown"),
        balance_state: non_empty_or(summary.balance_state.clone(), "unknown"),
        session_type: non_empty_or(summary.session_type.clone(), "Unknown"),
        session_segment: "None".to_string(),
        single_prints_direction: non_empty_or(summary.single_prints_direction.clone(), "none"),
    }
}

fn analog_distance(a: &ContextBuckets, b: &ContextBuckets) -> f64 {
    let weighted = 0.30 * categorical_distance(&a.day_type, &b.day_type)
        + 0.20 * categorical_distance(&a.profile_shape, &b.profile_shape)
        + 0.15 * bucket_ordinal_distance(&a.vwap_sigma, &b.vwap_sigma, &VWAP_SIGMA_BUCKETS)
        + 0.15 * bucket_ordinal_distance(&a.rvol, &b.rvol, &RVOL_BUCKETS)
        + 0.10 * categorical_distance(&a.ib_state, &b.ib_state)
        + 0.10 * categorical_distance(&a.single_prints_direction, &b.single_prints_direction);
    round4(weighted)
}

static VWAP_SIGMA_BUCKETS: [&str; 9] = [
    "ltNeg2",
    "neg2ToNeg1_5",
    "neg1_5ToNeg1",
    "neg1ToNeg0_5",
    "neg0_5To0_5",
    "0_5To1",
    "1To1_5",
    "1_5To2",
    "gt2",
];

static RVOL_BUCKETS: [&str; 5] = ["lt0_85", "0_85To1", "1To1_15", "1_15To1_30", "gt1_30"];

fn bucket_ordinal_distance(left: &str, right: &str, buckets: &[&str]) -> f64 {
    let li = buckets.iter().position(|v| *v == left);
    let ri = buckets.iter().position(|v| *v == right);
    match (li, ri) {
        (Some(a), Some(b)) if buckets.len() > 1 => {
            (a as f64 - b as f64).abs() / (buckets.len() - 1) as f64
        }
        _ => categorical_distance(left, right),
    }
}

fn categorical_distance(left: &str, right: &str) -> f64 {
    let left = normalize(left);
    let right = normalize(right);
    let left_unknown = left.is_empty() || left == "unknown";
    let right_unknown = right.is_empty() || right == "unknown";
    if left_unknown && right_unknown {
        1.0
    } else if left_unknown || right_unknown {
        0.5
    } else if left == right {
        0.0
    } else {
        1.0
    }
}

fn close_back_to_vwap_is_favorable(current_relation: &str, close_vs_vwap: &str) -> bool {
    match normalize(current_relation).as_str() {
        "above" => normalize(close_vs_vwap) != "above",
        "below" => normalize(close_vs_vwap) != "below",
        _ => normalize(close_vs_vwap) == "at",
    }
}

fn counts_for<F>(selected: &[&Candidate], field: F) -> OutcomeCounts
where
    F: Fn(&SessionSummary) -> &String,
{
    let mut counts = OutcomeCounts::default();
    for candidate in selected {
        match normalize(field(&candidate.summary)).as_str() {
            "above" => counts.above += 1,
            "below" => counts.below += 1,
            "at" => counts.at += 1,
            _ => counts.other += 1,
        }
    }
    counts
}

fn snapshot_matches_scope(live: &LiveContext, scope: &SessionScopeFilter) -> bool {
    if let Some(st) = scope.session_type.as_deref() {
        if live.session_type != st {
            return false;
        }
    }
    if let Some(ss) = scope.session_segment.as_deref() {
        if live.session_segment != ss {
            return false;
        }
    }
    if let Some(root) = scope.root_symbol.as_deref() {
        if live.root_symbol.as_deref() != Some(root) {
            return false;
        }
    }
    if let Some(contract) = scope.contract_symbol.as_deref() {
        if live.contract_symbol.as_deref() != Some(contract) {
            return false;
        }
    }
    true
}

fn cache_key(
    live: &LiveContext,
    buckets: &ContextBuckets,
    setup_id: Option<&str>,
    matching_mode: &MatchingMode,
) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        live.trading_day,
        live.session_type,
        live.root_symbol.as_deref().unwrap_or(""),
        live.contract_symbol.as_deref().unwrap_or(""),
        BUCKET_DEFINITION_VERSION,
        matching_mode.as_str(),
        normalize(&buckets.vwap_sigma),
        normalize(&buckets.rvol),
        normalize(&buckets.time_of_day),
        normalize(&buckets.ib_state),
        normalize(&buckets.value_area_location),
        normalize(&buckets.dnva_location),
        normalize(&buckets.day_type),
        normalize(&buckets.profile_shape),
        normalize(&buckets.balance_state),
        normalize(&buckets.single_prints_direction),
        normalize(&buckets.session_segment),
        setup_id.unwrap_or("")
    )
}

fn vwap_sigma(snapshot: &Value, last_price: f64, vwap: f64) -> Option<f64> {
    if vwap <= 0.0 {
        return None;
    }
    let sd = if last_price >= vwap {
        json_f64(snapshot, "vwap1sdUpper") - vwap
    } else {
        vwap - json_f64(snapshot, "vwap1sdLower")
    };
    if sd > 0.0 && sd.is_finite() {
        Some(round4((last_price - vwap) / sd))
    } else {
        None
    }
}

fn vwap_sigma_bucket(value: Option<f64>) -> String {
    match value {
        Some(v) if v < -2.0 => "ltNeg2",
        Some(v) if v < -1.5 => "neg2ToNeg1_5",
        Some(v) if v < -1.0 => "neg1_5ToNeg1",
        Some(v) if v < -0.5 => "neg1ToNeg0_5",
        Some(v) if v <= 0.5 => "neg0_5To0_5",
        Some(v) if v <= 1.0 => "0_5To1",
        Some(v) if v <= 1.5 => "1To1_5",
        Some(v) if v <= 2.0 => "1_5To2",
        Some(_) => "gt2",
        None => "unknown",
    }
    .to_string()
}

fn rvol_bucket(rvol: f64) -> String {
    match rvol {
        v if v > 0.0 && v < 0.85 => "lt0_85",
        v if v < 1.0 => "0_85To1",
        v if v < 1.15 => "1To1_15",
        v if v < 1.30 => "1_15To1_30",
        v if v >= 1.30 => "gt1_30",
        _ => "unknown",
    }
    .to_string()
}

fn time_of_day_bucket(
    timestamp_ms: Option<f64>,
    session_type: &str,
    session_segment: &str,
) -> String {
    if session_type == "Globex" {
        return match session_segment {
            "Asia" => "globexAsia",
            "London" => "globexLondon",
            _ => "globexOther",
        }
        .to_string();
    }
    let Some(ts) = timestamp_ms else {
        return "unknown".to_string();
    };
    let Some(ctx) = tick_time_context_from_timestamp_ms(ts) else {
        return "unknown".to_string();
    };
    match ctx.minute_of_session {
        m if m < 0 => "preRth",
        0..=29 => "rthOpeningDrive",
        30..=59 => "postOrPreIb",
        60..=149 => "postIbMorning",
        150..=239 => "lunch",
        240..=374 => "afternoon",
        _ => "mocWindow",
    }
    .to_string()
}

fn ib_state(snapshot: &Value, price: f64) -> String {
    let high = json_f64(snapshot, "ibHigh");
    let low = json_f64(snapshot, "ibLow");
    if high <= 0.0 || low <= 0.0 || high <= low {
        return "unknown".to_string();
    }
    let range = high - low;
    if (low..=high).contains(&price) {
        "insideIb"
    } else if price > high + 1.5 * range {
        "aboveIb1_5xExtension"
    } else if price > high + range {
        "aboveIb1xExtension"
    } else if price > high + 0.5 * range {
        "aboveIb0_5xExtension"
    } else if price > high {
        "aboveIb"
    } else if price < low - 1.5 * range {
        "belowIb1_5xExtension"
    } else if price < low - range {
        "belowIb1xExtension"
    } else if price < low - 0.5 * range {
        "belowIb0_5xExtension"
    } else {
        "belowIb"
    }
    .to_string()
}

fn ib_state_from_summary(summary: &SessionSummary) -> String {
    if summary.ib_high <= 0.0 || summary.ib_low <= 0.0 {
        "unknown".to_string()
    } else if summary.close > summary.ib_high {
        "aboveIb".to_string()
    } else if summary.close < summary.ib_low {
        "belowIb".to_string()
    } else {
        "insideIb".to_string()
    }
}

fn relation_to_level(price: f64, level: f64) -> String {
    if level <= 0.0 {
        "unknown".to_string()
    } else if price > level + NQ_TICK_SIZE {
        "above".to_string()
    } else if price < level - NQ_TICK_SIZE {
        "below".to_string()
    } else {
        "at".to_string()
    }
}

fn location_in_range(price: f64, low: f64, high: f64) -> String {
    if low <= 0.0 || high <= 0.0 || high < low {
        "unknown".to_string()
    } else if price > high + NQ_TICK_SIZE {
        "above".to_string()
    } else if price < low - NQ_TICK_SIZE {
        "below".to_string()
    } else {
        "inside".to_string()
    }
}

fn tier(sample_size: usize) -> ReliabilityTier {
    reliability_tier(sample_size)
}

fn reliability_caveat(sample_size: usize, tier: &ReliabilityTier) -> String {
    match tier {
        ReliabilityTier::Insufficient => format!(
            "insufficient sample (N={sample_size}); treat as context only, not a reliable edge"
        ),
        ReliabilityTier::Directional => {
            format!("directional sample (N={sample_size}); cite with caveats")
        }
        ReliabilityTier::Reportable => {
            format!("reportable sample (N={sample_size}); still scope-bound and not advisory")
        }
    }
}

fn pct(count: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        round4(count as f64 / total as f64 * 100.0)
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(round4(values.iter().sum::<f64>() / values.len() as f64))
    }
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        Some(round4((sorted[mid - 1] + sorted[mid]) / 2.0))
    } else {
        Some(round4(sorted[mid]))
    }
}

fn round4(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}

fn json_f64(value: &Value, key: &str) -> f64 {
    value.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

fn json_string(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn optional_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn non_empty_or(value: String, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.to_string()
    } else {
        value
    }
}

fn normalize(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Database, SessionSummary, SignalOutcome};
    use chrono::TimeZone;
    use chrono_tz::US::Eastern;

    fn snapshot() -> Value {
        serde_json::json!({
            "lastPrice": 21468.0,
            "vwap": 21450.0,
            "vwap1sdUpper": 21465.0,
            "vwap1sdLower": 21435.0,
            "vaHigh": 21480.0,
            "vaLow": 21420.0,
            "dnvaHigh": 21475.0,
            "dnvaLow": 21425.0,
            "ibHigh": 21460.0,
            "ibLow": 21420.0,
            "rvolRatio": 1.18,
            "dayType": "doubleDistribution",
            "profileShape": "dShape",
            "balanceState": "imbalanced",
            "singlePrintsDirection": "abovePoc",
            "sessionType": "RTH",
            "sessionSegment": "None",
            "tradingDay": "2026-03-05",
            "rootSymbol": "NQ",
            "contractSymbol": "NQM26.CME",
            "carryForwardLevelsValid": true
        })
    }

    fn summary(date: &str, close_vs_vwap: &str) -> SessionSummary {
        SessionSummary {
            session_date: date.to_string(),
            session_type: "RTH".to_string(),
            root_symbol: "NQ".to_string(),
            contract_symbol: "NQM26.CME".to_string(),
            contract_month: None,
            symbol_resolution_mode: "hybrid".to_string(),
            carry_forward_levels_valid: true,
            rollover_warning: None,
            open_price: 21400.0,
            high: 21500.0,
            low: 21350.0,
            close: 21445.0,
            poc: 21440.0,
            vah: 21480.0,
            val: 21420.0,
            ib_high: 21460.0,
            ib_low: 21420.0,
            ib_range: 40.0,
            ib_mid: 21440.0,
            or_high: 21455.0,
            or_low: 21425.0,
            day_type: "doubleDistribution".to_string(),
            profile_shape: "dShape".to_string(),
            balance_state: "imbalanced".to_string(),
            total_volume: 1_000.0,
            tick_count: 500,
            session_delta: 100.0,
            cumulative_delta: 100.0,
            dnp: 21450.0,
            dnva_high: 21475.0,
            dnva_low: 21425.0,
            vwap_close: 21450.0,
            signal_count: 0,
            single_prints_direction: "abovePoc".to_string(),
            excess_high: false,
            excess_low: false,
            poor_high: false,
            poor_low: false,
            rvol_ratio: 1.18,
            close_vs_ib_mid: "above".to_string(),
            close_vs_vwap: close_vs_vwap.to_string(),
            close_vs_poc: "above".to_string(),
            snapshot_json: None,
        }
    }

    fn resolved_outcome(signal_id: &str, r_result: f64) -> SignalOutcome {
        SignalOutcome {
            signal_id: signal_id.to_string(),
            setup_id: "or5-mid-retest".to_string(),
            setup_name: Some("OR5 Mid Retest".to_string()),
            session_date: "2026-03-05".to_string(),
            root_symbol: Some("NQ".to_string()),
            contract_symbol: Some("NQM26.CME".to_string()),
            source: "test".to_string(),
            job_id: None,
            fired_at_ms: test_timestamp_ms(),
            fired_price: 21468.0,
            target_price: Some(21490.0),
            stop_price: Some(21440.0),
            outcome: if r_result > 0.0 {
                "targetHit"
            } else {
                "stopHit"
            }
            .to_string(),
            outcome_at_ms: Some(test_timestamp_ms() + 300_000.0),
            max_favorable_excursion: Some(22.0),
            max_adverse_excursion: Some(8.0),
            r_result: Some(r_result),
            time_to_outcome_ms: Some(300_000.0),
            rvol_at_fire: Some(1.18),
            rvol_bucket_at_fire: Some(3),
        }
    }

    fn test_timestamp_ms() -> f64 {
        Eastern
            .with_ymd_and_hms(2026, 3, 5, 11, 0, 0)
            .single()
            .unwrap()
            .timestamp_millis() as f64
    }

    #[test]
    fn buckets_are_stable_for_original_vwap_context() {
        let snap = snapshot();
        let live = live_context_from_snapshot(
            &snap,
            &ContextFrameOptions {
                snapshot_timestamp_ms: Some(1_777_000_000_000.0),
                ..Default::default()
            },
        );
        let buckets = buckets_from_snapshot(&snap, &live, Some(1_777_000_000_000.0));
        assert_eq!(buckets.vwap_sigma, "1To1_5");
        assert_eq!(buckets.rvol, "1_15To1_30");
        assert_eq!(buckets.day_type, "doubleDistribution");
    }

    #[test]
    fn weighted_analog_fallback_reports_effective_sample() {
        let live = live_context_from_snapshot(&snapshot(), &ContextFrameOptions::default());
        let candidates = vec![Candidate {
            distance: 0.7,
            summary: summary("2026-03-01", "below"),
        }];
        let context = analog_context_from_candidates(
            "test",
            candidates,
            &live,
            &MatchingMode::WeightedAnalog,
        );
        assert!(context.meta.top_k_fallback_used);
        assert_eq!(context.meta.effective_sample_size, 1);
        assert_eq!(
            context.close_back_to_vwap.reliability_tier,
            ReliabilityTier::Insufficient
        );
    }

    #[test]
    fn strict_bucket_mode_does_not_use_top_k_fallback() {
        let live = live_context_from_snapshot(&snapshot(), &ContextFrameOptions::default());
        let candidates = vec![
            Candidate {
                distance: 0.0,
                summary: summary("2026-03-01", "below"),
            },
            Candidate {
                distance: 0.1,
                summary: summary("2026-03-02", "above"),
            },
        ];
        let context =
            analog_context_from_candidates("test", candidates, &live, &MatchingMode::StrictBucket);
        assert_eq!(context.meta.matching_mode, "strictBucket");
        assert!(!context.meta.top_k_fallback_used);
        assert_eq!(context.meta.effective_sample_size, 1);
    }

    #[test]
    fn unknown_categories_do_not_match_perfectly() {
        assert_eq!(categorical_distance("unknown", "unknown"), 1.0);
        assert_eq!(categorical_distance("", "unknown"), 1.0);
        assert_eq!(categorical_distance("Trend", "unknown"), 0.5);
    }

    #[test]
    fn cache_key_includes_regime_and_location_buckets() {
        let snap = snapshot();
        let options = ContextFrameOptions {
            snapshot_timestamp_ms: Some(test_timestamp_ms()),
            ..Default::default()
        };
        let base_key = cache_key_for_snapshot(&snap, &options);
        let mut changed = snap.clone();
        changed["dayType"] = serde_json::json!("trend");
        let changed_key = cache_key_for_snapshot(&changed, &options);
        assert_ne!(base_key, changed_key);

        let mut changed = snap.clone();
        changed["profileShape"] = serde_json::json!("pShape");
        let changed_key = cache_key_for_snapshot(&changed, &options);
        assert_ne!(base_key, changed_key);
    }

    #[test]
    fn rollover_ambiguity_adds_caveat_and_suppresses_history_without_symbol_scope() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();
        let mut snap = snapshot();
        snap["carryForwardLevelsValid"] = serde_json::json!(false);
        snap.as_object_mut().unwrap().remove("rootSymbol");
        snap.as_object_mut().unwrap().remove("contractSymbol");
        let frame = build_context_frame(
            &db,
            &snap,
            ContextFrameOptions {
                include_historical: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(frame.historical_analogs.is_none());
        assert!(frame.caveats.iter().any(|c| c.contains("carry-forward")));
        assert!(frame.caveats.iter().any(|c| c.contains("symbol scope")));
    }

    #[test]
    fn original_double_distribution_vwap_fixture_returns_reportable_context() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();

        for i in 0..35 {
            let date = if i < 31 {
                format!("2026-03-{:02}", i + 1)
            } else {
                format!("2026-04-{:02}", i - 30)
            };
            let close_vs_vwap = if i < 24 { "below" } else { "above" };
            db.upsert_session_summary(&summary(&date, close_vs_vwap))
                .unwrap();
        }
        let context = snapshot_context_buckets(&snapshot(), test_timestamp_ms());
        db.insert_pipeline_snapshot_with_context(test_timestamp_ms(), &snapshot(), &context)
            .unwrap();
        db.insert_signal_outcome(&resolved_outcome("sig-win", 1.2))
            .unwrap();
        db.insert_signal_outcome(&resolved_outcome("sig-loss", -0.7))
            .unwrap();

        let frame = build_context_frame(
            &db,
            &snapshot(),
            ContextFrameOptions {
                mode: ContextFrameMode::Live,
                snapshot_timestamp_ms: Some(test_timestamp_ms()),
                setup_id: Some("or5-mid-retest".to_string()),
                include_historical: true,
                ..Default::default()
            },
        )
        .unwrap();

        let analogs = frame.historical_analogs.expect("historical analogs");
        assert_eq!(analogs.meta.reliability_tier, ReliabilityTier::Reportable);
        assert_eq!(analogs.meta.effective_sample_size, 35);
        assert!(analogs.close_back_to_vwap.probability_pct > 68.0);
        assert!(frame.intraday_forward_stats.is_some());
        let setup = frame.setup_outcomes.expect("setup outcomes");
        assert_eq!(setup.sample_size, 2);
        assert_eq!(setup.reliability_tier, ReliabilityTier::Insufficient);
    }

    #[test]
    fn indexed_intraday_query_filters_before_payload_scoring() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();
        db.upsert_session_summary(&summary("2026-03-05", "below"))
            .unwrap();
        db.upsert_session_summary(&summary("2026-03-06", "above"))
            .unwrap();

        let matching = snapshot();
        let matching_context = snapshot_context_buckets(&matching, test_timestamp_ms());
        db.insert_pipeline_snapshot_with_context(test_timestamp_ms(), &matching, &matching_context)
            .unwrap();

        let mut different = snapshot();
        different["dayType"] = serde_json::json!("trend");
        different["tradingDay"] = serde_json::json!("2026-03-06");
        let different_ts = test_timestamp_ms() + 86_400_000.0;
        let different_context = snapshot_context_buckets(&different, different_ts);
        db.insert_pipeline_snapshot_with_context(different_ts, &different, &different_context)
            .unwrap();

        // Legacy rows without denormalized context should not enter the indexed query path.
        db.insert_pipeline_snapshot(test_timestamp_ms() + 1_000.0, &matching)
            .unwrap();

        let frame = build_context_frame(
            &db,
            &matching,
            ContextFrameOptions {
                mode: ContextFrameMode::Live,
                snapshot_timestamp_ms: Some(test_timestamp_ms()),
                matching_mode: MatchingMode::StrictBucket,
                include_historical: true,
                ..Default::default()
            },
        )
        .unwrap();
        let intraday = frame.intraday_forward_stats.expect("intraday stats");
        assert_eq!(intraday.meta.matching_mode, "strictBucket");
        assert_eq!(intraday.meta.rows_scanned, 1);
    }

    #[test]
    fn weighted_analog_intraday_query_narrows_by_regime_before_scoring() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();
        db.upsert_session_summary(&summary("2026-03-05", "below"))
            .unwrap();
        db.upsert_session_summary(&summary("2026-03-06", "above"))
            .unwrap();

        let matching = snapshot();
        let matching_context = snapshot_context_buckets(&matching, test_timestamp_ms());
        db.insert_pipeline_snapshot_with_context(test_timestamp_ms(), &matching, &matching_context)
            .unwrap();

        let mut different_regime = snapshot();
        different_regime["dayType"] = serde_json::json!("trend");
        different_regime["profileShape"] = serde_json::json!("pShape");
        different_regime["tradingDay"] = serde_json::json!("2026-03-06");
        let different_ts = test_timestamp_ms() + 86_400_000.0;
        let different_context = snapshot_context_buckets(&different_regime, different_ts);
        db.insert_pipeline_snapshot_with_context(
            different_ts,
            &different_regime,
            &different_context,
        )
        .unwrap();

        let frame = build_context_frame(
            &db,
            &matching,
            ContextFrameOptions {
                mode: ContextFrameMode::Live,
                snapshot_timestamp_ms: Some(test_timestamp_ms()),
                matching_mode: MatchingMode::WeightedAnalog,
                include_historical: true,
                ..Default::default()
            },
        )
        .unwrap();
        let intraday = frame.intraday_forward_stats.expect("intraday stats");
        assert_eq!(intraday.meta.matching_mode, "weightedAnalog");
        assert_eq!(intraday.meta.rows_scanned, 1);
        assert!(!intraday.meta.top_k_fallback_used);
    }

    #[test]
    fn diverse_analog_fixture_ranks_best_matches_first() {
        let live = live_context_from_snapshot(&snapshot(), &ContextFrameOptions::default());
        let mut exact = summary("2026-03-01", "below");
        exact.snapshot_json = Some(serde_json::to_string(&snapshot()).unwrap());

        let mut near = summary("2026-03-02", "below");
        let mut near_snapshot = snapshot();
        near_snapshot["rvolRatio"] = serde_json::json!(1.08);
        near.snapshot_json = Some(serde_json::to_string(&near_snapshot).unwrap());

        let mut far = summary("2026-03-03", "above");
        let mut far_snapshot = snapshot();
        far_snapshot["dayType"] = serde_json::json!("trend");
        far_snapshot["profileShape"] = serde_json::json!("pShape");
        far_snapshot["rvolRatio"] = serde_json::json!(0.7);
        far.snapshot_json = Some(serde_json::to_string(&far_snapshot).unwrap());

        let current = buckets_from_snapshot(&snapshot(), &live, Some(test_timestamp_ms()));
        let candidates = score_summaries(vec![far, near, exact], &current);
        let context = analog_context_from_candidates(
            "test",
            candidates,
            &live,
            &MatchingMode::WeightedAnalog,
        );

        assert_eq!(context.analogs[0].session_date, "2026-03-01");
        assert!(context.analogs[0].weighted_distance <= context.analogs[1].weighted_distance);
    }
}
