pub mod context_frame;
pub mod hypothesis;

use crate::db::{Database, SessionScopeFilter};
use crate::depth::DomSummary;
use serde::{Deserialize, Serialize};

/// Named percentile convention used by all research distribution queries.
///
/// This is Hyndman/Fan type 7: sort values, compute `h = (n - 1) * p`,
/// then linearly interpolate between floor(h) and ceil(h). It matches the
/// default convention used by many analytical tools for inclusive percentiles.
pub const RESEARCH_PERCENTILE_METHOD: &str = "linear_interpolation_type7";

/// Standard deviation convention used by research distribution queries.
///
/// The research tables represent the full historical population currently
/// available under the requested scope, so variance is divided by `n`.
pub const RESEARCH_STDDEV_METHOD: &str = "population";

const RESEARCH_SUMMARY_QUERY_LIMIT: usize = 100_000;
const DOM_FEATURE_RESEARCH_LIMIT: usize = 200_000;
const MARKET_EVENT_RESEARCH_LIMIT: usize = 100_000;

/// Reliability tier for historical statistics.
///
/// Keep these thresholds aligned with `AGENT.md` "Research Sample Size Policy".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ReliabilityTier {
    Insufficient,
    Directional,
    Reportable,
}

pub(crate) fn reliability_tier(sample_size: usize) -> ReliabilityTier {
    match sample_size {
        0..=19 => ReliabilityTier::Insufficient,
        20..=29 => ReliabilityTier::Directional,
        _ => ReliabilityTier::Reportable,
    }
}

/// Data-quality metadata attached to research statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResearchQueryMeta {
    pub population_size: usize,
    pub sample_size: usize,
    pub effective_sample_size: usize,
    pub scope: Option<SessionScopeFilter>,
    pub percentile_method: Option<String>,
    pub stddev_method: Option<String>,
    pub reliability_tier: ReliabilityTier,
    pub rows_scanned: usize,
    pub truncated: bool,
    pub notes: Vec<String>,
}

fn research_meta(
    population_size: usize,
    sample_size: usize,
    scope: Option<&SessionScopeFilter>,
    rows_scanned: usize,
) -> ResearchQueryMeta {
    research_meta_with_effective(
        population_size,
        sample_size,
        sample_size,
        scope,
        rows_scanned,
    )
}

fn research_meta_with_effective(
    population_size: usize,
    sample_size: usize,
    effective_sample_size: usize,
    scope: Option<&SessionScopeFilter>,
    rows_scanned: usize,
) -> ResearchQueryMeta {
    ResearchQueryMeta {
        population_size,
        sample_size,
        effective_sample_size,
        scope: scope.cloned(),
        percentile_method: None,
        stddev_method: None,
        reliability_tier: reliability_tier(effective_sample_size),
        rows_scanned,
        truncated: false,
        notes: Vec::new(),
    }
}

fn distribution_meta(
    sample_size: usize,
    scope: Option<&SessionScopeFilter>,
    rows_scanned: usize,
) -> ResearchQueryMeta {
    let mut meta = research_meta(sample_size, sample_size, scope, rows_scanned);
    meta.percentile_method = Some(RESEARCH_PERCENTILE_METHOD.to_string());
    meta.stddev_method = Some(RESEARCH_STDDEV_METHOD.to_string());
    meta
}

fn resolved_research_window<'a>(
    start_date: Option<&'a str>,
    end_date: Option<&'a str>,
    scope: Option<&'a SessionScopeFilter>,
) -> (Option<&'a str>, Option<&'a str>) {
    (
        scope
            .and_then(|s| s.trading_day_start.as_deref())
            .or(start_date),
        scope
            .and_then(|s| s.trading_day_end.as_deref())
            .or(end_date),
    )
}

fn mark_truncated(meta: &mut ResearchQueryMeta, source: &str, limit: usize) {
    meta.truncated = true;
    meta.reliability_tier = ReliabilityTier::Insufficient;
    meta.notes.push(format!(
        "{source} scan exceeded {limit} rows; result is truncated and should not be treated as reportable"
    ));
}

/// Result of a frequency query: "How often does event X happen?"
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FrequencyResult {
    pub event_type: String,
    pub total_occurrences: i64,
    pub sessions_with_event: i64,
    pub total_sessions: i64,
    pub per_session_avg: f64,
    pub pct_sessions_with_event: f64,
    pub meta: ResearchQueryMeta,
}

/// Result of a conditional probability query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConditionalResult {
    pub condition_description: String,
    pub outcome_description: String,
    pub probability: f64,
    pub sample_size: i64,
    pub condition_met_count: i64,
    pub outcome_met_count: i64,
    pub total_sessions: i64,
    pub meta: ResearchQueryMeta,
}

/// Result of a distribution query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DistributionResult {
    pub metric: String,
    pub sample_count: usize,
    pub mean: f64,
    pub median: f64,
    pub stddev: f64,
    pub min: f64,
    pub max: f64,
    pub p10: f64,
    pub p25: f64,
    pub p75: f64,
    pub p90: f64,
    pub meta: ResearchQueryMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomBehaviorFrequencyResult {
    pub behavior: String,
    pub total_occurrences: i64,
    pub sessions_with_behavior: i64,
    pub total_sessions: i64,
    pub per_session_avg: f64,
    pub pct_sessions_with_behavior: f64,
    pub avg_bias_duration_ms: f64,
    pub avg_flip_count_last_60s: f64,
    pub meta: ResearchQueryMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomBehaviorConditionalResult {
    pub behavior: String,
    pub setup_id: Option<String>,
    pub sample_count: i64,
    pub total_outcomes: i64,
    pub win_rate: f64,
    pub avg_r: f64,
    pub avg_bias_duration_ms: f64,
    pub meta: ResearchQueryMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomReactionAtLevelsResult {
    pub event_type: String,
    pub behavior: String,
    pub sample_count: i64,
    pub matched_count: i64,
    pub match_rate: f64,
    pub avg_bias_duration_ms: f64,
    pub avg_pull_stack_bias: f64,
    pub meta: ResearchQueryMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeBreakdown {
    pub target_hit: i64,
    pub stop_hit: i64,
    pub time_exit: i64,
    pub other: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalOutcomeExcursionsResult {
    pub sample_count: usize,
    pub outcome_breakdown: OutcomeBreakdown,
    pub mfe_distribution: DistributionResult,
    pub mae_distribution: DistributionResult,
    pub time_to_outcome_minutes_distribution: DistributionResult,
    pub mfe_mae_ratio_distribution: DistributionResult,
    pub meta: ResearchQueryMeta,
}

fn percentile_type7(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }

    let clamped = p.clamp(0.0, 100.0) / 100.0;
    let h = clamped * (sorted.len() - 1) as f64;
    let lower_idx = h.floor() as usize;
    let upper_idx = h.ceil() as usize;
    if lower_idx == upper_idx {
        return sorted[lower_idx];
    }
    let weight = h - lower_idx as f64;
    sorted[lower_idx] + (sorted[upper_idx] - sorted[lower_idx]) * weight
}

fn distribution_from_values_with_meta(
    metric: &str,
    values: &[f64],
    scope: Option<&SessionScopeFilter>,
) -> DistributionResult {
    let meta = distribution_meta(values.len(), scope, values.len());
    if values.is_empty() {
        return DistributionResult {
            metric: metric.to_string(),
            sample_count: 0,
            mean: 0.0,
            median: 0.0,
            stddev: 0.0,
            min: 0.0,
            max: 0.0,
            p10: 0.0,
            p25: 0.0,
            p75: 0.0,
            p90: 0.0,
            meta,
        };
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();
    let sum: f64 = sorted.iter().sum();
    let mean = sum / n as f64;
    let variance = sorted.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n as f64;
    let stddev = variance.sqrt();

    DistributionResult {
        metric: metric.to_string(),
        sample_count: n,
        mean,
        median: percentile_type7(&sorted, 50.0),
        stddev,
        min: sorted[0],
        max: sorted[n - 1],
        p10: percentile_type7(&sorted, 10.0),
        p25: percentile_type7(&sorted, 25.0),
        p75: percentile_type7(&sorted, 75.0),
        p90: percentile_type7(&sorted, 90.0),
        meta,
    }
}

#[cfg(test)]
fn distribution_from_values(metric: &str, values: &[f64]) -> DistributionResult {
    distribution_from_values_with_meta(metric, values, None)
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn parse_dom_summary(value: &serde_json::Value) -> Option<DomSummary> {
    value
        .get("domSummary")
        .cloned()
        .and_then(|summary| serde_json::from_value(summary).ok())
}

fn dom_behavior_matches(summary: &DomSummary, behavior: &str, min_duration_ms: f64) -> bool {
    match behavior {
        "bid_support_persisted" => {
            summary.liquidity_bias == "bid_support"
                && summary.current_bias_duration_ms >= min_duration_ms
        }
        "ask_resistance_persisted" => {
            summary.liquidity_bias == "ask_resistance"
                && summary.current_bias_duration_ms >= min_duration_ms
        }
        "liquidity_flip" => summary.flip_count_last_60s >= 2,
        "pulling_acceleration" => {
            summary.bid_pull_rate.max(summary.ask_pull_rate) >= 0.65
                && summary.touch_level_churn_per_minute >= 20.0
        }
        "stacking_acceleration" => {
            summary.stack_bias.abs() >= 0.35
                && summary.touch_level_churn_per_minute >= 20.0
                && summary.refill_rate.unwrap_or(0.0) >= 1.0
        }
        _ => false,
    }
}

pub fn dom_behavior_frequency(
    db: &Database,
    behavior: &str,
    min_duration_ms: f64,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<DomBehaviorFrequencyResult, String> {
    let rows = db
        .list_dom_feature_snapshots_for_research(start_date, end_date, DOM_FEATURE_RESEARCH_LIMIT)
        .map_err(|e| e.to_string())?;
    let total_sessions = db
        .list_session_summaries_scoped(start_date, end_date, None, None, 10_000, None)
        .map_err(|e| e.to_string())?
        .len() as i64;

    let mut total_occurrences = 0_i64;
    let mut sessions_with_behavior = std::collections::BTreeSet::new();
    let mut durations = Vec::new();
    let mut flips = Vec::new();
    for (trading_day, _timestamp_ms, payload) in &rows {
        let Some(summary) = parse_dom_summary(payload) else {
            continue;
        };
        if dom_behavior_matches(&summary, behavior, min_duration_ms) {
            total_occurrences += 1;
            sessions_with_behavior.insert(trading_day.clone());
            durations.push(summary.current_bias_duration_ms);
            flips.push(summary.flip_count_last_60s as f64);
        }
    }

    let sessions_with = sessions_with_behavior.len() as i64;
    let per_session_avg = if total_sessions > 0 {
        total_occurrences as f64 / total_sessions as f64
    } else {
        0.0
    };
    let pct_sessions_with_behavior = if total_sessions > 0 {
        sessions_with as f64 / total_sessions as f64 * 100.0
    } else {
        0.0
    };
    let mut meta = research_meta_with_effective(
        total_sessions.max(0) as usize,
        total_sessions.max(0) as usize,
        sessions_with.max(0) as usize,
        None,
        rows.len(),
    );
    if rows.len() >= DOM_FEATURE_RESEARCH_LIMIT {
        mark_truncated(
            &mut meta,
            "DOM feature snapshot",
            DOM_FEATURE_RESEARCH_LIMIT,
        );
    }

    Ok(DomBehaviorFrequencyResult {
        behavior: behavior.to_string(),
        total_occurrences,
        sessions_with_behavior: sessions_with,
        total_sessions,
        per_session_avg,
        pct_sessions_with_behavior,
        avg_bias_duration_ms: mean(&durations).unwrap_or(0.0),
        avg_flip_count_last_60s: mean(&flips).unwrap_or(0.0),
        meta,
    })
}

pub fn dom_behavior_conditional(
    db: &Database,
    behavior: &str,
    setup_id: Option<&str>,
    min_duration_ms: f64,
    start_date: Option<&str>,
    end_date: Option<&str>,
    scope: Option<&SessionScopeFilter>,
) -> Result<DomBehaviorConditionalResult, String> {
    let (window_start, window_end) = resolved_research_window(start_date, end_date, scope);
    let outcomes = db
        .list_signal_outcomes_with_context(setup_id, window_start, window_end, scope)
        .map_err(|e| e.to_string())?;

    let mut matched = 0_i64;
    let mut wins = 0_i64;
    let mut r_values = Vec::new();
    let mut durations = Vec::new();

    for outcome in &outcomes {
        let Some((_, payload)) = db
            .get_dom_feature_near(outcome.fired_at_ms)
            .map_err(|e| e.to_string())?
        else {
            continue;
        };
        let Some(summary) = parse_dom_summary(&payload) else {
            continue;
        };
        if !dom_behavior_matches(&summary, behavior, min_duration_ms) {
            continue;
        }
        matched += 1;
        durations.push(summary.current_bias_duration_ms);
        if let Some(r) = outcome.r_result {
            r_values.push(r);
            if r > 0.0 {
                wins += 1;
            }
        }
    }

    Ok(DomBehaviorConditionalResult {
        behavior: behavior.to_string(),
        setup_id: setup_id.map(|value| value.to_string()),
        sample_count: matched,
        total_outcomes: outcomes.len() as i64,
        win_rate: if matched > 0 {
            wins as f64 / matched as f64
        } else {
            0.0
        },
        avg_r: mean(&r_values).unwrap_or(0.0),
        avg_bias_duration_ms: mean(&durations).unwrap_or(0.0),
        meta: research_meta(
            outcomes.len(),
            matched.max(0) as usize,
            scope,
            outcomes.len(),
        ),
    })
}

pub fn dom_reaction_at_levels(
    db: &Database,
    event_type: &str,
    behavior: &str,
    min_duration_ms: f64,
    start_date: Option<&str>,
    end_date: Option<&str>,
    scope: Option<&SessionScopeFilter>,
) -> Result<DomReactionAtLevelsResult, String> {
    let (window_start, window_end) = resolved_research_window(start_date, end_date, scope);
    let events = db
        .list_market_events_for_research(
            event_type,
            window_start,
            window_end,
            scope,
            MARKET_EVENT_RESEARCH_LIMIT,
        )
        .map_err(|e| e.to_string())?;

    let mut matched_count = 0_i64;
    let mut durations = Vec::new();
    let mut pull_stack_biases = Vec::new();
    for event in &events {
        let Some(timestamp_ms) = event.get("timestampMs").and_then(|value| value.as_f64()) else {
            continue;
        };
        let Some((_, payload)) = db
            .get_dom_feature_near(timestamp_ms)
            .map_err(|e| e.to_string())?
        else {
            continue;
        };
        let Some(summary) = parse_dom_summary(&payload) else {
            continue;
        };
        if dom_behavior_matches(&summary, behavior, min_duration_ms) {
            matched_count += 1;
            durations.push(summary.current_bias_duration_ms);
            pull_stack_biases.push(summary.pull_stack_bias);
        }
    }

    let sample_count = events.len() as i64;
    let mut meta = research_meta(
        sample_count.max(0) as usize,
        matched_count.max(0) as usize,
        scope,
        events.len(),
    );
    if events.len() >= MARKET_EVENT_RESEARCH_LIMIT {
        mark_truncated(&mut meta, "market event", MARKET_EVENT_RESEARCH_LIMIT);
    }
    Ok(DomReactionAtLevelsResult {
        event_type: event_type.to_string(),
        behavior: behavior.to_string(),
        sample_count,
        matched_count,
        match_rate: if sample_count > 0 {
            matched_count as f64 / sample_count as f64
        } else {
            0.0
        },
        avg_bias_duration_ms: mean(&durations).unwrap_or(0.0),
        avg_pull_stack_bias: mean(&pull_stack_biases).unwrap_or(0.0),
        meta,
    })
}

/// "How often does event X happen per session?"
pub fn event_frequency(
    db: &Database,
    event_type: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
    scope: Option<&SessionScopeFilter>,
) -> Result<FrequencyResult, String> {
    let (window_start, window_end) = resolved_research_window(start_date, end_date, scope);
    let stats = db
        .count_events_by_type_stats(event_type, window_start, window_end, scope)
        .map_err(|e| e.to_string())?;
    let total = stats.total_occurrences;
    let sessions_with = stats.sessions_with_event;
    let total_sessions = stats.total_sessions;

    let per_session_avg = if total_sessions > 0 {
        total as f64 / total_sessions as f64
    } else {
        0.0
    };
    let pct = if total_sessions > 0 {
        sessions_with as f64 / total_sessions as f64 * 100.0
    } else {
        0.0
    };

    Ok(FrequencyResult {
        event_type: event_type.to_string(),
        total_occurrences: total,
        sessions_with_event: sessions_with,
        total_sessions,
        per_session_avg,
        pct_sessions_with_event: pct,
        meta: {
            let mut meta = research_meta_with_effective(
                total_sessions.max(0) as usize,
                total_sessions.max(0) as usize,
                sessions_with.max(0) as usize,
                scope,
                stats.rows_scanned,
            );
            if stats.truncated {
                mark_truncated(
                    &mut meta,
                    "market event frequency",
                    crate::db::RESEARCH_EVENT_SCAN_LIMIT,
                );
            }
            meta
        },
    })
}

/// "When condition A (event count >= threshold), how often is outcome B true?"
///
/// Condition: event_type occurs >= `min_count` times in a session.
/// Outcome: a field in session_summaries matches a value.
#[allow(clippy::too_many_arguments)]
pub fn conditional_probability(
    db: &Database,
    event_type: &str,
    min_count: i64,
    outcome_field: &str,
    outcome_value: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
    scope: Option<&SessionScopeFilter>,
) -> Result<ConditionalResult, String> {
    let (window_start, window_end) = resolved_research_window(start_date, end_date, scope);
    let count_stats = db
        .event_counts_per_session_context_stats(event_type, window_start, window_end, scope)
        .map_err(|e| e.to_string())?;
    let counts = count_stats.counts;

    let mut summaries = db
        .list_session_summaries_scoped(
            window_start,
            window_end,
            None,
            scope.and_then(|s| s.session_type.as_deref()),
            RESEARCH_SUMMARY_QUERY_LIMIT + 1,
            scope,
        )
        .map_err(|e| e.to_string())?;
    let truncated = summaries.len() > RESEARCH_SUMMARY_QUERY_LIMIT;
    if truncated {
        summaries.truncate(RESEARCH_SUMMARY_QUERY_LIMIT);
    }

    let summary_map: std::collections::HashMap<(String, String), &crate::db::SessionSummary> =
        summaries
            .iter()
            .map(|s| ((s.session_date.clone(), s.session_type.clone()), s))
            .collect();

    let mut condition_met = 0_i64;
    let mut outcome_met = 0_i64;
    let mut missing_outcome_sessions = 0_usize;

    for (date, session_type, count) in &counts {
        if *count >= min_count {
            if let Some(summary) = summary_map.get(&(date.clone(), session_type.clone())) {
                condition_met += 1;
                let owned: String;
                let field_val: &str = match outcome_field {
                    "close_vs_ib_mid" => &summary.close_vs_ib_mid,
                    "close_vs_vwap" => &summary.close_vs_vwap,
                    "close_vs_poc" => &summary.close_vs_poc,
                    "day_type" => &summary.day_type,
                    "profile_shape" => &summary.profile_shape,
                    "balance_state" => &summary.balance_state,
                    "single_prints_direction" => &summary.single_prints_direction,
                    "poor_high" => {
                        owned = summary.poor_high.to_string();
                        &owned
                    }
                    "poor_low" => {
                        owned = summary.poor_low.to_string();
                        &owned
                    }
                    "excess_high" => {
                        owned = summary.excess_high.to_string();
                        &owned
                    }
                    "excess_low" => {
                        owned = summary.excess_low.to_string();
                        &owned
                    }
                    _ => continue,
                };
                if field_val == outcome_value {
                    outcome_met += 1;
                }
            } else {
                missing_outcome_sessions += 1;
            }
        }
    }

    let probability = if condition_met > 0 {
        outcome_met as f64 / condition_met as f64
    } else {
        0.0
    };

    let mut meta = research_meta(
        summaries.len(),
        condition_met.max(0) as usize,
        scope,
        count_stats.rows_scanned + summaries.len(),
    );
    meta.truncated = truncated;
    if count_stats.truncated {
        mark_truncated(
            &mut meta,
            "market event conditional",
            crate::db::RESEARCH_EVENT_SCAN_LIMIT,
        );
    }
    if truncated {
        meta.notes.push(format!(
            "session summary scan exceeded {RESEARCH_SUMMARY_QUERY_LIMIT} rows; results use the first {RESEARCH_SUMMARY_QUERY_LIMIT} rows returned by the database"
        ));
    }
    if missing_outcome_sessions > 0 {
        meta.notes.push(format!(
            "{missing_outcome_sessions} condition-matched sessions had no matching session summary for outcome evaluation"
        ));
    }

    Ok(ConditionalResult {
        condition_description: format!("{event_type} >= {min_count} times per session"),
        outcome_description: format!("{outcome_field} = {outcome_value}"),
        probability,
        sample_size: condition_met,
        condition_met_count: condition_met,
        outcome_met_count: outcome_met,
        total_sessions: summaries.len() as i64,
        meta,
    })
}

/// Distribution of R-results from signal_outcomes for a setup.
/// Answers: "When setup X fires, what is the distribution of R-results?"
pub fn signal_outcome_distribution(
    db: &Database,
    setup_id: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
    scope: Option<&SessionScopeFilter>,
) -> Result<DistributionResult, String> {
    let (window_start, window_end) = resolved_research_window(start_date, end_date, scope);
    let outcomes = db
        .list_signal_outcomes_for_research(Some(setup_id), window_start, window_end, scope)
        .map_err(|e| e.to_string())?;

    let values: Vec<f64> = outcomes.into_iter().filter_map(|(_, _, r, _)| r).collect();
    Ok(distribution_from_values_with_meta(
        &format!("r_result (setup {setup_id})"),
        &values,
        scope,
    ))
}

/// Conditional win rate: when setup X fires and session has field=value, what is the win rate?
/// Joins signal_outcomes (via session_date from fired_at_ms) with session_summaries.
pub fn signal_outcome_conditional(
    db: &Database,
    setup_id: &str,
    session_field: &str,
    field_value: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
    scope: Option<&SessionScopeFilter>,
) -> Result<ConditionalResult, String> {
    let (window_start, window_end) = resolved_research_window(start_date, end_date, scope);
    let outcomes = db
        .list_signal_outcomes_for_research_with_session_key(
            Some(setup_id),
            window_start,
            window_end,
            scope,
        )
        .map_err(|e| e.to_string())?;

    let mut summaries = db
        .list_session_summaries_scoped(
            window_start,
            window_end,
            None,
            scope.and_then(|s| s.session_type.as_deref()),
            RESEARCH_SUMMARY_QUERY_LIMIT + 1,
            scope,
        )
        .map_err(|e| e.to_string())?;
    let truncated = summaries.len() > RESEARCH_SUMMARY_QUERY_LIMIT;
    if truncated {
        summaries.truncate(RESEARCH_SUMMARY_QUERY_LIMIT);
    }

    let summary_map: std::collections::HashMap<(String, String), &crate::db::SessionSummary> =
        summaries
            .iter()
            .map(|s| ((s.session_date.clone(), s.session_type.clone()), s))
            .collect();

    let mut condition_met = 0_i64;
    let mut outcome_met = 0_i64;
    let mut missing_outcome_sessions = 0_usize;

    for outcome in &outcomes {
        if let Some(summary) =
            summary_map.get(&(outcome.analysis_day.clone(), outcome.session_type.clone()))
        {
            let field_val: &str = match session_field {
                "day_type" => &summary.day_type,
                "profile_shape" => &summary.profile_shape,
                "balance_state" => &summary.balance_state,
                "close_vs_ib_mid" => &summary.close_vs_ib_mid,
                "close_vs_vwap" => &summary.close_vs_vwap,
                "single_prints_direction" => &summary.single_prints_direction,
                _ => continue,
            };
            if field_val != field_value {
                continue;
            }
            condition_met += 1;
            if let Some(r) = outcome.r_result {
                if r > 0.0 {
                    outcome_met += 1;
                }
            }
        } else {
            missing_outcome_sessions += 1;
        }
    }

    let probability = if condition_met > 0 {
        outcome_met as f64 / condition_met as f64
    } else {
        0.0
    };

    let mut meta = research_meta(
        summaries.len(),
        condition_met.max(0) as usize,
        scope,
        outcomes.len() + summaries.len(),
    );
    meta.truncated = truncated;
    if truncated {
        meta.notes.push(format!(
            "session summary scan exceeded {RESEARCH_SUMMARY_QUERY_LIMIT} rows; results use the first {RESEARCH_SUMMARY_QUERY_LIMIT} rows returned by the database"
        ));
    }
    if missing_outcome_sessions > 0 {
        meta.notes.push(format!(
            "{missing_outcome_sessions} signal outcomes had no matching session summary for conditional evaluation"
        ));
    }

    Ok(ConditionalResult {
        condition_description: format!(
            "setup {setup_id} fires, session {session_field} = {field_value}"
        ),
        outcome_description: "r_result > 0 (win)".to_string(),
        probability,
        sample_size: condition_met,
        condition_met_count: condition_met,
        outcome_met_count: outcome_met,
        total_sessions: summaries.len() as i64,
        meta,
    })
}

/// Distribution diagnostics for setup outcome excursions (MFE/MAE/time-to-outcome).
pub fn signal_outcome_excursions(
    db: &Database,
    setup_id: Option<&str>,
    start_date: Option<&str>,
    end_date: Option<&str>,
    scope: Option<&SessionScopeFilter>,
) -> Result<SignalOutcomeExcursionsResult, String> {
    let (window_start, window_end) = resolved_research_window(start_date, end_date, scope);
    let rows = db
        .list_signal_outcomes_for_excursions_filtered(setup_id, window_start, window_end, scope)
        .map_err(|e| e.to_string())?;

    let mut target_hit = 0_i64;
    let mut stop_hit = 0_i64;
    let mut time_exit = 0_i64;
    let mut other = 0_i64;

    let mut mfe_values = Vec::new();
    let mut mae_values = Vec::new();
    let mut time_minutes = Vec::new();
    let mut mfe_mae_ratio = Vec::new();

    for row in &rows {
        match row.outcome.as_str() {
            "target_hit" => target_hit += 1,
            "stop_hit" => stop_hit += 1,
            "time_exit" => time_exit += 1,
            _ => other += 1,
        }

        if let Some(v) = row.max_favorable_excursion {
            mfe_values.push(v);
        }
        if let Some(v) = row.max_adverse_excursion {
            mae_values.push(v);
        }
        if let Some(ms) = row.time_to_outcome_ms {
            time_minutes.push(ms / 60_000.0);
        }
        if let (Some(mfe), Some(mae)) = (row.max_favorable_excursion, row.max_adverse_excursion) {
            if mae.abs() > 1e-9 {
                mfe_mae_ratio.push(mfe / mae.abs());
            }
        }
    }

    Ok(SignalOutcomeExcursionsResult {
        sample_count: rows.len(),
        outcome_breakdown: OutcomeBreakdown {
            target_hit,
            stop_hit,
            time_exit,
            other,
        },
        mfe_distribution: distribution_from_values_with_meta(
            "max_favorable_excursion",
            &mfe_values,
            scope,
        ),
        mae_distribution: distribution_from_values_with_meta(
            "max_adverse_excursion",
            &mae_values,
            scope,
        ),
        time_to_outcome_minutes_distribution: distribution_from_values_with_meta(
            "time_to_outcome_minutes",
            &time_minutes,
            scope,
        ),
        mfe_mae_ratio_distribution: distribution_from_values_with_meta(
            "mfe_mae_ratio",
            &mfe_mae_ratio,
            scope,
        ),
        meta: research_meta(rows.len(), rows.len(), scope, rows.len()),
    })
}

/// Win-rate breakdown for a setup grouped by RVOL regime at signal fire time.
///
/// Buckets: Low (<0.85), Normal (0.85–1.0), Elevated (1.0–1.15), High (>1.15).
/// Returns a `ConditionalResult` for each regime that has at least one observation.
pub fn signal_outcome_by_rvol_regime(
    db: &Database,
    setup_id: Option<&str>,
    start_date: Option<&str>,
    end_date: Option<&str>,
    scope: Option<&SessionScopeFilter>,
) -> Result<Vec<(String, ConditionalResult)>, String> {
    let regimes: &[(&str, f64, f64)] = &[
        ("Low", 0.0, 0.85),
        ("Normal", 0.85, 1.0),
        ("Elevated", 1.0, 1.15),
        ("High", 1.15, f64::INFINITY),
    ];

    let (window_start, window_end) = resolved_research_window(start_date, end_date, scope);
    let outcomes = db
        .list_signal_outcomes_with_rvol(setup_id, window_start, window_end, scope)
        .map_err(|e| e.to_string())?;

    let mut results = Vec::new();
    for (label, lo, hi) in regimes {
        let in_regime: Vec<_> = outcomes
            .iter()
            .filter(|(rvol, _r, _)| *rvol >= *lo && *rvol < *hi)
            .collect();
        if in_regime.is_empty() {
            continue;
        }
        let total = in_regime.len() as i64;
        let wins = in_regime
            .iter()
            .filter(|(_, r, _)| r.map(|v| v > 0.0).unwrap_or(false))
            .count() as i64;
        let probability = if total > 0 {
            wins as f64 / total as f64
        } else {
            0.0
        };
        results.push((
            label.to_string(),
            ConditionalResult {
                condition_description: format!(
                    "setup fires with RVOL in {label} regime ({lo:.2}–{hi_label})",
                    hi_label = if hi.is_infinite() {
                        "∞".to_string()
                    } else {
                        format!("{hi:.2}")
                    }
                ),
                outcome_description: "r_result > 0 (win)".to_string(),
                probability,
                sample_size: total,
                condition_met_count: total,
                outcome_met_count: wins,
                total_sessions: outcomes.len() as i64,
                meta: research_meta(outcomes.len(), total.max(0) as usize, scope, outcomes.len()),
            },
        ));
    }
    Ok(results)
}

/// Distribution of a numeric metric from session_summaries.
pub fn metric_distribution(
    db: &Database,
    metric: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
    scope: Option<&SessionScopeFilter>,
) -> Result<DistributionResult, String> {
    let (summary_start, summary_end) = resolved_research_window(start_date, end_date, scope);
    let values = db
        .metric_values_scoped(
            metric,
            summary_start,
            summary_end,
            scope.and_then(|s| s.session_type.as_deref()),
            scope,
        )
        .map_err(|e| e.to_string())?;
    Ok(distribution_from_values_with_meta(metric, &values, scope))
}

/// User-configurable weights for multi-dimensional session similarity.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SimilarityWeights {
    pub ib_range: f64,
    pub day_type: f64,
    pub profile_shape: f64,
    pub balance_state: f64,
    pub rvol_ratio: f64,
    pub session_delta_sign: f64,
    pub single_prints_direction: f64,
}

impl Default for SimilarityWeights {
    fn default() -> Self {
        Self {
            ib_range: 1.0,
            day_type: 0.8,
            profile_shape: 0.6,
            balance_state: 0.6,
            rvol_ratio: 0.5,
            session_delta_sign: 0.4,
            single_prints_direction: 0.3,
        }
    }
}

/// Query for multi-dimensional session similarity.
#[derive(Debug, Clone, Default)]
pub struct SessionSimilarityQuery {
    pub ib_range: Option<f64>,
    pub day_type: Option<String>,
    pub profile_shape: Option<String>,
    pub balance_state: Option<String>,
    pub rvol_ratio: Option<f64>,
    pub session_delta_sign: Option<String>, // "positive" | "negative" | "neutral"
    pub single_prints_direction: Option<String>,
    pub weights: SimilarityWeights,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSimilarityResult {
    pub results: Vec<serde_json::Value>,
    pub meta: ResearchQueryMeta,
}

/// Compare today's session against similar historical sessions using weighted multi-dimensional similarity.
pub fn compare_sessions(
    db: &Database,
    current_ib_range: f64,
    current_day_type: Option<&str>,
    max_results: usize,
) -> Result<Vec<serde_json::Value>, String> {
    compare_sessions_multi(
        db,
        &SessionSimilarityQuery {
            ib_range: Some(current_ib_range),
            day_type: current_day_type.map(String::from),
            ..Default::default()
        },
        max_results,
    )
}

/// Multi-dimensional session similarity: weighted Euclidean distance for continuous,
/// exact-match penalty for categorical dimensions.
pub fn compare_sessions_multi(
    db: &Database,
    query: &SessionSimilarityQuery,
    max_results: usize,
) -> Result<Vec<serde_json::Value>, String> {
    Ok(compare_sessions_multi_with_meta(db, query, max_results)?.results)
}

/// Multi-dimensional session similarity with research metadata.
pub fn compare_sessions_multi_with_meta(
    db: &Database,
    query: &SessionSimilarityQuery,
    max_results: usize,
) -> Result<SessionSimilarityResult, String> {
    let summaries = db
        .list_session_summaries_scoped(None, None, None, None, 500, None)
        .map_err(|e| e.to_string())?;

    let filtered: Vec<&crate::db::SessionSummary> =
        summaries.iter().filter(|s| s.ib_range > 0.0).collect();

    if filtered.is_empty() {
        return Ok(SessionSimilarityResult {
            results: Vec::new(),
            meta: research_meta(0, 0, None, summaries.len()),
        });
    }

    let w = &query.weights;

    // Compute ranges for normalization (avoid div by zero)
    let ib_ranges: Vec<f64> = filtered.iter().map(|s| s.ib_range).collect();
    let rvol_ratios: Vec<f64> = filtered
        .iter()
        .map(|s| {
            if s.rvol_ratio > 0.0 {
                s.rvol_ratio
            } else {
                1.0
            }
        })
        .collect();
    let ib_range_max = ib_ranges.iter().cloned().fold(0.0_f64, f64::max);
    let ib_range_min = ib_ranges.iter().cloned().fold(f64::INFINITY, f64::min);
    let ib_range_span = (ib_range_max - ib_range_min).max(1.0);
    let rvol_max = rvol_ratios.iter().cloned().fold(0.0_f64, f64::max);
    let rvol_min = rvol_ratios.iter().cloned().fold(f64::INFINITY, f64::min);
    let rvol_span = (rvol_max - rvol_min).max(0.1);

    let current_ib = query.ib_range.unwrap_or(0.0);
    let current_day = query.day_type.as_deref().unwrap_or("");
    let current_profile = query.profile_shape.as_deref().unwrap_or("");
    let current_balance = query.balance_state.as_deref().unwrap_or("");
    let current_rvol = query.rvol_ratio.unwrap_or(1.0);
    let current_delta_sign = query.session_delta_sign.as_deref().unwrap_or("");
    let current_single_prints = query.single_prints_direction.as_deref().unwrap_or("");

    let mut scored: Vec<(f64, &crate::db::SessionSummary)> = filtered
        .iter()
        .map(|s| {
            let delta_sign = if s.session_delta > 0.5 {
                "positive"
            } else if s.session_delta < -0.5 {
                "negative"
            } else {
                "neutral"
            };

            let ib_norm = ((s.ib_range - current_ib).abs() / ib_range_span) * w.ib_range;
            let day_penalty = if current_day.is_empty() || s.day_type == current_day {
                0.0
            } else {
                w.day_type
            };
            let profile_penalty =
                if current_profile.is_empty() || s.profile_shape == current_profile {
                    0.0
                } else {
                    w.profile_shape
                };
            let balance_penalty =
                if current_balance.is_empty() || s.balance_state == current_balance {
                    0.0
                } else {
                    w.balance_state
                };
            let rvol_norm = ((s.rvol_ratio - current_rvol).abs() / rvol_span) * w.rvol_ratio;
            let delta_sign_penalty =
                if current_delta_sign.is_empty() || delta_sign == current_delta_sign {
                    0.0
                } else {
                    w.session_delta_sign
                };
            let single_prints_penalty = if current_single_prints.is_empty()
                || s.single_prints_direction == current_single_prints
            {
                0.0
            } else {
                w.single_prints_direction
            };

            let score = (ib_norm * ib_norm
                + day_penalty * day_penalty
                + profile_penalty * profile_penalty
                + balance_penalty * balance_penalty
                + rvol_norm * rvol_norm
                + delta_sign_penalty * delta_sign_penalty
                + single_prints_penalty * single_prints_penalty)
                .sqrt();

            (score, *s)
        })
        .collect();

    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    let results: Vec<serde_json::Value> = scored
        .into_iter()
        .take(max_results)
        .map(|(score, s)| {
            serde_json::json!({
                "sessionDate": s.session_date,
                "similarityScore": score,
                "ibRange": s.ib_range,
                "dayType": s.day_type,
                "profileShape": s.profile_shape,
                "balanceState": s.balance_state,
                "rvolRatio": s.rvol_ratio,
                "sessionDelta": s.session_delta,
                "singlePrintsDirection": s.single_prints_direction,
                "close": s.close,
                "closeVsIbMid": s.close_vs_ib_mid,
                "closeVsVwap": s.close_vs_vwap,
                "high": s.high,
                "low": s.low,
            })
        })
        .collect();
    let mut meta = research_meta(filtered.len(), results.len(), None, summaries.len());
    meta.notes.push(format!(
        "session similarity considered {} eligible sessions and returned at most {max_results}",
        filtered.len()
    ));
    if summaries.len() >= 500 {
        mark_truncated(&mut meta, "session similarity summary", 500);
    }

    Ok(SessionSimilarityResult { results, meta })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{SessionSummary, SignalOutcome};
    use crate::depth::DomSummary;
    use crate::pipelines::event_detector::MarketEvent;
    use chrono::TimeZone;
    use chrono_tz::US::Eastern;

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    fn summary(
        session_date: &str,
        session_type: &str,
        ib_range: f64,
        close_vs_ib_mid: &str,
    ) -> SessionSummary {
        SessionSummary {
            session_date: session_date.into(),
            session_type: session_type.into(),
            root_symbol: "NQ".into(),
            contract_symbol: "NQH26.CME".into(),
            contract_month: Some("H26".into()),
            symbol_resolution_mode: "test".into(),
            carry_forward_levels_valid: true,
            rollover_warning: None,
            open_price: 21000.0,
            high: 21000.0 + ib_range,
            low: 21000.0,
            close: 21000.0 + ib_range / 2.0,
            poc: 21010.0,
            vah: 21020.0,
            val: 21000.0,
            ib_high: 21000.0 + ib_range,
            ib_low: 21000.0,
            ib_range,
            ib_mid: 21000.0 + ib_range / 2.0,
            or_high: 21005.0,
            or_low: 20995.0,
            day_type: if ib_range >= 40.0 { "Trend" } else { "Normal" }.into(),
            profile_shape: "D".into(),
            balance_state: "balanced".into(),
            total_volume: 1000.0 + ib_range,
            tick_count: 1000,
            session_delta: ib_range * 10.0,
            cumulative_delta: ib_range * 10.0,
            dnp: 21010.0,
            dnva_high: 21020.0,
            dnva_low: 21000.0,
            vwap_close: 21012.0,
            signal_count: 0,
            single_prints_direction: String::new(),
            excess_high: false,
            excess_low: false,
            poor_high: false,
            poor_low: false,
            rvol_ratio: 1.0,
            close_vs_ib_mid: close_vs_ib_mid.into(),
            close_vs_vwap: "above".into(),
            close_vs_poc: "above".into(),
            snapshot_json: None,
        }
    }

    fn event(
        session_date: &str,
        timestamp_ms: f64,
        session_type: &str,
        segment: &str,
        trading_day: &str,
    ) -> MarketEvent {
        MarketEvent {
            session_date: session_date.into(),
            timestamp_ms,
            event_type: "ib_mid_test".into(),
            level_name: Some("ib_mid".into()),
            price: 21010.0,
            direction: Some("from_below".into()),
            sequence_num: None,
            metadata: None,
            session_type: session_type.into(),
            session_segment: segment.into(),
            trading_day: trading_day.into(),
        }
    }

    fn eastern_ms(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> f64 {
        Eastern
            .with_ymd_and_hms(year, month, day, hour, minute, 0)
            .unwrap()
            .timestamp_millis() as f64
    }

    fn signal_outcome(
        signal_id: &str,
        setup_id: &str,
        session_date: &str,
        fired_at_ms: f64,
        r_result: f64,
    ) -> SignalOutcome {
        SignalOutcome {
            signal_id: signal_id.into(),
            setup_id: setup_id.into(),
            setup_name: Some("Setup".into()),
            session_date: session_date.into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: Some("NQH26.CME".into()),
            source: "backtest".into(),
            job_id: None,
            fired_at_ms,
            fired_price: 21000.0,
            target_price: Some(21010.0),
            stop_price: Some(20990.0),
            outcome: "target_hit".into(),
            outcome_at_ms: Some(fired_at_ms + 60_000.0),
            max_favorable_excursion: Some(12.0),
            max_adverse_excursion: Some(4.0),
            r_result: Some(r_result),
            time_to_outcome_ms: Some(60_000.0),
            rvol_at_fire: Some(1.05),
            rvol_bucket_at_fire: Some(2),
        }
    }

    fn dom_payload(liquidity_bias: &str, duration_ms: f64) -> serde_json::Value {
        let summary = DomSummary {
            liquidity_bias: liquidity_bias.into(),
            current_bias_duration_ms: duration_ms,
            flip_count_last_60s: 0,
            context_confidence: "test".into(),
            liquidity_narrative: "test".into(),
            ..Default::default()
        };
        serde_json::json!({ "domSummary": summary })
    }

    fn seed_research_fixture(db: &Database) {
        for row in [
            summary("2026-03-02", "RTH", 20.0, "above"),
            summary("2026-03-03", "RTH", 30.0, "below"),
            summary("2026-03-04", "RTH", 40.0, "above"),
            summary("2026-03-05", "RTH", 50.0, "below"),
        ] {
            db.upsert_session_summary(&row).unwrap();
        }

        db.insert_market_events_batch(&[
            event("2026-03-02", 1.0, "RTH", "None", "2026-03-02"),
            event("2026-03-02", 2.0, "RTH", "None", "2026-03-02"),
            event("2026-03-02", 2.0, "RTH", "None", "2026-03-02"),
            event("2026-03-03", 3.0, "RTH", "None", "2026-03-03"),
            event("2026-03-04", 4.0, "RTH", "None", "2026-03-04"),
            event("2026-03-04", 5.0, "RTH", "None", "2026-03-04"),
            event("2026-03-04", 6.0, "RTH", "None", "2026-03-04"),
        ])
        .unwrap();
    }

    #[test]
    fn distribution_handles_empty() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();
        let result = metric_distribution(&db, "ib_range", None, None, None).unwrap();
        assert_eq!(result.sample_count, 0);
    }

    #[test]
    fn distribution_rejects_unsupported_metric_at_research_boundary() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();
        let err = metric_distribution(&db, "not_a_metric", None, None, None).unwrap_err();
        assert!(err.contains("unsupported session_summaries metric"));
    }

    #[test]
    fn distribution_uses_type7_percentiles_and_population_stddev() {
        let result = distribution_from_values("fixture", &[10.0, 20.0, 30.0, 40.0]);

        assert_eq!(result.sample_count, 4);
        assert_close(result.mean, 25.0);
        assert_close(result.median, 25.0);
        assert_close(result.stddev, 125.0_f64.sqrt());
        assert_close(result.p10, 13.0);
        assert_close(result.p25, 17.5);
        assert_close(result.p75, 32.5);
        assert_close(result.p90, 37.0);
        assert_eq!(
            result.meta.percentile_method.as_deref(),
            Some(RESEARCH_PERCENTILE_METHOD)
        );
        assert_eq!(
            result.meta.stddev_method.as_deref(),
            Some(RESEARCH_STDDEV_METHOD)
        );
    }

    #[test]
    fn golden_frequency_conditional_and_distribution_match_known_fixture() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();
        seed_research_fixture(&db);

        let frequency = event_frequency(&db, "ib_mid_test", None, None, None).unwrap();
        assert_eq!(frequency.total_occurrences, 6);
        assert_eq!(frequency.sessions_with_event, 3);
        assert_eq!(frequency.total_sessions, 4);
        assert_close(frequency.per_session_avg, 1.5);
        assert_close(frequency.pct_sessions_with_event, 75.0);
        assert_eq!(frequency.meta.population_size, 4);
        assert_eq!(
            frequency.meta.reliability_tier,
            ReliabilityTier::Insufficient
        );

        let conditional = conditional_probability(
            &db,
            "ib_mid_test",
            2,
            "close_vs_ib_mid",
            "above",
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(conditional.sample_size, 2);
        assert_eq!(conditional.condition_met_count, 2);
        assert_eq!(conditional.outcome_met_count, 2);
        assert_close(conditional.probability, 1.0);
        assert!(conditional.meta.notes.is_empty());

        let distribution = metric_distribution(&db, "ib_range", None, None, None).unwrap();
        assert_eq!(distribution.sample_count, 4);
        assert_close(distribution.mean, 35.0);
        assert_close(distribution.median, 35.0);
        assert_close(distribution.p25, 27.5);
        assert_close(distribution.p75, 42.5);
    }

    #[test]
    fn scoped_event_aggregation_respects_session_context_and_rollover_filter() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();

        db.upsert_session_summary(&summary("2026-03-06", "RTH", 20.0, "above"))
            .unwrap();
        db.upsert_session_summary(&summary("2026-03-06", "Globex", 12.0, "above"))
            .unwrap();
        let mut invalid_roll = summary("2026-03-07", "RTH", 24.0, "below");
        invalid_roll.carry_forward_levels_valid = false;
        invalid_roll.rollover_warning = Some("test rollover mismatch".into());
        db.upsert_session_summary(&invalid_roll).unwrap();

        db.insert_market_events_batch(&[
            event("2026-03-06", 10.0, "RTH", "None", "2026-03-06"),
            event("2026-03-06", 11.0, "Globex", "Asia", "2026-03-06"),
            event("2026-03-06", 12.0, "Globex", "London", "2026-03-06"),
            event("2026-03-07", 13.0, "RTH", "None", "2026-03-07"),
        ])
        .unwrap();

        let asia_scope = SessionScopeFilter {
            session_type: Some("Globex".into()),
            session_segment: Some("Asia".into()),
            include_rollover_sessions: true,
            ..Default::default()
        };
        let asia_counts = db
            .event_counts_per_session_context("ib_mid_test", None, None, Some(&asia_scope))
            .unwrap();
        assert_eq!(asia_counts, vec![("2026-03-06".into(), "Globex".into(), 1)]);

        let rth_scope = SessionScopeFilter {
            session_type: Some("RTH".into()),
            include_rollover_sessions: true,
            ..Default::default()
        };
        let rth_frequency =
            event_frequency(&db, "ib_mid_test", None, None, Some(&rth_scope)).unwrap();
        assert_eq!(rth_frequency.total_occurrences, 2);
        assert_eq!(rth_frequency.sessions_with_event, 2);
        assert_eq!(rth_frequency.total_sessions, 2);

        let no_rollover_scope = SessionScopeFilter {
            include_rollover_sessions: false,
            ..rth_scope
        };
        let filtered_frequency =
            event_frequency(&db, "ib_mid_test", None, None, Some(&no_rollover_scope)).unwrap();
        assert_eq!(filtered_frequency.total_occurrences, 1);
        assert_eq!(filtered_frequency.sessions_with_event, 1);
        assert_eq!(filtered_frequency.total_sessions, 1);
    }

    #[test]
    fn conditional_probability_joins_globex_event_by_trading_day_not_storage_date() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();

        db.upsert_session_summary(&summary("2026-03-06", "Globex", 40.0, "above"))
            .unwrap();
        db.insert_market_events_batch(&[event(
            "2026-03-05",
            eastern_ms(2026, 3, 5, 19, 0),
            "Globex",
            "Asia",
            "2026-03-06",
        )])
        .unwrap();

        let scope = SessionScopeFilter {
            session_type: Some("Globex".into()),
            trading_day_start: Some("2026-03-06".into()),
            trading_day_end: Some("2026-03-06".into()),
            include_rollover_sessions: true,
            ..Default::default()
        };
        let result = conditional_probability(
            &db,
            "ib_mid_test",
            1,
            "day_type",
            "Trend",
            None,
            None,
            Some(&scope),
        )
        .unwrap();
        assert_eq!(result.sample_size, 1);
        assert_eq!(result.outcome_met_count, 1);
        assert!(result.meta.notes.is_empty());
    }

    #[test]
    fn signal_outcome_conditional_uses_compound_session_key() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();

        let mut rth = summary("2026-03-08", "RTH", 40.0, "above");
        rth.close_vs_vwap = "above".into();
        let mut globex = summary("2026-03-08", "Globex", 20.0, "below");
        globex.close_vs_vwap = "below".into();
        db.upsert_session_summary(&rth).unwrap();
        db.upsert_session_summary(&globex).unwrap();
        db.insert_signal_outcome(&signal_outcome(
            "sig-rth",
            "setup-rth",
            "2026-03-08",
            eastern_ms(2026, 3, 8, 10, 0),
            1.0,
        ))
        .unwrap();

        let result = signal_outcome_conditional(
            &db,
            "setup-rth",
            "close_vs_vwap",
            "above",
            Some("2026-03-08"),
            Some("2026-03-08"),
            None,
        )
        .unwrap();
        assert_eq!(result.sample_size, 1);
        assert_eq!(result.outcome_met_count, 1);
    }

    #[test]
    fn signal_outcome_conditional_joins_globex_outcome_by_trading_day() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();

        db.upsert_session_summary(&summary("2026-03-06", "Globex", 40.0, "above"))
            .unwrap();
        db.upsert_session_summary(&summary("2026-03-06", "RTH", 20.0, "below"))
            .unwrap();
        db.insert_signal_outcome(&signal_outcome(
            "sig-globex",
            "setup-globex",
            "2026-03-05",
            eastern_ms(2026, 3, 5, 19, 30),
            1.0,
        ))
        .unwrap();

        let scope = SessionScopeFilter {
            session_type: Some("Globex".into()),
            trading_day_start: Some("2026-03-06".into()),
            trading_day_end: Some("2026-03-06".into()),
            include_rollover_sessions: true,
            ..Default::default()
        };
        let result = signal_outcome_conditional(
            &db,
            "setup-globex",
            "day_type",
            "Trend",
            None,
            None,
            Some(&scope),
        )
        .unwrap();
        assert_eq!(result.sample_size, 1);
        assert_eq!(result.outcome_met_count, 1);
        assert!(result.meta.notes.is_empty());
    }

    #[test]
    fn dom_research_results_include_top_level_metadata() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();
        db.upsert_session_summary(&summary("2026-03-09", "RTH", 30.0, "above"))
            .unwrap();
        db.insert_dom_feature_snapshot(
            "NQ.depth",
            eastern_ms(2026, 3, 9, 10, 0),
            "2026-03-09",
            &dom_payload("bid_support", 2_000.0),
        )
        .unwrap();

        let result =
            dom_behavior_frequency(&db, "bid_support_persisted", 1_000.0, None, None).unwrap();
        assert_eq!(result.total_occurrences, 1);
        assert_eq!(result.sessions_with_behavior, 1);
        assert_eq!(result.meta.rows_scanned, 1);
        assert_eq!(result.meta.effective_sample_size, 1);
    }

    #[test]
    fn session_similarity_returns_metadata_wrapper() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();
        db.upsert_session_summary(&summary("2026-03-10", "RTH", 20.0, "above"))
            .unwrap();
        db.upsert_session_summary(&summary("2026-03-11", "RTH", 40.0, "below"))
            .unwrap();

        let result = compare_sessions_multi_with_meta(
            &db,
            &SessionSimilarityQuery {
                ib_range: Some(30.0),
                ..Default::default()
            },
            1,
        )
        .unwrap();
        assert_eq!(result.results.len(), 1);
        assert_eq!(result.meta.population_size, 2);
        assert_eq!(result.meta.sample_size, 1);
        assert_eq!(result.meta.rows_scanned, 2);
    }

    #[test]
    fn excursions_query_computes_breakdown_and_distributions() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();
        db.insert_signal_outcome(&SignalOutcome {
            signal_id: "e1".into(),
            setup_id: "s1".into(),
            setup_name: Some("Setup 1".into()),
            session_date: "2026-03-04".into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: Some("NQH26.CME".into()),
            source: "live".into(),
            job_id: None,
            fired_at_ms: 1_000.0,
            fired_price: 21000.0,
            target_price: Some(21010.0),
            stop_price: Some(20990.0),
            outcome: "target_hit".into(),
            outcome_at_ms: Some(2_000.0),
            max_favorable_excursion: Some(15.0),
            max_adverse_excursion: Some(5.0),
            r_result: Some(1.0),
            time_to_outcome_ms: Some(60_000.0),
            rvol_at_fire: None,
            rvol_bucket_at_fire: None,
        })
        .unwrap();
        db.insert_signal_outcome(&SignalOutcome {
            signal_id: "e2".into(),
            setup_id: "s1".into(),
            setup_name: Some("Setup 1".into()),
            session_date: "2026-03-04".into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: Some("NQH26.CME".into()),
            source: "live".into(),
            job_id: None,
            fired_at_ms: 3_000.0,
            fired_price: 21000.0,
            target_price: Some(21010.0),
            stop_price: Some(20990.0),
            outcome: "time_exit".into(),
            outcome_at_ms: Some(4_000.0),
            max_favorable_excursion: Some(6.0),
            max_adverse_excursion: Some(3.0),
            r_result: Some(0.2),
            time_to_outcome_ms: Some(120_000.0),
            rvol_at_fire: None,
            rvol_bucket_at_fire: None,
        })
        .unwrap();

        let result = signal_outcome_excursions(&db, Some("s1"), None, None, None).unwrap();
        assert_eq!(result.sample_count, 2);
        assert_eq!(result.outcome_breakdown.target_hit, 1);
        assert_eq!(result.outcome_breakdown.time_exit, 1);
        assert_eq!(result.mfe_distribution.sample_count, 2);
        assert_eq!(result.mae_distribution.sample_count, 2);
        assert_eq!(result.time_to_outcome_minutes_distribution.sample_count, 2);
        assert_eq!(result.mfe_mae_ratio_distribution.sample_count, 2);
        assert_eq!(result.meta.sample_size, 2);
        assert_eq!(result.meta.effective_sample_size, 2);
    }
}
