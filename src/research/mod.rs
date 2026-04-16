use crate::db::Database;
use crate::db::SessionScopeFilter;
use crate::depth::DomSummary;
use serde::{Deserialize, Serialize};

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
}

fn distribution_from_values(metric: &str, values: &[f64]) -> DistributionResult {
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
        };
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();
    let sum: f64 = sorted.iter().sum();
    let mean = sum / n as f64;
    let variance = sorted.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n as f64;
    let stddev = variance.sqrt();

    let percentile = |p: f64| -> f64 {
        let idx = (p / 100.0 * (n - 1) as f64).round() as usize;
        sorted[idx.min(n - 1)]
    };

    DistributionResult {
        metric: metric.to_string(),
        sample_count: n,
        mean,
        median: percentile(50.0),
        stddev,
        min: sorted[0],
        max: sorted[n - 1],
        p10: percentile(10.0),
        p25: percentile(25.0),
        p75: percentile(75.0),
        p90: percentile(90.0),
    }
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
        .list_dom_feature_snapshots_for_research(start_date, end_date, 200_000)
        .map_err(|e| e.to_string())?;
    let total_sessions = db
        .list_session_summaries_scoped(start_date, end_date, None, None, 10_000, None)
        .map_err(|e| e.to_string())?
        .len() as i64;

    let mut total_occurrences = 0_i64;
    let mut sessions_with_behavior = std::collections::BTreeSet::new();
    let mut durations = Vec::new();
    let mut flips = Vec::new();
    for (trading_day, _timestamp_ms, payload) in rows {
        let Some(summary) = parse_dom_summary(&payload) else {
            continue;
        };
        if dom_behavior_matches(&summary, behavior, min_duration_ms) {
            total_occurrences += 1;
            sessions_with_behavior.insert(trading_day);
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

    Ok(DomBehaviorFrequencyResult {
        behavior: behavior.to_string(),
        total_occurrences,
        sessions_with_behavior: sessions_with,
        total_sessions,
        per_session_avg,
        pct_sessions_with_behavior,
        avg_bias_duration_ms: mean(&durations).unwrap_or(0.0),
        avg_flip_count_last_60s: mean(&flips).unwrap_or(0.0),
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
    let outcomes = db
        .list_signal_outcomes_with_context(setup_id, start_date, end_date, scope)
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
    let events = db
        .list_market_events_for_research(event_type, start_date, end_date, scope, 100_000)
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
    let (total, sessions_with, total_sessions) = db
        .count_events_by_type(event_type, start_date, end_date, scope)
        .map_err(|e| e.to_string())?;

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
    let summary_start = scope
        .and_then(|s| s.trading_day_start.as_deref())
        .or(start_date);
    let summary_end = scope
        .and_then(|s| s.trading_day_end.as_deref())
        .or(end_date);
    let counts = db
        .event_counts_per_session(event_type, start_date, end_date, scope)
        .map_err(|e| e.to_string())?;

    let summaries = db
        .list_session_summaries_scoped(summary_start, summary_end, None, None, 10_000, scope)
        .map_err(|e| e.to_string())?;

    let summary_map: std::collections::HashMap<String, &crate::db::SessionSummary> = summaries
        .iter()
        .map(|s| (s.session_date.clone(), s))
        .collect();

    let mut condition_met = 0_i64;
    let mut outcome_met = 0_i64;

    for (date, count) in &counts {
        if *count >= min_count {
            condition_met += 1;
            if let Some(summary) = summary_map.get(date) {
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
            }
        }
    }

    let probability = if condition_met > 0 {
        outcome_met as f64 / condition_met as f64
    } else {
        0.0
    };

    Ok(ConditionalResult {
        condition_description: format!("{event_type} >= {min_count} times per session"),
        outcome_description: format!("{outcome_field} = {outcome_value}"),
        probability,
        sample_size: condition_met,
        condition_met_count: condition_met,
        outcome_met_count: outcome_met,
        total_sessions: summaries.len() as i64,
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
    let outcomes = db
        .list_signal_outcomes_for_research(Some(setup_id), start_date, end_date, scope)
        .map_err(|e| e.to_string())?;

    let values: Vec<f64> = outcomes.into_iter().filter_map(|(_, _, r, _)| r).collect();
    Ok(distribution_from_values(
        &format!("r_result (setup {setup_id})"),
        &values,
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
    let summary_start = scope
        .and_then(|s| s.trading_day_start.as_deref())
        .or(start_date);
    let summary_end = scope
        .and_then(|s| s.trading_day_end.as_deref())
        .or(end_date);
    let outcomes = db
        .list_signal_outcomes_for_research(Some(setup_id), start_date, end_date, scope)
        .map_err(|e| e.to_string())?;

    let summaries = db
        .list_session_summaries_scoped(summary_start, summary_end, None, None, 10_000, scope)
        .map_err(|e| e.to_string())?;

    let summary_map: std::collections::HashMap<String, &crate::db::SessionSummary> = summaries
        .iter()
        .map(|s| (s.session_date.clone(), s))
        .collect();

    let mut condition_met = 0_i64;
    let mut outcome_met = 0_i64;

    for (_, session_date, r_result, _) in &outcomes {
        if let Some(summary) = summary_map.get(session_date) {
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
            if let Some(r) = r_result {
                if *r > 0.0 {
                    outcome_met += 1;
                }
            }
        }
    }

    let probability = if condition_met > 0 {
        outcome_met as f64 / condition_met as f64
    } else {
        0.0
    };

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
    let rows = db
        .list_signal_outcomes_for_excursions_filtered(setup_id, start_date, end_date, scope)
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
        mfe_distribution: distribution_from_values("max_favorable_excursion", &mfe_values),
        mae_distribution: distribution_from_values("max_adverse_excursion", &mae_values),
        time_to_outcome_minutes_distribution: distribution_from_values(
            "time_to_outcome_minutes",
            &time_minutes,
        ),
        mfe_mae_ratio_distribution: distribution_from_values("mfe_mae_ratio", &mfe_mae_ratio),
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

    let outcomes = db
        .list_signal_outcomes_with_rvol(setup_id, start_date, end_date, scope)
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
    let summary_start = scope
        .and_then(|s| s.trading_day_start.as_deref())
        .or(start_date);
    let summary_end = scope
        .and_then(|s| s.trading_day_end.as_deref())
        .or(end_date);
    let values = db
        .metric_values_scoped(
            metric,
            summary_start,
            summary_end,
            scope.and_then(|s| s.session_type.as_deref()),
            scope,
        )
        .map_err(|e| e.to_string())?;
    Ok(distribution_from_values(metric, &values))
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
    let summaries = db
        .list_session_summaries_scoped(None, None, None, None, 500, None)
        .map_err(|e| e.to_string())?;

    let filtered: Vec<&crate::db::SessionSummary> =
        summaries.iter().filter(|s| s.ib_range > 0.0).collect();

    if filtered.is_empty() {
        return Ok(Vec::new());
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

    Ok(scored
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
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SignalOutcome;

    #[test]
    fn distribution_handles_empty() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();
        let result = metric_distribution(&db, "ib_range", None, None, None).unwrap();
        assert_eq!(result.sample_count, 0);
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
    }
}
