use crate::db::Database;
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

/// "How often does event X happen per session?"
pub fn event_frequency(
    db: &Database,
    event_type: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<FrequencyResult, String> {
    let (total, sessions_with, total_sessions) = db
        .count_events_by_type(event_type, start_date, end_date)
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
pub fn conditional_probability(
    db: &Database,
    event_type: &str,
    min_count: i64,
    outcome_field: &str,
    outcome_value: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<ConditionalResult, String> {
    let counts = db
        .event_counts_per_session(event_type, start_date, end_date)
        .map_err(|e| e.to_string())?;

    let summaries = db
        .list_session_summaries(start_date, end_date, None, 10_000)
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
) -> Result<DistributionResult, String> {
    let outcomes = db
        .list_signal_outcomes_for_research(Some(setup_id), start_date, end_date)
        .map_err(|e| e.to_string())?;

    let values: Vec<f64> = outcomes.into_iter().filter_map(|(_, _, r, _)| r).collect();

    if values.is_empty() {
        return Ok(DistributionResult {
            metric: format!("r_result (setup {setup_id})"),
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
        });
    }

    let mut sorted = values.clone();
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

    Ok(DistributionResult {
        metric: format!("r_result (setup {setup_id})"),
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
    })
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
) -> Result<ConditionalResult, String> {
    let outcomes = db
        .list_signal_outcomes_for_research(Some(setup_id), start_date, end_date)
        .map_err(|e| e.to_string())?;

    let summaries = db
        .list_session_summaries(start_date, end_date, None, 10_000)
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

/// Distribution of a numeric metric from session_summaries.
pub fn metric_distribution(
    db: &Database,
    metric: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<DistributionResult, String> {
    let mut values = db
        .metric_values(metric, start_date, end_date)
        .map_err(|e| e.to_string())?;

    if values.is_empty() {
        return Ok(DistributionResult {
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
        });
    }

    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = values.len();
    let sum: f64 = values.iter().sum();
    let mean = sum / n as f64;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n as f64;
    let stddev = variance.sqrt();

    let percentile = |p: f64| -> f64 {
        let idx = (p / 100.0 * (n - 1) as f64).round() as usize;
        values[idx.min(n - 1)]
    };

    Ok(DistributionResult {
        metric: metric.to_string(),
        sample_count: n,
        mean,
        median: percentile(50.0),
        stddev,
        min: values[0],
        max: values[n - 1],
        p10: percentile(10.0),
        p25: percentile(25.0),
        p75: percentile(75.0),
        p90: percentile(90.0),
    })
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
        .list_session_summaries(None, None, None, 500)
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

    #[test]
    fn distribution_handles_empty() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::open(file.path().to_string_lossy().as_ref()).unwrap();
        let result = metric_distribution(&db, "ib_range", None, None).unwrap();
        assert_eq!(result.sample_count, 0);
    }
}
