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

/// Compare today's session against similar historical sessions.
pub fn compare_sessions(
    db: &Database,
    current_ib_range: f64,
    current_day_type: Option<&str>,
    max_results: usize,
) -> Result<Vec<serde_json::Value>, String> {
    let summaries = db
        .list_session_summaries(None, None, current_day_type, 500)
        .map_err(|e| e.to_string())?;

    let mut scored: Vec<(f64, &crate::db::SessionSummary)> = summaries
        .iter()
        .filter(|s| s.ib_range > 0.0)
        .map(|s| {
            let ib_diff = (s.ib_range - current_ib_range).abs();
            (ib_diff, s)
        })
        .collect();

    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    Ok(scored
        .into_iter()
        .take(max_results)
        .map(|(diff, s)| {
            serde_json::json!({
                "sessionDate": s.session_date,
                "ibRange": s.ib_range,
                "ibRangeDiff": diff,
                "dayType": s.day_type,
                "close": s.close,
                "closeVsIbMid": s.close_vs_ib_mid,
                "closeVsVwap": s.close_vs_vwap,
                "sessionDelta": s.session_delta,
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
