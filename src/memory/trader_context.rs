use crate::db::{Database, SessionScopeFilter, TradeRecord};
use crate::memory::{
    time_bucket_from_timestamp_ms, AgentInsightQuery, BehavioralPatternQuery,
    BehavioralPatternRecord, MemoryError, MemoryFollowupQuery,
};
use crate::research::{
    context_frame::{build_context_frame, ContextFrameMode, ContextFrameOptions},
    reliability_tier, ReliabilityTier,
};
use crate::trading_day_from_timestamp_ms;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashSet;
use std::str::FromStr;

const PATTERN_LOAD_CAP: usize = 300;
const OPPORTUNITY_COMPARISON_FLOOR: usize = 20;
const EXECUTION_CONFLICT_MIN_GAP_R: f64 = 0.20;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TraderContextIntent {
    SessionStart,
    #[default]
    SetupCheck,
    TradeTaken,
    TradeClosed,
    SessionReview,
}

impl FromStr for TraderContextIntent {
    type Err = MemoryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "sessionStart" | "session_start" => Ok(Self::SessionStart),
            "setupCheck" | "setup_check" => Ok(Self::SetupCheck),
            "tradeTaken" | "trade_taken" => Ok(Self::TradeTaken),
            "tradeClosed" | "trade_closed" => Ok(Self::TradeClosed),
            "sessionReview" | "session_review" => Ok(Self::SessionReview),
            other => Err(MemoryError::Validation(format!(
                "unsupported trader context intent `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraderContextFitQuery {
    pub intent: TraderContextIntent,
    pub setup_id: Option<String>,
    pub session_id: Option<String>,
    pub trade_account: Option<String>,
    pub trading_day: Option<String>,
    pub timestamp_ms: Option<f64>,
    pub session_type: Option<String>,
    pub session_segment: Option<String>,
    pub time_bucket: Option<String>,
    pub day_type: Option<String>,
    pub profile_shape: Option<String>,
    pub balance_state: Option<String>,
    pub include_opportunity: Option<bool>,
    pub include_coaching_memory: Option<bool>,
    #[serde(skip)]
    pub context_snapshot: Option<serde_json::Value>,
}

impl Default for TraderContextFitQuery {
    fn default() -> Self {
        Self {
            intent: TraderContextIntent::SetupCheck,
            setup_id: None,
            session_id: None,
            trade_account: None,
            trading_day: None,
            timestamp_ms: None,
            session_type: None,
            session_segment: None,
            time_bucket: None,
            day_type: None,
            profile_shape: None,
            balance_state: None,
            include_opportunity: Some(true),
            include_coaching_memory: Some(true),
            context_snapshot: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraderContextFit {
    pub intent: TraderContextIntent,
    pub current_context: serde_json::Value,
    pub execution_fit: serde_json::Value,
    pub opportunity_fit: serde_json::Value,
    pub coaching_memory: serde_json::Value,
    pub risk_context: serde_json::Value,
    pub reliability: serde_json::Value,
    pub provenance: serde_json::Value,
    pub maintenance: serde_json::Value,
}

fn budget(intent: TraderContextIntent) -> (usize, usize, usize) {
    match intent {
        TraderContextIntent::SessionStart => (3, 1, 5),
        TraderContextIntent::SetupCheck => (3, 2, 3),
        TraderContextIntent::TradeTaken => (2, 1, 2),
        TraderContextIntent::TradeClosed => (3, 1, 3),
        TraderContextIntent::SessionReview => (12, 6, 12),
    }
}

fn time_bucket_to_camel(value: &str) -> String {
    match value {
        "rth_open" => "rthOpen",
        "rth_midday" => "rthMidday",
        "rth_afternoon" => "rthAfternoon",
        "pre_open" => "preOpen",
        "globex_asia" => "globexAsia",
        "globex_london" => "globexLondon",
        other => other,
    }
    .to_string()
}

fn tier_string(tier: &ReliabilityTier) -> &'static str {
    match tier {
        ReliabilityTier::Insufficient => "insufficient",
        ReliabilityTier::Directional => "directional",
        ReliabilityTier::Reportable => "reportable",
    }
}

fn metric_f64(pattern: &BehavioralPatternRecord, key: &str) -> Option<f64> {
    pattern.metric.get(key).and_then(|value| value.as_f64())
}

fn scope_string(pattern: &BehavioralPatternRecord, key: &str) -> Option<String> {
    pattern
        .scope
        .get(key)
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

fn scope_match_score(pattern: &BehavioralPatternRecord, query: &TraderContextFitQuery) -> i64 {
    let mut score = 0;
    if let Some(setup_id) = &query.setup_id {
        if scope_string(pattern, "setupId").as_deref() == Some(setup_id.as_str()) {
            score += 4;
        }
    }
    if let Some(time_bucket) = &query.time_bucket {
        if scope_string(pattern, "timeBucket").as_deref() == Some(time_bucket.as_str()) {
            score += 3;
        }
    }
    if let Some(day_type) = &query.day_type {
        if scope_string(pattern, "dayType").as_deref() == Some(day_type.as_str()) {
            score += 3;
        }
    }
    if let Some(session_type) = &query.session_type {
        if scope_string(pattern, "sessionType").as_deref() == Some(session_type.as_str()) {
            score += 2;
        }
    }
    if pattern.scope.get("postLossState").is_some() {
        score += 1;
    }
    score
}

fn pattern_specificity(pattern: &BehavioralPatternRecord) -> i64 {
    [
        "setupId",
        "timeBucket",
        "dayType",
        "sessionType",
        "postLossState",
    ]
    .iter()
    .filter(|key| pattern.scope.get(**key).is_some())
    .count() as i64
}

fn tier_rank(pattern: &BehavioralPatternRecord) -> i64 {
    match reliability_tier(pattern.sample_size.max(0) as usize) {
        ReliabilityTier::Insufficient => 0,
        ReliabilityTier::Directional => 1,
        ReliabilityTier::Reportable => 2,
    }
}

fn rankable_scope_score(pattern: &BehavioralPatternRecord, query: &TraderContextFitQuery) -> i64 {
    let tier = reliability_tier(pattern.sample_size.max(0) as usize);
    if tier == ReliabilityTier::Insufficient {
        0
    } else {
        scope_match_score(pattern, query)
    }
}

fn has_numeric_floor(pattern: &BehavioralPatternRecord) -> bool {
    pattern.sample_size >= 3
}

fn pattern_to_evidence(pattern: &BehavioralPatternRecord) -> serde_json::Value {
    let n = pattern.sample_size.max(0) as usize;
    let tier = reliability_tier(n);
    let suppress_numeric = n < 3;
    let win_rate = (!suppress_numeric)
        .then(|| metric_f64(pattern, "winRate"))
        .flatten();
    let avg_r = (!suppress_numeric)
        .then(|| metric_f64(pattern, "avgR"))
        .flatten();
    json!({
        "id": pattern.id,
        "patternType": pattern.pattern_type,
        "description": pattern.description,
        "n": n,
        "reliabilityTier": tier_string(&tier),
        "winRate": win_rate,
        "avgR": avg_r,
        "scope": pattern.scope,
        "queryHint": pattern.scope,
        "source": "behavioral_patterns",
        "caveats": if n < 3 {
            vec!["Numeric claims suppressed below small-N floor.".to_string()]
        } else if tier == ReliabilityTier::Insufficient {
            vec!["N < 20 is insufficient; treat as low-confidence context.".to_string()]
        } else {
            Vec::new()
        }
    })
}

fn post_loss_state_from_recent_trades(trades_desc: &[TradeRecord]) -> Option<&'static str> {
    let mut consecutive_losses = 0usize;
    for trade in trades_desc.iter().filter(|trade| trade.exit_time.is_some()) {
        let Some(result_r) = trade.result_r else {
            continue;
        };
        if result_r < 0.0 {
            consecutive_losses += 1;
            continue;
        }
        break;
    }
    match consecutive_losses {
        0 => None,
        1 => Some("afterOneLoss"),
        _ => Some("afterTwoPlusLosses"),
    }
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        Some((values[mid - 1] + values[mid]) / 2.0)
    } else {
        Some(values[mid])
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| round_metric(values.iter().sum::<f64>() / values.len() as f64))
}

fn round_metric(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}

fn numeric_json(value: f64) -> serde_json::Value {
    serde_json::Number::from_f64(round_metric(value))
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null)
}

fn reliability_caveat(n: usize, tier: &ReliabilityTier) -> Option<String> {
    match tier {
        ReliabilityTier::Insufficient => Some(format!(
            "N={n} is below the reportable floor; treat setup opportunity as low-confidence context."
        )),
        ReliabilityTier::Directional => {
            Some(format!("N={n} is directional; include caveats with any opportunity read."))
        }
        ReliabilityTier::Reportable => None,
    }
}

fn session_scope_from_query(query: &TraderContextFitQuery) -> SessionScopeFilter {
    SessionScopeFilter {
        session_type: query.session_type.clone(),
        session_segment: query.session_segment.clone(),
        ..SessionScopeFilter::default()
    }
}

fn opportunity_scope_json(query: &TraderContextFitQuery) -> serde_json::Value {
    let mut scope = serde_json::Map::new();
    if let Some(setup_id) = &query.setup_id {
        scope.insert("setupId".to_string(), json!(setup_id));
    }
    if let Some(session_type) = &query.session_type {
        scope.insert("sessionType".to_string(), json!(session_type));
    }
    if let Some(session_segment) = &query.session_segment {
        scope.insert("sessionSegment".to_string(), json!(session_segment));
    }
    serde_json::Value::Object(scope)
}

fn build_setup_outcome(
    db: &Database,
    query: &TraderContextFitQuery,
) -> Result<(serde_json::Value, Vec<serde_json::Value>), MemoryError> {
    let Some(setup_id) = query.setup_id.as_deref() else {
        return Ok((
            serde_json::Value::Null,
            vec![json!({
                "kind": "missingSetupId",
                "reason": "Setup-level opportunity context requires setupId."
            })],
        ));
    };
    if db.get_setup(setup_id)?.is_none() {
        return Ok((
            serde_json::Value::Null,
            vec![json!({
                "kind": "unknownSetup",
                "setupId": setup_id,
                "reason": "No setup definition exists for this setupId."
            })],
        ));
    }

    let scope = session_scope_from_query(query);
    let performance =
        db.signal_performance_filtered(Some(setup_id), None, None, None, None, Some(&scope))?;
    let total = performance
        .get("totalSignals")
        .and_then(|value| value.as_i64())
        .unwrap_or(0);
    if total == 0 {
        return Ok((
            serde_json::Value::Null,
            vec![json!({
                "kind": "noSignalOutcomes",
                "setupId": setup_id,
                "reason": "No signal_outcomes rows matched this setup and scope."
            })],
        ));
    }

    let resolved = performance
        .get("resolved")
        .and_then(|value| value.as_i64())
        .unwrap_or(0)
        .max(0) as usize;
    let pending = performance
        .get("pending")
        .and_then(|value| value.as_i64())
        .unwrap_or(0)
        .max(0) as usize;
    let outcomes =
        db.list_signal_outcomes_with_context(Some(setup_id), None, None, Some(&scope))?;
    let mut r_values: Vec<f64> = outcomes.iter().filter_map(|row| row.r_result).collect();
    let mfe_values: Vec<f64> = outcomes
        .iter()
        .filter_map(|row| row.max_favorable_excursion)
        .collect();
    let mae_values: Vec<f64> = outcomes
        .iter()
        .filter_map(|row| row.max_adverse_excursion)
        .collect();
    let tier = reliability_tier(resolved);
    let mut caveats = Vec::new();
    if let Some(caveat) = reliability_caveat(resolved, &tier) {
        caveats.push(caveat);
    }
    if pending > 0 {
        caveats.push("Pending setup signals are reported separately and excluded from resolved opportunity statistics.".to_string());
    }

    Ok((
        json!({
            "setupId": setup_id,
            "n": resolved,
            "sampleSize": resolved,
            "totalSignals": total.max(0) as usize,
            "resolved": resolved,
            "pending": pending,
            "reliabilityTier": tier_string(&tier),
            "winRate": (resolved > 0).then(|| performance.get("winRate").and_then(|value| value.as_f64())).flatten(),
            "avgR": (!r_values.is_empty()).then(|| performance.get("avgR").and_then(|value| value.as_f64()).map(round_metric)).flatten(),
            "medianR": median(&mut r_values),
            "avgMfe": mean(&mfe_values),
            "avgMae": mean(&mae_values),
            "source": "signal_outcomes",
            "scope": opportunity_scope_json(query),
            "caveats": caveats,
        }),
        Vec::new(),
    ))
}

fn compact_context_frame_analog(db: &Database, query: &TraderContextFitQuery) -> serde_json::Value {
    let Some(snapshot) = &query.context_snapshot else {
        return json!({
            "available": true,
            "source": "get_context_frame",
            "detailAvailableByCalling": "get_context_frame",
            "caveats": ["Context-frame analogs are not executed-trade memory."]
        });
    };

    let frame = match build_context_frame(
        db,
        snapshot,
        ContextFrameOptions {
            mode: ContextFrameMode::Live,
            snapshot_timestamp_ms: query.timestamp_ms,
            setup_id: query.setup_id.clone(),
            include_historical: true,
            ..ContextFrameOptions::default()
        },
    ) {
        Ok(frame) => frame,
        Err(error) => {
            return json!({
                "available": false,
                "source": "get_context_frame",
                "detailAvailableByCalling": "get_context_frame",
                "error": {
                    "kind": "contextFrameUnavailable",
                    "message": error,
                },
                "caveats": ["Context-frame analogs are not executed-trade memory."]
            });
        }
    };

    let analog = frame
        .historical_analogs
        .as_ref()
        .or(frame.intraday_forward_stats.as_ref());
    let Some(analog) = analog else {
        let mut caveats = vec!["Context-frame analogs are not executed-trade memory.".to_string()];
        caveats.extend(frame.caveats);
        return json!({
            "available": false,
            "source": "get_context_frame",
            "detailAvailableByCalling": "get_context_frame",
            "caveats": caveats,
        });
    };
    let mut caveats = vec!["Context-frame analogs are not executed-trade memory.".to_string()];
    caveats.extend(frame.caveats);
    json!({
        "available": true,
        "source": "get_context_frame",
        "detailAvailableByCalling": "get_context_frame",
        "analogSource": analog.source,
        "effectiveSampleSize": analog.meta.effective_sample_size,
        "sampleSize": analog.meta.sample_size,
        "reliabilityTier": tier_string(&analog.meta.reliability_tier),
        "matchingMode": analog.meta.matching_mode,
        "topKFallbackUsed": analog.meta.top_k_fallback_used,
        "closeBackToVwap": analog.close_back_to_vwap,
        "caveats": caveats,
    })
}

fn select_execution_comparison_slice(
    patterns: &[BehavioralPatternRecord],
    query: &TraderContextFitQuery,
) -> Option<serde_json::Value> {
    patterns
        .iter()
        .filter(|pattern| scope_match_score(pattern, query) > 0)
        .find(|pattern| {
            !pattern.pattern_type.contains("post_loss")
                && (pattern.sample_size.max(0) as usize) >= OPPORTUNITY_COMPARISON_FLOOR
                && metric_f64(pattern, "avgR").is_some()
        })
        .map(pattern_to_evidence)
}

fn build_execution_conflict(
    setup_outcome: &serde_json::Value,
    execution_slice: Option<&serde_json::Value>,
) -> serde_json::Value {
    let opportunity_reliability_tier = setup_outcome
        .get("reliabilityTier")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let opportunity_n = setup_outcome
        .get("sampleSize")
        .or_else(|| setup_outcome.get("n"))
        .and_then(|value| value.as_u64())
        .unwrap_or(0) as usize;
    let opportunity_avg_r = setup_outcome.get("avgR").and_then(|value| value.as_f64());
    let opportunity_avg_r_json = opportunity_avg_r
        .map(numeric_json)
        .unwrap_or(serde_json::Value::Null);
    if opportunity_n < OPPORTUNITY_COMPARISON_FLOOR || opportunity_avg_r.is_none() {
        return json!({
            "detected": false,
            "reason": "insufficientOpportunitySample",
            "comparedExecutionSlice": null,
            "executionAvgR": null,
            "executionN": null,
            "opportunityAvgR": opportunity_avg_r_json,
            "opportunityN": opportunity_n,
            "opportunityReliabilityTier": opportunity_reliability_tier,
            "gapR": null,
        });
    }

    let Some(execution_slice) = execution_slice else {
        return json!({
            "detected": false,
            "reason": "insufficientExecutionSample",
            "comparedExecutionSlice": null,
            "executionAvgR": null,
            "executionN": null,
            "opportunityAvgR": opportunity_avg_r_json,
            "opportunityN": opportunity_n,
            "opportunityReliabilityTier": opportunity_reliability_tier,
            "gapR": null,
        });
    };
    let execution_avg_r = execution_slice
        .get("avgR")
        .and_then(|value| value.as_f64())
        .unwrap_or(0.0);
    let execution_n = execution_slice
        .get("n")
        .and_then(|value| value.as_u64())
        .unwrap_or(0) as usize;
    let opportunity_avg_r = opportunity_avg_r.unwrap_or(0.0);
    let gap_r = (opportunity_avg_r - execution_avg_r).abs();
    let signs_differ = opportunity_avg_r.signum() != execution_avg_r.signum()
        && opportunity_avg_r != 0.0
        && execution_avg_r != 0.0;
    let detected = signs_differ && gap_r >= EXECUTION_CONFLICT_MIN_GAP_R;
    let interpretation = if detected && opportunity_avg_r > 0.0 && execution_avg_r < 0.0 {
        "Setup signal historically positive; trader's executed result negative."
    } else if detected && opportunity_avg_r < 0.0 && execution_avg_r > 0.0 {
        "Trader execution has outperformed weak setup signal history."
    } else if signs_differ {
        "Opportunity and execution signs differ, but the average-R gap is below the conflict threshold."
    } else {
        "Opportunity and execution averages do not materially conflict."
    };

    json!({
        "detected": detected,
        "reason": if detected { "signMismatch" } else if signs_differ { "nonMaterialGap" } else { "sameDirection" },
        "comparedExecutionSlice": {
            "id": execution_slice.get("id").cloned().unwrap_or(serde_json::Value::Null),
            "patternType": execution_slice.get("patternType").cloned().unwrap_or(serde_json::Value::Null),
            "n": execution_n,
            "avgR": numeric_json(execution_avg_r),
            "reliabilityTier": execution_slice.get("reliabilityTier").cloned().unwrap_or(serde_json::Value::Null),
        },
        "executionAvgR": numeric_json(execution_avg_r),
        "executionN": execution_n,
        "opportunityAvgR": numeric_json(opportunity_avg_r),
        "opportunityN": opportunity_n,
        "opportunityReliabilityTier": opportunity_reliability_tier,
        "gapR": numeric_json(gap_r),
        "interpretation": interpretation,
    })
}

fn current_risk_context(
    db: &Database,
    query: &TraderContextFitQuery,
) -> Result<serde_json::Value, MemoryError> {
    let Some(session_id) = &query.session_id else {
        return Ok(json!({
            "tradeOrdinalInSession": null,
            "postLossStateInSession": null,
            "openTradePresent": false,
            "riskDeviation": {
                "available": false,
                "availability": {
                    "kind": "missingFields",
                    "reason": "No current session supplied for live risk-memory context."
                }
            },
            "ruleAdherenceFlags": []
        }));
    };
    let trades = db.list_recent_session_trades(session_id, 20)?;
    let open_trade_present = trades.iter().any(|trade| trade.exit_time.is_none());
    let ordinal = match trades.len() + 1 {
        1 => "first",
        2 => "second",
        _ => "thirdPlus",
    };
    let post_loss = post_loss_state_from_recent_trades(&trades);
    Ok(json!({
        "tradeOrdinalInSession": ordinal,
        "postLossStateInSession": post_loss,
        "openTradePresent": open_trade_present,
        "riskDeviation": {
            "available": false,
            "availability": {
                "kind": "preCapture",
                "reason": "Risk deviation cannot be evaluated for this row; do not infer risk compliance from this absence."
            }
        },
        "ruleAdherenceFlags": []
    }))
}

pub fn build_trader_context_fit(
    db: &Database,
    mut query: TraderContextFitQuery,
) -> Result<TraderContextFit, MemoryError> {
    if query.time_bucket.is_none() {
        if let Some(timestamp_ms) = query.timestamp_ms {
            query.time_bucket = Some(time_bucket_from_timestamp_ms(timestamp_ms));
        }
    }
    if query.trading_day.is_none() {
        if let Some(timestamp_ms) = query.timestamp_ms {
            query.trading_day = Some(trading_day_from_timestamp_ms(timestamp_ms));
        }
    }

    let (execution_budget, _opportunity_budget, coaching_budget) = budget(query.intent);
    let maintenance = db.get_memory_maintenance_state()?;
    let mut patterns = db.list_behavioral_patterns(&BehavioralPatternQuery {
        active_only: Some(true),
        limit: Some(PATTERN_LOAD_CAP),
        ..BehavioralPatternQuery::default()
    })?;

    let truncation_warning = (patterns.len() >= PATTERN_LOAD_CAP).then(|| {
        format!("active behavioral pattern load reached cap {PATTERN_LOAD_CAP}; lower-ranked rows may be omitted")
    });
    let active_pattern_ids: HashSet<String> =
        patterns.iter().map(|pattern| pattern.id.clone()).collect();

    patterns.sort_by(|a, b| {
        tier_rank(b)
            .cmp(&tier_rank(a))
            .then_with(|| rankable_scope_score(b, &query).cmp(&rankable_scope_score(a, &query)))
            .then_with(|| pattern_specificity(b).cmp(&pattern_specificity(a)))
            .then_with(|| {
                metric_f64(b, "avgR")
                    .unwrap_or(0.0)
                    .abs()
                    .partial_cmp(&metric_f64(a, "avgR").unwrap_or(0.0).abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });

    let matching: Vec<_> = patterns
        .iter()
        .filter(|pattern| scope_match_score(pattern, &query) > 0)
        .take(execution_budget)
        .map(pattern_to_evidence)
        .collect();

    let positive: Vec<_> = patterns
        .iter()
        .filter(|pattern| scope_match_score(pattern, &query) > 0)
        .filter(|pattern| has_numeric_floor(pattern))
        .filter(|pattern| metric_f64(pattern, "avgR").unwrap_or(0.0) >= 0.0)
        .take(execution_budget)
        .map(pattern_to_evidence)
        .collect();

    let caution: Vec<_> = patterns
        .iter()
        .filter(|pattern| scope_match_score(pattern, &query) > 0)
        .filter(|pattern| has_numeric_floor(pattern))
        .filter(|pattern| {
            metric_f64(pattern, "avgR").unwrap_or(0.0) < 0.0
                || pattern.pattern_type.contains("post_loss")
        })
        .take(execution_budget)
        .map(pattern_to_evidence)
        .collect();

    let insights = if query.include_coaching_memory.unwrap_or(true) {
        db.list_agent_insights(&AgentInsightQuery {
            setup_id: query.setup_id.clone(),
            limit: Some(coaching_budget),
            ..AgentInsightQuery::default()
        })?
    } else {
        Vec::new()
    };
    let followups = if query.include_coaching_memory.unwrap_or(true) {
        db.list_memory_followups(&MemoryFollowupQuery {
            setup_id: query.setup_id.clone(),
            limit: Some(coaching_budget),
            ..MemoryFollowupQuery::default()
        })?
    } else {
        Vec::new()
    };

    let pattern_ids: Vec<String> = matching
        .iter()
        .filter_map(|value| {
            value
                .get("id")
                .and_then(|id| id.as_str())
                .map(str::to_string)
        })
        .collect();
    let insight_ids: Vec<String> = insights.iter().map(|insight| insight.id.clone()).collect();
    let followup_ids: Vec<String> = followups
        .iter()
        .map(|followup| followup.id.clone())
        .collect();
    let insights_last_updated = insights
        .iter()
        .map(|insight| insight.updated_at_ms)
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let insights_json: Vec<serde_json::Value> = insights
        .iter()
        .map(|insight| {
            let stale_pattern_evidence = insight
                .evidence
                .get("patternIds")
                .and_then(|value| value.as_array())
                .into_iter()
                .flatten()
                .filter_map(|value| value.as_str())
                .any(|pattern_id| !active_pattern_ids.contains(pattern_id));
            let mut value = serde_json::to_value(insight).unwrap_or_else(|_| json!({}));
            if let Some(obj) = value.as_object_mut() {
                obj.insert(
                    "stalePatternEvidence".to_string(),
                    json!(stale_pattern_evidence),
                );
            }
            value
        })
        .collect();

    let time_bucket_out = query.time_bucket.as_deref().map(time_bucket_to_camel);
    let account_scope = query
        .trade_account
        .clone()
        .unwrap_or_else(|| "allAccounts".to_string());
    let mut global_caveats =
        vec!["Execution and opportunity samples are reported separately.".to_string()];
    if query.trade_account.is_none() {
        let accounts: HashSet<String> = db
            .list_recent_trades(1_000)?
            .into_iter()
            .filter_map(|trade| trade.trade_account)
            .collect();
        if accounts.len() > 1 {
            global_caveats
                .push("Execution memory is pooled across multiple trade accounts.".to_string());
        }
    } else {
        global_caveats.push(
            "Account-specific live context is scoped, but historical behavioral patterns are currently pooled unless account-scoped pattern slices have been refreshed.".to_string(),
        );
    }

    let include_opportunity = query.include_opportunity.unwrap_or(true);
    let (setup_outcome, mut opportunity_missing_data) = if include_opportunity {
        build_setup_outcome(db, &query)?
    } else {
        (serde_json::Value::Null, Vec::new())
    };
    let context_frame_analog = if include_opportunity {
        compact_context_frame_analog(db, &query)
    } else {
        json!({
            "available": false,
            "source": "get_context_frame",
            "caveats": ["Opportunity context was not requested."]
        })
    };
    let execution_conflict = if include_opportunity {
        let conflict_slice = select_execution_comparison_slice(&patterns, &query);
        build_execution_conflict(&setup_outcome, conflict_slice.as_ref())
    } else {
        json!({
            "detected": false,
            "reason": "opportunityNotRequested",
            "comparedExecutionSlice": null,
            "executionAvgR": null,
            "executionN": null,
            "opportunityAvgR": null,
            "opportunityN": null,
            "opportunityReliabilityTier": null,
            "gapR": null,
        })
    };
    if !include_opportunity {
        opportunity_missing_data.clear();
    }

    let risk_context = current_risk_context(db, &query)?;

    Ok(TraderContextFit {
        intent: query.intent,
        current_context: json!({
            "tradingDay": query.trading_day,
            "accountScope": account_scope,
            "sessionType": query.session_type,
            "sessionSegment": query.session_segment,
            "timeBucket": time_bucket_out,
            "dayType": query.day_type,
            "profileShape": query.profile_shape,
            "balanceState": query.balance_state,
            "setupId": query.setup_id,
        }),
        execution_fit: json!({
            "summary": if matching.is_empty() { "No scoped execution memory matched this context." } else { "Scoped execution memory matched this context." },
            "matchingSlices": matching,
            "strongestPositiveEvidence": positive,
            "strongestCautionEvidence": caution,
            "missingData": [],
        }),
        opportunity_fit: json!({
            "summary": if include_opportunity { "Opportunity data is separate from trader execution." } else { "Opportunity data was not requested." },
            "setupOutcome": setup_outcome,
            "contextFrameAnalog": context_frame_analog,
            "executionConflict": execution_conflict,
            "caveats": if include_opportunity {
                vec!["Opportunity stats describe setup signals, not trader execution."]
            } else {
                vec!["Opportunity context was not requested."]
            },
            "missingData": opportunity_missing_data
        }),
        coaching_memory: json!({
            "patterns": pattern_ids,
            "insights": insights_json,
            "followups": followups,
        }),
        risk_context,
        reliability: json!({
            "overallTier": if matching.is_empty() { "insufficient" } else { "directional" },
            "caveats": global_caveats
        }),
        provenance: json!({
            "executionSources": ["behavioral_patterns", "trades", "sessions", "session_summaries"],
            "opportunitySources": ["signal_outcomes", "context_frame"],
            "coachingSources": ["behavioral_patterns", "agent_insights", "memory_followups"],
            "evidenceIds": {
                "patterns": pattern_ids,
                "insights": insight_ids,
                "followups": followup_ids,
            },
            "evidenceTimestampsMs": {
                "patternsRefreshedAtMs": maintenance.patterns_last_refreshed_at_ms,
                "insightsLastUpdatedMs": insights_last_updated,
            },
            "truncationWarning": truncation_warning,
        }),
        maintenance: json!({
            "refreshSuggested": maintenance.refresh_suggested,
            "dirtyReasons": maintenance.dirty_reasons,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Database, SessionRecord, SignalOutcome, TradeRecord};
    use crate::memory::{
        AgentInsightRecord, BehavioralPatternRecord, MemoryFollowupRecord, INSIGHT_CANDIDATE,
    };
    use crate::rules::SetupDefinition;
    use tempfile::NamedTempFile;

    fn test_db() -> Database {
        let file = NamedTempFile::new().expect("temp db");
        Database::open(file.path().to_string_lossy().as_ref()).expect("open")
    }

    fn pattern(
        id: &str,
        pattern_type: &str,
        sample_size: i64,
        avg_r: f64,
        scope: serde_json::Value,
    ) -> BehavioralPatternRecord {
        BehavioralPatternRecord {
            id: id.to_string(),
            detected_at_ms: 1_000.0,
            pattern_type: pattern_type.to_string(),
            description: id.to_string(),
            metric: json!({
                "resolved": sample_size,
                "wins": sample_size / 2,
                "winRate": 0.5,
                "avgR": avg_r,
            }),
            scope,
            sample_size,
            confidence: 0.5,
            active: true,
            superseded_by: None,
        }
    }

    fn session(id: &str) -> SessionRecord {
        SessionRecord {
            id: id.to_string(),
            date: "2026-05-01".to_string(),
            session_type: "RTH".to_string(),
            start_time: 1_777_632_000_000.0,
            end_time: None,
            recording_path: None,
            pre_session_note: None,
        }
    }

    fn trade(id: &str, session_id: &str, entry_time: f64, result_r: Option<f64>) -> TradeRecord {
        TradeRecord {
            id: id.to_string(),
            session_id: Some(session_id.to_string()),
            setup_id: None,
            instrument: None,
            trade_account: None,
            entry_time,
            entry_price: 21_000.0,
            exit_time: result_r.map(|_| entry_time + 1_000.0),
            exit_price: result_r.map(|_| 21_001.0),
            direction: "long".to_string(),
            size: 1,
            max_open_size: Some(1),
            stop_price: None,
            target_prices: Vec::new(),
            result_r,
            gross_points: None,
            planned: true,
            rules_followed: None,
            emotional_state: None,
            thesis: None,
            review_tags: Vec::new(),
            mistake_tags: Vec::new(),
            entry_fill_count: 1,
            exit_fill_count: i64::from(result_r.is_some()),
            import_batch_id: None,
            planned_r_points_at_entry: None,
            planned_r_dollars_at_entry: None,
            notes: String::new(),
            source: "test".to_string(),
        }
    }

    fn seed_setup(db: &Database, id: &str) {
        db.upsert_setup(&SetupDefinition {
            id: id.to_string(),
            name: id.to_string(),
            active: true,
            ..SetupDefinition::default()
        })
        .expect("setup");
    }

    fn signal_outcome_for_date(
        id: &str,
        setup_id: &str,
        session_date: &str,
        fired_at_ms: f64,
        outcome: &str,
        r_result: f64,
    ) -> SignalOutcome {
        SignalOutcome {
            signal_id: id.to_string(),
            setup_id: setup_id.to_string(),
            setup_name: Some(setup_id.to_string()),
            session_date: session_date.to_string(),
            root_symbol: None,
            contract_symbol: None,
            source: "test".to_string(),
            job_id: None,
            fired_at_ms,
            fired_price: 21_000.0,
            target_price: Some(21_010.0),
            stop_price: Some(20_990.0),
            outcome: outcome.to_string(),
            outcome_at_ms: Some(fired_at_ms + 60_000.0),
            max_favorable_excursion: Some(if r_result > 0.0 { 1.5 } else { 0.3 }),
            max_adverse_excursion: Some(if r_result > 0.0 { -0.2 } else { -1.0 }),
            r_result: Some(r_result),
            time_to_outcome_ms: Some(60_000.0),
            rvol_at_fire: None,
            rvol_bucket_at_fire: None,
        }
    }

    fn signal_outcome(
        id: &str,
        setup_id: &str,
        fired_at_ms: f64,
        outcome: &str,
        r_result: f64,
    ) -> SignalOutcome {
        signal_outcome_for_date(id, setup_id, "2026-05-01", fired_at_ms, outcome, r_result)
    }

    fn seed_positive_or5_signal_outcomes(db: &Database) {
        let base_ms = 1_777_644_000_000.0;
        for idx in 0..32 {
            let (outcome, r_result) = if idx < 20 {
                ("target_hit", 1.0)
            } else {
                ("stop_hit", -1.0)
            };
            db.insert_signal_outcome(&signal_outcome(
                &format!("or5-signal-{idx}"),
                "or5",
                base_ms + (idx as f64 * 60_000.0),
                outcome,
                r_result,
            ))
            .expect("signal outcome");
        }
    }

    #[test]
    fn context_fit_shapes_matching_pattern_and_suppresses_tiny_n() {
        let db = test_db();
        db.upsert_behavioral_pattern(&pattern(
            "win_rate_by_setup_time_bucket:or5:rth_open",
            "win_rate_by_setup_time_bucket",
            2,
            -0.25,
            json!({ "setupId": "or5", "timeBucket": "rth_open" }),
        ))
        .expect("pattern");

        let fit = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                intent: TraderContextIntent::SetupCheck,
                setup_id: Some("or5".to_string()),
                time_bucket: Some("rth_open".to_string()),
                include_coaching_memory: Some(false),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");

        let slices = fit
            .execution_fit
            .get("matchingSlices")
            .and_then(|value| value.as_array())
            .expect("matching slices");
        assert_eq!(slices.len(), 1);
        assert!(slices[0].get("avgR").is_some_and(|value| value.is_null()));
        assert_eq!(
            fit.current_context
                .get("timeBucket")
                .and_then(|value| value.as_str()),
            Some("rthOpen")
        );
    }

    #[test]
    fn broader_reliable_slice_outranks_specific_insufficient_slice() {
        let db = test_db();
        db.upsert_behavioral_pattern(&pattern(
            "win_rate_by_setup:or5",
            "win_rate_by_setup",
            40,
            -0.05,
            json!({ "setupId": "or5" }),
        ))
        .expect("broad");
        db.upsert_behavioral_pattern(&pattern(
            "win_rate_by_setup_time_bucket:or5:rth_open",
            "win_rate_by_setup_time_bucket",
            2,
            1.0,
            json!({ "setupId": "or5", "timeBucket": "rth_open" }),
        ))
        .expect("specific");

        let fit = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                intent: TraderContextIntent::SetupCheck,
                setup_id: Some("or5".to_string()),
                time_bucket: Some("rth_open".to_string()),
                include_coaching_memory: Some(false),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");

        let first_id = fit
            .execution_fit
            .get("matchingSlices")
            .and_then(|value| value.as_array())
            .and_then(|values| values.first())
            .and_then(|value| value.get("id"))
            .and_then(|value| value.as_str());
        assert_eq!(first_id, Some("win_rate_by_setup:or5"));
    }

    #[test]
    fn live_risk_context_counts_open_trades_and_two_loss_streak() {
        let db = test_db();
        db.upsert_session(&session("s1")).expect("session");
        db.upsert_trade(&trade("loss1", "s1", 1_000.0, Some(-1.0)))
            .expect("loss1");
        db.upsert_trade(&trade("loss2", "s1", 2_000.0, Some(-0.5)))
            .expect("loss2");
        db.upsert_trade(&trade("open", "s1", 3_000.0, None))
            .expect("open");

        let fit = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                intent: TraderContextIntent::SetupCheck,
                session_id: Some("s1".to_string()),
                include_coaching_memory: Some(false),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");

        assert_eq!(
            fit.risk_context
                .get("tradeOrdinalInSession")
                .and_then(|value| value.as_str()),
            Some("thirdPlus")
        );
        assert_eq!(
            fit.risk_context
                .get("postLossStateInSession")
                .and_then(|value| value.as_str()),
            Some("afterTwoPlusLosses")
        );
        assert_eq!(
            fit.risk_context
                .get("openTradePresent")
                .and_then(|value| value.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn account_pooling_adds_caveat_when_multiple_accounts_exist() {
        let db = test_db();
        db.upsert_session(&session("s1")).expect("session");
        let mut first = trade("a", "s1", 1_000.0, Some(1.0));
        first.trade_account = Some("acct-a".to_string());
        let mut second = trade("b", "s1", 2_000.0, Some(-1.0));
        second.trade_account = Some("acct-b".to_string());
        db.upsert_trade(&first).expect("first");
        db.upsert_trade(&second).expect("second");

        let fit = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                include_coaching_memory: Some(false),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");

        let caveats = fit
            .reliability
            .get("caveats")
            .and_then(|value| value.as_array())
            .expect("caveats");
        assert!(caveats.iter().any(|value| {
            value
                .as_str()
                .is_some_and(|text| text.contains("multiple trade accounts"))
        }));
    }

    #[test]
    fn insights_with_inactive_pattern_references_are_flagged() {
        let db = test_db();
        db.upsert_agent_insight(&AgentInsightRecord {
            id: "insight-1".to_string(),
            created_at_ms: 1_000.0,
            updated_at_ms: 1_000.0,
            session_id: None,
            trade_id: None,
            setup_id: None,
            category: "behavioral".to_string(),
            status: INSIGHT_CANDIDATE.to_string(),
            summary: "Old pattern evidence".to_string(),
            evidence: json!({ "patternIds": ["inactive-pattern"] }),
            tags: Vec::new(),
            scope: json!({ "setupId": "or5" }),
            confidence: 0.5,
            salience: 0.5,
            times_surfaced: 0,
            last_surfaced_ms: None,
            superseded_by: None,
            source: "test".to_string(),
            helpful_count: 0,
            irrelevant_count: 0,
            wrong_count: 0,
        })
        .expect("insight");

        let fit = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                setup_id: None,
                include_coaching_memory: Some(true),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");

        let stale = fit
            .coaching_memory
            .get("insights")
            .and_then(|value| value.as_array())
            .and_then(|values| values.first())
            .and_then(|value| value.get("stalePatternEvidence"))
            .and_then(|value| value.as_bool());
        assert_eq!(stale, Some(true));
    }

    #[test]
    fn setup_check_or5_matches_json_fixture() {
        let db = test_db();
        db.upsert_session(&session("s1")).expect("session");
        seed_setup(&db, "or5");
        db.upsert_behavioral_pattern(&pattern(
            "win_rate_by_setup:or5",
            "win_rate_by_setup",
            40,
            -0.25,
            json!({ "setupId": "or5" }),
        ))
        .expect("setup pattern");
        db.upsert_behavioral_pattern(&pattern(
            "win_rate_by_setup_time_bucket:or5:rth_open",
            "win_rate_by_setup_time_bucket",
            24,
            0.35,
            json!({ "setupId": "or5", "timeBucket": "rth_open" }),
        ))
        .expect("time pattern");
        db.upsert_behavioral_pattern(&pattern(
            "post_loss_after_one:setup:or5",
            "post_loss_after_one",
            22,
            -0.4,
            json!({ "setupId": "or5", "postLossState": "afterOneLoss" }),
        ))
        .expect("post-loss pattern");
        db.upsert_agent_insight(&AgentInsightRecord {
            id: "insight-or5-1".to_string(),
            created_at_ms: 2_000.0,
            updated_at_ms: 2_500.0,
            session_id: Some("s1".to_string()),
            trade_id: None,
            setup_id: Some("or5".to_string()),
            category: "behavioral".to_string(),
            status: INSIGHT_CANDIDATE.to_string(),
            summary: "OR5 hesitation improves when first pullback confirms above VWAP.".to_string(),
            evidence: json!({ "patternIds": ["win_rate_by_setup:or5"] }),
            tags: vec!["or5".to_string(), "execution".to_string()],
            scope: json!({ "setupId": "or5", "timeBucket": "rth_open" }),
            confidence: 0.8,
            salience: 0.7,
            times_surfaced: 1,
            last_surfaced_ms: Some(2_400.0),
            superseded_by: None,
            source: "test".to_string(),
            helpful_count: 1,
            irrelevant_count: 0,
            wrong_count: 0,
        })
        .expect("insight");
        db.upsert_memory_followup(&MemoryFollowupRecord {
            id: "followup-or5-1".to_string(),
            created_at_ms: 3_000.0,
            resolved_at_ms: None,
            session_id: Some("s1".to_string()),
            trade_id: None,
            source: "test".to_string(),
            title: "Review OR5 entry timing".to_string(),
            detail: "Check whether the entry waited for the first pullback confirmation."
                .to_string(),
            status: "open".to_string(),
            tags: vec!["or5".to_string()],
            due_context: json!({ "setupId": "or5" }),
        })
        .expect("followup");

        let fit = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                intent: TraderContextIntent::SetupCheck,
                setup_id: Some("or5".to_string()),
                session_id: Some("s1".to_string()),
                trade_account: Some("prop-a".to_string()),
                trading_day: Some("2026-05-01".to_string()),
                session_type: Some("RTH".to_string()),
                session_segment: Some("rth_open".to_string()),
                time_bucket: Some("rth_open".to_string()),
                day_type: Some("trend".to_string()),
                include_opportunity: Some(true),
                include_coaching_memory: Some(true),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");

        let actual = serde_json::to_value(fit).expect("actual json");
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/trader_context_fit/setup_check_or5.json"
        ))
        .expect("fixture json");
        assert_eq!(actual, expected);
    }

    #[test]
    fn setup_check_or5_opportunity_matches_json_fixture() {
        let db = test_db();
        seed_setup(&db, "or5");
        db.upsert_behavioral_pattern(&pattern(
            "win_rate_by_setup:or5",
            "win_rate_by_setup",
            40,
            -0.25,
            json!({ "setupId": "or5" }),
        ))
        .expect("pattern");
        seed_positive_or5_signal_outcomes(&db);

        let fit = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                intent: TraderContextIntent::SetupCheck,
                setup_id: Some("or5".to_string()),
                trading_day: Some("2026-05-01".to_string()),
                session_type: Some("RTH".to_string()),
                include_opportunity: Some(true),
                include_coaching_memory: Some(false),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");

        let actual = serde_json::to_value(fit).expect("actual json");
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/trader_context_fit/setup_check_or5_opportunity.json"
        ))
        .expect("fixture json");
        assert_eq!(actual, expected);
    }

    #[test]
    fn opportunity_context_reports_missing_setup_id() {
        let db = test_db();
        let fit = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                setup_id: None,
                include_opportunity: Some(true),
                include_coaching_memory: Some(false),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");

        let missing_kind = fit
            .opportunity_fit
            .get("missingData")
            .and_then(|value| value.as_array())
            .and_then(|values| values.first())
            .and_then(|value| value.get("kind"))
            .and_then(|value| value.as_str());
        assert_eq!(missing_kind, Some("missingSetupId"));
    }

    #[test]
    fn opportunity_context_reports_unknown_setup_id() {
        let db = test_db();
        let fit = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                setup_id: Some("typo-setup".to_string()),
                include_opportunity: Some(true),
                include_coaching_memory: Some(false),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");

        let missing_kind = fit
            .opportunity_fit
            .get("missingData")
            .and_then(|value| value.as_array())
            .and_then(|values| values.first())
            .and_then(|value| value.get("kind"))
            .and_then(|value| value.as_str());
        assert_eq!(missing_kind, Some("unknownSetup"));
    }

    #[test]
    fn opportunity_outcomes_span_history_not_just_query_trading_day() {
        let db = test_db();
        seed_setup(&db, "or5");
        for idx in 0..30 {
            db.insert_signal_outcome(&signal_outcome_for_date(
                &format!("april-signal-{idx}"),
                "or5",
                "2026-04-01",
                1_775_052_000_000.0 + (idx as f64 * 60_000.0),
                "target_hit",
                1.0,
            ))
            .expect("april signal");
        }
        for idx in 0..5 {
            db.insert_signal_outcome(&signal_outcome_for_date(
                &format!("may-signal-{idx}"),
                "or5",
                "2026-05-01",
                1_777_644_000_000.0 + (idx as f64 * 60_000.0),
                "stop_hit",
                -1.0,
            ))
            .expect("may signal");
        }

        let fit = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                setup_id: Some("or5".to_string()),
                trading_day: Some("2026-05-01".to_string()),
                session_type: Some("RTH".to_string()),
                include_coaching_memory: Some(false),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");

        assert_eq!(
            fit.opportunity_fit
                .get("setupOutcome")
                .and_then(|value| value.get("n"))
                .and_then(|value| value.as_u64()),
            Some(35)
        );
        assert!(
            fit.opportunity_fit
                .get("setupOutcome")
                .and_then(|value| value.get("scope"))
                .and_then(|value| value.get("tradingDay"))
                .is_none(),
            "opportunity scope should not report tradingDay as a filter"
        );
    }

    #[test]
    fn execution_conflict_requires_sufficient_opportunity_and_execution_samples() {
        let db = test_db();
        seed_setup(&db, "or5");
        db.upsert_behavioral_pattern(&pattern(
            "win_rate_by_setup:or5",
            "win_rate_by_setup",
            40,
            -0.25,
            json!({ "setupId": "or5" }),
        ))
        .expect("pattern");
        for idx in 0..10 {
            db.insert_signal_outcome(&signal_outcome(
                &format!("small-signal-{idx}"),
                "or5",
                1_777_644_000_000.0 + (idx as f64 * 60_000.0),
                "target_hit",
                1.0,
            ))
            .expect("signal");
        }

        let small_opportunity = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                setup_id: Some("or5".to_string()),
                trading_day: Some("2026-05-01".to_string()),
                session_type: Some("RTH".to_string()),
                include_coaching_memory: Some(false),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");
        assert_eq!(
            small_opportunity
                .opportunity_fit
                .get("executionConflict")
                .and_then(|value| value.get("reason"))
                .and_then(|value| value.as_str()),
            Some("insufficientOpportunitySample")
        );

        let db = test_db();
        seed_setup(&db, "or5");
        seed_positive_or5_signal_outcomes(&db);
        let no_execution = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                setup_id: Some("or5".to_string()),
                trading_day: Some("2026-05-01".to_string()),
                session_type: Some("RTH".to_string()),
                include_coaching_memory: Some(false),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");
        assert_eq!(
            no_execution
                .opportunity_fit
                .get("executionConflict")
                .and_then(|value| value.get("reason"))
                .and_then(|value| value.as_str()),
            Some("insufficientExecutionSample")
        );
    }

    #[test]
    fn execution_conflict_ignores_non_material_sign_difference() {
        let db = test_db();
        seed_setup(&db, "or5");
        db.upsert_behavioral_pattern(&pattern(
            "win_rate_by_setup:or5",
            "win_rate_by_setup",
            40,
            -0.05,
            json!({ "setupId": "or5" }),
        ))
        .expect("pattern");
        let base_ms = 1_777_644_000_000.0;
        for idx in 0..20 {
            let (outcome, r_result) = if idx < 11 {
                ("target_hit", 1.0)
            } else {
                ("stop_hit", -1.0)
            };
            db.insert_signal_outcome(&signal_outcome(
                &format!("small-gap-signal-{idx}"),
                "or5",
                base_ms + (idx as f64 * 60_000.0),
                outcome,
                r_result,
            ))
            .expect("signal");
        }

        let fit = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                setup_id: Some("or5".to_string()),
                trading_day: Some("2026-05-01".to_string()),
                session_type: Some("RTH".to_string()),
                include_coaching_memory: Some(false),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");

        let conflict = fit
            .opportunity_fit
            .get("executionConflict")
            .expect("conflict");
        assert_eq!(
            conflict.get("detected").and_then(|value| value.as_bool()),
            Some(false)
        );
        assert_eq!(
            conflict.get("reason").and_then(|value| value.as_str()),
            Some("nonMaterialGap")
        );
    }

    #[test]
    fn execution_conflict_slice_is_not_limited_by_display_budget() {
        let db = test_db();
        seed_setup(&db, "or5");
        seed_positive_or5_signal_outcomes(&db);
        db.upsert_behavioral_pattern(&pattern(
            "post_loss_after_one:setup:or5",
            "post_loss_after_one",
            40,
            -0.6,
            json!({ "setupId": "or5", "postLossState": "afterOneLoss" }),
        ))
        .expect("post loss one");
        db.upsert_behavioral_pattern(&pattern(
            "post_loss_after_two_plus:setup:or5",
            "post_loss_after_two_plus",
            40,
            -0.7,
            json!({ "setupId": "or5", "postLossState": "afterTwoPlusLosses" }),
        ))
        .expect("post loss two");
        db.upsert_behavioral_pattern(&pattern(
            "win_rate_by_setup:or5",
            "win_rate_by_setup",
            40,
            -0.25,
            json!({ "setupId": "or5" }),
        ))
        .expect("setup pattern");

        let fit = build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                intent: TraderContextIntent::TradeTaken,
                setup_id: Some("or5".to_string()),
                trading_day: Some("2026-05-01".to_string()),
                session_type: Some("RTH".to_string()),
                include_coaching_memory: Some(false),
                ..TraderContextFitQuery::default()
            },
        )
        .expect("fit");

        let matching_len = fit
            .execution_fit
            .get("matchingSlices")
            .and_then(|value| value.as_array())
            .map(Vec::len);
        assert_eq!(matching_len, Some(2));
        assert_eq!(
            fit.opportunity_fit
                .get("executionConflict")
                .and_then(|value| value.get("comparedExecutionSlice"))
                .and_then(|value| value.get("id"))
                .and_then(|value| value.as_str()),
            Some("win_rate_by_setup:or5")
        );
    }
}
