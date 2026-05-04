use crate::db::{Database, TradeRecord};
use crate::memory::{
    time_bucket_from_timestamp_ms, AgentInsightQuery, BehavioralPatternQuery,
    BehavioralPatternRecord, MemoryError, MemoryFollowupQuery,
};
use crate::research::{reliability_tier, ReliabilityTier};
use crate::trading_day_from_timestamp_ms;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashSet;
use std::str::FromStr;

const PATTERN_LOAD_CAP: usize = 300;

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
            "summary": "Opportunity data is separate from trader execution.",
            "setupOutcome": null,
            "contextFrameAnalog": {
                "available": true,
                "source": "get_context_frame",
                "detailAvailableByCalling": "get_context_frame",
                "caveats": ["Context-frame analogs are not executed-trade memory."]
            },
            "missingData": []
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
    use crate::db::{Database, SessionRecord, TradeRecord};
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
        db.upsert_setup(&SetupDefinition {
            id: "or5".to_string(),
            name: "OR5".to_string(),
            active: true,
            ..SetupDefinition::default()
        })
        .expect("setup");
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
}
