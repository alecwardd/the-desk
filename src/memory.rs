use crate::db::{Database, DbError, SessionRecord, SessionSummary, TradeRecord};
use crate::tick_time_context_from_timestamp_ms;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use thiserror::Error;

pub const INSIGHT_CANDIDATE: &str = "candidate";
pub const INSIGHT_VALIDATED: &str = "validated";
pub const INSIGHT_STALE: &str = "stale";
pub const INSIGHT_SUPERSEDED: &str = "superseded";
pub const INSIGHT_DISMISSED: &str = "dismissed";
pub const INSIGHT_PINNED: &str = "pinned";

pub const FOLLOWUP_OPEN: &str = "open";
pub const FOLLOWUP_RESOLVED: &str = "resolved";
pub const FOLLOWUP_DISMISSED: &str = "dismissed";

#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("{0}")]
    Validation(String),
    #[error(transparent)]
    Db(#[from] DbError),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AgentInsightRecord {
    pub id: String,
    pub created_at_ms: f64,
    pub updated_at_ms: f64,
    pub session_id: Option<String>,
    pub trade_id: Option<String>,
    pub setup_id: Option<String>,
    pub category: String,
    pub status: String,
    pub summary: String,
    pub evidence: serde_json::Value,
    pub tags: Vec<String>,
    pub scope: serde_json::Value,
    pub confidence: f64,
    pub salience: f64,
    pub times_surfaced: i64,
    pub last_surfaced_ms: Option<f64>,
    pub superseded_by: Option<String>,
    pub source: String,
    pub helpful_count: i64,
    pub irrelevant_count: i64,
    pub wrong_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AgentInsightQuery {
    pub category: Option<String>,
    pub setup_id: Option<String>,
    pub statuses: Option<Vec<String>>,
    pub tag: Option<String>,
    pub session_type: Option<String>,
    pub session_segment: Option<String>,
    pub time_bucket: Option<String>,
    pub day_type: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehavioralPatternRecord {
    pub id: String,
    pub detected_at_ms: f64,
    pub pattern_type: String,
    pub description: String,
    pub metric: serde_json::Value,
    pub scope: serde_json::Value,
    pub sample_size: i64,
    pub confidence: f64,
    pub active: bool,
    pub superseded_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BehavioralPatternQuery {
    pub pattern_type: Option<String>,
    pub session_type: Option<String>,
    pub session_segment: Option<String>,
    pub time_bucket: Option<String>,
    pub day_type: Option<String>,
    pub setup_id: Option<String>,
    pub min_sample_size: Option<i64>,
    pub active_only: Option<bool>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MemoryFollowupRecord {
    pub id: String,
    pub created_at_ms: f64,
    pub resolved_at_ms: Option<f64>,
    pub session_id: Option<String>,
    pub trade_id: Option<String>,
    pub source: String,
    pub title: String,
    pub detail: String,
    pub status: String,
    pub tags: Vec<String>,
    pub due_context: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MemoryFollowupQuery {
    pub status: Option<String>,
    pub session_id: Option<String>,
    pub trade_id: Option<String>,
    pub setup_id: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MemoryBriefQuery {
    pub intent: String,
    pub session_id: Option<String>,
    pub setup_id: Option<String>,
    pub session_type: Option<String>,
    pub session_segment: Option<String>,
    pub day_type: Option<String>,
    pub time_bucket: Option<String>,
    pub pre_session_note: Option<String>,
    pub limit: Option<usize>,
    pub include_recent_sessions: Option<bool>,
    pub include_patterns: Option<bool>,
    pub include_insights: Option<bool>,
    pub include_followups: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemorySessionSnapshot {
    pub session: SessionRecord,
    pub trade_count: usize,
    pub closed_trade_count: usize,
    pub gross_points: f64,
    pub net_r: f64,
    pub emotional_states: Vec<String>,
    pub mistake_tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryMaintenanceState {
    pub patterns_last_refreshed_at_ms: Option<f64>,
    pub insights_lifecycle_last_refreshed_at_ms: Option<f64>,
    pub patterns_dirty: bool,
    pub insights_lifecycle_dirty: bool,
    pub dirty_since_ms: Option<f64>,
    pub dirty_reasons: Vec<String>,
    pub last_refresh_reason: Option<String>,
    pub refresh_suggested: bool,
}

impl Default for MemoryMaintenanceState {
    fn default() -> Self {
        Self {
            patterns_last_refreshed_at_ms: None,
            insights_lifecycle_last_refreshed_at_ms: None,
            patterns_dirty: true,
            insights_lifecycle_dirty: true,
            dirty_since_ms: None,
            dirty_reasons: Vec::new(),
            last_refresh_reason: None,
            refresh_suggested: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MemoryRefreshOptions {
    pub refresh_patterns: bool,
    pub refresh_insight_lifecycle: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryRefreshResult {
    pub refreshed_at_ms: f64,
    pub stale_insights_updated: usize,
    pub patterns: Vec<BehavioralPatternRecord>,
    pub maintenance: MemoryMaintenanceState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryBrief {
    pub recent_sessions: Vec<MemorySessionSnapshot>,
    pub patterns: Vec<BehavioralPatternRecord>,
    pub insights: Vec<AgentInsightRecord>,
    pub followups: Vec<MemoryFollowupRecord>,
    pub summary: serde_json::Value,
    pub pre_session_note: Option<String>,
    pub retrieval_context: serde_json::Value,
    pub memory_maintenance: MemoryMaintenanceState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SaveAgentInsightInput {
    pub id: Option<String>,
    pub session_id: Option<String>,
    pub trade_id: Option<String>,
    pub setup_id: Option<String>,
    pub category: String,
    pub summary: String,
    pub evidence: serde_json::Value,
    pub tags: Option<Vec<String>>,
    pub scope: Option<serde_json::Value>,
    pub confidence: Option<f64>,
    pub salience: Option<f64>,
    pub source: Option<String>,
}

fn scope_value(scope: &serde_json::Value, key: &str) -> Option<String> {
    scope
        .get(key)
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

fn clamp_unit(value: Option<f64>, default: f64) -> f64 {
    value.unwrap_or(default).clamp(0.0, 1.0)
}

fn normalize_memory_maintenance_state(mut state: MemoryMaintenanceState) -> MemoryMaintenanceState {
    let mut dirty_reasons = std::mem::take(&mut state.dirty_reasons);
    let mut deduped = Vec::with_capacity(dirty_reasons.len());
    for reason in dirty_reasons.drain(..) {
        let trimmed = reason.trim();
        if trimmed.is_empty() || deduped.iter().any(|existing| existing == trimmed) {
            continue;
        }
        deduped.push(trimmed.to_string());
    }
    if deduped.len() > 8 {
        let keep_from = deduped.len() - 8;
        deduped.drain(0..keep_from);
    }
    state.dirty_reasons = deduped;
    if !state.patterns_dirty && !state.insights_lifecycle_dirty {
        state.dirty_since_ms = None;
        state.dirty_reasons.clear();
    }
    state.refresh_suggested = state.patterns_dirty
        || state.insights_lifecycle_dirty
        || state.patterns_last_refreshed_at_ms.is_none()
        || state.insights_lifecycle_last_refreshed_at_ms.is_none();
    state
}

fn load_memory_maintenance_state(db: &Database) -> Result<MemoryMaintenanceState, MemoryError> {
    Ok(normalize_memory_maintenance_state(
        db.get_memory_maintenance_state()?,
    ))
}

fn persist_memory_maintenance_state(
    db: &Database,
    state: MemoryMaintenanceState,
) -> Result<MemoryMaintenanceState, MemoryError> {
    let normalized = normalize_memory_maintenance_state(state);
    db.upsert_memory_maintenance_state(&normalized)?;
    Ok(normalized)
}

fn set_memory_maintenance_fresh(
    db: &Database,
    patterns_refreshed_at_ms: Option<f64>,
    insights_lifecycle_refreshed_at_ms: Option<f64>,
    reason: Option<&str>,
) -> Result<MemoryMaintenanceState, MemoryError> {
    let mut state = load_memory_maintenance_state(db)?;
    if let Some(timestamp_ms) = patterns_refreshed_at_ms {
        state.patterns_last_refreshed_at_ms = Some(timestamp_ms);
        state.patterns_dirty = false;
    }
    if let Some(timestamp_ms) = insights_lifecycle_refreshed_at_ms {
        state.insights_lifecycle_last_refreshed_at_ms = Some(timestamp_ms);
        state.insights_lifecycle_dirty = false;
    }
    if let Some(reason) = reason.map(str::trim).filter(|reason| !reason.is_empty()) {
        state.last_refresh_reason = Some(reason.to_string());
    }
    persist_memory_maintenance_state(db, state)
}

pub fn mark_memory_dirty(
    db: &Database,
    patterns_dirty: bool,
    insights_lifecycle_dirty: bool,
    reason: &str,
) -> Result<MemoryMaintenanceState, MemoryError> {
    let now_ms = Utc::now().timestamp_millis() as f64;
    let mut state = load_memory_maintenance_state(db)?;
    if patterns_dirty {
        state.patterns_dirty = true;
    }
    if insights_lifecycle_dirty {
        state.insights_lifecycle_dirty = true;
    }
    if (patterns_dirty || insights_lifecycle_dirty) && state.dirty_since_ms.is_none() {
        state.dirty_since_ms = Some(now_ms);
    }
    let trimmed = reason.trim();
    if !trimmed.is_empty()
        && !state
            .dirty_reasons
            .iter()
            .any(|existing| existing == trimmed)
    {
        state.dirty_reasons.push(trimmed.to_string());
    }
    persist_memory_maintenance_state(db, state)
}

pub fn time_bucket_from_timestamp_ms(timestamp_ms: f64) -> String {
    if let Some(ctx) = tick_time_context_from_timestamp_ms(timestamp_ms) {
        if ctx.session_type == crate::SessionType::Globex {
            return match ctx.session_segment {
                crate::SessionSegment::Asia => "globex_asia".to_string(),
                crate::SessionSegment::London => "globex_london".to_string(),
                crate::SessionSegment::None => "globex".to_string(),
            };
        }
        if ctx.et_minutes < crate::RTH_OPEN_ET {
            return "pre_open".to_string();
        }
        if ctx.et_minutes < 10 * 60 + 30 {
            return "rth_open".to_string();
        }
        if ctx.et_minutes < 13 * 60 {
            return "rth_midday".to_string();
        }
        if ctx.et_minutes < crate::RTH_CLOSE_ET {
            return "rth_afternoon".to_string();
        }
    }
    "transition".to_string()
}

fn normalized_text(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase()
}

fn active_recall_status(status: &str) -> bool {
    matches!(
        status,
        INSIGHT_CANDIDATE | INSIGHT_VALIDATED | INSIGHT_PINNED
    )
}

fn empty_json(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => true,
        serde_json::Value::Object(map) => map.is_empty(),
        serde_json::Value::Array(items) => items.is_empty(),
        _ => false,
    }
}

fn status_weight(status: &str) -> f64 {
    match status {
        INSIGHT_PINNED => 3.0,
        INSIGHT_VALIDATED => 2.0,
        INSIGHT_CANDIDATE => 1.0,
        _ => 0.0,
    }
}

fn scope_match_score(
    scope: &serde_json::Value,
    setup_id: Option<&str>,
    session_type: Option<&str>,
    session_segment: Option<&str>,
    day_type: Option<&str>,
    time_bucket: Option<&str>,
) -> f64 {
    let mut score = 0.0;
    if let Some(setup_id) = setup_id {
        if scope_value(scope, "setupId").as_deref() == Some(setup_id) {
            score += 5.0;
        }
    }
    if let Some(session_type) = session_type {
        if scope_value(scope, "sessionType")
            .map(|value| value.eq_ignore_ascii_case(session_type))
            .unwrap_or(false)
        {
            score += 2.0;
        }
    }
    if let Some(session_segment) = session_segment {
        if scope_value(scope, "sessionSegment")
            .map(|value| value.eq_ignore_ascii_case(session_segment))
            .unwrap_or(false)
        {
            score += 1.5;
        }
    }
    if let Some(day_type) = day_type {
        if scope_value(scope, "dayType")
            .map(|value| value.eq_ignore_ascii_case(day_type))
            .unwrap_or(false)
        {
            score += 1.5;
        }
    }
    if let Some(time_bucket) = time_bucket {
        if scope_value(scope, "timeBucket").as_deref() == Some(time_bucket) {
            score += 1.5;
        }
    }
    score
}

fn insight_rank(
    insight: &AgentInsightRecord,
    setup_id: Option<&str>,
    session_type: Option<&str>,
    session_segment: Option<&str>,
    day_type: Option<&str>,
    time_bucket: Option<&str>,
    now_ms: f64,
) -> f64 {
    let context = scope_match_score(
        &insight.scope,
        setup_id,
        session_type,
        session_segment,
        day_type,
        time_bucket,
    );
    let recency_days = ((now_ms - insight.updated_at_ms).max(0.0)) / 86_400_000.0;
    let recency = (30.0 - recency_days).max(0.0) / 30.0;
    let cooldown_days = insight
        .last_surfaced_ms
        .map(|last_surfaced_ms| ((now_ms - last_surfaced_ms).max(0.0)) / 86_400_000.0)
        .unwrap_or(999.0);
    let cooldown_penalty = if cooldown_days < 1.0 { 1.5 } else { 0.0 };
    (context * 10.0)
        + (status_weight(&insight.status) * 5.0)
        + (insight.confidence * 2.5)
        + insight.salience
        + recency
        - cooldown_penalty
}

fn pattern_rank(
    pattern: &BehavioralPatternRecord,
    setup_id: Option<&str>,
    session_type: Option<&str>,
    session_segment: Option<&str>,
    day_type: Option<&str>,
    time_bucket: Option<&str>,
) -> f64 {
    let context = scope_match_score(
        &pattern.scope,
        setup_id,
        session_type,
        session_segment,
        day_type,
        time_bucket,
    );
    (context * 10.0) + pattern.confidence * 3.0 + (pattern.sample_size as f64 / 10.0)
}

fn followup_rank(
    followup: &MemoryFollowupRecord,
    setup_id: Option<&str>,
    session_id: Option<&str>,
    session_type: Option<&str>,
    session_segment: Option<&str>,
    day_type: Option<&str>,
    time_bucket: Option<&str>,
) -> f64 {
    let mut score = scope_match_score(
        &followup.due_context,
        setup_id,
        session_type,
        session_segment,
        day_type,
        time_bucket,
    );
    if let Some(session_id) = session_id {
        if followup.session_id.as_deref() == Some(session_id) {
            score += 3.0;
        }
    }
    score + (followup.created_at_ms / 86_400_000.0)
}

fn trade_result_sign(trade: &TradeRecord) -> Option<i32> {
    if let Some(result_r) = trade.result_r {
        if result_r > 0.0 {
            Some(1)
        } else if result_r < 0.0 {
            Some(-1)
        } else {
            Some(0)
        }
    } else {
        trade.gross_points.map(|gross_points| {
            if gross_points > 0.0 {
                1
            } else if gross_points < 0.0 {
                -1
            } else {
                0
            }
        })
    }
}

fn session_summary_by_record<'a>(
    session: &SessionRecord,
    summaries: &'a [SessionSummary],
) -> Option<&'a SessionSummary> {
    let target_type = match session.session_type.to_ascii_lowercase().as_str() {
        "rth" => "RTH",
        "globex" => "Globex",
        _ => "Unknown",
    };
    summaries
        .iter()
        .find(|summary| summary.session_date == session.date && summary.session_type == target_type)
}

fn build_recent_session_snapshots(
    db: &Database,
    limit: usize,
) -> Result<Vec<MemorySessionSnapshot>, DbError> {
    let sessions = db.list_sessions(limit)?;
    let mut snapshots = Vec::new();
    for session in sessions
        .into_iter()
        .filter(|session| session.end_time.is_some())
    {
        let trades = db.list_trades_for_session(&session.id)?;
        let trade_count = trades.len();
        let closed_trade_count = trades
            .iter()
            .filter(|trade| trade.exit_time.is_some())
            .count();
        let gross_points = trades
            .iter()
            .filter_map(|trade| trade.gross_points)
            .sum::<f64>();
        let net_r = trades
            .iter()
            .filter_map(|trade| trade.result_r)
            .sum::<f64>();
        let mut emotional_states: Vec<String> = trades
            .iter()
            .filter_map(|trade| trade.emotional_state.clone())
            .collect();
        emotional_states.sort();
        emotional_states.dedup();
        let mut mistake_counts: HashMap<String, usize> = HashMap::new();
        for trade in &trades {
            for tag in &trade.mistake_tags {
                *mistake_counts.entry(tag.clone()).or_default() += 1;
            }
        }
        let mut mistake_tags: Vec<(String, usize)> = mistake_counts.into_iter().collect();
        mistake_tags.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        snapshots.push(MemorySessionSnapshot {
            session,
            trade_count,
            closed_trade_count,
            gross_points,
            net_r,
            emotional_states,
            mistake_tags: mistake_tags
                .into_iter()
                .take(3)
                .map(|(tag, _)| tag)
                .collect(),
        });
    }
    Ok(snapshots)
}

pub fn refresh_insight_lifecycle(db: &Database, now_ms: f64) -> Result<usize, MemoryError> {
    let sessions = db.list_sessions(500)?;
    let insights = db.list_agent_insights(&AgentInsightQuery {
        limit: Some(1000),
        ..AgentInsightQuery::default()
    })?;
    let mut stale_updates = 0usize;
    for insight in insights {
        if matches!(
            insight.status.as_str(),
            INSIGHT_PINNED | INSIGHT_DISMISSED | INSIGHT_SUPERSEDED | INSIGHT_STALE
        ) {
            continue;
        }
        let age_days = ((now_ms - insight.updated_at_ms).max(0.0)) / 86_400_000.0;
        let later_sessions = sessions
            .iter()
            .filter(|session| session.start_time > insight.created_at_ms)
            .count();
        let should_stale = match insight.status.as_str() {
            INSIGHT_CANDIDATE => age_days >= 14.0 || later_sessions >= 5,
            INSIGHT_VALIDATED => age_days >= 45.0 || later_sessions >= 15,
            _ => false,
        };
        if should_stale {
            db.update_agent_insight_status(&insight.id, INSIGHT_STALE, now_ms)?;
            stale_updates += 1;
        }
    }
    set_memory_maintenance_fresh(db, None, Some(now_ms), Some("refresh_insight_lifecycle"))?;
    Ok(stale_updates)
}

pub fn save_agent_insight(
    db: &Database,
    input: SaveAgentInsightInput,
) -> Result<AgentInsightRecord, MemoryError> {
    let summary = input.summary.trim();
    if summary.is_empty() {
        return Err(MemoryError::Validation(
            "insight summary must not be empty".to_string(),
        ));
    }
    if empty_json(&input.evidence) {
        return Err(MemoryError::Validation(
            "insight evidence must not be empty".to_string(),
        ));
    }

    let now_ms = Utc::now().timestamp_millis() as f64;
    refresh_insight_lifecycle(db, now_ms)?;
    let mut scope = input.scope.unwrap_or_else(|| json!({}));
    if scope.get("setupId").is_none() {
        if let Some(setup_id) = &input.setup_id {
            scope["setupId"] = json!(setup_id);
        }
    }
    if scope.get("timeBucket").is_none() {
        if let Some(trade_id) = &input.trade_id {
            if let Some(trade) = db.get_trade(trade_id)? {
                scope["timeBucket"] = json!(time_bucket_from_timestamp_ms(trade.entry_time));
            }
        } else if let Some(session_id) = &input.session_id {
            if let Some(session) = db.get_session(session_id)? {
                scope["timeBucket"] = json!(time_bucket_from_timestamp_ms(session.start_time));
            }
        }
    }

    let normalized = normalized_text(summary);
    let existing = db.list_agent_insights(&AgentInsightQuery {
        category: Some(input.category.clone()),
        setup_id: input.setup_id.clone(),
        limit: Some(250),
        ..AgentInsightQuery::default()
    })?;
    let has_pattern_support = input
        .evidence
        .get("patternIds")
        .and_then(|value| value.as_array())
        .map(|ids| !ids.is_empty())
        .unwrap_or(false);
    let reinforced = existing.iter().any(|other| {
        other.id != input.id.clone().unwrap_or_default()
            && normalized_text(&other.summary) == normalized
            && other.session_id != input.session_id
            && active_recall_status(&other.status)
    });
    let status = if reinforced || has_pattern_support {
        INSIGHT_VALIDATED.to_string()
    } else {
        INSIGHT_CANDIDATE.to_string()
    };
    let record = AgentInsightRecord {
        id: input.id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
        session_id: input.session_id,
        trade_id: input.trade_id,
        setup_id: input.setup_id,
        category: input.category,
        status,
        summary: summary.to_string(),
        evidence: input.evidence,
        tags: input.tags.unwrap_or_default(),
        scope,
        confidence: clamp_unit(input.confidence, 0.5),
        salience: clamp_unit(input.salience, 0.5),
        times_surfaced: 0,
        last_surfaced_ms: None,
        superseded_by: None,
        source: input.source.unwrap_or_else(|| "agent".to_string()),
        helpful_count: 0,
        irrelevant_count: 0,
        wrong_count: 0,
    };
    db.upsert_agent_insight(&record)?;
    if record.status == INSIGHT_VALIDATED {
        for other in existing.into_iter().filter(|other| {
            normalized_text(&other.summary) == normalized && other.status == INSIGHT_CANDIDATE
        }) {
            db.update_agent_insight_status(&other.id, INSIGHT_VALIDATED, now_ms)?;
        }
    }
    Ok(record)
}

pub fn detect_behavioral_patterns(
    db: &Database,
) -> Result<Vec<BehavioralPatternRecord>, MemoryError> {
    let sessions = db.list_sessions(250)?;
    let summaries = db.list_session_summaries(None, None, None, None, 500)?;
    let now_ms = Utc::now().timestamp_millis() as f64;
    let mut patterns = Vec::new();
    let mut setup_stats: HashMap<String, (i64, i64, f64)> = HashMap::new();
    let mut bucket_stats: HashMap<String, (i64, i64, f64)> = HashMap::new();
    let mut emotion_stats: HashMap<String, (i64, i64, i64)> = HashMap::new();
    let mut ordinal_stats: HashMap<String, (i64, i64)> = HashMap::new();
    let mut segment_stats: HashMap<String, (i64, i64)> = HashMap::new();
    let mut mistake_tag_counts: HashMap<String, i64> = HashMap::new();
    let mut day_type_stats: HashMap<String, (i64, f64, f64)> = HashMap::new();
    let mut after_loss_rules_broken = 0i64;
    let mut after_loss_total = 0i64;

    for session in &sessions {
        let trades = db.list_trades_for_session(&session.id)?;
        let session_summary = session_summary_by_record(session, &summaries);
        let day_type = session_summary.map(|summary| summary.day_type.clone());
        for (index, trade) in trades.iter().enumerate() {
            if let Some(outcome) = trade_result_sign(trade) {
                let bucket = time_bucket_from_timestamp_ms(trade.entry_time);
                let setup_key = trade
                    .setup_id
                    .clone()
                    .unwrap_or_else(|| "unclassified".to_string());
                let setup_entry = setup_stats.entry(setup_key).or_insert((0, 0, 0.0));
                setup_entry.0 += 1;
                if outcome > 0 {
                    setup_entry.1 += 1;
                }
                setup_entry.2 += trade.result_r.unwrap_or(0.0);

                let bucket_entry = bucket_stats.entry(bucket.clone()).or_insert((0, 0, 0.0));
                bucket_entry.0 += 1;
                if outcome > 0 {
                    bucket_entry.1 += 1;
                }
                bucket_entry.2 += trade.result_r.unwrap_or(0.0);

                let ordinal_key = if index >= 2 {
                    "trade_3_plus".to_string()
                } else {
                    format!("trade_{}", index + 1)
                };
                let ordinal_entry = ordinal_stats.entry(ordinal_key).or_insert((0, 0));
                ordinal_entry.0 += 1;
                if outcome > 0 {
                    ordinal_entry.1 += 1;
                }

                if let Some(emotion) = &trade.emotional_state {
                    let emotion_entry = emotion_stats.entry(emotion.clone()).or_insert((0, 0, 0));
                    emotion_entry.0 += 1;
                    if outcome > 0 {
                        emotion_entry.1 += 1;
                    } else if outcome < 0 {
                        emotion_entry.2 += 1;
                    }
                }

                for tag in &trade.mistake_tags {
                    *mistake_tag_counts.entry(tag.clone()).or_default() += 1;
                }

                if let Some(day_type) = &day_type {
                    let day_entry = day_type_stats
                        .entry(day_type.clone())
                        .or_insert((0, 0.0, 0.0));
                    day_entry.0 += 1;
                    day_entry.1 += trade.result_r.unwrap_or(0.0);
                    day_entry.2 += trade.gross_points.unwrap_or(0.0);
                }

                if let Some(ctx) = tick_time_context_from_timestamp_ms(trade.entry_time) {
                    let segment_key = if ctx.session_type == crate::SessionType::Globex {
                        match ctx.session_segment {
                            crate::SessionSegment::Asia => "Asia",
                            crate::SessionSegment::London => "London",
                            crate::SessionSegment::None => "Globex",
                        }
                    } else {
                        "RTH"
                    };
                    let segment_entry = segment_stats
                        .entry(segment_key.to_string())
                        .or_insert((0, 0));
                    segment_entry.0 += 1;
                    if !trade.planned {
                        segment_entry.1 += 1;
                    }
                }
            }

            if index > 0 && trade_result_sign(&trades[index - 1]) == Some(-1) {
                after_loss_total += 1;
                if matches!(trade.rules_followed, Some(false)) {
                    after_loss_rules_broken += 1;
                }
            }
        }
    }

    db.deactivate_behavioral_patterns()?;

    for (setup_id, (resolved, wins, total_r)) in setup_stats {
        if resolved < 2 {
            continue;
        }
        let win_rate = wins as f64 / resolved as f64;
        patterns.push(BehavioralPatternRecord {
            id: format!("win_rate_by_setup:{setup_id}"),
            detected_at_ms: now_ms,
            pattern_type: "win_rate_by_setup".to_string(),
            description: format!(
                "{setup_id} resolved {resolved} trades with {:.0}% win rate and {:.2}R average.",
                win_rate * 100.0,
                total_r / resolved as f64
            ),
            metric: json!({
                "resolved": resolved,
                "wins": wins,
                "winRate": win_rate,
                "avgR": total_r / resolved as f64,
            }),
            scope: json!({ "setupId": setup_id }),
            sample_size: resolved,
            confidence: clamp_unit(Some((resolved as f64 / 20.0).min(1.0)), 0.5),
            active: true,
            superseded_by: None,
        });
    }

    for (bucket, (resolved, wins, total_r)) in bucket_stats {
        if resolved < 3 {
            continue;
        }
        let win_rate = wins as f64 / resolved as f64;
        patterns.push(BehavioralPatternRecord {
            id: format!("win_rate_by_time_bucket:{bucket}"),
            detected_at_ms: now_ms,
            pattern_type: "win_rate_by_time_bucket".to_string(),
            description: format!(
                "{bucket} resolved {resolved} trades with {:.0}% win rate and {:.2}R average.",
                win_rate * 100.0,
                total_r / resolved as f64
            ),
            metric: json!({
                "resolved": resolved,
                "wins": wins,
                "winRate": win_rate,
                "avgR": total_r / resolved as f64,
            }),
            scope: json!({ "timeBucket": bucket }),
            sample_size: resolved,
            confidence: clamp_unit(Some((resolved as f64 / 20.0).min(1.0)), 0.5),
            active: true,
            superseded_by: None,
        });
    }

    if after_loss_total >= 3 {
        let frequency = after_loss_rules_broken as f64 / after_loss_total as f64;
        patterns.push(BehavioralPatternRecord {
            id: "rules_broken_after_loss".to_string(),
            detected_at_ms: now_ms,
            pattern_type: "rules_broken_after_loss".to_string(),
            description: format!(
                "After a loss, rules were broken on {:.0}% of the next trades across {after_loss_total} opportunities.",
                frequency * 100.0
            ),
            metric: json!({
                "opportunities": after_loss_total,
                "rulesBroken": after_loss_rules_broken,
                "frequency": frequency,
            }),
            scope: json!({}),
            sample_size: after_loss_total,
            confidence: clamp_unit(Some((after_loss_total as f64 / 15.0).min(1.0)), 0.5),
            active: true,
            superseded_by: None,
        });
    }

    for (segment, (count, unplanned)) in segment_stats {
        if count < 3 {
            continue;
        }
        let unplanned_rate = unplanned as f64 / count as f64;
        patterns.push(BehavioralPatternRecord {
            id: format!("planned_vs_unplanned_by_segment:{segment}"),
            detected_at_ms: now_ms,
            pattern_type: "planned_vs_unplanned_by_session_segment".to_string(),
            description: format!(
                "{segment} trades were unplanned {:.0}% of the time across {count} trades.",
                unplanned_rate * 100.0
            ),
            metric: json!({
                "tradeCount": count,
                "unplannedCount": unplanned,
                "unplannedRate": unplanned_rate,
            }),
            scope: json!({ "sessionSegment": segment }),
            sample_size: count,
            confidence: clamp_unit(Some((count as f64 / 20.0).min(1.0)), 0.5),
            active: true,
            superseded_by: None,
        });
    }

    for (emotion, (count, wins, losses)) in emotion_stats {
        if count < 2 {
            continue;
        }
        let win_rate = wins as f64 / count as f64;
        patterns.push(BehavioralPatternRecord {
            id: format!("emotional_state_by_outcome:{emotion}"),
            detected_at_ms: now_ms,
            pattern_type: "emotional_state_by_outcome".to_string(),
            description: format!(
                "{emotion} appeared on {count} trades with {:.0}% win rate ({wins} wins / {losses} losses).",
                win_rate * 100.0
            ),
            metric: json!({
                "tradeCount": count,
                "wins": wins,
                "losses": losses,
                "winRate": win_rate,
            }),
            scope: json!({ "emotionalState": emotion }),
            sample_size: count,
            confidence: clamp_unit(Some((count as f64 / 10.0).min(1.0)), 0.5),
            active: true,
            superseded_by: None,
        });
    }

    for (day_type, (count, total_r, total_points)) in day_type_stats {
        if count < 2 {
            continue;
        }
        patterns.push(BehavioralPatternRecord {
            id: format!("gross_points_avg_r_by_day_type:{day_type}"),
            detected_at_ms: now_ms,
            pattern_type: "gross_points_avg_r_by_day_type".to_string(),
            description: format!(
                "{day_type} days averaged {:.2}R and {:.2} gross points across {count} trades.",
                total_r / count as f64,
                total_points / count as f64
            ),
            metric: json!({
                "tradeCount": count,
                "avgR": total_r / count as f64,
                "avgGrossPoints": total_points / count as f64,
            }),
            scope: json!({ "dayType": day_type }),
            sample_size: count,
            confidence: clamp_unit(Some((count as f64 / 15.0).min(1.0)), 0.5),
            active: true,
            superseded_by: None,
        });
    }

    for (ordinal, (count, wins)) in ordinal_stats {
        if count < 2 {
            continue;
        }
        patterns.push(BehavioralPatternRecord {
            id: format!("trade_count_position_vs_outcome:{ordinal}"),
            detected_at_ms: now_ms,
            pattern_type: "trade_count_position_vs_outcome".to_string(),
            description: format!(
                "{ordinal} resolved {count} trades with {:.0}% win rate.",
                wins as f64 / count as f64 * 100.0
            ),
            metric: json!({
                "tradeCount": count,
                "wins": wins,
                "winRate": wins as f64 / count as f64,
            }),
            scope: json!({ "tradeOrdinal": ordinal }),
            sample_size: count,
            confidence: clamp_unit(Some((count as f64 / 12.0).min(1.0)), 0.5),
            active: true,
            superseded_by: None,
        });
    }

    let mut mistake_tags: Vec<(String, i64)> = mistake_tag_counts.into_iter().collect();
    mistake_tags.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    for (tag, count) in mistake_tags.into_iter().take(8) {
        patterns.push(BehavioralPatternRecord {
            id: format!("mistake_tag_frequency:{tag}"),
            detected_at_ms: now_ms,
            pattern_type: "mistake_tag_frequency".to_string(),
            description: format!(
                "Mistake tag `{tag}` appeared {count} times in the trailing review set."
            ),
            metric: json!({ "count": count }),
            scope: json!({ "mistakeTag": tag }),
            sample_size: count,
            confidence: clamp_unit(Some((count as f64 / 10.0).min(1.0)), 0.5),
            active: true,
            superseded_by: None,
        });
    }

    for pattern in &patterns {
        db.upsert_behavioral_pattern(pattern)?;
    }
    set_memory_maintenance_fresh(db, Some(now_ms), None, Some("detect_behavioral_patterns"))?;
    Ok(patterns)
}

pub fn refresh_memory_state(
    db: &Database,
    options: MemoryRefreshOptions,
    reason: Option<&str>,
) -> Result<MemoryRefreshResult, MemoryError> {
    let now_ms = Utc::now().timestamp_millis() as f64;
    let stale_insights_updated = if options.refresh_insight_lifecycle {
        refresh_insight_lifecycle(db, now_ms)?
    } else {
        0
    };
    let patterns = if options.refresh_patterns {
        detect_behavioral_patterns(db)?
    } else {
        Vec::new()
    };
    let maintenance = if options.refresh_patterns || options.refresh_insight_lifecycle {
        set_memory_maintenance_fresh(
            db,
            options.refresh_patterns.then_some(now_ms),
            options.refresh_insight_lifecycle.then_some(now_ms),
            reason,
        )?
    } else {
        load_memory_maintenance_state(db)?
    };
    Ok(MemoryRefreshResult {
        refreshed_at_ms: now_ms,
        stale_insights_updated,
        patterns,
        maintenance,
    })
}

pub fn build_memory_brief(
    db: &Database,
    query: MemoryBriefQuery,
) -> Result<MemoryBrief, MemoryError> {
    let now_ms = Utc::now().timestamp_millis() as f64;
    let limit = query.limit.unwrap_or(5).max(1);
    let include_recent_sessions = query.include_recent_sessions.unwrap_or(true);
    let include_patterns = query.include_patterns.unwrap_or(true);
    let include_insights = query.include_insights.unwrap_or(true);
    let include_followups = query.include_followups.unwrap_or(true);
    let current_context = if let Some(session_id) = &query.session_id {
        db.get_session(session_id)?
            .map(|session| {
                json!({
                    "sessionType": session.session_type,
                    "timeBucket": time_bucket_from_timestamp_ms(session.start_time),
                })
            })
            .unwrap_or_else(|| json!({}))
    } else if let Some(ctx) = tick_time_context_from_timestamp_ms(now_ms) {
        json!({
            "sessionType": match ctx.session_type {
                crate::SessionType::Rth => "RTH",
                crate::SessionType::Globex => "Globex",
                crate::SessionType::Unknown => "Unknown",
            },
            "sessionSegment": match ctx.session_segment {
                crate::SessionSegment::Asia => "Asia",
                crate::SessionSegment::London => "London",
                crate::SessionSegment::None => "None",
            },
            "timeBucket": time_bucket_from_timestamp_ms(now_ms),
        })
    } else {
        json!({})
    };

    let session_type = query
        .session_type
        .clone()
        .or_else(|| scope_value(&current_context, "sessionType"));
    let session_segment = query
        .session_segment
        .clone()
        .or_else(|| scope_value(&current_context, "sessionSegment"));
    let time_bucket = query
        .time_bucket
        .clone()
        .or_else(|| scope_value(&current_context, "timeBucket"));

    let mut patterns = if include_patterns {
        db.list_behavioral_patterns(&BehavioralPatternQuery {
            setup_id: query.setup_id.clone(),
            active_only: Some(true),
            limit: Some(50),
            ..BehavioralPatternQuery::default()
        })?
    } else {
        Vec::new()
    };
    if include_patterns {
        patterns.sort_by(|a, b| {
            pattern_rank(
                b,
                query.setup_id.as_deref(),
                session_type.as_deref(),
                session_segment.as_deref(),
                query.day_type.as_deref(),
                time_bucket.as_deref(),
            )
            .partial_cmp(&pattern_rank(
                a,
                query.setup_id.as_deref(),
                session_type.as_deref(),
                session_segment.as_deref(),
                query.day_type.as_deref(),
                time_bucket.as_deref(),
            ))
            .unwrap_or(std::cmp::Ordering::Equal)
        });
        patterns.truncate(limit);
    }

    let mut insights = if include_insights {
        db.list_agent_insights(&AgentInsightQuery {
            setup_id: query.setup_id.clone(),
            statuses: Some(vec![
                INSIGHT_PINNED.to_string(),
                INSIGHT_VALIDATED.to_string(),
                INSIGHT_CANDIDATE.to_string(),
            ]),
            limit: Some(100),
            ..AgentInsightQuery::default()
        })?
    } else {
        Vec::new()
    };
    if include_insights {
        insights.retain(|insight| active_recall_status(&insight.status));
        insights.sort_by(|a, b| {
            insight_rank(
                b,
                query.setup_id.as_deref(),
                session_type.as_deref(),
                session_segment.as_deref(),
                query.day_type.as_deref(),
                time_bucket.as_deref(),
                now_ms,
            )
            .partial_cmp(&insight_rank(
                a,
                query.setup_id.as_deref(),
                session_type.as_deref(),
                session_segment.as_deref(),
                query.day_type.as_deref(),
                time_bucket.as_deref(),
                now_ms,
            ))
            .unwrap_or(std::cmp::Ordering::Equal)
        });
        insights.truncate(limit);
    }

    let mut followups = if include_followups {
        db.list_memory_followups(&MemoryFollowupQuery {
            status: Some(FOLLOWUP_OPEN.to_string()),
            session_id: query.session_id.clone(),
            setup_id: query.setup_id.clone(),
            limit: Some(50),
            ..MemoryFollowupQuery::default()
        })?
    } else {
        Vec::new()
    };
    if include_followups {
        followups.sort_by(|a, b| {
            followup_rank(
                b,
                query.setup_id.as_deref(),
                query.session_id.as_deref(),
                session_type.as_deref(),
                session_segment.as_deref(),
                query.day_type.as_deref(),
                time_bucket.as_deref(),
            )
            .partial_cmp(&followup_rank(
                a,
                query.setup_id.as_deref(),
                query.session_id.as_deref(),
                session_type.as_deref(),
                session_segment.as_deref(),
                query.day_type.as_deref(),
                time_bucket.as_deref(),
            ))
            .unwrap_or(std::cmp::Ordering::Equal)
        });
        followups.truncate(limit);
    }

    let recent_sessions = if include_recent_sessions {
        build_recent_session_snapshots(db, limit.min(5))?
    } else {
        Vec::new()
    };
    let memory_maintenance = load_memory_maintenance_state(db)?;
    let pre_session_note = if let Some(pre_session_note) = query.pre_session_note {
        Some(pre_session_note)
    } else if let Some(session_id) = &query.session_id {
        db.get_session(session_id)?
            .and_then(|session| session.pre_session_note)
    } else {
        None
    };
    let summary = json!({
        "recentSessionCount": recent_sessions.len(),
        "patternCount": patterns.len(),
        "insightCount": insights.len(),
        "followupCount": followups.len(),
        "topPatternType": patterns.first().map(|pattern| pattern.pattern_type.clone()),
        "topInsightStatus": insights.first().map(|insight| insight.status.clone()),
        "requestedSections": {
            "recentSessions": include_recent_sessions,
            "patterns": include_patterns,
            "insights": include_insights,
            "followups": include_followups,
        },
        "refreshSuggested": memory_maintenance.refresh_suggested,
    });

    Ok(MemoryBrief {
        recent_sessions,
        patterns,
        insights,
        followups,
        summary,
        pre_session_note,
        retrieval_context: json!({
            "intent": query.intent,
            "setupId": query.setup_id,
            "sessionId": query.session_id,
            "sessionType": session_type,
            "sessionSegment": session_segment,
            "dayType": query.day_type,
            "timeBucket": time_bucket,
            "includeRecentSessions": include_recent_sessions,
            "includePatterns": include_patterns,
            "includeInsights": include_insights,
            "includeFollowups": include_followups,
        }),
        memory_maintenance,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{SessionRecord, TradeRecord};
    use crate::rules::SetupDefinition;

    fn test_db() -> Database {
        Database::open(":memory:").expect("db")
    }

    fn seed_session(db: &Database, id: &str, date: &str, start_time: f64) {
        db.create_session(&SessionRecord {
            id: id.to_string(),
            date: date.to_string(),
            session_type: "rth".to_string(),
            start_time,
            end_time: Some(start_time + 10_000.0),
            recording_path: None,
            pre_session_note: Some("Focus on process".to_string()),
        })
        .expect("session");
    }

    fn seed_setup(db: &Database, id: &str) {
        let setup = SetupDefinition {
            id: id.to_string(),
            name: id.to_string(),
            active: true,
            ..SetupDefinition::default()
        };
        db.upsert_setup(&setup).expect("setup");
    }

    #[test]
    fn save_agent_insight_promotes_reinforced_items() {
        let db = test_db();
        seed_session(&db, "s1", "2026-03-04", 1_000.0);
        seed_session(&db, "s2", "2026-03-05", 2_000.0);
        save_agent_insight(
            &db,
            SaveAgentInsightInput {
                session_id: Some("s1".to_string()),
                category: "behavioral".to_string(),
                summary: "hesitates on clean OR5 confirmation".to_string(),
                evidence: json!({"example": 1}),
                ..SaveAgentInsightInput::default()
            },
        )
        .expect("candidate");
        let second = save_agent_insight(
            &db,
            SaveAgentInsightInput {
                session_id: Some("s2".to_string()),
                category: "behavioral".to_string(),
                summary: "hesitates on clean OR5 confirmation".to_string(),
                evidence: json!({"example": 2}),
                ..SaveAgentInsightInput::default()
            },
        )
        .expect("validated");
        assert_eq!(second.status, INSIGHT_VALIDATED);
    }

    #[test]
    fn candidate_insight_decays_to_stale() {
        let db = test_db();
        seed_session(&db, "s1", "2026-03-01", 0.0);
        let insight = save_agent_insight(
            &db,
            SaveAgentInsightInput {
                session_id: Some("s1".to_string()),
                category: "behavioral".to_string(),
                summary: "forces entries after a miss".to_string(),
                evidence: json!({"sample": 1}),
                ..SaveAgentInsightInput::default()
            },
        )
        .expect("save");
        refresh_insight_lifecycle(&db, insight.updated_at_ms + (20.0 * 86_400_000.0))
            .expect("refresh");
        let loaded = db
            .get_agent_insight(&insight.id)
            .expect("get")
            .expect("exists");
        assert_eq!(loaded.status, INSIGHT_STALE);
    }

    #[test]
    fn memory_brief_prefers_matching_setup_context() {
        let db = test_db();
        seed_session(&db, "s1", "2026-03-04", 1_000.0);
        seed_setup(&db, "or5");
        db.upsert_behavioral_pattern(&BehavioralPatternRecord {
            id: "pattern-1".to_string(),
            detected_at_ms: 1.0,
            pattern_type: "win_rate_by_setup".to_string(),
            description: "OR5 has worked well".to_string(),
            metric: json!({"winRate": 0.7}),
            scope: json!({"setupId": "or5"}),
            sample_size: 8,
            confidence: 0.8,
            active: true,
            superseded_by: None,
        })
        .expect("pattern");
        save_agent_insight(
            &db,
            SaveAgentInsightInput {
                session_id: Some("s1".to_string()),
                setup_id: Some("or5".to_string()),
                category: "playbook".to_string(),
                summary: "OR5 retests work best after patience".to_string(),
                evidence: json!({"sample": 1}),
                scope: Some(json!({"setupId": "or5"})),
                ..SaveAgentInsightInput::default()
            },
        )
        .expect("insight");
        let brief = build_memory_brief(
            &db,
            MemoryBriefQuery {
                intent: "setup_check".to_string(),
                setup_id: Some("or5".to_string()),
                limit: Some(3),
                ..MemoryBriefQuery::default()
            },
        )
        .expect("brief");
        assert_eq!(brief.patterns.len(), 1);
        assert_eq!(brief.insights.len(), 1);
        assert_eq!(brief.patterns[0].scope["setupId"], "or5");
    }

    #[test]
    fn detect_behavioral_patterns_builds_repeatable_outputs() {
        let db = test_db();
        seed_session(&db, "s1", "2026-03-04", 1_000.0);
        seed_setup(&db, "or5");
        db.upsert_trade(&TradeRecord {
            id: "t1".to_string(),
            session_id: Some("s1".to_string()),
            setup_id: Some("or5".to_string()),
            instrument: None,
            trade_account: None,
            entry_time: 1_000.0,
            entry_price: 21_000.0,
            exit_time: Some(2_000.0),
            exit_price: Some(21_010.0),
            direction: "long".to_string(),
            size: 1,
            max_open_size: None,
            stop_price: None,
            target_prices: Vec::new(),
            result_r: Some(1.0),
            gross_points: Some(10.0),
            planned: true,
            rules_followed: Some(true),
            emotional_state: Some("Calm".to_string()),
            thesis: None,
            review_tags: Vec::new(),
            mistake_tags: vec!["late_entry".to_string()],
            entry_fill_count: 1,
            exit_fill_count: 1,
            import_batch_id: None,
            notes: String::new(),
            source: "manual".to_string(),
        })
        .expect("trade1");
        db.upsert_trade(&TradeRecord {
            id: "t2".to_string(),
            session_id: Some("s1".to_string()),
            setup_id: Some("or5".to_string()),
            instrument: None,
            trade_account: None,
            entry_time: 1_500.0,
            entry_price: 21_000.0,
            exit_time: Some(2_500.0),
            exit_price: Some(20_990.0),
            direction: "long".to_string(),
            size: 1,
            max_open_size: None,
            stop_price: None,
            target_prices: Vec::new(),
            result_r: Some(-1.0),
            gross_points: Some(-10.0),
            planned: false,
            rules_followed: Some(false),
            emotional_state: Some("Frustrated".to_string()),
            thesis: None,
            review_tags: Vec::new(),
            mistake_tags: vec!["late_entry".to_string()],
            entry_fill_count: 1,
            exit_fill_count: 1,
            import_batch_id: None,
            notes: String::new(),
            source: "manual".to_string(),
        })
        .expect("trade2");
        db.upsert_session_summary(&SessionSummary {
            session_date: "2026-03-04".to_string(),
            session_type: "RTH".to_string(),
            root_symbol: "NQ".to_string(),
            contract_symbol: "NQH26.CME".to_string(),
            contract_month: Some("2026-03".to_string()),
            symbol_resolution_mode: "hybrid".to_string(),
            carry_forward_levels_valid: true,
            rollover_warning: None,
            open_price: 21_000.0,
            high: 21_020.0,
            low: 20_980.0,
            close: 21_005.0,
            poc: 21_000.0,
            vah: 21_010.0,
            val: 20_990.0,
            ib_high: 21_010.0,
            ib_low: 20_990.0,
            ib_range: 20.0,
            ib_mid: 21_000.0,
            or_high: 21_005.0,
            or_low: 20_995.0,
            day_type: "Trend".to_string(),
            profile_shape: "P".to_string(),
            balance_state: "Imbalanced".to_string(),
            total_volume: 1000.0,
            tick_count: 100,
            session_delta: 200.0,
            cumulative_delta: 200.0,
            dnp: 21_000.0,
            dnva_high: 21_005.0,
            dnva_low: 20_995.0,
            vwap_close: 21_001.0,
            signal_count: 1,
            single_prints_direction: "up".to_string(),
            excess_high: false,
            excess_low: false,
            poor_high: false,
            poor_low: false,
            rvol_ratio: 1.1,
            close_vs_ib_mid: "above".to_string(),
            close_vs_vwap: "above".to_string(),
            close_vs_poc: "above".to_string(),
            snapshot_json: None,
        })
        .expect("summary");
        let patterns = detect_behavioral_patterns(&db).expect("detect");
        assert!(patterns
            .iter()
            .any(|pattern| pattern.pattern_type == "win_rate_by_setup"));
        assert!(patterns
            .iter()
            .any(|pattern| pattern.pattern_type == "mistake_tag_frequency"));
    }

    #[test]
    fn build_memory_brief_is_read_only_and_reports_refresh_state() {
        let db = test_db();
        seed_session(&db, "s1", "2026-03-04", 1_000.0);
        db.upsert_agent_insight(&AgentInsightRecord {
            id: "candidate-1".to_string(),
            created_at_ms: 1.0,
            updated_at_ms: 1.0,
            session_id: Some("s1".to_string()),
            trade_id: None,
            setup_id: None,
            category: "behavioral".to_string(),
            status: INSIGHT_CANDIDATE.to_string(),
            summary: "forces late entries".to_string(),
            evidence: json!({"sample": 1}),
            tags: Vec::new(),
            scope: json!({}),
            confidence: 0.5,
            salience: 0.5,
            times_surfaced: 0,
            last_surfaced_ms: None,
            superseded_by: None,
            source: "agent".to_string(),
            helpful_count: 0,
            irrelevant_count: 0,
            wrong_count: 0,
        })
        .expect("seed insight");
        mark_memory_dirty(&db, true, true, "seeded_test_state").expect("dirty");

        let brief = build_memory_brief(
            &db,
            MemoryBriefQuery {
                intent: "weekly_review".to_string(),
                limit: Some(3),
                ..MemoryBriefQuery::default()
            },
        )
        .expect("brief");

        let loaded = db
            .get_agent_insight("candidate-1")
            .expect("get")
            .expect("exists");
        assert_eq!(loaded.status, INSIGHT_CANDIDATE);
        assert!(brief.memory_maintenance.refresh_suggested);
        assert!(brief.memory_maintenance.patterns_dirty);
        assert!(brief.memory_maintenance.insights_lifecycle_dirty);
    }

    #[test]
    fn refresh_memory_state_clears_dirty_flags_and_honors_section_flags() {
        let db = test_db();
        seed_session(&db, "s1", "2026-03-04", 1_000.0);
        seed_setup(&db, "or5");
        db.upsert_trade(&TradeRecord {
            id: "t1".to_string(),
            session_id: Some("s1".to_string()),
            setup_id: Some("or5".to_string()),
            instrument: None,
            trade_account: None,
            entry_time: 1_000.0,
            entry_price: 21_000.0,
            exit_time: Some(2_000.0),
            exit_price: Some(21_010.0),
            direction: "long".to_string(),
            size: 1,
            max_open_size: None,
            stop_price: None,
            target_prices: Vec::new(),
            result_r: Some(1.0),
            gross_points: Some(10.0),
            planned: true,
            rules_followed: Some(true),
            emotional_state: Some("Calm".to_string()),
            thesis: None,
            review_tags: Vec::new(),
            mistake_tags: vec!["late_entry".to_string()],
            entry_fill_count: 1,
            exit_fill_count: 1,
            import_batch_id: None,
            notes: String::new(),
            source: "manual".to_string(),
        })
        .expect("trade");
        db.upsert_trade(&TradeRecord {
            id: "t2".to_string(),
            session_id: Some("s1".to_string()),
            setup_id: Some("or5".to_string()),
            instrument: None,
            trade_account: None,
            entry_time: 1_500.0,
            entry_price: 21_000.0,
            exit_time: Some(2_500.0),
            exit_price: Some(20_995.0),
            direction: "long".to_string(),
            size: 1,
            max_open_size: None,
            stop_price: None,
            target_prices: Vec::new(),
            result_r: Some(-0.5),
            gross_points: Some(-5.0),
            planned: true,
            rules_followed: Some(false),
            emotional_state: Some("Calm".to_string()),
            thesis: None,
            review_tags: Vec::new(),
            mistake_tags: vec!["late_entry".to_string()],
            entry_fill_count: 1,
            exit_fill_count: 1,
            import_batch_id: None,
            notes: String::new(),
            source: "manual".to_string(),
        })
        .expect("trade2");
        mark_memory_dirty(&db, true, true, "trade_import").expect("dirty");

        let refresh = refresh_memory_state(
            &db,
            MemoryRefreshOptions {
                refresh_patterns: true,
                refresh_insight_lifecycle: true,
            },
            Some("manual_refresh"),
        )
        .expect("refresh");
        assert!(!refresh.maintenance.patterns_dirty);
        assert!(!refresh.maintenance.insights_lifecycle_dirty);
        assert_eq!(
            refresh.maintenance.last_refresh_reason.as_deref(),
            Some("manual_refresh")
        );
        assert!(refresh.maintenance.patterns_last_refreshed_at_ms.is_some());
        assert!(refresh
            .patterns
            .iter()
            .any(|pattern| pattern.pattern_type == "win_rate_by_setup"));

        let brief = build_memory_brief(
            &db,
            MemoryBriefQuery {
                intent: "setup_check".to_string(),
                setup_id: Some("or5".to_string()),
                include_recent_sessions: Some(false),
                include_followups: Some(false),
                limit: Some(3),
                ..MemoryBriefQuery::default()
            },
        )
        .expect("brief");
        assert!(brief.recent_sessions.is_empty());
        assert!(brief.followups.is_empty());
        assert!(!brief.memory_maintenance.refresh_suggested);
    }
}
