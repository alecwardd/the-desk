use crate::db::{
    market_event_id, stable_hash_hex, AttentionSignalEventRecord, AttentionSignalRecord,
    SetupRuntimeStateRecord, TradeIdeaCardRecord,
};
use crate::pipelines::{MarketEvent, MarketState};
use crate::risk::RiskState;
use crate::rules::{SetupReadiness, SetupState};
use std::collections::{BTreeMap, BTreeSet};

const DEFAULT_SIGNAL_TTL_MS: f64 = 30.0 * 60_000.0;
const ABSENCE_READY_MINUTE: i32 = 90;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttentionPulseKind {
    EventDriven,
    Periodic,
}

#[derive(Debug, Clone)]
pub struct SignalComposerConfig {
    pub signal_ttl_ms: f64,
    pub absence_ready_minute: i32,
}

impl Default for SignalComposerConfig {
    fn default() -> Self {
        Self {
            signal_ttl_ms: DEFAULT_SIGNAL_TTL_MS,
            absence_ready_minute: ABSENCE_READY_MINUTE,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SignalComposerInput<'a> {
    pub pulse_kind: AttentionPulseKind,
    pub events: &'a [MarketEvent],
    pub setup_states: &'a [SetupRuntimeStateRecord],
    pub risk_state: Option<&'a RiskState>,
    pub market_snapshot: &'a MarketState,
    pub prior_active_signals: &'a [AttentionSignalRecord],
    pub timestamp_ms: f64,
    pub source: &'a str,
    pub job_id: Option<&'a str>,
}

#[derive(Debug, Clone, Default)]
pub struct AttentionComposeOutput {
    pub signals: Vec<AttentionSignalRecord>,
    pub signal_events: Vec<AttentionSignalEventRecord>,
    pub idea_cards: Vec<TradeIdeaCardRecord>,
}

#[derive(Debug, Clone)]
pub struct SignalComposer {
    config: SignalComposerConfig,
}

impl Default for SignalComposer {
    fn default() -> Self {
        Self::new(SignalComposerConfig::default())
    }
}

impl SignalComposer {
    pub fn new(config: SignalComposerConfig) -> Self {
        Self { config }
    }

    pub fn compose(&self, input: SignalComposerInput<'_>) -> AttentionComposeOutput {
        let mut output = AttentionComposeOutput::default();
        let mut grouped_events: BTreeMap<String, Vec<&MarketEvent>> = BTreeMap::new();
        for event in input.events {
            let Some((kind, _)) = classify_event_kind(&event.event_type) else {
                continue;
            };
            let subject = event_subject(event);
            let scope = signal_scope(input.market_snapshot, event);
            grouped_events
                .entry(format!("{kind}:{subject}:{scope}"))
                .or_default()
                .push(event);
        }
        for events in grouped_events.values() {
            if let Some(signal) = self.signal_from_market_event_group(&input, events) {
                self.push_signal(&input, &mut output, signal);
            }
        }

        for setup in input.setup_states {
            if let Some(signal) = self.signal_from_setup_state(&input, setup) {
                self.push_signal(&input, &mut output, signal);
            }
            if let Some(idea) = self.idea_from_setup_state(&input, setup) {
                output.idea_cards.push(idea);
            }
        }

        if let Some(risk_state) = input.risk_state {
            if let Some(signal) = self.signal_from_risk_state(&input, risk_state) {
                self.push_signal(&input, &mut output, signal);
            }
        }

        if input.pulse_kind == AttentionPulseKind::Periodic {
            if let Some(signal) = self.absence_signal(&input) {
                self.push_signal(&input, &mut output, signal);
            }
        }

        output
    }

    fn push_signal(
        &self,
        input: &SignalComposerInput<'_>,
        output: &mut AttentionComposeOutput,
        signal: AttentionSignalRecord,
    ) {
        let previous = input
            .prior_active_signals
            .iter()
            .find(|prior| prior.signal_id == signal.signal_id);
        if should_suppress_update(previous, &signal, input.timestamp_ms) {
            return;
        }
        let event_type = match previous {
            None => Some("created"),
            Some(prior) if prior.priority != signal.priority => Some("priority_changed"),
            Some(prior) if source_events_grew(prior, &signal) => Some("evidence_added"),
            Some(_) => None,
        };
        if let Some(event_type) = event_type {
            let event_id = format!(
                "ase_{}",
                stable_hash_hex(&format!(
                    "{}|{}|{:.3}|{}|{}",
                    signal.signal_id,
                    event_type,
                    input.timestamp_ms,
                    signal.priority,
                    signal.source_event_ids.join(",")
                ))
            );
            output.signal_events.push(AttentionSignalEventRecord {
                event_id,
                signal_id: signal.signal_id.clone(),
                event_type: event_type.to_string(),
                occurred_at_ms: input.timestamp_ms,
                session_date: signal.session_date.clone(),
                source: input.source.to_string(),
                actor: None,
                note: None,
                payload: serde_json::json!({
                    "priority": signal.priority,
                    "priorityScore": signal.priority_score,
                    "kind": signal.kind,
                    "dedupeKey": signal.dedupe_key,
                }),
            });
        }
        output.signals.push(signal);
    }

    fn signal_from_market_event_group(
        &self,
        input: &SignalComposerInput<'_>,
        events: &[&MarketEvent],
    ) -> Option<AttentionSignalRecord> {
        let event = *events.first()?;
        let (kind, base_weight) = classify_event_kind(&event.event_type)?;
        let subject = event_subject(event);
        let scope = signal_scope(input.market_snapshot, event);
        let dedupe_key = format!("{kind}:{subject}:{scope}");
        let raw_event_ids: Vec<String> =
            events.iter().map(|event| market_event_id(event)).collect();
        let signal_id = signal_id(&dedupe_key, &event.session_date, input.source, input.job_id);
        let source_event_ids = merged_source_event_ids(input, &signal_id, raw_event_ids);
        let event_bonus = events
            .iter()
            .map(|event| event_severity_bonus(event))
            .fold(0.0, f64::max);
        let composite_bonus = if events.len() > 1 { 10.0 } else { 0.0 };
        let staleness_decay = staleness_decay(input, &signal_id);
        let priority_score =
            (base_weight + event_bonus + composite_bonus - staleness_decay).max(0.0);
        let priority = priority_bucket(priority_score);
        let title = match kind {
            "market_structure_change" if events.len() > 1 => {
                format!("Composite structure change at {subject}")
            }
            "flow_confirmation" if events.len() > 1 => {
                format!("Composite flow change at {subject}")
            }
            "market_structure_change" => format!("Structure changed at {subject}"),
            "flow_confirmation" => format!("Flow changed at {subject}"),
            _ => format!("Market event: {}", event.event_type),
        };
        let summary = format!(
            "Your playbook says this is an attention event, not an entry by itself: {} event(s) near {:.2}.",
            events.len(), event.price
        );
        Some(base_signal(
            self.config.signal_ttl_ms,
            input,
            signal_id,
            dedupe_key,
            kind,
            title,
            summary,
            priority,
            priority_score,
            source_event_ids,
            None,
            None,
            None,
            suggested_tools_for_kind(kind),
            serde_json::json!({
                "eventTypes": events.iter().map(|event| event.event_type.clone()).collect::<Vec<_>>(),
                "levelName": event.level_name,
                "direction": event.direction,
                "sequenceNum": event.sequence_num,
                "metadata": events.iter().map(|event| event.metadata.clone()).collect::<Vec<_>>(),
                "priorityBreakdown": {
                    "kindWeight": base_weight,
                    "eventSeverityBonus": event_bonus,
                    "compositeBonus": composite_bonus,
                    "lifecycleWeight": 0.0,
                    "riskWeight": 0.0,
                    "stalenessDecay": staleness_decay
                },
                "conditionFields": events.iter().map(|event| event.event_type.clone()).collect::<Vec<_>>(),
            }),
        ))
    }

    fn signal_from_setup_state(
        &self,
        input: &SignalComposerInput<'_>,
        setup: &SetupRuntimeStateRecord,
    ) -> Option<AttentionSignalRecord> {
        let lifecycle_weight = lifecycle_weight(&setup.state, &setup.readiness)?;
        let subject = setup.setup_id.clone();
        let scope = setup.session_date.clone();
        let dedupe_key = format!("setup_lifecycle_change:{subject}:{scope}");
        let signal_id = signal_id(&dedupe_key, &setup.session_date, input.source, input.job_id);
        let idea_id = idea_id_for_setup(&setup.session_date, &setup.setup_id, input.source);
        let staleness_decay = staleness_decay(input, &signal_id);
        let priority_score =
            (35.0 + lifecycle_weight + setup.readiness_score * 20.0 - staleness_decay).max(0.0);
        let priority = priority_bucket(priority_score);
        let setup_name = setup
            .setup_name
            .clone()
            .unwrap_or_else(|| setup.setup_id.clone());
        let title = format!("Setup lifecycle changed: {setup_name}");
        let summary = format!(
            "Your playbook says {setup_name} is {:?} with readiness {:?}; discretionary confirmation still belongs to the trader.",
            setup.state, setup.readiness
        );
        Some(base_signal(
            self.config.signal_ttl_ms,
            input,
            signal_id,
            dedupe_key,
            "setup_lifecycle_change",
            title,
            summary,
            priority,
            priority_score,
            Vec::new(),
            Some(setup.setup_id.clone()),
            None,
            Some(idea_id),
            vec![
                "get_setup_context".to_string(),
                "get_setup_state_history".to_string(),
                "get_risk_state".to_string(),
            ],
            serde_json::json!({
                "setupId": setup.setup_id,
                "setupName": setup.setup_name,
                "state": setup.state,
                "readiness": setup.readiness,
                "readinessScore": setup.readiness_score,
                "metConditions": setup.met_conditions,
                "missingConditions": setup.missing_conditions,
                "priorityBreakdown": {
                    "kindWeight": 35.0,
                    "lifecycleWeight": lifecycle_weight,
                    "riskWeight": 0.0,
                    "stalenessDecay": staleness_decay
                },
                "conditionFields": setup.met_conditions,
            }),
        ))
    }

    fn signal_from_risk_state(
        &self,
        input: &SignalComposerInput<'_>,
        risk: &RiskState,
    ) -> Option<AttentionSignalRecord> {
        let risk_weight = if risk.at_limit {
            55.0
        } else if risk.consecutive_losses >= 3 {
            45.0
        } else if risk.drawdown_r >= 2.0 {
            35.0
        } else {
            return None;
        };
        let subject = if risk.at_limit {
            "at_limit"
        } else if risk.consecutive_losses >= 3 {
            "consecutive_losses"
        } else {
            "drawdown"
        };
        let dedupe_key = format!(
            "risk_context_change:{}:{}",
            subject, input.market_snapshot.trading_day
        );
        let signal_id = signal_id(
            &dedupe_key,
            &input.market_snapshot.trading_day,
            input.source,
            input.job_id,
        );
        let staleness_decay = staleness_decay(input, &signal_id);
        let priority_score = (30.0 + risk_weight - staleness_decay).max(0.0);
        let priority = priority_bucket(priority_score);
        Some(base_signal(
            self.config.signal_ttl_ms,
            input,
            signal_id,
            dedupe_key,
            "risk_context_change",
            "Risk context changed".to_string(),
            "Your risk framework says this state deserves attention before evaluating any new trade idea.".to_string(),
            priority,
            priority_score,
            Vec::new(),
            None,
            None,
            None,
            vec!["get_risk_state".to_string(), "get_risk_config".to_string()],
            serde_json::json!({
                "riskState": {
                    "dailyPnlR": risk.daily_pnl_r,
                    "tradeCount": risk.trade_count,
                    "consecutiveLosses": risk.consecutive_losses,
                    "consecutiveWins": risk.consecutive_wins,
                    "drawdownR": risk.drawdown_r,
                    "maxDailyLossR": risk.max_daily_loss_r,
                    "atLimit": risk.at_limit
                },
                "priorityBreakdown": {
                    "kindWeight": 30.0,
                    "riskWeight": risk_weight,
                    "lifecycleWeight": 0.0,
                    "stalenessDecay": staleness_decay
                },
                "conditionFields": ["risk_state"],
            }),
        ))
    }

    fn absence_signal(&self, input: &SignalComposerInput<'_>) -> Option<AttentionSignalRecord> {
        if input.market_snapshot.session_type != "RTH" {
            return None;
        }
        let minute = crate::minute_of_session_from_timestamp(input.timestamp_ms);
        if minute < self.config.absence_ready_minute {
            return None;
        }
        let any_ready = input.setup_states.iter().any(|setup| {
            matches!(
                setup.readiness,
                SetupReadiness::DeterministicReady
                    | SetupReadiness::Confirmed
                    | SetupReadiness::InTrade
            )
        });
        if any_ready {
            return None;
        }
        let dedupe_key = format!(
            "absence_or_staleness:no_ready_setup:{}",
            input.market_snapshot.trading_day
        );
        let signal_id = signal_id(
            &dedupe_key,
            &input.market_snapshot.trading_day,
            input.source,
            input.job_id,
        );
        let staleness_decay = staleness_decay(input, &signal_id);
        let priority_score = (42.0 - staleness_decay).max(0.0);
        Some(base_signal(
            self.config.signal_ttl_ms,
            input,
            signal_id,
            dedupe_key,
            "absence_or_staleness",
            "No setup has reached deterministic-ready".to_string(),
            "Your playbook has not produced a deterministic-ready setup after the opening development window; this is a stand-aside attention check, not a trade instruction.".to_string(),
            priority_bucket(priority_score),
            priority_score,
            Vec::new(),
            None,
            None,
            None,
            vec!["evaluate_playbook".to_string(), "get_day_type".to_string(), "get_risk_state".to_string()],
            serde_json::json!({
                "minuteOfSession": minute,
                "absenceRule": "no_setup_ready_after_opening_development",
                "priorityBreakdown": {
                    "kindWeight": 42.0,
                    "lifecycleWeight": 0.0,
                    "riskWeight": 0.0,
                    "stalenessDecay": staleness_decay
                },
                "conditionFields": ["setup_readiness"],
            }),
        ))
    }

    fn idea_from_setup_state(
        &self,
        input: &SignalComposerInput<'_>,
        setup: &SetupRuntimeStateRecord,
    ) -> Option<TradeIdeaCardRecord> {
        let lifecycle = idea_lifecycle(&setup.state, &setup.readiness)?;
        let setup_name = setup
            .setup_name
            .clone()
            .unwrap_or_else(|| setup.setup_id.clone());
        let idea_id = idea_id_for_setup(&setup.session_date, &setup.setup_id, input.source);
        let attention_dedupe_key = format!(
            "setup_lifecycle_change:{}:{}",
            setup.setup_id, setup.session_date
        );
        let attention_signal_id = signal_id(
            &attention_dedupe_key,
            &setup.session_date,
            input.source,
            input.job_id,
        );
        let risk_context = input
            .risk_state
            .map(|risk| {
                serde_json::json!({
                    "dailyPnlR": risk.daily_pnl_r,
                    "tradeCount": risk.trade_count,
                    "consecutiveLosses": risk.consecutive_losses,
                    "consecutiveWins": risk.consecutive_wins,
                    "drawdownR": risk.drawdown_r,
                    "maxDailyLossR": risk.max_daily_loss_r,
                    "atLimit": risk.at_limit
                })
            })
            .unwrap_or_else(|| serde_json::json!({}));
        Some(TradeIdeaCardRecord {
            idea_id,
            status: if lifecycle == "resolved" {
                "closed".to_string()
            } else {
                "active".to_string()
            },
            lifecycle: lifecycle.to_string(),
            thesis: format!(
                "Your playbook is tracking {setup_name} as {}.",
                setup_state_label(&setup.state)
            ),
            missing_confirmation: setup.missing_conditions.clone(),
            invalidation: setup
                .missing_conditions
                .iter()
                .map(|condition| format!("Idea weakens if condition remains missing: {condition}"))
                .collect(),
            management_context: serde_json::json!({
                "currentPrice": setup.current_price,
                "state": setup_state_label(&setup.state),
                "readiness": setup_readiness_label(&setup.readiness),
            }),
            risk_context,
            linked_setup_id: Some(setup.setup_id.clone()),
            linked_signal_outcome_id: None,
            linked_attention_signal_id: Some(attention_signal_id),
            session_date: setup.session_date.clone(),
            trading_day: input.market_snapshot.trading_day.clone(),
            root_symbol: setup.root_symbol.clone(),
            contract_symbol: setup.contract_symbol.clone(),
            created_at_ms: setup.last_transition_at_ms,
            updated_at_ms: input.timestamp_ms,
            resolved_at_ms: if lifecycle == "resolved" {
                Some(input.timestamp_ms)
            } else {
                None
            },
            payload: serde_json::json!({
                "setupId": setup.setup_id,
                "setupName": setup.setup_name,
                "metConditions": setup.met_conditions,
                "requiresDiscretionary": setup.requires_discretionary,
            }),
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn base_signal(
    ttl_ms: f64,
    input: &SignalComposerInput<'_>,
    signal_id: String,
    dedupe_key: String,
    kind: &str,
    title: String,
    summary: String,
    priority: &str,
    priority_score: f64,
    source_event_ids: Vec<String>,
    linked_setup_id: Option<String>,
    linked_signal_outcome_id: Option<String>,
    linked_idea_id: Option<String>,
    suggested_tools: Vec<String>,
    payload: serde_json::Value,
) -> AttentionSignalRecord {
    let snapshot = input.market_snapshot;
    AttentionSignalRecord {
        signal_id,
        dedupe_key,
        status: "active".to_string(),
        priority: priority.to_string(),
        priority_score,
        confidence: 1.0,
        kind: kind.to_string(),
        title,
        summary,
        created_at_ms: input.timestamp_ms,
        updated_at_ms: input.timestamp_ms,
        last_seen_ms: input.timestamp_ms,
        expires_at_ms: Some(input.timestamp_ms + ttl_ms),
        session_date: if snapshot.session_type == "Globex" {
            snapshot.trading_day.clone()
        } else {
            crate::session_date_from_timestamp_ms(input.timestamp_ms)
        },
        trading_day: snapshot.trading_day.clone(),
        session_type: snapshot.session_type.clone(),
        session_segment: snapshot.session_segment.clone(),
        root_symbol: Some(snapshot.root_symbol.clone()).filter(|s| !s.is_empty()),
        contract_symbol: Some(snapshot.contract_symbol.clone()).filter(|s| !s.is_empty()),
        current_price: snapshot.last_price,
        source: input.source.to_string(),
        job_id: input.job_id.map(str::to_string),
        source_event_ids,
        linked_setup_id,
        linked_setup_transition_id: None,
        linked_signal_outcome_id,
        linked_idea_id,
        suggested_tools,
        acknowledged_by: None,
        acknowledged_at_ms: None,
        acknowledgement_note: None,
        payload,
    }
}

fn classify_event_kind(event_type: &str) -> Option<(&'static str, f64)> {
    match event_type {
        "dnp_cross"
        | "or5_mid_retest"
        | "ib_extension_hit"
        | "day_type_change"
        | "new_session_high"
        | "new_session_low"
        | "poor_high_detected"
        | "poor_low_detected"
        | "excess_high_detected"
        | "excess_low_detected" => Some(("market_structure_change", 25.0)),
        "absorption_confirmed"
        | "absorption_invalidated"
        | "pinch_detected"
        | "acceleration_zone_held"
        | "large_trade_cluster" => Some(("flow_confirmation", 32.0)),
        "absorption_detected" | "acceleration_zone_created" => Some(("flow_confirmation", 22.0)),
        _ => None,
    }
}

fn event_severity_bonus(event: &MarketEvent) -> f64 {
    event
        .metadata
        .as_ref()
        .and_then(|m| m.get("severity"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0)
        .clamp(0.0, 5.0)
        * 4.0
}

fn event_subject(event: &MarketEvent) -> String {
    event
        .level_name
        .clone()
        .or_else(|| event.direction.clone())
        .unwrap_or_else(|| event.event_type.clone())
}

fn merged_source_event_ids(
    input: &SignalComposerInput<'_>,
    signal_id: &str,
    new_ids: Vec<String>,
) -> Vec<String> {
    let mut ids = input
        .prior_active_signals
        .iter()
        .find(|prior| prior.signal_id == signal_id)
        .map(|prior| prior.source_event_ids.clone())
        .unwrap_or_default();
    ids.extend(new_ids);
    ids.sort();
    ids.dedup();
    ids
}

fn source_events_grew(previous: &AttentionSignalRecord, signal: &AttentionSignalRecord) -> bool {
    let previous_ids = previous.source_event_ids.iter().collect::<BTreeSet<_>>();
    signal
        .source_event_ids
        .iter()
        .any(|event_id| !previous_ids.contains(event_id))
}

fn should_suppress_update(
    previous: Option<&AttentionSignalRecord>,
    signal: &AttentionSignalRecord,
    timestamp_ms: f64,
) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    if previous.priority != signal.priority {
        return false;
    }
    let window_ms = suppression_window_ms(&signal.kind);
    timestamp_ms - previous.last_seen_ms < window_ms
}

fn suppression_window_ms(kind: &str) -> f64 {
    match kind {
        "absence_or_staleness" => 15.0 * 60_000.0,
        "risk_context_change" => 5.0 * 60_000.0,
        "setup_lifecycle_change" => 60_000.0,
        "market_structure_change" => 30_000.0,
        "flow_confirmation" => 30_000.0,
        _ => 60_000.0,
    }
}

fn staleness_decay(input: &SignalComposerInput<'_>, signal_id: &str) -> f64 {
    input
        .prior_active_signals
        .iter()
        .find(|prior| prior.signal_id == signal_id)
        .map(|prior| ((input.timestamp_ms - prior.last_seen_ms).max(0.0) / 60_000.0).min(20.0))
        .unwrap_or(0.0)
}

fn lifecycle_weight(state: &SetupState, readiness: &SetupReadiness) -> Option<f64> {
    match (state, readiness) {
        (SetupState::NotActive, SetupReadiness::Inactive) => None,
        (_, SetupReadiness::InTrade) | (SetupState::InTrade, _) => Some(40.0),
        (_, SetupReadiness::Confirmed) | (SetupState::Confirmed, _) => Some(34.0),
        (_, SetupReadiness::DeterministicReady) | (SetupState::ConditionsMet, _) => Some(28.0),
        (_, SetupReadiness::Partial) | (SetupState::Approaching, _) => Some(12.0),
        (_, SetupReadiness::Closed) | (SetupState::Closed, _) => Some(6.0),
    }
}

fn idea_id_for_setup(session_date: &str, setup_id: &str, source: &str) -> String {
    format!(
        "idea_{}",
        stable_hash_hex(&format!("{session_date}|{setup_id}|{source}"))
    )
}

fn setup_state_label(state: &SetupState) -> &'static str {
    match state {
        SetupState::NotActive => "not active",
        SetupState::Approaching => "approaching",
        SetupState::ConditionsMet => "conditions met",
        SetupState::Confirmed => "confirmed",
        SetupState::InTrade => "in trade",
        SetupState::Closed => "closed",
    }
}

fn setup_readiness_label(readiness: &SetupReadiness) -> &'static str {
    match readiness {
        SetupReadiness::Inactive => "inactive",
        SetupReadiness::Partial => "partial",
        SetupReadiness::DeterministicReady => "deterministic ready",
        SetupReadiness::Confirmed => "confirmed",
        SetupReadiness::InTrade => "in trade",
        SetupReadiness::Closed => "closed",
    }
}

fn idea_lifecycle(state: &SetupState, readiness: &SetupReadiness) -> Option<&'static str> {
    match (state, readiness) {
        (SetupState::NotActive, SetupReadiness::Inactive) => None,
        (SetupState::InTrade, _) | (_, SetupReadiness::InTrade) => Some("in_trade"),
        (SetupState::Confirmed, _) | (_, SetupReadiness::Confirmed) => Some("confirmed"),
        (SetupState::Closed, _) | (_, SetupReadiness::Closed) => Some("resolved"),
        (SetupState::ConditionsMet, _) | (_, SetupReadiness::DeterministicReady) => Some("forming"),
        (SetupState::Approaching, _) | (_, SetupReadiness::Partial) => Some("observing"),
    }
}

fn priority_bucket(score: f64) -> &'static str {
    if score >= 80.0 {
        "urgent"
    } else if score >= 60.0 {
        "high"
    } else if score >= 35.0 {
        "normal"
    } else {
        "low"
    }
}

fn signal_id(dedupe_key: &str, session_date: &str, source: &str, job_id: Option<&str>) -> String {
    format!(
        "sig_{}",
        stable_hash_hex(&format!(
            "{}|{}|{}|{}",
            dedupe_key,
            session_date,
            source,
            job_id.unwrap_or("")
        ))
    )
}

fn signal_scope(snapshot: &MarketState, event: &MarketEvent) -> String {
    format!(
        "{}:{}:{}:{}",
        event.trading_day, snapshot.root_symbol, snapshot.contract_symbol, event.session_type
    )
}

fn suggested_tools_for_kind(kind: &str) -> Vec<String> {
    match kind {
        "flow_confirmation" => vec![
            "get_absorption_events".to_string(),
            "get_footprint".to_string(),
            "get_tape_pace".to_string(),
        ],
        "market_structure_change" => vec![
            "get_market_snapshot".to_string(),
            "get_key_levels".to_string(),
            "get_proximity_report".to_string(),
        ],
        _ => vec!["get_market_snapshot".to_string()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipelines::MarketState;
    use chrono::TimeZone;

    #[test]
    fn composer_dedupes_ids_for_same_event_subject() {
        let composer = SignalComposer::default();
        let event = MarketEvent {
            session_date: "2026-03-05".to_string(),
            timestamp_ms: 1_000.0,
            event_type: "dnp_cross".to_string(),
            level_name: Some("dnp".to_string()),
            price: 21000.0,
            direction: Some("from_below".to_string()),
            sequence_num: None,
            metadata: None,
            session_type: "RTH".to_string(),
            session_segment: "None".to_string(),
            trading_day: "2026-03-05".to_string(),
        };
        let state = MarketState {
            last_price: 21000.0,
            session_type: "RTH".to_string(),
            session_segment: "None".to_string(),
            trading_day: "2026-03-05".to_string(),
            root_symbol: "NQ".to_string(),
            contract_symbol: "NQM26.CME".to_string(),
            ..Default::default()
        };
        let input = SignalComposerInput {
            pulse_kind: AttentionPulseKind::EventDriven,
            events: &[event],
            setup_states: &[],
            risk_state: None,
            market_snapshot: &state,
            prior_active_signals: &[],
            timestamp_ms: 1_000.0,
            source: "live",
            job_id: None,
        };
        let out1 = composer.compose(input.clone());
        let out2 = composer.compose(input);
        assert_eq!(out1.signals[0].signal_id, out2.signals[0].signal_id);
        assert_eq!(out1.signals[0].priority, "low");
    }

    #[test]
    fn periodic_pulse_emits_absence_signal_after_opening_development() {
        let composer = SignalComposer::default();
        let timestamp_ms = chrono::Utc
            .with_ymd_and_hms(2026, 3, 5, 16, 0, 0)
            .single()
            .expect("timestamp")
            .timestamp_millis() as f64;
        let state = MarketState {
            last_price: 21000.0,
            session_type: "RTH".to_string(),
            session_segment: "None".to_string(),
            trading_day: "2026-03-05".to_string(),
            root_symbol: "NQ".to_string(),
            contract_symbol: "NQH26.CME".to_string(),
            ..Default::default()
        };
        let output = composer.compose(SignalComposerInput {
            pulse_kind: AttentionPulseKind::Periodic,
            events: &[],
            setup_states: &[],
            risk_state: None,
            market_snapshot: &state,
            prior_active_signals: &[],
            timestamp_ms,
            source: "live",
            job_id: None,
        });
        assert_eq!(output.signals.len(), 1);
        assert_eq!(output.signals[0].kind, "absence_or_staleness");
    }

    #[test]
    fn repeated_periodic_pulse_does_not_emit_changelog_spam() {
        let composer = SignalComposer::default();
        let timestamp_ms = chrono::Utc
            .with_ymd_and_hms(2026, 3, 5, 16, 0, 0)
            .single()
            .expect("timestamp")
            .timestamp_millis() as f64;
        let state = MarketState {
            last_price: 21000.0,
            session_type: "RTH".to_string(),
            session_segment: "None".to_string(),
            trading_day: "2026-03-05".to_string(),
            root_symbol: "NQ".to_string(),
            contract_symbol: "NQH26.CME".to_string(),
            ..Default::default()
        };
        let first = composer.compose(SignalComposerInput {
            pulse_kind: AttentionPulseKind::Periodic,
            events: &[],
            setup_states: &[],
            risk_state: None,
            market_snapshot: &state,
            prior_active_signals: &[],
            timestamp_ms,
            source: "live",
            job_id: None,
        });
        assert_eq!(first.signal_events.len(), 1);

        let second = composer.compose(SignalComposerInput {
            pulse_kind: AttentionPulseKind::Periodic,
            events: &[],
            setup_states: &[],
            risk_state: None,
            market_snapshot: &state,
            prior_active_signals: &first.signals,
            timestamp_ms: timestamp_ms + 5_000.0,
            source: "live",
            job_id: None,
        });
        assert!(second.signals.is_empty());
        assert!(second.signal_events.is_empty());
    }
}
