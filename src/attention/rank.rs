//! Attention inbox as a ranked view over the formalized event stream (SIL-M4).
//!
//! `get_attention_inbox` must not be a parallel source of truth. Event-linked
//! signals inherit lifecycle from `get_events`; ranking is severity then recency.

use crate::catalog::{
    collapse_events_latest_per_dedup, event_dedup_identity_id, is_dom_family_event_type,
    EventLifecycle, EventSeverity, KernelEvent,
};
use crate::db::{stable_hash_hex, AttentionSignalRecord};
use crate::pipelines::MarketEvent;

/// Provenance marker stored on event-stream attention rows.
pub const EVENT_STREAM_VIEW: &str = "eventStream";

/// Build the deterministic signal id used for an event-stream attention row.
pub fn event_stream_signal_id(
    dedup_identity_id: &str,
    session_date: &str,
    source: &str,
    job_id: Option<&str>,
) -> String {
    format!(
        "sig_{}",
        stable_hash_hex(&format!(
            "event_stream:{dedup_identity_id}|{session_date}|{source}|{}",
            job_id.unwrap_or("")
        ))
    )
}

/// Map a kernel event into an attention row (playbook-grounded copy, not advice).
pub fn attention_signal_from_kernel_event(
    event: &KernelEvent,
    timestamp_ms: f64,
    source: &str,
    job_id: Option<&str>,
) -> AttentionSignalRecord {
    let session_date = event
        .session_date
        .clone()
        .unwrap_or_else(|| event.trading_day.clone().unwrap_or_default());
    let signal_id = event_stream_signal_id(&event.dedup_identity_id, &session_date, source, job_id);
    let kind = attention_kind_for_event(&event.event_type);
    let subject = event
        .level_name
        .clone()
        .or_else(|| event.direction.clone())
        .unwrap_or_else(|| event.event_type.clone());
    let title = format!("Structure/flow event at {subject}");
    let title = if is_dom_family_event_type(&event.event_type) {
        format!("DOM-family event at {subject}")
    } else {
        title
    };
    let price = event.price.unwrap_or(0.0);
    let summary = format!(
        "Your playbook / your rules say this is an attention event, not an entry by itself: {} near {:.2} (lifecycle {}).",
        event.event_type, price, event.lifecycle.as_str()
    );
    let (priority, priority_score) = priority_for_severity(event.severity);
    AttentionSignalRecord {
        signal_id,
        dedupe_key: format!("event_stream:{}", event.dedup_identity_id),
        status: event.lifecycle.attention_status().to_string(),
        priority: priority.to_string(),
        priority_score,
        confidence: 1.0,
        kind: kind.to_string(),
        title,
        summary,
        created_at_ms: event.timestamp_ms,
        updated_at_ms: timestamp_ms,
        last_seen_ms: timestamp_ms,
        expires_at_ms: None,
        session_date,
        trading_day: event.trading_day.clone().unwrap_or_default(),
        session_type: "Unknown".into(),
        session_segment: "None".into(),
        root_symbol: event
            .root_symbol
            .clone()
            .or_else(|| event.frame_ref.root_symbol.clone()),
        contract_symbol: None,
        current_price: price,
        source: source.to_string(),
        job_id: job_id.map(str::to_string),
        source_event_ids: vec![event.identity_id.clone()],
        linked_setup_id: None,
        linked_setup_transition_id: None,
        linked_signal_outcome_id: None,
        linked_idea_id: None,
        suggested_tools: suggested_tools_for_event_kind(kind),
        acknowledged_by: None,
        acknowledged_at_ms: None,
        acknowledgement_note: None,
        payload: serde_json::json!({
            "viewOf": EVENT_STREAM_VIEW,
            "lifecycle": event.lifecycle.as_str(),
            "severity": event.severity.as_str(),
            "identityId": event.identity_id,
            "dedupIdentityId": event.dedup_identity_id,
            "frameRef": event.frame_ref,
            "family": event.family.as_str(),
            "requiresCapsule": event.requires_capsule,
            "eventType": event.event_type,
            "conditionFields": [event.event_type],
        }),
    }
}

/// Map an event type onto the attention `kind` label used by the inbox.
pub fn attention_kind_for_event(event_type: &str) -> &'static str {
    if is_dom_family_event_type(event_type) {
        return "dom_family";
    }
    match crate::catalog::classify_event_family(event_type) {
        crate::catalog::EventFamily::Flow => "flow_confirmation",
        crate::catalog::EventFamily::Structure => "market_structure_change",
        crate::catalog::EventFamily::Dom => "dom_family",
        crate::catalog::EventFamily::Other => "market_event",
    }
}

/// Map event severity onto the inbox priority label and score.
///
/// Label is derived from severity so `low` cannot land in the `normal` bucket
/// via the old `20 + rank*15` off-by-one (Low rank 1 → 35.0 ≥ 35).
fn priority_for_severity(severity: EventSeverity) -> (&'static str, f64) {
    match severity {
        EventSeverity::Urgent => ("urgent", 80.0),
        EventSeverity::High => ("high", 60.0),
        EventSeverity::Normal => ("normal", 40.0),
        EventSeverity::Low => ("low", 20.0),
        EventSeverity::Unspecified => ("low", 10.0),
    }
}

fn priority_label_rank(label: &str) -> i32 {
    match label.trim().to_ascii_lowercase().as_str() {
        "urgent" => 4,
        "high" => 3,
        "normal" => 2,
        "low" => 1,
        _ => 0,
    }
}

/// True when a ranked inbox row matches the caller’s status / minPriority filters.
pub fn signal_matches_inbox_filters(
    signal: &AttentionSignalRecord,
    status: Option<&str>,
    min_priority: Option<&str>,
) -> bool {
    if let Some(want) = status.map(str::trim).filter(|s| !s.is_empty()) {
        if signal.status != want {
            return false;
        }
    }
    if let Some(min) = min_priority.map(str::trim).filter(|s| !s.is_empty()) {
        if priority_label_rank(&signal.priority) < priority_label_rank(min) {
            return false;
        }
    }
    true
}

/// Apply a last-signal cursor after ranking. A missing cursor id is treated as
/// stale (empty page) so clients cannot loop on page 1.
pub fn apply_inbox_cursor(
    mut signals: Vec<AttentionSignalRecord>,
    last_signal_id: Option<&str>,
    limit: usize,
) -> Vec<AttentionSignalRecord> {
    if let Some(last_id) = last_signal_id.map(str::trim).filter(|s| !s.is_empty()) {
        match signals
            .iter()
            .position(|signal| signal.signal_id == last_id)
        {
            Some(pos) => {
                signals = signals.split_off(pos.saturating_add(1));
            }
            None => return Vec::new(),
        }
    }
    if signals.len() > limit {
        signals.truncate(limit);
    }
    signals
}

fn suggested_tools_for_event_kind(kind: &str) -> Vec<String> {
    match kind {
        "dom_family" | "flow_confirmation" => vec![
            "get_events".to_string(),
            "get_attention_inbox".to_string(),
            "get_footprint".to_string(),
        ],
        _ => vec![
            "get_events".to_string(),
            "get_attention_inbox".to_string(),
            "get_market_snapshot".to_string(),
        ],
    }
}

/// Overlay event-stream lifecycle onto inbox rows and rank.
///
/// Event-linked signals follow `get_events` lifecycle. Missing signals for
/// live (open/updated) events are synthesized so the inbox cannot disagree
/// with the event stream. Setup/risk/absence overlays remain after event rows.
pub fn rank_attention_inbox(
    mut signals: Vec<AttentionSignalRecord>,
    events: &[KernelEvent],
    include_expired: bool,
    now_ms: f64,
    source: &str,
) -> Vec<AttentionSignalRecord> {
    let events = collapse_events_latest_per_dedup(events.to_vec());
    let mut by_identity: std::collections::BTreeMap<&str, &KernelEvent> =
        std::collections::BTreeMap::new();
    let mut by_dedup: std::collections::BTreeMap<&str, &KernelEvent> =
        std::collections::BTreeMap::new();
    for event in &events {
        by_identity
            .entry(event.identity_id.as_str())
            .or_insert(event);
        by_dedup
            .entry(event.dedup_identity_id.as_str())
            .or_insert(event);
    }

    for signal in &mut signals {
        if let Some(event) = matching_event(signal, &by_identity, &by_dedup) {
            apply_event_lifecycle(signal, event);
        }
    }

    let mut covered_dedup = std::collections::BTreeSet::new();
    for signal in &signals {
        if let Some(event) = matching_event(signal, &by_identity, &by_dedup) {
            covered_dedup.insert(event.dedup_identity_id.clone());
        }
        if let Some(id) = signal
            .payload
            .get("dedupIdentityId")
            .and_then(|v| v.as_str())
        {
            covered_dedup.insert(id.to_string());
        }
    }
    for event in &events {
        let live = matches!(
            event.lifecycle,
            EventLifecycle::Open | EventLifecycle::Updated
        );
        if !live && !include_expired {
            continue;
        }
        if covered_dedup.contains(&event.dedup_identity_id) {
            continue;
        }
        covered_dedup.insert(event.dedup_identity_id.clone());
        signals.push(attention_signal_from_kernel_event(
            event, now_ms, source, None,
        ));
    }

    if !include_expired {
        signals.retain(|signal| signal.status != "expired" && signal.status != "resolved");
    }

    signals.sort_by(|a, b| {
        event_stream_rank(b)
            .cmp(&event_stream_rank(a))
            .then_with(|| {
                b.priority_score
                    .partial_cmp(&a.priority_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| {
                b.updated_at_ms
                    .partial_cmp(&a.updated_at_ms)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| a.signal_id.cmp(&b.signal_id))
    });
    signals
}

fn matching_event<'a>(
    signal: &AttentionSignalRecord,
    by_identity: &std::collections::BTreeMap<&str, &'a KernelEvent>,
    by_dedup: &std::collections::BTreeMap<&str, &'a KernelEvent>,
) -> Option<&'a KernelEvent> {
    for id in &signal.source_event_ids {
        if let Some(event) = by_identity.get(id.as_str()) {
            return Some(*event);
        }
    }
    if let Some(id) = signal
        .payload
        .get("dedupIdentityId")
        .and_then(|v| v.as_str())
    {
        if let Some(event) = by_dedup.get(id) {
            return Some(*event);
        }
    }
    if let Some(rest) = signal.dedupe_key.strip_prefix("event_stream:") {
        if let Some(event) = by_dedup.get(rest) {
            return Some(*event);
        }
    }
    None
}

fn apply_event_lifecycle(signal: &mut AttentionSignalRecord, event: &KernelEvent) {
    let next = event.lifecycle.attention_status();
    if signal.status == "acknowledged"
        && matches!(
            event.lifecycle,
            EventLifecycle::Open | EventLifecycle::Updated
        )
    {
        // Acknowledge is a typed workflow mutation — keep it while the event is live.
    } else {
        signal.status = next.to_string();
    }
    if let Some(obj) = signal.payload.as_object_mut() {
        obj.insert(
            "lifecycle".into(),
            serde_json::Value::String(event.lifecycle.as_str().into()),
        );
        obj.insert(
            "viewOf".into(),
            serde_json::Value::String(EVENT_STREAM_VIEW.into()),
        );
        obj.insert(
            "identityId".into(),
            serde_json::Value::String(event.identity_id.clone()),
        );
        obj.insert(
            "dedupIdentityId".into(),
            serde_json::Value::String(event.dedup_identity_id.clone()),
        );
        if let Ok(frame) = serde_json::to_value(&event.frame_ref) {
            obj.insert("frameRef".into(), frame);
        }
    }
}

fn event_stream_rank(signal: &AttentionSignalRecord) -> i32 {
    let is_event = signal.dedupe_key.starts_with("event_stream:")
        || signal.payload.get("viewOf").and_then(|v| v.as_str()) == Some(EVENT_STREAM_VIEW)
        || !signal.source_event_ids.is_empty();
    let live = signal.status == "active" || signal.status == "acknowledged";
    match (is_event, live) {
        (true, true) => 3,
        (false, true) => 2,
        (true, false) => 1,
        (false, false) => 0,
    }
}

/// Dedup identity for a detector row (used by SignalComposer grouping).
pub fn composer_dedup_key(event: &MarketEvent, root_symbol: Option<&str>) -> String {
    event_dedup_identity_id(event, root_symbol)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{kernel_event_from_market_event_scoped, EventLifecycle, EventSeverity};
    use crate::pipelines::MarketEvent;

    fn event(event_type: &str, ts: f64) -> KernelEvent {
        let ev = MarketEvent {
            session_date: "2026-08-13".into(),
            timestamp_ms: ts,
            event_type: event_type.into(),
            level_name: Some("ib_high".into()),
            price: 21000.0,
            direction: Some("from_below".into()),
            sequence_num: Some(1),
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2026-08-13".into(),
        };
        kernel_event_from_market_event_scoped(&ev, Some("NQ"), None)
    }

    fn overlay_signal(kind: &str, status: &str) -> AttentionSignalRecord {
        AttentionSignalRecord {
            signal_id: format!("sig_{kind}"),
            dedupe_key: format!("{kind}:x"),
            status: status.into(),
            priority: "low".into(),
            priority_score: 10.0,
            confidence: 1.0,
            kind: kind.into(),
            title: "Your playbook says overlay".into(),
            summary: "Your playbook / your rules say this is overlay context.".into(),
            created_at_ms: 1.0,
            updated_at_ms: 1.0,
            last_seen_ms: 1.0,
            expires_at_ms: None,
            session_date: "2026-08-13".into(),
            trading_day: "2026-08-13".into(),
            session_type: "RTH".into(),
            session_segment: "None".into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: None,
            current_price: 1.0,
            source: "live".into(),
            job_id: None,
            source_event_ids: vec![],
            linked_setup_id: Some("or5".into()),
            linked_setup_transition_id: None,
            linked_signal_outcome_id: None,
            linked_idea_id: None,
            suggested_tools: vec!["evaluate_playbook".into()],
            acknowledged_by: None,
            acknowledged_at_ms: None,
            acknowledgement_note: None,
            payload: serde_json::json!({ "conditionFields": ["or5"] }),
        }
    }

    #[test]
    fn inbox_ranks_event_stream_ahead_of_overlay_and_synthesizes_missing() {
        let open = event("ib_extension_hit", 1_700_000_000_000.0);
        let overlay = overlay_signal("setup_lifecycle_change", "active");
        let ranked = rank_attention_inbox(
            vec![overlay],
            std::slice::from_ref(&open),
            false,
            open.timestamp_ms,
            "live",
        );
        assert!(ranked.len() >= 2);
        assert_eq!(ranked[0].payload["viewOf"], EVENT_STREAM_VIEW);
        assert_eq!(ranked[0].status, "active");
        assert!(ranked[0].summary.contains("Your playbook"));
        assert!(!ranked[0]
            .summary
            .to_ascii_lowercase()
            .contains("you should buy"));
        assert_eq!(ranked[1].kind, "setup_lifecycle_change");
    }

    #[test]
    fn inbox_collapses_repeat_occurrences_to_one_ranked_row() {
        let open = event("absorption_detected", 1_700_000_000_000.0);
        let mut updated = event("absorption_confirmed", 1_700_000_001_000.0);
        updated.lifecycle = EventLifecycle::Updated;
        updated.dedup_identity_id = open.dedup_identity_id.clone();
        updated.dedup_identity_key = open.dedup_identity_key.clone();
        let ranked =
            rank_attention_inbox(vec![], &[open, updated], false, 1_700_000_001_000.0, "live");
        let event_rows: Vec<_> = ranked
            .iter()
            .filter(|s| s.payload["viewOf"] == EVENT_STREAM_VIEW)
            .collect();
        assert_eq!(event_rows.len(), 1);
        assert_eq!(event_rows[0].payload["lifecycle"], "updated");
        assert!(event_rows[0].summary.contains("Your playbook"));
    }

    #[test]
    fn inbox_follows_event_lifecycle_not_stale_signal_status() {
        let mut resolved = event("absorption_invalidated", 1_700_000_000_000.0);
        resolved.lifecycle = EventLifecycle::Resolved;
        resolved.severity = EventSeverity::High;
        let mut stale =
            attention_signal_from_kernel_event(&resolved, resolved.timestamp_ms, "live", None);
        stale.status = "active".into();
        let ranked =
            rank_attention_inbox(vec![stale], &[resolved], true, 1_700_000_000_000.0, "live");
        assert_eq!(ranked[0].status, "resolved");
        assert_eq!(ranked[0].payload["lifecycle"], "resolved");
    }

    #[test]
    fn live_resolved_events_drop_from_default_inbox() {
        let mut resolved = event("absorption_invalidated", 1_700_000_000_000.0);
        resolved.lifecycle = EventLifecycle::Resolved;
        let ranked = rank_attention_inbox(vec![], &[resolved], false, 1.0, "live");
        assert!(ranked.is_empty());
    }

    #[test]
    fn low_severity_maps_to_low_priority_not_normal() {
        let mut open = event("dnp_cross", 1_700_000_000_000.0);
        open.severity = EventSeverity::Low;
        let signal = attention_signal_from_kernel_event(&open, open.timestamp_ms, "live", None);
        assert_eq!(signal.priority, "low");
        assert!(signal.priority_score < 35.0);
        assert!(signal_matches_inbox_filters(&signal, None, Some("low")));
        assert!(!signal_matches_inbox_filters(&signal, None, Some("normal")));
    }

    #[test]
    fn stale_inbox_cursor_returns_empty_instead_of_replaying_page_one() {
        let open = event("ib_extension_hit", 1_700_000_000_000.0);
        let ranked = rank_attention_inbox(vec![], &[open], false, 1.0, "live");
        assert_eq!(ranked.len(), 1);
        let page = apply_inbox_cursor(ranked, Some("sig_missing"), 25);
        assert!(page.is_empty());
    }

    #[test]
    fn inbox_filters_apply_after_event_stream_synthesis() {
        let open = event("pinch_detected", 1_700_000_000_000.0);
        let overlay = overlay_signal("setup_lifecycle_change", "active");
        let ranked = rank_attention_inbox(
            vec![overlay],
            std::slice::from_ref(&open),
            false,
            open.timestamp_ms,
            "live",
        );
        let high_only: Vec<_> = ranked
            .iter()
            .filter(|s| signal_matches_inbox_filters(s, None, Some("high")))
            .collect();
        assert!(high_only
            .iter()
            .all(|s| s.payload["viewOf"] == EVENT_STREAM_VIEW));
        assert!(high_only.iter().all(|s| s.priority == "high"));
    }
}
