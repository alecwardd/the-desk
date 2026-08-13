//! Formalized event rows for `get_events` (SIL-M4).
//!
//! Every row carries lifecycle (`open` → `updated` → `resolved`|`expired`),
//! severity, occurrence + dedup identity, and `frame_ref` joining the event
//! to the Journal Frame that produced it. DOM-family rows also carry
//! `capsuleRef` (never silently omitted; nulls OK while pending).
//!
//! SQLite stores every occurrence. `get_events` / `get_attention_inbox` collapse
//! to the latest row per **dedup identity** so a persistent condition is not a
//! stream of new Events. Research frequency still counts occurrence rows.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

use super::envelope::TrustLevel;
use super::event_lifecycle::{
    classify_event_family, detection_kind_for_event_type, event_dedup_identity_id,
    event_dedup_identity_key, next_lifecycle_for_detection, requires_capsule,
    resolve_event_severity, DetectionKind, EventFamily, EventLifecycle, EventSeverity, FrameRef,
    CAPSULE_AFTER_MS, CAPSULE_LOOKBACK_MS,
};
use super::types::{TrustCeiling, CATALOG_VERSION};
use crate::db::{
    journal_frame_second_from_ts, market_event_id, market_event_identity, CapsuleRecord,
};
use crate::pipelines::MarketEvent;

/// Legacy alias — severity is now a real field; unspecified is the fallback.
pub const SEVERITY_PLACEHOLDER: &str = super::event_lifecycle::SEVERITY_UNSPECIFIED;

/// Join from a DOM-family Event to its Capsule dump.
///
/// Present on every `requires_capsule` row (nulls OK while pending). Omitted
/// for non-DOM rows so they do not pretend to have Capsules.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapsuleRef {
    pub id: Option<String>,
    pub window_start_ms: Option<f64>,
    pub window_end_ms: Option<f64>,
    pub start_frame_second: Option<i64>,
    pub end_frame_second: Option<i64>,
    pub completeness: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub degraded: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_count: Option<i64>,
}

impl CapsuleRef {
    /// Placeholder while the dump has not been persisted yet.
    pub fn pending_for_event(timestamp_ms: f64) -> Self {
        let start = timestamp_ms - CAPSULE_LOOKBACK_MS;
        let end = timestamp_ms + CAPSULE_AFTER_MS;
        Self {
            id: None,
            window_start_ms: Some(start),
            window_end_ms: Some(end),
            start_frame_second: journal_frame_second_from_ts(start),
            end_frame_second: journal_frame_second_from_ts(end),
            completeness: Some("pending".into()),
            degraded: None,
            sample_count: None,
        }
    }

    /// Lift a persisted Capsule row.
    pub fn from_record(record: &CapsuleRecord) -> Self {
        Self {
            id: Some(record.id.clone()),
            window_start_ms: Some(record.window_start_ms),
            window_end_ms: Some(record.window_end_ms),
            start_frame_second: record.start_frame_second,
            end_frame_second: record.end_frame_second,
            completeness: Some(record.completeness.clone()),
            degraded: Some(record.degraded),
            sample_count: Some(record.sample_count),
        }
    }
}

/// One event row returned by `get_events`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KernelEvent {
    /// Occurrence identity of this detector row (unique per insert).
    pub identity_id: String,
    /// Deterministic occurrence identity string (pre-hash) retained for debugging.
    pub identity_key: String,
    /// Stable condition identity so a persistent condition is not a stream of new events.
    pub dedup_identity_id: String,
    /// Deterministic dedup identity string (pre-hash).
    pub dedup_identity_key: String,
    pub event_type: String,
    pub timestamp_ms: f64,
    pub lifecycle: EventLifecycle,
    pub severity: EventSeverity,
    /// Journal Frame join: `(journalFrameSecond, rootSymbol)`.
    pub frame_ref: FrameRef,
    pub family: EventFamily,
    /// True only for DOM-family types (SIL-M3b Capsules are mandatory).
    pub requires_capsule: bool,
    /// Capsule join. Never silently omitted for DOM-family rows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capsule_ref: Option<CapsuleRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trading_day: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

/// Envelope returned by `get_events`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventsEnvelope {
    pub events: Vec<KernelEvent>,
    pub catalog_version: String,
    pub trust_ceiling: TrustCeiling,
    pub trust_level: TrustLevel,
    pub count: usize,
    /// SIL-M4: lifecycle is formalized on every row.
    pub lifecycle_formalized: bool,
}

impl EventsEnvelope {
    pub fn from_events(events: Vec<KernelEvent>) -> Self {
        let count = events.len();
        Self {
            events,
            catalog_version: CATALOG_VERSION.to_string(),
            trust_ceiling: TrustCeiling::L3,
            trust_level: TrustLevel::L0,
            count,
            lifecycle_formalized: true,
        }
    }
}

/// Lift a typed [`MarketEvent`] into a kernel event (no prior → `open` or `resolved`).
pub fn kernel_event_from_market_event(event: &MarketEvent) -> KernelEvent {
    kernel_event_from_market_event_scoped(event, None, None)
}

/// Lift a detector row with optional root and prior lifecycle.
pub fn kernel_event_from_market_event_scoped(
    event: &MarketEvent,
    root_symbol: Option<&str>,
    prior_lifecycle: Option<EventLifecycle>,
) -> KernelEvent {
    let kind = match prior_lifecycle {
        Some(_)
            if !matches!(
                detection_kind_for_event_type(&event.event_type),
                DetectionKind::Invalidated
            ) =>
        {
            DetectionKind::Repeat
        }
        _ => detection_kind_for_event_type(&event.event_type),
    };
    let lifecycle =
        next_lifecycle_for_detection(prior_lifecycle, kind).unwrap_or(EventLifecycle::Open);
    assemble_kernel_event(
        event,
        root_symbol,
        lifecycle,
        market_event_id(event),
        market_event_identity(event),
    )
}

fn assemble_kernel_event(
    event: &MarketEvent,
    root_symbol: Option<&str>,
    lifecycle: EventLifecycle,
    identity_id: String,
    identity_key: String,
) -> KernelEvent {
    let family = classify_event_family(&event.event_type);
    let requires = requires_capsule(&event.event_type);
    KernelEvent {
        identity_id,
        identity_key,
        dedup_identity_id: event_dedup_identity_id(event, root_symbol),
        dedup_identity_key: event_dedup_identity_key(event, root_symbol),
        event_type: event.event_type.clone(),
        timestamp_ms: event.timestamp_ms,
        lifecycle,
        severity: resolve_event_severity(event),
        frame_ref: FrameRef::from_event(event.timestamp_ms, root_symbol),
        family,
        requires_capsule: requires,
        capsule_ref: if requires {
            Some(CapsuleRef::pending_for_event(event.timestamp_ms))
        } else {
            None
        },
        level_name: event.level_name.clone(),
        price: Some(event.price),
        direction: event.direction.clone(),
        session_date: Some(event.session_date.clone()),
        trading_day: Some(event.trading_day.clone()),
        root_symbol: root_symbol
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        metadata: event.metadata.clone(),
    }
}

/// Reconstruct a kernel event from persisted identity + lifecycle columns.
#[allow(clippy::too_many_arguments)]
pub fn kernel_event_from_persisted(
    event: &MarketEvent,
    root_symbol: Option<&str>,
    lifecycle: EventLifecycle,
    identity_id: String,
    identity_key: String,
    journal_frame_second: Option<i64>,
    severity: Option<EventSeverity>,
    dedup_identity_id: Option<String>,
    dedup_identity_key: Option<String>,
) -> KernelEvent {
    let mut row = assemble_kernel_event(event, root_symbol, lifecycle, identity_id, identity_key);
    if journal_frame_second.is_some() {
        row.frame_ref.journal_frame_second = journal_frame_second;
    }
    if let Some(severity) = severity {
        row.severity = severity;
    }
    if let Some(id) = dedup_identity_id {
        if !id.is_empty() {
            row.dedup_identity_id = id;
        }
    }
    if let Some(key) = dedup_identity_key {
        if !key.is_empty() {
            row.dedup_identity_key = key;
        }
    }
    row
}

/// Lift a DB JSON market_events row into a kernel event. Lifecycle, severity,
/// identity, and `frame_ref` are always populated (never silently omitted).
pub fn kernel_event_from_db_row(row: &Value) -> KernelEvent {
    let event_type = row
        .get("eventType")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let timestamp_ms = row
        .get("timestampMs")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let session_date = row
        .get("sessionDate")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let level_name = row
        .get("levelName")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let price = row.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let direction = row
        .get("direction")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let sequence_num = row
        .get("sequenceNum")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32);
    let session_type = row
        .get("sessionType")
        .and_then(|v| v.as_str())
        .unwrap_or("Unknown")
        .to_string();
    let session_segment = row
        .get("sessionSegment")
        .and_then(|v| v.as_str())
        .unwrap_or("None")
        .to_string();
    let trading_day = row
        .get("tradingDay")
        .and_then(|v| v.as_str())
        .unwrap_or(&session_date)
        .to_string();
    let metadata = row.get("metadata").cloned();
    let root_symbol = row
        .get("rootSymbol")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let journal_frame_second = row
        .get("journalFrameSecond")
        .and_then(|v| v.as_i64())
        .or_else(|| {
            row.get("frameRef")
                .and_then(|f| f.get("journalFrameSecond"))
                .and_then(|v| v.as_i64())
        });

    let synthetic = MarketEvent {
        session_date: session_date.clone(),
        timestamp_ms,
        event_type: event_type.clone(),
        level_name: level_name.clone(),
        price,
        direction: direction.clone(),
        sequence_num,
        metadata: metadata.clone(),
        session_type,
        session_segment,
        trading_day: trading_day.clone(),
    };

    let stored_lifecycle = row
        .get("lifecycle")
        .and_then(|v| v.as_str())
        .and_then(EventLifecycle::parse);
    let stored_severity = row
        .get("severity")
        .and_then(|v| v.as_str())
        .map(EventSeverity::parse)
        .filter(|severity| *severity != EventSeverity::Unspecified);
    let identity_id = row
        .get("identityId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            row.get("eventId")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| market_event_id(&synthetic));
    let identity_key = row
        .get("identityKey")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| market_event_identity(&synthetic));
    let dedup_id = row
        .get("dedupIdentityId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let dedup_key = row
        .get("dedupIdentityKey")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let lifecycle = stored_lifecycle.unwrap_or_else(|| {
        kernel_event_from_market_event_scoped(&synthetic, root_symbol.as_deref(), None).lifecycle
    });

    let mut row_out = kernel_event_from_persisted(
        &synthetic,
        root_symbol.as_deref(),
        lifecycle,
        identity_id,
        identity_key,
        journal_frame_second,
        stored_severity,
        dedup_id,
        dedup_key,
    );
    if session_date.is_empty() {
        row_out.session_date = None;
    }
    row_out
}

/// Cap when loading occurrence rows before collapsing to the coaching stream.
pub const COACHING_EVENT_FETCH_CAP: usize = 500;

/// Collapse occurrence rows to the latest Event per dedup identity.
///
/// Newest timestamp wins. Research occurrence counts must not use this.
pub fn collapse_events_latest_per_dedup(mut events: Vec<KernelEvent>) -> Vec<KernelEvent> {
    events.sort_by(|a, b| {
        b.timestamp_ms
            .partial_cmp(&a.timestamp_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.identity_id.cmp(&a.identity_id))
    });
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(events.len());
    for event in events {
        if !seen.insert(event.dedup_identity_id.clone()) {
            continue;
        }
        out.push(event);
    }
    out
}

/// Coaching `get_events` view: latest-per-dedup, then type filter, then limit.
pub fn coaching_kernel_events_from_db_rows(
    rows: &[Value],
    event_type: Option<&str>,
    limit: usize,
) -> Vec<KernelEvent> {
    let mut events: Vec<KernelEvent> = rows.iter().map(kernel_event_from_db_row).collect();
    events = collapse_events_latest_per_dedup(events);
    if let Some(want) = event_type.map(str::trim).filter(|s| !s.is_empty()) {
        events.retain(|event| event.event_type.eq_ignore_ascii_case(want));
    }
    let limit = limit.max(1);
    if events.len() > limit {
        events.truncate(limit);
    }
    events
}

/// Overlay persisted Capsule rows onto coaching events.
///
/// DOM-family rows always keep a `capsuleRef` (pending placeholder if no row).
/// Non-DOM rows stay without one.
pub fn attach_capsule_refs(events: &mut [KernelEvent], capsules: &[CapsuleRecord]) {
    for event in events.iter_mut() {
        if !event.requires_capsule {
            event.capsule_ref = None;
            continue;
        }
        let picked = pick_capsule_for_event(event, capsules);
        event.capsule_ref = Some(picked.map_or_else(
            || CapsuleRef::pending_for_event(event.timestamp_ms),
            CapsuleRef::from_record,
        ));
    }
}

fn event_root_symbol(event: &KernelEvent) -> Option<&str> {
    event
        .root_symbol
        .as_deref()
        .or(event.frame_ref.root_symbol.as_deref())
}

fn capsule_matches_event_root(capsule: &CapsuleRecord, event: &KernelEvent) -> bool {
    match event_root_symbol(event) {
        Some(root) => capsule.root_symbol == root,
        // Unscoped rows (embedded fallback / unscoped insert) fall back to identity.
        None => true,
    }
}

fn pick_capsule_for_event<'a>(
    event: &KernelEvent,
    capsules: &'a [CapsuleRecord],
) -> Option<&'a CapsuleRecord> {
    // Occurrence identity is not namespaced by root (pre-existing M4). Match
    // the persisted Capsule root instead of rewriting `market_event_id`.
    if let Some(hit) = capsules.iter().find(|c| {
        c.trigger_identity_id == event.identity_id && capsule_matches_event_root(c, event)
    }) {
        return Some(hit);
    }
    capsules
        .iter()
        .filter(|c| c.dedup_identity_id == event.dedup_identity_id)
        .filter(|c| capsule_matches_event_root(c, event))
        .filter(|c| c.event_timestamp_ms <= event.timestamp_ms + 0.5)
        .max_by(|a, b| {
            a.event_timestamp_ms
                .partial_cmp(&b.event_timestamp_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        })
}

/// Assert envelope contract: lifecycle, severity, identities, and frame_ref
/// are always present on every row. DOM-family rows must carry capsuleRef.
pub fn kernel_event_envelope_fields_present(event: &KernelEvent) -> bool {
    let ids_ok = !event.identity_id.is_empty()
        && !event.identity_key.is_empty()
        && !event.dedup_identity_id.is_empty()
        && !event.dedup_identity_key.is_empty()
        && !event.event_type.is_empty();
    if !ids_ok {
        return false;
    }
    if event.requires_capsule {
        event.capsule_ref.is_some()
    } else {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> MarketEvent {
        MarketEvent {
            session_date: "2026-08-11".into(),
            timestamp_ms: 1_700_000_000_000.0,
            event_type: "ib_extension_hit".into(),
            level_name: Some("ib_high".into()),
            price: 21000.0,
            direction: Some("from_below".into()),
            sequence_num: Some(1),
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2026-08-11".into(),
        }
    }

    #[test]
    fn kernel_event_carries_lifecycle_severity_identity_frame_ref() {
        let event = sample();
        let row = kernel_event_from_market_event_scoped(&event, Some("NQ"), None);
        assert_eq!(row.event_type, "ib_extension_hit");
        assert_eq!(row.timestamp_ms, 1_700_000_000_000.0);
        assert_eq!(row.lifecycle, EventLifecycle::Open);
        assert_eq!(row.severity, EventSeverity::Normal);
        assert!(row.identity_id.starts_with("evt_"));
        assert!(row.dedup_identity_id.starts_with("dedup_"));
        assert_eq!(row.frame_ref.journal_frame_second, Some(1_700_000_000));
        assert_eq!(row.frame_ref.root_symbol.as_deref(), Some("NQ"));
        assert!(!row.requires_capsule);
        assert!(row.capsule_ref.is_none());
        assert!(kernel_event_envelope_fields_present(&row));

        let envelope = EventsEnvelope::from_events(vec![row.clone()]);
        assert_eq!(envelope.trust_level, TrustLevel::L0);
        assert!(envelope.lifecycle_formalized);
        assert_eq!(envelope.count, 1);
        let wire = serde_json::to_value(&envelope).expect("wire");
        let evt = &wire["events"][0];
        assert!(evt.get("lifecycle").is_some());
        assert!(evt.get("severity").is_some());
        assert!(evt.get("identityId").is_some());
        assert!(evt.get("dedupIdentityId").is_some());
        assert!(evt.get("frameRef").is_some());
        assert_eq!(evt["frameRef"]["journalFrameSecond"], 1_700_000_000i64);
        assert_eq!(wire["lifecycleFormalized"], true);
        assert_eq!(wire["trustLevel"], "L0");
    }

    #[test]
    fn kernel_event_never_silently_omits_frame_ref_when_unknown() {
        let mut event = sample();
        event.timestamp_ms = 0.0;
        let row = kernel_event_from_market_event(&event);
        let wire = serde_json::to_value(&row).expect("wire");
        assert!(wire.get("frameRef").is_some());
        assert!(wire["frameRef"].get("journalFrameSecond").is_some());
        assert!(wire["frameRef"].get("rootSymbol").is_some());
        assert!(wire["frameRef"]["journalFrameSecond"].is_null());
        assert!(wire["frameRef"]["rootSymbol"].is_null());
        assert!(wire.get("lifecycle").is_some());
        assert!(wire.get("severity").is_some());
        assert!(wire.get("dedupIdentityId").is_some());
    }

    #[test]
    fn db_row_uses_stored_lifecycle_and_severity() {
        let row = json!({
            "eventType": "absorption_confirmed",
            "timestampMs": 1_700_000_000_500.0,
            "sessionDate": "2026-08-11",
            "price": 1.0,
            "sessionType": "RTH",
            "sessionSegment": "None",
            "tradingDay": "2026-08-11",
            "rootSymbol": "NQ",
            "journalFrameSecond": 1_700_000_000i64,
            "lifecycle": "updated",
            "severity": "high",
            "identityId": "evt_abc",
            "identityKey": "key",
            "dedupIdentityId": "dedup_abc",
            "dedupIdentityKey": "dedup-key",
            "metadata": { "severity": "high" }
        });
        let evt = kernel_event_from_db_row(&row);
        assert_eq!(evt.severity, EventSeverity::High);
        assert_eq!(evt.lifecycle, EventLifecycle::Updated);
        assert_eq!(evt.identity_id, "evt_abc");
        assert_eq!(evt.dedup_identity_id, "dedup_abc");
        assert_eq!(evt.frame_ref.journal_frame_second, Some(1_700_000_000));
        assert_eq!(evt.frame_ref.root_symbol.as_deref(), Some("NQ"));
        assert_eq!(evt.event_type, "absorption_confirmed");
        assert_eq!(evt.family, EventFamily::Flow);
    }

    #[test]
    fn unspecified_stored_severity_does_not_override_family_default() {
        let row = json!({
            "eventType": "pinch_detected",
            "timestampMs": 1.0,
            "sessionDate": "2026-08-11",
            "price": 1.0,
            "lifecycle": "open",
            "severity": "unspecified",
            "identityId": "evt_x",
            "identityKey": "k",
            "dedupIdentityId": "dedup_x",
            "dedupIdentityKey": "k"
        });
        let evt = kernel_event_from_db_row(&row);
        assert_eq!(evt.severity, EventSeverity::High);
    }

    #[test]
    fn coaching_view_collapses_to_latest_per_dedup() {
        let detected = json!({
            "eventType": "absorption_detected",
            "timestampMs": 1_700_000_000_000.0,
            "sessionDate": "2026-08-11",
            "price": 1.0,
            "sessionType": "RTH",
            "sessionSegment": "None",
            "tradingDay": "2026-08-11",
            "rootSymbol": "NQ",
            "journalFrameSecond": 1_700_000_000i64,
            "lifecycle": "open",
            "severity": "high",
            "identityId": "evt_a",
            "identityKey": "key-a",
            "dedupIdentityId": "dedup_same",
            "dedupIdentityKey": "absorption|day"
        });
        let mut confirmed = detected.clone();
        confirmed["eventType"] = json!("absorption_confirmed");
        confirmed["timestampMs"] = json!(1_700_000_001_000.0);
        confirmed["lifecycle"] = json!("updated");
        confirmed["identityId"] = json!("evt_b");
        confirmed["journalFrameSecond"] = json!(1_700_000_001i64);
        let mut other = detected.clone();
        other["dedupIdentityId"] = json!("dedup_other");
        other["identityId"] = json!("evt_c");
        other["eventType"] = json!("ib_extension_hit");
        other["lifecycle"] = json!("open");
        other["severity"] = json!("normal");
        let collapsed =
            coaching_kernel_events_from_db_rows(&[detected, confirmed, other], None, 10);
        assert_eq!(collapsed.len(), 2);
        assert_eq!(collapsed[0].event_type, "absorption_confirmed");
        assert_eq!(collapsed[0].lifecycle, EventLifecycle::Updated);
        assert_eq!(collapsed[0].dedup_identity_id, "dedup_same");
        assert_eq!(collapsed[1].dedup_identity_id, "dedup_other");
        let typed = coaching_kernel_events_from_db_rows(
            &[json!({
                "eventType": "absorption_confirmed",
                "timestampMs": 1.0,
                "sessionDate": "2026-08-11",
                "price": 1.0,
                "lifecycle": "updated",
                "severity": "high",
                "identityId": "evt_b",
                "identityKey": "k",
                "dedupIdentityId": "dedup_same",
                "dedupIdentityKey": "k"
            })],
            Some("absorption_confirmed"),
            10,
        );
        assert_eq!(typed.len(), 1);
        assert!(typed.iter().all(kernel_event_envelope_fields_present));
    }

    #[test]
    fn invalidated_row_without_prior_is_resolved() {
        let event = MarketEvent {
            event_type: "absorption_invalidated".into(),
            ..sample()
        };
        let row = kernel_event_from_market_event(&event);
        assert_eq!(row.lifecycle, EventLifecycle::Resolved);
        assert_eq!(row.family, EventFamily::Flow);
    }

    #[test]
    fn repeat_against_open_becomes_updated() {
        let event = sample();
        let row =
            kernel_event_from_market_event_scoped(&event, Some("NQ"), Some(EventLifecycle::Open));
        assert_eq!(row.lifecycle, EventLifecycle::Updated);
    }

    #[test]
    fn dom_family_row_flags_capsule_and_never_omits_ref() {
        let event = MarketEvent {
            event_type: "stop_run".into(),
            ..sample()
        };
        let row = kernel_event_from_market_event_scoped(&event, Some("NQ"), None);
        assert!(row.requires_capsule);
        assert_eq!(row.family, EventFamily::Dom);
        assert_eq!(row.severity, EventSeverity::High);
        let cap = row.capsule_ref.as_ref().expect("capsuleRef required");
        assert!(cap.id.is_none());
        assert_eq!(cap.completeness.as_deref(), Some("pending"));
        assert_eq!(
            cap.window_start_ms,
            Some(event.timestamp_ms - CAPSULE_LOOKBACK_MS)
        );
        assert_eq!(
            cap.window_end_ms,
            Some(event.timestamp_ms + CAPSULE_AFTER_MS)
        );
        let wire = serde_json::to_value(&row).expect("wire");
        assert!(wire.get("capsuleRef").is_some());
        assert!(wire["capsuleRef"]["id"].is_null());
    }

    fn sample_capsule(id: &str, trigger: &str, root: &str) -> CapsuleRecord {
        CapsuleRecord {
            id: id.into(),
            trigger_identity_id: trigger.into(),
            dedup_identity_id: "dedup_x".into(),
            root_symbol: root.into(),
            event_type: "stop_run".into(),
            event_timestamp_ms: 1_700_000_000_000.0,
            window_start_ms: 1_700_000_000_000.0 - 30_000.0,
            window_end_ms: 1_700_000_000_000.0 + 60_000.0,
            observed_start_ms: None,
            observed_end_ms: None,
            start_frame_second: Some(1_699_999_970),
            end_frame_second: Some(1_700_000_060),
            completeness: "complete".into(),
            degraded: false,
            sample_count: 1,
            payload: json!({}),
            created_at_ms: 1_700_000_000_000.0,
            updated_at_ms: 1_700_000_000_000.0,
        }
    }

    #[test]
    fn attach_capsule_refs_requires_matching_root() {
        let event = MarketEvent {
            event_type: "stop_run".into(),
            ..sample()
        };
        let row = kernel_event_from_market_event_scoped(&event, Some("NQ"), None);
        let nq = sample_capsule("cap_nq", &row.identity_id, "NQ");
        let es = sample_capsule("cap_es", &row.identity_id, "ES");

        let mut events = [row.clone()];
        attach_capsule_refs(&mut events, &[es.clone(), nq.clone()]);
        assert_eq!(
            events[0].capsule_ref.as_ref().and_then(|c| c.id.as_deref()),
            Some("cap_nq")
        );

        let mut events = [row];
        attach_capsule_refs(&mut events, &[es]);
        let cap = events[0].capsule_ref.as_ref().expect("capsuleRef required");
        assert!(
            cap.id.is_none(),
            "ES Capsule must not attach to an NQ event that shares occurrence identity"
        );
        assert_eq!(cap.completeness.as_deref(), Some("pending"));
    }

    #[test]
    fn rootless_event_falls_back_to_identity_capsule_match() {
        let event = MarketEvent {
            event_type: "stop_run".into(),
            ..sample()
        };
        let mut row = kernel_event_from_market_event(&event);
        assert!(row.root_symbol.is_none());
        assert!(row.frame_ref.root_symbol.is_none());
        let cap = sample_capsule("cap_nq", &row.identity_id, "NQ");
        attach_capsule_refs(std::slice::from_mut(&mut row), &[cap]);
        assert_eq!(
            row.capsule_ref.as_ref().and_then(|c| c.id.as_deref()),
            Some("cap_nq")
        );
    }

    #[test]
    fn dom_invalidation_keeps_capsule_ref_on_coaching_row() {
        let open = MarketEvent {
            event_type: "stop_run".into(),
            ..sample()
        };
        let mut invalidated = open.clone();
        invalidated.event_type = "stop_run_invalidated".into();
        invalidated.timestamp_ms += 5_000.0;
        let open_row = kernel_event_from_market_event_scoped(&open, Some("NQ"), None);
        let inv_row = kernel_event_from_market_event_scoped(
            &invalidated,
            Some("NQ"),
            Some(EventLifecycle::Open),
        );
        assert_eq!(inv_row.family, EventFamily::Dom);
        assert!(inv_row.requires_capsule);
        assert_eq!(inv_row.lifecycle, EventLifecycle::Resolved);
        assert_eq!(open_row.dedup_identity_id, inv_row.dedup_identity_id);
        let mut cap = sample_capsule("cap_nq", &open_row.identity_id, "NQ");
        cap.dedup_identity_id = open_row.dedup_identity_id.clone();
        cap.event_timestamp_ms = open_row.timestamp_ms;
        let collapsed = collapse_events_latest_per_dedup(vec![open_row, inv_row]);
        assert_eq!(collapsed.len(), 1);
        assert_eq!(collapsed[0].event_type, "stop_run_invalidated");
        let mut events = collapsed;
        attach_capsule_refs(&mut events, &[cap]);
        assert_eq!(
            events[0].capsule_ref.as_ref().and_then(|c| c.id.as_deref()),
            Some("cap_nq")
        );
    }
}
