//! SIL-M4 event lifecycle, severity, dedup identity, `frame_ref`, and DOM-family taxonomy.
//!
//! Formalizes the existing detector stream — it does not rebuild EventDetector.
//! DOM-family types are Capsule-mandatory (SIL-M3b). This module names the
//! taxonomy and `requires_capsule` policy; the dump lives in `engine::capsule`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::db::{
    journal_frame_second_from_ts, market_event_dedup_id, market_event_dedup_identity,
    market_event_family_key as db_family_key,
};
use crate::pipelines::MarketEvent;

/// Wire value when severity cannot be classified (legacy rows only).
pub const SEVERITY_UNSPECIFIED: &str = "unspecified";

/// Wire lifecycle labels in transition order (`open` → `updated` → `resolved`|`expired`).
pub const EVENT_LIFECYCLE_STATES: &[&str] = &["open", "updated", "resolved", "expired"];

/// Default time-to-live for `open`/`updated` events before they **expire**.
///
/// Measured on the MarketRouter clock (event time), never wall clock, so
/// historical inserts are not immediately expired.
pub const EVENT_LIFECYCLE_TTL_MS: f64 = 30.0 * 60_000.0;

/// Lifecycle of one canonical event (open → updated → resolved|expired).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EventLifecycle {
    Open,
    Updated,
    Resolved,
    Expired,
}

impl EventLifecycle {
    /// Wire label used in `get_events` / SQLite.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Updated => "updated",
            Self::Resolved => "resolved",
            Self::Expired => "expired",
        }
    }

    /// Parse a stored/wire lifecycle label. Unknown values are not coerced.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "open" => Some(Self::Open),
            "updated" => Some(Self::Updated),
            "resolved" => Some(Self::Resolved),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }

    /// Terminal states cannot transition further.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Resolved | Self::Expired)
    }

    /// Attention-signal status that must not disagree with this lifecycle.
    pub fn attention_status(self) -> &'static str {
        match self {
            Self::Open | Self::Updated => "active",
            Self::Resolved => "resolved",
            Self::Expired => "expired",
        }
    }
}

impl std::fmt::Display for EventLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Illegal lifecycle transition.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("illegal event lifecycle transition {from} -> {to}")]
pub struct LifecycleError {
    pub from: EventLifecycle,
    pub to: EventLifecycle,
}

/// Apply an explicit transition. Birth (`None` → `open`) is handled by
/// [`next_lifecycle_for_detection`], not this function.
pub fn apply_lifecycle_transition(
    from: EventLifecycle,
    to: EventLifecycle,
) -> Result<EventLifecycle, LifecycleError> {
    let legal = match (from, to) {
        (EventLifecycle::Open, EventLifecycle::Updated)
        | (EventLifecycle::Open, EventLifecycle::Resolved)
        | (EventLifecycle::Open, EventLifecycle::Expired)
        | (EventLifecycle::Updated, EventLifecycle::Updated)
        | (EventLifecycle::Updated, EventLifecycle::Resolved)
        | (EventLifecycle::Updated, EventLifecycle::Expired) => true,
        (from, to) if from == to && from.is_terminal() => false,
        _ => false,
    };
    if legal {
        Ok(to)
    } else {
        Err(LifecycleError { from, to })
    }
}

/// Kind of detection relative to an existing canonical event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectionKind {
    /// First observation of a condition (or a new episode after terminal).
    Fresh,
    /// Same dedup identity while the condition is still live.
    Repeat,
    /// Detector reported invalidation / completion.
    Invalidated,
    /// TTL elapsed on the MarketRouter clock.
    TimedOut,
}

/// Compute the next lifecycle for a detection against optional prior state.
pub fn next_lifecycle_for_detection(
    prior: Option<EventLifecycle>,
    kind: DetectionKind,
) -> Result<EventLifecycle, LifecycleError> {
    match (prior, kind) {
        (None, DetectionKind::Fresh) | (None, DetectionKind::Repeat) => Ok(EventLifecycle::Open),
        (None, DetectionKind::Invalidated) => Ok(EventLifecycle::Resolved),
        (None, DetectionKind::TimedOut) => Ok(EventLifecycle::Expired),
        (Some(prior), DetectionKind::Fresh) if prior.is_terminal() => Ok(EventLifecycle::Open),
        (Some(prior), DetectionKind::Repeat) if prior.is_terminal() => Ok(EventLifecycle::Open),
        (Some(prior), DetectionKind::Invalidated) if prior.is_terminal() => Ok(prior),
        (Some(prior), DetectionKind::TimedOut) if prior.is_terminal() => Ok(prior),
        (Some(prior), DetectionKind::Fresh) | (Some(prior), DetectionKind::Repeat) => {
            apply_lifecycle_transition(prior, EventLifecycle::Updated)
        }
        (Some(prior), DetectionKind::Invalidated) => {
            apply_lifecycle_transition(prior, EventLifecycle::Resolved)
        }
        (Some(prior), DetectionKind::TimedOut) => {
            apply_lifecycle_transition(prior, EventLifecycle::Expired)
        }
    }
}

/// Severity carried on every `get_events` row. Never silently omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EventSeverity {
    Low,
    Normal,
    High,
    Urgent,
    /// Legacy / unclassifiable rows only — still present on the wire.
    Unspecified,
}

impl EventSeverity {
    /// Wire label used in `get_events` / SQLite.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
            Self::Urgent => "urgent",
            Self::Unspecified => SEVERITY_UNSPECIFIED,
        }
    }

    /// Parse a stored/wire severity label. Unknown values map to `Unspecified`.
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "low" => Self::Low,
            "normal" => Self::Normal,
            "high" => Self::High,
            "urgent" => Self::Urgent,
            "unspecified" | "" => Self::Unspecified,
            _ => Self::Unspecified,
        }
    }

    /// Rank used by the attention inbox view (higher = more attention).
    pub fn rank(self) -> i32 {
        match self {
            Self::Urgent => 4,
            Self::High => 3,
            Self::Normal => 2,
            Self::Low => 1,
            Self::Unspecified => 0,
        }
    }
}

impl std::fmt::Display for EventSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Operator family for Capsule policy and attention ranking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EventFamily {
    /// Depth-of-market family — Capsules are mandatory (SIL-M3b).
    Dom,
    Flow,
    Structure,
    Other,
}

impl EventFamily {
    /// Wire label used in `get_events` (`dom` / `flow` / `structure` / `other`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dom => "dom",
            Self::Flow => "flow",
            Self::Structure => "structure",
            Self::Other => "other",
        }
    }
}

/// DOM-family event types Capsules key off.
///
/// These types are not emitted by today's detectors (SIL-M5e). Tests inject
/// synthetic rows. The registry may extend this list.
pub const DOM_FAMILY_EVENT_TYPES: &[&str] = &[
    "stop_run",
    "iceberg_reload",
    "pull_intent",
    "book_velocity_regime_shift",
];

/// True when `event_type` is in the DOM family (Capsule-mandatory).
pub fn is_dom_family_event_type(event_type: &str) -> bool {
    let normalized = normalize_event_type_key(event_type);
    DOM_FAMILY_EVENT_TYPES
        .iter()
        .any(|name| *name == normalized)
}

/// True when this type is a DOM-family stem, including `*_invalidated` rows.
pub fn is_dom_family_stem(event_type: &str) -> bool {
    if is_dom_family_event_type(event_type) {
        return true;
    }
    let family = event_family_key(event_type);
    DOM_FAMILY_EVENT_TYPES.iter().any(|name| *name == family)
}

/// Capsule dump is mandatory for this event type (DOM-family only in M3b).
///
/// Invalidation rows of a DOM stem also require `capsuleRef` so the coaching
/// view still joins the dump after collapse-to-latest-per-dedup.
pub fn requires_capsule(event_type: &str) -> bool {
    is_dom_family_stem(event_type)
}

/// Lookback before the triggering Event (ADR-024 / #10).
/// Named constants — tests must not require `config.toml`.
pub const CAPSULE_LOOKBACK_MS: f64 = 30_000.0;
/// After-window following the triggering Event (ADR-024 / #10).
/// Named constants — tests must not require `config.toml`.
pub const CAPSULE_AFTER_MS: f64 = 60_000.0;

fn normalize_event_type_key(event_type: &str) -> String {
    event_type
        .trim()
        .to_ascii_lowercase()
        .replace(['-', ' '], "_")
}

/// Join key from an event to the Journal Frame that produced it.
///
/// `(journal_frame_second, root_symbol)` where
/// `journal_frame_second = floor(event.timestamp_ms / 1000)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FrameRef {
    pub journal_frame_second: Option<i64>,
    pub root_symbol: Option<String>,
}

impl FrameRef {
    /// Build from event time and optional root (NQ or ES).
    pub fn from_event(timestamp_ms: f64, root_symbol: Option<&str>) -> Self {
        Self {
            journal_frame_second: journal_frame_second_from_ts(timestamp_ms),
            root_symbol: root_symbol
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        }
    }
}

/// Family key used inside dedup identity (groups detected/confirmed/invalidated).
pub fn event_family_key(event_type: &str) -> String {
    db_family_key(event_type)
}

/// Stable condition identity (no timestamp) so a persistent condition is not
/// a stream of new events.
pub fn event_dedup_identity_key(event: &MarketEvent, root_symbol: Option<&str>) -> String {
    market_event_dedup_identity(event, root_symbol)
}

/// Hash of [`event_dedup_identity_key`].
pub fn event_dedup_identity_id(event: &MarketEvent, root_symbol: Option<&str>) -> String {
    market_event_dedup_id(event, root_symbol)
}

/// Classify an event type into an operator family.
pub fn classify_event_family(event_type: &str) -> EventFamily {
    if is_dom_family_stem(event_type) {
        return EventFamily::Dom;
    }
    let family = event_family_key(event_type);
    match family.as_str() {
        "absorption"
        | "pinch"
        | "acceleration_zone"
        | "large_trade_cluster"
        | "exhaustion"
        | "delta_divergence"
        | "leg_started"
        | "leg_completed" => EventFamily::Flow,
        "ib_extension"
        | "ib_formed"
        | "or_formed"
        | "or5_mid_retest"
        | "dnp_cross"
        | "day_type_change"
        | "new_session_high"
        | "new_session_low"
        | "poor_high"
        | "poor_low"
        | "excess_high"
        | "excess_low"
        | "ib_reentry_hit_mid"
        | "ib_reentry_full_traverse"
        | "level_test"
        | "vah_test"
        | "val_test"
        | "poc_test"
        | "vwap_test"
        | "ib_high_test"
        | "ib_low_test" => EventFamily::Structure,
        _ => EventFamily::Other,
    }
}

/// True when this detector row closes a live condition (→ resolved).
pub fn is_invalidation_event_type(event_type: &str) -> bool {
    normalize_event_type_key(event_type).ends_with("_invalidated")
}

/// Detection kind implied by a raw detector row (before prior lookup).
pub fn detection_kind_for_event_type(event_type: &str) -> DetectionKind {
    if is_invalidation_event_type(event_type) {
        DetectionKind::Invalidated
    } else {
        DetectionKind::Fresh
    }
}

/// Resolve severity from metadata, then family defaults. Always returns a value.
pub fn resolve_event_severity(event: &MarketEvent) -> EventSeverity {
    if let Some(raw) = event
        .metadata
        .as_ref()
        .and_then(|m| m.get("severity"))
        .and_then(|v| v.as_str())
    {
        let parsed = EventSeverity::parse(raw);
        if parsed != EventSeverity::Unspecified {
            return parsed;
        }
    }
    if let Some(n) = event
        .metadata
        .as_ref()
        .and_then(|m| m.get("severity"))
        .and_then(|v| v.as_f64())
    {
        return if n >= 4.0 {
            EventSeverity::Urgent
        } else if n >= 3.0 {
            EventSeverity::High
        } else if n >= 2.0 {
            EventSeverity::Normal
        } else if n > 0.0 {
            EventSeverity::Low
        } else {
            EventSeverity::Unspecified
        };
    }
    default_severity_for_event_type(&event.event_type)
}

fn default_severity_for_event_type(event_type: &str) -> EventSeverity {
    if is_dom_family_event_type(event_type) {
        return EventSeverity::High;
    }
    match classify_event_family(event_type) {
        EventFamily::Flow => EventSeverity::High,
        EventFamily::Structure => EventSeverity::Normal,
        EventFamily::Dom => EventSeverity::High,
        EventFamily::Other => EventSeverity::Normal,
    }
}

/// Whether a cheap-model hook may run for this pulse. Event-triggered only.
///
/// Continuous narration is out of scope. A periodic absence pulse is
/// deterministic (not a cheap-model / LLM narrator).
pub fn cheap_model_may_invoke(event_triggered: bool) -> bool {
    event_triggered
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event(event_type: &str) -> MarketEvent {
        MarketEvent {
            session_date: "2026-08-13".into(),
            timestamp_ms: 1_700_000_000_000.0,
            event_type: event_type.into(),
            level_name: Some("ib_high".into()),
            price: 21000.0,
            direction: Some("from_below".into()),
            sequence_num: Some(1),
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2026-08-13".into(),
        }
    }

    #[test]
    fn lifecycle_round_trip_open_updated_resolved() {
        let open = next_lifecycle_for_detection(None, DetectionKind::Fresh).unwrap();
        assert_eq!(open, EventLifecycle::Open);
        let updated = next_lifecycle_for_detection(Some(open), DetectionKind::Repeat).unwrap();
        assert_eq!(updated, EventLifecycle::Updated);
        let resolved =
            next_lifecycle_for_detection(Some(updated), DetectionKind::Invalidated).unwrap();
        assert_eq!(resolved, EventLifecycle::Resolved);
        assert!(resolved.is_terminal());
        assert!(apply_lifecycle_transition(resolved, EventLifecycle::Open).is_err());
        let new_episode =
            next_lifecycle_for_detection(Some(resolved), DetectionKind::Fresh).unwrap();
        assert_eq!(new_episode, EventLifecycle::Open);
    }

    #[test]
    fn lifecycle_round_trip_open_expired() {
        let open = EventLifecycle::Open;
        let expired = next_lifecycle_for_detection(Some(open), DetectionKind::TimedOut).unwrap();
        assert_eq!(expired, EventLifecycle::Expired);
        assert!(apply_lifecycle_transition(expired, EventLifecycle::Updated).is_err());
    }

    #[test]
    fn updated_may_self_transition() {
        let updated =
            apply_lifecycle_transition(EventLifecycle::Updated, EventLifecycle::Updated).unwrap();
        assert_eq!(updated, EventLifecycle::Updated);
    }

    #[test]
    fn dom_family_taxonomy_is_explicit() {
        for name in [
            "stop_run",
            "iceberg_reload",
            "pull_intent",
            "book_velocity_regime_shift",
            "stop-run",
            "book-velocity-regime-shift",
        ] {
            assert!(
                is_dom_family_event_type(name),
                "{name} must be DOM-family (Capsule-mandatory)"
            );
            assert_eq!(classify_event_family(name), EventFamily::Dom);
            assert!(requires_capsule(name), "{name}");
        }
        assert!(requires_capsule("stop_run_invalidated"));
        assert_eq!(
            classify_event_family("stop_run_invalidated"),
            EventFamily::Dom
        );
        assert!(!is_dom_family_event_type("stop_run_invalidated"));
        assert!(!is_dom_family_event_type("ib_extension_hit"));
        assert!(!is_dom_family_event_type("absorption_confirmed"));
        assert_eq!(DOM_FAMILY_EVENT_TYPES.len(), 4);
    }

    #[test]
    fn absorption_family_groups_lifecycle_variants() {
        assert_eq!(event_family_key("absorption_detected"), "absorption");
        assert_eq!(event_family_key("absorption_confirmed"), "absorption");
        assert_eq!(event_family_key("absorption_invalidated"), "absorption");
        assert!(is_invalidation_event_type("absorption_invalidated"));
        assert!(!is_invalidation_event_type("absorption_confirmed"));
    }

    #[test]
    fn dedup_identity_ignores_timestamp_and_groups_repeats() {
        let mut a = sample_event("absorption_detected");
        let mut b = sample_event("absorption_confirmed");
        b.timestamp_ms = a.timestamp_ms + 5_000.0;
        b.price = 21000.25;
        assert_eq!(
            event_dedup_identity_key(&a, Some("NQ")),
            event_dedup_identity_key(&b, Some("NQ"))
        );
        a.timestamp_ms += 1.0;
        assert_eq!(
            event_dedup_identity_id(&a, Some("NQ")),
            event_dedup_identity_id(&sample_event("absorption_detected"), Some("NQ"))
        );
        assert_ne!(
            event_dedup_identity_key(&a, Some("NQ")),
            event_dedup_identity_key(&a, Some("ES"))
        );
    }

    #[test]
    fn frame_ref_joins_journal_frame_second() {
        let event = sample_event("ib_extension_hit");
        let frame = FrameRef::from_event(event.timestamp_ms, Some("NQ"));
        assert_eq!(frame.journal_frame_second, Some(1_700_000_000));
        assert_eq!(frame.root_symbol.as_deref(), Some("NQ"));
        let missing = FrameRef::from_event(0.0, None);
        assert!(missing.journal_frame_second.is_none());
        assert!(missing.root_symbol.is_none());
    }

    #[test]
    fn severity_never_blank() {
        let event = sample_event("pinch_detected");
        assert_eq!(resolve_event_severity(&event), EventSeverity::High);
        let mut with_meta = sample_event("dnp_cross");
        with_meta.metadata = Some(serde_json::json!({ "severity": "urgent" }));
        assert_eq!(resolve_event_severity(&with_meta), EventSeverity::Urgent);
        let mut numeric = sample_event("ib_extension_hit");
        numeric.metadata = Some(serde_json::json!({ "severity": 1.0 }));
        assert_eq!(resolve_event_severity(&numeric), EventSeverity::Low);
        assert_eq!(
            resolve_event_severity(&sample_event("large_trade_cluster")),
            EventSeverity::High
        );
        assert_eq!(EventSeverity::parse("nope").as_str(), SEVERITY_UNSPECIFIED);
    }

    #[test]
    fn cheap_model_is_event_triggered_only() {
        assert!(cheap_model_may_invoke(true));
        assert!(!cheap_model_may_invoke(false));
    }
}
