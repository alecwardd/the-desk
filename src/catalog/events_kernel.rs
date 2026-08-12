//! Event identity rows for `get_events` (SIL-M1b placeholder).
//!
//! Full event lifecycle formalization belongs to a later ticket (#9). This
//! module only shapes enough identity for later formalization: type, time,
//! severity placeholder, and a stable identity id.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::envelope::TrustLevel;
use super::types::{TrustCeiling, CATALOG_VERSION};
use crate::db::{market_event_id, market_event_identity};
use crate::pipelines::MarketEvent;

/// Severity placeholder until lifecycle formalization lands.
pub const SEVERITY_PLACEHOLDER: &str = "unspecified";

/// One event row returned by `get_events`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KernelEvent {
    /// Stable identity for later lifecycle / dedup formalization.
    pub identity_id: String,
    /// Deterministic identity string (pre-hash) retained for debugging.
    pub identity_key: String,
    pub event_type: String,
    pub timestamp_ms: f64,
    /// Placeholder severity — not a scored priority yet.
    pub severity: String,
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
    /// Lifecycle formalization is deferred; callers must not assume open/resolved.
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
            lifecycle_formalized: false,
        }
    }
}

/// Lift a typed [`MarketEvent`] into a kernel event identity row.
pub fn kernel_event_from_market_event(event: &MarketEvent) -> KernelEvent {
    let severity = event
        .metadata
        .as_ref()
        .and_then(|m| m.get("severity"))
        .and_then(|v| v.as_str())
        .unwrap_or(SEVERITY_PLACEHOLDER)
        .to_string();
    KernelEvent {
        identity_id: market_event_id(event),
        identity_key: market_event_identity(event),
        event_type: event.event_type.clone(),
        timestamp_ms: event.timestamp_ms,
        severity,
        level_name: event.level_name.clone(),
        price: Some(event.price),
        direction: event.direction.clone(),
        session_date: Some(event.session_date.clone()),
        trading_day: Some(event.trading_day.clone()),
        metadata: event.metadata.clone(),
    }
}

/// Lift a DB JSON market_events row into a kernel event identity row.
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
    let sequence_num = row.get("sequenceNum").and_then(|v| v.as_i64()).map(|v| v as i32);
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
    let severity = metadata
        .as_ref()
        .and_then(|m| m.get("severity"))
        .and_then(|v| v.as_str())
        .unwrap_or(SEVERITY_PLACEHOLDER)
        .to_string();

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

    KernelEvent {
        identity_id: market_event_id(&synthetic),
        identity_key: market_event_identity(&synthetic),
        event_type,
        timestamp_ms,
        severity,
        level_name,
        price: Some(price),
        direction,
        session_date: if session_date.is_empty() {
            None
        } else {
            Some(session_date)
        },
        trading_day: Some(trading_day),
        metadata,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_event_carries_type_time_severity_identity() {
        let event = MarketEvent {
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
        };
        let row = kernel_event_from_market_event(&event);
        assert_eq!(row.event_type, "ib_extension_hit");
        assert_eq!(row.timestamp_ms, 1_700_000_000_000.0);
        assert_eq!(row.severity, SEVERITY_PLACEHOLDER);
        assert!(row.identity_id.starts_with("evt_"));
        assert!(!row.identity_key.is_empty());

        let envelope = EventsEnvelope::from_events(vec![row]);
        assert_eq!(envelope.trust_level, TrustLevel::L0);
        assert!(!envelope.lifecycle_formalized);
        assert_eq!(envelope.count, 1);
    }

    #[test]
    fn db_row_severity_from_metadata_when_present() {
        let row = serde_json::json!({
            "eventType": "absorption_confirmed",
            "timestampMs": 100.0,
            "sessionDate": "2026-08-11",
            "price": 1.0,
            "sessionType": "RTH",
            "sessionSegment": "None",
            "tradingDay": "2026-08-11",
            "metadata": { "severity": "high" }
        });
        let evt = kernel_event_from_db_row(&row);
        assert_eq!(evt.severity, "high");
        assert_eq!(evt.event_type, "absorption_confirmed");
    }
}
