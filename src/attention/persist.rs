//! Persist event-stream attention from formalized kernel events (SIL-M4).
//!
//! Engine host and embedded MCP share this path so overnight completeness is
//! SQLite + engine, not a connected agent or a standing LLM session.

use crate::catalog::{collapse_events_latest_per_dedup, KernelEvent};
use crate::db::{Database, DbError};
use crate::pipelines::MarketState;

use super::rank::attention_signal_from_kernel_event;

/// Upsert attention rows derived from the formalized event stream.
///
/// Setup/risk overlays stay on the MCP analysis pass. This function only
/// writes the event-stream view so `get_attention_inbox` cannot disagree
/// with `get_events` lifecycle after an overnight engine run.
pub fn persist_event_stream_attention(
    db: &Database,
    events: &[KernelEvent],
    snapshot: Option<&MarketState>,
    timestamp_ms: f64,
    source: &str,
    job_id: Option<&str>,
) -> Result<usize, DbError> {
    let events = collapse_events_latest_per_dedup(events.to_vec());
    if events.is_empty() {
        return Ok(0);
    }
    let mut written = 0usize;
    for event in &events {
        let mut signal = attention_signal_from_kernel_event(event, timestamp_ms, source, job_id);
        if let Some(snapshot) = snapshot {
            if signal.session_type == "Unknown" {
                signal.session_type = snapshot.session_type.clone();
            }
            if signal.session_segment == "None" {
                signal.session_segment = snapshot.session_segment.clone();
            }
            if signal.contract_symbol.is_none() {
                signal.contract_symbol =
                    Some(snapshot.contract_symbol.clone()).filter(|s| !s.is_empty());
            }
            if signal.current_price <= 0.0 {
                signal.current_price = snapshot.last_price;
            }
        }
        if let Some(existing) = db.get_attention_signal(&signal.signal_id)? {
            if existing.status == "acknowledged"
                && (signal.status == "active" || signal.status == "acknowledged")
            {
                signal.status = existing.status;
                signal.acknowledged_by = existing.acknowledged_by;
                signal.acknowledged_at_ms = existing.acknowledged_at_ms;
                signal.acknowledgement_note = existing.acknowledgement_note;
            }
            signal.created_at_ms = existing.created_at_ms;
        }
        db.upsert_attention_signal(&signal)?;
        written += 1;
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::kernel_event_from_market_event_scoped;
    use crate::pipelines::MarketEvent;
    use tempfile::NamedTempFile;

    #[test]
    fn persist_writes_event_stream_row_for_detail_and_ack() {
        let file = NamedTempFile::new().expect("temp");
        let db = Database::open(file.path().to_string_lossy().as_ref()).expect("open");
        let event = MarketEvent {
            session_date: "2026-08-13".into(),
            timestamp_ms: 1_700_000_000_000.0,
            event_type: "pinch_detected".into(),
            level_name: Some("vwap".into()),
            price: 21000.0,
            direction: Some("from_below".into()),
            sequence_num: None,
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2026-08-13".into(),
        };
        let kernel = kernel_event_from_market_event_scoped(&event, Some("NQ"), None);
        let written = persist_event_stream_attention(
            &db,
            std::slice::from_ref(&kernel),
            None,
            event.timestamp_ms,
            "live",
            None,
        )
        .expect("persist");
        assert_eq!(written, 1);
        let signal_id = crate::attention::event_stream_signal_id(
            &kernel.dedup_identity_id,
            &event.session_date,
            "live",
            None,
        );
        let loaded = db
            .get_attention_signal(&signal_id)
            .expect("load")
            .expect("row");
        assert_eq!(loaded.status, "active");
        assert_eq!(loaded.payload["viewOf"], "eventStream");
        assert_eq!(loaded.priority, "high");
        db.acknowledge_attention_signal(&signal_id, "trader", None, event.timestamp_ms)
            .expect("ack");
        let again = persist_event_stream_attention(
            &db,
            std::slice::from_ref(&kernel),
            None,
            event.timestamp_ms + 1.0,
            "live",
            None,
        )
        .expect("repersist");
        assert_eq!(again, 1);
        let kept = db
            .get_attention_signal(&signal_id)
            .expect("reload")
            .expect("row");
        assert_eq!(kept.status, "acknowledged");
        assert_eq!(kept.acknowledged_by.as_deref(), Some("trader"));
    }
}
