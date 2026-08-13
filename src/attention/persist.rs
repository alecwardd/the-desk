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
