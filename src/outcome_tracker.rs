//! Outcome tracking for playbook signals.
//!
//! Tracks MFE/MAE and resolves signals when target, stop, or session end is reached.
//! Callable from MCP tick processing and backfill replay.

use crate::db::Database;
use crate::outcomes::{self, OutcomeTickResult};
use crate::session_date_from_timestamp_ms;

/// Result of resolving a pending signal.
#[derive(Debug, Clone)]
pub struct Resolution {
    pub signal_id: String,
    pub outcome: String,
    pub r_result: Option<f64>,
}

/// Process a tick: update MFE/MAE for pending signals and resolve any that hit target or stop.
/// Returns list of newly resolved signals.
///
/// R-results are computed from the fire-time `risk_points` stored on each row.
pub fn on_tick(db: &Database, price: f64, timestamp_ms: f64) -> Result<Vec<Resolution>, String> {
    on_tick_filtered(db, price, timestamp_ms, None, None)
}

pub fn on_tick_filtered(
    db: &Database,
    price: f64,
    timestamp_ms: f64,
    source: Option<&str>,
    job_id: Option<&str>,
) -> Result<Vec<Resolution>, String> {
    let pending = db
        .pending_signal_outcomes_filtered(source, job_id)
        .map_err(|e| e.to_string())?;
    let current_session = session_date_from_timestamp_ms(timestamp_ms);
    let mut resolved = Vec::new();

    for sig in pending {
        let fired_session = session_date_from_timestamp_ms(sig.fired_at_ms);

        // Session end: resolve as time_exit using the last tick observed in
        // the signal's own session, never the first tick of the next session.
        if fired_session != current_session {
            let exit_price = sig.last_observed_price.unwrap_or(sig.fired_price);
            let exit_ts = sig.last_observed_at_ms.unwrap_or(sig.fired_at_ms);
            let mut updated = sig.clone();
            if matches!(
                outcomes::finalize_time_exit(&mut updated, exit_price, exit_ts),
                OutcomeTickResult::Resolved
            ) {
                db.update_signal_outcome_state(&updated)
                    .map_err(|e| e.to_string())?;
                resolved.push(Resolution {
                    signal_id: updated.signal_id.clone(),
                    outcome: updated.outcome.clone(),
                    r_result: updated.r_result,
                });
            }
            continue;
        }
        let mut updated = sig.clone();
        let tick_result = outcomes::apply_tick(&mut updated, price, timestamp_ms);
        match tick_result {
            OutcomeTickResult::Resolved => {
                db.update_signal_outcome_state(&updated)
                    .map_err(|e| e.to_string())?;
                resolved.push(Resolution {
                    signal_id: updated.signal_id.clone(),
                    outcome: updated.outcome.clone(),
                    r_result: updated.r_result,
                });
            }
            OutcomeTickResult::StillPending => {
                db.update_signal_outcome_progress(
                    &updated.signal_id,
                    updated.max_favorable_excursion,
                    updated.max_adverse_excursion,
                    updated.last_observed_price,
                    updated.last_observed_at_ms,
                )
                .map_err(|e| e.to_string())?;
            }
            OutcomeTickResult::Ignored => {}
        }
    }

    Ok(resolved)
}
