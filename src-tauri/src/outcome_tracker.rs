//! Outcome tracking for playbook signals.
//!
//! Tracks MFE/MAE and resolves signals when target, stop, or session end is reached.
//! Callable from Tauri processing loop, MCP tick processing, and backfill replay.

use crate::db::Database;
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
/// `r_value_points` is used to compute R-result when target or stop is hit.
/// Pass `None` to use default from risk_config.
pub fn on_tick(
    db: &Database,
    price: f64,
    timestamp_ms: f64,
    r_value_points: Option<f64>,
) -> Result<Vec<Resolution>, String> {
    let r_val = r_value_points.unwrap_or_else(|| {
        db.load_risk_config()
            .ok()
            .map(|c| c.r_value_points)
            .unwrap_or(50.0)
    });

    let pending = db.pending_signal_outcomes().map_err(|e| e.to_string())?;
    let current_session = session_date_from_timestamp_ms(timestamp_ms);
    let mut resolved = Vec::new();

    for sig in pending {
        let fired_session = session_date_from_timestamp_ms(sig.fired_at_ms);

        // Session end: resolve as time_exit if we've crossed into a new session
        if fired_session != current_session {
            let (is_long, _) = infer_direction(
                sig.fired_price,
                sig.target_price.as_ref(),
                sig.stop_price.as_ref(),
            );
            let r_result = if is_long {
                (price - sig.fired_price) / r_val
            } else {
                (sig.fired_price - price) / r_val
            };
            db.resolve_signal_outcome(
                &sig.signal_id,
                "time_exit",
                timestamp_ms,
                sig.max_favorable_excursion.unwrap_or(0.0),
                sig.max_adverse_excursion.unwrap_or(0.0),
                Some(r_result),
            )
            .map_err(|e| e.to_string())?;
            resolved.push(Resolution {
                signal_id: sig.signal_id.clone(),
                outcome: "time_exit".to_string(),
                r_result: Some(r_result),
            });
            continue;
        }

        let target = sig.target_price.unwrap_or(0.0);
        let stop = sig.stop_price.unwrap_or(0.0);
        let entry = sig.fired_price;

        let (is_long, is_short) =
            infer_direction(entry, sig.target_price.as_ref(), sig.stop_price.as_ref());

        let mut mfe = sig.max_favorable_excursion.unwrap_or(0.0);
        let mut mae = sig.max_adverse_excursion.unwrap_or(0.0);

        if is_long {
            let favorable = price - entry;
            let adverse = entry - price;
            if favorable > mfe {
                mfe = favorable;
            }
            if adverse > mae {
                mae = adverse;
            }
        } else if is_short {
            let favorable = entry - price;
            let adverse = price - entry;
            if favorable > mfe {
                mfe = favorable;
            }
            if adverse > mae {
                mae = adverse;
            }
        }

        // Check target hit (long: price >= target, short: price <= target)
        let target_hit = if is_long {
            target > 0.0 && price >= target
        } else if is_short {
            target > 0.0 && price <= target
        } else {
            false
        };

        // Check stop hit (long: price <= stop, short: price >= stop)
        let stop_hit = if is_long {
            stop != 0.0 && price <= stop
        } else if is_short {
            stop != 0.0 && price >= stop
        } else {
            false
        };

        let (outcome, r_result) = if target_hit {
            let r = if is_long {
                (target - entry) / r_val
            } else {
                (entry - target) / r_val
            };
            ("target_hit", Some(r))
        } else if stop_hit {
            let r = if is_long {
                (stop - entry) / r_val
            } else {
                (entry - stop) / r_val
            };
            ("stop_hit", Some(r))
        } else {
            // Update MFE/MAE in DB without resolving
            db.update_signal_outcome_mfe_mae(&sig.signal_id, mfe, mae)
                .map_err(|e| e.to_string())?;
            continue;
        };

        db.resolve_signal_outcome(&sig.signal_id, outcome, timestamp_ms, mfe, mae, r_result)
            .map_err(|e| e.to_string())?;
        resolved.push(Resolution {
            signal_id: sig.signal_id.clone(),
            outcome: outcome.to_string(),
            r_result,
        });
    }

    Ok(resolved)
}

/// Infer trade direction from target and stop relative to entry.
/// Returns (is_long, is_short).
fn infer_direction(entry: f64, target: Option<&f64>, stop: Option<&f64>) -> (bool, bool) {
    let target = target.copied().unwrap_or(0.0);
    let stop = stop.unwrap_or(&0.0);
    let is_long = target > entry && (*stop == 0.0 || *stop < entry);
    let is_short = target < entry && (*stop == 0.0 || *stop > entry);
    (is_long, is_short)
}
