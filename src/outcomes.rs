//! Deterministic setup signal outcome evaluation.
//!
//! This module owns the verified `signal_outcomes` contract so live replay,
//! historical backtests, and manual trade bridging cannot drift.

use crate::db::SignalOutcome;
use crate::rules::{SetupDefinition, RULES_ENGINE_SCHEMA_VERSION};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const OUTCOME_ENGINE_VERSION: &str = "outcome-engine-v1";
pub const QUALITY_VERIFIED: &str = "verified";
pub const QUALITY_NOT_BACKTESTABLE: &str = "notBacktestable";
pub const QUALITY_LEGACY_UNVERIFIED: &str = "legacyUnverified";
pub const QUALITY_INVALID: &str = "invalid";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeDirection {
    Long,
    Short,
}

impl OutcomeDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "long" | "up" => Some(Self::Long),
            "short" | "down" => Some(Self::Short),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OutcomeTickResult {
    StillPending,
    Resolved,
    Ignored,
}

/// Initialize verification fields for a newly-fired signal.
pub fn initialize_outcome(outcome: &mut SignalOutcome, setup: &SetupDefinition) {
    outcome.entry_price = Some(outcome.fired_price);
    outcome.last_observed_price = Some(outcome.fired_price);
    outcome.last_observed_at_ms = Some(outcome.fired_at_ms);
    outcome.outcome_engine_version = Some(OUTCOME_ENGINE_VERSION.to_string());
    outcome.rules_schema_version = Some(RULES_ENGINE_SCHEMA_VERSION.to_string());
    outcome.setup_template_hash = Some(setup_template_hash(setup));

    let entry = outcome.fired_price;
    let target = valid_price(outcome.target_price);
    let stop = valid_price(outcome.stop_price);
    let mut flags = Vec::new();

    if stop.is_none() {
        flags.push("noNumericStop".to_string());
    }
    if target.is_none() {
        flags.push("noNumericTarget".to_string());
    }

    let direction = infer_direction(entry, target, stop);
    if direction.is_none() {
        flags.push("directionIndeterminate".to_string());
    }

    let risk = risk_points(entry, stop, setup);
    if risk.is_none() {
        flags.push("missingRiskPoints".to_string());
    }

    outcome.direction = direction.map(|d| d.as_str().to_string());
    outcome.risk_points = risk;

    if direction.is_some() && target.is_some() && stop.is_some() && risk.is_some() {
        outcome.outcome_quality = QUALITY_VERIFIED.to_string();
    } else {
        outcome.outcome_quality = QUALITY_NOT_BACKTESTABLE.to_string();
        outcome.outcome = "not_backtestable".to_string();
    }
    outcome.quality_flags = flags;
}

/// Apply a tick to a pending verified outcome.
pub fn apply_tick(outcome: &mut SignalOutcome, price: f64, timestamp_ms: f64) -> OutcomeTickResult {
    if outcome.outcome != "pending" || outcome.outcome_quality != QUALITY_VERIFIED {
        return OutcomeTickResult::Ignored;
    }
    if timestamp_ms < outcome.fired_at_ms {
        outcome.quality_flags.push("tickBeforeFire".to_string());
        outcome.outcome_quality = QUALITY_INVALID.to_string();
        return OutcomeTickResult::Ignored;
    }

    let Some(direction) = outcome
        .direction
        .as_deref()
        .and_then(OutcomeDirection::parse)
    else {
        outcome.quality_flags.push("missingDirection".to_string());
        outcome.outcome_quality = QUALITY_INVALID.to_string();
        return OutcomeTickResult::Ignored;
    };
    let entry = outcome.entry_price.unwrap_or(outcome.fired_price);

    update_excursions(outcome, direction, entry, price);
    outcome.last_observed_price = Some(price);
    outcome.last_observed_at_ms = Some(timestamp_ms);

    let target_hit = match (direction, outcome.target_price) {
        (OutcomeDirection::Long, Some(target)) => price >= target,
        (OutcomeDirection::Short, Some(target)) => price <= target,
        _ => false,
    };
    let stop_hit = match (direction, outcome.stop_price) {
        (OutcomeDirection::Long, Some(stop)) => price <= stop,
        (OutcomeDirection::Short, Some(stop)) => price >= stop,
        _ => false,
    };

    if target_hit {
        resolve_at_price(
            outcome,
            "target_hit",
            outcome.target_price.unwrap(),
            timestamp_ms,
        );
        OutcomeTickResult::Resolved
    } else if stop_hit {
        resolve_at_price(
            outcome,
            "stop_hit",
            outcome.stop_price.unwrap(),
            timestamp_ms,
        );
        OutcomeTickResult::Resolved
    } else {
        OutcomeTickResult::StillPending
    }
}

/// Finalize a pending verified outcome at the last valid in-session price.
pub fn finalize_time_exit(
    outcome: &mut SignalOutcome,
    exit_price: f64,
    exit_time_ms: f64,
) -> OutcomeTickResult {
    if outcome.outcome != "pending" || outcome.outcome_quality != QUALITY_VERIFIED {
        return OutcomeTickResult::Ignored;
    }
    if exit_time_ms < outcome.fired_at_ms {
        outcome.quality_flags.push("exitBeforeFire".to_string());
        outcome.outcome_quality = QUALITY_INVALID.to_string();
        return OutcomeTickResult::Ignored;
    }
    let Some(direction) = outcome
        .direction
        .as_deref()
        .and_then(OutcomeDirection::parse)
    else {
        outcome.quality_flags.push("missingDirection".to_string());
        outcome.outcome_quality = QUALITY_INVALID.to_string();
        return OutcomeTickResult::Ignored;
    };
    let entry = outcome.entry_price.unwrap_or(outcome.fired_price);
    update_excursions(outcome, direction, entry, exit_price);
    resolve_at_price(outcome, "time_exit", exit_price, exit_time_ms);
    OutcomeTickResult::Resolved
}

pub fn recompute_r_result(outcome: &SignalOutcome) -> Option<f64> {
    recompute_r_result_fields(
        outcome.direction.as_deref(),
        outcome.entry_price,
        outcome.fired_price,
        outcome.exit_price,
        outcome.risk_points,
    )
}

pub fn recompute_r_result_fields(
    direction: Option<&str>,
    entry_price: Option<f64>,
    fired_price: f64,
    exit_price: Option<f64>,
    risk_points: Option<f64>,
) -> Option<f64> {
    let direction = direction.and_then(OutcomeDirection::parse)?;
    let entry = entry_price.unwrap_or(fired_price);
    let exit = exit_price?;
    let risk = risk_points.filter(|v| v.is_finite() && *v > 0.0)?;
    Some(match direction {
        OutcomeDirection::Long => (exit - entry) / risk,
        OutcomeDirection::Short => (entry - exit) / risk,
    })
}

fn resolve_at_price(
    outcome: &mut SignalOutcome,
    outcome_label: &str,
    exit_price: f64,
    outcome_at_ms: f64,
) {
    outcome.outcome = outcome_label.to_string();
    outcome.outcome_at_ms = Some(outcome_at_ms);
    outcome.exit_price = Some(exit_price);
    outcome.time_to_outcome_ms = Some((outcome_at_ms - outcome.fired_at_ms).max(0.0));
    outcome.r_result = recompute_r_result(outcome);
}

fn update_excursions(
    outcome: &mut SignalOutcome,
    direction: OutcomeDirection,
    entry: f64,
    price: f64,
) {
    let (favorable, adverse) = match direction {
        OutcomeDirection::Long => (price - entry, entry - price),
        OutcomeDirection::Short => (entry - price, price - entry),
    };
    outcome.max_favorable_excursion = Some(
        outcome
            .max_favorable_excursion
            .unwrap_or(0.0)
            .max(favorable.max(0.0)),
    );
    outcome.max_adverse_excursion = Some(
        outcome
            .max_adverse_excursion
            .unwrap_or(0.0)
            .max(adverse.max(0.0)),
    );
}

fn infer_direction(entry: f64, target: Option<f64>, stop: Option<f64>) -> Option<OutcomeDirection> {
    match (target, stop) {
        (Some(target), Some(stop)) if target > entry && stop < entry => {
            Some(OutcomeDirection::Long)
        }
        (Some(target), Some(stop)) if target < entry && stop > entry => {
            Some(OutcomeDirection::Short)
        }
        _ => None,
    }
}

fn risk_points(entry: f64, stop: Option<f64>, setup: &SetupDefinition) -> Option<f64> {
    let stop_risk = valid_price(stop)
        .map(|s| (entry - s).abs())
        .filter(|v| v.is_finite() && *v > 0.0);
    stop_risk.or_else(|| {
        setup
            .position_sizing
            .get("r_points")
            .or_else(|| setup.position_sizing.get("rPoints"))
            .and_then(|v| v.as_f64())
            .filter(|v| v.is_finite() && *v > 0.0)
    })
}

fn valid_price(price: Option<f64>) -> Option<f64> {
    price.filter(|v| v.is_finite() && *v > 0.0)
}

pub fn setup_template_hash(setup: &SetupDefinition) -> String {
    let payload = serde_json::json!({
        "conditions": setup.conditions,
        "entry_logic": setup.entry_logic,
        "stop_logic": setup.stop_logic,
        "targets": setup.targets,
        "position_sizing": setup.position_sizing,
        "min_delta": setup.min_delta,
    });
    let canonical = canonical_json(&payload);
    let digest = Sha256::digest(canonical.as_bytes());
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => serde_json::to_string(v).unwrap_or_else(|_| "\"\"".to_string()),
        Value::Array(items) => {
            let body = items
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{body}]")
        }
        Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            let body = entries
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).unwrap_or_else(|_| "\"\"".to_string()),
                        canonical_json(value)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{body}}}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> SetupDefinition {
        SetupDefinition {
            id: "test".into(),
            conditions: vec!["price_vs_vwap=above".into()],
            targets: vec![serde_json::json!({"price": 110.0})],
            stop_logic: serde_json::json!({"price": 95.0}),
            position_sizing: serde_json::json!({"r_points": 5.0}),
            min_delta: 10.0,
            ..Default::default()
        }
    }

    fn outcome() -> SignalOutcome {
        SignalOutcome {
            signal_id: "sig".into(),
            setup_id: "test".into(),
            setup_name: Some("Test".into()),
            session_date: "2026-03-04".into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: Some("NQH26.CME".into()),
            source: "backtest".into(),
            job_id: Some("job".into()),
            fired_at_ms: 1_000.0,
            fired_price: 100.0,
            target_price: Some(110.0),
            stop_price: Some(95.0),
            outcome: "pending".into(),
            outcome_at_ms: None,
            max_favorable_excursion: None,
            max_adverse_excursion: None,
            r_result: None,
            time_to_outcome_ms: None,
            rvol_at_fire: None,
            rvol_bucket_at_fire: None,
            direction: None,
            entry_price: None,
            risk_points: None,
            exit_price: None,
            outcome_quality: QUALITY_LEGACY_UNVERIFIED.into(),
            quality_flags: Vec::new(),
            outcome_engine_version: None,
            rules_schema_version: None,
            setup_template_hash: None,
            last_observed_price: None,
            last_observed_at_ms: None,
        }
    }

    #[test]
    fn resolves_long_target_with_recomputed_r() {
        let mut outcome = outcome();
        initialize_outcome(&mut outcome, &setup());
        assert_eq!(outcome.outcome_quality, QUALITY_VERIFIED);

        assert_eq!(
            apply_tick(&mut outcome, 111.0, 2_000.0),
            OutcomeTickResult::Resolved
        );

        assert_eq!(outcome.outcome, "target_hit");
        assert_eq!(outcome.exit_price, Some(110.0));
        assert_eq!(outcome.time_to_outcome_ms, Some(1_000.0));
        assert_eq!(outcome.r_result, Some(2.0));
        assert_eq!(outcome.max_favorable_excursion, Some(11.0));
    }

    #[test]
    fn narrative_stop_is_not_backtestable() {
        let mut setup = setup();
        setup.stop_logic = serde_json::json!({"type": "below_value_area"});
        let mut outcome = outcome();
        outcome.stop_price = None;

        initialize_outcome(&mut outcome, &setup);

        assert_eq!(outcome.outcome_quality, QUALITY_NOT_BACKTESTABLE);
        assert_eq!(outcome.outcome, "not_backtestable");
        assert!(outcome.quality_flags.contains(&"noNumericStop".to_string()));
    }
}
