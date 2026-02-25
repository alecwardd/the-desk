use crate::pipelines::MarketState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Lifecycle state of a playbook setup evaluation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SetupState {
    /// No conditions are currently satisfied.
    NotActive,
    /// Some but not all conditions are met.
    Approaching,
    /// All conditions satisfied — eligible for coaching prompt.
    ConditionsMet,
    /// Coaching prompt has been generated and acknowledged.
    Confirmed,
    /// Trader has entered a position for this setup.
    InTrade,
    /// Trade has been closed out.
    Closed,
}

fn default_suppression_ms() -> u64 {
    2000
}

/// A trader-defined playbook setup with conditions for the rules engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupDefinition {
    /// Unique identifier for this setup.
    pub id: String,
    /// Human-readable setup name.
    pub name: String,
    /// Optional longer description of the setup.
    #[serde(default)]
    pub description: String,
    /// Whether this setup is currently enabled for evaluation.
    pub active: bool,
    /// Condition strings evaluated against market state.
    #[serde(default)]
    pub conditions: Vec<String>,
    /// Minimum absolute session delta required to pass the delta gate.
    #[serde(default)]
    pub min_delta: f64,
    /// Whether price must be above VWAP for this setup.
    #[serde(default)]
    pub require_above_vwap: bool,
    /// Milliseconds to suppress duplicate alerts after firing.
    #[serde(default = "default_suppression_ms")]
    pub duplicate_suppression_ms: u64,
}

/// Alert emitted when a setup transitions to a new state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupAlert {
    /// ID of the setup that fired.
    pub setup_id: String,
    /// Human-readable name of the setup that fired.
    pub setup_name: String,
    /// The new state the setup transitioned into.
    pub state_transition: SetupState,
    /// Condition strings that were satisfied at firing time.
    pub triggered_conditions: Vec<String>,
    /// Price at the moment the alert was generated.
    pub current_price: f64,
    /// UTC timestamp in milliseconds when the alert was generated.
    pub timestamp: f64,
}

/// Deterministic rules engine that evaluates playbook setups against market state.
#[derive(Default)]
pub struct RulesEngine {
    runtimes: HashMap<String, SetupRuntime>,
}

#[derive(Debug)]
struct SetupRuntime {
    state: SetupState,
    last_alert_emitted_at: Option<Instant>,
}

impl Default for SetupRuntime {
    fn default() -> Self {
        Self {
            state: SetupState::NotActive,
            last_alert_emitted_at: None,
        }
    }
}

/// Proximity threshold (in points) for `price_near_poc` condition.
const POC_PROXIMITY_THRESHOLD: f64 = 5.0;

/// Evaluate a single condition string against current market state.
/// Returns `true` if the condition is satisfied.
fn evaluate_condition(condition: &str, market: &MarketState) -> bool {
    match condition {
        "price_vs_vwap=above" => market.last_price > market.vwap,
        "price_vs_vwap=below" => market.last_price < market.vwap,
        "session_delta=positive" => market.session_delta > 0.0,
        "session_delta=negative" => market.session_delta < 0.0,
        "price_near_poc" => (market.last_price - market.poc).abs() <= POC_PROXIMITY_THRESHOLD,
        "price_in_va" => market.last_price >= market.va_low && market.last_price <= market.va_high,
        _ => false,
    }
}

impl RulesEngine {
    /// Clear all runtime state for a new session.
    pub fn reset(&mut self) {
        self.runtimes.clear();
    }

    /// Evaluate deterministic conditions and return alerts on transitions.
    pub fn evaluate(
        &mut self,
        setup: &SetupDefinition,
        market: &MarketState,
        risk_at_limit: bool,
    ) -> Option<SetupAlert> {
        if !setup.active {
            return None;
        }
        let runtime = self.runtimes.entry(setup.id.clone()).or_default();
        if risk_at_limit {
            runtime.state = SetupState::NotActive;
            return None;
        }

        let delta_gate = market.session_delta.abs() >= setup.min_delta;

        let mut met_conditions: Vec<String> = setup
            .conditions
            .iter()
            .filter(|cond| evaluate_condition(cond, market))
            .cloned()
            .collect();

        if delta_gate {
            met_conditions.push("min_delta".to_string());
        }

        let conditions_pass =
            met_conditions.len() - usize::from(delta_gate) == setup.conditions.len();
        let all_met = conditions_pass && delta_gate;

        let next = if all_met {
            SetupState::ConditionsMet
        } else if !met_conditions.is_empty() {
            SetupState::Approaching
        } else {
            SetupState::NotActive
        };

        if next != runtime.state {
            if matches!(next, SetupState::ConditionsMet) {
                let suppress_window =
                    Duration::from_millis(setup.duplicate_suppression_ms.max(250));
                if let Some(last_emitted) = runtime.last_alert_emitted_at {
                    if last_emitted.elapsed() < suppress_window {
                        runtime.state = next;
                        return None;
                    }
                }
                runtime.last_alert_emitted_at = Some(Instant::now());
            }
            runtime.state = next.clone();
            return Some(SetupAlert {
                setup_id: setup.id.clone(),
                setup_name: setup.name.clone(),
                state_transition: next,
                triggered_conditions: met_conditions,
                current_price: market.last_price,
                timestamp: chrono::Utc::now().timestamp_millis() as f64,
            });
        }
        None
    }

    /// Mark that prompt generation completed for the setup.
    pub fn acknowledge_prompt(&mut self, setup_id: &str) -> Option<SetupState> {
        let runtime = self.runtimes.get_mut(setup_id)?;
        runtime.state = SetupState::Confirmed;
        Some(runtime.state.clone())
    }

    /// Mark setup as in-trade.
    pub fn mark_in_trade(&mut self, setup_id: &str) -> Option<SetupState> {
        let runtime = self.runtimes.get_mut(setup_id)?;
        runtime.state = SetupState::InTrade;
        Some(runtime.state.clone())
    }

    /// Mark setup as closed and reset to inactive.
    pub fn close_trade(&mut self, setup_id: &str) -> Option<SetupState> {
        let runtime = self.runtimes.get_mut(setup_id)?;
        runtime.state = SetupState::Closed;
        Some(runtime.state.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_setup(conditions: Vec<&str>, min_delta: f64) -> SetupDefinition {
        SetupDefinition {
            id: "s1".to_string(),
            name: "Test Setup".to_string(),
            description: String::new(),
            active: true,
            conditions: conditions.into_iter().map(String::from).collect(),
            min_delta,
            require_above_vwap: false,
            duplicate_suppression_ms: 1500,
        }
    }

    #[test]
    fn transitions_to_conditions_met() {
        let setup = make_setup(vec!["price_vs_vwap=above", "session_delta=positive"], 100.0);
        let market = MarketState {
            last_price: 21010.0,
            vwap: 21000.0,
            session_delta: 150.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let alert = engine
            .evaluate(&setup, &market, false)
            .expect("should transition");
        assert_eq!(alert.state_transition, SetupState::ConditionsMet);
    }

    #[test]
    fn suppresses_duplicate_alerts_inside_window() {
        let setup = SetupDefinition {
            duplicate_suppression_ms: 60_000,
            ..make_setup(vec!["price_vs_vwap=above"], 50.0)
        };
        let market = MarketState {
            last_price: 21010.0,
            vwap: 21000.0,
            session_delta: 150.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        assert!(engine.evaluate(&setup, &market, false).is_some());
        let fallback = MarketState {
            last_price: 20990.0,
            vwap: 21000.0,
            session_delta: 10.0,
            ..Default::default()
        };
        let _ = engine.evaluate(&setup, &fallback, false);
        assert!(engine.evaluate(&setup, &market, false).is_none());
    }

    #[test]
    fn condition_price_below_vwap() {
        let setup = make_setup(vec!["price_vs_vwap=below"], 0.0);
        let market = MarketState {
            last_price: 20990.0,
            vwap: 21000.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let alert = engine
            .evaluate(&setup, &market, false)
            .expect("should fire");
        assert_eq!(alert.state_transition, SetupState::ConditionsMet);
    }

    #[test]
    fn condition_session_delta_negative() {
        let setup = make_setup(vec!["session_delta=negative"], 0.0);
        let market = MarketState {
            session_delta: -50.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let alert = engine
            .evaluate(&setup, &market, false)
            .expect("should fire");
        assert_eq!(alert.state_transition, SetupState::ConditionsMet);
    }

    #[test]
    fn condition_price_near_poc() {
        let setup = make_setup(vec!["price_near_poc"], 0.0);
        let market = MarketState {
            last_price: 21003.0,
            poc: 21000.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let alert = engine
            .evaluate(&setup, &market, false)
            .expect("should fire");
        assert_eq!(alert.state_transition, SetupState::ConditionsMet);
    }

    #[test]
    fn condition_price_near_poc_too_far() {
        let setup = make_setup(vec!["price_near_poc"], 0.0);
        let market = MarketState {
            last_price: 21010.0,
            poc: 21000.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let alert = engine.evaluate(&setup, &market, false);
        assert!(
            alert.is_none()
                || alert.as_ref().unwrap().state_transition != SetupState::ConditionsMet
        );
    }

    #[test]
    fn condition_price_in_va() {
        let setup = make_setup(vec!["price_in_va"], 0.0);
        let market = MarketState {
            last_price: 21025.0,
            va_low: 21000.0,
            va_high: 21050.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let alert = engine
            .evaluate(&setup, &market, false)
            .expect("should fire");
        assert_eq!(alert.state_transition, SetupState::ConditionsMet);
    }

    #[test]
    fn delta_gate_blocks_when_insufficient() {
        let setup = make_setup(vec!["price_vs_vwap=above"], 200.0);
        let market = MarketState {
            last_price: 21010.0,
            vwap: 21000.0,
            session_delta: 50.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let alert = engine.evaluate(&setup, &market, false);
        assert!(
            alert.is_none()
                || alert.as_ref().unwrap().state_transition != SetupState::ConditionsMet
        );
    }

    #[test]
    fn no_conditions_uses_delta_gate_only() {
        let setup = make_setup(vec![], 100.0);
        let market = MarketState {
            session_delta: 150.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let alert = engine
            .evaluate(&setup, &market, false)
            .expect("should fire");
        assert_eq!(alert.state_transition, SetupState::ConditionsMet);
    }

    #[test]
    fn risk_at_limit_suppresses_all() {
        let setup = make_setup(vec!["price_vs_vwap=above"], 0.0);
        let market = MarketState {
            last_price: 21010.0,
            vwap: 21000.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        assert!(engine.evaluate(&setup, &market, true).is_none());
    }
}
