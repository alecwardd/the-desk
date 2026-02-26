pub mod setup_templates;

use crate::pipelines::MarketState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Setup state machine
// ---------------------------------------------------------------------------

/// Lifecycle state of a playbook setup evaluation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SetupState {
    NotActive,
    Approaching,
    ConditionsMet,
    Confirmed,
    InTrade,
    Closed,
}

// ---------------------------------------------------------------------------
// Typed condition system
// ---------------------------------------------------------------------------

/// What market data field a condition refers to.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConditionField {
    PriceVsVwap,
    PriceVsVaHigh,
    PriceVsVaLow,
    PriceVsPoc,
    PriceVsDnvaHigh,
    PriceVsDnvaLow,
    PriceVsDnp,
    PriceVsPriorHigh,
    PriceVsPriorLow,
    PriceVsPriorClose,
    PriceVsPriorVaHigh,
    PriceVsPriorVaLow,
    PriceVsPriorPoc,
    PriceVsOvernightHigh,
    PriceVsOvernightLow,
    PriceVsOrHigh,
    PriceVsOrLow,
    PriceVsIbHigh,
    PriceVsIbLow,
    PriceVsKeyLevel,
    SessionDeltaSign,
    SessionDeltaThreshold,
    CumulativeDelta,
    TimeOfDay,
    DayOfWeek,
    TpoSinglePrintsPresent,
    TpoVaWidth,
    PriceNearPoc,
    PriceInVa,
    PriceInDnva,

    // --- Microstructure ---
    TapePacePercentile,
    AbsorptionAtPrice,
    ExhaustionDetected,
    PinchDetected,

    // --- Profile / Day Type ---
    DayType,
    ProfileShape,
    BalanceState,
    SinglePrintsDirection,

    // --- Volume Context ---
    RvolClassification,

    // --- 5-Min OR (Leo's setup) ---
    PriceVsOr5High,
    PriceVsOr5Low,
    PriceVsOr5Mid,
    Or5BrokenDirection,

    // --- Zones ---
    ActiveRebidZone,
    ActiveReofferZone,
    RebidZoneHeld,

    // --- Inventory ---
    SessionInventoryState,
    DeltaConfirmationAtLevel,

    // --- IB Extensions ---
    PriceVsIbExtension,
}

/// Comparison operator for a condition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConditionOperator {
    Above,
    Below,
    CrossesAbove,
    CrossesBelow,
    Within,
    Outside,
    Equals,
    GreaterThan,
    LessThan,
}

/// Value a condition compares against.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConditionValue {
    Number(f64),
    Text(String),
    Bool(bool),
    #[default]
    None,
}

/// A single typed condition for the rules engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupCondition {
    pub id: String,
    pub field: ConditionField,
    pub operator: ConditionOperator,
    #[serde(default)]
    pub value: ConditionValue,
    #[serde(default)]
    pub label: Option<String>,
}

// ---------------------------------------------------------------------------
// Setup definition
// ---------------------------------------------------------------------------

fn default_suppression_ms() -> u64 {
    2000
}

fn default_json_object() -> serde_json::Value {
    serde_json::json!({})
}

/// A trader-defined playbook setup with conditions for the rules engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupDefinition {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub active: bool,
    /// Legacy string conditions (backward compat).
    #[serde(default)]
    pub conditions: Vec<String>,
    #[serde(default)]
    pub min_delta: f64,
    #[serde(default)]
    pub require_above_vwap: bool,
    #[serde(default = "default_suppression_ms")]
    pub duplicate_suppression_ms: u64,
    #[serde(default = "default_json_object")]
    pub entry_logic: serde_json::Value,
    #[serde(default = "default_json_object")]
    pub stop_logic: serde_json::Value,
    #[serde(default)]
    pub targets: Vec<serde_json::Value>,
    #[serde(default = "default_json_object")]
    pub position_sizing: serde_json::Value,
    #[serde(default = "default_json_object")]
    pub market_context: serde_json::Value,
    #[serde(default)]
    pub invalidation: Vec<serde_json::Value>,
    #[serde(default = "default_json_object")]
    pub backtest_results: serde_json::Value,
    #[serde(default)]
    pub context_backtest_results: Vec<serde_json::Value>,
    /// Natural language discretionary conditions for LLM interpretation.
    #[serde(default)]
    pub discretionary_conditions: Vec<String>,
    #[serde(default)]
    pub template_source: Option<String>,
}

impl Default for SetupDefinition {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            description: String::new(),
            active: false,
            conditions: Vec::new(),
            min_delta: 0.0,
            require_above_vwap: false,
            duplicate_suppression_ms: 2000,
            entry_logic: serde_json::json!({}),
            stop_logic: serde_json::json!({}),
            targets: Vec::new(),
            position_sizing: serde_json::json!({}),
            market_context: serde_json::json!({}),
            invalidation: Vec::new(),
            backtest_results: serde_json::json!({}),
            context_backtest_results: Vec::new(),
            discretionary_conditions: Vec::new(),
            template_source: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Alert
// ---------------------------------------------------------------------------

/// Alert emitted when a setup transitions to a new state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupAlert {
    pub setup_id: String,
    pub setup_name: String,
    pub state_transition: SetupState,
    pub triggered_conditions: Vec<String>,
    pub current_price: f64,
    pub timestamp: f64,
    /// Whether this alert is for discretionary confirmation.
    #[serde(default)]
    pub discretionary: bool,
    /// Discretionary condition texts that need manual confirmation.
    #[serde(default)]
    pub discretionary_texts: Vec<String>,
    /// Backtest metrics for this setup (if available).
    #[serde(default)]
    pub backtest_summary: Option<serde_json::Value>,
    /// Stop price from setup definition.
    #[serde(default)]
    pub stop_price: Option<f64>,
    /// Target prices from setup definition.
    #[serde(default)]
    pub target_prices: Vec<f64>,
}

// ---------------------------------------------------------------------------
// Condition evaluator
// ---------------------------------------------------------------------------

const POC_PROXIMITY_THRESHOLD: f64 = 5.0;

/// Evaluate a legacy string condition against market state.
fn evaluate_string_condition(condition: &str, market: &MarketState) -> bool {
    match condition {
        "price_vs_vwap=above" => market.last_price > market.vwap,
        "price_vs_vwap=below" => market.last_price < market.vwap,
        "session_delta=positive" => market.session_delta > 0.0,
        "session_delta=negative" => market.session_delta < 0.0,
        "price_near_poc" => (market.last_price - market.poc).abs() <= POC_PROXIMITY_THRESHOLD,
        "price_in_va" => market.last_price >= market.va_low && market.last_price <= market.va_high,
        "price_in_dnva" => {
            market.last_price >= market.dnva_low && market.last_price <= market.dnva_high
        }
        "price_vs_dnva_high=above" => market.last_price > market.dnva_high,
        "price_vs_dnva_low=below" => market.last_price < market.dnva_low,
        "price_vs_dnp=above" => market.last_price > market.dnp,
        "price_vs_dnp=below" => market.last_price < market.dnp,
        "price_vs_prior_high=above" => market.last_price > market.prior_day_high,
        "price_vs_prior_low=below" => market.last_price < market.prior_day_low,
        "price_vs_overnight_high=above" => market.last_price > market.overnight_high,
        "price_vs_overnight_low=below" => market.last_price < market.overnight_low,
        "price_vs_or_high=above" => market.last_price > market.or_high,
        "price_vs_or_low=below" => market.last_price < market.or_low,
        "price_vs_ib_high=above" => market.last_price > market.ib_high,
        "price_vs_ib_low=below" => market.last_price < market.ib_low,
        _ => false,
    }
}

/// Evaluate a typed condition against current and optional previous market state.
fn evaluate_typed_condition(
    cond: &SetupCondition,
    market: &MarketState,
    prev: Option<&MarketState>,
) -> bool {
    let price = market.last_price;

    let reference_value = match cond.field {
        ConditionField::PriceVsVwap => market.vwap,
        ConditionField::PriceVsVaHigh => market.va_high,
        ConditionField::PriceVsVaLow => market.va_low,
        ConditionField::PriceVsPoc => market.poc,
        ConditionField::PriceVsDnvaHigh => market.dnva_high,
        ConditionField::PriceVsDnvaLow => market.dnva_low,
        ConditionField::PriceVsDnp => market.dnp,
        ConditionField::PriceVsPriorHigh => market.prior_day_high,
        ConditionField::PriceVsPriorLow => market.prior_day_low,
        ConditionField::PriceVsPriorClose => market.prior_day_close,
        ConditionField::PriceVsPriorVaHigh => market.prior_va_high,
        ConditionField::PriceVsPriorVaLow => market.prior_va_low,
        ConditionField::PriceVsPriorPoc => market.prior_poc,
        ConditionField::PriceVsOvernightHigh => market.overnight_high,
        ConditionField::PriceVsOvernightLow => market.overnight_low,
        ConditionField::PriceVsOrHigh => market.or_high,
        ConditionField::PriceVsOrLow => market.or_low,
        ConditionField::PriceVsIbHigh => market.ib_high,
        ConditionField::PriceVsIbLow => market.ib_low,
        ConditionField::PriceVsKeyLevel => {
            if let ConditionValue::Number(v) = &cond.value {
                *v
            } else {
                return false;
            }
        }
        ConditionField::PriceNearPoc => {
            return (price - market.poc).abs() <= POC_PROXIMITY_THRESHOLD;
        }
        ConditionField::PriceInVa => {
            return price >= market.va_low && price <= market.va_high;
        }
        ConditionField::PriceInDnva => {
            return price >= market.dnva_low && price <= market.dnva_high;
        }
        ConditionField::SessionDeltaSign => {
            return match &cond.operator {
                ConditionOperator::Above | ConditionOperator::GreaterThan => {
                    market.session_delta > 0.0
                }
                ConditionOperator::Below | ConditionOperator::LessThan => {
                    market.session_delta < 0.0
                }
                _ => false,
            };
        }
        ConditionField::SessionDeltaThreshold => {
            if let ConditionValue::Number(threshold) = &cond.value {
                return match &cond.operator {
                    ConditionOperator::GreaterThan | ConditionOperator::Above => {
                        market.session_delta.abs() > *threshold
                    }
                    ConditionOperator::LessThan | ConditionOperator::Below => {
                        market.session_delta.abs() < *threshold
                    }
                    _ => market.session_delta.abs() >= *threshold,
                };
            }
            return false;
        }
        ConditionField::CumulativeDelta => {
            if let ConditionValue::Number(threshold) = &cond.value {
                return match &cond.operator {
                    ConditionOperator::Above | ConditionOperator::GreaterThan => {
                        market.cumulative_delta > *threshold
                    }
                    ConditionOperator::Below | ConditionOperator::LessThan => {
                        market.cumulative_delta < *threshold
                    }
                    _ => false,
                };
            }
            return false;
        }
        ConditionField::TimeOfDay => {
            // Value should be minutes since midnight; not enough info in MarketState
            return false;
        }
        ConditionField::DayOfWeek => {
            if let ConditionValue::Text(day) = &cond.value {
                let today = chrono::Local::now().format("%A").to_string().to_lowercase();
                return today == day.to_lowercase();
            }
            return false;
        }
        ConditionField::TpoSinglePrintsPresent => {
            // We check via string conditions for now; single prints presence
            // would need to be passed in the MarketState (future enhancement)
            return false;
        }
        ConditionField::TpoVaWidth => {
            let width = market.va_high - market.va_low;
            if let ConditionValue::Number(threshold) = &cond.value {
                return match &cond.operator {
                    ConditionOperator::GreaterThan | ConditionOperator::Above => width > *threshold,
                    ConditionOperator::LessThan | ConditionOperator::Below => width < *threshold,
                    _ => false,
                };
            }
            return false;
        }

        // --- Microstructure ---
        ConditionField::TapePacePercentile => {
            if let ConditionValue::Number(threshold) = &cond.value {
                return match &cond.operator {
                    ConditionOperator::GreaterThan | ConditionOperator::Above => {
                        market.pace_percentile > *threshold
                    }
                    ConditionOperator::LessThan | ConditionOperator::Below => {
                        market.pace_percentile < *threshold
                    }
                    _ => false,
                };
            }
            return false;
        }
        ConditionField::AbsorptionAtPrice => {
            return market.absorption_event_count > 0;
        }
        ConditionField::ExhaustionDetected => {
            return market.absorption_event_count > 0;
        }
        ConditionField::PinchDetected => {
            return market.pinch_event_count > 0;
        }

        // --- Profile / Day Type ---
        ConditionField::DayType => {
            if let ConditionValue::Text(expected) = &cond.value {
                let actual = format!("{:?}", market.day_type);
                return actual.to_lowercase() == expected.to_lowercase();
            }
            return false;
        }
        ConditionField::ProfileShape => {
            if let ConditionValue::Text(expected) = &cond.value {
                let actual = format!("{:?}", market.profile_shape);
                return actual.to_lowercase() == expected.to_lowercase();
            }
            return false;
        }
        ConditionField::BalanceState => {
            if let ConditionValue::Text(expected) = &cond.value {
                let actual = format!("{:?}", market.balance_state);
                return actual.to_lowercase() == expected.to_lowercase();
            }
            return false;
        }
        ConditionField::SinglePrintsDirection => {
            if let ConditionValue::Text(expected) = &cond.value {
                let actual = format!("{:?}", market.single_prints_direction);
                return actual.to_lowercase() == expected.to_lowercase();
            }
            // Boolean check: any single prints present
            return market.single_prints_direction != crate::pipelines::SinglePrintsDirection::None;
        }

        // --- Volume Context ---
        ConditionField::RvolClassification => {
            if let ConditionValue::Text(expected) = &cond.value {
                let actual = format!("{:?}", market.rvol_classification);
                return actual.to_lowercase() == expected.to_lowercase();
            }
            return false;
        }

        // --- 5-Min OR ---
        ConditionField::PriceVsOr5High => {
            if !market.or5_locked {
                return false;
            }
            market.or5_high
        }
        ConditionField::PriceVsOr5Low => {
            if !market.or5_locked {
                return false;
            }
            market.or5_low
        }
        ConditionField::PriceVsOr5Mid => {
            if !market.or5_locked {
                return false;
            }
            market.or5_mid
        }
        ConditionField::Or5BrokenDirection => {
            if let ConditionValue::Text(expected) = &cond.value {
                let actual = format!("{:?}", market.or5_break_direction);
                return actual.to_lowercase() == expected.to_lowercase();
            }
            return market.or5_break_direction != crate::pipelines::Or5BreakDirection::None;
        }

        // --- Zones ---
        ConditionField::ActiveRebidZone => {
            return market.active_zone_count > 0;
        }
        ConditionField::ActiveReofferZone => {
            return market.active_zone_count > 0;
        }
        ConditionField::RebidZoneHeld => {
            return false; // requires zone-level detail not in MarketState
        }

        // --- Inventory ---
        ConditionField::SessionInventoryState => {
            if let ConditionValue::Text(expected) = &cond.value {
                let actual = format!("{:?}", market.inventory_state);
                return actual.to_lowercase() == expected.to_lowercase();
            }
            return false;
        }
        ConditionField::DeltaConfirmationAtLevel => {
            return false; // requires price + direction context not in MarketState
        }

        // --- IB Extensions ---
        ConditionField::PriceVsIbExtension => {
            if let ConditionValue::Number(multiplier) = &cond.value {
                let ib_range = market.ib_high - market.ib_low;
                let ext_high = market.ib_high + ib_range * multiplier;
                let ext_low = market.ib_low - ib_range * multiplier;
                return match &cond.operator {
                    ConditionOperator::Above => price > ext_high,
                    ConditionOperator::Below => price < ext_low,
                    _ => false,
                };
            }
            return false;
        }
    };

    match &cond.operator {
        ConditionOperator::Above | ConditionOperator::GreaterThan => price > reference_value,
        ConditionOperator::Below | ConditionOperator::LessThan => price < reference_value,
        ConditionOperator::Equals => (price - reference_value).abs() < 0.01,
        ConditionOperator::Within => {
            if let ConditionValue::Number(threshold) = &cond.value {
                (price - reference_value).abs() <= *threshold
            } else {
                (price - reference_value).abs() <= POC_PROXIMITY_THRESHOLD
            }
        }
        ConditionOperator::Outside => {
            if let ConditionValue::Number(threshold) = &cond.value {
                (price - reference_value).abs() > *threshold
            } else {
                false
            }
        }
        ConditionOperator::CrossesAbove => {
            if let Some(prev_market) = prev {
                let prev_price = prev_market.last_price;
                prev_price <= reference_value && price > reference_value
            } else {
                price > reference_value
            }
        }
        ConditionOperator::CrossesBelow => {
            if let Some(prev_market) = prev {
                let prev_price = prev_market.last_price;
                prev_price >= reference_value && price < reference_value
            } else {
                price < reference_value
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Rules engine
// ---------------------------------------------------------------------------

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

/// Deterministic rules engine that evaluates playbook setups against market state.
#[derive(Default)]
pub struct RulesEngine {
    runtimes: HashMap<String, SetupRuntime>,
    prev_market: Option<MarketState>,
}

impl RulesEngine {
    /// Clear all runtime state for a new session.
    pub fn reset(&mut self) {
        self.runtimes.clear();
        self.prev_market = None;
    }

    /// Store previous market state for crosses-above/below detection.
    pub fn update_prev_market(&mut self, market: &MarketState) {
        self.prev_market = Some(market.clone());
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
            .filter(|cond| evaluate_string_condition(cond, market))
            .cloned()
            .collect();

        // Also try parsing conditions as typed SetupConditions from the conditions JSON
        let typed_conditions: Vec<SetupCondition> = setup
            .conditions
            .iter()
            .filter_map(|s| serde_json::from_str(s).ok())
            .collect();

        for tc in &typed_conditions {
            if evaluate_typed_condition(tc, market, self.prev_market.as_ref()) {
                let label = tc
                    .label
                    .clone()
                    .unwrap_or_else(|| format!("{:?}", tc.field));
                if !met_conditions.contains(&label) {
                    met_conditions.push(label);
                }
            }
        }

        if delta_gate {
            met_conditions.push("min_delta".to_string());
        }

        let string_conditions_count = setup
            .conditions
            .iter()
            .filter(|c| serde_json::from_str::<SetupCondition>(c).is_err())
            .count();
        let typed_conditions_count = typed_conditions.len();
        let total_conditions = string_conditions_count + typed_conditions_count;

        let string_met = setup
            .conditions
            .iter()
            .filter(|c| serde_json::from_str::<SetupCondition>(c).is_err())
            .filter(|cond| evaluate_string_condition(cond, market))
            .count();
        let typed_met = typed_conditions
            .iter()
            .filter(|tc| evaluate_typed_condition(tc, market, self.prev_market.as_ref()))
            .count();

        let conditions_pass = (string_met + typed_met) == total_conditions;
        let all_met = conditions_pass && delta_gate;

        let has_discretionary = !setup.discretionary_conditions.is_empty();

        let next = if all_met {
            if has_discretionary {
                SetupState::Approaching
            } else {
                SetupState::ConditionsMet
            }
        } else if !met_conditions.is_empty() {
            SetupState::Approaching
        } else {
            SetupState::NotActive
        };

        if next != runtime.state {
            let is_alert = matches!(next, SetupState::ConditionsMet)
                || (has_discretionary && all_met && matches!(next, SetupState::Approaching));

            if is_alert {
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

            let stop_price = setup.stop_logic.get("points").and_then(|v| v.as_f64());
            let target_prices: Vec<f64> = setup
                .targets
                .iter()
                .filter_map(|t| t.get("price").and_then(|v| v.as_f64()))
                .collect();

            let backtest_summary = if setup.backtest_results != serde_json::json!({}) {
                Some(setup.backtest_results.clone())
            } else {
                None
            };

            runtime.state = next.clone();
            return Some(SetupAlert {
                setup_id: setup.id.clone(),
                setup_name: setup.name.clone(),
                state_transition: next,
                triggered_conditions: met_conditions,
                current_price: market.last_price,
                timestamp: chrono::Utc::now().timestamp_millis() as f64,
                discretionary: has_discretionary && all_met,
                discretionary_texts: if has_discretionary && all_met {
                    setup.discretionary_conditions.clone()
                } else {
                    Vec::new()
                },
                backtest_summary,
                stop_price,
                target_prices,
            });
        }
        None
    }

    pub fn acknowledge_prompt(&mut self, setup_id: &str) -> Option<SetupState> {
        let runtime = self.runtimes.get_mut(setup_id)?;
        runtime.state = SetupState::Confirmed;
        Some(runtime.state.clone())
    }

    pub fn mark_in_trade(&mut self, setup_id: &str) -> Option<SetupState> {
        let runtime = self.runtimes.get_mut(setup_id)?;
        runtime.state = SetupState::InTrade;
        Some(runtime.state.clone())
    }

    pub fn close_trade(&mut self, setup_id: &str) -> Option<SetupState> {
        let runtime = self.runtimes.get_mut(setup_id)?;
        runtime.state = SetupState::Closed;
        Some(runtime.state.clone())
    }

    /// Get current state for a setup.
    pub fn get_state(&self, setup_id: &str) -> SetupState {
        self.runtimes
            .get(setup_id)
            .map(|r| r.state.clone())
            .unwrap_or(SetupState::NotActive)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_setup(conditions: Vec<&str>, min_delta: f64) -> SetupDefinition {
        SetupDefinition {
            id: "s1".to_string(),
            name: "Test Setup".to_string(),
            active: true,
            conditions: conditions.into_iter().map(String::from).collect(),
            min_delta,
            duplicate_suppression_ms: 1500,
            ..Default::default()
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

    #[test]
    fn dnva_conditions() {
        let setup = make_setup(vec!["price_in_dnva"], 0.0);
        let market = MarketState {
            last_price: 21025.0,
            dnva_low: 21000.0,
            dnva_high: 21050.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let alert = engine
            .evaluate(&setup, &market, false)
            .expect("should fire");
        assert_eq!(alert.state_transition, SetupState::ConditionsMet);
    }

    #[test]
    fn discretionary_conditions_fire_approaching() {
        let setup = SetupDefinition {
            discretionary_conditions: vec!["Strong DOM initiation".to_string()],
            ..make_setup(vec!["price_vs_vwap=above"], 0.0)
        };
        let market = MarketState {
            last_price: 21010.0,
            vwap: 21000.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let alert = engine
            .evaluate(&setup, &market, false)
            .expect("should fire");
        assert_eq!(alert.state_transition, SetupState::Approaching);
        assert!(alert.discretionary);
        assert_eq!(alert.discretionary_texts, vec!["Strong DOM initiation"]);
    }

    #[test]
    fn typed_condition_evaluates() {
        let tc = SetupCondition {
            id: "c1".into(),
            field: ConditionField::PriceVsVwap,
            operator: ConditionOperator::Above,
            value: ConditionValue::None,
            label: None,
        };
        let market = MarketState {
            last_price: 21010.0,
            vwap: 21000.0,
            ..Default::default()
        };
        assert!(evaluate_typed_condition(&tc, &market, None));
    }

    #[test]
    fn crosses_above_requires_prev() {
        let tc = SetupCondition {
            id: "c1".into(),
            field: ConditionField::PriceVsVwap,
            operator: ConditionOperator::CrossesAbove,
            value: ConditionValue::None,
            label: None,
        };
        let prev = MarketState {
            last_price: 20990.0,
            vwap: 21000.0,
            ..Default::default()
        };
        let curr = MarketState {
            last_price: 21010.0,
            vwap: 21000.0,
            ..Default::default()
        };
        assert!(evaluate_typed_condition(&tc, &curr, Some(&prev)));
        assert!(!evaluate_typed_condition(&tc, &prev, Some(&prev)));
    }

    #[test]
    fn alert_includes_backtest_and_targets() {
        let setup = SetupDefinition {
            backtest_results: serde_json::json!({"winRate": 0.65, "samples": 100}),
            targets: vec![
                serde_json::json!({"price": 21050.0}),
                serde_json::json!({"price": 21100.0}),
            ],
            stop_logic: serde_json::json!({"points": 10.0}),
            ..make_setup(vec!["price_vs_vwap=above"], 0.0)
        };
        let market = MarketState {
            last_price: 21010.0,
            vwap: 21000.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let alert = engine
            .evaluate(&setup, &market, false)
            .expect("should fire");
        assert!(alert.backtest_summary.is_some());
        assert_eq!(alert.target_prices, vec![21050.0, 21100.0]);
        assert_eq!(alert.stop_price, Some(10.0));
    }
}
