pub mod setup_templates;

use crate::pipelines::{normalize_day_type_label, normalize_profile_shape_label, MarketState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Semantic version for rules-engine condition/evaluator behavior.
///
/// Stored as text in `signal_outcomes.rules_schema_version` so future
/// non-numeric pre-release labels can use the same column.
///
/// Increment this when `ConditionField`, `ConditionOperator`, or evaluator
/// semantics change in a way that invalidates cached backtest statistics.
/// Examples: adding/removing condition variants, changing comparison semantics
/// in `evaluate_typed_condition`, or changing `resolve_price_expression` modes.
pub const RULES_ENGINE_SCHEMA_VERSION: u32 = 3;

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

/// Readiness describes progress within a lifecycle state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SetupReadiness {
    Inactive,
    Partial,
    DeterministicReady,
    Confirmed,
    InTrade,
    Closed,
}

/// Durable lifecycle of a setup row outside the live evaluation state machine.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SetupLifecycleStatus {
    Hypothesis,
    Draft,
    Active,
    Failed,
    RejectedByHuman,
    #[default]
    Retired,
}

impl SetupLifecycleStatus {
    /// Stable database representation for setup lifecycle.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Hypothesis => "hypothesis",
            Self::Draft => "draft",
            Self::Active => "active",
            Self::Failed => "failed",
            Self::RejectedByHuman => "rejectedByHuman",
            Self::Retired => "retired",
        }
    }
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
    DomTouchImbalanceRatio,
    DomSpreadTicks,
    DomNearTouchDepthRatio,
    DomPullStackBias,

    // --- Profile / Day Type ---
    DayType,
    ProfileShape,
    BalanceState,
    SinglePrintsDirection,

    // --- Volume Context ---
    RvolClassification,
    /// Percentile rank of RVOL vs historical days at same time-of-day (0–100).
    RvolPercentile,
    /// Rate of change of RVOL ratio per 5-minute bucket (positive = accelerating).
    RvolVelocity,

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

    // --- Regime (IDEA-000) ---
    /// Session regime: OneSidedAcceptance | Migration | Transition | Unclear.
    Regime,
    /// Live 0.5x IB extension state: None | UpOnly | DownOnly | BothSides.
    IbExtensionState,

    // --- Absorption failure (IDEA-012) ---
    /// Whether a recently invalidated (failed) absorption is live.
    AbsorptionInvalidated,
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
    #[serde(default)]
    pub lifecycle_status: SetupLifecycleStatus,
    #[serde(default)]
    pub parent_hypothesis_id: Option<String>,
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
            lifecycle_status: SetupLifecycleStatus::Retired,
            parent_hypothesis_id: None,
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

/// Current deterministic evaluation of a playbook setup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupEvaluation {
    pub setup_id: String,
    pub setup_name: String,
    pub state: SetupState,
    pub readiness: SetupReadiness,
    pub readiness_score: f64,
    pub met_conditions: Vec<String>,
    pub missing_conditions: Vec<String>,
    pub met_count: usize,
    pub total_count: usize,
    pub deterministic_all_met: bool,
    pub requires_discretionary: bool,
    pub current_price: f64,
    pub timestamp_ms: f64,
}

/// A meaningful state/progress transition to persist.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupTransition {
    pub setup_id: String,
    pub setup_name: String,
    pub previous_state: SetupState,
    pub next_state: SetupState,
    pub previous_readiness: SetupReadiness,
    pub next_readiness: SetupReadiness,
    pub readiness_score: f64,
    pub met_count: usize,
    pub total_count: usize,
    pub met_conditions: Vec<String>,
    pub missing_conditions: Vec<String>,
    pub deterministic_all_met: bool,
    pub requires_discretionary: bool,
    pub current_price: f64,
    pub timestamp_ms: f64,
    pub reason: String,
    pub alert_emitted: bool,
    pub last_alert_emitted_at_ms: Option<f64>,
}

/// Serializable runtime snapshot for restart rehydration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupRuntimeSnapshot {
    pub setup_id: String,
    pub setup_name: Option<String>,
    pub state: SetupState,
    pub readiness: SetupReadiness,
    pub readiness_score: f64,
    pub met_conditions: Vec<String>,
    pub missing_conditions: Vec<String>,
    pub met_count: usize,
    pub total_count: usize,
    pub deterministic_all_met: bool,
    pub requires_discretionary: bool,
    pub current_price: f64,
    pub last_evaluated_at_ms: f64,
    pub last_transition_at_ms: f64,
    pub last_alert_emitted_at_ms: Option<f64>,
    pub source: String,
}

/// Full result of evaluating a setup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupEvaluationOutcome {
    pub evaluation: SetupEvaluation,
    pub alert: Option<SetupAlert>,
    pub transition: Option<SetupTransition>,
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
        "absorption_at_price" => market.has_recent_confirmed_absorption,
        "exhaustion_detected" => market.has_recent_confirmed_exhaustion,
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

fn named_level_value(name: &str, market: &MarketState) -> Option<f64> {
    let value = match name {
        "vwap" => market.vwap,
        "poc" => market.poc,
        "va_high" => market.va_high,
        "va_low" => market.va_low,
        "dnva_high" => market.dnva_high,
        "dnva_low" => market.dnva_low,
        "dnp" => market.dnp,
        "prior_high" => market.prior_day_high,
        "prior_low" => market.prior_day_low,
        "prior_close" => market.prior_day_close,
        "prior_va_high" => market.prior_va_high,
        "prior_va_low" => market.prior_va_low,
        "prior_poc" => market.prior_poc,
        "overnight_high" => market.overnight_high,
        "overnight_low" => market.overnight_low,
        "or_high" => market.or_high,
        "or_low" => market.or_low,
        "ib_high" => market.ib_high,
        "ib_low" => market.ib_low,
        "or5_high" => market.or5_high,
        "or5_low" => market.or5_low,
        "or5_mid" => market.or5_mid,
        _ => return None,
    };
    (value > 0.0 && value.is_finite()).then_some(value)
}

fn signed_offset(direction: &str, points: f64, is_stop: bool) -> Option<f64> {
    // `direction` is the trade direction. A long target is above entry, but a
    // long stop is below entry; shorts invert that relationship.
    match direction {
        "long" | "up" | "above" if is_stop => Some(-points.abs()),
        "long" | "up" | "above" => Some(points.abs()),
        "short" | "down" | "below" if is_stop => Some(points.abs()),
        "short" | "down" | "below" => Some(-points.abs()),
        _ => None,
    }
}

fn resolve_price_expression(
    expr: &serde_json::Value,
    market: &MarketState,
    is_stop: bool,
) -> Option<f64> {
    if let Some(price) = expr.get("price").and_then(|v| v.as_f64()) {
        return Some(price);
    }

    match expr.get("mode").and_then(|v| v.as_str()).unwrap_or("") {
        "fixed_points" => {
            let direction = expr.get("direction").and_then(|v| v.as_str())?;
            let points = expr
                .get("points")
                .or_else(|| expr.get("targetPoints"))
                .or_else(|| expr.get("stopPoints"))
                .and_then(|v| v.as_f64())?;
            signed_offset(direction, points, is_stop).map(|offset| market.last_price + offset)
        }
        "named_level_offset" => {
            let level = expr.get("level").and_then(|v| v.as_str())?;
            let base = named_level_value(level, market)?;
            let offset_ticks = expr
                .get("offsetTicks")
                .or_else(|| expr.get("offset_ticks"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let offset_points = expr
                .get("offsetPoints")
                .or_else(|| expr.get("offset_points"))
                .and_then(|v| v.as_f64())
                .unwrap_or(offset_ticks * 0.25);
            Some(base + offset_points)
        }
        _ => None,
    }
}

fn resolve_stop_price(setup: &SetupDefinition, market: &MarketState) -> Option<f64> {
    resolve_price_expression(&setup.stop_logic, market, true).or_else(|| {
        // Backward compatibility for existing setup templates/tests.
        setup.stop_logic.get("points").and_then(|v| v.as_f64())
    })
}

fn resolve_target_prices(setup: &SetupDefinition, market: &MarketState) -> Vec<f64> {
    setup
        .targets
        .iter()
        .filter_map(|target| resolve_price_expression(target, market, false))
        .collect()
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
                let today = chrono::Utc::now()
                    .with_timezone(&chrono_tz::US::Eastern)
                    .format("%A")
                    .to_string()
                    .to_lowercase();
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
            return market.has_recent_confirmed_absorption;
        }
        ConditionField::ExhaustionDetected => {
            return market.has_recent_confirmed_exhaustion;
        }
        ConditionField::PinchDetected => {
            return market.pinch_event_count > 0;
        }
        ConditionField::DomTouchImbalanceRatio => {
            let Some(value) = market
                .dom_summary
                .as_ref()
                .and_then(|dom| dom.touch_imbalance_ratio)
            else {
                return false;
            };
            if let ConditionValue::Number(threshold) = &cond.value {
                return match &cond.operator {
                    ConditionOperator::GreaterThan | ConditionOperator::Above => value > *threshold,
                    ConditionOperator::LessThan | ConditionOperator::Below => value < *threshold,
                    ConditionOperator::Within => (value - *threshold).abs() <= 0.1,
                    ConditionOperator::Outside => (value - *threshold).abs() > 0.1,
                    ConditionOperator::Equals => (value - *threshold).abs() <= 0.01,
                    _ => false,
                };
            }
            return value > 1.0;
        }
        ConditionField::DomSpreadTicks => {
            let Some(value) = market
                .dom_summary
                .as_ref()
                .and_then(|dom| dom.spread_ticks)
                .map(|v| v as f64)
            else {
                return false;
            };
            if let ConditionValue::Number(threshold) = &cond.value {
                return match &cond.operator {
                    ConditionOperator::GreaterThan | ConditionOperator::Above => value > *threshold,
                    ConditionOperator::LessThan | ConditionOperator::Below => value < *threshold,
                    ConditionOperator::Within => value <= *threshold,
                    ConditionOperator::Outside => value > *threshold,
                    ConditionOperator::Equals => (value - *threshold).abs() <= 0.01,
                    _ => false,
                };
            }
            return value <= 1.0;
        }
        ConditionField::DomNearTouchDepthRatio => {
            let Some(value) = market
                .dom_summary
                .as_ref()
                .and_then(|dom| dom.near_touch_depth_ratio)
            else {
                return false;
            };
            if let ConditionValue::Number(threshold) = &cond.value {
                return match &cond.operator {
                    ConditionOperator::GreaterThan | ConditionOperator::Above => value > *threshold,
                    ConditionOperator::LessThan | ConditionOperator::Below => value < *threshold,
                    ConditionOperator::Within => (value - *threshold).abs() <= 0.1,
                    ConditionOperator::Outside => (value - *threshold).abs() > 0.1,
                    ConditionOperator::Equals => (value - *threshold).abs() <= 0.01,
                    _ => false,
                };
            }
            return value > 1.0;
        }
        ConditionField::DomPullStackBias => {
            let Some(value) = market.dom_summary.as_ref().map(|dom| dom.pull_stack_bias) else {
                return false;
            };
            if let ConditionValue::Number(threshold) = &cond.value {
                return match &cond.operator {
                    ConditionOperator::GreaterThan | ConditionOperator::Above => value > *threshold,
                    ConditionOperator::LessThan | ConditionOperator::Below => value < *threshold,
                    ConditionOperator::Within => value.abs() <= *threshold,
                    ConditionOperator::Outside => value.abs() > *threshold,
                    ConditionOperator::Equals => (value - *threshold).abs() <= 0.01,
                    _ => false,
                };
            }
            return value > 0.0;
        }

        // --- Profile / Day Type ---
        ConditionField::DayType => {
            if let ConditionValue::Text(expected) = &cond.value {
                let actual = format!("{:?}", market.day_type);
                return normalize_day_type_label(&actual) == normalize_day_type_label(expected);
            }
            return false;
        }
        ConditionField::ProfileShape => {
            if let ConditionValue::Text(expected) = &cond.value {
                let actual = format!("{:?}", market.profile_shape);
                return normalize_profile_shape_label(&actual)
                    == normalize_profile_shape_label(expected);
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
        ConditionField::RvolPercentile => {
            if let ConditionValue::Number(threshold) = &cond.value {
                return match &cond.operator {
                    ConditionOperator::GreaterThan | ConditionOperator::Above => {
                        market.rvol_percentile > *threshold
                    }
                    ConditionOperator::LessThan | ConditionOperator::Below => {
                        market.rvol_percentile < *threshold
                    }
                    _ => false,
                };
            }
            return false;
        }
        ConditionField::RvolVelocity => {
            if let ConditionValue::Number(threshold) = &cond.value {
                return match &cond.operator {
                    ConditionOperator::GreaterThan | ConditionOperator::Above => {
                        market.rvol_velocity > *threshold
                    }
                    ConditionOperator::LessThan | ConditionOperator::Below => {
                        market.rvol_velocity < *threshold
                    }
                    _ => false,
                };
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

        // --- Regime (IDEA-000) ---
        ConditionField::Regime => {
            if let ConditionValue::Text(expected) = &cond.value {
                let actual = format!("{:?}", market.regime);
                return actual.to_lowercase() == expected.to_lowercase();
            }
            return false;
        }
        ConditionField::IbExtensionState => {
            if let ConditionValue::Text(expected) = &cond.value {
                return market.ib_extension_state.to_lowercase() == expected.to_lowercase();
            }
            return false;
        }

        // --- Absorption failure (IDEA-012) ---
        ConditionField::AbsorptionInvalidated => {
            return market.has_recent_invalidated_absorption;
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

#[derive(Debug, Clone)]
struct SetupRuntime {
    state: SetupState,
    readiness: SetupReadiness,
    readiness_score: f64,
    met_conditions: Vec<String>,
    missing_conditions: Vec<String>,
    met_count: usize,
    total_count: usize,
    deterministic_all_met: bool,
    requires_discretionary: bool,
    current_price: f64,
    last_evaluated_at_ms: f64,
    last_transition_at_ms: f64,
    last_alert_emitted_at_ms: Option<f64>,
    setup_name: Option<String>,
}

impl Default for SetupRuntime {
    fn default() -> Self {
        Self {
            state: SetupState::NotActive,
            readiness: SetupReadiness::Inactive,
            readiness_score: 0.0,
            met_conditions: Vec::new(),
            missing_conditions: Vec::new(),
            met_count: 0,
            total_count: 0,
            deterministic_all_met: false,
            requires_discretionary: false,
            current_price: 0.0,
            last_evaluated_at_ms: 0.0,
            last_transition_at_ms: 0.0,
            last_alert_emitted_at_ms: None,
            setup_name: None,
        }
    }
}

fn condition_label(raw: &str) -> String {
    serde_json::from_str::<SetupCondition>(raw)
        .ok()
        .and_then(|tc| tc.label.or_else(|| Some(format!("{:?}", tc.field))))
        .unwrap_or_else(|| raw.to_string())
}

fn manual_readiness(state: &SetupState) -> Option<SetupReadiness> {
    match state {
        SetupState::Confirmed => Some(SetupReadiness::Confirmed),
        SetupState::InTrade => Some(SetupReadiness::InTrade),
        SetupState::Closed => Some(SetupReadiness::Closed),
        _ => None,
    }
}

fn transition_reason(
    previous_state: &SetupState,
    next_state: &SetupState,
    previous_readiness: &SetupReadiness,
    next_readiness: &SetupReadiness,
    conditions_changed: bool,
) -> Option<String> {
    if previous_state != next_state {
        Some("stateChanged".to_string())
    } else if previous_readiness != next_readiness {
        if matches!(next_readiness, SetupReadiness::DeterministicReady) {
            Some("deterministicReady".to_string())
        } else {
            Some("readinessChanged".to_string())
        }
    } else if conditions_changed {
        Some("conditionsChanged".to_string())
    } else {
        None
    }
}

/// Deterministic rules engine that evaluates playbook setups against market state.
#[derive(Default, Clone)]
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

    /// Evaluate deterministic conditions and return full progress metadata.
    pub fn evaluate_detailed(
        &mut self,
        setup: &SetupDefinition,
        market: &MarketState,
        risk_at_limit: bool,
    ) -> SetupEvaluationOutcome {
        self.evaluate_detailed_at(
            setup,
            market,
            risk_at_limit,
            chrono::Utc::now().timestamp_millis() as f64,
        )
    }

    /// Evaluate deterministic conditions at a market-data timestamp.
    pub fn evaluate_detailed_at(
        &mut self,
        setup: &SetupDefinition,
        market: &MarketState,
        risk_at_limit: bool,
        evaluation_ts_ms: f64,
    ) -> SetupEvaluationOutcome {
        let now_ms = evaluation_ts_ms;
        let setup_id = setup.id.clone();
        let setup_name = setup.name.clone();
        if !setup.active {
            let evaluation = SetupEvaluation {
                setup_id,
                setup_name,
                state: SetupState::NotActive,
                readiness: SetupReadiness::Inactive,
                readiness_score: 0.0,
                met_conditions: Vec::new(),
                missing_conditions: Vec::new(),
                met_count: 0,
                total_count: 0,
                deterministic_all_met: false,
                requires_discretionary: false,
                current_price: market.last_price,
                timestamp_ms: now_ms,
            };
            return SetupEvaluationOutcome {
                evaluation,
                alert: None,
                transition: None,
            };
        }
        let runtime = self.runtimes.entry(setup.id.clone()).or_default();
        runtime.setup_name = Some(setup.name.clone());
        let previous_state = runtime.state.clone();
        let previous_readiness = runtime.readiness.clone();
        let previous_met = runtime.met_conditions.clone();
        let previous_missing = runtime.missing_conditions.clone();
        if risk_at_limit {
            runtime.state = SetupState::NotActive;
            runtime.readiness = SetupReadiness::Inactive;
            runtime.readiness_score = 0.0;
            runtime.met_conditions.clear();
            runtime.missing_conditions.clear();
            runtime.met_count = 0;
            runtime.total_count = 0;
            runtime.deterministic_all_met = false;
            runtime.requires_discretionary = !setup.discretionary_conditions.is_empty();
            runtime.current_price = market.last_price;
            runtime.last_evaluated_at_ms = now_ms;
            let transition = (previous_state != runtime.state
                || previous_readiness != runtime.readiness)
                .then(|| {
                    runtime.last_transition_at_ms = now_ms;
                    SetupTransition {
                        setup_id: setup.id.clone(),
                        setup_name: setup.name.clone(),
                        previous_state,
                        next_state: runtime.state.clone(),
                        previous_readiness,
                        next_readiness: runtime.readiness.clone(),
                        readiness_score: runtime.readiness_score,
                        met_count: runtime.met_count,
                        total_count: runtime.total_count,
                        met_conditions: runtime.met_conditions.clone(),
                        missing_conditions: runtime.missing_conditions.clone(),
                        deterministic_all_met: false,
                        requires_discretionary: runtime.requires_discretionary,
                        current_price: market.last_price,
                        timestamp_ms: now_ms,
                        reason: "riskGate".to_string(),
                        alert_emitted: false,
                        last_alert_emitted_at_ms: runtime.last_alert_emitted_at_ms,
                    }
                });
            return SetupEvaluationOutcome {
                evaluation: SetupEvaluation {
                    setup_id: setup.id.clone(),
                    setup_name: setup.name.clone(),
                    state: runtime.state.clone(),
                    readiness: runtime.readiness.clone(),
                    readiness_score: runtime.readiness_score,
                    met_conditions: runtime.met_conditions.clone(),
                    missing_conditions: runtime.missing_conditions.clone(),
                    met_count: runtime.met_count,
                    total_count: runtime.total_count,
                    deterministic_all_met: false,
                    requires_discretionary: runtime.requires_discretionary,
                    current_price: market.last_price,
                    timestamp_ms: now_ms,
                },
                alert: None,
                transition,
            };
        }

        let delta_gate = market.session_delta.abs() >= setup.min_delta;

        let mut met_conditions = Vec::new();
        let mut missing_conditions = Vec::new();
        for raw in &setup.conditions {
            let label = condition_label(raw);
            let passed = if let Ok(tc) = serde_json::from_str::<SetupCondition>(raw) {
                evaluate_typed_condition(&tc, market, self.prev_market.as_ref())
            } else {
                evaluate_string_condition(raw, market)
            };
            if passed {
                met_conditions.push(label);
            } else {
                missing_conditions.push(label);
            }
        }

        if delta_gate {
            met_conditions.push("min_delta".to_string());
        } else {
            missing_conditions.push("min_delta".to_string());
        }

        let total_conditions = setup.conditions.len() + 1;
        let met_count = met_conditions.len();
        let all_met = missing_conditions.is_empty();

        let has_discretionary = !setup.discretionary_conditions.is_empty();

        let computed_state = if all_met {
            if has_discretionary {
                SetupState::Approaching
            } else {
                SetupState::ConditionsMet
            }
        } else if met_count > 0 {
            SetupState::Approaching
        } else {
            SetupState::NotActive
        };

        let computed_readiness = if all_met {
            SetupReadiness::DeterministicReady
        } else if met_count > 0 {
            SetupReadiness::Partial
        } else {
            SetupReadiness::Inactive
        };

        let next = if manual_readiness(&runtime.state).is_some() {
            runtime.state.clone()
        } else {
            computed_state
        };
        let next_readiness = manual_readiness(&runtime.state).unwrap_or(computed_readiness);
        let readiness_score = if total_conditions == 0 {
            0.0
        } else {
            met_count as f64 / total_conditions as f64
        };

        let mut alert = None;
        let state_changed = next != runtime.state;
        let readiness_changed = next_readiness != runtime.readiness;
        let entering_alert_state = matches!(next, SetupState::ConditionsMet) && state_changed;
        let entering_discretionary_ready = has_discretionary
            && all_met
            && matches!(next_readiness, SetupReadiness::DeterministicReady)
            && (state_changed || readiness_changed);

        if entering_alert_state || entering_discretionary_ready {
            let suppress_window_ms = setup.duplicate_suppression_ms.max(250) as f64;
            let suppressed = runtime
                .last_alert_emitted_at_ms
                .map(|last| now_ms - last < suppress_window_ms)
                .unwrap_or(false);

            if !suppressed {
                runtime.last_alert_emitted_at_ms = Some(now_ms);
                let stop_price = resolve_stop_price(setup, market);
                let target_prices = resolve_target_prices(setup, market);

                let backtest_summary = if setup.backtest_results != serde_json::json!({}) {
                    Some(setup.backtest_results.clone())
                } else {
                    None
                };

                alert = Some(SetupAlert {
                    setup_id: setup.id.clone(),
                    setup_name: setup.name.clone(),
                    state_transition: next.clone(),
                    triggered_conditions: met_conditions.clone(),
                    current_price: market.last_price,
                    timestamp: now_ms,
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
        }

        let conditions_changed =
            previous_met != met_conditions || previous_missing != missing_conditions;
        let reason = transition_reason(
            &previous_state,
            &next,
            &previous_readiness,
            &next_readiness,
            conditions_changed,
        );

        runtime.state = next.clone();
        runtime.readiness = next_readiness.clone();
        runtime.readiness_score = readiness_score;
        runtime.met_conditions = met_conditions.clone();
        runtime.missing_conditions = missing_conditions.clone();
        runtime.met_count = met_count;
        runtime.total_count = total_conditions;
        runtime.deterministic_all_met = all_met;
        runtime.requires_discretionary = has_discretionary;
        runtime.current_price = market.last_price;
        runtime.last_evaluated_at_ms = now_ms;

        let transition = reason.map(|reason| {
            runtime.last_transition_at_ms = now_ms;
            SetupTransition {
                setup_id: setup.id.clone(),
                setup_name: setup.name.clone(),
                previous_state,
                next_state: next.clone(),
                previous_readiness,
                next_readiness: next_readiness.clone(),
                readiness_score,
                met_count,
                total_count: total_conditions,
                met_conditions: met_conditions.clone(),
                missing_conditions: missing_conditions.clone(),
                deterministic_all_met: all_met,
                requires_discretionary: has_discretionary,
                current_price: market.last_price,
                timestamp_ms: now_ms,
                reason,
                alert_emitted: alert.is_some(),
                last_alert_emitted_at_ms: runtime.last_alert_emitted_at_ms,
            }
        });

        SetupEvaluationOutcome {
            evaluation: SetupEvaluation {
                setup_id: setup.id.clone(),
                setup_name: setup.name.clone(),
                state: next,
                readiness: next_readiness,
                readiness_score,
                met_conditions,
                missing_conditions,
                met_count,
                total_count: total_conditions,
                deterministic_all_met: all_met,
                requires_discretionary: has_discretionary,
                current_price: market.last_price,
                timestamp_ms: now_ms,
            },
            alert,
            transition,
        }
    }

    /// Evaluate deterministic conditions and return alerts on alerting transitions.
    pub fn evaluate(
        &mut self,
        setup: &SetupDefinition,
        market: &MarketState,
        risk_at_limit: bool,
    ) -> Option<SetupAlert> {
        self.evaluate_detailed(setup, market, risk_at_limit).alert
    }

    /// Mark a discretionary prompt as confirmed.
    pub fn acknowledge_prompt(&mut self, setup_id: &str) -> Option<SetupState> {
        self.acknowledge_prompt_at(setup_id, chrono::Utc::now().timestamp_millis() as f64)
    }

    /// Mark a discretionary prompt as confirmed at a specific timestamp.
    pub fn acknowledge_prompt_at(
        &mut self,
        setup_id: &str,
        timestamp_ms: f64,
    ) -> Option<SetupState> {
        let runtime = self.runtimes.get_mut(setup_id)?;
        runtime.state = SetupState::Confirmed;
        runtime.readiness = SetupReadiness::Confirmed;
        runtime.last_transition_at_ms = timestamp_ms;
        runtime.last_evaluated_at_ms = timestamp_ms;
        Some(runtime.state.clone())
    }

    /// Mark a setup as currently in a trade.
    pub fn mark_in_trade(&mut self, setup_id: &str) -> Option<SetupState> {
        self.mark_in_trade_at(setup_id, chrono::Utc::now().timestamp_millis() as f64)
    }

    /// Mark a setup as currently in a trade at a specific timestamp.
    pub fn mark_in_trade_at(&mut self, setup_id: &str, timestamp_ms: f64) -> Option<SetupState> {
        let runtime = self.runtimes.get_mut(setup_id)?;
        runtime.state = SetupState::InTrade;
        runtime.readiness = SetupReadiness::InTrade;
        runtime.last_transition_at_ms = timestamp_ms;
        runtime.last_evaluated_at_ms = timestamp_ms;
        Some(runtime.state.clone())
    }

    /// Mark a setup's trade lifecycle as closed.
    pub fn close_trade(&mut self, setup_id: &str) -> Option<SetupState> {
        self.close_trade_at(setup_id, chrono::Utc::now().timestamp_millis() as f64)
    }

    /// Mark a setup's trade lifecycle as closed at a specific timestamp.
    pub fn close_trade_at(&mut self, setup_id: &str, timestamp_ms: f64) -> Option<SetupState> {
        let runtime = self.runtimes.get_mut(setup_id)?;
        runtime.state = SetupState::Closed;
        runtime.readiness = SetupReadiness::Closed;
        runtime.last_transition_at_ms = timestamp_ms;
        runtime.last_evaluated_at_ms = timestamp_ms;
        Some(runtime.state.clone())
    }

    /// Get current state for a setup.
    pub fn get_state(&self, setup_id: &str) -> SetupState {
        self.runtimes
            .get(setup_id)
            .map(|r| r.state.clone())
            .unwrap_or(SetupState::NotActive)
    }

    /// Get current runtime snapshot for a setup.
    pub fn runtime_snapshot(&self, setup_id: &str) -> Option<SetupRuntimeSnapshot> {
        self.runtimes
            .get(setup_id)
            .map(|runtime| SetupRuntimeSnapshot {
                setup_id: setup_id.to_string(),
                setup_name: runtime.setup_name.clone(),
                state: runtime.state.clone(),
                readiness: runtime.readiness.clone(),
                readiness_score: runtime.readiness_score,
                met_conditions: runtime.met_conditions.clone(),
                missing_conditions: runtime.missing_conditions.clone(),
                met_count: runtime.met_count,
                total_count: runtime.total_count,
                deterministic_all_met: runtime.deterministic_all_met,
                requires_discretionary: runtime.requires_discretionary,
                current_price: runtime.current_price,
                last_evaluated_at_ms: runtime.last_evaluated_at_ms,
                last_transition_at_ms: runtime.last_transition_at_ms,
                last_alert_emitted_at_ms: runtime.last_alert_emitted_at_ms,
                source: "memory".to_string(),
            })
    }

    /// Rehydrate setup runtime state from persisted snapshots.
    pub fn rehydrate(&mut self, snapshots: Vec<SetupRuntimeSnapshot>) {
        for snapshot in snapshots {
            self.runtimes.insert(
                snapshot.setup_id.clone(),
                SetupRuntime {
                    state: snapshot.state,
                    readiness: snapshot.readiness,
                    readiness_score: snapshot.readiness_score,
                    met_conditions: snapshot.met_conditions,
                    missing_conditions: snapshot.missing_conditions,
                    met_count: snapshot.met_count,
                    total_count: snapshot.total_count,
                    deterministic_all_met: snapshot.deterministic_all_met,
                    requires_discretionary: snapshot.requires_discretionary,
                    current_price: snapshot.current_price,
                    last_evaluated_at_ms: snapshot.last_evaluated_at_ms,
                    last_transition_at_ms: snapshot.last_transition_at_ms,
                    last_alert_emitted_at_ms: snapshot.last_alert_emitted_at_ms,
                    setup_name: snapshot.setup_name,
                },
            );
        }
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
    fn detailed_evaluation_reports_partial_readiness() {
        let setup = make_setup(vec!["price_vs_vwap=above", "session_delta=positive"], 100.0);
        let market = MarketState {
            last_price: 21010.0,
            vwap: 21000.0,
            session_delta: 50.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let outcome = engine.evaluate_detailed(&setup, &market, false);

        assert_eq!(outcome.evaluation.state, SetupState::Approaching);
        assert_eq!(outcome.evaluation.readiness, SetupReadiness::Partial);
        assert!(outcome
            .evaluation
            .missing_conditions
            .contains(&"min_delta".to_string()));
        assert!(outcome.transition.is_some());
    }

    #[test]
    fn discretionary_all_met_is_deterministic_ready() {
        let mut setup = make_setup(vec!["price_vs_vwap=above", "session_delta=positive"], 100.0);
        setup.discretionary_conditions = vec!["Strong DOM initiation".to_string()];
        let market = MarketState {
            last_price: 21010.0,
            vwap: 21000.0,
            session_delta: 150.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let outcome = engine.evaluate_detailed(&setup, &market, false);

        assert_eq!(outcome.evaluation.state, SetupState::Approaching);
        assert_eq!(
            outcome.evaluation.readiness,
            SetupReadiness::DeterministicReady
        );
        assert!(outcome.alert.as_ref().is_some_and(|a| a.discretionary));
    }

    #[test]
    fn approaching_to_deterministic_ready_emits_progress_transition() {
        let mut setup = make_setup(vec!["price_vs_vwap=above", "session_delta=positive"], 100.0);
        setup.discretionary_conditions = vec!["Strong DOM initiation".to_string()];
        let partial_market = MarketState {
            last_price: 21010.0,
            vwap: 21000.0,
            session_delta: 50.0,
            ..Default::default()
        };
        let ready_market = MarketState {
            last_price: 21010.0,
            vwap: 21000.0,
            session_delta: 150.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let _ = engine.evaluate_detailed(&setup, &partial_market, false);
        let outcome = engine.evaluate_detailed(&setup, &ready_market, false);

        assert_eq!(outcome.evaluation.state, SetupState::Approaching);
        assert_eq!(
            outcome.evaluation.readiness,
            SetupReadiness::DeterministicReady
        );
        let transition = outcome.transition.expect("progress transition");
        assert_eq!(transition.reason, "deterministicReady");
        assert_eq!(transition.previous_state, SetupState::Approaching);
        assert_eq!(transition.next_state, SetupState::Approaching);
    }

    #[test]
    fn confirmed_state_is_not_overwritten_by_ordinary_evaluation() {
        let setup = make_setup(vec!["price_vs_vwap=above"], 0.0);
        let active_market = MarketState {
            last_price: 21010.0,
            vwap: 21000.0,
            ..Default::default()
        };
        let inactive_market = MarketState {
            last_price: 20990.0,
            vwap: 21000.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let _ = engine.evaluate_detailed(&setup, &active_market, false);
        engine.acknowledge_prompt(&setup.id).expect("confirm");
        let outcome = engine.evaluate_detailed(&setup, &inactive_market, false);

        assert_eq!(outcome.evaluation.state, SetupState::Confirmed);
        assert_eq!(outcome.evaluation.readiness, SetupReadiness::Confirmed);
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
    fn condition_absorption_at_price_uses_live_signal_state() {
        let setup = make_setup(vec!["absorption_at_price"], 1.0);
        let market = MarketState {
            confirmed_absorption_event_count: 1,
            has_recent_confirmed_absorption: false,
            session_delta: 0.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let alert = engine.evaluate(&setup, &market, false);
        assert!(
            alert.is_none(),
            "stale cumulative counts should not satisfy condition"
        );

        let active_market = MarketState {
            has_recent_confirmed_absorption: true,
            session_delta: 10.0,
            ..Default::default()
        };
        let alert = engine.evaluate(&setup, &active_market, false);
        assert!(
            alert.is_some(),
            "active confirmed absorption should satisfy condition"
        );
    }

    #[test]
    fn condition_exhaustion_detected_uses_live_signal_state() {
        let setup = make_setup(vec!["exhaustion_detected"], 1.0);
        let market = MarketState {
            confirmed_exhaustion_event_count: 1,
            has_recent_confirmed_exhaustion: false,
            session_delta: 0.0,
            ..Default::default()
        };
        let mut engine = RulesEngine::default();
        let alert = engine.evaluate(&setup, &market, false);
        assert!(
            alert.is_none(),
            "stale exhaustion counts should not satisfy condition"
        );

        let active_market = MarketState {
            has_recent_confirmed_exhaustion: true,
            session_delta: 10.0,
            ..Default::default()
        };
        let alert = engine.evaluate(&setup, &active_market, false);
        assert!(
            alert.is_some(),
            "active confirmed exhaustion should satisfy condition"
        );
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
    fn regime_condition_matches_state() {
        let market = MarketState {
            regime: crate::pipelines::Regime::OneSidedAcceptance,
            ..Default::default()
        };
        let hit: SetupCondition = serde_json::from_str(
            r#"{"id":"c1","field":"regime","operator":"equals","value":"OneSidedAcceptance"}"#,
        )
        .unwrap();
        assert!(evaluate_typed_condition(&hit, &market, None));
        let miss: SetupCondition = serde_json::from_str(
            r#"{"id":"c1","field":"regime","operator":"equals","value":"Migration"}"#,
        )
        .unwrap();
        assert!(!evaluate_typed_condition(&miss, &market, None));
    }

    #[test]
    fn ib_extension_state_condition_matches_state() {
        let market = MarketState {
            ib_extension_state: "UpOnly".to_string(),
            ..Default::default()
        };
        let hit: SetupCondition = serde_json::from_str(
            r#"{"id":"c1","field":"ib_extension_state","operator":"equals","value":"UpOnly"}"#,
        )
        .unwrap();
        assert!(evaluate_typed_condition(&hit, &market, None));
        let miss: SetupCondition = serde_json::from_str(
            r#"{"id":"c1","field":"ib_extension_state","operator":"equals","value":"BothSides"}"#,
        )
        .unwrap();
        assert!(!evaluate_typed_condition(&miss, &market, None));
    }

    #[test]
    fn absorption_invalidated_condition_matches_state() {
        let cond: SetupCondition = serde_json::from_str(
            r#"{"id":"c1","field":"absorption_invalidated","operator":"equals","value":true}"#,
        )
        .unwrap();
        let failed = MarketState {
            has_recent_invalidated_absorption: true,
            ..Default::default()
        };
        assert!(evaluate_typed_condition(&cond, &failed, None));
        let calm = MarketState::default();
        assert!(!evaluate_typed_condition(&cond, &calm, None));
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

    #[test]
    fn alert_resolves_fixed_point_hypothesis_exits() {
        let setup = SetupDefinition {
            targets: vec![serde_json::json!({
                "mode": "fixed_points",
                "direction": "long",
                "points": 18.0
            })],
            stop_logic: serde_json::json!({
                "mode": "fixed_points",
                "direction": "long",
                "points": 12.0
            }),
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
        assert_eq!(alert.target_prices, vec![21028.0]);
        assert_eq!(alert.stop_price, Some(20998.0));
    }

    #[test]
    fn dom_touch_imbalance_condition_evaluates() {
        let tc = SetupCondition {
            id: "dom-imbalance".into(),
            field: ConditionField::DomTouchImbalanceRatio,
            operator: ConditionOperator::GreaterThan,
            value: ConditionValue::Number(1.1),
            label: None,
        };
        let market = MarketState {
            dom_summary: Some(crate::depth::DomSummary {
                touch_imbalance_ratio: Some(1.4),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(evaluate_typed_condition(&tc, &market, None));
    }

    #[test]
    fn dom_pull_stack_bias_condition_evaluates() {
        let tc = SetupCondition {
            id: "dom-bias".into(),
            field: ConditionField::DomPullStackBias,
            operator: ConditionOperator::Above,
            value: ConditionValue::Number(5.0),
            label: None,
        };
        let market = MarketState {
            dom_summary: Some(crate::depth::DomSummary {
                pull_stack_bias: 12.0,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(evaluate_typed_condition(&tc, &market, None));
    }
}
