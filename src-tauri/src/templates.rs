use crate::rules::SetupDefinition;

/// Pre-loaded setup templates matching common NQ trading strategies.
pub fn builtin_templates() -> Vec<SetupDefinition> {
    vec![
        vwap_pullback(),
        or_breakout(),
        dnva_reversion(),
        single_print_retest(),
    ]
}

fn vwap_pullback() -> SetupDefinition {
    SetupDefinition {
        id: "template_vwap_pullback".into(),
        name: "VWAP Pullback".into(),
        description: "Long entry on pullback to VWAP in an uptrend day, with positive session delta confirming buying pressure.".into(),
        active: false,
        conditions: vec![
            "price_near_poc".into(),
            "session_delta=positive".into(),
        ],
        min_delta: 50.0,
        stop_logic: serde_json::json!({
            "type": "fixed_points",
            "points": 10.0,
            "description": "10 points below VWAP"
        }),
        targets: vec![
            serde_json::json!({"level": 1, "description": "1SD above VWAP", "management": "Trail stop to entry"}),
            serde_json::json!({"level": 2, "description": "2SD above VWAP", "management": "Take remaining"}),
        ],
        position_sizing: serde_json::json!({"type": "fixed", "contracts": 1}),
        market_context: serde_json::json!({"type": "trend", "direction": "up"}),
        discretionary_conditions: vec!["Strong bid-side DOM absorption on pullback".into()],
        backtest_results: serde_json::json!({
            "period": "Sample template",
            "samples": 0,
            "winRate": 0.0,
            "source": "template"
        }),
        template_source: Some("builtin:vwap_pullback".into()),
        ..Default::default()
    }
}

fn or_breakout() -> SetupDefinition {
    SetupDefinition {
        id: "template_or_breakout".into(),
        name: "OR Breakout".into(),
        description: "Breakout above the Opening Range high with volume confirmation. Targets IB high and prior day high.".into(),
        active: false,
        conditions: vec![
            "price_vs_or_high=above".into(),
            "session_delta=positive".into(),
        ],
        min_delta: 0.0,
        stop_logic: serde_json::json!({
            "type": "structural",
            "description": "Below OR midpoint"
        }),
        targets: vec![
            serde_json::json!({"level": 1, "description": "IB High", "management": "Move stop to breakeven"}),
            serde_json::json!({"level": 2, "description": "Prior Day High", "management": "Trail by 1SD"}),
        ],
        position_sizing: serde_json::json!({"type": "r_based", "riskPerTrade": 1.0}),
        market_context: serde_json::json!({"type": "any"}),
        discretionary_conditions: vec!["Aggressive lifting on the offer at breakout level".into()],
        backtest_results: serde_json::json!({
            "period": "Sample template",
            "samples": 0,
            "winRate": 0.0,
            "source": "template"
        }),
        template_source: Some("builtin:or_breakout".into()),
        ..Default::default()
    }
}

fn dnva_reversion() -> SetupDefinition {
    SetupDefinition {
        id: "template_dnva_reversion".into(),
        name: "DNVA Reversion".into(),
        description: "Mean reversion trade when price reaches the edge of the Delta Neutral Value Area, expecting rotation back toward DNP.".into(),
        active: false,
        conditions: vec![
            "price_in_dnva".into(),
        ],
        min_delta: 100.0,
        stop_logic: serde_json::json!({
            "type": "fixed_points",
            "points": 12.0,
            "description": "Beyond DNVA edge"
        }),
        targets: vec![
            serde_json::json!({"level": 1, "description": "Delta Neutral Pivot (DNP)", "management": "Take 50%"}),
            serde_json::json!({"level": 2, "description": "Opposite DNVA edge", "management": "Take remaining"}),
        ],
        position_sizing: serde_json::json!({"type": "fixed", "contracts": 1}),
        market_context: serde_json::json!({"type": "range"}),
        backtest_results: serde_json::json!({
            "period": "Sample template",
            "samples": 0,
            "winRate": 0.0,
            "source": "template"
        }),
        template_source: Some("builtin:dnva_reversion".into()),
        ..Default::default()
    }
}

fn single_print_retest() -> SetupDefinition {
    SetupDefinition {
        id: "template_single_print_retest".into(),
        name: "Single Print Retest".into(),
        description: "Trade the retest of a single-print zone from an earlier session period. Single prints represent initiative activity and often act as support/resistance.".into(),
        active: false,
        conditions: vec![
            "price_near_poc".into(),
        ],
        min_delta: 0.0,
        stop_logic: serde_json::json!({
            "type": "structural",
            "description": "Beyond the single-print zone"
        }),
        targets: vec![
            serde_json::json!({"level": 1, "description": "POC or VA edge", "management": "Move stop to breakeven"}),
        ],
        position_sizing: serde_json::json!({"type": "r_based", "riskPerTrade": 0.5}),
        market_context: serde_json::json!({"type": "any"}),
        discretionary_conditions: vec![
            "Price stalls and reverses at single-print zone".into(),
            "Visible absorption on DOM at the level".into(),
        ],
        backtest_results: serde_json::json!({
            "period": "Sample template",
            "samples": 0,
            "winRate": 0.0,
            "source": "template"
        }),
        template_source: Some("builtin:single_print_retest".into()),
        ..Default::default()
    }
}
