use super::SetupDefinition;

/// Pre-built playbook setup templates encoding PTT methodology.
/// Users can customize thresholds. Loaded into the rules engine on startup.
pub fn all_templates() -> Vec<SetupDefinition> {
    vec![
        or5_mid_retest(),
        rebid_at_support(),
        reoffer_at_resistance(),
        single_print_continuation(),
        dnva_retest(),
        delta_pinch_reversal(),
        ib_extension_play(),
        session_inventory_clear(),
        vwap_band_zone_entry(),
    ]
}

/// Leo's A+ setup: after 5-min OR break, wait for retest of midpoint.
fn or5_mid_retest() -> SetupDefinition {
    SetupDefinition {
        id: "tpl_or5_mid_retest".into(),
        name: "OR5 Mid Retest".into(),
        description: "After 5-min Opening Range break, price retests the midpoint. \
                       Requires: OR5 broken, delta confirmation at mid on retest, \
                       RVOL >= Normal. Targets: opposite OR5 extreme, then 75%/100% extensions."
            .into(),
        active: false,
        conditions: vec![
            r#"{"id":"c1","field":"or5_broken_direction","operator":"equals","value":"Up","label":"OR5 broken upward"}"#.into(),
            r#"{"id":"c2","field":"rvol_classification","operator":"equals","value":"Normal","label":"RVOL at least Normal"}"#.into(),
        ],
        min_delta: 0.0,
        duplicate_suppression_ms: 5000,
        entry_logic: serde_json::json!({
            "trigger": "price_retests_or5_mid_after_break",
            "confirmation": "delta_positive_on_retest_for_long",
            "note": "Wait for mid retest, don't chase the break"
        }),
        stop_logic: serde_json::json!({
            "type": "opposite_or5_extreme",
            "note": "Stop at opposite OR5 boundary"
        }),
        targets: vec![
            serde_json::json!({"label": "OR5 opposite extreme", "type": "or5_extreme"}),
            serde_json::json!({"label": "75% extension", "type": "or5_ext_75"}),
            serde_json::json!({"label": "100% extension", "type": "or5_ext_100"}),
        ],
        discretionary_conditions: vec![
            "DOM shows aggressive initiation on retest".into(),
        ],
        template_source: Some("leo_playbook".into()),
        ..Default::default()
    }
}

/// Leo's rebid: price returns to a buy acceleration zone.
fn rebid_at_support() -> SetupDefinition {
    SetupDefinition {
        id: "tpl_rebid_support".into(),
        name: "Rebid at Support".into(),
        description: "Price returns to a buy acceleration zone. Requires: zone status = Retested, \
                       delta turning positive. Stop: other side of zone. Target: prior swing high."
            .into(),
        active: false,
        conditions: vec![
            r#"{"id":"c1","field":"active_rebid_zone","operator":"equals","value":true,"label":"Active rebid zone near price"}"#.into(),
        ],
        min_delta: 0.0,
        duplicate_suppression_ms: 5000,
        entry_logic: serde_json::json!({
            "trigger": "price_enters_rebid_zone",
            "confirmation": "delta_turns_positive_on_retest"
        }),
        stop_logic: serde_json::json!({
            "type": "opposite_side_of_zone",
            "note": "Place stop below the zone low"
        }),
        targets: vec![
            serde_json::json!({"label": "Prior swing high", "type": "swing_high"}),
        ],
        discretionary_conditions: vec![
            "Buyers re-engage visibly on tape".into(),
        ],
        template_source: Some("leo_playbook".into()),
        ..Default::default()
    }
}

/// Mirror of rebid for short side.
fn reoffer_at_resistance() -> SetupDefinition {
    SetupDefinition {
        id: "tpl_reoffer_resistance".into(),
        name: "Reoffer at Resistance".into(),
        description: "Price returns to a sell acceleration zone. Requires: zone status = Retested, \
                       delta turning negative. Stop: other side of zone. Target: prior swing low."
            .into(),
        active: false,
        conditions: vec![
            r#"{"id":"c1","field":"active_reoffer_zone","operator":"equals","value":true,"label":"Active reoffer zone near price"}"#.into(),
        ],
        min_delta: 0.0,
        duplicate_suppression_ms: 5000,
        entry_logic: serde_json::json!({
            "trigger": "price_enters_reoffer_zone",
            "confirmation": "delta_turns_negative_on_retest"
        }),
        stop_logic: serde_json::json!({
            "type": "opposite_side_of_zone",
            "note": "Place stop above the zone high"
        }),
        targets: vec![
            serde_json::json!({"label": "Prior swing low", "type": "swing_low"}),
        ],
        template_source: Some("leo_playbook".into()),
        ..Default::default()
    }
}

/// Single prints present, price aligned with direction. "Never fade single prints."
fn single_print_continuation() -> SetupDefinition {
    SetupDefinition {
        id: "tpl_single_print_continuation".into(),
        name: "Single Print Continuation".into(),
        description: "Single prints present, price aligned with single print direction. \
                       Day type = Trend or NormalVariation. Leo's data: 72%% of sessions close \
                       in the direction of single prints. Never fade a day with single prints."
            .into(),
        active: false,
        conditions: vec![
            r#"{"id":"c1","field":"single_prints_direction","operator":"equals","value":"AbovePoc","label":"Single prints above POC"}"#.into(),
            r#"{"id":"c2","field":"day_type","operator":"equals","value":"Trend","label":"Trend day"}"#.into(),
        ],
        min_delta: 0.0,
        duplicate_suppression_ms: 10000,
        entry_logic: serde_json::json!({
            "trigger": "single_prints_present_with_directional_profile",
            "note": "Trade in direction of single prints, 72% close in that direction"
        }),
        stop_logic: serde_json::json!({
            "type": "below_value_area",
            "note": "Stop below VA low for longs, above VA high for shorts"
        }),
        targets: vec![
            serde_json::json!({"label": "IB extension", "type": "ib_ext_1x"}),
        ],
        template_source: Some("leo_playbook".into()),
        ..Default::default()
    }
}

/// User's concept: price returns to DNVA boundary with delta confirmation.
fn dnva_retest() -> SetupDefinition {
    SetupDefinition {
        id: "tpl_dnva_retest".into(),
        name: "DNVA Retest".into(),
        description: "Price returns to DNVA boundary. Delta confirms: buyers re-engage at DNVA low \
                       (for longs) or sellers at DNVA high (for shorts). Delta positioning \
                       building in trade direction."
            .into(),
        active: false,
        conditions: vec![
            r#"{"id":"c1","field":"price_in_dnva","operator":"equals","value":true,"label":"Price within DNVA"}"#.into(),
            r#"{"id":"c2","field":"session_inventory_state","operator":"equals","value":"Building","label":"Inventory building"}"#.into(),
        ],
        min_delta: 0.0,
        duplicate_suppression_ms: 5000,
        entry_logic: serde_json::json!({
            "trigger": "price_at_dnva_boundary",
            "confirmation": "delta_confirms_direction_at_boundary"
        }),
        stop_logic: serde_json::json!({
            "type": "beyond_dnva",
            "note": "Stop beyond opposite DNVA boundary"
        }),
        targets: vec![
            serde_json::json!({"label": "DNP (delta neutral pivot)", "type": "dnp"}),
            serde_json::json!({"label": "Opposite DNVA boundary", "type": "dnva_opposite"}),
        ],
        template_source: Some("user_playbook".into()),
        ..Default::default()
    }
}

/// User's concept: pinch event with severity + inventory shift confirms.
fn delta_pinch_reversal() -> SetupDefinition {
    SetupDefinition {
        id: "tpl_delta_pinch_reversal".into(),
        name: "Delta Pinch Reversal".into(),
        description: "Pinch event detected with severity >= threshold. Inventory shift confirms: \
                       new delta direction aligns with key level proximity. Entry after pinch \
                       completes."
            .into(),
        active: false,
        conditions: vec![
            r#"{"id":"c1","field":"pinch_detected","operator":"equals","value":true,"label":"Pinch event detected"}"#.into(),
        ],
        min_delta: 0.0,
        duplicate_suppression_ms: 10000,
        entry_logic: serde_json::json!({
            "trigger": "pinch_event_with_severity_threshold",
            "confirmation": "new_delta_direction_aligns_with_key_level",
            "note": "Wait for pinch to complete before entry"
        }),
        stop_logic: serde_json::json!({
            "type": "pre_pinch_extreme",
            "note": "Stop at the pre-pinch price extreme"
        }),
        targets: vec![
            serde_json::json!({"label": "Key level in new direction", "type": "nearest_key_level"}),
        ],
        discretionary_conditions: vec![
            "Pinch severity is high enough to warrant entry".into(),
            "Key level proximity confirms direction".into(),
        ],
        template_source: Some("user_playbook".into()),
        ..Default::default()
    }
}

/// Price breaks IB range with momentum.
fn ib_extension_play() -> SetupDefinition {
    SetupDefinition {
        id: "tpl_ib_extension".into(),
        name: "IB Extension Play".into(),
        description: "Price breaks IB range with momentum. Requires: RVOL >= Normal, \
                       delta confirming direction, no single prints opposing. \
                       Targets: 0.5x, 1.0x, 1.5x IB extensions."
            .into(),
        active: false,
        conditions: vec![
            r#"{"id":"c1","field":"price_vs_ib_high","operator":"above","label":"Price above IB high"}"#.into(),
            r#"{"id":"c2","field":"rvol_classification","operator":"equals","value":"Normal","label":"RVOL at least Normal"}"#.into(),
            r#"{"id":"c3","field":"session_delta_sign","operator":"above","label":"Session delta positive"}"#.into(),
        ],
        min_delta: 0.0,
        duplicate_suppression_ms: 10000,
        entry_logic: serde_json::json!({
            "trigger": "price_breaks_ib_with_momentum",
            "confirmation": "delta_confirming_and_rvol_supporting"
        }),
        stop_logic: serde_json::json!({
            "type": "inside_ib",
            "note": "Stop back inside IB range"
        }),
        targets: vec![
            serde_json::json!({"label": "IB 0.5x extension", "price_type": "ib_ext_05x"}),
            serde_json::json!({"label": "IB 1.0x extension", "price_type": "ib_ext_10x"}),
            serde_json::json!({"label": "IB 1.5x extension", "price_type": "ib_ext_15x"}),
        ],
        template_source: Some("user_playbook".into()),
        ..Default::default()
    }
}

/// Prior session heavily one-sided, current session opposing.
fn session_inventory_clear() -> SetupDefinition {
    SetupDefinition {
        id: "tpl_session_inventory_clear".into(),
        name: "Session Inventory Clear".into(),
        description: "Prior session delta was heavily one-sided. Current session delta is \
                       opposing (clearing). Look for directional continuation in clearing \
                       direction at key levels."
            .into(),
        active: false,
        conditions: vec![
            r#"{"id":"c1","field":"session_inventory_state","operator":"equals","value":"Clearing","label":"Inventory clearing"}"#.into(),
        ],
        min_delta: 0.0,
        duplicate_suppression_ms: 10000,
        entry_logic: serde_json::json!({
            "trigger": "inventory_clearing_at_key_level",
            "confirmation": "delta_direction_aligns_with_clearing"
        }),
        stop_logic: serde_json::json!({
            "type": "key_level_invalidation",
            "note": "Stop if clearing direction reverses at key level"
        }),
        targets: vec![
            serde_json::json!({"label": "Next key level in clearing direction", "type": "key_level"}),
        ],
        discretionary_conditions: vec![
            "Tape confirms aggressive clearing flow at key level".into(),
        ],
        template_source: Some("user_playbook".into()),
        ..Default::default()
    }
}

/// Stowe's concept: price in VWAP band zone with delta confirmation.
fn vwap_band_zone_entry() -> SetupDefinition {
    SetupDefinition {
        id: "tpl_vwap_band_zone".into(),
        name: "VWAP Band Zone Entry".into(),
        description: "Price in specific VWAP band zone. Delta confirmation required. \
                       Multi-timeframe alignment checked. Stowe's framework."
            .into(),
        active: false,
        conditions: vec![
            r#"{"id":"c1","field":"price_vs_vwap","operator":"above","label":"Price above VWAP"}"#.into(),
            r#"{"id":"c2","field":"session_delta_sign","operator":"above","label":"Session delta positive"}"#.into(),
        ],
        min_delta: 50.0,
        duplicate_suppression_ms: 5000,
        entry_logic: serde_json::json!({
            "trigger": "price_in_vwap_band_zone",
            "confirmation": "delta_confirms_and_multi_tf_aligned"
        }),
        stop_logic: serde_json::json!({
            "type": "vwap_band",
            "note": "Stop at next lower VWAP band"
        }),
        targets: vec![
            serde_json::json!({"label": "Next VWAP band", "type": "vwap_band"}),
        ],
        template_source: Some("stowe_playbook".into()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_templates_load_and_have_ids() {
        let templates = all_templates();
        assert_eq!(templates.len(), 9);
        for t in &templates {
            assert!(!t.id.is_empty(), "Template missing ID");
            assert!(!t.name.is_empty(), "Template missing name");
            assert!(
                t.id.starts_with("tpl_"),
                "Template ID should start with tpl_"
            );
        }
    }

    #[test]
    fn template_conditions_are_valid_json() {
        let templates = all_templates();
        for t in &templates {
            for cond in &t.conditions {
                let parsed: Result<serde_json::Value, _> = serde_json::from_str(cond);
                assert!(
                    parsed.is_ok() || !cond.starts_with('{'),
                    "Condition in {} is invalid JSON: {}",
                    t.name,
                    cond
                );
            }
        }
    }
}
