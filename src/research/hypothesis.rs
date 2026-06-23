use crate::db::{
    stable_hash_hex, Database, HypothesisSignalOutcomeRow, ResearchHypothesisRecord,
    SessionScopeFilter,
};
use crate::outcomes;
use crate::rules::{
    ConditionField, SetupCondition, SetupDefinition, SetupLifecycleStatus,
    RULES_ENGINE_SCHEMA_VERSION,
};
use crate::tick_time_context_from_timestamp_ms;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const DEFAULT_EXPECTANCY_FLOOR_R: f64 = 0.25;
const MIN_REPORTABLE_N: usize = 30;
const MIN_REGIME_N: usize = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HypothesisMetadata {
    pub hypothesis_id: String,
    pub version: i64,
    pub doc_reference: String,
    pub prose_summary: String,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub session_scope: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterHypothesisRequest {
    pub metadata: HypothesisMetadata,
    pub setup_definition: SetupDefinition,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComparableSetup {
    pub setup_id: String,
    pub hypothesis_id: Option<String>,
    pub similarity: f64,
    pub exact: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterHypothesisResponse {
    pub setup_id: String,
    pub dry_run: bool,
    pub condition_fingerprint: String,
    pub feasible_for_n30: bool,
    pub sessions_in_scope: i64,
    pub projected_sample_size: f64,
    pub comparable_setups: Vec<ComparableSetup>,
    pub warnings: Vec<String>,
    pub registered: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HypothesisRunSummary {
    pub setup_id: String,
    pub job_id: String,
    pub total_signals: usize,
    pub resolved: usize,
    pub pending: usize,
    pub expectancy_r: f64,
    pub win_rate: f64,
    pub r_distribution: DistributionSummary,
    pub mfe_distribution_r: DistributionSummary,
    pub mae_distribution_r: DistributionSummary,
    pub day_type_breakdown: BTreeMap<String, BreakdownSummary>,
    pub rvol_breakdown: BTreeMap<String, BreakdownSummary>,
    pub time_of_day_breakdown: BTreeMap<String, BreakdownSummary>,
    pub session_segment_breakdown: BTreeMap<String, BreakdownSummary>,
    pub engine_version: serde_json::Value,
    pub gate: GateDecision,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DistributionSummary {
    pub sample_count: usize,
    pub mean: f64,
    pub p25: f64,
    pub median: f64,
    pub p75: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BreakdownSummary {
    pub n: usize,
    pub expectancy_r: f64,
    pub win_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GateDecision {
    pub passed: bool,
    pub reason: String,
}

pub fn current_engine_version() -> serde_json::Value {
    serde_json::json!({
        "cargo": env!("CARGO_PKG_VERSION"),
        "gitSha": option_env!("VERGEN_GIT_SHA").unwrap_or("unknown"),
        "rulesSchema": RULES_ENGINE_SCHEMA_VERSION,
    })
}

fn engine_version_is_current(engine_version: &serde_json::Value) -> bool {
    engine_version == &current_engine_version()
}

pub fn setup_id_for_hypothesis(hypothesis_id: &str, version: i64) -> String {
    format!("hyp_{}_v{}", sanitize_id(hypothesis_id), version.max(1))
}

fn sanitize_id(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn rth_scope(metadata: &HypothesisMetadata) -> Result<SessionScopeFilter, String> {
    let scope = if metadata.session_scope.is_empty() {
        vec!["rth".to_string()]
    } else {
        metadata
            .session_scope
            .iter()
            .map(|s| s.trim().to_ascii_lowercase())
            .collect()
    };
    if scope != ["rth"] {
        return Err("first-slice hypotheses must declare sessionScope=[\"rth\"]".to_string());
    }
    Ok(SessionScopeFilter {
        session_type: Some("RTH".to_string()),
        trading_day_start: metadata.start_date.clone(),
        trading_day_end: metadata.end_date.clone(),
        ..Default::default()
    })
}

fn typed_conditions(setup: &SetupDefinition) -> Result<Vec<SetupCondition>, String> {
    let mut ids = BTreeSet::new();
    let mut out = Vec::new();
    for raw in &setup.conditions {
        let cond: SetupCondition = serde_json::from_str(raw)
            .map_err(|e| format!("condition must be typed SetupCondition JSON: {e}"))?;
        if cond.id.trim().is_empty() {
            return Err("condition id must not be empty".to_string());
        }
        if !ids.insert(cond.id.clone()) {
            return Err(format!("duplicate condition id `{}`", cond.id));
        }
        // These variants deserialize but do not produce deterministic backtest
        // evidence today: TimeOfDay is not carried in MarketState, DayOfWeek
        // reads wall-clock time, and TpoSinglePrintsPresent is a placeholder.
        match cond.field {
            ConditionField::TimeOfDay
            | ConditionField::DayOfWeek
            | ConditionField::TpoSinglePrintsPresent => {
                return Err(format!(
                    "condition field {:?} is not supported for hypothesis backtests",
                    cond.field
                ));
            }
            _ => {}
        }
        out.push(cond);
    }
    if out.is_empty() {
        return Err("hypothesis must include at least one typed condition".to_string());
    }
    Ok(out)
}

fn condition_tokens(conditions: &[SetupCondition]) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    for cond in conditions {
        let field = serde_json::to_value(&cond.field)
            .map_err(|e| e.to_string())?
            .as_str()
            .unwrap_or_default()
            .to_string();
        let operator = serde_json::to_value(&cond.operator)
            .map_err(|e| e.to_string())?
            .as_str()
            .unwrap_or_default()
            .to_string();
        let value = serde_json::to_string(&cond.value).map_err(|e| e.to_string())?;
        tokens.push(format!("{field}|{operator}|{value}"));
    }
    tokens.sort();
    Ok(tokens)
}

fn fingerprint(tokens: &[String]) -> String {
    stable_hash_hex(&tokens.join("||"))
}

fn jaccard(a: &[String], b: &[String]) -> f64 {
    let a: BTreeSet<_> = a.iter().collect();
    let b: BTreeSet<_> = b.iter().collect();
    let union = a.union(&b).count();
    if union == 0 {
        return 0.0;
    }
    a.intersection(&b).count() as f64 / union as f64
}

fn validate_price_expr(expr: &serde_json::Value, first_target: bool) -> Result<(), String> {
    if expr.get("price").and_then(|v| v.as_f64()).is_some() {
        return Ok(());
    }
    match expr.get("mode").and_then(|v| v.as_str()) {
        Some("fixed_points") => {
            let has_direction = expr.get("direction").and_then(|v| v.as_str()).is_some();
            let has_points = expr
                .get("points")
                .or_else(|| expr.get("targetPoints"))
                .or_else(|| expr.get("stopPoints"))
                .and_then(|v| v.as_f64())
                .filter(|v| *v > 0.0)
                .is_some();
            if has_direction && has_points {
                Ok(())
            } else {
                Err(
                    "fixed_points exit expressions require direction and positive points"
                        .to_string(),
                )
            }
        }
        Some("named_level_offset") => {
            if expr.get("level").and_then(|v| v.as_str()).is_some() {
                Ok(())
            } else {
                Err("named_level_offset exit expressions require level".to_string())
            }
        }
        _ if first_target => Err("first target must be numerically resolvable".to_string()),
        _ => Ok(()),
    }
}

fn normalize_exit_model(setup: &mut SetupDefinition) -> Result<(), String> {
    validate_price_expr(&setup.stop_logic, false)?;
    let first = setup
        .targets
        .first()
        .ok_or_else(|| "hypothesis requires at least one target".to_string())?;
    validate_price_expr(first, true)?;

    let r_points = setup
        .stop_logic
        .get("points")
        .or_else(|| setup.stop_logic.get("stopPoints"))
        .and_then(|v| v.as_f64())
        .or_else(|| {
            setup
                .position_sizing
                .get("r_points")
                .or_else(|| setup.position_sizing.get("rPoints"))
                .and_then(|v| v.as_f64())
        })
        .filter(|v| *v > 0.0)
        .ok_or_else(|| "exit model must provide positive stop distance / r_points".to_string())?;

    let mut position_sizing = setup
        .position_sizing
        .as_object()
        .cloned()
        .unwrap_or_default();
    position_sizing.insert("r_points".to_string(), serde_json::json!(r_points));
    setup.position_sizing = serde_json::Value::Object(position_sizing);
    Ok(())
}

fn setup_condition_tokens(setup: &SetupDefinition) -> Option<Vec<String>> {
    typed_conditions(setup)
        .ok()
        .and_then(|conditions| condition_tokens(&conditions).ok())
}

fn comparable_setups(
    db: &Database,
    requested_setup_id: &str,
    hypothesis_id: &str,
    tokens: &[String],
) -> Result<Vec<ComparableSetup>, String> {
    let mut out = Vec::new();
    for setup in db.list_setups().map_err(|e| e.to_string())? {
        if setup.id == requested_setup_id {
            continue;
        }
        let Some(other_tokens) = setup_condition_tokens(&setup) else {
            continue;
        };
        let similarity = jaccard(tokens, &other_tokens);
        if similarity >= 0.75 {
            let other_hypothesis = setup.parent_hypothesis_id.clone();
            let exact = similarity >= 1.0;
            if exact && other_hypothesis.as_deref() == Some(hypothesis_id) {
                continue;
            }
            out.push(ComparableSetup {
                setup_id: setup.id,
                hypothesis_id: other_hypothesis,
                similarity,
                exact,
            });
        }
    }
    Ok(out)
}

pub fn register_hypothesis(
    db: &Database,
    request: RegisterHypothesisRequest,
) -> Result<RegisterHypothesisResponse, String> {
    let metadata = request.metadata;
    if metadata.hypothesis_id.trim().is_empty() {
        return Err("hypothesisId must not be empty".to_string());
    }
    if metadata.version < 1 {
        return Err("version must be >= 1".to_string());
    }
    let scope = rth_scope(&metadata)?;
    let setup_id = setup_id_for_hypothesis(&metadata.hypothesis_id, metadata.version);
    let mut setup = request.setup_definition;
    setup.id = setup_id.clone();
    setup.active = false;
    setup.lifecycle_status = SetupLifecycleStatus::Hypothesis;
    setup.parent_hypothesis_id = Some(metadata.hypothesis_id.clone());
    setup.template_source = Some(format!(
        "hypothesis:{}:v{}",
        metadata.hypothesis_id, metadata.version
    ));
    normalize_exit_model(&mut setup)?;

    let conditions = typed_conditions(&setup)?;
    let tokens = condition_tokens(&conditions)?;
    let fingerprint = fingerprint(&tokens);
    let comparable = comparable_setups(db, &setup_id, &metadata.hypothesis_id, &tokens)?;
    if let Some(exact) = comparable.iter().find(|c| c.exact) {
        return Err(format!(
            "duplicate_fingerprint: existing setup {} has the same condition fingerprint",
            exact.setup_id
        ));
    }

    let sessions_in_scope = db
        .count_session_summaries_for_scope(
            metadata.start_date.as_deref(),
            metadata.end_date.as_deref(),
            Some(&scope),
        )
        .map_err(|e| e.to_string())?;
    let (projected_sample_size, projected_from_fallback) =
        projected_sample_size(db, &comparable, &conditions, sessions_in_scope, &scope)?;
    let feasible_for_n30 = projected_sample_size >= MIN_REPORTABLE_N as f64;
    let mut warnings = Vec::new();
    if !feasible_for_n30 {
        warnings.push(
            "scope is unlikely to produce N>=30; widen date range or relax conditions".to_string(),
        );
    }
    if projected_from_fallback {
        warnings.push(
            "sample-size projection used market-event fallback because no comparable setup exists"
                .to_string(),
        );
    }
    if comparable.iter().any(|c| c.similarity >= 0.75) {
        warnings.push(
            "similar setup fingerprint found; review comparableSetups before proceeding"
                .to_string(),
        );
    }

    if !request.dry_run {
        if let Some(existing) = db.get_setup(&setup_id).map_err(|e| e.to_string())? {
            if existing.parent_hypothesis_id.as_deref() == Some(metadata.hypothesis_id.as_str()) {
                return Ok(RegisterHypothesisResponse {
                    setup_id,
                    dry_run: false,
                    condition_fingerprint: fingerprint,
                    feasible_for_n30,
                    sessions_in_scope,
                    projected_sample_size,
                    comparable_setups: comparable,
                    warnings,
                    registered: false,
                });
            }
        }
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        db.upsert_setup(&setup).map_err(|e| e.to_string())?;
        db.upsert_research_hypothesis(&ResearchHypothesisRecord {
            hypothesis_id: metadata.hypothesis_id,
            current_version: metadata.version,
            setup_id: setup_id.clone(),
            doc_reference: metadata.doc_reference,
            prose_summary: metadata.prose_summary,
            owner: metadata.owner,
            lifecycle: "hypothesis".to_string(),
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            condition_fingerprint: fingerprint.clone(),
            session_scope: vec!["rth".to_string()],
            canonical_run_job_id: None,
            last_gate_decision: serde_json::json!({}),
            engine_version: current_engine_version(),
        })
        .map_err(|e| e.to_string())?;
    }

    Ok(RegisterHypothesisResponse {
        setup_id,
        dry_run: request.dry_run,
        condition_fingerprint: fingerprint,
        feasible_for_n30,
        sessions_in_scope,
        projected_sample_size,
        comparable_setups: comparable,
        warnings,
        registered: !request.dry_run,
    })
}

fn projected_sample_size(
    db: &Database,
    comparable: &[ComparableSetup],
    conditions: &[SetupCondition],
    sessions_in_scope: i64,
    scope: &SessionScopeFilter,
) -> Result<(f64, bool), String> {
    if let Some(best) = comparable
        .iter()
        .filter(|c| c.similarity > 0.0)
        .max_by(|a, b| a.similarity.partial_cmp(&b.similarity).unwrap())
    {
        let rows = db
            .list_signal_outcomes_with_context(Some(&best.setup_id), None, None, None)
            .map_err(|e| e.to_string())?;
        let historical_sessions = db
            .count_session_summaries_for_scope(None, None, None)
            .map_err(|e| e.to_string())?
            .max(1);
        let fire_rate = rows.len() as f64 / historical_sessions as f64;
        return Ok((fire_rate * sessions_in_scope.max(0) as f64, false));
    }

    if let Some(event_type) = conditions.iter().find_map(dominant_event_type) {
        let event_sessions = db
            .event_counts_per_session_context(event_type, None, None, Some(scope))
            .map_err(|e| e.to_string())?
            .len();
        return Ok((event_sessions as f64, true));
    }

    Ok((0.0, true))
}

fn dominant_event_type(condition: &SetupCondition) -> Option<&'static str> {
    match condition.field {
        ConditionField::AbsorptionAtPrice => Some("absorption_detected"),
        ConditionField::PinchDetected => Some("pinch_detected"),
        ConditionField::RvolClassification
        | ConditionField::RvolPercentile
        | ConditionField::RvolVelocity => Some("rvol_spike"),
        ConditionField::PriceVsDnp => Some("dnp_cross"),
        ConditionField::Or5BrokenDirection | ConditionField::PriceVsOr5Mid => {
            Some("or5_mid_retest")
        }
        ConditionField::PriceVsIbExtension
        | ConditionField::PriceVsIbHigh
        | ConditionField::PriceVsIbLow
        | ConditionField::IbExtensionState => Some("ib_extension_hit"),
        ConditionField::ActiveRebidZone | ConditionField::ActiveReofferZone => {
            Some("acceleration_zone_created")
        }
        ConditionField::RebidZoneHeld => Some("acceleration_zone_held"),
        ConditionField::DayType
        | ConditionField::ProfileShape
        | ConditionField::BalanceState
        | ConditionField::Regime => Some("day_type_change"),
        _ => None,
    }
}

fn distribution(values: &[f64]) -> DistributionSummary {
    if values.is_empty() {
        return DistributionSummary {
            sample_count: 0,
            mean: 0.0,
            p25: 0.0,
            median: 0.0,
            p75: 0.0,
        };
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
    DistributionSummary {
        sample_count: sorted.len(),
        mean,
        p25: percentile(&sorted, 25.0),
        median: percentile(&sorted, 50.0),
        p75: percentile(&sorted, 75.0),
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let h = (p.clamp(0.0, 100.0) / 100.0) * (sorted.len().saturating_sub(1)) as f64;
    let lo = h.floor() as usize;
    let hi = h.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        sorted[lo] + (sorted[hi] - sorted[lo]) * (h - lo as f64)
    }
}

fn add_breakdown(map: &mut BTreeMap<String, Vec<f64>>, key: impl Into<String>, r: Option<f64>) {
    if let Some(r) = r {
        map.entry(key.into()).or_default().push(r);
    }
}

fn finalize_breakdown(map: BTreeMap<String, Vec<f64>>) -> BTreeMap<String, BreakdownSummary> {
    map.into_iter()
        .map(|(k, values)| {
            let n = values.len();
            let expectancy_r = if n > 0 {
                values.iter().sum::<f64>() / n as f64
            } else {
                0.0
            };
            let win_rate = if n > 0 {
                values.iter().filter(|v| **v > 0.0).count() as f64 / n as f64
            } else {
                0.0
            };
            (
                k,
                BreakdownSummary {
                    n,
                    expectancy_r,
                    win_rate,
                },
            )
        })
        .collect()
}

fn r_points(setup: &SetupDefinition) -> f64 {
    setup
        .position_sizing
        .get("r_points")
        .or_else(|| setup.position_sizing.get("rPoints"))
        .and_then(|v| v.as_f64())
        .filter(|v| *v > 0.0)
        .unwrap_or(1.0)
}

pub fn summarize_hypothesis_run(
    db: &Database,
    setup_id: &str,
    job_id: &str,
) -> Result<HypothesisRunSummary, String> {
    let job = db
        .get_historical_job_run(job_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "historical job not found".to_string())?;
    if job.status != "completed" {
        return Err("historical job must be completed".to_string());
    }
    let backtest_run = db
        .get_backtest_run_for_job_id(job_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "completed backtest_runs row not found for jobId".to_string())?;
    let run_engine_version = backtest_run
        .get("params")
        .and_then(|p| p.get("engineVersion"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    let setup = db
        .get_setup(setup_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "setup not found".to_string())?;
    let scope = SessionScopeFilter {
        session_type: Some("RTH".to_string()),
        ..Default::default()
    };
    let rows = db
        .list_hypothesis_signal_outcomes(setup_id, job_id, Some(&scope))
        .map_err(|e| e.to_string())?;
    summarize_rows(setup_id, job_id, &setup, rows, run_engine_version)
}

fn summarize_rows(
    setup_id: &str,
    job_id: &str,
    setup: &SetupDefinition,
    rows: Vec<HypothesisSignalOutcomeRow>,
    engine_version: serde_json::Value,
) -> Result<HypothesisRunSummary, String> {
    let r_points = r_points(setup);
    let total = rows.len();
    let pending = rows.iter().filter(|r| r.outcome == "pending").count();
    let resolved = total.saturating_sub(pending);
    let r_values: Vec<f64> = rows.iter().filter_map(recomputed_row_r).collect();
    let expectancy = if r_values.is_empty() {
        0.0
    } else {
        r_values.iter().sum::<f64>() / r_values.len() as f64
    };
    let win_rate = if resolved > 0 {
        rows.iter()
            .filter(|r| r.outcome == "target_hit" || recomputed_row_r(r).is_some_and(|v| v > 0.0))
            .count() as f64
            / resolved as f64
    } else {
        0.0
    };
    let mfe_r: Vec<f64> = rows
        .iter()
        .filter_map(|r| r.max_favorable_excursion.map(|v| v / r_points))
        .collect();
    let mae_r: Vec<f64> = rows
        .iter()
        .filter_map(|r| r.max_adverse_excursion.map(|v| v / r_points))
        .collect();

    let mut day_type = BTreeMap::new();
    let mut rvol = BTreeMap::new();
    let mut tod = BTreeMap::new();
    let mut segment = BTreeMap::new();
    for row in &rows {
        let r_result = recomputed_row_r(row);
        add_breakdown(
            &mut day_type,
            row.day_type
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            r_result,
        );
        add_breakdown(
            &mut rvol,
            row.rvol_bucket_at_fire
                .map(|b| format!("bucket_{b}"))
                .unwrap_or_else(|| "unknown".to_string()),
            r_result,
        );
        let ctx = tick_time_context_from_timestamp_ms(row.fired_at_ms);
        let bucket = ctx
            .as_ref()
            .map(|c| {
                let start = (c.minute_of_session / 30) * 30;
                format!("rth_{start}_to_{}", start + 30)
            })
            .unwrap_or_else(|| "unknown".to_string());
        add_breakdown(&mut tod, bucket, r_result);
        add_breakdown(
            &mut segment,
            ctx.map(|c| format!("{:?}", c.session_segment))
                .unwrap_or_else(|| "unknown".to_string()),
            r_result,
        );
    }

    let day_type_breakdown = finalize_breakdown(day_type);
    let rvol_breakdown = finalize_breakdown(rvol);
    let gate = gate_decision(
        total,
        pending,
        expectancy,
        distribution(&mfe_r).p25,
        &day_type_breakdown,
        &rvol_breakdown,
    );
    let mut warnings = Vec::new();
    if !engine_version_is_current(&engine_version) {
        warnings.push(
            "engine_version_drift: backtest run engine version differs from current engine"
                .to_string(),
        );
    }

    Ok(HypothesisRunSummary {
        setup_id: setup_id.to_string(),
        job_id: job_id.to_string(),
        total_signals: total,
        resolved,
        pending,
        expectancy_r: expectancy,
        win_rate,
        r_distribution: distribution(&r_values),
        mfe_distribution_r: distribution(&mfe_r),
        mae_distribution_r: distribution(&mae_r),
        day_type_breakdown,
        rvol_breakdown,
        time_of_day_breakdown: finalize_breakdown(tod),
        session_segment_breakdown: finalize_breakdown(segment),
        engine_version,
        gate,
        warnings,
    })
}

fn recomputed_row_r(row: &HypothesisSignalOutcomeRow) -> Option<f64> {
    outcomes::recompute_r_result_fields(
        row.direction.as_deref(),
        row.entry_price,
        row.fired_price,
        row.exit_price,
        row.risk_points,
    )
    .or(row.r_result)
}

fn gate_decision(
    n: usize,
    pending: usize,
    expectancy: f64,
    mfe_p25_r: f64,
    day_type: &BTreeMap<String, BreakdownSummary>,
    rvol: &BTreeMap<String, BreakdownSummary>,
) -> GateDecision {
    if n < MIN_REPORTABLE_N {
        return GateDecision {
            passed: false,
            reason: "insufficient_sample".to_string(),
        };
    }
    if expectancy < DEFAULT_EXPECTANCY_FLOOR_R {
        return GateDecision {
            passed: false,
            reason: "expectancy_below_floor".to_string(),
        };
    }
    if mfe_p25_r < 0.0 {
        return GateDecision {
            passed: false,
            reason: "mfe_p25_below_zero".to_string(),
        };
    }
    if pending > 0 {
        return GateDecision {
            passed: false,
            reason: "pending_outcomes".to_string(),
        };
    }
    let survives = [day_type, rvol].iter().any(|partition| {
        partition
            .values()
            .filter(|b| b.n >= MIN_REGIME_N && b.expectancy_r >= 0.0)
            .count()
            >= 2
    });
    if !survives {
        return GateDecision {
            passed: false,
            reason: "single_regime_artifact".to_string(),
        };
    }
    GateDecision {
        passed: true,
        reason: "passed".to_string(),
    }
}

pub fn propose_draft_setup(
    db: &Database,
    setup_id: &str,
    job_id: &str,
) -> Result<serde_json::Value, String> {
    let summary = summarize_hypothesis_run(db, setup_id, job_id)?;
    if !engine_version_is_current(&summary.engine_version) {
        return Err("engine_version_drift: rerun backtest before proposing draft".to_string());
    }
    let mut record = db
        .get_research_hypothesis_by_setup(setup_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "research hypothesis metadata not found".to_string())?;
    let decision = serde_json::json!({
        "decision": if summary.gate.passed { "passed" } else { "failed" },
        "reason": summary.gate.reason,
        "jobId": job_id,
        "evaluatedAt": chrono::Utc::now().timestamp_millis(),
        "engineVersion": summary.engine_version,
    });
    let summary_json = serde_json::to_value(&summary).map_err(|e| e.to_string())?;
    let lifecycle = if summary.gate.passed {
        "draft"
    } else {
        "failed"
    };
    db.update_setup_lifecycle(
        setup_id,
        false,
        lifecycle,
        summary.gate.passed.then_some(&summary_json),
    )
    .map_err(|e| e.to_string())?;
    record.lifecycle = lifecycle.to_string();
    db.update_research_hypothesis_decision(
        &record.hypothesis_id,
        lifecycle,
        summary.gate.passed.then_some(job_id),
        &decision,
        &summary.engine_version,
        chrono::Utc::now().timestamp_millis() as f64,
    )
    .map_err(|e| e.to_string())?;
    Ok(serde_json::json!({
        "setupId": setup_id,
        "jobId": job_id,
        "lifecycleStatus": lifecycle,
        "gate": summary.gate,
        "summary": summary,
    }))
}

pub fn activate_draft_setup(
    db: &Database,
    setup_id: &str,
    trader_confirmation: &str,
) -> Result<serde_json::Value, String> {
    if trader_confirmation.trim().is_empty() {
        return Err("traderConfirmation must not be empty".to_string());
    }
    let setup = db
        .get_setup(setup_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "setup not found".to_string())?;
    if setup.lifecycle_status != SetupLifecycleStatus::Draft {
        return Err("setup must be lifecycleStatus=draft".to_string());
    }
    let cached_engine = setup.backtest_results.get("engineVersion").cloned();
    if cached_engine.as_ref() != Some(&current_engine_version()) {
        return Err("engine_version_drift: rerun backtest before activation".to_string());
    }
    db.update_setup_lifecycle(setup_id, true, "active", None)
        .map_err(|e| e.to_string())?;
    if let Some(record) = db
        .get_research_hypothesis_by_setup(setup_id)
        .map_err(|e| e.to_string())?
    {
        db.update_research_hypothesis_decision(
            &record.hypothesis_id,
            "active",
            record.canonical_run_job_id.as_deref(),
            &serde_json::json!({
                "decision": "activated",
                "reason": trader_confirmation,
                "evaluatedAt": chrono::Utc::now().timestamp_millis(),
                "engineVersion": current_engine_version(),
            }),
            &current_engine_version(),
            chrono::Utc::now().timestamp_millis() as f64,
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(serde_json::json!({
        "setupId": setup_id,
        "lifecycleStatus": "active",
        "active": true,
    }))
}

pub fn set_hypothesis_lifecycle(
    db: &Database,
    setup_id: &str,
    target: &str,
    reason: &str,
) -> Result<serde_json::Value, String> {
    if reason.trim().is_empty() {
        return Err("reason must not be empty".to_string());
    }
    if target != "rejectedByHuman" && target != "retired" {
        return Err("target must be rejectedByHuman or retired".to_string());
    }
    let changed = db
        .update_setup_lifecycle(setup_id, false, target, None)
        .map_err(|e| e.to_string())?;
    if !changed {
        return Err("setup not found".to_string());
    }
    if let Some(record) = db
        .get_research_hypothesis_by_setup(setup_id)
        .map_err(|e| e.to_string())?
    {
        db.update_research_hypothesis_decision(
            &record.hypothesis_id,
            target,
            record.canonical_run_job_id.as_deref(),
            &serde_json::json!({
                "decision": target,
                "reason": reason,
                "evaluatedAt": chrono::Utc::now().timestamp_millis(),
                "engineVersion": current_engine_version(),
            }),
            &current_engine_version(),
            chrono::Utc::now().timestamp_millis() as f64,
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(serde_json::json!({
        "setupId": setup_id,
        "lifecycleStatus": target,
        "active": false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{HistoricalJobRun, SessionSummary, SignalOutcome};
    use tempfile::NamedTempFile;

    fn test_db() -> Database {
        let file = NamedTempFile::new().expect("temp db");
        Database::open(file.path().to_string_lossy().as_ref()).expect("open db")
    }

    fn typed_condition(id: &str, field: &str, operator: &str, value: serde_json::Value) -> String {
        serde_json::json!({
            "id": id,
            "field": field,
            "operator": operator,
            "value": value,
        })
        .to_string()
    }

    fn hypothesis_setup() -> SetupDefinition {
        SetupDefinition {
            id: "ignored".to_string(),
            name: "Hypothesis".to_string(),
            active: true,
            conditions: vec![
                typed_condition("c1", "price_vs_vwap", "above", serde_json::Value::Null),
                typed_condition("c2", "session_delta_sign", "above", serde_json::Value::Null),
            ],
            stop_logic: serde_json::json!({
                "mode": "fixed_points",
                "direction": "long",
                "points": 12.0
            }),
            targets: vec![serde_json::json!({
                "mode": "fixed_points",
                "direction": "long",
                "points": 18.0
            })],
            position_sizing: serde_json::json!({}),
            ..Default::default()
        }
    }

    fn metadata(version: i64) -> HypothesisMetadata {
        HypothesisMetadata {
            hypothesis_id: "IDEA-TEST".to_string(),
            version,
            doc_reference: "IDEA-TEST".to_string(),
            prose_summary: "Test hypothesis".to_string(),
            owner: Some("tester".to_string()),
            session_scope: vec!["rth".to_string()],
            start_date: None,
            end_date: None,
        }
    }

    fn rth_summary(day: &str) -> SessionSummary {
        rth_summary_with_day_type(day, "Trend")
    }

    fn rth_summary_with_day_type(day: &str, day_type: &str) -> SessionSummary {
        SessionSummary {
            session_date: day.to_string(),
            session_type: "RTH".to_string(),
            root_symbol: "NQ".to_string(),
            contract_symbol: "NQH26.CME".to_string(),
            contract_month: Some("H26".to_string()),
            symbol_resolution_mode: "test".to_string(),
            carry_forward_levels_valid: true,
            rollover_warning: None,
            open_price: 21000.0,
            high: 21100.0,
            low: 20950.0,
            close: 21050.0,
            poc: 21025.0,
            vah: 21075.0,
            val: 20975.0,
            ib_high: 21080.0,
            ib_low: 20980.0,
            ib_range: 100.0,
            ib_mid: 21030.0,
            ib_extension_state: "None".to_string(),
            first_ib_extension_direction: None,
            first_ib_extension_timestamp_ms: None,
            or_high: 21050.0,
            or_low: 20990.0,
            day_type: day_type.to_string(),
            profile_shape: day_type.to_string(),
            balance_state: "Building".to_string(),
            total_volume: 1000.0,
            tick_count: 100,
            session_delta: 100.0,
            cumulative_delta: 100.0,
            dnp: 21010.0,
            dnva_high: 21070.0,
            dnva_low: 20990.0,
            vwap_close: 21020.0,
            signal_count: 1,
            single_prints_direction: "AbovePoc".to_string(),
            excess_high: false,
            excess_low: false,
            poor_high: false,
            poor_low: false,
            rvol_ratio: 1.2,
            close_vs_ib_mid: "above".to_string(),
            close_vs_vwap: "above".to_string(),
            close_vs_poc: "above".to_string(),
            snapshot_json: None,
        }
    }

    fn insert_completed_backtest_job(
        db: &Database,
        job_id: &str,
        engine_version: serde_json::Value,
    ) {
        db.insert_historical_job_run(&HistoricalJobRun {
            id: job_id.to_string(),
            job_type: "backtest".to_string(),
            status: "completed".to_string(),
            params: serde_json::json!({}),
            progress: serde_json::json!({}),
            result: Some(serde_json::json!({})),
            warnings: Vec::new(),
            error: None,
            submitted_at_ms: 1.0,
            started_at_ms: Some(1.0),
            finished_at_ms: Some(2.0),
        })
        .expect("insert job");
        db.insert_backtest_run(
            &format!("run-{job_id}"),
            2.0,
            &serde_json::json!({ "jobId": job_id, "engineVersion": engine_version }),
            &serde_json::json!({}),
            &serde_json::json!({}),
        )
        .expect("insert backtest run");
    }

    fn insert_outcome(
        db: &Database,
        signal_id: &str,
        setup_id: &str,
        job_id: &str,
        session_date: &str,
        r_result: f64,
        rvol_bucket: i32,
    ) {
        db.insert_signal_outcome(&SignalOutcome {
            signal_id: signal_id.to_string(),
            setup_id: setup_id.to_string(),
            setup_name: Some("Hypothesis".to_string()),
            session_date: session_date.to_string(),
            root_symbol: Some("NQ".to_string()),
            contract_symbol: Some("NQH26.CME".to_string()),
            source: "backtest".to_string(),
            job_id: Some(job_id.to_string()),
            fired_at_ms: 1_772_720_000_000.0,
            fired_price: 21000.0,
            target_price: Some(21018.0),
            stop_price: Some(20988.0),
            outcome: "target_hit".to_string(),
            outcome_at_ms: Some(1_772_720_060_000.0),
            max_favorable_excursion: Some(18.0),
            max_adverse_excursion: Some(0.0),
            r_result: Some(r_result),
            time_to_outcome_ms: Some(60_000.0),
            rvol_at_fire: Some(1.2),
            rvol_bucket_at_fire: Some(rvol_bucket),
            direction: Some("long".to_string()),
            entry_price: Some(21000.0),
            risk_points: Some(12.0),
            exit_price: Some(21018.0),
            outcome_quality: "verified".to_string(),
            quality_flags: Vec::new(),
            outcome_engine_version: Some("test".to_string()),
            rules_schema_version: Some("test".to_string()),
            setup_template_hash: Some("test".to_string()),
            last_observed_price: Some(21018.0),
            last_observed_at_ms: Some(1_772_720_060_000.0),
        })
        .expect("insert outcome");
    }

    #[test]
    fn dry_run_registers_nothing_but_returns_preview() {
        let db = test_db();
        let response = register_hypothesis(
            &db,
            RegisterHypothesisRequest {
                metadata: metadata(1),
                setup_definition: hypothesis_setup(),
                dry_run: true,
            },
        )
        .expect("dry run");

        assert_eq!(response.setup_id, "hyp_idea-test_v1");
        assert!(!response.registered);
        assert!(db.get_setup(&response.setup_id).unwrap().is_none());
    }

    #[test]
    fn register_is_idempotent_for_same_version() {
        let db = test_db();
        let request = RegisterHypothesisRequest {
            metadata: metadata(1),
            setup_definition: hypothesis_setup(),
            dry_run: false,
        };
        let first = register_hypothesis(&db, request.clone()).expect("first register");
        let second = register_hypothesis(&db, request).expect("second register");

        assert!(first.registered);
        assert!(!second.registered);
        let setup = db.get_setup(&first.setup_id).unwrap().unwrap();
        assert_eq!(setup.lifecycle_status, SetupLifecycleStatus::Hypothesis);
        assert_eq!(setup.position_sizing["r_points"], serde_json::json!(12.0));
    }

    #[test]
    fn registering_new_version_with_same_conditions_creates_row() {
        let db = test_db();
        let v1 = register_hypothesis(
            &db,
            RegisterHypothesisRequest {
                metadata: metadata(1),
                setup_definition: hypothesis_setup(),
                dry_run: false,
            },
        )
        .expect("v1");
        let v2 = register_hypothesis(
            &db,
            RegisterHypothesisRequest {
                metadata: metadata(2),
                setup_definition: hypothesis_setup(),
                dry_run: false,
            },
        )
        .expect("v2");

        assert_ne!(v1.setup_id, v2.setup_id);
        assert!(db.get_setup(&v1.setup_id).unwrap().is_some());
        assert!(db.get_setup(&v2.setup_id).unwrap().is_some());
        let record = db.get_research_hypothesis("IDEA-TEST").unwrap().unwrap();
        assert_eq!(record.current_version, 2);
        assert_eq!(record.setup_id, v2.setup_id);
    }

    #[test]
    fn older_version_registration_does_not_downgrade_current_metadata() {
        let db = test_db();
        let v3 = register_hypothesis(
            &db,
            RegisterHypothesisRequest {
                metadata: metadata(3),
                setup_definition: hypothesis_setup(),
                dry_run: false,
            },
        )
        .expect("v3");
        insert_completed_backtest_job(&db, "job-v3", current_engine_version());
        db.upsert_session_summary(&rth_summary("2026-03-05"))
            .expect("summary");
        for i in 0..30 {
            insert_outcome(
                &db,
                &format!("sig-v3-{i}"),
                &v3.setup_id,
                "job-v3",
                "2026-03-05",
                1.0,
                if i < 15 { 1 } else { 2 },
            );
        }
        propose_draft_setup(&db, &v3.setup_id, "job-v3").expect("promote v3");

        register_hypothesis(
            &db,
            RegisterHypothesisRequest {
                metadata: metadata(2),
                setup_definition: hypothesis_setup(),
                dry_run: false,
            },
        )
        .expect("v2");

        let record = db.get_research_hypothesis("IDEA-TEST").unwrap().unwrap();
        assert_eq!(record.current_version, 3);
        assert_eq!(record.setup_id, v3.setup_id);
        assert_eq!(record.lifecycle, "draft");
        assert_eq!(record.canonical_run_job_id.as_deref(), Some("job-v3"));
        assert_eq!(
            record.last_gate_decision["decision"],
            serde_json::json!("passed")
        );
        assert_eq!(record.engine_version, current_engine_version());
    }

    #[test]
    fn feasibility_without_comparable_or_event_mapping_is_unknown_not_optimistic() {
        let db = test_db();
        for day in 1..=40 {
            db.upsert_session_summary(&rth_summary(&format!("2026-03-{day:02}")))
                .expect("summary");
        }
        let response = register_hypothesis(
            &db,
            RegisterHypothesisRequest {
                metadata: metadata(1),
                setup_definition: hypothesis_setup(),
                dry_run: true,
            },
        )
        .expect("dry run");

        assert_eq!(response.sessions_in_scope, 40);
        assert_eq!(response.projected_sample_size, 0.0);
        assert!(!response.feasible_for_n30);
        assert!(response
            .warnings
            .iter()
            .any(|warning| warning.contains("sample-size projection used")));
    }

    #[test]
    fn unsupported_condition_field_rejected() {
        let db = test_db();
        let mut setup = hypothesis_setup();
        setup.conditions = vec![typed_condition(
            "c1",
            "time_of_day",
            "equals",
            serde_json::json!(570),
        )];
        let err = register_hypothesis(
            &db,
            RegisterHypothesisRequest {
                metadata: metadata(1),
                setup_definition: setup,
                dry_run: true,
            },
        )
        .unwrap_err();
        assert!(err.contains("not supported"));
    }

    #[test]
    fn summarize_rejects_incomplete_job() {
        let db = test_db();
        db.insert_historical_job_run(&HistoricalJobRun {
            id: "job-1".to_string(),
            job_type: "backtest".to_string(),
            status: "running".to_string(),
            params: serde_json::json!({}),
            progress: serde_json::json!({}),
            result: None,
            warnings: Vec::new(),
            error: None,
            submitted_at_ms: 1.0,
            started_at_ms: Some(1.0),
            finished_at_ms: None,
        })
        .expect("insert job");
        let err = summarize_hypothesis_run(&db, "setup", "job-1").unwrap_err();
        assert!(err.contains("completed"));
    }

    #[test]
    fn lifecycle_reject_records_manual_terminal_state() {
        let db = test_db();
        let registered = register_hypothesis(
            &db,
            RegisterHypothesisRequest {
                metadata: metadata(1),
                setup_definition: hypothesis_setup(),
                dry_run: false,
            },
        )
        .expect("register");
        let out = set_hypothesis_lifecycle(&db, &registered.setup_id, "rejectedByHuman", "noise")
            .expect("reject");
        assert_eq!(out["lifecycleStatus"], serde_json::json!("rejectedByHuman"));
        let setup = db.get_setup(&registered.setup_id).unwrap().unwrap();
        assert_eq!(
            setup.lifecycle_status,
            SetupLifecycleStatus::RejectedByHuman
        );
        assert!(!setup.active);
    }

    #[test]
    fn lifecycle_reject_unknown_setup_errors() {
        let db = test_db();
        let err = set_hypothesis_lifecycle(&db, "missing", "rejectedByHuman", "typo").unwrap_err();
        assert!(err.contains("setup not found"));
    }

    #[test]
    fn gate_passes_and_promotes_to_draft_with_two_rvol_buckets() {
        let db = test_db();
        let registered = register_hypothesis(
            &db,
            RegisterHypothesisRequest {
                metadata: metadata(1),
                setup_definition: hypothesis_setup(),
                dry_run: false,
            },
        )
        .expect("register");
        insert_completed_backtest_job(&db, "job-pass", current_engine_version());
        db.upsert_session_summary(&rth_summary("2026-03-05"))
            .expect("summary");
        for i in 0..30 {
            let bucket = if i < 15 { 1 } else { 2 };
            insert_outcome(
                &db,
                &format!("sig-pass-{i}"),
                &registered.setup_id,
                "job-pass",
                "2026-03-05",
                1.0,
                bucket,
            );
        }

        let promoted = propose_draft_setup(&db, &registered.setup_id, "job-pass").expect("promote");
        assert_eq!(promoted["gate"]["passed"], serde_json::json!(true));
        let setup = db.get_setup(&registered.setup_id).unwrap().unwrap();
        assert_eq!(setup.lifecycle_status, SetupLifecycleStatus::Draft);
        assert!(!setup.active);
        assert_eq!(
            setup.backtest_results["engineVersion"],
            current_engine_version()
        );
    }

    #[test]
    fn propose_rejects_engine_version_drift() {
        let db = test_db();
        let registered = register_hypothesis(
            &db,
            RegisterHypothesisRequest {
                metadata: metadata(1),
                setup_definition: hypothesis_setup(),
                dry_run: false,
            },
        )
        .expect("register");
        insert_completed_backtest_job(
            &db,
            "job-old",
            serde_json::json!({"cargo":"0.0.0","gitSha":"old","rulesSchema":0}),
        );
        let summary =
            summarize_hypothesis_run(&db, &registered.setup_id, "job-old").expect("summary warns");
        assert!(summary
            .warnings
            .iter()
            .any(|warning| warning.contains("engine_version_drift")));
        let err = propose_draft_setup(&db, &registered.setup_id, "job-old").unwrap_err();
        assert!(err.contains("engine_version_drift"));
    }

    #[test]
    fn gate_fails_when_pending_outcomes_exist() {
        let db = test_db();
        let registered = register_hypothesis(
            &db,
            RegisterHypothesisRequest {
                metadata: metadata(1),
                setup_definition: hypothesis_setup(),
                dry_run: false,
            },
        )
        .expect("register");
        insert_completed_backtest_job(&db, "job-2", current_engine_version());
        db.upsert_session_summary(&rth_summary("2026-03-05"))
            .expect("session summary");
        db.insert_signal_outcome(&SignalOutcome {
            signal_id: "sig-1".to_string(),
            setup_id: registered.setup_id.clone(),
            setup_name: Some("Hypothesis".to_string()),
            session_date: "2026-03-05".to_string(),
            root_symbol: Some("NQ".to_string()),
            contract_symbol: Some("NQH26.CME".to_string()),
            source: "backtest".to_string(),
            job_id: Some("job-2".to_string()),
            fired_at_ms: 1_772_720_000_000.0,
            fired_price: 21000.0,
            target_price: Some(21018.0),
            stop_price: Some(20988.0),
            outcome: "pending".to_string(),
            outcome_at_ms: None,
            max_favorable_excursion: None,
            max_adverse_excursion: None,
            r_result: None,
            time_to_outcome_ms: None,
            rvol_at_fire: Some(1.2),
            rvol_bucket_at_fire: Some(12),
            direction: Some("long".to_string()),
            entry_price: Some(21000.0),
            risk_points: Some(12.0),
            exit_price: None,
            outcome_quality: "verified".to_string(),
            quality_flags: Vec::new(),
            outcome_engine_version: Some("test".to_string()),
            rules_schema_version: Some("test".to_string()),
            setup_template_hash: Some("test".to_string()),
            last_observed_price: Some(21000.0),
            last_observed_at_ms: Some(1_772_720_000_000.0),
        })
        .expect("insert outcome");
        let summary =
            summarize_hypothesis_run(&db, &registered.setup_id, "job-2").expect("summarize");
        assert_eq!(summary.pending, 1);
        assert!(!summary.gate.passed);
    }

    #[test]
    fn idea_000_doc_anchor_has_typed_hypothesis_example() {
        let doc = include_str!("../../docs/setup-ideas-and-backtesting.md");
        let after_anchor = doc
            .split("<!-- hypothesis-anchor: IDEA-000 -->")
            .nth(1)
            .expect("anchor exists");
        let json_block = after_anchor
            .split("```json")
            .nth(1)
            .and_then(|rest| rest.split("```").next())
            .expect("json block");
        let mut request: RegisterHypothesisRequest =
            serde_json::from_str(json_block).expect("typed hypothesis example parses");
        request.dry_run = true;
        let db = test_db();
        register_hypothesis(&db, request).expect("IDEA-000 dry-run validates");
    }
}
