//! IDEA-020 zone backtest runner — exit target/stop sweep. Pins NQH6.CME + force replay.

use serde_json::json;
use std::sync::atomic::AtomicBool;
use the_desk_backend::backfill::{
    self, BackfillJobParams, BackfillReplayOptions, HistoricalJobType,
};
use the_desk_backend::db::{Database, HistoricalJobRun, SessionScopeFilter};
use the_desk_backend::feed::{load_feed_config, scid_reader::ScidReader, ContractMetadata};
use the_desk_backend::research::hypothesis::{
    register_hypothesis, summarize_hypothesis_run, RegisterHypothesisRequest,
    RegisterHypothesisResponse,
};
use the_desk_backend::research::{
    signal_outcome_conditional, signal_outcome_distribution, signal_outcome_excursions,
};

const START: &str = "2025-11-28";
const END: &str = "2026-03-06";

fn data_dir() -> std::path::PathBuf {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    std::path::PathBuf::from(home).join(".the-desk")
}

fn db_path() -> std::path::PathBuf {
    std::env::var("THE_DESK_BACKTEST_DB")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| data_dir().join("data.db"))
}

fn backtest_reader() -> ScidReader {
    let config = load_feed_config();
    let path = format!(
        r"{}\NQH6.CME.scid",
        config
            .sierra_data_dir
            .trim_end_matches('\\')
            .trim_end_matches('/')
    );
    ScidReader::with_price_scale(path, config.price_scale)
}

fn replay_options() -> BackfillReplayOptions {
    let config = load_feed_config();
    let scid_path = format!(
        r"{}\NQH6.CME.scid",
        config
            .sierra_data_dir
            .trim_end_matches('\\')
            .trim_end_matches('/')
    );
    BackfillReplayOptions {
        contract_metadata: Some(ContractMetadata {
            root_symbol: "NQ".to_string(),
            contract_symbol: "NQH6.CME".to_string(),
            contract_month: Some("202603".to_string()),
            expiry_year_month: Some("202603".to_string()),
            symbol_resolution_mode: "manual".to_string(),
            symbol_resolution_source: "backtest_window_override".to_string(),
            configured_symbol: "NQH6.CME".to_string(),
            active_symbol_override: Some("NQH6.CME".to_string()),
            scid_path,
            scid_file_exists: true,
            depth_prefix: "NQH6.CME".to_string(),
            depth_file_count: 0,
            warnings: vec!["backtest runner pinned NQH6 for Nov–Mar window".to_string()],
        }),
        ..Default::default()
    }
}

fn scope() -> SessionScopeFilter {
    SessionScopeFilter {
        session_type: Some("RTH".to_string()),
        ..Default::default()
    }
}

fn request_from_json(v: serde_json::Value, dry_run: bool) -> RegisterHypothesisRequest {
    let mut req: RegisterHypothesisRequest =
        serde_json::from_value(v).expect("hypothesis JSON parses");
    req.dry_run = dry_run;
    req
}

fn hypotheses() -> Vec<serde_json::Value> {
    struct Entry {
        id_base: &'static str,
        hyp_id: &'static str,
        name: &'static str,
        field: &'static str,
        direction: &'static str,
        description: &'static str,
    }

    let entries = [
        Entry {
            id_base: "rebid_retest_long",
            hyp_id: "IDEA-020-rebid-retest-long",
            name: "Rebid Zone Retest (Long)",
            field: "rebid_zone_retested",
            direction: "long",
            description: "Enter long when a footprint rebid zone is retested.",
        },
        Entry {
            id_base: "reoffer_retest_short",
            hyp_id: "IDEA-020-reoffer-retest-short",
            name: "Reoffer Zone Retest (Short)",
            field: "reoffer_zone_retested",
            direction: "short",
            description: "Enter short when a footprint reoffer zone is retested.",
        },
        Entry {
            id_base: "rebid_held_long",
            hyp_id: "IDEA-020-rebid-held-long",
            name: "Rebid Zone Held (Long)",
            field: "rebid_zone_held",
            direction: "long",
            description: "Enter long after rebid zone holds (fires back in zone direction).",
        },
        Entry {
            id_base: "reoffer_held_short",
            hyp_id: "IDEA-020-reoffer-held-short",
            name: "Reoffer Zone Held (Short)",
            field: "reoffer_zone_held",
            direction: "short",
            description: "Enter short after reoffer zone holds.",
        },
    ];

    let targets: [(f64, f64, &str); 3] = [(1.0, 3.0, "1R"), (1.5, 4.5, "1_5R"), (2.0, 6.0, "2R")];
    const STOP: f64 = 3.0;
    const R_POINTS: f64 = 3.0;

    let mut out = Vec::new();
    for e in entries {
        for (idx, (r_mult, target_pts, suffix)) in targets.iter().enumerate() {
            let version = (idx as i64) + 2; // v1 = original 3R run; sweep uses v2–v4
            let dir = e.direction;
            out.push(json!({
                "metadata": {
                    "hypothesisId": e.hyp_id,
                    "version": version,
                    "docReference": "IDEA-020",
                    "proseSummary": format!("{} @ {} target ({} pt stop).", e.name, suffix.replace('_', "."), STOP),
                    "owner": "user",
                    "sessionScope": ["rth"]
                },
                "setupDefinition": {
                    "id": format!("hyp_IDEA-020_{}_{}", e.id_base, suffix),
                    "name": format!("IDEA-020 {} {}", e.name, suffix.replace('_', ".")),
                    "description": e.description,
                    "active": false,
                    "duplicateSuppressionMs": 300000,
                    "conditions": [
                        format!("{{\"id\":\"c1\",\"field\":\"{}\",\"operator\":\"equals\",\"value\":true}}", e.field)
                    ],
                    "stopLogic": { "mode": "fixed_points", "direction": dir, "points": STOP },
                    "targets": [{
                        "mode": "fixed_points",
                        "direction": dir,
                        "points": target_pts,
                        "label": format!("{r_mult}R fixed target")
                    }],
                    "positionSizing": { "r_points": R_POINTS },
                    "templateSource": format!("hypothesis:{}:v{}", e.id_base, version)
                }
            }));
        }
    }

    // Wider-stop probe on best long variant (rebid held) — version 5 under same hypothesisId.
    out.push(json!({
        "metadata": {
            "hypothesisId": "IDEA-020-rebid-held-long",
            "version": 5,
            "docReference": "IDEA-020",
            "proseSummary": "Rebid held long with wider 4.5 pt stop / 6.75 pt target (1.5R).",
            "owner": "user",
            "sessionScope": ["rth"]
        },
        "setupDefinition": {
            "id": "hyp_IDEA-020_rebid_held_long_widestop",
            "name": "IDEA-020 Rebid Zone Held (Long) widestop",
            "description": "Rebid zone held long with 4.5 pt stop and 6.75 pt target.",
            "active": false,
            "duplicateSuppressionMs": 300000,
            "conditions": [
                "{\"id\":\"c1\",\"field\":\"rebid_zone_held\",\"operator\":\"equals\",\"value\":true}"
            ],
            "stopLogic": { "mode": "fixed_points", "direction": "long", "points": 4.5 },
            "targets": [{
                "mode": "fixed_points",
                "direction": "long",
                "points": 6.75,
                "label": "1.5R fixed target (wide stop)"
            }],
            "positionSizing": { "r_points": 4.5 },
            "templateSource": "hypothesis:IDEA-020:rebid-held-long:v5"
        }
    }));

    out
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = db_path();
    eprintln!("Backtest DB: {}", db_path.display());
    let db = Database::open(&db_path.to_string_lossy())?;
    let reader = backtest_reader();
    if !reader.path().exists() {
        eprintln!("SCID not found: {}", reader.path().display());
        std::process::exit(1);
    }
    eprintln!("Backtest SCID: {}", reader.path().display());

    let baseline_integrity = db.signal_outcome_integrity_report(Some("backtest"), None, None)?;
    eprintln!(
        "Baseline backtest integrity: {}",
        serde_json::to_string(&baseline_integrity)?
    );

    let sc = scope();
    let mut results = Vec::new();
    let mut registered: Vec<(String, String, RegisterHypothesisResponse)> = Vec::new();

    for hyp_json in hypotheses() {
        let label = hyp_json["metadata"]["hypothesisId"]
            .as_str()
            .unwrap_or("?")
            .to_string();
        let dry = register_hypothesis(&db, request_from_json(hyp_json.clone(), true))?;
        eprintln!(
            "\n=== {label} v2 dry-run: setupId={} feasibleForN30={} projected={} warnings={:?}",
            dry.setup_id, dry.feasible_for_n30, dry.projected_sample_size, dry.warnings
        );
        if !dry.feasible_for_n30 {
            eprintln!(
                "  WARNING: feasibleForN30=false (projected={}) — registering anyway for actual backtest N",
                dry.projected_sample_size
            );
        }
        let reg = register_hypothesis(&db, request_from_json(hyp_json, false))?;
        eprintln!(
            "  registered: {} (registered={})",
            reg.setup_id, reg.registered
        );
        registered.push((label, reg.setup_id.clone(), dry));
    }

    let expected = hypotheses().len();
    if registered.len() != expected {
        eprintln!(
            "\nABORT: only {}/{} hypotheses registered — will not backtest partial set.",
            registered.len(),
            expected
        );
        println!("{}", serde_json::to_string_pretty(&results)?);
        std::process::exit(1);
    }

    let setup_ids: Vec<String> = registered.iter().map(|(_, id, _)| id.clone()).collect();
    let job_id = uuid::Uuid::new_v4().to_string();
    eprintln!("\nRunning combined backtest {job_id} for {setup_ids:?}...");

    let cancel = AtomicBool::new(false);
    let job_result = backfill::run_backfill_job_with_options(
        &reader,
        &db,
        &BackfillJobParams {
            job_id: job_id.clone(),
            job_type: HistoricalJobType::Backtest,
            start_date: Some(START.to_string()),
            end_date: Some(END.to_string()),
            force: true,
            run_rules: true,
            setup_ids: Some(setup_ids),
        },
        |progress| {
            eprint!(
                "\r  {} | {} sessions | {}",
                progress.current_phase,
                progress.sessions_completed,
                progress.current_session_date.as_deref().unwrap_or("—")
            );
        },
        &cancel,
        replay_options(),
    )?;
    eprintln!(
        "\nBacktest done: sessions={} signals={} integrity_status={} analysis_passes={} ticks_per_analysis_avg={:.2} cadence={}ms/{}ticks warnings={:?}",
        job_result.sessions_processed,
        job_result.signals_fired,
        job_result.integrity_status,
        job_result.analysis_passes,
        job_result.ticks_per_analysis_avg,
        job_result.analysis_min_interval_ms,
        job_result.analysis_max_ticks,
        job_result.warnings
    );

    let finished_ms = chrono::Utc::now().timestamp_millis() as f64;
    db.insert_historical_job_run(&HistoricalJobRun {
        id: job_id.clone(),
        job_type: "backtest".to_string(),
        status: "completed".to_string(),
        params: serde_json::json!({
            "jobId": job_id,
            "startDate": START,
            "endDate": END,
            "force": true,
            "setupIds": registered.iter().map(|(_, id, _)| id.clone()).collect::<Vec<_>>(),
        }),
        progress: serde_json::json!({
            "sessionsCompleted": job_result.sessions_processed,
            "currentPhase": "completed",
        }),
        result: Some(serde_json::json!({
            "signalsFired": job_result.signals_fired,
            "integrityStatus": job_result.integrity_status,
            "analysisPasses": job_result.analysis_passes,
            "ticksPerAnalysisAvg": job_result.ticks_per_analysis_avg,
            "analysisMinIntervalMs": job_result.analysis_min_interval_ms,
            "analysisMaxTicks": job_result.analysis_max_ticks,
            "warnings": job_result.warnings,
        })),
        warnings: job_result.warnings.clone(),
        error: None,
        submitted_at_ms: finished_ms,
        started_at_ms: Some(finished_ms),
        finished_at_ms: Some(finished_ms),
    })?;

    for (label, setup_id, dry) in registered {
        let integrity =
            db.signal_outcome_integrity_report(Some("backtest"), Some(&job_id), Some(&setup_id))?;
        let integrity_status = integrity
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        if integrity_status != "ok" {
            results.push(json!({
                "hypothesisId": label,
                "setupId": setup_id,
                "jobId": job_id,
                "status": if integrity_status == "warning" { "integrity_warning" } else { "integrity_failed" },
                "integrity": integrity,
                "backtestIntegrityStatus": job_result.integrity_status,
                "backtestWarnings": job_result.warnings,
            }));
            continue;
        }

        let summary = summarize_hypothesis_run(&db, &setup_id, &job_id)?;

        let dist = signal_outcome_distribution(
            &db,
            &setup_id,
            Some(START),
            Some(END),
            Some(&sc),
            Some("backtest"),
            Some(&job_id),
            false,
        )?;

        let cond = signal_outcome_conditional(
            &db,
            &setup_id,
            "day_type",
            "Trend",
            Some(START),
            Some(END),
            Some(&sc),
            Some("backtest"),
            Some(&job_id),
            false,
        );

        let excursions = signal_outcome_excursions(
            &db,
            Some(&setup_id),
            Some(START),
            Some(END),
            Some(&sc),
            Some("backtest"),
            Some(&job_id),
            false,
        )?;

        results.push(json!({
            "hypothesisId": label,
            "setupId": setup_id,
            "jobId": job_id,
            "status": "ok",
            "integrity": integrity,
            "backtestIntegrityStatus": job_result.integrity_status,
            "backtestWarnings": job_result.warnings,
            "analysisPasses": job_result.analysis_passes,
            "ticksPerAnalysisAvg": job_result.ticks_per_analysis_avg,
            "analysisMinIntervalMs": job_result.analysis_min_interval_ms,
            "analysisMaxTicks": job_result.analysis_max_ticks,
            "N": dist.meta.effective_sample_size,
            "verifiedSampleSize": dist.sample_count,
            "winRate": summary.win_rate,
            "expectancyR": dist.mean,
            "mfeMeanR": summary.mfe_distribution_r.mean,
            "mfeP50R": summary.mfe_distribution_r.median,
            "maeMeanR": summary.mae_distribution_r.mean,
            "maeP50R": summary.mae_distribution_r.median,
            "signalsPerActiveSession": summary.signals_per_active_session,
            "activeSessionCount": summary.active_session_count,
            "chatty": summary.chatty,
            "summaryWarnings": summary.warnings,
            "distribution": dist,
            "conditional_day_type_trend": cond,
            "excursions": excursions,
            "dryRun": {
                "feasibleForN30": dry.feasible_for_n30,
                "projectedSampleSize": dry.projected_sample_size,
                "warnings": dry.warnings,
            },
        }));
    }

    println!("{}", serde_json::to_string_pretty(&results)?);
    Ok(())
}
