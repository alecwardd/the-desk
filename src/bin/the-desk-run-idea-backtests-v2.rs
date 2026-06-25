//! IDEA-020 A/B backtest runner — recent-window zone detection vs prior session-cumulative baselines.

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
const R_POINTS: f64 = 12.0;

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

    const STOP: f64 = 12.0;
    // v6/v7 = session-cumulative excursion diagnostic; v8/v9 = recent-window A/B (this run).
    let targets: [(i64, f64, &str); 2] = [(8, 9.0, "9pt"), (9, 12.0, "12pt")];

    let mut out = Vec::new();
    for e in &entries {
        for (version, target_pts, label) in targets {
            out.push(json!({
                "metadata": {
                    "hypothesisId": e.hyp_id,
                    "version": version,
                    "docReference": "IDEA-020",
                    "proseSummary": format!(
                        "{} — recent-window zones, 12pt stop / {} target (A/B vs session-cumulative).",
                        e.name, label
                    ),
                    "owner": "user",
                    "sessionScope": ["rth"]
                },
                "setupDefinition": {
                    "id": format!("hyp_IDEA-020_{}_{}", e.id_base, label.replace('.', "_")),
                    "name": format!("IDEA-020 {} {}", e.name, label),
                    "description": e.description,
                    "active": false,
                    "duplicateSuppressionMs": 300000,
                    "conditions": [
                        format!("{{\"id\":\"c1\",\"field\":\"{}\",\"operator\":\"equals\",\"value\":true}}", e.field)
                    ],
                    "stopLogic": { "mode": "fixed_points", "direction": e.direction, "points": STOP },
                    "targets": [{
                        "mode": "fixed_points",
                        "direction": e.direction,
                        "points": target_pts,
                        "label": format!("{label} target")
                    }],
                    "positionSizing": { "r_points": R_POINTS },
                    "templateSource": format!("hypothesis:IDEA-020:{}:recent-window:{}", e.id_base, label)
                }
            }));
        }
    }

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
            "\n=== {label} recent-window A/B dry-run: setupId={} feasibleForN30={} projected={} warnings={:?}",
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
            "expectancyPoints": dist.mean * R_POINTS,
            "expectancyR": dist.mean,
            "mfePoints": {
                "mean": excursions.mfe_distribution.mean,
                "p10": excursions.mfe_distribution.p10,
                "p25": excursions.mfe_distribution.p25,
                "p50": excursions.mfe_distribution.median,
                "p75": excursions.mfe_distribution.p75,
                "p90": excursions.mfe_distribution.p90,
            },
            "maePoints": {
                "mean": excursions.mae_distribution.mean,
                "p10": excursions.mae_distribution.p10,
                "p25": excursions.mae_distribution.p25,
                "p50": excursions.mae_distribution.median,
                "p75": excursions.mae_distribution.p75,
                "p90": excursions.mae_distribution.p90,
            },
            "outcomeBreakdown": excursions.outcome_breakdown,
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
