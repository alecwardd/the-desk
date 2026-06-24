//! Report signal outcome stats for a completed backtest job (standalone runner companion).
//!
//! Modes (via `REPORT_MODE` env, default `full`):
//! - `full` — distribution + excursions + summarize (default)
//! - `mfe-diagnostic` — MFE/MAE percentile table in R + win-rate-at-target upper bounds

use serde_json::json;
use the_desk_backend::db::{Database, HistoricalJobRun, SessionScopeFilter};
use the_desk_backend::research::hypothesis::summarize_hypothesis_run;
use the_desk_backend::research::{
    signal_outcome_conditional, signal_outcome_distribution, signal_outcome_excursions,
};

const START: &str = "2025-11-28";
const END: &str = "2026-03-06";

fn ensure_historical_job(db: &Database, job_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    if db.get_historical_job_run(job_id)?.is_none() {
        db.insert_historical_job_run(&HistoricalJobRun {
            id: job_id.to_string(),
            job_type: "backtest".to_string(),
            status: "completed".to_string(),
            params: serde_json::json!({ "jobId": job_id }),
            progress: serde_json::json!({}),
            result: Some(serde_json::json!({})),
            warnings: Vec::new(),
            error: None,
            submitted_at_ms: 1.0,
            started_at_ms: Some(1.0),
            finished_at_ms: Some(2.0),
        })?;
    }
    Ok(())
}

fn pct_in_r(points: f64, r_points: f64) -> f64 {
    if r_points > 0.0 {
        points / r_points
    } else {
        0.0
    }
}

fn run_mfe_diagnostic(
    db: &Database,
    job_id: &str,
    setup_ids: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let scope = SessionScopeFilter {
        session_type: Some("RTH".to_string()),
        ..Default::default()
    };
    let target_rs = [1.0_f64, 1.5, 2.0];
    let mut setups = Vec::new();

    for setup_id in setup_ids {
        let rows = db.list_hypothesis_signal_outcomes(setup_id, job_id, Some(&scope))?;
        let verified: Vec<_> = rows
            .into_iter()
            .filter(|r| r.outcome_quality == "verified")
            .collect();
        let n = verified.len();
        let default_r = 3.0_f64;

        let mfe_r: Vec<f64> = verified
            .iter()
            .filter_map(|r| {
                r.max_favorable_excursion
                    .map(|mfe| pct_in_r(mfe, r.risk_points.unwrap_or(default_r)))
            })
            .collect();
        let _mae_r: Vec<f64> = verified
            .iter()
            .filter_map(|r| {
                r.max_adverse_excursion
                    .map(|mae| pct_in_r(mae, r.risk_points.unwrap_or(default_r)))
            })
            .collect();

        let excursions = signal_outcome_excursions(
            db,
            Some(setup_id),
            Some(START),
            Some(END),
            Some(&scope),
            Some("backtest"),
            Some(job_id),
            false,
        )?;
        let mfe = &excursions.mfe_distribution;
        let mae = &excursions.mae_distribution;

        let win_rate_at_target: serde_json::Map<String, serde_json::Value> = target_rs
            .iter()
            .map(|&t| {
                let count = mfe_r.iter().filter(|&&v| v >= t).count();
                let frac = if n > 0 { count as f64 / n as f64 } else { 0.0 };
                (
                    format!("{t}R"),
                    json!({
                        "countMfeGte": count,
                        "fraction": frac,
                        "note": "upper bound — ignores stop-before-target ordering"
                    }),
                )
            })
            .collect();

        setups.push(json!({
            "setupId": setup_id,
            "N": n,
            "mfePercentilesR": {
                "p10": pct_in_r(mfe.p10, default_r),
                "p25": pct_in_r(mfe.p25, default_r),
                "p50": pct_in_r(mfe.median, default_r),
                "p75": pct_in_r(mfe.p75, default_r),
                "p90": pct_in_r(mfe.p90, default_r),
                "mean": pct_in_r(mfe.mean, default_r),
            },
            "maePercentilesR": {
                "p10": pct_in_r(mae.p10, default_r),
                "p25": pct_in_r(mae.p25, default_r),
                "p50": pct_in_r(mae.median, default_r),
                "p75": pct_in_r(mae.p75, default_r),
                "p90": pct_in_r(mae.p90, default_r),
                "mean": pct_in_r(mae.mean, default_r),
            },
            "winRateAtTargetUpperBound": win_rate_at_target,
        }));
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "mode": "mfe-diagnostic",
            "jobId": job_id,
            "rPointsAssumed": 3.0,
            "setups": setups,
        }))?
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let job_id = std::env::var("JOB_ID").expect("JOB_ID env required");
    let db_path = std::env::var("THE_DESK_BACKTEST_DB").unwrap_or_else(|_| {
        let home = std::env::var("USERPROFILE").unwrap_or_else(|_| ".".to_string());
        format!("{home}\\.the-desk\\data.db")
    });
    let setup_ids: Vec<String> = std::env::var("SETUP_IDS")
        .expect("SETUP_IDS comma-separated")
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let db = Database::open(&db_path)?;
    ensure_historical_job(&db, &job_id)?;

    let mode = std::env::var("REPORT_MODE").unwrap_or_else(|_| "full".to_string());
    if mode == "mfe-diagnostic" {
        return run_mfe_diagnostic(&db, &job_id, &setup_ids);
    }

    // Standalone runner does not write historical_job_runs; summarize requires it.
    if db.get_historical_job_run(&job_id)?.is_none() {
        ensure_historical_job(&db, &job_id)?;
    }

    let sc = SessionScopeFilter {
        session_type: Some("RTH".to_string()),
        ..Default::default()
    };

    let backtest_run = db
        .get_backtest_run_for_job_id(&job_id)?
        .ok_or("backtest_runs row not found")?;
    let job_integrity = backtest_run
        .get("metrics")
        .and_then(|m| m.get("signalOutcomeIntegrity"))
        .cloned()
        .unwrap_or(json!({}));

    let mut results = Vec::new();
    for setup_id in setup_ids {
        let integrity =
            db.signal_outcome_integrity_report(Some("backtest"), Some(&job_id), Some(&setup_id))?;
        let integrity_status = integrity
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        if integrity_status != "ok" {
            results.push(json!({
                "setupId": setup_id,
                "status": integrity_status,
                "integrity": integrity,
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

        results.push(json!({
            "setupId": setup_id,
            "jobId": job_id,
            "status": "ok",
            "integrity": integrity,
            "N": dist.meta.effective_sample_size,
            "verifiedSampleSize": dist.sample_count,
            "winRate": summary.win_rate,
            "expectancyR": dist.mean,
            "mfeMeanR": summary.mfe_distribution_r.mean,
            "maeMeanR": summary.mae_distribution_r.mean,
            "signalsPerActiveSession": summary.signals_per_active_session,
            "activeSessionCount": summary.active_session_count,
            "chatty": summary.chatty,
            "summaryWarnings": summary.warnings,
            "distribution": dist,
            "excursions": excursions,
            "conditional_day_type_trend": cond,
        }));
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "jobId": job_id,
            "jobIntegrity": job_integrity,
            "setups": results,
        }))?
    );
    Ok(())
}
