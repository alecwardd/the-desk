//! IDEA-025D 9AM continuation campaign harness (Stage 2).
//!
//! Safety:
//! - Never opens a live DB (no default/fallback to `T:\TheDesk\state\data.db`).
//! - Prepare-only by default; `--execute` runs local-SCID replay with prepared windows.
//! - Run dirs must be strictly beneath the isolated v1 campaign root.
//!
//! Example:
//!   cargo run --release --bin the-desk-nine-am-continuation -- ^
//!     --run-dir "T:\TheDesk\temp\backtests\nine-am-continuation-campaign-2026-07-22\v1\run-YYYYMMDD-HHMMSS" ^
//!     --execute

use std::path::PathBuf;
use std::process::Command;

use the_desk_backend::research::ib_campaign::RolloverVolumeSource;
use the_desk_backend::research::nine_am_continuation::{
    self, ExecuteV1Request, PrepareV1Request, V1_CAMPAIGN_ROOT, V1_ISOLATED_DB_FILENAME,
};

fn git_commit() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".into())
}

fn git_dirty() -> bool {
    Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .map(|o| !String::from_utf8_lossy(&o.stdout).trim().is_empty())
        .unwrap_or(true)
}

struct Args {
    run_dir: PathBuf,
    overwrite: bool,
    db_filename: String,
    execute: bool,
}

fn parse_args() -> Args {
    let mut run_dir: Option<PathBuf> = None;
    let mut overwrite = false;
    let mut execute = false;
    let mut db_filename = V1_ISOLATED_DB_FILENAME.to_string();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--run-dir" | "--artifact-dir" => {
                if let Some(v) = args.next() {
                    run_dir = Some(PathBuf::from(v));
                }
            }
            "--overwrite" => overwrite = true,
            "--execute" => execute = true,
            "--db-filename" => {
                if let Some(v) = args.next() {
                    db_filename = v;
                }
            }
            "--from" | "--db" | "--skip-seed" => {
                eprintln!(
                    "REFUSING legacy flag `{arg}`: IDEA-025D never opens a live DB and does not seed from one.\n\
                     Use --run-dir under {V1_CAMPAIGN_ROOT}, optional --db-filename, --overwrite, --execute."
                );
                std::process::exit(2);
            }
            "--help" | "-h" => {
                eprintln!(
                    "the-desk-nine-am-continuation — IDEA-025D Stage 2 harness

Required:
  --run-dir DIR     Run directory strictly beneath
                    {V1_CAMPAIGN_ROOT}

Optional:
  --overwrite       Delete exact prior run-owned artifacts before recreate
  --db-filename NAME Isolated DB basename (default: {V1_ISOLATED_DB_FILENAME})
  --execute         After prepare, run one-pass local SCID replay using
                    crossover-derived H6/M6/U6 windows (prepare-only default)

Rollover: production default is RolloverVolumeSource::ScanLocalScid
(H6→M6 and M6→U6 required; windows through 2026-07-21).

This binary does NOT open T:\\TheDesk\\state\\data.db or any live DB.
"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(2);
            }
        }
    }

    let Some(run_dir) = run_dir else {
        eprintln!(
            "missing required --run-dir (must be strictly beneath {V1_CAMPAIGN_ROOT}); no live-DB fallback"
        );
        std::process::exit(2);
    };

    Args {
        run_dir,
        overwrite,
        db_filename,
        execute,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    let campaign_root = PathBuf::from(V1_CAMPAIGN_ROOT);
    if !campaign_root.exists() {
        std::fs::create_dir_all(&campaign_root)?;
    }

    let commands = vec![format!(
        "the-desk-nine-am-continuation --run-dir {}{}{} --db-filename {}",
        args.run_dir.display(),
        if args.overwrite { " --overwrite" } else { "" },
        if args.execute { " --execute" } else { "" },
        args.db_filename
    )];

    let commit = git_commit();
    let dirty = git_dirty();
    eprintln!("git={commit} dirty={dirty}");
    eprintln!(
        "Preparing IDEA-025D Stage-2 run under {} (ScanLocalScid; no live DB)...",
        campaign_root.display()
    );

    let prepared = match nine_am_continuation::prepare_v1_campaign_run(PrepareV1Request {
        run_dir: &args.run_dir,
        campaign_root: &campaign_root,
        overwrite: args.overwrite,
        db_filename: &args.db_filename,
        git_commit: &commit,
        git_dirty: dirty,
        commands: commands.clone(),
        volume_source: RolloverVolumeSource::ScanLocalScid,
    }) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("BLOCKED: {e}");
            std::process::exit(1);
        }
    };

    eprintln!("canonical_run_dir={}", prepared.canonical_run_dir);
    eprintln!("canonical_db_path={}", prepared.canonical_db_path);
    eprintln!(
        "study={} v{} stage={} volume_source={}",
        prepared.provenance.study_id,
        prepared.provenance.study_version,
        prepared.provenance.stage,
        prepared.rollover.volume_source
    );
    for w in &prepared.windows {
        eprintln!(
            "window {} {}..{} ({:?})",
            w.contract, w.start_date, w.end_date, w.role
        );
    }
    for t in &prepared.rollover.transitions {
        eprintln!(
            "rollover {} → {}: crossover={:?} front_last={:?} exclusions={}",
            t.from_contract,
            t.to_contract,
            t.crossover_date,
            t.front_last_included_date,
            t.transition_exclusions.len()
        );
    }

    if !args.execute {
        eprintln!(
            "Wrote provenance.json + rollover_evidence.json (prepare-only; pass --execute for full SCID replay)."
        );
        return Ok(());
    }

    eprintln!("--execute: one-pass local SCID replay on prepared crossover windows...");
    let run_dir = PathBuf::from(&prepared.canonical_run_dir);
    let db_path = PathBuf::from(&prepared.canonical_db_path);
    let report = match nine_am_continuation::execute_v1_campaign(ExecuteV1Request {
        run_dir: &run_dir,
        isolated_db_path: &db_path,
        windows: prepared.windows.clone(),
        rollover: prepared.rollover.clone(),
        git_commit: &commit,
        git_dirty: dirty,
        commands,
        campaign_root: &campaign_root,
    }) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("BLOCKED: {e}");
            std::process::exit(1);
        }
    };

    eprintln!(
        "\n=== VERDICT: {} / {} ===",
        report.verdict.result, report.verdict.disposition
    );
    eprintln!(
        "population={} usable={} excluded={} green_ny_n={} red_ny_n={} full_n={}",
        report.population.population_sessions,
        report.population.usable_sessions,
        report.population.excluded_sessions,
        report.green_ny_primary.n,
        report.red_ny_primary.n,
        report.population.full_session_denominator
    );
    eprintln!(
        "green_ny rate={:.4} [{:.4},{:.4}] tier={:?}",
        report.green_ny_primary.continuation_rate,
        report.green_ny_primary.wilson_ci95[0],
        report.green_ny_primary.wilson_ci95[1],
        report.green_ny_primary.reliability_tier
    );
    eprintln!(
        "red_ny rate={:.4} [{:.4},{:.4}] tier={:?}",
        report.red_ny_primary.continuation_rate,
        report.red_ny_primary.wilson_ci95[0],
        report.red_ny_primary.wilson_ci95[1],
        report.red_ny_primary.reliability_tier
    );
    eprintln!("artifacts under {}", run_dir.display());
    Ok(())
}
