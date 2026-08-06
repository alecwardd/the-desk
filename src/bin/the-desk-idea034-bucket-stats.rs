//! IDEA-034 Stage 1 bucket-stats campaign harness (offline research only).
//!
//! Safety:
//! - Never opens a live DB (no default/fallback to `T:\TheDesk\state\data.db`).
//! - Prepare-only by default; `--execute` runs local-SCID replay with prepared windows.
//! - Run dirs must be strictly beneath the isolated campaign root.
//!
//! Example:
//!   cargo run --release --bin the-desk-idea034-bucket-stats -- ^
//!     --run-dir "T:\TheDesk\temp\backtests\idea-034-bucket-stats\run-YYYYMMDDTHHMMSSZ" ^
//!     --execute

use std::path::PathBuf;
use std::process::Command;

use the_desk_backend::research::idea034_bucket_stats::{
    self, ExecuteRequest, PrepareRequest, CAMPAIGN_ROOT, ISOLATED_DB_FILENAME,
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
    let mut db_filename = ISOLATED_DB_FILENAME.to_string();

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
                    "REFUSING legacy flag `{arg}`: IDEA-034 never opens a live DB and does not seed from one.\n\
                     Use --run-dir under {CAMPAIGN_ROOT}, optional --db-filename, --overwrite, --execute."
                );
                std::process::exit(2);
            }
            "--help" | "-h" => {
                eprintln!(
                    "the-desk-idea034-bucket-stats — IDEA-034 Stage 1 bucket-stats harness

Required:
  --run-dir DIR     Run directory strictly beneath
                    {CAMPAIGN_ROOT}

Optional:
  --overwrite       Delete exact prior run-owned artifacts before recreate
  --db-filename NAME Isolated DB basename (default: {ISOLATED_DB_FILENAME})
  --execute         After prepare, run one-pass local SCID replay

This binary does NOT open T:\\TheDesk\\state\\data.db or any live DB.
It does NOT register hypotheses, run Stage 2, or emit trading signals.
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
            "missing required --run-dir (must be strictly beneath {CAMPAIGN_ROOT}); no live-DB fallback"
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
    let campaign_root = PathBuf::from(CAMPAIGN_ROOT);
    if !campaign_root.exists() {
        std::fs::create_dir_all(&campaign_root)?;
    }

    let commands = vec![format!(
        "the-desk-idea034-bucket-stats --run-dir {}{}{} --db-filename {}",
        args.run_dir.display(),
        if args.overwrite { " --overwrite" } else { "" },
        if args.execute { " --execute" } else { "" },
        args.db_filename
    )];

    let commit = git_commit();
    let dirty = git_dirty();
    eprintln!("git={commit} dirty={dirty}");
    eprintln!(
        "Preparing IDEA-034 Stage-1 bucket-stats under {} (ScanLocalScid; no live DB)...",
        campaign_root.display()
    );

    let prepared = match idea034_bucket_stats::prepare_campaign_run(PrepareRequest {
        run_dir: &args.run_dir,
        campaign_root: &campaign_root,
        overwrite: args.overwrite,
        db_filename: &args.db_filename,
        git_commit: &commit,
        git_dirty: dirty,
        commands: commands.clone(),
    }) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("BLOCKED: {e}");
            std::process::exit(1);
        }
    };

    eprintln!("canonical_run_dir={}", prepared.canonical_run_dir.display());
    eprintln!("canonical_db_path={}", prepared.canonical_db_path.display());
    eprintln!(
        "study={} v{} stage={}",
        prepared.provenance.study_id, prepared.provenance.study_version, prepared.provenance.stage
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

    eprintln!("--execute: one-pass local SCID bucket-stats replay...");
    let summary = match idea034_bucket_stats::execute_campaign(ExecuteRequest {
        run_dir: &prepared.canonical_run_dir,
        campaign_root: &campaign_root,
        isolated_db_path: &prepared.canonical_db_path,
        windows: &prepared.windows,
        rollover: &prepared.rollover,
        git_commit: &commit,
        git_dirty: dirty,
        commands,
    }) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("BLOCKED: {e}");
            std::process::exit(1);
        }
    };

    eprintln!(
        "DONE days_observed={} eligible_days={} bucket_rows={} eligible_bucket_rows={}",
        summary.trading_days_observed,
        summary.trading_days_eligible,
        summary.bucket_stat_rows,
        summary.eligible_bucket_rows
    );
    eprintln!(
        "coverage in_scope={} minN={} medianN={} N>=30={}",
        summary.coverage_summary.in_scope_buckets,
        summary.coverage_summary.min_eligible_n,
        summary.coverage_summary.median_eligible_n,
        summary.coverage_summary.buckets_n_ge_30
    );
    eprintln!("artifacts under {}", prepared.canonical_run_dir.display());
    Ok(())
}
