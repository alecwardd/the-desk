//! Isolated IDEA-011 + IDEA-025B IB campaign harness (V2).
//!
//! Safety:
//! - Never opens a live DB (no default/fallback to `T:\TheDesk\state\data.db`).
//! - Tick research / rollover volumes come from local SCID.
//! - Isolated campaign DB is created empty, strictly beneath the V2 root.
//! - Existing run artifacts are refused unless `--overwrite` is explicit.
//! - Default is prepare-only; `--execute` runs the full study replay.
//!
//! Example (prepare + execute):
//!   cargo run --release --bin the-desk-ib-campaign -- ^
//!     --run-dir T:\TheDesk\temp\backtests\ib-campaign-2026-07-22\v2\run-001 ^
//!     --execute

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use the_desk_backend::research::ib_campaign::{
    self, DailyOverlapVolume, ExecuteV2Request, RolloverVolumeSource, V1_CAMPAIGN_ARTIFACT_DIR,
    V2_CAMPAIGN_ROOT, V2_ISOLATED_DB_FILENAME,
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
    /// Test/override fixture only — production default is local SCID overlap scan.
    rollover_volumes_json: Option<PathBuf>,
    v1_artifact_dir: PathBuf,
}

fn parse_args() -> Args {
    let mut run_dir: Option<PathBuf> = None;
    let mut overwrite = false;
    let mut execute = false;
    let mut db_filename = V2_ISOLATED_DB_FILENAME.to_string();
    let mut rollover_volumes_json: Option<PathBuf> = None;
    let mut v1_artifact_dir = PathBuf::from(V1_CAMPAIGN_ARTIFACT_DIR);

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
            "--rollover-volumes-json" => {
                if let Some(v) = args.next() {
                    rollover_volumes_json = Some(PathBuf::from(v));
                }
            }
            "--v1-artifact-dir" => {
                if let Some(v) = args.next() {
                    v1_artifact_dir = PathBuf::from(v);
                }
            }
            "--from" | "--db" | "--skip-seed" => {
                eprintln!(
                    "REFUSING legacy flag `{arg}`: V2 never opens a live DB and does not seed from one.\n\
                     Use --run-dir under {V2_CAMPAIGN_ROOT}, optional --db-filename, --overwrite, --execute."
                );
                std::process::exit(2);
            }
            "--help" | "-h" => {
                eprintln!(
                    "the-desk-ib-campaign — IDEA-011 + IDEA-025B V2 offline IB study

Required:
  --run-dir DIR              Run directory strictly beneath
                             {V2_CAMPAIGN_ROOT}

Optional:
  --execute                  After prepare, run full SCID study replay using the
                             prepared crossover-derived windows (default: prepare-only)
  --overwrite                Delete exact prior run-owned artifacts before recreate
  --db-filename NAME         Isolated DB basename under run-dir (default: {V2_ISOLATED_DB_FILENAME})
  --rollover-volumes-json P  TEST/OVERRIDE ONLY. Production default scans local SCID.
  --v1-artifact-dir DIR      Preserved v1 JSON parent for reconciliation
                             (default: {V1_CAMPAIGN_ARTIFACT_DIR})
  --artifact-dir DIR         Alias of --run-dir

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
            "missing required --run-dir (must be strictly beneath {V2_CAMPAIGN_ROOT}); no live-DB fallback"
        );
        std::process::exit(2);
    };

    Args {
        run_dir,
        overwrite,
        db_filename,
        execute,
        rollover_volumes_json,
        v1_artifact_dir,
    }
}

fn load_rollover_fixture(
    path: &Path,
) -> Result<BTreeMap<(String, String), Vec<DailyOverlapVolume>>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let raw: BTreeMap<String, Vec<DailyOverlapVolume>> =
        serde_json::from_str(&text).map_err(|e| e.to_string())?;
    let mut out = BTreeMap::new();
    for (k, rows) in raw {
        let mut parts = k.split('|');
        let front = parts
            .next()
            .ok_or_else(|| format!("bad volumes key: {k}"))?
            .to_string();
        let back = parts
            .next()
            .ok_or_else(|| format!("bad volumes key (need front|back): {k}"))?
            .to_string();
        out.insert((front, back), rows);
    }
    Ok(out)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    let v2_root = PathBuf::from(V2_CAMPAIGN_ROOT);

    let mut commands = vec![format!(
        "the-desk-ib-campaign --run-dir {}{}{} --db-filename {}",
        args.run_dir.display(),
        if args.overwrite { " --overwrite" } else { "" },
        if args.execute { " --execute" } else { "" },
        args.db_filename
    )];

    let fixture;
    let volume_source = if let Some(ref path) = args.rollover_volumes_json {
        commands.push(format!("--rollover-volumes-json {}", path.display()));
        eprintln!(
            "WARNING: using --rollover-volumes-json fixture override (not production default)"
        );
        fixture = load_rollover_fixture(path)?;
        RolloverVolumeSource::Fixture(&fixture)
    } else {
        eprintln!("Rollover volume source: local SCID bounded overlap scan (production default)");
        RolloverVolumeSource::ScanLocalScid
    };

    let commit = git_commit();
    let dirty = git_dirty();
    eprintln!("git={commit} dirty={dirty}");
    eprintln!(
        "Preparing V2 run under {} (no live DB)...",
        v2_root.display()
    );

    let prepared = match ib_campaign::prepare_v2_campaign_run(ib_campaign::PrepareV2Request {
        run_dir: &args.run_dir,
        v2_root: &v2_root,
        overwrite: args.overwrite,
        db_filename: &args.db_filename,
        git_commit: &commit,
        git_dirty: dirty,
        commands: commands.clone(),
        volume_source,
    }) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("BLOCKED: {e}");
            std::process::exit(1);
        }
    };

    eprintln!("canonical_run_dir={}", prepared.canonical_run_dir);
    eprintln!("canonical_db_path={}", prepared.canonical_db_path);
    eprintln!("study_version={}", prepared.provenance.study_version);
    eprintln!("volume_source={}", prepared.rollover.volume_source);
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
            "Wrote provenance.json + rollover_evidence.json (prepare-only; pass --execute for full replay)."
        );
        return Ok(());
    }

    eprintln!(
        "--execute: replaying prepared crossover-derived windows (no legacy run_campaign)..."
    );
    let run_dir = PathBuf::from(&prepared.canonical_run_dir);
    let db_path = PathBuf::from(&prepared.canonical_db_path);
    let report = match ib_campaign::execute_v2_campaign(ExecuteV2Request {
        run_dir: &run_dir,
        isolated_db_path: &db_path,
        windows: prepared.windows.clone(),
        rollover: prepared.rollover.clone(),
        git_commit: &commit,
        git_dirty: dirty,
        commands,
        v1_artifact_dir: &args.v1_artifact_dir,
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
    eprintln!("{}", report.verdict.rationale);
    eprintln!(
        "sessions population={} usable={} trades={} matched_primary={}",
        report.descriptive_all.population_sessions,
        report.descriptive_all.usable_sessions,
        report
            .tradable
            .get("all")
            .map(|m| m.values().map(|s| s.n).sum::<usize>())
            .unwrap_or(0),
        report.primary_contrast.matched_n
    );
    eprintln!("Artifacts: {}", run_dir.display());
    Ok(())
}
