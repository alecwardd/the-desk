//! One-shot binary to run a historical backfill job.
//!
//! Loads .scid data into the database (session summaries, market events, signal outcomes).
//! Use this to populate the research database before weekend analysis.
//!
//! Run:
//!   cargo run --bin the-desk-backfill                    # All available data
//!   cargo run --bin the-desk-backfill -- --start 2025-03-03 --end 2025-03-06
//!   cargo run --bin the-desk-backfill -- --start 2025-03-03 --end 2025-03-06 --run-rules
//!
//! Config: ~/.the-desk/config.toml (sierra_data_dir, symbol)

use std::sync::atomic::AtomicBool;

use the_desk_backend::backfill::{self, BackfillJobParams, HistoricalJobType};
use the_desk_backend::db::Database;
use the_desk_backend::feed::{load_feed_config, scid_reader::ScidReader};

fn data_dir() -> std::path::PathBuf {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    let dir = std::path::PathBuf::from(home).join(".the-desk");
    std::fs::create_dir_all(&dir).ok();
    dir
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mut start_date: Option<String> = None;
    let mut end_date: Option<String> = None;
    let mut force = false;
    let mut run_rules = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--start" | "-s" => {
                start_date = args.next();
            }
            "--end" | "-e" => {
                end_date = args.next();
            }
            "--force" | "-f" => {
                force = true;
            }
            "--run-rules" | "-r" => {
                run_rules = true;
            }
            "--help" | "-h" => {
                eprintln!(
                    r#"the-desk-backfill — Load historical .scid data into the database

Usage:
  the-desk-backfill [OPTIONS]

Options:
  --start, -s DATE    Start date (YYYY-MM-DD). Omit for all available.
  --end, -e DATE      End date (YYYY-MM-DD). Omit for through today.
  --force, -f         Reprocess sessions even if summaries exist.
  --run-rules, -r     Run rules engine to populate signal outcomes (backtest).
  --help, -h          Show this help.

Examples:
  # Load this week (Mon Mar 3 - Fri Mar 6, 2025)
  the-desk-backfill --start 2025-03-03 --end 2025-03-06

  # Load all available data with rules evaluation
  the-desk-backfill --run-rules

  # Force reprocess this week
  the-desk-backfill --start 2025-03-03 --end 2025-03-06 --force

Config: ~/.the-desk/config.toml (sierra_data_dir, symbol)
"#
                );
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown argument: {arg}. Use --help for usage.");
                std::process::exit(1);
            }
        }
    }

    let config = load_feed_config();
    let reader = ScidReader::from_feed_config(&config);

    if !reader.path().exists() {
        eprintln!("SCID file not found: {}", reader.path().display());
        eprintln!("Ensure Sierra Chart data path is configured in ~/.the-desk/config.toml");
        eprintln!("Default: sierra_data_dir = \"C:\\\\SierraChart\\\\Data\", symbol = \"NQ\"");
        std::process::exit(1);
    }

    let db_path = data_dir().join("data.db");
    let db = Database::open(&db_path.to_string_lossy())?;

    let job_id = uuid::Uuid::new_v4().to_string();
    let cancel_flag = AtomicBool::new(false);

    let params = BackfillJobParams {
        job_id: job_id.clone(),
        job_type: HistoricalJobType::ResearchBackfill,
        start_date: start_date.clone(),
        end_date: end_date.clone(),
        force,
        run_rules,
        setup_ids: None,
    };

    eprintln!("Starting backfill from {}", reader.path().display());
    if let (Some(s), Some(e)) = (&start_date, &end_date) {
        eprintln!("  Date range: {} to {}", s, e);
    } else {
        eprintln!("  Date range: all available");
    }
    eprintln!("  Force: {}, Run rules: {}", force, run_rules);
    eprintln!();

    let result = backfill::run_backfill_job(
        &reader,
        &db,
        &params,
        |progress| {
            let pct = if progress.estimated_records > 0 {
                (progress.records_scanned as f64 / progress.estimated_records as f64 * 100.0)
                    .min(100.0)
            } else {
                0.0
            };
            eprint!(
                "\r  {} | {}/{} records ({:.1}%) | {} sessions | {}",
                progress.current_phase,
                progress.records_scanned,
                progress.estimated_records,
                pct,
                progress.sessions_completed,
                progress.current_session_date.as_deref().unwrap_or("—")
            );
        },
        &cancel_flag,
    );

    eprintln!();

    match result {
        Ok(r) => {
            println!("Backfill complete.");
            println!("  Sessions processed: {}", r.sessions_processed);
            println!("  Sessions skipped:   {}", r.sessions_skipped);
            println!("  Total ticks:        {}", r.total_ticks);
            println!("  Total events:       {}", r.total_events);
            println!("  Signals fired:      {}", r.signals_fired);
            if !r.warnings.is_empty() {
                println!("  Warnings:");
                for w in &r.warnings {
                    println!("    - {}", w);
                }
            }
        }
        Err(backfill::BackfillJobError::Cancelled) => {
            eprintln!("Backfill cancelled.");
            std::process::exit(130);
        }
        Err(e) => {
            eprintln!("Backfill failed: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}
