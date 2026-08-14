//! SIL-M3e three-path Episode Query benchmark (`the-desk-eq-bench`).
//!
//! Not an MCP tool. Not a tenth kernel operator. Generates synthetic 1 Hz
//! NQ+ES Journal Frames in a temp dir (never committed) and times:
//!
//! - Path A: `query_episodes` over SQLite `journal_frames` JSON blobs
//! - Path B: SQLite side table of promoted flagship columns
//! - Path C: DuckDB `read_json` of the M3d JSONL.zst hive (feature `duckdb-bench`)
//!
//! Example:
//!   cargo run --release --bin the-desk-eq-bench -- --rth-days 10 --iters 5
//!   cargo run --release --features duckdb-bench --bin the-desk-eq-bench -- --rth-days 10

use std::fs;
use std::path::PathBuf;
use std::process;
use std::time::Instant;

use the_desk_backend::db::Database;
use the_desk_backend::engine::ColdFrameStore;
use the_desk_backend::research::episode_query_bench::{
    generate_rth_dataset, rth_day_stepdown, run_harness, seed_golden_fixture, DuckDbVerdict,
    TWO_WEEK_RTH_DAYS,
};

struct Args {
    rth_days: usize,
    warmup: usize,
    iters: usize,
    out: Option<PathBuf>,
    correctness_only: bool,
    golden_only: bool,
}

fn parse_args() -> Args {
    let mut rth_days = TWO_WEEK_RTH_DAYS;
    let mut warmup = 1usize;
    let mut iters = 5usize;
    let mut out = None;
    let mut correctness_only = false;
    let mut golden_only = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--rth-days" => {
                if let Some(v) = args.next() {
                    rth_days = v.parse().unwrap_or(TWO_WEEK_RTH_DAYS);
                }
            }
            "--warmup" => {
                if let Some(v) = args.next() {
                    warmup = v.parse().unwrap_or(1);
                }
            }
            "--iters" => {
                if let Some(v) = args.next() {
                    iters = v.parse().unwrap_or(5).max(1);
                }
            }
            "--out" => {
                if let Some(v) = args.next() {
                    out = Some(PathBuf::from(v));
                }
            }
            "--correctness-only" => correctness_only = true,
            "--golden-only" => golden_only = true,
            "--help" | "-h" => {
                eprintln!(
                    "the-desk-eq-bench [--rth-days N] [--warmup N] [--iters N] [--out FILE]\n  \
                     [--correctness-only] [--golden-only]\n\n  \
                     Path C requires --features duckdb-bench. Default cargo test does not."
                );
                process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                process::exit(2);
            }
        }
    }
    Args {
        rth_days,
        warmup,
        iters,
        out,
        correctness_only,
        golden_only,
    }
}

fn temp_root() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "the-desk-eq-bench-{}-{}",
        std::process::id(),
        nanos
    ))
}

fn main() {
    let args = parse_args();
    let root = temp_root();
    if let Err(e) = fs::create_dir_all(&root) {
        eprintln!("failed to create temp dir {}: {e}", root.display());
        process::exit(1);
    }
    let db_path = root.join("bench.db");
    let hive_path = root.join("journal-frames");
    let db = match Database::open(db_path.to_string_lossy().as_ref()) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("open bench db: {e}");
            process::exit(1);
        }
    };
    let (dataset, hive) = if args.golden_only {
        let hive = ColdFrameStore::new(&hive_path);
        match seed_golden_fixture(&db, &hive) {
            Ok(ds) => (ds, hive),
            Err(e) => {
                eprintln!("golden fixture: {e}");
                process::exit(1);
            }
        }
    } else {
        let mut generated = None;
        for days in rth_day_stepdown(args.rth_days) {
            eprintln!(
                "generating {days} RTH day(s) of 1 Hz NQ+ES Journal Frames in {}",
                root.display()
            );
            let hive = ColdFrameStore::new(root.join(format!("journal-frames-{days}")));
            let t0 = Instant::now();
            match generate_rth_dataset(&db, &hive, days) {
                Ok(ds) => {
                    eprintln!(
                        "generated {} frames / {} planted matches in {:.1}s",
                        ds.frame_count,
                        ds.planted_matches,
                        t0.elapsed().as_secs_f64()
                    );
                    generated = Some((ds, hive));
                    break;
                }
                Err(e) => {
                    eprintln!("generation failed for {days} RTH days ({e}); stepping down");
                }
            }
        }
        match generated {
            Some(pair) => pair,
            None => {
                eprintln!("could not generate a synthetic dataset");
                process::exit(1);
            }
        }
    };

    let warmup = if args.correctness_only {
        0
    } else {
        args.warmup
    };
    let iters = if args.correctness_only { 1 } else { args.iters };
    match run_harness(&db, &hive, &dataset, warmup, iters) {
        Ok(report) => {
            let json = serde_json::to_string_pretty(&report).unwrap_or_else(|_| "{}".into());
            println!("{json}");
            if let Some(path) = args.out {
                if let Err(e) = fs::write(&path, &json) {
                    eprintln!("write {}: {e}", path.display());
                    process::exit(1);
                }
            }
            if !report.correctness.agreed {
                eprintln!(
                    "correctness: A/B/C did not agree: {:?}",
                    report.correctness.notes
                );
                process::exit(1);
            }
            match report.decision.verdict {
                DuckDbVerdict::Defer => {
                    eprintln!("decision: DEFER DuckDB — {}", report.decision.reason)
                }
                DuckDbVerdict::Adopt => {
                    eprintln!("decision: ADOPT DuckDB — {}", report.decision.reason)
                }
            }
            eprintln!(
                "temp dataset left at {} (not committed; delete when done)",
                root.display()
            );
        }
        Err(e) => {
            eprintln!("harness failed: {e}");
            process::exit(1);
        }
    }
}
