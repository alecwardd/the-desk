//! Storage maintenance for The Desk's local SQLite database.
//!
//! Use outside market hours:
//!   cargo run --bin the-desk-storage -- --status
//!   cargo run --bin the-desk-storage -- --maintain --vacuum
//!   cargo run --bin the-desk-storage -- --compact-into X:\TheDesk\state\data_compacted.db

use chrono::{Datelike, Days, Local, NaiveDate};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use the_desk_backend::feed::load_storage_config;

#[derive(Debug)]
struct Args {
    status: bool,
    archive: bool,
    vacuum: bool,
    compact_into: Option<String>,
    verify_db: Option<String>,
    compare_db: Option<String>,
    cutoff: Option<String>,
    retention_days: Option<u64>,
    prune_depth: bool,
    depth_cutoff: Option<String>,
    depth_retention_days: Option<u64>,
}

#[derive(Debug)]
struct ArchiveRange {
    month: String,
    start_date: String,
    end_date_exclusive: String,
    row_count: i64,
}

fn data_dir() -> PathBuf {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    let dir = PathBuf::from(home).join(".the-desk");
    fs::create_dir_all(&dir).ok();
    dir
}

fn usage() -> &'static str {
    r#"the-desk-storage — Manage local SQLite warm/cold storage

Usage:
  the-desk-storage --status
  the-desk-storage --archive [--cutoff YYYY-MM-DD]
  the-desk-storage --maintain [--cutoff YYYY-MM-DD] [--vacuum]
  the-desk-storage --prune-depth [--depth-cutoff YYYY-MM-DD]
  the-desk-storage --compact-into PATH
  the-desk-storage --verify-db PATH [--compare-db PATH] [--cutoff YYYY-MM-DD]

Options:
  --status                  Print raw tick coverage and storage settings.
  --archive                 Archive raw_ticks older than the cutoff (also prunes depth_events).
  --maintain                Archive raw_ticks + prune depth_events. Use with --vacuum to reclaim disk.
  --vacuum                  Compact SQLite after archiving. Requires free disk space near current DB size.
  --prune-depth             Delete depth_events older than the depth cutoff (the .depth files remain
                            the durable source, so pruned rows are re-ingestable). Chunked + WAL-bounded.
  --depth-cutoff DATE       Prune depth_events with trading_day < DATE. Default: today - depth_retention_days.
  --depth-retention-days N  Override configured depth retention days when deriving the depth cutoff.
  --compact-into PATH       Checkpoint WAL, VACUUM INTO PATH, then verify destination integrity.
                            Refuses to overwrite an existing destination file.
  --verify-db PATH          Verify a database copy before a reclaim swap.
  --compare-db PATH         With --verify-db, compare key table row counts against this source DB.
  --cutoff DATE             Archive rows with session_date < DATE. Default: today - warm_retention_days.
  --retention-days N        Override configured warm retention days when deriving cutoff.
  --help, -h                Show this help.

Config: ~/.the-desk/config.toml [storage]
"#
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut parsed = Args {
        status: false,
        archive: false,
        vacuum: false,
        compact_into: None,
        verify_db: None,
        compare_db: None,
        cutoff: None,
        retention_days: None,
        prune_depth: false,
        depth_cutoff: None,
        depth_retention_days: None,
    };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--status" => parsed.status = true,
            "--archive" => parsed.archive = true,
            "--maintain" => parsed.archive = true,
            "--vacuum" => parsed.vacuum = true,
            "--compact-into" => parsed.compact_into = args.next(),
            "--verify-db" => parsed.verify_db = args.next(),
            "--compare-db" => parsed.compare_db = args.next(),
            "--cutoff" => parsed.cutoff = args.next(),
            "--retention-days" => {
                parsed.retention_days = args.next().and_then(|s| s.parse::<u64>().ok());
            }
            "--prune-depth" => parsed.prune_depth = true,
            "--depth-cutoff" => parsed.depth_cutoff = args.next(),
            "--depth-retention-days" => {
                parsed.depth_retention_days = args.next().and_then(|s| s.parse::<u64>().ok());
            }
            "--help" | "-h" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown argument: {arg}");
                eprintln!("{}", usage());
                std::process::exit(1);
            }
        }
    }

    if !parsed.status
        && !parsed.archive
        && !parsed.vacuum
        && !parsed.prune_depth
        && parsed.compact_into.is_none()
        && parsed.verify_db.is_none()
    {
        parsed.status = true;
    }

    parsed
}

fn derive_cutoff(args: &Args) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(cutoff) = &args.cutoff {
        NaiveDate::parse_from_str(cutoff, "%Y-%m-%d")?;
        return Ok(cutoff.clone());
    }

    let storage = load_storage_config();
    let retention_days = args
        .retention_days
        .unwrap_or(u64::from(storage.warm_retention_days));
    let today = Local::now().date_naive();
    let cutoff = today
        .checked_sub_days(Days::new(retention_days))
        .ok_or("retention cutoff underflow")?;
    Ok(cutoff.format("%Y-%m-%d").to_string())
}

/// Derive the `depth_events` prune cutoff (`trading_day < cutoff`). Honors an explicit
/// `--depth-cutoff`, else `today - depth_retention_days`.
fn derive_depth_cutoff(args: &Args) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(cutoff) = &args.depth_cutoff {
        NaiveDate::parse_from_str(cutoff, "%Y-%m-%d")?;
        return Ok(cutoff.clone());
    }

    let storage = load_storage_config();
    let days = args
        .depth_retention_days
        .unwrap_or(u64::from(storage.depth_retention_days));
    let today = Local::now().date_naive();
    let cutoff = today
        .checked_sub_days(Days::new(days))
        .ok_or("depth retention cutoff underflow")?;
    Ok(cutoff.format("%Y-%m-%d").to_string())
}

/// Delete `depth_events` older than `cutoff` in bounded chunks.
///
/// `depth_events` can hold billions of rows, so we delete in batches and checkpoint
/// the WAL periodically — a single giant `DELETE` would build a WAL larger than the
/// (often near-full) data drive. The `.depth` source files remain the durable record,
/// so pruned rows are re-ingestable. Rows with a NULL `trading_day` are left in place
/// (they cannot be dated). Reclaim the freed pages afterward with `--compact-into`.
fn prune_depth_events(conn: &Connection, cutoff: &str) -> Result<i64, Box<dyn std::error::Error>> {
    if !table_exists(conn, "depth_events")? {
        println!("No depth_events table; nothing to prune.");
        return Ok(0);
    }

    println!("Pruning depth_events with trading_day < {cutoff} in chunks (.depth files remain the source)...");
    const BATCH: i64 = 200_000;
    let started = Instant::now();
    let mut deleted_total: i64 = 0;
    let mut batches: u64 = 0;
    loop {
        let n = conn.execute(
            "DELETE FROM depth_events
             WHERE rowid IN (
                 SELECT rowid FROM depth_events WHERE trading_day < ?1 LIMIT ?2
             )",
            params![cutoff, BATCH],
        )? as i64;
        deleted_total += n;
        batches += 1;
        // Bound the WAL on a near-full drive: truncate-checkpoint every ~2M rows.
        if batches.is_multiple_of(10) {
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
            println!(
                "  pruned {deleted_total} depth rows so far in {:.0?}...",
                started.elapsed()
            );
        }
        if n < BATCH {
            break;
        }
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    println!(
        "Pruned {deleted_total} depth_events rows in {:.1?}. Run --compact-into to reclaim the freed pages.",
        started.elapsed()
    );
    Ok(deleted_total)
}

/// Report DOM `depth_events` coverage. Uses index endpoints and `MAX(rowid)` only —
/// never a full `COUNT(*)`, which would scan billions of rows.
fn print_depth_status(
    conn: &Connection,
    depth_cutoff: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if !table_exists(conn, "depth_events")? {
        return Ok(());
    }
    // SQLite only index-optimizes ONE MIN/MAX aggregate per query; selecting both in
    // a single statement forces a full scan of billions of rows. Query them separately
    // so each resolves via an index endpoint.
    let lo: Option<String> =
        conn.query_row("SELECT MIN(trading_day) FROM depth_events", [], |r| {
            r.get(0)
        })?;
    let hi: Option<String> =
        conn.query_row("SELECT MAX(trading_day) FROM depth_events", [], |r| {
            r.get(0)
        })?;
    let approx_rows: i64 = conn
        .query_row("SELECT MAX(rowid) FROM depth_events", [], |r| {
            r.get::<_, Option<i64>>(0)
        })?
        .unwrap_or(0);
    println!(
        "Depth events: {} through {}, approx_rows(max rowid)={approx_rows}",
        lo.as_deref().unwrap_or("none"),
        hi.as_deref().unwrap_or("none"),
    );
    println!("Depth prune cutoff: trading_day < {depth_cutoff}");
    Ok(())
}

fn sqlite_temp_dir() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let storage = load_storage_config();
    let archive_dir = PathBuf::from(storage.cold_archive_dir);
    let base_dir = archive_dir
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(data_dir);
    let temp_dir = base_dir.join("temp");
    fs::create_dir_all(&temp_dir)?;

    // SQLite consults the process temp environment when it needs large temp files.
    // Keep those files on the trading/data drive rather than filling C: during VACUUM.
    std::env::set_var("TMP", &temp_dir);
    std::env::set_var("TEMP", &temp_dir);
    std::env::set_var("SQLITE_TMPDIR", &temp_dir);
    Ok(temp_dir)
}

fn open_db(path: &Path, temp_dir: &Path) -> Result<Connection, Box<dyn std::error::Error>> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(std::time::Duration::from_secs(30))?;
    let temp_sql = temp_dir.to_string_lossy().replace('\'', "''");
    conn.execute_batch(&format!(
        "PRAGMA journal_mode=WAL;
         PRAGMA foreign_keys=ON;
         PRAGMA temp_store=FILE;
         PRAGMA temp_store_directory='{temp_sql}';"
    ))?;
    Ok(conn)
}

fn db_size_gb(path: &Path) -> f64 {
    fs::metadata(path)
        .map(|m| m.len() as f64 / 1024.0 / 1024.0 / 1024.0)
        .unwrap_or(0.0)
}

fn print_status(
    conn: &Connection,
    db_path: &Path,
    cutoff: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = load_storage_config();
    let raw: (Option<String>, Option<String>, i64) = conn.query_row(
        "SELECT MIN(session_date), MAX(session_date), COUNT(1) FROM raw_ticks",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    let archive_count: i64 = conn.query_row(
        "SELECT COUNT(1) FROM raw_ticks WHERE session_date < ?1",
        params![cutoff],
        |r| r.get(0),
    )?;
    let keep_count: i64 = conn.query_row(
        "SELECT COUNT(1) FROM raw_ticks WHERE session_date >= ?1",
        params![cutoff],
        |r| r.get(0),
    )?;
    let sessions: (Option<String>, Option<String>, i64) = conn.query_row(
        "SELECT MIN(session_date), MAX(session_date), COUNT(1) FROM session_summaries",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    let page_count: i64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0))?;
    let freelist_count: i64 = conn.query_row("PRAGMA freelist_count", [], |r| r.get(0))?;
    let page_size: i64 = conn.query_row("PRAGMA page_size", [], |r| r.get(0))?;
    let freelist_size_gb = (freelist_count as f64 * page_size as f64) / 1024.0 / 1024.0 / 1024.0;

    println!("Database: {}", db_path.display());
    println!("Database size: {:.2} GB", db_size_gb(db_path));
    println!(
        "Raw ticks: {} through {}, rows={}",
        raw.0.as_deref().unwrap_or("none"),
        raw.1.as_deref().unwrap_or("none"),
        raw.2
    );
    println!("Archive cutoff: session_date < {cutoff}");
    println!("Rows to archive: {archive_count}");
    println!("Rows to keep warm: {keep_count}");
    println!(
        "Session summaries: {} through {}, rows={}",
        sessions.0.as_deref().unwrap_or("none"),
        sessions.1.as_deref().unwrap_or("none"),
        sessions.2
    );
    println!(
        "SQLite pages: total={page_count}, freelist={freelist_count}, page_size={page_size}, freelist_size={freelist_size_gb:.2} GB"
    );
    println!("Cold archive dir: {}", storage.cold_archive_dir);
    println!("Auto archive flag: {}", storage.auto_archive);
    Ok(())
}

fn month_after(month: &str) -> Result<String, Box<dyn std::error::Error>> {
    let date = NaiveDate::parse_from_str(&format!("{month}-01"), "%Y-%m-%d")?;
    let next = if date.month() == 12 {
        NaiveDate::from_ymd_opt(date.year() + 1, 1, 1).ok_or("invalid next year")?
    } else {
        NaiveDate::from_ymd_opt(date.year(), date.month() + 1, 1).ok_or("invalid next month")?
    };
    Ok(next.format("%Y-%m-%d").to_string())
}

fn archive_ranges(
    conn: &Connection,
    cutoff: &str,
) -> Result<Vec<ArchiveRange>, Box<dyn std::error::Error>> {
    let mut stmt = conn.prepare(
        "SELECT substr(session_date, 1, 7) AS month,
                MIN(session_date),
                COUNT(1)
         FROM raw_ticks
         WHERE session_date < ?1
         GROUP BY month
         ORDER BY month",
    )?;
    let rows = stmt.query_map(params![cutoff], |row| {
        let month: String = row.get(0)?;
        let start_date: String = row.get(1)?;
        let row_count: i64 = row.get(2)?;
        Ok((month, start_date, row_count))
    })?;

    let mut ranges = Vec::new();
    for row in rows {
        let (month, start_date, row_count) = row?;
        let next_month = month_after(&month)?;
        let end_date_exclusive = std::cmp::min(next_month, cutoff.to_string());
        ranges.push(ArchiveRange {
            month,
            start_date,
            end_date_exclusive,
            row_count,
        });
    }
    Ok(ranges)
}

fn archive_range(
    conn: &Connection,
    archive_dir: &Path,
    range: &ArchiveRange,
) -> Result<i64, Box<dyn std::error::Error>> {
    fs::create_dir_all(archive_dir)?;
    let final_path = archive_dir.join(format!(
        "raw_ticks_{}_{}_to_{}.csv.zst",
        range.month, range.start_date, range.end_date_exclusive
    ));
    if final_path.exists() {
        return Err(format!(
            "archive file already exists, refusing to overwrite: {}",
            final_path.display()
        )
        .into());
    }

    let temp_path = final_path.with_extension("csv.zst.tmp");
    if temp_path.exists() {
        fs::remove_file(&temp_path)?;
    }

    let file = File::create(&temp_path)?;
    let writer = BufWriter::new(file);
    let mut encoder = zstd::stream::write::Encoder::new(writer, 3)?;
    writeln!(
        encoder,
        "timestamp_ms,price,volume,bid,ask,is_buy,session_date,root_symbol,contract_symbol"
    )?;

    let mut stmt = conn.prepare(
        "SELECT timestamp_ms, price, volume, bid, ask, is_buy, session_date,
                COALESCE(root_symbol, ''), COALESCE(contract_symbol, '')
         FROM raw_ticks
         WHERE session_date >= ?1 AND session_date < ?2
         ORDER BY timestamp_ms",
    )?;
    let mut rows = stmt.query(params![range.start_date, range.end_date_exclusive])?;
    let mut written = 0_i64;
    while let Some(row) = rows.next()? {
        let timestamp_ms: f64 = row.get(0)?;
        let price: f64 = row.get(1)?;
        let volume: f64 = row.get(2)?;
        let bid: f64 = row.get(3)?;
        let ask: f64 = row.get(4)?;
        let is_buy: i64 = row.get(5)?;
        let session_date: String = row.get(6)?;
        let root_symbol: String = row.get(7)?;
        let contract_symbol: String = row.get(8)?;
        writeln!(
            encoder,
            "{timestamp_ms},{price},{volume},{bid},{ask},{is_buy},{session_date},{root_symbol},{contract_symbol}"
        )?;
        written += 1;
    }
    encoder.finish()?;

    if written != range.row_count {
        fs::remove_file(&temp_path).ok();
        return Err(format!(
            "archive row count mismatch for {}: expected {}, wrote {}",
            range.month, range.row_count, written
        )
        .into());
    }

    fs::rename(&temp_path, &final_path)?;
    let deleted = conn.execute(
        "DELETE FROM raw_ticks WHERE session_date >= ?1 AND session_date < ?2",
        params![range.start_date, range.end_date_exclusive],
    )?;
    Ok(deleted as i64)
}

fn archive_old_ticks(
    conn: &mut Connection,
    cutoff: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let storage = load_storage_config();
    let archive_dir = PathBuf::from(storage.cold_archive_dir);
    let ranges = archive_ranges(conn, cutoff)?;
    if ranges.is_empty() {
        println!("No raw_ticks rows older than {cutoff}; nothing to archive.");
        return Ok(0);
    }

    let total: i64 = ranges.iter().map(|r| r.row_count).sum();
    println!(
        "Archiving {total} raw tick rows across {} monthly range(s) to {}",
        ranges.len(),
        archive_dir.display()
    );

    let tx = conn.transaction()?;
    let mut deleted_total = 0_i64;
    for range in &ranges {
        let started = Instant::now();
        println!(
            "  {}: {} to {} ({} rows)",
            range.month, range.start_date, range.end_date_exclusive, range.row_count
        );
        let deleted = archive_range(&tx, &archive_dir, range)?;
        deleted_total += deleted;
        println!(
            "    archived and deleted {deleted} rows in {:.1?}",
            started.elapsed()
        );
    }
    tx.commit()?;

    println!("Archived and deleted {deleted_total} rows.");
    Ok(deleted_total)
}

fn run_vacuum(conn: &Connection, db_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    println!("Checkpointing WAL before VACUUM...");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    println!("Running VACUUM. This can take several minutes for a large database...");
    conn.execute_batch("VACUUM;")?;
    conn.execute_batch("PRAGMA optimize; PRAGMA wal_checkpoint(TRUNCATE);")?;
    println!(
        "VACUUM complete in {:.1?}. Database size is now {:.2} GB.",
        started.elapsed(),
        db_size_gb(db_path)
    );
    Ok(())
}

fn verify_sqlite_integrity(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let integrity: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    if integrity != "ok" {
        return Err(format!("integrity_check failed for {}: {integrity}", path.display()).into());
    }
    Ok(())
}

fn open_readonly_db(path: &Path) -> Result<Connection, Box<dyn std::error::Error>> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(std::time::Duration::from_secs(30))?;
    Ok(conn)
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(1) FROM sqlite_schema WHERE type='table' AND name=?1",
        params![table],
        |r| r.get(0),
    )?;
    Ok(count == 1)
}

fn table_row_count(conn: &Connection, table: &str) -> Result<i64, Box<dyn std::error::Error>> {
    if !table
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(format!("unsafe table name: {table}").into());
    }
    let sql = format!("SELECT COUNT(1) FROM {table}");
    Ok(conn.query_row(&sql, [], |r| r.get(0))?)
}

fn verify_reclaim_copy(
    copy_path: &Path,
    compare_path: Option<&Path>,
    cutoff: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Verifying database copy: {}", copy_path.display());
    verify_sqlite_integrity(copy_path)?;
    let copy = open_readonly_db(copy_path)?;

    const REQUIRED_TABLES: &[&str] = &[
        "raw_ticks",
        "session_summaries",
        "market_events",
        "signal_outcomes",
        "historical_job_runs",
        "research_hypotheses",
        "setups",
        "playbook_signals",
        "journal_entries",
        "risk_state",
        "account_state",
    ];
    for table in REQUIRED_TABLES {
        if !table_exists(&copy, table)? {
            return Err(format!("required table missing from copy: {table}").into());
        }
    }

    let session_summaries = table_row_count(&copy, "session_summaries")?;
    if session_summaries <= 0 {
        return Err("session_summaries is empty in database copy".into());
    }

    let old_raw_ticks: i64 = copy.query_row(
        "SELECT COUNT(1) FROM raw_ticks WHERE session_date < ?1",
        params![cutoff],
        |r| r.get(0),
    )?;
    if old_raw_ticks != 0 {
        return Err(format!(
            "database copy still contains {old_raw_ticks} raw_ticks rows older than {cutoff}"
        )
        .into());
    }

    if let Some(compare_path) = compare_path {
        let source = open_readonly_db(compare_path)?;
        for table in REQUIRED_TABLES {
            let source_count = table_row_count(&source, table)?;
            let copy_count = table_row_count(&copy, table)?;
            if source_count != copy_count {
                return Err(format!(
                    "row-count mismatch for {table}: source={source_count}, copy={copy_count}"
                )
                .into());
            }
        }
    }

    println!(
        "Database copy verified: integrity ok, session_summaries={session_summaries}, cutoff={cutoff}."
    );
    Ok(())
}

fn run_compact_into(
    conn: &Connection,
    db_path: &Path,
    dest_path: &Path,
    cutoff: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();

    if dest_path.exists() {
        return Err(format!(
            "compact destination already exists, refusing to overwrite: {}",
            dest_path.display()
        )
        .into());
    }

    let explicit_parent = dest_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    if let Some(parent) = explicit_parent {
        fs::create_dir_all(parent)?;
    }

    let source = fs::canonicalize(db_path)?;
    let dest_parent = if let Some(parent) = explicit_parent {
        fs::canonicalize(parent)?
    } else {
        std::env::current_dir()?
    };
    let dest_name = dest_path
        .file_name()
        .ok_or("compact destination must include a file name")?;
    let canonical_intent = dest_parent.join(dest_name);
    if source == canonical_intent {
        return Err("compact destination must be different from the source database".into());
    }

    println!("Checkpointing WAL before VACUUM INTO...");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;

    let dest = dest_path.to_string_lossy().to_string();
    println!("Running VACUUM INTO {}...", dest_path.display());
    conn.execute("VACUUM INTO ?1", params![dest])?;

    verify_reclaim_copy(dest_path, Some(db_path), cutoff)?;
    println!(
        "VACUUM INTO complete in {:.1?}. Destination size is {:.2} GB.",
        started.elapsed(),
        db_size_gb(dest_path)
    );
    Ok(())
}

fn ensure_no_existing_archive_for_cutoff(
    archive_dir: &Path,
    conn: &Connection,
    cutoff: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for range in archive_ranges(conn, cutoff)? {
        let final_path = archive_dir.join(format!(
            "raw_ticks_{}_{}_to_{}.csv.zst",
            range.month, range.start_date, range.end_date_exclusive
        ));
        if final_path.exists() {
            let archived_rows: Option<i64> = conn
                .query_row(
                    "SELECT COUNT(1) FROM raw_ticks WHERE session_date >= ?1 AND session_date < ?2",
                    params![range.start_date, range.end_date_exclusive],
                    |r| r.get(0),
                )
                .optional()?;
            return Err(format!(
                "archive target already exists while {} matching rows remain in SQLite: {}",
                archived_rows.unwrap_or(0),
                final_path.display()
            )
            .into());
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    let cutoff = derive_cutoff(&args)?;
    let depth_cutoff = derive_depth_cutoff(&args)?;
    let db_path = data_dir().join("data.db");

    if let Some(path) = &args.verify_db {
        let compare_path = args.compare_db.as_deref().map(Path::new);
        verify_reclaim_copy(Path::new(path), compare_path, &cutoff)?;
        return Ok(());
    }

    let temp_dir = sqlite_temp_dir()?;
    println!("SQLite temp dir: {}", temp_dir.display());
    let mut conn = open_db(&db_path, &temp_dir)?;

    if args.status {
        print_status(&conn, &db_path, &cutoff)?;
        print_depth_status(&conn, &depth_cutoff)?;
        if !args.archive && !args.vacuum && !args.prune_depth {
            return Ok(());
        }
    }

    if args.archive {
        let storage = load_storage_config();
        ensure_no_existing_archive_for_cutoff(
            Path::new(&storage.cold_archive_dir),
            &conn,
            &cutoff,
        )?;
        archive_old_ticks(&mut conn, &cutoff)?;
    }

    // --maintain/--archive and --prune-depth both prune DOM depth to its retention window.
    if args.archive || args.prune_depth {
        prune_depth_events(&conn, &depth_cutoff)?;
    }

    if args.vacuum {
        run_vacuum(&conn, &db_path)?;
    }

    if let Some(dest) = &args.compact_into {
        run_compact_into(&conn, &db_path, Path::new(dest), &cutoff)?;
    }

    print_status(&conn, &db_path, &cutoff)?;
    print_depth_status(&conn, &depth_cutoff)?;
    Ok(())
}
