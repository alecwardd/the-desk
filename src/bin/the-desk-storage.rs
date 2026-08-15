//! Storage maintenance for The Desk's local SQLite database.
//!
//! Use outside market hours:
//!   cargo run --bin the-desk-storage -- --status
//!   cargo run --bin the-desk-storage -- --maintain --vacuum
//!   cargo run --bin the-desk-storage -- --compact-into X:\TheDesk\state\data_compacted.db
//!   cargo run --bin the-desk-storage -- --backup --abort-if-mcp
//!
//! Exit codes:
//!   0 = success
//!   1 = general failure
//!   2 = deferred (MCP writer active; `--abort-if-mcp`)
//!   3 = config error
//!   4 = integrity/validation failure
//!   5 = storage I/O failure

use chrono::{Datelike, Days, NaiveDate, TimeZone, Utc};
use chrono_tz::US::Eastern;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use the_desk_backend::backup::{self, BackupConfig, BackupError, BackupOutcome};
use the_desk_backend::db::Database;
use the_desk_backend::feed::load_storage_config;
use thiserror::Error;

/// Typed storage CLI failures mapped to process exit codes.
#[derive(Debug, Error)]
enum StorageError {
    #[error("deferred: the-desk-mcp writer is active; refuse to mutate while MCP holds the DB")]
    DeferredMcpActive,
    #[error("config error: {0}")]
    Config(String),
    #[error("integrity/validation failure: {0}")]
    Integrity(String),
    #[error("storage I/O failure: {0}")]
    Io(String),
    #[error("{0}")]
    General(String),
}

impl StorageError {
    fn exit_code(&self) -> i32 {
        match self {
            Self::DeferredMcpActive => 2,
            Self::Config(_) => 3,
            Self::Integrity(_) => 4,
            Self::Io(_) => 5,
            Self::General(_) => 1,
        }
    }
}

impl From<rusqlite::Error> for StorageError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Io(e.to_string())
    }
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

#[derive(Debug)]
struct Args {
    status: bool,
    archive: bool,
    vacuum: bool,
    backup: bool,
    compact_into: Option<String>,
    verify_db: Option<String>,
    compare_db: Option<String>,
    cutoff: Option<String>,
    retention_days: Option<u64>,
    prune_depth: bool,
    depth_cutoff: Option<String>,
    depth_retention_days: Option<u64>,
    prune_snapshots: bool,
    abort_if_mcp: bool,
}

#[derive(Debug)]
struct ArchiveRange {
    month: String,
    start_date: String,
    end_date_exclusive: String,
    row_count: i64,
}

#[derive(Debug, Clone)]
struct SnapshotCutoffs {
    pipeline_date: String,
    pipeline_ms: f64,
    dom_date: String,
    dom_feature_date: String,
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
  the-desk-storage --prune-snapshots
  the-desk-storage --backup [--abort-if-mcp]
  the-desk-storage --compact-into PATH
  the-desk-storage --verify-db PATH [--compare-db PATH] [--cutoff YYYY-MM-DD]

Options:
  --status                  Print raw tick coverage, snapshot tables, and storage settings.
  --archive                 Archive raw_ticks older than the cutoff (also prunes depth + snapshots).
  --maintain                Archive raw_ticks + prune leftover depth_events + prune snapshots. Use with --vacuum.
  --vacuum                  Compact SQLite after archiving. Requires free disk space near current DB size.
  --prune-depth             Delete leftover depth_events older than the depth cutoff (the .depth files remain
                            the durable source). Live ingest no longer writes this table (SIL-M3f). Chunked + WAL-bounded.
  --prune-snapshots         Delete old pipeline/dom/dom_feature snapshots past configured retention.
  --backup                  Create a verified full backup in the configured [backup] directory.
                            Uses desk-<UTC timestamp>.db naming and keeps unarchived history.
                            Ignores backup enabled/min_interval; standard retention pruning applies.
  --depth-cutoff DATE       Prune depth_events with trading_day < DATE. Default: ET today - depth_retention_days.
  --depth-retention-days N  Override configured depth retention days when deriving the depth cutoff.
  --compact-into PATH       Checkpoint WAL, VACUUM INTO PATH, then verify destination integrity.
                            Refuses to overwrite an existing destination file.
  --verify-db PATH          Verify a database copy before a reclaim swap.
  --compare-db PATH         With --verify-db, compare key table row counts against this source DB.
  --cutoff DATE             Archive rows with session_date < DATE. Default: ET today - warm_retention_days.
  --retention-days N        Override configured warm retention days when deriving cutoff.
  --abort-if-mcp            Exit 2 if the-desk-mcp is running before any mutating work.
  --help, -h                Show this help.

Config: ~/.the-desk/config.toml [storage] and [backup]
Exit codes: 0 ok, 1 general, 2 deferred (MCP active), 3 config, 4 integrity, 5 I/O
"#
}

fn parse_args() -> Result<Args, StorageError> {
    let mut args = std::env::args().skip(1);
    let mut parsed = Args {
        status: false,
        archive: false,
        vacuum: false,
        backup: false,
        compact_into: None,
        verify_db: None,
        compare_db: None,
        cutoff: None,
        retention_days: None,
        prune_depth: false,
        depth_cutoff: None,
        depth_retention_days: None,
        prune_snapshots: false,
        abort_if_mcp: false,
    };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--status" => parsed.status = true,
            "--archive" => parsed.archive = true,
            "--maintain" => parsed.archive = true,
            "--vacuum" => parsed.vacuum = true,
            "--backup" => parsed.backup = true,
            "--compact-into" => {
                parsed.compact_into = Some(args.next().ok_or_else(|| {
                    StorageError::Config("--compact-into requires a PATH".into())
                })?);
            }
            "--verify-db" => {
                parsed.verify_db =
                    Some(args.next().ok_or_else(|| {
                        StorageError::Config("--verify-db requires a PATH".into())
                    })?);
            }
            "--compare-db" => {
                parsed.compare_db =
                    Some(args.next().ok_or_else(|| {
                        StorageError::Config("--compare-db requires a PATH".into())
                    })?);
            }
            "--cutoff" => {
                parsed.cutoff = Some(
                    args.next()
                        .ok_or_else(|| StorageError::Config("--cutoff requires a DATE".into()))?,
                );
            }
            "--retention-days" => {
                let raw = args
                    .next()
                    .ok_or_else(|| StorageError::Config("--retention-days requires N".into()))?;
                parsed.retention_days = Some(raw.parse::<u64>().map_err(|_| {
                    StorageError::Config(format!("invalid --retention-days value: {raw}"))
                })?);
            }
            "--prune-depth" => parsed.prune_depth = true,
            "--prune-snapshots" => parsed.prune_snapshots = true,
            "--depth-cutoff" => {
                parsed.depth_cutoff = Some(args.next().ok_or_else(|| {
                    StorageError::Config("--depth-cutoff requires a DATE".into())
                })?);
            }
            "--depth-retention-days" => {
                let raw = args.next().ok_or_else(|| {
                    StorageError::Config("--depth-retention-days requires N".into())
                })?;
                parsed.depth_retention_days = Some(raw.parse::<u64>().map_err(|_| {
                    StorageError::Config(format!("invalid --depth-retention-days value: {raw}"))
                })?);
            }
            "--abort-if-mcp" => parsed.abort_if_mcp = true,
            "--help" | "-h" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            _ => {
                return Err(StorageError::Config(format!("Unknown argument: {arg}")));
            }
        }
    }

    if !parsed.status
        && !parsed.archive
        && !parsed.vacuum
        && !parsed.backup
        && !parsed.prune_depth
        && !parsed.prune_snapshots
        && parsed.compact_into.is_none()
        && parsed.verify_db.is_none()
    {
        parsed.status = true;
    }

    Ok(parsed)
}

fn will_mutate(args: &Args) -> bool {
    args.archive
        || args.vacuum
        || args.backup
        || args.prune_depth
        || args.prune_snapshots
        || args.compact_into.is_some()
}

fn validate_modes(args: &Args) -> Result<(), StorageError> {
    if args.backup
        && (args.status
            || args.archive
            || args.vacuum
            || args.compact_into.is_some()
            || args.verify_db.is_some()
            || args.compare_db.is_some()
            || args.cutoff.is_some()
            || args.retention_days.is_some()
            || args.prune_depth
            || args.depth_cutoff.is_some()
            || args.depth_retention_days.is_some()
            || args.prune_snapshots)
    {
        return Err(StorageError::Config(
            "--backup is a standalone mode; only --abort-if-mcp may accompany it".into(),
        ));
    }
    Ok(())
}

/// Current calendar date in US Eastern (not Globex trading-day roll).
fn et_today() -> NaiveDate {
    Utc::now().with_timezone(&Eastern).date_naive()
}

fn parse_ymd(date: &str) -> Result<NaiveDate, StorageError> {
    NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .map_err(|e| StorageError::Config(format!("invalid date '{date}': {e}")))
}

fn cutoff_from_retention_days(days: u64) -> Result<String, StorageError> {
    let cutoff = et_today()
        .checked_sub_days(Days::new(days))
        .ok_or_else(|| StorageError::Config("retention cutoff underflow".into()))?;
    Ok(cutoff.format("%Y-%m-%d").to_string())
}

/// Midnight US Eastern on `date` as epoch milliseconds.
fn et_midnight_ms(date: &str) -> Result<f64, StorageError> {
    let d = parse_ymd(date)?;
    let naive = d
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| StorageError::Config(format!("invalid midnight for {date}")))?;
    let dt = Eastern
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| StorageError::Config(format!("ambiguous ET midnight for {date}")))?;
    Ok(dt.timestamp_millis() as f64)
}

fn derive_cutoff(args: &Args) -> Result<String, StorageError> {
    if let Some(cutoff) = &args.cutoff {
        parse_ymd(cutoff)?;
        return Ok(cutoff.clone());
    }

    let storage = load_storage_config();
    let retention_days = args
        .retention_days
        .unwrap_or(u64::from(storage.warm_retention_days));
    cutoff_from_retention_days(retention_days)
}

/// Derive the `depth_events` prune cutoff (`trading_day < cutoff`). Honors an explicit
/// `--depth-cutoff`, else `ET today - depth_retention_days`.
fn derive_depth_cutoff(args: &Args) -> Result<String, StorageError> {
    if let Some(cutoff) = &args.depth_cutoff {
        parse_ymd(cutoff)?;
        return Ok(cutoff.clone());
    }

    let storage = load_storage_config();
    let days = args
        .depth_retention_days
        .unwrap_or(u64::from(storage.depth_retention_days));
    cutoff_from_retention_days(days)
}

fn derive_snapshot_cutoffs() -> Result<SnapshotCutoffs, StorageError> {
    let storage = load_storage_config();
    let pipeline_date =
        cutoff_from_retention_days(u64::from(storage.pipeline_snapshot_retention_days))?;
    let dom_date = cutoff_from_retention_days(u64::from(storage.dom_snapshot_retention_days))?;
    let dom_feature_date =
        cutoff_from_retention_days(u64::from(storage.dom_feature_snapshot_retention_days))?;
    let pipeline_ms = et_midnight_ms(&pipeline_date)?;
    Ok(SnapshotCutoffs {
        pipeline_date,
        pipeline_ms,
        dom_date,
        dom_feature_date,
    })
}

/// True when a process named `the-desk-mcp` (or `.exe`) appears to be running.
fn is_mcp_process_running() -> bool {
    #[cfg(windows)]
    {
        let output = std::process::Command::new("tasklist")
            .args(["/FI", "IMAGENAME eq the-desk-mcp.exe", "/NH"])
            .output();
        match output {
            Ok(out) => {
                let s = String::from_utf8_lossy(&out.stdout).to_ascii_lowercase();
                s.contains("the-desk-mcp.exe")
            }
            Err(_) => false,
        }
    }
    #[cfg(not(windows))]
    {
        let output = std::process::Command::new("pgrep")
            .args(["-x", "the-desk-mcp"])
            .output();
        matches!(output, Ok(out) if out.status.success() && !out.stdout.is_empty())
    }
}

fn abort_if_mcp_active(args: &Args) -> Result<(), StorageError> {
    if !args.abort_if_mcp || !will_mutate(args) {
        return Ok(());
    }
    if is_mcp_process_running() {
        return Err(StorageError::DeferredMcpActive);
    }
    Ok(())
}

/// Smallest rowid whose `trading_day >= cutoff`, found by binary search over the
/// primary key. `depth_events.rowid` (the AUTOINCREMENT id) is monotonic with
/// `trading_day` because ingestion is chronological, so each probe is an O(log n)
/// primary-key lookup.
///
/// This deliberately avoids `WHERE trading_day < cutoff`: once the table is large the
/// planner abandons the trading_day index and full-scans the whole (multi-hundred-GB)
/// table on every delete batch, collapsing the prune to ~8k rows/s. Returns `None` for
/// an empty table. Near a day boundary, slight ingestion non-monotonicity can shift the
/// threshold by a few rows — acceptable, since the `.depth` files are the source and a
/// stray row is re-ingestable or pruned on the next run.
fn depth_prune_threshold_rowid(
    conn: &Connection,
    cutoff: &str,
) -> Result<Option<i64>, StorageError> {
    let min_rowid: Option<i64> =
        conn.query_row("SELECT MIN(rowid) FROM depth_events", [], |r| r.get(0))?;
    let max_rowid: Option<i64> =
        conn.query_row("SELECT MAX(rowid) FROM depth_events", [], |r| r.get(0))?;
    let (mut lo, mut hi) = match (min_rowid, max_rowid) {
        (Some(lo), Some(hi)) => (lo, hi),
        _ => return Ok(None),
    };
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let probe: Option<(i64, Option<String>)> = conn
            .query_row(
                "SELECT rowid, trading_day FROM depth_events WHERE rowid >= ?1 ORDER BY rowid LIMIT 1",
                params![mid],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?;
        match probe {
            None => hi = mid,
            // NULL trading_day (undatable) or older than cutoff -> on the delete side.
            Some((rid, td)) if td.as_deref().is_none_or(|d| d < cutoff) => lo = rid + 1,
            Some((rid, _)) => hi = rid,
        }
    }
    Ok(Some(lo))
}

/// Delete leftover `depth_events` older than `cutoff` in bounded chunks, by rowid.
/// SIL-M3f: live ingest no longer writes this table; this is for existing DBs.
///
/// We resolve the retention cutoff to a rowid boundary (see
/// [`depth_prune_threshold_rowid`]) and then delete `rowid < threshold` in batches,
/// checkpointing the WAL periodically — a single giant `DELETE` would build a WAL larger
/// than the (often near-full) data drive. The `.depth` source files remain the durable
/// record, so pruned rows are re-ingestable. Reclaim the freed pages with `--compact-into`.
fn prune_depth_events(conn: &Connection, cutoff: &str) -> Result<i64, StorageError> {
    if !table_exists(conn, "depth_events")? {
        println!("No depth_events table; nothing to prune.");
        return Ok(0);
    }

    let threshold = match depth_prune_threshold_rowid(conn, cutoff)? {
        Some(t) => t,
        None => {
            println!("depth_events is empty; nothing to prune.");
            return Ok(0);
        }
    };
    let min_rowid: i64 = conn
        .query_row("SELECT MIN(rowid) FROM depth_events", [], |r| {
            r.get::<_, Option<i64>>(0)
        })?
        .unwrap_or(threshold);
    if threshold <= min_rowid {
        println!("No depth_events older than {cutoff}; nothing to prune.");
        return Ok(0);
    }

    println!(
        "Pruning depth_events older than {cutoff} (rowid < {threshold}) in chunks (.depth files remain the source)..."
    );
    const BATCH: i64 = 200_000;
    let started = Instant::now();
    let mut deleted_total: i64 = 0;
    let mut batches: u64 = 0;
    loop {
        let n = conn.execute(
            "DELETE FROM depth_events
             WHERE rowid IN (
                 SELECT rowid FROM depth_events WHERE rowid < ?1 LIMIT ?2
             )",
            params![threshold, BATCH],
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

/// Chunked delete for snapshot tables using an indexed predicate, with WAL checkpoints.
fn prune_table_chunked(
    conn: &Connection,
    table: &str,
    where_sql: &str,
    bind: f64,
    label: &str,
) -> Result<i64, StorageError> {
    if !table
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(StorageError::Config(format!("unsafe table name: {table}")));
    }
    if !table_exists(conn, table)? {
        println!("No {table} table; nothing to prune.");
        return Ok(0);
    }

    const BATCH: i64 = 50_000;
    let started = Instant::now();
    let mut deleted_total: i64 = 0;
    let mut batches: u64 = 0;
    let delete_sql = format!(
        "DELETE FROM {table}
         WHERE rowid IN (
             SELECT rowid FROM {table} WHERE {where_sql} LIMIT ?2
         )"
    );

    println!("Pruning {label}...");
    loop {
        let n = conn.execute(&delete_sql, params![bind, BATCH])? as i64;
        deleted_total += n;
        batches += 1;
        if batches.is_multiple_of(10) {
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
            println!(
                "  pruned {deleted_total} {table} rows so far in {:.0?}...",
                started.elapsed()
            );
        }
        if n < BATCH {
            break;
        }
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    println!(
        "Pruned {deleted_total} {table} rows in {:.1?}.",
        started.elapsed()
    );
    Ok(deleted_total)
}

/// Chunked prune for tables keyed by `trading_day` string comparison.
fn prune_table_by_trading_day(
    conn: &Connection,
    table: &str,
    cutoff: &str,
) -> Result<i64, StorageError> {
    if !table
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(StorageError::Config(format!("unsafe table name: {table}")));
    }
    if !table_exists(conn, table)? {
        println!("No {table} table; nothing to prune.");
        return Ok(0);
    }

    const BATCH: i64 = 50_000;
    let started = Instant::now();
    let mut deleted_total: i64 = 0;
    let mut batches: u64 = 0;
    let delete_sql = format!(
        "DELETE FROM {table}
         WHERE rowid IN (
             SELECT rowid FROM {table} WHERE trading_day < ?1 LIMIT ?2
         )"
    );

    println!("Pruning {table} with trading_day < {cutoff}...");
    loop {
        let n = conn.execute(&delete_sql, params![cutoff, BATCH])? as i64;
        deleted_total += n;
        batches += 1;
        if batches.is_multiple_of(10) {
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
            println!(
                "  pruned {deleted_total} {table} rows so far in {:.0?}...",
                started.elapsed()
            );
        }
        if n < BATCH {
            break;
        }
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    println!(
        "Pruned {deleted_total} {table} rows in {:.1?}.",
        started.elapsed()
    );
    Ok(deleted_total)
}

fn prune_snapshots(conn: &Connection, cutoffs: &SnapshotCutoffs) -> Result<i64, StorageError> {
    let mut total = 0_i64;
    total += prune_table_chunked(
        conn,
        "pipeline_snapshots",
        "timestamp_ms < ?1",
        cutoffs.pipeline_ms,
        &format!(
            "pipeline_snapshots timestamp_ms < {} (ET date {})",
            cutoffs.pipeline_ms, cutoffs.pipeline_date
        ),
    )?;
    total += prune_table_by_trading_day(conn, "dom_snapshots", &cutoffs.dom_date)?;
    total += prune_table_by_trading_day(conn, "dom_feature_snapshots", &cutoffs.dom_feature_date)?;
    Ok(total)
}

/// Report DOM `depth_events` coverage. Uses index endpoints and `MAX(rowid)` only —
/// never a full `COUNT(*)`, which would scan billions of rows.
fn print_depth_status(conn: &Connection, depth_cutoff: &str) -> Result<(), StorageError> {
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

fn print_snapshot_table_status_by_ms(
    conn: &Connection,
    table: &str,
    cutoff_ms: f64,
    cutoff_date: &str,
    retention_days: u32,
) -> Result<(), StorageError> {
    if !table_exists(conn, table)? {
        println!("{table}: (table missing)");
        return Ok(());
    }
    let lo: Option<f64> =
        conn.query_row(&format!("SELECT MIN(timestamp_ms) FROM {table}"), [], |r| {
            r.get(0)
        })?;
    let hi: Option<f64> =
        conn.query_row(&format!("SELECT MAX(timestamp_ms) FROM {table}"), [], |r| {
            r.get(0)
        })?;
    let rows: i64 = conn.query_row(&format!("SELECT COUNT(1) FROM {table}"), [], |r| r.get(0))?;
    let reclaimable: i64 = conn.query_row(
        &format!("SELECT COUNT(1) FROM {table} WHERE timestamp_ms < ?1"),
        params![cutoff_ms],
        |r| r.get(0),
    )?;
    println!(
        "{table}: rows={rows}, timestamp_ms={} through {}, retention_days={retention_days}, cutoff_date(ET)={cutoff_date}, reclaimable={reclaimable}",
        lo.map(|v| format!("{v:.0}")).as_deref().unwrap_or("none"),
        hi.map(|v| format!("{v:.0}")).as_deref().unwrap_or("none"),
    );
    Ok(())
}

fn print_snapshot_table_status_by_day(
    conn: &Connection,
    table: &str,
    cutoff: &str,
    retention_days: u32,
) -> Result<(), StorageError> {
    if !table_exists(conn, table)? {
        println!("{table}: (table missing)");
        return Ok(());
    }
    let lo: Option<String> =
        conn.query_row(&format!("SELECT MIN(trading_day) FROM {table}"), [], |r| {
            r.get(0)
        })?;
    let hi: Option<String> =
        conn.query_row(&format!("SELECT MAX(trading_day) FROM {table}"), [], |r| {
            r.get(0)
        })?;
    let rows: i64 = conn.query_row(&format!("SELECT COUNT(1) FROM {table}"), [], |r| r.get(0))?;
    let reclaimable: i64 = conn.query_row(
        &format!("SELECT COUNT(1) FROM {table} WHERE trading_day < ?1"),
        params![cutoff],
        |r| r.get(0),
    )?;
    println!(
        "{table}: rows={rows}, trading_day={} through {}, retention_days={retention_days}, cutoff={cutoff}, reclaimable={reclaimable}",
        lo.as_deref().unwrap_or("none"),
        hi.as_deref().unwrap_or("none"),
    );
    Ok(())
}

fn print_snapshot_status(conn: &Connection, cutoffs: &SnapshotCutoffs) -> Result<(), StorageError> {
    let storage = load_storage_config();
    println!(
        "Snapshot retention (config): pipeline={}d, dom={}d, dom_feature={}d",
        storage.pipeline_snapshot_retention_days,
        storage.dom_snapshot_retention_days,
        storage.dom_feature_snapshot_retention_days
    );
    print_snapshot_table_status_by_ms(
        conn,
        "pipeline_snapshots",
        cutoffs.pipeline_ms,
        &cutoffs.pipeline_date,
        storage.pipeline_snapshot_retention_days,
    )?;
    print_snapshot_table_status_by_day(
        conn,
        "dom_snapshots",
        &cutoffs.dom_date,
        storage.dom_snapshot_retention_days,
    )?;
    print_snapshot_table_status_by_day(
        conn,
        "dom_feature_snapshots",
        &cutoffs.dom_feature_date,
        storage.dom_feature_snapshot_retention_days,
    )?;
    Ok(())
}

fn sqlite_temp_dir() -> Result<PathBuf, StorageError> {
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

fn open_db(path: &Path, temp_dir: &Path) -> Result<Connection, StorageError> {
    let conn = Connection::open(path)
        .map_err(|e| StorageError::Io(format!("failed to open {}: {e}", path.display())))?;
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

fn print_status(conn: &Connection, db_path: &Path, cutoff: &str) -> Result<(), StorageError> {
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
    println!("Warm retention days: {}", storage.warm_retention_days);
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
    println!("Depth retention days: {}", storage.depth_retention_days);
    Ok(())
}

fn month_after(month: &str) -> Result<String, StorageError> {
    let date = parse_ymd(&format!("{month}-01"))?;
    let next = if date.month() == 12 {
        NaiveDate::from_ymd_opt(date.year() + 1, 1, 1)
            .ok_or_else(|| StorageError::General("invalid next year".into()))?
    } else {
        NaiveDate::from_ymd_opt(date.year(), date.month() + 1, 1)
            .ok_or_else(|| StorageError::General("invalid next month".into()))?
    };
    Ok(next.format("%Y-%m-%d").to_string())
}

fn archive_ranges(conn: &Connection, cutoff: &str) -> Result<Vec<ArchiveRange>, StorageError> {
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
) -> Result<i64, StorageError> {
    fs::create_dir_all(archive_dir)?;
    let final_path = archive_dir.join(format!(
        "raw_ticks_{}_{}_to_{}.csv.zst",
        range.month, range.start_date, range.end_date_exclusive
    ));
    if final_path.exists() {
        return Err(StorageError::Integrity(format!(
            "archive file already exists, refusing to overwrite: {}",
            final_path.display()
        )));
    }

    let temp_path = final_path.with_extension("csv.zst.tmp");
    if temp_path.exists() {
        fs::remove_file(&temp_path)?;
    }

    let file = File::create(&temp_path)?;
    let writer = BufWriter::new(file);
    let mut encoder = zstd::stream::write::Encoder::new(writer, 3)
        .map_err(|e| StorageError::Io(format!("zstd encoder create failed: {e}")))?;
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
    encoder
        .finish()
        .map_err(|e| StorageError::Io(format!("zstd finish failed: {e}")))?;

    if written != range.row_count {
        fs::remove_file(&temp_path).ok();
        return Err(StorageError::Integrity(format!(
            "archive row count mismatch for {}: expected {}, wrote {}",
            range.month, range.row_count, written
        )));
    }

    fs::rename(&temp_path, &final_path)?;
    let deleted = conn.execute(
        "DELETE FROM raw_ticks WHERE session_date >= ?1 AND session_date < ?2",
        params![range.start_date, range.end_date_exclusive],
    )?;
    Ok(deleted as i64)
}

fn archive_old_ticks(conn: &mut Connection, cutoff: &str) -> Result<i64, StorageError> {
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

fn run_vacuum(conn: &Connection, db_path: &Path) -> Result<(), StorageError> {
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

fn verify_sqlite_integrity(path: &Path) -> Result<(), StorageError> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let integrity: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    if integrity != "ok" {
        return Err(StorageError::Integrity(format!(
            "integrity_check failed for {}: {integrity}",
            path.display()
        )));
    }
    Ok(())
}

fn open_readonly_db(path: &Path) -> Result<Connection, StorageError> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(std::time::Duration::from_secs(30))?;
    Ok(conn)
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool, StorageError> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(1) FROM sqlite_schema WHERE type='table' AND name=?1",
        params![table],
        |r| r.get(0),
    )?;
    Ok(count == 1)
}

fn table_row_count(conn: &Connection, table: &str) -> Result<i64, StorageError> {
    if !table
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(StorageError::Config(format!("unsafe table name: {table}")));
    }
    let sql = format!("SELECT COUNT(1) FROM {table}");
    Ok(conn.query_row(&sql, [], |r| r.get(0))?)
}

fn verify_reclaim_retention(copy: &Connection, cutoff: &str) -> Result<(), StorageError> {
    let old_raw_ticks: i64 = copy.query_row(
        "SELECT COUNT(1) FROM raw_ticks WHERE session_date < ?1",
        params![cutoff],
        |r| r.get(0),
    )?;
    if old_raw_ticks != 0 {
        return Err(StorageError::Integrity(format!(
            "database copy still contains {old_raw_ticks} raw_ticks rows older than {cutoff}"
        )));
    }
    Ok(())
}

fn verify_reclaim_copy(
    copy_path: &Path,
    compare_path: Option<&Path>,
    cutoff: &str,
) -> Result<(), StorageError> {
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
            return Err(StorageError::Integrity(format!(
                "required table missing from copy: {table}"
            )));
        }
    }

    let session_summaries = table_row_count(&copy, "session_summaries")?;
    if session_summaries <= 0 {
        return Err(StorageError::Integrity(
            "session_summaries is empty in database copy".into(),
        ));
    }

    verify_reclaim_retention(&copy, cutoff)?;

    if let Some(compare_path) = compare_path {
        let source = open_readonly_db(compare_path)?;
        for table in REQUIRED_TABLES {
            let source_count = table_row_count(&source, table)?;
            let copy_count = table_row_count(&copy, table)?;
            if source_count != copy_count {
                return Err(StorageError::Integrity(format!(
                    "row-count mismatch for {table}: source={source_count}, copy={copy_count}"
                )));
            }
        }
    }

    println!(
        "Database copy verified: integrity ok, session_summaries={session_summaries}, cutoff={cutoff}."
    );
    Ok(())
}

fn map_backup_error(error: BackupError) -> StorageError {
    match error {
        error @ (BackupError::VerificationFailed { .. } | BackupError::Sqlite(_)) => {
            StorageError::Integrity(error.to_string())
        }
        error @ (BackupError::Db(_) | BackupError::Io(_)) => StorageError::Io(error.to_string()),
    }
}

fn create_verified_backup(
    db_path: &Path,
    config: &BackupConfig,
    now: chrono::DateTime<Utc>,
) -> Result<BackupOutcome, StorageError> {
    let db = Database::open(&db_path.to_string_lossy())
        .map_err(|error| StorageError::Io(error.to_string()))?;
    backup::perform_backup(
        &db,
        &config.directory_path(),
        now,
        config.retention_days,
        config.max_backups,
    )
    .map_err(map_backup_error)
}

fn run_backup(db_path: &Path) -> Result<(), StorageError> {
    let config = backup::load_backup_config();
    let started = Instant::now();
    let outcome = create_verified_backup(db_path, &config, Utc::now())?;
    println!(
        "Backup verified in {:.1?}: {} ({:.2} GB); pruned {} expired backup(s).",
        started.elapsed(),
        outcome.path.display(),
        outcome.size_bytes as f64 / 1_000_000_000.0,
        outcome.pruned.len()
    );
    Ok(())
}

fn run_compact_into(
    conn: &Connection,
    db_path: &Path,
    dest_path: &Path,
    cutoff: &str,
) -> Result<(), StorageError> {
    let started = Instant::now();

    if dest_path.exists() {
        return Err(StorageError::Integrity(format!(
            "compact destination already exists, refusing to overwrite: {}",
            dest_path.display()
        )));
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
    let dest_name = dest_path.file_name().ok_or_else(|| {
        StorageError::Config("compact destination must include a file name".into())
    })?;
    let canonical_intent = dest_parent.join(dest_name);
    if source == canonical_intent {
        return Err(StorageError::Config(
            "compact destination must be different from the source database".into(),
        ));
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
) -> Result<(), StorageError> {
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
            return Err(StorageError::Integrity(format!(
                "archive target already exists while {} matching rows remain in SQLite: {}",
                archived_rows.unwrap_or(0),
                final_path.display()
            )));
        }
    }
    Ok(())
}

fn run() -> Result<(), StorageError> {
    let args = parse_args()?;
    validate_modes(&args)?;
    abort_if_mcp_active(&args)?;

    let db_path = data_dir().join("data.db");
    if args.backup {
        return run_backup(&db_path);
    }

    let cutoff = derive_cutoff(&args)?;
    let depth_cutoff = derive_depth_cutoff(&args)?;
    let snapshot_cutoffs = derive_snapshot_cutoffs()?;
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
        print_snapshot_status(&conn, &snapshot_cutoffs)?;
        if !args.archive && !args.vacuum && !args.prune_depth && !args.prune_snapshots {
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

    // --maintain/--archive and --prune-snapshots prune snapshot tables after depth.
    if args.archive || args.prune_snapshots {
        prune_snapshots(&conn, &snapshot_cutoffs)?;
    }

    if args.vacuum {
        run_vacuum(&conn, &db_path)?;
    }

    if let Some(dest) = &args.compact_into {
        run_compact_into(&conn, &db_path, Path::new(dest), &cutoff)?;
    }

    print_status(&conn, &db_path, &cutoff)?;
    print_depth_status(&conn, &depth_cutoff)?;
    print_snapshot_status(&conn, &snapshot_cutoffs)?;
    Ok(())
}

fn main() {
    match run() {
        Ok(()) => std::process::exit(0),
        Err(err) => {
            eprintln!("{err}");
            if matches!(err, StorageError::Config(_)) {
                eprintln!("{}", usage());
            }
            std::process::exit(err.exit_code());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    /// Seed an in-memory `depth_events` with rows in chronological `(trading_day, count)`
    /// order, so rowid is monotonic with trading_day (mirrors live ingestion).
    fn seed(days: &[(&str, i64)]) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE depth_events (id INTEGER PRIMARY KEY AUTOINCREMENT, trading_day TEXT);",
        )
        .unwrap();
        for (day, count) in days {
            for _ in 0..*count {
                conn.execute(
                    "INSERT INTO depth_events (trading_day) VALUES (?1)",
                    params![day],
                )
                .unwrap();
            }
        }
        conn
    }

    fn seed_snapshots() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE pipeline_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms REAL NOT NULL,
                payload TEXT NOT NULL
             );
             CREATE INDEX idx_pipeline_snapshots_ts ON pipeline_snapshots(timestamp_ms);
             CREATE TABLE dom_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms REAL NOT NULL,
                trading_day TEXT NOT NULL,
                payload TEXT NOT NULL
             );
             CREATE INDEX idx_dom_snapshots_day ON dom_snapshots(trading_day, timestamp_ms);
             CREATE TABLE dom_feature_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms REAL NOT NULL,
                trading_day TEXT NOT NULL,
                payload TEXT NOT NULL
             );
             CREATE INDEX idx_dom_feature_snapshots_day
               ON dom_feature_snapshots(trading_day, timestamp_ms);",
        )
        .unwrap();
        conn
    }

    #[test]
    fn threshold_is_first_rowid_at_or_after_cutoff() {
        // rowids 1..=3 -> 06-17, 4..=7 -> 06-18, 8..=12 -> 06-19
        let conn = seed(&[("2026-06-17", 3), ("2026-06-18", 4), ("2026-06-19", 5)]);
        let thr = depth_prune_threshold_rowid(&conn, "2026-06-19")
            .unwrap()
            .unwrap();
        assert_eq!(thr, 8);
    }

    #[test]
    fn prune_deletes_only_rows_older_than_cutoff() {
        let conn = seed(&[("2026-06-17", 3), ("2026-06-18", 4), ("2026-06-19", 5)]);
        let deleted = prune_depth_events(&conn, "2026-06-19").unwrap();
        assert_eq!(deleted, 7);
        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM depth_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 5);
        let min_day: String = conn
            .query_row("SELECT MIN(trading_day) FROM depth_events", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(min_day, "2026-06-19");
    }

    #[test]
    fn prune_is_a_noop_when_nothing_is_older() {
        let conn = seed(&[("2026-06-19", 5)]);
        assert_eq!(prune_depth_events(&conn, "2026-06-19").unwrap(), 0);
    }

    #[test]
    fn threshold_none_for_empty_table() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE depth_events (id INTEGER PRIMARY KEY, trading_day TEXT);")
            .unwrap();
        assert_eq!(
            depth_prune_threshold_rowid(&conn, "2026-06-19").unwrap(),
            None
        );
    }

    #[test]
    fn storage_error_exit_codes() {
        assert_eq!(StorageError::General("x".into()).exit_code(), 1);
        assert_eq!(StorageError::DeferredMcpActive.exit_code(), 2);
        assert_eq!(StorageError::Config("x".into()).exit_code(), 3);
        assert_eq!(StorageError::Integrity("x".into()).exit_code(), 4);
        assert_eq!(StorageError::Io("x".into()).exit_code(), 5);
        assert_eq!(
            map_backup_error(BackupError::VerificationFailed {
                path: "copy.db".into(),
                detail: "quick_check failed".into(),
            })
            .exit_code(),
            4
        );
    }

    #[test]
    fn abort_if_mcp_skipped_when_flag_off_or_read_only() {
        let read_only = Args {
            status: true,
            archive: false,
            vacuum: false,
            backup: false,
            compact_into: None,
            verify_db: None,
            compare_db: None,
            cutoff: None,
            retention_days: None,
            prune_depth: false,
            depth_cutoff: None,
            depth_retention_days: None,
            prune_snapshots: false,
            abort_if_mcp: true,
        };
        // Status-only never mutates, so MCP presence is irrelevant.
        assert!(abort_if_mcp_active(&read_only).is_ok());

        let mutate_no_flag = Args {
            archive: true,
            abort_if_mcp: false,
            ..read_only
        };
        assert!(abort_if_mcp_active(&mutate_no_flag).is_ok());
    }

    #[test]
    fn backup_mode_is_standalone_and_mutating() {
        let backup = Args {
            status: false,
            archive: false,
            vacuum: false,
            backup: true,
            compact_into: None,
            verify_db: None,
            compare_db: None,
            cutoff: None,
            retention_days: None,
            prune_depth: false,
            depth_cutoff: None,
            depth_retention_days: None,
            prune_snapshots: false,
            abort_if_mcp: true,
        };
        assert!(will_mutate(&backup));
        assert!(validate_modes(&backup).is_ok());

        let conflicting = Args {
            compact_into: Some("copy.db".into()),
            ..backup
        };
        assert!(matches!(
            validate_modes(&conflicting),
            Err(StorageError::Config(_))
        ));
    }

    #[test]
    fn backup_keeps_unarchived_rows_while_reclaim_verification_rejects_them() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("source.db");
        let db = Database::open(&source_path.to_string_lossy()).unwrap();
        db.insert_raw_tick(1.0, 20_000.0, 1.0, 19_999.75, 20_000.0, true, "2026-01-01")
            .unwrap();
        drop(db);

        let backup_dir = temp.path().join("backups");
        let config = BackupConfig {
            enabled: false,
            directory: backup_dir.to_string_lossy().into_owned(),
            retention_days: 0,
            max_backups: 0,
            min_interval_hours: 24,
        };
        let now = Utc
            .with_ymd_and_hms(2026, 7, 22, 1, 15, 0)
            .single()
            .unwrap();
        let outcome = create_verified_backup(&source_path, &config, now).unwrap();

        assert!(outcome.verified);
        assert_eq!(
            outcome.path.file_name().and_then(|name| name.to_str()),
            Some("desk-2026-07-22-011500.db")
        );
        assert!(!outcome.path.with_extension("db-journal").exists());

        let backup = open_readonly_db(&outcome.path).unwrap();
        let reclaim_result = verify_reclaim_retention(&backup, "2026-06-22");
        assert!(matches!(reclaim_result, Err(StorageError::Integrity(_))));
    }

    #[test]
    fn et_midnight_ms_is_eastern_boundary() {
        // 2026-06-19 00:00 EDT = 2026-06-19 04:00 UTC
        let ms = et_midnight_ms("2026-06-19").unwrap();
        assert_eq!(ms, 1_781_841_600_000.0);

        // Winter: 2026-01-15 00:00 EST = 2026-01-15 05:00 UTC
        let ms_winter = et_midnight_ms("2026-01-15").unwrap();
        assert_eq!(ms_winter, 1_768_453_200_000.0);
    }

    #[test]
    fn et_midnight_rejects_bad_date() {
        assert!(matches!(
            et_midnight_ms("not-a-date"),
            Err(StorageError::Config(_))
        ));
    }

    #[test]
    fn snapshot_prune_respects_boundaries_and_is_idempotent() {
        let conn = seed_snapshots();
        let cutoff = SnapshotCutoffs {
            pipeline_date: "2026-06-19".into(),
            pipeline_ms: et_midnight_ms("2026-06-19").unwrap(),
            dom_date: "2026-06-19".into(),
            dom_feature_date: "2026-06-19".into(),
        };

        // pipeline: one before, one at, one after midnight ET
        conn.execute(
            "INSERT INTO pipeline_snapshots (timestamp_ms, payload) VALUES (?1, 'a')",
            params![cutoff.pipeline_ms - 1.0],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO pipeline_snapshots (timestamp_ms, payload) VALUES (?1, 'b')",
            params![cutoff.pipeline_ms],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO pipeline_snapshots (timestamp_ms, payload) VALUES (?1, 'c')",
            params![cutoff.pipeline_ms + 1.0],
        )
        .unwrap();

        for day in ["2026-06-17", "2026-06-18", "2026-06-19"] {
            conn.execute(
                "INSERT INTO dom_snapshots (timestamp_ms, trading_day, payload) VALUES (1, ?1, 'd')",
                params![day],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO dom_feature_snapshots (timestamp_ms, trading_day, payload)
                 VALUES (1, ?1, 'f')",
                params![day],
            )
            .unwrap();
        }

        let deleted = prune_snapshots(&conn, &cutoff).unwrap();
        // 1 pipeline + 2 dom + 2 feature = 5
        assert_eq!(deleted, 5);

        let pipe_left: i64 = conn
            .query_row("SELECT COUNT(*) FROM pipeline_snapshots", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pipe_left, 2);
        let min_pipe: f64 = conn
            .query_row(
                "SELECT MIN(timestamp_ms) FROM pipeline_snapshots",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(min_pipe, cutoff.pipeline_ms);

        let dom_left: i64 = conn
            .query_row("SELECT COUNT(*) FROM dom_snapshots", [], |r| r.get(0))
            .unwrap();
        assert_eq!(dom_left, 1);
        let feat_left: i64 = conn
            .query_row("SELECT COUNT(*) FROM dom_feature_snapshots", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(feat_left, 1);

        // Idempotent second pass.
        assert_eq!(prune_snapshots(&conn, &cutoff).unwrap(), 0);
    }

    #[test]
    fn snapshot_prune_empty_tables_is_noop() {
        let conn = seed_snapshots();
        let cutoff = SnapshotCutoffs {
            pipeline_date: "2026-06-19".into(),
            pipeline_ms: et_midnight_ms("2026-06-19").unwrap(),
            dom_date: "2026-06-19".into(),
            dom_feature_date: "2026-06-19".into(),
        };
        assert_eq!(prune_snapshots(&conn, &cutoff).unwrap(), 0);
    }

    #[test]
    fn storage_config_snapshot_defaults_are_backward_compatible() {
        let cfg: the_desk_backend::feed::StorageConfig = toml::from_str(
            r#"
            warm_retention_days = 30
            "#,
        )
        .unwrap();
        assert_eq!(cfg.pipeline_snapshot_retention_days, 14);
        assert_eq!(cfg.dom_snapshot_retention_days, 7);
        assert_eq!(cfg.dom_feature_snapshot_retention_days, 7);
        assert_eq!(cfg.depth_retention_days, 7);
    }

    #[test]
    fn backup_default_min_interval_is_24h() {
        assert_eq!(the_desk_backend::backup::DEFAULT_MIN_INTERVAL_HOURS, 24);
        assert_eq!(
            the_desk_backend::backup::BackupConfig::default().min_interval_hours,
            24
        );
    }
}
