//! Storage maintenance for The Desk's local SQLite database.
//!
//! Use outside market hours:
//!   cargo run --bin the-desk-storage -- --status
//!   cargo run --bin the-desk-storage -- --maintain --vacuum

use chrono::{Datelike, Days, Local, NaiveDate};
use rusqlite::{params, Connection, OptionalExtension};
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
    cutoff: Option<String>,
    retention_days: Option<u64>,
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

Options:
  --status              Print raw tick coverage and storage settings.
  --archive             Archive raw_ticks older than the cutoff.
  --maintain            Archive raw_ticks older than the cutoff. Use with --vacuum to reclaim disk.
  --vacuum              Compact SQLite after archiving. Requires free disk space near current DB size.
  --cutoff DATE         Archive rows with session_date < DATE. Default: today - warm_retention_days.
  --retention-days N    Override configured warm retention days when deriving cutoff.
  --help, -h            Show this help.

Config: ~/.the-desk/config.toml [storage]
"#
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut parsed = Args {
        status: false,
        archive: false,
        vacuum: false,
        cutoff: None,
        retention_days: None,
    };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--status" => parsed.status = true,
            "--archive" => parsed.archive = true,
            "--maintain" => parsed.archive = true,
            "--vacuum" => parsed.vacuum = true,
            "--cutoff" => parsed.cutoff = args.next(),
            "--retention-days" => {
                parsed.retention_days = args.next().and_then(|s| s.parse::<u64>().ok());
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

    if !parsed.status && !parsed.archive && !parsed.vacuum {
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
    println!("SQLite pages: total={page_count}, freelist={freelist_count}");
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
    let db_path = data_dir().join("data.db");
    let temp_dir = sqlite_temp_dir()?;
    println!("SQLite temp dir: {}", temp_dir.display());
    let mut conn = open_db(&db_path, &temp_dir)?;

    if args.status {
        print_status(&conn, &db_path, &cutoff)?;
        if !args.archive && !args.vacuum {
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

    if args.vacuum {
        run_vacuum(&conn, &db_path)?;
    }

    print_status(&conn, &db_path, &cutoff)?;
    Ok(())
}
