//! Database backup: timestamped `VACUUM INTO` snapshots with retention pruning.
//!
//! The Desk keeps everything — trades, journal, signal outcomes, the entire
//! memory layer — in one SQLite file. A single corrupted file would lose all of
//! it, so the MCP server takes a verified snapshot on startup (bounded by a
//! minimum interval so frequent restarts don't spam backups) and prunes old
//! snapshots by age and count. Backups can also be triggered on demand via the
//! `create_database_backup` MCP tool.
//!
//! Snapshots use [`Database::backup_to`] (`VACUUM INTO`), which writes a single
//! consistent file that already incorporates committed WAL state. Each snapshot
//! is verified with `PRAGMA quick_check` before older ones are pruned.
//!
//! Configuration lives under `[backup]` in `~/.the-desk/config.toml`; all
//! fields have production-safe defaults, so backups work with no config at all.

use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Deserialize;
use thiserror::Error;

use crate::db::{Database, DbError};

/// Default age, in days, after which a backup is pruned. `0` disables age-based
/// pruning (count-based pruning via `max_backups` still applies).
pub const DEFAULT_RETENTION_DAYS: u32 = 14;
/// Default hard cap on retained backups, oldest pruned first. `0` disables the
/// count cap (age-based pruning still applies).
pub const DEFAULT_MAX_BACKUPS: usize = 30;
/// Default minimum hours between automatic startup backups.
pub const DEFAULT_MIN_INTERVAL_HOURS: u64 = 24;

const FILE_PREFIX: &str = "desk-";
const FILE_SUFFIX: &str = ".db";
/// `chrono` format for the timestamp embedded in a backup filename.
const STAMP_FORMAT: &str = "%Y-%m-%d-%H%M%S";

/// Backup configuration, loaded from `[backup]` in `~/.the-desk/config.toml`.
#[derive(Debug, Clone, Deserialize)]
pub struct BackupConfig {
    /// Whether automatic startup backups run. On-demand backups ignore this.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Directory backups are written to. Defaults to `~/.the-desk/backups`.
    #[serde(default = "default_directory")]
    pub directory: String,
    /// Age in days after which a backup is pruned (`0` = keep regardless of age).
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
    /// Hard cap on retained backups (`0` = no count cap).
    #[serde(default = "default_max_backups")]
    pub max_backups: usize,
    /// Minimum hours between automatic startup backups.
    #[serde(default = "default_min_interval_hours")]
    pub min_interval_hours: u64,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            directory: default_directory(),
            retention_days: default_retention_days(),
            max_backups: default_max_backups(),
            min_interval_hours: default_min_interval_hours(),
        }
    }
}

impl BackupConfig {
    /// The configured backup directory as a path.
    pub fn directory_path(&self) -> PathBuf {
        PathBuf::from(&self.directory)
    }
}

fn default_enabled() -> bool {
    true
}

fn default_directory() -> String {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".the-desk")
        .join("backups")
        .to_string_lossy()
        .into_owned()
}

fn default_retention_days() -> u32 {
    DEFAULT_RETENTION_DAYS
}

fn default_max_backups() -> usize {
    DEFAULT_MAX_BACKUPS
}

fn default_min_interval_hours() -> u64 {
    DEFAULT_MIN_INTERVAL_HOURS
}

#[derive(Debug, Deserialize, Default)]
struct RootBackupConfig {
    #[serde(default)]
    backup: BackupConfig,
}

/// Load `[backup]` from `~/.the-desk/config.toml`, falling back to defaults when
/// the file or section is absent or malformed.
pub fn load_backup_config() -> BackupConfig {
    match std::fs::read_to_string(crate::feed::default_config_path()) {
        Ok(content) => toml::from_str::<RootBackupConfig>(&content)
            .map(|cfg| cfg.backup)
            .unwrap_or_default(),
        Err(_) => BackupConfig::default(),
    }
}

/// Errors that can occur while taking or maintaining backups.
#[derive(Debug, Error)]
pub enum BackupError {
    #[error("database error: {0}")]
    Db(#[from] DbError),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("backup verification failed for {path}: {detail}")]
    VerificationFailed { path: String, detail: String },
}

/// Metadata for a single backup file on disk.
#[derive(Debug, Clone)]
pub struct BackupFileInfo {
    /// Absolute path to the backup file.
    pub path: PathBuf,
    /// File name (e.g. `desk-2026-06-13-090000.db`).
    pub file_name: String,
    /// Size in bytes.
    pub size_bytes: u64,
    /// Timestamp parsed from the filename, if it matches the backup pattern.
    pub created_at: Option<DateTime<Utc>>,
}

/// Outcome of a performed backup.
#[derive(Debug, Clone)]
pub struct BackupOutcome {
    /// Path to the snapshot just written.
    pub path: PathBuf,
    /// Size of the snapshot in bytes.
    pub size_bytes: u64,
    /// Whether `PRAGMA quick_check` reported the snapshot sound.
    pub verified: bool,
    /// Backup files removed by retention pruning during this run.
    pub pruned: Vec<PathBuf>,
}

/// Why an automatic startup backup did not run.
#[derive(Debug, Clone)]
pub enum SkipReason {
    /// `[backup].enabled = false`.
    Disabled,
    /// A recent backup is still within `min_interval_hours`.
    WithinInterval {
        hours_since_last: f64,
        min_interval_hours: u64,
    },
}

/// Result of the startup backup attempt.
#[derive(Debug, Clone)]
pub enum StartupBackupReport {
    Created(BackupOutcome),
    Skipped(SkipReason),
}

/// Take a backup now, verify it, and prune old backups.
///
/// Creates `dir` if needed, writes `desk-<timestamp>.db`, verifies it with
/// SQLite header magic + `PRAGMA quick_check` + durable-table presence, then
/// prunes by age and count — never deleting the last header-valid backup.
/// Ignores [`BackupConfig::enabled`] and the minimum interval — those gate the
/// automatic startup path ([`run_startup_backup`]), not explicit requests.
pub fn perform_backup(
    db: &Database,
    dir: &Path,
    now: DateTime<Utc>,
    retention_days: u32,
    max_backups: usize,
) -> Result<BackupOutcome, BackupError> {
    std::fs::create_dir_all(dir)?;
    let dest = unique_destination(dir, now);
    let dest_str = dest.to_string_lossy().into_owned();

    // `VACUUM INTO` can fail partway through (most commonly: the destination drive
    // fills up — which is exactly when this fires, since the snapshot is ~DB-sized).
    // On failure it leaves a partial file behind; remove it so a doomed backup cannot
    // accumulate as a multi-GB orphan that itself fills the drive.
    if let Err(err) = db.backup_to(&dest_str) {
        let _ = std::fs::remove_file(&dest);
        let _ = std::fs::remove_file(format!("{dest_str}-journal"));
        return Err(err.into());
    }

    // Re-read magic from the filesystem after VACUUM INTO returns so a truncated
    // or zeroed first page cannot be mistaken for a restore point.
    if !has_sqlite_header(&dest)? {
        let _ = std::fs::remove_file(&dest);
        let _ = std::fs::remove_file(format!("{dest_str}-journal"));
        return Err(BackupError::VerificationFailed {
            path: dest_str,
            detail: "SQLite header magic missing after VACUUM INTO (page 0 zeroed or truncated)"
                .to_string(),
        });
    }

    match verify_backup_file(&dest) {
        Ok(true) => {}
        Ok(false) => {
            let _ = std::fs::remove_file(&dest);
            let _ = std::fs::remove_file(format!("{dest_str}-journal"));
            return Err(BackupError::VerificationFailed {
                path: dest_str,
                detail: "PRAGMA quick_check did not return ok".to_string(),
            });
        }
        Err(err) => {
            let _ = std::fs::remove_file(&dest);
            let _ = std::fs::remove_file(format!("{dest_str}-journal"));
            return Err(err);
        }
    }

    let size_bytes = std::fs::metadata(&dest).map(|m| m.len()).unwrap_or(0);
    let pruned = prune_backups(dir, retention_days, max_backups, now);

    Ok(BackupOutcome {
        path: dest,
        size_bytes,
        verified: true,
        pruned,
    })
}

/// Run the automatic startup backup, honoring `enabled` and the minimum
/// interval between backups.
pub fn run_startup_backup(
    db: &Database,
    config: &BackupConfig,
    now: DateTime<Utc>,
) -> Result<StartupBackupReport, BackupError> {
    if !config.enabled {
        return Ok(StartupBackupReport::Skipped(SkipReason::Disabled));
    }

    let dir = config.directory_path();
    if config.min_interval_hours > 0 {
        if let Some(hours) = hours_since_last_backup(&dir, now) {
            if hours < config.min_interval_hours as f64 {
                return Ok(StartupBackupReport::Skipped(SkipReason::WithinInterval {
                    hours_since_last: hours,
                    min_interval_hours: config.min_interval_hours,
                }));
            }
        }
    }

    let outcome = perform_backup(db, &dir, now, config.retention_days, config.max_backups)?;
    Ok(StartupBackupReport::Created(outcome))
}

/// Hours since the most recent backup in `dir`, or `None` if there are none.
pub fn hours_since_last_backup(dir: &Path, now: DateTime<Utc>) -> Option<f64> {
    list_backups(dir)
        .into_iter()
        .filter_map(|b| b.created_at)
        .map(|created| (now - created).num_seconds() as f64 / 3600.0)
        .reduce(f64::min)
}

/// List backup files in `dir`, newest first. Non-backup files are ignored.
pub fn list_backups(dir: &Path) -> Vec<BackupFileInfo> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !is_backup_name(&name) {
            continue;
        }
        let size_bytes = entry.metadata().map(|m| m.len()).unwrap_or(0);
        out.push(BackupFileInfo {
            path: entry.path(),
            created_at: parse_stamp(&name),
            file_name: name,
            size_bytes,
        });
    }
    // Sort by parsed timestamp (filename is lexically chronological too), newest
    // first; unparseable names sort last.
    out.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then(b.file_name.cmp(&a.file_name))
    });
    out
}

/// Prune backups in `dir` older than `retention_days` and beyond `max_backups`.
///
/// Age and count limits are independent: `0` disables that limit. Returns the
/// paths removed. Only files matching the `desk-<timestamp>.db` pattern are ever
/// touched. **Never deletes the newest header-valid backup** — corrupt/zero-header
/// files do not count as a safety net.
pub fn prune_backups(
    dir: &Path,
    retention_days: u32,
    max_backups: usize,
    now: DateTime<Utc>,
) -> Vec<PathBuf> {
    let backups = list_backups(dir); // newest first
    let mut removed = Vec::new();

    let age_cutoff =
        (retention_days > 0).then(|| now - chrono::Duration::days(retention_days as i64));

    // Pin the newest header-valid restore point so pruning cannot wipe the only
    // known-good snapshot (corrupt zero-header files are not a safety net).
    let protected = backups
        .iter()
        .find(|b| has_sqlite_header(&b.path).unwrap_or(false))
        .map(|b| b.path.clone());

    let mut kept = 0usize;
    for backup in backups {
        let too_old = match (age_cutoff, backup.created_at) {
            (Some(cutoff), Some(created)) => created < cutoff,
            _ => false,
        };
        let over_cap = max_backups > 0 && kept >= max_backups;
        let is_protected = protected.as_ref().is_some_and(|p| p == &backup.path);

        if (too_old || over_cap) && !is_protected {
            if std::fs::remove_file(&backup.path).is_ok() {
                let _ = std::fs::remove_file(format!("{}-journal", backup.path.display()));
                removed.push(backup.path);
            }
        } else {
            kept += 1;
        }
    }
    removed
}

/// Durable trader/control tables that must be present in a verified full backup.
pub const BACKUP_DURABLE_TABLES: &[&str] = &[
    "session_summaries",
    "setups",
    "research_hypotheses",
    "risk_config",
    "risk_state",
    "account_state",
];

/// True when `path` begins with the SQLite database header magic.
pub fn has_sqlite_header(path: &Path) -> Result<bool, BackupError> {
    let mut file = std::fs::File::open(path)?;
    let mut magic = [0u8; 16];
    use std::io::Read;
    let n = file.read(&mut magic)?;
    Ok(n >= 16 && magic.starts_with(b"SQLite format 3\0"))
}

/// Operator-facing backup directory health (latest file, header validity, free space).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupHealthReport {
    pub directory: String,
    pub backup_count: usize,
    pub header_valid_count: usize,
    pub latest: Option<BackupFileInfoJson>,
    pub latest_header_valid: bool,
    pub warnings: Vec<String>,
}

/// JSON-friendly backup file metadata.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupFileInfoJson {
    pub path: String,
    pub file_name: String,
    pub size_bytes: u64,
    pub created_at: Option<String>,
    pub header_valid: bool,
}

/// Inspect backup directory health without mutating files.
pub fn backup_health_report(dir: &Path) -> BackupHealthReport {
    let backups = list_backups(dir);
    let mut warnings = Vec::new();
    let mut header_valid_count = 0usize;
    let mut latest_json = None;
    let mut latest_header_valid = false;

    for (i, b) in backups.iter().enumerate() {
        let header_valid = has_sqlite_header(&b.path).unwrap_or(false);
        if header_valid {
            header_valid_count += 1;
        } else {
            warnings.push(format!(
                "backup {} missing SQLite header magic (page 0 zeroed or truncated)",
                b.file_name
            ));
        }
        if i == 0 {
            latest_header_valid = header_valid;
            latest_json = Some(BackupFileInfoJson {
                path: b.path.to_string_lossy().into_owned(),
                file_name: b.file_name.clone(),
                size_bytes: b.size_bytes,
                created_at: b.created_at.map(|t| t.to_rfc3339()),
                header_valid,
            });
        }
    }

    if backups.is_empty() {
        warnings.push("no desk-*.db backups found in directory".to_string());
    } else if header_valid_count == 0 {
        warnings.push(
            "NO header-valid backups remain — create a verified full backup before any destructive maintenance"
                .to_string(),
        );
    }

    BackupHealthReport {
        directory: dir.to_string_lossy().into_owned(),
        backup_count: backups.len(),
        header_valid_count,
        latest: latest_json,
        latest_header_valid,
        warnings,
    }
}

/// Build a unique destination path. `VACUUM INTO` refuses to overwrite, so if a
/// same-second file already exists, a numeric suffix is appended.
fn unique_destination(dir: &Path, now: DateTime<Utc>) -> PathBuf {
    let stamp = now.format(STAMP_FORMAT).to_string();
    let base = dir.join(format!("{FILE_PREFIX}{stamp}{FILE_SUFFIX}"));
    if !base.exists() {
        return base;
    }
    for n in 1..1000 {
        let candidate = dir.join(format!("{FILE_PREFIX}{stamp}-{n}{FILE_SUFFIX}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    base
}

/// Open `path` read-only and verify header + `PRAGMA quick_check` + durable tables.
///
/// Never opens with write flags — write opens against a snapshot have been observed
/// to leave orphan `*-journal` files and a zeroed page 0.
fn verify_backup_file(path: &Path) -> Result<bool, BackupError> {
    if !has_sqlite_header(path)? {
        return Err(BackupError::VerificationFailed {
            path: path.to_string_lossy().into_owned(),
            detail: "SQLite header magic missing".to_string(),
        });
    }

    // URI + immutable avoids journal creation even if a caller upgrades flags later.
    let uri = format!(
        "file:{}?mode=ro&immutable=1",
        path.to_string_lossy().replace('\\', "/")
    );
    let conn = rusqlite::Connection::open_with_flags(
        &uri,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )?;
    let result: String = conn.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
    if !result.eq_ignore_ascii_case("ok") {
        return Ok(false);
    }

    for table in BACKUP_DURABLE_TABLES {
        let exists: i64 = conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
            rusqlite::params![table],
            |r| r.get(0),
        )?;
        if exists == 0 {
            return Err(BackupError::VerificationFailed {
                path: path.to_string_lossy().into_owned(),
                detail: format!("required durable table missing: {table}"),
            });
        }
    }

    // Re-check magic after opening — catch mid-verify corruption.
    if !has_sqlite_header(path)? {
        return Err(BackupError::VerificationFailed {
            path: path.to_string_lossy().into_owned(),
            detail: "SQLite header magic disappeared after verification open".to_string(),
        });
    }
    Ok(true)
}

/// Whether `name` matches the `desk-<...>.db` backup pattern.
fn is_backup_name(name: &str) -> bool {
    name.starts_with(FILE_PREFIX) && name.ends_with(FILE_SUFFIX)
}

/// Parse the timestamp embedded in a backup filename, ignoring any numeric
/// disambiguation suffix.
fn parse_stamp(name: &str) -> Option<DateTime<Utc>> {
    let stem = name.strip_prefix(FILE_PREFIX)?.strip_suffix(FILE_SUFFIX)?;
    // Drop a trailing "-N" disambiguation suffix if present.
    let stamp = match stem.rsplit_once('-') {
        Some((head, tail)) if tail.chars().all(|c| c.is_ascii_digit()) && tail.len() < 4 => head,
        _ => stem,
    };
    NaiveDateTime::parse_from_str(stamp, STAMP_FORMAT)
        .ok()
        .map(|naive| naive.and_utc())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::RiskConfigRecord;

    fn ts(s: &str) -> DateTime<Utc> {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
            .expect("parse test timestamp")
            .and_utc()
    }

    fn seeded_db() -> (tempfile::TempDir, Database) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("data.db");
        let db = Database::open(path.to_string_lossy().as_ref()).expect("open");
        db.save_risk_config(&RiskConfigRecord {
            r_value_points: 7.0,
            ..RiskConfigRecord::default()
        })
        .expect("seed");
        (dir, db)
    }

    fn touch_backup(dir: &Path, name: &str) {
        std::fs::write(dir.join(name), b"not-a-real-db").expect("write fixture");
    }

    #[test]
    fn prune_never_deletes_newest_header_valid_backup() {
        let (_db_dir, db) = seeded_db();
        let dir = tempfile::tempdir().expect("dir");
        let good = perform_backup(&db, dir.path(), ts("2026-06-13 09:00:00"), 0, 0).expect("good");
        assert!(has_sqlite_header(&good.path).unwrap());
        // Corrupt-looking older siblings (no magic) plus age/cap pressure.
        touch_backup(dir.path(), "desk-2026-05-01-090000.db");
        touch_backup(dir.path(), "desk-2026-05-02-090000.db");
        let removed = prune_backups(dir.path(), 1, 1, ts("2026-06-13 09:00:00"));
        assert!(
            good.path.exists(),
            "newest header-valid backup must survive"
        );
        assert!(
            removed.iter().all(|p| p != &good.path),
            "protected path must not appear in removed list"
        );
        assert!(
            has_sqlite_header(&good.path).unwrap(),
            "protected backup header must remain valid"
        );
    }

    #[test]
    fn verify_rejects_zero_header_file() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("desk-2026-06-13-090000.db");
        // Simulate observed production corruption: page 0 zeroed, junk after.
        let mut bytes = vec![0u8; 8192];
        bytes[4096] = 5;
        std::fs::write(&path, &bytes).unwrap();
        assert!(!has_sqlite_header(&path).unwrap());
        let err = verify_backup_file(&path).expect_err("zero header must fail");
        assert!(matches!(err, BackupError::VerificationFailed { .. }));
    }

    #[test]
    fn perform_backup_writes_and_verifies_a_snapshot() {
        let (_db_dir, db) = seeded_db();
        let out_dir = tempfile::tempdir().expect("out dir");
        let outcome =
            perform_backup(&db, out_dir.path(), ts("2026-06-13 09:00:00"), 14, 30).expect("backup");

        assert!(outcome.verified);
        assert!(outcome.path.exists());
        assert!(outcome.size_bytes > 0);
        assert_eq!(
            outcome.path.file_name().unwrap().to_string_lossy(),
            "desk-2026-06-13-090000.db"
        );

        // Snapshot is a usable database with the seeded row.
        let restored = Database::open(outcome.path.to_string_lossy().as_ref()).expect("reopen");
        assert_eq!(restored.load_risk_config().unwrap().r_value_points, 7.0);
    }

    #[test]
    fn same_second_backups_get_unique_names() {
        let (_db_dir, db) = seeded_db();
        let out_dir = tempfile::tempdir().expect("out dir");
        let now = ts("2026-06-13 09:00:00");
        let a = perform_backup(&db, out_dir.path(), now, 0, 0).expect("a");
        let b = perform_backup(&db, out_dir.path(), now, 0, 0).expect("b");
        assert_ne!(a.path, b.path, "second same-second backup must not collide");
        assert!(b.path.exists());
    }

    #[test]
    fn prune_removes_old_and_over_cap_backups() {
        let dir = tempfile::tempdir().expect("dir");
        // Five daily backups; "now" is 2026-06-13.
        for day in 1..=5 {
            touch_backup(dir.path(), &format!("desk-2026-06-0{day}-090000.db"));
        }
        // A foreign file must never be touched.
        touch_backup(dir.path(), "notes.txt");
        std::fs::write(dir.path().join("notes.txt"), b"keep me").unwrap();

        // Retain 30 days (none old enough to expire), cap 3 → the two oldest of
        // five are dropped by the count cap alone.
        let now = ts("2026-06-13 09:00:00");
        let removed = prune_backups(dir.path(), 30, 3, now);
        assert_eq!(removed.len(), 2, "cap of 3 removes the 2 oldest");
        assert_eq!(list_backups(dir.path()).len(), 3);
        assert!(
            dir.path().join("notes.txt").exists(),
            "non-backup untouched"
        );

        // Newest three are the ones kept.
        let kept: Vec<String> = list_backups(dir.path())
            .into_iter()
            .map(|b| b.file_name)
            .collect();
        assert_eq!(
            kept,
            vec![
                "desk-2026-06-05-090000.db",
                "desk-2026-06-04-090000.db",
                "desk-2026-06-03-090000.db",
            ]
        );
    }

    #[test]
    fn prune_age_limit_drops_only_expired() {
        let dir = tempfile::tempdir().expect("dir");
        touch_backup(dir.path(), "desk-2026-05-01-090000.db"); // old
        touch_backup(dir.path(), "desk-2026-06-12-090000.db"); // fresh
        let now = ts("2026-06-13 09:00:00");
        // 14-day retention, no count cap.
        let removed = prune_backups(dir.path(), 14, 0, now);
        assert_eq!(removed.len(), 1);
        let kept: Vec<String> = list_backups(dir.path())
            .into_iter()
            .map(|b| b.file_name)
            .collect();
        assert_eq!(kept, vec!["desk-2026-06-12-090000.db"]);
    }

    #[test]
    fn startup_backup_respects_disabled_and_interval() {
        let (_db_dir, db) = seeded_db();
        let out_dir = tempfile::tempdir().expect("out dir");
        let config = BackupConfig {
            enabled: true,
            directory: out_dir.path().to_string_lossy().into_owned(),
            retention_days: 14,
            max_backups: 30,
            min_interval_hours: 12,
        };

        // Disabled → skipped, no file written.
        let disabled = BackupConfig {
            enabled: false,
            ..config.clone()
        };
        let report = run_startup_backup(&db, &disabled, ts("2026-06-13 09:00:00")).expect("run");
        assert!(matches!(
            report,
            StartupBackupReport::Skipped(SkipReason::Disabled)
        ));
        assert!(list_backups(out_dir.path()).is_empty());

        // First enabled run creates a backup.
        let first = run_startup_backup(&db, &config, ts("2026-06-13 09:00:00")).expect("first");
        assert!(matches!(first, StartupBackupReport::Created(_)));

        // A run 2 hours later is within the 12h interval → skipped.
        let soon = run_startup_backup(&db, &config, ts("2026-06-13 11:00:00")).expect("soon");
        assert!(matches!(
            soon,
            StartupBackupReport::Skipped(SkipReason::WithinInterval { .. })
        ));
        assert_eq!(list_backups(out_dir.path()).len(), 1);

        // A run 13 hours after the first clears the interval → new backup.
        let later = run_startup_backup(&db, &config, ts("2026-06-13 22:00:01")).expect("later");
        assert!(matches!(later, StartupBackupReport::Created(_)));
        assert_eq!(list_backups(out_dir.path()).len(), 2);
    }

    #[test]
    fn parse_stamp_handles_plain_and_suffixed_names() {
        assert_eq!(
            parse_stamp("desk-2026-06-13-090000.db"),
            Some(ts("2026-06-13 09:00:00"))
        );
        assert_eq!(
            parse_stamp("desk-2026-06-13-090000-2.db"),
            Some(ts("2026-06-13 09:00:00"))
        );
        assert_eq!(parse_stamp("notes.txt"), None);
        assert_eq!(parse_stamp("desk-garbage.db"), None);
    }
}
