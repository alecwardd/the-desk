//! SIL-M3d: cold, session-partitioned, rebuildable Journal Frame dumps.
//!
//! SQLite remains the transactional/control plane and the hot window. This
//! module writes hive-partitioned JSONL.zst files (Parquet-equivalent archival
//! layout that DuckDB can later `read_json` without adopting DuckDB here).
//! Partitions never mix RTH with Globex, and never mix NQ with ES.
//!
//! Rebuildable from `.scid`/`.depth` through MarketRouter — same 1 Hz frames
//! as the hot table. Research queries can point at this store via
//! [`FrameStoreKind::Cold`] without new MCP operators.
//!
//! Layout:
//! `{root}/trading_day=YYYY-MM-DD/session_type={RTH|Globex}/root={NQ|ES}/frames.jsonl.zst`

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::db::{Database, DbError, JournalFrameRecord};

use super::root::RouterRoot;

/// On-disk format id written to `_format.json`.
pub const COLD_FRAMES_FORMAT: &str = "desk-journal-frames-v1";
/// Hive file name inside a partition directory.
pub const COLD_FRAMES_FILE_NAME: &str = "frames.jsonl.zst";
/// zstd level — matches raw-tick cold archives (`the-desk-storage`).
pub const COLD_FRAMES_ZSTD_LEVEL: i32 = 3;

/// Research query frame source. Default is the SQLite hot window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FrameStoreKind {
    #[default]
    Hot,
    Cold,
}

impl FrameStoreKind {
    /// Parse wire labels `hot` (default) / `cold`. Unknown values fail closed.
    pub fn parse(raw: &str) -> Result<Self, ColdFrameError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "hot" => Ok(Self::Hot),
            "cold" => Ok(Self::Cold),
            other => Err(ColdFrameError::Invalid(format!(
                "unknown frame store `{other}` (expected hot or cold)"
            ))),
        }
    }

    /// Wire label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hot => "hot",
            Self::Cold => "cold",
        }
    }
}

/// Errors from the cold Journal Frame store.
#[derive(Debug, Error)]
pub enum ColdFrameError {
    #[error("{0}")]
    Io(String),
    #[error("{0}")]
    Invalid(String),
}

impl From<std::io::Error> for ColdFrameError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

impl From<serde_json::Error> for ColdFrameError {
    fn from(e: serde_json::Error) -> Self {
        Self::Invalid(e.to_string())
    }
}

impl From<DbError> for ColdFrameError {
    fn from(e: DbError) -> Self {
        Self::Io(e.to_string())
    }
}

/// Default cold-frame root (`~/.the-desk/journal-frames`). Not a config.toml knob.
pub fn default_cold_frames_dir() -> PathBuf {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".the-desk").join("journal-frames")
}

/// Session-partitioned cold Journal Frame dump store.
#[derive(Debug, Clone)]
pub struct ColdFrameStore {
    root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ColdPartition {
    trading_day: String,
    session_type: String,
    root_symbol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ColdFrameRow {
    clock_ms: f64,
    frame_second: i64,
    root_symbol: String,
    session_type: String,
    session_segment: String,
    trading_day: String,
    payload: serde_json::Value,
}

impl From<&JournalFrameRecord> for ColdFrameRow {
    fn from(frame: &JournalFrameRecord) -> Self {
        Self {
            clock_ms: frame.clock_ms,
            frame_second: frame.frame_second,
            root_symbol: frame.root_symbol.clone(),
            session_type: frame.session_type.clone(),
            session_segment: frame.session_segment.clone(),
            trading_day: frame.trading_day.clone(),
            payload: frame.payload.clone(),
        }
    }
}

impl From<ColdFrameRow> for JournalFrameRecord {
    fn from(row: ColdFrameRow) -> Self {
        Self {
            clock_ms: row.clock_ms,
            frame_second: row.frame_second,
            root_symbol: row.root_symbol,
            session_type: row.session_type,
            session_segment: row.session_segment,
            trading_day: row.trading_day,
            payload: row.payload,
        }
    }
}

impl ColdFrameStore {
    /// Open (or create later on write) a store at `root`. Does not create the
    /// directory until the first upsert.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Store root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Write `_format.json` and ensure the root exists.
    pub fn ensure_root(&self) -> Result<(), ColdFrameError> {
        fs::create_dir_all(&self.root)?;
        let format_path = self.root.join("_format.json");
        if !format_path.exists() {
            let doc = serde_json::json!({
                "format": COLD_FRAMES_FORMAT,
                "encoding": "jsonl.zst",
                "partitioning": ["trading_day", "session_type", "root"],
                "columns": [
                    "clockMs",
                    "frameSecond",
                    "rootSymbol",
                    "sessionType",
                    "sessionSegment",
                    "tradingDay",
                    "payload"
                ],
            });
            fs::write(format_path, serde_json::to_vec_pretty(&doc)?)?;
        }
        Ok(())
    }

    /// Upsert frames into session partitions. Duplicate `(frame_second, root)`
    /// keys keep the first write (INSERT OR IGNORE). Returns newly inserted count.
    pub fn upsert_frames(&self, frames: &[JournalFrameRecord]) -> Result<usize, ColdFrameError> {
        if frames.is_empty() {
            return Ok(0);
        }
        self.ensure_root()?;
        let mut by_part: BTreeMap<ColdPartition, Vec<&JournalFrameRecord>> = BTreeMap::new();
        for frame in frames {
            let part = partition_for(frame)?;
            by_part.entry(part).or_default().push(frame);
        }
        let mut inserted = 0usize;
        for (part, group) in by_part {
            inserted += self.upsert_partition(&part, &group)?;
        }
        Ok(inserted)
    }

    /// Journal Frames in `[start_ms, end_ms]` (inclusive), oldest-first.
    pub fn list_in_window(
        &self,
        start_ms: f64,
        end_ms: f64,
        root_symbol: Option<&str>,
        session_type: Option<&str>,
        limit: usize,
    ) -> Result<Vec<JournalFrameRecord>, ColdFrameError> {
        let mut rows = self.scan_window(start_ms, end_ms, root_symbol, session_type)?;
        rows.sort_by(|a, b| {
            a.clock_ms
                .total_cmp(&b.clock_ms)
                .then_with(|| a.root_symbol.cmp(&b.root_symbol))
                .then_with(|| a.frame_second.cmp(&b.frame_second))
        });
        rows.truncate(limit);
        Ok(rows)
    }

    /// Count of frames matching the same filters as [`Self::list_in_window`].
    pub fn count_in_window(
        &self,
        start_ms: f64,
        end_ms: f64,
        root_symbol: Option<&str>,
        session_type: Option<&str>,
    ) -> Result<i64, ColdFrameError> {
        Ok(self
            .scan_window(start_ms, end_ms, root_symbol, session_type)?
            .len() as i64)
    }

    /// Distinct `session_type` labels present in the clock window.
    pub fn session_types_in_window(
        &self,
        start_ms: f64,
        end_ms: f64,
    ) -> Result<Vec<String>, ColdFrameError> {
        let rows = self.scan_window(start_ms, end_ms, None, None)?;
        let types: BTreeSet<String> = rows.into_iter().map(|f| f.session_type).collect();
        Ok(types.into_iter().collect())
    }

    /// Decode every frame in the store (tests / rebuild comparison).
    pub fn list_all(&self) -> Result<Vec<JournalFrameRecord>, ColdFrameError> {
        let mut rows = Vec::new();
        for part in self.list_partitions()? {
            rows.extend(self.read_partition(&part)?);
        }
        rows.sort_by(|a, b| {
            a.clock_ms
                .total_cmp(&b.clock_ms)
                .then_with(|| a.root_symbol.cmp(&b.root_symbol))
        });
        Ok(rows)
    }

    fn upsert_partition(
        &self,
        part: &ColdPartition,
        incoming: &[&JournalFrameRecord],
    ) -> Result<usize, ColdFrameError> {
        let mut existing = self.read_partition(part)?;
        let mut seen: BTreeSet<(i64, String)> = existing
            .iter()
            .map(|f| (f.frame_second, f.root_symbol.clone()))
            .collect();
        let mut inserted = 0usize;
        for frame in incoming {
            let key = (frame.frame_second, frame.root_symbol.clone());
            if seen.contains(&key) {
                continue;
            }
            seen.insert(key);
            existing.push((*frame).clone());
            inserted += 1;
        }
        if inserted == 0 && self.partition_path(part).exists() {
            return Ok(0);
        }
        existing.sort_by(|a, b| {
            a.clock_ms
                .total_cmp(&b.clock_ms)
                .then_with(|| a.root_symbol.cmp(&b.root_symbol))
                .then_with(|| a.frame_second.cmp(&b.frame_second))
        });
        self.write_partition(part, &existing)?;
        Ok(inserted)
    }

    fn scan_window(
        &self,
        start_ms: f64,
        end_ms: f64,
        root_symbol: Option<&str>,
        session_type: Option<&str>,
    ) -> Result<Vec<JournalFrameRecord>, ColdFrameError> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for part in self.list_partitions()? {
            if let Some(root) = root_symbol {
                if part.root_symbol != root {
                    continue;
                }
            }
            if let Some(session) = session_type {
                if part.session_type != session {
                    continue;
                }
            }
            for frame in self.read_partition(&part)? {
                if frame.clock_ms < start_ms || frame.clock_ms > end_ms {
                    continue;
                }
                if let Some(root) = root_symbol {
                    if frame.root_symbol != root {
                        continue;
                    }
                }
                if let Some(session) = session_type {
                    if frame.session_type != session {
                        continue;
                    }
                }
                out.push(frame);
            }
        }
        Ok(out)
    }

    fn list_partitions(&self) -> Result<Vec<ColdPartition>, ColdFrameError> {
        let mut out = Vec::new();
        if !self.root.exists() {
            return Ok(out);
        }
        for day_ent in fs::read_dir(&self.root)? {
            let day_ent = day_ent?;
            let day_name = day_ent.file_name();
            let Some(day) = hive_value(day_name.to_string_lossy().as_ref(), "trading_day") else {
                continue;
            };
            if !is_trading_day(&day) {
                return Err(ColdFrameError::Invalid(format!(
                    "cold frame partition has invalid trading_day `{day}`"
                )));
            }
            if !day_ent.path().is_dir() {
                continue;
            }
            for session_ent in fs::read_dir(day_ent.path())? {
                let session_ent = session_ent?;
                let session_name = session_ent.file_name();
                let Some(session) =
                    hive_value(session_name.to_string_lossy().as_ref(), "session_type")
                else {
                    continue;
                };
                let session = normalize_stored_session(&session)?;
                if !session_ent.path().is_dir() {
                    continue;
                }
                for root_ent in fs::read_dir(session_ent.path())? {
                    let root_ent = root_ent?;
                    let root_name = root_ent.file_name();
                    let Some(root) = hive_value(root_name.to_string_lossy().as_ref(), "root")
                    else {
                        continue;
                    };
                    RouterRoot::parse(&root).map_err(|e| {
                        ColdFrameError::Invalid(format!("cold frame partition root `{root}`: {e}"))
                    })?;
                    out.push(ColdPartition {
                        trading_day: day.clone(),
                        session_type: session.to_string(),
                        root_symbol: root,
                    });
                }
            }
        }
        out.sort();
        Ok(out)
    }

    fn partition_path(&self, part: &ColdPartition) -> PathBuf {
        self.root
            .join(format!("trading_day={}", part.trading_day))
            .join(format!("session_type={}", part.session_type))
            .join(format!("root={}", part.root_symbol))
            .join(COLD_FRAMES_FILE_NAME)
    }

    fn read_partition(
        &self,
        part: &ColdPartition,
    ) -> Result<Vec<JournalFrameRecord>, ColdFrameError> {
        let path = self.partition_path(part);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let compressed = fs::read(&path)?;
        let raw = zstd::decode_all(compressed.as_slice())
            .map_err(|e| ColdFrameError::Io(format!("zstd decode {}: {e}", path.display())))?;
        let text = String::from_utf8(raw)
            .map_err(|e| ColdFrameError::Invalid(format!("utf8 {}: {e}", path.display())))?;
        let mut rows = Vec::new();
        for (i, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let row: ColdFrameRow = serde_json::from_str(line).map_err(|e| {
                ColdFrameError::Invalid(format!(
                    "cold frame {} line {}: {e}",
                    path.display(),
                    i + 1
                ))
            })?;
            if row.session_type != part.session_type {
                return Err(ColdFrameError::Invalid(format!(
                    "partition {} mixes session_type `{}` with row `{}`",
                    path.display(),
                    part.session_type,
                    row.session_type
                )));
            }
            if row.root_symbol != part.root_symbol {
                return Err(ColdFrameError::Invalid(format!(
                    "partition {} mixes root `{}` with row `{}`",
                    path.display(),
                    part.root_symbol,
                    row.root_symbol
                )));
            }
            rows.push(JournalFrameRecord::from(row));
        }
        Ok(rows)
    }

    fn write_partition(
        &self,
        part: &ColdPartition,
        frames: &[JournalFrameRecord],
    ) -> Result<(), ColdFrameError> {
        let path = self.partition_path(part);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut jsonl = String::new();
        for frame in frames {
            if frame.session_type != part.session_type {
                return Err(ColdFrameError::Invalid(
                    "refusing to mix RTH and Globex in one cold partition".into(),
                ));
            }
            if frame.root_symbol != part.root_symbol {
                return Err(ColdFrameError::Invalid(
                    "refusing to mix NQ and ES in one cold partition".into(),
                ));
            }
            let row = ColdFrameRow::from(frame);
            jsonl.push_str(&serde_json::to_string(&row)?);
            jsonl.push('\n');
        }
        let compressed = zstd::encode_all(jsonl.as_bytes(), COLD_FRAMES_ZSTD_LEVEL)
            .map_err(|e| ColdFrameError::Io(format!("zstd encode: {e}")))?;
        let tmp = path.with_file_name("frames.jsonl.zst.tmp");
        {
            let mut file = fs::File::create(&tmp)?;
            file.write_all(&compressed)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        Ok(())
    }
}

/// Hot SQLite window or cold session-partitioned dumps — same list/count shape.
#[derive(Clone, Copy)]
pub enum JournalFrameRead<'a> {
    /// SQLite `journal_frames` hot window (default).
    Hot(&'a Database),
    /// Session-partitioned cold dumps.
    Cold(&'a ColdFrameStore),
}

impl JournalFrameRead<'_> {
    /// Whether this read is served from cold dumps.
    pub fn is_cold(&self) -> bool {
        matches!(self, Self::Cold(_))
    }

    /// Frames in `[start_ms, end_ms]`.
    pub fn list_in_window(
        &self,
        start_ms: f64,
        end_ms: f64,
        root_symbol: Option<&str>,
        session_type: Option<&str>,
        limit: usize,
    ) -> Result<Vec<JournalFrameRecord>, ColdFrameError> {
        match self {
            Self::Hot(db) => Ok(db.list_journal_frames_in_window(
                start_ms,
                end_ms,
                root_symbol,
                session_type,
                limit,
            )?),
            Self::Cold(store) => {
                store.list_in_window(start_ms, end_ms, root_symbol, session_type, limit)
            }
        }
    }

    /// Count in `[start_ms, end_ms]`.
    pub fn count_in_window(
        &self,
        start_ms: f64,
        end_ms: f64,
        root_symbol: Option<&str>,
        session_type: Option<&str>,
    ) -> Result<i64, ColdFrameError> {
        match self {
            Self::Hot(db) => Ok(db.count_journal_frames_in_window(
                start_ms,
                end_ms,
                root_symbol,
                session_type,
            )?),
            Self::Cold(store) => store.count_in_window(start_ms, end_ms, root_symbol, session_type),
        }
    }

    /// Distinct session types in the clock window (mixed RTH+Globex detection).
    pub fn session_types_in_window(
        &self,
        start_ms: f64,
        end_ms: f64,
    ) -> Result<Vec<String>, ColdFrameError> {
        match self {
            Self::Hot(db) => Ok(db.list_journal_session_types_in_window(start_ms, end_ms)?),
            Self::Cold(store) => store.session_types_in_window(start_ms, end_ms),
        }
    }
}

fn partition_for(frame: &JournalFrameRecord) -> Result<ColdPartition, ColdFrameError> {
    let trading_day = if is_trading_day(&frame.trading_day) {
        frame.trading_day.clone()
    } else {
        crate::trading_day_from_timestamp_ms(frame.clock_ms)
    };
    if !is_trading_day(&trading_day) {
        return Err(ColdFrameError::Invalid(format!(
            "Journal Frame missing trading_day for cold partition (clock_ms={})",
            frame.clock_ms
        )));
    }
    let session = normalize_stored_session(&frame.session_type)?;
    RouterRoot::parse(&frame.root_symbol).map_err(|e| {
        ColdFrameError::Invalid(format!(
            "Journal Frame root `{}` is not a MarketRouter root: {e}",
            frame.root_symbol
        ))
    })?;
    Ok(ColdPartition {
        trading_day,
        session_type: session.to_string(),
        root_symbol: frame.root_symbol.clone(),
    })
}

fn normalize_stored_session(raw: &str) -> Result<&'static str, ColdFrameError> {
    if raw.eq_ignore_ascii_case("rth") {
        Ok("RTH")
    } else if raw.eq_ignore_ascii_case("globex") {
        Ok("Globex")
    } else if raw.eq_ignore_ascii_case("unknown") {
        Ok("Unknown")
    } else {
        Err(ColdFrameError::Invalid(format!(
            "session_type `{raw}` is not RTH, Globex, or Unknown"
        )))
    }
}

fn is_trading_day(raw: &str) -> bool {
    let b = raw.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b[0..4].iter().all(u8::is_ascii_digit)
        && b[5..7].iter().all(u8::is_ascii_digit)
        && b[8..10].iter().all(u8::is_ascii_digit)
}

fn hive_value(dir_name: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    dir_name.strip_prefix(&prefix).map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn frame(clock: f64, root: &str, session: &str, day: &str, price: f64) -> JournalFrameRecord {
        let second = (clock / 1000.0).floor() as i64;
        JournalFrameRecord {
            clock_ms: clock,
            frame_second: second,
            root_symbol: root.into(),
            session_type: session.into(),
            session_segment: "None".into(),
            trading_day: day.into(),
            payload: json!({ "lastPrice": price, "rootSymbol": root, "sessionType": session }),
        }
    }

    #[test]
    fn rth_and_globex_land_in_separate_partitions() {
        let dir = tempfile::tempdir().expect("tmp");
        let store = ColdFrameStore::new(dir.path());
        let rth = 1_704_207_600_000.0;
        let globex: f64 = 1_704_243_600_000.0;
        store
            .upsert_frames(&[
                frame(rth, "NQ", "RTH", "2024-01-02", 20_000.0),
                frame(rth, "ES", "RTH", "2024-01-02", 5_000.0),
                frame(globex, "NQ", "Globex", "2024-01-03", 20_010.0),
            ])
            .expect("upsert");
        let parts = store.list_partitions().expect("parts");
        let sessions: BTreeSet<_> = parts.iter().map(|p| p.session_type.as_str()).collect();
        assert!(sessions.contains("RTH"));
        assert!(sessions.contains("Globex"));
        assert!(!parts.iter().any(|p| p.session_type == "RTH"
            && p.root_symbol == "NQ"
            && store
                .read_partition(p)
                .expect("read")
                .iter()
                .any(|f| f.session_type == "Globex")));
        assert_eq!(store.list_all().expect("all").len(), 3);
        assert!(dir
            .path()
            .join("trading_day=2024-01-02")
            .join("session_type=RTH")
            .join("root=NQ")
            .join(COLD_FRAMES_FILE_NAME)
            .exists());
        assert!(dir
            .path()
            .join("trading_day=2024-01-02")
            .join("session_type=RTH")
            .join("root=ES")
            .join(COLD_FRAMES_FILE_NAME)
            .exists());
        assert!(dir
            .path()
            .join("trading_day=2024-01-03")
            .join("session_type=Globex")
            .join("root=NQ")
            .join(COLD_FRAMES_FILE_NAME)
            .exists());
        let format: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.path().join("_format.json")).expect("format"))
                .expect("format json");
        assert_eq!(format["format"], COLD_FRAMES_FORMAT);
        assert_eq!(format["encoding"], "jsonl.zst");
    }

    #[test]
    fn duplicate_frame_second_root_is_ignored() {
        let dir = tempfile::tempdir().expect("tmp");
        let store = ColdFrameStore::new(dir.path());
        let clock = 1_704_207_600_000.0;
        let first = frame(clock, "NQ", "RTH", "2024-01-02", 20_000.0);
        let mut dup = first.clone();
        dup.payload = json!({ "lastPrice": 99.0, "rootSymbol": "NQ" });
        assert_eq!(store.upsert_frames(&[first]).expect("first"), 1);
        assert_eq!(store.upsert_frames(&[dup]).expect("dup"), 0);
        let all = store.list_all().expect("all");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].payload["lastPrice"], 20_000.0);
    }

    #[test]
    fn rebuild_writes_identical_decoded_rows() {
        let a_dir = tempfile::tempdir().expect("a");
        let b_dir = tempfile::tempdir().expect("b");
        let frames = vec![
            frame(1_704_207_600_000.0, "NQ", "RTH", "2024-01-02", 20_000.0),
            frame(1_704_207_600_010.0, "ES", "RTH", "2024-01-02", 5_000.0),
        ];
        ColdFrameStore::new(a_dir.path())
            .upsert_frames(&frames)
            .expect("a");
        ColdFrameStore::new(b_dir.path())
            .upsert_frames(&frames)
            .expect("b");
        let a = ColdFrameStore::new(a_dir.path()).list_all().expect("a");
        let b = ColdFrameStore::new(b_dir.path()).list_all().expect("b");
        assert_eq!(a.len(), b.len());
        for (l, r) in a.iter().zip(b.iter()) {
            assert_eq!(l.frame_second, r.frame_second);
            assert_eq!(l.root_symbol, r.root_symbol);
            assert_eq!(l.session_type, r.session_type);
            assert_eq!(l.payload["lastPrice"], r.payload["lastPrice"]);
            assert_eq!(l.clock_ms, r.clock_ms);
        }
    }

    #[test]
    fn unknown_store_label_fails_closed() {
        let err = FrameStoreKind::parse("duckdb").unwrap_err();
        assert!(err.to_string().contains("hot or cold"));
    }
}
