//! SIL-M3e three-path Episode Query benchmark (DuckDB go/no-go).
//!
//! Path A — status-quo [`super::query_kernel::query_episodes`] over SQLite
//! `journal_frames` JSON blobs (today's hot window).
//! Path B — benchmark-only dense projection of the flagship fields (SQLite
//! side table, rebuildable from Journal Frames / M3d dumps). Not a live-path
//! rewrite of `journal_frames`.
//! Path C — DuckDB `read_json` of the existing M3d hive (`desk-journal-frames-v1`
//! JSONL.zst). Compile-gated behind Cargo feature `duckdb-bench` (off by default).
//!
//! This module is a harness, not an MCP tool and not a tenth kernel operator.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

#[cfg(feature = "duckdb-bench")]
use std::path::PathBuf;

use chrono::{Datelike, TimeZone, Weekday};
use chrono_tz::US::Eastern;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::catalog::{
    accept_levels_only_entry, DerivedLevels, PositioningEntryInput, PositioningRecord,
    PositioningWall, TrustCeiling, TrustLevel, LEVELS_ONLY_RECORD_KIND,
};
use crate::db::{journal_frame_second_from_ts, Database, DbError, JournalFrameRecord};
#[cfg(feature = "duckdb-bench")]
use crate::engine::cold_frames::COLD_FRAMES_FILE_NAME;
use crate::engine::{ColdFrameError, ColdFrameStore, JournalFrameRead};
use crate::research::{reliability_tier, ReliabilityTier};

use super::query_kernel::{
    flagship_episode_predicates, query_episodes, query_episodes_with_opts, QueryEpisodesOpts,
    QueryEpisodesRequest, QueryEpisodesResult, QueryWindow, DEFAULT_POSITIONING_NEAR_TICKS,
    FLAGSHIP_SELLER_AGGRESSION_SESSION_DELTA, FUTURES_TICK_SIZE, QUERY_EPISODES_MAX_MATCHES,
};

/// Owner SLO: Path B p95 over ~2 weeks of 1 Hz frames (upper end of 2–3 s).
pub const SLO_P95_TWO_WEEK_MS: f64 = 3_000.0;
/// Owner SLO: Path B p95 over ~90 days / ~1 year once that span exists.
pub const SLO_P95_LONG_HORIZON_MS: f64 = 10_000.0;
/// Minimum weekday span treated as "~2 weeks of RTH" (10 sessions).
pub const TWO_WEEK_RTH_DAYS: usize = 10;
/// Flagship ES last used by the synthetic generator (near Positioning flip).
pub const BENCH_ES_PRICE: f64 = 5_750.0;
/// Flagship NQ last used by the synthetic generator.
pub const BENCH_NQ_PRICE: f64 = 20_000.0;
/// Dense side-table name. Not part of the live schema / migrations.
pub const DENSE_TABLE: &str = "eq_bench_dense";

/// Errors from the three-path harness.
#[derive(Debug, Error)]
pub enum EpisodeBenchError {
    #[error("{0}")]
    Query(#[from] super::query_kernel::QueryKernelError),
    #[error("{0}")]
    Db(#[from] DbError),
    #[error("{0}")]
    Cold(#[from] ColdFrameError),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("{0}")]
    Invalid(String),
    #[error("DuckDB path C unavailable: {0}")]
    DuckDbUnavailable(String),
}

/// How Path C was compiled / executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PathCAvailability {
    /// Built with `--features duckdb-bench` and the query ran.
    Ran,
    /// Feature off (default). Default CI / clippy must stay here.
    FeatureOff,
    /// Feature on, but DuckDB failed to open or scan the hive.
    RuntimeBlocked,
}

/// Adopt vs defer DuckDB. Default bias is defer unless the SLO is honestly missed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DuckDbVerdict {
    Adopt,
    Defer,
}

/// Synthetic 1 Hz NQ+ES Journal Frame dataset (temp SQLite + M3d hive).
#[derive(Debug, Clone)]
pub struct SyntheticDataset {
    pub start_ms: f64,
    pub end_ms: f64,
    pub rth_days: usize,
    pub frame_count: usize,
    pub planted_matches: usize,
    pub planted_seconds: Vec<i64>,
}

/// Timing for one path (query only; rebuild is separate).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PathTiming {
    pub path: String,
    pub samples_ms: Vec<f64>,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub n_iters: usize,
    pub match_count: usize,
    pub fingerprint: String,
    pub reliability_tier: String,
    pub trust_level: String,
    pub rebuild_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

/// Written go/no-go attached to the measured evidence.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DuckDbDecision {
    pub verdict: DuckDbVerdict,
    pub reason: String,
    pub slo_p95_two_week_ms: f64,
    pub slo_p95_long_horizon_ms: f64,
    pub measured_rth_days: usize,
    pub measured_frame_count: usize,
    pub span_ms: f64,
    pub long_horizon_available: bool,
    pub path_b_hand_built_columnar: bool,
    pub path_c_availability: PathCAvailability,
}

/// Full harness report (correctness + latency).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BenchReport {
    pub flagship: String,
    pub machine: MachineClass,
    pub dataset: DatasetSummary,
    pub correctness: CorrectnessReport,
    pub timings: Vec<PathTiming>,
    pub decision: DuckDbDecision,
}

/// Host class recorded with latency numbers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineClass {
    pub os: String,
    pub arch: String,
    pub available_parallelism: usize,
}

/// Dataset scale actually measured (never commit the bytes).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetSummary {
    pub rth_days: usize,
    pub frame_count: usize,
    pub planted_matches: usize,
    pub start_ms: f64,
    pub end_ms: f64,
    pub span_ms: f64,
    pub session_type: String,
}

/// Golden/fixture-scale agreement across paths.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CorrectnessReport {
    pub agreed: bool,
    pub match_count: usize,
    pub fingerprint: String,
    pub reliability_tier: String,
    pub trust_level: String,
    pub trust_ceiling: String,
    pub path_c: PathCAvailability,
    pub notes: Vec<String>,
}

/// Flagship Episode Query request (same predicates / session / N contracts as M3c).
pub fn flagship_request(start_ms: f64, end_ms: f64, session_type: &str) -> QueryEpisodesRequest {
    QueryEpisodesRequest {
        window: QueryWindow {
            start_ms: Some(start_ms),
            end_ms: Some(end_ms),
            session_type: Some(session_type.into()),
            symbols: Some(vec!["NQ".into(), "ES".into()]),
        },
        predicates: flagship_episode_predicates(),
        forward_direction: Some("short".into()),
    }
}

/// Stable fingerprint of matches (sorted `frame_second` list).
pub fn match_fingerprint(seconds: &[i64]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"sil-m3e-v1\n");
    let mut sorted = seconds.to_vec();
    sorted.sort_unstable();
    for second in sorted {
        hasher.update(format!("{second}\n").as_bytes());
    }
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Host description for the decision note / PR body.
pub fn machine_class() -> MachineClass {
    MachineClass {
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        available_parallelism: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
    }
}

/// Whether Path C was compiled in.
pub fn duckdb_feature_enabled() -> bool {
    cfg!(feature = "duckdb-bench")
}

/// Seed a Levels-Only Record whose flip is the synthetic ES last (flagship near).
pub fn seed_flagship_positioning(db: &Database, as_of_ms: f64) -> Result<(), EpisodeBenchError> {
    let levels = DerivedLevels {
        flip: BENCH_ES_PRICE,
        walls: vec![PositioningWall {
            strike: 5_800.0,
            role: "call_wall".into(),
        }],
        balance: 5_745.0,
        upside_test: 5_825.0,
        downside_test: 5_680.0,
    };
    let record = accept_levels_only_entry(PositioningEntryInput {
        id: Some("pos-m3e-bench".into()),
        record_kind: Some(LEVELS_ONLY_RECORD_KIND.into()),
        completeness: Some(LEVELS_ONLY_RECORD_KIND.into()),
        trading_day: None,
        captured_at_ms: Some(as_of_ms),
        as_of_ms: Some(as_of_ms),
        derived_levels: Some(levels),
        now_ms: as_of_ms,
        ..Default::default()
    })
    .map_err(|e| EpisodeBenchError::Invalid(e.to_string()))?;
    db.upsert_positioning_record(&record, as_of_ms)?;
    Ok(())
}

fn es_payload(
    price: f64,
    delta: f64,
    poor_low: bool,
    bid_replenishing: Option<bool>,
) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "lastPrice": price,
        "sessionDelta": delta,
        "poorLow": poor_low,
        "rootSymbol": "ES",
        "sessionType": "RTH",
    });
    if let Some(flag) = bid_replenishing {
        payload["domSummary"] = serde_json::json!({ "bidReplenishing": flag });
    } else {
        payload["domSummary"] = serde_json::json!({});
    }
    payload
}

fn nq_payload(delta: f64) -> serde_json::Value {
    serde_json::json!({
        "lastPrice": BENCH_NQ_PRICE,
        "sessionDelta": delta,
        "rootSymbol": "NQ",
        "sessionType": "RTH",
    })
}

fn frame(
    clock_ms: f64,
    root: &str,
    trading_day: &str,
    payload: serde_json::Value,
) -> JournalFrameRecord {
    JournalFrameRecord {
        clock_ms,
        frame_second: journal_frame_second_from_ts(clock_ms).unwrap_or((clock_ms / 1000.0) as i64),
        root_symbol: root.into(),
        session_type: "RTH".into(),
        session_segment: "None".into(),
        trading_day: trading_day.into(),
        payload,
    }
}

fn rth_weekdays(year: i32, month: u32, first_day: u32, want: usize) -> Vec<chrono::NaiveDate> {
    let mut out = Vec::new();
    let mut d = chrono::NaiveDate::from_ymd_opt(year, month, first_day)
        .expect("synthetic generator start date");
    while out.len() < want {
        let wd = d.weekday();
        if wd != Weekday::Sat && wd != Weekday::Sun {
            out.push(d);
        }
        d += chrono::Duration::days(1);
    }
    out
}

fn rth_open_ms(date: chrono::NaiveDate) -> f64 {
    let dt = date.and_hms_opt(9, 30, 0).expect("rth open");
    Eastern
        .from_local_datetime(&dt)
        .single()
        .expect("ET datetime")
        .timestamp_millis() as f64
}

fn rth_last_second_ms(date: chrono::NaiveDate) -> f64 {
    // RTH is [09:30, 16:15) ET → last included second is 16:14:59.
    let dt = date.and_hms_opt(16, 14, 59).expect("rth last");
    Eastern
        .from_local_datetime(&dt)
        .single()
        .expect("ET datetime")
        .timestamp_millis() as f64
}

fn plant_clock_ms(date: chrono::NaiveDate) -> f64 {
    let dt = date.and_hms_opt(10, 0, 0).expect("plant");
    Eastern
        .from_local_datetime(&dt)
        .single()
        .expect("ET datetime")
        .timestamp_millis() as f64
}

/// Tiny golden fixture (M3c-scale): one matching second, one miss, same Positioning.
pub fn seed_golden_fixture(
    db: &Database,
    hive: &ColdFrameStore,
) -> Result<SyntheticDataset, EpisodeBenchError> {
    // 2024-01-02 10:00 ET (RTH) — same clock as the M3c flagship fixture.
    let match_clock = 1_704_207_600_000.0;
    let miss_clock = match_clock + 1_000.0;
    let day = "2024-01-02";
    let frames = vec![
        frame(
            match_clock,
            "ES",
            day,
            es_payload(BENCH_ES_PRICE, -500.0, true, Some(true)),
        ),
        frame(match_clock, "NQ", day, nq_payload(50.0)),
        frame(
            miss_clock,
            "ES",
            day,
            es_payload(5_900.0, -500.0, true, Some(true)),
        ),
        frame(miss_clock, "NQ", day, nq_payload(50.0)),
    ];
    db.insert_journal_frames(&frames)?;
    hive.upsert_frames(&frames)?;
    hive.compact()?;
    seed_flagship_positioning(db, match_clock)?;
    let planted = journal_frame_second_from_ts(match_clock).expect("second");
    Ok(SyntheticDataset {
        start_ms: match_clock,
        end_ms: miss_clock + 500.0,
        rth_days: 1,
        frame_count: 4,
        planted_matches: 1,
        planted_seconds: vec![planted],
    })
}

/// Generate 1 Hz NQ+ES RTH Journal Frames into a temp SQLite + M3d hive.
///
/// Does not commit the dataset. Does not require a Sierra `.scid` feed.
pub fn generate_rth_dataset(
    db: &Database,
    hive: &ColdFrameStore,
    rth_days: usize,
) -> Result<SyntheticDataset, EpisodeBenchError> {
    if rth_days == 0 {
        return Err(EpisodeBenchError::Invalid(
            "rth_days must be at least 1".into(),
        ));
    }
    db.sqlite_connection().execute_batch(
        "PRAGMA synchronous=OFF; PRAGMA temp_store=MEMORY; DELETE FROM journal_frames;",
    )?;
    let days = rth_weekdays(2024, 1, 8, rth_days);
    let start_ms = rth_open_ms(days[0]);
    let end_ms = rth_last_second_ms(*days.last().expect("days"));
    seed_flagship_positioning(db, start_ms)?;

    let mut frame_count = 0usize;
    let mut planted_seconds = Vec::new();
    const BATCH_SECONDS: i64 = 1_000;

    for date in &days {
        let day = date.format("%Y-%m-%d").to_string();
        let open = rth_open_ms(*date);
        let last = rth_last_second_ms(*date);
        let plant = plant_clock_ms(*date);
        planted_seconds.push(journal_frame_second_from_ts(plant).expect("plant second"));

        let mut batch = Vec::with_capacity((BATCH_SECONDS * 2) as usize);
        let mut clock = open;
        while clock <= last {
            let is_match = (clock - plant).abs() < 0.5;
            let es = if is_match {
                es_payload(BENCH_ES_PRICE, -500.0, true, Some(true))
            } else {
                es_payload(BENCH_ES_PRICE + 50.0, -50.0, false, None)
            };
            let nq = if is_match {
                nq_payload(50.0)
            } else {
                nq_payload(-20.0)
            };
            batch.push(frame(clock, "ES", &day, es));
            batch.push(frame(clock, "NQ", &day, nq));
            if batch.len() >= (BATCH_SECONDS * 2) as usize {
                frame_count += db.insert_journal_frames(&batch)?;
                hive.upsert_frames(&batch)?;
                batch.clear();
            }
            clock += 1_000.0;
        }
        if !batch.is_empty() {
            frame_count += db.insert_journal_frames(&batch)?;
            hive.upsert_frames(&batch)?;
        }
    }
    hive.compact()?;
    Ok(SyntheticDataset {
        start_ms,
        end_ms,
        rth_days,
        frame_count,
        planted_matches: planted_seconds.len(),
        planted_seconds,
    })
}

fn seconds_from_result(result: &QueryEpisodesResult) -> Vec<i64> {
    let mut seconds: Vec<i64> = result
        .matches
        .iter()
        .filter_map(|m| m.frame_ref.journal_frame_second)
        .collect();
    seconds.sort_unstable();
    seconds
}

fn contracts_ok(result: &QueryEpisodesResult) -> Result<(), EpisodeBenchError> {
    if result.meta.trust_level != TrustLevel::L0 {
        return Err(EpisodeBenchError::Invalid(
            "Episode Query trust level must stay L0".into(),
        ));
    }
    if result.meta.trust_ceiling != TrustCeiling::L3 {
        return Err(EpisodeBenchError::Invalid(
            "Trust Ceiling must stay L3".into(),
        ));
    }
    if result.meta.mutation_authority || result.meta.order_authority {
        return Err(EpisodeBenchError::Invalid(
            "Episode Query must not carry mutation or order authority".into(),
        ));
    }
    if result.meta.n != result.matches.len() {
        return Err(EpisodeBenchError::Invalid(
            "N must equal match count".into(),
        ));
    }
    if result.meta.reliability_tier != reliability_tier(result.meta.n) {
        return Err(EpisodeBenchError::Invalid(
            "reliabilityTier must follow AGENT.md sample-size policy".into(),
        ));
    }
    Ok(())
}

/// Path A: live `query_episodes` over SQLite JSON blobs.
pub fn path_a(
    db: &Database,
    req: &QueryEpisodesRequest,
    enforce_product_window_cap: bool,
) -> Result<QueryEpisodesResult, EpisodeBenchError> {
    let result = if enforce_product_window_cap {
        query_episodes(db, req)?
    } else {
        query_episodes_with_opts(
            db,
            JournalFrameRead::Hot(db),
            req,
            QueryEpisodesOpts {
                enforce_product_window_cap: false,
            },
        )?
    };
    contracts_ok(&result)?;
    Ok(result)
}

fn json_f64(payload: &serde_json::Value, key: &str) -> Option<f64> {
    payload.get(key).and_then(|v| {
        v.as_f64()
            .or_else(|| v.as_i64().map(|i| i as f64))
            .or_else(|| v.as_u64().map(|i| i as f64))
    })
}

fn json_bool_int(value: Option<&serde_json::Value>) -> Option<i64> {
    match value? {
        serde_json::Value::Bool(b) => Some(i64::from(*b)),
        serde_json::Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| i64::from(f != 0.0))),
        serde_json::Value::String(s) if s.eq_ignore_ascii_case("true") => Some(1),
        serde_json::Value::String(s) if s.eq_ignore_ascii_case("false") => Some(0),
        _ => None,
    }
}

fn positioning_level_prices(record: &PositioningRecord) -> Vec<f64> {
    let mut out = vec![
        record.derived_levels.flip,
        record.derived_levels.balance,
        record.derived_levels.upside_test,
        record.derived_levels.downside_test,
    ];
    for wall in &record.derived_levels.walls {
        out.push(wall.strike);
    }
    out.retain(|p| p.is_finite());
    out
}

fn select_positioning(records: &[PositioningRecord], clock_ms: f64) -> Option<&PositioningRecord> {
    let idx = records.partition_point(|r| r.as_of_ms <= clock_ms);
    idx.checked_sub(1).map(|i| &records[i])
}

fn es_near_positioning(price: f64, levels: &[f64]) -> bool {
    let band = DEFAULT_POSITIONING_NEAR_TICKS * FUTURES_TICK_SIZE;
    levels.iter().any(|lvl| (price - *lvl).abs() <= band)
}

/// Rebuild the benchmark-only dense projection from `journal_frames`.
///
/// Drops/recreates a side table. Does not ALTER the live `journal_frames`
/// schema. Rebuildable from Journal Frames / M3d dumps.
pub fn rebuild_dense_projection(db: &Database) -> Result<usize, EpisodeBenchError> {
    let conn = db.sqlite_connection();
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {DENSE_TABLE};
         CREATE TABLE {DENSE_TABLE} (
           frame_second INTEGER PRIMARY KEY,
           clock_ms REAL NOT NULL,
           session_type TEXT NOT NULL,
           trading_day TEXT NOT NULL,
           es_last_price REAL,
           es_session_delta REAL,
           es_poor_low INTEGER,
           es_bid_replenishing INTEGER,
           nq_last_price REAL,
           nq_session_delta REAL
         );
         CREATE INDEX idx_eq_bench_dense_clock ON {DENSE_TABLE}(clock_ms, session_type);"
    ))?;

    let frames = db.list_journal_frames()?;
    let mut by_second: BTreeMap<i64, BTreeMap<String, JournalFrameRecord>> = BTreeMap::new();
    for frame in frames {
        by_second
            .entry(frame.frame_second)
            .or_default()
            .insert(frame.root_symbol.clone(), frame);
    }

    let tx = conn.unchecked_transaction()?;
    let inserted = {
        let mut stmt = tx.prepare(&format!(
            "INSERT INTO {DENSE_TABLE}
             (frame_second, clock_ms, session_type, trading_day,
              es_last_price, es_session_delta, es_poor_low, es_bid_replenishing,
              nq_last_price, nq_session_delta)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
        ))?;
        let mut n = 0usize;
        for (second, roots) in &by_second {
            let sample = roots.values().next();
            let Some(sample) = sample else {
                continue;
            };
            let es = roots.get("ES");
            let nq = roots.get("NQ");
            let clock = roots.values().map(|f| f.clock_ms).fold(0.0_f64, f64::max);
            n += stmt.execute(params![
                *second,
                clock,
                sample.session_type.as_str(),
                sample.trading_day.as_str(),
                es.and_then(|f| json_f64(&f.payload, "lastPrice")),
                es.and_then(|f| json_f64(&f.payload, "sessionDelta")),
                es.and_then(|f| json_bool_int(f.payload.get("poorLow"))),
                es.and_then(|f| json_bool_int(
                    f.payload
                        .get("domSummary")
                        .and_then(|d| d.get("bidReplenishing"))
                )),
                nq.and_then(|f| json_f64(&f.payload, "lastPrice")),
                nq.and_then(|f| json_f64(&f.payload, "sessionDelta")),
            ])?;
        }
        n
    };
    tx.commit()?;
    Ok(inserted)
}

/// Path B: SQL over promoted flagship columns, then the same near-Positioning band as M3c.
pub fn path_b(
    db: &Database,
    start_ms: f64,
    end_ms: f64,
    session_type: &str,
) -> Result<(Vec<i64>, usize, ReliabilityTier), EpisodeBenchError> {
    let conn = db.sqlite_connection();
    let exists: i64 = conn.query_row(
        "SELECT COUNT(1) FROM sqlite_master WHERE type='table' AND name=?1",
        params![DENSE_TABLE],
        |row| row.get(0),
    )?;
    if exists == 0 {
        return Err(EpisodeBenchError::Invalid(
            "dense projection missing; call rebuild_dense_projection first".into(),
        ));
    }

    let positioning = db.list_positioning_records_covering_window(start_ms, end_ms)?;
    let mut stmt = conn.prepare(&format!(
        "SELECT frame_second, clock_ms, es_last_price
         FROM {DENSE_TABLE}
         WHERE clock_ms >= ?1 AND clock_ms <= ?2
           AND session_type = ?3
           AND es_session_delta IS NOT NULL
           AND es_session_delta <= ?4
           AND es_poor_low = 1
           AND es_bid_replenishing = 1
           AND nq_session_delta IS NOT NULL
           AND nq_session_delta >= 0
         ORDER BY clock_ms ASC, frame_second ASC
         LIMIT ?5"
    ))?;
    let candidates = stmt.query_map(
        params![
            start_ms,
            end_ms,
            session_type,
            FLAGSHIP_SELLER_AGGRESSION_SESSION_DELTA,
            QUERY_EPISODES_MAX_MATCHES as i64
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, Option<f64>>(2)?,
            ))
        },
    )?;

    let mut seconds = Vec::new();
    for row in candidates {
        let (frame_second, clock_ms, es_last) = row?;
        let Some(price) = es_last else {
            continue;
        };
        let Some(pos) = select_positioning(&positioning, clock_ms) else {
            continue;
        };
        if !es_near_positioning(price, &positioning_level_prices(pos)) {
            continue;
        }
        seconds.push(frame_second);
    }
    seconds.sort_unstable();
    let n = seconds.len();
    Ok((seconds, n, reliability_tier(n)))
}

/// Path C: DuckDB over the M3d JSONL.zst hive. Feature-gated.
pub fn path_c(
    hive_root: &Path,
    db: &Database,
    start_ms: f64,
    end_ms: f64,
    session_type: &str,
) -> Result<(Vec<i64>, PathCAvailability), EpisodeBenchError> {
    #[cfg(not(feature = "duckdb-bench"))]
    {
        let _ = (hive_root, db, start_ms, end_ms, session_type);
        Err(EpisodeBenchError::DuckDbUnavailable(
            "compiled without feature duckdb-bench (default; CI does not require DuckDB)".into(),
        ))
    }
    #[cfg(feature = "duckdb-bench")]
    {
        path_c_duckdb(hive_root, db, start_ms, end_ms, session_type)
    }
}

#[cfg(feature = "duckdb-bench")]
fn path_c_duckdb(
    hive_root: &Path,
    db: &Database,
    start_ms: f64,
    end_ms: f64,
    session_type: &str,
) -> Result<(Vec<i64>, PathCAvailability), EpisodeBenchError> {
    let conn = duckdb::Connection::open_in_memory().map_err(|e| {
        EpisodeBenchError::DuckDbUnavailable(format!("failed to open in-memory DuckDB: {e}"))
    })?;
    let zst_files = list_hive_files(hive_root, COLD_FRAMES_FILE_NAME)?;
    if zst_files.is_empty() {
        return Err(EpisodeBenchError::DuckDbUnavailable(format!(
            "no {COLD_FRAMES_FILE_NAME} files under {}",
            hive_root.display()
        )));
    }
    let rows = match query_duckdb_hive(&conn, &zst_files, true, start_ms, end_ms, session_type) {
        Ok(seconds) => seconds,
        Err(first) => {
            let decoded = decode_hive_to_jsonl(hive_root)?;
            let plain_files = list_hive_files(&decoded, "frames.jsonl")?;
            query_duckdb_hive(
                &conn,
                &plain_files,
                false,
                start_ms,
                end_ms,
                session_type,
            )
            .map_err(|second| {
                EpisodeBenchError::DuckDbUnavailable(format!(
                    "read_json of JSONL.zst failed ({first}); decompressed JSONL also failed ({second})"
                ))
            })?
        }
    };

    let positioning = db
        .list_positioning_records_covering_window(start_ms, end_ms)
        .map_err(EpisodeBenchError::Db)?;
    let mut seconds = Vec::new();
    for (frame_second, clock_ms, es_last) in rows {
        let Some(price) = es_last else {
            continue;
        };
        let Some(pos) = select_positioning(&positioning, clock_ms) else {
            continue;
        };
        if !es_near_positioning(price, &positioning_level_prices(pos)) {
            continue;
        }
        seconds.push(frame_second);
    }
    seconds.sort_unstable();
    seconds.dedup();
    seconds.truncate(QUERY_EPISODES_MAX_MATCHES);
    Ok((seconds, PathCAvailability::Ran))
}

#[cfg(feature = "duckdb-bench")]
fn hive_files_for_root(files: &[PathBuf], root: &str) -> Vec<PathBuf> {
    let unix = format!("/root={root}/");
    let win = format!("\\root={root}\\");
    files
        .iter()
        .filter(|p| {
            let s = p.to_string_lossy();
            s.contains(&unix) || s.contains(&win)
        })
        .cloned()
        .collect()
}

#[cfg(feature = "duckdb-bench")]
fn query_duckdb_hive(
    conn: &duckdb::Connection,
    files: &[PathBuf],
    zstd: bool,
    start_ms: f64,
    end_ms: f64,
    session_type: &str,
) -> Result<Vec<(i64, f64, Option<f64>)>, String> {
    let es_files = hive_files_for_root(files, "ES");
    let nq_files = hive_files_for_root(files, "NQ");
    if es_files.is_empty() || nq_files.is_empty() {
        return Err(format!(
            "hive missing ES ({}) or NQ ({}) partition files",
            es_files.len(),
            nq_files.len()
        ));
    }
    let sql = duckdb_flagship_sql(
        &duckdb_file_list(&es_files),
        &duckdb_file_list(&nq_files),
        zstd,
    );
    query_duckdb_seconds(conn, &sql, start_ms, end_ms, session_type)
}

#[cfg(feature = "duckdb-bench")]
fn list_hive_files(root: &Path, file_name: &str) -> Result<Vec<PathBuf>, EpisodeBenchError> {
    let mut out = Vec::new();
    if !root.exists() {
        return Ok(out);
    }
    for day_ent in std::fs::read_dir(root)? {
        let day_ent = day_ent?;
        let day_name = day_ent.file_name();
        let day_name = day_name.to_string_lossy();
        if !day_name.starts_with("trading_day=") || !day_ent.path().is_dir() {
            continue;
        }
        for session_ent in std::fs::read_dir(day_ent.path())? {
            let session_ent = session_ent?;
            if !session_ent
                .file_name()
                .to_string_lossy()
                .starts_with("session_type=")
                || !session_ent.path().is_dir()
            {
                continue;
            }
            for root_ent in std::fs::read_dir(session_ent.path())? {
                let root_ent = root_ent?;
                if !root_ent.file_name().to_string_lossy().starts_with("root=")
                    || !root_ent.path().is_dir()
                {
                    continue;
                }
                let file = root_ent.path().join(file_name);
                if file.is_file() {
                    out.push(file);
                }
            }
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(feature = "duckdb-bench")]
fn duckdb_file_list(files: &[PathBuf]) -> String {
    let inner = files
        .iter()
        .map(|p| {
            let s = p.to_string_lossy().replace('\\', "/").replace('\'', "''");
            format!("'{s}'")
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{inner}]")
}

#[cfg(feature = "duckdb-bench")]
fn duckdb_flagship_sql(es_files_sql: &str, nq_files_sql: &str, zstd: bool) -> String {
    let compression = if zstd { ", compression='zstd'" } else { "" };
    format!(
        "SELECT DISTINCT
            CAST(es.frameSecond AS BIGINT) AS frame_second,
            CAST(es.clockMs AS DOUBLE) AS clock_ms,
            CAST(es.payload.lastPrice AS DOUBLE) AS es_last_price
         FROM read_json({es_files_sql}, format='newline_delimited', union_by_name=true{compression}) es
         INNER JOIN read_json({nq_files_sql}, format='newline_delimited', union_by_name=true{compression}) nq
           ON CAST(es.frameSecond AS BIGINT) = CAST(nq.frameSecond AS BIGINT)
         WHERE es.sessionType = ?
           AND CAST(es.clockMs AS DOUBLE) >= ?
           AND CAST(es.clockMs AS DOUBLE) <= ?
           AND CAST(es.payload.sessionDelta AS DOUBLE) <= {delta}
           AND (es.payload.poorLow = true OR CAST(es.payload.poorLow AS INTEGER) = 1)
           AND (es.payload.domSummary.bidReplenishing = true
                OR CAST(es.payload.domSummary.bidReplenishing AS INTEGER) = 1)
           AND CAST(nq.payload.sessionDelta AS DOUBLE) >= 0
         ORDER BY clock_ms ASC, frame_second ASC
         LIMIT {limit}",
        delta = FLAGSHIP_SELLER_AGGRESSION_SESSION_DELTA,
        limit = QUERY_EPISODES_MAX_MATCHES,
    )
}

#[cfg(feature = "duckdb-bench")]
fn query_duckdb_seconds(
    conn: &duckdb::Connection,
    sql: &str,
    start_ms: f64,
    end_ms: f64,
    session_type: &str,
) -> Result<Vec<(i64, f64, Option<f64>)>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query(duckdb::params![session_type, start_ms, end_ms])
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let frame_second: i64 = row.get(0).map_err(|e| e.to_string())?;
        let clock_ms: f64 = row.get(1).map_err(|e| e.to_string())?;
        let es_last: Option<f64> = row.get(2).map_err(|e| e.to_string())?;
        out.push((frame_second, clock_ms, es_last));
    }
    Ok(out)
}

#[cfg(feature = "duckdb-bench")]
fn decode_hive_to_jsonl(hive_root: &Path) -> Result<PathBuf, EpisodeBenchError> {
    // Sibling of the hive so partition walkers never see decoded JSONL as
    // another `trading_day=` tree. Overwrite (never append): Path C latency
    // iters used to cartesian-join duplicate seconds when zstd fallback ran
    // more than once.
    let decoded_root = hive_root.with_file_name(format!(
        "{}-m3e-decoded-jsonl",
        hive_root
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "hive".into())
    ));
    let stamp = decoded_root.join(".complete");
    if stamp.is_file() {
        return Ok(decoded_root);
    }
    if decoded_root.exists() {
        std::fs::remove_dir_all(&decoded_root)?;
    }
    std::fs::create_dir_all(&decoded_root)?;
    let store = ColdFrameStore::new(hive_root);
    let mut by_part: BTreeMap<(String, String, String), String> = BTreeMap::new();
    for frame in store.list_all()? {
        let key = (
            frame.trading_day.clone(),
            frame.session_type.clone(),
            frame.root_symbol.clone(),
        );
        let row = serde_json::json!({
            "clockMs": frame.clock_ms,
            "frameSecond": frame.frame_second,
            "rootSymbol": frame.root_symbol,
            "sessionType": frame.session_type,
            "sessionSegment": frame.session_segment,
            "tradingDay": frame.trading_day,
            "payload": frame.payload,
        });
        let line =
            serde_json::to_string(&row).map_err(|e| EpisodeBenchError::Invalid(e.to_string()))?;
        let buf = by_part.entry(key).or_default();
        buf.push_str(&line);
        buf.push('\n');
    }
    for ((day, session, root), body) in by_part {
        let part = decoded_root
            .join(format!("trading_day={day}"))
            .join(format!("session_type={session}"))
            .join(format!("root={root}"));
        std::fs::create_dir_all(&part)?;
        std::fs::write(part.join("frames.jsonl"), body)?;
    }
    std::fs::write(&stamp, b"ok\n")?;
    Ok(decoded_root)
}

fn percentile_ms(samples: &[f64], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut v = samples.to_vec();
    v.sort_by(|a, b| a.total_cmp(b));
    let idx = ((p / 100.0) * (v.len().saturating_sub(1)) as f64).round() as usize;
    v[idx.min(v.len() - 1)]
}

fn time_iters<F>(
    warmup: usize,
    iters: usize,
    mut f: F,
) -> Result<(Vec<f64>, Vec<i64>), EpisodeBenchError>
where
    F: FnMut() -> Result<Vec<i64>, EpisodeBenchError>,
{
    let mut last = Vec::new();
    for _ in 0..warmup {
        last = f()?;
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        last = f()?;
        samples.push(t0.elapsed().as_secs_f64() * 1_000.0);
    }
    Ok((samples, last))
}

/// Compare A/B/(C) match lists at the current dataset scale.
pub fn run_correctness(
    db: &Database,
    hive: &ColdFrameStore,
    dataset: &SyntheticDataset,
) -> Result<CorrectnessReport, EpisodeBenchError> {
    let req = flagship_request(dataset.start_ms, dataset.end_ms, "RTH");
    let within_live_cap =
        (dataset.end_ms - dataset.start_ms) <= super::query_kernel::QUERY_EPISODES_MAX_WINDOW_MS;
    let a = path_a(db, &req, within_live_cap)?;
    contracts_ok(&a)?;
    let a_seconds = seconds_from_result(&a);
    let a_fp = match_fingerprint(&a_seconds);

    let _ = rebuild_dense_projection(db)?;
    let (b_seconds, b_n, b_tier) = path_b(db, dataset.start_ms, dataset.end_ms, "RTH")?;
    let b_fp = match_fingerprint(&b_seconds);

    let mut notes = Vec::new();
    let mut agreed = a_seconds == b_seconds && a.meta.n == b_n && a.meta.reliability_tier == b_tier;
    if a_seconds != b_seconds {
        notes.push(format!(
            "Path A/B match seconds differ (A={} {a_fp}, B={} {b_fp})",
            a_seconds.len(),
            b_seconds.len()
        ));
    }

    let path_c_status = match path_c(hive.root(), db, dataset.start_ms, dataset.end_ms, "RTH") {
        Ok((c_seconds, avail)) => {
            let c_fp = match_fingerprint(&c_seconds);
            if c_seconds != a_seconds {
                agreed = false;
                notes.push(format!(
                    "Path C match seconds differ from A (C fingerprint {c_fp})"
                ));
            }
            avail
        }
        Err(EpisodeBenchError::DuckDbUnavailable(msg)) => {
            notes.push(format!("Path C skipped: {msg}"));
            if duckdb_feature_enabled() {
                PathCAvailability::RuntimeBlocked
            } else {
                PathCAvailability::FeatureOff
            }
        }
        Err(e) => return Err(e),
    };

    if a.meta.n != dataset.planted_matches && dataset.planted_matches > 0 && a.meta.n > 0 {
        notes.push(format!(
            "match count {} vs planted {}",
            a.meta.n, dataset.planted_matches
        ));
    }

    Ok(CorrectnessReport {
        agreed,
        match_count: a.meta.n,
        fingerprint: a_fp,
        reliability_tier: format!("{:?}", a.meta.reliability_tier),
        trust_level: format!("{:?}", a.meta.trust_level),
        trust_ceiling: format!("{:?}", a.meta.trust_ceiling),
        path_c: path_c_status,
        notes,
    })
}

/// Time Paths A/B/(C) on an already-generated dataset.
pub fn run_latency(
    db: &Database,
    hive: &ColdFrameStore,
    dataset: &SyntheticDataset,
    warmup: usize,
    iters: usize,
) -> Result<Vec<PathTiming>, EpisodeBenchError> {
    let req = flagship_request(dataset.start_ms, dataset.end_ms, "RTH");
    let within_live_cap =
        (dataset.end_ms - dataset.start_ms) <= super::query_kernel::QUERY_EPISODES_MAX_WINDOW_MS;

    let (a_samples, a_seconds) = time_iters(warmup, iters, || {
        let result = path_a(db, &req, within_live_cap)?;
        Ok(seconds_from_result(&result))
    })?;
    let mut timings = vec![PathTiming {
        path: "A".into(),
        p50_ms: percentile_ms(&a_samples, 50.0),
        p95_ms: percentile_ms(&a_samples, 95.0),
        n_iters: a_samples.len(),
        match_count: a_seconds.len(),
        fingerprint: match_fingerprint(&a_seconds),
        reliability_tier: format!("{:?}", reliability_tier(a_seconds.len())),
        trust_level: format!("{:?}", TrustLevel::L0),
        rebuild_ms: None,
        samples_ms: a_samples,
        notes: Some("status-quo query_episodes over SQLite journal_frames JSON blobs".into()),
    }];

    let rebuild_t0 = Instant::now();
    rebuild_dense_projection(db)?;
    let rebuild_ms = rebuild_t0.elapsed().as_secs_f64() * 1_000.0;
    let (b_samples, b_seconds) = time_iters(warmup, iters, || {
        let (seconds, _, _) = path_b(db, dataset.start_ms, dataset.end_ms, "RTH")?;
        Ok(seconds)
    })?;
    timings.push(PathTiming {
        path: "B".into(),
        p50_ms: percentile_ms(&b_samples, 50.0),
        p95_ms: percentile_ms(&b_samples, 95.0),
        n_iters: b_samples.len(),
        match_count: b_seconds.len(),
        fingerprint: match_fingerprint(&b_seconds),
        reliability_tier: format!("{:?}", reliability_tier(b_seconds.len())),
        trust_level: format!("{:?}", TrustLevel::L0),
        rebuild_ms: Some(rebuild_ms),
        samples_ms: b_samples,
        notes: Some(
            "SQLite side table of promoted flagship columns (not a live journal_frames rewrite)"
                .into(),
        ),
    });

    match time_iters(warmup, iters, || {
        let (seconds, _) = path_c(hive.root(), db, dataset.start_ms, dataset.end_ms, "RTH")?;
        Ok(seconds)
    }) {
        Ok((c_samples, c_seconds)) => {
            timings.push(PathTiming {
                path: "C".into(),
                p50_ms: percentile_ms(&c_samples, 50.0),
                p95_ms: percentile_ms(&c_samples, 95.0),
                n_iters: c_samples.len(),
                match_count: c_seconds.len(),
                fingerprint: match_fingerprint(&c_seconds),
                reliability_tier: format!("{:?}", reliability_tier(c_seconds.len())),
                trust_level: format!("{:?}", TrustLevel::L0),
                rebuild_ms: None,
                samples_ms: c_samples,
                notes: Some(
                    "DuckDB read_json of desk-journal-frames-v1 JSONL.zst (not Apache Parquet)"
                        .into(),
                ),
            });
        }
        Err(EpisodeBenchError::DuckDbUnavailable(msg)) => {
            timings.push(PathTiming {
                path: "C".into(),
                p50_ms: 0.0,
                p95_ms: 0.0,
                n_iters: 0,
                match_count: 0,
                fingerprint: String::new(),
                reliability_tier: format!("{:?}", ReliabilityTier::Insufficient),
                trust_level: format!("{:?}", TrustLevel::L0),
                rebuild_ms: None,
                samples_ms: Vec::new(),
                notes: Some(format!("blocked: {msg}")),
            });
        }
        Err(e) => return Err(e),
    }

    Ok(timings)
}

/// Apply the owner SLO. Default bias: DEFER DuckDB.
pub fn decide_duckdb(
    dataset: &SyntheticDataset,
    timings: &[PathTiming],
    path_c: PathCAvailability,
) -> DuckDbDecision {
    let path_b = timings.iter().find(|t| t.path == "B");
    let path_c_timing = timings.iter().find(|t| t.path == "C");
    let b_p95 = path_b.map(|t| t.p95_ms).unwrap_or(0.0);
    let span_ms = dataset.end_ms - dataset.start_ms;
    let two_weekish = dataset.rth_days >= TWO_WEEK_RTH_DAYS;
    let long_horizon_available = false;
    let path_b_hand_built_columnar = false;

    let (verdict, reason) = if path_b_hand_built_columnar {
        (
            DuckDbVerdict::Adopt,
            "Path B degenerated into a hand-built columnar store".to_string(),
        )
    } else if two_weekish && b_p95 <= SLO_P95_TWO_WEEK_MS {
        (
            DuckDbVerdict::Defer,
            format!(
                "Path B p95={b_p95:.3} ms is inside the two-week SLO (p95 ≤ {SLO_P95_TWO_WEEK_MS} ms) \
                 on {} RTH days / {} frames. Default bias is DEFER unless B misses that bar. \
                 Path B is a SQLite side table of promoted columns, not a hand-built columnar store. \
                 90-day / 1-year data is not in this environment (SLO p95 ≤ {SLO_P95_LONG_HORIZON_MS} ms once available). \
                 Path C availability: {path_c:?}.",
                dataset.rth_days, dataset.frame_count
            ),
        )
    } else if dataset.rth_days < TWO_WEEK_RTH_DAYS && b_p95 <= SLO_P95_TWO_WEEK_MS {
        (
            DuckDbVerdict::Defer,
            format!(
                "Path B p95={b_p95:.3} ms is inside the two-week SLO on a smaller span \
                 ({} RTH days / {} frames). Fixture-scale milliseconds are not a go; this measured \
                 N does not trip the bar. 90-day / 1-year is not in this environment. Path C: {path_c:?}.",
                dataset.rth_days, dataset.frame_count
            ),
        )
    } else if matches!(
        path_c,
        PathCAvailability::FeatureOff | PathCAvailability::RuntimeBlocked
    ) {
        (
            DuckDbVerdict::Defer,
            format!(
                "Path B p95={b_p95:.3} ms over {} RTH days / {} frames, but Path C is not a runnable \
                 DuckDB measurement ({path_c:?}). Owner default is DEFER unless B misses the SLO *and* \
                 DuckDB can be run.",
                dataset.rth_days, dataset.frame_count
            ),
        )
    } else if two_weekish && b_p95 > SLO_P95_TWO_WEEK_MS {
        let c_p95 = path_c_timing.map(|t| t.p95_ms).unwrap_or(0.0);
        (
            DuckDbVerdict::Adopt,
            format!(
                "Path B p95={b_p95:.3} ms misses the ~2-week SLO (p95 ≤ {SLO_P95_TWO_WEEK_MS} ms) \
                 over {} RTH days / {} frames. Path C p95={c_p95:.3} ms.",
                dataset.rth_days, dataset.frame_count
            ),
        )
    } else if b_p95 > SLO_P95_TWO_WEEK_MS {
        (
            DuckDbVerdict::Adopt,
            format!(
                "Path B p95={b_p95:.3} ms already exceeds the 2–3 s two-week SLO on a smaller \
                 span ({} RTH days / {} frames); 90-day / 1-year data is not in this environment.",
                dataset.rth_days, dataset.frame_count
            ),
        )
    } else {
        (
            DuckDbVerdict::Defer,
            format!(
                "Path B p95={b_p95:.3} ms did not trip the owner SLO on {} RTH days / {} frames.",
                dataset.rth_days, dataset.frame_count
            ),
        )
    };

    DuckDbDecision {
        verdict,
        reason,
        slo_p95_two_week_ms: SLO_P95_TWO_WEEK_MS,
        slo_p95_long_horizon_ms: SLO_P95_LONG_HORIZON_MS,
        measured_rth_days: dataset.rth_days,
        measured_frame_count: dataset.frame_count,
        span_ms,
        long_horizon_available,
        path_b_hand_built_columnar,
        path_c_availability: path_c,
    }
}

/// End-to-end report used by `the-desk-eq-bench` and tests.
pub fn run_harness(
    db: &Database,
    hive: &ColdFrameStore,
    dataset: &SyntheticDataset,
    warmup: usize,
    iters: usize,
) -> Result<BenchReport, EpisodeBenchError> {
    let correctness = run_correctness(db, hive, dataset)?;
    let timings = run_latency(db, hive, dataset, warmup, iters)?;
    let path_c = timings
        .iter()
        .find(|t| t.path == "C")
        .and_then(|t| {
            if t.notes
                .as_deref()
                .is_some_and(|n| n.starts_with("blocked:"))
            {
                if duckdb_feature_enabled() {
                    Some(PathCAvailability::RuntimeBlocked)
                } else {
                    Some(PathCAvailability::FeatureOff)
                }
            } else if t.n_iters > 0 {
                Some(PathCAvailability::Ran)
            } else {
                None
            }
        })
        .unwrap_or(correctness.path_c);
    let decision = decide_duckdb(dataset, &timings, path_c);
    Ok(BenchReport {
        flagship: "flagship_episode_predicates (ES near Positioning ∧ ES sessionDelta ≤ -400 ∧ ES poorLow ∧ ES bidReplenishing ∧ NQ sessionDelta ≥ 0)".into(),
        machine: machine_class(),
        dataset: DatasetSummary {
            rth_days: dataset.rth_days,
            frame_count: dataset.frame_count,
            planted_matches: dataset.planted_matches,
            start_ms: dataset.start_ms,
            end_ms: dataset.end_ms,
            span_ms: dataset.end_ms - dataset.start_ms,
            session_type: "RTH".into(),
        },
        correctness,
        timings,
        decision,
    })
}

/// Suggested RTH-day targets, largest first. Step down on failure.
pub fn rth_day_stepdown(requested: usize) -> Vec<usize> {
    let mut out = vec![requested];
    for candidate in [TWO_WEEK_RTH_DAYS, 5, 2, 1] {
        if candidate < requested && !out.contains(&candidate) {
            out.push(candidate);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::research::query_kernel::{validate_window, QueryKind, QUERY_EPISODES_MAX_WINDOW_MS};
    use tempfile::{NamedTempFile, TempDir};

    fn test_db() -> (NamedTempFile, Database) {
        let file = NamedTempFile::new().expect("temp db");
        let db = Database::open(file.path().to_string_lossy().as_ref()).expect("open");
        (file, db)
    }

    #[test]
    fn golden_fixture_paths_a_and_b_agree() {
        let (_file, db) = test_db();
        let hive_dir = TempDir::new().expect("hive");
        let hive = ColdFrameStore::new(hive_dir.path());
        let dataset = seed_golden_fixture(&db, &hive).expect("seed");
        let report = run_correctness(&db, &hive, &dataset).expect("correctness");
        assert_eq!(report.match_count, 1);
        assert_eq!(
            report.fingerprint,
            match_fingerprint(&dataset.planted_seconds)
        );
        assert!(report.agreed, "A/B must agree: {:?}", report.notes);
        assert_eq!(report.trust_level, "L0");
        assert_eq!(report.trust_ceiling, "L3");
        assert_eq!(report.reliability_tier, "Insufficient");
        if duckdb_feature_enabled() {
            assert_eq!(report.path_c, PathCAvailability::Ran);
        } else {
            assert_eq!(report.path_c, PathCAvailability::FeatureOff);
        }
    }

    #[test]
    fn dense_projection_does_not_alter_journal_frames_schema() {
        let (_file, db) = test_db();
        let hive_dir = TempDir::new().expect("hive");
        let hive = ColdFrameStore::new(hive_dir.path());
        seed_golden_fixture(&db, &hive).expect("seed");
        let before: String = db
            .sqlite_connection()
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='journal_frames'",
                [],
                |row| row.get(0),
            )
            .expect("schema");
        rebuild_dense_projection(&db).expect("rebuild");
        let after: String = db
            .sqlite_connection()
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='journal_frames'",
                [],
                |row| row.get(0),
            )
            .expect("schema");
        assert_eq!(before, after);
        assert!(!after.contains("es_last_price"));
        let dense: i64 = db
            .sqlite_connection()
            .query_row(
                "SELECT COUNT(1) FROM sqlite_master WHERE name=?1",
                params![DENSE_TABLE],
                |row| row.get(0),
            )
            .expect("dense");
        assert_eq!(dense, 1);
    }

    #[test]
    fn live_query_still_enforces_seven_day_cap() {
        let start = 1_704_207_600_000.0;
        let end = start + QUERY_EPISODES_MAX_WINDOW_MS + 1.0;
        let err = validate_window(QueryKind::Episodes, Some(start), Some(end)).unwrap_err();
        assert!(matches!(
            err,
            crate::research::query_kernel::QueryKernelError::WindowTooLarge { .. }
        ));
    }

    #[test]
    fn slo_opts_allow_window_larger_than_product_cap() {
        let (_file, db) = test_db();
        let hive_dir = TempDir::new().expect("hive");
        let hive = ColdFrameStore::new(hive_dir.path());
        let dataset = seed_golden_fixture(&db, &hive).expect("seed");
        let req = flagship_request(
            dataset.start_ms,
            dataset.start_ms + QUERY_EPISODES_MAX_WINDOW_MS + 86_400_000.0,
            "RTH",
        );
        let live = query_episodes(&db, &req);
        assert!(live.is_err(), "live query_episodes must keep the 7-day cap");
        let slo = path_a(&db, &req, false).expect("slo scan");
        assert_eq!(slo.matches.len(), 1);
        assert_eq!(slo.meta.trust_level, TrustLevel::L0);
    }

    #[test]
    fn decide_defers_when_b_is_fast_and_c_is_gated() {
        let dataset = SyntheticDataset {
            start_ms: 1.0,
            end_ms: 1.0 + 10.0 * 86_400_000.0,
            rth_days: 10,
            frame_count: 400_000,
            planted_matches: 10,
            planted_seconds: vec![1],
        };
        let timings = vec![PathTiming {
            path: "B".into(),
            samples_ms: vec![12.0, 13.0],
            p50_ms: 12.0,
            p95_ms: 13.0,
            n_iters: 2,
            match_count: 10,
            fingerprint: "x".into(),
            reliability_tier: "Insufficient".into(),
            trust_level: "L0".into(),
            rebuild_ms: Some(100.0),
            notes: None,
        }];
        let d = decide_duckdb(&dataset, &timings, PathCAvailability::FeatureOff);
        assert_eq!(d.verdict, DuckDbVerdict::Defer);
        assert!(!d.long_horizon_available);
        assert!(!d.path_b_hand_built_columnar);
    }

    #[test]
    fn missing_bid_replenishing_fails_closed_on_path_b() {
        let (_file, db) = test_db();
        let clock = 1_704_207_600_000.0;
        db.insert_journal_frames(&[
            frame(
                clock,
                "ES",
                "2024-01-02",
                es_payload(BENCH_ES_PRICE, -500.0, true, None),
            ),
            frame(clock, "NQ", "2024-01-02", nq_payload(50.0)),
        ])
        .expect("frames");
        seed_flagship_positioning(&db, clock).expect("pos");
        rebuild_dense_projection(&db).expect("dense");
        let (seconds, n, _) = path_b(&db, clock, clock + 1.0, "RTH").expect("b");
        assert!(seconds.is_empty());
        assert_eq!(n, 0);
    }
}
