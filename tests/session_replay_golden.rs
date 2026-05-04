use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use chrono::TimeZone;
use chrono_tz::US::Eastern;
use serde::Serialize;
use serde_json::{json, Value};
use tempfile::NamedTempFile;
use the_desk_backend::backfill::{
    run_backfill_job_with_options, BackfillJobParams, BackfillReplayOptions, HistoricalJobType,
};
use the_desk_backend::db::{
    Database, PriorDayReference, ReplaySignalRecord, SessionSummary, SignalOutcome,
};
use the_desk_backend::feed::scid_reader::{ScidReader, SCID_RECORD_SIZE};
use the_desk_backend::feed::ContractMetadata;
use the_desk_backend::pipelines::MarketEvent;
use the_desk_backend::rules::SetupDefinition;

const SCID_HEADER_SIZE: usize = 56;
const SCID_MAGIC: &[u8; 4] = b"SCID";
const SC_TO_UNIX_EPOCH_US: i64 = 2_209_161_600_000_000;
const BLESS_ENV: &str = "THE_DESK_BLESS_GOLDENS";
const CORE_EVENT_TYPES: &[&str] = &[
    "new_session_high",
    "new_session_low",
    "prior_day_high_test",
    "prior_day_low_test",
    "or_formed",
    "ib_formed",
    "ib_mid_test",
    "large_trade_cluster",
];
const STRICT_NUMERIC_PATHS: &[&str] = &[
    "$.fixtureVersion",
    "$.result.sessionsProcessed",
    "$.result.sessionsSkipped",
    "$.result.totalTicks",
    "$.result.canonicalEventCount",
    "$.result.signalsFired",
    "$.result.nonMonotonicSkippedTicks",
];
const STRICT_NUMERIC_SUFFIXES: &[&str] = &[
    ".timestampMs",
    ".sequenceNum",
    ".tickCount",
    ".signalCount",
    ".totalVolume",
    ".openPrice",
    ".high",
    ".low",
    ".close",
    ".poc",
    ".vah",
    ".val",
    ".ibHigh",
    ".ibLow",
    ".ibRange",
    ".orHigh",
    ".orLow",
    ".price",
    ".firedAtMs",
    ".firedPrice",
    ".targetPrice",
    ".stopPrice",
    ".outcomeAtMs",
    ".timeToOutcomeMs",
    ".rvolBucketAtFire",
    ".signalsFired",
];
const DERIVED_NUMERIC_SUFFIXES: &[&str] = &[
    ".ibMid",
    ".sessionDelta",
    ".cumulativeDelta",
    ".dnp",
    ".dnvaHigh",
    ".dnvaLow",
    ".vwapClose",
    ".maxFavorableExcursion",
    ".maxAdverseExcursion",
    ".rResult",
    ".rvolAtFire",
];

#[derive(Debug, Clone)]
struct TickSpec {
    timestamp_ms: f64,
    price: f32,
    volume: u32,
    is_buy: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalSessionSummary {
    session_date: String,
    session_type: String,
    root_symbol: String,
    contract_symbol: String,
    open_price: f64,
    high: f64,
    low: f64,
    close: f64,
    poc: f64,
    vah: f64,
    val: f64,
    ib_high: f64,
    ib_low: f64,
    ib_range: f64,
    ib_mid: f64,
    or_high: f64,
    or_low: f64,
    day_type: String,
    profile_shape: String,
    balance_state: String,
    total_volume: f64,
    tick_count: i64,
    session_delta: f64,
    cumulative_delta: f64,
    dnp: f64,
    dnva_high: f64,
    dnva_low: f64,
    vwap_close: f64,
    signal_count: i64,
    single_prints_direction: String,
    close_vs_ib_mid: String,
    close_vs_vwap: String,
    close_vs_poc: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalMarketEvent {
    session_date: String,
    timestamp_ms: f64,
    event_type: String,
    level_name: Option<String>,
    price: f64,
    direction: Option<String>,
    sequence_num: Option<i32>,
    session_type: String,
    session_segment: String,
    trading_day: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalReplaySignal {
    signal_id: String,
    timestamp_ms: f64,
    session_date: String,
    root_symbol: Option<String>,
    contract_symbol: Option<String>,
    setup_id: String,
    source: String,
    job_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalSignalOutcome {
    signal_id: String,
    setup_id: String,
    setup_name: Option<String>,
    session_date: String,
    root_symbol: Option<String>,
    contract_symbol: Option<String>,
    source: String,
    job_id: Option<String>,
    fired_at_ms: f64,
    fired_price: f64,
    target_price: Option<f64>,
    stop_price: Option<f64>,
    outcome: String,
    outcome_at_ms: Option<f64>,
    max_favorable_excursion: Option<f64>,
    max_adverse_excursion: Option<f64>,
    r_result: Option<f64>,
    time_to_outcome_ms: Option<f64>,
    rvol_at_fire: Option<f64>,
    rvol_bucket_at_fire: Option<i32>,
}

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("session_replay")
        .join("v1")
}

fn eastern_ms(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> f64 {
    Eastern
        .with_ymd_and_hms(year, month, day, hour, minute, 0)
        .single()
        .expect("valid ET timestamp")
        .timestamp_millis() as f64
}

fn tick(year: i32, month: u32, day: u32, hour: u32, minute: u32, price: f32) -> TickSpec {
    TickSpec {
        timestamp_ms: eastern_ms(year, month, day, hour, minute),
        price,
        volume: 2,
        is_buy: true,
    }
}

fn sell_tick(year: i32, month: u32, day: u32, hour: u32, minute: u32, price: f32) -> TickSpec {
    TickSpec {
        timestamp_ms: eastern_ms(year, month, day, hour, minute),
        price,
        volume: 2,
        is_buy: false,
    }
}

fn large_tick(year: i32, month: u32, day: u32, hour: u32, minute: u32, price: f32) -> TickSpec {
    TickSpec {
        timestamp_ms: eastern_ms(year, month, day, hour, minute),
        price,
        volume: 24,
        is_buy: true,
    }
}

fn synthetic_ticks() -> Vec<TickSpec> {
    vec![
        tick(2026, 3, 2, 18, 0, 21000.0),
        tick(2026, 3, 2, 18, 5, 21004.0),
        sell_tick(2026, 3, 2, 18, 10, 20996.0),
        tick(2026, 3, 3, 9, 30, 21000.0),
        tick(2026, 3, 3, 9, 35, 21003.0),
        sell_tick(2026, 3, 3, 9, 40, 20998.0),
        tick(2026, 3, 3, 10, 0, 21001.0),
        tick(2026, 3, 3, 10, 15, 21005.0),
        tick(2026, 3, 3, 10, 30, 21002.0),
        tick(2026, 3, 3, 10, 31, 21005.0),
        sell_tick(2026, 3, 3, 10, 33, 21001.0),
        large_tick(2026, 3, 3, 11, 35, 21006.0),
        sell_tick(2026, 3, 3, 15, 59, 21004.0),
        tick(2026, 3, 3, 18, 0, 21100.0),
        sell_tick(2026, 3, 3, 18, 10, 21092.0),
        tick(2026, 3, 4, 9, 30, 21100.0),
        sell_tick(2026, 3, 4, 9, 36, 21096.0),
        tick(2026, 3, 4, 9, 42, 21104.0),
        tick(2026, 3, 4, 10, 0, 21102.0),
        sell_tick(2026, 3, 4, 10, 20, 21094.0),
        sell_tick(2026, 3, 4, 10, 30, 21098.0),
        tick(2026, 3, 4, 10, 32, 21104.0),
        sell_tick(2026, 3, 4, 10, 34, 21100.0),
        large_tick(2026, 3, 4, 11, 40, 21106.0),
        sell_tick(2026, 3, 4, 15, 59, 21099.0),
    ]
}

fn non_monotonic_ticks() -> Vec<TickSpec> {
    let mut ticks = synthetic_ticks();
    let duplicate = ticks[1].clone();
    let mut backward = ticks[1].clone();
    backward.timestamp_ms -= 1.0;
    backward.price = 21004.25;
    ticks.insert(2, duplicate);
    ticks.insert(3, backward);
    ticks
}

fn write_scid_header(file: &mut NamedTempFile) {
    let mut header = vec![0_u8; SCID_HEADER_SIZE];
    header[0..4].copy_from_slice(SCID_MAGIC);
    header[4..8].copy_from_slice(&(SCID_HEADER_SIZE as u32).to_le_bytes());
    header[8..12].copy_from_slice(&(SCID_RECORD_SIZE as u32).to_le_bytes());
    file.write_all(&header).expect("write SCID header");
}

fn write_scid_record(file: &mut NamedTempFile, tick: &TickSpec) {
    let mut rec = [0_u8; SCID_RECORD_SIZE];
    let unix_us = (tick.timestamp_ms * 1_000.0).round() as i64;
    let sc_us = SC_TO_UNIX_EPOCH_US + unix_us;
    let bid = tick.price - 0.25;
    let ask = tick.price + 0.25;
    let bid_volume = if tick.is_buy { 0_u32 } else { tick.volume };
    let ask_volume = if tick.is_buy { tick.volume } else { 0_u32 };

    rec[0..8].copy_from_slice(&sc_us.to_le_bytes());
    rec[12..16].copy_from_slice(&ask.to_le_bytes());
    rec[16..20].copy_from_slice(&bid.to_le_bytes());
    rec[20..24].copy_from_slice(&tick.price.to_le_bytes());
    rec[24..28].copy_from_slice(&(1_u32).to_le_bytes());
    rec[28..32].copy_from_slice(&tick.volume.to_le_bytes());
    rec[32..36].copy_from_slice(&bid_volume.to_le_bytes());
    rec[36..40].copy_from_slice(&ask_volume.to_le_bytes());
    file.write_all(&rec).expect("write SCID record");
}

fn write_scid_fixture(ticks: Vec<TickSpec>) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("temp SCID fixture");
    write_scid_header(&mut file);
    for tick in ticks {
        write_scid_record(&mut file, &tick);
    }
    file.flush().expect("flush SCID fixture");
    file
}

fn write_synthetic_scid() -> NamedTempFile {
    write_scid_fixture(synthetic_ticks())
}

fn write_non_monotonic_scid() -> NamedTempFile {
    write_scid_fixture(non_monotonic_ticks())
}

fn test_contract(scid_path: &Path) -> ContractMetadata {
    ContractMetadata {
        root_symbol: "NQ".to_string(),
        contract_symbol: "NQM6.CME".to_string(),
        contract_month: Some("M".to_string()),
        expiry_year_month: Some("2026-06".to_string()),
        symbol_resolution_mode: "test".to_string(),
        symbol_resolution_source: "golden_fixture".to_string(),
        configured_symbol: "NQM6.CME".to_string(),
        active_symbol_override: Some("NQM6.CME".to_string()),
        scid_path: scid_path.to_string_lossy().to_string(),
        scid_file_exists: true,
        depth_prefix: "NQM6.CME".to_string(),
        ..Default::default()
    }
}

fn fixture_prior_references() -> Vec<PriorDayReference> {
    vec![PriorDayReference {
        date: "2026-03-02".to_string(),
        high: 21003.75,
        low: 20997.0,
        close: 21000.0,
        va_high: Some(21002.0),
        va_low: Some(20998.0),
        poc: Some(21000.0),
        dnva_high: Some(21002.0),
        dnva_low: Some(20998.0),
        dnp: Some(21000.0),
        root_symbol: Some("NQ".to_string()),
        contract_symbol: Some("NQM6.CME".to_string()),
    }]
}

#[derive(Debug, Clone)]
struct ReplayCase {
    job_id: String,
    job_type: HistoricalJobType,
    run_rules: bool,
    seed_rules_setup: bool,
    start_date: Option<String>,
    end_date: Option<String>,
    price_scale: f64,
}

impl ReplayCase {
    fn core() -> Self {
        Self {
            job_id: "golden-core".to_string(),
            job_type: HistoricalJobType::ResearchBackfill,
            run_rules: false,
            seed_rules_setup: false,
            start_date: Some("2026-03-02".to_string()),
            end_date: Some("2026-03-04".to_string()),
            price_scale: 1.0,
        }
    }

    fn rules() -> Self {
        Self {
            job_id: "golden-rules".to_string(),
            job_type: HistoricalJobType::Backtest,
            run_rules: true,
            seed_rules_setup: true,
            start_date: Some("2026-03-02".to_string()),
            end_date: Some("2026-03-04".to_string()),
            price_scale: 1.0,
        }
    }
}

fn golden_setup() -> SetupDefinition {
    SetupDefinition {
        id: "golden_price_above_vwap".to_string(),
        name: "Golden Price Above VWAP".to_string(),
        description: "Synthetic setup for golden replay signal/outcome coverage.".to_string(),
        active: true,
        conditions: vec!["price_vs_vwap=above".to_string()],
        duplicate_suppression_ms: 3_600_000,
        targets: vec![serde_json::json!({
            "mode": "fixed_points",
            "direction": "long",
            "points": 3.0
        })],
        stop_logic: serde_json::json!({
            "mode": "fixed_points",
            "direction": "long",
            "points": 80.0
        }),
        template_source: Some("golden_replay".to_string()),
        ..Default::default()
    }
}

fn canonical_summary(summary: SessionSummary) -> CanonicalSessionSummary {
    CanonicalSessionSummary {
        session_date: summary.session_date,
        session_type: summary.session_type,
        root_symbol: summary.root_symbol,
        contract_symbol: summary.contract_symbol,
        open_price: summary.open_price,
        high: summary.high,
        low: summary.low,
        close: summary.close,
        poc: summary.poc,
        vah: summary.vah,
        val: summary.val,
        ib_high: summary.ib_high,
        ib_low: summary.ib_low,
        ib_range: summary.ib_range,
        ib_mid: summary.ib_mid,
        or_high: summary.or_high,
        or_low: summary.or_low,
        day_type: summary.day_type,
        profile_shape: summary.profile_shape,
        balance_state: summary.balance_state,
        total_volume: summary.total_volume,
        tick_count: summary.tick_count,
        session_delta: summary.session_delta,
        cumulative_delta: summary.cumulative_delta,
        dnp: summary.dnp,
        dnva_high: summary.dnva_high,
        dnva_low: summary.dnva_low,
        vwap_close: summary.vwap_close,
        signal_count: summary.signal_count,
        single_prints_direction: summary.single_prints_direction,
        close_vs_ib_mid: summary.close_vs_ib_mid,
        close_vs_vwap: summary.close_vs_vwap,
        close_vs_poc: summary.close_vs_poc,
    }
}

fn canonical_event(event: MarketEvent) -> CanonicalMarketEvent {
    CanonicalMarketEvent {
        session_date: event.session_date,
        timestamp_ms: event.timestamp_ms,
        event_type: event.event_type,
        level_name: event.level_name,
        price: event.price,
        direction: event.direction,
        sequence_num: event.sequence_num,
        session_type: event.session_type,
        session_segment: event.session_segment,
        trading_day: event.trading_day,
    }
}

fn canonical_signal(signal: ReplaySignalRecord) -> CanonicalReplaySignal {
    CanonicalReplaySignal {
        signal_id: signal.signal_id,
        timestamp_ms: signal.timestamp_ms,
        session_date: signal.session_date,
        root_symbol: signal.root_symbol,
        contract_symbol: signal.contract_symbol,
        setup_id: signal.setup_id,
        source: signal.source,
        job_id: signal.job_id,
    }
}

fn canonical_outcome(outcome: SignalOutcome) -> CanonicalSignalOutcome {
    CanonicalSignalOutcome {
        signal_id: outcome.signal_id,
        setup_id: outcome.setup_id,
        setup_name: outcome.setup_name,
        session_date: outcome.session_date,
        root_symbol: outcome.root_symbol,
        contract_symbol: outcome.contract_symbol,
        source: outcome.source,
        job_id: outcome.job_id,
        fired_at_ms: outcome.fired_at_ms,
        fired_price: outcome.fired_price,
        target_price: outcome.target_price,
        stop_price: outcome.stop_price,
        outcome: outcome.outcome,
        outcome_at_ms: outcome.outcome_at_ms,
        max_favorable_excursion: outcome.max_favorable_excursion,
        max_adverse_excursion: outcome.max_adverse_excursion,
        r_result: outcome.r_result,
        time_to_outcome_ms: outcome.time_to_outcome_ms,
        rvol_at_fire: outcome.rvol_at_fire,
        rvol_bucket_at_fire: outcome.rvol_bucket_at_fire,
    }
}

fn run_core_replay(scid_path: &Path, replay_case: &ReplayCase) -> Value {
    let db = Database::open(":memory:").expect("temp replay db");
    if replay_case.seed_rules_setup {
        db.upsert_setup(&golden_setup()).expect("seed golden setup");
    }
    let cancel_flag = AtomicBool::new(false);
    let params = BackfillJobParams {
        job_id: replay_case.job_id.clone(),
        job_type: replay_case.job_type,
        start_date: replay_case.start_date.clone(),
        end_date: replay_case.end_date.clone(),
        force: true,
        run_rules: replay_case.run_rules,
        setup_ids: None,
    };
    let result = run_backfill_job_with_options(
        &ScidReader::with_price_scale(scid_path, replay_case.price_scale),
        &db,
        &params,
        |_| {},
        &cancel_flag,
        BackfillReplayOptions {
            contract_metadata: Some(test_contract(scid_path)),
            rth_rvol_curves: Some(Vec::new()),
            globex_rvol_curves: Some(Vec::new()),
            prior_day_references: fixture_prior_references(),
        },
    )
    .expect("golden replay backfill");

    let mut summaries = db
        .list_session_summaries(
            replay_case.start_date.as_deref(),
            replay_case.end_date.as_deref(),
            None,
            None,
            100,
        )
        .expect("session summaries");
    summaries.sort_by(|a, b| {
        (a.session_date.as_str(), a.session_type.as_str())
            .cmp(&(b.session_date.as_str(), b.session_type.as_str()))
    });
    let events = db
        .list_market_events_for_replay(
            replay_case.start_date.as_deref(),
            replay_case.end_date.as_deref(),
        )
        .expect("market events");
    let canonical_events = events
        .into_iter()
        .filter(|event| CORE_EVENT_TYPES.contains(&event.event_type.as_str()))
        .map(canonical_event)
        .collect::<Vec<_>>();
    let source = Some(replay_case.job_type.replay_source());
    let signals = db
        .list_playbook_signals_for_replay(source, Some(replay_case.job_id.as_str()))
        .expect("playbook signals")
        .into_iter()
        .map(canonical_signal)
        .collect::<Vec<_>>();
    let outcomes = db
        .list_signal_outcomes_for_replay(source, Some(replay_case.job_id.as_str()))
        .expect("signal outcomes")
        .into_iter()
        .map(canonical_outcome)
        .collect::<Vec<_>>();

    json!({
        "fixtureVersion": 1,
        "result": {
            "sessionsProcessed": result.sessions_processed,
            "sessionsSkipped": result.sessions_skipped,
            "totalTicks": result.total_ticks,
            "canonicalEventCount": canonical_events.len(),
            "sessionDates": result.session_dates,
            "signalsFired": result.signals_fired,
            "integrityStatus": result.integrity_status,
            "warnings": result.warnings,
            "nonMonotonicSkippedTicks": result.scid_timestamp_monotonicity.skipped_non_monotonic_ticks
        },
        "sessionSummaries": summaries.into_iter().map(canonical_summary).collect::<Vec<_>>(),
        "marketEvents": canonical_events,
        "playbookSignals": signals,
        "signalOutcomes": outcomes
    })
}

fn assert_json_close(path: &str, expected: &Value, actual: &Value) {
    match (expected, actual) {
        (Value::Object(expected_obj), Value::Object(actual_obj)) => {
            assert_eq!(
                expected_obj.len(),
                actual_obj.len(),
                "object key count mismatch at {path}: expected keys {:?}, actual keys {:?}",
                expected_obj.keys().collect::<Vec<_>>(),
                actual_obj.keys().collect::<Vec<_>>()
            );
            for (key, expected_value) in expected_obj {
                let actual_value = actual_obj
                    .get(key)
                    .unwrap_or_else(|| panic!("missing key at {path}.{key}"));
                assert_json_close(&format!("{path}.{key}"), expected_value, actual_value);
            }
        }
        (Value::Array(expected_items), Value::Array(actual_items)) => {
            assert_eq!(
                expected_items.len(),
                actual_items.len(),
                "array length mismatch at {path}"
            );
            for (idx, (expected_item, actual_item)) in
                expected_items.iter().zip(actual_items.iter()).enumerate()
            {
                assert_json_close(&format!("{path}[{idx}]"), expected_item, actual_item);
            }
        }
        (Value::Number(expected_num), Value::Number(actual_num)) => {
            let expected_f = expected_num.as_f64().expect("expected numeric value");
            let actual_f = actual_num.as_f64().expect("actual numeric value");
            let tolerance = numeric_tolerance(path);
            assert!(
                (expected_f - actual_f).abs() <= tolerance,
                "numeric mismatch at {path}: expected {expected_f}, actual {actual_f}, tolerance {tolerance}"
            );
        }
        _ => assert_eq!(expected, actual, "value mismatch at {path}"),
    }
}

fn numeric_tolerance(path: &str) -> f64 {
    if STRICT_NUMERIC_PATHS.contains(&path)
        || STRICT_NUMERIC_SUFFIXES
            .iter()
            .any(|suffix| path.ends_with(suffix))
    {
        0.0
    } else if DERIVED_NUMERIC_SUFFIXES
        .iter()
        .any(|suffix| path.ends_with(suffix))
    {
        1e-6
    } else {
        panic!("no numeric tolerance registered for {path}");
    }
}

fn compare_or_bless(expected_path: &Path, actual: &Value) {
    let actual_pretty = serde_json::to_string_pretty(actual).expect("serialize actual golden");
    if std::env::var(BLESS_ENV).ok().as_deref() == Some("1") {
        fs::write(expected_path, format!("{actual_pretty}\n")).expect("write blessed golden");
        return;
    }
    let expected_raw = fs::read_to_string(expected_path).unwrap_or_else(|err| {
        panic!(
            "missing golden file {} ({err}); rerun with {BLESS_ENV}=1 after reviewing output",
            expected_path.display()
        )
    });
    let expected: Value = serde_json::from_str(&expected_raw).expect("parse expected golden");
    assert_json_close("$", &expected, actual);
}

#[test]
fn synthetic_two_session_replay_matches_core_golden() {
    let scid = write_synthetic_scid();
    let actual = run_core_replay(scid.path(), &ReplayCase::core());
    compare_or_bless(&fixture_dir().join("expected_core.json"), &actual);
}

#[test]
fn synthetic_two_session_replay_with_rules_matches_golden() {
    let scid = write_synthetic_scid();
    let actual = run_core_replay(scid.path(), &ReplayCase::rules());
    compare_or_bless(&fixture_dir().join("expected_rules.json"), &actual);
}

#[test]
fn non_monotonic_replay_matches_golden() {
    let scid = write_non_monotonic_scid();
    let actual = run_core_replay(scid.path(), &ReplayCase::core());
    compare_or_bless(&fixture_dir().join("expected_non_monotonic.json"), &actual);
}

#[test]
#[ignore = "requires private real Sierra Chart fixtures outside the repository"]
fn private_real_data_replay_matches_goldens() {
    let scid_dir = std::env::var("THE_DESK_GOLDEN_SCID_DIR")
        .expect("THE_DESK_GOLDEN_SCID_DIR is required for private golden replay");
    let expected_dir = std::env::var("THE_DESK_GOLDEN_EXPECTED_DIR")
        .expect("THE_DESK_GOLDEN_EXPECTED_DIR is required for private golden replay");
    let start_date = std::env::var("THE_DESK_GOLDEN_START_DATE").ok();
    let end_date = std::env::var("THE_DESK_GOLDEN_END_DATE").ok();
    let price_scale = std::env::var("THE_DESK_GOLDEN_PRICE_SCALE")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(1.0);

    let mut scid_paths = fs::read_dir(&scid_dir)
        .expect("read private SCID dir")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("scid"))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    scid_paths.sort();
    assert!(
        !scid_paths.is_empty(),
        "no .scid files found in {}",
        scid_dir
    );

    for scid_path in scid_paths {
        let stem = scid_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .expect("SCID file stem");
        let expected_path = Path::new(&expected_dir).join(format!("{stem}.expected_core.json"));
        let replay_case = ReplayCase {
            start_date: start_date.clone(),
            end_date: end_date.clone(),
            price_scale,
            ..ReplayCase::core()
        };
        let actual = run_core_replay(&scid_path, &replay_case);
        compare_or_bless(&expected_path, &actual);
    }
}
