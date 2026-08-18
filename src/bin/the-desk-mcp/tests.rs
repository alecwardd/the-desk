#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};
#[allow(unused_imports)]
use chrono::{TimeZone, Utc};
#[allow(unused_imports)]
use rmcp::model::{CallToolResult, RawContent};
#[allow(unused_imports)]
use rmcp::{handler::server::wrapper::Parameters, ErrorData as McpError};
use std::io::Write;
use std::path::Path;
#[allow(unused_imports)]
use std::sync::atomic::Ordering;
use tempfile::{tempdir, NamedTempFile};
#[allow(unused_imports)]
use the_desk_backend::db::{
    Database, HistoricalJobRun, RiskConfigRecord, SessionScopeFilter, SetupPerformanceSortBy,
    SetupRuntimeStateRecord, RESEARCH_DISTRIBUTION_METRICS,
};
use the_desk_backend::db::{PriorDayReference, SessionSummary, SignalOutcome};
#[allow(unused_imports)]
use the_desk_backend::depth::{
    DepthBook, DepthReader, DepthRecord, ScanControl as DepthScanControl,
};
#[allow(unused_imports)]
use the_desk_backend::feed::scid_reader::ScidReader;
#[allow(unused_imports)]
use the_desk_backend::feed::scid_reader::SCID_RECORD_SIZE;
#[allow(unused_imports)]
use the_desk_backend::feed::symbol_resolution::resolve_contract_metadata;
#[allow(unused_imports)]
use the_desk_backend::feed::{load_feed_config, TradeSide};
#[allow(unused_imports)]
use the_desk_backend::observability::RuntimeEventLevel;
#[allow(unused_imports)]
use the_desk_backend::pipelines::event_detector::MarketEvent;
use the_desk_backend::pipelines::PriorSessionData;
#[allow(unused_imports)]
use the_desk_backend::pipelines::{EventDetector, FlowEventEmitter, MarketState, PipelineEngine};
#[allow(unused_imports)]
use the_desk_backend::research;
#[allow(unused_imports)]
use the_desk_backend::risk::RiskState;
use the_desk_backend::rollover::{ContractRolloverAgentAction, ContractRolloverStatusKind};
use the_desk_backend::rules::SetupReadiness;
#[allow(unused_imports)]
use the_desk_backend::rules::{SetupDefinition, SetupState};
#[allow(unused_imports)]
use the_desk_backend::{
    minute_of_session_from_timestamp, outcomes, session_date_from_timestamp_ms,
    trading_day_from_timestamp_ms, DeltaSegment, SessionType,
};

fn summary_row(
    session_date: &str,
    session_type: &str,
    dnva_high: f64,
    dnva_low: f64,
    dnp: f64,
) -> SessionSummary {
    SessionSummary {
        session_date: session_date.to_string(),
        session_type: session_type.to_string(),
        root_symbol: "NQ".to_string(),
        contract_symbol: "NQH26.CME".to_string(),
        contract_month: Some("2026-03".to_string()),
        symbol_resolution_mode: "hybrid".to_string(),
        carry_forward_levels_valid: true,
        rollover_warning: None,
        open_price: dnva_low,
        high: dnva_high,
        low: dnva_low,
        close: dnp,
        poc: dnp,
        vah: dnva_high,
        val: dnva_low,
        ib_high: 0.0,
        ib_low: 0.0,
        ib_range: 0.0,
        ib_mid: 0.0,
        ib_extension_state: "None".to_string(),
        first_ib_extension_direction: None,
        first_ib_extension_timestamp_ms: None,
        or_high: 0.0,
        or_low: 0.0,
        day_type: String::new(),
        profile_shape: String::new(),
        balance_state: String::new(),
        total_volume: 0.0,
        tick_count: 0,
        session_delta: 0.0,
        cumulative_delta: 0.0,
        dnp,
        dnva_high,
        dnva_low,
        vwap_close: 0.0,
        signal_count: 0,
        single_prints_direction: String::new(),
        excess_high: false,
        excess_low: false,
        poor_high: false,
        poor_low: false,
        rvol_ratio: 0.0,
        close_vs_ib_mid: "n/a".to_string(),
        close_vs_vwap: "n/a".to_string(),
        close_vs_poc: "n/a".to_string(),
        snapshot_json: None,
    }
}

fn test_server() -> TheDeskMcp {
    let db = Database::open(":memory:").expect("db");
    let server = TheDeskMcp::new(db, PipelineEngine::new(), ":memory:".into());
    server
        .hydrate_playbook_runtime_cache()
        .expect("hydrate playbook cache");
    server
}

fn test_server_with_sil() -> TheDeskMcp {
    let db = Database::open(":memory:").expect("db");
    let logging_config = the_desk_backend::observability::LoggingConfig {
        destination: "none".to_string(),
        runtime_event_suppression_window_ms: 0,
        ..the_desk_backend::observability::LoggingConfig::default()
    };
    let mut server = TheDeskMcp::with_runtime_events_and_sil(
        db,
        PipelineEngine::new(),
        ":memory:".into(),
        std::sync::Arc::new(the_desk_backend::observability::RuntimeEventStore::new(
            &logging_config,
        )),
        the_desk_backend::catalog::SilConfig {
            catalog_discovery: true,
            ..Default::default()
        },
    );
    let artifacts = tempfile::tempdir().expect("research artifacts");
    server.research_artifact_dir = artifacts.path().to_path_buf();
    std::mem::forget(artifacts);
    server
        .hydrate_playbook_runtime_cache()
        .expect("hydrate playbook cache");
    server
}

fn test_contract_metadata() -> the_desk_backend::feed::ContractMetadata {
    the_desk_backend::feed::ContractMetadata {
        root_symbol: "NQ".to_string(),
        contract_symbol: "NQH26".to_string(),
        contract_month: Some("2026-03".to_string()),
        symbol_resolution_mode: "manual".to_string(),
        symbol_resolution_source: "test".to_string(),
        configured_symbol: "NQH26".to_string(),
        scid_file_exists: true,
        depth_file_count: 1,
        ..Default::default()
    }
}

fn pending_outcome(signal_id: &str, fired_at_ms: f64) -> SignalOutcome {
    SignalOutcome {
        signal_id: signal_id.to_string(),
        setup_id: "or5-mid-retest".to_string(),
        setup_name: Some("OR5 Mid Retest".to_string()),
        session_date: session_date_from_timestamp_ms(fired_at_ms),
        root_symbol: Some("NQ".to_string()),
        contract_symbol: Some("NQH26.CME".to_string()),
        source: "live".to_string(),
        job_id: None,
        fired_at_ms,
        fired_price: 100.0,
        target_price: Some(101.0),
        stop_price: Some(99.0),
        outcome: "pending".to_string(),
        outcome_at_ms: None,
        max_favorable_excursion: None,
        max_adverse_excursion: None,
        r_result: None,
        time_to_outcome_ms: None,
        rvol_at_fire: None,
        rvol_bucket_at_fire: None,
        direction: Some("long".to_string()),
        entry_price: Some(100.0),
        risk_points: Some(1.0),
        exit_price: None,
        outcome_quality: "verified".to_string(),
        quality_flags: Vec::new(),
        outcome_engine_version: None,
        rules_schema_version: None,
        setup_template_hash: None,
        last_observed_price: None,
        last_observed_at_ms: None,
    }
}

#[test]
fn in_memory_pending_outcomes_match_db_tracker_chronological_resolution() {
    let old_db = Database::open(":memory:").expect("old db");
    let new_db = Database::open(":memory:").expect("new db");
    let fired_at_ms = 1_766_162_400_000.0;
    let row = pending_outcome("sig-chronological", fired_at_ms);
    old_db.insert_signal_outcome(&row).expect("insert old");
    new_db.insert_signal_outcome(&row).expect("insert new");

    let mut pending = PendingOutcomeSet::default();
    pending
        .reconcile_from_db(&new_db)
        .expect("seed pending outcomes");

    for (price, ts) in [
        (101.0, fired_at_ms + 1_000.0),
        (99.0, fired_at_ms + 2_000.0),
    ] {
        the_desk_backend::outcome_tracker::on_tick(&old_db, price, ts).expect("db tick");
        pending.observe_tick(price, ts);
    }
    pending.flush_to_db(&new_db).expect("flush pending");

    let old_row = old_db
        .list_signal_outcomes_for_replay(Some("live"), None)
        .expect("old replay")
        .pop()
        .expect("old row");
    let new_row = new_db
        .list_signal_outcomes_for_replay(Some("live"), None)
        .expect("new replay")
        .pop()
        .expect("new row");

    assert_eq!(old_row.outcome, new_row.outcome);
    assert_eq!(old_row.outcome, "target_hit");
    assert_eq!(old_row.outcome_at_ms, new_row.outcome_at_ms);
    assert_eq!(old_row.exit_price, new_row.exit_price);
    assert_eq!(old_row.r_result, new_row.r_result);
    assert_eq!(
        old_row.max_favorable_excursion,
        new_row.max_favorable_excursion
    );
    assert_eq!(old_row.max_adverse_excursion, new_row.max_adverse_excursion);
}

#[test]
fn cached_boundary_data_installs_before_first_rth_tick_without_db_lookup() {
    let pipelines = std::sync::Arc::new(std::sync::Mutex::new(PipelineEngine::new()));
    let db = std::sync::Arc::new(std::sync::Mutex::new(
        Database::open(":memory:").expect("db"),
    ));
    let boundary_cache = std::sync::Arc::new(std::sync::Mutex::new(BoundarySessionCache {
        cached: Some(BoundarySessionCacheEntry {
            lookup_date: "2026-03-09".to_string(),
            new_session: SessionType::Rth,
            new_segment: DeltaSegment::Rth,
            contract_symbol: "NQH26".to_string(),
            prior_reference: Some(PriorDayReference {
                date: "2026-03-08".to_string(),
                high: 21100.0,
                low: 20900.0,
                close: 21025.0,
                va_high: Some(21080.0),
                va_low: Some(20940.0),
                poc: Some(21010.0),
                dnva_high: Some(21090.0),
                dnva_low: Some(20935.0),
                dnp: Some(21000.0),
                root_symbol: Some("NQ".to_string()),
                contract_symbol: Some("NQH26".to_string()),
            }),
            rollover_status: None,
            prior_inventory: vec![PriorSessionData {
                final_delta: 1_250.0,
                dnva_high: 21090.0,
                dnva_low: 20935.0,
                dnp: 21000.0,
            }],
            rth_rvol_curves: Vec::new(),
            globex_rvol_curves: Vec::new(),
            refreshed_at: std::time::Instant::now(),
        }),
    }));
    let boundary_ts = Utc
        .with_ymd_and_hms(2026, 3, 9, 13, 30, 0)
        .single()
        .expect("boundary")
        .timestamp_millis() as f64;

    prepare_for_new_session_with_cache(
        &pipelines,
        &db,
        None,
        &boundary_cache,
        SessionType::Rth,
        DeltaSegment::Rth,
        boundary_ts,
        &test_contract_metadata(),
    );

    let p = pipelines.lock().expect("pipelines");
    assert_eq!(p.levels.prior_day_high, 21100.0);
    assert_eq!(p.levels.prior_day_low, 20900.0);
    assert_eq!(p.levels.prior_day_close, 21025.0);
    assert_eq!(p.levels.prior_va_high, 21080.0);
    assert_eq!(p.levels.prior_dnva_high, 21090.0);
    assert_eq!(p.session_inventory.prior_sessions().len(), 1);
}

#[test]
fn open_burst_capped_chunks_match_uncapped_pipeline_state() {
    fn replay(prices: &[f64], chunk_size: usize) -> the_desk_backend::pipelines::MarketState {
        let pipelines = std::sync::Arc::new(std::sync::Mutex::new(PipelineEngine::new()));
        let detector = std::sync::Arc::new(std::sync::Mutex::new(EventDetector::new()));
        let flow_emitter = std::sync::Arc::new(std::sync::Mutex::new(FlowEventEmitter::new()));
        let last_bid = std::sync::Arc::new(std::sync::Mutex::new(0.0));
        let last_ask = std::sync::Arc::new(std::sync::Mutex::new(0.0));
        let mut events = Vec::new();
        let start_ts = Utc
            .with_ymd_and_hms(2026, 3, 9, 13, 30, 0)
            .single()
            .expect("start")
            .timestamp_millis() as f64;
        let mut latest = None;
        let mut tick_index = 0usize;

        for chunk in prices.chunks(chunk_size) {
            for price in chunk {
                latest = ingest_tick(
                    &pipelines,
                    &detector,
                    &flow_emitter,
                    None,
                    &last_bid,
                    &last_ask,
                    *price,
                    1.0 + (tick_index % 3) as f64,
                    tick_index.is_multiple_of(2),
                    start_ts + (tick_index as f64 * 250.0),
                    *price - 0.25,
                    *price + 0.25,
                    &mut events,
                )
                .map(|outcome| outcome.snapshot);
                tick_index += 1;
            }
        }

        latest.expect("snapshot")
    }

    let prices = [
        21000.0, 21000.25, 21000.5, 21001.0, 21000.75, 21001.25, 21001.5, 21001.0, 21000.5,
        21000.25,
    ];
    let uncapped = replay(&prices, prices.len());
    let capped = replay(&prices, 3);

    assert_eq!(uncapped.last_price, capped.last_price);
    assert_eq!(uncapped.session_delta, capped.session_delta);
    assert_eq!(uncapped.cumulative_delta, capped.cumulative_delta);
    assert_eq!(uncapped.session_high, capped.session_high);
    assert_eq!(uncapped.session_low, capped.session_low);
    assert_eq!(uncapped.vwap, capped.vwap);
}

#[tokio::test]
async fn get_runtime_events_returns_recent_and_persisted_events() {
    let server = test_server();
    record_runtime_event(
        &server.runtime_events,
        Some(&server.db),
        RuntimeEventLevel::Warn,
        "scid.tail_reset",
        "scid",
        "test tail reset",
        serde_json::json!({ "offset": 512 }),
    );

    let payload = parse_text_tool_result(
        server
            .get_runtime_events(Parameters(RuntimeEventsParams {
                limit: Some(10),
                min_level: Some("warn".to_string()),
                category: Some("scid".to_string()),
                include_persisted: Some(true),
                ..Default::default()
            }))
            .await
            .expect("runtime events"),
    );
    assert_eq!(payload["recentCount"].as_u64(), Some(1));
    assert_eq!(payload["persistedCount"].as_u64(), Some(1));
    let events = payload["events"].as_array().expect("events array");
    assert!(events
        .iter()
        .any(|event| event["eventName"].as_str() == Some("scid.tail_reset")));
}

#[tokio::test]
async fn get_runtime_events_rejects_level_and_min_level_together() {
    let server = test_server();
    let result = server
        .get_runtime_events(Parameters(RuntimeEventsParams {
            level: Some("warn".to_string()),
            min_level: Some("info".to_string()),
            ..Default::default()
        }))
        .await;
    assert!(result.is_err());
}

#[test]
fn rollover_runtime_event_does_not_relock_held_db_mutex() {
    use std::sync::mpsc;
    use std::time::Duration;

    let server = test_server();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let db = server.db.lock().expect("db lock");
        let contract = rollover_contract_metadata("NQM26");
        let result = server.rollover_status_for_date(
            &db,
            &contract,
            Some(&test_contract_metadata()),
            "2026-03-06",
            None,
        );
        let _ = tx.send(result.is_ok());
    });

    assert!(rx
        .recv_timeout(Duration::from_secs(2))
        .expect("no deadlock"));
}

fn rollover_contract_metadata(contract_symbol: &str) -> the_desk_backend::feed::ContractMetadata {
    the_desk_backend::feed::ContractMetadata {
        root_symbol: "NQ".to_string(),
        contract_symbol: contract_symbol.to_string(),
        contract_month: Some("2026-03".to_string()),
        symbol_resolution_mode: "manual".to_string(),
        symbol_resolution_source: "test".to_string(),
        configured_symbol: contract_symbol.to_string(),
        scid_file_exists: true,
        depth_file_count: 1,
        ..Default::default()
    }
}

#[test]
fn rollover_status_helper_accepts_same_contract_prior_reference() {
    let db = Database::open(":memory:").expect("db");
    db.save_prior_day_full_with_dnva_contract(
        "2026-03-04",
        21_100.0,
        20_900.0,
        21_000.0,
        21_050.0,
        20_950.0,
        21_000.0,
        Some(21_025.0),
        Some(20_975.0),
        Some(21_000.0),
        Some("NQ"),
        Some("NQH26"),
    )
    .expect("save prior");
    let contract = rollover_contract_metadata("NQH26");
    let status =
        build_rollover_status_from_db(&db, &contract, Some(&contract), "2026-03-05", Some(1_000.0))
            .expect("status");

    assert_eq!(status.status, ContractRolloverStatusKind::Ok);
    assert_eq!(
        status.agent_action,
        ContractRolloverAgentAction::UsePriorLevels
    );
    assert_eq!(
        status
            .prior_day_reference
            .as_ref()
            .and_then(|r| r.contract_symbol.as_deref()),
        Some("NQH26")
    );
    assert!(status.prior_references_authoritative);
}

#[test]
fn rollover_status_helper_labels_previous_contract_reference_as_legacy() {
    let db = Database::open(":memory:").expect("db");
    db.save_prior_day_full_with_dnva_contract(
        "2026-03-04",
        21_100.0,
        20_900.0,
        21_000.0,
        21_050.0,
        20_950.0,
        21_000.0,
        Some(21_025.0),
        Some(20_975.0),
        Some(21_000.0),
        Some("NQ"),
        Some("NQH26"),
    )
    .expect("save prior");
    let active = rollover_contract_metadata("NQM26");
    let status =
        build_rollover_status_from_db(&db, &active, Some(&active), "2026-03-05", Some(1_000.0))
            .expect("status");

    assert_eq!(status.status, ContractRolloverStatusKind::RolloverDetected);
    assert_eq!(
        status.agent_action,
        ContractRolloverAgentAction::LegacyContextOnly
    );
    assert_eq!(
        status
            .legacy_contract_reference
            .as_ref()
            .and_then(|r| r.contract_symbol.as_deref()),
        Some("NQH26")
    );
    assert!(!status.prior_references_authoritative);
    assert!(status.should_clear_prior_levels);
}

#[tokio::test]
async fn validate_contract_rollover_tool_returns_structured_status() {
    let server = test_server();
    let contract = resolve_contract_metadata(&load_feed_config());
    {
        let mut pipelines = server.pipelines.lock().expect("pipelines");
        pipelines.set_contract_metadata(contract.clone());
    }
    if !contract.root_symbol.is_empty() && !contract.contract_symbol.is_empty() {
        let db = server.db.lock().expect("db");
        db.save_prior_day_full_with_dnva_contract(
            "2026-03-04",
            21_100.0,
            20_900.0,
            21_000.0,
            21_050.0,
            20_950.0,
            21_000.0,
            Some(21_025.0),
            Some(20_975.0),
            Some(21_000.0),
            Some(contract.root_symbol.as_str()),
            Some(contract.contract_symbol.as_str()),
        )
        .expect("save prior");
    }

    let result = parse_text_tool_result(
        server
            .validate_contract_rollover()
            .await
            .expect("validate rollover"),
    );
    assert!(result.get("status").is_some());
    assert_eq!(
        result
            .get("activeContractSymbol")
            .and_then(|value| value.as_str()),
        Some(contract.contract_symbol.to_ascii_uppercase().as_str())
    );
    assert!(result.get("priorReferenceTrust").is_some());
}

fn write_scid_header(file: &mut NamedTempFile) {
    const SCID_HEADER_SIZE_TEST: usize = 56;
    let mut header = vec![0_u8; SCID_HEADER_SIZE_TEST];
    header[0..4].copy_from_slice(b"SCID");
    header[4..8].copy_from_slice(&(SCID_HEADER_SIZE_TEST as u32).to_le_bytes());
    header[8..12].copy_from_slice(&(SCID_RECORD_SIZE as u32).to_le_bytes());
    file.write_all(&header).expect("header");
    file.flush().expect("flush");
}

fn append_scid_record(file: &mut NamedTempFile, price: f32, timestamp_ms: f64) {
    const SC_TO_UNIX_EPOCH_US_TEST: i64 = 2_209_161_600_000_000;
    let mut rec = [0_u8; SCID_RECORD_SIZE];
    let unix_us = (timestamp_ms * 1_000.0).round() as i64;
    let sc_us = SC_TO_UNIX_EPOCH_US_TEST + unix_us;
    rec[0..8].copy_from_slice(&sc_us.to_le_bytes());
    rec[12..16].copy_from_slice(&(price + 0.25).to_le_bytes());
    rec[16..20].copy_from_slice(&(price - 0.25).to_le_bytes());
    rec[20..24].copy_from_slice(&price.to_le_bytes());
    rec[24..28].copy_from_slice(&(1_u32).to_le_bytes());
    rec[28..32].copy_from_slice(&(2_u32).to_le_bytes());
    rec[32..36].copy_from_slice(&(0_u32).to_le_bytes());
    rec[36..40].copy_from_slice(&(2_u32).to_le_bytes());
    file.write_all(&rec).expect("record");
}

fn append_scid_record_with_scale(
    file: &mut NamedTempFile,
    price: f64,
    timestamp_ms: f64,
    price_scale: f64,
) {
    const SC_TO_UNIX_EPOCH_US_TEST: i64 = 2_209_161_600_000_000;
    let mut rec = [0_u8; SCID_RECORD_SIZE];
    let unix_us = (timestamp_ms * 1_000.0).round() as i64;
    let sc_us = SC_TO_UNIX_EPOCH_US_TEST + unix_us;
    let raw_price = (price * price_scale) as f32;
    let raw_bid = ((price - 0.25) * price_scale) as f32;
    let raw_ask = ((price + 0.25) * price_scale) as f32;
    rec[0..8].copy_from_slice(&sc_us.to_le_bytes());
    rec[12..16].copy_from_slice(&raw_ask.to_le_bytes());
    rec[16..20].copy_from_slice(&raw_bid.to_le_bytes());
    rec[20..24].copy_from_slice(&raw_price.to_le_bytes());
    rec[24..28].copy_from_slice(&(1_u32).to_le_bytes());
    rec[28..32].copy_from_slice(&(2_u32).to_le_bytes());
    rec[32..36].copy_from_slice(&(0_u32).to_le_bytes());
    rec[36..40].copy_from_slice(&(2_u32).to_le_bytes());
    file.write_all(&rec).expect("scaled record");
}

fn append_scid_sequence(file: &mut NamedTempFile, start_idx: usize, prices: &[f32]) {
    let base_ts_ms = Utc
        .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
        .single()
        .expect("base timestamp")
        .timestamp_millis() as f64;
    for (idx, price) in prices.iter().enumerate() {
        let ts_ms = base_ts_ms + (start_idx + idx) as f64;
        append_scid_record(file, *price, ts_ms);
    }
    file.flush().expect("flush");
}

fn append_scid_scaled_sequence(
    file: &mut NamedTempFile,
    start_idx: usize,
    prices: &[f64],
    price_scale: f64,
) {
    let base_ts_ms = Utc
        .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
        .single()
        .expect("base timestamp")
        .timestamp_millis() as f64;
    for (idx, price) in prices.iter().enumerate() {
        let ts_ms = base_ts_ms + (start_idx + idx) as f64;
        append_scid_record_with_scale(file, *price, ts_ms, price_scale);
    }
    file.flush().expect("flush");
}

fn write_test_depth_file(path: &Path, records: &[(i64, u8, u8, u16, f32, u32)]) {
    const DEPTH_HEADER_SIZE_TEST: usize = 64;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"SCDD");
    bytes.extend_from_slice(&(DEPTH_HEADER_SIZE_TEST as u32).to_le_bytes());
    bytes.extend_from_slice(&(24_u32).to_le_bytes());
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&[0_u8; DEPTH_HEADER_SIZE_TEST - 16]);
    for (dt, cmd, flags, num_orders, price, qty) in records {
        bytes.extend_from_slice(&dt.to_le_bytes());
        bytes.push(*cmd);
        bytes.push(*flags);
        bytes.extend_from_slice(&num_orders.to_le_bytes());
        bytes.extend_from_slice(&price.to_le_bytes());
        bytes.extend_from_slice(&qty.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
    }
    std::fs::write(path, bytes).expect("write depth");
}

fn unix_ms_to_sc_depth(ms: i64) -> i64 {
    ms * 1_000 + 2_209_161_600_000_000
}

fn parse_text_tool_result(result: CallToolResult) -> serde_json::Value {
    match &result.content[0].raw {
        RawContent::Text(text) => serde_json::from_str(&text.text).expect("json text result"),
        other => panic!("expected text tool result, got {other:?}"),
    }
}

#[test]
fn scid_poll_step_reads_new_ticks_once_from_resume_offset() {
    let mut file = NamedTempFile::new().expect("temp");
    write_scid_header(&mut file);
    append_scid_sequence(&mut file, 0, &[21000.0, 21000.25, 21000.5]);
    let reader = ScidReader::new(file.path());

    let first = read_scid_poll_step(&reader, safe_scid_data_offset(&reader)).expect("first");
    append_scid_sequence(&mut file, 3, &[21000.75, 21001.0]);
    let second = read_scid_poll_step(&reader, first.next_offset).expect("second");

    assert_eq!(first.ticks.len(), 3);
    assert_eq!(first.ticks[0].price, 21000.0);
    assert_eq!(second.ticks.len(), 2);
    assert_eq!(second.ticks[0].price, 21000.75);
    assert!(second.next_offset > first.next_offset);
}

#[test]
fn scid_poll_step_preserves_configured_price_scale() {
    let mut file = NamedTempFile::new().expect("temp");
    write_scid_header(&mut file);
    append_scid_record_with_scale(&mut file, 21000.0, 1_700_000_000_000.0, 100.0);
    append_scid_record_with_scale(&mut file, 21000.25, 1_700_000_000_001.0, 100.0);
    file.flush().expect("flush");

    let reader = ScidReader::with_price_scale(file.path(), 100.0);
    let batch = read_scid_poll_step(&reader, safe_scid_data_offset(&reader)).expect("step");

    assert_eq!(batch.ticks.len(), 2);
    assert!((batch.ticks[0].price - 21000.0).abs() < 1e-9);
    assert!((batch.ticks[1].price - 21000.25).abs() < 1e-9);
    assert!((batch.ticks[0].ask - 21000.25).abs() < 1e-9);
    assert!((batch.ticks[0].bid - 20999.75).abs() < 1e-9);
}

#[test]
fn tape_pace_response_marks_live_and_recomputes_event_lag() {
    let payload = serde_json::json!({
        "ticksPerSec5s": 1.2,
        "ticksPerSec30s": 1.0,
        "ticksPerSec5m": 0.8,
        "volumePerSec5s": 12.0,
        "volumePerSec30s": 10.0,
        "volumePerSec5m": 8.0,
        "acceleration": 0.15,
        "rawAcceleration": 0.2,
        "pacePercentile": 0.7,
        "rollingPacePercentile": 0.8,
        "regimeTicksPerSec30mEma": 0.9,
        "regimeVolumePerSec30mEma": 9.0,
        "windowCoverage5s": 1.0,
        "windowCoverage30s": 1.0,
        "windowCoverage5m": 1.0,
        "isValid5s": true,
        "isValid30s": true,
        "isValid5m": true,
        "windowAnchorTimestampMs": 12_000.0,
        "lastTradeTimestampMs": 12_000.0,
        "dwellAtCurrentPriceMs": 2_500.0,
        "currentPrice": 21000.25
    });
    let rendered = build_tape_pace_response(payload, 250.0, true, 12_900.0);
    assert_eq!(
        rendered.get("dataQuality").and_then(|v| v.as_str()),
        Some("LIVE")
    );
    assert_eq!(rendered.get("isLive").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
        rendered.get("eventTimeLagMs").and_then(|v| v.as_f64()),
        Some(900.0)
    );
}

#[test]
fn tape_pace_response_marks_partial_when_payload_is_missing_fields() {
    let payload = serde_json::json!({
        "ticksPerSec5s": 1.2,
        "pacePercentile": 0.7,
        "lastTradeTimestampMs": 12_000.0
    });
    let rendered = build_tape_pace_response(payload, 2_000.0, false, 13_000.0);
    assert_eq!(
        rendered.get("dataQuality").and_then(|v| v.as_str()),
        Some("PARTIAL")
    );
    assert_eq!(
        rendered.get("isLive").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert_eq!(
        rendered.get("eventTimeLagMs").and_then(|v| v.as_f64()),
        Some(1_000.0)
    );
}

#[test]
fn tick_ms_bits_roundtrip_positive() {
    let t = 1_700_000_000_123.0;
    assert_eq!(tick_ms_from_bits(tick_ms_to_bits(t)), Some(t));
    assert_eq!(tick_ms_to_bits(0.0), 0);
    assert_eq!(tick_ms_from_bits(0), None);
}

#[test]
fn documented_mcp_tool_count_matches_router() {
    let tool_count = TheDeskMcp::tool_router().list_all().len();
    let expected = format!("{tool_count} MCP tools");

    // Every authoritative surface that states the count must agree with the live
    // router (the generated tool-reference.md is the catalog; these are the prose,
    // config, and runtime-instruction surfaces a reader or agent might trust).
    for (path, contents) in [
        ("AGENT.md", include_str!("../../../AGENT.md")),
        ("README.md", include_str!("../../../README.md")),
        ("CLAUDE.md", include_str!("../../../CLAUDE.md")),
        (".cursorrules", include_str!("../../../.cursorrules")),
        (
            "docs/mcp/README.md",
            include_str!("../../../docs/mcp/README.md"),
        ),
        (
            "skills/mcp-tools/SKILL.md",
            include_str!("../../../skills/mcp-tools/SKILL.md"),
        ),
        (
            "handler.rs (server instructions)",
            include_str!("handler.rs"),
        ),
    ] {
        assert!(
            contents.contains(&expected),
            "{path} must state the actual tool count: \"{expected}\""
        );
    }
}

#[test]
fn domain_routers_cover_combined_router_exactly() {
    let combined: std::collections::BTreeSet<String> = TheDeskMcp::tool_router()
        .list_all()
        .into_iter()
        .map(|t| t.name.to_string())
        .collect();
    let mut from_domains = std::collections::BTreeSet::new();
    for domain in crate::docs::tool_domains() {
        for tool in (domain.router)().list_all() {
            assert!(
                from_domains.insert(tool.name.to_string()),
                "tool `{}` appears in more than one domain router",
                tool.name
            );
        }
    }
    assert_eq!(
        combined, from_domains,
        "docs::tool_domains() must list the same routers service.rs combines"
    );
}

#[test]
fn tool_reference_doc_is_current() {
    let expected = crate::docs::render_tool_reference();
    let path = crate::docs::tool_reference_path();
    let on_disk = std::fs::read_to_string(&path)
        .unwrap_or_default()
        .replace("\r\n", "\n");
    assert!(
        on_disk == expected.replace("\r\n", "\n"),
        "docs/mcp/tool-reference.md is stale; regenerate with \
         `cargo run --bin the-desk-mcp -- --write-tool-docs`"
    );
}

#[test]
fn specialty_market_tools_are_frozen_until_catalog_v0() {
    let live: Vec<String> = TheDeskMcp::market_router()
        .list_all()
        .into_iter()
        .map(|t| t.name.to_string())
        .collect();
    let mut sorted_live = live.clone();
    sorted_live.sort();
    let frozen: Vec<String> = crate::tool_telemetry::FROZEN_MARKET_TOOLS
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    assert_eq!(
        sorted_live, frozen,
        "SIL-M0 freeze set remains the specialty market allowlist. Catalog v0 \
         adds the enforceable rule no-catalog-entry → no new market tool \
         (see specialty_market_tools_require_catalog_allowlist_entry). \
         Expanding the set requires updating FROZEN_MARKET_TOOLS and the \
         catalog specialty_market_tools allowlist together — do not re-bless \
         the SIL-M0 telemetry baseline unless the orientation-chain contract changes."
    );
    assert!(
        crate::tool_telemetry::FROZEN_MARKET_TOOLS
            .windows(2)
            .all(|w| w[0] < w[1]),
        "FROZEN_MARKET_TOOLS must stay sorted"
    );
}

#[test]
fn specialty_market_tools_require_catalog_allowlist_entry() {
    let catalog = the_desk_backend::catalog::build_catalog();
    let live: std::collections::BTreeSet<String> = TheDeskMcp::market_router()
        .list_all()
        .into_iter()
        .map(|t| t.name.to_string())
        .collect();
    let allow: std::collections::BTreeSet<String> =
        catalog.specialty_market_tools.iter().cloned().collect();
    assert_eq!(
        live, allow,
        "Catalog v0: every specialty market tool must have a catalog allowlist entry \
         (no catalog entry → no new market tool)"
    );
}

/// Pinned non-detector specialty market tools. A new live market tool must be
/// added here **or** to `DETECTOR_SPECIALTY_TOOLS` (with an active registry id).
const NON_DETECTOR_MARKET_TOOLS: &[&str] = &[
    "check_delta_confirmation",
    "get_context_frame",
    "get_day_type",
    "get_delta_at_price",
    "get_delta_profile",
    "get_footprint",
    "get_footprint_window",
    "get_imbalances",
    "get_key_levels",
    "get_market_snapshot",
    "get_or5_status",
    "get_proximity_report",
    "get_rvol",
    "get_session_context",
    "get_session_inventory",
    "get_session_summary",
    "get_snapshot_at",
    "get_tape_pace",
    "get_tpo_detail",
    "get_tpo_profile",
];

#[test]
fn detector_specialty_tools_partition_live_market_router() {
    let live: std::collections::BTreeSet<String> = TheDeskMcp::market_router()
        .list_all()
        .into_iter()
        .map(|t| t.name.to_string())
        .collect();
    let detector: std::collections::BTreeSet<String> =
        the_desk_backend::catalog::DETECTOR_SPECIALTY_TOOLS
            .iter()
            .map(|(tool, _)| (*tool).to_string())
            .collect();
    let non_detector: std::collections::BTreeSet<String> = NON_DETECTOR_MARKET_TOOLS
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    assert!(
        NON_DETECTOR_MARKET_TOOLS.windows(2).all(|w| w[0] < w[1]),
        "NON_DETECTOR_MARKET_TOOLS must stay sorted"
    );
    assert!(
        detector.is_disjoint(&non_detector),
        "a market tool cannot be both detector-backed and pinned non-detector"
    );
    let classified: std::collections::BTreeSet<String> =
        detector.union(&non_detector).cloned().collect();
    assert_eq!(
        live, classified,
        "every live market tool must be classified as detector-backed \
         (DETECTOR_SPECIALTY_TOOLS + active Feature Registry id) or pinned \
         non-detector (NON_DETECTOR_MARKET_TOOLS). No catalog/registry entry \
         → no new detector tool."
    );
    let catalog = the_desk_backend::catalog::build_catalog();
    for (tool, id) in the_desk_backend::catalog::DETECTOR_SPECIALTY_TOOLS {
        assert!(
            live.contains(*tool),
            "{tool} must be on the live market router"
        );
        let detector = catalog
            .base_detectors
            .iter()
            .find(|d| d.id == *id)
            .unwrap_or_else(|| panic!("{id} must be a shipped Base Detector"));
        assert_eq!(
            detector.promotion_state,
            the_desk_backend::catalog::PromotionState::Active,
            "{id} must be active before {tool} can exist"
        );
        assert!(the_desk_backend::catalog::concept_has_catalog_or_registry_entry(&catalog, id));
    }
}

#[test]
fn builtin_feature_event_types_are_in_the_canonical_emitted_set() {
    let mut canonical: std::collections::BTreeSet<String> = RESEARCH_EVENT_TYPES
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    for name in RESEARCH_LEVEL_TEST_NAMES {
        canonical.insert(format!("{name}_test"));
    }
    // EventDetector emits current-session VA/POC tests and prior_* aliases
    // that the research query allowlist names differently.
    for name in [
        "vah",
        "val",
        "poc",
        "prior_close",
        "prior_vah",
        "prior_val",
        "prior_poc",
    ] {
        canonical.insert(format!("{name}_test"));
    }
    canonical.insert("rvol_at_ib_close".into());
    let catalog = the_desk_backend::catalog::build_catalog();
    for detector in &catalog.base_detectors {
        for event in &detector.schema.event_types {
            assert!(
                canonical.contains(event),
                "{} lists event type `{event}` which is not in RESEARCH_EVENT_TYPES, \
                 RESEARCH_LEVEL_TEST_NAMES-derived `_test` names, or EventDetector aliases",
                detector.id
            );
        }
    }
}

#[test]
fn discovery_tools_absent_from_default_router() {
    let names: std::collections::BTreeSet<_> = TheDeskMcp::tool_router()
        .list_all()
        .into_iter()
        .map(|t| t.name.to_string())
        .collect();
    for tool in the_desk_backend::catalog::KERNEL_READ_QUERY_TOOLS {
        assert!(
            !names.contains(*tool),
            "{tool} must be omitted from the default router when SIL discovery is off"
        );
    }
    assert_eq!(names.len(), 123);
}

#[test]
fn discovery_tools_present_when_sil_flag_on() {
    let sil = the_desk_backend::catalog::SilConfig {
        catalog_discovery: true,
        ..Default::default()
    };
    let names: std::collections::BTreeSet<_> = TheDeskMcp::tool_router_with_sil(&sil)
        .list_all()
        .into_iter()
        .map(|t| t.name.to_string())
        .collect();
    for tool in the_desk_backend::catalog::KERNEL_READ_QUERY_TOOLS {
        assert!(
            names.contains(*tool),
            "{tool} must appear when [sil].catalog_discovery = true"
        );
    }
    assert_eq!(names.len(), 132);
}

#[test]
fn kernel_read_query_tools_are_trust_level_l0_without_mutation_authority() {
    let caps = the_desk_backend::catalog::kernel_read_query_capabilities();
    for tool in the_desk_backend::catalog::KERNEL_READ_QUERY_TOOLS {
        let cap = caps
            .get(tool)
            .unwrap_or_else(|| panic!("missing cap for {tool}"));
        assert_eq!(
            cap.trust_level,
            the_desk_backend::catalog::TrustLevel::L0,
            "{tool} must be Trust Level L0"
        );
        assert!(
            !cap.mutation_authority,
            "{tool} must not carry mutation authority"
        );
        assert!(
            !cap.order_authority,
            "{tool} must not carry order authority"
        );
        assert!(
            !the_desk_backend::catalog::tool_name_implies_mutation(tool),
            "{tool} must not match mutation-verb naming"
        );
    }
    // Router surface: every kernel tool present under SIL must remain non-mutating.
    let sil = the_desk_backend::catalog::SilConfig {
        catalog_discovery: true,
        ..Default::default()
    };
    let names: Vec<String> = TheDeskMcp::tool_router_with_sil(&sil)
        .list_all()
        .into_iter()
        .map(|t| t.name.to_string())
        .collect();
    for tool in the_desk_backend::catalog::KERNEL_READ_QUERY_TOOLS {
        assert!(names.iter().any(|n| n == tool));
        assert!(!the_desk_backend::catalog::tool_name_implies_mutation(tool));
    }
}

#[test]
fn desk_catalog_docs_are_current() {
    let catalog = the_desk_backend::catalog::build_catalog();
    let expected_json = the_desk_backend::catalog::render_catalog_json(&catalog);
    let expected_md = the_desk_backend::catalog::render_catalog_markdown(&catalog);
    let json_path = the_desk_backend::catalog::catalog_json_path();
    let md_path = the_desk_backend::catalog::catalog_markdown_path();
    let on_disk_json = std::fs::read_to_string(&json_path)
        .unwrap_or_default()
        .replace("\r\n", "\n");
    let on_disk_md = std::fs::read_to_string(&md_path)
        .unwrap_or_default()
        .replace("\r\n", "\n");
    assert!(
        on_disk_json == expected_json.replace("\r\n", "\n"),
        "docs/mcp/desk-catalog-v0.json is stale; regenerate with \
         `cargo run --bin the-desk-mcp -- --write-catalog-docs`"
    );
    assert!(
        on_disk_md == expected_md.replace("\r\n", "\n"),
        "docs/mcp/desk-catalog-v0.md is stale; regenerate with \
         `cargo run --bin the-desk-mcp -- --write-catalog-docs`"
    );
}

#[tokio::test]
async fn discovery_tools_return_metadata_only() {
    let server = test_server_with_sil();
    let env = parse_text_tool_result(
        server
            .describe_environment()
            .await
            .expect("describe_environment"),
    );
    assert_eq!(env["metadataOnly"], true);
    assert!(env.get("catalogVersion").is_some());
    assert!(env.get("lastPrice").is_none());
    assert!(env.get("vwap").is_none());
    assert_eq!(env["trustLevel"], "L0");
    assert_eq!(env["mutationAuthority"], false);

    let domain = parse_text_tool_result(
        server
            .describe_domain(Parameters(DescribeDomainParams {
                domain: Some("positioning".into()),
            }))
            .await
            .expect("describe_domain"),
    );
    assert_eq!(domain["metadataOnly"], true);
    let kinds = domain["recordKinds"]
        .as_array()
        .expect("positioning recordKinds");
    let kind_ids: std::collections::BTreeSet<_> = kinds
        .iter()
        .filter_map(|k| k.get("id").and_then(|v| v.as_str()))
        .collect();
    assert!(kind_ids.contains("position_grid"));
    assert!(kind_ids.contains("positions_by_strike"));
    assert!(kind_ids.contains("slice"));
    assert!(kind_ids.contains("levels_only"));
    assert!(domain.get("lastPrice").is_none());

    let hits = parse_text_tool_result(
        server
            .search_catalog(Parameters(SearchCatalogParams {
                query: Some("poc".into()),
            }))
            .await
            .expect("search_catalog"),
    );
    assert_eq!(hits["metadataOnly"], true);
    assert!(hits["hitCount"].as_u64().unwrap_or(0) >= 1);

    let pinch = parse_text_tool_result(
        server
            .search_catalog(Parameters(SearchCatalogParams {
                query: Some("pinch".into()),
            }))
            .await
            .expect("search_catalog pinch"),
    );
    let feature_hits = pinch["featureHits"].as_array().expect("featureHits");
    assert!(feature_hits
        .iter()
        .any(|h| h["id"] == "detector.pinch" && h["promotionState"] == "active"));
    assert_eq!(env["featureRegistry"]["humanGated"], true);
    assert_eq!(env["featureRegistry"]["writeVerb"], "feature_registry");
    assert_eq!(env["featureRegistry"]["discoveryEnabled"], true);
    assert_eq!(env["featureRegistry"]["readRequiresCatalogDiscovery"], true);
    assert_eq!(env["featureRegistry"]["featureIr"], true);
    assert_eq!(env["featureRegistry"]["codegen"], true);
    assert_eq!(
        env["featureRegistry"]["newOperatorFamilyGate"],
        "registry_change_proposal"
    );
}

#[tokio::test]
async fn feature_registry_register_and_promote_is_human_gated() {
    let server = test_server_with_sil();
    let registered = parse_text_tool_result(
        server
            .feature_registry(Parameters(
                the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                    action: Some("register".into()),
                    feature_id: Some("detector.example_candidate".into()),
                    name: Some("example_candidate".into()),
                    description: Some("Schema-only Base Detector candidate.".into()),
                    domain_id: Some("flow".into()),
                    event_types: Some(vec!["example_candidate_detected".into()]),
                    rust_module: Some("unwired".into()),
                    kind: Some("baseDetector".into()),
                    ..Default::default()
                },
            ))
            .await
            .expect("register"),
    );
    assert_eq!(registered["feature"]["promotionState"], "candidate");
    assert_eq!(registered["mutationAuthority"], true);
    assert_eq!(registered["orderAuthority"], false);
    assert_eq!(registered["readOperator"], "search_catalog");
    assert_eq!(registered["discoveryEnabled"], true);
    assert_eq!(registered["readRequiresCatalogDiscovery"], true);
    assert_eq!(registered["feature"]["schema"]["unit"], "count");
    assert_eq!(registered["feature"]["schema"]["sessionScope"], "session");
    assert_eq!(
        registered["feature"]["schema"]["freshness"],
        "liveTickAnchored"
    );
    assert_eq!(registered["feature"]["schema"]["costHint"], "R1");

    let missing_gate = server
        .feature_registry(Parameters(
            the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                action: Some("promote".into()),
                feature_id: Some("detector.example_candidate".into()),
                target_state: Some("shadow".into()),
                trader_confirmation: Some("".into()),
                ..Default::default()
            },
        ))
        .await;
    assert!(missing_gate.is_err(), "empty traderConfirmation must fail");

    let skip = server
        .feature_registry(Parameters(
            the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                action: Some("promote".into()),
                feature_id: Some("detector.example_candidate".into()),
                target_state: Some("active".into()),
                trader_confirmation: Some("skip is not allowed".into()),
                ..Default::default()
            },
        ))
        .await;
    assert!(skip.is_err(), "candidate → active must fail");

    let shadow = parse_text_tool_result(
        server
            .feature_registry(Parameters(
                the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                    action: Some("promote".into()),
                    feature_id: Some("detector.example_candidate".into()),
                    target_state: Some("shadow".into()),
                    trader_confirmation: Some(
                        "Your rules say this candidate may run in shadow.".into(),
                    ),
                    ..Default::default()
                },
            ))
            .await
            .expect("shadow"),
    );
    assert_eq!(shadow["feature"]["promotionState"], "shadow");

    let active = parse_text_tool_result(
        server
            .feature_registry(Parameters(
                the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                    action: Some("promote".into()),
                    feature_id: Some("detector.example_candidate".into()),
                    target_state: Some("active".into()),
                    trader_confirmation: Some(
                        "Your playbook indicates this descriptor may be active.".into(),
                    ),
                    ..Default::default()
                },
            ))
            .await
            .expect("active"),
    );
    assert_eq!(active["feature"]["promotionState"], "active");

    let hits = parse_text_tool_result(
        server
            .search_catalog(Parameters(SearchCatalogParams {
                query: Some("example_candidate".into()),
            }))
            .await
            .expect("search overlay"),
    );
    let feature_hits = hits["featureHits"].as_array().expect("featureHits");
    assert!(feature_hits
        .iter()
        .any(|h| { h["id"] == "detector.example_candidate" && h["promotionState"] == "active" }));

    let builtin = server
        .feature_registry(Parameters(
            the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                action: Some("promote".into()),
                feature_id: Some("detector.absorption".into()),
                target_state: Some("shadow".into()),
                trader_confirmation: Some("must not mutate shipped math".into()),
                ..Default::default()
            },
        ))
        .await;
    assert!(builtin.is_err(), "shipped absorption must stay immutable");
}

#[tokio::test]
async fn feature_registry_register_accepts_schema_enums_and_rejects_unknown() {
    let server = test_server_with_sil();
    let registered = parse_text_tool_result(
        server
            .feature_registry(Parameters(
                the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                    action: Some("register".into()),
                    feature_id: Some("detector.schema_enums".into()),
                    name: Some("schema_enums".into()),
                    description: Some("Candidate with explicit schema enums.".into()),
                    domain_id: Some("location_structure".into()),
                    event_types: Some(vec!["schema_enums_detected".into()]),
                    rust_module: Some("unwired".into()),
                    kind: Some("baseDetector".into()),
                    unit: Some("ticks".into()),
                    session_scope: Some("rth".into()),
                    freshness: Some("sessionScoped".into()),
                    cost_hint: Some("R2".into()),
                    ..Default::default()
                },
            ))
            .await
            .expect("register schema enums"),
    );
    assert_eq!(registered["feature"]["schema"]["unit"], "ticks");
    assert_eq!(registered["feature"]["schema"]["sessionScope"], "rth");
    assert_eq!(
        registered["feature"]["schema"]["freshness"],
        "sessionScoped"
    );
    assert_eq!(registered["feature"]["schema"]["costHint"], "R2");

    let unknown = server
        .feature_registry(Parameters(
            the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                action: Some("register".into()),
                feature_id: Some("detector.bad_unit".into()),
                name: Some("bad_unit".into()),
                description: Some("Unknown unit must fail closed.".into()),
                domain_id: Some("flow".into()),
                event_types: Some(vec!["bad_unit_detected".into()]),
                rust_module: Some("unwired".into()),
                unit: Some("furlongs".into()),
                ..Default::default()
            },
        ))
        .await;
    assert!(unknown.is_err(), "unknown unit must be rejected");
}

#[tokio::test]
async fn feature_registry_reports_discovery_disabled_when_sil_off() {
    let server = test_server();
    assert!(!server.sil_config.catalog_discovery);
    let registered = parse_text_tool_result(
        server
            .feature_registry(Parameters(
                the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                    action: Some("register".into()),
                    feature_id: Some("detector.default_surface".into()),
                    name: Some("default_surface".into()),
                    description: Some("Write verb stays on the default surface.".into()),
                    domain_id: Some("flow".into()),
                    event_types: Some(vec!["default_surface_detected".into()]),
                    rust_module: Some("unwired".into()),
                    ..Default::default()
                },
            ))
            .await
            .expect("register on default surface"),
    );
    assert_eq!(registered["discoveryEnabled"], false);
    assert_eq!(registered["readRequiresCatalogDiscovery"], true);
    assert_eq!(registered["readOperator"], "search_catalog");
}

#[tokio::test]
async fn feature_registry_register_derived_feature_and_rejects_unfunded_family() {
    let server = test_server_with_sil();
    let registered = parse_text_tool_result(
        server
            .feature_registry(Parameters(
                the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                    action: Some("register".into()),
                    feature_id: Some("feature.session_last_price_percentile".into()),
                    name: Some("session_last_price_percentile".into()),
                    description: Some(
                        "Session-distribution percentiles of lastPrice (Feature-IR).".into(),
                    ),
                    domain_id: Some("location_structure".into()),
                    kind: Some("derivedFeature".into()),
                    unit: Some("percent".into()),
                    cost_hint: Some("R2".into()),
                    program: Some(serde_json::json!({
                        "family": "sessionDistributionPercentiles",
                        "field": "market.location_structure.lastPrice"
                    })),
                    ..Default::default()
                },
            ))
            .await
            .expect("register derived"),
    );
    assert_eq!(registered["feature"]["kind"], "derivedFeature");
    assert_eq!(registered["feature"]["promotionState"], "candidate");
    assert_eq!(registered["feature"]["provenance"]["source"], "feature_ir");
    assert_eq!(
        registered["feature"]["program"]["family"],
        "sessionDistributionPercentiles"
    );
    assert_eq!(registered["orderAuthority"], false);
    assert!(
        registered["note"]
            .as_str()
            .is_some_and(|n| n.contains("five kernel artifacts")),
        "agents must know an active Derived Feature is codegen'd onto the existing kernel"
    );

    let skip = server
        .feature_registry(Parameters(
            the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                action: Some("promote".into()),
                feature_id: Some("feature.session_last_price_percentile".into()),
                target_state: Some("active".into()),
                trader_confirmation: Some("Your rules say skip is not allowed.".into()),
                ..Default::default()
            },
        ))
        .await;
    assert!(
        skip.is_err(),
        "candidate → active must fail for derived too"
    );

    let surface = server
        .feature_registry(Parameters(
            the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                action: Some("register".into()),
                feature_id: Some("feature.surface_lookup".into()),
                name: Some("surface_lookup".into()),
                description: Some("Unfunded surface interpolation must fail.".into()),
                domain_id: Some("positioning".into()),
                kind: Some("derivedFeature".into()),
                program: Some(serde_json::json!({
                    "family": "surfaceLookup",
                    "field": "positioning.derivedLevels"
                })),
                ..Default::default()
            },
        ))
        .await;
    assert!(
        surface.is_err(),
        "surface lookup must be rejected at declaration"
    );

    let unknown_key = server
        .feature_registry(Parameters(
            the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                action: Some("register".into()),
                feature_id: Some("feature.typo_baseline".into()),
                name: Some("typo_baseline".into()),
                description: Some("Wrong-case sameTimeOfDay must fail closed.".into()),
                domain_id: Some("location_structure".into()),
                kind: Some("derivedFeature".into()),
                program: Some(serde_json::json!({
                    "family": "historicalBaselines",
                    "field": "market.location_structure.lastPrice",
                    "lookbackDays": 5,
                    "sametimeofday": false
                })),
                ..Default::default()
            },
        ))
        .await;
    assert!(
        unknown_key.is_err(),
        "unknown program keys must be rejected at declaration"
    );

    let missing_program = server
        .feature_registry(Parameters(
            the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                action: Some("register".into()),
                feature_id: Some("feature.no_program".into()),
                name: Some("no_program".into()),
                description: Some("Derived Feature without a program.".into()),
                domain_id: Some("flow".into()),
                kind: Some("derivedFeature".into()),
                ..Default::default()
            },
        ))
        .await;
    assert!(missing_program.is_err(), "derivedFeature requires program");
}

#[tokio::test]
async fn sil_m5c_accepted_derived_feature_codegen_end_to_end_on_existing_kernel() {
    use the_desk_backend::db::JournalFrameRecord;

    let server = test_server_with_sil();
    let feature_id = "feature.session_last_price_percentile";
    let registered = parse_text_tool_result(
        server
            .feature_registry(Parameters(
                the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                    action: Some("register".into()),
                    feature_id: Some(feature_id.into()),
                    name: Some("session_last_price_percentile".into()),
                    description: Some(
                        "Session-distribution percentiles of lastPrice (Feature-IR).".into(),
                    ),
                    domain_id: Some("location_structure".into()),
                    kind: Some("derivedFeature".into()),
                    unit: Some("percent".into()),
                    cost_hint: Some("R1".into()),
                    program: Some(serde_json::json!({
                        "family": "sessionDistributionPercentiles",
                        "field": "market.location_structure.lastPrice"
                    })),
                    ..Default::default()
                },
            ))
            .await
            .expect("register derived"),
    );
    assert_eq!(registered["feature"]["promotionState"], "candidate");

    let shadow = parse_text_tool_result(
        server
            .feature_registry(Parameters(
                the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                    action: Some("promote".into()),
                    feature_id: Some(feature_id.into()),
                    target_state: Some("shadow".into()),
                    trader_confirmation: Some(
                        "Your rules say this derived feature may run in shadow.".into(),
                    ),
                    ..Default::default()
                },
            ))
            .await
            .expect("shadow"),
    );
    assert_eq!(shadow["feature"]["promotionState"], "shadow");

    let active = parse_text_tool_result(
        server
            .feature_registry(Parameters(
                the_desk_backend::mcp::feature_registry::FeatureRegistryParams {
                    action: Some("promote".into()),
                    feature_id: Some(feature_id.into()),
                    target_state: Some("active".into()),
                    trader_confirmation: Some(
                        "Your playbook indicates this descriptor may be active.".into(),
                    ),
                    ..Default::default()
                },
            ))
            .await
            .expect("active"),
    );
    assert_eq!(active["feature"]["promotionState"], "active");

    let t0 = 1_704_207_600_000.0;
    {
        let db = server.db.lock().expect("db");
        let frames: Vec<JournalFrameRecord> = (0..5)
            .map(|i| {
                let clock = t0 + i as f64 * 1000.0;
                JournalFrameRecord {
                    clock_ms: clock,
                    frame_second: the_desk_backend::db::journal_frame_second_from_ts(clock)
                        .expect("frame second"),
                    root_symbol: "NQ".into(),
                    session_type: "RTH".into(),
                    session_segment: "None".into(),
                    trading_day: "2023-12-31".into(),
                    payload: serde_json::json!({
                        "lastPrice": 21_000.0 + i as f64,
                        "rootSymbol": "NQ",
                        "sessionType": "RTH",
                        "tradingDay": "2023-12-31"
                    }),
                }
            })
            .collect();
        db.insert_journal_frames(&frames).expect("insert frames");
    }

    let env = parse_text_tool_result(
        server
            .describe_environment()
            .await
            .expect("describe_environment"),
    );
    assert_eq!(env["featureRegistry"]["codegen"], true);
    assert_eq!(TheDeskMcp::tool_router().list_all().len(), 123);

    let search = parse_text_tool_result(
        server
            .search_catalog(Parameters(SearchCatalogParams {
                query: Some("session_last_price_percentile".into()),
            }))
            .await
            .expect("search_catalog"),
    );
    let hits = search["hits"].as_array().expect("hits");
    assert!(
        hits.iter()
            .any(|h| h["id"] == feature_id && h["rustField"] == "feature_ir"),
        "accepted descriptor must emit an agent-schema / runtime field"
    );
    let feature_hits = search["featureHits"].as_array().expect("featureHits");
    assert!(feature_hits
        .iter()
        .any(|h| h["id"] == feature_id && h["promotionState"] == "active"));

    let domain = parse_text_tool_result(
        server
            .describe_domain(Parameters(DescribeDomainParams {
                domain: Some("location_structure".into()),
            }))
            .await
            .expect("describe_domain"),
    );
    let fields = domain["fields"].as_array().expect("fields");
    assert!(fields.iter().any(|f| f["id"] == feature_id));

    let as_of = t0 + 4000.0;
    let state = parse_text_tool_result(
        server
            .get_state(Parameters(GetStateParams {
                symbols: Some(vec!["NQ".into()]),
                domains: Some(vec!["location_structure".into()]),
                fields: Some(vec![feature_id.into()]),
                resolution: Some("R1".into()),
                as_of: Some(as_of),
                budget_tokens: None,
            }))
            .await
            .expect("get_state as_of"),
    );
    assert_eq!(state["trustLevel"], "L0");
    let values = state["values"].as_object().expect("values");
    let served = values
        .get(feature_id)
        .and_then(|v| v.as_f64())
        .expect("get_state must serve the generated runtime field");
    assert!(
        (served - 90.0).abs() < 1e-9,
        "session percentile rank of the last of five prices must be 90, got {served}"
    );

    let series = parse_text_tool_result(
        server
            .query_series(Parameters(QuerySeriesParams {
                start_ms: Some(t0),
                end_ms: Some(as_of),
                session_type: Some("RTH".into()),
                symbols: Some(vec!["NQ".into()]),
                fields: Some(vec![feature_id.into()]),
                store: None,
            }))
            .await
            .expect("query_series"),
    );
    assert_eq!(series["trustLevel"], "L0");
    let points = series["points"].as_array().expect("points");
    assert_eq!(points.len(), 5);
    let last = points
        .last()
        .and_then(|p| p["values"][feature_id].as_f64())
        .expect("query_series must address the generated dimension");
    assert!((last - 90.0).abs() < 1e-9);
}

#[tokio::test]
async fn get_state_returns_provenance_and_degraded_flags() {
    let server = test_server_with_sil();
    let out = parse_text_tool_result(
        server
            .get_state(Parameters(GetStateParams {
                symbols: Some(vec!["NQ".into()]),
                domains: Some(vec![
                    "location_structure".into(),
                    "positioning".into(),
                    "identity".into(),
                ]),
                fields: None,
                resolution: Some("R1".into()),
                as_of: None,
                budget_tokens: None,
            }))
            .await
            .expect("get_state"),
    );
    assert_eq!(out["trustLevel"], "L0");
    assert_eq!(out["mutationAuthority"], false);
    assert_eq!(out["orderAuthority"], false);
    assert_eq!(out["resolution"], "R1");
    let provenance = out["provenance"].as_object().expect("provenance");
    let degraded = out["degraded"].as_object().expect("degraded");
    for domain in ["location_structure", "positioning", "identity"] {
        assert!(
            provenance.contains_key(domain),
            "missing provenance for {domain}"
        );
        assert!(
            degraded.contains_key(domain),
            "missing degraded for {domain}"
        );
    }
    // Positioning stub always fail-closed + present (never silently omitted, never vendor).
    assert_eq!(degraded["positioning"], true);
    assert_eq!(provenance["positioning"]["source"], "manual");
    assert!(provenance["positioning"]["vendor"].is_null());
    assert_eq!(out["values"]["positioning.freshnessOk"], false);
    // No live snapshot in empty test server → market domains degraded, not a hard fail.
    assert_eq!(degraded["location_structure"], true);
    assert_eq!(degraded["identity"], true);
}

#[tokio::test]
async fn get_state_rejects_mes_and_mnq() {
    let server = test_server_with_sil();
    let err = server
        .get_state(Parameters(GetStateParams {
            symbols: Some(vec!["MES".into()]),
            resolution: Some("R0".into()),
            ..Default::default()
        }))
        .await
        .expect_err("MES is out of MarketRouter v0 scope");
    let msg = err.to_string();
    assert!(
        msg.contains("MES") || msg.contains("micros") || msg.contains("NQ and ES"),
        "unexpected error: {msg}"
    );

    let err = server
        .get_state(Parameters(GetStateParams {
            symbols: Some(vec!["MNQ".into()]),
            resolution: Some("R0".into()),
            ..Default::default()
        }))
        .await
        .expect_err("MNQ is out of MarketRouter v0 scope");
    assert!(err.to_string().contains("MNQ") || err.to_string().contains("micros"));
}

#[tokio::test]
async fn get_state_returns_nq_and_es_in_one_envelope() {
    let server = test_server_with_sil();
    let ts = 1_704_207_600_000.0;
    server.market_router.apply_tick(
        the_desk_backend::engine::RouterRoot::Nq,
        &the_desk_backend::engine::SourceTick {
            timestamp_ms: ts,
            price: 20_000.0,
            volume: 1.0,
            bid: 19_999.75,
            ask: 20_000.25,
            side: TradeSide::Buy,
            root_symbol: Some("NQ".into()),
        },
    );
    server.market_router.apply_tick(
        the_desk_backend::engine::RouterRoot::Es,
        &the_desk_backend::engine::SourceTick {
            timestamp_ms: ts + 100.0,
            price: 5_000.0,
            volume: 1.0,
            bid: 4_999.75,
            ask: 5_000.25,
            side: TradeSide::Buy,
            root_symbol: Some("ES".into()),
        },
    );
    let out = parse_text_tool_result(
        server
            .get_state(Parameters(GetStateParams {
                symbols: Some(vec!["NQ".into(), "ES".into()]),
                domains: Some(vec!["identity".into(), "location_structure".into()]),
                fields: None,
                resolution: Some("R0".into()),
                as_of: None,
                budget_tokens: None,
            }))
            .await
            .expect("get_state"),
    );
    assert_eq!(out["trustLevel"], "L0");
    assert_eq!(out["mutationAuthority"], false);
    assert_eq!(out["orderAuthority"], false);
    assert_eq!(out["clockMs"], ts + 100.0);
    let values = out["values"].as_object().expect("values");
    assert_eq!(
        values.get("NQ.market.location_structure.lastPrice"),
        Some(&serde_json::json!(20_000.0))
    );
    assert_eq!(
        values.get("ES.market.location_structure.lastPrice"),
        Some(&serde_json::json!(5_000.0))
    );
    let provenance = out["provenance"].as_object().expect("provenance");
    assert!(provenance.contains_key("NQ.identity"));
    assert!(provenance.contains_key("ES.identity"));
    assert!(provenance.contains_key("NQ.location_structure"));
    assert!(provenance.contains_key("ES.location_structure"));
}

#[tokio::test]
async fn positioning_entry_round_trips_into_get_state_as_first_class_levels_only() {
    let server = test_server_with_sil();
    let now_ms = Utc::now().timestamp_millis() as f64;
    let trading_day = trading_day_from_timestamp_ms(now_ms);
    let written = parse_text_tool_result(
        server
            .positioning_entry(Parameters(PositioningEntryParams {
                trading_day: Some(trading_day.clone()),
                captured_at_ms: Some(now_ms),
                as_of_ms: Some(now_ms),
                derived_levels: Some(DerivedLevelsParams {
                    flip: Some(5750.0),
                    walls: Some(vec![
                        PositioningWallParams {
                            strike: Some(5800.0),
                            role: Some("call_wall".into()),
                        },
                        PositioningWallParams {
                            strike: Some(5700.0),
                            role: Some("put_wall".into()),
                        },
                    ]),
                    balance: Some(5745.0),
                    upside_test: Some(5825.0),
                    downside_test: Some(5680.0),
                }),
                note: Some(
                    "Your annotated sessions / your methodology say this is the morning card."
                        .into(),
                ),
                ..Default::default()
            }))
            .await
            .expect("positioning_entry"),
    );
    assert_eq!(written["completeness"], "levels_only");
    assert_eq!(written["firstClass"], true);
    assert_eq!(written["mutationAuthority"], true);
    assert_eq!(written["orderAuthority"], false);
    assert_eq!(written["trustCeiling"], "L3");
    assert_eq!(written["readOperator"], "get_state");
    assert_eq!(written["record"]["recordKind"], "levels_only");
    assert!(written["record"]["provenance"]["firstClass"]
        .as_bool()
        .unwrap());
    assert!(written["record"]["dataTime"].is_null());
    assert!(written["record"].get("capturedAt").is_some());
    assert!(written["record"].get("asOf").is_some());
    let note = written["note"].as_str().unwrap_or("").to_lowercase();
    assert!(note.contains("your annotated sessions") || note.contains("your methodology"));
    assert!(!note.contains("you should buy"));
    assert!(!note.contains("fallback"));

    let out = parse_text_tool_result(
        server
            .get_state(Parameters(GetStateParams {
                domains: Some(vec!["positioning".into()]),
                resolution: Some("R1".into()),
                ..Default::default()
            }))
            .await
            .expect("get_state"),
    );
    assert_eq!(out["trustLevel"], "L0");
    assert_eq!(out["mutationAuthority"], false);
    assert_eq!(out["orderAuthority"], false);
    assert_eq!(out["degraded"]["positioning"], false);
    assert_eq!(out["provenance"]["positioning"]["source"], "manual");
    assert!(out["provenance"]["positioning"]["vendor"].is_null());
    assert_eq!(out["values"]["positioning.completeness"], "levels_only");
    assert_eq!(out["values"]["positioning.recordKind"], "levels_only");
    assert_eq!(out["values"]["positioning.freshnessOk"], true);
    assert_eq!(out["values"]["positioning.derivedLevels"]["flip"], 5750.0);
    assert!(out["values"]["positioning.dataTime"].is_null());
    assert!(!out["values"]
        .as_object()
        .unwrap()
        .keys()
        .any(|k| k.starts_with("NQ.positioning") || k.starts_with("ES.positioning")));
}

#[tokio::test]
async fn positioning_entry_rejects_slice_and_vendor_pretence() {
    let server = test_server_with_sil();
    let levels = DerivedLevelsParams {
        flip: Some(5750.0),
        walls: Some(vec![PositioningWallParams {
            strike: Some(5800.0),
            role: Some("call_wall".into()),
        }]),
        balance: Some(5745.0),
        upside_test: Some(5825.0),
        downside_test: Some(5680.0),
    };
    let err = server
        .positioning_entry(Parameters(PositioningEntryParams {
            record_kind: Some("slice".into()),
            derived_levels: Some(levels.clone()),
            ..Default::default()
        }))
        .await
        .expect_err("slice writes are later");
    assert!(
        err.to_string().contains("levels_only") || err.to_string().contains("Levels-Only"),
        "unexpected: {err}"
    );
    let err = server
        .positioning_entry(Parameters(PositioningEntryParams {
            vendor: Some("Menthor Q".into()),
            derived_levels: Some(levels.clone()),
            ..Default::default()
        }))
        .await
        .expect_err("unrecognized vendor stamp rejected");
    assert!(
        err.to_string().to_lowercase().contains("vendor")
            || err.to_string().contains("Levels-Only"),
        "unexpected: {err}"
    );
}

#[tokio::test]
async fn get_state_positioning_prior_day_fails_closed_not_vendor() {
    let server = test_server_with_sil();
    server
        .positioning_entry(Parameters(PositioningEntryParams {
            trading_day: Some("2026-02-18".into()),
            captured_at_ms: Some(1_771_372_800_000.0),
            as_of_ms: Some(1_771_372_800_000.0),
            derived_levels: Some(DerivedLevelsParams {
                flip: Some(5750.0),
                walls: Some(vec![PositioningWallParams {
                    strike: Some(5800.0),
                    role: Some("call_wall".into()),
                }]),
                balance: Some(5745.0),
                upside_test: Some(5825.0),
                downside_test: Some(5680.0),
            }),
            ..Default::default()
        }))
        .await
        .expect("write backlog card");
    let out = parse_text_tool_result(
        server
            .get_state(Parameters(GetStateParams {
                symbols: Some(vec!["NQ".into(), "ES".into()]),
                domains: Some(vec!["positioning".into(), "identity".into()]),
                resolution: Some("R1".into()),
                ..Default::default()
            }))
            .await
            .expect("get_state"),
    );
    assert_eq!(out["degraded"]["positioning"], true);
    assert_eq!(out["values"]["positioning.freshnessOk"], false);
    assert_eq!(out["values"]["positioning.completeness"], "levels_only");
    assert_eq!(out["provenance"]["positioning"]["source"], "manual");
    assert!(out["provenance"]["positioning"]["vendor"].is_null());
    assert!(!out["provenance"]
        .as_object()
        .unwrap()
        .contains_key("NQ.positioning"));
    assert!(!out["provenance"]
        .as_object()
        .unwrap()
        .contains_key("ES.positioning"));
    let note = out["provenance"]["positioning"]["note"]
        .as_str()
        .unwrap_or("")
        .to_lowercase();
    assert!(note.contains("first-class"));
    assert!(!note.contains("fallback"));
    assert!(!note.contains("volsignal"));
}

#[tokio::test]
async fn get_state_as_of_serves_dated_levels_only_without_staling_or_vendor() {
    let server = test_server_with_sil();
    let as_of = 1_771_372_800_000.0;
    server
        .positioning_entry(Parameters(PositioningEntryParams {
            trading_day: Some("2026-02-18".into()),
            captured_at_ms: Some(as_of),
            as_of_ms: Some(as_of),
            derived_levels: Some(DerivedLevelsParams {
                flip: Some(5750.0),
                walls: Some(vec![PositioningWallParams {
                    strike: Some(5800.0),
                    role: Some("call_wall".into()),
                }]),
                balance: Some(5745.0),
                upside_test: Some(5825.0),
                downside_test: Some(5680.0),
            }),
            ..Default::default()
        }))
        .await
        .expect("write dated card");
    let out = parse_text_tool_result(
        server
            .get_state(Parameters(GetStateParams {
                domains: Some(vec!["positioning".into()]),
                resolution: Some("R1".into()),
                as_of: Some(as_of),
                ..Default::default()
            }))
            .await
            .expect("get_state as_of"),
    );
    assert_eq!(out["trustLevel"], "L0");
    assert_eq!(out["mutationAuthority"], false);
    assert_eq!(out["orderAuthority"], false);
    assert_eq!(out["degraded"]["positioning"], false);
    assert_eq!(out["values"]["positioning.freshnessOk"], true);
    assert_eq!(out["values"]["positioning.completeness"], "levels_only");
    assert_eq!(out["provenance"]["positioning"]["source"], "manual");
    assert!(out["provenance"]["positioning"]["vendor"].is_null());
    assert!(out["provenance"]["positioning"]["dataTime"].is_null());
    assert_eq!(out["values"]["positioning.asOf"], as_of);
}

#[tokio::test]
async fn get_state_as_of_other_day_fails_closed_not_vendor() {
    let server = test_server_with_sil();
    let card_as_of = 1_771_372_800_000.0;
    server
        .positioning_entry(Parameters(PositioningEntryParams {
            trading_day: Some("2026-02-18".into()),
            captured_at_ms: Some(card_as_of),
            as_of_ms: Some(card_as_of),
            derived_levels: Some(DerivedLevelsParams {
                flip: Some(5750.0),
                walls: Some(vec![PositioningWallParams {
                    strike: Some(5800.0),
                    role: Some("call_wall".into()),
                }]),
                balance: Some(5745.0),
                upside_test: Some(5825.0),
                downside_test: Some(5680.0),
            }),
            ..Default::default()
        }))
        .await
        .expect("write february card");
    let later = 1_786_629_600_000.0; // 2026-08-13 14:00 UTC
    let out = parse_text_tool_result(
        server
            .get_state(Parameters(GetStateParams {
                domains: Some(vec!["positioning".into()]),
                resolution: Some("R1".into()),
                as_of: Some(later),
                ..Default::default()
            }))
            .await
            .expect("get_state as_of later day"),
    );
    assert_eq!(out["degraded"]["positioning"], true);
    assert_eq!(out["values"]["positioning.freshnessOk"], false);
    assert_eq!(out["values"]["positioning.completeness"], "levels_only");
    assert_eq!(out["provenance"]["positioning"]["source"], "manual");
    assert!(out["provenance"]["positioning"]["vendor"].is_null());
}

#[test]
fn positioning_entry_params_accept_catalog_and_ms_timestamp_names() {
    let catalog_names = serde_json::json!({
        "capturedAt": 1_771_372_800_000.0,
        "asOf": 1_771_372_800_000.0,
        "derivedLevels": {
            "flip": 5750.0,
            "balance": 5745.0,
            "upsideTest": 5825.0,
            "downsideTest": 5680.0
        }
    });
    let from_catalog: PositioningEntryParams =
        serde_json::from_value(catalog_names).expect("catalog names");
    assert_eq!(from_catalog.captured_at_ms, Some(1_771_372_800_000.0));
    assert_eq!(from_catalog.as_of_ms, Some(1_771_372_800_000.0));

    let ms_names = serde_json::json!({
        "capturedAtMs": 1_771_372_800_000.0,
        "asOfMs": 1_771_372_800_000.0
    });
    let from_ms: PositioningEntryParams = serde_json::from_value(ms_names).expect("ms names");
    assert_eq!(from_ms.captured_at_ms, Some(1_771_372_800_000.0));
    assert_eq!(from_ms.as_of_ms, Some(1_771_372_800_000.0));
}

#[tokio::test]
async fn get_state_as_of_served_from_journal_frames_not_pipeline_snapshots() {
    let server = test_server_with_sil();
    let ts = 1_704_207_600_000.0;
    server.market_router.apply_tick(
        the_desk_backend::engine::RouterRoot::Nq,
        &the_desk_backend::engine::SourceTick {
            timestamp_ms: ts,
            price: 20_000.0,
            volume: 1.0,
            bid: 19_999.75,
            ask: 20_000.25,
            side: TradeSide::Buy,
            root_symbol: Some("NQ".into()),
        },
    );
    server.market_router.apply_tick(
        the_desk_backend::engine::RouterRoot::Es,
        &the_desk_backend::engine::SourceTick {
            timestamp_ms: ts + 100.0,
            price: 5_000.0,
            volume: 1.0,
            bid: 4_999.75,
            ask: 5_000.25,
            side: TradeSide::Buy,
            root_symbol: Some("ES".into()),
        },
    );
    {
        let db = server.db.lock().expect("db");
        db.insert_pipeline_snapshot(
            ts + 50.0,
            &serde_json::json!({"lastPrice": 99_999.0, "rootSymbol": "NQ"}),
        )
        .expect("decoy pipeline snapshot");
        server
            .market_router
            .persist_journal(&db)
            .expect("journal frames");
    }

    let out = parse_text_tool_result(
        server
            .get_state(Parameters(GetStateParams {
                symbols: Some(vec!["NQ".into(), "ES".into()]),
                domains: Some(vec!["identity".into(), "location_structure".into()]),
                fields: None,
                resolution: Some("R0".into()),
                as_of: Some(ts + 100.0),
                budget_tokens: None,
            }))
            .await
            .expect("get_state as_of"),
    );
    assert_eq!(out["trustLevel"], "L0");
    assert_eq!(out["mutationAuthority"], false);
    assert_eq!(out["orderAuthority"], false);
    assert_eq!(out["asOf"], ts + 100.0);
    let values = out["values"].as_object().expect("values");
    assert_eq!(
        values.get("NQ.market.location_structure.lastPrice"),
        Some(&serde_json::json!(20_000.0))
    );
    assert_eq!(
        values.get("ES.market.location_structure.lastPrice"),
        Some(&serde_json::json!(5_000.0))
    );
    assert_ne!(
        values.get("NQ.market.location_structure.lastPrice"),
        Some(&serde_json::json!(99_999.0)),
        "as_of must not fall back to pipeline_snapshots"
    );
    let provenance = out["provenance"].as_object().expect("provenance");
    assert_eq!(provenance["NQ.location_structure"]["source"], "journal");
    assert_eq!(provenance["ES.location_structure"]["source"], "journal");
    assert!(provenance["NQ.location_structure"]["note"]
        .as_str()
        .unwrap_or("")
        .contains("Journal Frames"));
}

#[tokio::test]
async fn get_state_as_of_missing_frames_degrades_with_journal_provenance() {
    let server = test_server_with_sil();
    let out = parse_text_tool_result(
        server
            .get_state(Parameters(GetStateParams {
                symbols: Some(vec!["NQ".into()]),
                domains: Some(vec!["location_structure".into()]),
                resolution: Some("R1".into()),
                as_of: Some(1_704_207_600_000.0),
                ..Default::default()
            }))
            .await
            .expect("degraded as_of"),
    );
    assert_eq!(out["trustLevel"], "L0");
    assert_eq!(out["degraded"]["location_structure"], true);
    assert_eq!(out["provenance"]["location_structure"]["source"], "journal");
}

#[tokio::test]
async fn get_state_as_of_omitted_symbols_keeps_unprefixed_when_only_nq_frame() {
    let server = test_server_with_sil();
    let ts = 1_704_207_600_000.0;
    server.market_router.apply_tick(
        the_desk_backend::engine::RouterRoot::Nq,
        &the_desk_backend::engine::SourceTick {
            timestamp_ms: ts,
            price: 20_000.0,
            volume: 1.0,
            bid: 19_999.75,
            ask: 20_000.25,
            side: TradeSide::Buy,
            root_symbol: Some("NQ".into()),
        },
    );
    {
        let db = server.db.lock().expect("db");
        server.market_router.persist_journal(&db).expect("nq frame");
    }
    let out = parse_text_tool_result(
        server
            .get_state(Parameters(GetStateParams {
                symbols: None,
                domains: Some(vec!["location_structure".into()]),
                resolution: Some("R0".into()),
                as_of: Some(ts + 100.0),
                ..Default::default()
            }))
            .await
            .expect("as_of omitted symbols"),
    );
    assert_eq!(out["trustLevel"], "L0");
    assert_eq!(out["provenance"]["location_structure"]["source"], "journal");
    let values = out["values"].as_object().expect("values");
    assert!(
        values.contains_key("market.location_structure.lastPrice"),
        "omitted symbols with only NQ present must keep the M1b unprefixed envelope: {values:?}"
    );
    assert!(
        !values.keys().any(|k| k.starts_with("ES.")),
        "must not emit prefixed ES keys when ES has no Journal Frame: {values:?}"
    );
}

#[tokio::test]
async fn get_state_as_of_omitted_symbols_serves_es_when_only_es_frame() {
    let server = test_server_with_sil();
    let ts = 1_704_207_600_000.0;
    server.market_router.apply_tick(
        the_desk_backend::engine::RouterRoot::Es,
        &the_desk_backend::engine::SourceTick {
            timestamp_ms: ts,
            price: 5_000.0,
            volume: 1.0,
            bid: 4_999.75,
            ask: 5_000.25,
            side: TradeSide::Buy,
            root_symbol: Some("ES".into()),
        },
    );
    {
        let db = server.db.lock().expect("db");
        server.market_router.persist_journal(&db).expect("es frame");
    }
    let out = parse_text_tool_result(
        server
            .get_state(Parameters(GetStateParams {
                symbols: None,
                domains: Some(vec!["location_structure".into()]),
                resolution: Some("R0".into()),
                as_of: Some(ts + 100.0),
                ..Default::default()
            }))
            .await
            .expect("as_of ES-only"),
    );
    assert_eq!(out["trustLevel"], "L0");
    assert_eq!(out["provenance"]["location_structure"]["source"], "journal");
    assert_eq!(out["degraded"]["location_structure"], false);
    let values = out["values"].as_object().expect("values");
    assert_eq!(
        values.get("market.location_structure.lastPrice"),
        Some(&serde_json::json!(5_000.0)),
        "omitted symbols with only ES present must serve the ES frame, not a degraded NQ placeholder: {values:?}"
    );
}

#[tokio::test]
async fn get_state_rejects_r2_r3() {
    let server = test_server_with_sil();
    let err = server
        .get_state(Parameters(GetStateParams {
            resolution: Some("R2".into()),
            ..Default::default()
        }))
        .await
        .expect_err("R2 must fail");
    assert!(err.to_string().contains("R0") || err.to_string().contains("R2"));

    let err = server
        .get_state(Parameters(GetStateParams {
            resolution: Some("R3".into()),
            ..Default::default()
        }))
        .await
        .expect_err("R3 must fail");
    assert!(err.to_string().contains("R0") || err.to_string().contains("R3"));
}

#[tokio::test]
async fn get_events_returns_identity_rows() {
    let server = test_server_with_sil();
    {
        let db = server.db.lock().expect("db");
        db.insert_market_events_batch(&[MarketEvent {
            session_date: "2026-08-11".into(),
            timestamp_ms: 1_700_000_000_000.0,
            event_type: "ib_extension_hit".into(),
            level_name: Some("ib_high".into()),
            price: 21000.0,
            direction: Some("from_below".into()),
            sequence_num: Some(1),
            metadata: Some(serde_json::json!({ "severity": "high" })),
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2026-08-11".into(),
        }])
        .expect("insert event");
    }
    let out = parse_text_tool_result(
        server
            .get_events(Parameters(GetEventsParams {
                symbols: None,
                event_type: Some("ib_extension_hit".into()),
                since_ms: None,
                limit: Some(10),
            }))
            .await
            .expect("get_events"),
    );
    assert_eq!(out["trustLevel"], "L0");
    assert_eq!(out["lifecycleFormalized"], true);
    assert_eq!(out["mutationAuthority"], false);
    assert_eq!(out["orderAuthority"], false);
    assert_eq!(out["count"], 1);
    let evt = &out["events"][0];
    assert_eq!(evt["eventType"], "ib_extension_hit");
    assert_eq!(evt["timestampMs"], 1_700_000_000_000.0);
    assert_eq!(evt["severity"], "high");
    assert_eq!(evt["lifecycle"], "open");
    assert!(evt["identityId"].as_str().unwrap().starts_with("evt_"));
    assert!(evt["dedupIdentityId"]
        .as_str()
        .unwrap()
        .starts_with("dedup_"));
    assert!(
        evt.get("frameRef").is_some(),
        "frame_ref must never be omitted"
    );
    assert_eq!(evt["frameRef"]["journalFrameSecond"], 1_700_000_000i64);
    assert_eq!(evt["requiresCapsule"], false);
    assert!(
        evt.get("capsuleRef").is_none(),
        "non-DOM rows must not pretend to have Capsules"
    );
}

#[tokio::test]
async fn get_events_collapses_repeat_identity_to_latest_lifecycle() {
    let server = test_server_with_sil();
    {
        let db = server.db.lock().expect("db");
        let detected = MarketEvent {
            session_date: "2026-08-11".into(),
            timestamp_ms: 1_700_000_000_000.0,
            event_type: "absorption_detected".into(),
            level_name: Some("zone".into()),
            price: 21000.0,
            direction: Some("bid".into()),
            sequence_num: None,
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2026-08-11".into(),
        };
        let mut confirmed = detected.clone();
        confirmed.event_type = "absorption_confirmed".into();
        confirmed.timestamp_ms = 1_700_000_001_000.0;
        db.insert_market_events_batch_scoped(Some("NQ"), &[detected, confirmed])
            .expect("insert events");
    }
    let out = parse_text_tool_result(
        server
            .get_events(Parameters(GetEventsParams {
                symbols: None,
                event_type: None,
                since_ms: None,
                limit: Some(10),
            }))
            .await
            .expect("get_events"),
    );
    assert_eq!(out["count"], 1);
    assert_eq!(out["lifecycleFormalized"], true);
    assert_eq!(out["trustLevel"], "L0");
    let evt = &out["events"][0];
    assert_eq!(evt["eventType"], "absorption_confirmed");
    assert_eq!(evt["lifecycle"], "updated");
    assert_eq!(evt["severity"], "high");
    assert!(evt.get("frameRef").is_some());
    assert!(evt["dedupIdentityId"]
        .as_str()
        .unwrap()
        .starts_with("dedup_"));
}

#[tokio::test]
async fn get_events_identity_distinguishes_sequence_num() {
    let server = test_server_with_sil();
    {
        let db = server.db.lock().expect("db");
        let base = MarketEvent {
            session_date: "2026-08-11".into(),
            timestamp_ms: 1_700_000_100_000.0,
            event_type: "level_test".into(),
            level_name: Some("vwap".into()),
            price: 21000.0,
            direction: Some("from_below".into()),
            sequence_num: Some(1),
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2026-08-11".into(),
        };
        let mut second = base.clone();
        second.sequence_num = Some(2);
        db.insert_market_events_batch(&[base, second])
            .expect("insert events");
    }
    let out = parse_text_tool_result(
        server
            .get_events(Parameters(GetEventsParams {
                event_type: Some("level_test".into()),
                limit: Some(10),
                ..Default::default()
            }))
            .await
            .expect("get_events"),
    );
    assert_eq!(out["count"], 2);
    let id0 = out["events"][0]["identityId"].as_str().expect("id0");
    let id1 = out["events"][1]["identityId"].as_str().expect("id1");
    assert_ne!(id0, id1, "sequence_num must differentiate identityId");
}

#[tokio::test]
async fn get_events_dom_family_never_omits_capsule_ref() {
    let server = test_server_with_sil();
    {
        let db = server.db.lock().expect("db");
        db.insert_market_events_batch_scoped(
            Some("NQ"),
            &[MarketEvent {
                session_date: "2026-08-11".into(),
                timestamp_ms: 1_700_000_000_000.0,
                event_type: "stop_run".into(),
                level_name: None,
                price: 21000.0,
                direction: Some("up".into()),
                sequence_num: Some(1),
                metadata: None,
                session_type: "RTH".into(),
                session_segment: "None".into(),
                trading_day: "2026-08-11".into(),
            }],
        )
        .expect("insert stop_run");
    }
    let out = parse_text_tool_result(
        server
            .get_events(Parameters(GetEventsParams {
                event_type: Some("stop_run".into()),
                limit: Some(10),
                ..Default::default()
            }))
            .await
            .expect("get_events"),
    );
    assert_eq!(out["trustLevel"], "L0");
    assert_eq!(out["count"], 1);
    let evt = &out["events"][0];
    assert_eq!(evt["requiresCapsule"], true);
    assert_eq!(evt["family"], "dom");
    let cap = evt
        .get("capsuleRef")
        .expect("DOM-family rows must carry capsuleRef");
    assert!(cap.get("id").is_some());
    assert!(cap.get("windowStartMs").is_some());
    assert!(cap.get("windowEndMs").is_some());
    assert!(cap.get("completeness").is_some());
    assert_eq!(cap["completeness"], "pending");
    assert!(cap["id"].is_null());
    assert_eq!(cap["windowStartMs"], 1_700_000_000_000.0 - 30_000.0);
    assert_eq!(cap["windowEndMs"], 1_700_000_000_000.0 + 60_000.0);
}

#[tokio::test]
async fn get_events_rejects_invalid_since_ms() {
    let server = test_server_with_sil();
    let err = server
        .get_events(Parameters(GetEventsParams {
            since_ms: Some(-1.0),
            ..Default::default()
        }))
        .await
        .expect_err("invalid sinceMs");
    assert!(err.to_string().contains("sinceMs"));
}

#[tokio::test]
async fn query_episodes_flagship_is_expressible_and_l0() {
    use the_desk_backend::catalog::{
        accept_levels_only_entry, DerivedLevels, PositioningEntryInput, LEVELS_ONLY_RECORD_KIND,
    };
    use the_desk_backend::db::JournalFrameRecord;
    use the_desk_backend::research::query_kernel::{
        flagship_episode_predicates, FIELD_LAST_PRICE, FUTURES_TICK_SIZE,
    };

    let server = test_server_with_sil();
    let clock = 1_704_207_600_000.0;
    let second = the_desk_backend::db::journal_frame_second_from_ts(clock).unwrap();
    {
        let db = server.db.lock().expect("db");
        db.insert_journal_frames(&[
            JournalFrameRecord {
                clock_ms: clock,
                frame_second: second,
                root_symbol: "ES".into(),
                session_type: "RTH".into(),
                session_segment: "None".into(),
                trading_day: "2024-01-02".into(),
                payload: serde_json::json!({
                    "lastPrice": 5750.0,
                    "sessionDelta": -500.0,
                    "poorLow": true,
                    "domSummary": { "bidReplenishing": true },
                    "rootSymbol": "ES"
                }),
            },
            JournalFrameRecord {
                clock_ms: clock,
                frame_second: second,
                root_symbol: "NQ".into(),
                session_type: "RTH".into(),
                session_segment: "None".into(),
                trading_day: "2024-01-02".into(),
                payload: serde_json::json!({
                    "lastPrice": 20000.0,
                    "sessionDelta": 50.0,
                    "rootSymbol": "NQ"
                }),
            },
        ])
        .expect("frames");
        let record = accept_levels_only_entry(PositioningEntryInput {
            id: Some("pos-mcp".into()),
            record_kind: Some(LEVELS_ONLY_RECORD_KIND.into()),
            completeness: Some(LEVELS_ONLY_RECORD_KIND.into()),
            trading_day: Some("2024-01-02".into()),
            captured_at_ms: Some(clock),
            as_of_ms: Some(clock),
            derived_levels: Some(DerivedLevels {
                flip: 5750.0,
                walls: vec![],
                balance: 5745.0,
                upside_test: 5825.0,
                downside_test: 5680.0,
            }),
            now_ms: clock,
            ..Default::default()
        })
        .expect("pos");
        db.upsert_positioning_record(&record, clock).expect("pos");
        db.insert_raw_tick_with_contract(
            clock + 1_000.0,
            5750.0 - FUTURES_TICK_SIZE,
            1.0,
            5749.5,
            5750.0,
            false,
            "2024-01-02",
            Some("ES"),
            Some("ESH24.CME"),
        )
        .expect("tick");
    }

    let predicates: Vec<CatalogPredicateParams> = flagship_episode_predicates()
        .into_iter()
        .map(|p| CatalogPredicateParams {
            id: p.id,
            symbol: p.symbol,
            field: Some(p.field),
            op: Some(
                match p.op {
                    the_desk_backend::research::query_kernel::PredicateOp::Eq => "eq",
                    the_desk_backend::research::query_kernel::PredicateOp::Ne => "ne",
                    the_desk_backend::research::query_kernel::PredicateOp::Gt => "gt",
                    the_desk_backend::research::query_kernel::PredicateOp::Gte => "gte",
                    the_desk_backend::research::query_kernel::PredicateOp::Lt => "lt",
                    the_desk_backend::research::query_kernel::PredicateOp::Lte => "lte",
                    the_desk_backend::research::query_kernel::PredicateOp::Near => "near",
                    the_desk_backend::research::query_kernel::PredicateOp::Exists => "exists",
                }
                .into(),
            ),
            value: p.value,
            path: p.path,
            tolerance_ticks: p.tolerance_ticks,
            event_type: p.event_type,
        })
        .collect();

    let out = parse_text_tool_result(
        server
            .query_episodes(Parameters(QueryEpisodesParams {
                start_ms: Some(clock),
                end_ms: Some(clock + 4_000.0),
                session_type: Some("RTH".into()),
                symbols: Some(vec!["NQ".into(), "ES".into()]),
                predicates: Some(predicates),
                forward_direction: Some("short".into()),
                store: None,
            }))
            .await
            .expect("query_episodes"),
    );
    assert_eq!(out["trustLevel"], "L0");
    assert_eq!(out["mutationAuthority"], false);
    assert_eq!(out["orderAuthority"], false);
    assert_eq!(out["meta"]["n"], 1);
    assert_eq!(out["meta"]["reliabilityTier"], "insufficient");
    assert_eq!(out["matches"].as_array().expect("matches").len(), 1);
    assert_eq!(out["matches"][0]["journalBacked"], true);
    assert!(out["matches"][0]["forward"]["mfePoints"].is_number());
    assert!(out["matches"][0]["positioningId"].is_string());
    let _ = FIELD_LAST_PRICE;
}

#[tokio::test]
async fn query_episodes_file_backed_db_uses_read_pool() {
    use the_desk_backend::db::JournalFrameRecord;

    let dir = tempfile::tempdir().expect("tmpdir");
    let db_path = dir.path().join("data.db");
    let path_str = db_path.to_string_lossy().to_string();
    let db = Database::open(&path_str).expect("db");
    let logging_config = the_desk_backend::observability::LoggingConfig {
        destination: "none".to_string(),
        runtime_event_suppression_window_ms: 0,
        ..the_desk_backend::observability::LoggingConfig::default()
    };
    let mut server = TheDeskMcp::with_runtime_events_and_sil(
        db,
        PipelineEngine::new(),
        path_str,
        std::sync::Arc::new(the_desk_backend::observability::RuntimeEventStore::new(
            &logging_config,
        )),
        the_desk_backend::catalog::SilConfig {
            catalog_discovery: true,
            ..Default::default()
        },
    );
    server.research_artifact_dir = dir.path().join("artifacts");
    server
        .hydrate_playbook_runtime_cache()
        .expect("hydrate playbook cache");

    let clock = 1_704_207_600_000.0;
    let second = the_desk_backend::db::journal_frame_second_from_ts(clock).unwrap();
    {
        let db = server.db.lock().expect("db");
        db.insert_journal_frames(&[JournalFrameRecord {
            clock_ms: clock,
            frame_second: second,
            root_symbol: "ES".into(),
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2024-01-02".into(),
            payload: serde_json::json!({
                "lastPrice": 5750.0,
                "poorLow": true,
                "rootSymbol": "ES"
            }),
        }])
        .expect("frames");
        db.insert_raw_tick_with_contract(
            clock + 1_000.0,
            5749.0,
            1.0,
            5748.75,
            5749.25,
            false,
            "2024-01-02",
            Some("ES"),
            Some("ESH24.CME"),
        )
        .expect("tick");
    }

    let out = parse_text_tool_result(
        server
            .query_episodes(Parameters(QueryEpisodesParams {
                start_ms: Some(clock),
                end_ms: Some(clock + 4_000.0),
                session_type: Some("RTH".into()),
                predicates: Some(vec![CatalogPredicateParams {
                    id: Some("poorAuctionEfficiency".into()),
                    symbol: Some("ES".into()),
                    field: Some("market.location_structure.poorLow".into()),
                    op: Some("eq".into()),
                    value: Some(serde_json::json!(true)),
                    ..Default::default()
                }]),
                forward_direction: Some("short".into()),
                ..Default::default()
            }))
            .await
            .expect("query_episodes via read pool"),
    );
    assert_eq!(out["trustLevel"], "L0");
    assert_eq!(out["meta"]["n"], 1);
    assert_eq!(out["matches"].as_array().expect("matches").len(), 1);
}

#[tokio::test]
async fn query_raw_rejects_unbounded_window() {
    let server = test_server_with_sil();
    let err = server
        .query_raw(Parameters(QueryRawParams {
            start_ms: None,
            end_ms: Some(1_704_207_600_000.0),
            source: Some("journal_frames".into()),
            ..Default::default()
        }))
        .await
        .expect_err("unbounded");
    assert!(
        err.to_string().contains("unbounded"),
        "rejection must mention unbounded, got {err}"
    );
}

#[tokio::test]
async fn run_job_returns_artifact_handle_not_rows() {
    use the_desk_backend::db::JournalFrameRecord;

    let server = test_server_with_sil();
    let clock = 1_704_207_600_000.0;
    let second = the_desk_backend::db::journal_frame_second_from_ts(clock).unwrap();
    {
        let db = server.db.lock().expect("db");
        db.insert_journal_frames(&[JournalFrameRecord {
            clock_ms: clock,
            frame_second: second,
            root_symbol: "NQ".into(),
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2024-01-02".into(),
            payload: serde_json::json!({ "lastPrice": 20000.0, "sessionDelta": 1.0 }),
        }])
        .expect("frame");
    }
    let out = parse_text_tool_result(
        server
            .run_job(Parameters(RunJobParams {
                kind: Some("series".into()),
                start_ms: Some(clock),
                end_ms: Some(clock + 1_000.0),
                session_type: Some("RTH".into()),
                symbols: Some(vec!["NQ".into()]),
                fields: Some(vec!["market.location_structure.lastPrice".into()]),
                ..Default::default()
            }))
            .await
            .expect("run_job"),
    );
    assert_eq!(out["trustLevel"], "L0");
    assert_eq!(out["mutationAuthority"], false);
    assert_eq!(out["orderAuthority"], false);
    assert!(out["jobId"].as_str().unwrap_or("").starts_with("rq-"));
    assert!(out["artifact"]["columnarPath"]
        .as_str()
        .unwrap_or("")
        .ends_with("columns.csv"));
    assert!(out["artifact"]["summaryPath"]
        .as_str()
        .unwrap_or("")
        .ends_with("summary.json"));
    let columnar = out["artifact"]["columnarPath"].as_str().unwrap_or("");
    assert!(
        std::path::Path::new(columnar).starts_with(&server.research_artifact_dir),
        "run_job must write under the test artifact dir, got {columnar}"
    );
    assert!(
        !columnar.contains(".the-desk/research-artifacts"),
        "run_job must not write to $HOME/.the-desk/research-artifacts in tests"
    );
    assert!(out.get("points").is_none());
    assert_eq!(out["meta"]["n"], 1);
}

#[tokio::test]
async fn query_series_store_cold_keeps_l0_and_rejects_unknown_store() {
    use the_desk_backend::db::JournalFrameRecord;
    use the_desk_backend::engine::ColdFrameStore;

    let mut server = test_server_with_sil();
    let cold_dir = tempfile::tempdir().expect("cold");
    server.cold_frames_dir = cold_dir.path().to_path_buf();
    let clock = 1_704_207_600_000.0;
    let second = the_desk_backend::db::journal_frame_second_from_ts(clock).unwrap();
    let frame = JournalFrameRecord {
        clock_ms: clock,
        frame_second: second,
        root_symbol: "NQ".into(),
        session_type: "RTH".into(),
        session_segment: "None".into(),
        trading_day: "2024-01-02".into(),
        payload: serde_json::json!({
            "lastPrice": 20000.0,
            "rootSymbol": "NQ",
            "sessionType": "RTH"
        }),
    };
    ColdFrameStore::new(cold_dir.path())
        .upsert_frames(&[frame])
        .expect("cold upsert");

    let cold = parse_text_tool_result(
        server
            .query_series(Parameters(QuerySeriesParams {
                start_ms: Some(clock),
                end_ms: Some(clock + 1_000.0),
                session_type: Some("RTH".into()),
                symbols: Some(vec!["NQ".into()]),
                fields: Some(vec!["market.location_structure.lastPrice".into()]),
                store: Some("cold".into()),
            }))
            .await
            .expect("query_series store=cold"),
    );
    assert_eq!(cold["trustLevel"], "L0");
    assert_eq!(cold["mutationAuthority"], false);
    assert_eq!(cold["orderAuthority"], false);
    assert_eq!(cold["meta"]["n"], 1);
    assert_eq!(cold["meta"]["reliabilityTier"], "insufficient");
    assert!(
        cold["meta"]["notes"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|n| n.as_str().unwrap_or("").contains("cold")),
        "cold path must note the store without changing L0 fields: {cold}"
    );

    let hot = parse_text_tool_result(
        server
            .query_series(Parameters(QuerySeriesParams {
                start_ms: Some(clock),
                end_ms: Some(clock + 1_000.0),
                session_type: Some("RTH".into()),
                symbols: Some(vec!["NQ".into()]),
                fields: Some(vec!["market.location_structure.lastPrice".into()]),
                store: None,
            }))
            .await
            .expect("query_series default hot"),
    );
    assert_eq!(
        hot["meta"]["n"], 0,
        "default hot path must not silently read cold dumps"
    );

    let err = server
        .query_series(Parameters(QuerySeriesParams {
            start_ms: Some(clock),
            end_ms: Some(clock + 1_000.0),
            session_type: Some("RTH".into()),
            symbols: Some(vec!["NQ".into()]),
            fields: Some(vec!["market.location_structure.lastPrice".into()]),
            store: Some("duckdb".into()),
        }))
        .await
        .expect_err("unknown store");
    assert!(
        err.to_string().contains("hot or cold"),
        "unknown store must fail closed, got {err}"
    );
}

#[tokio::test]
async fn get_attention_inbox_is_ranked_view_over_event_stream() {
    let server = test_server_with_sil();
    {
        let db = server.db.lock().expect("db");
        db.insert_market_events_batch_scoped(
            Some("NQ"),
            &[MarketEvent {
                session_date: "2026-08-11".into(),
                timestamp_ms: 1_700_000_000_000.0,
                event_type: "pinch_detected".into(),
                level_name: Some("vwap".into()),
                price: 21000.0,
                direction: Some("from_below".into()),
                sequence_num: None,
                metadata: None,
                session_type: "RTH".into(),
                session_segment: "None".into(),
                trading_day: "2026-08-11".into(),
            }],
        )
        .expect("insert event");
    }
    let events = parse_text_tool_result(
        server
            .get_events(Parameters(GetEventsParams {
                event_type: Some("pinch_detected".into()),
                limit: Some(10),
                ..Default::default()
            }))
            .await
            .expect("get_events"),
    );
    assert_eq!(events["lifecycleFormalized"], true);
    assert_eq!(events["trustLevel"], "L0");
    let inbox = parse_text_tool_result(
        server
            .get_attention_inbox(Parameters(AttentionInboxParams::default()))
            .await
            .expect("inbox"),
    );
    assert_eq!(inbox["viewOf"], "eventStream");
    assert_eq!(inbox["lifecycleFormalized"], true);
    assert_eq!(inbox["sourceOperator"], "get_events");
    let signals = inbox["signals"].as_array().expect("signals");
    assert!(!signals.is_empty());
    assert_eq!(signals[0]["payload"]["viewOf"], "eventStream");
    assert_eq!(signals[0]["status"], "active");
    let summary = signals[0]["summary"].as_str().unwrap_or("");
    assert!(summary.contains("Your playbook") || summary.contains("your rules"));
    assert!(!summary.to_ascii_lowercase().contains("you should buy"));
    let signal_id = signals[0]["signalId"]
        .as_str()
        .expect("signalId")
        .to_string();
    let detail = parse_text_tool_result(
        server
            .get_signal_detail(Parameters(AttentionSignalDetailParams {
                signal_id: signal_id.clone(),
            }))
            .await
            .expect("detail"),
    );
    assert_eq!(detail["signal"]["signalId"], signal_id);
    assert_eq!(detail["signal"]["payload"]["viewOf"], "eventStream");
    let ack = parse_text_tool_result(
        server
            .acknowledge_attention_signal(Parameters(AttentionSignalAcknowledgeParams {
                signal_id: signal_id.clone(),
                acknowledged_by: "trader".into(),
                note: Some("reviewed".into()),
            }))
            .await
            .expect("ack"),
    );
    assert_eq!(ack["acknowledged"], true);
}

#[tokio::test]
async fn orientation_specialty_getters_shim_when_sil_on() {
    let server = test_server_with_sil();
    *server.last_bid.lock().expect("bid") = 21000.0;
    *server.last_ask.lock().expect("ask") = 21000.25;
    {
        let mut pipelines = server.pipelines.lock().expect("pipelines");
        let ts = chrono::Utc::now().timestamp_millis() as f64;
        pipelines.on_trade_with_timestamp(21000.0, 1.0, true, 30, ts);
    }

    let snap = parse_text_tool_result(
        server
            .get_market_snapshot()
            .await
            .expect("get_market_snapshot"),
    );
    assert_eq!(snap["deprecated"], true);
    assert_eq!(snap["suggestedReplacementOperator"], "get_state");

    let ctx = parse_text_tool_result(
        server
            .get_session_context()
            .await
            .expect("get_session_context"),
    );
    assert_eq!(ctx["deprecated"], true);
    assert_eq!(ctx["suggestedReplacementOperator"], "get_state");
}

#[tokio::test]
async fn orientation_shims_absent_when_sil_off() {
    let server = test_server();
    assert!(!server.sil_config.catalog_discovery);
    *server.last_bid.lock().expect("bid") = 21000.0;
    *server.last_ask.lock().expect("ask") = 21000.25;
    {
        let mut pipelines = server.pipelines.lock().expect("pipelines");
        let ts = chrono::Utc::now().timestamp_millis() as f64;
        pipelines.on_trade_with_timestamp(21000.0, 1.0, true, 30, ts);
    }
    let snap = parse_text_tool_result(
        server
            .get_market_snapshot()
            .await
            .expect("get_market_snapshot"),
    );
    assert!(snap.get("deprecated").is_none());
    assert!(snap.get("suggestedReplacementOperator").is_none());
}

#[tokio::test]
async fn opinionated_bundles_remain_available() {
    let server = test_server_with_sil();
    let names: std::collections::BTreeSet<_> = TheDeskMcp::tool_router()
        .list_all()
        .into_iter()
        .map(|t| t.name.to_string())
        .collect();
    for tool in [
        "get_context_frame",
        "get_attention_inbox",
        "evaluate_playbook",
    ] {
        assert!(names.contains(tool), "{tool} must remain available");
    }
    let _ = server.evaluate_playbook().await.expect("evaluate_playbook");
    let _ = server
        .get_attention_inbox(Parameters(AttentionInboxParams::default()))
        .await
        .expect("get_attention_inbox");
}

#[test]
fn sil_m0_baseline_untouched_by_m1b_shims() {
    // Soft dependency: M0 baseline remains the frozen before-figure.
    let path = crate::tool_telemetry::checked_in_baseline_path();
    let on_disk = crate::tool_telemetry::read_snapshot_file(&path).expect("baseline");
    assert_eq!(on_disk.tool_surface_count, 121);
    assert_eq!(
        on_disk.orientation_chain,
        crate::tool_telemetry::ORIENTATION_CHAIN
            .iter()
            .map(|s| (*s).to_string())
            .collect::<Vec<_>>()
    );
    // Shim rollout must not re-bless this artifact.
    let baseline_src = include_str!("../../../docs/mcp/sil-m0-tool-telemetry-baseline.json");
    assert!(baseline_src.contains("\"toolSurfaceCount\": 121"));
    assert!(!baseline_src.contains("get_state"));
}

#[test]
fn docs_state_specialty_market_tool_freeze_policy() {
    let readme = include_str!("../../../docs/mcp/README.md");
    let skill = include_str!("../../../skills/mcp-tools/SKILL.md");
    for (path, contents) in [
        ("docs/mcp/README.md", readme),
        ("skills/mcp-tools/SKILL.md", skill),
    ] {
        assert!(
            contents.contains("no new specialty market tools"),
            "{path} must document the SIL-M0 freeze: no new specialty market tools"
        );
        assert!(
            contents.contains("no catalog entry"),
            "{path} must document the post-Catalog-v0 rule: no catalog entry → no new market tool"
        );
    }
}

#[test]
fn sil_m0_telemetry_baseline_is_durable_and_current() {
    let path = crate::tool_telemetry::checked_in_baseline_path();
    let on_disk = crate::tool_telemetry::read_snapshot_file(&path)
        .unwrap_or_else(|e| panic!("missing SIL-M0 baseline at {}: {e}", path.display()));

    assert_eq!(
        on_disk.schema_version,
        crate::tool_telemetry::BASELINE_SCHEMA_VERSION
    );
    assert_eq!(
        on_disk.policy,
        "specialty_market_tools_frozen_until_catalog_v0"
    );
    assert_eq!(
        on_disk.orientation_chain,
        crate::tool_telemetry::ORIENTATION_CHAIN
            .iter()
            .map(|s| (*s).to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        on_disk.frozen_market_tools,
        crate::tool_telemetry::FROZEN_MARKET_TOOLS
            .iter()
            .map(|s| (*s).to_string())
            .collect::<Vec<_>>()
    );
    // toolSurfaceCount is frozen at the M0 capture moment — do not couple it to
    // the live router (workflow domains may still grow; the baseline stays put).
    assert_eq!(on_disk.tool_surface_count, 121);
    assert!(
        on_disk.orientation_chain_cost.fully_observed,
        "checked-in baseline must include a cold orientation-chain probe"
    );
    assert_eq!(
        on_disk.orientation_chain_cost.call_count,
        crate::tool_telemetry::ORIENTATION_CHAIN.len() as u64,
        "probe records exactly one call per orientation-chain tool"
    );
    assert!(on_disk.orientation_chain_cost.total_approx_tokens > 0);
    for tool in crate::tool_telemetry::ORIENTATION_CHAIN {
        let stats = on_disk
            .per_tool
            .get(*tool)
            .unwrap_or_else(|| panic!("baseline missing orientation probe for {tool}"));
        assert_eq!(
            stats.error_count, 0,
            "baseline probe for {tool} must be error-free"
        );
    }
}

#[tokio::test]
async fn mcp_dispatch_records_tool_call_telemetry() {
    let server = test_server();
    // Domain methods bypass ServerHandler::call_tool; exercise the same
    // observation helper the handler invokes after router dispatch.
    let result = server.get_session_context().await;
    server.tool_telemetry.record("get_session_context", &result);
    result.expect("session context");

    let snap = server
        .tool_telemetry
        .snapshot(TheDeskMcp::tool_router().list_all().len());
    let stats = snap
        .per_tool
        .get("get_session_context")
        .expect("telemetry for get_session_context");
    assert_eq!(stats.call_count, 1);
    assert!(stats.total_response_bytes > 0);
    assert!(stats.total_approx_tokens > 0);
    assert!(snap.orientation_chain_cost.call_count >= 1);

    // Handler wiring smoke: ServerHandler::call_tool must record telemetry.
    let handler_src = include_str!("handler.rs");
    assert!(
        handler_src.contains("tool_telemetry.record"),
        "ServerHandler::call_tool must record into tool_telemetry"
    );
    assert!(
        handler_src.contains("maybe_persist_tool_telemetry"),
        "ServerHandler::call_tool must periodically persist the runtime snapshot"
    );
}

#[test]
fn pipeline_lock_recently_contended_uses_a_latched_window() {
    let runtime = McpFeedRuntimeState::default();
    runtime.record_pipeline_lock_sample(true, 10_000);
    assert!(runtime.pipeline_lock_recently_contended(10_000));

    runtime.record_pipeline_lock_sample(false, 10_500);
    assert!(runtime.pipeline_lock_recently_contended(14_999));
    assert!(!runtime
        .pipeline_lock_recently_contended(10_000 + PIPELINE_CONTENTION_RECENT_WINDOW_MS + 1));
}

#[test]
fn current_market_snapshot_payload_surfaces_structured_contention_gap() {
    let server = test_server();
    let pipeline_ts = 1_700_000_000_000.0;
    server
        .feed_runtime
        .last_scid_tick_ms_bits
        .store(tick_ms_to_bits(pipeline_ts), Ordering::Release);

    let _pipeline_guard = server.pipelines.lock().expect("pipelines");
    let payload = server
        .current_market_snapshot_payload()
        .expect("structured contention payload");

    assert_eq!(
        payload.get("snapshotAvailable").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert_eq!(
        payload.get("snapshotSource").and_then(|v| v.as_str()),
        Some("contention_unavailable")
    );
    assert_eq!(
        payload.get("freshnessStatus").and_then(|v| v.as_str()),
        Some("contended")
    );
    assert_eq!(
        payload.get("degradationReason").and_then(|v| v.as_str()),
        Some("pipeline_lock_contended; no_persisted_feature_state_available_yet")
    );
    assert_eq!(
        payload
            .get("pipelineProcessedThroughMs")
            .and_then(|v| v.as_f64()),
        Some(pipeline_ts)
    );
    assert_eq!(
        payload.get("dbLockContended").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert_eq!(
            payload.get("message").and_then(|v| v.as_str()),
            Some(
                "Current market snapshot is temporarily unavailable while live pipeline contention is active. Retry shortly."
            )
        );
}

#[test]
fn parse_setup_perf_sort_validates_values() {
    assert_eq!(
        parse_setup_perf_sort(None).expect("default"),
        SetupPerformanceSortBy::Resolved
    );
    assert_eq!(
        parse_setup_perf_sort(Some("winRate")).expect("winRate"),
        SetupPerformanceSortBy::WinRate
    );
    assert!(parse_setup_perf_sort(Some("bogus")).is_err());
}

#[test]
fn build_session_scope_filter_validates_and_infers_segment() {
    let invalid = SessionScopeParams {
        session_type: Some("RTH".into()),
        session_segment: Some("Asia".into()),
        ..Default::default()
    };
    assert!(build_session_scope_filter(&invalid).is_err());

    let inferred = SessionScopeParams {
        session_segment: Some("London".into()),
        ..Default::default()
    };
    let scope = build_session_scope_filter(&inferred)
        .expect("scope")
        .expect("some");
    assert_eq!(scope.session_type.as_deref(), Some("Globex"));
    assert_eq!(scope.session_segment.as_deref(), Some("London"));

    let root_only = SessionScopeParams {
        root_symbol: Some("NQ".into()),
        ..Default::default()
    };
    let scope = build_session_scope_filter(&root_only)
        .expect("root-only scope")
        .expect("some");
    assert_eq!(scope.root_symbol.as_deref(), Some("NQ"));
}

#[test]
fn parse_scope_value_validates_loose_scope_payloads() {
    assert!(parse_scope_value(Some(serde_json::json!({
        "sessionType": "bad"
    })))
    .is_err());

    let scope = parse_scope_value(Some(serde_json::json!({
        "rootSymbol": "NQ",
        "continuousMode": true
    })))
    .expect("scope")
    .expect("some");
    assert_eq!(scope.root_symbol.as_deref(), Some("NQ"));
    assert!(scope.continuous_mode);
}

#[test]
fn research_field_validators_accept_supported_values() {
    assert_eq!(
        parse_research_event_type("ib_mid_test").expect("event"),
        "ib_mid_test"
    );
    assert_eq!(
        parse_research_event_type("IB_REENTRY").expect("event"),
        "ib_reentry"
    );
    assert_eq!(
        parse_research_outcome_field("close_vs_vwap").expect("field"),
        "close_vs_vwap"
    );
    assert_eq!(
        parse_distribution_metric("session_delta").expect("metric"),
        "session_delta"
    );
    assert_eq!(
        parse_distribution_metric("IB_RANGE").expect("metric"),
        "ib_range"
    );
    assert!(RESEARCH_DISTRIBUTION_METRICS.contains(&"ib_range"));
    assert!(RESEARCH_DISTRIBUTION_METRICS.contains(&"rvol_ratio"));
    assert_eq!(
        parse_signal_outcome_session_field("balance_state").expect("session field"),
        "balance_state"
    );
    assert_eq!(
        parse_dom_behavior_name("Liquidity_Flip").expect("behavior"),
        "liquidity_flip"
    );
    assert_eq!(
        research::RESEARCH_PERCENTILE_METHOD,
        "linear_interpolation_type7"
    );
    assert_eq!(research::RESEARCH_STDDEV_METHOD, "population");
}

#[test]
fn research_field_validators_reject_invalid_inputs() {
    assert!(parse_research_event_type("made_up_event").is_err());
    assert!(parse_research_event_type("made_up_test").is_err());
    assert!(parse_research_outcome_field("not_a_field").is_err());
    assert!(parse_distribution_metric("not_a_metric").is_err());
    assert!(parse_signal_outcome_session_field("not_a_field").is_err());
    assert!(parse_dom_behavior_name("not_a_behavior").is_err());
    assert!(parse_research_min_count(Some(-1)).is_err());
    assert!(parse_research_min_count(Some(0)).is_err());
    assert!(parse_nonnegative_i64("minResolved", Some(-1), 0, MAX_MIN_RESOLVED).is_err());
    assert!(parse_bounded_limit("limit", Some(0), 20, MAX_RESEARCH_RESULT_LIMIT).is_err());
    assert!(parse_dom_behavior_min_duration(Some(f64::INFINITY)).is_err());
    assert!(parse_dom_behavior_min_duration(Some(-1.0)).is_err());
}

#[test]
fn research_json_payloads_expose_metadata_contract() {
    let db = Database::open(":memory:").expect("db");
    let mut summary = summary_row("2026-03-05", "RTH", 21_010.0, 20_990.0, 21_000.0);
    summary.ib_range = 20.0;
    db.upsert_session_summary(&summary).expect("summary");

    let payload = serde_json::to_value(
        research::metric_distribution(&db, "ib_range", None, None, None).expect("metric"),
    )
    .expect("json");
    assert_eq!(
        payload
            .pointer("/meta/percentileMethod")
            .and_then(|v| v.as_str()),
        Some(research::RESEARCH_PERCENTILE_METHOD)
    );
    assert_eq!(
        payload
            .pointer("/meta/effectiveSampleSize")
            .and_then(|v| v.as_u64()),
        Some(1)
    );
}

#[test]
fn validate_ymd_range_rejects_invalid_and_reversed_dates() {
    assert!(validate_ymd_range(
        "startDate",
        Some("2026-03-04"),
        "endDate",
        Some("2026-03-05")
    )
    .is_ok());
    assert!(validate_ymd_range(
        "startDate",
        Some("2026-03-05"),
        "endDate",
        Some("2026-03-04")
    )
    .is_err());
    assert!(validate_ymd_range(
        "startDate",
        Some("03-05-2026"),
        "endDate",
        Some("2026-03-06")
    )
    .is_err());
}

#[test]
fn normalize_signal_source_validates_values() {
    assert_eq!(normalize_signal_source("live"), Some("live"));
    assert_eq!(normalize_signal_source("backtest"), Some("backtest"));
    assert_eq!(normalize_signal_source("backfill"), Some("backfill"));
    assert_eq!(normalize_signal_source("paper"), None);
}

#[test]
fn normalize_db_absorption_event_matches_live_shape() {
    let row = serde_json::json!({
        "timestampMs": 1234.0,
        "eventType": "absorption_confirmed",
        "price": 21000.0,
        "direction": "down",
        "metadata": {
            "eventSubtype": "absorption",
            "status": "confirmed",
            "severity": 3.5,
            "zoneLow": 20999.5,
            "zoneHigh": 21000.5,
            "keyLevel": "PriorDayHigh",
            "confirmationDeadlineMs": 1500.0,
            "confirmedAtMs": 1400.0,
            "invalidatedAtMs": null,
            "invalidationReason": null,
            "pacePercentile": 0.8,
            "rvolRatio": 1.1,
            "localVolatilityTicks": 4.0,
            "regimePhase": "open"
        }
    });

    let normalized = normalize_db_absorption_event(&row);
    assert_eq!(normalized["eventType"], "absorption");
    assert_eq!(normalized["status"], "confirmed");
    assert_eq!(normalized["zoneLow"], 20999.5);
    assert_eq!(normalized["pacePercentile"], 0.8);
    assert!(normalized.get("metadata").is_none());
}

#[tokio::test]
async fn dom_window_tool_returns_persisted_feature_snapshots() {
    let server = test_server();
    {
        let db = server.db.lock().expect("db lock");
        let payload = serde_json::json!({
            "domSummary": {
                "liquidityBias": "bid_support",
                "pullStackBias": 12.0
            },
            "activity": {
                "topPullLevels": [],
                "topStackLevels": []
            }
        });
        db.insert_dom_feature_snapshot("NQ.depth", 1_000.0, "2026-03-05", &payload)
            .expect("insert feature");
    }

    let result = server
        .get_dom_window(Parameters(DomWindowParams {
            start_time_ms: Some(900.0),
            end_time_ms: Some(1_100.0),
            price_low: None,
            price_high: None,
            limit: Some(10),
            include_aggregate: Some(true),
        }))
        .await
        .expect("tool call");

    let rendered = format!("{result:?}");
    assert!(rendered.contains("bid_support"));
}

#[tokio::test]
async fn get_key_levels_rth_uses_same_day_asia_and_london_dnva() {
    let server = test_server();
    {
        let db = server.db.lock().expect("db lock");
        db.upsert_session_summary(&summary_row(
            "2026-03-05",
            "Asia",
            21010.0,
            20990.0,
            21000.0,
        ))
        .expect("insert asia");
        db.upsert_session_summary(&summary_row(
            "2026-03-05",
            "London",
            21025.0,
            21005.0,
            21015.0,
        ))
        .expect("insert london");
        db.upsert_feature_state(
            1_000.0,
            &serde_json::json!({
                "sessionType": "RTH",
                "sessionSegment": "None",
                "tradingDay": "2026-03-05"
            }),
        )
        .expect("seed feature state");
    }

    let result = server.get_key_levels().await.expect("tool call");
    let rendered = format!("{result:?}");
    assert!(rendered.contains("priorAsiaDnvaHigh"));
    assert!(rendered.contains("21010.0"));
    assert!(rendered.contains("priorLondonDnvaHigh"));
    assert!(rendered.contains("21025.0"));
}

#[tokio::test]
async fn get_key_levels_globex_london_uses_same_day_asia_and_prior_london() {
    let server = test_server();
    {
        let db = server.db.lock().expect("db lock");
        db.upsert_session_summary(&summary_row(
            "2026-03-05",
            "Asia",
            21030.0,
            21010.0,
            21020.0,
        ))
        .expect("insert asia same day");
        db.upsert_session_summary(&summary_row(
            "2026-03-04",
            "London",
            21040.0,
            21020.0,
            21030.0,
        ))
        .expect("insert london prior");
        db.upsert_session_summary(&summary_row(
            "2026-03-05",
            "London",
            21999.0,
            21990.0,
            21994.5,
        ))
        .expect("insert london same day");
        db.upsert_feature_state(
            1_000.0,
            &serde_json::json!({
                "sessionType": "Globex",
                "sessionSegment": "London",
                "tradingDay": "2026-03-05"
            }),
        )
        .expect("seed feature state");
    }

    let result = server.get_key_levels().await.expect("tool call");
    let rendered = format!("{result:?}");
    assert!(rendered.contains("priorAsiaDnvaHigh"));
    assert!(rendered.contains("21030.0"));
    assert!(rendered.contains("priorLondonDnvaHigh"));
    assert!(rendered.contains("21040.0"));
}

/// Regression for Comment 1: exercise the actual validation and live-snapshot helper paths in
/// opposing phase order. If either path starts nesting `db` and `pipelines` again, this test
/// becomes a deadlock candidate instead of a clean join.
#[test]
fn validation_and_live_snapshot_helpers_join_under_opposing_phase_order() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let server = test_server();
    *server.last_bid.lock().expect("bid lock") = 21_000.0;
    *server.last_ask.lock().expect("ask lock") = 21_000.25;

    let barrier = Arc::new(Barrier::new(2));

    let validation_server = server.clone();
    let validation_barrier = Arc::clone(&barrier);
    let validation = thread::spawn(move || {
        for _ in 0..200 {
            let _ = collect_validation_db_snapshot(&validation_server.db).expect("db snapshot");
            validation_barrier.wait();
            let _ = collect_pipeline_invariants(&validation_server.pipelines)
                .expect("pipeline invariants");
        }
    });

    let snapshot_server = server.clone();
    let snapshot_barrier = Arc::clone(&barrier);
    let snapshot = thread::spawn(move || {
        for idx in 0..200 {
            let (timestamp_ms, payload) = build_live_feature_state_snapshot_payload(
                &snapshot_server.pipelines,
                &snapshot_server.last_bid,
                &snapshot_server.last_ask,
                1_000.0 + idx as f64,
            )
            .expect("live snapshot payload");
            snapshot_barrier.wait();
            persist_feature_state_payload(&snapshot_server.db, timestamp_ms, &payload);
        }
    });

    validation.join().expect("validation join");
    snapshot.join().expect("snapshot join");

    let db = server.db.lock().expect("db lock");
    assert!(db
        .latest_feature_state()
        .expect("latest feature state")
        .is_some());
    assert_eq!(db.raw_tick_count().expect("raw tick count"), 0);
}

#[test]
fn startup_cutover_replay_plus_live_resume_applies_ticks_once() {
    let server = test_server();
    let mut file = NamedTempFile::new().expect("temp");
    write_scid_header(&mut file);
    append_scid_sequence(&mut file, 0, &[21000.0, 21000.25, 21000.5]);

    let reader = ScidReader::new(file.path());
    let since = Utc
        .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
        .single()
        .expect("since timestamp")
        .timestamp_millis() as f64;
    let cutover = reader.current_aligned_end_offset().expect("cutover");

    // Simulate ticks arriving during startup while warm replay is in progress.
    append_scid_sequence(&mut file, 3, &[21000.75, 21001.0]);

    let warm = run_startup_warm_replay(
        &reader,
        &server.pipelines,
        &server.flow_emitter,
        &server.rules,
        &server.playbook_cache,
        &server.db,
        &server.runtime_events,
        &server.feed_runtime,
        since,
        cutover,
        &test_contract_metadata(),
    );
    let live = reader
        .read_bulk_from_offset(warm.cutover_offset)
        .expect("live resume");
    let mut event_buffer = Vec::new();
    for tick in &live.ticks {
        process_tick(
            &server.pipelines,
            &server.detector,
            &server.flow_emitter,
            &server.rules,
            &server.playbook_cache,
            &server.db,
            &server.runtime_events,
            &server.last_bid,
            &server.last_ask,
            tick.price,
            tick.volume,
            matches!(tick.side, TradeSide::Buy),
            tick.timestamp_ms,
            tick.bid,
            tick.ask,
            &mut event_buffer,
        );
    }

    let (bid, ask) = current_best_bid_ask(&server.last_bid, &server.last_ask);
    let snapshot = server
        .pipelines
        .lock()
        .expect("pipelines lock")
        .snapshot(bid, ask);

    assert_eq!(warm.cutover_offset, cutover);
    assert_eq!(warm.applied_tick_count, 3);
    assert_eq!(live.ticks.len(), 2);
    assert_eq!(snapshot.last_price, 21001.0);
    assert!((snapshot.vwap - 21000.5).abs() < 1e-9);
    assert_eq!(snapshot.session_low, 21000.0);
    assert_eq!(snapshot.session_high, 21001.0);
}

#[test]
fn startup_cutover_and_live_resume_preserve_scaled_prices() {
    let server = test_server();
    let mut file = NamedTempFile::new().expect("temp");
    write_scid_header(&mut file);
    append_scid_scaled_sequence(&mut file, 0, &[21000.0, 21000.25, 21000.5], 100.0);

    let reader = ScidReader::with_price_scale(file.path(), 100.0);
    let since = Utc
        .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
        .single()
        .expect("since timestamp")
        .timestamp_millis() as f64;
    let cutover = reader.current_aligned_end_offset().expect("cutover");

    append_scid_scaled_sequence(&mut file, 3, &[21000.75, 21001.0], 100.0);

    let warm = run_startup_warm_replay(
        &reader,
        &server.pipelines,
        &server.flow_emitter,
        &server.rules,
        &server.playbook_cache,
        &server.db,
        &server.runtime_events,
        &server.feed_runtime,
        since,
        cutover,
        &test_contract_metadata(),
    );
    let live = read_scid_poll_step(&reader, warm.cutover_offset).expect("live step");

    assert_eq!(warm.applied_tick_count, 3);
    assert_eq!(live.ticks.len(), 2);
    assert!((live.ticks[0].price - 21000.75).abs() < 1e-9);
    assert!((live.ticks[1].price - 21001.0).abs() < 1e-9);
}

#[tokio::test]
async fn warm_replay_reports_non_monotonic_ticks_in_health_and_integrity() {
    let server = test_server();
    let mut file = NamedTempFile::new().expect("temp");
    write_scid_header(&mut file);
    let base_ts_ms = Utc::now().timestamp_millis() as f64;
    append_scid_record(&mut file, 21000.0, base_ts_ms);
    append_scid_record(&mut file, 21000.25, base_ts_ms);
    append_scid_record(&mut file, 21000.5, base_ts_ms - 1.0);
    append_scid_record(&mut file, 21000.75, base_ts_ms + 2.0);
    file.flush().expect("flush");

    let reader = ScidReader::new(file.path());
    let warm = run_startup_warm_replay(
        &reader,
        &server.pipelines,
        &server.flow_emitter,
        &server.rules,
        &server.playbook_cache,
        &server.db,
        &server.runtime_events,
        &server.feed_runtime,
        base_ts_ms - 10.0,
        reader.current_aligned_end_offset().expect("cutover"),
        &test_contract_metadata(),
    );

    assert_eq!(warm.applied_tick_count, 2);

    let health = parse_text_tool_result(server.get_feed_health().await.expect("feed health"));
    assert_eq!(health["skippedNonMonotonicTicks"].as_u64(), Some(2));
    assert_eq!(health["duplicateTimestampTicks"].as_u64(), Some(1));
    assert_eq!(health["backwardTimestampTicks"].as_u64(), Some(1));
    assert_eq!(
        health["lastNonMonotonicTimestampMs"].as_f64(),
        Some(base_ts_ms - 1.0)
    );

    let integrity = parse_text_tool_result(
        server
            .validate_data_integrity()
            .await
            .expect("validate integrity"),
    );
    assert_eq!(integrity["skippedNonMonotonicTicks"].as_u64(), Some(2));
    assert_eq!(integrity["duplicateTimestampTicks"].as_u64(), Some(1));
    assert_eq!(integrity["backwardTimestampTicks"].as_u64(), Some(1));
    assert_eq!(
        integrity["checks"]["monotonicTimestamps"]["passed"].as_bool(),
        Some(false)
    );
}

#[test]
fn depth_shrink_recovery_preserves_previous_book_when_fragment_has_no_clear() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("NQ.depth");
    write_test_depth_file(
        &path,
        &[
            (unix_ms_to_sc_depth(1_000), 1, 0, 0, 0.0, 0),
            (unix_ms_to_sc_depth(1_000), 2, 0, 1, 100.0, 10),
            (unix_ms_to_sc_depth(1_000), 2, 0, 1, 99.75, 5),
            (unix_ms_to_sc_depth(1_000), 3, 0, 1, 100.25, 7),
        ],
    );

    let reader = DepthReader::new(&path, 1.0);
    let mut state = DepthPollWorkerState {
        active_path: Some(path.clone()),
        offset: reader.current_aligned_end_offset().expect("aligned end"),
        batch_id: 12,
        book: DepthBook::default(),
    };
    for record in reader.read_bulk().expect("read bulk") {
        state.book.apply(&record);
    }

    write_test_depth_file(&path, &[(unix_ms_to_sc_depth(2_000), 4, 0, 1, 100.0, 8)]);

    let work = recover_depth_state_after_shrink(&reader, &mut state)
        .expect("recover")
        .expect("work");

    let snapshot = work.snapshot;
    assert!(work.records.is_empty());
    assert_eq!(
        state.offset,
        reader.current_aligned_end_offset().expect("aligned end")
    );
    assert_eq!(snapshot.best_bid, Some(100.0));
    assert_eq!(snapshot.best_ask, Some(100.25));
    assert_eq!(
        snapshot
            .bids
            .iter()
            .find(|level| (level.price - 100.0).abs() < 1e-9)
            .map(|level| level.quantity),
        Some(8)
    );
    assert_eq!(
        snapshot
            .bids
            .iter()
            .find(|level| (level.price - 99.75).abs() < 1e-9)
            .map(|level| level.quantity),
        Some(5)
    );
}

fn sample_depth_persist_work(records: Vec<DepthRecord>) -> DepthPersistWork {
    let mut book = DepthBook::default();
    for record in &records {
        book.apply(record);
    }
    let last = records.last().expect("records");
    let source_file = "NQ.depth".to_string();
    let snapshot = book.snapshot(&source_file, last.timestamp_ms, 10);
    let feature = default_depth_feature_snapshot(
        &snapshot,
        &source_file,
        &records,
        (last.timestamp_ms - 60_000.0).max(0.0),
        last.timestamp_ms,
    );
    DepthPersistWork {
        source_file,
        trading_day: session_date_from_timestamp_ms(last.timestamp_ms),
        last_record_timestamp_ms: last.timestamp_ms,
        records,
        snapshot,
        feature,
        batch_id: 7,
    }
}

#[test]
fn apply_depth_persist_work_does_not_insert_depth_events() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("NQ.depth");
    write_test_depth_file(
        &path,
        &[
            (unix_ms_to_sc_depth(1_000), 1, 0, 0, 0.0, 0),
            (unix_ms_to_sc_depth(1_100), 2, 0, 1, 21000.0, 10),
            (unix_ms_to_sc_depth(1_200), 3, 0, 1, 21000.25, 12),
            (unix_ms_to_sc_depth(1_300), 6, 0, 0, 21000.0, 0),
        ],
    );
    let reader = DepthReader::new(&path, 1.0);
    let mut state = DepthPollWorkerState {
        active_path: Some(path.clone()),
        offset: reader.data_start_offset(),
        batch_id: 7,
        book: DepthBook::default(),
    };
    let mut new_records = Vec::new();
    reader
        .scan_new_records(&mut state.offset, |record| {
            state.book.apply(&record);
            new_records.push(record);
            Ok(DepthScanControl::Continue)
        })
        .expect("scan fixture depth");
    assert!(
        new_records.len() > 1,
        "fixture poll must have bulk rows, got {}",
        new_records.len()
    );

    let server = test_server();
    let work = sample_depth_persist_work(new_records);
    apply_depth_persist_work(
        &server.db,
        &server.pipelines,
        &server.last_bid,
        &server.last_ask,
        work,
        server.feed_runtime.as_ref(),
    );

    let db = server.db.lock().expect("db lock");
    assert_eq!(
        db.count_depth_events().expect("count depth_events"),
        0,
        "hot persist must not bulk-append depth_events"
    );
    let features = db
        .query_dom_feature_snapshots(Some(0.0), Some(10_000.0), 10)
        .expect("feature snapshots");
    assert_eq!(
        features.len(),
        1,
        "compact dom_feature_snapshots must write"
    );
    assert!(
        db.get_dom_snapshot_near(1_300.0)
            .expect("snapshot near")
            .is_some(),
        "compact dom_snapshots must write"
    );
    drop(db);

    let summary = server
        .pipelines
        .lock()
        .expect("pipelines")
        .snapshot(21000.0, 21000.25)
        .dom_summary;
    assert!(
        summary.is_some(),
        "poll persist must still publish domSummary into pipelines"
    );
}

#[test]
fn count_depth_records_from_reader_reads_fixture_not_sqlite() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("NQ.depth");
    write_test_depth_file(
        &path,
        &[
            (unix_ms_to_sc_depth(1_000), 1, 0, 0, 0.0, 0),
            (unix_ms_to_sc_depth(1_100), 2, 0, 1, 21000.0, 10),
            (unix_ms_to_sc_depth(1_200), 3, 0, 1, 21000.25, 12),
            (unix_ms_to_sc_depth(1_300), 6, 0, 0, 21000.0, 0),
        ],
    );
    let reader = DepthReader::new(&path, 1.0);
    let count = count_depth_records_from_reader(&reader, 1_000.0, 1_400.0, None, None)
        .expect("count from .depth");
    assert!(
        count >= 3,
        "fixture .depth must yield book records, got {count}"
    );
}

#[test]
fn count_depth_records_from_reader_filters_price_band() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("NQ.depth");
    write_test_depth_file(
        &path,
        &[
            (unix_ms_to_sc_depth(1_000), 1, 0, 0, 0.0, 0),
            (unix_ms_to_sc_depth(1_100), 2, 0, 1, 21000.0, 10),
            (unix_ms_to_sc_depth(1_200), 3, 0, 1, 21000.25, 12),
            (unix_ms_to_sc_depth(1_300), 2, 0, 1, 21001.0, 8),
        ],
    );
    let reader = DepthReader::new(&path, 1.0);
    let in_band =
        count_depth_records_from_reader(&reader, 1_000.0, 1_400.0, Some(20999.75), Some(21000.25))
            .expect("banded count");
    assert_eq!(
        in_band, 2,
        "band must count 21000.00 and 21000.25, not the clear or 21001.00"
    );
    let unfiltered = count_depth_records_from_reader(&reader, 1_000.0, 1_400.0, None, None)
        .expect("unfiltered count");
    assert_eq!(unfiltered, 4);
}

#[test]
fn count_depth_records_from_reader_caps_matching_records() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("NQ.depth");
    let extra = 5;
    let mut records = Vec::with_capacity(BOOK_REACTION_DEPTH_RECORD_CAP + extra);
    for i in 0..(BOOK_REACTION_DEPTH_RECORD_CAP + extra) {
        records.push((unix_ms_to_sc_depth(1_000 + i as i64), 2, 0, 1, 21000.0, 10));
    }
    write_test_depth_file(&path, &records);
    let reader = DepthReader::new(&path, 1.0);
    let count = count_depth_records_from_reader(&reader, 1_000.0, 2_000.0, None, None)
        .expect("capped count");
    assert_eq!(count, BOOK_REACTION_DEPTH_RECORD_CAP);
}

#[test]
fn count_depth_records_bounded_stops_after_scan_cap() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("NQ.depth");
    write_test_depth_file(
        &path,
        &[
            (unix_ms_to_sc_depth(1_000), 2, 0, 1, 19999.0, 1),
            (unix_ms_to_sc_depth(1_010), 2, 0, 1, 19999.0, 1),
            (unix_ms_to_sc_depth(1_020), 2, 0, 1, 19999.0, 1),
            (unix_ms_to_sc_depth(1_030), 2, 0, 1, 19999.0, 1),
            (unix_ms_to_sc_depth(1_040), 2, 0, 1, 19999.0, 1),
            (unix_ms_to_sc_depth(1_050), 2, 0, 1, 21000.0, 10),
            (unix_ms_to_sc_depth(1_060), 2, 0, 1, 21000.0, 10),
        ],
    );
    let reader = DepthReader::new(&path, 1.0);
    let stopped = count_depth_records_bounded(
        &reader,
        1_000.0,
        1_100.0,
        Some(20999.75),
        Some(21000.25),
        BOOK_REACTION_DEPTH_RECORD_CAP,
        5,
    )
    .expect("scan-capped count");
    assert_eq!(
        stopped, 0,
        "inspect cap must stop before later in-band records"
    );
    let reached = count_depth_records_bounded(
        &reader,
        1_000.0,
        1_100.0,
        Some(20999.75),
        Some(21000.25),
        BOOK_REACTION_DEPTH_RECORD_CAP,
        20,
    )
    .expect("uncapped inspect");
    assert_eq!(reached, 2);
}

#[test]
fn feature_activity_depth_count_requires_overlapping_window() {
    let overlapping = serde_json::json!({
        "activity": {
            "recordCount": 48,
            "startTimeMs": 1_000.0,
            "endTimeMs": 1_400.0
        }
    });
    assert_eq!(
        feature_activity_depth_count(&overlapping, 1_000.0, 1_400.0),
        (48, "dom_feature_activity")
    );

    let far = serde_json::json!({
        "activity": {
            "recordCount": 48,
            "startTimeMs": 1_700_000_000_000.0,
            "endTimeMs": 1_700_000_060_000.0
        }
    });
    assert_eq!(
        feature_activity_depth_count(&far, 1_000.0, 1_400.0),
        (0, "none")
    );

    let missing_window = serde_json::json!({
        "activity": { "recordCount": 48 }
    });
    assert_eq!(
        feature_activity_depth_count(&missing_window, 1_000.0, 1_400.0),
        (0, "none")
    );
}

#[test]
fn resolve_book_reaction_falls_through_when_depth_file_misses_window() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("NQ.depth");
    write_test_depth_file(
        &path,
        &[
            (unix_ms_to_sc_depth(1_000), 2, 0, 1, 21000.0, 10),
            (unix_ms_to_sc_depth(1_200), 3, 0, 1, 21000.25, 12),
        ],
    );
    let reader = DepthReader::new(&path, 1.0);
    let overlapping_activity = serde_json::json!({
        "activity": {
            "recordCount": 48,
            "startTimeMs": 50_000.0,
            "endTimeMs": 51_000.0
        }
    });
    let (count, source) = resolve_book_reaction_depth_count_from(
        Some(&reader),
        50_000.0,
        51_000.0,
        None,
        None,
        &overlapping_activity,
    )
    .expect("resolve");
    assert_eq!(source, "dom_feature_activity");
    assert_eq!(count, 48);
}

#[test]
fn resolve_book_reaction_falls_through_when_in_band_count_is_zero() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("NQ.depth");
    write_test_depth_file(
        &path,
        &[
            (unix_ms_to_sc_depth(1_000), 1, 0, 0, 0.0, 0),
            (unix_ms_to_sc_depth(1_100), 2, 0, 1, 19999.0, 10),
        ],
    );
    let reader = DepthReader::new(&path, 1.0);
    let overlapping_activity = serde_json::json!({
        "activity": {
            "recordCount": 48,
            "startTimeMs": 1_000.0,
            "endTimeMs": 1_400.0
        }
    });
    let (count, source) = resolve_book_reaction_depth_count_from(
        Some(&reader),
        1_000.0,
        1_400.0,
        Some(20999.75),
        Some(21000.25),
        &overlapping_activity,
    )
    .expect("resolve");
    assert_eq!(source, "dom_feature_activity");
    assert_eq!(count, 48);
}

#[test]
fn explain_book_reaction_payload_grounds_when_depth_events_empty() {
    let db = Database::open(":memory:").expect("db");
    assert_eq!(db.count_depth_events().expect("empty"), 0);

    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("NQ.depth");
    write_test_depth_file(
        &path,
        &[
            (unix_ms_to_sc_depth(1_000), 1, 0, 0, 0.0, 0),
            (unix_ms_to_sc_depth(1_100), 2, 0, 1, 21000.0, 10),
            (unix_ms_to_sc_depth(1_200), 3, 0, 1, 21000.25, 12),
        ],
    );
    let reader = DepthReader::new(&path, 1.0);
    let depth_event_count =
        count_depth_records_from_reader(&reader, 1_000.0, 1_400.0, None, None).expect("count");
    assert!(depth_event_count > 0);

    let feature_payload = serde_json::json!({
        "domSummary": {
            "bidPullRate": 0.62,
            "askPullRate": 0.18,
            "pullStackBias": 28.0,
            "liquidityBias": "bid_support"
        },
        "activity": {
            "recordCount": depth_event_count,
            "topPullLevels": [{
                "price": 21000.0,
                "side": "bid",
                "estimatedPulledQuantity": 10.0
            }],
            "topStackLevels": [{
                "price": 20999.75,
                "side": "bid",
                "stackedQuantity": 8.0
            }]
        }
    });
    let payload = build_book_reaction_payload(BookReactionInputs {
        timestamp_ms: 1_200.0,
        start_time_ms: 1_000.0,
        end_time_ms: 1_400.0,
        price: Some(21000.0),
        radius_ticks: Some(6),
        feature_payload,
        depth_event_count,
        depth_source: "depth_file",
        ticks: Vec::new(),
    });
    let explanation = payload["explanation"].as_str().expect("explanation");
    assert!(
        explanation.contains("Bids pulled"),
        "expected grounded pull-rate narrative, got {explanation}"
    );
    assert!(
        explanation.contains("depth records in window"),
        "expected .depth density narrative, got {explanation}"
    );
    assert_eq!(payload["depthSource"], "depth_file");
    assert_eq!(payload["depthEventCount"], depth_event_count);
    assert_eq!(payload["liquidityBias"], "bid_support");
}

#[tokio::test]
async fn explain_book_reaction_uses_feature_snapshot_when_depth_events_empty() {
    let server = test_server();
    {
        let db = server.db.lock().expect("db lock");
        assert_eq!(db.count_depth_events().expect("empty"), 0);
        let payload = serde_json::json!({
            "domSummary": {
                "bidPullRate": 0.7,
                "askPullRate": 0.2,
                "pullStackBias": 40.0,
                "liquidityBias": "bid_support"
            },
            "activity": {
                "recordCount": 48,
                "startTimeMs": 1_000.0,
                "endTimeMs": 1_400.0,
                "topPullLevels": [{
                    "price": 21000.0,
                    "side": "bid",
                    "estimatedPulledQuantity": 15.0
                }],
                "topStackLevels": []
            }
        });
        db.insert_dom_feature_snapshot("NQ.depth", 1_200.0, "2026-03-05", &payload)
            .expect("insert feature");
    }

    let result = server
        .explain_book_reaction(Parameters(ExplainBookReactionParams {
            timestamp_ms: Some(1_200.0),
            price: Some(21000.0),
            start_time_ms: Some(1_000.0),
            end_time_ms: Some(1_400.0),
            radius_ticks: Some(6),
        }))
        .await
        .expect("tool call");
    let json = parse_text_tool_result(result);
    let explanation = json["explanation"].as_str().expect("explanation");
    assert!(
        explanation.contains("Bids pulled"),
        "tool must not silently return empty book activity, got {explanation}"
    );
    let depth_source = json["depthSource"].as_str().expect("depthSource");
    assert!(
        depth_source == "dom_feature_activity" || depth_source == "depth_file",
        "expected snapshot or .depth source, got {depth_source}"
    );
    if depth_source == "dom_feature_activity" {
        assert_eq!(json["depthEventCount"], 48);
        let explanation = json["explanation"].as_str().expect("explanation");
        assert!(
            explanation.contains("overlapping compact DOM feature snapshot"),
            "activity fallback must not claim an in-window .depth count, got {explanation}"
        );
    } else {
        assert!(json["depthEventCount"].as_u64().unwrap_or(0) > 0);
    }
    assert_eq!(json["liquidityBias"], "bid_support");
}

#[test]
fn playbook_cache_hydration_loads_active_setups_and_risk_gate() {
    let db = Database::open(":memory:").expect("db");
    db.upsert_setup(&SetupDefinition {
        id: "active_seed".to_string(),
        name: "Active Seed".to_string(),
        active: true,
        ..Default::default()
    })
    .expect("insert active");
    db.upsert_setup(&SetupDefinition {
        id: "inactive_seed".to_string(),
        name: "Inactive Seed".to_string(),
        active: false,
        ..Default::default()
    })
    .expect("insert inactive");
    db.save_risk_state(&RiskState {
        at_limit: true,
        ..Default::default()
    })
    .expect("save risk state");

    let server = TheDeskMcp::new(db, PipelineEngine::new(), ":memory:".into());
    server
        .hydrate_playbook_runtime_cache()
        .expect("hydrate playbook cache");
    let (setups, risk_at_limit) = server.playbook_cache.snapshot();

    assert_eq!(setups.len(), 1);
    assert_eq!(setups[0].id, "active_seed");
    assert!(risk_at_limit);
}

#[test]
fn playbook_cache_hydration_rehydrates_setup_runtime_state() {
    let db = Database::open(":memory:").expect("db");
    db.upsert_setup(&SetupDefinition {
        id: "rehydrated_setup".to_string(),
        name: "Rehydrated Setup".to_string(),
        active: true,
        ..Default::default()
    })
    .expect("insert setup");
    db.upsert_setup_runtime_state(&SetupRuntimeStateRecord {
        session_date: the_desk_backend::et_now_trading_day(),
        root_symbol: Some("NQ".to_string()),
        contract_symbol: Some("NQH26.CME".to_string()),
        setup_id: "rehydrated_setup".to_string(),
        setup_name: Some("Rehydrated Setup".to_string()),
        state: SetupState::Approaching,
        readiness: SetupReadiness::DeterministicReady,
        readiness_score: 1.0,
        met_count: 1,
        total_count: 1,
        met_conditions: vec!["min_delta".to_string()],
        missing_conditions: Vec::new(),
        deterministic_all_met: true,
        requires_discretionary: true,
        current_price: 21010.0,
        last_evaluated_at_ms: 1_000.0,
        last_transition_at_ms: 1_000.0,
        last_alert_emitted_at_ms: Some(1_000.0),
        source: "live".to_string(),
        updated_at_ms: 1_000.0,
    })
    .expect("seed runtime");

    let server = TheDeskMcp::new(db, PipelineEngine::new(), ":memory:".into());
    server
        .hydrate_playbook_runtime_cache()
        .expect("hydrate playbook cache");
    let snapshot = server
        .rules
        .lock()
        .expect("rules lock")
        .runtime_snapshot("rehydrated_setup")
        .expect("runtime snapshot");

    assert_eq!(snapshot.readiness, SetupReadiness::DeterministicReady);
    assert!(server
        .feed_runtime
        .setup_runtime_rehydrated
        .load(Ordering::Acquire));
}

#[test]
fn process_tick_uses_cached_risk_gate_for_alert_suppression() {
    let server = test_server();
    server
        .playbook_cache
        .replace_active_setups(vec![SetupDefinition {
            id: "risk_gated_setup".to_string(),
            name: "Risk Gated Setup".to_string(),
            active: true,
            min_delta: 0.0,
            conditions: Vec::new(),
            ..Default::default()
        }]);
    server.playbook_cache.set_risk_at_limit(true);

    let mut event_buffer = Vec::new();
    process_tick(
        &server.pipelines,
        &server.detector,
        &server.flow_emitter,
        &server.rules,
        &server.playbook_cache,
        &server.db,
        &server.runtime_events,
        &server.last_bid,
        &server.last_ask,
        21_000.0,
        1.0,
        true,
        Utc::now().timestamp_millis() as f64,
        20_999.75,
        21_000.25,
        &mut event_buffer,
    );

    let db = server.db.lock().expect("db lock");
    assert_eq!(db.count_playbook_signals().expect("signal count"), 0);
    drop(db);
    let state = server
        .rules
        .lock()
        .expect("rules lock")
        .get_state("risk_gated_setup");
    assert_eq!(format!("{state:?}"), "NotActive");
}

#[test]
fn process_tick_persists_setup_runtime_and_history() {
    let server = test_server();
    server
        .playbook_cache
        .replace_active_setups(vec![SetupDefinition {
            id: "persisted_setup".to_string(),
            name: "Persisted Setup".to_string(),
            active: true,
            min_delta: 0.0,
            conditions: Vec::new(),
            ..Default::default()
        }]);
    server.playbook_cache.set_risk_at_limit(false);
    let ts = Utc
        .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
        .single()
        .expect("timestamp")
        .timestamp_millis() as f64;

    let mut event_buffer = Vec::new();
    process_tick(
        &server.pipelines,
        &server.detector,
        &server.flow_emitter,
        &server.rules,
        &server.playbook_cache,
        &server.db,
        &server.runtime_events,
        &server.last_bid,
        &server.last_ask,
        21_000.0,
        1.0,
        true,
        ts,
        20_999.75,
        21_000.25,
        &mut event_buffer,
    );

    let db = server.db.lock().expect("db lock");
    let rows = db
        .load_setup_runtime_state_for_session(&session_date_from_timestamp_ms(ts))
        .expect("runtime rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].setup_id, "persisted_setup");
    assert_eq!(rows[0].last_evaluated_at_ms, ts);
    let history = db
        .query_setup_state_history(
            Some("persisted_setup"),
            Some(&session_date_from_timestamp_ms(ts)),
            None,
            10,
        )
        .expect("history");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].timestamp_ms, ts);
    let outcome_rows = db
        .list_signal_outcomes_for_replay(Some("live"), None)
        .expect("signal outcomes");
    assert_eq!(outcome_rows.len(), 1);
    assert_eq!(outcome_rows[0].fired_at_ms, ts);
    assert_eq!(
        outcome_rows[0].outcome_quality,
        outcomes::QUALITY_NOT_BACKTESTABLE
    );
    assert_eq!(outcome_rows[0].outcome, "not_backtestable");
    assert_eq!(db.count_playbook_signals().expect("signals"), 1);
}

#[test]
fn setup_lifecycle_uses_trading_day_across_globex_manual_and_live_paths() {
    use chrono::NaiveDate;
    use chrono_tz::US::Eastern;

    let server = test_server();
    server
        .playbook_cache
        .replace_active_setups(vec![SetupDefinition {
            id: "globex_setup".to_string(),
            name: "Globex Setup".to_string(),
            active: true,
            min_delta: 0.0,
            conditions: Vec::new(),
            ..Default::default()
        }]);
    server.playbook_cache.set_risk_at_limit(false);
    let globex_ts = Eastern
        .from_local_datetime(
            &NaiveDate::from_ymd_opt(2026, 3, 5)
                .expect("date")
                .and_hms_opt(18, 30, 0)
                .expect("time"),
        )
        .single()
        .expect("non-ambiguous ET timestamp")
        .timestamp_millis() as f64;
    assert_ne!(
        session_date_from_timestamp_ms(globex_ts),
        trading_day_from_timestamp_ms(globex_ts)
    );

    let mut event_buffer = Vec::new();
    process_tick(
        &server.pipelines,
        &server.detector,
        &server.flow_emitter,
        &server.rules,
        &server.playbook_cache,
        &server.db,
        &server.runtime_events,
        &server.last_bid,
        &server.last_ask,
        21_000.0,
        1.0,
        true,
        globex_ts,
        20_999.75,
        21_000.25,
        &mut event_buffer,
    );

    let manual_ts = globex_ts + 60_000.0;
    let (before, after) = {
        let mut rules = server.rules.lock().expect("rules lock");
        let before = rules.runtime_snapshot("globex_setup");
        rules
            .acknowledge_prompt_at("globex_setup", manual_ts)
            .expect("acknowledge setup");
        let after = rules
            .runtime_snapshot("globex_setup")
            .expect("runtime snapshot");
        (before, after)
    };
    server
        .persist_manual_setup_state_change(
            "globex_setup",
            before,
            after,
            "manualConfirmed",
            manual_ts,
        )
        .expect("persist manual state");

    let db = server.db.lock().expect("db lock");
    let trading_day_rows = db
        .load_setup_runtime_state_for_session(&trading_day_from_timestamp_ms(globex_ts))
        .expect("trading-day runtime rows");
    assert_eq!(trading_day_rows.len(), 1);
    assert_eq!(trading_day_rows[0].setup_id, "globex_setup");
    assert_eq!(trading_day_rows[0].state, SetupState::Confirmed);

    let calendar_rows = db
        .load_setup_runtime_state_for_session(&session_date_from_timestamp_ms(globex_ts))
        .expect("calendar-date runtime rows");
    assert!(calendar_rows.is_empty());
}

#[test]
fn process_tick_skips_runtime_write_when_progress_is_unchanged() {
    let server = test_server();
    server
        .playbook_cache
        .replace_active_setups(vec![SetupDefinition {
            id: "coalesced_setup".to_string(),
            name: "Coalesced Setup".to_string(),
            active: true,
            min_delta: 0.0,
            conditions: Vec::new(),
            ..Default::default()
        }]);
    server.playbook_cache.set_risk_at_limit(false);
    let ts = Utc
        .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
        .single()
        .expect("timestamp")
        .timestamp_millis() as f64;
    let mut event_buffer = Vec::new();

    process_tick(
        &server.pipelines,
        &server.detector,
        &server.flow_emitter,
        &server.rules,
        &server.playbook_cache,
        &server.db,
        &server.runtime_events,
        &server.last_bid,
        &server.last_ask,
        21_000.0,
        1.0,
        true,
        ts,
        20_999.75,
        21_000.25,
        &mut event_buffer,
    );
    process_tick(
        &server.pipelines,
        &server.detector,
        &server.flow_emitter,
        &server.rules,
        &server.playbook_cache,
        &server.db,
        &server.runtime_events,
        &server.last_bid,
        &server.last_ask,
        21_000.25,
        1.0,
        true,
        ts + 1_000.0,
        21_000.0,
        21_000.5,
        &mut event_buffer,
    );

    let db = server.db.lock().expect("db lock");
    let rows = db
        .load_setup_runtime_state_for_session(&session_date_from_timestamp_ms(ts))
        .expect("runtime rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].last_evaluated_at_ms, ts);
    let history = db
        .query_setup_state_history(
            Some("coalesced_setup"),
            Some(&session_date_from_timestamp_ms(ts)),
            None,
            10,
        )
        .expect("history");
    assert_eq!(history.len(), 1);
    assert_eq!(db.count_playbook_signals().expect("signals"), 1);
}

#[test]
fn startup_warm_replay_persists_setup_runtime_without_live_signals() {
    let server = test_server();
    server
        .playbook_cache
        .replace_active_setups(vec![SetupDefinition {
            id: "replay_setup".to_string(),
            name: "Replay Setup".to_string(),
            active: true,
            min_delta: 0.0,
            conditions: Vec::new(),
            ..Default::default()
        }]);
    server.playbook_cache.set_risk_at_limit(false);
    let mut file = NamedTempFile::new().expect("temp");
    write_scid_header(&mut file);
    append_scid_sequence(&mut file, 0, &[21000.0, 21000.25]);
    let reader = ScidReader::new(file.path());
    let since = Utc
        .with_ymd_and_hms(2026, 3, 5, 14, 59, 0)
        .single()
        .expect("since timestamp")
        .timestamp_millis() as f64;
    let cutover = reader.current_aligned_end_offset().expect("cutover");

    let warm = run_startup_warm_replay(
        &reader,
        &server.pipelines,
        &server.flow_emitter,
        &server.rules,
        &server.playbook_cache,
        &server.db,
        &server.runtime_events,
        &server.feed_runtime,
        since,
        cutover,
        &test_contract_metadata(),
    );

    assert_eq!(warm.applied_tick_count, 2);
    let trading_day = trading_day_from_timestamp_ms(
        Utc.with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
            .single()
            .expect("base timestamp")
            .timestamp_millis() as f64,
    );
    let db = server.db.lock().expect("db lock");
    let rows = db
        .load_setup_runtime_state_for_session(&trading_day)
        .expect("runtime rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].setup_id, "replay_setup");
    let history = db
        .query_setup_state_history(Some("replay_setup"), Some(&trading_day), None, 10)
        .expect("history");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].source, "startup_replay");
    assert_eq!(db.count_playbook_signals().expect("signals"), 0);
    assert!(db
        .pending_signal_outcomes()
        .expect("pending outcomes")
        .is_empty());
}

#[tokio::test]
async fn evaluate_playbook_uses_external_engine_published_market_state() {
    let db = Database::open(":memory:").expect("db");
    let logging_config = the_desk_backend::observability::LoggingConfig {
        destination: "none".to_string(),
        runtime_event_suppression_window_ms: 0,
        ..the_desk_backend::observability::LoggingConfig::default()
    };
    let server = TheDeskMcp::with_runtime_events_and_sil(
        db,
        PipelineEngine::new(),
        ":memory:".into(),
        std::sync::Arc::new(the_desk_backend::observability::RuntimeEventStore::new(
            &logging_config,
        )),
        the_desk_backend::catalog::SilConfig {
            catalog_discovery: true,
            engine_mode: the_desk_backend::catalog::EngineMode::External,
            ..Default::default()
        },
    );
    let store = server
        .engine_published
        .as_ref()
        .expect("external mode publishes store")
        .clone();
    store.store(the_desk_backend::engine::PublishedEngineState {
        generation: 3,
        engine_pid: 1,
        published_at_ms: 1.0,
        data_time_ms: Some(1_704_207_600_000.0),
        source_provider: the_desk_backend::engine::SourceProviderKind::File,
        market_state: serde_json::to_value(MarketState {
            last_price: 20_500.0,
            bid: 20_499.75,
            ask: 20_500.25,
            vwap: 20_490.0,
            ..Default::default()
        })
        .expect("market state json"),
        recent_events: vec![],
        health: the_desk_backend::engine::EngineHealth::unavailable("test"),
        degraded: false,
        degraded_note: None,
        by_symbol: Default::default(),
        clock_ms: Some(1_704_207_600_000.0),
        primary_root: "NQ".into(),
    });
    server
        .playbook_cache
        .replace_active_setups(vec![SetupDefinition {
            id: "engine_pub_setup".to_string(),
            name: "Engine Pub Setup".to_string(),
            active: true,
            min_delta: 0.0,
            conditions: Vec::new(),
            ..Default::default()
        }]);
    let result = server.evaluate_playbook().await.expect("evaluate");
    let rendered = format!("{result:?}");
    assert!(
        rendered.contains("engine_pub_setup"),
        "evaluate_playbook must use published MarketState in external mode: {rendered}"
    );
    assert!(!rendered.contains("\"state\": \"unknown\""));
}

#[tokio::test]
async fn evaluate_playbook_reads_cache_snapshot() {
    let server = test_server();
    server
        .playbook_cache
        .replace_active_setups(vec![SetupDefinition {
            id: "cache_only_setup".to_string(),
            name: "Cache Only Setup".to_string(),
            active: true,
            min_delta: 0.0,
            conditions: Vec::new(),
            ..Default::default()
        }]);
    server.playbook_cache.set_risk_at_limit(false);
    *server.last_bid.lock().expect("bid lock") = 21_000.0;
    *server.last_ask.lock().expect("ask lock") = 21_000.25;

    let result = server.evaluate_playbook().await.expect("evaluate");
    let rendered = format!("{result:?}");
    assert!(rendered.contains("cache_only_setup"));
    assert_eq!(
        server
            .rules
            .lock()
            .expect("rules lock")
            .get_state("cache_only_setup"),
        SetupState::NotActive
    );

    let ts = Utc
        .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
        .single()
        .expect("timestamp")
        .timestamp_millis() as f64;
    let mut event_buffer = Vec::new();
    process_tick(
        &server.pipelines,
        &server.detector,
        &server.flow_emitter,
        &server.rules,
        &server.playbook_cache,
        &server.db,
        &server.runtime_events,
        &server.last_bid,
        &server.last_ask,
        21_000.0,
        1.0,
        true,
        ts,
        20_999.75,
        21_000.25,
        &mut event_buffer,
    );
    assert_eq!(
        server
            .db
            .lock()
            .expect("db lock")
            .count_playbook_signals()
            .expect("signals"),
        1
    );
}

#[tokio::test]
async fn manual_setup_lifecycle_persists_runtime_transition_timestamp() {
    let server = test_server();
    server
        .playbook_cache
        .replace_active_setups(vec![SetupDefinition {
            id: "manual_setup".to_string(),
            name: "Manual Setup".to_string(),
            active: true,
            min_delta: 0.0,
            conditions: Vec::new(),
            ..Default::default()
        }]);
    server.playbook_cache.set_risk_at_limit(false);
    let ts = Utc
        .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
        .single()
        .expect("timestamp")
        .timestamp_millis() as f64;
    let mut event_buffer = Vec::new();
    process_tick(
        &server.pipelines,
        &server.detector,
        &server.flow_emitter,
        &server.rules,
        &server.playbook_cache,
        &server.db,
        &server.runtime_events,
        &server.last_bid,
        &server.last_ask,
        21_000.0,
        1.0,
        true,
        ts,
        20_999.75,
        21_000.25,
        &mut event_buffer,
    );

    server
        .acknowledge_setup_prompt(Parameters(SetupLifecycleParams {
            setup_id: "manual_setup".to_string(),
        }))
        .await
        .expect("acknowledge setup");

    let db = server.db.lock().expect("db lock");
    let latest_history = db
        .query_setup_state_history(Some("manual_setup"), None, None, 1)
        .expect("history")
        .pop()
        .expect("manual history row");
    assert_eq!(latest_history.reason, "manualConfirmed");

    let rows = db
        .load_setup_runtime_state_for_session(&the_desk_backend::et_now_trading_day())
        .expect("runtime rows");
    let manual_row = rows
        .iter()
        .find(|row| row.setup_id == "manual_setup")
        .expect("manual runtime row");
    assert_eq!(
        manual_row.last_transition_at_ms,
        latest_history.timestamp_ms
    );
}

#[tokio::test]
async fn risk_state_mutation_tools_sync_playbook_cache() {
    let server = test_server();
    {
        let db = server.db.lock().expect("db lock");
        db.save_risk_config(&RiskConfigRecord {
            max_daily_loss_r: 1.0,
            ..Default::default()
        })
        .expect("save risk config");
    }

    server.playbook_cache.set_risk_at_limit(true);
    server.init_risk_state().await.expect("init risk");
    assert!(!server.playbook_cache.snapshot().1);
    {
        let db = server.db.lock().expect("db lock");
        assert!(
            !db.load_risk_state()
                .expect("load risk")
                .expect("risk state")
                .at_limit
        );
    }

    server
        .record_trade_result(Parameters(RecordTradeResultParams {
            direction: "long".to_string(),
            size: 1,
            entry_price: 21_000.0,
            exit_price: 20_990.0,
            result_r: -2.0,
            setup_id: None,
            stop_price: None,
            notes: None,
        }))
        .await
        .expect("record trade");
    assert!(server.playbook_cache.snapshot().1);
    {
        let db = server.db.lock().expect("db lock");
        assert!(
            db.load_risk_state()
                .expect("load risk")
                .expect("risk state")
                .at_limit
        );
    }

    let trade_id = "risk_sync_trade".to_string();
    server
        .upsert_trade_entry(Parameters(UpsertTradeEntryParams {
            id: Some(trade_id.clone()),
            direction: "long".to_string(),
            size: 1,
            entry_price: 21_005.0,
            ..Default::default()
        }))
        .await
        .expect("upsert trade");
    server
        .close_trade_entry(Parameters(CloseTradeEntryParams {
            id: trade_id,
            exit_price: 21_015.0,
            exit_time_ms: None,
            result_r: Some(5.0),
            gross_points: Some(10.0),
            notes: None,
            update_risk_state: Some(true),
        }))
        .await
        .expect("close trade");

    assert!(!server.playbook_cache.snapshot().1);
    let db = server.db.lock().expect("db lock");
    assert!(
        !db.load_risk_state()
            .expect("load risk")
            .expect("risk state")
            .at_limit
    );
}

/// Build an epoch-ms timestamp for an RTH wall-clock time on a fixed test
/// date (2026-03-05, a Thursday in DST). Used by the boundary-recovery
/// tests to drive `finalize_rth_close` deterministically.
fn rth_ts(hour: u32, minute: u32, second: u32) -> f64 {
    use chrono::NaiveDate;
    use chrono_tz::US::Eastern;
    let naive = NaiveDate::from_ymd_opt(2026, 3, 5)
        .expect("date")
        .and_hms_opt(hour, minute, second)
        .expect("time");
    Eastern
        .from_local_datetime(&naive)
        .single()
        .expect("non-ambiguous ET timestamp")
        .timestamp_millis() as f64
}

/// Drive a few RTH ticks through the pipeline so finalize_rth_close has
/// real session state to snapshot. Mirrors the live ingest call shape but
/// skips the rules engine to keep tests focused on boundary persistence.
fn warm_rth_session(server: &TheDeskMcp, prices: &[f64]) {
    let mut p = server.pipelines.lock().expect("pipelines");
    for (i, price) in prices.iter().enumerate() {
        let ts = rth_ts(15, 30, i as u32);
        let minute = minute_of_session_from_timestamp(ts);
        p.on_trade_with_timestamp(*price, 1.0, i % 2 == 0, minute, ts);
    }
}

/// Boundary recovery: a single live RTH→Unknown transition must persist
/// `session_summaries` and `prior_day_levels` in one transaction, refresh
/// in-memory carry-forward, and leave `session_inventory` aware of the
/// just-closed session before any further DB read happens.
#[test]
fn finalize_rth_close_persists_summary_and_carry_forward_atomically() {
    let server = test_server();
    warm_rth_session(&server, &[21_000.0, 21_005.0, 21_010.0, 21_015.0, 21_012.0]);

    let boundary_ts = rth_ts(16, 0, 1);
    let result = finalize_rth_close(
        &server.pipelines,
        &server.db,
        &[],
        None,
        None,
        None,
        boundary_ts,
        21_011.75,
        21_012.25,
        &test_contract_metadata(),
    )
    .expect("close finalize")
    .expect("close result");

    assert_eq!(result.session_date, "2026-03-05");
    assert!((result.high - 21_015.0).abs() < 1e-6);
    assert!((result.low - 21_000.0).abs() < 1e-6);

    let db = server.db.lock().expect("db");
    assert!(db
        .has_session_summary_for("2026-03-05", "RTH")
        .expect("summary lookup"));
    let prior = db
        .load_prior_day_full("2026-03-06")
        .expect("prior load")
        .expect("prior row exists");
    assert!((prior.0 - 21_015.0).abs() < 1e-6);
    assert!((prior.1 - 21_000.0).abs() < 1e-6);
    drop(db);

    // In-memory carry-forward should match the just-built end-state without
    // any extra DB reload.
    let p = server.pipelines.lock().expect("pipelines");
    assert!((p.levels.prior_day_high - 21_015.0).abs() < 1e-6);
    assert!((p.levels.prior_day_low - 21_000.0).abs() < 1e-6);
    assert!(!p.levels.rth_started());
}

#[test]
fn finalize_rth_close_persists_pending_ib_extension_event_context() {
    let server = test_server();
    warm_rth_session(&server, &[21_000.0, 21_005.0, 21_010.0]);
    let first_extension_ts = rth_ts(10, 31, 0);
    let pending_events = vec![MarketEvent {
        session_date: "2026-03-05".to_string(),
        timestamp_ms: first_extension_ts,
        event_type: "ib_extension_hit".to_string(),
        level_name: Some("ib_ext_0.5x_high".to_string()),
        price: 21_020.0,
        direction: Some("from_below".to_string()),
        sequence_num: None,
        metadata: Some(serde_json::json!({"extensionDirection": "up"})),
        session_type: "RTH".to_string(),
        session_segment: "None".to_string(),
        trading_day: "2026-03-05".to_string(),
    }];

    finalize_rth_close(
        &server.pipelines,
        &server.db,
        &pending_events,
        None,
        None,
        None,
        rth_ts(16, 0, 1),
        21_009.75,
        21_010.25,
        &test_contract_metadata(),
    )
    .expect("close finalize")
    .expect("close result");

    let summaries = server
        .db
        .lock()
        .expect("db")
        .list_session_summaries(None, None, None, Some("RTH"), 5)
        .expect("summaries");
    assert_eq!(summaries[0].ib_extension_state, "UpOnly");
    assert_eq!(
        summaries[0].first_ib_extension_direction.as_deref(),
        Some("up")
    );
    assert_eq!(
        summaries[0].first_ib_extension_timestamp_ms,
        Some(first_extension_ts)
    );
}

/// Restart idempotency: calling `finalize_rth_close` again after the
/// session has been reset must be a no-op (returns None) and must not
/// clobber the persisted summary or write a duplicate row.
#[test]
fn finalize_rth_close_is_idempotent_on_replay() {
    let server = test_server();
    warm_rth_session(&server, &[21_000.0, 21_005.0, 21_010.0]);

    let boundary_ts = rth_ts(16, 0, 1);
    let _ = finalize_rth_close(
        &server.pipelines,
        &server.db,
        &[],
        None,
        None,
        None,
        boundary_ts,
        21_009.75,
        21_010.25,
        &test_contract_metadata(),
    )
    .expect("first close");

    let summary_v1 = {
        let db = server.db.lock().expect("db");
        db.list_session_summaries(None, None, None, Some("RTH"), 5)
            .expect("list")
    };
    assert_eq!(summary_v1.len(), 1);

    // Second call: pipeline has been reset, so finalize_rth_close should
    // return None rather than re-persisting an empty snapshot.
    let second = finalize_rth_close(
        &server.pipelines,
        &server.db,
        &[],
        None,
        None,
        None,
        boundary_ts,
        21_009.75,
        21_010.25,
        &test_contract_metadata(),
    )
    .expect("second finalize");
    assert!(second.is_none());

    let summary_v2 = {
        let db = server.db.lock().expect("db");
        db.list_session_summaries(None, None, None, Some("RTH"), 5)
            .expect("list")
    };
    assert_eq!(summary_v2.len(), 1);
    assert_eq!(summary_v1[0].session_date, summary_v2[0].session_date);
    assert!((summary_v1[0].high - summary_v2[0].high).abs() < 1e-9);
}

/// Cross-session inventory must see the just-closed RTH session via the
/// in-memory `prior_sessions()` list immediately after `finalize_rth_close`,
/// without waiting for a same-turn DB reload (which can race with the
/// `date < ?1` semantics in `load_prior_day_full`).
#[test]
fn finalize_rth_close_makes_session_inventory_visible_in_memory() {
    let server = test_server();
    warm_rth_session(&server, &[21_000.0, 21_010.0, 21_005.0, 21_015.0]);

    // Before close: session_inventory has no prior sessions.
    {
        let p = server.pipelines.lock().expect("pipelines");
        assert!(p.session_inventory.prior_sessions().is_empty());
    }

    let _ = finalize_rth_close(
        &server.pipelines,
        &server.db,
        &[],
        None,
        None,
        None,
        rth_ts(16, 0, 1),
        21_014.75,
        21_015.25,
        &test_contract_metadata(),
    )
    .expect("close finalize")
    .expect("close result");

    let p = server.pipelines.lock().expect("pipelines");
    let inv = p.session_inventory.prior_sessions();
    assert_eq!(
        inv.len(),
        1,
        "session_inventory should expose the just-closed RTH session"
    );
    assert!(
        inv[0].dnp > 0.0,
        "just-closed entry must carry a usable DNP"
    );
}

/// `persist_live_session_close` must commit `session_summaries` and
/// `prior_day_levels` in one transaction. This direct DB-level test
/// guards against the row-by-row regression where a crash between writes
/// would leave the next session reading half-updated levels.
#[test]
fn persist_live_session_close_writes_summary_and_prior_day_together() {
    let db = Database::open(":memory:").expect("db");
    let summary = summary_row("2026-03-05", "RTH", 21_010.0, 20_990.0, 21_000.0);
    db.persist_live_session_close(
        &summary,
        (
            21_020.0, 20_980.0, 21_000.0, 21_015.0, 20_995.0, 21_005.0, 21_010.0, 20_990.0,
            21_000.0,
        ),
        None,
    )
    .expect("atomic close");

    assert!(db
        .has_session_summary_for("2026-03-05", "RTH")
        .expect("summary check"));
    let row = db
        .load_prior_day_full("2026-03-06")
        .expect("prior load")
        .expect("prior row");
    assert!((row.0 - 21_020.0).abs() < 1e-9);
    assert!((row.1 - 20_980.0).abs() < 1e-9);
    assert_eq!(row.6, Some(21_010.0));
}

#[test]
fn prepare_for_new_session_scopes_contract_data_and_restores_inventory_order() {
    let server = test_server();
    {
        let db = server.db.lock().expect("db");
        db.save_prior_day_full_with_dnva_contract(
            "2026-03-04",
            22_000.0,
            21_900.0,
            21_950.0,
            21_980.0,
            21_920.0,
            21_950.0,
            Some(21_970.0),
            Some(21_930.0),
            Some(21_950.0),
            Some("NQ"),
            Some("NQM26"),
        )
        .expect("wrong-contract prior day");
        db.save_prior_day_full_with_dnva_contract(
            "2026-03-03",
            21_100.0,
            20_900.0,
            21_000.0,
            21_050.0,
            20_950.0,
            21_000.0,
            Some(21_025.0),
            Some(20_975.0),
            Some(21_000.0),
            Some("NQ"),
            Some("NQH26"),
        )
        .expect("matching-contract prior day");

        let mut older = summary_row("2026-03-03", "RTH", 21_025.0, 20_975.0, 21_000.0);
        older.contract_symbol = "NQH26".to_string();
        let mut newer = summary_row("2026-03-04", "RTH", 21_075.0, 21_000.0, 21_050.0);
        newer.contract_symbol = "NQH26".to_string();
        let mut wrong_contract = summary_row("2026-03-02", "RTH", 22_075.0, 22_000.0, 22_050.0);
        wrong_contract.contract_symbol = "NQM26".to_string();
        db.upsert_session_summary(&older).expect("older summary");
        db.upsert_session_summary(&newer).expect("newer summary");
        db.upsert_session_summary(&wrong_contract)
            .expect("wrong-contract summary");
    }

    prepare_for_new_session(
        &server.pipelines,
        &server.db,
        None,
        SessionType::Rth,
        DeltaSegment::Rth,
        rth_ts(9, 30, 0),
        &test_contract_metadata(),
    );

    let p = server.pipelines.lock().expect("pipelines");
    assert!((p.levels.prior_day_high - 21_100.0).abs() < 1e-9);
    assert_eq!(p.levels.prior_day_contract_symbol.as_deref(), Some("NQH26"));
    let inv = p.session_inventory.prior_sessions();
    assert_eq!(inv.len(), 2);
    assert!(
        (inv.last().expect("newest prior session").dnp - 21_050.0).abs() < 1e-9,
        "newest same-contract session should be the comparison anchor"
    );
}
