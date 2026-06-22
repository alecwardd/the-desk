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
use the_desk_backend::db::SessionSummary;
#[allow(unused_imports)]
use the_desk_backend::db::{
    Database, HistoricalJobRun, RiskConfigRecord, SessionScopeFilter, SetupPerformanceSortBy,
    SetupRuntimeStateRecord, RESEARCH_DISTRIBUTION_METRICS,
};
#[allow(unused_imports)]
use the_desk_backend::depth::{DepthBook, DepthReader};
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
#[allow(unused_imports)]
use the_desk_backend::pipelines::PipelineEngine;
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
