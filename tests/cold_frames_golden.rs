//! SIL-M3d: cold session-partitioned Journal Frames rebuild from `.scid`
//! within the same strict/derived golden tolerances as M3a, and research
//! operators pointed at cold dumps keep the M3c contracts (same tools, L0,
//! window/session/N/reliability).

use std::io::Write;

use serde_json::{json, Value};
use tempfile::NamedTempFile;
use the_desk_backend::db::Database;
use the_desk_backend::engine::{
    ColdFrameStore, FileProvider, JournalFrameRead, MarketRouter, RouterRoot, SourceProvider,
    SourceProviderKind,
};
use the_desk_backend::research::query_kernel::{
    query_episodes_with, query_raw, query_raw_with, query_series, query_series_with,
    QueryEpisodesRequest, QueryRawRequest, QuerySeriesRequest, QueryWindow, FIELD_LAST_PRICE,
};

const SCID_HEADER_SIZE: usize = 56;
const SCID_RECORD_SIZE: usize = 40;
const SC_TO_UNIX_EPOCH_US: i64 = 2_209_161_600_000_000;
/// 2024-01-02 10:00 ET (RTH).
const RTH_TS: f64 = 1_704_207_600_000.0;

fn write_scid(ticks: &[(f64, f64, f64)]) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    let mut header = vec![0u8; SCID_HEADER_SIZE];
    header[0..4].copy_from_slice(b"SCID");
    header[4..8].copy_from_slice(&(SCID_HEADER_SIZE as u32).to_le_bytes());
    header[8..12].copy_from_slice(&(SCID_RECORD_SIZE as u32).to_le_bytes());
    file.write_all(&header).unwrap();
    for &(ts_ms, price, volume) in ticks {
        let mut rec = [0u8; SCID_RECORD_SIZE];
        let sc_us = (ts_ms * 1_000.0) as i64 + SC_TO_UNIX_EPOCH_US;
        rec[0..8].copy_from_slice(&sc_us.to_le_bytes());
        let ask = (price + 0.25) as f32;
        let bid = (price - 0.25) as f32;
        rec[12..16].copy_from_slice(&ask.to_le_bytes());
        rec[16..20].copy_from_slice(&bid.to_le_bytes());
        rec[20..24].copy_from_slice(&(price as f32).to_le_bytes());
        rec[28..32].copy_from_slice(&(volume as u32).to_le_bytes());
        rec[36..40].copy_from_slice(&1u32.to_le_bytes());
        file.write_all(&rec).unwrap();
    }
    file.flush().unwrap();
    file
}

fn fixture_ticks(base_price: f64, offset_ms: f64) -> Vec<(f64, f64, f64)> {
    (0..13)
        .map(|i| {
            (
                RTH_TS + offset_ms + (i as f64) * 250.0,
                base_price + i as f64 * 0.25,
                1.0,
            )
        })
        .collect()
}

fn frame_fingerprint(payload: &Value) -> Value {
    json!({
        "lastPrice": payload.get("lastPrice"),
        "rootSymbol": payload.get("rootSymbol"),
        "sessionType": payload.get("sessionType"),
        "sessionHigh": payload.get("sessionHigh"),
        "sessionLow": payload.get("sessionLow"),
    })
}

fn persist_from_scid(
    nq: &NamedTempFile,
    es: &NamedTempFile,
    cold: Option<&ColdFrameStore>,
) -> (Database, Value) {
    let db = Database::open(":memory:").expect("db");
    let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "cold-golden");
    if let Some(store) = cold {
        router.set_cold_frame_store(store.clone());
    }
    let mut providers = std::collections::BTreeMap::new();
    providers.insert(
        RouterRoot::Nq,
        Box::new(FileProvider::from_paths(nq.path(), vec![], 1.0)) as Box<dyn SourceProvider>,
    );
    providers.insert(
        RouterRoot::Es,
        Box::new(FileProvider::from_paths(es.path(), vec![], 1.0)) as Box<dyn SourceProvider>,
    );
    router.poll_once(&mut providers, 10_000).expect("poll");
    router.persist_journal(&db).expect("persist");
    let frames = db.list_journal_frames().expect("list");
    let golden = json!({
        "frameCount": frames.len(),
        "seconds": frames.iter().map(|f| f.frame_second).collect::<std::collections::BTreeSet<_>>(),
        "nq": frames.iter().filter(|f| f.root_symbol == "NQ").map(|f| json!({
            "frameSecond": f.frame_second,
            "clockMs": f.clock_ms,
            "state": frame_fingerprint(&f.payload),
        })).collect::<Vec<_>>(),
        "es": frames.iter().filter(|f| f.root_symbol == "ES").map(|f| json!({
            "frameSecond": f.frame_second,
            "clockMs": f.clock_ms,
            "state": frame_fingerprint(&f.payload),
        })).collect::<Vec<_>>(),
    });
    (db, golden)
}

fn series_req(start: f64, end: f64) -> QuerySeriesRequest {
    QuerySeriesRequest {
        window: QueryWindow {
            start_ms: Some(start),
            end_ms: Some(end),
            session_type: Some("RTH".into()),
            symbols: Some(vec!["NQ".into(), "ES".into()]),
        },
        fields: vec![FIELD_LAST_PRICE.into()],
    }
}

#[test]
fn cold_frames_rebuild_from_scid_match_hot_golden_strict_fields() {
    let nq = write_scid(&fixture_ticks(20_000.0, 0.0));
    let es = write_scid(&fixture_ticks(5_000.0, 10.0));
    let cold_a = tempfile::tempdir().expect("cold a");
    let cold_b = tempfile::tempdir().expect("cold b");
    let store_a = ColdFrameStore::new(cold_a.path());
    let store_b = ColdFrameStore::new(cold_b.path());

    let (db_a, hot_a) = persist_from_scid(&nq, &es, Some(&store_a));
    let (_db_b, hot_b) = persist_from_scid(&nq, &es, Some(&store_b));

    assert_eq!(hot_a["frameCount"], 8);
    assert_eq!(
        hot_a, hot_b,
        "hot Journal Frames must rebuild identically from the same .scid"
    );

    let cold_rows_a = store_a.list_all().expect("cold a");
    let cold_rows_b = store_b.list_all().expect("cold b");
    let hot_rows = db_a.list_journal_frames().expect("hot list");
    assert_eq!(cold_rows_a.len(), hot_rows.len());
    assert_eq!(cold_rows_a.len(), cold_rows_b.len());
    for (cold, hot) in cold_rows_a.iter().zip(hot_rows.iter()) {
        assert_eq!(cold.frame_second, hot.frame_second);
        assert_eq!(cold.root_symbol, hot.root_symbol);
        assert_eq!(cold.session_type, hot.session_type);
        assert_eq!(cold.clock_ms, hot.clock_ms);
        assert_eq!(
            frame_fingerprint(&cold.payload),
            frame_fingerprint(&hot.payload)
        );
    }
    for (a, b) in cold_rows_a.iter().zip(cold_rows_b.iter()) {
        assert_eq!(frame_fingerprint(&a.payload), frame_fingerprint(&b.payload));
        assert_eq!(a.session_type, b.session_type);
        assert_eq!(a.root_symbol, b.root_symbol);
    }

    let parts = store_a.list_all().expect("parts via rows");
    let sessions: std::collections::BTreeSet<_> =
        parts.iter().map(|f| f.session_type.as_str()).collect();
    assert_eq!(sessions, ["RTH"].into_iter().collect());
    assert!(parts.iter().any(|f| f.root_symbol == "NQ"));
    assert!(parts.iter().any(|f| f.root_symbol == "ES"));
    assert!(
        cold_a
            .path()
            .join("trading_day=2024-01-02")
            .join("session_type=RTH")
            .join("root=NQ")
            .join("frames.jsonl.zst")
            .exists(),
        "cold frames must be hive-partitioned by trading_day/session_type/root"
    );
    assert!(
        cold_a.path().join("_format.json").exists(),
        "cold store root must declare desk-journal-frames-v1"
    );
}

#[test]
fn query_operators_on_cold_match_hot_n_reliability_and_l0() {
    let nq = write_scid(&fixture_ticks(20_000.0, 0.0));
    let es = write_scid(&fixture_ticks(5_000.0, 10.0));
    let cold_dir = tempfile::tempdir().expect("cold");
    let store = ColdFrameStore::new(cold_dir.path());
    let (db, _) = persist_from_scid(&nq, &es, Some(&store));

    let req = series_req(RTH_TS, RTH_TS + 4_000.0);
    let hot = query_series(&db, &req).expect("hot series");
    let cold = query_series_with(JournalFrameRead::Cold(&store), &req).expect("cold series");

    assert_eq!(hot.meta.n, cold.meta.n);
    assert_eq!(hot.meta.reliability_tier, cold.meta.reliability_tier);
    assert_eq!(hot.meta.trust_level, cold.meta.trust_level);
    assert!(!hot.meta.mutation_authority);
    assert!(!cold.meta.mutation_authority);
    assert!(!hot.meta.order_authority);
    assert!(!cold.meta.order_authority);
    assert_eq!(hot.points.len(), cold.points.len());
    assert_eq!(hot.meta.n, 8);
    for (h, c) in hot.points.iter().zip(cold.points.iter()) {
        assert_eq!(h.frame_second, c.frame_second);
        assert_eq!(h.root_symbol, c.root_symbol);
        assert_eq!(h.session_type, c.session_type);
        assert_eq!(
            h.values.get(FIELD_LAST_PRICE),
            c.values.get(FIELD_LAST_PRICE)
        );
    }
    assert!(
        cold.meta.notes.iter().any(|n| n.contains("cold")),
        "cold path must note the store without changing L0 envelope fields"
    );
    assert!(
        !hot.meta.notes.iter().any(|n| n.contains("cold")),
        "default hot path must not advertise a cold store"
    );

    let raw_req = QueryRawRequest {
        window: QueryWindow {
            start_ms: Some(RTH_TS),
            end_ms: Some(RTH_TS + 4_000.0),
            session_type: Some("RTH".into()),
            symbols: None,
        },
        source: "journal_frames".into(),
        limit: Some(100),
    };
    let hot_raw = query_raw(&db, &raw_req).expect("hot raw");
    let cold_raw = query_raw_with(&db, JournalFrameRead::Cold(&store), &raw_req).expect("cold raw");
    assert_eq!(hot_raw.meta.n, cold_raw.meta.n);
    assert_eq!(hot_raw.rows.len(), cold_raw.rows.len());
    assert_eq!(hot_raw.meta.trust_level, cold_raw.meta.trust_level);

    let err = query_raw_with(
        &db,
        JournalFrameRead::Cold(&store),
        &QueryRawRequest {
            window: raw_req.window.clone(),
            source: "events".into(),
            limit: Some(10),
        },
    )
    .expect_err("events are not in the cold frame store");
    assert!(
        err.to_string().contains("journal_frames"),
        "fail closed: {err}"
    );
}

#[test]
fn cold_query_refuses_mixed_rth_globex_without_session_type() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = ColdFrameStore::new(dir.path());
    let rth = RTH_TS + 6.0 * 3_600_000.0; // 16:00 ET — still RTH
    let globex = RTH_TS + 8.0 * 3_600_000.0; // 18:00 ET — Globex (session roll)
    let second_rth = (rth / 1000.0).floor() as i64;
    let second_globex = (globex / 1000.0).floor() as i64;
    store
        .upsert_frames(&[
            the_desk_backend::db::JournalFrameRecord {
                clock_ms: rth,
                frame_second: second_rth,
                root_symbol: "NQ".into(),
                session_type: "RTH".into(),
                session_segment: "None".into(),
                trading_day: "2024-01-02".into(),
                payload: json!({ "lastPrice": 20000.0, "rootSymbol": "NQ", "sessionType": "RTH" }),
            },
            the_desk_backend::db::JournalFrameRecord {
                clock_ms: globex,
                frame_second: second_globex,
                root_symbol: "NQ".into(),
                session_type: "Globex".into(),
                session_segment: "None".into(),
                trading_day: "2024-01-03".into(),
                payload: json!({ "lastPrice": 20010.0, "rootSymbol": "NQ", "sessionType": "Globex" }),
            },
        ])
        .expect("upsert mixed");

    let err = query_series_with(
        JournalFrameRead::Cold(&store),
        &QuerySeriesRequest {
            window: QueryWindow {
                start_ms: Some(rth),
                end_ms: Some(globex + 1_000.0),
                session_type: None,
                symbols: Some(vec!["NQ".into()]),
            },
            fields: vec![FIELD_LAST_PRICE.into()],
        },
    )
    .expect_err("mixed session");
    assert!(
        err.to_string().contains("RTH") || err.to_string().contains("Globex"),
        "{err}"
    );
}

#[test]
fn query_episodes_on_cold_frames_keeps_flagship_n_and_l0() {
    use the_desk_backend::catalog::{
        accept_levels_only_entry, DerivedLevels, PositioningEntryInput, LEVELS_ONLY_RECORD_KIND,
    };
    use the_desk_backend::research::query_kernel::flagship_episode_predicates;

    let db = Database::open(":memory:").expect("db");
    let dir = tempfile::tempdir().expect("cold");
    let store = ColdFrameStore::new(dir.path());
    let clock = RTH_TS;
    let second = (clock / 1000.0).floor() as i64;
    let frames = vec![
        the_desk_backend::db::JournalFrameRecord {
            clock_ms: clock,
            frame_second: second,
            root_symbol: "ES".into(),
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2024-01-02".into(),
            payload: json!({
                "lastPrice": 5750.0,
                "sessionDelta": -500.0,
                "poorLow": true,
                "domSummary": { "bidReplenishing": true },
                "rootSymbol": "ES",
                "sessionType": "RTH",
            }),
        },
        the_desk_backend::db::JournalFrameRecord {
            clock_ms: clock,
            frame_second: second,
            root_symbol: "NQ".into(),
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2024-01-02".into(),
            payload: json!({
                "lastPrice": 20000.0,
                "sessionDelta": 50.0,
                "rootSymbol": "NQ",
                "sessionType": "RTH",
            }),
        },
    ];
    db.insert_journal_frames(&frames).expect("hot");
    store.upsert_frames(&frames).expect("cold");
    let record = accept_levels_only_entry(PositioningEntryInput {
        id: Some("pos-cold".into()),
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
        5740.0,
        1.0,
        5739.75,
        5740.25,
        false,
        "2024-01-02",
        Some("ES"),
        Some("ESH24.CME"),
    )
    .expect("tick");

    let req = QueryEpisodesRequest {
        window: QueryWindow {
            start_ms: Some(clock),
            end_ms: Some(clock + 4_000.0),
            session_type: Some("RTH".into()),
            symbols: Some(vec!["NQ".into(), "ES".into()]),
        },
        predicates: flagship_episode_predicates(),
        forward_direction: Some("short".into()),
    };
    let hot = the_desk_backend::research::query_kernel::query_episodes(&db, &req).expect("hot");
    let cold = query_episodes_with(&db, JournalFrameRead::Cold(&store), &req).expect("cold");
    assert_eq!(hot.meta.n, cold.meta.n);
    assert_eq!(hot.meta.n, 1);
    assert_eq!(hot.meta.trust_level, cold.meta.trust_level);
    assert!(!cold.meta.mutation_authority);
    assert!(!cold.meta.order_authority);
    assert_eq!(hot.matches.len(), cold.matches.len());
    assert!(cold.matches[0].journal_backed);
}
