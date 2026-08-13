//! Golden replay: NQ+ES Journal Frames rebuild from `.scid` within MarketRouter
//! strict field equality (lastPrice / rootSymbol / sessionType / clock).
//!
//! 1 Hz frames are captured on the shared MarketRouter clock. 250 ms publishes
//! are not persisted. Capsules live in a separate table (see `capsules_golden`).

use std::io::Write;

use serde_json::{json, Value};
use tempfile::NamedTempFile;
use the_desk_backend::db::Database;
use the_desk_backend::engine::{
    FileProvider, MarketRouter, RouterRoot, SourceProvider, SourceProviderKind,
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
    // 250 ms cadence across 3 seconds — must collapse to 4 Journal Frame seconds.
    // `offset_ms` staggers ES vs NQ on the shared clock without leaving the second.
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

fn persist_from_scid(nq: &NamedTempFile, es: &NamedTempFile) -> (Database, Value) {
    let db = Database::open(":memory:").expect("db");
    let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "golden");
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
        "clockMs": frames.iter().map(|f| f.clock_ms).fold(None, |acc: Option<f64>, c| {
            Some(acc.map(|a| a.max(c)).unwrap_or(c))
        }),
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

#[test]
fn golden_journal_frames_rebuild_from_scid_within_strict_fields() {
    let nq = write_scid(&fixture_ticks(20_000.0, 0.0));
    let es = write_scid(&fixture_ticks(5_000.0, 10.0));
    let (db_a, a) = persist_from_scid(&nq, &es);
    let (_db_b, b) = persist_from_scid(&nq, &es);

    assert_eq!(a["frameCount"], 8);
    assert_eq!(a["seconds"].as_array().map(|s| s.len()), Some(4));
    assert_eq!(
        a, b,
        "Journal Frames must rebuild identically from the same .scid"
    );

    // Staggered NQ/ES prints in the same second share the first pinned clock.
    let nq_frames = a["nq"].as_array().expect("nq");
    let es_frames = a["es"].as_array().expect("es");
    assert_eq!(nq_frames.len(), es_frames.len());
    for (nq_f, es_f) in nq_frames.iter().zip(es_frames.iter()) {
        assert_eq!(nq_f["frameSecond"], es_f["frameSecond"]);
        assert_eq!(
            nq_f["clockMs"], es_f["clockMs"],
            "NQ and ES frames in the same second must share the pinned MarketRouter clock"
        );
    }

    // 250 ms fixture must not persist 13 frames per symbol.
    assert_eq!(db_a.count_journal_frames().expect("count"), 8);

    let as_of = db_a
        .get_journal_frames_as_of(RTH_TS + 3_000.0)
        .expect("as_of")
        .expect("present");
    assert!(as_of.by_root.contains_key("NQ"));
    assert!(as_of.by_root.contains_key("ES"));
    assert_eq!(as_of.by_root["NQ"]["rootSymbol"], "NQ");
    assert_eq!(as_of.by_root["ES"]["rootSymbol"], "ES");
}
