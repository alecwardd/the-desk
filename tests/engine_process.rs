//! Process-seam tests for SIL-M2a engine extract.
//!
//! Covers: engine survives MCP/client disconnect; kill-the-engine degrade +
//! recover; embedded-vs-socket coaching-path parity over fixture ticks.

use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use tempfile::NamedTempFile;
use the_desk_backend::engine::{
    coaching_parity_fingerprint, EngineClient, EngineHost, EngineSocketServer, FileProvider,
    SourceProviderKind,
};
use tokio::sync::watch;

const SCID_HEADER_SIZE: usize = 56;
const SCID_RECORD_SIZE: usize = 40;
const SC_TO_UNIX_EPOCH_US: i64 = 2_209_161_600_000_000;

fn write_scid(path: &std::path::Path, ticks: &[(f64, f64, f64)]) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .expect("open scid");
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
}

fn append_scid_tick(path: &std::path::Path, ts_ms: f64, price: f64, volume: f64) {
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("append scid");
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
    file.flush().unwrap();
}

fn rth_ticks() -> Vec<(f64, f64, f64)> {
    // 2024-01-02 10:00 ET
    let ts = 1_704_207_600_000.0;
    vec![
        (ts, 20_000.0, 1.0),
        (ts + 250.0, 20_000.25, 1.0),
        (ts + 500.0, 20_000.50, 2.0),
    ]
}

#[tokio::test]
async fn embedded_and_socket_paths_are_coaching_parity() {
    let tmp = NamedTempFile::new().unwrap();
    write_scid(tmp.path(), &rth_ticks());

    // Embedded path: in-process host publish.
    let mut provider_a = FileProvider::from_paths(tmp.path(), vec![], 1.0);
    let embedded = EngineHost::new(SourceProviderKind::File, "embedded");
    embedded.poll_once(&mut provider_a, 100).unwrap();
    let embedded_fp = coaching_parity_fingerprint(&embedded.load_published());

    // Socket path: second host + server; client reads published state.
    let mut provider_b = FileProvider::from_paths(tmp.path(), vec![], 1.0);
    let socket_host = Arc::new(EngineHost::new(SourceProviderKind::File, "socket"));
    socket_host.poll_once(&mut provider_b, 100).unwrap();
    let store = socket_host.published_store();

    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bind = probe.local_addr().unwrap().to_string();
    drop(probe);
    let (tx, rx) = watch::channel(false);
    let store_bg = store.clone();
    let server = EngineSocketServer::new(bind.clone());
    let serve = tokio::spawn(async move { server.serve(store_bg, rx).await });

    let client = EngineClient::new(bind);
    let mut published = client.get_published().await;
    for _ in 0..50 {
        if !published.degraded && published.generation > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        published = client.get_published().await;
    }
    let socket_fp = coaching_parity_fingerprint(&published);
    assert_eq!(embedded_fp, socket_fp);
    let _ = tx.send(true);
    let _ = serve.await;
}

#[tokio::test]
async fn kill_the_engine_degrades_and_recovers() {
    let tmp = NamedTempFile::new().unwrap();
    write_scid(tmp.path(), &rth_ticks());
    let mut provider = FileProvider::from_paths(tmp.path(), vec![], 1.0);
    let host = Arc::new(EngineHost::new(SourceProviderKind::File, "external"));
    host.poll_once(&mut provider, 100).unwrap();
    let store = host.published_store();

    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bind = probe.local_addr().unwrap().to_string();
    drop(probe);

    let (tx, rx) = watch::channel(false);
    let store_bg = store.clone();
    let server = EngineSocketServer::new(bind.clone());
    let serve = tokio::spawn(async move { server.serve(store_bg, rx).await });

    let client = EngineClient::new(bind.clone());
    for _ in 0..50 {
        if client.ping().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let live = client.get_published().await;
    assert!(!live.degraded);

    // Kill the engine socket.
    let _ = tx.send(true);
    let _ = serve.await;

    let degraded = client.get_published().await;
    assert!(
        degraded.degraded,
        "adapter must degrade cleanly — never silent stale success"
    );
    assert!(client.last_error().is_some());

    // Recover: new engine on same bind.
    let (tx2, rx2) = watch::channel(false);
    host.poll_once(&mut provider, 100).unwrap();
    let store_bg = store.clone();
    let server = EngineSocketServer::new(bind.clone());
    let serve2 = tokio::spawn(async move { server.serve(store_bg, rx2).await });
    let mut recovered = client.get_published().await;
    for _ in 0..50 {
        if !recovered.degraded {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        recovered = client.get_published().await;
    }
    assert!(
        !recovered.degraded,
        "adapter must recover when engine returns"
    );
    let _ = tx2.send(true);
    let _ = serve2.await;
}

#[test]
fn engine_binary_keeps_ingest_after_client_disconnect() {
    let exe = env!("CARGO_BIN_EXE_the-desk-engine");
    let dir = tempfile::tempdir().unwrap();
    let scid_path = dir.path().join("NQ.scid");
    let ts = 1_704_207_600_000.0;
    write_scid(&scid_path, &[(ts, 20_000.0, 1.0)]);

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = probe.local_addr().unwrap().to_string();
    drop(probe);

    let mut child = Command::new(exe)
        .arg("--bind")
        .arg(&bind)
        .arg("--scid")
        .arg(&scid_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the-desk-engine");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let client = EngineClient::new(bind.clone());
    let first_gen = rt.block_on(async {
        let mut last = 0u64;
        for _ in 0..100 {
            if let Ok(_pid) = client.ping().await {
                let pub_state = client.get_published().await;
                if pub_state.generation > 0 {
                    last = pub_state.generation;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        last
    });
    assert!(
        first_gen > 0,
        "engine must publish before client disconnect"
    );

    // Drop client (end of MCP/agent session simulation) — engine must keep running.
    drop(client);

    append_scid_tick(&scid_path, ts + 1_000.0, 20_001.0, 1.0);
    std::thread::sleep(Duration::from_millis(800));

    let client2 = EngineClient::new(bind);
    let second = rt.block_on(async {
        let mut gen = 0u64;
        for _ in 0..40 {
            let pub_state = client2.get_published().await;
            if !pub_state.degraded {
                gen = pub_state.generation;
                if gen > first_gen {
                    return gen;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        gen
    });

    let _ = child.kill();
    let _ = child.wait();
    assert!(
        second > first_gen,
        "engine must continue ingest after client disconnect (gen {first_gen} -> {second})"
    );
}

#[test]
fn trust_ceiling_engine_surface_has_no_order_authority() {
    // Structural: published/socket types are read-only; engine binary help text
    // asserts L3. Capability gate for kernel tools remains in catalog trust helpers.
    use the_desk_backend::catalog::{
        is_kernel_read_query_tool, kernel_read_query_capabilities, tool_name_implies_mutation,
    };
    let caps = kernel_read_query_capabilities();
    for name in ["get_state", "get_events"] {
        assert!(is_kernel_read_query_tool(name));
        let cap = caps.get(name).expect(name);
        assert!(!cap.order_authority);
        assert!(!cap.mutation_authority);
    }
    assert!(!tool_name_implies_mutation("get_feed_health"));
}
