//! `the-desk-engine` — headless SIL engine host (SIL-M2a).
//!
//! Owns ingest (SourceProvider / FileProvider), pipelines, and event detection
//! so intelligence survives MCP/agent disconnect. Publishes lock-free state on a
//! read-only localhost TCP socket for the MCP adapter.
//!
//! Lifecycle: launch on Sierra hours (Task Scheduler) with Globex overnight
//! coverage — the engine runs whenever Sierra records. Trust Ceiling stays L3.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use the_desk_backend::engine::{
    load_engine_bind_addr, EngineHost, EngineSocketServer, FileProvider, SourceProvider,
    SourceProviderKind, ENGINE_DEFAULT_BIND,
};
use the_desk_backend::feed::load_feed_config;
use the_desk_backend::observability::{init_logging, load_logging_config};
use tokio::sync::watch;

fn parse_bind_arg() -> String {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--bind" {
            if let Some(addr) = args.next() {
                return addr;
            }
        } else if let Some(addr) = arg.strip_prefix("--bind=") {
            return addr.to_string();
        }
    }
    load_engine_bind_addr()
}

fn parse_scid_override() -> Option<PathBuf> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--scid" {
            return args.next().map(PathBuf::from);
        }
        if let Some(path) = arg.strip_prefix("--scid=") {
            return Some(PathBuf::from(path));
        }
    }
    None
}

fn print_help() {
    eprintln!(
        "the-desk-engine — headless Desk intelligence host (SIL-M2a)\n\n\
         Usage:\n\
           the-desk-engine [--bind ADDR] [--scid PATH]\n\n\
         Defaults:\n\
           --bind {ENGINE_DEFAULT_BIND} (or [sil].engine_bind in config.toml)\n\
           SCID path from feed config / FileProvider\n\n\
         Ops: register via Task Scheduler on Sierra hours; keep running through\n\
         Globex overnight whenever Sierra Chart is recording. MCP adapters connect\n\
         read-only; closing an agent session must not stop this process.\n\
         Trust Ceiling remains L3 — this binary never places orders."
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|a| a == "--help" || a == "-h") {
        print_help();
        return Ok(());
    }

    let logging_config = load_logging_config();
    let _logging_runtime = init_logging(&logging_config).unwrap_or_else(|_| {
        init_logging(&the_desk_backend::observability::LoggingConfig::stderr_only())
            .unwrap_or_else(|_| the_desk_backend::observability::LoggingRuntime::disabled())
    });

    let bind = parse_bind_arg();
    let feed = load_feed_config();
    let mut provider: Box<dyn SourceProvider> = if let Some(scid) = parse_scid_override() {
        Box::new(FileProvider::from_paths(scid, vec![], feed.price_scale))
    } else {
        Box::new(FileProvider::from_feed_config(&feed))
    };

    let host = Arc::new(EngineHost::new(SourceProviderKind::File, "external"));
    let store = host.published_store();

    // Initial publish so clients see health even before first tick.
    host.publish(None, &provider.health());

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let stop_flag = Arc::new(AtomicBool::new(false));
    {
        let stop_flag = Arc::clone(&stop_flag);
        let shutdown_tx = shutdown_tx.clone();
        ctrlc_shim(stop_flag, shutdown_tx);
    }

    let server = EngineSocketServer::new(bind.clone());
    let server_handle = tokio::spawn(async move { server.serve(store, shutdown_rx).await });

    let poll_ms = feed.flush_poll_ms.max(250);
    let max_ticks = feed.max_ticks_per_poll.max(1);
    let host_bg = Arc::clone(&host);
    let stop_bg = Arc::clone(&stop_flag);

    tracing::info!(
        %bind,
        pid = std::process::id(),
        "the-desk-engine.started"
    );

    while !stop_bg.load(Ordering::Acquire) {
        match host_bg.poll_once(provider.as_mut(), max_ticks) {
            Ok(n) if n > 0 => {
                // Drain without sleeping while catching up.
                continue;
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(error = %err, "the-desk-engine.poll_error");
                host_bg.publish(None, &provider.health());
            }
        }
        tokio::time::sleep(Duration::from_millis(poll_ms)).await;
    }

    host.mark_stopped();
    let _ = shutdown_tx.send(true);
    let _ = server_handle.await;
    Ok(())
}

fn ctrlc_shim(stop_flag: Arc<AtomicBool>, shutdown_tx: watch::Sender<bool>) {
    // Avoid pulling signal-hook; use a simple background watcher on Unix via
    // tokio ctrl_c when available.
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            stop_flag.store(true, Ordering::Release);
            let _ = shutdown_tx.send(true);
        }
    });
}
