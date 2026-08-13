//! `the-desk-engine` — headless SIL engine host (SIL-M2a + MarketRouter v0).
//!
//! Owns ingest (SourceProvider / FileProvider), pipelines, and event detection
//! so intelligence survives MCP/agent disconnect. **MarketRouter** runs NQ and
//! ES concurrently on one clock. Publishes lock-free state on a read-only
//! localhost TCP socket for the MCP adapter.
//!
//! Lifecycle: launch on Sierra hours (Task Scheduler) with Globex overnight
//! coverage — the engine runs whenever Sierra records. Trust Ceiling stays L3.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use the_desk_backend::engine::{
    load_engine_bind_addr, EngineSocketServer, FileProvider, MarketRouter, RouterRoot,
    SourceProvider, SourceProviderKind, ENGINE_DEFAULT_BIND,
};
use the_desk_backend::feed::{load_feed_config, resolve_contract_metadata};
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

fn parse_path_flag(flag: &str) -> Option<PathBuf> {
    let eq = format!("{flag}=");
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == flag {
            return args.next().map(PathBuf::from);
        }
        if let Some(path) = arg.strip_prefix(&eq) {
            return Some(PathBuf::from(path));
        }
    }
    None
}

fn print_help() {
    eprintln!(
        "the-desk-engine — headless Desk intelligence host (SIL-M2b MarketRouter)\n\n\
         Usage:\n\
           the-desk-engine [--bind ADDR] [--scid PATH] [--scid-es PATH]\n\n\
         Defaults:\n\
           --bind {ENGINE_DEFAULT_BIND} (or [sil].engine_bind in config.toml)\n\
           NQ/ES SCID paths from feed config / FileProvider (MarketRouter v0)\n\n\
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
    let primary = the_desk_backend::engine::primary_root_from_config(&feed.base_symbol)?;

    let mut providers: BTreeMap<RouterRoot, Box<dyn SourceProvider>> = BTreeMap::new();
    let nq_provider: Box<dyn SourceProvider> = if let Some(scid) = parse_path_flag("--scid") {
        Box::new(FileProvider::from_paths(scid, vec![], feed.price_scale))
    } else {
        Box::new(FileProvider::from_feed_config_for_root(
            &feed,
            RouterRoot::Nq,
        ))
    };
    providers.insert(RouterRoot::Nq, nq_provider);

    let es_provider: Box<dyn SourceProvider> = if let Some(scid) = parse_path_flag("--scid-es") {
        Box::new(FileProvider::from_paths(scid, vec![], feed.price_scale))
    } else {
        Box::new(FileProvider::from_feed_config_for_root(
            &feed,
            RouterRoot::Es,
        ))
    };
    providers.insert(RouterRoot::Es, es_provider);

    let router = Arc::new(MarketRouter::new(
        primary,
        SourceProviderKind::File,
        "external",
    ));
    router.set_contract_metadata(
        RouterRoot::Nq,
        resolve_contract_metadata(&{
            let mut cfg = feed.clone();
            cfg.base_symbol = "NQ".into();
            cfg.symbol = "NQ".into();
            cfg
        }),
    );
    router.set_contract_metadata(
        RouterRoot::Es,
        resolve_contract_metadata(&{
            let mut cfg = feed.clone();
            cfg.base_symbol = "ES".into();
            cfg.symbol = "ES".into();
            cfg
        }),
    );
    let store = router.published_store();

    // Initial publish so clients see health even before first tick.
    let mut boot_health = BTreeMap::new();
    for (root, provider) in providers.iter() {
        boot_health.insert(*root, provider.health());
    }
    router.publish(&boot_health);

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
    let router_bg = Arc::clone(&router);
    let stop_bg = Arc::clone(&stop_flag);

    tracing::info!(
        %bind,
        pid = std::process::id(),
        primary = primary.as_str(),
        "the-desk-engine.started"
    );

    while !stop_bg.load(Ordering::Acquire) {
        match router_bg.poll_once(&mut providers, max_ticks) {
            Ok(n) if n > 0 => {
                continue;
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(error = %err, "the-desk-engine.poll_error");
                let mut health = BTreeMap::new();
                for (root, provider) in providers.iter() {
                    health.insert(*root, provider.health());
                }
                router_bg.publish(&health);
            }
        }
        tokio::time::sleep(Duration::from_millis(poll_ms)).await;
    }

    router.mark_stopped();
    let _ = shutdown_tx.send(true);
    let _ = server_handle.await;
    Ok(())
}

fn ctrlc_shim(stop_flag: Arc<AtomicBool>, shutdown_tx: watch::Sender<bool>) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            stop_flag.store(true, Ordering::Release);
            let _ = shutdown_tx.send(true);
        }
    });
}
