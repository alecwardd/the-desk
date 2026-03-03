//! One-shot binary to initialize risk config and risk state.
//! Run: cargo run --bin the-desk-init-risk
//!
//! Uses the same database as the-desk-mcp and Tauri app (~/.the-desk/data.db).

use the_desk_backend::db::{Database, RiskConfigRecord};
use the_desk_backend::risk::RiskState;

fn data_dir() -> std::path::PathBuf {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    let dir = std::path::PathBuf::from(home).join(".the-desk");
    std::fs::create_dir_all(&dir).ok();
    dir
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = data_dir().join("data.db");
    let db = Database::open(&db_path.to_string_lossy())?;

    // 1. Save risk config with sensible defaults
    let config = RiskConfigRecord {
        r_value_points: 50.0,
        r_value_dollars: 250.0,
        max_daily_loss_r: 3.0,
        max_consecutive_losses: 3,
        max_trades_per_session: Some(8),
        no_trade_zones: Vec::new(),
        max_daily_loss_dollars: Some(750.0),
    };
    db.save_risk_config(&config)?;
    println!(
        "Saved risk config: R=50pts/$250, max 3R daily, 3-loss circuit breaker, 8 trades/session"
    );

    // 2. Initialize risk state
    let state = RiskState {
        daily_pnl_r: 0.0,
        trade_count: 0,
        consecutive_losses: 0,
        consecutive_wins: 0,
        drawdown_r: 0.0,
        max_daily_loss_r: config.max_daily_loss_r,
        at_limit: false,
    };
    db.save_risk_state(&state)?;
    println!("Initialized risk state: 0 P&L, 0 trades, no streaks");

    println!("Done. Run get_risk_state and get_account_state via MCP to verify.");
    Ok(())
}
