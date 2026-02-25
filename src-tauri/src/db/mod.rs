use crate::risk::RiskState;
use crate::rules::SetupDefinition;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors returned by database operations.
#[derive(Debug, Error)]
pub enum DbError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Input payload for inserting a new session event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionEventInput {
    /// Category of the event (e.g. "trade_entry", "session_start").
    pub event_type: String,
    /// Optional setup ID this event is associated with.
    pub setup_id: Option<String>,
    /// Arbitrary JSON data attached to the event.
    pub data: serde_json::Value,
}

/// A persisted session event with its database row ID.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionEventRecord {
    /// Auto-incremented row ID.
    pub id: i64,
    /// Category of the event (e.g. "trade_entry", "session_start").
    pub event_type: String,
    /// Optional setup ID this event is associated with.
    pub setup_id: Option<String>,
    /// Arbitrary JSON data attached to the event.
    pub data: serde_json::Value,
}

/// SQLite-backed local data store for setups, events, risk state, and prior-day levels.
pub struct Database {
    conn: Connection,
}

impl Database {
    /// Open database and initialize schema.
    pub fn open(path: &str) -> Result<Self, DbError> {
        let conn = Connection::open(path)?;
        let db = Self { conn };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS setups (
              id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              description TEXT NOT NULL DEFAULT '',
              active INTEGER NOT NULL,
              conditions TEXT NOT NULL DEFAULT '[]',
              min_delta REAL NOT NULL DEFAULT 0.0,
              require_above_vwap INTEGER NOT NULL DEFAULT 0,
              duplicate_suppression_ms INTEGER NOT NULL DEFAULT 2000
            );
            CREATE TABLE IF NOT EXISTS session_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              event_type TEXT NOT NULL,
              setup_id TEXT NULL,
              data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS risk_state (
              singleton INTEGER PRIMARY KEY,
              daily_pnl_r REAL NOT NULL,
              trade_count INTEGER NOT NULL,
              consecutive_losses INTEGER NOT NULL,
              drawdown_r REAL NOT NULL,
              max_daily_loss_r REAL NOT NULL,
              at_limit INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS prior_day_levels (
              date TEXT PRIMARY KEY,
              high REAL NOT NULL,
              low REAL NOT NULL,
              close REAL NOT NULL
            );
            ",
        )?;
        Ok(())
    }

    /// Insert or update setup.
    pub fn upsert_setup(&self, setup: &SetupDefinition) -> Result<(), DbError> {
        let conditions_json =
            serde_json::to_string(&setup.conditions).unwrap_or_else(|_| "[]".to_string());
        self.conn.execute(
            "
            INSERT INTO setups (id, name, description, active, conditions, min_delta, require_above_vwap, duplicate_suppression_ms)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(id) DO UPDATE SET
              name=excluded.name,
              description=excluded.description,
              active=excluded.active,
              conditions=excluded.conditions,
              min_delta=excluded.min_delta,
              require_above_vwap=excluded.require_above_vwap,
              duplicate_suppression_ms=excluded.duplicate_suppression_ms
            ",
            params![
                setup.id,
                setup.name,
                setup.description,
                i64::from(setup.active),
                conditions_json,
                setup.min_delta,
                i64::from(setup.require_above_vwap),
                setup.duplicate_suppression_ms as i64
            ],
        )?;
        Ok(())
    }

    /// List setups.
    pub fn list_setups(&self) -> Result<Vec<SetupDefinition>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, description, active, conditions, min_delta, require_above_vwap, duplicate_suppression_ms FROM setups",
        )?;
        let rows = stmt.query_map([], |row| {
            let conditions_str: String = row.get(4)?;
            let conditions: Vec<String> = serde_json::from_str(&conditions_str).unwrap_or_default();
            Ok(SetupDefinition {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                active: row.get::<_, i64>(3)? == 1,
                conditions,
                min_delta: row.get(5)?,
                require_above_vwap: row.get::<_, i64>(6)? == 1,
                duplicate_suppression_ms: row.get::<_, i64>(7)? as u64,
            })
        })?;
        let setups = rows.collect::<Result<Vec<_>, _>>()?;
        Ok(setups)
    }

    /// Add event row.
    pub fn add_event(&self, event: &SessionEventInput) -> Result<(), DbError> {
        let payload = serde_json::to_string(&event.data)?;
        self.conn.execute(
            "INSERT INTO session_events (event_type, setup_id, data) VALUES (?1, ?2, ?3)",
            params![event.event_type, event.setup_id, payload],
        )?;
        Ok(())
    }

    /// Persist risk state singleton.
    pub fn save_risk_state(&self, state: &RiskState) -> Result<(), DbError> {
        self.conn.execute(
            "
            INSERT INTO risk_state (singleton, daily_pnl_r, trade_count, consecutive_losses, drawdown_r, max_daily_loss_r, at_limit)
            VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(singleton) DO UPDATE SET
              daily_pnl_r=excluded.daily_pnl_r,
              trade_count=excluded.trade_count,
              consecutive_losses=excluded.consecutive_losses,
              drawdown_r=excluded.drawdown_r,
              max_daily_loss_r=excluded.max_daily_loss_r,
              at_limit=excluded.at_limit
            ",
            params![
                state.daily_pnl_r,
                state.trade_count as i64,
                state.consecutive_losses as i64,
                state.drawdown_r,
                state.max_daily_loss_r,
                i64::from(state.at_limit)
            ],
        )?;
        Ok(())
    }

    /// Load current risk state snapshot if present.
    pub fn load_risk_state(&self) -> Result<Option<RiskState>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT daily_pnl_r, trade_count, consecutive_losses, drawdown_r, max_daily_loss_r, at_limit
             FROM risk_state WHERE singleton = 1",
        )?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let state = RiskState {
                daily_pnl_r: row.get(0)?,
                trade_count: row.get::<_, i64>(1)? as usize,
                consecutive_losses: row.get::<_, i64>(2)? as usize,
                drawdown_r: row.get(3)?,
                max_daily_loss_r: row.get(4)?,
                at_limit: row.get::<_, i64>(5)? == 1,
            };
            Ok(Some(state))
        } else {
            Ok(None)
        }
    }

    /// Save prior-day levels for a given date (YYYY-MM-DD).
    pub fn save_prior_day(
        &self,
        date: &str,
        high: f64,
        low: f64,
        close: f64,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO prior_day_levels (date, high, low, close)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(date) DO UPDATE SET
               high=excluded.high, low=excluded.low, close=excluded.close",
            params![date, high, low, close],
        )?;
        Ok(())
    }

    /// Load the most recent prior-day levels (latest date before the given date).
    pub fn load_prior_day(&self, before_date: &str) -> Result<Option<(f64, f64, f64)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT high, low, close FROM prior_day_levels
             WHERE date < ?1
             ORDER BY date DESC LIMIT 1",
        )?;
        let mut rows = stmt.query(params![before_date])?;
        if let Some(row) = rows.next()? {
            Ok(Some((row.get(0)?, row.get(1)?, row.get(2)?)))
        } else {
            Ok(None)
        }
    }

    /// List most recent session events.
    pub fn list_session_events(&self, limit: usize) -> Result<Vec<SessionEventRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, event_type, setup_id, data
             FROM session_events
             ORDER BY id DESC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |row| {
            let data_text: String = row.get(3)?;
            let data = serde_json::from_str::<serde_json::Value>(&data_text)
                .unwrap_or_else(|_| serde_json::json!({}));
            Ok(SessionEventRecord {
                id: row.get(0)?,
                event_type: row.get(1)?,
                setup_id: row.get(2)?,
                data,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn inserts_and_lists_setups() {
        let file = NamedTempFile::new().expect("temp");
        let db = Database::open(file.path().to_string_lossy().as_ref()).expect("open");
        db.upsert_setup(&SetupDefinition {
            id: "s1".to_string(),
            name: "DNVA Reversion".to_string(),
            description: "Mean reversion at DNVA".to_string(),
            active: true,
            conditions: vec!["price_at_dnva".to_string()],
            min_delta: 0.0,
            require_above_vwap: false,
            duplicate_suppression_ms: 5000,
        })
        .expect("insert");
        let setups = db.list_setups().expect("list");
        assert_eq!(setups.len(), 1);
    }

    #[test]
    fn saves_and_loads_prior_day_levels() {
        let file = NamedTempFile::new().expect("temp");
        let db = Database::open(file.path().to_string_lossy().as_ref()).expect("open");
        db.save_prior_day("2026-02-24", 21100.0, 20900.0, 21050.0)
            .expect("save");
        let result = db.load_prior_day("2026-02-25").expect("load");
        assert!(result.is_some());
        let (h, l, c) = result.unwrap();
        assert_eq!(h, 21100.0);
        assert_eq!(l, 20900.0);
        assert_eq!(c, 21050.0);
    }

    #[test]
    fn load_prior_day_returns_none_when_empty() {
        let file = NamedTempFile::new().expect("temp");
        let db = Database::open(file.path().to_string_lossy().as_ref()).expect("open");
        let result = db.load_prior_day("2026-02-25").expect("load");
        assert!(result.is_none());
    }
}
