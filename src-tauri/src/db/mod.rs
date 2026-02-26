use crate::risk::RiskState;
use crate::rules::SetupDefinition;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionEventInput {
    pub event_type: String,
    pub setup_id: Option<String>,
    pub data: serde_json::Value,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionEventRecord {
    pub id: i64,
    pub event_type: String,
    pub setup_id: Option<String>,
    pub data: serde_json::Value,
    pub session_id: Option<String>,
    pub timestamp: Option<f64>,
}

// ---------------------------------------------------------------------------
// Session entity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionRecord {
    pub id: String,
    pub date: String,
    pub session_type: String,
    pub start_time: f64,
    pub end_time: Option<f64>,
    pub recording_path: Option<String>,
    pub pre_session_note: Option<String>,
}

// ---------------------------------------------------------------------------
// Trade entity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeRecord {
    pub id: String,
    pub session_id: Option<String>,
    pub setup_id: Option<String>,
    pub entry_time: f64,
    pub entry_price: f64,
    pub exit_time: Option<f64>,
    pub exit_price: Option<f64>,
    pub direction: String,
    pub size: i64,
    pub stop_price: Option<f64>,
    pub target_prices: Vec<f64>,
    pub result_r: Option<f64>,
    pub planned: bool,
    pub rules_followed: Option<bool>,
    pub emotional_state: Option<String>,
    pub notes: String,
    pub source: String,
}

// ---------------------------------------------------------------------------
// Journal entity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalEntry {
    pub id: String,
    pub session_id: Option<String>,
    pub date: String,
    pub content: String,
    pub tags: Vec<String>,
    pub setup_references: Vec<String>,
    pub trade_references: Vec<String>,
    pub created_at: f64,
}

// ---------------------------------------------------------------------------
// Risk config entity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RiskConfigRecord {
    pub r_value_points: f64,
    pub r_value_dollars: f64,
    pub max_daily_loss_r: f64,
    pub max_consecutive_losses: i64,
    pub max_trades_per_session: Option<i64>,
    pub no_trade_zones: Vec<serde_json::Value>,
}

impl Default for RiskConfigRecord {
    fn default() -> Self {
        Self {
            r_value_points: 8.0,
            r_value_dollars: 40.0,
            max_daily_loss_r: 3.0,
            max_consecutive_losses: 3,
            max_trades_per_session: Some(8),
            no_trade_zones: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: &str) -> Result<Self, DbError> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let db = Self { conn };
        db.run_migrations()?;
        Ok(db)
    }

    // ------------------------------------------------------------------
    // Schema migration
    // ------------------------------------------------------------------

    fn run_migrations(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER NOT NULL DEFAULT 0
            )",
        )?;

        let version: i32 = self
            .conn
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_version",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        if version < 1 {
            self.migrate_v1()?;
        }
        if version < 2 {
            self.migrate_v2()?;
        }

        Ok(())
    }

    /// V1: original schema (creates tables if not exists for backward compat).
    fn migrate_v1(&self) -> Result<(), DbError> {
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
            INSERT OR REPLACE INTO schema_version (version) VALUES (1);
            ",
        )?;
        Ok(())
    }

    /// V2: extended schema — new tables, new columns, indexes.
    fn migrate_v2(&self) -> Result<(), DbError> {
        // Extend setups with rich playbook fields
        let setup_columns = [
            ("entry_logic", "TEXT NOT NULL DEFAULT '{}'"),
            ("stop_logic", "TEXT NOT NULL DEFAULT '{}'"),
            ("targets", "TEXT NOT NULL DEFAULT '[]'"),
            ("position_sizing", "TEXT NOT NULL DEFAULT '{}'"),
            ("market_context", "TEXT NOT NULL DEFAULT '{}'"),
            ("invalidation", "TEXT NOT NULL DEFAULT '[]'"),
            ("backtest_results", "TEXT NOT NULL DEFAULT '{}'"),
            ("context_backtest_results", "TEXT NOT NULL DEFAULT '[]'"),
            ("discretionary_conditions", "TEXT NOT NULL DEFAULT '[]'"),
            ("template_source", "TEXT NULL"),
        ];
        for (col, def) in &setup_columns {
            let sql = format!("ALTER TABLE setups ADD COLUMN {col} {def}");
            self.conn.execute_batch(&sql).ok(); // ignore if column already exists
        }

        // Extend session_events
        let event_columns = [("session_id", "TEXT NULL"), ("timestamp", "REAL NULL")];
        for (col, def) in &event_columns {
            let sql = format!("ALTER TABLE session_events ADD COLUMN {col} {def}");
            self.conn.execute_batch(&sql).ok();
        }

        // Extend prior_day_levels with VA/POC
        let level_columns = [
            ("va_high", "REAL NULL"),
            ("va_low", "REAL NULL"),
            ("poc", "REAL NULL"),
        ];
        for (col, def) in &level_columns {
            let sql = format!("ALTER TABLE prior_day_levels ADD COLUMN {col} {def}");
            self.conn.execute_batch(&sql).ok();
        }

        // Extend risk_state with consecutive wins
        self.conn
            .execute_batch(
                "ALTER TABLE risk_state ADD COLUMN consecutive_wins INTEGER NOT NULL DEFAULT 0",
            )
            .ok();

        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS sessions (
              id TEXT PRIMARY KEY,
              date TEXT NOT NULL,
              session_type TEXT NOT NULL DEFAULT 'rth',
              start_time REAL NOT NULL,
              end_time REAL NULL,
              recording_path TEXT NULL,
              pre_session_note TEXT NULL
            );

            CREATE TABLE IF NOT EXISTS trades (
              id TEXT PRIMARY KEY,
              session_id TEXT NULL REFERENCES sessions(id),
              setup_id TEXT NULL REFERENCES setups(id),
              entry_time REAL NOT NULL,
              entry_price REAL NOT NULL,
              exit_time REAL NULL,
              exit_price REAL NULL,
              direction TEXT NOT NULL,
              size INTEGER NOT NULL,
              stop_price REAL NULL,
              target_prices TEXT NOT NULL DEFAULT '[]',
              result_r REAL NULL,
              planned INTEGER NOT NULL DEFAULT 0,
              rules_followed INTEGER NULL,
              emotional_state TEXT NULL,
              notes TEXT NOT NULL DEFAULT '',
              source TEXT NOT NULL DEFAULT 'manual'
            );

            CREATE TABLE IF NOT EXISTS journal_entries (
              id TEXT PRIMARY KEY,
              session_id TEXT NULL REFERENCES sessions(id),
              date TEXT NOT NULL,
              content TEXT NOT NULL DEFAULT '',
              tags TEXT NOT NULL DEFAULT '[]',
              setup_references TEXT NOT NULL DEFAULT '[]',
              trade_references TEXT NOT NULL DEFAULT '[]',
              created_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS risk_config (
              singleton INTEGER PRIMARY KEY DEFAULT 1,
              r_value_points REAL NOT NULL DEFAULT 8.0,
              r_value_dollars REAL NOT NULL DEFAULT 40.0,
              max_daily_loss_r REAL NOT NULL DEFAULT 3.0,
              max_consecutive_losses INTEGER NOT NULL DEFAULT 3,
              max_trades_per_session INTEGER NULL DEFAULT 8,
              no_trade_zones TEXT NOT NULL DEFAULT '[]'
            );

            CREATE INDEX IF NOT EXISTS idx_session_events_session
              ON session_events(session_id, event_type);
            CREATE INDEX IF NOT EXISTS idx_trades_session
              ON trades(session_id);
            CREATE INDEX IF NOT EXISTS idx_journal_session
              ON journal_entries(session_id);

            UPDATE schema_version SET version = 2;
            ",
        )?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Setup CRUD
    // ------------------------------------------------------------------

    pub fn upsert_setup(&self, setup: &SetupDefinition) -> Result<(), DbError> {
        let conditions_json =
            serde_json::to_string(&setup.conditions).unwrap_or_else(|_| "[]".to_string());
        let entry_logic = serde_json::to_string(&setup.entry_logic).unwrap_or_else(|_| "{}".into());
        let stop_logic = serde_json::to_string(&setup.stop_logic).unwrap_or_else(|_| "{}".into());
        let targets = serde_json::to_string(&setup.targets).unwrap_or_else(|_| "[]".into());
        let position_sizing =
            serde_json::to_string(&setup.position_sizing).unwrap_or_else(|_| "{}".into());
        let market_context =
            serde_json::to_string(&setup.market_context).unwrap_or_else(|_| "{}".into());
        let invalidation =
            serde_json::to_string(&setup.invalidation).unwrap_or_else(|_| "[]".into());
        let backtest_results =
            serde_json::to_string(&setup.backtest_results).unwrap_or_else(|_| "{}".into());
        let context_backtest =
            serde_json::to_string(&setup.context_backtest_results).unwrap_or_else(|_| "[]".into());
        let discretionary =
            serde_json::to_string(&setup.discretionary_conditions).unwrap_or_else(|_| "[]".into());

        self.conn.execute(
            "INSERT INTO setups (id, name, description, active, conditions, min_delta,
                require_above_vwap, duplicate_suppression_ms, entry_logic, stop_logic,
                targets, position_sizing, market_context, invalidation,
                backtest_results, context_backtest_results, discretionary_conditions, template_source)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
            ON CONFLICT(id) DO UPDATE SET
              name=excluded.name, description=excluded.description,
              active=excluded.active, conditions=excluded.conditions,
              min_delta=excluded.min_delta, require_above_vwap=excluded.require_above_vwap,
              duplicate_suppression_ms=excluded.duplicate_suppression_ms,
              entry_logic=excluded.entry_logic, stop_logic=excluded.stop_logic,
              targets=excluded.targets, position_sizing=excluded.position_sizing,
              market_context=excluded.market_context, invalidation=excluded.invalidation,
              backtest_results=excluded.backtest_results,
              context_backtest_results=excluded.context_backtest_results,
              discretionary_conditions=excluded.discretionary_conditions,
              template_source=excluded.template_source",
            params![
                setup.id,
                setup.name,
                setup.description,
                i64::from(setup.active),
                conditions_json,
                setup.min_delta,
                i64::from(setup.require_above_vwap),
                setup.duplicate_suppression_ms as i64,
                entry_logic,
                stop_logic,
                targets,
                position_sizing,
                market_context,
                invalidation,
                backtest_results,
                context_backtest,
                discretionary,
                setup.template_source,
            ],
        )?;
        Ok(())
    }

    pub fn list_setups(&self) -> Result<Vec<SetupDefinition>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, description, active, conditions, min_delta,
                    require_above_vwap, duplicate_suppression_ms,
                    entry_logic, stop_logic, targets, position_sizing,
                    market_context, invalidation, backtest_results,
                    context_backtest_results, discretionary_conditions, template_source
             FROM setups",
        )?;
        let rows = stmt.query_map([], |row| {
            let conditions_str: String = row.get(4)?;
            let conditions: Vec<String> = serde_json::from_str(&conditions_str).unwrap_or_default();
            let entry_logic: serde_json::Value = row
                .get::<_, String>(8)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or(serde_json::json!({}));
            let stop_logic: serde_json::Value = row
                .get::<_, String>(9)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or(serde_json::json!({}));
            let targets: Vec<serde_json::Value> = row
                .get::<_, String>(10)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            let position_sizing: serde_json::Value = row
                .get::<_, String>(11)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or(serde_json::json!({}));
            let market_context: serde_json::Value = row
                .get::<_, String>(12)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or(serde_json::json!({}));
            let invalidation: Vec<serde_json::Value> = row
                .get::<_, String>(13)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            let backtest_results: serde_json::Value = row
                .get::<_, String>(14)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or(serde_json::json!({}));
            let context_backtest: Vec<serde_json::Value> = row
                .get::<_, String>(15)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            let discretionary: Vec<String> = row
                .get::<_, String>(16)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            let template_source: Option<String> = row.get(17).ok().flatten();

            Ok(SetupDefinition {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                active: row.get::<_, i64>(3)? == 1,
                conditions,
                min_delta: row.get(5)?,
                require_above_vwap: row.get::<_, i64>(6)? == 1,
                duplicate_suppression_ms: row.get::<_, i64>(7)? as u64,
                entry_logic,
                stop_logic,
                targets,
                position_sizing,
                market_context,
                invalidation,
                backtest_results,
                context_backtest_results: context_backtest,
                discretionary_conditions: discretionary,
                template_source,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn delete_setup(&self, id: &str) -> Result<(), DbError> {
        self.conn
            .execute("DELETE FROM setups WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn toggle_setup(&self, id: &str, active: bool) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE setups SET active = ?1 WHERE id = ?2",
            params![i64::from(active), id],
        )?;
        Ok(())
    }

    pub fn get_setup(&self, id: &str) -> Result<Option<SetupDefinition>, DbError> {
        let setups = self.list_setups()?;
        Ok(setups.into_iter().find(|s| s.id == id))
    }

    // ------------------------------------------------------------------
    // Session CRUD
    // ------------------------------------------------------------------

    pub fn create_session(&self, session: &SessionRecord) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO sessions (id, date, session_type, start_time, end_time, recording_path, pre_session_note)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                session.id, session.date, session.session_type,
                session.start_time, session.end_time,
                session.recording_path, session.pre_session_note,
            ],
        )?;
        Ok(())
    }

    pub fn update_session_end(
        &self,
        id: &str,
        end_time: f64,
        recording_path: Option<&str>,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE sessions SET end_time = ?1, recording_path = ?2 WHERE id = ?3",
            params![end_time, recording_path, id],
        )?;
        Ok(())
    }

    pub fn get_session(&self, id: &str) -> Result<Option<SessionRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, date, session_type, start_time, end_time, recording_path, pre_session_note
             FROM sessions WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(SessionRecord {
                id: row.get(0)?,
                date: row.get(1)?,
                session_type: row.get(2)?,
                start_time: row.get(3)?,
                end_time: row.get(4)?,
                recording_path: row.get(5)?,
                pre_session_note: row.get(6)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn list_sessions(&self, limit: usize) -> Result<Vec<SessionRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, date, session_type, start_time, end_time, recording_path, pre_session_note
             FROM sessions ORDER BY start_time DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |row| {
            Ok(SessionRecord {
                id: row.get(0)?,
                date: row.get(1)?,
                session_type: row.get(2)?,
                start_time: row.get(3)?,
                end_time: row.get(4)?,
                recording_path: row.get(5)?,
                pre_session_note: row.get(6)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    // ------------------------------------------------------------------
    // Trade CRUD
    // ------------------------------------------------------------------

    pub fn insert_trade(&self, trade: &TradeRecord) -> Result<(), DbError> {
        let targets_json = serde_json::to_string(&trade.target_prices)?;
        self.conn.execute(
            "INSERT INTO trades (id, session_id, setup_id, entry_time, entry_price,
                exit_time, exit_price, direction, size, stop_price, target_prices,
                result_r, planned, rules_followed, emotional_state, notes, source)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                trade.id,
                trade.session_id,
                trade.setup_id,
                trade.entry_time,
                trade.entry_price,
                trade.exit_time,
                trade.exit_price,
                trade.direction,
                trade.size,
                trade.stop_price,
                targets_json,
                trade.result_r,
                i64::from(trade.planned),
                trade.rules_followed.map(i64::from),
                trade.emotional_state,
                trade.notes,
                trade.source,
            ],
        )?;
        Ok(())
    }

    pub fn update_trade_exit(
        &self,
        id: &str,
        exit_time: f64,
        exit_price: f64,
        result_r: f64,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE trades SET exit_time = ?1, exit_price = ?2, result_r = ?3 WHERE id = ?4",
            params![exit_time, exit_price, result_r, id],
        )?;
        Ok(())
    }

    pub fn update_trade_review(
        &self,
        id: &str,
        planned: bool,
        rules_followed: Option<bool>,
        emotional_state: Option<&str>,
        notes: &str,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE trades SET planned = ?1, rules_followed = ?2, emotional_state = ?3, notes = ?4 WHERE id = ?5",
            params![i64::from(planned), rules_followed.map(i64::from), emotional_state, notes, id],
        )?;
        Ok(())
    }

    pub fn list_trades_for_session(&self, session_id: &str) -> Result<Vec<TradeRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, setup_id, entry_time, entry_price, exit_time, exit_price,
                    direction, size, stop_price, target_prices, result_r, planned,
                    rules_followed, emotional_state, notes, source
             FROM trades WHERE session_id = ?1 ORDER BY entry_time",
        )?;
        let rows = stmt.query_map([session_id], |row| {
            let targets_str: String = row.get(10)?;
            let target_prices: Vec<f64> = serde_json::from_str(&targets_str).unwrap_or_default();
            Ok(TradeRecord {
                id: row.get(0)?,
                session_id: row.get(1)?,
                setup_id: row.get(2)?,
                entry_time: row.get(3)?,
                entry_price: row.get(4)?,
                exit_time: row.get(5)?,
                exit_price: row.get(6)?,
                direction: row.get(7)?,
                size: row.get(8)?,
                stop_price: row.get(9)?,
                target_prices,
                result_r: row.get(11)?,
                planned: row.get::<_, i64>(12)? == 1,
                rules_followed: row.get::<_, Option<i64>>(13)?.map(|v| v == 1),
                emotional_state: row.get(14)?,
                notes: row.get::<_, String>(15)?,
                source: row.get(16)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_open_trade(&self, session_id: &str) -> Result<Option<TradeRecord>, DbError> {
        let trades = self.list_trades_for_session(session_id)?;
        Ok(trades.into_iter().find(|t| t.exit_time.is_none()))
    }

    // ------------------------------------------------------------------
    // Journal CRUD
    // ------------------------------------------------------------------

    pub fn insert_journal_entry(&self, entry: &JournalEntry) -> Result<(), DbError> {
        let tags = serde_json::to_string(&entry.tags)?;
        let setup_refs = serde_json::to_string(&entry.setup_references)?;
        let trade_refs = serde_json::to_string(&entry.trade_references)?;
        self.conn.execute(
            "INSERT INTO journal_entries (id, session_id, date, content, tags, setup_references, trade_references, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                entry.id, entry.session_id, entry.date, entry.content,
                tags, setup_refs, trade_refs, entry.created_at,
            ],
        )?;
        Ok(())
    }

    pub fn update_journal_entry(
        &self,
        id: &str,
        content: &str,
        tags: &[String],
    ) -> Result<(), DbError> {
        let tags_json = serde_json::to_string(tags)?;
        self.conn.execute(
            "UPDATE journal_entries SET content = ?1, tags = ?2 WHERE id = ?3",
            params![content, tags_json, id],
        )?;
        Ok(())
    }

    pub fn get_journal_for_session(&self, session_id: &str) -> Result<Vec<JournalEntry>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, date, content, tags, setup_references, trade_references, created_at
             FROM journal_entries WHERE session_id = ?1 ORDER BY created_at",
        )?;
        let rows = stmt.query_map([session_id], |row| {
            let tags: Vec<String> = row
                .get::<_, String>(4)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            let setup_refs: Vec<String> = row
                .get::<_, String>(5)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            let trade_refs: Vec<String> = row
                .get::<_, String>(6)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            Ok(JournalEntry {
                id: row.get(0)?,
                session_id: row.get(1)?,
                date: row.get(2)?,
                content: row.get(3)?,
                tags,
                setup_references: setup_refs,
                trade_references: trade_refs,
                created_at: row.get(7)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    // ------------------------------------------------------------------
    // Risk config
    // ------------------------------------------------------------------

    pub fn save_risk_config(&self, config: &RiskConfigRecord) -> Result<(), DbError> {
        let zones = serde_json::to_string(&config.no_trade_zones)?;
        self.conn.execute(
            "INSERT INTO risk_config (singleton, r_value_points, r_value_dollars, max_daily_loss_r,
                max_consecutive_losses, max_trades_per_session, no_trade_zones)
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(singleton) DO UPDATE SET
               r_value_points=excluded.r_value_points,
               r_value_dollars=excluded.r_value_dollars,
               max_daily_loss_r=excluded.max_daily_loss_r,
               max_consecutive_losses=excluded.max_consecutive_losses,
               max_trades_per_session=excluded.max_trades_per_session,
               no_trade_zones=excluded.no_trade_zones",
            params![
                config.r_value_points,
                config.r_value_dollars,
                config.max_daily_loss_r,
                config.max_consecutive_losses,
                config.max_trades_per_session,
                zones,
            ],
        )?;
        Ok(())
    }

    pub fn load_risk_config(&self) -> Result<RiskConfigRecord, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT r_value_points, r_value_dollars, max_daily_loss_r,
                    max_consecutive_losses, max_trades_per_session, no_trade_zones
             FROM risk_config WHERE singleton = 1",
        )?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let zones_str: String = row.get::<_, String>(5)?;
            let zones: Vec<serde_json::Value> =
                serde_json::from_str(&zones_str).unwrap_or_default();
            Ok(RiskConfigRecord {
                r_value_points: row.get(0)?,
                r_value_dollars: row.get(1)?,
                max_daily_loss_r: row.get(2)?,
                max_consecutive_losses: row.get(3)?,
                max_trades_per_session: row.get(4)?,
                no_trade_zones: zones,
            })
        } else {
            Ok(RiskConfigRecord::default())
        }
    }

    // ------------------------------------------------------------------
    // Session events (extended)
    // ------------------------------------------------------------------

    pub fn add_event(&self, event: &SessionEventInput) -> Result<i64, DbError> {
        let payload = serde_json::to_string(&event.data)?;
        let ts = chrono::Utc::now().timestamp_millis() as f64;
        self.conn.execute(
            "INSERT INTO session_events (event_type, setup_id, data, session_id, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                event.event_type,
                event.setup_id,
                payload,
                event.session_id,
                ts
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn list_session_events(&self, limit: usize) -> Result<Vec<SessionEventRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, event_type, setup_id, data, session_id, timestamp
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
                session_id: row.get(4)?,
                timestamp: row.get(5)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn list_events_for_session(
        &self,
        session_id: &str,
    ) -> Result<Vec<SessionEventRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, event_type, setup_id, data, session_id, timestamp
             FROM session_events
             WHERE session_id = ?1
             ORDER BY id ASC",
        )?;
        let rows = stmt.query_map([session_id], |row| {
            let data_text: String = row.get(3)?;
            let data = serde_json::from_str::<serde_json::Value>(&data_text)
                .unwrap_or_else(|_| serde_json::json!({}));
            Ok(SessionEventRecord {
                id: row.get(0)?,
                event_type: row.get(1)?,
                setup_id: row.get(2)?,
                data,
                session_id: row.get(4)?,
                timestamp: row.get(5)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    // ------------------------------------------------------------------
    // Risk state persistence (extended with consecutive_wins)
    // ------------------------------------------------------------------

    pub fn save_risk_state(&self, state: &RiskState) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO risk_state (singleton, daily_pnl_r, trade_count, consecutive_losses, consecutive_wins, drawdown_r, max_daily_loss_r, at_limit)
            VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(singleton) DO UPDATE SET
              daily_pnl_r=excluded.daily_pnl_r,
              trade_count=excluded.trade_count,
              consecutive_losses=excluded.consecutive_losses,
              consecutive_wins=excluded.consecutive_wins,
              drawdown_r=excluded.drawdown_r,
              max_daily_loss_r=excluded.max_daily_loss_r,
              at_limit=excluded.at_limit",
            params![
                state.daily_pnl_r,
                state.trade_count as i64,
                state.consecutive_losses as i64,
                state.consecutive_wins as i64,
                state.drawdown_r,
                state.max_daily_loss_r,
                i64::from(state.at_limit),
            ],
        )?;
        Ok(())
    }

    pub fn load_risk_state(&self) -> Result<Option<RiskState>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT daily_pnl_r, trade_count, consecutive_losses, consecutive_wins, drawdown_r, max_daily_loss_r, at_limit
             FROM risk_state WHERE singleton = 1",
        )?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            Ok(Some(RiskState {
                daily_pnl_r: row.get(0)?,
                trade_count: row.get::<_, i64>(1)? as usize,
                consecutive_losses: row.get::<_, i64>(2)? as usize,
                consecutive_wins: row.get::<_, i64>(3).unwrap_or(0) as usize,
                drawdown_r: row.get(4)?,
                max_daily_loss_r: row.get(5)?,
                at_limit: row.get::<_, i64>(6)? == 1,
            }))
        } else {
            Ok(None)
        }
    }

    // ------------------------------------------------------------------
    // Prior day levels (extended with VA/POC)
    // ------------------------------------------------------------------

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
             ON CONFLICT(date) DO UPDATE SET high=excluded.high, low=excluded.low, close=excluded.close",
            params![date, high, low, close],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn save_prior_day_full(
        &self,
        date: &str,
        high: f64,
        low: f64,
        close: f64,
        va_high: f64,
        va_low: f64,
        poc: f64,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO prior_day_levels (date, high, low, close, va_high, va_low, poc)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(date) DO UPDATE SET
               high=excluded.high, low=excluded.low, close=excluded.close,
               va_high=excluded.va_high, va_low=excluded.va_low, poc=excluded.poc",
            params![date, high, low, close, va_high, va_low, poc],
        )?;
        Ok(())
    }

    pub fn load_prior_day(&self, before_date: &str) -> Result<Option<(f64, f64, f64)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT high, low, close FROM prior_day_levels WHERE date < ?1 ORDER BY date DESC LIMIT 1",
        )?;
        let mut rows = stmt.query(params![before_date])?;
        if let Some(row) = rows.next()? {
            Ok(Some((row.get(0)?, row.get(1)?, row.get(2)?)))
        } else {
            Ok(None)
        }
    }

    /// Load prior-day levels including VA/POC if available.
    #[allow(clippy::type_complexity)]
    pub fn load_prior_day_full(
        &self,
        before_date: &str,
    ) -> Result<Option<(f64, f64, f64, Option<f64>, Option<f64>, Option<f64>)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT high, low, close, va_high, va_low, poc
             FROM prior_day_levels WHERE date < ?1 ORDER BY date DESC LIMIT 1",
        )?;
        let mut rows = stmt.query(params![before_date])?;
        if let Some(row) = rows.next()? {
            Ok(Some((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            )))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn test_db() -> Database {
        let file = NamedTempFile::new().expect("temp");
        Database::open(file.path().to_string_lossy().as_ref()).expect("open")
    }

    #[test]
    fn inserts_and_lists_setups() {
        let db = test_db();
        db.upsert_setup(&SetupDefinition {
            id: "s1".to_string(),
            name: "DNVA Reversion".to_string(),
            description: "Mean reversion at DNVA".to_string(),
            active: true,
            conditions: vec!["price_at_dnva".to_string()],
            min_delta: 0.0,
            require_above_vwap: false,
            duplicate_suppression_ms: 5000,
            ..Default::default()
        })
        .expect("insert");
        let setups = db.list_setups().expect("list");
        assert_eq!(setups.len(), 1);
    }

    #[test]
    fn toggle_and_delete_setup() {
        let db = test_db();
        db.upsert_setup(&SetupDefinition {
            id: "s1".to_string(),
            name: "Test".to_string(),
            active: true,
            ..Default::default()
        })
        .expect("insert");
        db.toggle_setup("s1", false).expect("toggle");
        let setups = db.list_setups().expect("list");
        assert!(!setups[0].active);
        db.delete_setup("s1").expect("delete");
        assert!(db.list_setups().expect("list").is_empty());
    }

    #[test]
    fn session_lifecycle() {
        let db = test_db();
        let session = SessionRecord {
            id: "sess1".into(),
            date: "2026-02-25".into(),
            session_type: "rth".into(),
            start_time: 1740000000.0,
            end_time: None,
            recording_path: None,
            pre_session_note: Some("Focus on VWAP plays".into()),
        };
        db.create_session(&session).expect("create");
        db.update_session_end("sess1", 1740030000.0, Some("/path/rec.desk"))
            .expect("update");
        let loaded = db.get_session("sess1").expect("get").expect("exists");
        assert!(loaded.end_time.is_some());
        assert_eq!(loaded.pre_session_note.unwrap(), "Focus on VWAP plays");
    }

    #[test]
    fn trade_lifecycle() {
        let db = test_db();
        db.create_session(&SessionRecord {
            id: "sess1".into(),
            date: "2026-02-25".into(),
            session_type: "rth".into(),
            start_time: 1740000000.0,
            end_time: None,
            recording_path: None,
            pre_session_note: None,
        })
        .expect("session");
        let trade = TradeRecord {
            id: "t1".into(),
            session_id: Some("sess1".into()),
            setup_id: None,
            entry_time: 1740001000.0,
            entry_price: 21000.0,
            exit_time: None,
            exit_price: None,
            direction: "long".into(),
            size: 1,
            stop_price: Some(20990.0),
            target_prices: vec![21020.0, 21040.0],
            result_r: None,
            planned: true,
            rules_followed: None,
            emotional_state: None,
            notes: String::new(),
            source: "manual".into(),
        };
        db.insert_trade(&trade).expect("insert");
        let open = db.get_open_trade("sess1").expect("open").expect("exists");
        assert_eq!(open.entry_price, 21000.0);
        db.update_trade_exit("t1", 1740002000.0, 21020.0, 2.0)
            .expect("exit");
        assert!(db.get_open_trade("sess1").expect("open").is_none());
    }

    #[test]
    fn risk_config_persistence() {
        let db = test_db();
        let config = RiskConfigRecord {
            max_daily_loss_r: 5.0,
            ..Default::default()
        };
        db.save_risk_config(&config).expect("save");
        let loaded = db.load_risk_config().expect("load");
        assert_eq!(loaded.max_daily_loss_r, 5.0);
    }

    #[test]
    fn saves_and_loads_prior_day_levels() {
        let db = test_db();
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
    fn prior_day_full_with_va_poc() {
        let db = test_db();
        db.save_prior_day_full(
            "2026-02-24",
            21100.0,
            20900.0,
            21050.0,
            21080.0,
            20950.0,
            21020.0,
        )
        .expect("save");
        let result = db
            .load_prior_day_full("2026-02-25")
            .expect("load")
            .expect("exists");
        assert_eq!(result.3, Some(21080.0));
        assert_eq!(result.4, Some(20950.0));
        assert_eq!(result.5, Some(21020.0));
    }

    #[test]
    fn journal_entry_lifecycle() {
        let db = test_db();
        db.create_session(&SessionRecord {
            id: "sess1".into(),
            date: "2026-02-25".into(),
            session_type: "rth".into(),
            start_time: 1740000000.0,
            end_time: None,
            recording_path: None,
            pre_session_note: None,
        })
        .expect("session");
        let entry = JournalEntry {
            id: "j1".into(),
            session_id: Some("sess1".into()),
            date: "2026-02-25".into(),
            content: "Good session".into(),
            tags: vec!["discipline".into()],
            setup_references: vec![],
            trade_references: vec![],
            created_at: 1740030000.0,
        };
        db.insert_journal_entry(&entry).expect("insert");
        let entries = db.get_journal_for_session("sess1").expect("get");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].content, "Good session");
    }
}
