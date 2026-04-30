use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const DEFAULT_RUNTIME_EVENT_BUFFER: usize = 500;
const DEFAULT_RUNTIME_EVENT_RETENTION_DAYS: u32 = 7;
const DEFAULT_RUNTIME_EVENT_MAX_ROWS: usize = 10_000;
const DEFAULT_RUNTIME_EVENT_SUPPRESSION_WINDOW_MS: u64 = 1_000;
const DEFAULT_LOG_FILE_RETENTION_DAYS: u32 = 14;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeEventLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl RuntimeEventLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }

    pub fn rank(self) -> u8 {
        match self {
            Self::Trace => 0,
            Self::Debug => 1,
            Self::Info => 2,
            Self::Warn => 3,
            Self::Error => 4,
        }
    }
}

impl std::str::FromStr for RuntimeEventLevel {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "trace" => Ok(Self::Trace),
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" | "warning" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            other => Err(format!("unknown runtime event level: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeEvent {
    pub id: Option<i64>,
    pub emitted_at_ms: f64,
    pub level: RuntimeEventLevel,
    pub event_name: String,
    pub category: String,
    pub message: String,
    pub session_date: Option<String>,
    pub root_symbol: Option<String>,
    pub contract_symbol: Option<String>,
    pub fields: Value,
}

impl RuntimeEvent {
    pub fn new(
        level: RuntimeEventLevel,
        event_name: impl Into<String>,
        category: impl Into<String>,
        message: impl Into<String>,
        fields: Value,
    ) -> Self {
        Self {
            id: None,
            emitted_at_ms: chrono::Utc::now().timestamp_millis() as f64,
            level,
            event_name: event_name.into(),
            category: category.into(),
            message: message.into(),
            session_date: None,
            root_symbol: None,
            contract_symbol: None,
            fields,
        }
    }

    pub fn with_session_date(mut self, session_date: impl Into<Option<String>>) -> Self {
        self.session_date = session_date.into();
        self
    }

    pub fn with_contract(
        mut self,
        root_symbol: impl Into<Option<String>>,
        contract_symbol: impl Into<Option<String>>,
    ) -> Self {
        self.root_symbol = root_symbol.into();
        self.contract_symbol = contract_symbol.into();
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeEventFilter {
    pub since_ms: Option<f64>,
    pub level: Option<RuntimeEventLevel>,
    pub min_level: Option<RuntimeEventLevel>,
    pub category: Option<String>,
    pub event_name: Option<String>,
    pub limit: usize,
}

#[derive(Debug)]
pub struct RuntimeEventStore {
    inner: Mutex<VecDeque<RuntimeEvent>>,
    suppression: Mutex<HashMap<String, SuppressionState>>,
    log_sink: RuntimeEventLogSink,
    capacity: usize,
    persist_runtime_events: bool,
    retention_days: u32,
    max_persisted_rows: usize,
    suppression_window_ms: u64,
}

impl RuntimeEventStore {
    pub fn new(config: &LoggingConfig) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(config.runtime_event_buffer)),
            suppression: Mutex::new(HashMap::new()),
            log_sink: RuntimeEventLogSink::new(config),
            capacity: config.runtime_event_buffer.max(1),
            persist_runtime_events: config.persist_runtime_events,
            retention_days: config.runtime_event_retention_days,
            max_persisted_rows: config.runtime_event_max_rows,
            suppression_window_ms: config.runtime_event_suppression_window_ms,
        }
    }

    pub fn record(&self, mut event: RuntimeEvent) -> Option<RuntimeEvent> {
        let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        if self.should_suppress(&mut event, now_ms) {
            return None;
        }

        self.log_sink.write_event(&event);
        let Ok(mut guard) = self.inner.lock() else {
            return Some(event);
        };
        while guard.len() >= self.capacity {
            guard.pop_front();
        }
        guard.push_back(event.clone());
        Some(event)
    }

    pub fn query(&self, filter: &RuntimeEventFilter) -> Vec<RuntimeEvent> {
        let Ok(guard) = self.inner.lock() else {
            return Vec::new();
        };
        let limit = filter.limit.max(1);
        guard
            .iter()
            .rev()
            .filter(|event| event_matches_filter(event, filter))
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn stats(&self) -> RuntimeEventStats {
        let Ok(guard) = self.inner.lock() else {
            return RuntimeEventStats::default();
        };
        let last_warning = guard
            .iter()
            .rev()
            .find(|event| event.level == RuntimeEventLevel::Warn)
            .map(RuntimeEventSummary::from_event);
        let last_error = guard
            .iter()
            .rev()
            .find(|event| event.level == RuntimeEventLevel::Error)
            .map(RuntimeEventSummary::from_event);
        let mut recent_event_name_counts = BTreeMap::new();
        for event in guard.iter() {
            *recent_event_name_counts
                .entry(event.event_name.clone())
                .or_insert(0) += 1;
        }
        RuntimeEventStats {
            recent_event_count: guard.len(),
            last_warning_at_ms: last_warning.as_ref().map(|event| event.emitted_at_ms),
            last_error_at_ms: last_error.as_ref().map(|event| event.emitted_at_ms),
            last_warning,
            last_error,
            recent_event_name_counts,
        }
    }

    pub fn persist_runtime_events(&self) -> bool {
        self.persist_runtime_events
    }

    pub fn retention_days(&self) -> u32 {
        self.retention_days
    }

    pub fn max_persisted_rows(&self) -> usize {
        self.max_persisted_rows
    }

    fn should_suppress(&self, event: &mut RuntimeEvent, now_ms: u64) -> bool {
        if self.suppression_window_ms == 0 {
            return false;
        }
        let Ok(mut guard) = self.suppression.lock() else {
            return false;
        };
        let state = guard.entry(event.event_name.clone()).or_default();
        if state.last_emitted_wall_ms > 0
            && now_ms.saturating_sub(state.last_emitted_wall_ms) < self.suppression_window_ms
        {
            state.suppressed_count = state.suppressed_count.saturating_add(1);
            state.last_suppressed_wall_ms = now_ms;
            return true;
        }
        if state.suppressed_count > 0 {
            let mut fields = event.fields.as_object().cloned().unwrap_or_else(Map::new);
            fields.insert(
                "suppressedSinceLastEmit".to_string(),
                serde_json::json!(state.suppressed_count),
            );
            fields.insert(
                "suppressionWindowMs".to_string(),
                serde_json::json!(self.suppression_window_ms),
            );
            fields.insert(
                "lastSuppressedWallMs".to_string(),
                serde_json::json!(state.last_suppressed_wall_ms),
            );
            event.fields = Value::Object(fields);
            state.suppressed_count = 0;
            state.last_suppressed_wall_ms = 0;
        }
        state.last_emitted_wall_ms = now_ms;
        false
    }
}

#[derive(Debug, Clone, Default)]
struct SuppressionState {
    last_emitted_wall_ms: u64,
    suppressed_count: u64,
    last_suppressed_wall_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeEventStats {
    pub recent_event_count: usize,
    pub last_warning_at_ms: Option<f64>,
    pub last_error_at_ms: Option<f64>,
    pub last_warning: Option<RuntimeEventSummary>,
    pub last_error: Option<RuntimeEventSummary>,
    pub recent_event_name_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeEventSummary {
    pub emitted_at_ms: f64,
    pub level: RuntimeEventLevel,
    pub event_name: String,
    pub category: String,
    pub message: String,
}

impl RuntimeEventSummary {
    fn from_event(event: &RuntimeEvent) -> Self {
        Self {
            emitted_at_ms: event.emitted_at_ms,
            level: event.level,
            event_name: event.event_name.clone(),
            category: event.category.clone(),
            message: event.message.clone(),
        }
    }
}

pub fn event_matches_filter(event: &RuntimeEvent, filter: &RuntimeEventFilter) -> bool {
    if let Some(since_ms) = filter.since_ms {
        if event.emitted_at_ms < since_ms {
            return false;
        }
    }
    if let Some(level) = filter.level {
        if event.level != level {
            return false;
        }
    }
    if let Some(min_level) = filter.min_level {
        if event.level.rank() < min_level.rank() {
            return false;
        }
    }
    if let Some(category) = filter.category.as_deref() {
        if event.category != category {
            return false;
        }
    }
    if let Some(event_name) = filter.event_name.as_deref() {
        if event.event_name != event_name {
            return false;
        }
    }
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
    #[serde(default = "default_log_destination")]
    pub destination: String,
    #[serde(default = "default_log_file_path")]
    pub file_path: String,
    #[serde(default = "default_runtime_event_buffer")]
    pub runtime_event_buffer: usize,
    #[serde(default = "default_persist_runtime_events")]
    pub persist_runtime_events: bool,
    #[serde(default = "default_runtime_event_retention_days")]
    pub runtime_event_retention_days: u32,
    #[serde(default = "default_runtime_event_max_rows")]
    pub runtime_event_max_rows: usize,
    #[serde(default = "default_runtime_event_suppression_window_ms")]
    pub runtime_event_suppression_window_ms: u64,
    #[serde(default = "default_log_file_retention_days")]
    pub file_retention_days: u32,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
            destination: default_log_destination(),
            file_path: default_log_file_path(),
            runtime_event_buffer: default_runtime_event_buffer(),
            persist_runtime_events: default_persist_runtime_events(),
            runtime_event_retention_days: default_runtime_event_retention_days(),
            runtime_event_max_rows: default_runtime_event_max_rows(),
            runtime_event_suppression_window_ms: default_runtime_event_suppression_window_ms(),
            file_retention_days: default_log_file_retention_days(),
        }
    }
}

impl LoggingConfig {
    fn format_kind(&self) -> LogFormat {
        match self.format.trim().to_ascii_lowercase().as_str() {
            "compact" | "text" => LogFormat::Compact,
            _ => LogFormat::Json,
        }
    }

    fn destination_kind(&self) -> LogDestination {
        match self.destination.trim().to_ascii_lowercase().as_str() {
            "file" => LogDestination::File,
            "both" => LogDestination::Both,
            "none" | "off" => LogDestination::None,
            _ => LogDestination::Stderr,
        }
    }

    pub fn resolved_file_path(&self) -> PathBuf {
        let path = self.file_path.trim();
        if path.is_empty() {
            PathBuf::from(default_log_file_path())
        } else {
            PathBuf::from(path)
        }
    }

    pub fn stderr_only() -> Self {
        Self {
            destination: "stderr".to_string(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum LogFormat {
    Json,
    Compact,
}

#[derive(Debug, Clone, Copy)]
enum LogDestination {
    Stderr,
    File,
    Both,
    None,
}

#[derive(Debug, Deserialize)]
struct RootLoggingConfig {
    #[serde(default)]
    logging: LoggingConfig,
}

pub struct LoggingRuntime {
    _guard: Option<WorkerGuard>,
}

impl LoggingRuntime {
    pub fn disabled() -> Self {
        Self { _guard: None }
    }
}

/// Load `[logging]` from `~/.the-desk/config.toml`; falls back to production-safe defaults.
pub fn load_logging_config() -> LoggingConfig {
    let path = crate::feed::default_config_path();
    let raw = std::fs::read_to_string(path);
    match raw {
        Ok(content) => toml::from_str::<RootLoggingConfig>(&content)
            .map(|cfg| cfg.logging)
            .unwrap_or_default(),
        Err(_) => LoggingConfig::default(),
    }
}

/// Initialize process logging. Writers must never target stdout because MCP stdio owns it.
pub fn init_logging(config: &LoggingConfig) -> Result<LoggingRuntime, String> {
    let filter =
        EnvFilter::try_new(config.level.clone()).unwrap_or_else(|_| EnvFilter::new("info"));
    match (config.destination_kind(), config.format_kind()) {
        (LogDestination::None, _) => Ok(LoggingRuntime::disabled()),
        (LogDestination::Stderr, LogFormat::Json) => {
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .flatten_event(true)
                        .with_ansi(false)
                        .with_writer(std::io::stderr),
                )
                .try_init()
                .map_err(|e| e.to_string())?;
            Ok(LoggingRuntime { _guard: None })
        }
        (LogDestination::Stderr, LogFormat::Compact) => {
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .compact()
                        .with_writer(std::io::stderr),
                )
                .try_init()
                .map_err(|e| e.to_string())?;
            Ok(LoggingRuntime { _guard: None })
        }
        (LogDestination::File, LogFormat::Json) => {
            let (writer, guard) = file_log_writer(config)?;
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .flatten_event(true)
                        .with_ansi(false)
                        .with_writer(writer),
                )
                .try_init()
                .map_err(|e| e.to_string())?;
            Ok(LoggingRuntime {
                _guard: Some(guard),
            })
        }
        (LogDestination::File, LogFormat::Compact) => {
            let (writer, guard) = file_log_writer(config)?;
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .compact()
                        .with_ansi(false)
                        .with_writer(writer),
                )
                .try_init()
                .map_err(|e| e.to_string())?;
            Ok(LoggingRuntime {
                _guard: Some(guard),
            })
        }
        (LogDestination::Both, LogFormat::Json) => {
            let (writer, guard) = file_log_writer(config)?;
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .flatten_event(true)
                        .with_ansi(false)
                        .with_writer(std::io::stderr),
                )
                .with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .flatten_event(true)
                        .with_ansi(false)
                        .with_writer(writer),
                )
                .try_init()
                .map_err(|e| e.to_string())?;
            Ok(LoggingRuntime {
                _guard: Some(guard),
            })
        }
        (LogDestination::Both, LogFormat::Compact) => {
            let (writer, guard) = file_log_writer(config)?;
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .compact()
                        .with_writer(std::io::stderr),
                )
                .with(
                    tracing_subscriber::fmt::layer()
                        .compact()
                        .with_ansi(false)
                        .with_writer(writer),
                )
                .try_init()
                .map_err(|e| e.to_string())?;
            Ok(LoggingRuntime {
                _guard: Some(guard),
            })
        }
    }
}

fn file_log_writer(
    config: &LoggingConfig,
) -> Result<(tracing_appender::non_blocking::NonBlocking, WorkerGuard), String> {
    let path = config.resolved_file_path();
    let parent = path
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    std::fs::create_dir_all(&parent).map_err(|e| e.to_string())?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("the-desk-mcp.jsonl");
    prune_log_files(&parent, file_name, config.file_retention_days).ok();
    let appender = tracing_appender::rolling::daily(parent, file_name);
    Ok(tracing_appender::non_blocking(appender))
}

#[derive(Debug)]
struct RuntimeEventLogSink {
    format: LogFormat,
    write_stderr: bool,
    write_file: bool,
    file: Mutex<Option<DailyRuntimeEventFile>>,
    file_path: PathBuf,
    file_retention_days: u32,
}

impl RuntimeEventLogSink {
    fn new(config: &LoggingConfig) -> Self {
        let destination = config.destination_kind();
        let file_path = config.resolved_file_path();
        let write_file = matches!(destination, LogDestination::File | LogDestination::Both);
        if write_file {
            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent).ok();
                let file_name = file_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("the-desk-mcp.jsonl");
                prune_log_files(parent, file_name, config.file_retention_days).ok();
            }
        }
        Self {
            format: config.format_kind(),
            write_stderr: matches!(destination, LogDestination::Stderr | LogDestination::Both),
            write_file,
            file: Mutex::new(None),
            file_path,
            file_retention_days: config.file_retention_days,
        }
    }

    fn write_event(&self, event: &RuntimeEvent) {
        let line = match self.format {
            LogFormat::Json => runtime_event_log_json(event).to_string(),
            LogFormat::Compact => runtime_event_log_text(event),
        };
        if self.write_stderr {
            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr, "{line}");
        }
        if self.write_file {
            if let Ok(mut guard) = self.file.lock() {
                if let Some(file) = self.open_file(&mut guard) {
                    let _ = writeln!(file, "{line}");
                }
            }
        }
    }

    fn open_file<'a>(&self, slot: &'a mut Option<DailyRuntimeEventFile>) -> Option<&'a mut File> {
        let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let needs_open = slot.as_ref().map(|f| f.date != date).unwrap_or(true);
        if needs_open {
            let path = daily_log_path(&self.file_path, &date);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).ok()?;
                let file_name = self
                    .file_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("the-desk-mcp.jsonl");
                prune_log_files(parent, file_name, self.file_retention_days).ok();
            }
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .ok()?;
            *slot = Some(DailyRuntimeEventFile { date, file });
        }
        slot.as_mut().map(|f| &mut f.file)
    }
}

#[derive(Debug)]
struct DailyRuntimeEventFile {
    date: String,
    file: File,
}

pub fn runtime_event_log_json(event: &RuntimeEvent) -> Value {
    let mut obj = Map::new();
    obj.insert(
        "emittedAtMs".to_string(),
        serde_json::json!(event.emitted_at_ms),
    );
    obj.insert("level".to_string(), serde_json::json!(event.level.as_str()));
    obj.insert(
        "eventName".to_string(),
        serde_json::json!(&event.event_name),
    );
    obj.insert("category".to_string(), serde_json::json!(&event.category));
    obj.insert("message".to_string(), serde_json::json!(&event.message));
    obj.insert(
        "sessionDate".to_string(),
        serde_json::json!(&event.session_date),
    );
    obj.insert(
        "rootSymbol".to_string(),
        serde_json::json!(&event.root_symbol),
    );
    obj.insert(
        "contractSymbol".to_string(),
        serde_json::json!(&event.contract_symbol),
    );
    obj.insert("fields".to_string(), event.fields.clone());
    if let Some(fields) = event.fields.as_object() {
        for (key, value) in fields {
            obj.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
    Value::Object(obj)
}

fn runtime_event_log_text(event: &RuntimeEvent) -> String {
    format!(
        "{} {} {} {}",
        event.level.as_str(),
        event.category,
        event.event_name,
        event.message
    )
}

fn daily_log_path(path: &Path, date: &str) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("the-desk-mcp.jsonl");
    let (stem, ext) = match file_name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() && !ext.is_empty() => (stem, Some(ext)),
        _ => (file_name, None),
    };
    let dated = match ext {
        Some(ext) => format!("{stem}.{date}.{ext}"),
        None => format!("{stem}.{date}"),
    };
    parent.join(dated)
}

fn prune_log_files(
    dir: &Path,
    configured_file_name: &str,
    retention_days: u32,
) -> std::io::Result<()> {
    if retention_days == 0 {
        return Ok(());
    }
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(retention_days as u64 * 86_400))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let prefix = configured_file_name
        .rsplit_once('.')
        .map(|(stem, _)| format!("{stem}."))
        .unwrap_or_else(|| format!("{configured_file_name}."));
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with(&prefix) {
            continue;
        }
        let meta = entry.metadata()?;
        if meta.modified().map(|mtime| mtime < cutoff).unwrap_or(false) {
            std::fs::remove_file(entry.path()).ok();
        }
    }
    Ok(())
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "json".to_string()
}

fn default_log_destination() -> String {
    "stderr".to_string()
}

fn default_log_file_path() -> String {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".the-desk")
        .join("logs")
        .join("the-desk-mcp.jsonl")
        .to_string_lossy()
        .to_string()
}

fn default_runtime_event_buffer() -> usize {
    DEFAULT_RUNTIME_EVENT_BUFFER
}

fn default_persist_runtime_events() -> bool {
    true
}

fn default_runtime_event_retention_days() -> u32 {
    DEFAULT_RUNTIME_EVENT_RETENTION_DAYS
}

fn default_runtime_event_max_rows() -> usize {
    DEFAULT_RUNTIME_EVENT_MAX_ROWS
}

fn default_runtime_event_suppression_window_ms() -> u64 {
    DEFAULT_RUNTIME_EVENT_SUPPRESSION_WINDOW_MS
}

fn default_log_file_retention_days() -> u32 {
    DEFAULT_LOG_FILE_RETENTION_DAYS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_event_store_filters_recent_events() {
        let config = LoggingConfig {
            runtime_event_buffer: 2,
            runtime_event_suppression_window_ms: 0,
            destination: "none".to_string(),
            ..LoggingConfig::default()
        };
        let store = RuntimeEventStore::new(&config);
        store.record(RuntimeEvent::new(
            RuntimeEventLevel::Info,
            "mcp.startup",
            "mcp",
            "started",
            serde_json::json!({}),
        ));
        store.record(RuntimeEvent::new(
            RuntimeEventLevel::Warn,
            "scid.tail_reset",
            "scid",
            "reset",
            serde_json::json!({ "offset": 100 }),
        ));
        store.record(RuntimeEvent::new(
            RuntimeEventLevel::Error,
            "depth.poll_failed",
            "depth",
            "failed",
            serde_json::json!({ "error": "join" }),
        ));

        let events = store.query(&RuntimeEventFilter {
            category: Some("scid".to_string()),
            limit: 10,
            ..RuntimeEventFilter::default()
        });
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_name, "scid.tail_reset");

        let stats = store.stats();
        assert_eq!(stats.recent_event_count, 2);
        assert!(stats.last_warning_at_ms.is_some());
        assert!(stats.last_error_at_ms.is_some());
    }

    #[test]
    fn runtime_event_level_parses_warning_alias() {
        assert_eq!(
            "warning".parse::<RuntimeEventLevel>().unwrap(),
            RuntimeEventLevel::Warn
        );
    }

    #[test]
    fn min_level_filter_includes_more_severe_events() {
        let warn = RuntimeEvent::new(
            RuntimeEventLevel::Warn,
            "scid.tail_reset",
            "scid",
            "reset",
            serde_json::json!({}),
        );
        let info = RuntimeEvent::new(
            RuntimeEventLevel::Info,
            "mcp.startup",
            "mcp",
            "started",
            serde_json::json!({}),
        );
        let filter = RuntimeEventFilter {
            min_level: Some(RuntimeEventLevel::Warn),
            limit: 10,
            ..RuntimeEventFilter::default()
        };
        assert!(event_matches_filter(&warn, &filter));
        assert!(!event_matches_filter(&info, &filter));
    }

    #[test]
    fn runtime_event_json_flattens_fields_without_stringifying_payload() {
        let event = RuntimeEvent::new(
            RuntimeEventLevel::Warn,
            "setup.transition",
            "setup",
            "transition",
            serde_json::json!({ "setupId": "abc", "nextState": "Active" }),
        );
        let logged = runtime_event_log_json(&event);
        assert_eq!(logged["setupId"].as_str(), Some("abc"));
        assert_eq!(logged["fields"]["setupId"].as_str(), Some("abc"));
        assert!(logged["fields"].is_object());
    }

    #[test]
    fn runtime_event_store_suppresses_flapping_event_names() {
        let config = LoggingConfig {
            runtime_event_suppression_window_ms: 60_000,
            destination: "none".to_string(),
            ..LoggingConfig::default()
        };
        let store = RuntimeEventStore::new(&config);
        assert!(store
            .record(RuntimeEvent::new(
                RuntimeEventLevel::Warn,
                "depth.poll_failed",
                "depth",
                "failed",
                serde_json::json!({ "attempt": 1 }),
            ))
            .is_some());
        assert!(store
            .record(RuntimeEvent::new(
                RuntimeEventLevel::Warn,
                "depth.poll_failed",
                "depth",
                "failed",
                serde_json::json!({ "attempt": 2 }),
            ))
            .is_none());
        assert_eq!(store.stats().recent_event_count, 1);
    }
}
