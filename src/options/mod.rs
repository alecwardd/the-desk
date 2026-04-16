use crate::feed::default_config_path;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_CONVEXVALUE_BASE_URL: &str = "https://convexvalue.com";
const DEFAULT_CONVEXVALUE_ROOT: &str = "SPX";
const DEFAULT_CONVEXVALUE_EMAIL_ENV: &str = "CONVEXVALUE_EMAIL";
const DEFAULT_CONVEXVALUE_PASSWORD_ENV: &str = "CONVEXVALUE_PASSWORD";
const DEFAULT_TOP_LEVELS: usize = 12;

/// Options integration settings loaded from `~/.the-desk/config.toml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_convexvalue_base_url")]
    pub convexvalue_base_url: String,
    #[serde(default = "default_convexvalue_probe_root")]
    pub convexvalue_probe_root: String,
    #[serde(default = "default_convexvalue_probe_params")]
    pub convexvalue_probe_params: Vec<String>,
    #[serde(default = "default_convexvalue_context_params")]
    pub convexvalue_context_params: Vec<String>,
    #[serde(default)]
    pub convexvalue_probe_exps: Vec<u32>,
    #[serde(default)]
    pub convexvalue_probe_range: Option<f64>,
    #[serde(default = "default_convexvalue_email_env")]
    pub convexvalue_email_env: String,
    #[serde(default = "default_convexvalue_password_env")]
    pub convexvalue_password_env: String,
    #[serde(default = "default_convexvalue_cache_ttl_ms")]
    pub convexvalue_cache_ttl_ms: u64,
}

impl Default for OptionsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            convexvalue_base_url: default_convexvalue_base_url(),
            convexvalue_probe_root: default_convexvalue_probe_root(),
            convexvalue_probe_params: default_convexvalue_probe_params(),
            convexvalue_context_params: default_convexvalue_context_params(),
            convexvalue_probe_exps: Vec::new(),
            convexvalue_probe_range: Some(0.10),
            convexvalue_email_env: default_convexvalue_email_env(),
            convexvalue_password_env: default_convexvalue_password_env(),
            convexvalue_cache_ttl_ms: default_convexvalue_cache_ttl_ms(),
        }
    }
}

fn default_convexvalue_base_url() -> String {
    DEFAULT_CONVEXVALUE_BASE_URL.to_string()
}

fn default_convexvalue_probe_root() -> String {
    DEFAULT_CONVEXVALUE_ROOT.to_string()
}

fn default_convexvalue_probe_params() -> Vec<String> {
    [
        "gxoi",
        "gxvolm",
        "gamma",
        "oi",
        "oi_ch",
        "volm_bs",
        "volm",
        "value_bs",
        "dxoi",
        "vanna",
        "vomma",
        "charm",
        "volatility",
        "delta",
        "volm_5m",
        "spread",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

fn default_convexvalue_context_params() -> Vec<String> {
    [
        "price",
        "change",
        "gxoi",
        "dxoi",
        "put_call_ratio",
        "flownet",
        "vannaxoi",
        "charmxoi",
        "value_bs",
        "volm_bs",
        "call_volume",
        "put_volume",
        "option_volume",
        "volatility",
        // flow decomposition
        "flowratio",
        "value_call_bs",
        "value_put_bs",
        "volm_call_bs",
        "volm_put_bs",
        // exposure splits
        "call_gxoi",
        "put_gxoi",
        "call_dxoi",
        "put_dxoi",
        // vol surface
        "front_volatility",
        "back_volatility",
        // premium flow
        "value_buy",
        "value_sell",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

fn default_convexvalue_email_env() -> String {
    DEFAULT_CONVEXVALUE_EMAIL_ENV.to_string()
}

fn default_convexvalue_password_env() -> String {
    DEFAULT_CONVEXVALUE_PASSWORD_ENV.to_string()
}

fn default_convexvalue_cache_ttl_ms() -> u64 {
    5 * 60 * 1000
}

#[derive(Debug, Deserialize)]
struct RootOptionsConfig {
    #[serde(default)]
    options: OptionsConfig,
}

/// Load options config from disk; fall back to defaults if missing or invalid.
pub fn load_options_config() -> OptionsConfig {
    let path = default_config_path();
    let raw = std::fs::read_to_string(path);
    match raw {
        Ok(content) => toml::from_str::<RootOptionsConfig>(&content)
            .map(|cfg| cfg.options)
            .unwrap_or_default(),
        Err(_) => OptionsConfig::default(),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OptionsError {
    #[error("HTTP client setup failed: {0}")]
    HttpClient(String),
    #[error("ConvexValue login failed ({status}): {body}")]
    LoginFailed { status: u16, body: String },
    #[error("ConvexValue request failed ({status}): {body}")]
    RequestFailed { status: u16, body: String },
    #[error("Missing credentials. Set {email_env} and {password_env}, or use --input.")]
    MissingCredentials {
        email_env: String,
        password_env: String,
    },
    #[error("Unexpected ConvexValue response: {0}")]
    InvalidResponse(String),
}

/// Minimal ConvexValue HTTP client for chain probing.
pub struct ConvexValueClient {
    base_url: String,
    client: Client,
}

impl ConvexValueClient {
    /// Build a cookie-backed client so the login session persists across requests.
    pub fn new(base_url: &str) -> Result<Self, OptionsError> {
        let client = Client::builder()
            .cookie_store(true)
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|e| OptionsError::HttpClient(e.to_string()))?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client,
        })
    }

    /// Authenticate with ConvexValue using the same session model as the Python wrapper.
    pub async fn login(&self, email: &str, password: &str) -> Result<(), OptionsError> {
        let response = self
            .client
            .post(format!("{}/api/access/login", self.base_url))
            .json(&serde_json::json!({
                "email": email,
                "password": password,
            }))
            .send()
            .await
            .map_err(|e| OptionsError::RequestFailed {
                status: 0,
                body: e.to_string(),
            })?;

        if response.status().is_success() {
            return Ok(());
        }

        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "failed to read error body".to_string());
        Err(OptionsError::LoginFailed { status, body })
    }

    /// Fetch one options chain snapshot for a root symbol.
    pub async fn get_chain(
        &self,
        root: &str,
        params: &[String],
        exps: Option<&[u32]>,
        range: Option<f64>,
    ) -> Result<Value, OptionsError> {
        let response = self
            .client
            .post(format!("{}/api/core/get/chain", self.base_url))
            .json(&serde_json::json!({
                "symbols": [root],
                "params": params,
                "exps": exps,
                "rng": range,
            }))
            .send()
            .await
            .map_err(|e| OptionsError::RequestFailed {
                status: 0,
                body: e.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "failed to read error body".to_string());
            return Err(OptionsError::RequestFailed { status, body });
        }

        response
            .json()
            .await
            .map_err(|e| OptionsError::InvalidResponse(e.to_string()))
    }

    /// Fetch aggregate underlying-level options metrics for one or more symbols.
    pub async fn get_und(
        &self,
        symbols: &[String],
        params: &[String],
    ) -> Result<Value, OptionsError> {
        let payload = serde_json::json!([{
            "Und": {
                "s": symbols,
                "v": params,
            }
        }]);
        let response = self
            .client
            .post(format!("{}/api/core/get", self.base_url))
            .json(&payload)
            .send()
            .await
            .map_err(|e| OptionsError::RequestFailed {
                status: 0,
                body: e.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "failed to read error body".to_string());
            return Err(OptionsError::RequestFailed { status, body });
        }

        response
            .json()
            .await
            .map_err(|e| OptionsError::InvalidResponse(e.to_string()))
    }
}

/// Probe output credentials resolved from environment variable names in config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionsCredentials {
    pub email: String,
    pub password: String,
}

impl OptionsCredentials {
    pub fn from_env(config: &OptionsConfig) -> Result<Self, OptionsError> {
        let email = std::env::var(&config.convexvalue_email_env).ok();
        let password = std::env::var(&config.convexvalue_password_env).ok();
        match (email, password) {
            (Some(email), Some(password)) if !email.trim().is_empty() && !password.is_empty() => {
                Ok(Self { email, password })
            }
            _ => Err(OptionsError::MissingCredentials {
                email_env: config.convexvalue_email_env.clone(),
                password_env: config.convexvalue_password_env.clone(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OptionKind {
    Call,
    Put,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConvexOptionRow {
    pub option_symbol: String,
    pub expiration: i64,
    pub strike: f64,
    pub option_kind: OptionKind,
    pub values: HashMap<String, Option<f64>>,
}

impl ConvexOptionRow {
    pub fn value(&self, name: &str) -> f64 {
        self.values.get(name).copied().flatten().unwrap_or(0.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConvexUnderlyingRow {
    pub symbol: String,
    pub values: HashMap<String, Option<f64>>,
}

impl ConvexUnderlyingRow {
    pub fn value(&self, name: &str) -> Option<f64> {
        self.values.get(name).copied().flatten()
    }
}

/// Aggregated strike-level gamma concentration summary derived from ConvexValue fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GammaLevel {
    pub strike: f64,
    pub total_gxoi: f64,
    pub call_gxoi: f64,
    pub put_gxoi: f64,
    pub total_gxvolm: f64,
    pub call_open_interest: f64,
    pub put_open_interest: f64,
    pub total_oi_change: f64,
    pub call_oi_change: f64,
    pub put_oi_change: f64,
    pub total_volume: f64,
    pub net_volume_bias: f64,
    pub net_value_bias: f64,
    pub total_vomma: f64,
    pub total_volm_5m: f64,
    pub avg_spread: f64,
    pub total_contracts_seen: usize,
    pub expiration_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GammaLevelsReport {
    pub root: String,
    pub requested_params: Vec<String>,
    pub total_rows: usize,
    pub distinct_expirations: usize,
    pub distinct_strikes: usize,
    pub top_gamma_concentration_levels: Vec<GammaLevel>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OptionsContextReport {
    pub root: String,
    pub requested_params: Vec<String>,
    pub price: Option<f64>,
    pub change: Option<f64>,
    pub aggregate_gxoi: Option<f64>,
    pub aggregate_dxoi: Option<f64>,
    pub put_call_ratio: Option<f64>,
    pub flow_net: Option<f64>,
    pub net_value_bias: Option<f64>,
    pub net_volume_bias: Option<f64>,
    pub total_vanna_xoi: Option<f64>,
    pub total_charm_xoi: Option<f64>,
    pub call_volume: Option<f64>,
    pub put_volume: Option<f64>,
    pub option_volume: Option<f64>,
    pub implied_volatility: Option<f64>,
    // flow decomposition
    pub flow_ratio: Option<f64>,
    pub value_call_bs: Option<f64>,
    pub value_put_bs: Option<f64>,
    pub volm_call_bs: Option<f64>,
    pub volm_put_bs: Option<f64>,
    // exposure splits
    pub call_gxoi: Option<f64>,
    pub put_gxoi: Option<f64>,
    pub call_dxoi: Option<f64>,
    pub put_dxoi: Option<f64>,
    // vol surface
    pub front_volatility: Option<f64>,
    pub back_volatility: Option<f64>,
    pub vol_term_spread: Option<f64>,
    // premium flow
    pub value_buy: Option<f64>,
    pub value_sell: Option<f64>,
    // regimes
    pub gamma_regime: Option<String>,
    pub dex_regime: Option<String>,
    pub vanna_regime: Option<String>,
    pub charm_regime: Option<String>,
    pub flow_direction: Option<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OptionsSnapshot {
    pub root: String,
    pub requested_exps: Option<Vec<u32>>,
    pub requested_range: Option<f64>,
    pub fetched_at_ms: f64,
    pub cache_ttl_ms: u64,
    pub chain_params: Vec<String>,
    pub context_params: Vec<String>,
    pub gamma_levels: GammaLevelsReport,
    pub context: OptionsContextReport,
}

impl OptionsSnapshot {
    pub fn age_ms(&self, now_ms: f64) -> f64 {
        (now_ms - self.fetched_at_ms).max(0.0)
    }

    pub fn is_fresh(&self, now_ms: f64) -> bool {
        self.age_ms(now_ms) <= self.cache_ttl_ms as f64
    }

    pub fn matches_request(
        &self,
        root: &str,
        exps: &[u32],
        range: Option<f64>,
        chain_params: &[String],
        context_params: &[String],
    ) -> bool {
        self.root.eq_ignore_ascii_case(root)
            && self.requested_exps.as_deref().unwrap_or(&[]) == exps
            && normalized_range_key(self.requested_range) == normalized_range_key(range)
            && self.chain_params == chain_params
            && self.context_params == context_params
    }
}

#[derive(Debug, Default)]
struct GammaAccumulator {
    strike: f64,
    total_gxoi: f64,
    call_gxoi: f64,
    put_gxoi: f64,
    total_gxvolm: f64,
    call_open_interest: f64,
    put_open_interest: f64,
    total_oi_change: f64,
    call_oi_change: f64,
    put_oi_change: f64,
    total_volume: f64,
    net_volume_bias: f64,
    net_value_bias: f64,
    total_vomma: f64,
    total_volm_5m: f64,
    spread_sum: f64,
    spread_count: usize,
    total_contracts_seen: usize,
    expirations: HashSet<i64>,
}

impl GammaAccumulator {
    fn into_level(self) -> GammaLevel {
        let avg_spread = if self.spread_count > 0 {
            self.spread_sum / self.spread_count as f64
        } else {
            0.0
        };
        GammaLevel {
            strike: self.strike,
            total_gxoi: self.total_gxoi,
            call_gxoi: self.call_gxoi,
            put_gxoi: self.put_gxoi,
            total_gxvolm: self.total_gxvolm,
            call_open_interest: self.call_open_interest,
            put_open_interest: self.put_open_interest,
            total_oi_change: self.total_oi_change,
            call_oi_change: self.call_oi_change,
            put_oi_change: self.put_oi_change,
            total_volume: self.total_volume,
            net_volume_bias: self.net_volume_bias,
            net_value_bias: self.net_value_bias,
            total_vomma: self.total_vomma,
            total_volm_5m: self.total_volm_5m,
            avg_spread,
            total_contracts_seen: self.total_contracts_seen,
            expiration_count: self.expirations.len(),
        }
    }
}

fn strike_key(strike: f64) -> i64 {
    (strike * 100.0).round() as i64
}

fn normalized_range_key(range: Option<f64>) -> Option<i64> {
    range.map(|value| (value * 1_000_000.0).round() as i64)
}

fn regime_from_sign(value: Option<f64>) -> Option<String> {
    value.map(|metric| {
        if metric > 0.0 {
            "positive".to_string()
        } else if metric < 0.0 {
            "negative".to_string()
        } else {
            "neutral".to_string()
        }
    })
}

fn flow_direction_from_metrics(
    flow_net: Option<f64>,
    value_bias: Option<f64>,
    volume_bias: Option<f64>,
) -> Option<String> {
    let signal = flow_net.or(value_bias).or(volume_bias)?;
    Some(if signal > 0.0 {
        "net_buying".to_string()
    } else if signal < 0.0 {
        "net_selling".to_string()
    } else {
        "balanced".to_string()
    })
}

fn now_timestamp_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as f64)
        .unwrap_or(0.0)
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn required_array<'a>(value: &'a Value, context: &str) -> Result<&'a Vec<Value>, OptionsError> {
    value
        .as_array()
        .ok_or_else(|| OptionsError::InvalidResponse(format!("missing array for {context}")))
}

fn parse_side_row(
    values: &[Value],
    expiration: i64,
    strike: f64,
    option_kind: OptionKind,
    params: &[String],
) -> Result<ConvexOptionRow, OptionsError> {
    if values.is_empty() {
        return Err(OptionsError::InvalidResponse(
            "option row is missing symbol".to_string(),
        ));
    }

    let option_symbol = values
        .first()
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    let mut mapped = HashMap::new();
    for (idx, param) in params.iter().enumerate() {
        mapped.insert(param.clone(), values.get(idx + 1).and_then(value_to_f64));
    }

    Ok(ConvexOptionRow {
        option_symbol,
        expiration,
        strike,
        option_kind,
        values: mapped,
    })
}

/// Parse the nested ConvexValue chain payload into flat rows.
pub fn parse_chain_rows(
    raw: &Value,
    params: &[String],
) -> Result<Vec<ConvexOptionRow>, OptionsError> {
    let data = required_array(
        raw.get("data")
            .ok_or_else(|| OptionsError::InvalidResponse("missing data field".to_string()))?,
        "data",
    )?;
    let first = data
        .first()
        .ok_or_else(|| OptionsError::InvalidResponse("data array is empty".to_string()))?;
    let chain = required_array(
        first
            .get("chain")
            .ok_or_else(|| OptionsError::InvalidResponse("missing chain field".to_string()))?,
        "chain",
    )?;

    let mut rows = Vec::new();
    for expiration_block in chain {
        let expiration_values = required_array(expiration_block, "expiration block")?;
        if expiration_values.len() < 2 {
            return Err(OptionsError::InvalidResponse(
                "expiration block must contain expiration and strike rows".to_string(),
            ));
        }

        let expiration = value_to_f64(&expiration_values[0])
            .ok_or_else(|| OptionsError::InvalidResponse("expiration is not numeric".to_string()))?
            as i64;
        let strike_rows = required_array(&expiration_values[1], "strike rows")?;

        for strike_row in strike_rows {
            let strike_values = required_array(strike_row, "strike row")?;
            if strike_values.len() < 3 {
                return Err(OptionsError::InvalidResponse(
                    "strike row must contain strike, call, and put".to_string(),
                ));
            }

            let strike = value_to_f64(&strike_values[0]).ok_or_else(|| {
                OptionsError::InvalidResponse("strike is not numeric".to_string())
            })?;
            let call_values = required_array(&strike_values[1], "call row")?;
            let put_values = required_array(&strike_values[2], "put row")?;

            rows.push(parse_side_row(
                call_values,
                expiration,
                strike,
                OptionKind::Call,
                params,
            )?);
            rows.push(parse_side_row(
                put_values,
                expiration,
                strike,
                OptionKind::Put,
                params,
            )?);
        }
    }

    Ok(rows)
}

/// Parse the ConvexValue underlying aggregate payload into flat symbol rows.
pub fn parse_underlying_rows(
    raw: &Value,
    params: &[String],
) -> Result<Vec<ConvexUnderlyingRow>, OptionsError> {
    let data = required_array(
        raw.get("data")
            .ok_or_else(|| OptionsError::InvalidResponse("missing data field".to_string()))?,
        "data",
    )?;

    let mut rows = Vec::new();
    for response_group in data {
        let group_rows = required_array(response_group, "underlying response group")?;
        for row in group_rows {
            let values = required_array(row, "underlying row")?;
            if values.is_empty() {
                return Err(OptionsError::InvalidResponse(
                    "underlying row is missing symbol".to_string(),
                ));
            }

            let symbol = values
                .first()
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let mut mapped = HashMap::new();
            for (idx, param) in params.iter().enumerate() {
                mapped.insert(param.clone(), values.get(idx + 1).and_then(value_to_f64));
            }

            rows.push(ConvexUnderlyingRow {
                symbol,
                values: mapped,
            });
        }
    }

    Ok(rows)
}

/// Aggregate chain rows into strike-level gamma concentration rankings.
pub fn build_gamma_levels_report(
    root: &str,
    requested_params: &[String],
    rows: &[ConvexOptionRow],
    top_n: Option<usize>,
) -> GammaLevelsReport {
    let mut by_strike: BTreeMap<i64, GammaAccumulator> = BTreeMap::new();
    let mut expirations = HashSet::new();

    for row in rows {
        expirations.insert(row.expiration);
        let entry = by_strike
            .entry(strike_key(row.strike))
            .or_insert_with(|| GammaAccumulator {
                strike: row.strike,
                ..GammaAccumulator::default()
            });

        entry.total_gxoi += row.value("gxoi");
        entry.total_gxvolm += row.value("gxvolm");
        entry.total_volume += row.value("volm");
        entry.net_volume_bias += row.value("volm_bs");
        entry.net_value_bias += row.value("value_bs");
        entry.total_vomma += row.value("vomma");
        entry.total_volm_5m += row.value("volm_5m");
        let spread = row.value("spread");
        if spread > 0.0 {
            entry.spread_sum += spread;
            entry.spread_count += 1;
        }
        entry.total_contracts_seen += 1;
        entry.expirations.insert(row.expiration);

        let oi_ch = row.value("oi_ch");
        entry.total_oi_change += oi_ch;
        match row.option_kind {
            OptionKind::Call => {
                entry.call_gxoi += row.value("gxoi");
                entry.call_open_interest += row.value("oi");
                entry.call_oi_change += oi_ch;
            }
            OptionKind::Put => {
                entry.put_gxoi += row.value("gxoi");
                entry.put_open_interest += row.value("oi");
                entry.put_oi_change += oi_ch;
            }
        }
    }

    let mut levels: Vec<GammaLevel> = by_strike
        .into_values()
        .map(GammaAccumulator::into_level)
        .collect();
    levels.sort_by(|a, b| {
        b.total_gxoi
            .abs()
            .partial_cmp(&a.total_gxoi.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                b.total_gxvolm
                    .abs()
                    .partial_cmp(&a.total_gxvolm.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    levels.truncate(top_n.unwrap_or(DEFAULT_TOP_LEVELS));

    GammaLevelsReport {
        root: root.to_string(),
        requested_params: requested_params.to_vec(),
        total_rows: rows.len(),
        distinct_expirations: expirations.len(),
        distinct_strikes: rows
            .iter()
            .map(|row| strike_key(row.strike))
            .collect::<HashSet<_>>()
            .len(),
        top_gamma_concentration_levels: levels,
        notes: vec![
            "This report ranks gamma concentration using ConvexValue's gxoi/gxvolm fields.".to_string(),
            "It is a fast live overlay for strike importance, not a signed dealer-GEX model.".to_string(),
            "Gamma flip and long-vs-short dealer regime still require explicit sign methodology or a richer provider.".to_string(),
        ],
    }
}

/// Build a high-level options regime summary from ConvexValue's underlying aggregate fields.
pub fn build_options_context_report(
    root: &str,
    requested_params: &[String],
    rows: &[ConvexUnderlyingRow],
) -> Result<OptionsContextReport, OptionsError> {
    let row = rows
        .iter()
        .find(|row| row.symbol.eq_ignore_ascii_case(root))
        .or_else(|| rows.first())
        .ok_or_else(|| {
            OptionsError::InvalidResponse("underlying data array is empty".to_string())
        })?;
    let flow_net = row.value("flownet");
    let net_value_bias = row.value("value_bs");
    let net_volume_bias = row.value("volm_bs");

    let front_vol = row.value("front_volatility");
    let back_vol = row.value("back_volatility");
    let vol_term_spread = match (front_vol, back_vol) {
        (Some(f), Some(b)) => Some(f - b),
        _ => None,
    };

    Ok(OptionsContextReport {
        root: row.symbol.clone(),
        requested_params: requested_params.to_vec(),
        price: row.value("price"),
        change: row.value("change"),
        aggregate_gxoi: row.value("gxoi"),
        aggregate_dxoi: row.value("dxoi"),
        put_call_ratio: row.value("put_call_ratio"),
        flow_net,
        net_value_bias,
        net_volume_bias,
        total_vanna_xoi: row.value("vannaxoi"),
        total_charm_xoi: row.value("charmxoi"),
        call_volume: row.value("call_volume"),
        put_volume: row.value("put_volume"),
        option_volume: row.value("option_volume"),
        implied_volatility: row.value("volatility"),
        // flow decomposition
        flow_ratio: row.value("flowratio"),
        value_call_bs: row.value("value_call_bs"),
        value_put_bs: row.value("value_put_bs"),
        volm_call_bs: row.value("volm_call_bs"),
        volm_put_bs: row.value("volm_put_bs"),
        // exposure splits
        call_gxoi: row.value("call_gxoi"),
        put_gxoi: row.value("put_gxoi"),
        call_dxoi: row.value("call_dxoi"),
        put_dxoi: row.value("put_dxoi"),
        // vol surface
        front_volatility: front_vol,
        back_volatility: back_vol,
        vol_term_spread,
        // premium flow
        value_buy: row.value("value_buy"),
        value_sell: row.value("value_sell"),
        // regimes
        gamma_regime: regime_from_sign(row.value("gxoi")),
        dex_regime: regime_from_sign(row.value("dxoi")),
        vanna_regime: regime_from_sign(row.value("vannaxoi")),
        charm_regime: regime_from_sign(row.value("charmxoi")),
        flow_direction: flow_direction_from_metrics(flow_net, net_value_bias, net_volume_bias),
        notes: vec![
            "Aggregate gxoi/dxoi/vannaxoi/charmxoi are returned directly by ConvexValue.".to_string(),
            "flowDirection is inferred from flownet first, then value_bs, then volm_bs when flownet is missing.".to_string(),
            "volTermSpread = frontVolatility - backVolatility; positive means front-month IV elevated (near-term fear).".to_string(),
        ],
    })
}

async fn fetch_options_snapshot_once(
    config: &OptionsConfig,
    credentials: &OptionsCredentials,
    root: &str,
    exps: Option<&[u32]>,
    range: Option<f64>,
) -> Result<OptionsSnapshot, OptionsError> {
    let client = ConvexValueClient::new(&config.convexvalue_base_url)?;
    client
        .login(&credentials.email, &credentials.password)
        .await?;

    let chain_raw = client
        .get_chain(root, &config.convexvalue_probe_params, exps, range)
        .await?;
    let underlying_raw = client
        .get_und(&[root.to_string()], &config.convexvalue_context_params)
        .await?;

    let chain_rows = parse_chain_rows(&chain_raw, &config.convexvalue_probe_params)?;
    let underlying_rows =
        parse_underlying_rows(&underlying_raw, &config.convexvalue_context_params)?;
    let gamma_levels = build_gamma_levels_report(
        root,
        &config.convexvalue_probe_params,
        &chain_rows,
        Some(usize::MAX),
    );
    let context =
        build_options_context_report(root, &config.convexvalue_context_params, &underlying_rows)?;

    Ok(OptionsSnapshot {
        root: root.to_string(),
        requested_exps: exps
            .map(|values| values.to_vec())
            .filter(|values| !values.is_empty()),
        requested_range: range,
        fetched_at_ms: now_timestamp_ms(),
        cache_ttl_ms: config.convexvalue_cache_ttl_ms,
        chain_params: config.convexvalue_probe_params.clone(),
        context_params: config.convexvalue_context_params.clone(),
        gamma_levels,
        context,
    })
}

/// Fetch a combined options snapshot, retrying once on session-auth failures.
pub async fn fetch_options_snapshot(
    config: &OptionsConfig,
    credentials: &OptionsCredentials,
    root: &str,
    exps: Option<&[u32]>,
    range: Option<f64>,
) -> Result<OptionsSnapshot, OptionsError> {
    match fetch_options_snapshot_once(config, credentials, root, exps, range).await {
        Ok(snapshot) => Ok(snapshot),
        Err(OptionsError::LoginFailed { status: 401, .. })
        | Err(OptionsError::RequestFailed { status: 401, .. }) => {
            fetch_options_snapshot_once(config, credentials, root, exps, range).await
        }
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_convex_chain_rows() {
        let params = vec![
            "gxoi".to_string(),
            "gxvolm".to_string(),
            "oi".to_string(),
            "volm_bs".to_string(),
        ];
        let raw = json!({
            "data": [
                {
                    "chain": [
                        [
                            20260417,
                            [
                                [
                                    5200.0,
                                    ["SPXW240417C05200000", 1200.0, 22.0, 500.0, 15.0],
                                    ["SPXW240417P05200000", 900.0, 18.0, 420.0, -9.0]
                                ]
                            ]
                        ]
                    ]
                }
            ]
        });

        let rows = parse_chain_rows(&raw, &params).expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].option_kind, OptionKind::Call);
        assert_eq!(rows[0].strike, 5200.0);
        assert_eq!(rows[0].value("gxoi"), 1200.0);
        assert_eq!(rows[1].option_kind, OptionKind::Put);
        assert_eq!(rows[1].value("volm_bs"), -9.0);
    }

    #[test]
    fn ranks_gamma_levels_by_total_gxoi() {
        // params: gxoi, gxvolm, oi, oi_ch, volm_bs, volm, value_bs, vomma, volm_5m, spread
        let params = vec![
            "gxoi".to_string(),
            "gxvolm".to_string(),
            "oi".to_string(),
            "oi_ch".to_string(),
            "volm_bs".to_string(),
            "volm".to_string(),
            "value_bs".to_string(),
            "vomma".to_string(),
            "volm_5m".to_string(),
            "spread".to_string(),
        ];
        let raw = json!({
            "data": [
                {
                    "chain": [
                        [
                            20260417,
                            [
                                [
                                    5200.0,
                                    ["C1", 1200.0, 18.0, 500.0, 30.0, 12.0, 50.0, 2000.0, 5.5, 20.0, 1.20],
                                    ["P1", 900.0, 10.0, 430.0, -15.0, -6.0, 40.0, -1000.0, 3.2, 10.0, 1.50]
                                ],
                                [
                                    5300.0,
                                    ["C2", 200.0, 8.0, 110.0, 5.0, 3.0, 10.0, 300.0, 1.1, 4.0, 2.00],
                                    ["P2", 150.0, 7.0, 95.0, -3.0, -2.0, 8.0, -250.0, 0.8, 3.0, 2.30]
                                ]
                            ]
                        ]
                    ]
                }
            ]
        });

        let rows = parse_chain_rows(&raw, &params).expect("rows");
        let report = build_gamma_levels_report("SPX", &params, &rows, Some(2));
        assert_eq!(report.top_gamma_concentration_levels.len(), 2);
        let top = &report.top_gamma_concentration_levels[0];
        assert_eq!(top.strike, 5200.0);
        assert_eq!(top.total_gxoi, 2100.0);
        assert_eq!(top.net_volume_bias, 6.0);
        // new fields
        assert_eq!(top.total_oi_change, 15.0); // 30 + (-15)
        assert_eq!(top.call_oi_change, 30.0);
        assert_eq!(top.put_oi_change, -15.0);
        assert!((top.total_vomma - 8.7).abs() < 0.01); // 5.5 + 3.2
        assert!((top.total_volm_5m - 30.0).abs() < 0.01); // 20 + 10
        assert!((top.avg_spread - 1.35).abs() < 0.01); // (1.20 + 1.50) / 2
    }

    #[test]
    fn parses_underlying_rows() {
        let params = vec![
            "price".to_string(),
            "gxoi".to_string(),
            "dxoi".to_string(),
            "put_call_ratio".to_string(),
        ];
        let raw = json!({
            "data": [
                [
                    ["SPX", 6123.25, -145.0, 9988.0, 1.12]
                ]
            ]
        });

        let rows = parse_underlying_rows(&raw, &params).expect("underlying rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol, "SPX");
        assert_eq!(rows[0].value("gxoi"), Some(-145.0));
        assert_eq!(rows[0].value("put_call_ratio"), Some(1.12));
    }

    #[test]
    fn builds_options_context_report() {
        let params = vec![
            "price".to_string(),
            "gxoi".to_string(),
            "dxoi".to_string(),
            "put_call_ratio".to_string(),
            "flownet".to_string(),
            "vannaxoi".to_string(),
            "charmxoi".to_string(),
            "flowratio".to_string(),
            "value_call_bs".to_string(),
            "value_put_bs".to_string(),
            "call_gxoi".to_string(),
            "put_gxoi".to_string(),
            "call_dxoi".to_string(),
            "put_dxoi".to_string(),
            "front_volatility".to_string(),
            "back_volatility".to_string(),
            "value_buy".to_string(),
            "value_sell".to_string(),
        ];
        let rows = vec![ConvexUnderlyingRow {
            symbol: "SPX".to_string(),
            values: HashMap::from([
                ("price".to_string(), Some(6123.25)),
                ("gxoi".to_string(), Some(-145.0)),
                ("dxoi".to_string(), Some(9988.0)),
                ("put_call_ratio".to_string(), Some(1.12)),
                ("flownet".to_string(), Some(-250_000.0)),
                ("vannaxoi".to_string(), Some(450.0)),
                ("charmxoi".to_string(), Some(-75.0)),
                ("flowratio".to_string(), Some(0.85)),
                ("value_call_bs".to_string(), Some(120_000.0)),
                ("value_put_bs".to_string(), Some(-180_000.0)),
                ("call_gxoi".to_string(), Some(-50.0)),
                ("put_gxoi".to_string(), Some(-95.0)),
                ("call_dxoi".to_string(), Some(6000.0)),
                ("put_dxoi".to_string(), Some(3988.0)),
                ("front_volatility".to_string(), Some(22.5)),
                ("back_volatility".to_string(), Some(19.8)),
                ("value_buy".to_string(), Some(500_000.0)),
                ("value_sell".to_string(), Some(750_000.0)),
            ]),
        }];

        let report = build_options_context_report("SPX", &params, &rows).expect("context");
        assert_eq!(report.gamma_regime.as_deref(), Some("negative"));
        assert_eq!(report.dex_regime.as_deref(), Some("positive"));
        assert_eq!(report.flow_direction.as_deref(), Some("net_selling"));
        assert_eq!(report.put_call_ratio, Some(1.12));
        // new fields
        assert_eq!(report.flow_ratio, Some(0.85));
        assert_eq!(report.value_call_bs, Some(120_000.0));
        assert_eq!(report.value_put_bs, Some(-180_000.0));
        assert_eq!(report.call_gxoi, Some(-50.0));
        assert_eq!(report.put_gxoi, Some(-95.0));
        assert_eq!(report.call_dxoi, Some(6000.0));
        assert_eq!(report.put_dxoi, Some(3988.0));
        assert_eq!(report.front_volatility, Some(22.5));
        assert_eq!(report.back_volatility, Some(19.8));
        assert!((report.vol_term_spread.unwrap() - 2.7).abs() < 0.01);
        assert_eq!(report.value_buy, Some(500_000.0));
        assert_eq!(report.value_sell, Some(750_000.0));
    }
}
