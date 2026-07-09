use super::FeedConfig;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

const FUTURES_MONTH_CODES: &str = "FGHJKMNQUVXZ";

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SymbolMode {
    Manual,
    Auto,
    #[default]
    Hybrid,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContractMetadata {
    pub root_symbol: String,
    pub contract_symbol: String,
    pub contract_month: Option<String>,
    pub expiry_year_month: Option<String>,
    pub symbol_resolution_mode: String,
    pub symbol_resolution_source: String,
    pub configured_symbol: String,
    pub active_symbol_override: Option<String>,
    pub scid_path: String,
    pub scid_file_exists: bool,
    pub depth_prefix: String,
    pub depth_file_count: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
struct SymbolCandidate {
    symbol: String,
    modified_ms: f64,
    path: PathBuf,
}

pub fn symbol_to_scid_file(symbol: &str) -> String {
    let trimmed = symbol.trim();
    if trimmed.to_ascii_lowercase().ends_with(".scid") {
        trimmed.to_string()
    } else {
        format!("{trimmed}.scid")
    }
}

pub fn infer_root_symbol(symbol: &str) -> String {
    let normalized = strip_known_suffixes(symbol);
    let trimmed_digits = normalized.trim_end_matches(|c: char| c.is_ascii_digit());
    let removed_year_digits = trimmed_digits.len() != normalized.len();
    let Some((last_idx, last_char)) = trimmed_digits.char_indices().last() else {
        return normalized;
    };
    if removed_year_digits && FUTURES_MONTH_CODES.contains(last_char) && last_idx > 0 {
        trimmed_digits[..last_idx].to_string()
    } else {
        trimmed_digits.to_string()
    }
}

pub fn infer_contract_month(symbol: &str) -> Option<String> {
    let normalized = strip_known_suffixes(symbol);
    let root = infer_root_symbol(&normalized);
    if normalized.len() <= root.len() {
        return None;
    }
    let suffix = &normalized[root.len()..];
    let mut chars = suffix.chars();
    let month_code = chars.next()?;
    if !FUTURES_MONTH_CODES.contains(month_code) {
        return None;
    }
    let month = match month_code {
        'F' => 1,
        'G' => 2,
        'H' => 3,
        'J' => 4,
        'K' => 5,
        'M' => 6,
        'N' => 7,
        'Q' => 8,
        'U' => 9,
        'V' => 10,
        'X' => 11,
        'Z' => 12,
        _ => return None,
    };
    let year_raw: String = chars.collect();
    let year = match year_raw.len() {
        1 => 2020 + year_raw.parse::<i32>().ok()?,
        2 => 2000 + year_raw.parse::<i32>().ok()?,
        4 => year_raw.parse::<i32>().ok()?,
        _ => return None,
    };
    Some(format!("{year:04}-{month:02}"))
}

pub fn resolve_contract_metadata(config: &FeedConfig) -> ContractMetadata {
    let root_symbol = if !config.base_symbol.trim().is_empty() {
        config.base_symbol.trim().to_string()
    } else if !config.symbol.trim().is_empty() {
        infer_root_symbol(&config.symbol)
    } else {
        "NQ".to_string()
    };
    let configured_symbol = config.effective_configured_symbol();
    let override_symbol = config
        .active_symbol_override
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);

    let scid_candidates = discover_scid_candidates(&config.sierra_data_dir, &root_symbol);
    let freshest_scid = scid_candidates.first().cloned();
    let auto_symbol = freshest_scid
        .as_ref()
        .map(|candidate| candidate.symbol.clone())
        .unwrap_or_else(|| configured_symbol.clone());

    let mut warnings = Vec::new();
    let (contract_symbol, source) = match config.symbol_mode {
        SymbolMode::Manual => (
            override_symbol
                .clone()
                .unwrap_or_else(|| configured_symbol.clone()),
            if override_symbol.is_some() {
                "manual_override".to_string()
            } else {
                "configured_symbol".to_string()
            },
        ),
        SymbolMode::Auto => {
            if freshest_scid.is_none() {
                warnings.push(format!(
                    "No .scid candidates found for root symbol {root_symbol}; falling back to configured symbol {configured_symbol}."
                ));
            }
            (auto_symbol.clone(), "auto_detected".to_string())
        }
        SymbolMode::Hybrid => {
            if let Some(override_symbol) = override_symbol.clone() {
                if scid_path_for_symbol(&config.sierra_data_dir, &override_symbol).exists() {
                    if auto_symbol != override_symbol {
                        warnings.push(format!(
                            "Manual override {override_symbol} differs from freshest auto-detected candidate {auto_symbol}."
                        ));
                    }
                    (override_symbol, "manual_override".to_string())
                } else {
                    warnings.push(format!(
                        "Manual override {} has no matching .scid file; falling back to auto-detected candidate {}.",
                        override_symbol, auto_symbol
                    ));
                    (auto_symbol.clone(), "auto_detected".to_string())
                }
            } else {
                if freshest_scid.is_none() {
                    warnings.push(format!(
                        "No .scid candidates found for root symbol {root_symbol}; falling back to configured symbol {configured_symbol}."
                    ));
                } else if configured_symbol != auto_symbol {
                    warnings.push(format!(
                        "Configured symbol {configured_symbol} is not the freshest candidate; auto-detected {auto_symbol}."
                    ));
                }
                (auto_symbol.clone(), "auto_detected".to_string())
            }
        }
    };

    let scid_path = scid_path_for_symbol(&config.sierra_data_dir, &contract_symbol);
    let scid_file_exists = scid_path.exists();
    if !scid_file_exists {
        warnings.push(format!(
            "Resolved SCID file is missing for contract symbol {}.",
            contract_symbol
        ));
    }

    let depth_count = discover_depth_file_count(&config.sierra_data_dir, &contract_symbol);
    if depth_count == 0 {
        warnings.push(format!(
            "No MarketDepthData files found for resolved contract symbol {}.",
            contract_symbol
        ));
    }

    ContractMetadata {
        root_symbol,
        contract_symbol: contract_symbol.clone(),
        contract_month: infer_contract_month(&contract_symbol),
        expiry_year_month: infer_contract_month(&contract_symbol),
        symbol_resolution_mode: format!("{:?}", config.symbol_mode).to_ascii_lowercase(),
        symbol_resolution_source: source,
        configured_symbol,
        active_symbol_override: override_symbol,
        scid_path: scid_path.to_string_lossy().to_string(),
        scid_file_exists,
        depth_prefix: contract_symbol,
        depth_file_count: depth_count,
        warnings,
    }
}

fn scid_path_for_symbol(sierra_data_dir: &str, symbol: &str) -> PathBuf {
    PathBuf::from(sierra_data_dir).join(symbol_to_scid_file(symbol))
}

/// Build [`ContractMetadata`] for an explicit contract symbol, independent of
/// the live `active_symbol_override`.
///
/// Used by backtest routing: a historical replay can pin the contract that was
/// front during its window (e.g. `NQH6.CME`) without mutating global feed
/// config, so concurrent live trading stays on the current front month.
pub fn resolve_contract_metadata_for_symbol(config: &FeedConfig, symbol: &str) -> ContractMetadata {
    let contract_symbol = symbol.trim().to_string();
    let root_symbol = infer_root_symbol(&contract_symbol);
    let scid_path = scid_path_for_symbol(&config.sierra_data_dir, &contract_symbol);
    let scid_file_exists = scid_path.exists();
    let depth_count = discover_depth_file_count(&config.sierra_data_dir, &contract_symbol);
    let mut warnings = Vec::new();
    if !scid_file_exists {
        warnings.push(format!(
            "Resolved SCID file is missing for backtest contract symbol {contract_symbol}."
        ));
    }
    ContractMetadata {
        root_symbol,
        contract_symbol: contract_symbol.clone(),
        contract_month: infer_contract_month(&contract_symbol),
        expiry_year_month: infer_contract_month(&contract_symbol),
        symbol_resolution_mode: "manual".to_string(),
        symbol_resolution_source: "backtest_contract_override".to_string(),
        configured_symbol: contract_symbol.clone(),
        active_symbol_override: Some(contract_symbol.clone()),
        scid_path: scid_path.to_string_lossy().to_string(),
        scid_file_exists,
        depth_prefix: contract_symbol,
        depth_file_count: depth_count,
        warnings,
    }
}

fn discover_scid_candidates(sierra_data_dir: &str, root_symbol: &str) -> Vec<SymbolCandidate> {
    let dir = PathBuf::from(sierra_data_dir);
    let Ok(entries) = fs::read_dir(&dir) else {
        return Vec::new();
    };
    let root_lower = root_symbol.to_ascii_lowercase();
    let mut candidates = entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?.to_string();
            if !name.to_ascii_lowercase().ends_with(".scid") {
                return None;
            }
            let symbol = name.trim_end_matches(".scid").trim_end_matches(".SCID");
            if !symbol.to_ascii_lowercase().starts_with(&root_lower) {
                return None;
            }
            let modified_ms = modified_ms(&path).unwrap_or(0.0);
            Some(SymbolCandidate {
                symbol: symbol.to_string(),
                modified_ms,
                path,
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|a, b| {
        b.modified_ms
            .partial_cmp(&a.modified_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                let sa = fs::metadata(&a.path).map(|m| m.len()).unwrap_or(0);
                let sb = fs::metadata(&b.path).map(|m| m.len()).unwrap_or(0);
                sb.cmp(&sa)
            })
            .then_with(|| a.symbol.cmp(&b.symbol))
    });
    candidates
}

fn discover_depth_file_count(sierra_data_dir: &str, contract_symbol: &str) -> usize {
    let dir = PathBuf::from(sierra_data_dir).join("MarketDepthData");
    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };
    let prefix = format!("{}.", contract_symbol.to_ascii_lowercase());
    entries
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .map(|name| {
                    let lower = name.to_ascii_lowercase();
                    lower.starts_with(&prefix) && lower.ends_with(".depth")
                })
                .unwrap_or(false)
        })
        .count()
}

fn modified_ms(path: &Path) -> Option<f64> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    modified
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis() as f64)
}

fn strip_known_suffixes(symbol: &str) -> String {
    let trimmed = symbol
        .trim()
        .trim_end_matches(".scid")
        .trim_end_matches(".SCID");
    trimmed
        .split('.')
        .next()
        .unwrap_or(trimmed)
        .trim()
        .to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infers_root_symbol_from_contract_symbol() {
        assert_eq!(infer_root_symbol("NQM26.CME"), "NQ");
        assert_eq!(infer_root_symbol("ESH6"), "ES");
        assert_eq!(infer_root_symbol("NQ"), "NQ");
    }

    #[test]
    fn infers_contract_month_from_symbol() {
        assert_eq!(
            infer_contract_month("NQM26.CME").as_deref(),
            Some("2026-06")
        );
        assert_eq!(infer_contract_month("ESH6").as_deref(), Some("2026-03"));
        assert_eq!(infer_contract_month("NQ").as_deref(), None);
    }

    #[test]
    fn builds_metadata_for_explicit_backtest_symbol() {
        let config = FeedConfig {
            sierra_data_dir: "C:/nonexistent-test-dir".to_string(),
            ..FeedConfig::default()
        };
        let meta = resolve_contract_metadata_for_symbol(&config, "  NQH6.CME  ");
        // Trims input and derives root/month from the symbol.
        assert_eq!(meta.contract_symbol, "NQH6.CME");
        assert_eq!(meta.root_symbol, "NQ");
        assert_eq!(meta.contract_month.as_deref(), Some("2026-03"));
        // Pins the explicit contract without claiming live-config provenance.
        assert_eq!(meta.symbol_resolution_source, "backtest_contract_override");
        assert_eq!(meta.active_symbol_override.as_deref(), Some("NQH6.CME"));
        assert!(meta.scid_path.ends_with("NQH6.CME.scid"));
        // Missing file is reported, not silently accepted.
        assert!(!meta.scid_file_exists);
        assert!(meta.warnings.iter().any(|w| w.contains("NQH6.CME")));
    }
}
