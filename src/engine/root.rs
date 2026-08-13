//! MarketRouter v0 roots: exactly **NQ** and **ES**.
//!
//! MES/MNQ and any other root are out of scope for SIL-M2b. Session scope
//! (RTH / Globex) is classified per tick on the owning symbol's pipeline —
//! never shared across roots.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::feed::ContractMetadata;

/// Supported MarketRouter v0 root symbols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum RouterRoot {
    /// Nasdaq-100 mini (primary coaching default).
    Nq,
    /// S&P 500 mini.
    Es,
}

impl RouterRoot {
    /// The two roots hosted by MarketRouter v0, in apply-tie-break order (NQ then ES).
    pub const ALL: [Self; 2] = [Self::Nq, Self::Es];

    /// Wire / catalog label (`NQ` / `ES`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nq => "NQ",
            Self::Es => "ES",
        }
    }

    /// Parse a requested root. Micros and unknown roots are rejected.
    pub fn parse(raw: &str) -> Result<Self, RouterRootError> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "NQ" => Ok(Self::Nq),
            "ES" => Ok(Self::Es),
            "MNQ" | "MES" => Err(RouterRootError::MicroNotInScope(raw.trim().to_string())),
            "" => Err(RouterRootError::Empty),
            other => Err(RouterRootError::Unsupported(other.to_string())),
        }
    }

    /// Placeholder contract metadata so snapshots carry `rootSymbol` before FileProvider resolution.
    pub fn placeholder_metadata(self) -> ContractMetadata {
        ContractMetadata {
            root_symbol: self.as_str().to_string(),
            contract_symbol: self.as_str().to_string(),
            configured_symbol: self.as_str().to_string(),
            symbol_resolution_mode: "market_router".to_string(),
            symbol_resolution_source: "router_root".to_string(),
            ..Default::default()
        }
    }
}

impl std::fmt::Display for RouterRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Errors when a caller asks MarketRouter for a root it does not host.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RouterRootError {
    #[error("MarketRouter v0 hosts NQ and ES only; micros ({0}) are out of scope")]
    MicroNotInScope(String),
    #[error("MarketRouter v0 hosts NQ and ES only; `{0}` is not a supported root")]
    Unsupported(String),
    #[error("MarketRouter root symbol must be non-empty")]
    Empty,
}

/// Parse a list of requested roots. Empty input → both NQ and ES.
pub fn parse_requested_roots(raw: Option<&[String]>) -> Result<Vec<RouterRoot>, RouterRootError> {
    let Some(list) = raw else {
        return Ok(RouterRoot::ALL.to_vec());
    };
    let mut out = Vec::new();
    for item in list {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        let root = RouterRoot::parse(trimmed)?;
        if !out.contains(&root) {
            out.push(root);
        }
    }
    if out.is_empty() {
        Ok(RouterRoot::ALL.to_vec())
    } else {
        out.sort();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nq_and_es_only() {
        assert_eq!(RouterRoot::parse("nq").unwrap(), RouterRoot::Nq);
        assert_eq!(RouterRoot::parse("ES").unwrap(), RouterRoot::Es);
        assert!(matches!(
            RouterRoot::parse("MNQ"),
            Err(RouterRootError::MicroNotInScope(_))
        ));
        assert!(matches!(
            RouterRoot::parse("MES"),
            Err(RouterRootError::MicroNotInScope(_))
        ));
        assert!(matches!(
            RouterRoot::parse("YM"),
            Err(RouterRootError::Unsupported(_))
        ));
    }

    #[test]
    fn empty_request_means_both_roots() {
        assert_eq!(
            parse_requested_roots(None).unwrap(),
            vec![RouterRoot::Nq, RouterRoot::Es]
        );
        assert_eq!(
            parse_requested_roots(Some(&[])).unwrap(),
            vec![RouterRoot::Nq, RouterRoot::Es]
        );
    }
}
