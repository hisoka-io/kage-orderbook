use std::path::{Path, PathBuf};

use super::{AppConfig, ConfigError, Network};

impl AppConfig {
    pub fn load(network: Network) -> Result<Self, ConfigError> {
        let path = std::env::var("KAGE_ORDERBOOK_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(format!("config/{network}.json")));
        Self::load_from(path)
    }

    pub fn load_from(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let json = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::from_json(&json)
    }

    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        let config: Self = serde_json::from_str(json)?;
        config.validate()?;
        Ok(config)
    }

    pub fn pricing_assets(&self) -> Vec<String> {
        let mut assets: Vec<String> = self
            .chains
            .iter()
            .flat_map(|chain| chain.tokens.iter())
            .map(|token| token.pricing_asset.clone())
            .collect();
        assets.sort();
        assets.dedup();
        assets
    }
}
