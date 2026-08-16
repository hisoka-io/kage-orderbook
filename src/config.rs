use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use alloy_primitives::Address;
use serde::Deserialize;
use thiserror::Error;

use crate::core::guards::{ApprovedMarket, OrderPolicy};

const MAX_TOKEN_DECIMALS: u8 = 77;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub order: OrderSettings,
    pub database: DatabaseSettings,
    pub runtime: RuntimeSettings,
    pub pricing: PricingSettings,
    pub chains: Vec<ChainConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderSettings {
    pub default_ttl_seconds: u32,
    pub min_ttl_seconds: u32,
    pub max_ttl_seconds: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseSettings {
    pub max_connections: u32,
    pub busy_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSettings {
    pub command_capacity: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PricingSettings {
    pub max_age_ms: u64,
    pub reconnect_delay_ms: u64,
    pub idle_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    pub chain_id: u64,
    pub name: String,
    pub tokens: Vec<TokenConfig>,
    pub markets: Vec<MarketConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenConfig {
    pub symbol: String,
    pub address: Address,
    pub decimals: u8,
    pub pricing_asset: String,
    pub max_price_deviation_bps: u16,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketConfig {
    pub token_in: String,
    pub token_out: String,
    pub max_price_deviation_bps: Option<u16>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid config JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let path = std::env::var("KAGE_ORDERBOOK_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("config.json"));
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

    pub fn order_policy(&self) -> OrderPolicy {
        let supported_chains = self.chains.iter().map(|chain| chain.chain_id).collect();
        let approved_markets = self
            .chains
            .iter()
            .flat_map(|chain| {
                let tokens: HashMap<&str, Address> = chain
                    .tokens
                    .iter()
                    .map(|token| (token.symbol.as_str(), token.address))
                    .collect();
                chain.markets.iter().map(move |market| ApprovedMarket {
                    chain_id: chain.chain_id,
                    token_in: tokens[market.token_in.as_str()],
                    token_out: tokens[market.token_out.as_str()],
                })
            })
            .collect();

        OrderPolicy {
            default_ttl_seconds: self.order.default_ttl_seconds,
            min_ttl_seconds: self.order.min_ttl_seconds,
            max_ttl_seconds: self.order.max_ttl_seconds,
            supported_chains,
            approved_markets,
        }
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

    pub fn token(&self, chain_id: u64, address: Address) -> Option<&TokenConfig> {
        self.chains
            .iter()
            .find(|chain| chain.chain_id == chain_id)?
            .tokens
            .iter()
            .find(|token| token.address == address)
    }

    pub fn market_max_price_deviation_bps(
        &self,
        chain_id: u64,
        token_in: Address,
        token_out: Address,
    ) -> Option<u16> {
        let chain = self
            .chains
            .iter()
            .find(|chain| chain.chain_id == chain_id)?;
        let token_in = chain
            .tokens
            .iter()
            .find(|token| token.address == token_in)?;
        let token_out = chain
            .tokens
            .iter()
            .find(|token| token.address == token_out)?;
        let market = chain.markets.iter().find(|market| {
            market.token_in == token_in.symbol && market.token_out == token_out.symbol
        })?;
        let token_limit = token_in
            .max_price_deviation_bps
            .min(token_out.max_price_deviation_bps);
        Some(
            market
                .max_price_deviation_bps
                .unwrap_or(token_limit)
                .min(token_limit),
        )
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.order.min_ttl_seconds == 0
            || self.order.min_ttl_seconds > self.order.default_ttl_seconds
            || self.order.default_ttl_seconds > self.order.max_ttl_seconds
        {
            return Err(invalid("order TTL must satisfy 0 < min <= default <= max"));
        }
        if self.database.max_connections == 0 || self.database.busy_timeout_ms == 0 {
            return Err(invalid("database values must be greater than zero"));
        }
        if self.runtime.command_capacity == 0 {
            return Err(invalid(
                "runtime.command_capacity must be greater than zero",
            ));
        }
        if self.pricing.max_age_ms == 0
            || self.pricing.reconnect_delay_ms == 0
            || self.pricing.idle_timeout_ms == 0
        {
            return Err(invalid("pricing durations must be greater than zero"));
        }
        if self.chains.is_empty() {
            return Err(invalid("at least one chain must be configured"));
        }

        let mut chain_ids = HashSet::new();
        for chain in &self.chains {
            if chain.chain_id == 0 || !chain_ids.insert(chain.chain_id) {
                return Err(invalid(format!(
                    "chain_id {} must be non-zero and unique",
                    chain.chain_id
                )));
            }
            if chain.name.trim().is_empty() || chain.tokens.is_empty() {
                return Err(invalid(format!(
                    "chain {} requires a name and at least one token",
                    chain.chain_id
                )));
            }

            let mut symbols = HashSet::new();
            let mut token_bps = HashMap::new();
            let mut addresses = HashSet::new();
            for token in &chain.tokens {
                if token.symbol.is_empty() || token.symbol != token.symbol.to_uppercase() {
                    return Err(invalid(format!(
                        "token symbol {} must be non-empty uppercase",
                        token.symbol
                    )));
                }
                if token.pricing_asset.is_empty()
                    || token.pricing_asset != token.pricing_asset.to_uppercase()
                {
                    return Err(invalid(format!(
                        "pricing asset {} must be non-empty uppercase",
                        token.pricing_asset
                    )));
                }
                if token.address == Address::ZERO {
                    return Err(invalid(format!(
                        "token {} has a zero address",
                        token.symbol
                    )));
                }
                if token.decimals > MAX_TOKEN_DECIMALS {
                    return Err(invalid(format!(
                        "token {} decimals must not exceed {MAX_TOKEN_DECIMALS}",
                        token.symbol
                    )));
                }
                if !symbols.insert(token.symbol.as_str()) || !addresses.insert(token.address) {
                    return Err(invalid(format!(
                        "token symbols and addresses must be unique on chain {}",
                        chain.chain_id
                    )));
                }
                validate_bps(token.max_price_deviation_bps)?;
                token_bps.insert(token.symbol.as_str(), token.max_price_deviation_bps);
            }

            let mut markets = HashSet::new();
            for market in &chain.markets {
                if market.token_in == market.token_out
                    || !symbols.contains(market.token_in.as_str())
                    || !symbols.contains(market.token_out.as_str())
                {
                    return Err(invalid(format!(
                        "market {} -> {} is invalid on chain {}",
                        market.token_in, market.token_out, chain.chain_id
                    )));
                }
                if !markets.insert((market.token_in.as_str(), market.token_out.as_str())) {
                    return Err(invalid(format!(
                        "duplicate market {} -> {} on chain {}",
                        market.token_in, market.token_out, chain.chain_id
                    )));
                }
                if let Some(bps) = market.max_price_deviation_bps {
                    validate_bps(bps)?;
                    let token_limit = token_bps[market.token_in.as_str()]
                        .min(token_bps[market.token_out.as_str()]);
                    if bps > token_limit {
                        return Err(invalid(format!(
                            "market BPS cannot exceed token limit {token_limit}"
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

fn validate_bps(bps: u16) -> Result<(), ConfigError> {
    if bps == 0 || bps > 10_000 {
        return Err(invalid(format!(
            "max_price_deviation_bps must be between 1 and 10000, got {bps}"
        )));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConfigError {
    ConfigError::Invalid(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_CONFIG: &str = r#"
    {
      "order": { "default_ttl_seconds": 60, "min_ttl_seconds": 5, "max_ttl_seconds": 300 },
      "database": { "max_connections": 1, "busy_timeout_ms": 5000 },
      "runtime": { "command_capacity": 256 },
      "pricing": { "max_age_ms": 5000, "reconnect_delay_ms": 1000, "idle_timeout_ms": 30000 },
      "chains": [{
        "chain_id": 31337,
        "name": "local",
        "tokens": [
          {
            "symbol": "ETH",
            "address": "0x0101010101010101010101010101010101010101",
            "decimals": 18,
            "pricing_asset": "ETH",
            "max_price_deviation_bps": 50
          },
          {
            "symbol": "USDC",
            "address": "0x0202020202020202020202020202020202020202",
            "decimals": 6,
            "pricing_asset": "USDC",
            "max_price_deviation_bps": 20
          }
        ],
        "markets": [{
          "token_in": "ETH",
          "token_out": "USDC",
          "max_price_deviation_bps": 20
        }]
      }]
    }
    "#;

    #[test]
    fn loads_metadata_and_builds_order_policy() {
        let config = AppConfig::from_json(VALID_CONFIG).unwrap();
        let policy = config.order_policy();

        assert!(policy.supported_chains.contains(&31_337));
        assert!(policy.approved_markets.contains(&ApprovedMarket {
            chain_id: 31_337,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
        }));
        assert_eq!(config.pricing_assets(), vec!["ETH", "USDC"]);
        assert_eq!(
            config
                .token(31_337, Address::repeat_byte(2))
                .unwrap()
                .decimals,
            6
        );
        assert_eq!(
            config.market_max_price_deviation_bps(
                31_337,
                Address::repeat_byte(1),
                Address::repeat_byte(2),
            ),
            Some(20)
        );
    }

    #[test]
    fn rejects_unknown_fields_and_invalid_policy() {
        let unknown = VALID_CONFIG.replace(
            "\"command_capacity\": 256",
            "\"command_capacity\": 256, \"unexpected\": true",
        );
        assert!(matches!(
            AppConfig::from_json(&unknown),
            Err(ConfigError::Json(_))
        ));

        let bad_bps = VALID_CONFIG.replace(
            "\"max_price_deviation_bps\": 50",
            "\"max_price_deviation_bps\": 0",
        );
        assert!(matches!(
            AppConfig::from_json(&bad_bps),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn checked_in_config_is_valid() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.json");
        let config = AppConfig::load_from(path).unwrap();

        assert_eq!(config.pricing_assets(), vec!["ETH", "LINK", "USDC", "USDT"]);
    }
}
