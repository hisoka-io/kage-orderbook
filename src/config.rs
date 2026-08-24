use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

use alloy_primitives::Address;
use serde::Deserialize;
use thiserror::Error;

use crate::core::guards::{ApprovedMarket, OrderPolicy};

const MAX_TOKEN_DECIMALS: u8 = 77;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Network {
    Localnet,
    Testnet,
    Mainnet,
}

impl Network {
    pub fn bootstrap(explicit: Option<String>) -> Result<Self, ConfigError> {
        let network: Self = explicit
            .or_else(|| std::env::var("KAGE_NETWORK").ok())
            .unwrap_or_else(|| Self::Localnet.as_str().to_owned())
            .parse()?;
        let file = format!(".env.{network}");
        dotenvy::from_filename(&file).map_err(|source| ConfigError::Env { file, source })?;
        Ok(network)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Localnet => "localnet",
            Self::Testnet => "testnet",
            Self::Mainnet => "mainnet",
        }
    }

    pub fn stamp(self) -> i64 {
        match self {
            Self::Localnet => 1,
            Self::Testnet => 2,
            Self::Mainnet => 3,
        }
    }

    pub fn from_stamp(stamp: i64) -> Option<Self> {
        [Self::Localnet, Self::Testnet, Self::Mainnet]
            .into_iter()
            .find(|network| network.stamp() == stamp)
    }
}

impl FromStr for Network {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "localnet" => Ok(Self::Localnet),
            "testnet" => Ok(Self::Testnet),
            "mainnet" => Ok(Self::Mainnet),
            other => Err(invalid(format!(
                "unknown network {other}, expected localnet|testnet|mainnet"
            ))),
        }
    }
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default)]
    pub api: ApiSettings,
    pub order: OrderSettings,
    pub database: DatabaseSettings,
    pub runtime: RuntimeSettings,
    pub pricing: PricingSettings,
    pub chains: Vec<ChainConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiSettings {
    pub request_timeout_ms: u64,
    pub max_body_bytes: usize,
    pub websocket_max_message_bytes: usize,
    pub rate_limit_replenish_ms: u64,
    pub rate_limit_burst: u32,
    pub cors_max_age_seconds: u64,
    pub allowed_origins: Vec<String>,
}

impl Default for ApiSettings {
    fn default() -> Self {
        Self {
            request_timeout_ms: 10_000,
            // EncryptedProofRequest serializes bytes as a JSON array, which is larger
            // than the 900 KiB encrypted transport payload it carries.
            max_body_bytes: 5_000_000,
            websocket_max_message_bytes: 16 * 1024,
            rate_limit_replenish_ms: 100,
            rate_limit_burst: 100,
            cors_max_age_seconds: 600,
            allowed_origins: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderSettings {
    pub default_ttl_seconds: u32,
    pub min_ttl_seconds: u32,
    pub max_ttl_seconds: u32,
    pub max_order_usd_cents: u64,
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
    pub darkpool: Address,
    pub registry: Address,
    pub registry_deploy_block: u64,
    pub confirmations: u64,
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
    #[error("failed to load env file {file}: {source}")]
    Env {
        file: String,
        source: dotenvy::Error,
    },
    #[error("invalid config JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

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

    fn validate(&self) -> Result<(), ConfigError> {
        if self.api.request_timeout_ms == 0
            || self.api.max_body_bytes == 0
            || self.api.websocket_max_message_bytes == 0
            || self.api.rate_limit_replenish_ms == 0
            || self.api.rate_limit_burst == 0
            || self.api.cors_max_age_seconds == 0
        {
            return Err(invalid(
                "API limits and durations must be greater than zero",
            ));
        }
        let mut origins = HashSet::new();
        for origin in &self.api.allowed_origins {
            let parsed = reqwest::Url::parse(origin)
                .map_err(|_| invalid(format!("invalid API allowed origin {origin}")))?;
            let normalized = parsed.origin().ascii_serialization();
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host_str().is_none()
                || parsed.path() != "/"
                || parsed.query().is_some()
                || parsed.fragment().is_some()
                || normalized != *origin
                || !origins.insert(origin)
            {
                return Err(invalid(format!("invalid API allowed origin {origin}")));
            }
        }
        if self.order.min_ttl_seconds == 0
            || self.order.min_ttl_seconds > self.order.default_ttl_seconds
            || self.order.default_ttl_seconds > self.order.max_ttl_seconds
        {
            return Err(invalid("order TTL must satisfy 0 < min <= default <= max"));
        }
        if self.order.max_order_usd_cents == 0 {
            return Err(invalid(
                "order.max_order_usd_cents must be greater than zero",
            ));
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
            if chain.darkpool == Address::ZERO || chain.registry == Address::ZERO {
                return Err(invalid(format!(
                    "chain {} requires non-zero darkpool and registry addresses",
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
      "order": { "default_ttl_seconds": 60, "min_ttl_seconds": 5, "max_ttl_seconds": 300, "max_order_usd_cents": 25000 },
      "database": { "max_connections": 1, "busy_timeout_ms": 5000 },
      "runtime": { "command_capacity": 256 },
      "pricing": { "max_age_ms": 5000, "reconnect_delay_ms": 1000, "idle_timeout_ms": 30000 },
      "chains": [{
        "chain_id": 31337,
        "name": "local",
        "darkpool": "0x0303030303030303030303030303030303030303",
        "registry": "0x0404040404040404040404040404040404040404",
        "registry_deploy_block": 100,
        "confirmations": 0,
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
        assert_eq!(config.order.max_order_usd_cents, 25_000);
        assert_eq!(config.chains[0].darkpool, Address::repeat_byte(3));
        assert_eq!(config.chains[0].registry, Address::repeat_byte(4));
        assert_eq!(config.chains[0].registry_deploy_block, 100);
        assert_eq!(config.api.request_timeout_ms, 10_000);
        assert_eq!(config.api.max_body_bytes, 5_000_000);
        assert!(config.api.allowed_origins.is_empty());
    }

    #[test]
    fn validates_api_limits_and_exact_origins() {
        let api = r#""api": {
          "request_timeout_ms": 10000,
          "max_body_bytes": 5000000,
          "websocket_max_message_bytes": 16384,
          "rate_limit_replenish_ms": 100,
          "rate_limit_burst": 100,
          "cors_max_age_seconds": 600,
          "allowed_origins": ["https://app.example.com"]
        },
        "order":"#;
        let configured = VALID_CONFIG.replace("\"order\":", api);
        let config = AppConfig::from_json(&configured).unwrap();
        assert_eq!(config.api.allowed_origins, vec!["https://app.example.com"]);

        let zero_limit =
            configured.replace("\"request_timeout_ms\": 10000", "\"request_timeout_ms\": 0");
        assert!(matches!(
            AppConfig::from_json(&zero_limit),
            Err(ConfigError::Invalid(_))
        ));

        let origin_with_path =
            configured.replace("https://app.example.com", "https://app.example.com/path");
        assert!(matches!(
            AppConfig::from_json(&origin_with_path),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_zero_contract_addresses() {
        for byte in [3, 4] {
            let configured = Address::repeat_byte(byte).to_string();
            let zeroed = VALID_CONFIG.replace(&configured, &Address::ZERO.to_string());
            assert_ne!(zeroed, VALID_CONFIG, "{configured} is not in the fixture");
            assert!(
                matches!(AppConfig::from_json(&zeroed), Err(ConfigError::Invalid(_))),
                "zero address accepted in place of {configured}"
            );
        }
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

        let zero_order_cap = VALID_CONFIG.replace(
            "\"max_order_usd_cents\": 25000",
            "\"max_order_usd_cents\": 0",
        );
        assert!(matches!(
            AppConfig::from_json(&zero_order_cap),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn checked_in_config_is_valid() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/localnet.json");
        let config = AppConfig::load_from(path).unwrap();

        assert_eq!(config.pricing_assets(), vec!["ETH", "USDC", "USDT"]);
    }

    #[test]
    fn network_names_and_stamps_are_distinct() {
        for network in [Network::Localnet, Network::Testnet, Network::Mainnet] {
            assert_eq!(network.as_str().parse::<Network>().unwrap(), network);
            assert_eq!(Network::from_stamp(network.stamp()), Some(network));
        }
        assert!("mainet".parse::<Network>().is_err());
        assert!(Network::from_stamp(0).is_none());
    }
}
