use std::{fmt, path::PathBuf, str::FromStr};

use alloy_primitives::Address;
use kage_price_estimate::config::OracleConfig;
use serde::Deserialize;
use thiserror::Error;

pub(crate) const MAX_TOKEN_DECIMALS: u8 = kage_price_estimate::calculation::MAX_TOKEN_DECIMALS;

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
    /// Solvers operated by this deployment. Startup rejects an empty list.
    pub allowed_solvers: Vec<Address>,
    pub proof_orders: ProofOrderSettings,
    pub fee_categories: Vec<FeeCategoryConfig>,
    pub database: DatabaseSettings,
    pub runtime: RuntimeSettings,
    pub pricing: PricingSettings,
    #[serde(default)]
    pub pricing_oracle: Option<OracleConfig>,
    pub chains: Vec<ChainConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProofOrderSettings {
    pub proof_lifetime_seconds: u32,
    pub minimum_remaining_seconds: u32,
    pub preview_lifetime_seconds: u32,
    pub reservation_attempt_timeout_ms: u64,
    pub max_recipients: usize,
    pub preview_cleanup_grace_seconds: u64,
    pub ciphertext_cleanup_grace_seconds: u64,
    pub complaint_window_seconds: u64,
    pub evidence_retention_seconds: u64,
    pub resolved_complaint_retention_seconds: u64,
    #[serde(default)]
    pub complaint_finality: ComplaintFinalityPolicy,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ComplaintFinalityPolicy {
    #[default]
    Finalized,
    Confirmations {
        count: u64,
    },
}

impl Default for ProofOrderSettings {
    fn default() -> Self {
        Self {
            proof_lifetime_seconds: 30,
            minimum_remaining_seconds: 15,
            preview_lifetime_seconds: 15,
            reservation_attempt_timeout_ms: 2_000,
            max_recipients: kage_types::routing::MAX_PROOF_RECIPIENTS,
            preview_cleanup_grace_seconds: 300,
            ciphertext_cleanup_grace_seconds: 300,
            complaint_window_seconds: 2_592_000,
            evidence_retention_seconds: 2_592_000,
            resolved_complaint_retention_seconds: 2_592_000,
            complaint_finality: ComplaintFinalityPolicy::Finalized,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeCategoryConfig {
    pub id: String,
    pub fee_bps: u16,
    pub markets: Vec<String>,
    pub solver_ids: Vec<Address>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiSettings {
    pub request_timeout_ms: u64,
    pub max_body_bytes: usize,
    #[serde(default = "default_max_order_request_bytes")]
    pub max_order_request_bytes: usize,
    #[serde(default = "default_max_ciphertext_bytes")]
    pub max_ciphertext_bytes: usize,
    pub websocket_max_message_bytes: usize,
    #[serde(default = "default_websocket_max_subscriptions")]
    pub websocket_max_subscriptions: usize,
    #[serde(default = "default_websocket_message_window_ms")]
    pub websocket_message_window_ms: u64,
    #[serde(default = "default_websocket_message_burst")]
    pub websocket_message_burst: u32,
    #[serde(default = "default_websocket_heartbeat_interval_ms")]
    pub websocket_heartbeat_interval_ms: u64,
    #[serde(default = "default_websocket_idle_timeout_ms")]
    pub websocket_idle_timeout_ms: u64,
    #[serde(default = "default_solver_auth_recheck_ms")]
    pub solver_auth_recheck_ms: u64,
    pub rate_limit_replenish_ms: u64,
    pub rate_limit_burst: u32,
    pub cors_max_age_seconds: u64,
    pub allowed_origins: Vec<String>,
}

impl Default for ApiSettings {
    fn default() -> Self {
        Self {
            request_timeout_ms: 10_000,
            max_body_bytes: default_max_order_request_bytes(),
            max_order_request_bytes: default_max_order_request_bytes(),
            max_ciphertext_bytes: default_max_ciphertext_bytes(),
            websocket_max_message_bytes: 16 * 1024,
            websocket_max_subscriptions: default_websocket_max_subscriptions(),
            websocket_message_window_ms: default_websocket_message_window_ms(),
            websocket_message_burst: default_websocket_message_burst(),
            websocket_heartbeat_interval_ms: default_websocket_heartbeat_interval_ms(),
            websocket_idle_timeout_ms: default_websocket_idle_timeout_ms(),
            solver_auth_recheck_ms: default_solver_auth_recheck_ms(),
            rate_limit_replenish_ms: 100,
            rate_limit_burst: 100,
            cors_max_age_seconds: 600,
            allowed_origins: Vec::new(),
        }
    }
}

fn default_max_order_request_bytes() -> usize {
    8 * 1024 * 1024
}

fn default_max_ciphertext_bytes() -> usize {
    7 * 1024 * 1024
}

fn default_websocket_max_subscriptions() -> usize {
    32
}

fn default_websocket_message_window_ms() -> u64 {
    1_000
}

fn default_websocket_message_burst() -> u32 {
    20
}

fn default_websocket_heartbeat_interval_ms() -> u64 {
    30_000
}

fn default_websocket_idle_timeout_ms() -> u64 {
    90_000
}

fn default_solver_auth_recheck_ms() -> u64 {
    5_000
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
    #[serde(default)]
    pub movement_allowance_bps: u16,
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

pub(super) fn invalid(message: impl Into<String>) -> ConfigError {
    ConfigError::Invalid(message.into())
}
