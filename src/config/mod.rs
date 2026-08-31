mod loading;
mod model;
mod validation;

pub(crate) use model::MAX_TOKEN_DECIMALS;
pub use model::{
    ApiSettings, AppConfig, ChainConfig, ComplaintFinalityPolicy, ConfigError, DatabaseSettings,
    FeeCategoryConfig, MarketConfig, Network, PricingSettings, ProofOrderSettings, RuntimeSettings,
    TokenConfig,
};

#[cfg(test)]
mod tests;
