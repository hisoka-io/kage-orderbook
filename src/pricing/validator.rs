use std::{collections::HashMap, sync::Arc};

use alloy_primitives::{Address, U256, U512};
use thiserror::Error;

use super::{PricingHandle, PricingReadError, now_ms};
use crate::{config::AppConfig, order::TradeTerms};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct MarketKey {
    chain_id: u64,
    token_in: Address,
    token_out: Address,
}

#[derive(Clone, Debug)]
struct MarketPricing {
    asset_in: String,
    asset_out: String,
    decimals_in: u8,
    decimals_out: u8,
    max_deviation_bps: u16,
}

#[derive(Clone)]
pub struct PricingValidator {
    pricing: PricingHandle,
    markets: Arc<HashMap<MarketKey, MarketPricing>>,
    max_order_usd_cents: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PriceValidationError {
    #[error(transparent)]
    Pricing(#[from] PricingReadError),
    #[error("market is not configured for pricing validation")]
    UnsupportedMarket,
    #[error("price validation arithmetic overflow")]
    Arithmetic,
    #[error("quote exceeds the maximum deviation of {max_bps} BPS")]
    DeviationExceeded { max_bps: u16 },
    #[error("{side} value exceeds the configured maximum of {max_usd_cents} USD cents")]
    OrderValueExceeded {
        side: &'static str,
        max_usd_cents: u64,
    },
}

impl PricingValidator {
    pub fn new(pricing: PricingHandle, config: &AppConfig) -> Self {
        let markets = config
            .chains
            .iter()
            .flat_map(|chain| {
                chain.markets.iter().map(move |market| {
                    let token_in = chain
                        .tokens
                        .iter()
                        .find(|token| token.symbol == market.token_in)
                        .expect("validated market input token");
                    let token_out = chain
                        .tokens
                        .iter()
                        .find(|token| token.symbol == market.token_out)
                        .expect("validated market output token");
                    let token_limit = token_in
                        .max_price_deviation_bps
                        .min(token_out.max_price_deviation_bps);
                    let pricing = MarketPricing {
                        asset_in: token_in.pricing_asset.clone(),
                        asset_out: token_out.pricing_asset.clone(),
                        decimals_in: token_in.decimals,
                        decimals_out: token_out.decimals,
                        max_deviation_bps: market
                            .max_price_deviation_bps
                            .unwrap_or(token_limit)
                            .min(token_limit),
                    };
                    (
                        MarketKey {
                            chain_id: chain.chain_id,
                            token_in: token_in.address,
                            token_out: token_out.address,
                        },
                        pricing,
                    )
                })
            })
            .collect();

        Self {
            pricing,
            markets: Arc::new(markets),
            max_order_usd_cents: config.order.max_order_usd_cents,
        }
    }

    pub fn validate(&self, terms: &TradeTerms) -> Result<(), PriceValidationError> {
        self.validate_at(terms, now_ms())
    }

    fn validate_at(&self, terms: &TradeTerms, now_ms: u64) -> Result<(), PriceValidationError> {
        let market = self
            .markets
            .get(&MarketKey {
                chain_id: terms.chain_id,
                token_in: terms.token_in,
                token_out: terms.token_out,
            })
            .ok_or(PriceValidationError::UnsupportedMarket)?;
        let (price_in, price_out) =
            self.pricing
                .fresh_pair_at(&market.asset_in, &market.asset_out, now_ms)?;

        validate_order_value(
            terms.amount_in,
            price_in.price_e18,
            market.decimals_in,
            "input",
            self.max_order_usd_cents,
        )?;
        validate_order_value(
            terms.amount_out,
            price_out.price_e18,
            market.decimals_out,
            "output",
            self.max_order_usd_cents,
        )?;

        let input_value =
            normalized_value(terms.amount_in, price_in.price_e18, market.decimals_out)?;
        let output_value =
            normalized_value(terms.amount_out, price_out.price_e18, market.decimals_in)?;
        let difference = input_value.abs_diff(output_value);
        let deviation = difference
            .checked_mul(U512::from(10_000_u16))
            .ok_or(PriceValidationError::Arithmetic)?;
        let permitted = input_value
            .checked_mul(U512::from(market.max_deviation_bps))
            .ok_or(PriceValidationError::Arithmetic)?;

        if deviation > permitted {
            return Err(PriceValidationError::DeviationExceeded {
                max_bps: market.max_deviation_bps,
            });
        }
        Ok(())
    }
}

fn validate_order_value(
    amount: U256,
    price_e18: U256,
    decimals: u8,
    side: &'static str,
    max_usd_cents: u64,
) -> Result<(), PriceValidationError> {
    let value_cents_scaled = U512::from(amount)
        .checked_mul(U512::from(price_e18))
        .and_then(|value| value.checked_mul(U512::from(100_u8)))
        .ok_or(PriceValidationError::Arithmetic)?;
    let token_scale = U512::from(10_u8)
        .checked_pow(U512::from(decimals))
        .ok_or(PriceValidationError::Arithmetic)?;
    let price_scale = U512::from(10_u8)
        .checked_pow(U512::from(18_u8))
        .ok_or(PriceValidationError::Arithmetic)?;
    let limit_cents_scaled = U512::from(max_usd_cents)
        .checked_mul(token_scale)
        .and_then(|value| value.checked_mul(price_scale))
        .ok_or(PriceValidationError::Arithmetic)?;

    if value_cents_scaled > limit_cents_scaled {
        return Err(PriceValidationError::OrderValueExceeded {
            side,
            max_usd_cents,
        });
    }
    Ok(())
}

fn normalized_value(
    amount: U256,
    price_e18: U256,
    opposite_decimals: u8,
) -> Result<U512, PriceValidationError> {
    let scale = U512::from(10_u8)
        .checked_pow(U512::from(opposite_decimals))
        .ok_or(PriceValidationError::Arithmetic)?;
    U512::from(amount)
        .checked_mul(U512::from(price_e18))
        .and_then(|value| value.checked_mul(scale))
        .ok_or(PriceValidationError::Arithmetic)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use alloy_primitives::Address;
    use tokio::sync::watch;

    use super::*;
    use crate::{
        config::AppConfig,
        pricing::{PricePoint, PricingSnapshot},
    };

    const NOW_MS: u64 = 10_000;
    fn config() -> AppConfig {
        AppConfig::from_json(
            r#"{
              "order": { "default_ttl_seconds": 60, "min_ttl_seconds": 5, "max_ttl_seconds": 300, "max_order_usd_cents": 25000 },
              "database": { "max_connections": 1, "busy_timeout_ms": 5000 },
              "runtime": { "command_capacity": 256 },
              "pricing": { "max_age_ms": 1000, "reconnect_delay_ms": 1000, "idle_timeout_ms": 30000 },
              "chains": [{
                "chain_id": 31337,
                "name": "local",
                "registry": "0x0404040404040404040404040404040404040404",
                "registry_deploy_block": 100,
                "confirmations": 0,
                "tokens": [
                  { "symbol": "ETH", "address": "0x0101010101010101010101010101010101010101", "decimals": 18, "pricing_asset": "ETH", "max_price_deviation_bps": 100 },
                  { "symbol": "USDC", "address": "0x0202020202020202020202020202020202020202", "decimals": 6, "pricing_asset": "USDC", "max_price_deviation_bps": 80 }
                ],
                "markets": [{ "token_in": "ETH", "token_out": "USDC", "max_price_deviation_bps": 50 }]
              }]
            }"#,
        )
        .unwrap()
    }

    fn terms(amount_out: u64) -> TradeTerms {
        TradeTerms {
            chain_id: 31_337,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
            amount_in: U256::from(100_000_000_000_000_000_u64),
            amount_out: U256::from(amount_out),
            expires_at_ms: 60_000,
        }
    }

    fn validator(observed_at_ms: u64, include_usdc: bool) -> PricingValidator {
        validator_with_eth_price(
            observed_at_ms,
            include_usdc,
            U256::from(2_000_u64) * U256::from(10_u64).pow(U256::from(18_u8)),
        )
    }

    fn validator_with_eth_price(
        observed_at_ms: u64,
        include_usdc: bool,
        eth_price: U256,
    ) -> PricingValidator {
        let mut prices = HashMap::from([(
            "ETH".to_owned(),
            PricePoint {
                price_e18: eth_price,
                observed_at_ms,
                sequence: 1,
            },
        )]);
        if include_usdc {
            prices.insert(
                "USDC".to_owned(),
                PricePoint {
                    price_e18: U256::from(10_u64).pow(U256::from(18_u8)),
                    observed_at_ms,
                    sequence: 1,
                },
            );
        }
        let mut snapshot = PricingSnapshot::new(vec!["ETH".into(), "USDC".into()]);
        snapshot.replace_prices(prices);
        let (_, receiver) = watch::channel(Arc::new(snapshot));
        let handle = PricingHandle {
            receiver,
            max_age: Duration::from_millis(1_000),
        };
        PricingValidator::new(handle, &config())
    }

    #[test]
    fn accepts_exact_boundary_and_mixed_decimals() {
        let validator = validator(NOW_MS, true);
        assert_eq!(validator.validate_at(&terms(200_000_000), NOW_MS), Ok(()));
        assert_eq!(validator.validate_at(&terms(199_000_000), NOW_MS), Ok(()));
    }

    #[test]
    fn accepts_within_limit_and_rejects_outside_limit() {
        let validator = validator(NOW_MS, true);
        assert_eq!(validator.validate_at(&terms(199_200_000), NOW_MS), Ok(()));
        assert_eq!(
            validator.validate_at(&terms(198_000_000), NOW_MS),
            Err(PriceValidationError::DeviationExceeded { max_bps: 50 })
        );
    }

    #[test]
    fn accepts_exact_order_cap_and_rejects_either_side_above_it() {
        let validator = validator(NOW_MS, true);
        let mut boundary = terms(250_000_000);
        boundary.amount_in = U256::from(125_000_000_000_000_000_u64);
        assert_eq!(validator.validate_at(&boundary, NOW_MS), Ok(()));

        let mut input_over = boundary;
        input_over.amount_in += U256::from(1_u8);
        assert_eq!(
            validator.validate_at(&input_over, NOW_MS),
            Err(PriceValidationError::OrderValueExceeded {
                side: "input",
                max_usd_cents: 25_000,
            })
        );

        let mut output_over = boundary;
        output_over.amount_out += U256::from(1_u8);
        assert_eq!(
            validator.validate_at(&output_over, NOW_MS),
            Err(PriceValidationError::OrderValueExceeded {
                side: "output",
                max_usd_cents: 25_000,
            })
        );
    }

    #[test]
    fn rejects_stale_and_missing_prices() {
        assert!(matches!(
            validator(NOW_MS - 1_001, true).validate_at(&terms(200_000_000), NOW_MS),
            Err(PriceValidationError::Pricing(PricingReadError::Stale(_)))
        ));
        assert!(matches!(
            validator(NOW_MS, false).validate_at(&terms(200_000_000), NOW_MS),
            Err(PriceValidationError::Pricing(PricingReadError::Missing(_)))
        ));
    }

    #[test]
    fn rejects_arithmetic_overflow() {
        let validator = validator_with_eth_price(NOW_MS, true, U256::MAX);
        let mut overflowing = terms(200_000_000);
        overflowing.amount_in = U256::MAX;
        assert_eq!(
            validator.validate_at(&overflowing, NOW_MS),
            Err(PriceValidationError::Arithmetic)
        );
    }

    #[test]
    fn rejects_an_unconfigured_direction() {
        let validator = validator(NOW_MS, true);
        let mut reversed = terms(200_000_000);
        (reversed.token_in, reversed.token_out) = (reversed.token_out, reversed.token_in);
        reversed.amount_in = U256::from(200_000_000_u64);
        reversed.amount_out = U256::from(100_000_000_000_000_000_u64);
        assert_eq!(
            validator.validate_at(&reversed, NOW_MS),
            Err(PriceValidationError::UnsupportedMarket)
        );
    }
}
