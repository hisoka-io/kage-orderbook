use std::collections::HashSet;

use alloy_primitives::{Address, B256, U256};
use thiserror::Error;

use crate::order::{OrderCommitment, TokenAddress, TradeTerms};

pub const DEFAULT_ORDER_TTL_SECONDS: u32 = 60;
pub const MIN_ORDER_TTL_SECONDS: u32 = 5;
pub const MAX_ORDER_TTL_SECONDS: u32 = 300;
pub const MOCK_CHAIN_ID: u64 = 31_337;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ApprovedMarket {
    pub chain_id: u64,
    pub token_in: Address,
    pub token_out: Address,
}

#[derive(Debug, Clone, Copy)]
pub struct CreateOrderInput {
    pub order_commitment: OrderCommitment,
    pub chain_id: u64,
    pub token_in: TokenAddress,
    pub token_out: TokenAddress,
    pub amount_in: U256,
    pub amount_out: U256,
    pub ttl_seconds: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
pub struct ValidatedCreateOrder {
    pub order_commitment: OrderCommitment,
    pub terms: TradeTerms,
}

#[derive(Debug, Clone)]
pub struct OrderPolicy {
    pub default_ttl_seconds: u32,
    pub min_ttl_seconds: u32,
    pub max_ttl_seconds: u32,
    pub supported_chains: HashSet<u64>,
    pub approved_markets: HashSet<ApprovedMarket>,
}

impl Default for OrderPolicy {
    fn default() -> Self {
        let market = ApprovedMarket {
            chain_id: MOCK_CHAIN_ID,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
        };
        Self {
            default_ttl_seconds: DEFAULT_ORDER_TTL_SECONDS,
            min_ttl_seconds: MIN_ORDER_TTL_SECONDS,
            max_ttl_seconds: MAX_ORDER_TTL_SECONDS,
            supported_chains: HashSet::from([MOCK_CHAIN_ID]),
            approved_markets: HashSet::from([market]),
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GuardError {
    #[error("ttl_seconds must be between {min} and {max}")]
    InvalidTtl { min: u32, max: u32 },
    #[error("expiry timestamp overflow")]
    ExpiryOverflow,
    #[error("unsupported chain_id {0}")]
    UnsupportedChain(u64),
    #[error("token pair is not approved for chain_id {0}")]
    UnsupportedMarket(u64),
    #[error("order commitment cannot be zero")]
    InvalidCommitment,
    #[error("order amounts must be greater than zero")]
    InvalidAmount,
}

pub fn validate_create_order(
    input: CreateOrderInput,
    received_at_ms: i64,
    policy: &OrderPolicy,
) -> Result<ValidatedCreateOrder, GuardError> {
    if input.order_commitment == B256::ZERO {
        return Err(GuardError::InvalidCommitment);
    }
    if input.amount_in == U256::ZERO || input.amount_out == U256::ZERO {
        return Err(GuardError::InvalidAmount);
    }
    validate_market(input.chain_id, input.token_in, input.token_out, policy)?;
    let expires_at_ms = resolve_expiry_ms(input.ttl_seconds, received_at_ms, policy)?;

    Ok(ValidatedCreateOrder {
        order_commitment: input.order_commitment,
        terms: TradeTerms {
            chain_id: input.chain_id,
            token_in: input.token_in,
            token_out: input.token_out,
            amount_in: input.amount_in,
            amount_out: input.amount_out,
            expires_at_ms,
        },
    })
}

fn resolve_expiry_ms(
    ttl_seconds: Option<u32>,
    received_at_ms: i64,
    policy: &OrderPolicy,
) -> Result<i64, GuardError> {
    let ttl_seconds = ttl_seconds.unwrap_or(policy.default_ttl_seconds);
    if ttl_seconds < policy.min_ttl_seconds || ttl_seconds > policy.max_ttl_seconds {
        return Err(GuardError::InvalidTtl {
            min: policy.min_ttl_seconds,
            max: policy.max_ttl_seconds,
        });
    }

    received_at_ms
        .checked_add(i64::from(ttl_seconds) * 1_000)
        .ok_or(GuardError::ExpiryOverflow)
}

fn validate_market(
    chain_id: u64,
    token_in: Address,
    token_out: Address,
    policy: &OrderPolicy,
) -> Result<(), GuardError> {
    if !policy.supported_chains.contains(&chain_id) {
        return Err(GuardError::UnsupportedChain(chain_id));
    }
    if !policy.approved_markets.contains(&ApprovedMarket {
        chain_id,
        token_in,
        token_out,
    }) {
        return Err(GuardError::UnsupportedMarket(chain_id));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> CreateOrderInput {
        CreateOrderInput {
            order_commitment: B256::repeat_byte(1),
            chain_id: MOCK_CHAIN_ID,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
            amount_in: U256::from(100),
            amount_out: U256::from(200),
            ttl_seconds: None,
        }
    }

    #[test]
    fn resolves_default_and_explicit_ttl() {
        let policy = OrderPolicy::default();
        assert_eq!(resolve_expiry_ms(None, 1_000, &policy), Ok(61_000));
        assert_eq!(resolve_expiry_ms(Some(5), 1_000, &policy), Ok(6_000));
    }

    #[test]
    fn rejects_ttl_outside_policy() {
        let policy = OrderPolicy::default();
        assert!(matches!(
            resolve_expiry_ms(Some(4), 1_000, &policy),
            Err(GuardError::InvalidTtl { .. })
        ));
        assert!(matches!(
            resolve_expiry_ms(Some(301), 1_000, &policy),
            Err(GuardError::InvalidTtl { .. })
        ));
    }

    #[test]
    fn validates_and_builds_trade_terms() {
        let policy = OrderPolicy::default();
        let validated = validate_create_order(input(), 1_000, &policy).unwrap();

        assert_eq!(validated.order_commitment, B256::repeat_byte(1));
        assert_eq!(validated.terms.chain_id, MOCK_CHAIN_ID);
        assert_eq!(validated.terms.expires_at_ms, 61_000);
    }

    #[test]
    fn rejects_zero_commitment_and_amounts() {
        let policy = OrderPolicy::default();
        let mut invalid_commitment = input();
        invalid_commitment.order_commitment = B256::ZERO;
        assert!(matches!(
            validate_create_order(invalid_commitment, 1_000, &policy),
            Err(GuardError::InvalidCommitment)
        ));

        let mut invalid_amount = input();
        invalid_amount.amount_in = U256::ZERO;
        assert!(matches!(
            validate_create_order(invalid_amount, 1_000, &policy),
            Err(GuardError::InvalidAmount)
        ));
    }
}
