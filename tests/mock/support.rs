use alloy_primitives::{Address, B256, U256};
use kage_orderbook::order::TradeTerms;

pub fn commitment(n: u64) -> B256 {
    let mut bytes = [0_u8; 32];
    bytes[24..].copy_from_slice(&n.to_be_bytes());
    B256::from(bytes)
}

pub fn terms(n: u64) -> TradeTerms {
    TradeTerms {
        token_in: Address::ZERO,
        token_out: Address::repeat_byte(1),
        amount_in: U256::from(n),
        amount_out: U256::from(n * 2),
        expires_at_ms: chrono::Utc::now().timestamp_millis() + 60_000,
    }
}
