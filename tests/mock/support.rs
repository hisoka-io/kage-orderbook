use alloy_primitives::{Address, U256};
use kage_orderbook::order::TradeTerms;

pub fn terms(n: u64) -> TradeTerms {
    TradeTerms {
        token_in: Address::ZERO,
        token_out: Address::repeat_byte(1),
        amount_in: U256::from(n),
        amount_out: U256::from(n * 2),
    }
}
