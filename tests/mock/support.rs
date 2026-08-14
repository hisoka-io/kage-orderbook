use alloy_primitives::{Address, B256, U256};
use kage_orderbook::order::TradeTerms;
use kage_orderbook::registry::{SolverProfile, SolverRegistry};

pub fn solver_address(n: u8) -> Address {
    Address::repeat_byte(n)
}

pub fn noise_key(n: u8) -> B256 {
    B256::repeat_byte(n)
}

pub fn registry() -> SolverRegistry {
    SolverRegistry::from_profiles([
        (
            solver_address(0x11),
            SolverProfile {
                noise_key: noise_key(0x33),
                active: true,
            },
        ),
        (
            solver_address(0x22),
            SolverProfile {
                noise_key: noise_key(0x44),
                active: true,
            },
        ),
    ])
}

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
