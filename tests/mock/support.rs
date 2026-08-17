use alloy_primitives::{Address, B256, U256};
use kage_orderbook::{core::guards::MOCK_CHAIN_ID, registry::SolverRegistry};
use kage_types::{orders::TradeTerms, registry::SolverProfile};

use super::proof_transport;

pub fn solver_address(n: u8) -> Address {
    Address::repeat_byte(n)
}

pub fn noise_private_key(n: u8) -> [u8; 32] {
    [n; 32]
}

pub fn noise_public_key(n: u8) -> B256 {
    B256::from(proof_transport::public_key(&noise_private_key(n)).unwrap())
}

pub fn registry() -> SolverRegistry {
    SolverRegistry::from_profiles([
        (
            solver_address(0x11),
            SolverProfile {
                noise_public_key: noise_public_key(0x33),
                active: true,
            },
        ),
        (
            solver_address(0x22),
            SolverProfile {
                noise_public_key: noise_public_key(0x44),
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
        chain_id: MOCK_CHAIN_ID,
        token_in: Address::repeat_byte(1),
        token_out: Address::repeat_byte(2),
        amount_in: U256::from(n),
        amount_out: U256::from(n * 2),
        expires_at_ms: chrono::Utc::now().timestamp_millis() + 60_000,
    }
}
