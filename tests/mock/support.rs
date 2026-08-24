use alloy_primitives::{Address, B256, U256};
use kage_orderbook::{core::guards::MOCK_CHAIN_ID, registry::SolverRegistry};
use kage_types::{orders::TradeTerms, registry::SolverProfile};

use super::proof_transport;

pub fn solver_key(n: u8) -> k256::ecdsa::SigningKey {
    k256::ecdsa::SigningKey::from_slice(&[n; 32]).expect("valid scalar")
}

pub fn solver_address(n: u8) -> Address {
    let key = solver_key(n);
    let encoded = key.verifying_key().to_encoded_point(false);
    Address::from_raw_public_key(&encoded.as_bytes()[1..])
}

pub async fn bearer(http_url: &str, n: u8) -> String {
    #[derive(serde::Deserialize)]
    struct Challenge {
        nonce: B256,
        message: String,
    }
    #[derive(serde::Deserialize)]
    struct Session {
        token: String,
    }

    let client = reqwest::Client::new();
    let challenge: Challenge = client
        .post(format!("{http_url}/solver/challenge"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let hash = alloy_primitives::eip191_hash_message(&challenge.message);
    let (signature, recovery) = solver_key(n)
        .sign_prehash_recoverable(hash.as_slice())
        .unwrap();
    let mut bytes = [0_u8; 65];
    bytes[..64].copy_from_slice(&signature.to_bytes());
    bytes[64] = 27 + recovery.to_byte();

    let session: Session = client
        .post(format!("{http_url}/solver/session"))
        .json(&serde_json::json!({
            "nonce": challenge.nonce,
            "signature": format!("0x{}", alloy_primitives::hex::encode(bytes)),
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    format!("Bearer {}", session.token)
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
