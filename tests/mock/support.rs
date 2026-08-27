use alloy_primitives::{Address, B256, U256};
use kage_orderbook::{
    assignment::AssignmentIssuer,
    core::guards::MOCK_CHAIN_ID,
    registry::{SolverProfile, SolverRegistry},
};
use kage_types::orders::TradeTerms;

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
        .post(format!("{http_url}/v1/solver/challenge"))
        .json(&serde_json::json!({
            "solver_endpoint": format!("http://127.0.0.1:{}", 3100 + u16::from(n))
        }))
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
        .post(format!("{http_url}/v1/solver/session"))
        .json(&serde_json::json!({
            "nonce": challenge.nonce,
            "signature": format!("0x{}", alloy_primitives::hex::encode(bytes)),
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    format!("Bearer {}", session.token)
}

pub fn registry() -> SolverRegistry {
    SolverRegistry::from_profiles([
        (
            solver_address(0x11),
            SolverProfile {
                noise_public_key: B256::repeat_byte(0x33),
                active: true,
            },
        ),
        (
            solver_address(0x22),
            SolverProfile {
                noise_public_key: B256::repeat_byte(0x44),
                active: true,
            },
        ),
    ])
}

pub fn assignment_issuer() -> AssignmentIssuer {
    AssignmentIssuer::for_test(
        alloy::signers::local::PrivateKeySigner::from_slice(&[7; 32]).unwrap(),
        60_000,
    )
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
