use alloy_primitives::{Address, B256, U256, address, hex};
use kage_types::routing::{SolverCapabilities, SolverMarket};

use super::{capabilities::validate_capabilities, *};

const NOW: u64 = 1_000_000;
const KEY: [u8; 32] = hex!("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d");
const SIGNER: Address = address!("70997970C51812dc3A010C7d01b50e0d17dc79C8");

fn sign(message: &str) -> String {
    let key = k256::ecdsa::SigningKey::from_slice(&KEY).unwrap();
    let hash = alloy_primitives::eip191_hash_message(message);
    let (signature, recovery) = key.sign_prehash_recoverable(hash.as_slice()).unwrap();
    let mut bytes = [0_u8; 65];
    bytes[..64].copy_from_slice(&signature.to_bytes());
    bytes[64] = 27 + recovery.to_byte();
    format!("0x{}", hex::encode(bytes))
}

fn sessions() -> SolverSessions {
    SolverSessions::new("kage-orderbook:localnet:31337")
}

fn answer(sessions: &SolverSessions, now_ms: u64) -> SessionRequest {
    let challenge = sessions.issue_challenge(now_ms);
    SessionRequest {
        nonce: challenge.nonce,
        signature: sign(&challenge.message),
    }
}

#[test]
fn signed_challenge_recovers_the_solver_address() {
    let sessions = sessions();
    let recovered = sessions.recover(&answer(&sessions, NOW), NOW).unwrap();
    assert_eq!(recovered, SIGNER);
}

#[test]
fn a_challenge_cannot_be_answered_twice() {
    let sessions = sessions();
    let request = answer(&sessions, NOW);

    assert!(sessions.recover(&request, NOW).is_ok());
    assert_eq!(
        sessions.recover(&request, NOW),
        Err(AuthError::UnknownChallenge),
        "a captured signature was replayable"
    );
}

#[test]
fn an_expired_challenge_is_refused() {
    let sessions = sessions();
    let request = answer(&sessions, NOW);
    assert_eq!(
        sessions.recover(&request, NOW + CHALLENGE_TTL_MS),
        Err(AuthError::UnknownChallenge)
    );
}

#[test]
fn a_signature_from_another_domain_does_not_recover_the_registered_solver() {
    let sessions = sessions();
    let challenge = sessions.issue_challenge(NOW);
    let request = SessionRequest {
        nonce: challenge.nonce,
        signature: sign(&format!("kage-orderbook:mainnet:1:{}", challenge.nonce)),
    };
    assert_ne!(sessions.recover(&request, NOW).unwrap(), SIGNER);
}

#[test]
fn sessions_lease_the_authenticated_solver_identity() {
    let sessions = sessions();
    let solver_id = sessions.recover(&answer(&sessions, NOW), NOW).unwrap();
    let session = sessions.open(solver_id, NOW);

    assert_eq!(session.expires_at_ms, NOW + 15 * 60_000);
    assert_eq!(sessions.resolve(&session.token, NOW), Some(SIGNER));
    assert_eq!(
        sessions.resolve(&session.token, session.expires_at_ms),
        None
    );
}

#[test]
fn opening_a_new_session_invalidates_the_previous_bearer() {
    let sessions = sessions();
    let first = sessions.open(SIGNER, NOW);
    let second = sessions.open(SIGNER, NOW + 1);

    assert_eq!(sessions.resolve(&first.token, NOW + 1), None);
    assert_eq!(sessions.resolve(&second.token, NOW + 1), Some(SIGNER));
    assert_ne!(first.token, second.token);
}

#[test]
fn authenticated_capabilities_are_revisioned_and_expire() {
    let sessions = sessions();
    let solver_id = sessions.recover(&answer(&sessions, NOW), NOW).unwrap();
    let session = sessions.open(solver_id, NOW);
    let capabilities = SolverCapabilities {
        revision: 1,
        max_in_flight: 2,
        encryption_key_id: B256::repeat_byte(4),
        encryption_public_key: vec![5; 32],
        key_expires_at_ms: (NOW + 120_000) as i64,
        markets: vec![SolverMarket {
            chain_id: 31_337,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
            min_amount_in: U256::from(1),
            max_amount_in: U256::from(1_000),
            available_amount_out: U256::from(2_000),
            minimum_margin_bps: 35,
        }],
    };
    sessions
        .register_capabilities(&session.token, capabilities.clone(), NOW)
        .unwrap();
    assert_eq!(
        sessions
            .routes_for_market(
                31_337,
                Address::repeat_byte(1),
                Address::repeat_byte(2),
                NOW,
            )
            .len(),
        1
    );
    assert_eq!(
        sessions.register_capabilities(&session.token, capabilities.clone(), NOW),
        Err(AuthError::StaleCapabilityRevision),
    );
    let restarted = sessions.open(SIGNER, NOW + 1);
    assert_eq!(sessions.resolve(&session.token, NOW + 1), None);
    sessions
        .register_capabilities(&restarted.token, capabilities, NOW + 1)
        .unwrap();
    assert!(
        sessions
            .routes_for_market(
                31_337,
                Address::repeat_byte(1),
                Address::repeat_byte(2),
                NOW + 1 + CAPABILITY_TTL_MS,
            )
            .is_empty()
    );
}

#[test]
fn capabilities_reject_duplicate_markets_and_zero_live_liquidity() {
    let market = SolverMarket {
        chain_id: 31_337,
        token_in: Address::repeat_byte(1),
        token_out: Address::repeat_byte(2),
        min_amount_in: U256::from(1),
        max_amount_in: U256::from(1_000),
        available_amount_out: U256::from(2_000),
        minimum_margin_bps: 35,
    };
    let mut capabilities = SolverCapabilities {
        revision: 1,
        max_in_flight: 2,
        encryption_key_id: B256::repeat_byte(4),
        encryption_public_key: vec![5; 32],
        key_expires_at_ms: (NOW + 120_000) as i64,
        markets: vec![market.clone(), market.clone()],
    };
    assert_eq!(
        validate_capabilities(&capabilities, NOW),
        Err(AuthError::InvalidCapabilities)
    );
    capabilities.markets = vec![market];
    capabilities.markets[0].available_amount_out = U256::ZERO;
    assert_eq!(
        validate_capabilities(&capabilities, NOW),
        Err(AuthError::InvalidCapabilities)
    );
}
