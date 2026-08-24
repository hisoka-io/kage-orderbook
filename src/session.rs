use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use alloy_primitives::{Address, B256, Signature};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const CHALLENGE_TTL_MS: u64 = 60_000;
const SESSION_TTL_MS: u64 = 3_600_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChallengeResponse {
    pub nonce: B256,
    pub message: String,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRequest {
    pub nonce: B256,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionResponse {
    pub token: String,
    pub solver_id: Address,
    pub expires_at_ms: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("challenge is unknown, expired, or already used")]
    UnknownChallenge,
    #[error("invalid signature")]
    InvalidSignature,
}

#[derive(Clone)]
pub struct SolverSessions {
    state: Arc<Mutex<State>>,
    domain: String,
}

#[derive(Default)]
struct State {
    challenges: HashMap<B256, u64>,
    tokens: HashMap<String, Session>,
}

#[derive(Clone, Copy)]
struct Session {
    solver_id: Address,
    expires_at_ms: u64,
}

pub fn domain(network: crate::config::Network, chain_id: u64) -> String {
    format!("kage-orderbook:{network}:{chain_id}")
}

impl SolverSessions {
    pub fn new(domain: impl Into<String>) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            domain: domain.into(),
        }
    }

    pub fn issue_challenge(&self, now_ms: u64) -> ChallengeResponse {
        let nonce = B256::random();
        let expires_at_ms = now_ms + CHALLENGE_TTL_MS;
        let mut state = self.lock();
        state.challenges.retain(|_, expiry| *expiry > now_ms);
        state.challenges.insert(nonce, expires_at_ms);

        ChallengeResponse {
            nonce,
            message: self.message_for(nonce),
            expires_at_ms,
        }
    }

    fn message_for(&self, nonce: B256) -> String {
        format!("{}:{nonce}", self.domain)
    }

    pub fn recover(&self, request: &SessionRequest, now_ms: u64) -> Result<Address, AuthError> {
        let expiry = self
            .lock()
            .challenges
            .remove(&request.nonce)
            .ok_or(AuthError::UnknownChallenge)?;
        if expiry <= now_ms {
            return Err(AuthError::UnknownChallenge);
        }

        let signature: Signature = request
            .signature
            .parse()
            .map_err(|_| AuthError::InvalidSignature)?;
        signature
            .recover_address_from_msg(self.message_for(request.nonce))
            .map_err(|_| AuthError::InvalidSignature)
    }

    pub fn open(&self, solver_id: Address, now_ms: u64) -> SessionResponse {
        let token = Uuid::new_v4().simple().to_string();
        let expires_at_ms = now_ms + SESSION_TTL_MS;
        let mut state = self.lock();
        state
            .tokens
            .retain(|_, session| session.expires_at_ms > now_ms);
        state.tokens.insert(
            token.clone(),
            Session {
                solver_id,
                expires_at_ms,
            },
        );

        SessionResponse {
            token,
            solver_id,
            expires_at_ms,
        }
    }

    pub fn resolve(&self, token: &str, now_ms: u64) -> Option<Address> {
        let session = *self.lock().tokens.get(token)?;
        (session.expires_at_ms > now_ms).then_some(session.solver_id)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, hex};

    use super::*;

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
    fn a_signed_challenge_recovers_the_signing_address() {
        let sessions = sessions();
        assert_eq!(sessions.recover(&answer(&sessions, NOW), NOW), Ok(SIGNER));
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
    fn a_signature_from_another_domain_does_not_authenticate() {
        let sessions = sessions();
        let challenge = sessions.issue_challenge(NOW);
        let request = SessionRequest {
            nonce: challenge.nonce,
            signature: sign(&format!("kage-orderbook:mainnet:1:{}", challenge.nonce)),
        };
        assert_ne!(sessions.recover(&request, NOW), Ok(SIGNER));
    }

    #[test]
    fn tokens_resolve_until_they_expire() {
        let sessions = sessions();
        let session = sessions.open(SIGNER, NOW);

        assert_eq!(sessions.resolve(&session.token, NOW), Some(SIGNER));
        assert_eq!(
            sessions.resolve(&session.token, session.expires_at_ms),
            None
        );
        assert_eq!(sessions.resolve("not-a-token", NOW), None);
    }
}
