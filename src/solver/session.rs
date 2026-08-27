use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use alloy_primitives::{Address, B256, Signature};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::config::Network;

const CHALLENGE_TTL_MS: u64 = 60_000;
const SESSION_TTL_MS: u64 = 3_600_000;
const MAX_SOLVER_ENDPOINT_BYTES: usize = 512;

#[derive(Debug, Clone, Deserialize)]
pub struct ChallengeRequest {
    pub solver_endpoint: String,
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedSolver {
    pub solver_id: Address,
    pub solver_endpoint: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("challenge is unknown, expired, or already used")]
    UnknownChallenge,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("solver endpoint must be a canonical HTTP(S) URL (HTTPS outside localnet)")]
    InvalidEndpoint,
}

#[derive(Clone)]
pub struct SolverSessions {
    state: Arc<Mutex<State>>,
    domain: String,
    network: Network,
}

#[derive(Default)]
struct State {
    challenges: HashMap<B256, PendingChallenge>,
    tokens: HashMap<String, Session>,
    endpoints: HashMap<Address, EndpointLease>,
}

struct PendingChallenge {
    solver_endpoint: String,
    expires_at_ms: u64,
}

#[derive(Clone, Copy)]
struct Session {
    solver_id: Address,
    expires_at_ms: u64,
}

struct EndpointLease {
    solver_endpoint: String,
    expires_at_ms: u64,
}

pub fn domain(network: Network, chain_id: u64) -> String {
    format!("kage-orderbook:{network}:{chain_id}")
}

impl SolverSessions {
    pub fn new(domain: impl Into<String>, network: Network) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            domain: domain.into(),
            network,
        }
    }

    pub fn issue_challenge(
        &self,
        solver_endpoint: String,
        now_ms: u64,
    ) -> Result<ChallengeResponse, AuthError> {
        validate_solver_endpoint(self.network, &solver_endpoint)?;
        let nonce = B256::random();
        let expires_at_ms = now_ms + CHALLENGE_TTL_MS;
        let message = self.message_for(nonce, &solver_endpoint);
        let mut state = self.lock();
        state
            .challenges
            .retain(|_, challenge| challenge.expires_at_ms > now_ms);
        state.challenges.insert(
            nonce,
            PendingChallenge {
                solver_endpoint,
                expires_at_ms,
            },
        );

        Ok(ChallengeResponse {
            nonce,
            message,
            expires_at_ms,
        })
    }

    fn message_for(&self, nonce: B256, solver_endpoint: &str) -> String {
        format!("{}:{nonce}:solver_endpoint={solver_endpoint}", self.domain)
    }

    pub fn recover(
        &self,
        request: &SessionRequest,
        now_ms: u64,
    ) -> Result<AuthenticatedSolver, AuthError> {
        let challenge = self
            .lock()
            .challenges
            .remove(&request.nonce)
            .ok_or(AuthError::UnknownChallenge)?;
        if challenge.expires_at_ms <= now_ms {
            return Err(AuthError::UnknownChallenge);
        }

        let signature: Signature = request
            .signature
            .parse()
            .map_err(|_| AuthError::InvalidSignature)?;
        let solver_id = signature
            .recover_address_from_msg(self.message_for(request.nonce, &challenge.solver_endpoint))
            .map_err(|_| AuthError::InvalidSignature)?;
        Ok(AuthenticatedSolver {
            solver_id,
            solver_endpoint: challenge.solver_endpoint,
        })
    }

    pub fn open(&self, solver: AuthenticatedSolver, now_ms: u64) -> SessionResponse {
        let token = Uuid::new_v4().simple().to_string();
        let expires_at_ms = now_ms + SESSION_TTL_MS;
        let mut state = self.lock();
        state
            .tokens
            .retain(|_, session| session.expires_at_ms > now_ms);
        state
            .endpoints
            .retain(|_, endpoint| endpoint.expires_at_ms > now_ms);
        state.tokens.insert(
            token.clone(),
            Session {
                solver_id: solver.solver_id,
                expires_at_ms,
            },
        );
        state.endpoints.insert(
            solver.solver_id,
            EndpointLease {
                solver_endpoint: solver.solver_endpoint,
                expires_at_ms,
            },
        );

        SessionResponse {
            token,
            solver_id: solver.solver_id,
            expires_at_ms,
        }
    }

    pub fn resolve(&self, token: &str, now_ms: u64) -> Option<Address> {
        let session = *self.lock().tokens.get(token)?;
        (session.expires_at_ms > now_ms).then_some(session.solver_id)
    }

    pub fn solver_endpoint(&self, solver_id: Address, now_ms: u64) -> Option<String> {
        let mut state = self.lock();
        state
            .endpoints
            .retain(|_, endpoint| endpoint.expires_at_ms > now_ms);
        state
            .endpoints
            .get(&solver_id)
            .map(|endpoint| endpoint.solver_endpoint.clone())
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn validate_solver_endpoint(network: Network, value: &str) -> Result<(), AuthError> {
    if value.len() > MAX_SOLVER_ENDPOINT_BYTES {
        return Err(AuthError::InvalidEndpoint);
    }
    let parsed = reqwest::Url::parse(value).map_err(|_| AuthError::InvalidEndpoint)?;
    let valid_scheme = match network {
        Network::Localnet => matches!(parsed.scheme(), "http" | "https"),
        Network::Testnet | Network::Mainnet => parsed.scheme() == "https",
    };
    if !valid_scheme
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || value.ends_with('/')
    {
        return Err(AuthError::InvalidEndpoint);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, hex};

    use super::*;

    const NOW: u64 = 1_000_000;
    const ENDPOINT: &str = "http://127.0.0.1:3100";
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
        SolverSessions::new("kage-orderbook:localnet:31337", Network::Localnet)
    }

    fn answer(sessions: &SolverSessions, now_ms: u64) -> SessionRequest {
        let challenge = sessions
            .issue_challenge(ENDPOINT.to_owned(), now_ms)
            .unwrap();
        SessionRequest {
            nonce: challenge.nonce,
            signature: sign(&challenge.message),
        }
    }

    #[test]
    fn signed_challenge_binds_the_solver_address_and_endpoint() {
        let sessions = sessions();
        let recovered = sessions.recover(&answer(&sessions, NOW), NOW).unwrap();
        assert_eq!(recovered.solver_id, SIGNER);
        assert_eq!(recovered.solver_endpoint, ENDPOINT);
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
        let challenge = sessions.issue_challenge(ENDPOINT.to_owned(), NOW).unwrap();
        let request = SessionRequest {
            nonce: challenge.nonce,
            signature: sign(&format!(
                "kage-orderbook:mainnet:1:{}:solver_endpoint={ENDPOINT}",
                challenge.nonce
            )),
        };
        assert_ne!(sessions.recover(&request, NOW).unwrap().solver_id, SIGNER);
    }

    #[test]
    fn sessions_lease_the_authenticated_endpoint() {
        let sessions = sessions();
        let authenticated = sessions.recover(&answer(&sessions, NOW), NOW).unwrap();
        let session = sessions.open(authenticated, NOW);

        assert_eq!(sessions.resolve(&session.token, NOW), Some(SIGNER));
        assert_eq!(
            sessions.solver_endpoint(SIGNER, NOW),
            Some(ENDPOINT.to_owned())
        );
        assert_eq!(
            sessions.resolve(&session.token, session.expires_at_ms),
            None
        );
        assert_eq!(
            sessions.solver_endpoint(SIGNER, session.expires_at_ms),
            None
        );
    }

    #[test]
    fn production_networks_require_canonical_https_endpoints() {
        let sessions = SolverSessions::new("kage-orderbook:testnet:1", Network::Testnet);
        assert!(
            sessions
                .issue_challenge("https://solver.kage.test".to_owned(), NOW)
                .is_ok()
        );
        for endpoint in [
            "http://solver.kage.test",
            "https://solver.kage.test/",
            "https://solver.kage.test/path",
            "https://user@solver.kage.test",
        ] {
            assert!(matches!(
                sessions.issue_challenge(endpoint.to_owned(), NOW),
                Err(AuthError::InvalidEndpoint)
            ));
        }
    }
}
