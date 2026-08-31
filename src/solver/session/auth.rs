use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, B256, Signature};
use uuid::Uuid;

use crate::config::Network;

use super::{
    AuthError, AuthenticatedSession, CHALLENGE_TTL_MS, ChallengeResponse, SESSION_TTL_MS,
    SessionRequest, SessionResponse, SolverSessions, State,
};

pub fn domain(network: Network, chain_id: u64) -> String {
    format!("kage-orderbook:{network}:{chain_id}")
}

impl SolverSessions {
    pub fn new(domain: impl Into<String>) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            capacity_serial: Arc::new(tokio::sync::Mutex::new(())),
            domain: domain.into(),
        }
    }

    pub fn issue_challenge(&self, now_ms: u64) -> ChallengeResponse {
        let nonce = B256::random();
        let expires_at_ms = now_ms + CHALLENGE_TTL_MS;
        let message = self.message_for(nonce);
        let mut state = self.lock();
        state
            .challenges
            .retain(|_, challenge_expires_at_ms| *challenge_expires_at_ms > now_ms);
        state.challenges.insert(nonce, expires_at_ms);

        ChallengeResponse {
            nonce,
            message,
            expires_at_ms,
        }
    }

    fn message_for(&self, nonce: B256) -> String {
        format!("{}:{nonce}", self.domain)
    }

    pub fn recover(&self, request: &SessionRequest, now_ms: u64) -> Result<Address, AuthError> {
        let expires_at_ms = self
            .lock()
            .challenges
            .remove(&request.nonce)
            .ok_or(AuthError::UnknownChallenge)?;
        if expires_at_ms <= now_ms {
            return Err(AuthError::UnknownChallenge);
        }

        let signature: Signature = request
            .signature
            .parse()
            .map_err(|_| AuthError::InvalidSignature)?;
        let solver_id = signature
            .recover_address_from_msg(self.message_for(request.nonce))
            .map_err(|_| AuthError::InvalidSignature)?;
        Ok(solver_id)
    }

    pub(crate) fn open(&self, solver_id: Address, now_ms: u64) -> SessionResponse {
        let token = Uuid::new_v4().simple().to_string();
        let expires_at_ms = now_ms + SESSION_TTL_MS;
        let mut state = self.lock();
        state
            .tokens
            .retain(|_, session| session.expires_at_ms > now_ms && session.solver_id != solver_id);
        state.capabilities.remove(&solver_id);
        state.tokens.insert(
            token.clone(),
            AuthenticatedSession {
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

    pub(crate) fn resolve_session(&self, token: &str, now_ms: u64) -> Option<AuthenticatedSession> {
        let session = *self.lock().tokens.get(token)?;
        (session.expires_at_ms > now_ms).then_some(session)
    }

    pub fn resolve(&self, token: &str, now_ms: u64) -> Option<Address> {
        self.resolve_session(token, now_ms)
            .map(|session| session.solver_id)
    }
}
