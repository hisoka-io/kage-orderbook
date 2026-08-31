use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use alloy_primitives::{Address, B256};
use kage_types::routing::SolverCapabilities;

mod auth;
mod capabilities;
mod types;

pub use auth::domain;
pub(crate) use types::AuthenticatedSession;
pub use types::{AuthError, CapabilityRoute, ChallengeResponse, SessionRequest, SessionResponse};

const CHALLENGE_TTL_MS: u64 = 60_000;
const SESSION_TTL_MS: u64 = 15 * 60_000;
const CAPABILITY_TTL_MS: u64 = 60_000;

#[derive(Clone)]
pub struct SolverSessions {
    state: Arc<Mutex<State>>,
    capacity_serial: Arc<tokio::sync::Mutex<()>>,
    domain: String,
}

#[derive(Default)]
struct State {
    challenges: HashMap<B256, u64>,
    tokens: HashMap<String, AuthenticatedSession>,
    capabilities: HashMap<Address, CapabilityLease>,
}

struct CapabilityLease {
    capabilities: SolverCapabilities,
    expires_at_ms: u64,
}

impl SolverSessions {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) async fn capacity_guard(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.capacity_serial.clone().lock_owned().await
    }
}

#[cfg(test)]
mod tests;
