use alloy_primitives::{Address, B256};
use kage_types::routing::PreviewRoute;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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
    #[error("solver session is unknown or expired")]
    UnknownSession,
    #[error("solver capabilities are invalid")]
    InvalidCapabilities,
    #[error("solver capability revision must increase")]
    StaleCapabilityRevision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AuthenticatedSession {
    pub solver_id: Address,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct CapabilityRoute {
    pub route: PreviewRoute,
    pub minimum_margin_bps: u16,
    pub max_jobs_total: u16,
    pub amount_out_total: alloy_primitives::U256,
    pub required_proof_lifetime_seconds: u64,
}
