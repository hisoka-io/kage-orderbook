use std::{collections::HashSet, sync::Arc};

use alloy_primitives::Address;

use crate::{
    Shutdown,
    assignment::AssignmentIssuer,
    complaint::{ComplaintEvidenceCipher, ComplaintVerifier},
    config::{ApiSettings, ProofOrderSettings},
    core::engine::OrderbookHandle,
    preview::PreviewService,
    readiness::ServiceReadiness,
    registry::SolverRegistry,
    session::SolverSessions,
    storage::ProofOrderRepository,
};

#[derive(Clone)]
pub(super) struct ApiState {
    pub(super) assignment_issuer: AssignmentIssuer,
    pub(super) orderbook: OrderbookHandle,
    pub(super) registry: SolverRegistry,
    pub(super) sessions: SolverSessions,
    pub(super) readiness: ServiceReadiness,
    pub(super) api: ApiSettings,
    pub(super) preview: Option<PreviewService>,
    pub(super) proof_orders: ProofOrderRepository,
    pub(super) complaint_verifier: Option<ComplaintVerifier>,
    pub(super) complaint_evidence_cipher: Option<ComplaintEvidenceCipher>,
    pub(super) allowed_solvers: Arc<HashSet<Address>>,
    pub(super) proof_order_settings: ProofOrderSettings,
    pub(super) shutdown: Shutdown,
}

pub(super) fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}
