use alloy_primitives::U256;
pub use kage_types::{
    identifiers::{OrderAccessTokenHash, OrderId, SolverId, TokenAddress},
    orders::TradeTerms,
    proof_orders::ProofOrderState,
};
use serde::{Deserialize, Serialize};

use crate::core::events::OrderEvent;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub state: ProofOrderState,
    pub version: u64,
    pub chain_id: u64,

    pub token_in: TokenAddress,
    pub token_out: TokenAddress,

    pub amount_in: U256,
    pub amount_out: U256,
    pub expires_at_ms: Option<i64>,

    pub solver: Option<SolverId>,
}

impl Order {
    pub fn new(id: OrderId, terms: TradeTerms) -> Self {
        Self {
            id,
            state: ProofOrderState::Submitted,
            version: 0,
            chain_id: terms.chain_id,
            token_in: terms.token_in,
            token_out: terms.token_out,
            amount_in: terms.amount_in,
            amount_out: terms.amount_out,
            expires_at_ms: Some(terms.expires_at_ms),
            solver: None,
        }
    }

    pub fn is_expired_at(&self, timestamp_ms: i64) -> bool {
        !matches!(
            self.state,
            ProofOrderState::Expired | ProofOrderState::Closed
        ) && self
            .expires_at_ms
            .is_some_and(|expires_at_ms| expires_at_ms <= timestamp_ms)
    }

    pub fn apply(&mut self, event: &OrderEvent) {
        match event {
            OrderEvent::OrderCreated { .. } => {
                self.state = ProofOrderState::Submitted;
            }
            OrderEvent::OrderValidated { .. } => {
                self.state = ProofOrderState::Submitted;
            }
            OrderEvent::SolverReservationRequested { .. } => {
                self.solver = None;
                self.state = ProofOrderState::ReservationPending;
            }
            OrderEvent::SolverAssigned { solver_id, .. } => {
                self.solver = Some(*solver_id);
                self.state = ProofOrderState::Assigned;
            }
            OrderEvent::ProofDisclosed { .. } => {
                self.state = ProofOrderState::ProofDelivered;
            }
            OrderEvent::OrderExpired { .. } => {
                self.state = ProofOrderState::Expired;
            }
        }
        self.version += 1;
    }
}
