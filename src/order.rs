use alloy_primitives::U256;
pub use kage_types::{
    identifiers::{OrderCommitment, OrderId, SolverId, TokenAddress, TxHash},
    orders::{OrderState, OrderV1, TradeTerms},
};
use serde::{Deserialize, Serialize};

use crate::core::events::OrderEvent;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub state: OrderState,
    pub version: u64,
    pub chain_id: u64,

    pub token_in: TokenAddress,
    pub token_out: TokenAddress,

    pub amount_in: U256,
    pub amount_out: U256,
    pub expires_at_ms: Option<i64>,

    pub solver: Option<SolverId>,
    pub solver_noise_public_key: Option<Vec<u8>>,
    pub tx_hash: Option<TxHash>,
}

impl Order {
    pub fn new(id: OrderId, terms: TradeTerms) -> Self {
        Self {
            id,
            state: OrderState::Created,
            version: 0,
            chain_id: terms.chain_id,
            token_in: terms.token_in,
            token_out: terms.token_out,
            amount_in: terms.amount_in,
            amount_out: terms.amount_out,
            expires_at_ms: Some(terms.expires_at_ms),
            solver: None,
            solver_noise_public_key: None,
            tx_hash: None,
        }
    }

    pub fn is_expired_at(&self, timestamp_ms: i64) -> bool {
        !self.state.is_terminal()
            && self
                .expires_at_ms
                .is_some_and(|expires_at_ms| expires_at_ms <= timestamp_ms)
    }

    pub fn apply(&mut self, event: &OrderEvent) {
        match event {
            OrderEvent::OrderCreated { .. } => {
                self.state = OrderState::Created;
            }
            OrderEvent::OrderValidated { .. } => {
                self.state = OrderState::Validated;
            }
            OrderEvent::SolverReservationRequested { .. } => {
                self.solver = None;
                self.solver_noise_public_key = None;
                self.tx_hash = None;
                self.state = OrderState::Reserving;
            }
            OrderEvent::SolverAssigned { solver_id, .. } => {
                self.solver = Some(*solver_id);
                self.state = OrderState::Assigned;
            }
            OrderEvent::SolverSessionReady {
                noise_public_key, ..
            } => {
                self.solver_noise_public_key = Some(noise_public_key.clone());
                self.state = OrderState::AwaitingUserProof;
            }
            OrderEvent::ProofRelayed { .. } => {
                self.state = OrderState::ProofRelayed;
            }
            OrderEvent::ExecutionStarted { tx_hash, .. } => {
                self.tx_hash = Some(*tx_hash);
                self.state = OrderState::Executing;
            }
            OrderEvent::OrderFilled { .. } => {
                self.state = OrderState::Filled;
            }
            OrderEvent::OrderExpired { .. } => {
                self.state = OrderState::Expired;
            }
        }
        self.version += 1;
    }
}

impl From<&Order> for OrderV1 {
    fn from(order: &Order) -> Self {
        Self {
            id: order.id,
            state: order.state,
            version: order.version,
            chain_id: order.chain_id,
            token_in: order.token_in,
            token_out: order.token_out,
            amount_in: order.amount_in,
            amount_out: order.amount_out,
            expires_at_ms: order.expires_at_ms,
            solver: order.solver,
            solver_noise_public_key: order.solver_noise_public_key.clone(),
            tx_hash: order.tx_hash,
        }
    }
}
