use alloy_primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::core::events::OrderEvent;

pub type OrderId = Uuid;
pub type TokenAddress = Address;
pub type TxHash = B256;
pub type SolverId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub state: OrderState,
    pub version: u64,

    pub token_in: TokenAddress,
    pub token_out: TokenAddress,

    pub amount_in: U256,
    pub amount_out: U256,

    pub solver: Option<SolverId>,
    pub solver_noise_public_key: Option<Vec<u8>>,
    pub tx_hash: Option<TxHash>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderState {
    Created,
    Validated,
    Reserving,
    Assigned,
    AwaitingUserProof,
    ProofRelayed,
    Executing,

    Filled,
    Expired,
    Cancelled,
    Failed,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TradeTerms {
    pub token_in: TokenAddress,
    pub token_out: TokenAddress,
    pub amount_in: U256,
    pub amount_out: U256,
}

impl Order {
    pub fn new(id: OrderId, terms: TradeTerms) -> Self {
        Self {
            id,
            state: OrderState::Created,
            version: 0,
            token_in: terms.token_in,
            token_out: terms.token_out,
            amount_in: terms.amount_in,
            amount_out: terms.amount_out,
            solver: None,
            solver_noise_public_key: None,
            tx_hash: None,
        }
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
