use serde::{Deserialize, Serialize};

use crate::order::{OrderId, SolverId, TradeTerms, TxHash};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderEvent {
    OrderCreated {
        order_id: OrderId,
        terms: TradeTerms,
    },

    OrderValidated {
        order_id: OrderId,
    },

    SolverReservationRequested {
        order_id: OrderId,
    },

    SolverAssigned {
        order_id: OrderId,
        solver_id: SolverId,
    },

    SolverSessionReady {
        order_id: OrderId,
        solver_id: SolverId,
        noise_public_key: Vec<u8>,
    },

    ProofRelayed {
        order_id: OrderId,
        solver_id: SolverId,
    },

    ExecutionStarted {
        order_id: OrderId,
        tx_hash: TxHash,
    },

    OrderFilled {
        order_id: OrderId,
        tx_hash: TxHash,
    },

    OrderExpired {
        order_id: OrderId,
    },
}

impl OrderEvent {
    pub fn order_id(&self) -> OrderId {
        match self {
            OrderEvent::OrderCreated { order_id, .. }
            | OrderEvent::OrderValidated { order_id }
            | OrderEvent::SolverReservationRequested { order_id }
            | OrderEvent::SolverAssigned { order_id, .. }
            | OrderEvent::SolverSessionReady { order_id, .. }
            | OrderEvent::ProofRelayed { order_id, .. }
            | OrderEvent::ExecutionStarted { order_id, .. }
            | OrderEvent::OrderFilled { order_id, .. }
            | OrderEvent::OrderExpired { order_id } => *order_id,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            OrderEvent::OrderCreated { .. } => "OrderCreated",
            OrderEvent::OrderValidated { .. } => "OrderValidated",
            OrderEvent::SolverReservationRequested { .. } => "SolverReservationRequested",
            OrderEvent::SolverAssigned { .. } => "SolverAssigned",
            OrderEvent::SolverSessionReady { .. } => "SolverSessionReady",
            OrderEvent::ProofRelayed { .. } => "ProofRelayed",
            OrderEvent::ExecutionStarted { .. } => "ExecutionStarted",
            OrderEvent::OrderFilled { .. } => "OrderFilled",
            OrderEvent::OrderExpired { .. } => "OrderExpired",
        }
    }
}
