use crate::order::{OrderCommitment, OrderId, SolverId, TradeTerms};

pub enum Command {
    CreateOrder {
        order_id: OrderId,
        order_commitment: OrderCommitment,
        terms: TradeTerms,
    },

    SolverReserved {
        order_id: OrderId,
        solver_id: SolverId,
        noise_public_key: Vec<u8>,
    },

    SolverDeclined {
        order_id: OrderId,
        solver_id: SolverId,
    },

    ExpireOrder {
        order_id: OrderId,
    },
}

impl Command {
    pub fn order_id(&self) -> OrderId {
        match self {
            Command::CreateOrder { order_id, .. }
            | Command::SolverReserved { order_id, .. }
            | Command::SolverDeclined { order_id, .. }
            | Command::ExpireOrder { order_id } => *order_id,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Command::CreateOrder { .. } => "CreateOrder",
            Command::SolverReserved { .. } => "SolverReserved",
            Command::SolverDeclined { .. } => "SolverDeclined",
            Command::ExpireOrder { .. } => "ExpireOrder",
        }
    }
}
