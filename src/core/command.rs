use crate::order::{OrderId, SolverId, TradeTerms};

pub enum Command {
    SubmitProofOrder {
        order_id: OrderId,
        terms: TradeTerms,
    },

    SolverReserved {
        order_id: OrderId,
        solver_id: SolverId,
    },

    SolverDeclined {
        order_id: OrderId,
        solver_id: SolverId,
    },

    RetryReservation {
        order_id: OrderId,
    },

    ExpireOrder {
        order_id: OrderId,
    },
}

impl Command {
    pub fn order_id(&self) -> OrderId {
        match self {
            Command::SubmitProofOrder { order_id, .. }
            | Command::SolverReserved { order_id, .. }
            | Command::SolverDeclined { order_id, .. }
            | Command::RetryReservation { order_id }
            | Command::ExpireOrder { order_id } => *order_id,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Command::SubmitProofOrder { .. } => "SubmitProofOrder",
            Command::SolverReserved { .. } => "SolverReserved",
            Command::SolverDeclined { .. } => "SolverDeclined",
            Command::RetryReservation { .. } => "RetryReservation",
            Command::ExpireOrder { .. } => "ExpireOrder",
        }
    }
}
