use crate::order::{OrderId, SolverId, TradeTerms, TxHash};

pub enum Command {
    CreateOrder {
        order_id: OrderId,
        terms: TradeTerms,
    },

    SolverReserved {
        order_id: OrderId,
        solver_id: SolverId,
        noise_public_key: Vec<u8>,
    },

    RelayEncryptedProof {
        order_id: OrderId,
        ciphertext: Vec<u8>,
    },

    ExecutionStarted {
        order_id: OrderId,
        solver_id: SolverId,
        tx_hash: TxHash,
    },

    SettlementObserved {
        order_id: OrderId,
        tx_hash: TxHash,
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
            | Command::RelayEncryptedProof { order_id, .. }
            | Command::ExecutionStarted { order_id, .. }
            | Command::SettlementObserved { order_id, .. }
            | Command::ExpireOrder { order_id } => *order_id,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Command::CreateOrder { .. } => "CreateOrder",
            Command::SolverReserved { .. } => "SolverReserved",
            Command::RelayEncryptedProof { .. } => "RelayEncryptedProof",
            Command::ExecutionStarted { .. } => "ExecutionStarted",
            Command::SettlementObserved { .. } => "SettlementObserved",
            Command::ExpireOrder { .. } => "ExpireOrder",
        }
    }
}
