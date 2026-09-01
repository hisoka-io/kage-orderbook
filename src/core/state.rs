use std::collections::HashMap;

use alloy_primitives::U256;

use super::{command::Command, events::OrderEvent};
use crate::order::{Order, OrderId, ProofOrderState, TradeTerms};

#[derive(Debug, Clone)]
pub struct CreateOrderOutcome {
    pub order: Order,
    pub created: bool,
}

pub struct Transition {
    pub events: Vec<OrderEvent>,
}

#[derive(Default)]
pub struct Orderbook {
    pub(super) orders: HashMap<OrderId, Order>,
}

#[derive(Debug)]
pub enum OrderError {
    AlreadyExists,
    InvalidTerms,
    InvalidPayload,
    NotFound,
    InvalidState,
}

pub fn handle(order: Option<&Order>, command: Command) -> Result<Transition, OrderError> {
    match command {
        Command::SubmitProofOrder { order_id, terms } => {
            if order.is_some() {
                return Err(OrderError::AlreadyExists);
            }
            if terms.chain_id == 0
                || terms.token_in == terms.token_out
                || terms.amount_in == U256::ZERO
                || terms.amount_out == U256::ZERO
            {
                return Err(OrderError::InvalidTerms);
            }

            Ok(Transition {
                events: vec![
                    OrderEvent::OrderCreated { order_id, terms },
                    OrderEvent::OrderValidated { order_id },
                    OrderEvent::SolverReservationRequested { order_id, terms },
                ],
            })
        }
        Command::SolverReserved {
            order_id,
            solver_id,
        } => {
            let order = order.ok_or(OrderError::NotFound)?;
            if order.state != ProofOrderState::ReservationPending {
                return Err(OrderError::InvalidState);
            }

            Ok(Transition {
                events: vec![
                    OrderEvent::SolverAssigned {
                        order_id,
                        solver_id,
                    },
                    OrderEvent::ProofDisclosed {
                        order_id,
                        solver_id,
                    },
                ],
            })
        }
        Command::SolverDeclined {
            order_id,
            solver_id,
        } => {
            let order = order.ok_or(OrderError::NotFound)?;
            if order.solver != Some(solver_id)
                || !matches!(
                    order.state,
                    ProofOrderState::Assigned | ProofOrderState::ProofDelivered
                )
            {
                return Err(OrderError::InvalidState);
            }
            let expires_at_ms = order.expires_at_ms.ok_or(OrderError::InvalidState)?;
            let terms = TradeTerms {
                chain_id: order.chain_id,
                token_in: order.token_in,
                token_out: order.token_out,
                amount_in: order.amount_in,
                amount_out: order.amount_out,
                expires_at_ms,
            };

            Ok(Transition {
                events: vec![OrderEvent::SolverReservationRequested { order_id, terms }],
            })
        }
        Command::RetryReservation { order_id } => {
            let order = order.ok_or(OrderError::NotFound)?;
            if order.state != ProofOrderState::ReservationPending {
                return Err(OrderError::InvalidState);
            }
            let terms = TradeTerms {
                chain_id: order.chain_id,
                token_in: order.token_in,
                token_out: order.token_out,
                amount_in: order.amount_in,
                amount_out: order.amount_out,
                expires_at_ms: order.expires_at_ms.ok_or(OrderError::InvalidState)?,
            };
            Ok(Transition {
                events: vec![OrderEvent::SolverReservationRequested { order_id, terms }],
            })
        }
        Command::ExpireOrder { order_id } => {
            let order = order.ok_or(OrderError::NotFound)?;
            if order.state == ProofOrderState::Expired {
                return Err(OrderError::InvalidState);
            }

            Ok(Transition {
                events: vec![OrderEvent::OrderExpired { order_id }],
            })
        }
    }
}

impl Orderbook {
    pub(super) fn from_orders(orders: impl IntoIterator<Item = Order>) -> Self {
        Self {
            orders: orders.into_iter().map(|order| (order.id, order)).collect(),
        }
    }

    pub(super) fn prepare(
        &self,
        command: Command,
        timestamp_ms: i64,
    ) -> Result<Option<PreparedTransition>, OrderError> {
        if let Command::SubmitProofOrder { terms, .. } = &command
            && terms.expires_at_ms <= timestamp_ms
        {
            return Err(OrderError::InvalidTerms);
        }
        let order_id = command.order_id();
        let current = self.orders.get(&order_id);
        if current.is_some_and(|order| order.is_expired_at(timestamp_ms))
            && !matches!(&command, Command::ExpireOrder { .. })
        {
            return Err(OrderError::InvalidState);
        }
        if is_idempotent(current, &command) {
            return Ok(None);
        }
        let expected_version = current.map(|order| order.version);
        let transition = handle(current, command)?;
        let mut order = match current {
            Some(order) => order.clone(),
            None => match transition.events.first() {
                Some(OrderEvent::OrderCreated { order_id, terms }) => Order::new(*order_id, *terms),
                _ => return Err(OrderError::NotFound),
            },
        };

        for event in &transition.events {
            order.apply(event);
        }
        Ok(Some(PreparedTransition {
            order,
            expected_version,
            transition,
        }))
    }

    pub(super) fn commit(&mut self, prepared: &PreparedTransition) {
        self.orders
            .insert(prepared.order.id, prepared.order.clone());
    }
}

fn is_idempotent(order: Option<&Order>, command: &Command) -> bool {
    let Some(order) = order else {
        return false;
    };

    match command {
        Command::SolverReserved { solver_id, .. } => {
            order.state == ProofOrderState::ProofDelivered && order.solver == Some(*solver_id)
        }
        Command::SolverDeclined { .. } => order.state == ProofOrderState::ReservationPending,
        Command::RetryReservation { .. } => false,
        Command::SubmitProofOrder { .. } | Command::ExpireOrder { .. } => false,
    }
}

pub(super) struct PreparedTransition {
    pub(super) order: Order,
    pub(super) expected_version: Option<u64>,
    pub(super) transition: Transition,
}
