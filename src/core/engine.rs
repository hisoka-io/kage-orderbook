use std::collections::HashMap;

use alloy_primitives::{B256, U256};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::command::Command;
use super::events::OrderEvent;
use crate::logging::short_id;
use crate::order::{Order, OrderId, OrderState, SolverId};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SolverProofDelivery {
    pub order_id: OrderId,
    pub ciphertext: Vec<u8>,
}

pub struct SolverDelivery {
    solver_id: SolverId,
    proof: SolverProofDelivery,
}

pub struct Transition {
    pub events: Vec<OrderEvent>,
    pub deliveries: Vec<SolverDelivery>,
}

#[derive(Default)]
pub struct Orderbook {
    orders: HashMap<OrderId, Order>,
    solver_proofs: HashMap<SolverId, Vec<SolverProofDelivery>>,
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
        Command::CreateOrder { order_id, terms } => {
            if order.is_some() {
                return Err(OrderError::AlreadyExists);
            }
            if terms.token_in == terms.token_out
                || terms.amount_in == U256::ZERO
                || terms.amount_out == U256::ZERO
            {
                return Err(OrderError::InvalidTerms);
            }

            Ok(Transition {
                events: vec![
                    OrderEvent::OrderCreated { order_id, terms },
                    OrderEvent::OrderValidated { order_id },
                    OrderEvent::SolverReservationRequested { order_id },
                ],
                deliveries: vec![],
            })
        }

        Command::SolverReserved {
            order_id,
            solver_id,
            noise_public_key,
        } => {
            let order = order.ok_or(OrderError::NotFound)?;
            if order.state != OrderState::Reserving {
                return Err(OrderError::InvalidState);
            }
            if noise_public_key.is_empty() || noise_public_key.len() > 128 {
                return Err(OrderError::InvalidPayload);
            }

            Ok(Transition {
                events: vec![
                    OrderEvent::SolverAssigned {
                        order_id,
                        solver_id,
                    },
                    OrderEvent::SolverSessionReady {
                        order_id,
                        solver_id,
                        noise_public_key,
                    },
                ],
                deliveries: vec![],
            })
        }

        Command::RelayEncryptedProof {
            order_id,
            ciphertext,
        } => {
            let order = order.ok_or(OrderError::NotFound)?;
            if order.state != OrderState::AwaitingUserProof {
                return Err(OrderError::InvalidState);
            }
            if ciphertext.is_empty() || ciphertext.len() > 1024 * 1024 {
                return Err(OrderError::InvalidPayload);
            }
            let solver_id = order.solver.ok_or(OrderError::InvalidState)?;

            Ok(Transition {
                events: vec![OrderEvent::ProofRelayed {
                    order_id,
                    solver_id,
                }],
                deliveries: vec![SolverDelivery {
                    solver_id,
                    proof: SolverProofDelivery {
                        order_id,
                        ciphertext,
                    },
                }],
            })
        }

        Command::ExecutionStarted {
            order_id,
            solver_id,
            tx_hash,
        } => {
            let order = order.ok_or(OrderError::NotFound)?;
            if order.state != OrderState::ProofRelayed || order.solver != Some(solver_id) {
                return Err(OrderError::InvalidState);
            }
            if tx_hash == B256::ZERO {
                return Err(OrderError::InvalidPayload);
            }

            Ok(Transition {
                events: vec![OrderEvent::ExecutionStarted { order_id, tx_hash }],
                deliveries: vec![],
            })
        }

        Command::SettlementObserved { order_id, tx_hash } => {
            let order = order.ok_or(OrderError::NotFound)?;
            if order.state != OrderState::Executing || order.tx_hash != Some(tx_hash) {
                return Err(OrderError::InvalidState);
            }

            Ok(Transition {
                events: vec![OrderEvent::OrderFilled { order_id, tx_hash }],
                deliveries: vec![],
            })
        }

        Command::ExpireOrder { order_id } => {
            let order = order.ok_or(OrderError::NotFound)?;
            if matches!(
                order.state,
                OrderState::Filled
                    | OrderState::Expired
                    | OrderState::Cancelled
                    | OrderState::Failed
            ) {
                return Err(OrderError::InvalidState);
            }

            Ok(Transition {
                events: vec![OrderEvent::OrderExpired { order_id }],
                deliveries: vec![],
            })
        }
    }
}

impl Orderbook {
    pub fn process(&mut self, command: Command) -> Result<Vec<OrderEvent>, OrderError> {
        let order_id = command.order_id();
        let transition = handle(self.orders.get(&order_id), command)?;

        for event in &transition.events {
            self.apply_event(event);
        }

        for delivery in transition.deliveries {
            self.solver_proofs
                .entry(delivery.solver_id)
                .or_default()
                .push(delivery.proof);
        }

        Ok(transition.events)
    }

    pub fn orders(&self) -> &HashMap<OrderId, Order> {
        &self.orders
    }

    fn apply_event(&mut self, event: &OrderEvent) {
        if let OrderEvent::OrderCreated { order_id, terms } = event {
            self.orders.insert(*order_id, Order::new(*order_id, *terms));
        }

        if let Some(order) = self.orders.get_mut(&event.order_id()) {
            order.apply(event);
        }
    }
}

enum Request {
    Execute {
        command: Command,
        reply: oneshot::Sender<Result<(), OrderError>>,
    },
    GetOrder {
        order_id: OrderId,
        reply: oneshot::Sender<Option<Order>>,
    },
    ReservingOrders {
        reply: oneshot::Sender<Vec<Order>>,
    },
    ExecutingOrders {
        reply: oneshot::Sender<Vec<Order>>,
    },
    TakeSolverProofs {
        solver_id: SolverId,
        reply: oneshot::Sender<Vec<SolverProofDelivery>>,
    },
}

#[derive(Clone)]
pub struct OrderbookHandle {
    requests: mpsc::Sender<Request>,
    events: broadcast::Sender<OrderEvent>,
}

#[derive(Debug)]
pub enum ServiceError {
    Closed,
    Order(OrderError),
}

impl OrderbookHandle {
    pub async fn execute(&self, command: Command) -> Result<(), ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Execute { command, reply })
            .await
            .map_err(|_| ServiceError::Closed)?;

        result
            .await
            .map_err(|_| ServiceError::Closed)?
            .map_err(ServiceError::Order)
    }

    pub async fn get_order(&self, order_id: OrderId) -> Result<Option<Order>, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::GetOrder { order_id, reply })
            .await
            .map_err(|_| ServiceError::Closed)?;

        result.await.map_err(|_| ServiceError::Closed)
    }

    pub async fn reserving_orders(&self) -> Result<Vec<Order>, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::ReservingOrders { reply })
            .await
            .map_err(|_| ServiceError::Closed)?;

        result.await.map_err(|_| ServiceError::Closed)
    }

    pub async fn executing_orders(&self) -> Result<Vec<Order>, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::ExecutingOrders { reply })
            .await
            .map_err(|_| ServiceError::Closed)?;

        result.await.map_err(|_| ServiceError::Closed)
    }

    pub async fn take_solver_proofs(
        &self,
        solver_id: SolverId,
    ) -> Result<Vec<SolverProofDelivery>, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::TakeSolverProofs { solver_id, reply })
            .await
            .map_err(|_| ServiceError::Closed)?;

        result.await.map_err(|_| ServiceError::Closed)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<OrderEvent> {
        self.events.subscribe()
    }
}

pub fn start_orderbook() -> OrderbookHandle {
    start_orderbook_with_capacity(256)
}

pub fn start_orderbook_with_capacity(capacity: usize) -> OrderbookHandle {
    let (request_tx, mut request_rx) = mpsc::channel(capacity);
    let (event_tx, _) = broadcast::channel(capacity);
    let events = event_tx.clone();

    tokio::spawn(async move {
        let mut orderbook = Orderbook::default();

        while let Some(request) = request_rx.recv().await {
            match request {
                Request::Execute { command, reply } => {
                    let order_id = command.order_id();
                    let command_name = command.name();
                    crate::service_log!(
                        "orderbook",
                        "received command={command_name} order={}",
                        short_id(order_id)
                    );
                    let result = orderbook.process(command).map(|produced| {
                        for event in produced {
                            crate::service_log!(
                                "orderbook",
                                "emitted event={} order={}",
                                event.name(),
                                short_id(event.order_id())
                            );
                            let _ = events.send(event);
                        }
                    });
                    if let Err(error) = &result {
                        crate::service_error!(
                            "orderbook",
                            "rejected command={command_name} order={} error={error:?}",
                            short_id(order_id)
                        );
                    }
                    let _ = reply.send(result);
                }
                Request::GetOrder { order_id, reply } => {
                    let _ = reply.send(orderbook.orders.get(&order_id).cloned());
                }
                Request::ReservingOrders { reply } => {
                    let orders = orderbook
                        .orders
                        .values()
                        .filter(|order| order.state == OrderState::Reserving)
                        .cloned()
                        .collect();
                    let _ = reply.send(orders);
                }
                Request::ExecutingOrders { reply } => {
                    let orders = orderbook
                        .orders
                        .values()
                        .filter(|order| order.state == OrderState::Executing)
                        .cloned()
                        .collect();
                    let _ = reply.send(orders);
                }
                Request::TakeSolverProofs { solver_id, reply } => {
                    let proofs = orderbook
                        .solver_proofs
                        .remove(&solver_id)
                        .unwrap_or_default();
                    let _ = reply.send(proofs);
                }
            }
        }
    });

    OrderbookHandle {
        requests: request_tx,
        events: event_tx,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::TradeTerms;
    use alloy_primitives::{Address, U256};
    use uuid::Uuid;

    fn terms() -> TradeTerms {
        TradeTerms {
            token_in: Address::ZERO,
            token_out: Address::repeat_byte(1),
            amount_in: U256::from(1),
            amount_out: U256::from(2),
        }
    }

    #[test]
    fn drives_an_order_from_created_to_filled() {
        let mut book = Orderbook::default();
        let order_id = Uuid::new_v4();

        book.process(Command::CreateOrder {
            order_id,
            terms: terms(),
        })
        .unwrap();
        assert_eq!(book.orders[&order_id].state, OrderState::Reserving);

        let solver_id = Uuid::new_v4();
        book.process(Command::SolverReserved {
            order_id,
            solver_id,
            noise_public_key: vec![7; 32],
        })
        .unwrap();
        assert_eq!(book.orders[&order_id].state, OrderState::AwaitingUserProof);
        book.process(Command::RelayEncryptedProof {
            order_id,
            ciphertext: vec![1, 2, 3],
        })
        .unwrap();
        let tx_hash = B256::repeat_byte(9);
        book.process(Command::ExecutionStarted {
            order_id,
            solver_id,
            tx_hash,
        })
        .unwrap();
        book.process(Command::SettlementObserved { order_id, tx_hash })
            .unwrap();

        assert_eq!(book.orders[&order_id].state, OrderState::Filled);
        assert_eq!(book.orders[&order_id].version, 8);
    }

    #[test]
    fn rejects_proof_before_solver_assignment() {
        let mut book = Orderbook::default();
        let order_id = Uuid::new_v4();

        book.process(Command::CreateOrder {
            order_id,
            terms: terms(),
        })
        .unwrap();

        let error = book
            .process(Command::RelayEncryptedProof {
                order_id,
                ciphertext: vec![1],
            })
            .unwrap_err();

        assert!(matches!(error, OrderError::InvalidState));
    }
}
