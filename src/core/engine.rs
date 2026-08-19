use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{B256, U256};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::{command::Command, events::OrderEvent};
use crate::{
    logging::short_id,
    order::{Order, OrderCommitment, OrderId, OrderState, SolverId, TradeTerms},
    storage::{OrderRepository, RepositoryError},
};

pub use kage_types::api_types::SolverProofDeliveryV1 as SolverProofDelivery;

#[derive(Debug, Clone)]
pub struct CreateOrderOutcome {
    pub order: Order,
    pub created: bool,
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
        Command::CreateOrder {
            order_id,
            order_commitment,
            terms,
        } => {
            if order.is_some() {
                return Err(OrderError::AlreadyExists);
            }
            if order_commitment == B256::ZERO {
                return Err(OrderError::InvalidPayload);
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
            if noise_public_key.len() != 32 {
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
    fn from_orders(orders: impl IntoIterator<Item = Order>) -> Self {
        Self {
            orders: orders.into_iter().map(|order| (order.id, order)).collect(),
        }
    }

    fn prepare(
        &self,
        command: Command,
        stored_proof: Option<&[u8]>,
        timestamp_ms: i64,
    ) -> Result<Option<PreparedTransition>, OrderError> {
        let order_commitment = match &command {
            Command::CreateOrder {
                order_commitment, ..
            } => Some(*order_commitment),
            _ => None,
        };
        if let Command::CreateOrder { terms, .. } = &command
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
        if is_idempotent(current, &command, stored_proof) {
            return Ok(None);
        }
        let expected_version = current.map(|order| order.version);
        let delete_proof = matches!(
            &command,
            Command::ExecutionStarted { .. }
                | Command::SettlementObserved { .. }
                | Command::ExpireOrder { .. }
        );
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
            order_commitment,
            expected_version,
            transition,
            delete_proof,
        }))
    }

    fn commit(&mut self, prepared: &PreparedTransition) {
        self.orders
            .insert(prepared.order.id, prepared.order.clone());
    }

    pub fn process(&mut self, command: Command) -> Result<Vec<OrderEvent>, OrderError> {
        let Some(prepared) = self.prepare(command, None, now_ms())? else {
            return Ok(vec![]);
        };
        let events = prepared.transition.events.clone();
        self.commit(&prepared);
        Ok(events)
    }

    pub fn orders(&self) -> &HashMap<OrderId, Order> {
        &self.orders
    }
}

fn is_idempotent(order: Option<&Order>, command: &Command, stored_proof: Option<&[u8]>) -> bool {
    let Some(order) = order else {
        return false;
    };

    match command {
        Command::SolverReserved {
            solver_id,
            noise_public_key,
            ..
        } => {
            matches!(
                order.state,
                OrderState::AwaitingUserProof
                    | OrderState::ProofRelayed
                    | OrderState::Executing
                    | OrderState::Filled
            ) && order.solver == Some(*solver_id)
                && order.solver_noise_public_key.as_deref() == Some(noise_public_key)
        }
        Command::RelayEncryptedProof { ciphertext, .. } => {
            order.state == OrderState::Executing
                || (order.state == OrderState::ProofRelayed && stored_proof == Some(ciphertext))
        }
        Command::ExecutionStarted {
            solver_id, tx_hash, ..
        } => {
            matches!(order.state, OrderState::Executing | OrderState::Filled)
                && order.solver == Some(*solver_id)
                && order.tx_hash == Some(*tx_hash)
        }
        Command::SettlementObserved { tx_hash, .. } => {
            order.state == OrderState::Filled && order.tx_hash == Some(*tx_hash)
        }
        Command::CreateOrder { .. } | Command::ExpireOrder { .. } => false,
    }
}

struct PreparedTransition {
    order: Order,
    order_commitment: Option<OrderCommitment>,
    expected_version: Option<u64>,
    transition: Transition,
    delete_proof: bool,
}

enum Request {
    CreateOrder {
        order_id: OrderId,
        order_commitment: OrderCommitment,
        terms: TradeTerms,
        reply: oneshot::Sender<Result<CreateOrderOutcome, ServiceError>>,
    },
    Execute {
        command: Command,
        reply: oneshot::Sender<Result<(), ServiceError>>,
    },
    GetOrder {
        order_id: OrderId,
        reply: oneshot::Sender<Result<Option<Order>, RepositoryError>>,
    },
    GetOrderByCommitment {
        order_id: OrderId,
        order_commitment: OrderCommitment,
        reply: oneshot::Sender<Result<Option<Order>, RepositoryError>>,
    },
    FindOrderByCommitment {
        order_commitment: OrderCommitment,
        reply: oneshot::Sender<Result<Option<Order>, RepositoryError>>,
    },
    ExecuteAuthorized {
        command: Command,
        order_commitment: OrderCommitment,
        reply: oneshot::Sender<Result<(), ServiceError>>,
    },
    ReservingOrders {
        reply: oneshot::Sender<Vec<Order>>,
    },
    ExecutingOrders {
        reply: oneshot::Sender<Vec<Order>>,
    },
    TakeSolverProofs {
        solver_id: SolverId,
        reply: oneshot::Sender<Result<Vec<SolverProofDelivery>, RepositoryError>>,
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
    Repository(RepositoryError),
}

impl OrderbookHandle {
    pub fn is_available(&self) -> bool {
        !self.requests.is_closed()
    }

    pub async fn create_order(
        &self,
        order_id: OrderId,
        order_commitment: OrderCommitment,
        terms: TradeTerms,
    ) -> Result<CreateOrderOutcome, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::CreateOrder {
                order_id,
                order_commitment,
                terms,
                reply,
            })
            .await
            .map_err(|_| ServiceError::Closed)?;

        result.await.map_err(|_| ServiceError::Closed)?
    }

    pub async fn execute(&self, command: Command) -> Result<(), ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Execute { command, reply })
            .await
            .map_err(|_| ServiceError::Closed)?;

        result.await.map_err(|_| ServiceError::Closed)?
    }

    pub async fn get_order(&self, order_id: OrderId) -> Result<Option<Order>, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::GetOrder { order_id, reply })
            .await
            .map_err(|_| ServiceError::Closed)?;

        result
            .await
            .map_err(|_| ServiceError::Closed)?
            .map_err(ServiceError::Repository)
    }

    pub async fn get_order_by_commitment(
        &self,
        order_id: OrderId,
        order_commitment: OrderCommitment,
    ) -> Result<Option<Order>, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::GetOrderByCommitment {
                order_id,
                order_commitment,
                reply,
            })
            .await
            .map_err(|_| ServiceError::Closed)?;

        result
            .await
            .map_err(|_| ServiceError::Closed)?
            .map_err(ServiceError::Repository)
    }

    pub async fn find_order_by_commitment(
        &self,
        order_commitment: OrderCommitment,
    ) -> Result<Option<Order>, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::FindOrderByCommitment {
                order_commitment,
                reply,
            })
            .await
            .map_err(|_| ServiceError::Closed)?;

        result
            .await
            .map_err(|_| ServiceError::Closed)?
            .map_err(ServiceError::Repository)
    }

    pub async fn relay_encrypted_proof(
        &self,
        order_id: OrderId,
        order_commitment: OrderCommitment,
        ciphertext: Vec<u8>,
    ) -> Result<(), ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::ExecuteAuthorized {
                command: Command::RelayEncryptedProof {
                    order_id,
                    ciphertext,
                },
                order_commitment,
                reply,
            })
            .await
            .map_err(|_| ServiceError::Closed)?;

        result.await.map_err(|_| ServiceError::Closed)?
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

        result
            .await
            .map_err(|_| ServiceError::Closed)?
            .map_err(ServiceError::Repository)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<OrderEvent> {
        self.events.subscribe()
    }
}

pub async fn start_orderbook(database_url: &str) -> Result<OrderbookHandle, RepositoryError> {
    start_orderbook_with_capacity(database_url, 256).await
}

pub async fn start_orderbook_with_capacity(
    database_url: &str,
    capacity: usize,
) -> Result<OrderbookHandle, RepositoryError> {
    let repository = OrderRepository::connect(database_url).await?;
    start_orderbook_with_repository(repository, capacity).await
}

pub async fn start_orderbook_with_repository(
    repository: OrderRepository,
    capacity: usize,
) -> Result<OrderbookHandle, RepositoryError> {
    let restored = repository.load_non_terminal_orders().await?;
    let mut orderbook =
        Orderbook::from_orders(restored.into_iter().map(|persisted| persisted.order));
    let restored_count = orderbook.orders.len();
    let (request_tx, mut request_rx) = mpsc::channel(capacity);
    let (event_tx, _) = broadcast::channel(capacity);
    let events = event_tx.clone();

    tokio::spawn(async move {
        crate::service_log!("orderbook", "restored active_orders={restored_count}");
        let mut expiry_interval = tokio::time::interval(Duration::from_millis(100));
        expiry_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let request = tokio::select! {
                _ = expiry_interval.tick() => {
                    expire_due_orders(&mut orderbook, &repository, &events).await;
                    continue;
                }
                request = request_rx.recv() => {
                    let Some(request) = request else {
                        break;
                    };
                    request
                }
            };

            expire_due_orders(&mut orderbook, &repository, &events).await;
            match request {
                Request::CreateOrder {
                    order_id,
                    order_commitment,
                    terms,
                    reply,
                } => {
                    let result = create_order_idempotently(
                        &mut orderbook,
                        &repository,
                        &events,
                        order_id,
                        order_commitment,
                        terms,
                    )
                    .await;
                    let _ = reply.send(result);
                }
                Request::Execute { command, reply } => {
                    let result =
                        execute_command(&mut orderbook, &repository, &events, command).await;
                    let _ = reply.send(result);
                }
                Request::GetOrder { order_id, reply } => {
                    let result = match orderbook.orders.get(&order_id).cloned() {
                        Some(order) => Ok(Some(order)),
                        None => repository
                            .get_order(order_id)
                            .await
                            .map(|stored| stored.map(|stored| stored.order)),
                    };
                    let _ = reply.send(result);
                }
                Request::GetOrderByCommitment {
                    order_id,
                    order_commitment,
                    reply,
                } => {
                    let result = repository
                        .get_order_by_commitment(order_id, order_commitment)
                        .await
                        .map(|stored| stored.map(|stored| stored.order));
                    let _ = reply.send(result);
                }
                Request::FindOrderByCommitment {
                    order_commitment,
                    reply,
                } => {
                    let result = repository
                        .find_order_by_commitment(order_commitment)
                        .await
                        .map(|stored| stored.map(|stored| stored.order));
                    let _ = reply.send(result);
                }
                Request::ExecuteAuthorized {
                    command,
                    order_commitment,
                    reply,
                } => {
                    let order_id = command.order_id();
                    let result = match repository
                        .get_order_by_commitment(order_id, order_commitment)
                        .await
                    {
                        Ok(Some(_)) => {
                            execute_command(&mut orderbook, &repository, &events, command).await
                        }
                        Ok(None) => Err(ServiceError::Order(OrderError::NotFound)),
                        Err(error) => Err(ServiceError::Repository(error)),
                    };
                    let _ = reply.send(result);
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
                    let proofs = repository
                        .load_solver_proofs(solver_id)
                        .await
                        .map(|proofs| {
                            proofs
                                .into_iter()
                                .map(|proof| SolverProofDelivery {
                                    order_id: proof.order_id,
                                    ciphertext: proof.ciphertext,
                                })
                                .collect()
                        });
                    let _ = reply.send(proofs);
                }
            }
        }
    });

    Ok(OrderbookHandle {
        requests: request_tx,
        events: event_tx,
    })
}

async fn create_order_idempotently(
    orderbook: &mut Orderbook,
    repository: &OrderRepository,
    events: &broadcast::Sender<OrderEvent>,
    order_id: OrderId,
    order_commitment: OrderCommitment,
    terms: TradeTerms,
) -> Result<CreateOrderOutcome, ServiceError> {
    if let Some(existing) = repository
        .find_order_by_commitment(order_commitment)
        .await
        .map_err(ServiceError::Repository)?
    {
        if !same_create_terms(&existing.order, &terms) {
            crate::service_error!(
                "orderbook",
                "rejected command=CreateOrder order={} existing_order={} error=ConflictingCommitment",
                short_id(order_id),
                short_id(existing.order.id)
            );
            return Err(ServiceError::Order(OrderError::AlreadyExists));
        }

        crate::service_log!(
            "orderbook",
            "idempotent command=CreateOrder order={}",
            short_id(existing.order.id)
        );
        return Ok(CreateOrderOutcome {
            order: existing.order,
            created: false,
        });
    }

    execute_command(
        orderbook,
        repository,
        events,
        Command::CreateOrder {
            order_id,
            order_commitment,
            terms,
        },
    )
    .await?;

    let order = orderbook
        .orders
        .get(&order_id)
        .cloned()
        .ok_or(ServiceError::Order(OrderError::NotFound))?;
    Ok(CreateOrderOutcome {
        order,
        created: true,
    })
}

fn same_create_terms(order: &Order, terms: &TradeTerms) -> bool {
    order.chain_id == terms.chain_id
        && order.token_in == terms.token_in
        && order.token_out == terms.token_out
        && order.amount_in == terms.amount_in
        && order.amount_out == terms.amount_out
}

async fn execute_command(
    orderbook: &mut Orderbook,
    repository: &OrderRepository,
    events: &broadcast::Sender<OrderEvent>,
    command: Command,
) -> Result<(), ServiceError> {
    let order_id = command.order_id();
    let command_name = command.name();
    crate::service_log!(
        "orderbook",
        "received command={command_name} order={}",
        short_id(order_id)
    );

    let result: Result<(), ServiceError> = async {
        let stored_order = if orderbook.orders.contains_key(&order_id) {
            None
        } else {
            repository
                .get_order(order_id)
                .await
                .map_err(ServiceError::Repository)?
                .map(|stored| stored.order)
        };
        if let Some(stored_order) = stored_order {
            orderbook.orders.entry(order_id).or_insert(stored_order);
        }

        let stored_proof = if matches!(&command, Command::RelayEncryptedProof { .. }) {
            repository
                .get_proof(order_id)
                .await
                .map_err(ServiceError::Repository)?
                .map(|proof| proof.ciphertext)
        } else {
            None
        };
        let timestamp_ms = now_ms();

        match orderbook
            .prepare(command, stored_proof.as_deref(), timestamp_ms)
            .map_err(ServiceError::Order)?
        {
            Some(prepared) => {
                let persisted = if let Some(expected_version) = prepared.expected_version {
                    let proof =
                        prepared.transition.deliveries.first().map(|delivery| {
                            (delivery.solver_id, delivery.proof.ciphertext.as_slice())
                        });
                    repository
                        .persist_transition(
                            &prepared.order,
                            expected_version,
                            timestamp_ms,
                            proof,
                            prepared.delete_proof,
                        )
                        .await
                } else {
                    repository
                        .insert_order(
                            &prepared.order,
                            prepared
                                .order_commitment
                                .ok_or_else(|| RepositoryError::InvalidData {
                                    field: "order_commitment",
                                    value: "missing from create transition".to_owned(),
                                })
                                .map_err(ServiceError::Repository)?,
                            timestamp_ms,
                        )
                        .await
                };

                persisted.map_err(ServiceError::Repository)?;
                orderbook.commit(&prepared);
                for event in prepared.transition.events {
                    crate::service_log!(
                        "orderbook",
                        "emitted event={} order={}",
                        event.name(),
                        short_id(event.order_id())
                    );
                    let _ = events.send(event);
                }
            }
            None => crate::service_log!(
                "orderbook",
                "idempotent command={command_name} order={}",
                short_id(order_id)
            ),
        }
        Ok(())
    }
    .await;

    if let Err(error) = &result {
        crate::service_error!(
            "orderbook",
            "rejected command={command_name} order={} error={error:?}",
            short_id(order_id)
        );
    }
    result
}

async fn expire_due_orders(
    orderbook: &mut Orderbook,
    repository: &OrderRepository,
    events: &broadcast::Sender<OrderEvent>,
) {
    let timestamp_ms = now_ms();
    let due = orderbook
        .orders
        .values()
        .filter(|order| order.is_expired_at(timestamp_ms))
        .map(|order| order.id)
        .collect::<Vec<_>>();

    for order_id in due {
        let _ = execute_command(
            orderbook,
            repository,
            events,
            Command::ExpireOrder { order_id },
        )
        .await;
    }
}

fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::TradeTerms;
    use alloy_primitives::{Address, U256};
    use uuid::Uuid;

    fn terms() -> TradeTerms {
        TradeTerms {
            chain_id: 31_337,
            token_in: Address::ZERO,
            token_out: Address::repeat_byte(1),
            amount_in: U256::from(1),
            amount_out: U256::from(2),
            expires_at_ms: i64::MAX,
        }
    }

    #[test]
    fn drives_an_order_from_created_to_filled() {
        let mut book = Orderbook::default();
        let order_id = Uuid::new_v4();
        let trade_terms = terms();

        let events = book
            .process(Command::CreateOrder {
                order_id,
                order_commitment: B256::repeat_byte(1),
                terms: trade_terms,
            })
            .unwrap();
        assert_eq!(
            events.last(),
            Some(&OrderEvent::SolverReservationRequested {
                order_id,
                terms: trade_terms,
            })
        );
        assert_eq!(book.orders[&order_id].state, OrderState::Reserving);

        let solver_id = Address::repeat_byte(3);
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
            order_commitment: B256::repeat_byte(1),
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

    #[tokio::test]
    async fn persists_and_restores_an_active_order() {
        let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
        let orderbook = start_orderbook_with_repository(repository.clone(), 16)
            .await
            .unwrap();
        let order_id = Uuid::new_v4();

        orderbook
            .execute(Command::CreateOrder {
                order_id,
                order_commitment: B256::repeat_byte(1),
                terms: terms(),
            })
            .await
            .unwrap();

        let stored = repository.get_order(order_id).await.unwrap().unwrap();
        assert_eq!(stored.order.state, OrderState::Reserving);
        drop(orderbook);

        let restored = start_orderbook_with_repository(repository, 16)
            .await
            .unwrap();
        let order = restored.get_order(order_id).await.unwrap().unwrap();
        assert_eq!(order.state, OrderState::Reserving);
        assert_eq!(order.version, 3);
    }
}
