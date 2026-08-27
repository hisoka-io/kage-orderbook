use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{B256, U256};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::{command::Command, events::OrderEvent};
use crate::{
    logging::short_id,
    order::{Order, OrderCommitment, OrderId, OrderState, TradeTerms},
    storage::{OrderRepository, RepositoryError},
};

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
                    OrderState::Assigned | OrderState::AwaitingUserProof
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

        Command::ExpireOrder { order_id } => {
            let order = order.ok_or(OrderError::NotFound)?;
            if order.state == OrderState::Expired {
                return Err(OrderError::InvalidState);
            }

            Ok(Transition {
                events: vec![OrderEvent::OrderExpired { order_id }],
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
            order_commitment,
            expected_version,
            transition,
        }))
    }

    fn commit(&mut self, prepared: &PreparedTransition) {
        self.orders
            .insert(prepared.order.id, prepared.order.clone());
    }

    pub fn process(&mut self, command: Command) -> Result<Vec<OrderEvent>, OrderError> {
        let Some(prepared) = self.prepare(command, now_ms())? else {
            return Ok(vec![]);
        };
        let events = prepared.transition.events.clone();
        self.commit(&prepared);
        Ok(events)
    }
}

fn is_idempotent(order: Option<&Order>, command: &Command) -> bool {
    let Some(order) = order else {
        return false;
    };

    match command {
        Command::SolverReserved {
            solver_id,
            noise_public_key,
            ..
        } => {
            order.state == OrderState::AwaitingUserProof
                && order.solver == Some(*solver_id)
                && order.solver_noise_public_key.as_deref() == Some(noise_public_key)
        }
        Command::SolverDeclined { .. } => order.state == OrderState::Reserving,
        Command::CreateOrder { .. } | Command::ExpireOrder { .. } => false,
    }
}

struct PreparedTransition {
    order: Order,
    order_commitment: Option<OrderCommitment>,
    expected_version: Option<u64>,
    transition: Transition,
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
    ReservingOrders {
        reply: oneshot::Sender<Vec<Order>>,
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

    pub async fn reserving_orders(&self) -> Result<Vec<Order>, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::ReservingOrders { reply })
            .await
            .map_err(|_| ServiceError::Closed)?;

        result.await.map_err(|_| ServiceError::Closed)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<OrderEvent> {
        self.events.subscribe()
    }
}

pub async fn start_orderbook(database_url: &str) -> Result<OrderbookHandle, RepositoryError> {
    let repository = OrderRepository::connect(database_url).await?;
    start_orderbook_with_repository(repository, 256).await
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
        if restored_count > 0 {
            crate::service_log!("orderbook", "orders restored active={restored_count}");
        } else {
            tracing::debug!(target: "orderbook", "no active orders to restore");
        }
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
                Request::ReservingOrders { reply } => {
                    let orders = orderbook
                        .orders
                        .values()
                        .filter(|order| order.state == OrderState::Reserving)
                        .cloned()
                        .collect();
                    let _ = reply.send(orders);
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
            crate::service_warn!(
                "orderbook",
                "rejected command=CreateOrder order_id={} existing_order_id={} error=ConflictingCommitment",
                short_id(order_id),
                short_id(existing.order.id)
            );
            return Err(ServiceError::Order(OrderError::AlreadyExists));
        }

        tracing::debug!(
            target: "orderbook",
            order_id = %short_id(existing.order.id),
            "idempotent create"
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
    tracing::debug!(
        target: "orderbook",
        command = command_name,
        order_id = %short_id(order_id),
        "command received"
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

        let timestamp_ms = now_ms();

        match orderbook
            .prepare(command, timestamp_ms)
            .map_err(ServiceError::Order)?
        {
            Some(prepared) => {
                let persisted = if let Some(expected_version) = prepared.expected_version {
                    repository
                        .persist_transition(&prepared.order, expected_version, timestamp_ms)
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
                    log_order_event(&event);
                    let _ = events.send(event);
                }
            }
            None => tracing::debug!(
                target: "orderbook",
                command = command_name,
                order_id = %short_id(order_id),
                "idempotent command"
            ),
        }
        Ok(())
    }
    .await;

    if let Err(error) = &result {
        match error {
            ServiceError::Order(_) => crate::service_warn!(
                "orderbook",
                "command rejected command={command_name} order_id={} error={error:?}",
                short_id(order_id)
            ),
            ServiceError::Closed | ServiceError::Repository(_) => crate::service_error!(
                "orderbook",
                "command failed command={command_name} order_id={} error={error:?}",
                short_id(order_id)
            ),
        }
    }
    result
}

fn log_order_event(event: &OrderEvent) {
    match event {
        OrderEvent::SolverAssigned {
            order_id,
            solver_id,
        } => crate::service_log!(
            "orderbook",
            "order assigned order_id={} solver={solver_id}",
            short_id(*order_id)
        ),
        OrderEvent::OrderExpired { order_id } => {
            crate::service_log!(
                "orderbook",
                "order expired order_id={}",
                short_id(*order_id)
            )
        }
        _ => tracing::debug!(
            target: "orderbook",
            event = event.name(),
            order_id = %short_id(event.order_id()),
            "order transition"
        ),
    }
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
    fn drives_an_order_from_created_to_direct_assignment() {
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
        assert_eq!(book.orders[&order_id].state, OrderState::AwaitingUserProof);
        assert_eq!(book.orders[&order_id].version, 5);
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
