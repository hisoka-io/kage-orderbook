use alloy_primitives::Address;
use kage_types::proof_orders::{AssignmentTicket, ReservationAck};
use tokio::sync::broadcast;

use super::{handle::ServiceError, maintenance::now_ms};
use crate::{
    core::{
        command::Command,
        events::OrderEvent,
        state::{CreateOrderOutcome, OrderError, Orderbook},
    },
    logging::short_id,
    order::OrderId,
    storage::{InsertOutcome, NewProofOrder, OrderRepository, RepositoryError},
};

pub(in crate::core::engine) async fn create_proof_order_idempotently(
    orderbook: &mut Orderbook,
    repository: &OrderRepository,
    events: &broadcast::Sender<OrderEvent>,
    input: NewProofOrder,
) -> Result<CreateOrderOutcome, ServiceError> {
    if let Some(existing) = repository
        .get_order(input.order_id)
        .await
        .map_err(ServiceError::Repository)?
    {
        match repository
            .proof_orders()
            .insert_authoritative(&existing.order, &input)
            .await
            .map_err(ServiceError::Repository)?
        {
            InsertOutcome::Existing => {
                orderbook
                    .orders
                    .entry(existing.order.id)
                    .or_insert_with(|| existing.order.clone());
                return Ok(CreateOrderOutcome {
                    order: existing.order,
                    created: false,
                });
            }
            InsertOutcome::Created => {
                return Err(ServiceError::Repository(RepositoryError::InvalidData {
                    field: "proof_order",
                    value: "created against an existing core order".to_owned(),
                }));
            }
        }
    }

    let command = Command::SubmitProofOrder {
        order_id: input.order_id,
        terms: input.terms,
    };
    let prepared = orderbook
        .prepare(command, input.created_at_ms)
        .map_err(ServiceError::Order)?
        .ok_or(ServiceError::Order(OrderError::AlreadyExists))?;
    match repository
        .proof_orders()
        .insert_authoritative(&prepared.order, &input)
        .await
        .map_err(ServiceError::Repository)?
    {
        InsertOutcome::Created => {
            orderbook.commit(&prepared);
            for event in prepared.transition.events {
                log_order_event(&event);
                let _ = events.send(event);
            }
            Ok(CreateOrderOutcome {
                order: prepared.order,
                created: true,
            })
        }
        InsertOutcome::Existing => {
            let existing = repository
                .get_order(input.order_id)
                .await
                .map_err(ServiceError::Repository)?
                .ok_or(ServiceError::Order(OrderError::NotFound))?;
            orderbook
                .orders
                .entry(existing.order.id)
                .or_insert_with(|| existing.order.clone());
            Ok(CreateOrderOutcome {
                order: existing.order,
                created: false,
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::core::engine) async fn assign_and_disclose_proof_order(
    orderbook: &mut Orderbook,
    repository: &OrderRepository,
    events: &broadcast::Sender<OrderEvent>,
    order_id: OrderId,
    solver_id: Address,
    reservation_ack: &ReservationAck,
    ticket: &AssignmentTicket,
) -> Result<bool, ServiceError> {
    if !orderbook.orders.contains_key(&order_id)
        && let Some(stored) = repository
            .get_order(order_id)
            .await
            .map_err(ServiceError::Repository)?
    {
        orderbook.orders.insert(order_id, stored.order);
    }
    let Some(prepared) = orderbook
        .prepare(
            Command::SolverReserved {
                order_id,
                solver_id,
            },
            now_ms(),
        )
        .map_err(ServiceError::Order)?
    else {
        return Ok(false);
    };
    let expected_version = prepared
        .expected_version
        .ok_or(ServiceError::Order(OrderError::InvalidState))?;
    if ticket.claims.bindings.order_id != order_id || ticket.claims.bindings.solver_id != solver_id
    {
        return Err(ServiceError::Order(OrderError::InvalidPayload));
    }
    let persisted = repository
        .proof_orders()
        .assign_and_disclose(
            &prepared.order,
            expected_version,
            solver_id,
            reservation_ack,
            ticket,
            now_ms(),
        )
        .await
        .map_err(ServiceError::Repository)?;
    if !persisted {
        return Ok(false);
    }
    orderbook.commit(&prepared);
    for event in prepared.transition.events {
        log_order_event(&event);
        let _ = events.send(event);
    }
    Ok(true)
}

pub(in crate::core::engine) async fn execute_command(
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
                let expected_version = prepared
                    .expected_version
                    .ok_or(ServiceError::Order(OrderError::InvalidState))?;
                let persisted = repository
                    .persist_transition(&prepared.order, expected_version, timestamp_ms)
                    .await;

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

pub(in crate::core::engine) fn log_order_event(event: &OrderEvent) {
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
