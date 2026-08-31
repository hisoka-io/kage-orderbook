use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;

use super::{
    admission::AdmissionGate,
    handle::ServiceError,
    operations::{execute_command, log_order_event},
};
use crate::{
    config::ProofOrderSettings,
    core::{command::Command, events::OrderEvent, state::Orderbook},
    logging::short_id,
    order::{OrderId, ProofOrderState, TradeTerms},
    storage::{AdvanceOutcome, OrderRepository},
};

pub(in crate::core::engine) async fn expire_due_orders(
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

pub(in crate::core::engine) async fn maintain_proof_reservations(
    orderbook: &mut Orderbook,
    repository: &OrderRepository,
    events: &broadcast::Sender<OrderEvent>,
    policy: &ProofOrderSettings,
    admission: Option<&AdmissionGate>,
) {
    let outcomes = if let Some(admission) = admission {
        advance_due_attempts(repository, policy, admission).await
    } else {
        repository
            .proof_orders()
            .expire_due_attempts(
                now_ms(),
                policy.reservation_attempt_timeout_ms,
                policy.minimum_remaining_seconds,
            )
            .await
            .map_err(ServiceError::Repository)
    };
    match outcomes {
        Ok(outcomes) => {
            for (order_id, outcome) in outcomes {
                match outcome {
                    AdvanceOutcome::Advanced(solver_id) => {
                        crate::service_warn!(
                            "orderbook",
                            "reservation attempt timed out order_id={} next_solver={solver_id}",
                            short_id(order_id)
                        );
                        broadcast_reservation_request(orderbook, events, order_id);
                    }
                    AdvanceOutcome::AwaitingCapacity => {}
                    AdvanceOutcome::Exhausted => {
                        close_core_projection(orderbook, order_id);
                        crate::service_warn!(
                            "orderbook",
                            "reservation attempts exhausted after timeout order_id={}",
                            short_id(order_id)
                        );
                    }
                }
            }
        }
        Err(error) => crate::service_error!(
            "orderbook",
            "reservation deadline maintenance failed error={error:?}"
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::core::engine) async fn decline_and_route(
    repository: &OrderRepository,
    policy: &ProofOrderSettings,
    admission: Option<&AdmissionGate>,
    order_id: OrderId,
    solver_id: alloy_primitives::Address,
    encoded_decline: &[u8],
) -> Result<Option<AdvanceOutcome>, ServiceError> {
    let proof_orders = repository.proof_orders();
    let Some(admission) = admission else {
        return proof_orders
            .decline_and_advance(
                order_id,
                solver_id,
                Some(encoded_decline),
                now_ms(),
                policy.reservation_attempt_timeout_ms,
                policy.minimum_remaining_seconds,
            )
            .await
            .map_err(ServiceError::Repository);
    };
    let (terms, fee_bps) = proof_orders
        .routing_terms(order_id)
        .await
        .map_err(ServiceError::Repository)?
        .ok_or(ServiceError::Order(
            crate::core::state::OrderError::NotFound,
        ))?;
    let candidates = proof_orders
        .untried_reservation_candidates(order_id)
        .await
        .map_err(ServiceError::Repository)?;
    let permit = match admission
        .select_fallback(&proof_orders, &terms, fee_bps, &candidates)
        .await
    {
        Ok(permit) => Some(permit),
        Err(error) => {
            crate::service_error!(
                "orderbook",
                "fallback selection deferred order_id={} error={error:?}",
                short_id(order_id)
            );
            None
        }
    };
    let next_solver = permit.as_ref().and_then(|permit| permit.next_solver);
    let timestamp_ms = now_ms();
    proof_orders
        .decline_and_advance_to(
            order_id,
            solver_id,
            Some(encoded_decline),
            timestamp_ms,
            policy.reservation_attempt_timeout_ms,
            policy.minimum_remaining_seconds,
            next_solver,
        )
        .await
        .map_err(ServiceError::Repository)
}

async fn advance_due_attempts(
    repository: &OrderRepository,
    policy: &ProofOrderSettings,
    admission: &AdmissionGate,
) -> Result<Vec<(OrderId, AdvanceOutcome)>, ServiceError> {
    let proof_orders = repository.proof_orders();
    let scan_time_ms = now_ms();
    let waiting = proof_orders
        .awaiting_capacity_order_ids()
        .await
        .map_err(ServiceError::Repository)?;
    let due = proof_orders
        .due_reservation_attempts(scan_time_ms)
        .await
        .map_err(ServiceError::Repository)?;
    let mut outcomes = Vec::with_capacity(due.len().saturating_add(waiting.len()));
    for (order_id, solver_id) in due {
        match advance_due_attempt(&proof_orders, policy, admission, order_id, solver_id).await {
            Ok(Some(outcome)) => outcomes.push((order_id, outcome)),
            Ok(None) => {}
            Err(error) => {
                rotate_failed_retry(&proof_orders, order_id).await;
                crate::service_error!(
                    "orderbook",
                    "reservation deadline advance failed order_id={} error={error:?}",
                    short_id(order_id)
                );
            }
        }
    }
    for order_id in waiting {
        match advance_awaiting_attempt(&proof_orders, policy, admission, order_id).await {
            Ok(Some(outcome)) => outcomes.push((order_id, outcome)),
            Ok(None) => {}
            Err(error) => {
                rotate_failed_retry(&proof_orders, order_id).await;
                crate::service_error!(
                    "orderbook",
                    "awaiting fallback advance failed order_id={} error={error:?}",
                    short_id(order_id)
                );
            }
        }
    }
    Ok(outcomes)
}

async fn rotate_failed_retry(
    proof_orders: &crate::storage::ProofOrderRepository,
    order_id: OrderId,
) {
    if let Err(error) = proof_orders
        .rotate_maintenance_retry(order_id, now_ms())
        .await
    {
        crate::service_error!(
            "orderbook",
            "maintenance retry rotation failed order_id={} error={error}",
            short_id(order_id)
        );
    }
}

async fn advance_due_attempt(
    proof_orders: &crate::storage::ProofOrderRepository,
    policy: &ProofOrderSettings,
    admission: &AdmissionGate,
    order_id: OrderId,
    solver_id: alloy_primitives::Address,
) -> Result<Option<AdvanceOutcome>, ServiceError> {
    let (terms, fee_bps) = proof_orders
        .routing_terms(order_id)
        .await
        .map_err(ServiceError::Repository)?
        .ok_or(ServiceError::Order(
            crate::core::state::OrderError::NotFound,
        ))?;
    let candidates = proof_orders
        .untried_reservation_candidates(order_id)
        .await
        .map_err(ServiceError::Repository)?;
    let permit = match admission
        .select_fallback(proof_orders, &terms, fee_bps, &candidates)
        .await
    {
        Ok(permit) => Some(permit),
        Err(error) => {
            crate::service_error!(
                "orderbook",
                "timed-out fallback selection deferred order_id={} error={error:?}",
                short_id(order_id)
            );
            None
        }
    };
    let next_solver = permit.as_ref().and_then(|permit| permit.next_solver);
    let timestamp_ms = now_ms();
    proof_orders
        .timeout_and_advance_to(
            order_id,
            solver_id,
            timestamp_ms,
            policy.reservation_attempt_timeout_ms,
            policy.minimum_remaining_seconds,
            next_solver,
        )
        .await
        .map_err(ServiceError::Repository)
}

async fn advance_awaiting_attempt(
    proof_orders: &crate::storage::ProofOrderRepository,
    policy: &ProofOrderSettings,
    admission: &AdmissionGate,
    order_id: OrderId,
) -> Result<Option<AdvanceOutcome>, ServiceError> {
    let (terms, fee_bps) = proof_orders
        .routing_terms(order_id)
        .await
        .map_err(ServiceError::Repository)?
        .ok_or(ServiceError::Order(
            crate::core::state::OrderError::NotFound,
        ))?;
    let candidates = proof_orders
        .untried_reservation_candidates(order_id)
        .await
        .map_err(ServiceError::Repository)?;
    let permit = match candidates.is_empty() {
        true => None,
        false => match admission
            .select_fallback(proof_orders, &terms, fee_bps, &candidates)
            .await
        {
            Ok(permit) => Some(permit),
            Err(error) => {
                crate::service_error!(
                    "orderbook",
                    "awaiting fallback selection deferred order_id={} error={error:?}",
                    short_id(order_id)
                );
                None
            }
        },
    };
    let next_solver = permit.as_ref().and_then(|permit| permit.next_solver);
    let timestamp_ms = now_ms();
    proof_orders
        .advance_awaiting_to(
            order_id,
            timestamp_ms,
            policy.reservation_attempt_timeout_ms,
            policy.minimum_remaining_seconds,
            next_solver,
        )
        .await
        .map_err(ServiceError::Repository)
}

pub(in crate::core::engine) fn close_core_projection(orderbook: &mut Orderbook, order_id: OrderId) {
    if let Some(order) = orderbook.orders.get_mut(&order_id) {
        order.state = ProofOrderState::Expired;
        order.version = order.version.saturating_add(1);
    }
}

pub(in crate::core::engine) fn broadcast_reservation_request(
    orderbook: &Orderbook,
    events: &broadcast::Sender<OrderEvent>,
    order_id: OrderId,
) {
    let Some(order) = orderbook.orders.get(&order_id) else {
        return;
    };
    let Some(expires_at_ms) = order.expires_at_ms else {
        return;
    };
    let terms = TradeTerms {
        chain_id: order.chain_id,
        token_in: order.token_in,
        token_out: order.token_out,
        amount_in: order.amount_in,
        amount_out: order.amount_out,
        expires_at_ms,
    };
    let event = OrderEvent::SolverReservationRequested { order_id, terms };
    log_order_event(&event);
    let _ = events.send(event);
}

pub(in crate::core::engine) async fn cleanup_proof_orders(
    repository: &OrderRepository,
    policy: &ProofOrderSettings,
) {
    match repository
        .proof_orders()
        .cleanup(now_ms(), policy.evidence_retention_seconds)
        .await
    {
        Ok(outcome)
            if outcome.payloads_erased > 0
                || outcome.complaints_erased > 0
                || outcome.orders_erased > 0 =>
        {
            crate::service_log!(
                "orderbook",
                "proof retention cleanup payloads={} complaints={} orders={}",
                outcome.payloads_erased,
                outcome.complaints_erased,
                outcome.orders_erased
            );
        }
        Ok(_) => {}
        Err(error) => {
            crate::service_error!("orderbook", "proof retention cleanup failed error={error}")
        }
    }
}

pub(in crate::core::engine) fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}
