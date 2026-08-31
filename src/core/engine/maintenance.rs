use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;

use super::operations::{execute_command, log_order_event};
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
) {
    match repository
        .proof_orders()
        .expire_due_attempts(
            now_ms(),
            policy.reservation_attempt_timeout_ms,
            policy.minimum_remaining_seconds,
        )
        .await
    {
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
            "reservation deadline maintenance failed error={error}"
        ),
    }
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
