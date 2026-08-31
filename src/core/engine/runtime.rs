use std::time::Duration;

use kage_types::api_types::ComplaintStatus;
use tokio::sync::{broadcast, mpsc};

use super::{
    admission::AdmissionGate,
    handle::{OrderbookHandle, Request, ServiceError},
    maintenance::{
        broadcast_reservation_request, cleanup_proof_orders, close_core_projection,
        decline_and_route, expire_due_orders, maintain_proof_reservations, now_ms,
    },
    operations::{assign_and_disclose_proof_order, create_proof_order_idempotently},
};
use crate::{
    config::ProofOrderSettings,
    core::state::Orderbook,
    storage::{AdvanceOutcome, OrderRepository, RepositoryError},
};

#[cfg(test)]
pub(crate) async fn start_orderbook_with_repository(
    repository: OrderRepository,
    capacity: usize,
) -> Result<OrderbookHandle, RepositoryError> {
    start_orderbook_with_repository_and_policy(repository, capacity, ProofOrderSettings::default())
        .await
}

#[cfg(test)]
pub(crate) async fn start_orderbook_with_repository_and_policy(
    repository: OrderRepository,
    capacity: usize,
    proof_policy: ProofOrderSettings,
) -> Result<OrderbookHandle, RepositoryError> {
    start_orderbook_runtime(repository, capacity, proof_policy, None).await
}

pub async fn start_orderbook_with_admission(
    repository: OrderRepository,
    capacity: usize,
    proof_policy: ProofOrderSettings,
    admission: AdmissionGate,
) -> Result<OrderbookHandle, RepositoryError> {
    start_orderbook_runtime(repository, capacity, proof_policy, Some(admission)).await
}

async fn start_orderbook_runtime(
    repository: OrderRepository,
    capacity: usize,
    proof_policy: ProofOrderSettings,
    admission: Option<AdmissionGate>,
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
        let mut reservation_interval = tokio::time::interval(Duration::from_millis(250));
        reservation_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut cleanup_interval = tokio::time::interval(Duration::from_secs(60));
        cleanup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let request = tokio::select! {
                _ = expiry_interval.tick() => {
                    expire_due_orders(&mut orderbook, &repository, &events).await;
                    continue;
                }
                _ = reservation_interval.tick() => {
                    maintain_proof_reservations(
                        &mut orderbook,
                        &repository,
                        &events,
                        &proof_policy,
                        admission.as_ref(),
                    ).await;
                    continue;
                }
                _ = cleanup_interval.tick() => {
                    cleanup_proof_orders(&repository, &proof_policy).await;
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
                Request::CreateProofOrder { input, reply } => {
                    let result = create_proof_order_idempotently(
                        &mut orderbook,
                        &repository,
                        &events,
                        *input,
                        admission.as_ref(),
                        &proof_policy,
                    )
                    .await;
                    let _ = reply.send(result);
                }
                Request::AssignAndDiscloseProofOrder {
                    order_id,
                    solver_id,
                    session_token,
                    reservation_ack,
                    ticket,
                    reply,
                } => {
                    let result = assign_and_disclose_proof_order(
                        &mut orderbook,
                        &repository,
                        &events,
                        order_id,
                        solver_id,
                        session_token.as_deref(),
                        &reservation_ack,
                        &ticket,
                        admission.as_ref(),
                    )
                    .await;
                    let _ = reply.send(result);
                }
                Request::DeclineProofOrder {
                    order_id,
                    solver_id,
                    decline,
                    reply,
                } => {
                    let encoded = rmp_serde::to_vec_named(&decline).map_err(|error| {
                        ServiceError::Repository(RepositoryError::InvalidData {
                            field: "solver_decline",
                            value: error.to_string(),
                        })
                    });
                    let result = match encoded {
                        Ok(encoded) => {
                            decline_and_route(
                                &repository,
                                &proof_policy,
                                admission.as_ref(),
                                order_id,
                                solver_id,
                                &encoded,
                            )
                            .await
                        }
                        Err(error) => Err(error),
                    };
                    if matches!(result, Ok(Some(AdvanceOutcome::Advanced(_)))) {
                        broadcast_reservation_request(&orderbook, &events, order_id);
                    } else if matches!(result, Ok(Some(AdvanceOutcome::Exhausted))) {
                        close_core_projection(&mut orderbook, order_id);
                    }
                    let _ = reply.send(result);
                }
                Request::UpdateProofResult {
                    order_id,
                    solver_id,
                    decision,
                    reply,
                } => {
                    let result = repository
                        .proof_orders()
                        .update_result(order_id, solver_id, &decision, now_ms())
                        .await
                        .map_err(ServiceError::Repository);
                    let _ = reply.send(result);
                }
                Request::InsertProofComplaint {
                    order_id,
                    evidence_kind,
                    opening,
                    status,
                    reason,
                    admitted_at_ms,
                    reply,
                } => {
                    let retention_seconds = match status {
                        ComplaintStatus::Verified => proof_policy.evidence_retention_seconds,
                        ComplaintStatus::Rejected | ComplaintStatus::Resolved => {
                            proof_policy.resolved_complaint_retention_seconds
                        }
                    };
                    let result = repository
                        .proof_orders()
                        .insert_complaint(
                            order_id,
                            evidence_kind,
                            &opening,
                            status,
                            &reason,
                            admitted_at_ms,
                            retention_seconds,
                        )
                        .await
                        .map_err(ServiceError::Repository);
                    let _ = reply.send(result);
                }
                Request::ResolveProofComplaint { order_id, reply } => {
                    let result = repository
                        .proof_orders()
                        .resolve_complaint(
                            order_id,
                            now_ms(),
                            proof_policy.resolved_complaint_retention_seconds,
                        )
                        .await
                        .map_err(ServiceError::Repository);
                    let _ = reply.send(result);
                }
                Request::SetProofComplaintLegalHold {
                    order_id,
                    held,
                    reply,
                } => {
                    let result = repository
                        .proof_orders()
                        .set_complaint_legal_hold(order_id, held, now_ms())
                        .await
                        .map_err(ServiceError::Repository);
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
            }
        }
    });

    Ok(OrderbookHandle {
        requests: request_tx,
        events: event_tx,
    })
}
