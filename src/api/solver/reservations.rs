use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use kage_types::proof_orders::{ReservationAck, ReservationDecline};

use super::{
    super::{ApiState, auth, error::status_for_error, now_ms},
    body::decode_solver_body,
    validation::{reservation_ack_is_valid, reservation_decline_is_valid, signature_matches},
};
use crate::{logging::short_id, order::OrderId, storage::AdvanceOutcome};

pub(in crate::api) async fn reserve_order(
    State(state): State<ApiState>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<axum::response::Response, StatusCode> {
    let session_token = auth::bearer_token(&headers)?;
    let solver_id = state
        .sessions
        .resolve(&session_token, now_ms())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    auth::active_solver(&state, solver_id)?;
    let reservation_ack: ReservationAck = decode_solver_body(&headers, &body)?;
    let proof_orders = &state.proof_orders;
    let delivery = proof_orders
        .assigned_delivery(order_id, solver_id)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    if let Some(delivery) = delivery {
        let persisted = proof_orders
            .assigned_reservation_ack(order_id, solver_id)
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
            .ok_or(StatusCode::CONFLICT)?;
        if persisted != reservation_ack
            || !signature_matches(
                &reservation_ack.signature,
                reservation_ack.claims.signing_bytes(),
                solver_id,
            )
        {
            return Err(StatusCode::CONFLICT);
        }
        let _session_guard = state.sessions.capacity_guard().await;
        if state.sessions.resolve(&session_token, now_ms()) != Some(solver_id) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        auth::active_solver(&state, solver_id)?;
        return Ok(Json(delivery).into_response());
    }

    let pending = proof_orders
        .pending_reservation(order_id, solver_id)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .ok_or(StatusCode::FORBIDDEN)?;
    let now = now_ms();
    if !reservation_ack_is_valid(&reservation_ack, &pending, solver_id, now) {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }
    let ticket = state
        .assignment_issuer
        .issue_proof_assignment(
            pending.claims.bindings.clone(),
            pending.settlement_commitment,
            pending.key_id,
            now,
        )
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    state
        .orderbook
        .assign_and_disclose_proof_order(
            order_id,
            solver_id,
            Some(session_token.clone()),
            reservation_ack.clone(),
            ticket,
        )
        .await
        .map_err(status_for_error)?;
    let delivery = proof_orders
        .assigned_delivery(order_id, solver_id)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .ok_or(StatusCode::CONFLICT)?;
    if proof_orders
        .assigned_reservation_ack(order_id, solver_id)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        != Some(reservation_ack)
    {
        return Err(StatusCode::CONFLICT);
    }
    let _session_guard = state.sessions.capacity_guard().await;
    if state.sessions.resolve(&session_token, now_ms()) != Some(solver_id) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    auth::active_solver(&state, solver_id)?;
    Ok(Json(delivery).into_response())
}

pub(in crate::api) async fn decline_order(
    State(state): State<ApiState>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let solver_id = auth::authenticated_solver(&state, &headers)?;
    auth::active_solver(&state, solver_id)?;
    let decline: ReservationDecline = decode_solver_body(&headers, &body)?;
    let pending = state
        .proof_orders
        .pending_reservation(order_id, solver_id)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .ok_or(StatusCode::FORBIDDEN)?;
    if !reservation_decline_is_valid(&decline, &pending, solver_id, now_ms()) {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }
    crate::service_warn!(
        "orderbook",
        "solver declined order_id={} solver={solver_id} reason={:?}",
        short_id(order_id),
        decline.claims.reason
    );
    match state
        .orderbook
        .decline_proof_order(order_id, solver_id, decline)
        .await
        .map_err(status_for_error)?
    {
        Some(AdvanceOutcome::Advanced(next)) => {
            crate::service_log!(
                "orderbook",
                "order rerouted order_id={} solver={next}",
                short_id(order_id)
            );
            Ok(StatusCode::NO_CONTENT)
        }
        Some(AdvanceOutcome::AwaitingCapacity) => {
            crate::service_log!(
                "orderbook",
                "order awaiting fallback capacity order_id={}",
                short_id(order_id)
            );
            Ok(StatusCode::NO_CONTENT)
        }
        Some(AdvanceOutcome::Exhausted) => {
            crate::service_warn!(
                "orderbook",
                "solver routes exhausted order_id={}",
                short_id(order_id)
            );
            Ok(StatusCode::NO_CONTENT)
        }
        None => Err(StatusCode::FORBIDDEN),
    }
}
