use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use kage_types::proof_orders::SolverProofDecisionRequest;

use super::{
    super::{ApiState, auth, error::status_for_error, now_ms},
    body::decode_solver_body,
    validation::{proof_acceptance_is_valid, proof_rejection_is_valid},
};
use crate::{order::OrderId, storage::SignedProofDecision};

pub(in crate::api) async fn solver_order_result(
    State(state): State<ApiState>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let solver_id = auth::authenticated_solver(&state, &headers)?;
    auth::active_solver(&state, solver_id)?;
    let request: SolverProofDecisionRequest = decode_solver_body(&headers, &body)?;
    let proof_orders = &state.proof_orders;
    let binding = proof_orders
        .binding(order_id)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .ok_or(StatusCode::NOT_FOUND)?;
    if binding.bindings.solver_id != solver_id {
        return Err(StatusCode::FORBIDDEN);
    }
    let now = i64::try_from(now_ms()).map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let decision = match request {
        SolverProofDecisionRequest::ProofAccepted { acceptance } => {
            if !proof_acceptance_is_valid(&acceptance, &binding, solver_id, now) {
                return Err(StatusCode::UNPROCESSABLE_ENTITY);
            }
            SignedProofDecision::Accepted(acceptance)
        }
        SolverProofDecisionRequest::ProofRejected { rejection } => {
            if !proof_rejection_is_valid(&rejection, &binding, solver_id, now) {
                return Err(StatusCode::UNPROCESSABLE_ENTITY);
            }
            SignedProofDecision::Rejected(rejection)
        }
    };
    let updated = state
        .orderbook
        .update_proof_result(order_id, solver_id, decision)
        .await
        .map_err(status_for_error)?;
    if updated {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::CONFLICT)
    }
}
