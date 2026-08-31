use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use kage_types::proof_orders::ReservationOffer;

use super::super::{ApiState, auth, now_ms};

pub(in crate::api) async fn reserving_orders(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<axum::response::Response, StatusCode> {
    let solver_id = auth::authenticated_solver(&state, &headers)?;
    auth::active_solver(&state, solver_id)?;
    let pending = state
        .proof_orders
        .pending_reservations(
            solver_id,
            i64::try_from(now_ms()).map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?,
        )
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let offers = pending
        .into_iter()
        .map(|pending| {
            let request = state
                .assignment_issuer
                .issue_reservation_request(pending.claims)
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
            Ok(ReservationOffer {
                request,
                terms: pending.terms,
                domain_hash: pending.domain_hash,
                fee_bps: pending.fee_bps,
                settlement_commitment: pending.settlement_commitment,
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;
    Ok(Json(offers).into_response())
}
