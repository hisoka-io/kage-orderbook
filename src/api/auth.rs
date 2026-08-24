use axum::http::{HeaderMap, StatusCode};
use axum_extra::headers::{
    Header,
    authorization::{Authorization, Bearer},
};

use super::{ApiState, ORDER_COMMITMENT_HEADER, now_ms};
use crate::{
    order::{OrderCommitment, SolverId},
    registry::SolverProfile,
};

pub(super) fn commitment_from_headers(headers: &HeaderMap) -> Result<OrderCommitment, StatusCode> {
    headers
        .get(ORDER_COMMITMENT_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .ok_or(StatusCode::NOT_FOUND)
}

pub(super) fn active_solver(
    state: &ApiState,
    solver_id: SolverId,
) -> Result<SolverProfile, StatusCode> {
    state.registry.health().map_err(|error| {
        crate::service_warn!(
            "orderbook",
            "solver lookup deferred solver={solver_id} {error}"
        );
        StatusCode::SERVICE_UNAVAILABLE
    })?;
    let profile = state
        .registry
        .get(solver_id)
        .filter(|profile| profile.active)
        .ok_or(StatusCode::FORBIDDEN)?;
    if profile.noise_public_key == alloy_primitives::B256::ZERO {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(profile)
}

pub(super) fn authenticated_solver(
    state: &ApiState,
    headers: &HeaderMap,
) -> Result<SolverId, StatusCode> {
    let Authorization(bearer) = Authorization::<Bearer>::decode(
        &mut headers.get_all(axum::http::header::AUTHORIZATION).iter(),
    )
    .map_err(|_| StatusCode::UNAUTHORIZED)?;
    state
        .sessions
        .resolve(bearer.token(), now_ms())
        .ok_or(StatusCode::UNAUTHORIZED)
}
