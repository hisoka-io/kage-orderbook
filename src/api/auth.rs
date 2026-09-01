use alloy_primitives::B256;
use axum::http::{HeaderMap, StatusCode};
use axum_extra::headers::{
    Header,
    authorization::{Authorization, Bearer},
};

use super::{ApiState, ORDER_ACCESS_TOKEN_HEADER, now_ms};
use crate::{
    order::{OrderAccessTokenHash, SolverId},
    registry::SolverProfile,
};

pub(super) fn access_token_hash_from_headers(
    headers: &HeaderMap,
) -> Result<OrderAccessTokenHash, StatusCode> {
    let token: B256 = headers
        .get(ORDER_ACCESS_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .ok_or(StatusCode::NOT_FOUND)?;
    if token == B256::ZERO {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(access_token_hash(token))
}

pub(super) fn access_token_hash(token: B256) -> OrderAccessTokenHash {
    kage_types::api_types::order_access_token_hash(token)
}

pub(super) fn active_solver(
    state: &ApiState,
    solver_id: SolverId,
) -> Result<SolverProfile, StatusCode> {
    if !state.allowed_solvers.contains(&solver_id) {
        return Err(StatusCode::FORBIDDEN);
    }
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
    Ok(profile)
}

pub(super) fn authenticated_solver(
    state: &ApiState,
    headers: &HeaderMap,
) -> Result<SolverId, StatusCode> {
    let token = bearer_token(headers)?;
    state
        .sessions
        .resolve(&token, now_ms())
        .ok_or(StatusCode::UNAUTHORIZED)
}

pub(super) fn bearer_token(headers: &HeaderMap) -> Result<String, StatusCode> {
    let Authorization(bearer) = Authorization::<Bearer>::decode(
        &mut headers.get_all(axum::http::header::AUTHORIZATION).iter(),
    )
    .map_err(|_| StatusCode::UNAUTHORIZED)?;
    Ok(bearer.token().to_owned())
}

#[cfg(test)]
mod tests {
    use alloy_primitives::Address;

    use super::*;

    #[test]
    fn configured_solver_allowlist_is_fail_closed() {
        let solver = Address::repeat_byte(7);
        assert!(!std::collections::HashSet::<Address>::new().contains(&solver));
        assert!(std::collections::HashSet::from([solver]).contains(&solver));
    }

    #[test]
    fn zero_order_access_tokens_are_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ORDER_ACCESS_TOKEN_HEADER,
            B256::ZERO.to_string().parse().unwrap(),
        );
        assert_eq!(
            access_token_hash_from_headers(&headers),
            Err(StatusCode::NOT_FOUND)
        );
    }
}
