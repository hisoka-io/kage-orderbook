use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use kage_types::routing::SolverCapabilities;

use super::super::{ApiState, auth, now_ms};

pub(in crate::api) async fn register_solver_capabilities(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(capabilities): Json<SolverCapabilities>,
) -> Result<StatusCode, StatusCode> {
    let token = auth::bearer_token(&headers)?;
    let solver_id = state
        .sessions
        .resolve(&token, now_ms())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    auth::active_solver(&state, solver_id)?;
    state
        .sessions
        .register_capabilities(&token, capabilities, now_ms())
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|error| match error {
            crate::session::AuthError::UnknownSession => StatusCode::UNAUTHORIZED,
            crate::session::AuthError::StaleCapabilityRevision => StatusCode::CONFLICT,
            _ => StatusCode::UNPROCESSABLE_ENTITY,
        })
}
