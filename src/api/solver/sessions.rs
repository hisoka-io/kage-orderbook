use axum::{Json, extract::State, http::StatusCode};

use super::super::{ApiState, auth, now_ms};
use crate::session::{ChallengeResponse, SessionRequest, SessionResponse};

pub(in crate::api) async fn solver_challenge(
    State(state): State<ApiState>,
) -> Json<ChallengeResponse> {
    Json(state.sessions.issue_challenge(now_ms()))
}

pub(in crate::api) async fn solver_session(
    State(state): State<ApiState>,
    Json(request): Json<SessionRequest>,
) -> Result<Json<SessionResponse>, StatusCode> {
    let now = now_ms();
    let solver_id = state.sessions.recover(&request, now).map_err(|error| {
        crate::service_warn!("orderbook", "solver authentication failed reason={error}");
        StatusCode::UNAUTHORIZED
    })?;
    let _guard = state.sessions.capacity_guard().await;
    auth::active_solver(&state, solver_id)?;

    tracing::debug!(target: "orderbook", %solver_id, "solver authenticated");
    Ok(Json(state.sessions.open(solver_id, now)))
}
