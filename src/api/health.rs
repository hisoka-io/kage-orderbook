use axum::{Json, extract::State, http::StatusCode};

use super::ApiState;
use crate::readiness::ReadinessSnapshot;

pub(super) async fn liveness(State(state): State<ApiState>) -> StatusCode {
    if state.readiness.is_live() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

pub(super) async fn readiness_health(
    State(state): State<ApiState>,
) -> (StatusCode, Json<ReadinessSnapshot>) {
    let snapshot = state.readiness.snapshot();
    let status = if snapshot.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(snapshot))
}
