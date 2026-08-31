use axum::{Json, http::StatusCode};

use super::ApiErrorResponse;
use crate::{
    core::engine::{OrderError, ServiceError},
    storage::RepositoryError,
};

pub(super) type ApiError = (StatusCode, Json<ApiErrorResponse>);

pub(super) fn status_for_error(error: ServiceError) -> StatusCode {
    match error {
        ServiceError::RouteCapacityChanged => StatusCode::CONFLICT,
        ServiceError::AdmissionUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        ServiceError::ProofDeadlineChanged => StatusCode::UNPROCESSABLE_ENTITY,
        ServiceError::PreviewExpired => StatusCode::GONE,
        ServiceError::Repository(RepositoryError::IdempotencyConflict) => StatusCode::CONFLICT,
        ServiceError::Closed | ServiceError::Repository(_) => StatusCode::SERVICE_UNAVAILABLE,
        ServiceError::Order(OrderError::InvalidTerms | OrderError::InvalidPayload) => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        ServiceError::Order(OrderError::NotFound) => StatusCode::NOT_FOUND,
        ServiceError::Order(OrderError::AlreadyExists | OrderError::InvalidState) => {
            StatusCode::CONFLICT
        }
    }
}

pub(super) fn api_error(
    status: StatusCode,
    code: impl Into<String>,
    message: impl Into<String>,
    missing: Vec<String>,
) -> ApiError {
    (
        status,
        Json(ApiErrorResponse {
            code: code.into(),
            message: message.into(),
            missing,
        }),
    )
}

pub(super) fn api_error_for_service(error: ServiceError) -> ApiError {
    let status = match error {
        ServiceError::RouteCapacityChanged => {
            return api_error(
                StatusCode::CONFLICT,
                "route_capacity_changed",
                "solver capacity changed; request a fresh preview and retry",
                Vec::new(),
            );
        }
        ServiceError::AdmissionUnavailable => {
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "admission_unavailable",
                "order admission is temporarily unavailable; retry later",
                Vec::new(),
            );
        }
        ServiceError::ProofDeadlineChanged => {
            return api_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_proof_deadline",
                "proof deadline became too short while the order was being admitted; regenerate and retry",
                Vec::new(),
            );
        }
        ServiceError::PreviewExpired => {
            return api_error(
                StatusCode::GONE,
                "preview_expired",
                "preview expired while the order was being admitted; request a fresh preview and retry",
                Vec::new(),
            );
        }
        error => status_for_error(error),
    };
    let message = match status {
        StatusCode::SERVICE_UNAVAILABLE => "order service is unavailable",
        StatusCode::UNPROCESSABLE_ENTITY => "order failed lifecycle validation",
        StatusCode::NOT_FOUND => "order was not found",
        StatusCode::CONFLICT => "order conflicts with its current state",
        _ => "order request failed",
    };
    api_error(status, "order_service_error", message, Vec::new())
}
