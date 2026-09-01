use axum::{
    Json,
    extract::State,
    http::{StatusCode, header::CACHE_CONTROL},
    response::IntoResponse,
};

use super::super::{
    ApiState,
    error::{ApiError, api_error},
    now_ms,
};
use crate::preview::PreviewError;
use kage_types::routing::PreviewRequest;

pub(in crate::api) async fn create_preview(
    State(state): State<ApiState>,
    Json(request): Json<PreviewRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let preview = state
        .preview
        .as_ref()
        .ok_or_else(|| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "preview_unavailable",
                "preview service is unavailable",
                Vec::new(),
            )
        })?
        .create(request, now_ms())
        .await
        .map_err(preview_error)?;
    Ok(([(CACHE_CONTROL, "no-store")], Json(preview)))
}

pub(super) fn preview_error(error: PreviewError) -> ApiError {
    let (status, code) = match error {
        PreviewError::Pricing(_) => (StatusCode::SERVICE_UNAVAILABLE, "pricing_unavailable"),
        PreviewError::Registry(_) => (StatusCode::SERVICE_UNAVAILABLE, "registry_unavailable"),
        PreviewError::Storage(_) => (StatusCode::SERVICE_UNAVAILABLE, "preview_store_unavailable"),
        PreviewError::NoRoute => (StatusCode::SERVICE_UNAVAILABLE, "no_solver_route"),
        PreviewError::DeviationExceeded { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, "quote_deviation_exceeded")
        }
        PreviewError::UnknownPreview => (StatusCode::GONE, "preview_expired"),
        PreviewError::UnsupportedMarket
        | PreviewError::TermsMismatch
        | PreviewError::FeeCategoryUnavailable
        | PreviewError::InvalidRecipients
        | PreviewError::Arithmetic => (StatusCode::UNPROCESSABLE_ENTITY, "invalid_preview_order"),
    };
    api_error(status, code, error.to_string(), Vec::new())
}
