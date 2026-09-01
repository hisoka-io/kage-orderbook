use alloy_primitives::B256;
use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::IntoResponse,
};
use kage_types::{
    api_types::{CreateOrderResponse, ORDER_ACCESS_TOKEN_HEADER},
    proof_orders::{CreateOrderRequest, ProofOrderResponse, validate_recipient_set},
    routing::PROOF_ENVELOPE_SUITE,
};

use super::{
    super::{
        ApiState, auth,
        error::{ApiError, api_error, api_error_for_service},
        now_ms,
    },
    preview::preview_error,
};
use crate::{
    config::ProofOrderSettings,
    order::OrderId,
    storage::{InsertOutcome, NewProofOrder, RepositoryError},
};

pub(in crate::api) async fn create_encrypted_order(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    if body.len() > state.api.max_order_request_bytes {
        return Err(api_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "order_request_too_large",
            "encrypted order request exceeds its configured limit",
            Vec::new(),
        ));
    }
    let request: CreateOrderRequest = if headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/msgpack"))
    {
        rmp_serde::from_slice(&body).map_err(|_| {
            api_error(
                StatusCode::BAD_REQUEST,
                "invalid_body",
                "invalid MessagePack order",
                Vec::new(),
            )
        })?
    } else {
        serde_json::from_slice(&body).map_err(|_| {
            api_error(
                StatusCode::BAD_REQUEST,
                "invalid_body",
                "invalid JSON order",
                Vec::new(),
            )
        })?
    };
    if headers.contains_key(ORDER_ACCESS_TOKEN_HEADER) {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "raw_access_token_forbidden",
            "send only access_token_hash when creating an order",
            Vec::new(),
        ));
    }
    let access_hash = request.access_token_hash;
    let proof_orders = &state.proof_orders;
    match proof_orders.preflight_create_request(&request).await {
        Ok(Some(InsertOutcome::Existing)) => {
            return Ok((
                StatusCode::OK,
                Json(CreateOrderResponse {
                    order_id: request.client_order_id,
                    expires_at_ms: request.terms.expires_at_ms,
                    created: false,
                }),
            ));
        }
        Ok(Some(InsertOutcome::Created)) => unreachable!("preflight never creates an order"),
        Ok(None) => {}
        Err(RepositoryError::IdempotencyConflict) => {
            return Err(api_error(
                StatusCode::CONFLICT,
                "idempotency_conflict",
                "order ID or access token is bound to different immutable data",
                Vec::new(),
            ));
        }
        Err(error) => {
            crate::service_error!("orderbook", "proof-order preflight failed error={error}");
            return Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "proof_store_unavailable",
                "proof order storage is unavailable",
                Vec::new(),
            ));
        }
    }
    let now = now_ms();
    let terms = request.terms;
    if !proof_deadline_is_admissible(terms.expires_at_ms, now as i64, &state.proof_order_settings) {
        return Err(api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_proof_deadline",
            format!(
                "proof expiry must equal an exact Unix second and leave more than {} but no more than {} seconds",
                state.proof_order_settings.minimum_remaining_seconds,
                state.proof_order_settings.proof_lifetime_seconds
            ),
            Vec::new(),
        ));
    }
    if request.client_order_id.is_nil()
        || request.access_token_hash == B256::ZERO
        || request.preview_id == B256::ZERO
        || request.category_id.is_empty()
        || request.category_id.len() > 64
        || request.encrypted_proof.suite != PROOF_ENVELOPE_SUITE
        || request.encrypted_proof.nonce.len() != 24
        || request.encrypted_proof.nonce.iter().all(|byte| *byte == 0)
        || request.encrypted_proof.ciphertext.len() < 16
        || request.encrypted_proof.ciphertext.len() > state.api.max_ciphertext_bytes
        || request
            .encrypted_proof
            .ciphertext
            .iter()
            .all(|byte| *byte == 0)
        || request.encrypted_proof.recipients.len() > state.proof_order_settings.max_recipients
        || validate_recipient_set(&request.encrypted_proof).is_err()
        || request.encrypted_proof.recipients.iter().any(|recipient| {
            recipient.encapsulated_key.len() != 32
                || recipient.encapsulated_key.iter().all(|byte| *byte == 0)
                || recipient.wrapped_key.len() != 48
                || recipient.wrapped_key.iter().all(|byte| *byte == 0)
        })
        || request.encrypted_proof.ciphertext_digest == B256::ZERO
        || alloy_primitives::keccak256(&request.encrypted_proof.ciphertext)
            != request.encrypted_proof.ciphertext_digest
        || request.domain_hash == B256::ZERO
        || request.settlement_commitment == B256::ZERO
    {
        return Err(api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_envelope",
            "proof envelope is invalid",
            Vec::new(),
        ));
    }
    let preview = state.preview.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "preview_unavailable",
            "preview service is unavailable",
            Vec::new(),
        )
    })?;
    if preview.expected_domain(terms.chain_id) != Some(request.domain_hash) {
        return Err(api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_domain",
            "proof domain does not match the configured chain and DarkPool",
            Vec::new(),
        ));
    }
    let mut eligible = preview
        .eligible_routes(
            request.preview_id,
            &request.category_id,
            &terms,
            &request.encrypted_proof,
            now,
        )
        .await
        .map_err(preview_error)?;
    rotate_candidates(&mut eligible.routes, request.client_order_id);
    let input = NewProofOrder {
        order_id: request.client_order_id,
        access_token_hash: access_hash,
        preview_id: request.preview_id,
        category_id: eligible.category_id,
        terms,
        domain_hash: request.domain_hash,
        fee_bps: eligible.fee_bps,
        settlement_commitment: request.settlement_commitment,
        proof: request.encrypted_proof,
        candidates: eligible.routes,
        created_at_ms: now as i64,
        reservation_attempt_timeout_ms: state.proof_order_settings.reservation_attempt_timeout_ms,
        ciphertext_cleanup_grace_seconds: state
            .proof_order_settings
            .ciphertext_cleanup_grace_seconds,
    };
    match proof_orders.preflight_authoritative(&input).await {
        Ok(Some(InsertOutcome::Existing)) => {}
        Ok(Some(InsertOutcome::Created)) => unreachable!("preflight never creates an order"),
        Ok(None) => {
            let readiness = state.readiness.snapshot();
            if !readiness.ready {
                return Err(api_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "service_not_ready",
                    "orderbook is not accepting new orders",
                    readiness.missing,
                ));
            }
        }
        Err(RepositoryError::IdempotencyConflict) => {
            return Err(api_error(
                StatusCode::CONFLICT,
                "idempotency_conflict",
                "order ID or access token is bound to different immutable data",
                Vec::new(),
            ));
        }
        Err(error) => {
            crate::service_error!("orderbook", "proof-order preflight failed error={error}");
            return Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "proof_store_unavailable",
                "proof order storage is unavailable",
                Vec::new(),
            ));
        }
    }
    let requested_order_id = input.order_id;
    let outcome = state
        .orderbook
        .create_proof_order(input)
        .await
        .map_err(api_error_for_service)?;
    if outcome.order.id != requested_order_id {
        return Err(api_error(
            StatusCode::CONFLICT,
            "idempotency_conflict",
            "order access token is already bound to another order",
            Vec::new(),
        ));
    }
    Ok((
        if outcome.created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(CreateOrderResponse {
            order_id: outcome.order.id,
            expires_at_ms: outcome.order.expires_at_ms.unwrap_or(terms.expires_at_ms),
            created: outcome.created,
        }),
    ))
}

pub(in crate::api) async fn get_order(
    State(state): State<ApiState>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
) -> Result<Json<ProofOrderResponse>, StatusCode> {
    let access_token_hash = auth::access_token_hash_from_headers(&headers)?;
    let order = state
        .proof_orders
        .authenticated_snapshot(order_id, access_token_hash)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(order))
}

pub(in crate::api) fn expected_proof_expiry_secs(expires_at_ms: i64) -> Option<u64> {
    if expires_at_ms < 0 || expires_at_ms % 1_000 != 0 {
        return None;
    }
    u64::try_from(expires_at_ms / 1_000).ok()
}

pub(in crate::api) fn proof_deadline_is_admissible(
    expires_at_ms: i64,
    now_ms: i64,
    policy: &ProofOrderSettings,
) -> bool {
    if expected_proof_expiry_secs(expires_at_ms).is_none() {
        return false;
    }
    let remaining_ms = expires_at_ms.saturating_sub(now_ms);
    let minimum_ms = i64::from(policy.minimum_remaining_seconds).saturating_mul(1_000);
    let maximum_ms = i64::from(policy.proof_lifetime_seconds).saturating_mul(1_000);
    remaining_ms > minimum_ms && remaining_ms <= maximum_ms
}

pub(in crate::api) fn rotate_candidates(
    routes: &mut [kage_types::routing::PreviewRoute],
    order_id: OrderId,
) {
    if routes.len() < 2 {
        return;
    }
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&order_id.as_bytes()[..8]);
    let offset = (u64::from_be_bytes(prefix) as usize) % routes.len();
    routes.rotate_left(offset);
}
