use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use kage_types::{
    api_types::{ComplaintResponse, ComplaintStatus, CreateComplaintRequest},
    proof_orders::{ComplaintEvidenceKind, settlement_commitment},
};

use super::super::{
    ApiState, auth,
    error::{ApiError, api_error},
    now_ms,
    solver::validation::proof_acceptance_is_valid,
};
use crate::{complaint::ComplaintSecretOpening, logging::short_id, order::OrderId};

pub(in crate::api) async fn create_complaint(
    State(state): State<ApiState>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
    Json(request): Json<CreateComplaintRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let current_ms = i64::try_from(now_ms()).map_err(|_| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "clock_unavailable",
            "the complaint clock is unavailable",
            Vec::new(),
        )
    })?;
    create_complaint_at(state, order_id, headers, request, current_ms).await
}

pub(in crate::api) async fn create_complaint_at(
    state: ApiState,
    order_id: OrderId,
    headers: HeaderMap,
    request: CreateComplaintRequest,
    current_ms: i64,
) -> Result<impl IntoResponse, ApiError> {
    let access_hash = auth::access_token_hash_from_headers(&headers).map_err(|_| {
        api_error(
            StatusCode::NOT_FOUND,
            "order_not_found",
            "order was not found",
            Vec::new(),
        )
    })?;
    state
        .proof_orders
        .authenticated_snapshot(order_id, access_hash)
        .await
        .map_err(|_| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "proof_store_unavailable",
                "proof order storage is unavailable",
                Vec::new(),
            )
        })?
        .ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                "order_not_found",
                "order was not found",
                Vec::new(),
            )
        })?;
    let terms = state
        .proof_orders
        .terms(order_id)
        .await
        .map_err(|_| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "proof_store_unavailable",
                "proof order storage is unavailable",
                Vec::new(),
            )
        })?
        .ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                "order_not_found",
                "order was not found",
                Vec::new(),
            )
        })?;
    let store = &state.proof_orders;
    if let Some(existing) = store.complaint(order_id).await.map_err(|_| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "complaint_store_unavailable",
            "complaint store is unavailable",
            Vec::new(),
        )
    })? {
        return Ok((StatusCode::OK, Json(existing)));
    }
    let reason = request.reason.trim();
    if reason.is_empty() || reason.len() > 500 {
        return Err(api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_complaint",
            "reason must contain between 1 and 500 bytes",
            Vec::new(),
        ));
    }
    let evidence = store
        .accountability_evidence(order_id)
        .await
        .map_err(|_| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "complaint_store_unavailable",
                "complaint store is unavailable",
                Vec::new(),
            )
        })?
        .ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                "order_not_found",
                "order was not found",
                Vec::new(),
            )
        })?;
    let binding = store
        .binding(order_id)
        .await
        .map_err(|_| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "complaint_store_unavailable",
                "complaint store is unavailable",
                Vec::new(),
            )
        })?
        .ok_or_else(|| {
            api_error(
                StatusCode::CONFLICT,
                "proof_not_disclosed",
                "the proof was not disclosed to a solver",
                Vec::new(),
            )
        })?;
    if evidence.assigned_solver != Some(binding.bindings.solver_id)
        || evidence.disclosed_at_ms != Some(binding.disclosed_at_ms)
        || evidence.proof_expires_at_ms
            != i64::try_from(binding.bindings.proof_expires_at_secs)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000)
    {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "complaint_evidence_invalid",
            "stored complaint evidence is inconsistent",
            Vec::new(),
        ));
    }
    if evidence.rejection.is_some() {
        return Err(api_error(
            StatusCode::CONFLICT,
            "proof_rejected",
            "the solver returned signed proof-rejection evidence",
            Vec::new(),
        ));
    }
    let evidence_kind = if let Some(acceptance) = &evidence.acceptance {
        if !proof_acceptance_is_valid(acceptance, &binding, binding.bindings.solver_id, current_ms)
        {
            return Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "complaint_evidence_invalid",
                "stored proof-acceptance evidence is invalid",
                Vec::new(),
            ));
        }
        ComplaintEvidenceKind::AcceptedNotSettled
    } else {
        ComplaintEvidenceKind::NoResponseAfterDisclosure
    };
    let verifier = state.complaint_verifier.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "complaints_unavailable",
            "complaint verifier is unavailable",
            Vec::new(),
        )
    })?;
    let revealed = settlement_commitment(
        binding.domain_hash,
        terms.chain_id,
        verifier.darkpool(),
        request.nullifier,
        request.salt,
    );
    if revealed != evidence.settlement_commitment {
        return Err(api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "commitment_mismatch",
            "the revealed nullifier and salt do not match the settlement commitment",
            Vec::new(),
        ));
    }
    let expiry_ms = evidence.proof_expires_at_ms;
    if current_ms < expiry_ms {
        return Err(api_error(
            StatusCode::CONFLICT,
            "complaint_not_mature",
            "the proof has not expired",
            Vec::new(),
        ));
    }
    let complaint_window_ms = i64::try_from(state.proof_order_settings.complaint_window_seconds)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000);
    if current_ms > expiry_ms.saturating_add(complaint_window_ms) {
        return Err(api_error(
            StatusCode::GONE,
            "complaint_window_expired",
            "the complaint submission window has closed",
            Vec::new(),
        ));
    }
    let verified = verifier
        .is_nullifier_spent(request.nullifier, binding.bindings.proof_expires_at_secs)
        .await
        .map_err(|error| {
            crate::service_warn!(
                "orderbook",
                "complaint chain check failed order_id={} error={error}",
                short_id(order_id)
            );
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "chain_check_unavailable",
                "could not verify the nullifier on-chain",
                Vec::new(),
            )
        })?;
    if verified.spent {
        return Err(api_error(
            StatusCode::CONFLICT,
            "nullifier_already_spent",
            "the disclosed nullifier was already spent at the verification block",
            Vec::new(),
        ));
    }
    let cipher = state.complaint_evidence_cipher.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "complaints_unavailable",
            "complaint evidence encryption is unavailable",
            Vec::new(),
        )
    })?;
    let opening = cipher
        .encrypt(
            order_id,
            evidence_kind,
            ComplaintSecretOpening {
                nullifier: request.nullifier,
                salt: request.salt,
            },
        )
        .map_err(|_| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "complaint_encryption_failed",
                "complaint evidence could not be protected",
                Vec::new(),
            )
        })?;
    let inserted = state
        .orderbook
        .insert_proof_complaint(
            order_id,
            evidence_kind,
            opening,
            ComplaintStatus::Verified,
            reason.to_owned(),
            current_ms,
        )
        .await
        .map_err(|_| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "complaint_store_unavailable",
                "complaint store is unavailable",
                Vec::new(),
            )
        })?;
    if !inserted {
        return Err(api_error(
            StatusCode::CONFLICT,
            "complaint_evidence_changed",
            "the order is no longer eligible for this complaint class",
            Vec::new(),
        ));
    }
    let complaint = store
        .complaint(order_id)
        .await
        .map_err(|_| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "complaint_store_unavailable",
                "complaint store is unavailable",
                Vec::new(),
            )
        })?
        .ok_or_else(|| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "complaint_store_unavailable",
                "complaint was not persisted",
                Vec::new(),
            )
        })?;
    crate::service_warn!(
        "orderbook",
        "complaint recorded order_id={} solver={} kind={:?} status={:?} verification_block={}; manual review required",
        short_id(order_id),
        complaint.solver_id,
        complaint.evidence_kind,
        complaint.status,
        verified.block_number
    );
    Ok((StatusCode::CREATED, Json(complaint)))
}

pub(in crate::api) async fn get_complaint(
    State(state): State<ApiState>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
) -> Result<Json<ComplaintResponse>, ApiError> {
    let access_hash = auth::access_token_hash_from_headers(&headers).map_err(|_| {
        api_error(
            StatusCode::NOT_FOUND,
            "order_not_found",
            "order was not found",
            Vec::new(),
        )
    })?;
    state
        .proof_orders
        .authenticated_snapshot(order_id, access_hash)
        .await
        .map_err(|_| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "proof_store_unavailable",
                "proof order storage is unavailable",
                Vec::new(),
            )
        })?
        .ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                "order_not_found",
                "order was not found",
                Vec::new(),
            )
        })?;
    let complaint = state
        .proof_orders
        .complaint(order_id)
        .await
        .map_err(|_| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "complaint_store_unavailable",
                "complaint store is unavailable",
                Vec::new(),
            )
        })?
        .ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                "complaint_not_found",
                "no complaint exists for this order",
                Vec::new(),
            )
        })?;
    Ok(Json(complaint))
}
