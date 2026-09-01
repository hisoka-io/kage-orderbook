use alloy_primitives::{Address, Signature};
use kage_types::proof_orders::{
    ProofAcceptanceAck, ProofRejectionAck, ReservationAck, ReservationDecline,
};

use crate::storage::{PendingReservation, ProofOrderBinding};

pub(in crate::api) fn signature_matches(
    signature: &[u8],
    message: Vec<u8>,
    solver_id: Address,
) -> bool {
    Signature::try_from(signature)
        .ok()
        .and_then(|signature| signature.recover_address_from_msg(message).ok())
        == Some(solver_id)
}

pub(in crate::api) fn reservation_ack_is_valid(
    ack: &ReservationAck,
    pending: &PendingReservation,
    solver_id: Address,
    now_ms: u64,
) -> bool {
    let Ok(now_ms) = i64::try_from(now_ms) else {
        return false;
    };
    ack.claims.bindings == pending.claims.bindings
        && ack.claims.bindings.solver_id == solver_id
        && ack.claims.attempt_nonce == pending.claims.attempt_nonce
        && ack.claims.accepted_at_ms >= pending.claims.requested_at_ms
        && ack.claims.accepted_at_ms <= now_ms
        && ack.claims.accepted_at_ms < pending.claims.attempt_expires_at_ms
        && now_ms < pending.claims.attempt_expires_at_ms
        && now_ms < pending.terms.expires_at_ms
        && signature_matches(&ack.signature, ack.claims.signing_bytes(), solver_id)
}

pub(in crate::api) fn reservation_decline_is_valid(
    decline: &ReservationDecline,
    pending: &PendingReservation,
    solver_id: Address,
    now_ms: u64,
) -> bool {
    let Ok(now_ms) = i64::try_from(now_ms) else {
        return false;
    };
    decline.claims.bindings == pending.claims.bindings
        && decline.claims.bindings.solver_id == solver_id
        && decline.claims.attempt_nonce == pending.claims.attempt_nonce
        && decline.claims.declined_at_ms >= pending.claims.requested_at_ms
        && decline.claims.declined_at_ms <= now_ms
        && decline.claims.declined_at_ms < pending.claims.attempt_expires_at_ms
        && now_ms < pending.claims.attempt_expires_at_ms
        && now_ms < pending.terms.expires_at_ms
        && signature_matches(
            &decline.signature,
            decline.claims.signing_bytes(),
            solver_id,
        )
}

pub(in crate::api) fn proof_acceptance_is_valid(
    acceptance: &ProofAcceptanceAck,
    binding: &ProofOrderBinding,
    solver_id: Address,
    now_ms: i64,
) -> bool {
    let claims = &acceptance.claims;
    claims.bindings == binding.bindings
        && claims.bindings.solver_id == solver_id
        && claims.assignment_ticket_digest == binding.assignment_digest
        && claims.settlement_commitment == binding.settlement_commitment
        && proof_decision_timestamp_is_valid(claims.accepted_at_ms, binding, now_ms, true)
        && signature_matches(&acceptance.signature, claims.signing_bytes(), solver_id)
}

pub(in crate::api) fn proof_rejection_is_valid(
    rejection: &ProofRejectionAck,
    binding: &ProofOrderBinding,
    solver_id: Address,
    now_ms: i64,
) -> bool {
    let claims = &rejection.claims;
    claims.bindings == binding.bindings
        && claims.bindings.solver_id == solver_id
        && claims.assignment_ticket_digest == binding.assignment_digest
        && proof_decision_timestamp_is_valid(claims.rejected_at_ms, binding, now_ms, false)
        && signature_matches(&rejection.signature, claims.signing_bytes(), solver_id)
}

pub(in crate::api) fn proof_decision_timestamp_is_valid(
    decision_at_ms: i64,
    binding: &ProofOrderBinding,
    now_ms: i64,
    must_precede_expiry: bool,
) -> bool {
    let proof_expires_at_ms = i64::try_from(binding.bindings.proof_expires_at_secs)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000);
    decision_at_ms >= binding.disclosed_at_ms
        && decision_at_ms <= now_ms
        && (!must_precede_expiry || decision_at_ms < proof_expires_at_ms)
}
