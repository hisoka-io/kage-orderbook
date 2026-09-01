use super::*;

pub(super) async fn insert_core_order(
    transaction: &mut Transaction<'_, Sqlite>,
    order: &Order,
    created_at_ms: i64,
) -> Result<(), RepositoryError> {
    sqlx::query(
        "INSERT INTO orders (
            id, chain_id, state, version, token_in, token_out, amount_in, amount_out,
            created_at_ms, updated_at_ms,
            expires_at_ms, solver_address
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(order.id.to_string())
    .bind(i64::try_from(order.chain_id).map_err(|_| invalid("chain_id", order.chain_id))?)
    .bind(state_name(order.state))
    .bind(i64::try_from(order.version).map_err(|_| invalid("version", order.version))?)
    .bind(order.token_in.as_slice())
    .bind(order.token_out.as_slice())
    .bind(order.amount_in.to_string())
    .bind(order.amount_out.to_string())
    .bind(created_at_ms)
    .bind(created_at_ms)
    .bind(order.expires_at_ms)
    .bind(order.solver.map(|address| address.to_vec()))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(super) async fn update_core_order(
    transaction: &mut Transaction<'_, Sqlite>,
    order: &Order,
    expected_version: u64,
    now_ms: i64,
) -> Result<(), RepositoryError> {
    let result = sqlx::query(
        "UPDATE orders SET state = ?, version = ?, solver_address = ?,
             updated_at_ms = ?
         WHERE id = ? AND version = ?",
    )
    .bind(state_name(order.state))
    .bind(i64::try_from(order.version).map_err(|_| invalid("version", order.version))?)
    .bind(order.solver.map(|address| address.to_vec()))
    .bind(now_ms)
    .bind(order.id.to_string())
    .bind(i64::try_from(expected_version).map_err(|_| invalid("version", expected_version))?)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() != 1 {
        return Err(RepositoryError::VersionConflict);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn insert_attempt(
    transaction: &mut Transaction<'_, Sqlite>,
    order_id: OrderId,
    attempt_number: i64,
    solver_id: Address,
    key_id: B256,
    now_ms: i64,
    timeout_ms: u64,
) -> Result<(), RepositoryError> {
    let deadline_ms = now_ms.saturating_add(i64::try_from(timeout_ms).unwrap_or(i64::MAX));
    sqlx::query(
        "INSERT INTO proof_order_reservation_attempts (
            order_id, attempt_number, solver_id, key_id, attempt_nonce,
            requested_at_ms, deadline_ms, outcome
         ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')",
    )
    .bind(order_id.to_string())
    .bind(attempt_number)
    .bind(solver_id.as_slice())
    .bind(key_id.as_slice())
    .bind(B256::random().as_slice())
    .bind(now_ms)
    .bind(deadline_ms)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(super) fn request_digest(input: &NewProofOrder) -> Result<B256, RepositoryError> {
    request_digest_fields(
        input.order_id,
        input.access_token_hash,
        input.preview_id,
        &input.category_id,
        &input.terms,
        input.domain_hash,
        input.settlement_commitment,
        &input.proof,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn request_digest_fields(
    order_id: OrderId,
    access_token_hash: OrderAccessTokenHash,
    preview_id: B256,
    category_id: &str,
    terms: &TradeTerms,
    domain_hash: B256,
    settlement_commitment: B256,
    proof: &MultiRecipientProof,
) -> Result<B256, RepositoryError> {
    let envelope =
        rmp_serde::to_vec_named(proof).map_err(|error| invalid("envelope", error.to_string()))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"kage-proof-order/request/v1");
    bytes.extend_from_slice(order_id.as_bytes());
    bytes.extend_from_slice(access_token_hash.as_slice());
    bytes.extend_from_slice(preview_id.as_slice());
    bytes.extend_from_slice(&(category_id.len() as u64).to_be_bytes());
    bytes.extend_from_slice(category_id.as_bytes());
    bytes.extend_from_slice(exact_terms_digest(terms, domain_hash).as_slice());
    bytes.extend_from_slice(settlement_commitment.as_slice());
    bytes.extend_from_slice(&(envelope.len() as u64).to_be_bytes());
    bytes.extend_from_slice(&envelope);
    Ok(keccak256(bytes))
}

pub(super) fn pending_reservation_from_row(
    row: &sqlx::sqlite::SqliteRow,
    solver_id: Address,
) -> Result<PendingReservation, RepositoryError> {
    let order_id = parse_order_id(row.try_get("order_id")?)?;
    let proof_expires_at_ms: i64 = row.try_get("proof_expires_at_ms")?;
    if proof_expires_at_ms < 0 || proof_expires_at_ms % 1_000 != 0 {
        return Err(invalid("proof_expires_at_ms", proof_expires_at_ms));
    }
    let terms = terms_from_row(row)?;
    let domain_hash = parse_b256("domain_hash", row.try_get("domain_hash")?)?;
    let stored_terms_digest = parse_b256("exact_terms_digest", row.try_get("exact_terms_digest")?)?;
    if exact_terms_digest(&terms, domain_hash) != stored_terms_digest {
        return Err(invalid("exact_terms_digest", "does not match stored terms"));
    }
    Ok(PendingReservation {
        claims: ReservationRequestClaims {
            bindings: ProofOrderBindings {
                order_id,
                preview_id: parse_b256("preview_id", row.try_get("preview_id")?)?,
                category_id: row.try_get("category_id")?,
                solver_id,
                exact_terms_digest: stored_terms_digest,
                ciphertext_digest: parse_b256(
                    "ciphertext_digest",
                    row.try_get("ciphertext_digest")?,
                )?,
                proof_expires_at_secs: u64::try_from(proof_expires_at_ms / 1_000)
                    .map_err(|_| invalid("proof_expires_at_ms", proof_expires_at_ms))?,
            },
            attempt_nonce: parse_b256("attempt_nonce", row.try_get("attempt_nonce")?)?,
            requested_at_ms: row.try_get("requested_at_ms")?,
            attempt_expires_at_ms: row.try_get("deadline_ms")?,
        },
        terms,
        domain_hash,
        fee_bps: u16::try_from(row.try_get::<i64, _>("fee_bps")?)
            .map_err(|_| invalid("fee_bps", "out of range"))?,
        settlement_commitment: parse_b256(
            "settlement_commitment",
            row.try_get("settlement_commitment")?,
        )?,
        key_id: parse_b256("key_id", row.try_get("key_id")?)?,
    })
}

pub(super) fn terms_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<TradeTerms, RepositoryError> {
    let proof_expires_at_ms: i64 = row.try_get("proof_expires_at_ms")?;
    if proof_expires_at_ms < 0 || proof_expires_at_ms % 1_000 != 0 {
        return Err(invalid("proof_expires_at_ms", proof_expires_at_ms));
    }
    Ok(TradeTerms {
        chain_id: to_u64("chain_id", row.try_get("chain_id")?)?,
        token_in: Address::from(parse_fixed::<20>("token_in", row.try_get("token_in")?)?),
        token_out: Address::from(parse_fixed::<20>("token_out", row.try_get("token_out")?)?),
        amount_in: parse_u256("amount_in", row.try_get("amount_in")?)?,
        amount_out: parse_u256("amount_out", row.try_get("amount_out")?)?,
        expires_at_ms: proof_expires_at_ms,
    })
}

pub(super) fn delivery_from_row(
    row: &sqlx::sqlite::SqliteRow,
    order_id: OrderId,
    solver_id: Address,
    terms: TradeTerms,
) -> Result<SolverProofDelivery, RepositoryError> {
    Ok(SolverProofDelivery {
        suite: row.try_get("envelope_suite")?,
        order_id,
        preview_id: parse_b256("preview_id", row.try_get("preview_id")?)?,
        category_id: row.try_get("category_id")?,
        terms,
        domain_hash: parse_b256("domain_hash", row.try_get("domain_hash")?)?,
        fee_bps: u16::try_from(row.try_get::<i64, _>("fee_bps")?)
            .map_err(|_| invalid("fee_bps", "out of range"))?,
        settlement_commitment: parse_b256(
            "settlement_commitment",
            row.try_get("settlement_commitment")?,
        )?,
        assignment_ticket: rmp_serde::from_slice(&row.try_get::<Vec<u8>, _>("assignment_ticket")?)
            .map_err(|error| invalid("assignment_ticket", error.to_string()))?,
        nonce: row.try_get("nonce")?,
        ciphertext: row.try_get("ciphertext")?,
        ciphertext_digest: parse_b256("ciphertext_digest", row.try_get("ciphertext_digest")?)?,
        recipient: kage_types::routing::RecipientKeyWrap {
            solver_id,
            key_id: parse_b256("key_id", row.try_get("key_id")?)?,
            encapsulated_key: row.try_get("encapsulated_key")?,
            wrapped_key: row.try_get("wrapped_key")?,
        },
    })
}

pub(super) fn state_name(state: ProofOrderState) -> &'static str {
    match state {
        ProofOrderState::Submitted => "submitted",
        ProofOrderState::ReservationPending => "reservation_pending",
        ProofOrderState::Assigned => "assigned",
        ProofOrderState::ProofDelivered => "proof_delivered",
        ProofOrderState::ProofAccepted => "proof_accepted",
        ProofOrderState::ProofRejected => "proof_rejected",
        ProofOrderState::Expired => "expired",
        ProofOrderState::ComplaintVerified => "complaint_verified",
        ProofOrderState::Closed => "closed",
    }
}

pub(super) fn parse_state(value: &str) -> Result<ProofOrderState, RepositoryError> {
    for state in [
        ProofOrderState::Submitted,
        ProofOrderState::ReservationPending,
        ProofOrderState::Assigned,
        ProofOrderState::ProofDelivered,
        ProofOrderState::ProofAccepted,
        ProofOrderState::ProofRejected,
        ProofOrderState::Expired,
        ProofOrderState::ComplaintVerified,
        ProofOrderState::Closed,
    ] {
        if state_name(state) == value {
            return Ok(state);
        }
    }
    Err(invalid("proof_order_state", value))
}

pub(super) fn complaint_status_name(status: ComplaintStatus) -> &'static str {
    match status {
        ComplaintStatus::Verified => "verified",
        ComplaintStatus::Rejected => "rejected",
        ComplaintStatus::Resolved => "resolved",
    }
}

pub(super) fn complaint_evidence_kind_name(kind: ComplaintEvidenceKind) -> &'static str {
    match kind {
        ComplaintEvidenceKind::NoResponseAfterDisclosure => "no_response_after_disclosure",
        ComplaintEvidenceKind::AcceptedNotSettled => "accepted_not_settled",
    }
}

pub(super) fn operational_failure_name(kind: OperationalFailureKind) -> &'static str {
    match kind {
        OperationalFailureKind::Proving => "proving_failure",
        OperationalFailureKind::Submission => "submission_failure",
        OperationalFailureKind::Transaction => "transaction_failure",
    }
}

pub(super) fn parse_complaint_status(value: &str) -> Result<ComplaintStatus, RepositoryError> {
    match value {
        "verified" => Ok(ComplaintStatus::Verified),
        "rejected" => Ok(ComplaintStatus::Rejected),
        "resolved" => Ok(ComplaintStatus::Resolved),
        _ => Err(invalid("complaint_status", value)),
    }
}

pub(super) fn parse_complaint_evidence_kind(
    value: &str,
) -> Result<ComplaintEvidenceKind, RepositoryError> {
    match value {
        "no_response_after_disclosure" => Ok(ComplaintEvidenceKind::NoResponseAfterDisclosure),
        "accepted_not_settled" => Ok(ComplaintEvidenceKind::AcceptedNotSettled),
        _ => Err(invalid("complaint_evidence_kind", value)),
    }
}

pub(super) fn parse_order_id(value: String) -> Result<OrderId, RepositoryError> {
    uuid::Uuid::parse_str(&value).map_err(|_| invalid("order_id", value))
}

pub(super) fn parse_b256(field: &'static str, value: Vec<u8>) -> Result<B256, RepositoryError> {
    Ok(B256::from(parse_fixed::<32>(field, value)?))
}

pub(super) fn parse_u256(field: &'static str, value: String) -> Result<U256, RepositoryError> {
    U256::from_str(&value).map_err(|_| invalid(field, value))
}

pub(super) fn to_u64(field: &'static str, value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| invalid(field, value))
}

pub(super) fn parse_fixed<const N: usize>(
    field: &'static str,
    value: Vec<u8>,
) -> Result<[u8; N], RepositoryError> {
    value
        .try_into()
        .map_err(|value: Vec<u8>| invalid(field, format!("{} bytes", value.len())))
}

pub(super) fn invalid(field: &'static str, value: impl ToString) -> RepositoryError {
    RepositoryError::InvalidData {
        field,
        value: value.to_string(),
    }
}
