use super::{rows::*, *};

fn decision_evidence(decision: &SignedProofDecision) -> ProofDecisionEvidence<'_> {
    match decision {
        SignedProofDecision::Accepted(ack) => ProofDecisionEvidence::Accepted(ack),
        SignedProofDecision::Rejected(ack) => ProofDecisionEvidence::Rejected(ack),
    }
}

#[derive(serde::Serialize)]
#[serde(untagged)]
enum ProofDecisionEvidence<'a> {
    Accepted(&'a ProofAcceptanceAck),
    Rejected(&'a ProofRejectionAck),
}

impl ProofOrderRepository {
    pub async fn binding(
        &self,
        order_id: OrderId,
    ) -> Result<Option<ProofOrderBinding>, RepositoryError> {
        let row = sqlx::query(
            "SELECT p.assigned_solver, p.preview_id, p.category_id, p.domain_hash,
                    p.exact_terms_digest, p.ciphertext_digest, p.proof_expires_at_ms,
                    p.settlement_commitment, a.assignment_ticket_digest,
                    a.disclosed_at_ms
             FROM proof_orders p
             JOIN proof_order_assignments a ON a.order_id = p.order_id
             WHERE p.order_id = ?",
        )
        .bind(order_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let solver = row
                .try_get::<Option<Vec<u8>>, _>("assigned_solver")?
                .ok_or_else(|| invalid("assigned_solver", "missing"))?;
            let solver_id = Address::from(parse_fixed::<20>("assigned_solver", solver)?);
            let proof_expires_at_ms: i64 = row.try_get("proof_expires_at_ms")?;
            if proof_expires_at_ms < 0 || proof_expires_at_ms % 1_000 != 0 {
                return Err(invalid("proof_expires_at_ms", proof_expires_at_ms));
            }
            Ok(ProofOrderBinding {
                bindings: ProofOrderBindings {
                    order_id,
                    preview_id: parse_b256("preview_id", row.try_get("preview_id")?)?,
                    category_id: row.try_get("category_id")?,
                    solver_id,
                    exact_terms_digest: parse_b256(
                        "exact_terms_digest",
                        row.try_get("exact_terms_digest")?,
                    )?,
                    ciphertext_digest: parse_b256(
                        "ciphertext_digest",
                        row.try_get("ciphertext_digest")?,
                    )?,
                    proof_expires_at_secs: u64::try_from(proof_expires_at_ms / 1_000)
                        .map_err(|_| invalid("proof_expires_at_ms", proof_expires_at_ms))?,
                },
                domain_hash: parse_b256("domain_hash", row.try_get("domain_hash")?)?,
                settlement_commitment: parse_b256(
                    "settlement_commitment",
                    row.try_get("settlement_commitment")?,
                )?,
                assignment_digest: parse_b256(
                    "assignment_ticket_digest",
                    row.try_get("assignment_ticket_digest")?,
                )?,
                disclosed_at_ms: row.try_get("disclosed_at_ms")?,
            })
        })
        .transpose()
    }

    pub async fn update_result(
        &self,
        order_id: OrderId,
        solver_id: Address,
        decision: &SignedProofDecision,
        now_ms: i64,
    ) -> Result<bool, RepositoryError> {
        let evidence = rmp_serde::to_vec_named(&decision_evidence(decision))
            .map_err(|error| invalid("proof_decision", error.to_string()))?;
        let mut transaction = self.pool.begin().await?;
        let current: Option<String> = sqlx::query_scalar(
            "SELECT state FROM proof_orders WHERE order_id = ? AND assigned_solver = ?",
        )
        .bind(order_id.to_string())
        .bind(solver_id.as_slice())
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(current) = current else {
            transaction.rollback().await?;
            return Ok(false);
        };
        let (next_state, ack_column, time_column) = match decision {
            SignedProofDecision::Accepted(_) => {
                ("proof_accepted", "acceptance_ack", "accepted_at_ms")
            }
            SignedProofDecision::Rejected(_) => {
                ("proof_rejected", "rejection_ack", "rejected_at_ms")
            }
        };
        let stored = sqlx::query(
            "SELECT acceptance_ack, rejection_ack
             FROM proof_order_results WHERE order_id = ?",
        )
        .bind(order_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(stored) = stored {
            let acceptance: Option<Vec<u8>> = stored.try_get("acceptance_ack")?;
            let rejection: Option<Vec<u8>> = stored.try_get("rejection_ack")?;
            let exact_retry = match decision {
                SignedProofDecision::Accepted(_) => acceptance.as_deref() == Some(&evidence),
                SignedProofDecision::Rejected(_) => rejection.as_deref() == Some(&evidence),
            };
            transaction.rollback().await?;
            return Ok(exact_retry);
        }
        if current != "proof_delivered" && current != "expired" {
            transaction.rollback().await?;
            return Ok(false);
        }
        let result_row = sqlx::query(&format!(
            "INSERT INTO proof_order_results (order_id, {ack_column}, {time_column})
             VALUES (?, ?, ?)
             ON CONFLICT(order_id) DO UPDATE SET
                {ack_column} = excluded.{ack_column},
                {time_column} = excluded.{time_column}
             WHERE proof_order_results.acceptance_ack IS NULL
               AND proof_order_results.rejection_ack IS NULL"
        ))
        .bind(order_id.to_string())
        .bind(&evidence)
        .bind(now_ms)
        .execute(&mut *transaction)
        .await?;
        if result_row.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(false);
        }
        let updated = sqlx::query(
            "UPDATE proof_orders
             SET state = CASE WHEN state = 'proof_delivered' THEN ? ELSE state END,
                 version = version + 1,
                 proof_accepted_at_ms = CASE WHEN ? = 'proof_accepted' THEN ? ELSE proof_accepted_at_ms END,
                 updated_at_ms = ?
             WHERE order_id = ? AND assigned_solver = ?
               AND state IN ('proof_delivered', 'expired')",
        )
        .bind(next_state)
        .bind(next_state)
        .bind(now_ms)
        .bind(now_ms)
        .bind(order_id.to_string())
        .bind(solver_id.as_slice())
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(false);
        }
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn accountability_evidence(
        &self,
        order_id: OrderId,
    ) -> Result<Option<AccountabilityEvidence>, RepositoryError> {
        let row = sqlx::query(
            "SELECT p.settlement_commitment, p.proof_expires_at_ms, p.assigned_solver,
                    a.disclosed_at_ms, r.acceptance_ack, r.rejection_ack
             FROM proof_orders p
             LEFT JOIN proof_order_assignments a ON a.order_id = p.order_id
             LEFT JOIN proof_order_results r ON r.order_id = p.order_id
             WHERE p.order_id = ?",
        )
        .bind(order_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let acceptance = row
                .try_get::<Option<Vec<u8>>, _>("acceptance_ack")?
                .map(|encoded| {
                    rmp_serde::from_slice(&encoded)
                        .map_err(|error| invalid("acceptance_ack", error.to_string()))
                })
                .transpose()?;
            let rejection = row
                .try_get::<Option<Vec<u8>>, _>("rejection_ack")?
                .map(|encoded| {
                    rmp_serde::from_slice(&encoded)
                        .map_err(|error| invalid("rejection_ack", error.to_string()))
                })
                .transpose()?;
            let assigned_solver = row
                .try_get::<Option<Vec<u8>>, _>("assigned_solver")?
                .map(|solver| parse_fixed::<20>("assigned_solver", solver).map(Address::from))
                .transpose()?;
            Ok(AccountabilityEvidence {
                settlement_commitment: parse_b256(
                    "settlement_commitment",
                    row.try_get("settlement_commitment")?,
                )?,
                proof_expires_at_ms: row.try_get("proof_expires_at_ms")?,
                assigned_solver,
                disclosed_at_ms: row.try_get("disclosed_at_ms")?,
                acceptance,
                rejection,
            })
        })
        .transpose()
    }

    pub async fn record_operational_failure(
        &self,
        order_id: OrderId,
        kind: OperationalFailureKind,
        error_code: &str,
        retryable: bool,
        now_ms: i64,
    ) -> Result<bool, RepositoryError> {
        let result = sqlx::query(
            "INSERT INTO proof_order_operational_events (
                order_id, event_kind, error_code, retryable, occurred_at_ms
             )
             SELECT order_id, ?, ?, ?, ? FROM proof_orders
             WHERE order_id = ? AND proof_accepted_at_ms IS NOT NULL",
        )
        .bind(operational_failure_name(kind))
        .bind(error_code)
        .bind(retryable)
        .bind(now_ms)
        .bind(order_id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_complaint(
        &self,
        order_id: OrderId,
        evidence_kind: ComplaintEvidenceKind,
        opening: &EncryptedComplaintOpening,
        status: ComplaintStatus,
        reason: &str,
        now_ms: i64,
        retention_seconds: u64,
    ) -> Result<bool, RepositoryError> {
        if status != ComplaintStatus::Verified
            || opening.key_id == B256::ZERO
            || opening.ciphertext.len() != 80
            || reason.is_empty()
            || reason.len() > 500
        {
            return Err(invalid("complaint", "invalid encrypted complaint evidence"));
        }
        let lifecycle = complaint_status_name(status);
        let retain_until_ms = now_ms.saturating_add(
            i64::try_from(retention_seconds)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000),
        );
        let mut transaction = self.pool.begin().await?;
        let proof = sqlx::query(
            "SELECT p.state, p.proof_expires_at_ms, a.disclosed_at_ms,
                    r.acceptance_ack IS NOT NULL AS has_acceptance,
                    r.rejection_ack IS NOT NULL AS has_rejection
             FROM proof_orders p
             LEFT JOIN proof_order_assignments a ON a.order_id = p.order_id
             LEFT JOIN proof_order_results r ON r.order_id = p.order_id
             WHERE p.order_id = ?",
        )
        .bind(order_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(proof) = proof else {
            transaction.rollback().await?;
            return Ok(false);
        };
        if proof.try_get::<i64, _>("proof_expires_at_ms")? > now_ms {
            transaction.rollback().await?;
            return Ok(false);
        }
        let disclosed = proof
            .try_get::<Option<i64>, _>("disclosed_at_ms")?
            .is_some();
        let has_acceptance = proof.try_get::<i64, _>("has_acceptance")? != 0;
        let has_rejection = proof.try_get::<i64, _>("has_rejection")? != 0;
        let evidence_matches = match evidence_kind {
            ComplaintEvidenceKind::NoResponseAfterDisclosure => {
                disclosed && !has_acceptance && !has_rejection
            }
            ComplaintEvidenceKind::AcceptedNotSettled => {
                disclosed && has_acceptance && !has_rejection
            }
        };
        if !evidence_matches {
            transaction.rollback().await?;
            return Ok(false);
        }
        let mut proof_state: String = proof.try_get("state")?;
        if matches!(proof_state.as_str(), "proof_delivered" | "proof_accepted") {
            let expired = sqlx::query(
                "UPDATE proof_orders
                 SET state = 'expired', version = version + 1, updated_at_ms = ?
                 WHERE order_id = ? AND state = ?",
            )
            .bind(now_ms)
            .bind(order_id.to_string())
            .bind(&proof_state)
            .execute(&mut *transaction)
            .await?;
            if expired.rows_affected() != 1 {
                transaction.rollback().await?;
                return Ok(false);
            }
            proof_state = "expired".to_owned();
        }
        if proof_state != "expired" {
            transaction.rollback().await?;
            return Ok(false);
        }
        let result = sqlx::query(
            "INSERT INTO proof_order_complaints (
                order_id, evidence_kind, lifecycle_status, evidence_key_id,
                opening_nonce, opening_ciphertext, reason, created_at_ms,
                updated_at_ms, retain_until_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(order_id) DO NOTHING",
        )
        .bind(order_id.to_string())
        .bind(complaint_evidence_kind_name(evidence_kind))
        .bind(lifecycle)
        .bind(opening.key_id.as_slice())
        .bind(opening.nonce.as_slice())
        .bind(&opening.ciphertext)
        .bind(reason)
        .bind(now_ms)
        .bind(now_ms)
        .bind(retain_until_ms)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 1 && status == ComplaintStatus::Verified {
            sqlx::query(
                "UPDATE proof_orders
                 SET state = 'complaint_verified', version = version + 1, updated_at_ms = ?
                 WHERE order_id = ? AND state = 'expired'",
            )
            .bind(now_ms)
            .bind(order_id.to_string())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn complaint(
        &self,
        order_id: OrderId,
    ) -> Result<Option<ComplaintResponse>, RepositoryError> {
        let row = sqlx::query(
            "SELECT c.evidence_kind, c.lifecycle_status, c.reason,
                    c.created_at_ms, c.updated_at_ms,
                    p.assigned_solver, p.proof_expires_at_ms
             FROM proof_order_complaints c
             JOIN proof_orders p ON p.order_id = c.order_id
             WHERE c.order_id = ?",
        )
        .bind(order_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let status_text: String = row.try_get("lifecycle_status")?;
            let status = parse_complaint_status(&status_text)?;
            let evidence_kind_text: String = row.try_get("evidence_kind")?;
            let evidence_kind = parse_complaint_evidence_kind(&evidence_kind_text)?;
            let solver = row
                .try_get::<Option<Vec<u8>>, _>("assigned_solver")?
                .ok_or_else(|| invalid("assigned_solver", "missing"))?;
            let expiry_ms: i64 = row.try_get("proof_expires_at_ms")?;
            Ok(ComplaintResponse {
                order_id,
                status,
                evidence_kind,
                solver_id: Address::from(parse_fixed::<20>("assigned_solver", solver)?),
                proof_expires_at_secs: u64::try_from(expiry_ms / 1_000)
                    .map_err(|_| invalid("proof_expires_at_ms", expiry_ms))?,
                nullifier_spent: status == ComplaintStatus::Rejected,
                reason: row.try_get("reason")?,
                created_at_ms: row.try_get("created_at_ms")?,
                updated_at_ms: row.try_get("updated_at_ms")?,
            })
        })
        .transpose()
    }

    pub(crate) async fn set_complaint_legal_hold(
        &self,
        order_id: OrderId,
        held: bool,
        now_ms: i64,
    ) -> Result<bool, RepositoryError> {
        let result = sqlx::query(
            "UPDATE proof_order_complaints
             SET legal_hold = ?, updated_at_ms = ?
             WHERE order_id = ?",
        )
        .bind(held)
        .bind(now_ms)
        .bind(order_id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub(crate) async fn resolve_complaint(
        &self,
        order_id: OrderId,
        now_ms: i64,
        retention_seconds: u64,
    ) -> Result<bool, RepositoryError> {
        let retain_until_ms = now_ms.saturating_add(
            i64::try_from(retention_seconds)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000),
        );
        let mut transaction = self.pool.begin().await?;
        let complaint = sqlx::query(
            "UPDATE proof_order_complaints
             SET lifecycle_status = 'resolved', updated_at_ms = ?, retain_until_ms = ?
             WHERE order_id = ? AND lifecycle_status = 'verified'",
        )
        .bind(now_ms)
        .bind(retain_until_ms)
        .bind(order_id.to_string())
        .execute(&mut *transaction)
        .await?;
        if complaint.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(false);
        }
        let proof = sqlx::query(
            "UPDATE proof_orders
             SET state = 'closed', version = version + 1, updated_at_ms = ?
             WHERE order_id = ? AND state = 'complaint_verified'",
        )
        .bind(now_ms)
        .bind(order_id.to_string())
        .execute(&mut *transaction)
        .await?;
        if proof.rows_affected() != 1 {
            transaction.rollback().await?;
            return Err(RepositoryError::VersionConflict);
        }
        transaction.commit().await?;
        Ok(true)
    }

    pub(crate) async fn expire_with_core_transition(
        &self,
        order: &Order,
        expected_order_version: u64,
        now_ms: i64,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await?;
        update_core_order(&mut transaction, order, expected_order_version, now_ms).await?;
        sqlx::query(
            "UPDATE proof_orders
             SET state = 'expired', version = version + 1, updated_at_ms = ?
             WHERE order_id = ? AND state IN (
                'submitted', 'reservation_pending', 'assigned', 'proof_delivered',
                'proof_accepted', 'proof_rejected'
             )",
        )
        .bind(now_ms)
        .bind(order.id.to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}
