use super::{rows::*, *};

impl ProofOrderRepository {
    pub async fn insert_authoritative(
        &self,
        order: &Order,
        input: &NewProofOrder,
    ) -> Result<InsertOutcome, RepositoryError> {
        let request_digest = request_digest(input)?;
        let mut transaction = self.pool.begin().await?;
        if let Some(row) = sqlx::query(
            "SELECT order_id, request_digest FROM proof_orders
             WHERE order_id = ? OR access_token_hash = ?",
        )
        .bind(input.order_id.to_string())
        .bind(input.access_token_hash.as_slice())
        .fetch_optional(&mut *transaction)
        .await?
        {
            let existing_id: String = row.try_get("order_id")?;
            let existing_digest = parse_b256("request_digest", row.try_get("request_digest")?)?;
            transaction.rollback().await?;
            if existing_id == input.order_id.to_string() && existing_digest == request_digest {
                return Ok(InsertOutcome::Existing);
            }
            return Err(RepositoryError::IdempotencyConflict);
        }
        let conflicting_core: Option<String> =
            sqlx::query_scalar("SELECT id FROM orders WHERE id = ? LIMIT 1")
                .bind(input.order_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?;
        if conflicting_core.is_some() {
            transaction.rollback().await?;
            return Err(RepositoryError::IdempotencyConflict);
        }

        insert_core_order(&mut transaction, order, input.created_at_ms).await?;
        let terms_digest = exact_terms_digest(&input.terms, input.domain_hash);
        sqlx::query(
            "INSERT INTO proof_orders (
                order_id, access_token_hash, preview_id, category_id, state, version,
                chain_id, token_in, token_out, amount_in, amount_out, fee_bps,
                domain_hash, exact_terms_digest, settlement_commitment, ciphertext_digest,
                proof_expires_at_ms, request_digest, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, 'submitted', 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(input.order_id.to_string())
        .bind(input.access_token_hash.as_slice())
        .bind(input.preview_id.as_slice())
        .bind(&input.category_id)
        .bind(
            i64::try_from(input.terms.chain_id)
                .map_err(|_| invalid("chain_id", input.terms.chain_id))?,
        )
        .bind(input.terms.token_in.as_slice())
        .bind(input.terms.token_out.as_slice())
        .bind(input.terms.amount_in.to_string())
        .bind(input.terms.amount_out.to_string())
        .bind(i64::from(input.fee_bps))
        .bind(input.domain_hash.as_slice())
        .bind(terms_digest.as_slice())
        .bind(input.settlement_commitment.as_slice())
        .bind(input.proof.ciphertext_digest.as_slice())
        .bind(input.terms.expires_at_ms)
        .bind(request_digest.as_slice())
        .bind(input.created_at_ms)
        .bind(input.created_at_ms)
        .execute(&mut *transaction)
        .await?;

        let erase_after_ms = input.terms.expires_at_ms.saturating_add(
            i64::try_from(input.ciphertext_cleanup_grace_seconds)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000),
        );
        sqlx::query(
            "INSERT INTO proof_order_payloads (
                order_id, envelope_suite, nonce, ciphertext,
                ciphertext_digest, erase_after_ms
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(input.order_id.to_string())
        .bind(&input.proof.suite)
        .bind(&input.proof.nonce)
        .bind(&input.proof.ciphertext)
        .bind(input.proof.ciphertext_digest.as_slice())
        .bind(erase_after_ms)
        .execute(&mut *transaction)
        .await?;

        for (position, route) in input.candidates.iter().enumerate() {
            let recipient = input
                .proof
                .recipients
                .iter()
                .find(|recipient| {
                    recipient.solver_id == route.solver_id
                        && recipient.key_id == route.encryption_key_id
                })
                .ok_or_else(|| invalid("recipients", "candidate is missing its key wrap"))?;
            sqlx::query(
                "INSERT INTO proof_order_candidates (
                    order_id, position, solver_id, key_id, encapsulated_key, wrapped_key
                 ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(input.order_id.to_string())
            .bind(i64::try_from(position).map_err(|_| invalid("candidate_position", position))?)
            .bind(route.solver_id.as_slice())
            .bind(route.encryption_key_id.as_slice())
            .bind(&recipient.encapsulated_key)
            .bind(&recipient.wrapped_key)
            .execute(&mut *transaction)
            .await?;
        }
        let first = input
            .candidates
            .first()
            .ok_or_else(|| invalid("candidates", "empty"))?;
        insert_attempt(
            &mut transaction,
            input.order_id,
            0,
            first.solver_id,
            first.encryption_key_id,
            input.created_at_ms,
            input.reservation_attempt_timeout_ms,
        )
        .await?;
        let submitted = sqlx::query(
            "UPDATE proof_orders
             SET state = 'reservation_pending', version = 1, updated_at_ms = ?
             WHERE order_id = ? AND state = 'submitted' AND version = 0",
        )
        .bind(input.created_at_ms)
        .bind(input.order_id.to_string())
        .execute(&mut *transaction)
        .await?;
        if submitted.rows_affected() != 1 {
            transaction.rollback().await?;
            return Err(RepositoryError::VersionConflict);
        }
        transaction.commit().await?;
        Ok(InsertOutcome::Created)
    }

    pub async fn preflight_authoritative(
        &self,
        input: &NewProofOrder,
    ) -> Result<Option<InsertOutcome>, RepositoryError> {
        let request_digest = request_digest(input)?;
        let row = sqlx::query(
            "SELECT order_id, request_digest FROM proof_orders
             WHERE order_id = ? OR access_token_hash = ?",
        )
        .bind(input.order_id.to_string())
        .bind(input.access_token_hash.as_slice())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let existing_id: String = row.try_get("order_id")?;
        let existing_digest = parse_b256("request_digest", row.try_get("request_digest")?)?;
        if existing_id == input.order_id.to_string() && existing_digest == request_digest {
            Ok(Some(InsertOutcome::Existing))
        } else {
            Err(RepositoryError::IdempotencyConflict)
        }
    }

    /// Checks request-level idempotency before time-sensitive admission gates.
    /// This lets an exact retry recover the original response even after its
    /// preview or minimum-remaining window has elapsed.
    pub async fn preflight_create_request(
        &self,
        request: &CreateOrderRequest,
    ) -> Result<Option<InsertOutcome>, RepositoryError> {
        let request_digest = request_digest_fields(
            request.client_order_id,
            request.access_token_hash,
            request.preview_id,
            &request.category_id,
            &request.terms,
            request.domain_hash,
            request.settlement_commitment,
            &request.encrypted_proof,
        )?;
        let row = sqlx::query(
            "SELECT order_id, request_digest FROM proof_orders
             WHERE order_id = ? OR access_token_hash = ?",
        )
        .bind(request.client_order_id.to_string())
        .bind(request.access_token_hash.as_slice())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let existing_id: String = row.try_get("order_id")?;
        let existing_digest = parse_b256("request_digest", row.try_get("request_digest")?)?;
        if existing_id == request.client_order_id.to_string() && existing_digest == request_digest {
            Ok(Some(InsertOutcome::Existing))
        } else {
            Err(RepositoryError::IdempotencyConflict)
        }
    }

    pub async fn active_workload(
        &self,
        solver_id: Address,
        now_ms: i64,
    ) -> Result<u64, RepositoryError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM (
                SELECT order_id FROM proof_orders
                WHERE assigned_solver = ?
                  AND proof_expires_at_ms > ?
                  AND state IN ('assigned', 'proof_delivered', 'proof_accepted')
                UNION
                SELECT p.order_id
                FROM proof_orders p
                JOIN proof_order_reservation_attempts a ON a.order_id = p.order_id
                WHERE a.solver_id = ? AND a.outcome = 'pending'
                  AND p.state = 'reservation_pending'
                  AND p.proof_expires_at_ms > ?
             )",
        )
        .bind(solver_id.as_slice())
        .bind(now_ms)
        .bind(solver_id.as_slice())
        .bind(now_ms)
        .fetch_one(&self.pool)
        .await?;
        u64::try_from(count).map_err(|_| invalid("active_workload", count))
    }

    pub async fn authenticated_snapshot(
        &self,
        order_id: OrderId,
        access_token_hash: OrderAccessTokenHash,
    ) -> Result<Option<ProofOrderResponse>, RepositoryError> {
        let row = sqlx::query(
            "SELECT state, version, proof_expires_at_ms FROM proof_orders
             WHERE order_id = ? AND access_token_hash = ?",
        )
        .bind(order_id.to_string())
        .bind(access_token_hash.as_slice())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(ProofOrderResponse {
                order_id,
                state: parse_state(&row.try_get::<String, _>("state")?)?,
                version: to_u64("version", row.try_get("version")?)?,
                proof_expires_at_ms: row.try_get("proof_expires_at_ms")?,
            })
        })
        .transpose()
    }

    pub async fn terms(&self, order_id: OrderId) -> Result<Option<TradeTerms>, RepositoryError> {
        let row = sqlx::query(
            "SELECT chain_id, token_in, token_out, amount_in, amount_out,
                    proof_expires_at_ms FROM proof_orders WHERE order_id = ?",
        )
        .bind(order_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| terms_from_row(&row)).transpose()
    }

    pub async fn state(
        &self,
        order_id: OrderId,
    ) -> Result<Option<ProofOrderState>, RepositoryError> {
        let state =
            sqlx::query_scalar::<_, String>("SELECT state FROM proof_orders WHERE order_id = ?")
                .bind(order_id.to_string())
                .fetch_optional(&self.pool)
                .await?;
        state.map(|state| parse_state(&state)).transpose()
    }
}
