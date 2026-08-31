use super::{rows::*, *};

impl ProofOrderRepository {
    pub async fn targeted_order_ids(
        &self,
        solver_id: Address,
    ) -> Result<Vec<OrderId>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT a.order_id
             FROM proof_order_reservation_attempts a
             JOIN proof_orders p ON p.order_id = a.order_id
             WHERE a.solver_id = ? AND a.outcome = 'pending'
               AND p.state = 'reservation_pending'
             ORDER BY a.requested_at_ms, a.order_id",
        )
        .bind(solver_id.as_slice())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| parse_order_id(row.try_get("order_id")?))
            .collect()
    }

    pub async fn is_target(
        &self,
        order_id: OrderId,
        solver_id: Address,
    ) -> Result<bool, RepositoryError> {
        let found: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM proof_order_reservation_attempts a
             JOIN proof_orders p ON p.order_id = a.order_id
             WHERE a.order_id = ? AND a.solver_id = ? AND a.outcome = 'pending'
               AND p.state = 'reservation_pending'",
        )
        .bind(order_id.to_string())
        .bind(solver_id.as_slice())
        .fetch_optional(&self.pool)
        .await?;
        Ok(found.is_some())
    }

    pub async fn pending_reservations(
        &self,
        solver_id: Address,
    ) -> Result<Vec<PendingReservation>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT p.order_id, p.preview_id, p.category_id, p.chain_id,
                    p.token_in, p.token_out, p.amount_in, p.amount_out,
                    p.domain_hash, p.exact_terms_digest, p.fee_bps,
                    p.settlement_commitment, p.ciphertext_digest,
                    p.proof_expires_at_ms, a.key_id, a.attempt_nonce,
                    a.requested_at_ms, a.deadline_ms
             FROM proof_order_reservation_attempts a
             JOIN proof_orders p ON p.order_id = a.order_id
             WHERE a.solver_id = ? AND a.outcome = 'pending'
               AND p.state = 'reservation_pending'
             ORDER BY a.requested_at_ms, a.order_id",
        )
        .bind(solver_id.as_slice())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| pending_reservation_from_row(&row, solver_id))
            .collect()
    }

    pub async fn pending_reservation(
        &self,
        order_id: OrderId,
        solver_id: Address,
    ) -> Result<Option<PendingReservation>, RepositoryError> {
        let row = sqlx::query(
            "SELECT p.order_id, p.preview_id, p.category_id, p.chain_id,
                    p.token_in, p.token_out, p.amount_in, p.amount_out,
                    p.domain_hash, p.exact_terms_digest, p.fee_bps,
                    p.settlement_commitment, p.ciphertext_digest,
                    p.proof_expires_at_ms, a.key_id, a.attempt_nonce,
                    a.requested_at_ms, a.deadline_ms
             FROM proof_order_reservation_attempts a
             JOIN proof_orders p ON p.order_id = a.order_id
             WHERE a.order_id = ? AND a.solver_id = ? AND a.outcome = 'pending'
               AND p.state = 'reservation_pending'",
        )
        .bind(order_id.to_string())
        .bind(solver_id.as_slice())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| pending_reservation_from_row(&row, solver_id))
            .transpose()
    }

    pub async fn assigned_delivery(
        &self,
        order_id: OrderId,
        solver_id: Address,
    ) -> Result<Option<SolverProofDelivery>, RepositoryError> {
        let row = sqlx::query(
            "SELECT p.preview_id, p.category_id, p.domain_hash, p.fee_bps,
                    p.settlement_commitment, p.chain_id, p.token_in, p.token_out,
                    p.amount_in, p.amount_out, p.proof_expires_at_ms,
                    y.envelope_suite, y.nonce, y.ciphertext,
                    y.ciphertext_digest, c.key_id, c.encapsulated_key, c.wrapped_key,
                    a.assignment_ticket
             FROM proof_orders p
             JOIN proof_order_payloads y ON y.order_id = p.order_id
             JOIN proof_order_assignments a ON a.order_id = p.order_id
             JOIN proof_order_candidates c
               ON c.order_id = p.order_id AND c.solver_id = a.solver_id
             WHERE p.order_id = ? AND a.solver_id = ?
               AND p.state IN ('proof_delivered', 'proof_accepted', 'proof_rejected', 'expired', 'complaint_verified')",
        )
        .bind(order_id.to_string())
        .bind(solver_id.as_slice())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| delivery_from_row(&row, order_id, solver_id, terms_from_row(&row)?))
            .transpose()
    }

    pub async fn assigned_reservation_ack(
        &self,
        order_id: OrderId,
        solver_id: Address,
    ) -> Result<Option<ReservationAck>, RepositoryError> {
        let encoded: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT reservation_ack FROM proof_order_assignments
             WHERE order_id = ? AND solver_id = ?",
        )
        .bind(order_id.to_string())
        .bind(solver_id.as_slice())
        .fetch_optional(&self.pool)
        .await?
        .flatten();
        encoded
            .map(|encoded| {
                rmp_serde::from_slice(&encoded)
                    .map_err(|error| invalid("reservation_ack", error.to_string()))
            })
            .transpose()
    }

    pub async fn assign_and_disclose(
        &self,
        order: &Order,
        expected_order_version: u64,
        solver_id: Address,
        reservation_ack: &ReservationAck,
        ticket: &AssignmentTicket,
        now_ms: i64,
    ) -> Result<bool, RepositoryError> {
        let encoded_ticket = rmp_serde::to_vec_named(ticket)
            .map_err(|error| invalid("assignment_ticket", error.to_string()))?;
        let ticket_digest = assignment_ticket_digest(ticket);
        let encoded_ack = rmp_serde::to_vec_named(reservation_ack)
            .map_err(|error| invalid("reservation_ack", error.to_string()))?;
        let mut transaction = self.pool.begin().await?;
        let pending_row = sqlx::query(
            "SELECT p.order_id, p.preview_id, p.category_id, p.chain_id,
                    p.token_in, p.token_out, p.amount_in, p.amount_out,
                    p.domain_hash, p.exact_terms_digest, p.fee_bps,
                    p.settlement_commitment, p.ciphertext_digest,
                    p.proof_expires_at_ms, a.key_id, a.attempt_nonce,
                    a.requested_at_ms, a.deadline_ms
             FROM proof_order_reservation_attempts a
             JOIN proof_orders p ON p.order_id = a.order_id
             WHERE a.order_id = ? AND a.solver_id = ? AND a.outcome = 'pending'
               AND a.deadline_ms > ? AND p.state = 'reservation_pending'",
        )
        .bind(order.id.to_string())
        .bind(solver_id.as_slice())
        .bind(now_ms)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(pending_row) = pending_row else {
            transaction.rollback().await?;
            return Ok(false);
        };
        let pending = pending_reservation_from_row(&pending_row, solver_id)?;
        let key_id = pending.key_id;
        if reservation_ack.claims.bindings != pending.claims.bindings
            || reservation_ack.claims.attempt_nonce != pending.claims.attempt_nonce
            || ticket.claims.bindings != pending.claims.bindings
            || ticket.claims.settlement_commitment != pending.settlement_commitment
            || ticket.claims.proof_encryption_key_id != key_id
            || ticket.claims.issued_at_ms > now_ms
            || ticket.claims.expires_at_ms <= now_ms
            || ticket.claims.expires_at_ms != pending.terms.expires_at_ms
        {
            transaction.rollback().await?;
            return Err(invalid(
                "reservation_evidence",
                "immutable bindings do not match",
            ));
        }
        update_core_order(&mut transaction, order, expected_order_version, now_ms).await?;
        let assigned = sqlx::query(
            "UPDATE proof_orders
             SET state = 'assigned', version = version + 1,
                 assigned_solver = ?, assigned_key_id = ?, updated_at_ms = ?
             WHERE order_id = ? AND state = 'reservation_pending'",
        )
        .bind(solver_id.as_slice())
        .bind(key_id.as_slice())
        .bind(now_ms)
        .bind(order.id.to_string())
        .execute(&mut *transaction)
        .await?;
        if assigned.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(false);
        }
        sqlx::query(
            "UPDATE proof_order_reservation_attempts
             SET outcome = 'accepted', reservation_ack = ?, responded_at_ms = ?
             WHERE order_id = ? AND solver_id = ? AND outcome = 'pending'",
        )
        .bind(&encoded_ack)
        .bind(now_ms)
        .bind(order.id.to_string())
        .bind(solver_id.as_slice())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO proof_order_assignments (
                order_id, solver_id, key_id, assignment_ticket,
                assignment_ticket_digest, reservation_ack, assigned_at_ms, disclosed_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(order.id.to_string())
        .bind(solver_id.as_slice())
        .bind(key_id.as_slice())
        .bind(encoded_ticket)
        .bind(ticket_digest.as_slice())
        .bind(encoded_ack)
        .bind(now_ms)
        .bind(now_ms)
        .execute(&mut *transaction)
        .await?;
        sqlx::query("UPDATE proof_order_payloads SET disclosed_at_ms = ? WHERE order_id = ?")
            .bind(now_ms)
            .bind(order.id.to_string())
            .execute(&mut *transaction)
            .await?;
        let disclosed = sqlx::query(
            "UPDATE proof_orders
             SET state = 'proof_delivered', version = version + 1, updated_at_ms = ?
             WHERE order_id = ? AND state = 'assigned'",
        )
        .bind(now_ms)
        .bind(order.id.to_string())
        .execute(&mut *transaction)
        .await?;
        if disclosed.rows_affected() != 1 {
            transaction.rollback().await?;
            return Err(RepositoryError::VersionConflict);
        }
        sqlx::query("DELETE FROM proof_order_candidates WHERE order_id = ? AND solver_id != ?")
            .bind(order.id.to_string())
            .bind(solver_id.as_slice())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn decline_and_advance(
        &self,
        order_id: OrderId,
        solver_id: Address,
        signed_decline: Option<&[u8]>,
        now_ms: i64,
        reservation_attempt_timeout_ms: u64,
        minimum_remaining_seconds: u32,
    ) -> Result<Option<AdvanceOutcome>, RepositoryError> {
        self.finish_attempt_and_advance(
            order_id,
            solver_id,
            "declined",
            signed_decline,
            now_ms,
            reservation_attempt_timeout_ms,
            minimum_remaining_seconds,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_attempt_and_advance(
        &self,
        order_id: OrderId,
        solver_id: Address,
        outcome: &str,
        signed_decline: Option<&[u8]>,
        now_ms: i64,
        reservation_attempt_timeout_ms: u64,
        minimum_remaining_seconds: u32,
    ) -> Result<Option<AdvanceOutcome>, RepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let current = sqlx::query(
            "SELECT a.attempt_number, p.proof_expires_at_ms
             FROM proof_order_reservation_attempts a
             JOIN proof_orders p ON p.order_id = a.order_id
             WHERE a.order_id = ? AND a.solver_id = ? AND a.outcome = 'pending'
               AND p.state = 'reservation_pending'",
        )
        .bind(order_id.to_string())
        .bind(solver_id.as_slice())
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(current) = current else {
            transaction.rollback().await?;
            return Ok(None);
        };
        let attempt_number: i64 = current.try_get("attempt_number")?;
        let proof_expires_at_ms: i64 = current.try_get("proof_expires_at_ms")?;
        sqlx::query(
            "UPDATE proof_order_reservation_attempts
             SET outcome = ?, signed_decline = ?, responded_at_ms = ?
             WHERE order_id = ? AND solver_id = ? AND outcome = 'pending'",
        )
        .bind(outcome)
        .bind(signed_decline)
        .bind(now_ms)
        .bind(order_id.to_string())
        .bind(solver_id.as_slice())
        .execute(&mut *transaction)
        .await?;
        let next = sqlx::query(
            "SELECT c.solver_id, c.key_id
             FROM proof_order_candidates c
             WHERE c.order_id = ? AND NOT EXISTS (
                 SELECT 1 FROM proof_order_reservation_attempts a
                 WHERE a.order_id = c.order_id AND a.solver_id = c.solver_id
             )
             ORDER BY c.position LIMIT 1",
        )
        .bind(order_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let minimum_remaining_ms = i64::from(minimum_remaining_seconds).saturating_mul(1_000);
        let has_routing_time = proof_expires_at_ms.saturating_sub(now_ms) > minimum_remaining_ms;
        let result = if let Some(next) = next.filter(|_| has_routing_time) {
            let next_solver =
                Address::from(parse_fixed::<20>("solver_id", next.try_get("solver_id")?)?);
            let key_id = parse_b256("key_id", next.try_get("key_id")?)?;
            insert_attempt(
                &mut transaction,
                order_id,
                attempt_number.saturating_add(1),
                next_solver,
                key_id,
                now_ms,
                reservation_attempt_timeout_ms,
            )
            .await?;
            sqlx::query(
                "UPDATE proof_orders SET version = version + 1, updated_at_ms = ?
                 WHERE order_id = ? AND state = 'reservation_pending'",
            )
            .bind(now_ms)
            .bind(order_id.to_string())
            .execute(&mut *transaction)
            .await?;
            AdvanceOutcome::Advanced(next_solver)
        } else {
            let core = sqlx::query(
                "UPDATE orders
                 SET state = 'expired', version = version + 1, updated_at_ms = ?
                 WHERE id = ? AND state = 'reservation_pending'",
            )
            .bind(now_ms)
            .bind(order_id.to_string())
            .execute(&mut *transaction)
            .await?;
            if core.rows_affected() != 1 {
                transaction.rollback().await?;
                return Err(RepositoryError::VersionConflict);
            }
            let updated = sqlx::query(
                "UPDATE proof_orders
                 SET state = 'closed', version = version + 1, updated_at_ms = ?
                 WHERE order_id = ? AND state = 'reservation_pending'",
            )
            .bind(now_ms)
            .bind(order_id.to_string())
            .execute(&mut *transaction)
            .await?;
            if updated.rows_affected() != 1 {
                transaction.rollback().await?;
                return Ok(None);
            }
            AdvanceOutcome::Exhausted
        };
        transaction.commit().await?;
        Ok(Some(result))
    }

    pub async fn expire_due_attempts(
        &self,
        now_ms: i64,
        reservation_attempt_timeout_ms: u64,
        minimum_remaining_seconds: u32,
    ) -> Result<Vec<(OrderId, AdvanceOutcome)>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT a.order_id, a.solver_id
             FROM proof_order_reservation_attempts a
             JOIN proof_orders p ON p.order_id = a.order_id
             WHERE a.outcome = 'pending' AND a.deadline_ms <= ?
               AND p.state = 'reservation_pending'
             ORDER BY a.deadline_ms, a.id",
        )
        .bind(now_ms)
        .fetch_all(&self.pool)
        .await?;
        let mut outcomes = Vec::with_capacity(rows.len());
        for row in rows {
            let order_id = parse_order_id(row.try_get("order_id")?)?;
            let solver_id =
                Address::from(parse_fixed::<20>("solver_id", row.try_get("solver_id")?)?);
            if let Some(outcome) = self
                .finish_attempt_and_advance(
                    order_id,
                    solver_id,
                    "timed_out",
                    None,
                    now_ms,
                    reservation_attempt_timeout_ms,
                    minimum_remaining_seconds,
                )
                .await?
            {
                outcomes.push((order_id, outcome));
            }
        }
        Ok(outcomes)
    }
}
