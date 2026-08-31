use super::*;

impl ProofOrderRepository {
    pub async fn cleanup(
        &self,
        now_ms: i64,
        evidence_retention_seconds: u64,
    ) -> Result<CleanupOutcome, RepositoryError> {
        let result = self.cleanup_once(now_ms, evidence_retention_seconds).await;
        self.retention_metrics.record(&result);
        result
    }

    async fn cleanup_once(
        &self,
        now_ms: i64,
        evidence_retention_seconds: u64,
    ) -> Result<CleanupOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let payloads = sqlx::query("DELETE FROM proof_order_payloads WHERE erase_after_ms <= ?")
            .bind(now_ms)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
        let retired_complaint_orders = sqlx::query_scalar::<_, String>(
            "SELECT order_id FROM proof_order_complaints
             WHERE lifecycle_status IN ('rejected', 'resolved')
               AND legal_hold = 0 AND retain_until_ms <= ?
             ORDER BY order_id",
        )
        .bind(now_ms)
        .fetch_all(&mut *transaction)
        .await?;
        let complaints = sqlx::query(
            "DELETE FROM proof_order_complaints
             WHERE lifecycle_status IN ('rejected', 'resolved')
               AND legal_hold = 0 AND retain_until_ms <= ?",
        )
        .bind(now_ms)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let retention_ms = i64::try_from(evidence_retention_seconds)
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000);
        let cutoff = now_ms.saturating_sub(retention_ms);
        let mut removable = sqlx::query_scalar::<_, String>(
            "SELECT order_id FROM proof_orders
             WHERE state IN ('expired', 'closed') AND updated_at_ms < ?
               AND NOT EXISTS (
                 SELECT 1 FROM proof_order_complaints c
                 WHERE c.order_id = proof_orders.order_id
               )
             ORDER BY order_id",
        )
        .bind(cutoff)
        .fetch_all(&mut *transaction)
        .await?;
        removable.extend(retired_complaint_orders);
        removable.sort_unstable();
        removable.dedup();
        let mut orders = 0;
        for order_id in removable {
            let proof = sqlx::query("DELETE FROM proof_orders WHERE order_id = ?")
                .bind(&order_id)
                .execute(&mut *transaction)
                .await?;
            if proof.rows_affected() != 1 {
                continue;
            }
            let core = sqlx::query("DELETE FROM orders WHERE id = ?")
                .bind(&order_id)
                .execute(&mut *transaction)
                .await?;
            if core.rows_affected() != 1 {
                transaction.rollback().await?;
                return Err(RepositoryError::VersionConflict);
            }
            orders += 1;
        }
        transaction.commit().await?;
        Ok(CleanupOutcome {
            payloads_erased: payloads,
            complaints_erased: complaints,
            orders_erased: orders,
        })
    }
}
