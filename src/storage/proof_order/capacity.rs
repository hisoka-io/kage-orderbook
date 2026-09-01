use std::collections::HashMap;

use super::{rows::*, *};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OutputLiquidityKey {
    pub solver_id: Address,
    pub chain_id: u64,
    pub token_out: Address,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CapacityUsage {
    pub processing_jobs: HashMap<Address, u64>,
    pub output_liquidity: HashMap<OutputLiquidityKey, U256>,
}

impl CapacityUsage {
    pub fn active_workload(&self, solver_id: Address) -> u64 {
        self.processing_jobs.get(&solver_id).copied().unwrap_or(0)
    }

    pub fn held_output_amount(
        &self,
        solver_id: Address,
        chain_id: u64,
        token_out: Address,
    ) -> U256 {
        self.output_liquidity
            .get(&OutputLiquidityKey {
                solver_id,
                chain_id,
                token_out,
            })
            .copied()
            .unwrap_or(U256::ZERO)
    }
}

impl ProofOrderRepository {
    pub async fn capacity_usage(&self, now_ms: i64) -> Result<CapacityUsage, RepositoryError> {
        let rows = sqlx::query(
            "SELECT solver_id, chain_id, token_out, amount_out, processing_job
             FROM (
                SELECT a.solver_id AS solver_id, p.chain_id AS chain_id,
                       p.token_out AS token_out, p.amount_out AS amount_out,
                       1 AS processing_job
                FROM proof_orders p
                JOIN proof_order_reservation_attempts a ON a.order_id = p.order_id
                WHERE p.state = 'reservation_pending'
                  AND a.outcome = 'pending'
                  AND a.deadline_ms > ?
                  AND p.proof_expires_at_ms > ?
                UNION ALL
                SELECT p.assigned_solver AS solver_id, p.chain_id AS chain_id,
                       p.token_out AS token_out, p.amount_out AS amount_out,
                       CASE WHEN p.state IN ('assigned', 'proof_delivered')
                            THEN 1 ELSE 0 END AS processing_job
                FROM proof_orders p
                WHERE p.assigned_solver IS NOT NULL
                  AND p.state IN ('assigned', 'proof_delivered', 'proof_accepted')
                  AND p.proof_expires_at_ms > ?
             )",
        )
        .bind(now_ms)
        .bind(now_ms)
        .bind(now_ms)
        .fetch_all(&self.pool)
        .await?;

        let mut usage = CapacityUsage::default();
        for row in rows {
            let solver_id =
                Address::from(parse_fixed::<20>("solver_id", row.try_get("solver_id")?)?);
            let chain_id = to_u64("chain_id", row.try_get("chain_id")?)?;
            let token_out =
                Address::from(parse_fixed::<20>("token_out", row.try_get("token_out")?)?);
            let amount_out = parse_u256("amount_out", row.try_get("amount_out")?)?;
            let processing_job: i64 = row.try_get("processing_job")?;

            if processing_job == 1 {
                let count = usage.processing_jobs.entry(solver_id).or_default();
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| invalid("processing_jobs", "overflow"))?;
            } else if processing_job != 0 {
                return Err(invalid("processing_job", processing_job));
            }

            let held = usage
                .output_liquidity
                .entry(OutputLiquidityKey {
                    solver_id,
                    chain_id,
                    token_out,
                })
                .or_default();
            *held = held
                .checked_add(amount_out)
                .ok_or_else(|| invalid("output_liquidity", "overflow"))?;
        }
        Ok(usage)
    }

    pub async fn held_output_amount(
        &self,
        solver_id: Address,
        chain_id: u64,
        token_out: Address,
        now_ms: i64,
    ) -> Result<U256, RepositoryError> {
        let chain_id = i64::try_from(chain_id).map_err(|_| invalid("chain_id", chain_id))?;
        let rows = sqlx::query(
            "SELECT amount_out FROM (
                SELECT p.amount_out AS amount_out
                FROM proof_orders p
                JOIN proof_order_reservation_attempts a ON a.order_id = p.order_id
                WHERE p.state = 'reservation_pending'
                  AND a.outcome = 'pending'
                  AND a.solver_id = ?
                  AND p.chain_id = ?
                  AND p.token_out = ?
                  AND a.deadline_ms > ?
                  AND p.proof_expires_at_ms > ?
                UNION ALL
                SELECT p.amount_out AS amount_out
                FROM proof_orders p
                WHERE p.assigned_solver = ?
                  AND p.state IN ('assigned', 'proof_delivered', 'proof_accepted')
                  AND p.chain_id = ?
                  AND p.token_out = ?
                  AND p.proof_expires_at_ms > ?
             )",
        )
        .bind(solver_id.as_slice())
        .bind(chain_id)
        .bind(token_out.as_slice())
        .bind(now_ms)
        .bind(now_ms)
        .bind(solver_id.as_slice())
        .bind(chain_id)
        .bind(token_out.as_slice())
        .bind(now_ms)
        .fetch_all(&self.pool)
        .await?;

        let mut held = U256::ZERO;
        for row in rows {
            let amount = parse_u256("amount_out", row.try_get("amount_out")?)?;
            held = held
                .checked_add(amount)
                .ok_or_else(|| invalid("output_liquidity", "overflow"))?;
        }
        Ok(held)
    }
}
