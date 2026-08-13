use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::{Address, B256, U256};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{Row, SqlitePool};
use thiserror::Error;
use uuid::Uuid;

use crate::order::{Order, OrderId, OrderState, SolverId};

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("order version conflict")]
    VersionConflict,
    #[error("invalid stored {field}: {value}")]
    InvalidData { field: &'static str, value: String },
}

#[derive(Debug, Clone)]
pub struct PersistedOrder {
    pub order: Order,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub expires_at_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ProofPayload {
    pub order_id: OrderId,
    pub solver_id: SolverId,
    pub ciphertext: Vec<u8>,
    pub created_at_ms: i64,
    pub delivered_at_ms: Option<i64>,
}

#[derive(Clone)]
pub struct OrderRepository {
    pool: SqlitePool,
}

impl OrderRepository {
    pub async fn connect(database_url: &str) -> Result<Self, RepositoryError> {
        let options = SqliteConnectOptions::from_str(database_url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        let repository = Self { pool };
        repository.migrate().await?;
        Ok(repository)
    }

    pub async fn migrate(&self) -> Result<(), RepositoryError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub async fn insert_order(
        &self,
        order: &Order,
        created_at_ms: i64,
        expires_at_ms: Option<i64>,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            "INSERT INTO orders (
                id, state, version, token_in, token_out, amount_in, amount_out,
                solver_id, solver_noise_public_key, tx_hash,
                created_at_ms, updated_at_ms, expires_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(order.id.to_string())
        .bind(state_name(order.state))
        .bind(version_to_i64(order.version)?)
        .bind(order.token_in.as_slice())
        .bind(order.token_out.as_slice())
        .bind(order.amount_in.to_string())
        .bind(order.amount_out.to_string())
        .bind(order.solver.map(|id| id.to_string()))
        .bind(order.solver_noise_public_key.as_deref())
        .bind(order.tx_hash.map(|hash| hash.to_vec()))
        .bind(created_at_ms)
        .bind(created_at_ms)
        .bind(expires_at_ms)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_order(
        &self,
        order: &Order,
        expected_version: u64,
        updated_at_ms: i64,
    ) -> Result<(), RepositoryError> {
        let result = sqlx::query(
            "UPDATE orders SET
                state = ?, version = ?, token_in = ?, token_out = ?,
                amount_in = ?, amount_out = ?, solver_id = ?,
                solver_noise_public_key = ?, tx_hash = ?, updated_at_ms = ?
             WHERE id = ? AND version = ?",
        )
        .bind(state_name(order.state))
        .bind(version_to_i64(order.version)?)
        .bind(order.token_in.as_slice())
        .bind(order.token_out.as_slice())
        .bind(order.amount_in.to_string())
        .bind(order.amount_out.to_string())
        .bind(order.solver.map(|id| id.to_string()))
        .bind(order.solver_noise_public_key.as_deref())
        .bind(order.tx_hash.map(|hash| hash.to_vec()))
        .bind(updated_at_ms)
        .bind(order.id.to_string())
        .bind(version_to_i64(expected_version)?)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() != 1 {
            return Err(RepositoryError::VersionConflict);
        }
        Ok(())
    }

    pub async fn persist_transition(
        &self,
        order: &Order,
        expected_version: u64,
        timestamp_ms: i64,
        proof: Option<(SolverId, &[u8])>,
        delete_proof: bool,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE orders SET
                state = ?, version = ?, token_in = ?, token_out = ?,
                amount_in = ?, amount_out = ?, solver_id = ?,
                solver_noise_public_key = ?, tx_hash = ?, updated_at_ms = ?
             WHERE id = ? AND version = ?",
        )
        .bind(state_name(order.state))
        .bind(version_to_i64(order.version)?)
        .bind(order.token_in.as_slice())
        .bind(order.token_out.as_slice())
        .bind(order.amount_in.to_string())
        .bind(order.amount_out.to_string())
        .bind(order.solver.map(|id| id.to_string()))
        .bind(order.solver_noise_public_key.as_deref())
        .bind(order.tx_hash.map(|hash| hash.to_vec()))
        .bind(timestamp_ms)
        .bind(order.id.to_string())
        .bind(version_to_i64(expected_version)?)
        .execute(&mut *transaction)
        .await?;

        if result.rows_affected() != 1 {
            return Err(RepositoryError::VersionConflict);
        }

        if let Some((solver_id, ciphertext)) = proof {
            sqlx::query(
                "INSERT INTO proof_payloads (
                    order_id, solver_id, ciphertext, created_at_ms, delivered_at_ms
                 ) VALUES (?, ?, ?, ?, NULL)
                 ON CONFLICT(order_id) DO UPDATE SET
                    solver_id = excluded.solver_id,
                    ciphertext = excluded.ciphertext,
                    created_at_ms = excluded.created_at_ms,
                    delivered_at_ms = NULL",
            )
            .bind(order.id.to_string())
            .bind(solver_id.to_string())
            .bind(ciphertext)
            .bind(timestamp_ms)
            .execute(&mut *transaction)
            .await?;
        }

        if delete_proof {
            sqlx::query("DELETE FROM proof_payloads WHERE order_id = ?")
                .bind(order.id.to_string())
                .execute(&mut *transaction)
                .await?;
        }

        transaction.commit().await?;
        Ok(())
    }

    pub async fn get_order(
        &self,
        order_id: OrderId,
    ) -> Result<Option<PersistedOrder>, RepositoryError> {
        let row = sqlx::query("SELECT * FROM orders WHERE id = ?")
            .bind(order_id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(decode_order).transpose()
    }

    pub async fn get_proof(
        &self,
        order_id: OrderId,
    ) -> Result<Option<ProofPayload>, RepositoryError> {
        let row = sqlx::query(
            "SELECT order_id, solver_id, ciphertext, created_at_ms, delivered_at_ms
             FROM proof_payloads WHERE order_id = ?",
        )
        .bind(order_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(decode_proof).transpose()
    }

    pub async fn load_non_terminal_orders(&self) -> Result<Vec<PersistedOrder>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT * FROM orders
             WHERE state NOT IN ('filled', 'expired', 'cancelled', 'failed')
             ORDER BY created_at_ms, id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(decode_order).collect()
    }

    pub async fn store_proof(
        &self,
        order_id: OrderId,
        solver_id: SolverId,
        ciphertext: &[u8],
        created_at_ms: i64,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            "INSERT INTO proof_payloads (
                order_id, solver_id, ciphertext, created_at_ms, delivered_at_ms
             ) VALUES (?, ?, ?, ?, NULL)
             ON CONFLICT(order_id) DO UPDATE SET
                solver_id = excluded.solver_id,
                ciphertext = excluded.ciphertext,
                created_at_ms = excluded.created_at_ms,
                delivered_at_ms = NULL",
        )
        .bind(order_id.to_string())
        .bind(solver_id.to_string())
        .bind(ciphertext)
        .bind(created_at_ms)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn load_solver_proofs(
        &self,
        solver_id: SolverId,
    ) -> Result<Vec<ProofPayload>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT proof_payloads.order_id, proof_payloads.solver_id,
                    proof_payloads.ciphertext, proof_payloads.created_at_ms,
                    proof_payloads.delivered_at_ms
             FROM proof_payloads
             JOIN orders ON orders.id = proof_payloads.order_id
             WHERE proof_payloads.solver_id = ? AND orders.state = 'proof_relayed'
             ORDER BY proof_payloads.created_at_ms, proof_payloads.order_id",
        )
        .bind(solver_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(decode_proof).collect()
    }

    pub async fn mark_proof_delivered(
        &self,
        order_id: OrderId,
        delivered_at_ms: i64,
    ) -> Result<(), RepositoryError> {
        sqlx::query("UPDATE proof_payloads SET delivered_at_ms = ? WHERE order_id = ?")
            .bind(delivered_at_ms)
            .bind(order_id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_proof(&self, order_id: OrderId) -> Result<(), RepositoryError> {
        sqlx::query("DELETE FROM proof_payloads WHERE order_id = ?")
            .bind(order_id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

fn decode_order(row: SqliteRow) -> Result<PersistedOrder, RepositoryError> {
    let id = parse_uuid("id", row.try_get::<String, _>("id")?)?;
    let state_value = row.try_get::<String, _>("state")?;
    let state = parse_state(&state_value)?;
    let version = i64_to_u64("version", row.try_get("version")?)?;
    let token_in = parse_fixed::<20>("token_in", row.try_get("token_in")?)?;
    let token_out = parse_fixed::<20>("token_out", row.try_get("token_out")?)?;
    let amount_in = parse_u256("amount_in", row.try_get("amount_in")?)?;
    let amount_out = parse_u256("amount_out", row.try_get("amount_out")?)?;
    let solver = row
        .try_get::<Option<String>, _>("solver_id")?
        .map(|value| parse_uuid("solver_id", value))
        .transpose()?;
    let noise_public_key = row.try_get("solver_noise_public_key")?;
    let tx_hash = row
        .try_get::<Option<Vec<u8>>, _>("tx_hash")?
        .map(|value| parse_fixed::<32>("tx_hash", value).map(B256::from))
        .transpose()?;

    Ok(PersistedOrder {
        order: Order {
            id,
            state,
            version,
            token_in: Address::from(token_in),
            token_out: Address::from(token_out),
            amount_in,
            amount_out,
            solver,
            solver_noise_public_key: noise_public_key,
            tx_hash,
        },
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
        expires_at_ms: row.try_get("expires_at_ms")?,
    })
}

fn decode_proof(row: SqliteRow) -> Result<ProofPayload, RepositoryError> {
    Ok(ProofPayload {
        order_id: parse_uuid("order_id", row.try_get("order_id")?)?,
        solver_id: parse_uuid("solver_id", row.try_get("solver_id")?)?,
        ciphertext: row.try_get("ciphertext")?,
        created_at_ms: row.try_get("created_at_ms")?,
        delivered_at_ms: row.try_get("delivered_at_ms")?,
    })
}

fn state_name(state: OrderState) -> &'static str {
    match state {
        OrderState::Created => "created",
        OrderState::Validated => "validated",
        OrderState::Reserving => "reserving",
        OrderState::Assigned => "assigned",
        OrderState::AwaitingUserProof => "awaiting_user_proof",
        OrderState::ProofRelayed => "proof_relayed",
        OrderState::Executing => "executing",
        OrderState::Filled => "filled",
        OrderState::Expired => "expired",
        OrderState::Cancelled => "cancelled",
        OrderState::Failed => "failed",
    }
}

fn parse_state(value: &str) -> Result<OrderState, RepositoryError> {
    match value {
        "created" => Ok(OrderState::Created),
        "validated" => Ok(OrderState::Validated),
        "reserving" => Ok(OrderState::Reserving),
        "assigned" => Ok(OrderState::Assigned),
        "awaiting_user_proof" => Ok(OrderState::AwaitingUserProof),
        "proof_relayed" => Ok(OrderState::ProofRelayed),
        "executing" => Ok(OrderState::Executing),
        "filled" => Ok(OrderState::Filled),
        "expired" => Ok(OrderState::Expired),
        "cancelled" => Ok(OrderState::Cancelled),
        "failed" => Ok(OrderState::Failed),
        _ => Err(invalid("state", value)),
    }
}

fn parse_uuid(field: &'static str, value: String) -> Result<Uuid, RepositoryError> {
    Uuid::parse_str(&value).map_err(|_| invalid(field, value))
}

fn parse_u256(field: &'static str, value: String) -> Result<U256, RepositoryError> {
    U256::from_str(&value).map_err(|_| invalid(field, value))
}

fn parse_fixed<const N: usize>(
    field: &'static str,
    value: Vec<u8>,
) -> Result<[u8; N], RepositoryError> {
    value
        .try_into()
        .map_err(|value: Vec<u8>| invalid(field, format!("{} bytes", value.len())))
}

fn version_to_i64(version: u64) -> Result<i64, RepositoryError> {
    i64::try_from(version).map_err(|_| invalid("version", version.to_string()))
}

fn i64_to_u64(field: &'static str, value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| invalid(field, value.to_string()))
}

fn invalid(field: &'static str, value: impl Into<String>) -> RepositoryError {
    RepositoryError::InvalidData {
        field,
        value: value.into(),
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, U256};
    use uuid::Uuid;

    use super::*;

    fn order(state: OrderState, version: u64) -> Order {
        Order {
            id: Uuid::new_v4(),
            state,
            version,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
            amount_in: U256::from(100),
            amount_out: U256::from(200),
            solver: Some(Uuid::new_v4()),
            solver_noise_public_key: Some(vec![7; 32]),
            tx_hash: Some(B256::repeat_byte(9)),
        }
    }

    async fn repository() -> OrderRepository {
        OrderRepository::connect("sqlite::memory:").await.unwrap()
    }

    #[tokio::test]
    async fn stores_and_loads_an_order() {
        let repository = repository().await;
        let order = order(OrderState::Executing, 7);
        repository
            .insert_order(&order, 1000, Some(5000))
            .await
            .unwrap();

        let stored = repository.get_order(order.id).await.unwrap().unwrap();
        assert_eq!(stored.order.id, order.id);
        assert_eq!(stored.order.state, OrderState::Executing);
        assert_eq!(stored.order.version, 7);
        assert_eq!(stored.order.amount_in, order.amount_in);
        assert_eq!(stored.order.solver, order.solver);
        assert_eq!(stored.order.tx_hash, order.tx_hash);
        assert_eq!(stored.created_at_ms, 1000);
        assert_eq!(stored.updated_at_ms, 1000);
        assert_eq!(stored.expires_at_ms, Some(5000));
    }

    #[tokio::test]
    async fn rejects_a_stale_order_update() {
        let repository = repository().await;
        let mut order = order(OrderState::Reserving, 3);
        repository.insert_order(&order, 1000, None).await.unwrap();

        order.state = OrderState::AwaitingUserProof;
        order.version = 4;
        repository.update_order(&order, 3, 2000).await.unwrap();

        let error = repository.update_order(&order, 3, 3000).await.unwrap_err();
        assert!(matches!(error, RepositoryError::VersionConflict));
    }

    #[tokio::test]
    async fn loads_only_non_terminal_orders() {
        let repository = repository().await;
        let active = order(OrderState::ProofRelayed, 5);
        let filled = order(OrderState::Filled, 8);
        repository.insert_order(&active, 1000, None).await.unwrap();
        repository.insert_order(&filled, 1001, None).await.unwrap();

        let orders = repository.load_non_terminal_orders().await.unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order.id, active.id);
    }

    #[tokio::test]
    async fn proof_fetch_is_non_destructive() {
        let repository = repository().await;
        let order = order(OrderState::ProofRelayed, 5);
        let solver_id = order.solver.unwrap();
        repository.insert_order(&order, 1000, None).await.unwrap();
        repository
            .store_proof(order.id, solver_id, &[1, 2, 3], 1100)
            .await
            .unwrap();

        let first = repository.load_solver_proofs(solver_id).await.unwrap();
        let second = repository.load_solver_proofs(solver_id).await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(first[0].ciphertext, vec![1, 2, 3]);

        repository
            .mark_proof_delivered(order.id, 1200)
            .await
            .unwrap();
        let delivered = repository.load_solver_proofs(solver_id).await.unwrap();
        assert_eq!(delivered[0].delivered_at_ms, Some(1200));
    }
}
