use std::{str::FromStr, time::Duration};

use alloy_primitives::{Address, B256, U256};
use sqlx::{
    Row, SqlitePool,
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
    },
};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    config::Network,
    order::{Order, OrderCommitment, OrderId, OrderState, SolverId},
};

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("order version conflict")]
    VersionConflict,
    #[error("order commitment already exists")]
    DuplicateOrderCommitment,
    #[error("invalid stored {field}: {value}")]
    InvalidData { field: &'static str, value: String },
    #[error("database belongs to {found}, refusing to open it as {expected}")]
    NetworkMismatch { found: String, expected: Network },
}

#[derive(Debug, Clone)]
pub(crate) struct PersistedOrder {
    pub order: Order,
}

#[derive(Debug, Clone)]
pub(crate) struct ProofPayload {
    pub order_id: OrderId,
    pub ciphertext: Vec<u8>,
}

#[derive(Clone)]
pub struct OrderRepository {
    pool: SqlitePool,
}

impl OrderRepository {
    pub async fn connect(database_url: &str) -> Result<Self, RepositoryError> {
        Self::connect_with_options(database_url, Duration::from_secs(5), 1).await
    }

    pub async fn connect_with_options(
        database_url: &str,
        busy_timeout: Duration,
        max_connections: u32,
    ) -> Result<Self, RepositoryError> {
        let options = SqliteConnectOptions::from_str(database_url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(busy_timeout);
        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_with(options)
            .await?;
        let repository = Self { pool };
        repository.migrate().await?;
        Ok(repository)
    }

    pub async fn bind_network(&self, network: Network) -> Result<(), RepositoryError> {
        let found: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&self.pool)
            .await?;
        if found == network.stamp() {
            return Ok(());
        }
        if found != 0 {
            return Err(RepositoryError::NetworkMismatch {
                found: Network::from_stamp(found)
                    .map_or_else(|| format!("unknown stamp {found}"), |net| net.to_string()),
                expected: network,
            });
        }
        // PRAGMA does not accept bound parameters.
        sqlx::query(&format!("PRAGMA user_version = {}", network.stamp()))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn migrate(&self) -> Result<(), RepositoryError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub(crate) async fn insert_order(
        &self,
        order: &Order,
        order_commitment: OrderCommitment,
        created_at_ms: i64,
    ) -> Result<(), RepositoryError> {
        let result = sqlx::query(
            "INSERT INTO orders (
                id, chain_id, state, version, token_in, token_out, amount_in, amount_out,
                solver_noise_public_key, tx_hash, created_at_ms, updated_at_ms,
                expires_at_ms, order_commitment, solver_address
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(order.id.to_string())
        .bind(u64_to_i64("chain_id", order.chain_id)?)
        .bind(state_name(order.state))
        .bind(version_to_i64(order.version)?)
        .bind(order.token_in.as_slice())
        .bind(order.token_out.as_slice())
        .bind(order.amount_in.to_string())
        .bind(order.amount_out.to_string())
        .bind(order.solver_noise_public_key.as_deref())
        .bind(order.tx_hash.map(|hash| hash.to_vec()))
        .bind(created_at_ms)
        .bind(created_at_ms)
        .bind(order.expires_at_ms)
        .bind(order_commitment.as_slice())
        .bind(order.solver.map(|address| address.to_vec()))
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => {}
            Err(sqlx::Error::Database(error))
                if error.is_unique_violation()
                    && error.message().contains("orders.order_commitment") =>
            {
                return Err(RepositoryError::DuplicateOrderCommitment);
            }
            Err(error) => return Err(RepositoryError::Database(error)),
        }
        Ok(())
    }

    pub(crate) async fn persist_transition(
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
                amount_in = ?, amount_out = ?, solver_address = ?,
                solver_noise_public_key = ?, tx_hash = ?, updated_at_ms = ?
             WHERE id = ? AND version = ?",
        )
        .bind(state_name(order.state))
        .bind(version_to_i64(order.version)?)
        .bind(order.token_in.as_slice())
        .bind(order.token_out.as_slice())
        .bind(order.amount_in.to_string())
        .bind(order.amount_out.to_string())
        .bind(order.solver.map(|address| address.to_vec()))
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
                    order_id, solver_address, ciphertext, created_at_ms, delivered_at_ms
                 ) VALUES (?, ?, ?, ?, NULL)
                 ON CONFLICT(order_id) DO UPDATE SET
                    solver_address = excluded.solver_address,
                    ciphertext = excluded.ciphertext,
                    created_at_ms = excluded.created_at_ms,
                    delivered_at_ms = NULL",
            )
            .bind(order.id.to_string())
            .bind(solver_id.as_slice())
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

    pub(crate) async fn get_order(
        &self,
        order_id: OrderId,
    ) -> Result<Option<PersistedOrder>, RepositoryError> {
        let row = sqlx::query("SELECT * FROM orders WHERE id = ?")
            .bind(order_id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(decode_order).transpose()
    }

    pub(crate) async fn find_order_by_commitment(
        &self,
        order_commitment: OrderCommitment,
    ) -> Result<Option<PersistedOrder>, RepositoryError> {
        let row = sqlx::query(
            "SELECT * FROM orders
             WHERE order_commitment = ?",
        )
        .bind(order_commitment.as_slice())
        .fetch_optional(&self.pool)
        .await?;
        row.map(decode_order).transpose()
    }

    pub(crate) async fn get_order_by_commitment(
        &self,
        order_id: OrderId,
        order_commitment: OrderCommitment,
    ) -> Result<Option<PersistedOrder>, RepositoryError> {
        let row = sqlx::query(
            "SELECT * FROM orders
             WHERE id = ? AND order_commitment = ?",
        )
        .bind(order_id.to_string())
        .bind(order_commitment.as_slice())
        .fetch_optional(&self.pool)
        .await?;
        row.map(decode_order).transpose()
    }

    pub(crate) async fn get_proof(
        &self,
        order_id: OrderId,
    ) -> Result<Option<ProofPayload>, RepositoryError> {
        let row = sqlx::query(
            "SELECT order_id, ciphertext
             FROM proof_payloads WHERE order_id = ?",
        )
        .bind(order_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(decode_proof).transpose()
    }

    pub(crate) async fn load_non_terminal_orders(
        &self,
    ) -> Result<Vec<PersistedOrder>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT * FROM orders
             WHERE state NOT IN ('filled', 'expired', 'cancelled', 'failed')
             ORDER BY created_at_ms, id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(decode_order).collect()
    }

    pub(crate) async fn load_solver_proofs(
        &self,
        solver_id: SolverId,
    ) -> Result<Vec<ProofPayload>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT proof_payloads.order_id, proof_payloads.ciphertext
             FROM proof_payloads
             JOIN orders ON orders.id = proof_payloads.order_id
             WHERE proof_payloads.solver_address = ? AND orders.state = 'proof_relayed'
             ORDER BY proof_payloads.created_at_ms, proof_payloads.order_id",
        )
        .bind(solver_id.as_slice())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(decode_proof).collect()
    }
}

fn decode_order(row: SqliteRow) -> Result<PersistedOrder, RepositoryError> {
    let id = parse_uuid("id", row.try_get::<String, _>("id")?)?;
    let chain_id = i64_to_u64("chain_id", row.try_get("chain_id")?)?;
    let state_value = row.try_get::<String, _>("state")?;
    let state = parse_state(&state_value)?;
    let version = i64_to_u64("version", row.try_get("version")?)?;
    let token_in = parse_fixed::<20>("token_in", row.try_get("token_in")?)?;
    let token_out = parse_fixed::<20>("token_out", row.try_get("token_out")?)?;
    let amount_in = parse_u256("amount_in", row.try_get("amount_in")?)?;
    let amount_out = parse_u256("amount_out", row.try_get("amount_out")?)?;
    let solver = row
        .try_get::<Option<Vec<u8>>, _>("solver_address")?
        .map(|value| parse_fixed::<20>("solver_address", value).map(Address::from))
        .transpose()?;
    let noise_public_key = row.try_get("solver_noise_public_key")?;
    let tx_hash = row
        .try_get::<Option<Vec<u8>>, _>("tx_hash")?
        .map(|value| parse_fixed::<32>("tx_hash", value).map(B256::from))
        .transpose()?;
    let expires_at_ms = row.try_get("expires_at_ms")?;
    Ok(PersistedOrder {
        order: Order {
            id,
            chain_id,
            state,
            version,
            token_in: Address::from(token_in),
            token_out: Address::from(token_out),
            amount_in,
            amount_out,
            expires_at_ms,
            solver,
            solver_noise_public_key: noise_public_key,
            tx_hash,
        },
    })
}

fn decode_proof(row: SqliteRow) -> Result<ProofPayload, RepositoryError> {
    Ok(ProofPayload {
        order_id: parse_uuid("order_id", row.try_get("order_id")?)?,
        ciphertext: row.try_get("ciphertext")?,
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
    u64_to_i64("version", version)
}

fn u64_to_i64(field: &'static str, value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| invalid(field, value.to_string()))
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

    #[tokio::test]
    async fn network_stamp_is_sticky() {
        let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();

        repository.bind_network(Network::Localnet).await.unwrap();
        repository.bind_network(Network::Localnet).await.unwrap();

        assert!(matches!(
            repository.bind_network(Network::Mainnet).await,
            Err(RepositoryError::NetworkMismatch { .. })
        ));
    }

    fn order(state: OrderState, version: u64) -> Order {
        Order {
            id: Uuid::new_v4(),
            chain_id: 31_337,
            state,
            version,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
            amount_in: U256::from(100),
            amount_out: U256::from(200),
            expires_at_ms: Some(5000),
            solver: Some(Address::repeat_byte(3)),
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
        let commitment = B256::repeat_byte(1);
        repository
            .insert_order(&order, commitment, 1000)
            .await
            .unwrap();

        let stored = repository.get_order(order.id).await.unwrap().unwrap();
        assert_eq!(stored.order.id, order.id);
        assert_eq!(stored.order.chain_id, order.chain_id);
        assert_eq!(stored.order.state, OrderState::Executing);
        assert_eq!(stored.order.version, 7);
        assert_eq!(stored.order.amount_in, order.amount_in);
        assert_eq!(stored.order.solver, order.solver);
        assert_eq!(stored.order.tx_hash, order.tx_hash);
    }

    #[tokio::test]
    async fn rejects_a_stale_order_update() {
        let repository = repository().await;
        let mut order = order(OrderState::Reserving, 3);
        repository
            .insert_order(&order, B256::repeat_byte(1), 1000)
            .await
            .unwrap();

        order.state = OrderState::AwaitingUserProof;
        order.version = 4;
        repository
            .persist_transition(&order, 3, 2000, None, false)
            .await
            .unwrap();

        let error = repository
            .persist_transition(&order, 3, 3000, None, false)
            .await
            .unwrap_err();
        assert!(matches!(error, RepositoryError::VersionConflict));
    }

    #[tokio::test]
    async fn loads_only_non_terminal_orders() {
        let repository = repository().await;
        let active = order(OrderState::ProofRelayed, 5);
        let filled = order(OrderState::Filled, 8);
        repository
            .insert_order(&active, B256::repeat_byte(1), 1000)
            .await
            .unwrap();
        repository
            .insert_order(&filled, B256::repeat_byte(2), 1001)
            .await
            .unwrap();

        let orders = repository.load_non_terminal_orders().await.unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order.id, active.id);
    }

    #[tokio::test]
    async fn proof_fetch_is_non_destructive() {
        let repository = repository().await;
        let mut order = order(OrderState::AwaitingUserProof, 4);
        let solver_id = order.solver.unwrap();
        repository
            .insert_order(&order, B256::repeat_byte(1), 1000)
            .await
            .unwrap();
        order.state = OrderState::ProofRelayed;
        order.version = 5;
        repository
            .persist_transition(&order, 4, 1100, Some((solver_id, &[1, 2, 3])), false)
            .await
            .unwrap();

        let first = repository.load_solver_proofs(solver_id).await.unwrap();
        let second = repository.load_solver_proofs(solver_id).await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(first[0].ciphertext, vec![1, 2, 3]);
    }
}
