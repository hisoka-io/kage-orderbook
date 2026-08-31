use std::{str::FromStr, time::Duration};

use alloy_primitives::{Address, U256};
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
    order::{Order, OrderId, ProofOrderState},
};

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("order version conflict")]
    VersionConflict,
    #[error("order id or access token is already bound to different immutable proof-order data")]
    IdempotencyConflict,
    #[error("invalid stored {field}: {value}")]
    InvalidData { field: &'static str, value: String },
    #[error("database belongs to {found}, refusing to open it as {expected}")]
    NetworkMismatch { found: String, expected: Network },
}

#[derive(Debug, Clone)]
pub(crate) struct PersistedOrder {
    pub order: Order,
}

#[derive(Clone)]
pub struct OrderRepository {
    pool: SqlitePool,
    retention_metrics: super::RetentionMetrics,
}

impl OrderRepository {
    pub fn previews(&self) -> super::PreviewRepository {
        super::PreviewRepository::new(self.pool.clone())
    }

    pub fn proof_orders(&self) -> super::ProofOrderRepository {
        super::ProofOrderRepository::new(self.pool.clone(), self.retention_metrics.clone())
    }
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
        let repository = Self {
            pool,
            retention_metrics: super::RetentionMetrics::default(),
        };
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

    pub(crate) async fn persist_transition(
        &self,
        order: &Order,
        expected_version: u64,
        timestamp_ms: i64,
    ) -> Result<(), RepositoryError> {
        if order.state == ProofOrderState::Expired {
            return self
                .proof_orders()
                .expire_with_core_transition(order, expected_version, timestamp_ms)
                .await;
        }
        let result = sqlx::query(
            "UPDATE orders SET
                state = ?, version = ?, token_in = ?, token_out = ?,
                amount_in = ?, amount_out = ?, solver_address = ?,
                updated_at_ms = ?
             WHERE id = ? AND version = ?",
        )
        .bind(state_name(order.state))
        .bind(version_to_i64(order.version)?)
        .bind(order.token_in.as_slice())
        .bind(order.token_out.as_slice())
        .bind(order.amount_in.to_string())
        .bind(order.amount_out.to_string())
        .bind(order.solver.map(|address| address.to_vec()))
        .bind(timestamp_ms)
        .bind(order.id.to_string())
        .bind(version_to_i64(expected_version)?)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() != 1 {
            return Err(RepositoryError::VersionConflict);
        }

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

    pub(crate) async fn load_non_terminal_orders(
        &self,
    ) -> Result<Vec<PersistedOrder>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT * FROM orders
             WHERE state NOT IN ('expired', 'closed')
             ORDER BY created_at_ms, id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(decode_order).collect()
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
        },
    })
}

fn state_name(state: ProofOrderState) -> &'static str {
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

fn parse_state(value: &str) -> Result<ProofOrderState, RepositoryError> {
    match value {
        "submitted" => Ok(ProofOrderState::Submitted),
        "reservation_pending" => Ok(ProofOrderState::ReservationPending),
        "assigned" => Ok(ProofOrderState::Assigned),
        "proof_delivered" => Ok(ProofOrderState::ProofDelivered),
        "proof_accepted" => Ok(ProofOrderState::ProofAccepted),
        "proof_rejected" => Ok(ProofOrderState::ProofRejected),
        "expired" => Ok(ProofOrderState::Expired),
        "complaint_verified" => Ok(ProofOrderState::ComplaintVerified),
        "closed" => Ok(ProofOrderState::Closed),
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

    #[tokio::test]
    async fn initial_schema_creates_only_encrypted_complaint_columns() {
        let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
        let columns = sqlx::query("PRAGMA table_info(proof_order_complaints)")
            .fetch_all(&repository.pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String, _>("name").unwrap())
            .collect::<Vec<_>>();
        assert!(columns.iter().any(|column| column == "opening_ciphertext"));
        assert!(columns.iter().any(|column| column == "legal_hold"));
        assert!(!columns.iter().any(|column| column == "nullifier"));
        assert!(!columns.iter().any(|column| column == "salt"));
    }
}
