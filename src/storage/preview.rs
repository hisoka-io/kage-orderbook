use std::str::FromStr;

use alloy_primitives::{Address, B256, U256};
use kage_types::{
    proof_orders::PreviewCategory,
    routing::{PreviewResponse, PreviewRoute},
};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};

use super::RepositoryError;

#[derive(Clone)]
pub struct PreviewRepository {
    pool: SqlitePool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewSnapshot {
    pub response: PreviewResponse,
    pub price_in_e18: U256,
    pub price_out_e18: U256,
    pub price_in_lower_e18: U256,
    pub price_out_upper_e18: U256,
    pub pricing_sequence: u64,
    pub published_at_ms: i64,
    pub created_at_ms: i64,
    pub erase_after_ms: i64,
}

impl PreviewRepository {
    pub(super) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, snapshot: &PreviewSnapshot) -> Result<(), RepositoryError> {
        let response = &snapshot.response;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO proof_order_previews (
                preview_id, chain_id, token_in, token_out, token_in_decimals,
                token_out_decimals, amount_in, midpoint_amount_out,
                confidence_amount_out, oracle_adjustment_bps,
                oracle_adjustment_amount, price_in_e18, price_out_e18,
                price_in_lower_e18, price_out_upper_e18, pricing_sequence,
                published_at_ms, valid_until_ms,
                recommended_proof_lifetime_seconds, minimum_remaining_seconds,
                created_at_ms, erase_after_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(response.preview_id.as_slice())
        .bind(to_i64("chain_id", response.chain_id)?)
        .bind(response.token_in.as_slice())
        .bind(response.token_out.as_slice())
        .bind(i64::from(response.token_in_decimals))
        .bind(i64::from(response.token_out_decimals))
        .bind(response.amount_in.to_string())
        .bind(response.midpoint_amount_out.to_string())
        .bind(response.confidence_amount_out.to_string())
        .bind(i64::from(response.oracle_adjustment_bps))
        .bind(response.oracle_adjustment_amount.to_string())
        .bind(snapshot.price_in_e18.to_string())
        .bind(snapshot.price_out_e18.to_string())
        .bind(snapshot.price_in_lower_e18.to_string())
        .bind(snapshot.price_out_upper_e18.to_string())
        .bind(to_i64("pricing_sequence", snapshot.pricing_sequence)?)
        .bind(snapshot.published_at_ms)
        .bind(response.valid_until_ms)
        .bind(i64::from(response.recommended_proof_lifetime_seconds))
        .bind(i64::from(response.minimum_remaining_seconds))
        .bind(snapshot.created_at_ms)
        .bind(snapshot.erase_after_ms)
        .execute(&mut *transaction)
        .await?;

        for (category_position, category) in response.categories.iter().enumerate() {
            sqlx::query(
                "INSERT INTO proof_order_preview_categories (
                    preview_id, position, category_id, fee_bps,
                    exact_amount_out, fee_amount
                 ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(response.preview_id.as_slice())
            .bind(position(category_position)?)
            .bind(&category.id)
            .bind(i64::from(category.fee_bps))
            .bind(category.exact_amount_out.to_string())
            .bind(category.fee_amount.to_string())
            .execute(&mut *transaction)
            .await?;

            for (route_position, route) in category.routes.iter().enumerate() {
                insert_route(
                    &mut transaction,
                    response.preview_id,
                    &category.id,
                    route_position,
                    route,
                )
                .await?;
            }
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn get(&self, preview_id: B256) -> Result<Option<PreviewSnapshot>, RepositoryError> {
        let Some(row) = sqlx::query("SELECT * FROM proof_order_previews WHERE preview_id = ?")
            .bind(preview_id.as_slice())
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };

        let category_rows = sqlx::query(
            "SELECT category_id, fee_bps, exact_amount_out, fee_amount
             FROM proof_order_preview_categories
             WHERE preview_id = ? ORDER BY position",
        )
        .bind(preview_id.as_slice())
        .fetch_all(&self.pool)
        .await?;
        let mut categories = Vec::with_capacity(category_rows.len());
        for category_row in category_rows {
            let category_id: String = category_row.try_get("category_id")?;
            let route_rows = sqlx::query(
                "SELECT solver_id, min_amount_in, max_amount_in,
                        encryption_key_id, encryption_public_key, key_expires_at_ms
                 FROM proof_order_preview_routes
                 WHERE preview_id = ? AND category_id = ? ORDER BY position",
            )
            .bind(preview_id.as_slice())
            .bind(&category_id)
            .fetch_all(&self.pool)
            .await?;
            let routes = route_rows
                .into_iter()
                .map(|route| {
                    Ok(PreviewRoute {
                        solver_id: Address::from(parse_fixed::<20>(
                            "solver_id",
                            route.try_get("solver_id")?,
                        )?),
                        min_amount_in: parse_u256(
                            "min_amount_in",
                            route.try_get("min_amount_in")?,
                        )?,
                        max_amount_in: parse_u256(
                            "max_amount_in",
                            route.try_get("max_amount_in")?,
                        )?,
                        encryption_key_id: B256::from(parse_fixed::<32>(
                            "encryption_key_id",
                            route.try_get("encryption_key_id")?,
                        )?),
                        encryption_public_key: route.try_get("encryption_public_key")?,
                        key_expires_at_ms: route.try_get("key_expires_at_ms")?,
                    })
                })
                .collect::<Result<Vec<_>, RepositoryError>>()?;
            categories.push(PreviewCategory {
                id: category_id,
                fee_bps: u16::try_from(category_row.try_get::<i64, _>("fee_bps")?)
                    .map_err(|_| invalid("fee_bps", "out of range"))?,
                exact_amount_out: parse_u256(
                    "exact_amount_out",
                    category_row.try_get("exact_amount_out")?,
                )?,
                fee_amount: parse_u256("fee_amount", category_row.try_get("fee_amount")?)?,
                routes,
            });
        }

        let response = PreviewResponse {
            preview_id,
            chain_id: to_u64("chain_id", row.try_get("chain_id")?)?,
            token_in: Address::from(parse_fixed::<20>("token_in", row.try_get("token_in")?)?),
            token_out: Address::from(parse_fixed::<20>("token_out", row.try_get("token_out")?)?),
            token_in_decimals: u8::try_from(row.try_get::<i64, _>("token_in_decimals")?)
                .map_err(|_| invalid("token_in_decimals", "out of range"))?,
            token_out_decimals: u8::try_from(row.try_get::<i64, _>("token_out_decimals")?)
                .map_err(|_| invalid("token_out_decimals", "out of range"))?,
            amount_in: parse_u256("amount_in", row.try_get("amount_in")?)?,
            midpoint_amount_out: parse_u256(
                "midpoint_amount_out",
                row.try_get("midpoint_amount_out")?,
            )?,
            confidence_amount_out: parse_u256(
                "confidence_amount_out",
                row.try_get("confidence_amount_out")?,
            )?,
            oracle_adjustment_bps: u16::try_from(row.try_get::<i64, _>("oracle_adjustment_bps")?)
                .map_err(|_| invalid("oracle_adjustment_bps", "out of range"))?,
            oracle_adjustment_amount: parse_u256(
                "oracle_adjustment_amount",
                row.try_get("oracle_adjustment_amount")?,
            )?,
            valid_until_ms: row.try_get("valid_until_ms")?,
            recommended_proof_lifetime_seconds: u32::try_from(
                row.try_get::<i64, _>("recommended_proof_lifetime_seconds")?,
            )
            .map_err(|_| invalid("recommended_proof_lifetime_seconds", "out of range"))?,
            minimum_remaining_seconds: u32::try_from(
                row.try_get::<i64, _>("minimum_remaining_seconds")?,
            )
            .map_err(|_| invalid("minimum_remaining_seconds", "out of range"))?,
            categories,
        };
        Ok(Some(PreviewSnapshot {
            response,
            price_in_e18: parse_u256("price_in_e18", row.try_get("price_in_e18")?)?,
            price_out_e18: parse_u256("price_out_e18", row.try_get("price_out_e18")?)?,
            price_in_lower_e18: parse_u256(
                "price_in_lower_e18",
                row.try_get("price_in_lower_e18")?,
            )?,
            price_out_upper_e18: parse_u256(
                "price_out_upper_e18",
                row.try_get("price_out_upper_e18")?,
            )?,
            pricing_sequence: to_u64("pricing_sequence", row.try_get("pricing_sequence")?)?,
            published_at_ms: row.try_get("published_at_ms")?,
            created_at_ms: row.try_get("created_at_ms")?,
            erase_after_ms: row.try_get("erase_after_ms")?,
        }))
    }

    pub async fn cleanup(&self, now_ms: i64) -> Result<u64, RepositoryError> {
        Ok(
            sqlx::query("DELETE FROM proof_order_previews WHERE erase_after_ms <= ?")
                .bind(now_ms)
                .execute(&self.pool)
                .await?
                .rows_affected(),
        )
    }
}

async fn insert_route(
    transaction: &mut Transaction<'_, Sqlite>,
    preview_id: B256,
    category_id: &str,
    route_position: usize,
    route: &PreviewRoute,
) -> Result<(), RepositoryError> {
    sqlx::query(
        "INSERT INTO proof_order_preview_routes (
            preview_id, category_id, position, solver_id, min_amount_in,
            max_amount_in, encryption_key_id, encryption_public_key,
            key_expires_at_ms
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(preview_id.as_slice())
    .bind(category_id)
    .bind(position(route_position)?)
    .bind(route.solver_id.as_slice())
    .bind(route.min_amount_in.to_string())
    .bind(route.max_amount_in.to_string())
    .bind(route.encryption_key_id.as_slice())
    .bind(&route.encryption_public_key)
    .bind(route.key_expires_at_ms)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn position(value: usize) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| invalid("position", value))
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

fn to_i64(field: &'static str, value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| invalid(field, value))
}

fn to_u64(field: &'static str, value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| invalid(field, value))
}

fn invalid(field: &'static str, value: impl ToString) -> RepositoryError {
    RepositoryError::InvalidData {
        field,
        value: value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::OrderRepository;

    fn snapshot() -> PreviewSnapshot {
        let route = PreviewRoute {
            solver_id: Address::repeat_byte(1),
            min_amount_in: U256::from(1),
            max_amount_in: U256::from(1_000),
            encryption_key_id: B256::repeat_byte(2),
            encryption_public_key: vec![3; 32],
            key_expires_at_ms: 40_000,
        };
        PreviewSnapshot {
            response: PreviewResponse {
                preview_id: B256::repeat_byte(4),
                chain_id: 31_337,
                token_in: Address::repeat_byte(5),
                token_out: Address::repeat_byte(6),
                token_in_decimals: 18,
                token_out_decimals: 6,
                amount_in: U256::from(100),
                midpoint_amount_out: U256::from(210),
                confidence_amount_out: U256::from(205),
                oracle_adjustment_bps: 239,
                oracle_adjustment_amount: U256::from(5),
                valid_until_ms: 10_000,
                recommended_proof_lifetime_seconds: 30,
                minimum_remaining_seconds: 15,
                categories: vec![
                    PreviewCategory {
                        id: "fast-25".to_owned(),
                        fee_bps: 25,
                        exact_amount_out: U256::from(204),
                        fee_amount: U256::from(1),
                        routes: vec![route.clone()],
                    },
                    PreviewCategory {
                        id: "wide-50".to_owned(),
                        fee_bps: 50,
                        exact_amount_out: U256::from(203),
                        fee_amount: U256::from(2),
                        routes: vec![route],
                    },
                ],
            },
            price_in_e18: U256::from(2_000),
            price_out_e18: U256::from(1_000),
            price_in_lower_e18: U256::from(1_990),
            price_out_upper_e18: U256::from(1_010),
            pricing_sequence: 7,
            published_at_ms: 1_000,
            created_at_ms: 2_000,
            erase_after_ms: 20_000,
        }
    }

    #[tokio::test]
    async fn snapshot_round_trips_and_cleanup_uses_the_grace_deadline() {
        let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
        let previews = repository.previews();
        let expected = snapshot();
        previews.insert(&expected).await.unwrap();

        assert_eq!(
            previews.get(expected.response.preview_id).await.unwrap(),
            Some(expected.clone())
        );
        assert_eq!(previews.cleanup(19_999).await.unwrap(), 0);
        assert_eq!(previews.cleanup(20_000).await.unwrap(), 1);
        assert!(
            previews
                .get(expected.response.preview_id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(previews.cleanup(20_000).await.unwrap(), 0);
    }
}
