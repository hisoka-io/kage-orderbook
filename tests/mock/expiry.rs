use std::{path::PathBuf, time::Duration};

use kage_orderbook::core::{
    command::Command,
    engine::{OrderError, ServiceError, start_orderbook},
};
use kage_types::orders::OrderState;
use uuid::Uuid;

use super::support::{commitment, solver_address, terms};

struct TestDatabase(PathBuf);

impl TestDatabase {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("kage-orderbook-{}.db", Uuid::new_v4())))
    }

    fn url(&self) -> String {
        format!("sqlite://{}", self.0.display())
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        for suffix in ["", "-shm", "-wal"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

#[tokio::test]
async fn rejects_an_order_that_is_already_expired() {
    let orderbook = start_orderbook("sqlite::memory:").await.unwrap();
    let order_id = Uuid::new_v4();
    let mut expired_terms = terms(1);
    expired_terms.expires_at_ms = now_ms() - 1;

    let error = orderbook
        .execute(Command::CreateOrder {
            order_id,
            order_commitment: commitment(1),
            terms: expired_terms,
        })
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ServiceError::Order(OrderError::InvalidTerms)
    ));
    assert!(orderbook.get_order(order_id).await.unwrap().is_none());
}

#[tokio::test]
async fn expires_an_assigned_order_and_rejects_late_solver_actions() {
    let orderbook = start_orderbook("sqlite::memory:").await.unwrap();
    let order_id = Uuid::new_v4();
    let solver_id = solver_address(0x11);
    let mut expiring_terms = terms(1);
    expiring_terms.expires_at_ms = now_ms() + 150;

    orderbook
        .execute(Command::CreateOrder {
            order_id,
            order_commitment: commitment(1),
            terms: expiring_terms,
        })
        .await
        .unwrap();
    orderbook
        .execute(Command::SolverReserved {
            order_id,
            solver_id,
            noise_public_key: vec![7; 32],
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(250)).await;

    let expired = orderbook.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(expired.state, OrderState::Expired);
    assert_eq!(expired.version, 6);
    for command in [
        Command::SolverReserved {
            order_id,
            solver_id,
            noise_public_key: vec![7; 32],
        },
        Command::SolverDeclined {
            order_id,
            solver_id,
        },
    ] {
        assert!(matches!(
            orderbook.execute(command).await,
            Err(ServiceError::Order(OrderError::InvalidState))
        ));
    }
}

#[tokio::test]
async fn expires_an_order_while_the_orderbook_is_offline() {
    let database = TestDatabase::new();
    let database_url = database.url();
    let order_id = Uuid::new_v4();
    let mut expiring_terms = terms(1);
    expiring_terms.expires_at_ms = now_ms() + 50;

    let first = start_orderbook(&database_url).await.unwrap();
    first
        .execute(Command::CreateOrder {
            order_id,
            order_commitment: commitment(1),
            terms: expiring_terms,
        })
        .await
        .unwrap();
    drop(first);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let restarted = start_orderbook(&database_url).await.unwrap();
    let expired = restarted.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(expired.state, OrderState::Expired);
    assert_eq!(expired.version, 4);
    assert!(restarted.reserving_orders().await.unwrap().is_empty());
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
