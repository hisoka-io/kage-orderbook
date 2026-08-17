use std::time::Duration;

use alloy_primitives::B256;
use kage_orderbook::core::{
    command::Command,
    engine::{OrderError, ServiceError, start_orderbook},
};
use kage_types::orders::OrderState;
use uuid::Uuid;

use super::support::{commitment, solver_address, terms};

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
async fn expires_an_active_order_and_removes_its_proof() {
    let orderbook = start_orderbook("sqlite::memory:").await.unwrap();
    let order_id = Uuid::new_v4();
    let solver_id = solver_address(0x11);
    let tx_hash = B256::repeat_byte(9);
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
    orderbook
        .execute(Command::RelayEncryptedProof {
            order_id,
            ciphertext: vec![1, 2, 3],
        })
        .await
        .unwrap();
    assert_eq!(
        orderbook.take_solver_proofs(solver_id).await.unwrap().len(),
        1
    );

    tokio::time::sleep(Duration::from_millis(250)).await;

    let expired = orderbook.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(expired.state, OrderState::Expired);
    assert_eq!(expired.version, 7);
    assert!(
        orderbook
            .take_solver_proofs(solver_id)
            .await
            .unwrap()
            .is_empty()
    );

    for command in [
        Command::SolverReserved {
            order_id,
            solver_id,
            noise_public_key: vec![7; 32],
        },
        Command::RelayEncryptedProof {
            order_id,
            ciphertext: vec![1, 2, 3],
        },
        Command::ExecutionStarted {
            order_id,
            solver_id,
            tx_hash,
        },
        Command::SettlementObserved { order_id, tx_hash },
    ] {
        let error = orderbook.execute(command).await.unwrap_err();
        assert!(matches!(
            error,
            ServiceError::Order(OrderError::InvalidState)
        ));
    }
    assert_eq!(
        orderbook
            .get_order(order_id)
            .await
            .unwrap()
            .unwrap()
            .version,
        7
    );
}

#[tokio::test]
async fn expires_an_order_while_the_orderbook_is_offline() {
    let directory = tempfile::tempdir().unwrap();
    let database_url = format!(
        "sqlite://{}",
        directory.path().join("orderbook.db").display()
    );
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

    drop(restarted);
    tokio::task::yield_now().await;
    let final_restart = start_orderbook(&database_url).await.unwrap();
    let persisted = final_restart.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(persisted.state, OrderState::Expired);
    assert_eq!(persisted.version, 4);
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
