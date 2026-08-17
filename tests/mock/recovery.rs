use alloy_primitives::B256;
use kage_orderbook::{
    core::{command::Command, engine::start_orderbook, guards::MOCK_CHAIN_ID},
    storage::OrderRepository,
};
use kage_types::orders::OrderState;
use uuid::Uuid;

use super::{
    proof_transport,
    support::{commitment, noise_private_key, noise_public_key, solver_address, terms},
};

#[tokio::test]
async fn proof_relayed_order_survives_restart_and_reaches_filled() {
    let directory = tempfile::tempdir().unwrap();
    let database_url = format!(
        "sqlite://{}",
        directory.path().join("orderbook.db").display()
    );
    let order_id = Uuid::new_v4();
    let solver_id = solver_address(0x11);
    let noise_private_key = noise_private_key(0x33);
    let noise_public_key = noise_public_key(0x33).to_vec();
    let plaintext = b"private-proof";
    let ciphertext =
        proof_transport::encrypt_for_solver(order_id, &noise_public_key, plaintext).unwrap();
    let tx_hash = B256::repeat_byte(9);

    let first = start_orderbook(&database_url).await.unwrap();
    first
        .execute(Command::CreateOrder {
            order_id,
            order_commitment: commitment(1),
            terms: terms(1),
        })
        .await
        .unwrap();
    first
        .execute(Command::SolverReserved {
            order_id,
            solver_id,
            noise_public_key: noise_public_key.clone(),
        })
        .await
        .unwrap();
    first
        .execute(Command::RelayEncryptedProof {
            order_id,
            ciphertext: ciphertext.clone(),
        })
        .await
        .unwrap();

    let before_restart = first.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(before_restart.state, OrderState::ProofRelayed);
    assert_eq!(before_restart.version, 6);
    drop(first);
    tokio::task::yield_now().await;

    let restarted = start_orderbook(&database_url).await.unwrap();
    let restored = restarted.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(restored.chain_id, MOCK_CHAIN_ID);
    assert_eq!(restored.state, OrderState::ProofRelayed);
    assert_eq!(restored.version, 6);

    restarted
        .execute(Command::SolverReserved {
            order_id,
            solver_id,
            noise_public_key,
        })
        .await
        .unwrap();
    restarted
        .execute(Command::RelayEncryptedProof {
            order_id,
            ciphertext: ciphertext.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        restarted
            .get_order(order_id)
            .await
            .unwrap()
            .unwrap()
            .version,
        6
    );

    let proofs = restarted.take_solver_proofs(solver_id).await.unwrap();
    assert_eq!(proofs.len(), 1);
    assert_eq!(proofs[0].order_id, order_id);
    assert_eq!(proofs[0].ciphertext, ciphertext);
    assert_eq!(
        proof_transport::decrypt_from_user(order_id, &noise_private_key, &proofs[0].ciphertext,)
            .unwrap(),
        plaintext
    );

    restarted
        .execute(Command::ExecutionStarted {
            order_id,
            solver_id,
            tx_hash,
        })
        .await
        .unwrap();
    restarted
        .execute(Command::ExecutionStarted {
            order_id,
            solver_id,
            tx_hash,
        })
        .await
        .unwrap();
    let repository = OrderRepository::connect(&database_url).await.unwrap();
    assert!(repository.get_proof(order_id).await.unwrap().is_none());
    restarted
        .execute(Command::RelayEncryptedProof {
            order_id,
            ciphertext: ciphertext.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        restarted
            .get_order(order_id)
            .await
            .unwrap()
            .unwrap()
            .version,
        7
    );
    assert!(
        restarted
            .take_solver_proofs(solver_id)
            .await
            .unwrap()
            .is_empty()
    );

    restarted
        .execute(Command::SettlementObserved { order_id, tx_hash })
        .await
        .unwrap();
    restarted
        .execute(Command::SettlementObserved { order_id, tx_hash })
        .await
        .unwrap();

    let filled = restarted.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(filled.state, OrderState::Filled);
    assert_eq!(filled.version, 8);
    drop(restarted);
    tokio::task::yield_now().await;

    let final_restart = start_orderbook(&database_url).await.unwrap();
    final_restart
        .execute(Command::SettlementObserved { order_id, tx_hash })
        .await
        .unwrap();
    let persisted = final_restart.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(persisted.state, OrderState::Filled);
    assert_eq!(persisted.version, 8);
}
