use super::{maintenance::now_ms, *};
use crate::{
    order::{ProofOrderState, TradeTerms},
    storage::{NewProofOrder, OrderRepository},
};
use alloy_primitives::{Address, B256, U256};
use kage_types::routing::{
    MultiRecipientProof, PROOF_ENVELOPE_SUITE, PreviewRoute, RecipientKeyWrap,
};
use uuid::Uuid;

fn terms() -> TradeTerms {
    TradeTerms {
        chain_id: 31_337,
        token_in: Address::ZERO,
        token_out: Address::repeat_byte(1),
        amount_in: U256::from(1),
        amount_out: U256::from(2),
        expires_at_ms: i64::MAX,
    }
}

#[tokio::test]
async fn single_writer_admits_an_authoritative_proof_order_idempotently() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let orderbook = start_orderbook_with_repository(repository.clone(), 16)
        .await
        .unwrap();
    let order_id = Uuid::new_v4();
    let created_at_ms = now_ms();
    let terms = TradeTerms {
        expires_at_ms: created_at_ms.div_euclid(1_000) * 1_000 + 60_000,
        ..terms()
    };
    let solver_id = Address::repeat_byte(7);
    let key_id = B256::repeat_byte(8);
    let ciphertext = vec![9; 16];
    let input = NewProofOrder {
        order_id,
        access_token_hash: B256::repeat_byte(10),
        preview_id: B256::repeat_byte(11),
        category_id: "major-50".to_owned(),
        terms,
        domain_hash: B256::repeat_byte(12),
        fee_bps: 50,
        settlement_commitment: B256::repeat_byte(13),
        proof: MultiRecipientProof {
            suite: PROOF_ENVELOPE_SUITE.to_owned(),
            nonce: vec![14; 24],
            ciphertext: ciphertext.clone(),
            ciphertext_digest: alloy_primitives::keccak256(&ciphertext),
            recipients: vec![RecipientKeyWrap {
                solver_id,
                key_id,
                encapsulated_key: vec![15; 32],
                wrapped_key: vec![16; 48],
            }],
        },
        candidates: vec![PreviewRoute {
            solver_id,
            min_amount_in: U256::from(1),
            max_amount_in: U256::from(10),
            encryption_key_id: key_id,
            encryption_public_key: vec![17; 32],
            key_expires_at_ms: terms.expires_at_ms + 1_000,
        }],
        created_at_ms,
        reservation_attempt_timeout_ms: 2_000,
        ciphertext_cleanup_grace_seconds: 300,
    };

    let created = orderbook.create_proof_order(input.clone()).await.unwrap();
    assert!(created.created);
    let retried = orderbook.create_proof_order(input).await.unwrap();
    assert!(!retried.created);
    assert_eq!(retried.order.state, ProofOrderState::ReservationPending);
    assert_eq!(
        repository.proof_orders().state(order_id).await.unwrap(),
        Some(ProofOrderState::ReservationPending)
    );
}
