use super::{maintenance::now_ms, *};
use std::sync::Arc;

use crate::{
    config::AppConfig,
    order::{ProofOrderState, TradeTerms},
    registry::{SolverProfile, SolverRegistry},
    session::SolverSessions,
    storage::{NewProofOrder, OrderRepository, PendingReservation, PreviewSnapshot},
};
use alloy_primitives::{Address, B256, U256};
use kage_types::proof_orders::{
    AssignmentTicket, AssignmentTicketClaims, PreviewCategory, ReservationAck,
    ReservationAckClaims, ReservationDecline, ReservationDeclineClaims, ReservationDeclineReason,
};
use kage_types::routing::{
    MultiRecipientProof, PROOF_ENVELOPE_SUITE, PreviewResponse, PreviewRoute, RecipientKeyWrap,
    SolverCapabilities, SolverMarket,
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

fn capacity_input(
    seed: u8,
    solver_id: Address,
    key_id: B256,
    created_at_ms: i64,
    amount_out: U256,
) -> NewProofOrder {
    let terms = TradeTerms {
        chain_id: 31_337,
        token_in: Address::repeat_byte(1),
        token_out: Address::repeat_byte(2),
        amount_in: U256::from(10),
        amount_out,
        expires_at_ms: created_at_ms.div_euclid(1_000) * 1_000 + 60_000,
    };
    let ciphertext = vec![seed; 16];
    NewProofOrder {
        order_id: Uuid::new_v4(),
        access_token_hash: B256::repeat_byte(seed),
        preview_id: B256::repeat_byte(seed.saturating_add(20)),
        category_id: "major-50".to_owned(),
        terms,
        domain_hash: B256::repeat_byte(seed.saturating_add(40)),
        fee_bps: 50,
        settlement_commitment: B256::repeat_byte(seed.saturating_add(60)),
        proof: MultiRecipientProof {
            suite: PROOF_ENVELOPE_SUITE.to_owned(),
            nonce: vec![seed.saturating_add(1); 24],
            ciphertext: ciphertext.clone(),
            ciphertext_digest: alloy_primitives::keccak256(&ciphertext),
            recipients: vec![RecipientKeyWrap {
                solver_id,
                key_id,
                encapsulated_key: vec![seed.saturating_add(2); 32],
                wrapped_key: vec![seed.saturating_add(3); 48],
            }],
        },
        candidates: vec![PreviewRoute {
            solver_id,
            min_amount_in: U256::from(1),
            max_amount_in: U256::from(100),
            encryption_key_id: key_id,
            encryption_public_key: vec![9; 32],
            key_expires_at_ms: terms.expires_at_ms + 10_000,
        }],
        created_at_ms,
        reservation_attempt_timeout_ms: 2_000,
        ciphertext_cleanup_grace_seconds: 300,
    }
}

fn add_capacity_candidate(input: &mut NewProofOrder, solver_id: Address, key_id: B256, seed: u8) {
    input.proof.recipients.push(RecipientKeyWrap {
        solver_id,
        key_id,
        encapsulated_key: vec![seed; 32],
        wrapped_key: vec![seed.saturating_add(1); 48],
    });
    input.candidates.push(PreviewRoute {
        solver_id,
        min_amount_in: U256::from(1),
        max_amount_in: U256::from(100),
        encryption_key_id: key_id,
        encryption_public_key: vec![9; 32],
        key_expires_at_ms: input.terms.expires_at_ms + 10_000,
    });
}

async fn capacity_orderbook(
    repository: OrderRepository,
    solver_id: Address,
    key_id: B256,
    max_in_flight: u16,
    amount_out_total: U256,
    now_ms: i64,
) -> OrderbookHandle {
    capacity_orderbook_for(
        repository,
        &[(solver_id, key_id, max_in_flight, amount_out_total)],
        now_ms,
    )
    .await
}

async fn capacity_orderbook_for(
    repository: OrderRepository,
    solvers: &[(Address, B256, u16, U256)],
    now_ms: i64,
) -> OrderbookHandle {
    capacity_orderbook_parts(repository, solvers, now_ms)
        .await
        .0
}

async fn capacity_orderbook_parts(
    repository: OrderRepository,
    solvers: &[(Address, B256, u16, U256)],
    now_ms: i64,
) -> (OrderbookHandle, SolverSessions) {
    let sessions = SolverSessions::new("kage-orderbook:capacity-test");
    for (solver_id, key_id, max_in_flight, amount_out_total) in solvers {
        publish_capacity(
            &sessions,
            *solver_id,
            *key_id,
            *max_in_flight,
            *amount_out_total,
            25,
            now_ms,
        );
    }
    let registry = SolverRegistry::from_profiles(solvers.iter().map(|(solver_id, ..)| {
        (
            *solver_id,
            SolverProfile {
                noise_public_key: B256::repeat_byte(3),
                active: true,
            },
        )
    }));
    let gate = AdmissionGate::for_test(
        sessions.clone(),
        registry,
        Arc::new(solvers.iter().map(|(solver_id, ..)| *solver_id).collect()),
        10,
    );
    let orderbook = start_orderbook_with_admission(
        repository,
        16,
        crate::config::ProofOrderSettings::default(),
        gate,
    )
    .await
    .unwrap();
    (orderbook, sessions)
}

fn publish_capacity(
    sessions: &SolverSessions,
    solver_id: Address,
    key_id: B256,
    max_in_flight: u16,
    amount_out_total: U256,
    minimum_margin_bps: u16,
    now_ms: i64,
) -> String {
    publish_capacity_with_public_key(
        sessions,
        solver_id,
        key_id,
        vec![9; 32],
        max_in_flight,
        amount_out_total,
        minimum_margin_bps,
        now_ms,
    )
}

#[allow(clippy::too_many_arguments)]
fn publish_capacity_with_public_key(
    sessions: &SolverSessions,
    solver_id: Address,
    key_id: B256,
    encryption_public_key: Vec<u8>,
    max_in_flight: u16,
    amount_out_total: U256,
    minimum_margin_bps: u16,
    now_ms: i64,
) -> String {
    let now_ms_u64 = u64::try_from(now_ms).unwrap();
    let session = sessions.open(solver_id, now_ms_u64);
    sessions
        .register_capabilities(
            &session.token,
            SolverCapabilities {
                revision: 1,
                max_in_flight,
                encryption_key_id: key_id,
                encryption_public_key,
                key_expires_at_ms: now_ms + 70_000,
                markets: vec![SolverMarket {
                    chain_id: 31_337,
                    token_in: Address::repeat_byte(1),
                    token_out: Address::repeat_byte(2),
                    min_amount_in: U256::from(1),
                    max_amount_in: U256::from(100),
                    available_amount_out: amount_out_total,
                    minimum_margin_bps,
                }],
            },
            now_ms_u64,
        )
        .unwrap();
    session.token
}

fn production_capacity_config(solver_id: Address) -> AppConfig {
    let mut config = AppConfig::from_json(include_str!("../../../config/localnet.json")).unwrap();
    config.allowed_solvers = vec![solver_id];
    for category in &mut config.fee_categories {
        category.solver_ids = vec![solver_id];
    }
    let chain = &mut config.chains[0];
    chain
        .tokens
        .iter_mut()
        .find(|token| token.symbol == "WETH")
        .unwrap()
        .address = Address::repeat_byte(1);
    chain
        .tokens
        .iter_mut()
        .find(|token| token.symbol == "USDC")
        .unwrap()
        .address = Address::repeat_byte(2);
    config
}

fn preview_snapshot(input: &NewProofOrder, valid_until_ms: i64) -> PreviewSnapshot {
    PreviewSnapshot {
        response: PreviewResponse {
            preview_id: input.preview_id,
            chain_id: input.terms.chain_id,
            token_in: input.terms.token_in,
            token_out: input.terms.token_out,
            token_in_decimals: 18,
            token_out_decimals: 6,
            amount_in: input.terms.amount_in,
            midpoint_amount_out: input.terms.amount_out,
            confidence_amount_out: input.terms.amount_out,
            oracle_adjustment_bps: 0,
            oracle_adjustment_amount: U256::ZERO,
            valid_until_ms,
            recommended_proof_lifetime_seconds: 30,
            minimum_remaining_seconds: 15,
            categories: vec![PreviewCategory {
                id: input.category_id.clone(),
                fee_bps: input.fee_bps,
                exact_amount_out: input.terms.amount_out,
                fee_amount: U256::ZERO,
                routes: input.candidates.clone(),
            }],
        },
        price_in_e18: U256::from(1),
        price_out_e18: U256::from(1),
        price_in_lower_e18: U256::from(1),
        price_out_upper_e18: U256::from(1),
        pricing_sequence: 1,
        published_at_ms: input.created_at_ms,
        created_at_ms: input.created_at_ms,
        erase_after_ms: valid_until_ms.saturating_add(300_000),
    }
}

fn reservation_evidence(
    pending: &PendingReservation,
    seed: u8,
) -> (ReservationAck, AssignmentTicket) {
    let accepted_at_ms = now_ms();
    (
        ReservationAck {
            claims: ReservationAckClaims {
                bindings: pending.claims.bindings.clone(),
                attempt_nonce: pending.claims.attempt_nonce,
                accepted_at_ms,
            },
            signature: vec![seed; 65],
        },
        AssignmentTicket {
            claims: AssignmentTicketClaims {
                bindings: pending.claims.bindings.clone(),
                settlement_commitment: pending.settlement_commitment,
                proof_encryption_key_id: pending.key_id,
                issued_at_ms: accepted_at_ms,
                expires_at_ms: pending.terms.expires_at_ms,
                nonce: B256::repeat_byte(seed.saturating_add(1)),
            },
            signature: vec![seed.saturating_add(2); 65],
        },
    )
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

#[tokio::test]
async fn concurrent_orders_cannot_exceed_the_last_processing_slot() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let solver_id = Address::repeat_byte(7);
    let key_id = B256::repeat_byte(8);
    let created_at_ms = now_ms();
    let orderbook = capacity_orderbook(
        repository.clone(),
        solver_id,
        key_id,
        1,
        U256::from(1_000),
        created_at_ms,
    )
    .await;
    let first_input = capacity_input(21, solver_id, key_id, created_at_ms, U256::from(10));
    let second_input = capacity_input(22, solver_id, key_id, created_at_ms, U256::from(10));
    let first = orderbook.create_proof_order(first_input.clone());
    let second = orderbook.create_proof_order(second_input.clone());

    let (first, second) = tokio::join!(first, second);
    assert!(matches!(
        (&first, &second),
        (Ok(_), Err(ServiceError::RouteCapacityChanged))
            | (Err(ServiceError::RouteCapacityChanged), Ok(_))
    ));
    let retry = orderbook
        .create_proof_order(if first.is_ok() {
            first_input
        } else {
            second_input
        })
        .await
        .unwrap();
    assert!(!retry.created);
    assert_eq!(
        repository
            .proof_orders()
            .active_workload(solver_id, created_at_ms)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn concurrent_orders_cannot_overdraw_output_liquidity() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let solver_id = Address::repeat_byte(7);
    let key_id = B256::repeat_byte(8);
    let created_at_ms = now_ms();
    let orderbook = capacity_orderbook(
        repository.clone(),
        solver_id,
        key_id,
        2,
        U256::from(100),
        created_at_ms,
    )
    .await;
    let first = orderbook.create_proof_order(capacity_input(
        31,
        solver_id,
        key_id,
        created_at_ms,
        U256::from(60),
    ));
    let second = orderbook.create_proof_order(capacity_input(
        32,
        solver_id,
        key_id,
        created_at_ms,
        U256::from(60),
    ));

    let (first, second) = tokio::join!(first, second);
    assert!(matches!(
        (&first, &second),
        (Ok(_), Err(ServiceError::RouteCapacityChanged))
            | (Err(ServiceError::RouteCapacityChanged), Ok(_))
    ));
    assert_eq!(
        repository
            .proof_orders()
            .held_output_amount(solver_id, 31_337, Address::repeat_byte(2), created_at_ms,)
            .await
            .unwrap(),
        U256::from(60)
    );
}

#[tokio::test]
async fn final_admission_skips_a_full_candidate() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let full_solver = Address::repeat_byte(7);
    let next_solver = Address::repeat_byte(8);
    let full_key = B256::repeat_byte(17);
    let next_key = B256::repeat_byte(18);
    let created_at_ms = now_ms();
    let orderbook = capacity_orderbook_for(
        repository.clone(),
        &[
            (full_solver, full_key, 1, U256::from(1_000)),
            (next_solver, next_key, 1, U256::from(1_000)),
        ],
        created_at_ms,
    )
    .await;
    orderbook
        .create_proof_order(capacity_input(
            35,
            full_solver,
            full_key,
            created_at_ms,
            U256::from(10),
        ))
        .await
        .unwrap();
    let mut routed = capacity_input(36, full_solver, full_key, created_at_ms, U256::from(10));
    add_capacity_candidate(&mut routed, next_solver, next_key, 37);
    let order_id = routed.order_id;

    orderbook.create_proof_order(routed).await.unwrap();
    assert!(
        repository
            .proof_orders()
            .pending_reservation(order_id, next_solver)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn decline_skips_a_full_fallback_and_moves_capacity_to_the_next_solver() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let first_solver = Address::repeat_byte(7);
    let full_solver = Address::repeat_byte(8);
    let next_solver = Address::repeat_byte(9);
    let first_key = B256::repeat_byte(17);
    let full_key = B256::repeat_byte(18);
    let next_key = B256::repeat_byte(19);
    let created_at_ms = now_ms();
    let orderbook = capacity_orderbook_for(
        repository.clone(),
        &[
            (first_solver, first_key, 1, U256::from(1_000)),
            (full_solver, full_key, 1, U256::from(1_000)),
            (next_solver, next_key, 1, U256::from(1_000)),
        ],
        created_at_ms,
    )
    .await;

    let mut routed = capacity_input(41, first_solver, first_key, created_at_ms, U256::from(10));
    add_capacity_candidate(&mut routed, full_solver, full_key, 42);
    add_capacity_candidate(&mut routed, next_solver, next_key, 43);
    let routed_order_id = routed.order_id;
    orderbook.create_proof_order(routed).await.unwrap();
    orderbook
        .create_proof_order(capacity_input(
            44,
            full_solver,
            full_key,
            created_at_ms,
            U256::from(10),
        ))
        .await
        .unwrap();

    let pending = repository
        .proof_orders()
        .pending_reservation(routed_order_id, first_solver)
        .await
        .unwrap()
        .unwrap();
    let decline = ReservationDecline {
        claims: ReservationDeclineClaims {
            bindings: pending.claims.bindings,
            attempt_nonce: pending.claims.attempt_nonce,
            reason: ReservationDeclineReason::Busy,
            declined_at_ms: now_ms(),
        },
        signature: vec![45; 65],
    };
    assert_eq!(
        orderbook
            .decline_proof_order(routed_order_id, first_solver, decline)
            .await
            .unwrap(),
        Some(crate::storage::AdvanceOutcome::Advanced(next_solver))
    );
    let proof_orders = repository.proof_orders();
    assert!(
        proof_orders
            .pending_reservation(routed_order_id, full_solver)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        proof_orders
            .pending_reservation(routed_order_id, next_solver)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        proof_orders
            .active_workload(first_solver, created_at_ms)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        proof_orders
            .active_workload(full_solver, created_at_ms)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        proof_orders
            .active_workload(next_solver, created_at_ms)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn decline_releases_the_current_hold_while_waiting_for_fallback_capacity() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let first_solver = Address::repeat_byte(7);
    let full_solver = Address::repeat_byte(8);
    let first_key = B256::repeat_byte(17);
    let full_key = B256::repeat_byte(18);
    let created_at_ms = now_ms();
    let orderbook = capacity_orderbook_for(
        repository.clone(),
        &[
            (first_solver, first_key, 1, U256::from(1_000)),
            (full_solver, full_key, 1, U256::from(1_000)),
        ],
        created_at_ms,
    )
    .await;

    let mut routed = capacity_input(51, first_solver, first_key, created_at_ms, U256::from(10));
    add_capacity_candidate(&mut routed, full_solver, full_key, 52);
    let routed_order_id = routed.order_id;
    orderbook.create_proof_order(routed).await.unwrap();
    orderbook
        .create_proof_order(capacity_input(
            53,
            full_solver,
            full_key,
            created_at_ms,
            U256::from(10),
        ))
        .await
        .unwrap();

    let proof_orders = repository.proof_orders();
    let pending = proof_orders
        .pending_reservation(routed_order_id, first_solver)
        .await
        .unwrap()
        .unwrap();
    let decline = ReservationDecline {
        claims: ReservationDeclineClaims {
            bindings: pending.claims.bindings,
            attempt_nonce: pending.claims.attempt_nonce,
            reason: ReservationDeclineReason::Busy,
            declined_at_ms: now_ms(),
        },
        signature: vec![54; 65],
    };
    assert_eq!(
        orderbook
            .decline_proof_order(routed_order_id, first_solver, decline)
            .await
            .unwrap(),
        Some(crate::storage::AdvanceOutcome::AwaitingCapacity)
    );
    assert!(
        proof_orders
            .pending_reservation(routed_order_id, first_solver)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        proof_orders
            .pending_reservation(routed_order_id, full_solver)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        proof_orders
            .active_workload(first_solver, now_ms())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        proof_orders.awaiting_capacity_order_ids().await.unwrap(),
        vec![routed_order_id]
    );
}

#[tokio::test]
async fn final_admission_rechecks_the_current_solver_margin() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let solver_id = Address::repeat_byte(7);
    let key_id = B256::repeat_byte(8);
    let created_at_ms = now_ms();
    let (orderbook, sessions) = capacity_orderbook_parts(
        repository,
        &[(solver_id, key_id, 1, U256::from(1_000))],
        created_at_ms,
    )
    .await;
    publish_capacity(
        &sessions,
        solver_id,
        key_id,
        1,
        U256::from(1_000),
        41,
        created_at_ms + 1,
    );

    assert!(matches!(
        orderbook
            .create_proof_order(capacity_input(
                61,
                solver_id,
                key_id,
                created_at_ms,
                U256::from(10),
            ))
            .await,
        Err(ServiceError::RouteCapacityChanged)
    ));
}

#[tokio::test]
async fn final_admission_rechecks_the_remaining_proof_window() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let solver_id = Address::repeat_byte(7);
    let key_id = B256::repeat_byte(8);
    let created_at_ms = now_ms();
    let orderbook = capacity_orderbook(
        repository,
        solver_id,
        key_id,
        1,
        U256::from(1_000),
        created_at_ms,
    )
    .await;
    let mut input = capacity_input(62, solver_id, key_id, created_at_ms, U256::from(10));
    input.terms.expires_at_ms = created_at_ms.div_euclid(1_000) * 1_000 + 15_000;

    assert!(matches!(
        orderbook.create_proof_order(input).await,
        Err(ServiceError::ProofDeadlineChanged)
    ));
}

#[tokio::test]
async fn final_admission_requires_a_live_persisted_preview() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let solver_id = Address::repeat_byte(7);
    let key_id = B256::repeat_byte(8);
    let created_at_ms = now_ms();
    let sessions = SolverSessions::new("kage-orderbook:preview-expiry-test");
    publish_capacity(
        &sessions,
        solver_id,
        key_id,
        1,
        U256::from(1_000),
        25,
        created_at_ms,
    );
    let registry = SolverRegistry::from_profiles([(
        solver_id,
        SolverProfile {
            noise_public_key: B256::repeat_byte(3),
            active: true,
        },
    )]);
    let config = production_capacity_config(solver_id);
    let admission = AdmissionGate::from_config(sessions, registry, &config);
    let orderbook = start_orderbook_with_admission(repository, 16, config.proof_orders, admission)
        .await
        .unwrap();

    assert!(matches!(
        orderbook
            .create_proof_order(capacity_input(
                67,
                solver_id,
                key_id,
                created_at_ms,
                U256::from(10),
            ))
            .await,
        Err(ServiceError::PreviewExpired)
    ));
}

#[tokio::test]
async fn key_rotation_before_ack_prevents_proof_disclosure() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let solver_id = Address::repeat_byte(7);
    let key_id = B256::repeat_byte(8);
    let replacement_key_id = B256::repeat_byte(9);
    let created_at_ms = now_ms();
    let (orderbook, sessions) = capacity_orderbook_parts(
        repository.clone(),
        &[(solver_id, key_id, 1, U256::from(1_000))],
        created_at_ms,
    )
    .await;
    let input = capacity_input(63, solver_id, key_id, created_at_ms, U256::from(10));
    let order_id = input.order_id;
    orderbook.create_proof_order(input).await.unwrap();
    let proof_orders = repository.proof_orders();
    let pending = proof_orders
        .pending_reservation(order_id, solver_id)
        .await
        .unwrap()
        .unwrap();
    let (reservation_ack, ticket) = reservation_evidence(&pending, 64);
    publish_capacity(
        &sessions,
        solver_id,
        replacement_key_id,
        1,
        U256::from(1_000),
        25,
        now_ms(),
    );

    assert!(matches!(
        orderbook
            .assign_and_disclose_proof_order(order_id, solver_id, None, reservation_ack, ticket,)
            .await,
        Err(ServiceError::RouteCapacityChanged)
    ));
    assert_eq!(
        proof_orders.state(order_id).await.unwrap(),
        Some(ProofOrderState::ReservationPending)
    );
    assert!(proof_orders.binding(order_id).await.unwrap().is_none());
    assert!(
        proof_orders
            .assigned_delivery(order_id, solver_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn same_key_id_with_changed_public_key_cannot_disclose() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let solver_id = Address::repeat_byte(7);
    let key_id = B256::repeat_byte(8);
    let created_at_ms = now_ms();
    let input = capacity_input(70, solver_id, key_id, created_at_ms, U256::from(10));
    let mut snapshot = preview_snapshot(&input, created_at_ms.saturating_add(30_000));
    snapshot.erase_after_ms = snapshot.response.valid_until_ms.saturating_add(1);
    repository.previews().insert(&snapshot).await.unwrap();
    let sessions = SolverSessions::new("kage-orderbook:public-key-binding-test");
    publish_capacity(
        &sessions,
        solver_id,
        key_id,
        1,
        U256::from(1_000),
        25,
        created_at_ms,
    );
    let registry = SolverRegistry::from_profiles([(
        solver_id,
        SolverProfile {
            noise_public_key: B256::repeat_byte(3),
            active: true,
        },
    )]);
    let config = production_capacity_config(solver_id);
    let admission = AdmissionGate::from_config(sessions.clone(), registry, &config);
    let orderbook =
        start_orderbook_with_admission(repository.clone(), 16, config.proof_orders, admission)
            .await
            .unwrap();
    let order_id = input.order_id;
    orderbook.create_proof_order(input.clone()).await.unwrap();
    assert_eq!(
        repository
            .previews()
            .cleanup(snapshot.erase_after_ms)
            .await
            .unwrap(),
        0
    );
    let proof_orders = repository.proof_orders();
    let pending = proof_orders
        .pending_reservation(order_id, solver_id)
        .await
        .unwrap()
        .unwrap();
    let (reservation_ack, ticket) = reservation_evidence(&pending, 71);
    let replacement_token = publish_capacity_with_public_key(
        &sessions,
        solver_id,
        key_id,
        vec![10; 32],
        1,
        U256::from(1_000),
        25,
        now_ms(),
    );

    assert!(matches!(
        orderbook
            .assign_and_disclose_proof_order(
                order_id,
                solver_id,
                Some(replacement_token),
                reservation_ack,
                ticket,
            )
            .await,
        Err(ServiceError::RouteCapacityChanged)
    ));
    assert_eq!(
        proof_orders.state(order_id).await.unwrap(),
        Some(ProofOrderState::ReservationPending)
    );
    assert!(proof_orders.binding(order_id).await.unwrap().is_none());
}

#[tokio::test]
async fn a_replaced_solver_session_cannot_complete_an_old_ack_request() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let solver_id = Address::repeat_byte(7);
    let key_id = B256::repeat_byte(8);
    let created_at_ms = now_ms();
    let (orderbook, sessions) = capacity_orderbook_parts(
        repository.clone(),
        &[(solver_id, key_id, 1, U256::from(1_000))],
        created_at_ms,
    )
    .await;
    let old_token = publish_capacity(
        &sessions,
        solver_id,
        key_id,
        1,
        U256::from(1_000),
        25,
        created_at_ms + 1,
    );
    let input = capacity_input(68, solver_id, key_id, created_at_ms, U256::from(10));
    let order_id = input.order_id;
    orderbook.create_proof_order(input).await.unwrap();
    let proof_orders = repository.proof_orders();
    let pending = proof_orders
        .pending_reservation(order_id, solver_id)
        .await
        .unwrap()
        .unwrap();
    let (reservation_ack, ticket) = reservation_evidence(&pending, 69);
    publish_capacity(
        &sessions,
        solver_id,
        key_id,
        1,
        U256::from(1_000),
        25,
        now_ms(),
    );

    assert!(matches!(
        orderbook
            .assign_and_disclose_proof_order(
                order_id,
                solver_id,
                Some(old_token),
                reservation_ack,
                ticket,
            )
            .await,
        Err(ServiceError::RouteCapacityChanged)
    ));
    assert_eq!(
        proof_orders.state(order_id).await.unwrap(),
        Some(ProofOrderState::ReservationPending)
    );
    assert!(proof_orders.binding(order_id).await.unwrap().is_none());
}
