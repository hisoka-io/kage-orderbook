use kage_types::{
    proof_orders::{
        AssignmentTicketClaims, ProofAcceptanceClaims, ProofRejectionClaims, ProofRejectionReason,
        ReservationAckClaims,
    },
    routing::{PROOF_ENVELOPE_SUITE, RecipientKeyWrap},
};

use super::{
    rows::{parse_b256, parse_fixed, state_name},
    *,
};
use crate::{
    complaint::{ComplaintEvidenceCipher, ComplaintSecretOpening},
    order::ProofOrderState,
    storage::OrderRepository,
};

struct TemporaryDatabase {
    path: std::path::PathBuf,
}

impl TemporaryDatabase {
    fn new(label: &str) -> Self {
        Self {
            path: std::env::temp_dir().join(format!(
                "kage-orderbook-{label}-{}.db",
                uuid::Uuid::new_v4()
            )),
        }
    }

    fn url(&self) -> String {
        format!("sqlite://{}", self.path.display())
    }
}

impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        for suffix in ["", "-shm", "-wal"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}

fn terms() -> TradeTerms {
    TradeTerms {
        chain_id: 31_337,
        token_in: Address::repeat_byte(1),
        token_out: Address::repeat_byte(2),
        amount_in: U256::from(10),
        amount_out: U256::from(9),
        expires_at_ms: 50_000,
    }
}

fn core_order(order_id: OrderId) -> Order {
    let terms = terms();
    Order {
        id: order_id,
        state: ProofOrderState::ReservationPending,
        version: 3,
        chain_id: terms.chain_id,
        token_in: terms.token_in,
        token_out: terms.token_out,
        amount_in: terms.amount_in,
        amount_out: terms.amount_out,
        expires_at_ms: Some(terms.expires_at_ms),
        solver: None,
    }
}

fn route(solver: u8, key: u8) -> PreviewRoute {
    PreviewRoute {
        solver_id: Address::repeat_byte(solver),
        min_amount_in: U256::from(1),
        max_amount_in: U256::from(1_000),
        encryption_key_id: B256::repeat_byte(key),
        encryption_public_key: vec![key; 32],
        key_expires_at_ms: 100_000,
    }
}

fn input(order_id: OrderId) -> NewProofOrder {
    let routes = vec![route(3, 4), route(5, 6)];
    let ciphertext = vec![7; 16];
    NewProofOrder {
        order_id,
        access_token_hash: B256::repeat_byte(8),
        preview_id: B256::repeat_byte(9),
        category_id: "major-50".to_owned(),
        terms: terms(),
        domain_hash: B256::repeat_byte(10),
        fee_bps: 50,
        settlement_commitment: B256::repeat_byte(11),
        proof: MultiRecipientProof {
            suite: PROOF_ENVELOPE_SUITE.to_owned(),
            nonce: vec![12; 24],
            ciphertext: ciphertext.clone(),
            ciphertext_digest: keccak256(&ciphertext),
            recipients: routes
                .iter()
                .map(|route| RecipientKeyWrap {
                    solver_id: route.solver_id,
                    key_id: route.encryption_key_id,
                    encapsulated_key: vec![13; 32],
                    wrapped_key: vec![14; 48],
                })
                .collect(),
        },
        candidates: routes,
        created_at_ms: 1_000,
        reservation_attempt_timeout_ms: 2_000,
        ciphertext_cleanup_grace_seconds: 300,
    }
}

fn assigned_order(order_id: OrderId, solver_id: Address) -> Order {
    let mut order = core_order(order_id);
    order.state = ProofOrderState::ProofDelivered;
    order.version = 5;
    order.solver = Some(solver_id);
    order
}

fn bindings(order_id: OrderId, solver_id: Address) -> ProofOrderBindings {
    ProofOrderBindings {
        order_id,
        preview_id: B256::repeat_byte(9),
        category_id: "major-50".to_owned(),
        solver_id,
        exact_terms_digest: exact_terms_digest(&terms(), B256::repeat_byte(10)),
        ciphertext_digest: keccak256(vec![7; 16]),
        proof_expires_at_secs: 50,
    }
}

fn reservation_ack(pending: &PendingReservation, nonce: u8) -> ReservationAck {
    ReservationAck {
        claims: ReservationAckClaims {
            bindings: pending.claims.bindings.clone(),
            attempt_nonce: pending.claims.attempt_nonce,
            accepted_at_ms: 2_000,
        },
        signature: vec![nonce; 65],
    }
}

fn ticket(pending: &PendingReservation, nonce: u8) -> AssignmentTicket {
    AssignmentTicket {
        claims: AssignmentTicketClaims {
            bindings: pending.claims.bindings.clone(),
            settlement_commitment: pending.settlement_commitment,
            proof_encryption_key_id: pending.key_id,
            issued_at_ms: 2_000,
            expires_at_ms: 50_000,
            nonce: B256::repeat_byte(nonce),
        },
        signature: vec![nonce; 65],
    }
}

#[tokio::test]
async fn admission_is_atomic_and_exactly_idempotent() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let store = repository.proof_orders();
    let order_id = OrderId::from_u128(1);
    let admitted_input = input(order_id);
    assert_eq!(
        store
            .insert_authoritative(&core_order(order_id), &admitted_input)
            .await
            .unwrap(),
        InsertOutcome::Created
    );
    assert_eq!(
        store
            .insert_authoritative(&core_order(order_id), &admitted_input)
            .await
            .unwrap(),
        InsertOutcome::Existing
    );
    assert_eq!(
        store
            .active_workload(
                admitted_input.candidates[0].solver_id,
                admitted_input.created_at_ms
            )
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .active_workload(
                admitted_input.candidates[1].solver_id,
                admitted_input.created_at_ms
            )
            .await
            .unwrap(),
        0
    );
    let mut changed = admitted_input.clone();
    changed.settlement_commitment = B256::repeat_byte(99);
    assert!(matches!(
        store
            .insert_authoritative(&core_order(order_id), &changed)
            .await,
        Err(RepositoryError::IdempotencyConflict)
    ));

    let failed_id = OrderId::from_u128(2);
    let mut malformed = input(failed_id);
    malformed.access_token_hash = B256::repeat_byte(18);
    malformed.candidates[0].encryption_key_id = B256::repeat_byte(99);
    assert!(
        store
            .insert_authoritative(&core_order(failed_id), &malformed)
            .await
            .is_err()
    );
    assert!(repository.get_order(failed_id).await.unwrap().is_none());
    assert!(store.state(failed_id).await.unwrap().is_none());
}

#[tokio::test]
async fn assignment_and_disclosure_have_one_winner_and_one_persisted_delivery() {
    let database = TemporaryDatabase::new("assignment-restart");
    let repository = OrderRepository::connect(&database.url()).await.unwrap();
    let store = repository.proof_orders();
    let order_id = OrderId::from_u128(3);
    let input = input(order_id);
    let solver_id = input.candidates[0].solver_id;
    store
        .insert_authoritative(&core_order(order_id), &input)
        .await
        .unwrap();
    assert!(
        store
            .assigned_delivery(order_id, solver_id)
            .await
            .unwrap()
            .is_none()
    );
    let first = store.clone();
    let second = store.clone();
    let order = assigned_order(order_id, solver_id);
    let pending = store
        .pending_reservation(order_id, solver_id)
        .await
        .unwrap()
        .unwrap();
    let first_ticket = ticket(&pending, 20);
    let second_ticket = ticket(&pending, 21);
    let ack = reservation_ack(&pending, 19);
    let (a, b) = tokio::join!(
        first.assign_and_disclose(&order, 3, solver_id, &ack, &first_ticket, 2_000),
        second.assign_and_disclose(&order, 3, solver_id, &ack, &second_ticket, 2_000),
    );
    assert_eq!(usize::from(a.unwrap()) + usize::from(b.unwrap()), 1);

    let delivery = store
        .assigned_delivery(order_id, solver_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.ciphertext, input.proof.ciphertext);
    assert_eq!(delivery.recipient.solver_id, solver_id);
    assert_eq!(
        store
            .assigned_reservation_ack(order_id, solver_id)
            .await
            .unwrap(),
        Some(ack.clone())
    );
    assert_eq!(
        store.assigned_delivery(order_id, solver_id).await.unwrap(),
        Some(delivery.clone())
    );
    let persisted_ticket = delivery.assignment_ticket.clone();
    assert!(
        assignment_ticket_digest(&persisted_ticket) == assignment_ticket_digest(&first_ticket)
            || assignment_ticket_digest(&persisted_ticket)
                == assignment_ticket_digest(&second_ticket)
    );
    let candidates: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM proof_order_candidates WHERE order_id = ?")
            .bind(order_id.to_string())
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(candidates, 1);

    let other_solver = input.candidates[1].solver_id;
    let other_pending = PendingReservation {
        claims: ReservationRequestClaims {
            bindings: bindings(order_id, other_solver),
            attempt_nonce: B256::repeat_byte(29),
            requested_at_ms: 2_000,
            attempt_expires_at_ms: 4_000,
        },
        terms: terms(),
        domain_hash: input.domain_hash,
        fee_bps: input.fee_bps,
        settlement_commitment: input.settlement_commitment,
        key_id: input.candidates[1].encryption_key_id,
    };
    assert!(
        !store
            .assign_and_disclose(
                &assigned_order(order_id, other_solver),
                3,
                other_solver,
                &reservation_ack(&other_pending, 29),
                &ticket(&other_pending, 30),
                2_001,
            )
            .await
            .unwrap()
    );
    assert!(
        store
            .assigned_delivery(order_id, other_solver)
            .await
            .unwrap()
            .is_none()
    );

    let expected_wire = serde_json::to_vec(&delivery).unwrap();
    store.pool.close().await;
    drop(store);
    drop(repository);

    let restarted = OrderRepository::connect(&database.url()).await.unwrap();
    let restarted_store = restarted.proof_orders();
    let recovered = restarted_store
        .assigned_delivery(order_id, solver_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&recovered).unwrap(),
        expected_wire,
        "an orderbook restart must return the byte-identical assigned delivery"
    );
    assert_eq!(
        restarted_store
            .assigned_reservation_ack(order_id, solver_id)
            .await
            .unwrap(),
        Some(ack),
        "the exact signed reservation ACK must survive the same restart"
    );
    restarted_store.pool.close().await;
}

#[tokio::test]
async fn acceptance_is_irreversible_and_survives_expiry_and_payload_cleanup() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let store = repository.proof_orders();
    let order_id = OrderId::from_u128(4);
    let input = input(order_id);
    let solver_id = input.candidates[0].solver_id;
    store
        .insert_authoritative(&core_order(order_id), &input)
        .await
        .unwrap();
    let assigned = assigned_order(order_id, solver_id);
    let pending = store
        .pending_reservation(order_id, solver_id)
        .await
        .unwrap()
        .unwrap();
    let ticket = ticket(&pending, 22);
    let reservation_ack = reservation_ack(&pending, 21);
    assert!(
        store
            .assign_and_disclose(&assigned, 3, solver_id, &reservation_ack, &ticket, 2_000,)
            .await
            .unwrap()
    );
    let mut expired = assigned;
    expired.state = ProofOrderState::Expired;
    expired.version = 6;
    store
        .expire_with_core_transition(&expired, 5, 50_000)
        .await
        .unwrap();
    assert_eq!(
        store.state(order_id).await.unwrap(),
        Some(ProofOrderState::Expired)
    );
    let ack = ProofAcceptanceAck {
        claims: ProofAcceptanceClaims {
            bindings: pending.claims.bindings.clone(),
            assignment_ticket_digest: assignment_ticket_digest(&ticket),
            settlement_commitment: input.settlement_commitment,
            accepted_at_ms: 4_000,
        },
        signature: vec![24; 65],
    };
    let accepted = SignedProofDecision::Accepted(ack.clone());
    assert!(
        store
            .update_result(order_id, solver_id, &accepted, 4_000)
            .await
            .unwrap()
    );
    assert!(
        store
            .update_result(order_id, solver_id, &accepted, 5_000)
            .await
            .unwrap()
    );
    let mut altered_ack = ack;
    altered_ack.signature[0] ^= 1;
    assert!(
        !store
            .update_result(
                order_id,
                solver_id,
                &SignedProofDecision::Accepted(altered_ack),
                5_000,
            )
            .await
            .unwrap()
    );
    let rejected = SignedProofDecision::Rejected(ProofRejectionAck {
        claims: ProofRejectionClaims {
            bindings: pending.claims.bindings,
            assignment_ticket_digest: assignment_ticket_digest(&ticket),
            reason: ProofRejectionReason::InvalidProof,
            rejected_at_ms: 5_000,
        },
        signature: vec![25; 65],
    });
    assert!(
        !store
            .update_result(order_id, solver_id, &rejected, 5_000)
            .await
            .unwrap()
    );
    assert!(
        store
            .record_operational_failure(
                order_id,
                OperationalFailureKind::Submission,
                "rpc_timeout",
                true,
                6_000,
            )
            .await
            .unwrap()
    );
    assert_eq!(
        store.state(order_id).await.unwrap(),
        Some(ProofOrderState::Expired)
    );
    let cleanup = store.cleanup(400_000, 2_592_000).await.unwrap();
    assert_eq!(cleanup.payloads_erased, 1);
    assert_eq!(
        store.cleanup(400_000, 2_592_000).await.unwrap(),
        CleanupOutcome::default()
    );
    assert!(
        store
            .accountability_evidence(order_id)
            .await
            .unwrap()
            .unwrap()
            .acceptance
            .is_some()
    );
    let complaint_kind = ComplaintEvidenceKind::AcceptedNotSettled;
    let opening = ComplaintEvidenceCipher::new([31; 32])
        .unwrap()
        .encrypt(
            order_id,
            complaint_kind,
            ComplaintSecretOpening {
                nullifier: B256::repeat_byte(32),
                salt: B256::repeat_byte(33),
            },
        )
        .unwrap();
    assert!(
        store
            .insert_complaint(
                order_id,
                complaint_kind,
                &opening,
                ComplaintStatus::Verified,
                "accepted proof was not settled",
                410_000,
                100,
            )
            .await
            .unwrap()
    );
    let complaint = store.complaint(order_id).await.unwrap().unwrap();
    assert_eq!(complaint.status, ComplaintStatus::Verified);
    assert_eq!(complaint.evidence_kind, complaint_kind);
    assert_eq!(store.cleanup(600_000, 1).await.unwrap().orders_erased, 0);
    assert!(repository.get_order(order_id).await.unwrap().is_some());
}

#[tokio::test]
async fn no_response_complaints_require_disclosure_and_encrypt_the_opening_at_rest() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let store = repository.proof_orders();
    let order_id = OrderId::from_u128(5);
    let input = input(order_id);
    let solver_id = input.candidates[0].solver_id;
    store
        .insert_authoritative(&core_order(order_id), &input)
        .await
        .unwrap();
    let pending = store
        .pending_reservation(order_id, solver_id)
        .await
        .unwrap()
        .unwrap();
    store
        .assign_and_disclose(
            &assigned_order(order_id, solver_id),
            3,
            solver_id,
            &reservation_ack(&pending, 34),
            &ticket(&pending, 35),
            2_000,
        )
        .await
        .unwrap();

    let kind = ComplaintEvidenceKind::NoResponseAfterDisclosure;
    let secret = ComplaintSecretOpening {
        nullifier: B256::repeat_byte(36),
        salt: B256::repeat_byte(37),
    };
    let cipher = ComplaintEvidenceCipher::new([38; 32]).unwrap();
    let opening = cipher.encrypt(order_id, kind, secret).unwrap();
    assert!(
        !store
            .insert_complaint(
                order_id,
                kind,
                &opening,
                ComplaintStatus::Verified,
                "premature",
                49_999,
                100,
            )
            .await
            .unwrap()
    );
    assert!(
        !store
            .insert_complaint(
                order_id,
                ComplaintEvidenceKind::AcceptedNotSettled,
                &opening,
                ComplaintStatus::Verified,
                "missing acceptance",
                50_000,
                100,
            )
            .await
            .unwrap()
    );
    assert!(
        store
            .insert_complaint(
                order_id,
                kind,
                &opening,
                ComplaintStatus::Verified,
                "solver did not respond",
                50_000,
                100,
            )
            .await
            .unwrap()
    );
    let complaint = store.complaint(order_id).await.unwrap().unwrap();
    assert_eq!(complaint.evidence_kind, kind);
    assert!(!complaint.nullifier_spent);

    let row = sqlx::query(
        "SELECT evidence_key_id, opening_nonce, opening_ciphertext
             FROM proof_order_complaints WHERE order_id = ?",
    )
    .bind(order_id.to_string())
    .fetch_one(&store.pool)
    .await
    .unwrap();
    let encrypted = EncryptedComplaintOpening {
        key_id: parse_b256("evidence_key_id", row.try_get("evidence_key_id").unwrap()).unwrap(),
        nonce: parse_fixed::<24>("opening_nonce", row.try_get("opening_nonce").unwrap()).unwrap(),
        ciphertext: row.try_get("opening_ciphertext").unwrap(),
    };
    assert_eq!(cipher.decrypt(order_id, kind, &encrypted).unwrap(), secret);

    let columns = sqlx::query("PRAGMA table_info(proof_order_complaints)")
        .fetch_all(&store.pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String, _>("name").unwrap())
        .collect::<Vec<_>>();
    assert!(!columns.iter().any(|column| column == "nullifier"));
    assert!(!columns.iter().any(|column| column == "salt"));
}

#[tokio::test]
async fn resolved_complaint_cleanup_obeys_exact_deadlines_and_legal_holds() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let store = repository.proof_orders();
    let order_id = OrderId::from_u128(6);
    let input = input(order_id);
    let solver_id = input.candidates[0].solver_id;
    store
        .insert_authoritative(&core_order(order_id), &input)
        .await
        .unwrap();
    let pending = store
        .pending_reservation(order_id, solver_id)
        .await
        .unwrap()
        .unwrap();
    store
        .assign_and_disclose(
            &assigned_order(order_id, solver_id),
            3,
            solver_id,
            &reservation_ack(&pending, 39),
            &ticket(&pending, 40),
            2_000,
        )
        .await
        .unwrap();
    let kind = ComplaintEvidenceKind::NoResponseAfterDisclosure;
    let opening = ComplaintEvidenceCipher::new([41; 32])
        .unwrap()
        .encrypt(
            order_id,
            kind,
            ComplaintSecretOpening {
                nullifier: B256::repeat_byte(42),
                salt: B256::repeat_byte(43),
            },
        )
        .unwrap();
    assert!(
        store
            .insert_complaint(
                order_id,
                kind,
                &opening,
                ComplaintStatus::Verified,
                "manual review",
                50_000,
                100,
            )
            .await
            .unwrap()
    );
    assert!(store.resolve_complaint(order_id, 60_000, 10).await.unwrap());
    assert!(
        store
            .set_complaint_legal_hold(order_id, true, 61_000)
            .await
            .unwrap()
    );
    assert_eq!(store.cleanup(69_999, 1).await.unwrap().complaints_erased, 0);
    assert_eq!(store.cleanup(70_000, 1).await.unwrap().complaints_erased, 0);
    assert!(
        store
            .set_complaint_legal_hold(order_id, false, 70_000)
            .await
            .unwrap()
    );
    let cleanup = store.cleanup(70_000, 1).await.unwrap();
    assert_eq!(cleanup.complaints_erased, 1);
    assert_eq!(cleanup.orders_erased, 1);
    assert!(store.complaint(order_id).await.unwrap().is_none());
    assert!(repository.get_order(order_id).await.unwrap().is_none());
    assert_eq!(
        store.cleanup(70_000, 1).await.unwrap(),
        CleanupOutcome::default()
    );
    assert_eq!(
        store.retention_metrics(),
        RetentionMetricsSnapshot {
            cleanup_runs: 4,
            cleanup_failures: 0,
            payloads_erased: 0,
            complaints_erased: 1,
            orders_erased: 1,
        }
    );
}

#[tokio::test]
async fn rejection_evidence_survives_expiry_in_either_arrival_order() {
    for (index, reject_before_expiry) in [true, false].into_iter().enumerate() {
        let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
        let store = repository.proof_orders();
        let order_id = OrderId::from_u128(40 + index as u128);
        let input = input(order_id);
        let solver_id = input.candidates[0].solver_id;
        store
            .insert_authoritative(&core_order(order_id), &input)
            .await
            .unwrap();
        let assigned = assigned_order(order_id, solver_id);
        let pending = store
            .pending_reservation(order_id, solver_id)
            .await
            .unwrap()
            .unwrap();
        let ticket = ticket(&pending, 42);
        assert!(
            store
                .assign_and_disclose(
                    &assigned,
                    3,
                    solver_id,
                    &reservation_ack(&pending, 41),
                    &ticket,
                    2_000,
                )
                .await
                .unwrap()
        );
        let rejection = SignedProofDecision::Rejected(ProofRejectionAck {
            claims: ProofRejectionClaims {
                bindings: pending.claims.bindings,
                assignment_ticket_digest: assignment_ticket_digest(&ticket),
                reason: ProofRejectionReason::InvalidProof,
                rejected_at_ms: 4_000,
            },
            signature: vec![43; 65],
        });

        if reject_before_expiry {
            assert!(
                store
                    .update_result(order_id, solver_id, &rejection, 4_000)
                    .await
                    .unwrap()
            );
            assert_eq!(
                store.state(order_id).await.unwrap(),
                Some(ProofOrderState::ProofRejected)
            );
        }

        let mut expired = assigned;
        expired.state = ProofOrderState::Expired;
        expired.version = 6;
        store
            .expire_with_core_transition(&expired, 5, 50_000)
            .await
            .unwrap();

        if !reject_before_expiry {
            assert!(
                store
                    .update_result(order_id, solver_id, &rejection, 51_000)
                    .await
                    .unwrap()
            );
        }
        assert_eq!(
            store.state(order_id).await.unwrap(),
            Some(ProofOrderState::Expired)
        );
        assert!(
            store
                .accountability_evidence(order_id)
                .await
                .unwrap()
                .unwrap()
                .rejection
                .is_some()
        );
    }
}

#[tokio::test]
async fn reservation_deadlines_recover_and_never_retry_a_solver() {
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let store = repository.proof_orders();
    let order_id = OrderId::from_u128(5);
    let mut routing_input = input(order_id);
    routing_input.access_token_hash = B256::repeat_byte(25);
    store
        .insert_authoritative(&core_order(order_id), &routing_input)
        .await
        .unwrap();
    let first_solver = routing_input.candidates[0].solver_id;
    let pending = store
        .pending_reservation(order_id, first_solver)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !store
            .assign_and_disclose(
                &assigned_order(order_id, first_solver),
                3,
                first_solver,
                &reservation_ack(&pending, 30),
                &ticket(&pending, 31),
                3_000,
            )
            .await
            .unwrap()
    );
    let outcomes = store.expire_due_attempts(3_000, 2_000, 15).await.unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        outcomes[0],
        (
            order_id,
            AdvanceOutcome::Advanced(routing_input.candidates[1].solver_id)
        )
    );
    assert!(
        !store
            .is_target(order_id, routing_input.candidates[0].solver_id)
            .await
            .unwrap()
    );
    assert!(
        store
            .is_target(order_id, routing_input.candidates[1].solver_id)
            .await
            .unwrap()
    );
    let exhausted = store.expire_due_attempts(5_000, 2_000, 15).await.unwrap();
    assert_eq!(exhausted, vec![(order_id, AdvanceOutcome::Exhausted)]);
    assert_eq!(
        store.state(order_id).await.unwrap(),
        Some(ProofOrderState::Closed)
    );
    assert_eq!(
        repository
            .get_order(order_id)
            .await
            .unwrap()
            .unwrap()
            .order
            .state,
        ProofOrderState::Expired
    );
    assert_eq!(store.cleanup(6_000, 1).await.unwrap().orders_erased, 0);
    assert_eq!(store.cleanup(6_001, 1).await.unwrap().orders_erased, 1);
    assert!(repository.get_order(order_id).await.unwrap().is_none());
    assert!(store.state(order_id).await.unwrap().is_none());

    let near_expiry_id = OrderId::from_u128(6);
    let mut near_input = input(near_expiry_id);
    near_input.access_token_hash = B256::repeat_byte(26);
    near_input.terms.expires_at_ms = 18_000;
    let mut near_core = core_order(near_expiry_id);
    near_core.expires_at_ms = Some(18_000);
    store
        .insert_authoritative(&near_core, &near_input)
        .await
        .unwrap();
    assert_eq!(
        store.expire_due_attempts(3_000, 2_000, 15).await.unwrap(),
        vec![(near_expiry_id, AdvanceOutcome::Exhausted)]
    );
    assert!(
        !store
            .is_target(near_expiry_id, near_input.candidates[1].solver_id)
            .await
            .unwrap()
    );
}

#[test]
fn transition_table_is_explicit_and_regression_free() {
    use ProofOrderState::*;

    let states = [
        Submitted,
        ReservationPending,
        Assigned,
        ProofDelivered,
        ProofAccepted,
        ProofRejected,
        Expired,
        ComplaintVerified,
        Closed,
    ];
    let allowed = [
        (Submitted, ReservationPending),
        (Submitted, Expired),
        (Submitted, Closed),
        (ReservationPending, Assigned),
        (ReservationPending, Expired),
        (ReservationPending, Closed),
        (Assigned, ProofDelivered),
        (Assigned, Expired),
        (Assigned, Closed),
        (ProofDelivered, ProofAccepted),
        (ProofDelivered, ProofRejected),
        (ProofDelivered, Expired),
        (ProofDelivered, Closed),
        (ProofAccepted, Expired),
        (ProofAccepted, Closed),
        (ProofRejected, Expired),
        (ProofRejected, Closed),
        (Expired, ComplaintVerified),
        (Expired, Closed),
        (ComplaintVerified, Closed),
    ];

    for source in states {
        for destination in states {
            assert_eq!(
                source.can_transition_to(destination),
                allowed.contains(&(source, destination)),
                "unexpected transition {} -> {}",
                state_name(source),
                state_name(destination),
            );
        }
    }
    assert!(!ProofAccepted.can_transition_to(ProofRejected));
    assert!(!ProofAccepted.can_transition_to(ProofDelivered));
    assert!(!Closed.can_transition_to(Submitted));
}
