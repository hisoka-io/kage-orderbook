use alloy::signers::{SignerSync, local::PrivateKeySigner};
use alloy_primitives::{Address, B256, U256};
use axum::{http::StatusCode, response::IntoResponse};
use kage_types::proof_orders::{
    CreateOrderRequest, PreviewCategory, ProofAcceptanceClaims, ProofOrderBindings,
    ProofRejectionClaims, ProofRejectionReason, ReservationAckClaims, ReservationDeclineClaims,
    ReservationDeclineReason, ReservationRequestClaims,
};
use kage_types::routing::{
    MultiRecipientProof, PreviewResponse, PreviewRoute, RecipientKeyWrap, SolverProofDelivery,
};
use tokio::{net::TcpListener, task::JoinHandle};
use uuid::Uuid;

use super::*;
use crate::{
    config::{AppConfig, ComplaintFinalityPolicy},
    core::engine::{ServiceError, start_orderbook_with_repository_and_policy},
    order::ProofOrderState,
    readiness::ServiceReadiness,
    registry::SolverProfile,
    storage::{OrderRepository, PreviewRepository, PreviewSnapshot},
};

const PROOF_CONFIG: &str = r#"{
      "allowed_solvers": [
        "0x1111111111111111111111111111111111111111",
        "0x2222222222222222222222222222222222222222"
      ],
      "proof_orders": {
        "proof_lifetime_seconds": 30,
        "minimum_remaining_seconds": 15,
        "preview_lifetime_seconds": 15,
        "reservation_attempt_timeout_ms": 2000,
        "max_recipients": 8,
        "preview_cleanup_grace_seconds": 300,
        "ciphertext_cleanup_grace_seconds": 300,
        "complaint_window_seconds": 2592000,
        "evidence_retention_seconds": 2592000,
        "resolved_complaint_retention_seconds": 2592000
      },
      "fee_categories": [{
        "id": "major-50",
        "fee_bps": 50,
        "markets": ["ETH/USDC"],
        "solver_ids": [
          "0x1111111111111111111111111111111111111111",
          "0x2222222222222222222222222222222222222222"
        ]
      }],
      "database": { "max_connections": 1, "busy_timeout_ms": 5000 },
      "runtime": { "command_capacity": 256 },
      "pricing": {
        "max_age_ms": 5000,
        "reconnect_delay_ms": 50,
        "idle_timeout_ms": 1000
      },
      "chains": [{
        "chain_id": 31337,
        "name": "local",
        "darkpool": "0x3Aa5ebB10DC797CAC828524e59A333d0A371443c",
        "registry": "0x0404040404040404040404040404040404040404",
        "registry_deploy_block": 1,
        "confirmations": 0,
        "tokens": [
          {
            "symbol": "ETH",
            "address": "0x0101010101010101010101010101010101010101",
            "decimals": 18,
            "pricing_asset": "ETH",
            "max_price_deviation_bps": 100
          },
          {
            "symbol": "USDC",
            "address": "0x0202020202020202020202020202020202020202",
            "decimals": 6,
            "pricing_asset": "USDC",
            "max_price_deviation_bps": 100
          }
        ],
        "markets": [{
          "token_in": "ETH",
          "token_out": "USDC",
          "movement_allowance_bps": 10,
          "max_price_deviation_bps": 100
        }]
      }]
    }"#;

struct ProofApi {
    url: String,
    server: JoinHandle<()>,
    request: CreateOrderRequest,
    access_token: B256,
    readiness: ServiceReadiness,
    previews: PreviewRepository,
}

struct ResultApi {
    url: String,
    server: JoinHandle<()>,
    signer: PrivateKeySigner,
    token: String,
    order_id: OrderId,
    binding: ProofOrderBinding,
    proof_orders: ProofOrderRepository,
}

struct ReservationReplayApi {
    url: String,
    server: JoinHandle<()>,
    token: String,
    proof_orders: ProofOrderRepository,
}

impl ReservationReplayApi {
    async fn stop(self) {
        self.server.abort();
        let _ = self.server.await;
        drop(self.proof_orders);
        tokio::task::yield_now().await;
    }
}

struct TemporaryApiDatabase {
    path: std::path::PathBuf,
}

impl TemporaryApiDatabase {
    fn new(label: &str) -> Self {
        Self {
            path: std::env::temp_dir()
                .join(format!("kage-orderbook-api-{label}-{}.db", Uuid::new_v4())),
        }
    }

    fn url(&self) -> String {
        format!("sqlite://{}", self.path.display())
    }
}

impl Drop for TemporaryApiDatabase {
    fn drop(&mut self) {
        for suffix in ["", "-shm", "-wal"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}

#[derive(Clone, Copy)]
enum ComplaintDecision {
    None,
    Accepted,
    Rejected,
}

struct ComplaintApi {
    state: ApiState,
    headers: HeaderMap,
    request: CreateComplaintRequest,
    expiry_ms: i64,
    order_id: OrderId,
    proof_orders: ProofOrderRepository,
}

fn assignment_issuer() -> AssignmentIssuer {
    AssignmentIssuer::for_test(PrivateKeySigner::from_slice(&[7; 32]).unwrap())
}

#[derive(Clone, Copy)]
struct ComplaintRpcState {
    spent: bool,
    block_timestamp: u64,
}

async fn complaint_rpc(
    State(state): State<ComplaintRpcState>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let result = match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber" => serde_json::json!({
            "number": "0x2a",
            "hash": B256::repeat_byte(0xb1),
            "timestamp": format!("0x{:x}", state.block_timestamp),
        }),
        "eth_call" => {
            let mut encoded = [0_u8; 32];
            encoded[31] = u8::from(state.spent);
            serde_json::json!(format!("0x{}", alloy_primitives::hex::encode(encoded)))
        }
        method => panic!("unexpected RPC method {method}"),
    };
    Json(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": result,
    }))
}

async fn spawn_complaint_api(decision: ComplaintDecision, spent: bool) -> ComplaintApi {
    spawn_complaint_api_with_block_timestamp(decision, spent, u64::MAX).await
}

async fn spawn_complaint_api_with_block_timestamp(
    decision: ComplaintDecision,
    spent: bool,
    block_timestamp: u64,
) -> ComplaintApi {
    let solver = PrivateKeySigner::from_slice(&[0xa4; 32]).unwrap();
    let solver_id = solver.address();
    let access_token = B256::repeat_byte(0xa5);
    let order_id = Uuid::new_v4();
    let now = now_ms();
    let expiry_ms = i64::try_from((now / 1_000 + 120) * 1_000).unwrap();
    let nullifier = B256::repeat_byte(0xa6);
    let salt = B256::repeat_byte(0xa7);
    let darkpool = Address::repeat_byte(0xa8);
    let domain_hash = B256::repeat_byte(0xad);
    let settlement_commitment =
        settlement_commitment(domain_hash, 31_337, darkpool, nullifier, salt);
    let route = PreviewRoute {
        solver_id,
        min_amount_in: U256::from(1),
        max_amount_in: U256::from(1_000),
        encryption_key_id: B256::repeat_byte(0xa9),
        encryption_public_key: vec![0xaa; 32],
        key_expires_at_ms: expiry_ms + 10_000,
    };
    let ciphertext = vec![0xab; 32];
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let proof_orders = repository.proof_orders();
    let settings = ProofOrderSettings::default();
    let orderbook = start_orderbook_with_repository_and_policy(repository, 256, settings.clone())
        .await
        .unwrap();
    orderbook
        .create_proof_order(NewProofOrder {
            order_id,
            access_token_hash: auth::access_token_hash(access_token),
            preview_id: B256::repeat_byte(0xac),
            category_id: "major-50".to_owned(),
            terms: kage_types::orders::TradeTerms {
                chain_id: 31_337,
                token_in: Address::repeat_byte(1),
                token_out: Address::repeat_byte(2),
                amount_in: U256::from(100),
                amount_out: U256::from(99),
                expires_at_ms: expiry_ms,
            },
            domain_hash,
            fee_bps: 50,
            settlement_commitment,
            proof: MultiRecipientProof {
                suite: PROOF_ENVELOPE_SUITE.to_owned(),
                nonce: vec![0xae; 24],
                ciphertext_digest: alloy_primitives::keccak256(&ciphertext),
                ciphertext,
                recipients: vec![RecipientKeyWrap {
                    solver_id,
                    key_id: route.encryption_key_id,
                    encapsulated_key: vec![0xaf; 32],
                    wrapped_key: vec![0xb0; 48],
                }],
            },
            candidates: vec![route],
            created_at_ms: i64::try_from(now).unwrap(),
            reservation_attempt_timeout_ms: 60_000,
            ciphertext_cleanup_grace_seconds: 300,
        })
        .await
        .unwrap();
    let pending = proof_orders
        .pending_reservation(order_id, solver_id)
        .await
        .unwrap()
        .unwrap();
    let reservation_claims = ReservationAckClaims {
        bindings: pending.claims.bindings.clone(),
        attempt_nonce: pending.claims.attempt_nonce,
        accepted_at_ms: pending.claims.requested_at_ms,
    };
    let reservation_ack = ReservationAck {
        signature: solver
            .sign_message_sync(&reservation_claims.signing_bytes())
            .unwrap()
            .as_bytes()
            .to_vec(),
        claims: reservation_claims,
    };
    let issuer = assignment_issuer();
    let ticket = issuer
        .issue_proof_assignment(
            pending.claims.bindings,
            pending.settlement_commitment,
            pending.key_id,
            now_ms(),
        )
        .unwrap();
    orderbook
        .assign_and_disclose_proof_order(order_id, solver_id, None, reservation_ack, ticket)
        .await
        .unwrap();
    let binding = proof_orders.binding(order_id).await.unwrap().unwrap();
    match decision {
        ComplaintDecision::None => {}
        ComplaintDecision::Accepted => {
            let claims = ProofAcceptanceClaims {
                bindings: binding.bindings.clone(),
                assignment_ticket_digest: binding.assignment_digest,
                settlement_commitment: binding.settlement_commitment,
                accepted_at_ms: binding.disclosed_at_ms,
            };
            let acceptance = ProofAcceptanceAck {
                signature: solver
                    .sign_message_sync(&claims.signing_bytes())
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
                claims,
            };
            assert!(
                orderbook
                    .update_proof_result(
                        order_id,
                        solver_id,
                        SignedProofDecision::Accepted(acceptance),
                    )
                    .await
                    .unwrap()
            );
        }
        ComplaintDecision::Rejected => {
            let claims = ProofRejectionClaims {
                bindings: binding.bindings.clone(),
                assignment_ticket_digest: binding.assignment_digest,
                reason: ProofRejectionReason::InvalidProof,
                rejected_at_ms: binding.disclosed_at_ms,
            };
            let rejection = ProofRejectionAck {
                signature: solver
                    .sign_message_sync(&claims.signing_bytes())
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
                claims,
            };
            assert!(
                orderbook
                    .update_proof_result(
                        order_id,
                        solver_id,
                        SignedProofDecision::Rejected(rejection),
                    )
                    .await
                    .unwrap()
            );
        }
    }

    let rpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc_address = rpc_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            rpc_listener,
            Router::new()
                .route("/", post(complaint_rpc))
                .with_state(ComplaintRpcState {
                    spent,
                    block_timestamp,
                }),
        )
        .await
        .unwrap();
    });
    let mut headers = HeaderMap::new();
    headers.insert(
        ORDER_ACCESS_TOKEN_HEADER,
        access_token.to_string().parse().unwrap(),
    );
    ComplaintApi {
        state: ApiState {
            assignment_issuer: issuer,
            orderbook,
            registry: SolverRegistry::from_profiles([]),
            sessions: SolverSessions::new("kage-orderbook:complaint-api-test"),
            readiness: ServiceReadiness::always_ready(),
            api: ApiSettings::default(),
            preview: None,
            proof_orders: proof_orders.clone(),
            complaint_verifier: Some(ComplaintVerifier::new(
                format!("http://{rpc_address}"),
                darkpool,
                ComplaintFinalityPolicy::Finalized,
            )),
            complaint_evidence_cipher: Some(ComplaintEvidenceCipher::new([0xb3; 32]).unwrap()),
            allowed_solvers: Arc::new(HashSet::from([solver_id])),
            proof_order_settings: settings,
        },
        headers,
        request: CreateComplaintRequest {
            nullifier,
            salt,
            reason: "proof was not settled".to_owned(),
        },
        expiry_ms,
        order_id,
        proof_orders,
    }
}

async fn complaint_response(
    api: &ComplaintApi,
    headers: HeaderMap,
    request: CreateComplaintRequest,
    current_ms: i64,
) -> axum::response::Response {
    match create_complaint_at(
        api.state.clone(),
        api.order_id,
        headers,
        request,
        current_ms,
    )
    .await
    {
        Ok(response) => response.into_response(),
        Err(error) => error.into_response(),
    }
}

async fn spawn_result_api(active: bool, allowed: bool) -> ResultApi {
    let signer = PrivateKeySigner::from_slice(&[0x44; 32]).unwrap();
    let solver_id = signer.address();
    let now = now_ms();
    let proof_expires_at_ms = (now / 1_000 + 25) as i64 * 1_000;
    let order_id = Uuid::new_v4();
    let route = PreviewRoute {
        solver_id,
        min_amount_in: U256::from(1),
        max_amount_in: U256::from(1_000),
        encryption_key_id: B256::repeat_byte(0x45),
        encryption_public_key: vec![0x46; 32],
        key_expires_at_ms: proof_expires_at_ms + 10_000,
    };
    let ciphertext = vec![0x47; 32];
    let settings = ProofOrderSettings::default();
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let proof_orders = repository.proof_orders();
    let orderbook = start_orderbook_with_repository_and_policy(repository, 256, settings.clone())
        .await
        .unwrap();
    orderbook
        .create_proof_order(NewProofOrder {
            order_id,
            access_token_hash: B256::repeat_byte(0x48),
            preview_id: B256::repeat_byte(0x49),
            category_id: "major-50".to_owned(),
            terms: kage_types::orders::TradeTerms {
                chain_id: 31_337,
                token_in: Address::repeat_byte(1),
                token_out: Address::repeat_byte(2),
                amount_in: U256::from(100),
                amount_out: U256::from(99),
                expires_at_ms: proof_expires_at_ms,
            },
            domain_hash: B256::repeat_byte(0x4a),
            fee_bps: 50,
            settlement_commitment: B256::repeat_byte(0x4b),
            proof: MultiRecipientProof {
                suite: PROOF_ENVELOPE_SUITE.to_owned(),
                nonce: vec![0x4c; 24],
                ciphertext_digest: alloy_primitives::keccak256(&ciphertext),
                ciphertext,
                recipients: vec![RecipientKeyWrap {
                    solver_id,
                    key_id: route.encryption_key_id,
                    encapsulated_key: vec![0x4d; 32],
                    wrapped_key: vec![0x4e; 48],
                }],
            },
            candidates: vec![route],
            created_at_ms: now as i64,
            reservation_attempt_timeout_ms: 5_000,
            ciphertext_cleanup_grace_seconds: 300,
        })
        .await
        .unwrap();
    let pending = proof_orders
        .pending_reservation(order_id, solver_id)
        .await
        .unwrap()
        .unwrap();
    let reservation_claims = ReservationAckClaims {
        bindings: pending.claims.bindings.clone(),
        attempt_nonce: pending.claims.attempt_nonce,
        accepted_at_ms: pending.claims.requested_at_ms,
    };
    let reservation_ack = ReservationAck {
        signature: signer
            .sign_message_sync(&reservation_claims.signing_bytes())
            .unwrap()
            .as_bytes()
            .to_vec(),
        claims: reservation_claims,
    };
    let issuer = assignment_issuer();
    let ticket = issuer
        .issue_proof_assignment(
            pending.claims.bindings,
            pending.settlement_commitment,
            pending.key_id,
            now_ms(),
        )
        .unwrap();
    assert!(
        orderbook
            .assign_and_disclose_proof_order(order_id, solver_id, None, reservation_ack, ticket)
            .await
            .unwrap()
    );
    let binding = proof_orders.binding(order_id).await.unwrap().unwrap();
    let sessions = SolverSessions::new("kage-orderbook:proof-result-test");
    let session = sessions.open(solver_id, now_ms());
    let app = router_with_components(
        orderbook,
        SolverRegistry::from_profiles([(
            solver_id,
            SolverProfile {
                noise_public_key: B256::ZERO,
                active,
            },
        )]),
        sessions,
        None,
        proof_orders.clone(),
        None,
        None,
        ServiceReadiness::always_ready(),
        ApiSettings::default(),
        issuer,
        Arc::new(if allowed {
            HashSet::from([solver_id])
        } else {
            HashSet::new()
        }),
        settings,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    ResultApi {
        url: format!("http://{address}"),
        server,
        signer,
        token: session.token,
        order_id,
        binding,
        proof_orders,
    }
}

async fn spawn_reservation_replay_api(
    database_url: &str,
    solver_id: Address,
    input: Option<NewProofOrder>,
) -> ReservationReplayApi {
    let settings = ProofOrderSettings::default();
    let repository = OrderRepository::connect(database_url).await.unwrap();
    let proof_orders = repository.proof_orders();
    let orderbook = start_orderbook_with_repository_and_policy(repository, 256, settings.clone())
        .await
        .unwrap();
    if let Some(input) = input {
        orderbook.create_proof_order(input).await.unwrap();
    }
    let sessions = SolverSessions::new("kage-orderbook:reservation-replay-test");
    let session = sessions.open(solver_id, now_ms());
    let app = router_with_components(
        orderbook,
        SolverRegistry::from_profiles([(
            solver_id,
            SolverProfile {
                noise_public_key: B256::ZERO,
                active: true,
            },
        )]),
        sessions,
        None,
        proof_orders.clone(),
        None,
        None,
        ServiceReadiness::always_ready(),
        ApiSettings::default(),
        assignment_issuer(),
        Arc::new(HashSet::from([solver_id])),
        settings,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    ReservationReplayApi {
        url: format!("http://{address}"),
        server,
        token: session.token,
        proof_orders,
    }
}

fn acceptance_request(fixture: &ResultApi, accepted_at_ms: i64) -> SolverProofDecisionRequest {
    let claims = ProofAcceptanceClaims {
        bindings: fixture.binding.bindings.clone(),
        assignment_ticket_digest: fixture.binding.assignment_digest,
        settlement_commitment: fixture.binding.settlement_commitment,
        accepted_at_ms,
    };
    SolverProofDecisionRequest::ProofAccepted {
        acceptance: ProofAcceptanceAck {
            signature: fixture
                .signer
                .sign_message_sync(&claims.signing_bytes())
                .unwrap()
                .as_bytes()
                .to_vec(),
            claims,
        },
    }
}

fn rejection_request(
    fixture: &ResultApi,
    reason: ProofRejectionReason,
) -> SolverProofDecisionRequest {
    let claims = ProofRejectionClaims {
        bindings: fixture.binding.bindings.clone(),
        assignment_ticket_digest: fixture.binding.assignment_digest,
        reason,
        rejected_at_ms: fixture.binding.disclosed_at_ms,
    };
    SolverProofDecisionRequest::ProofRejected {
        rejection: ProofRejectionAck {
            signature: fixture
                .signer
                .sign_message_sync(&claims.signing_bytes())
                .unwrap()
                .as_bytes()
                .to_vec(),
            claims,
        },
    }
}

fn result_url(fixture: &ResultApi) -> String {
    format!("{}/v1/orders/{}/result", fixture.url, fixture.order_id)
}

async fn spawn_proof_api() -> ProofApi {
    spawn_proof_api_with_settings(ApiSettings::default()).await
}

async fn spawn_proof_api_with_settings(api: ApiSettings) -> ProofApi {
    let config = AppConfig::from_json(PROOF_CONFIG).unwrap();
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let previews = repository.previews();
    let proof_orders = repository.proof_orders();
    let now = now_ms();
    let proof_expires_at_ms = (now / 1_000 + 25) as i64 * 1_000;
    let routes = [
        (Address::repeat_byte(0x11), B256::repeat_byte(0x31)),
        (Address::repeat_byte(0x22), B256::repeat_byte(0x32)),
    ]
    .into_iter()
    .map(|(solver_id, encryption_key_id)| PreviewRoute {
        solver_id,
        min_amount_in: U256::from(1),
        max_amount_in: U256::from(1_000),
        encryption_key_id,
        encryption_public_key: vec![solver_id.as_slice()[0]; 32],
        key_expires_at_ms: proof_expires_at_ms + 10_000,
    })
    .collect::<Vec<_>>();
    let preview_id = B256::repeat_byte(0x41);
    let snapshot = PreviewSnapshot {
        response: PreviewResponse {
            preview_id,
            chain_id: 31_337,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
            token_in_decimals: 18,
            token_out_decimals: 6,
            amount_in: U256::from(100),
            midpoint_amount_out: U256::from(205),
            confidence_amount_out: U256::from(200),
            oracle_adjustment_bps: 244,
            oracle_adjustment_amount: U256::from(5),
            valid_until_ms: now as i64 + 10_000,
            recommended_proof_lifetime_seconds: 30,
            minimum_remaining_seconds: 15,
            categories: vec![PreviewCategory {
                id: "major-50".to_owned(),
                fee_bps: 50,
                exact_amount_out: U256::from(199),
                fee_amount: U256::from(1),
                routes: routes.clone(),
            }],
        },
        price_in_e18: U256::from(2_050),
        price_out_e18: U256::from(1_000),
        price_in_lower_e18: U256::from(2_000),
        price_out_upper_e18: U256::from(1_000),
        pricing_sequence: 1,
        published_at_ms: now as i64,
        created_at_ms: now as i64,
        erase_after_ms: now as i64 + 310_000,
    };
    previews.insert(&snapshot).await.unwrap();
    let preview = PreviewService::admission_only(previews.clone(), proof_orders.clone(), &config);
    let readiness = ServiceReadiness::always_ready();
    let orderbook =
        start_orderbook_with_repository_and_policy(repository, 256, config.proof_orders.clone())
            .await
            .unwrap();
    let app = router_with_components(
        orderbook,
        SolverRegistry::from_profiles([]),
        SolverSessions::new("kage-orderbook:proof-api-test"),
        Some(preview),
        proof_orders,
        None,
        None,
        readiness.clone(),
        api,
        assignment_issuer(),
        Arc::new(config.allowed_solvers.iter().copied().collect()),
        config.proof_orders.clone(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let ciphertext = vec![0x51; 64];
    let request = CreateOrderRequest {
        client_order_id: Uuid::new_v4(),
        access_token_hash: auth::access_token_hash(B256::repeat_byte(0xa1)),
        preview_id,
        category_id: "major-50".to_owned(),
        terms: kage_types::orders::TradeTerms {
            chain_id: 31_337,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
            amount_in: U256::from(100),
            amount_out: U256::from(199),
            expires_at_ms: proof_expires_at_ms,
        },
        domain_hash: crate::proof_domain::proof_domain(
            31_337,
            "0x3Aa5ebB10DC797CAC828524e59A333d0A371443c"
                .parse()
                .unwrap(),
        ),
        settlement_commitment: B256::repeat_byte(0x61),
        encrypted_proof: MultiRecipientProof {
            suite: PROOF_ENVELOPE_SUITE.to_owned(),
            nonce: vec![0x71; 24],
            ciphertext_digest: alloy_primitives::keccak256(&ciphertext),
            ciphertext,
            recipients: routes
                .into_iter()
                .map(|route| RecipientKeyWrap {
                    solver_id: route.solver_id,
                    key_id: route.encryption_key_id,
                    encapsulated_key: vec![0x81; 32],
                    wrapped_key: vec![0x91; 48],
                })
                .collect(),
        },
    };
    ProofApi {
        url: format!("http://{address}"),
        server,
        request,
        access_token: B256::repeat_byte(0xa1),
        readiness,
        previews,
    }
}

async fn post_proof_json(fixture: &ProofApi, request: &CreateOrderRequest) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}/v1/orders", fixture.url))
        .json(request)
        .send()
        .await
        .unwrap()
}

#[test]
fn admission_errors_have_stable_retryable_http_contracts() {
    let (status, Json(response)) =
        super::error::api_error_for_service(ServiceError::RouteCapacityChanged);
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(response.code, "route_capacity_changed");
    assert_eq!(
        response.message,
        "solver capacity changed; request a fresh preview and retry"
    );
    assert!(response.missing.is_empty());

    let (status, Json(response)) =
        super::error::api_error_for_service(ServiceError::AdmissionUnavailable);
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.code, "admission_unavailable");
    assert_eq!(
        response.message,
        "order admission is temporarily unavailable; retry later"
    );
    assert!(response.missing.is_empty());

    let (status, Json(response)) =
        super::error::api_error_for_service(ServiceError::ProofDeadlineChanged);
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response.code, "invalid_proof_deadline");
    assert!(response.missing.is_empty());

    let (status, Json(response)) =
        super::error::api_error_for_service(ServiceError::PreviewExpired);
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(response.code, "preview_expired");
    assert!(response.missing.is_empty());
}

#[tokio::test]
async fn proof_order_contract_supports_json_and_messagepack_only_at_orders() {
    let fixture = spawn_proof_api().await;
    let client = reqwest::Client::new();

    let created = post_proof_json(&fixture, &fixture.request).await;
    assert_eq!(created.status(), StatusCode::CREATED);
    assert!(created.json::<CreateOrderResponse>().await.unwrap().created);

    let order_url = format!(
        "{}/v1/orders/{}",
        fixture.url, fixture.request.client_order_id
    );
    assert_eq!(
        client
            .get(&order_url)
            .header(ORDER_ACCESS_TOKEN_HEADER, fixture.access_token.to_string())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .get(&order_url)
            .header(
                ORDER_ACCESS_TOKEN_HEADER,
                fixture.request.access_token_hash.to_string()
            )
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND,
        "the stored verifier must not work as a bearer credential"
    );

    let raw_on_create = client
        .post(format!("{}/v1/orders", fixture.url))
        .header(ORDER_ACCESS_TOKEN_HEADER, fixture.access_token.to_string())
        .json(&fixture.request)
        .send()
        .await
        .unwrap();
    assert_eq!(raw_on_create.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        raw_on_create.json::<ApiErrorResponse>().await.unwrap().code,
        "raw_access_token_forbidden"
    );

    let encoded = rmp_serde::to_vec_named(&fixture.request).unwrap();
    let replay = client
        .post(format!("{}/v1/orders", fixture.url))
        .header(CONTENT_TYPE, "application/msgpack")
        .body(encoded)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert!(!replay.json::<CreateOrderResponse>().await.unwrap().created);

    fixture.server.abort();
}

#[tokio::test]
async fn assigned_delivery_replay_is_byte_identical_after_orderbook_restart() {
    let database = TemporaryApiDatabase::new("assigned-delivery-replay");
    let signer = PrivateKeySigner::from_slice(&[0xc1; 32]).unwrap();
    let solver_id = signer.address();
    let order_id = Uuid::new_v4();
    let created_at_ms = i64::try_from(now_ms()).unwrap();
    let proof_expires_at_ms = (created_at_ms / 1_000 + 120) * 1_000;
    let route = PreviewRoute {
        solver_id,
        min_amount_in: U256::from(1),
        max_amount_in: U256::from(1_000),
        encryption_key_id: B256::repeat_byte(0xc2),
        encryption_public_key: vec![0xc3; 32],
        key_expires_at_ms: proof_expires_at_ms + 10_000,
    };
    let ciphertext = vec![0xc4; 64];
    let input = NewProofOrder {
        order_id,
        access_token_hash: B256::repeat_byte(0xc5),
        preview_id: B256::repeat_byte(0xc6),
        category_id: "major-50".to_owned(),
        terms: kage_types::orders::TradeTerms {
            chain_id: 31_337,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
            amount_in: U256::from(100),
            amount_out: U256::from(99),
            expires_at_ms: proof_expires_at_ms,
        },
        domain_hash: B256::repeat_byte(0xc7),
        fee_bps: 50,
        settlement_commitment: B256::repeat_byte(0xc8),
        proof: MultiRecipientProof {
            suite: PROOF_ENVELOPE_SUITE.to_owned(),
            nonce: vec![0xc9; 24],
            ciphertext_digest: alloy_primitives::keccak256(&ciphertext),
            ciphertext,
            recipients: vec![RecipientKeyWrap {
                solver_id,
                key_id: route.encryption_key_id,
                encapsulated_key: vec![0xca; 32],
                wrapped_key: vec![0xcb; 48],
            }],
        },
        candidates: vec![route],
        created_at_ms,
        reservation_attempt_timeout_ms: 60_000,
        ciphertext_cleanup_grace_seconds: 300,
    };
    let first = spawn_reservation_replay_api(&database.url(), solver_id, Some(input)).await;
    let pending = first
        .proof_orders
        .pending_reservation(order_id, solver_id)
        .await
        .unwrap()
        .unwrap();
    let claims = ReservationAckClaims {
        bindings: pending.claims.bindings,
        attempt_nonce: pending.claims.attempt_nonce,
        accepted_at_ms: i64::try_from(now_ms()).unwrap(),
    };
    let ack = ReservationAck {
        signature: signer
            .sign_message_sync(&claims.signing_bytes())
            .unwrap()
            .as_bytes()
            .to_vec(),
        claims,
    };
    let ack_body = serde_json::to_vec(&ack).unwrap();
    let client = reqwest::Client::new();
    let reserve_url = format!("{}/v1/orders/{order_id}/reserve", first.url);
    let first_response = client
        .post(&reserve_url)
        .bearer_auth(&first.token)
        .header(CONTENT_TYPE, "application/json")
        .body(ack_body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(first_response.status(), StatusCode::OK);
    let expected_delivery = first_response.bytes().await.unwrap();
    let _: SolverProofDelivery = serde_json::from_slice(&expected_delivery).unwrap();
    first.stop().await;

    let restarted = spawn_reservation_replay_api(&database.url(), solver_id, None).await;
    let reserve_url = format!("{}/v1/orders/{order_id}/reserve", restarted.url);
    let replay = client
        .post(&reserve_url)
        .bearer_auth(&restarted.token)
        .header(CONTENT_TYPE, "application/json")
        .body(ack_body)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    let replayed_delivery = replay.bytes().await.unwrap();
    assert!(
        replayed_delivery == expected_delivery,
        "the HTTP replay must return the byte-identical persisted delivery"
    );

    let mut altered_claims = ack.claims;
    altered_claims.accepted_at_ms = altered_claims.accepted_at_ms.saturating_add(1);
    while i64::try_from(now_ms()).unwrap() < altered_claims.accepted_at_ms {
        tokio::task::yield_now().await;
    }
    let altered_ack = ReservationAck {
        signature: signer
            .sign_message_sync(&altered_claims.signing_bytes())
            .unwrap()
            .as_bytes()
            .to_vec(),
        claims: altered_claims,
    };
    assert_eq!(
        client
            .post(&reserve_url)
            .bearer_auth(&restarted.token)
            .header(CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&altered_ack).unwrap())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    restarted.stop().await;
}

#[tokio::test]
async fn proof_acceptance_endpoint_requires_signed_evidence_and_conflicts_on_altered_retry() {
    let fixture = spawn_result_api(true, true).await;
    let client = reqwest::Client::new();
    let request = acceptance_request(&fixture, fixture.binding.disclosed_at_ms);
    let mut unsigned = serde_json::to_value(&request).unwrap();
    unsigned["acceptance"]
        .as_object_mut()
        .unwrap()
        .remove("signature");

    assert_eq!(
        client
            .post(result_url(&fixture))
            .bearer_auth(&fixture.token)
            .json(&unsigned)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );

    for _ in 0..2 {
        assert_eq!(
            client
                .post(result_url(&fixture))
                .bearer_auth(&fixture.token)
                .json(&request)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
    }
    assert_eq!(
        fixture.proof_orders.state(fixture.order_id).await.unwrap(),
        Some(ProofOrderState::ProofAccepted)
    );

    while now_ms() as i64 <= fixture.binding.disclosed_at_ms {
        tokio::task::yield_now().await;
    }
    let altered = acceptance_request(&fixture, fixture.binding.disclosed_at_ms + 1);
    assert_eq!(
        client
            .post(result_url(&fixture))
            .bearer_auth(&fixture.token)
            .json(&altered)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    fixture.server.abort();
}

#[tokio::test]
async fn proof_rejection_endpoint_supports_messagepack_and_exact_retries_only() {
    let fixture = spawn_result_api(true, true).await;
    let client = reqwest::Client::new();
    let request = rejection_request(&fixture, ProofRejectionReason::InvalidProof);
    let mut unsigned = serde_json::to_value(&request).unwrap();
    unsigned["rejection"]
        .as_object_mut()
        .unwrap()
        .remove("signature");
    assert_eq!(
        client
            .post(result_url(&fixture))
            .bearer_auth(&fixture.token)
            .json(&unsigned)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let encoded = rmp_serde::to_vec_named(&request).unwrap();

    for _ in 0..2 {
        assert_eq!(
            client
                .post(result_url(&fixture))
                .bearer_auth(&fixture.token)
                .header(CONTENT_TYPE, "application/msgpack")
                .body(encoded.clone())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
    }
    assert_eq!(
        fixture.proof_orders.state(fixture.order_id).await.unwrap(),
        Some(ProofOrderState::ProofRejected)
    );

    let altered = rejection_request(&fixture, ProofRejectionReason::PricingUnsafe);
    assert_eq!(
        client
            .post(result_url(&fixture))
            .bearer_auth(&fixture.token)
            .header(CONTENT_TYPE, "application/msgpack")
            .body(rmp_serde::to_vec_named(&altered).unwrap())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    fixture.server.abort();
}

#[tokio::test]
async fn proof_result_endpoint_rechecks_registry_and_allowlist_for_existing_sessions() {
    for (active, allowed) in [(false, true), (true, false)] {
        let fixture = spawn_result_api(active, allowed).await;
        let request = rejection_request(&fixture, ProofRejectionReason::InvalidProof);
        assert_eq!(
            reqwest::Client::new()
                .post(result_url(&fixture))
                .bearer_auth(&fixture.token)
                .json(&request)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        fixture.server.abort();
    }
}

#[tokio::test]
async fn proof_order_admission_rejects_every_mutable_binding() {
    let fixture = spawn_proof_api().await;
    let mut mutations = Vec::new();

    let mut category = fixture.request.clone();
    category.category_id = "other-50".to_owned();
    mutations.push(category);

    let mut output = fixture.request.clone();
    output.terms.amount_out += U256::from(1);
    mutations.push(output);

    let mut expiry = fixture.request.clone();
    expiry.terms.expires_at_ms += 1;
    mutations.push(expiry);

    let mut missing_recipient = fixture.request.clone();
    missing_recipient.encrypted_proof.recipients.pop();
    mutations.push(missing_recipient);

    let mut duplicate_recipient = fixture.request.clone();
    duplicate_recipient.encrypted_proof.recipients[1] =
        duplicate_recipient.encrypted_proof.recipients[0].clone();
    mutations.push(duplicate_recipient);

    let mut digest = fixture.request.clone();
    digest.encrypted_proof.ciphertext_digest = B256::repeat_byte(1);
    mutations.push(digest);

    let mut domain = fixture.request.clone();
    domain.domain_hash = B256::repeat_byte(2);
    mutations.push(domain);

    let mut commitment = fixture.request.clone();
    commitment.settlement_commitment = B256::ZERO;
    mutations.push(commitment);

    let mut wrap = fixture.request.clone();
    wrap.encrypted_proof.recipients[0].wrapped_key.pop();
    mutations.push(wrap);

    let mut nonce = fixture.request.clone();
    nonce.encrypted_proof.nonce.fill(0);
    mutations.push(nonce);

    let mut encapsulated_key = fixture.request.clone();
    encapsulated_key.encrypted_proof.recipients[0]
        .encapsulated_key
        .fill(0);
    mutations.push(encapsulated_key);

    for mutation in mutations {
        assert_eq!(
            post_proof_json(&fixture, &mutation).await.status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    assert_eq!(
        post_proof_json(&fixture, &fixture.request).await.status(),
        StatusCode::CREATED
    );
    let mut conflict = fixture.request.clone();
    conflict.settlement_commitment = B256::repeat_byte(0x62);
    let response = post_proof_json(&fixture, &conflict).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        response.json::<ApiErrorResponse>().await.unwrap().code,
        "idempotency_conflict"
    );
    fixture.server.abort();
}

#[tokio::test]
async fn readiness_blocks_only_new_proof_order_admission() {
    let fixture = spawn_proof_api().await;
    fixture.readiness.set_chain(false);
    let blocked = post_proof_json(&fixture, &fixture.request).await;
    assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        blocked.json::<ApiErrorResponse>().await.unwrap().code,
        "service_not_ready"
    );

    fixture.readiness.set_chain(true);
    assert_eq!(
        post_proof_json(&fixture, &fixture.request).await.status(),
        StatusCode::CREATED
    );
    fixture.readiness.set_chain(false);
    assert_eq!(
        post_proof_json(&fixture, &fixture.request).await.status(),
        StatusCode::OK
    );
    fixture.server.abort();
}

#[tokio::test]
async fn exact_retry_survives_preview_cleanup_and_readiness_loss() {
    let fixture = spawn_proof_api().await;
    assert_eq!(
        post_proof_json(&fixture, &fixture.request).await.status(),
        StatusCode::CREATED
    );
    assert_eq!(fixture.previews.cleanup(i64::MAX).await.unwrap(), 1);
    fixture.readiness.set_chain(false);
    let replay = post_proof_json(&fixture, &fixture.request).await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert!(!replay.json::<CreateOrderResponse>().await.unwrap().created);
    fixture.server.abort();
}

#[tokio::test]
async fn proof_request_and_ciphertext_limits_are_enforced_independently() {
    let request_limited = spawn_proof_api_with_settings(ApiSettings {
        max_order_request_bytes: 256,
        max_ciphertext_bytes: 128,
        ..ApiSettings::default()
    })
    .await;
    assert_eq!(
        post_proof_json(&request_limited, &request_limited.request)
            .await
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    request_limited.server.abort();

    let ciphertext_limited = spawn_proof_api_with_settings(ApiSettings {
        max_ciphertext_bytes: 32,
        ..ApiSettings::default()
    })
    .await;
    assert_eq!(
        post_proof_json(&ciphertext_limited, &ciphertext_limited.request)
            .await
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    ciphertext_limited.server.abort();
}

#[test]
fn proof_expiry_is_the_exact_order_deadline_in_seconds() {
    let policy = ProofOrderSettings {
        proof_lifetime_seconds: 20,
        minimum_remaining_seconds: 7,
        ..ProofOrderSettings::default()
    };
    assert!(proof_deadline_is_admissible(108_000, 100_000, &policy));
    assert!(proof_deadline_is_admissible(120_000, 100_000, &policy));
    assert!(!proof_deadline_is_admissible(107_000, 100_000, &policy));
    assert!(!proof_deadline_is_admissible(120_001, 100_000, &policy));
    assert!(!proof_deadline_is_admissible(121_000, 100_000, &policy));
    assert_eq!(
        expected_proof_expiry_secs(1_800_000_000_000),
        Some(1_800_000_000)
    );
    assert_eq!(expected_proof_expiry_secs(1_800_000_000_001), None);
    assert_eq!(expected_proof_expiry_secs(-1_000), None);
}

#[test]
fn candidate_rotation_is_deterministic_and_distributes_first_choice() {
    let routes = vec![
        PreviewRoute {
            solver_id: Address::repeat_byte(1),
            min_amount_in: U256::from(1),
            max_amount_in: U256::from(10),
            encryption_key_id: B256::repeat_byte(1),
            encryption_public_key: vec![1; 32],
            key_expires_at_ms: 10_000,
        },
        PreviewRoute {
            solver_id: Address::repeat_byte(2),
            min_amount_in: U256::from(1),
            max_amount_in: U256::from(10),
            encryption_key_id: B256::repeat_byte(2),
            encryption_public_key: vec![2; 32],
            key_expires_at_ms: 10_000,
        },
    ];
    let mut first = routes.clone();
    let mut first_again = routes.clone();
    let mut second = routes;
    rotate_candidates(&mut first, Uuid::from_u128(0));
    rotate_candidates(&mut first_again, Uuid::from_u128(0));
    rotate_candidates(&mut second, Uuid::from_u128(1_u128 << 64));
    assert_eq!(first, first_again);
    assert_ne!(first[0].solver_id, second[0].solver_id);
}

#[tokio::test]
async fn complaint_api_returns_both_evidence_classes() {
    for (decision, expected) in [
        (
            ComplaintDecision::None,
            ComplaintEvidenceKind::NoResponseAfterDisclosure,
        ),
        (
            ComplaintDecision::Accepted,
            ComplaintEvidenceKind::AcceptedNotSettled,
        ),
    ] {
        let api = spawn_complaint_api(decision, false).await;
        let response = complaint_response(
            &api,
            api.headers.clone(),
            api.request.clone(),
            api.expiry_ms,
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), 16 * 1024)
            .await
            .unwrap();
        let complaint: ComplaintResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(complaint.evidence_kind, expected);
        assert_eq!(complaint.status, ComplaintStatus::Verified);
        assert!(!complaint.nullifier_spent);
        assert_eq!(
            api.proof_orders
                .complaint(api.order_id)
                .await
                .unwrap()
                .unwrap()
                .evidence_kind,
            expected
        );
    }
}

#[tokio::test]
async fn complaint_api_rejects_unauthorized_premature_mismatched_expired_and_spent_claims() {
    let api = spawn_complaint_api(ComplaintDecision::None, false).await;
    let mut unauthorized = api.headers.clone();
    unauthorized.insert(
        ORDER_ACCESS_TOKEN_HEADER,
        B256::repeat_byte(0xff).to_string().parse().unwrap(),
    );
    assert_eq!(
        complaint_response(&api, unauthorized, api.request.clone(), api.expiry_ms,)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        complaint_response(
            &api,
            api.headers.clone(),
            api.request.clone(),
            api.expiry_ms - 1,
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    let mut mismatched = api.request.clone();
    mismatched.nullifier = B256::repeat_byte(0xfe);
    assert_eq!(
        complaint_response(&api, api.headers.clone(), mismatched, api.expiry_ms)
            .await
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let after_window = api.expiry_ms.saturating_add(
        i64::try_from(api.state.proof_order_settings.complaint_window_seconds)
            .unwrap()
            .saturating_mul(1_000),
    ) + 1;
    assert_eq!(
        complaint_response(&api, api.headers.clone(), api.request.clone(), after_window,)
            .await
            .status(),
        StatusCode::GONE
    );
    assert!(
        api.proof_orders
            .complaint(api.order_id)
            .await
            .unwrap()
            .is_none()
    );

    let spent = spawn_complaint_api(ComplaintDecision::Accepted, true).await;
    assert_eq!(
        complaint_response(
            &spent,
            spent.headers.clone(),
            spent.request.clone(),
            spent.expiry_ms,
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    assert!(
        spent
            .proof_orders
            .complaint(spent.order_id)
            .await
            .unwrap()
            .is_none()
    );

    let rejected = spawn_complaint_api(ComplaintDecision::Rejected, false).await;
    assert_eq!(
        complaint_response(
            &rejected,
            rejected.headers.clone(),
            rejected.request.clone(),
            rejected.expiry_ms,
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    assert!(
        rejected
            .proof_orders
            .complaint(rejected.order_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn complaint_api_fails_closed_without_a_post_expiry_verification_block() {
    let api = spawn_complaint_api_with_block_timestamp(ComplaintDecision::None, false, 0).await;
    assert_eq!(
        api.proof_orders.state(api.order_id).await.unwrap(),
        Some(ProofOrderState::ProofDelivered)
    );

    let response = complaint_response(
        &api,
        api.headers.clone(),
        api.request.clone(),
        api.expiry_ms,
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), 16 * 1024)
        .await
        .unwrap();
    let error: ApiErrorResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(error.code, "chain_check_unavailable");
    assert!(
        api.proof_orders
            .complaint(api.order_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        api.proof_orders.state(api.order_id).await.unwrap(),
        Some(ProofOrderState::ProofDelivered)
    );
}

#[test]
fn reservation_evidence_requires_exact_live_bindings_and_solver_signature() {
    let solver = PrivateKeySigner::from_slice(&[3; 32]).unwrap();
    let bindings = ProofOrderBindings {
        order_id: Uuid::from_u128(1),
        preview_id: B256::repeat_byte(2),
        category_id: "major-50".to_owned(),
        solver_id: solver.address(),
        exact_terms_digest: B256::repeat_byte(4),
        ciphertext_digest: B256::repeat_byte(5),
        proof_expires_at_secs: 20,
    };
    let pending = PendingReservation {
        claims: ReservationRequestClaims {
            bindings: bindings.clone(),
            attempt_nonce: B256::repeat_byte(6),
            requested_at_ms: 10_000,
            attempt_expires_at_ms: 12_000,
        },
        terms: kage_types::orders::TradeTerms {
            chain_id: 31_337,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
            amount_in: U256::from(10),
            amount_out: U256::from(9),
            expires_at_ms: 20_000,
        },
        domain_hash: B256::repeat_byte(7),
        fee_bps: 50,
        settlement_commitment: B256::repeat_byte(8),
        key_id: B256::repeat_byte(9),
    };
    let ack_claims = ReservationAckClaims {
        bindings: bindings.clone(),
        attempt_nonce: pending.claims.attempt_nonce,
        accepted_at_ms: 11_000,
    };
    let ack = ReservationAck {
        signature: solver
            .sign_message_sync(&ack_claims.signing_bytes())
            .unwrap()
            .as_bytes()
            .to_vec(),
        claims: ack_claims,
    };
    assert!(reservation_ack_is_valid(
        &ack,
        &pending,
        solver.address(),
        11_500
    ));
    let mut altered = ack.clone();
    altered.claims.bindings.ciphertext_digest = B256::repeat_byte(10);
    assert!(!reservation_ack_is_valid(
        &altered,
        &pending,
        solver.address(),
        11_500
    ));
    assert!(!reservation_ack_is_valid(
        &ack,
        &pending,
        solver.address(),
        12_000
    ));

    let decline_claims = ReservationDeclineClaims {
        bindings,
        attempt_nonce: pending.claims.attempt_nonce,
        reason: ReservationDeclineReason::InsufficientLiquidity,
        declined_at_ms: 11_000,
    };
    let decline = ReservationDecline {
        signature: solver
            .sign_message_sync(&decline_claims.signing_bytes())
            .unwrap()
            .as_bytes()
            .to_vec(),
        claims: decline_claims,
    };
    assert!(reservation_decline_is_valid(
        &decline,
        &pending,
        solver.address(),
        11_500
    ));
}

#[test]
fn proof_decisions_require_exact_signed_delivery_bindings_and_stable_wire_bytes() {
    let solver = PrivateKeySigner::from_slice(&[4; 32]).unwrap();
    let bindings = ProofOrderBindings {
        order_id: Uuid::from_u128(2),
        preview_id: B256::repeat_byte(3),
        category_id: "major-50".to_owned(),
        solver_id: solver.address(),
        exact_terms_digest: B256::repeat_byte(5),
        ciphertext_digest: B256::repeat_byte(6),
        proof_expires_at_secs: 20,
    };
    let binding = ProofOrderBinding {
        bindings: bindings.clone(),
        domain_hash: B256::repeat_byte(9),
        settlement_commitment: B256::repeat_byte(7),
        assignment_digest: B256::repeat_byte(8),
        disclosed_at_ms: 10_000,
    };
    let acceptance_claims = ProofAcceptanceClaims {
        bindings: bindings.clone(),
        assignment_ticket_digest: binding.assignment_digest,
        settlement_commitment: binding.settlement_commitment,
        accepted_at_ms: 11_000,
    };
    let acceptance = ProofAcceptanceAck {
        signature: solver
            .sign_message_sync(&acceptance_claims.signing_bytes())
            .unwrap()
            .as_bytes()
            .to_vec(),
        claims: acceptance_claims,
    };
    assert!(proof_acceptance_is_valid(
        &acceptance,
        &binding,
        solver.address(),
        12_000,
    ));
    let mut altered = acceptance.clone();
    altered.claims.settlement_commitment = B256::repeat_byte(9);
    assert!(!proof_acceptance_is_valid(
        &altered,
        &binding,
        solver.address(),
        12_000,
    ));

    let rejection_claims = ProofRejectionClaims {
        bindings,
        assignment_ticket_digest: binding.assignment_digest,
        reason: ProofRejectionReason::Expired,
        rejected_at_ms: 21_000,
    };
    let rejection = ProofRejectionAck {
        signature: solver
            .sign_message_sync(&rejection_claims.signing_bytes())
            .unwrap()
            .as_bytes()
            .to_vec(),
        claims: rejection_claims,
    };
    assert!(proof_rejection_is_valid(
        &rejection,
        &binding,
        solver.address(),
        22_000,
    ));

    let request = SolverProofDecisionRequest::ProofRejected { rejection };
    let json = serde_json::to_vec(&request).unwrap();
    let message_pack = rmp_serde::to_vec_named(&request).unwrap();
    assert_eq!(
        serde_json::from_slice::<SolverProofDecisionRequest>(&json).unwrap(),
        request,
    );
    assert_eq!(
        rmp_serde::from_slice::<SolverProofDecisionRequest>(&message_pack).unwrap(),
        request,
    );
    assert!(
        serde_json::from_value::<SolverProofDecisionRequest>(serde_json::json!({
            "result": "proof_rejected"
        }))
        .is_err()
    );
}
