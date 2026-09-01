use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, B256};
use axum::{Json, Router, extract::State, routing::post};
use kage_types::{identifiers::OrderId, proof_orders::ComplaintEvidenceKind};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use super::{verifier::parse_quantity, *};
use crate::config::ComplaintFinalityPolicy;

#[derive(Clone)]
struct RpcState {
    requests: Arc<Mutex<Vec<Value>>>,
    spent: bool,
    head: u64,
    finalized: Option<TestBlock>,
    confirmed: Option<TestBlock>,
}

#[derive(Clone, Copy)]
struct TestBlock {
    number: u64,
    hash: B256,
    timestamp: u64,
}

impl TestBlock {
    fn json(self) -> Value {
        json!({
            "number": format!("0x{:x}", self.number),
            "hash": self.hash,
            "timestamp": format!("0x{:x}", self.timestamp),
        })
    }
}

async fn rpc_handler(State(state): State<RpcState>, Json(request): Json<Value>) -> Json<Value> {
    state.requests.lock().unwrap().push(request.clone());
    let result = match request["method"].as_str().unwrap() {
        "eth_blockNumber" => json!(format!("0x{:x}", state.head)),
        "eth_getBlockByNumber" => {
            let selector = request["params"][0].as_str().unwrap();
            let block = if selector == "finalized" {
                state.finalized
            } else {
                state
                    .confirmed
                    .filter(|block| selector == format!("0x{:x}", block.number))
            };
            block.map_or(Value::Null, TestBlock::json)
        }
        "eth_call" => {
            let mut encoded = [0_u8; 32];
            encoded[31] = u8::from(state.spent);
            json!(format!("0x{}", alloy_primitives::hex::encode(encoded)))
        }
        method => panic!("unexpected RPC method {method}"),
    };
    Json(json!({"jsonrpc": "2.0", "id": 1, "result": result}))
}

async fn rpc_server(state: RpcState) -> (String, Arc<Mutex<Vec<Value>>>) {
    let requests = state.requests.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/", post(rpc_handler))
        .with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), requests)
}

fn rpc_state(spent: bool, finalized: TestBlock, confirmed: TestBlock) -> RpcState {
    let requests = Arc::new(Mutex::new(Vec::new()));
    RpcState {
        requests,
        spent,
        head: 100,
        finalized: Some(finalized),
        confirmed: Some(confirmed),
    }
}

#[test]
fn complaint_openings_are_authenticated_and_bound_to_order_and_kind() {
    let cipher = ComplaintEvidenceCipher::new([7; 32]).unwrap();
    let order_id = OrderId::from_u128(1);
    let opening = ComplaintSecretOpening {
        nullifier: B256::repeat_byte(2),
        salt: B256::repeat_byte(3),
    };
    let encrypted = cipher
        .encrypt(order_id, ComplaintEvidenceKind::AcceptedNotSettled, opening)
        .unwrap();
    assert_eq!(encrypted.ciphertext.len(), 80);
    assert_ne!(&encrypted.ciphertext[..32], opening.nullifier.as_slice());
    assert_eq!(
        cipher
            .decrypt(
                order_id,
                ComplaintEvidenceKind::AcceptedNotSettled,
                &encrypted,
            )
            .unwrap(),
        opening
    );
    assert_eq!(
        cipher.decrypt(
            OrderId::from_u128(2),
            ComplaintEvidenceKind::AcceptedNotSettled,
            &encrypted,
        ),
        Err(ComplaintEvidenceError::Decryption)
    );
    assert_eq!(
        cipher.decrypt(
            order_id,
            ComplaintEvidenceKind::NoResponseAfterDisclosure,
            &encrypted,
        ),
        Err(ComplaintEvidenceError::Decryption)
    );
}

#[test]
fn evidence_keys_are_validated_and_never_rendered() {
    assert!(matches!(
        ComplaintEvidenceCipher::new([0; 32]),
        Err(ComplaintEvidenceError::InvalidKey)
    ));
    let cipher = ComplaintEvidenceCipher::new([9; 32]).unwrap();
    assert!(!format!("{cipher:?}").contains(&"09".repeat(32)));
}

#[test]
fn parses_only_canonical_hex_quantities() {
    assert_eq!(parse_quantity("0x0"), Some(0));
    assert_eq!(parse_quantity("0x2a"), Some(42));
    assert_eq!(parse_quantity("42"), None);
    assert_eq!(parse_quantity("0xzz"), None);
}

#[tokio::test]
async fn finalized_policy_pins_the_call_to_the_canonical_finalized_hash() {
    let block = TestBlock {
        number: 42,
        hash: B256::repeat_byte(0x11),
        timestamp: 1_000,
    };
    let (url, requests) = rpc_server(rpc_state(false, block, block)).await;
    let verifier = ComplaintVerifier::new(
        url,
        Address::repeat_byte(7),
        ComplaintFinalityPolicy::Finalized,
    );
    let verified = verifier
        .is_nullifier_spent(B256::repeat_byte(8), 1_000)
        .await
        .unwrap();
    assert_eq!(
        verified,
        VerifiedNullifierStatus {
            spent: false,
            block_number: 42,
            block_hash: B256::repeat_byte(0x11),
            block_timestamp: 1_000,
        }
    );
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["method"], "eth_getBlockByNumber");
    assert_eq!(requests[0]["params"], json!(["finalized", false]));
    assert_eq!(requests[1]["method"], "eth_call");
    assert_eq!(
        requests[1]["params"][1],
        json!({
            "blockHash": B256::repeat_byte(0x11),
            "requireCanonical": true,
        })
    );
}

#[tokio::test]
async fn stale_finalized_fails_closed_while_current_confirmed_state_sees_the_spend() {
    let finalized = TestBlock {
        number: 42,
        hash: B256::repeat_byte(0x11),
        timestamp: 999,
    };
    let confirmed = TestBlock {
        number: 94,
        hash: B256::repeat_byte(0x22),
        timestamp: 1_000,
    };
    let finalized_state = rpc_state(true, finalized, confirmed);
    let finalized_requests = finalized_state.requests.clone();
    let (url, _) = rpc_server(finalized_state).await;
    let verifier = ComplaintVerifier::new(
        url,
        Address::repeat_byte(7),
        ComplaintFinalityPolicy::Finalized,
    );
    assert!(matches!(
        verifier
            .is_nullifier_spent(B256::repeat_byte(8), 1_000)
            .await,
        Err(ComplaintVerificationError::VerificationBlockTooOld)
    ));
    {
        let requests = finalized_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["params"], json!(["finalized", false]));
    }

    let confirmed_state = rpc_state(true, finalized, confirmed);
    let confirmed_requests = confirmed_state.requests.clone();
    let (url, _) = rpc_server(confirmed_state).await;
    let verifier = ComplaintVerifier::new(
        url,
        Address::repeat_byte(7),
        ComplaintFinalityPolicy::Confirmations { count: 6 },
    );
    let verified = verifier
        .is_nullifier_spent(B256::repeat_byte(8), 1_000)
        .await
        .unwrap();
    assert!(verified.spent);
    assert_eq!(verified.block_number, 94);
    assert_eq!(verified.block_timestamp, 1_000);
    let requests = confirmed_requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0]["method"], "eth_blockNumber");
    assert_eq!(requests[1]["params"], json!(["0x5e", false]));
    assert_eq!(
        requests[2]["params"][1],
        json!({
            "blockHash": B256::repeat_byte(0x22),
            "requireCanonical": true,
        })
    );
    assert_ne!(requests[2]["params"][1], json!("latest"));
}

#[tokio::test]
async fn confirmation_policy_fails_closed_without_an_adequately_current_canonical_block() {
    let finalized = TestBlock {
        number: 2,
        hash: B256::repeat_byte(0x11),
        timestamp: 1_000,
    };
    let confirmed = TestBlock {
        number: 3,
        hash: B256::repeat_byte(0x22),
        timestamp: 1_000,
    };
    let mut state = rpc_state(false, finalized, confirmed);
    state.head = 3;
    let requests = state.requests.clone();
    let (url, _) = rpc_server(state).await;
    let verifier = ComplaintVerifier::new(
        url,
        Address::repeat_byte(7),
        ComplaintFinalityPolicy::Confirmations { count: 6 },
    );
    assert!(matches!(
        verifier
            .is_nullifier_spent(B256::repeat_byte(8), 1_000)
            .await,
        Err(ComplaintVerificationError::InsufficientHistory)
    ));
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["method"], "eth_blockNumber");
}
