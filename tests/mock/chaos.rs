use std::collections::HashMap;
use std::time::Duration;

use alloy_primitives::B256;
use futures_util::future::join_all;
use kage_orderbook::api::{
    EncryptedProofRequest, ExecutionStartedRequest, ORDER_COMMITMENT_HEADER, ReserveOrderRequest,
    SettlementRequest,
};
use kage_orderbook::core::engine::SolverProofDelivery;
use kage_orderbook::logging::short_id;
use kage_orderbook::order::{Order, OrderId, OrderState, SolverId};
use reqwest::{Client, Response, StatusCode};
use uuid::Uuid;

use super::{create_order_with_commitment, server};

const ORDERS: u64 = 4;

#[tokio::test]
async fn delayed_services_accept_retries_and_reject_conflicts() {
    tokio::time::timeout(Duration::from_secs(10), run())
        .await
        .expect("harness timed out");
}

async fn run() {
    let (http_url, _, server) = server().await;
    let client = Client::new();
    let solver_id = Uuid::from_bytes([0x11; 16]);
    let wrong_solver_id = Uuid::from_bytes([0x22; 16]);
    let noise_key = solver_id.as_bytes().to_vec();

    let created = join_all((1..=ORDERS).map(|number| {
        let client = client.clone();
        let http_url = http_url.clone();
        async move { create_order_with_commitment(&client, &http_url, number).await }
    }))
    .await;
    let commitments = created.iter().copied().collect::<HashMap<_, _>>();
    let orders = created
        .into_iter()
        .map(|(order_id, _)| order_id)
        .collect::<Vec<_>>();
    harness_log(
        "user",
        format!("created {} concurrent orders", orders.len()),
    );

    let first = orders[0];
    expect(
        client
            .post(format!("{http_url}/orders/{first}/encrypted-proof"))
            .header(ORDER_COMMITMENT_HEADER, commitments[&first].to_string())
            .json(&EncryptedProofRequest {
                ciphertext: vec![1],
            })
            .send()
            .await
            .unwrap(),
        StatusCode::CONFLICT,
        "proof before reservation",
        first,
    );
    expect(
        client
            .post(format!("{http_url}/orders/{first}/execution-started"))
            .json(&ExecutionStartedRequest {
                solver_id,
                tx_hash: tx_hash(first),
            })
            .send()
            .await
            .unwrap(),
        StatusCode::CONFLICT,
        "execution before proof",
        first,
    );

    join_all(orders.iter().enumerate().map(|(index, order_id)| {
        let client = client.clone();
        let http_url = http_url.clone();
        let noise_key = noise_key.clone();
        let order_id = *order_id;
        async move {
            tokio::time::sleep(delay(index, 20)).await;
            let response = client
                .post(format!("{http_url}/orders/{order_id}/reserve"))
                .json(&ReserveOrderRequest {
                    solver_id,
                    noise_public_key: noise_key,
                })
                .send()
                .await
                .unwrap();
            expect(
                response,
                StatusCode::NO_CONTENT,
                "delayed solver reservation",
                order_id,
            );
        }
    }))
    .await;

    expect(
        client
            .post(format!("{http_url}/orders/{first}/reserve"))
            .json(&ReserveOrderRequest {
                solver_id,
                noise_public_key: noise_key.clone(),
            })
            .send()
            .await
            .unwrap(),
        StatusCode::NO_CONTENT,
        "idempotent reservation retry",
        first,
    );
    expect(
        client
            .post(format!("{http_url}/orders/{first}/reserve"))
            .json(&ReserveOrderRequest {
                solver_id: wrong_solver_id,
                noise_public_key: noise_key.clone(),
            })
            .send()
            .await
            .unwrap(),
        StatusCode::CONFLICT,
        "conflicting reservation",
        first,
    );

    join_all(orders.iter().enumerate().map(|(index, order_id)| {
        let client = client.clone();
        let http_url = http_url.clone();
        let noise_key = noise_key.clone();
        let order_id = *order_id;
        let order_commitment = commitments[&order_id];
        async move {
            tokio::time::sleep(delay(index, 15)).await;
            let ciphertext = xor(format!("proof:{order_id}").as_bytes(), &noise_key);
            let response = client
                .post(format!("{http_url}/orders/{order_id}/encrypted-proof"))
                .header(ORDER_COMMITMENT_HEADER, order_commitment.to_string())
                .json(&EncryptedProofRequest { ciphertext })
                .send()
                .await
                .unwrap();
            expect(
                response,
                StatusCode::NO_CONTENT,
                "delayed user proof",
                order_id,
            );
        }
    }))
    .await;

    let first_ciphertext = xor(format!("proof:{first}").as_bytes(), &noise_key);
    expect(
        client
            .post(format!("{http_url}/orders/{first}/encrypted-proof"))
            .header(ORDER_COMMITMENT_HEADER, commitments[&first].to_string())
            .json(&EncryptedProofRequest {
                ciphertext: first_ciphertext,
            })
            .send()
            .await
            .unwrap(),
        StatusCode::NO_CONTENT,
        "idempotent proof retry",
        first,
    );
    expect(
        client
            .post(format!("{http_url}/orders/{first}/encrypted-proof"))
            .header(ORDER_COMMITMENT_HEADER, commitments[&first].to_string())
            .json(&EncryptedProofRequest {
                ciphertext: vec![1],
            })
            .send()
            .await
            .unwrap(),
        StatusCode::CONFLICT,
        "conflicting proof",
        first,
    );
    expect(
        client
            .post(format!("{http_url}/orders/{first}/execution-started"))
            .json(&ExecutionStartedRequest {
                solver_id: wrong_solver_id,
                tx_hash: tx_hash(first),
            })
            .send()
            .await
            .unwrap(),
        StatusCode::CONFLICT,
        "execution by wrong solver",
        first,
    );

    let proofs = solver_proofs(&client, &http_url, solver_id).await;
    assert_eq!(proofs.len(), ORDERS as usize);
    let repeated = solver_proofs(&client, &http_url, solver_id).await;
    assert_eq!(repeated.len(), proofs.len());
    harness_log("solver", "proof polling is non-destructive");

    join_all(proofs.into_iter().enumerate().map(|(index, delivery)| {
        let client = client.clone();
        let http_url = http_url.clone();
        let noise_key = noise_key.clone();
        async move {
            tokio::time::sleep(delay(index, 25)).await;
            assert_eq!(
                xor(&delivery.ciphertext, &noise_key),
                format!("proof:{}", delivery.order_id).as_bytes()
            );
            let response = client
                .post(format!(
                    "{http_url}/orders/{}/execution-started",
                    delivery.order_id
                ))
                .json(&ExecutionStartedRequest {
                    solver_id,
                    tx_hash: tx_hash(delivery.order_id),
                })
                .send()
                .await
                .unwrap();
            expect(
                response,
                StatusCode::NO_CONTENT,
                "delayed solver execution",
                delivery.order_id,
            );
        }
    }))
    .await;

    expect(
        client
            .post(format!("{http_url}/orders/{first}/execution-started"))
            .json(&ExecutionStartedRequest {
                solver_id,
                tx_hash: tx_hash(first),
            })
            .send()
            .await
            .unwrap(),
        StatusCode::NO_CONTENT,
        "idempotent execution retry",
        first,
    );
    assert!(
        solver_proofs(&client, &http_url, solver_id)
            .await
            .is_empty()
    );

    expect(
        client
            .post(format!("{http_url}/orders/{first}/settlement"))
            .json(&SettlementRequest {
                tx_hash: B256::repeat_byte(0xff),
            })
            .send()
            .await
            .unwrap(),
        StatusCode::CONFLICT,
        "settlement with wrong transaction",
        first,
    );

    join_all(orders.iter().enumerate().map(|(index, order_id)| {
        let client = client.clone();
        let http_url = http_url.clone();
        let order_id = *order_id;
        async move {
            tokio::time::sleep(delay(index, 30)).await;
            let response = client
                .post(format!("{http_url}/orders/{order_id}/settlement"))
                .json(&SettlementRequest {
                    tx_hash: tx_hash(order_id),
                })
                .send()
                .await
                .unwrap();
            expect(
                response,
                StatusCode::NO_CONTENT,
                "delayed chain settlement",
                order_id,
            );
        }
    }))
    .await;

    expect(
        client
            .post(format!("{http_url}/orders/{first}/settlement"))
            .json(&SettlementRequest {
                tx_hash: tx_hash(first),
            })
            .send()
            .await
            .unwrap(),
        StatusCode::NO_CONTENT,
        "idempotent settlement retry",
        first,
    );

    for order_id in orders {
        let order = get_order(&client, &http_url, order_id, commitments[&order_id]).await;
        assert_eq!(order.state, OrderState::Filled);
        assert_eq!(order.version, 8);
        harness_log(
            "assert",
            format!("order={} final_state=Filled version=8", short_id(order_id)),
        );
    }

    let solver_jobs: Vec<Order> = get_json(&client, format!("{http_url}/solver/jobs")).await;
    let chain_jobs: Vec<Order> = get_json(&client, format!("{http_url}/chain/jobs")).await;
    assert!(solver_jobs.is_empty());
    assert!(chain_jobs.is_empty());
    harness_log("done", "all checks passed");
    server.abort();
}

async fn solver_proofs(
    client: &Client,
    http_url: &str,
    solver_id: SolverId,
) -> Vec<SolverProofDelivery> {
    get_json(client, format!("{http_url}/solver/{solver_id}/proofs")).await
}

async fn get_order(
    client: &Client,
    http_url: &str,
    order_id: OrderId,
    order_commitment: B256,
) -> Order {
    client
        .get(format!("{http_url}/orders/{order_id}"))
        .header(ORDER_COMMITMENT_HEADER, order_commitment.to_string())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn get_json<T: serde::de::DeserializeOwned>(client: &Client, url: String) -> T {
    client
        .get(url)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn expect(response: Response, expected: StatusCode, action: &str, order_id: OrderId) {
    assert_eq!(
        response.status(),
        expected,
        "{action} returned an unexpected status"
    );
    harness_log(
        "check",
        format!(
            "order={} action={action} status={expected}",
            short_id(order_id)
        ),
    );
}

fn delay(index: usize, step_ms: u64) -> Duration {
    Duration::from_millis((index as u64 + 1) * step_ms)
}

fn xor(bytes: &[u8], key: &[u8]) -> Vec<u8> {
    bytes
        .iter()
        .zip(key.iter().cycle())
        .map(|(byte, key)| byte ^ key)
        .collect()
}

fn tx_hash(order_id: OrderId) -> B256 {
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(order_id.as_bytes());
    bytes[16..].copy_from_slice(order_id.as_bytes());
    B256::from(bytes)
}

fn harness_log(stage: &str, message: impl std::fmt::Display) {
    kage_orderbook::service_log!("harness", "stage={stage} {message}");
}
