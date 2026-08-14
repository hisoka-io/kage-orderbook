use alloy_primitives::B256;
use futures_util::StreamExt;
use kage_orderbook::api::{ExecutionStartedRequest, SOLVER_ADDRESS_HEADER};
use kage_orderbook::core::engine::SolverProofDelivery;
use kage_orderbook::core::events::OrderEvent;
use kage_orderbook::logging::short_id;
use kage_orderbook::order::{Order, OrderId, SolverId};
use tokio::sync::oneshot;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

pub async fn run(
    http_url: String,
    ws_url: String,
    solver_id: SolverId,
    noise_key: Vec<u8>,
    ready: oneshot::Sender<()>,
) {
    let client = reqwest::Client::new();
    let mut request = ws_url.into_client_request().unwrap();
    request.headers_mut().insert(
        SOLVER_ADDRESS_HEADER,
        HeaderValue::from_str(&solver_id.to_string()).unwrap(),
    );
    let (mut socket, _) = connect_async(request).await.unwrap();

    let jobs: Vec<Order> = client
        .get(format!("{http_url}/solver/jobs"))
        .header(SOLVER_ADDRESS_HEADER, solver_id.to_string())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    for order in jobs {
        reserve(&client, &http_url, order.id, solver_id).await;
    }

    let _ = ready.send(());

    while let Some(message) = socket.next().await {
        let message = message.unwrap();
        if !message.is_text() {
            continue;
        }

        let event: OrderEvent = serde_json::from_str(message.to_text().unwrap()).unwrap();
        match event {
            OrderEvent::SolverReservationRequested { order_id } => {
                reserve(&client, &http_url, order_id, solver_id).await;
            }
            OrderEvent::ProofRelayed {
                solver_id: assigned_solver,
                ..
            } if assigned_solver == solver_id => {
                execute_proofs(&client, &http_url, solver_id, &noise_key).await;
            }
            _ => {}
        }
    }
}

async fn reserve(client: &reqwest::Client, http_url: &str, order_id: OrderId, solver_id: SolverId) {
    let response = client
        .post(format!("{http_url}/orders/{order_id}/reserve"))
        .header(SOLVER_ADDRESS_HEADER, solver_id.to_string())
        .send()
        .await
        .unwrap();

    if response.status().is_success() {
        kage_orderbook::service_log!(
            "solver",
            "reserved order={} solver={solver_id}",
            short_id(order_id)
        );
    }
}

async fn execute_proofs(
    client: &reqwest::Client,
    http_url: &str,
    solver_id: SolverId,
    noise_key: &[u8],
) {
    let deliveries: Vec<SolverProofDelivery> = client
        .get(format!("{http_url}/solver/proofs"))
        .header(SOLVER_ADDRESS_HEADER, solver_id.to_string())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    for delivery in deliveries {
        let proof = xor(&delivery.ciphertext, noise_key);
        assert_eq!(proof, format!("proof:{}", delivery.order_id).as_bytes());
        kage_orderbook::service_log!(
            "solver",
            "proof verified order={}",
            short_id(delivery.order_id)
        );

        client
            .post(format!(
                "{http_url}/orders/{}/execution-started",
                delivery.order_id
            ))
            .header(SOLVER_ADDRESS_HEADER, solver_id.to_string())
            .json(&ExecutionStartedRequest {
                tx_hash: tx_hash(delivery.order_id),
            })
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        kage_orderbook::service_log!(
            "solver",
            "execution submitted order={}",
            short_id(delivery.order_id)
        );
    }
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
