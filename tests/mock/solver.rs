use alloy_primitives::B256;
use futures_util::StreamExt;
use kage_orderbook::logging::short_id;
use kage_types::{
    api_types::{
        ExecutionStartedRequest, SOLVER_ADDRESS_HEADER,
        SolverProofDeliveryV1 as SolverProofDelivery,
    },
    events::OrderEvent,
    identifiers::{OrderId, SolverId},
    orders::SolverOrderV1 as Order,
};
use tokio::sync::oneshot;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue},
};

use super::proof_transport;

pub async fn run(
    http_url: String,
    ws_url: String,
    solver_id: SolverId,
    noise_private_key: [u8; 32],
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
            OrderEvent::SolverReservationRequested { order_id, .. } => {
                reserve(&client, &http_url, order_id, solver_id).await;
            }
            OrderEvent::ProofRelayed {
                solver_id: assigned_solver,
                ..
            } if assigned_solver == solver_id => {
                execute_proofs(&client, &http_url, solver_id, &noise_private_key).await;
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
    noise_private_key: &[u8; 32],
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
        let proof = proof_transport::decrypt_from_user(
            delivery.order_id,
            noise_private_key,
            &delivery.ciphertext,
        )
        .unwrap();
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

fn tx_hash(order_id: OrderId) -> B256 {
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(order_id.as_bytes());
    bytes[16..].copy_from_slice(order_id.as_bytes());
    B256::from(bytes)
}
