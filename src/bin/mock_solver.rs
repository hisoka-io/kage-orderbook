use std::error::Error;
use std::time::Duration;

use alloy_primitives::B256;
use futures_util::StreamExt;
use kage_orderbook::api::{ExecutionStartedRequest, ReserveOrderRequest};
use kage_orderbook::core::engine::SolverProofDelivery;
use kage_orderbook::core::events::OrderEvent;
use kage_orderbook::logging::short_id;
use kage_orderbook::order::{Order, OrderId, SolverId};
use reqwest::StatusCode;
use tokio_tungstenite::connect_async;
use uuid::Uuid;

type BoxError = Box<dyn Error + Send + Sync>;

#[tokio::main]
async fn main() {
    let http_url =
        std::env::var("ORDERBOOK_HTTP_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".to_owned());
    let ws_url = std::env::var("ORDERBOOK_WS_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:3000/events/solver/ws".to_owned());
    let solver_id = std::env::var("SOLVER_ID")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(Uuid::new_v4);
    let noise_key = solver_id.as_bytes().to_vec();

    kage_orderbook::service_log!("solver", "started solver={solver_id}");

    loop {
        if let Err(error) = run(&http_url, &ws_url, solver_id, &noise_key).await {
            kage_orderbook::service_error!("solver", "disconnected error={error}");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn run(
    http_url: &str,
    ws_url: &str,
    solver_id: SolverId,
    noise_key: &[u8],
) -> Result<(), BoxError> {
    let client = reqwest::Client::new();
    let (mut socket, _) = connect_async(ws_url).await?;
    let jobs = client
        .get(format!("{http_url}/solver/jobs"))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<Order>>()
        .await?;

    for order in jobs {
        reserve(&client, http_url, order.id, solver_id, noise_key).await?;
    }
    execute_proofs(&client, http_url, solver_id, noise_key).await?;

    while let Some(message) = socket.next().await {
        let message = message?;
        if !message.is_text() {
            continue;
        }

        let event: OrderEvent = serde_json::from_str(message.to_text()?)?;
        match event {
            OrderEvent::SolverReservationRequested { order_id } => {
                kage_orderbook::service_log!("solver", "available order={}", short_id(order_id));
                reserve(&client, http_url, order_id, solver_id, noise_key).await?;
            }
            OrderEvent::ProofRelayed {
                solver_id: assigned_solver,
                ..
            } if assigned_solver == solver_id => {
                kage_orderbook::service_log!(
                    "solver",
                    "encrypted proof available solver={solver_id}"
                );
                execute_proofs(&client, http_url, solver_id, noise_key).await?;
            }
            _ => {}
        }
    }

    Ok(())
}

async fn reserve(
    client: &reqwest::Client,
    http_url: &str,
    order_id: OrderId,
    solver_id: SolverId,
    noise_key: &[u8],
) -> Result<(), BoxError> {
    let response = client
        .post(format!("{http_url}/orders/{order_id}/reserve"))
        .json(&ReserveOrderRequest {
            solver_id,
            noise_public_key: noise_key.to_vec(),
        })
        .send()
        .await?;

    if response.status() != StatusCode::CONFLICT {
        response.error_for_status()?;
        kage_orderbook::service_log!(
            "solver",
            "reserved order={} solver={solver_id} noise_key_bytes={}",
            short_id(order_id),
            noise_key.len()
        );
    } else {
        kage_orderbook::service_log!(
            "solver",
            "reservation skipped order={} reason=already_reserved",
            short_id(order_id)
        );
    }
    Ok(())
}

async fn execute_proofs(
    client: &reqwest::Client,
    http_url: &str,
    solver_id: SolverId,
    noise_key: &[u8],
) -> Result<(), BoxError> {
    let proofs = client
        .get(format!("{http_url}/solver/{solver_id}/proofs"))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<SolverProofDelivery>>()
        .await?;

    for delivery in proofs {
        let proof = xor(&delivery.ciphertext, noise_key);
        kage_orderbook::service_log!(
            "solver",
            "decrypted proof order={} ciphertext_bytes={}",
            short_id(delivery.order_id),
            delivery.ciphertext.len()
        );
        if proof != expected_proof(delivery.order_id) {
            kage_orderbook::service_error!(
                "solver",
                "proof rejected order={}",
                short_id(delivery.order_id)
            );
            continue;
        }
        kage_orderbook::service_log!(
            "solver",
            "proof verified order={}",
            short_id(delivery.order_id)
        );

        let tx_hash = tx_hash(delivery.order_id);
        client
            .post(format!(
                "{http_url}/orders/{}/execution-started",
                delivery.order_id
            ))
            .json(&ExecutionStartedRequest { solver_id, tx_hash })
            .send()
            .await?
            .error_for_status()?;

        kage_orderbook::service_log!(
            "solver",
            "execution submitted order={} tx_hash={tx_hash}",
            short_id(delivery.order_id)
        );
    }

    Ok(())
}

fn expected_proof(order_id: OrderId) -> Vec<u8> {
    format!("proof:{order_id}").into_bytes()
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
