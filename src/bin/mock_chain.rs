use std::error::Error;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use axum::extract::{Path, State};
use axum::http::StatusCode as AxumStatusCode;
use axum::routing::get;
use axum::{Json, Router};
use futures_util::StreamExt;
use kage_orderbook::api::SettlementRequest;
use kage_orderbook::core::events::OrderEvent;
use kage_orderbook::logging::short_id;
use kage_orderbook::order::{Order, OrderId, TxHash};
use kage_orderbook::registry::SolverProfile;
use reqwest::StatusCode;
use tokio::net::TcpListener;
use tokio_tungstenite::connect_async;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Clone)]
struct MockRegistry {
    solver_id: Address,
    profile: SolverProfile,
}

#[tokio::main]
async fn main() {
    let http_url =
        std::env::var("ORDERBOOK_HTTP_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".to_owned());
    let ws_url = std::env::var("ORDERBOOK_WS_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:3000/events/chain/ws".to_owned());
    let delay = std::env::var("CHAIN_DELAY_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(500));
    let solver_id = std::env::var("SOLVER_ADDRESS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| Address::repeat_byte(0x11));
    let noise_key = std::env::var("SOLVER_NOISE_KEY")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| B256::repeat_byte(0x33));
    let registry_address =
        std::env::var("MOCK_REGISTRY_ADDRESS").unwrap_or_else(|_| "127.0.0.1:4000".to_owned());
    let registry = Router::new()
        .route("/solvers/{solver_id}", get(solver_profile))
        .route("/health", get(health))
        .with_state(MockRegistry {
            solver_id,
            profile: SolverProfile {
                noise_key,
                active: true,
            },
        });
    let listener = TcpListener::bind(&registry_address).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, registry).await.unwrap();
    });
    kage_orderbook::service_log!("chain", "registry listening address={registry_address}");

    loop {
        if let Err(error) = run(&http_url, &ws_url, delay).await {
            kage_orderbook::service_error!("chain", "disconnected error={error}");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn health() -> AxumStatusCode {
    AxumStatusCode::NO_CONTENT
}

async fn solver_profile(
    State(registry): State<MockRegistry>,
    Path(solver_id): Path<Address>,
) -> Result<Json<SolverProfile>, AxumStatusCode> {
    if solver_id != registry.solver_id {
        return Err(AxumStatusCode::NOT_FOUND);
    }
    Ok(Json(registry.profile))
}

async fn run(http_url: &str, ws_url: &str, delay: Duration) -> Result<(), BoxError> {
    let client = reqwest::Client::new();
    let (mut socket, _) = connect_async(ws_url).await?;
    let jobs = client
        .get(format!("{http_url}/chain/jobs"))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<Order>>()
        .await?;

    for order in jobs {
        if let Some(tx_hash) = order.tx_hash {
            settle(&client, http_url, order.id, tx_hash, delay).await?;
        }
    }

    while let Some(message) = socket.next().await {
        let message = message?;
        if !message.is_text() {
            continue;
        }

        let event: OrderEvent = serde_json::from_str(message.to_text()?)?;
        if let OrderEvent::ExecutionStarted { order_id, tx_hash } = event {
            kage_orderbook::service_log!(
                "chain",
                "execution observed order={} tx_hash={tx_hash}",
                short_id(order_id)
            );
            settle(&client, http_url, order_id, tx_hash, delay).await?;
        }
    }

    Ok(())
}

async fn settle(
    client: &reqwest::Client,
    http_url: &str,
    order_id: OrderId,
    tx_hash: TxHash,
    delay: Duration,
) -> Result<(), BoxError> {
    tokio::time::sleep(delay).await;
    let response = client
        .post(format!("{http_url}/orders/{order_id}/settlement"))
        .json(&SettlementRequest { tx_hash })
        .send()
        .await?;

    if response.status() == StatusCode::CONFLICT {
        return Ok(());
    }
    response.error_for_status()?;

    kage_orderbook::service_log!(
        "chain",
        "settlement confirmed order={} tx_hash={tx_hash}",
        short_id(order_id)
    );
    Ok(())
}
