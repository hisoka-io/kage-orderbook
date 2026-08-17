use std::{collections::HashMap, error::Error, sync::Arc, time::Duration};

use alloy_primitives::{Address, B256};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode as AxumStatusCode,
    routing::get,
};
use futures_util::StreamExt;
use kage_orderbook::logging::short_id;
use kage_types::{
    api_types::SettlementRequest,
    events::OrderEvent,
    identifiers::{OrderId, TxHash},
    orders::ChainOrderV1,
    registry::SolverProfile,
};
use reqwest::StatusCode;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_tungstenite::connect_async;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Default)]
struct MockRegistry {
    profiles: Arc<RwLock<HashMap<Address, SolverProfile>>>,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let http_url =
        std::env::var("ORDERBOOK_HTTP_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".to_owned());
    let ws_url = std::env::var("ORDERBOOK_WS_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:3000/events/chain/ws".to_owned());
    let delay = std::env::var("CHAIN_DELAY_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(500));
    let registry_address =
        std::env::var("MOCK_REGISTRY_ADDRESS").unwrap_or_else(|_| "127.0.0.1:4000".to_owned());
    let registry = Router::new()
        .route(
            "/solvers/{solver_id}",
            get(solver_profile).put(register_solver),
        )
        .route("/health", get(health))
        .with_state(MockRegistry::default());
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
    registry
        .profiles
        .read()
        .await
        .get(&solver_id)
        .copied()
        .map(Json)
        .ok_or(AxumStatusCode::NOT_FOUND)
}

async fn register_solver(
    State(registry): State<MockRegistry>,
    Path(solver_id): Path<Address>,
    Json(profile): Json<SolverProfile>,
) -> Result<AxumStatusCode, AxumStatusCode> {
    if profile.noise_public_key == B256::ZERO {
        return Err(AxumStatusCode::BAD_REQUEST);
    }

    let mut profiles = registry.profiles.write().await;
    let changed = profiles.get(&solver_id).is_none_or(|existing| {
        existing.noise_public_key != profile.noise_public_key || existing.active != profile.active
    });
    profiles.insert(solver_id, profile);
    drop(profiles);

    if changed {
        kage_orderbook::service_log!("chain", "solver registered solver={solver_id}");
    }
    Ok(AxumStatusCode::NO_CONTENT)
}

async fn run(http_url: &str, ws_url: &str, delay: Duration) -> Result<(), BoxError> {
    let client = reqwest::Client::new();
    let (mut socket, _) = connect_async(ws_url).await?;
    let jobs = client
        .get(format!("{http_url}/chain/jobs"))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<ChainOrderV1>>()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn solver_registers_its_derived_noise_public_key() {
        let registry = MockRegistry::default();
        let solver_id = Address::repeat_byte(0x11);
        let profile = SolverProfile {
            noise_public_key: B256::repeat_byte(0x22),
            active: true,
        };

        assert_eq!(
            register_solver(State(registry.clone()), Path(solver_id), Json(profile)).await,
            Ok(AxumStatusCode::NO_CONTENT)
        );
        let registered = solver_profile(State(registry), Path(solver_id))
            .await
            .unwrap()
            .0;
        assert_eq!(registered.noise_public_key, profile.noise_public_key);
        assert!(registered.active);
    }

    #[tokio::test]
    async fn registry_rejects_an_empty_noise_public_key() {
        let profile = SolverProfile {
            noise_public_key: B256::ZERO,
            active: true,
        };

        assert_eq!(
            register_solver(
                State(MockRegistry::default()),
                Path(Address::repeat_byte(0x11)),
                Json(profile)
            )
            .await,
            Err(AxumStatusCode::BAD_REQUEST)
        );
    }
}
