use alloy_primitives::{Address, B256, U256};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    response::Response,
    routing::post,
};
use futures_util::{StreamExt, stream};
use kage_orderbook::{
    api,
    config::AppConfig,
    core::engine::{OrderbookHandle, start_orderbook},
    pricing::{self, PricingConfig, PricingStatus, PricingValidator},
    registry::SolverRegistry,
};
use kage_types::api_types::{ApiErrorResponse, CreateOrderRequest};
use serde::Deserialize;
use std::{convert::Infallible, time::Duration};
use tokio::{net::TcpListener, task::JoinHandle};

const CONFIG: &str = r#"{
  "order": { "default_ttl_seconds": 60, "min_ttl_seconds": 5, "max_ttl_seconds": 300, "max_order_usd_cents": 25000 },
  "database": { "max_connections": 1, "busy_timeout_ms": 5000 },
  "runtime": { "command_capacity": 256 },
  "pricing": { "max_age_ms": 5000, "reconnect_delay_ms": 50, "idle_timeout_ms": 1000 },
  "chains": [{
    "chain_id": 31337,
    "name": "local",
    "darkpool": "0x0303030303030303030303030303030303030303",
    "registry": "0x0404040404040404040404040404040404040404",
    "registry_deploy_block": 100,
    "confirmations": 0,
    "tokens": [
      { "symbol": "ETH", "address": "0x0101010101010101010101010101010101010101", "decimals": 18, "pricing_asset": "ETH", "max_price_deviation_bps": 50 },
      { "symbol": "USDC", "address": "0x0202020202020202020202020202020202020202", "decimals": 6, "pricing_asset": "USDC", "max_price_deviation_bps": 20 }
    ],
    "markets": [{ "token_in": "ETH", "token_out": "USDC", "max_price_deviation_bps": 20 }]
  }]
}"#;

#[derive(Deserialize)]
struct FeedRequest {
    assets: Vec<String>,
}

async fn feed(
    axum::extract::State(observed_at_ms): axum::extract::State<u64>,
    Json(request): Json<FeedRequest>,
) -> Response {
    assert_eq!(request.assets, vec!["ETH", "USDC"]);
    let body = format!(
        concat!(
            "event: snapshot\n",
            "data: {{\"ETH\":{{\"price_e18\":\"2000000000000000000000\",",
            "\"observed_at_ms\":{0},\"sequence\":1}},",
            "\"USDC\":{{\"price_e18\":\"1000000000000000000\",",
            "\"observed_at_ms\":{0},\"sequence\":1}}}}\n\n"
        ),
        observed_at_ms
    );
    let body = stream::once(async move { Ok::<Bytes, Infallible>(Bytes::from(body)) })
        .chain(stream::pending());
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from_stream(body))
        .unwrap()
}

async fn spawn_feed(observed_at_ms: u64) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/feed", post(feed))
        .with_state(observed_at_ms);
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}/feed"), task)
}

async fn spawn_orderbook(
    config: &AppConfig,
    pricing_validator: PricingValidator,
) -> (String, OrderbookHandle, JoinHandle<()>) {
    let orderbook = start_orderbook("sqlite::memory:").await.unwrap();
    let inspection = orderbook.clone();
    let app = api::router_with_pricing(
        orderbook,
        SolverRegistry::from_profiles([]),
        config.order_policy(),
        pricing_validator,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (format!("http://{address}"), inspection, task)
}

fn request(commitment: u8, amount_out: u64) -> CreateOrderRequest {
    CreateOrderRequest {
        order_commitment: B256::repeat_byte(commitment),
        chain_id: 31_337,
        token_in: Address::repeat_byte(1),
        token_out: Address::repeat_byte(2),
        amount_in: U256::from(100_000_000_000_000_000_u64),
        amount_out: U256::from(amount_out),
        ttl_seconds: None,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

async fn pricing(
    config: &AppConfig,
    observed_at_ms: u64,
    expected_status: PricingStatus,
) -> (PricingValidator, JoinHandle<()>) {
    let (feed_url, feed_task) = spawn_feed(observed_at_ms).await;
    let mut handle = pricing::spawn(PricingConfig {
        feed_url,
        token: "test-token".into(),
        assets: config.pricing_assets(),
        max_age: Duration::from_millis(config.pricing.max_age_ms),
        reconnect_delay: Duration::from_millis(config.pricing.reconnect_delay_ms),
        idle_timeout: Duration::from_millis(config.pricing.idle_timeout_ms),
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while handle.status() != expected_status {
            handle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    (PricingValidator::new(handle, config), feed_task)
}

#[tokio::test]
async fn pricing_validation_controls_http_admission() {
    let config = AppConfig::from_json(CONFIG).unwrap();
    let (validator, feed) = pricing(&config, now_ms(), PricingStatus::Ready).await;
    let (url, orderbook, server) = spawn_orderbook(&config, validator).await;
    let client = reqwest::Client::new();

    let accepted = request(1, 200_000_000);
    let response = client
        .post(format!("{url}/v1/orders"))
        .json(&accepted)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    assert!(
        orderbook
            .find_order_by_commitment(accepted.order_commitment)
            .await
            .unwrap()
            .is_some()
    );

    let rejected = request(2, 199_000_000);
    let response = client
        .post(format!("{url}/v1/orders"))
        .json(&rejected)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let error = response.json::<ApiErrorResponse>().await.unwrap();
    assert_eq!(error.code, "invalid_quote");
    assert!(
        orderbook
            .find_order_by_commitment(rejected.order_commitment)
            .await
            .unwrap()
            .is_none()
    );

    let mut oversized = request(3, 2_000_000_000);
    oversized.amount_in = U256::from(1_000_000_000_000_000_000_u64);
    let response = client
        .post(format!("{url}/v1/orders"))
        .json(&oversized)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let error = response.json::<ApiErrorResponse>().await.unwrap();
    assert_eq!(error.code, "order_value_limit");
    assert!(
        orderbook
            .find_order_by_commitment(oversized.order_commitment)
            .await
            .unwrap()
            .is_none()
    );

    server.abort();
    feed.abort();
}

#[tokio::test]
async fn stale_pricing_returns_service_unavailable_without_persisting() {
    let config = AppConfig::from_json(CONFIG).unwrap();
    let stale_at = now_ms().saturating_sub(config.pricing.max_age_ms + 1);
    let (validator, feed) = pricing(&config, stale_at, PricingStatus::Stale).await;
    let (url, orderbook, server) = spawn_orderbook(&config, validator).await;
    let request = request(4, 200_000_000);

    let response = reqwest::Client::new()
        .post(format!("{url}/v1/orders"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        orderbook
            .find_order_by_commitment(request.order_commitment)
            .await
            .unwrap()
            .is_none()
    );

    server.abort();
    feed.abort();
}
