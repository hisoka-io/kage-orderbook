mod expiry;
mod support;

use std::{path::PathBuf, time::Duration};

use futures_util::{SinkExt, StreamExt};
use kage_orderbook::{
    api,
    core::{
        engine::start_orderbook,
        guards::{
            DEFAULT_ORDER_TTL_SECONDS, MAX_ORDER_TTL_SECONDS, MIN_ORDER_TTL_SECONDS, MOCK_CHAIN_ID,
        },
    },
};
use kage_types::{
    api_types::{
        CreateOrderRequest, CreateOrderResponse, ORDER_COMMITMENT_HEADER, UserEventClientMessage,
        UserEventServerMessage,
    },
    identifiers::{OrderCommitment, OrderId},
    orders::{OrderState, OrderV1 as Order, SolverJobV1},
};
use reqwest::header::AUTHORIZATION;
use support::{assignment_issuer, bearer, commitment, registry, terms};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use uuid::Uuid;

struct TestDatabase(PathBuf);

impl TestDatabase {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("kage-orderbook-{}.db", Uuid::new_v4())))
    }

    fn url(&self) -> String {
        format!("sqlite://{}", self.0.display())
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        for suffix in ["", "-shm", "-wal"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

async fn server() -> (String, String, JoinHandle<()>) {
    server_with_database("sqlite::memory:").await
}

async fn server_with_database(database_url: &str) -> (String, String, JoinHandle<()>) {
    let orderbook = start_orderbook(database_url).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            api::router(orderbook, registry(), assignment_issuer())
                .into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (
        format!("http://{address}"),
        format!("ws://{address}/v1/events/user/ws"),
        task,
    )
}

fn create_order_request(n: u64, ttl_seconds: Option<u32>) -> CreateOrderRequest {
    let terms = terms(n);
    CreateOrderRequest {
        order_commitment: commitment(n),
        chain_id: terms.chain_id,
        token_in: terms.token_in,
        token_out: terms.token_out,
        amount_in: terms.amount_in,
        amount_out: terms.amount_out,
        ttl_seconds,
    }
}

async fn create_order_with_commitment(
    client: &reqwest::Client,
    http_url: &str,
    n: u64,
) -> (OrderId, OrderCommitment) {
    let request = create_order_request(n, None);
    let order_id = client
        .post(format!("{http_url}/v1/orders"))
        .json(&request)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<CreateOrderResponse>()
        .await
        .unwrap()
        .order_id;
    (order_id, request.order_commitment)
}

async fn subscribe(
    socket: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    order_id: OrderId,
    order_commitment: OrderCommitment,
) -> UserEventServerMessage {
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&UserEventClientMessage::Subscribe {
                order_id,
                order_commitment,
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    let response = socket.next().await.unwrap().unwrap();
    serde_json::from_str(response.to_text().unwrap()).unwrap()
}

#[tokio::test]
async fn retries_return_the_existing_order_and_reject_conflicting_terms() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();
    let request = create_order_request(1, None);
    let first = client
        .post(format!("{http_url}/v1/orders"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), reqwest::StatusCode::CREATED);
    let first = first.json::<CreateOrderResponse>().await.unwrap();

    let retry = client
        .post(format!("{http_url}/v1/orders"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(retry.status(), reqwest::StatusCode::OK);
    let retry = retry.json::<CreateOrderResponse>().await.unwrap();
    assert_eq!(retry.order_id, first.order_id);
    assert_eq!(retry.expires_at_ms, first.expires_at_ms);

    let mut conflicting = request;
    conflicting.amount_out += alloy_primitives::U256::from(1);
    assert_eq!(
        client
            .post(format!("{http_url}/v1/orders"))
            .json(&conflicting)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    server.abort();
}

#[tokio::test]
async fn concurrent_retries_create_only_one_order() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();
    let request = create_order_request(1, None);
    let responses = futures_util::future::join_all((0..8).map(|_| {
        let client = client.clone();
        let url = format!("{http_url}/v1/orders");
        let request = request.clone();
        async move { client.post(url).json(&request).send().await.unwrap() }
    }))
    .await;

    let mut created = 0;
    let mut order_id = None;
    for response in responses {
        match response.status() {
            reqwest::StatusCode::CREATED => created += 1,
            reqwest::StatusCode::OK => {}
            status => panic!("unexpected retry status: {status}"),
        }
        let body = response.json::<CreateOrderResponse>().await.unwrap();
        assert!(order_id.is_none_or(|id| id == body.order_id));
        order_id = Some(body.order_id);
    }
    assert_eq!(created, 1);
    server.abort();
}

#[tokio::test]
async fn retry_after_restart_returns_the_original_order() {
    let database = TestDatabase::new();
    let database_url = database.url();
    let client = reqwest::Client::new();
    let request = create_order_request(1, None);
    let (http_url, _, first_server) = server_with_database(&database_url).await;
    let first = client
        .post(format!("{http_url}/v1/orders"))
        .json(&request)
        .send()
        .await
        .unwrap()
        .json::<CreateOrderResponse>()
        .await
        .unwrap();
    first_server.abort();
    let _ = first_server.await;

    let (http_url, _, restarted_server) = server_with_database(&database_url).await;
    let retry = client
        .post(format!("{http_url}/v1/orders"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(retry.status(), reqwest::StatusCode::OK);
    let retry = retry.json::<CreateOrderResponse>().await.unwrap();
    assert_eq!(retry.order_id, first.order_id);
    assert_eq!(retry.expires_at_ms, first.expires_at_ms);
    restarted_server.abort();
}

#[tokio::test]
async fn applies_default_ttl_and_rejects_out_of_range_ttl() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();
    let before = chrono::Utc::now().timestamp_millis();
    let created = client
        .post(format!("{http_url}/v1/orders"))
        .json(&create_order_request(1, None))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<CreateOrderResponse>()
        .await
        .unwrap();
    let after = chrono::Utc::now().timestamp_millis();
    let default_ttl_ms = i64::from(DEFAULT_ORDER_TTL_SECONDS) * 1_000;
    assert!(created.expires_at_ms >= before + default_ttl_ms);
    assert!(created.expires_at_ms <= after + default_ttl_ms);

    for (n, ttl) in [
        (2, MIN_ORDER_TTL_SECONDS - 1),
        (3, MAX_ORDER_TTL_SECONDS + 1),
    ] {
        assert_eq!(
            client
                .post(format!("{http_url}/v1/orders"))
                .json(&create_order_request(n, Some(ttl)))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::UNPROCESSABLE_ENTITY
        );
    }
    server.abort();
}

#[tokio::test]
async fn protects_order_access_with_the_commitment() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();
    let (order_id, order_commitment) = create_order_with_commitment(&client, &http_url, 1).await;
    let order_url = format!("{http_url}/v1/orders/{order_id}");
    assert_eq!(
        client.get(&order_url).send().await.unwrap().status(),
        reqwest::StatusCode::NOT_FOUND
    );
    assert_eq!(
        client
            .get(&order_url)
            .header(ORDER_COMMITMENT_HEADER, commitment(2).to_string())
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    assert_eq!(
        client
            .get(&order_url)
            .header(ORDER_COMMITMENT_HEADER, order_commitment.to_string())
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    server.abort();
}

#[tokio::test]
async fn user_event_stream_is_private_and_reconnectable() {
    let (http_url, ws_url, server) = server().await;
    let client = reqwest::Client::new();
    let token_11 = bearer(&http_url, 0x11).await;
    let (first_id, first_commitment) = create_order_with_commitment(&client, &http_url, 1).await;
    let (second_id, second_commitment) = create_order_with_commitment(&client, &http_url, 2).await;
    let (mut socket, _) = connect_async(&ws_url).await.unwrap();

    assert!(matches!(
        subscribe(&mut socket, first_id, second_commitment).await,
        UserEventServerMessage::Rejected { order_id } if order_id == first_id
    ));
    assert!(matches!(
        subscribe(&mut socket, first_id, first_commitment).await,
        UserEventServerMessage::Subscribed { order_id } if order_id == first_id
    ));
    client
        .post(format!("{http_url}/v1/orders/{second_id}/reserve"))
        .header(AUTHORIZATION, &token_11)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), socket.next())
            .await
            .is_err()
    );
    client
        .post(format!("{http_url}/v1/orders/{first_id}/reserve"))
        .header(AUTHORIZATION, &token_11)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let message = socket.next().await.unwrap().unwrap();
    let message: UserEventServerMessage = serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert!(matches!(
        message,
        UserEventServerMessage::Event { event } if event.order_id() == first_id
    ));

    drop(socket);
    let (mut reconnected, _) = connect_async(&ws_url).await.unwrap();
    assert!(matches!(
        subscribe(&mut reconnected, first_id, first_commitment).await,
        UserEventServerMessage::Subscribed { order_id } if order_id == first_id
    ));
    server.abort();
}

#[tokio::test]
async fn solver_can_reserve_decline_and_requeue_an_order() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();
    let token_11 = bearer(&http_url, 0x11).await;
    let token_22 = bearer(&http_url, 0x22).await;
    let (order_id, order_commitment) = create_order_with_commitment(&client, &http_url, 1).await;

    let jobs = client
        .get(format!("{http_url}/v1/solver/jobs"))
        .header(AUTHORIZATION, &token_11)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Vec<SolverJobV1>>()
        .await
        .unwrap();
    assert_eq!(
        jobs.iter().map(|job| job.id).collect::<Vec<_>>(),
        [order_id]
    );

    client
        .post(format!("{http_url}/v1/orders/{order_id}/reserve"))
        .header(AUTHORIZATION, &token_11)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(
        client
            .post(format!("{http_url}/v1/orders/{order_id}/decline"))
            .header(AUTHORIZATION, &token_22)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    client
        .post(format!("{http_url}/v1/orders/{order_id}/decline"))
        .header(AUTHORIZATION, &token_11)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let order = client
        .get(format!("{http_url}/v1/orders/{order_id}"))
        .header(ORDER_COMMITMENT_HEADER, order_commitment.to_string())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Order>()
        .await
        .unwrap();
    assert_eq!(order.state, OrderState::Reserving);
    assert_eq!(order.solver, None);
    server.abort();
}

#[tokio::test]
async fn solver_endpoints_reject_unproved_identities() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();
    let (order_id, _) = create_order_with_commitment(&client, &http_url, 1).await;
    for (method, url) in [
        ("get", format!("{http_url}/v1/solver/jobs")),
        ("post", format!("{http_url}/v1/orders/{order_id}/reserve")),
        ("post", format!("{http_url}/v1/orders/{order_id}/decline")),
    ] {
        let request = if method == "get" {
            client.get(&url)
        } else {
            client.post(&url)
        };
        assert_eq!(
            request.send().await.unwrap().status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(create_order_request(2, None).chain_id, MOCK_CHAIN_ID);
    server.abort();
}
