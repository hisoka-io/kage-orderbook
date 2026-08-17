mod chain;
mod chaos;
mod expiry;
#[path = "../../src/bin/mock_support/proof_transport.rs"]
mod proof_transport;
mod recovery;
mod solver;
mod support;
mod user;

use std::{collections::HashMap, time::Duration};

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
        CreateOrderRequest, CreateOrderResponse, EncryptedProofRequest, ORDER_COMMITMENT_HEADER,
        SOLVER_ADDRESS_HEADER, UserEventClientMessage, UserEventServerMessage,
    },
    events::OrderEvent,
    identifiers::{OrderCommitment, OrderId},
    orders::{OrderState, OrderV1 as Order},
};
use support::{commitment, noise_private_key, registry, solver_address, terms};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const USERS: u64 = 5;

async fn server() -> (String, String, JoinHandle<()>) {
    server_with_database("sqlite::memory:").await
}

async fn server_with_database(database_url: &str) -> (String, String, JoinHandle<()>) {
    let orderbook = start_orderbook(database_url).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, api::router(orderbook, registry()))
            .await
            .unwrap();
    });

    (
        format!("http://{address}"),
        format!("ws://{address}/events/user/ws"),
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

async fn create_order(client: &reqwest::Client, http_url: &str, n: u64) -> OrderId {
    create_order_with_commitment(client, http_url, n).await.0
}

async fn create_order_with_commitment(
    client: &reqwest::Client,
    http_url: &str,
    n: u64,
) -> (OrderId, alloy_primitives::B256) {
    let request = create_order_request(n, None);
    let order_id = client
        .post(format!("{http_url}/orders"))
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

async fn subscribe_user_events(
    socket: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    order_id: OrderId,
    order_commitment: OrderCommitment,
) -> UserEventServerMessage {
    let request = UserEventClientMessage::Subscribe {
        order_id,
        order_commitment,
    };
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&request).unwrap().into(),
        ))
        .await
        .unwrap();
    let response = socket.next().await.unwrap().unwrap();
    serde_json::from_str(response.to_text().unwrap()).unwrap()
}

#[tokio::test]
async fn retries_return_the_existing_order() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();
    let request = create_order_request(1, None);

    let first_response = client
        .post(format!("{http_url}/orders"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(first_response.status(), reqwest::StatusCode::CREATED);
    let first = first_response.json::<CreateOrderResponse>().await.unwrap();

    let retry_response = client
        .post(format!("{http_url}/orders"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(retry_response.status(), reqwest::StatusCode::OK);
    let retry = retry_response.json::<CreateOrderResponse>().await.unwrap();

    assert_eq!(retry.order_id, first.order_id);
    assert_eq!(retry.expires_at_ms, first.expires_at_ms);

    let jobs = client
        .get(format!("{http_url}/solver/jobs"))
        .header(SOLVER_ADDRESS_HEADER, solver_address(0x11).to_string())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Vec<Order>>()
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);

    server.abort();
}

#[tokio::test]
async fn retry_with_a_different_ttl_keeps_the_original_expiry() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();
    let request = create_order_request(1, Some(30));

    let first_response = client
        .post(format!("{http_url}/orders"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(first_response.status(), reqwest::StatusCode::CREATED);
    let first = first_response.json::<CreateOrderResponse>().await.unwrap();

    let mut retry_request = request.clone();
    retry_request.ttl_seconds = Some(120);
    let retry_response = client
        .post(format!("{http_url}/orders"))
        .json(&retry_request)
        .send()
        .await
        .unwrap();
    assert_eq!(retry_response.status(), reqwest::StatusCode::OK);
    let retry = retry_response.json::<CreateOrderResponse>().await.unwrap();

    assert_eq!(retry.order_id, first.order_id);
    assert_eq!(retry.expires_at_ms, first.expires_at_ms);

    let stored = client
        .get(format!("{http_url}/orders/{}", first.order_id))
        .header(
            ORDER_COMMITMENT_HEADER,
            request.order_commitment.to_string(),
        )
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Order>()
        .await
        .unwrap();
    assert_eq!(stored.expires_at_ms, Some(first.expires_at_ms));

    server.abort();
}

#[tokio::test]
async fn rejects_a_commitment_reused_for_different_terms() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();
    let request = create_order_request(1, None);
    client
        .post(format!("{http_url}/orders"))
        .json(&request)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let mut conflicting = request;
    conflicting.amount_out = alloy_primitives::U256::from(999);
    let response = client
        .post(format!("{http_url}/orders"))
        .json(&conflicting)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);

    server.abort();
}

#[tokio::test]
async fn concurrent_retries_create_only_one_order() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();
    let request = create_order_request(1, None);
    let responses = futures_util::future::join_all((0..8).map(|_| {
        let client = client.clone();
        let url = format!("{http_url}/orders");
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

    let jobs = client
        .get(format!("{http_url}/solver/jobs"))
        .header(SOLVER_ADDRESS_HEADER, solver_address(0x11).to_string())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Vec<Order>>()
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);

    server.abort();
}

#[tokio::test]
async fn retry_after_restart_returns_the_original_order() {
    let directory = tempfile::tempdir().unwrap();
    let database_url = format!(
        "sqlite://{}",
        directory.path().join("orderbook.db").display()
    );
    let client = reqwest::Client::new();
    let request = create_order_request(1, None);

    let (http_url, _, first_server) = server_with_database(&database_url).await;
    let first_response = client
        .post(format!("{http_url}/orders"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(first_response.status(), reqwest::StatusCode::CREATED);
    let first = first_response.json::<CreateOrderResponse>().await.unwrap();
    first_server.abort();
    let _ = first_server.await;
    tokio::task::yield_now().await;

    let (http_url, _, restarted_server) = server_with_database(&database_url).await;
    let retry_response = client
        .post(format!("{http_url}/orders"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(retry_response.status(), reqwest::StatusCode::OK);
    let retry = retry_response.json::<CreateOrderResponse>().await.unwrap();
    assert_eq!(retry.order_id, first.order_id);
    assert_eq!(retry.expires_at_ms, first.expires_at_ms);

    let jobs = client
        .get(format!("{http_url}/solver/jobs"))
        .header(SOLVER_ADDRESS_HEADER, solver_address(0x11).to_string())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Vec<Order>>()
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);

    restarted_server.abort();
}

#[tokio::test]
async fn applies_default_ttl_and_rejects_out_of_range_ttl() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();
    let before = chrono::Utc::now().timestamp_millis();
    let created = client
        .post(format!("{http_url}/orders"))
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

    for (n, ttl_seconds) in [
        (2, MIN_ORDER_TTL_SECONDS - 1),
        (3, MAX_ORDER_TTL_SECONDS + 1),
    ] {
        let response = client
            .post(format!("{http_url}/orders"))
            .json(&create_order_request(n, Some(ttl_seconds)))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    }

    server.abort();
}

#[tokio::test]
async fn rejects_unsupported_chains_and_token_markets() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();

    let mut unsupported_chain = create_order_request(1, None);
    unsupported_chain.chain_id = 1;
    let response = client
        .post(format!("{http_url}/orders"))
        .json(&unsupported_chain)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);

    let mut unsupported_market = create_order_request(2, None);
    unsupported_market.token_out = alloy_primitives::Address::repeat_byte(3);
    let response = client
        .post(format!("{http_url}/orders"))
        .json(&unsupported_market)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);

    assert_eq!(create_order_request(3, None).chain_id, MOCK_CHAIN_ID);
    server.abort();
}

#[tokio::test]
async fn protects_user_order_access_with_the_commitment() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();
    let (order_id, order_commitment) = create_order_with_commitment(&client, &http_url, 1).await;
    let order_url = format!("{http_url}/orders/{order_id}");

    let missing = client.get(&order_url).send().await.unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);

    let wrong = client
        .get(&order_url)
        .header(ORDER_COMMITMENT_HEADER, commitment(2).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), reqwest::StatusCode::NOT_FOUND);

    let valid = client
        .get(&order_url)
        .header(ORDER_COMMITMENT_HEADER, order_commitment.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(valid.status(), reqwest::StatusCode::OK);

    let wrong_proof = client
        .post(format!("{order_url}/encrypted-proof"))
        .header(ORDER_COMMITMENT_HEADER, commitment(2).to_string())
        .json(&EncryptedProofRequest {
            ciphertext: vec![1],
        })
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_proof.status(), reqwest::StatusCode::NOT_FOUND);

    let valid_proof = client
        .post(format!("{order_url}/encrypted-proof"))
        .header(ORDER_COMMITMENT_HEADER, order_commitment.to_string())
        .json(&EncryptedProofRequest {
            ciphertext: vec![1],
        })
        .send()
        .await
        .unwrap();
    assert_eq!(valid_proof.status(), reqwest::StatusCode::CONFLICT);

    server.abort();
}

#[tokio::test]
async fn user_event_stream_is_private_and_reconnectable() {
    let (http_url, user_ws_url, server) = server().await;
    let client = reqwest::Client::new();
    let (first_id, first_commitment) = create_order_with_commitment(&client, &http_url, 1).await;
    let (second_id, second_commitment) = create_order_with_commitment(&client, &http_url, 2).await;
    let (mut socket, _) = connect_async(&user_ws_url).await.unwrap();

    let rejected = subscribe_user_events(&mut socket, first_id, second_commitment).await;
    assert!(matches!(
        rejected,
        UserEventServerMessage::Rejected { order_id } if order_id == first_id
    ));

    let subscribed = subscribe_user_events(&mut socket, first_id, first_commitment).await;
    assert!(matches!(
        subscribed,
        UserEventServerMessage::Subscribed { order_id } if order_id == first_id
    ));

    client
        .post(format!("{http_url}/orders/{second_id}/reserve"))
        .header(SOLVER_ADDRESS_HEADER, solver_address(0x22).to_string())
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
        .post(format!("{http_url}/orders/{first_id}/reserve"))
        .header(SOLVER_ADDRESS_HEADER, solver_address(0x11).to_string())
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
    let (mut reconnected, _) = connect_async(&user_ws_url).await.unwrap();
    let subscribed = subscribe_user_events(&mut reconnected, first_id, first_commitment).await;
    assert!(matches!(
        subscribed,
        UserEventServerMessage::Subscribed { order_id } if order_id == first_id
    ));

    let restored: Order = client
        .get(format!("{http_url}/orders/{first_id}"))
        .header(ORDER_COMMITMENT_HEADER, first_commitment.to_string())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(restored.state, OrderState::AwaitingUserProof);

    server.abort();
}

#[tokio::test]
async fn orders_wait_for_an_external_solver() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();

    for i in 1..=USERS {
        create_order(&client, &http_url, i).await;
    }

    let orders: Vec<Order> = client
        .get(format!("{http_url}/solver/jobs"))
        .header(SOLVER_ADDRESS_HEADER, solver_address(0x11).to_string())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(orders.len() as u64, USERS);
    assert!(
        orders
            .iter()
            .all(|order| order.state == OrderState::Reserving)
    );

    server.abort();
}

#[tokio::test]
async fn external_services_drive_orders_to_filled() {
    let orderbook = start_orderbook("sqlite::memory:").await.unwrap();
    let mut events = orderbook.subscribe();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let http_url = format!("http://{address}");
    let user_ws_url = format!("ws://{address}/events/user/ws");
    let solver_ws_url = format!("ws://{address}/events/solver/ws");
    let chain_ws_url = format!("ws://{address}/events/chain/ws");
    let server = tokio::spawn(async move {
        axum::serve(listener, api::router(orderbook, registry()))
            .await
            .unwrap();
    });

    let (chain_ready_tx, chain_ready_rx) = oneshot::channel();
    let chain = tokio::spawn(chain::run(http_url.clone(), chain_ws_url, chain_ready_tx));
    chain_ready_rx.await.unwrap();

    let client = reqwest::Client::new();
    let mut commitments = HashMap::new();
    for i in 1..=USERS {
        let (order_id, order_commitment) =
            create_order_with_commitment(&client, &http_url, i).await;
        commitments.insert(order_id, order_commitment);
    }

    let (user_ready_tx, user_ready_rx) = oneshot::channel();
    let user = tokio::spawn(user::run(
        http_url.clone(),
        user_ws_url,
        commitments,
        user_ready_tx,
    ));
    user_ready_rx.await.unwrap();

    let (solver_ready_tx, solver_ready_rx) = oneshot::channel();
    let solver = tokio::spawn(solver::run(
        http_url,
        solver_ws_url,
        solver_address(0x11),
        noise_private_key(0x33),
        solver_ready_tx,
    ));
    solver_ready_rx.await.unwrap();

    let mut seen: HashMap<OrderId, Vec<&'static str>> = HashMap::new();
    let mut filled = 0;

    while filled < USERS {
        let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("lifecycle timed out")
            .expect("event stream closed");

        let order_id = event.order_id();
        let label = match event {
            OrderEvent::OrderCreated { .. } => "created",
            OrderEvent::OrderValidated { .. } => "validated",
            OrderEvent::SolverReservationRequested { .. } => "reserving",
            OrderEvent::SolverAssigned { .. } => "assigned",
            OrderEvent::SolverSessionReady { .. } => "awaiting_proof",
            OrderEvent::ProofRelayed { .. } => "proof_relayed",
            OrderEvent::ExecutionStarted { .. } => "executing",
            OrderEvent::OrderFilled { .. } => {
                filled += 1;
                "filled"
            }
            other => panic!("unexpected event: {other:?}"),
        };
        seen.entry(order_id).or_default().push(label);
    }

    assert_eq!(seen.len() as u64, USERS);
    for trail in seen.values() {
        assert_eq!(
            *trail,
            vec![
                "created",
                "validated",
                "reserving",
                "assigned",
                "awaiting_proof",
                "proof_relayed",
                "executing",
                "filled"
            ]
        );
    }

    solver.abort();
    chain.abort();
    user.abort();
    server.abort();
}
