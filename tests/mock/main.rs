mod chain;
mod chaos;
mod expiry;
mod recovery;
mod solver;
mod support;
mod user;

use std::collections::HashMap;
use std::time::Duration;

use kage_orderbook::api::{
    self, CreateOrderRequest, CreateOrderResponse, EncryptedProofRequest, ORDER_COMMITMENT_HEADER,
};
use kage_orderbook::core::engine::start_orderbook;
use kage_orderbook::core::events::OrderEvent;
use kage_orderbook::order::{Order, OrderId, OrderState};
use support::{commitment, terms};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const USERS: u64 = 5;

async fn server() -> (String, String, JoinHandle<()>) {
    let orderbook = start_orderbook("sqlite::memory:").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, api::router(orderbook)).await.unwrap();
    });

    (
        format!("http://{address}"),
        format!("ws://{address}/events/ws"),
        task,
    )
}

async fn create_order(client: &reqwest::Client, http_url: &str, n: u64) -> OrderId {
    create_order_with_commitment(client, http_url, n).await.0
}

async fn create_order_with_commitment(
    client: &reqwest::Client,
    http_url: &str,
    n: u64,
) -> (OrderId, alloy_primitives::B256) {
    let request = CreateOrderRequest {
        order_commitment: commitment(n),
        terms: terms(n),
    };
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

#[tokio::test]
async fn rejects_a_duplicate_order_commitment() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();
    let request = CreateOrderRequest {
        order_commitment: commitment(1),
        terms: terms(1),
    };

    let first = client
        .post(format!("{http_url}/orders"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), reqwest::StatusCode::CREATED);

    let second = client
        .post(format!("{http_url}/orders"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), reqwest::StatusCode::CONFLICT);

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
async fn orders_wait_for_an_external_solver() {
    let (http_url, _, server) = server().await;
    let client = reqwest::Client::new();

    for i in 1..=USERS {
        create_order(&client, &http_url, i).await;
    }

    let orders: Vec<Order> = client
        .get(format!("{http_url}/solver/jobs"))
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
    let ws_url = format!("ws://{address}/events/ws");
    let server = tokio::spawn(async move {
        axum::serve(listener, api::router(orderbook)).await.unwrap();
    });

    let (chain_ready_tx, chain_ready_rx) = oneshot::channel();
    let chain = tokio::spawn(chain::run(http_url.clone(), ws_url.clone(), chain_ready_tx));
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
        ws_url.clone(),
        commitments,
        user_ready_tx,
    ));
    user_ready_rx.await.unwrap();

    let (solver_ready_tx, solver_ready_rx) = oneshot::channel();
    let solver = tokio::spawn(solver::run(http_url, ws_url, solver_ready_tx));
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
