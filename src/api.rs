use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Path, State, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::core::command::Command;
use crate::core::engine::{OrderError, OrderbookHandle, ServiceError, SolverProofDelivery};
use crate::logging::short_id;
use crate::order::{Order, OrderCommitment, OrderId, SolverId, TradeTerms, TxHash};
use crate::storage::RepositoryError;

pub const ORDER_COMMITMENT_HEADER: &str = "x-order-commitment";

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateOrderRequest {
    pub order_commitment: OrderCommitment,
    #[serde(flatten)]
    pub terms: TradeTerms,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateOrderResponse {
    pub order_id: OrderId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReserveOrderRequest {
    pub solver_id: SolverId,
    pub noise_public_key: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EncryptedProofRequest {
    pub ciphertext: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecutionStartedRequest {
    pub solver_id: SolverId,
    pub tx_hash: TxHash,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SettlementRequest {
    pub tx_hash: TxHash,
}

pub fn router(orderbook: OrderbookHandle) -> Router {
    Router::new()
        .route("/orders", post(create_order))
        .route("/orders/{order_id}", get(get_order))
        .route("/solver/jobs", get(reserving_orders))
        .route("/solver/{solver_id}/proofs", get(take_solver_proofs))
        .route("/chain/jobs", get(executing_orders))
        .route("/orders/{order_id}/reserve", post(reserve_order))
        .route(
            "/orders/{order_id}/encrypted-proof",
            post(relay_encrypted_proof),
        )
        .route(
            "/orders/{order_id}/execution-started",
            post(execution_started),
        )
        .route("/orders/{order_id}/settlement", post(settlement))
        .route("/events/ws", get(events_ws))
        .with_state(orderbook)
}

async fn create_order(
    State(orderbook): State<OrderbookHandle>,
    Json(request): Json<CreateOrderRequest>,
) -> Result<impl IntoResponse, StatusCode> {
    let order_id = Uuid::new_v4();
    let terms = request.terms;
    crate::service_log!(
        "orderbook",
        "create request order={} token_in={} token_out={} amount_in={} amount_out={}",
        short_id(order_id),
        terms.token_in,
        terms.token_out,
        terms.amount_in,
        terms.amount_out
    );
    orderbook
        .execute(Command::CreateOrder {
            order_id,
            order_commitment: request.order_commitment,
            terms,
        })
        .await
        .map_err(status_for_error)?;

    crate::service_log!("orderbook", "create accepted order={}", short_id(order_id));
    Ok((StatusCode::CREATED, Json(CreateOrderResponse { order_id })))
}

async fn get_order(
    State(orderbook): State<OrderbookHandle>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
) -> Result<Json<Order>, StatusCode> {
    let order_commitment = commitment_from_headers(&headers)?;
    orderbook
        .get_order_by_commitment(order_id, order_commitment)
        .await
        .map_err(status_for_error)?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn reserving_orders(
    State(orderbook): State<OrderbookHandle>,
) -> Result<Json<Vec<Order>>, StatusCode> {
    orderbook
        .reserving_orders()
        .await
        .map(Json)
        .map_err(status_for_error)
}

async fn executing_orders(
    State(orderbook): State<OrderbookHandle>,
) -> Result<Json<Vec<Order>>, StatusCode> {
    orderbook
        .executing_orders()
        .await
        .map(Json)
        .map_err(status_for_error)
}

async fn take_solver_proofs(
    State(orderbook): State<OrderbookHandle>,
    Path(solver_id): Path<SolverId>,
) -> Result<Json<Vec<SolverProofDelivery>>, StatusCode> {
    orderbook
        .take_solver_proofs(solver_id)
        .await
        .map(Json)
        .map_err(status_for_error)
}

async fn reserve_order(
    State(orderbook): State<OrderbookHandle>,
    Path(order_id): Path<OrderId>,
    Json(request): Json<ReserveOrderRequest>,
) -> Result<StatusCode, StatusCode> {
    execute(
        &orderbook,
        Command::SolverReserved {
            order_id,
            solver_id: request.solver_id,
            noise_public_key: request.noise_public_key,
        },
    )
    .await
}

async fn relay_encrypted_proof(
    State(orderbook): State<OrderbookHandle>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
    Json(request): Json<EncryptedProofRequest>,
) -> Result<StatusCode, StatusCode> {
    let order_commitment = commitment_from_headers(&headers)?;
    orderbook
        .relay_encrypted_proof(order_id, order_commitment, request.ciphertext)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(status_for_error)
}

async fn execution_started(
    State(orderbook): State<OrderbookHandle>,
    Path(order_id): Path<OrderId>,
    Json(request): Json<ExecutionStartedRequest>,
) -> Result<StatusCode, StatusCode> {
    execute(
        &orderbook,
        Command::ExecutionStarted {
            order_id,
            solver_id: request.solver_id,
            tx_hash: request.tx_hash,
        },
    )
    .await
}

async fn settlement(
    State(orderbook): State<OrderbookHandle>,
    Path(order_id): Path<OrderId>,
    Json(request): Json<SettlementRequest>,
) -> Result<StatusCode, StatusCode> {
    execute(
        &orderbook,
        Command::SettlementObserved {
            order_id,
            tx_hash: request.tx_hash,
        },
    )
    .await
}

async fn execute(orderbook: &OrderbookHandle, command: Command) -> Result<StatusCode, StatusCode> {
    orderbook
        .execute(command)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(status_for_error)
}

async fn events_ws(
    ws: WebSocketUpgrade,
    State(orderbook): State<OrderbookHandle>,
) -> impl IntoResponse {
    let events = orderbook.subscribe();
    ws.on_upgrade(move |socket| forward_events(socket, events))
}

async fn forward_events(
    mut socket: WebSocket,
    mut events: tokio::sync::broadcast::Receiver<crate::core::events::OrderEvent>,
) {
    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        };

        let Ok(json) = serde_json::to_string(&event) else {
            continue;
        };

        if socket.send(Message::Text(json.into())).await.is_err() {
            return;
        }
    }
}

fn commitment_from_headers(headers: &HeaderMap) -> Result<OrderCommitment, StatusCode> {
    headers
        .get(ORDER_COMMITMENT_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .ok_or(StatusCode::NOT_FOUND)
}

fn status_for_error(error: ServiceError) -> StatusCode {
    match error {
        ServiceError::Repository(RepositoryError::DuplicateOrderCommitment) => StatusCode::CONFLICT,
        ServiceError::Closed | ServiceError::Repository(_) => StatusCode::SERVICE_UNAVAILABLE,
        ServiceError::Order(OrderError::InvalidTerms | OrderError::InvalidPayload) => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        ServiceError::Order(OrderError::NotFound) => StatusCode::NOT_FOUND,
        ServiceError::Order(OrderError::AlreadyExists | OrderError::InvalidState) => {
            StatusCode::CONFLICT
        }
    }
}
