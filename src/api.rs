use std::collections::HashSet;

use alloy_primitives::U256;
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
use crate::core::events::OrderEvent;
use crate::core::guards::{CreateOrderInput, OrderPolicy, validate_create_order};
use crate::logging::short_id;
use crate::order::{Order, OrderCommitment, OrderId, SolverId, TokenAddress, TxHash};
use crate::pricing::{PriceValidationError, PricingValidator};
use crate::registry::SolverRegistry;
use crate::storage::RepositoryError;

pub const ORDER_COMMITMENT_HEADER: &str = "x-order-commitment";
pub const SOLVER_ADDRESS_HEADER: &str = "x-solver-address";

#[derive(Clone)]
struct ApiState {
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    order_policy: OrderPolicy,
    pricing_validator: Option<PricingValidator>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrderRequest {
    pub order_commitment: OrderCommitment,
    pub chain_id: u64,
    pub token_in: TokenAddress,
    pub token_out: TokenAddress,
    pub amount_in: U256,
    pub amount_out: U256,
    pub ttl_seconds: Option<u32>,
}

impl From<CreateOrderRequest> for CreateOrderInput {
    fn from(request: CreateOrderRequest) -> Self {
        Self {
            order_commitment: request.order_commitment,
            chain_id: request.chain_id,
            token_in: request.token_in,
            token_out: request.token_out,
            amount_in: request.amount_in,
            amount_out: request.amount_out,
            ttl_seconds: request.ttl_seconds,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateOrderResponse {
    pub order_id: OrderId,
    pub expires_at_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EncryptedProofRequest {
    pub ciphertext: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecutionStartedRequest {
    pub tx_hash: TxHash,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SettlementRequest {
    pub tx_hash: TxHash,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UserEventClientMessage {
    Subscribe {
        order_id: OrderId,
        order_commitment: OrderCommitment,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UserEventServerMessage {
    Subscribed { order_id: OrderId },
    Rejected { order_id: OrderId },
    Event { event: OrderEvent },
}

pub fn router(orderbook: OrderbookHandle, registry: SolverRegistry) -> Router {
    router_with_policy(orderbook, registry, OrderPolicy::default())
}

pub fn router_with_policy(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    order_policy: OrderPolicy,
) -> Router {
    router_with_state(orderbook, registry, order_policy, None)
}

pub fn router_with_pricing(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    order_policy: OrderPolicy,
    pricing_validator: PricingValidator,
) -> Router {
    router_with_state(orderbook, registry, order_policy, Some(pricing_validator))
}

fn router_with_state(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    order_policy: OrderPolicy,
    pricing_validator: Option<PricingValidator>,
) -> Router {
    Router::new()
        .route("/orders", post(create_order))
        .route("/orders/{order_id}", get(get_order))
        .route("/solver/jobs", get(reserving_orders))
        .route("/solver/proofs", get(take_solver_proofs))
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
        .route("/events/user/ws", get(user_events_ws))
        .route("/events/solver/ws", get(solver_events_ws))
        .route("/events/chain/ws", get(chain_events_ws))
        .with_state(ApiState {
            orderbook,
            registry,
            order_policy,
            pricing_validator,
        })
}

async fn create_order(
    State(state): State<ApiState>,
    Json(request): Json<CreateOrderRequest>,
) -> Result<impl IntoResponse, StatusCode> {
    let order_id = Uuid::new_v4();

    let validated = validate_create_order(
        request.into(),
        chrono::Utc::now().timestamp_millis(),
        &state.order_policy,
    )
    .map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;
    let terms = validated.terms;

    let existing = state
        .orderbook
        .find_order_by_commitment(validated.order_commitment)
        .await
        .map_err(status_for_error)?;
    if existing.is_none()
        && let Some(validator) = &state.pricing_validator
        && let Err(error) = validator.validate(&terms)
    {
        let status = status_for_price_validation(&error);
        crate::service_error!(
            "orderbook",
            "create rejected request={} chain_id={} token_in={} token_out={} amount_in={} amount_out={} status={} reason={error}",
            short_id(order_id),
            terms.chain_id,
            terms.token_in,
            terms.token_out,
            terms.amount_in,
            terms.amount_out,
            status.as_u16()
        );
        return Err(status);
    }

    crate::service_log!(
        "orderbook",
        "create request order={} chain_id={} token_in={} token_out={} amount_in={} amount_out={} expires_at_ms={}",
        short_id(order_id),
        terms.chain_id,
        terms.token_in,
        terms.token_out,
        terms.amount_in,
        terms.amount_out,
        terms.expires_at_ms
    );

    let outcome = state
        .orderbook
        .create_order(order_id, validated.order_commitment, terms)
        .await
        .map_err(status_for_error)?;
    let expires_at_ms = outcome
        .order
        .expires_at_ms
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    let status = if outcome.created {
        crate::service_log!("orderbook", "create accepted order={}", short_id(order_id));
        StatusCode::CREATED
    } else {
        crate::service_log!(
            "orderbook",
            "create replayed order={}",
            short_id(outcome.order.id)
        );
        StatusCode::OK
    };

    Ok((
        status,
        Json(CreateOrderResponse {
            order_id: outcome.order.id,
            expires_at_ms,
        }),
    ))
}

async fn get_order(
    State(state): State<ApiState>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
) -> Result<Json<Order>, StatusCode> {
    let order_commitment = commitment_from_headers(&headers)?;
    state
        .orderbook
        .get_order_by_commitment(order_id, order_commitment)
        .await
        .map_err(status_for_error)?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn reserving_orders(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<Vec<Order>>, StatusCode> {
    solver_id_from_headers(&headers)?;
    state
        .orderbook
        .reserving_orders()
        .await
        .map(Json)
        .map_err(status_for_error)
}

async fn executing_orders(State(state): State<ApiState>) -> Result<Json<Vec<Order>>, StatusCode> {
    state
        .orderbook
        .executing_orders()
        .await
        .map(Json)
        .map_err(status_for_error)
}

async fn take_solver_proofs(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<Vec<SolverProofDelivery>>, StatusCode> {
    let solver_id = solver_id_from_headers(&headers)?;
    state
        .orderbook
        .take_solver_proofs(solver_id)
        .await
        .map(Json)
        .map_err(status_for_error)
}

async fn reserve_order(
    State(state): State<ApiState>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    let solver_id = solver_id_from_headers(&headers)?;
    let profile = state
        .registry
        .get(solver_id)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .filter(|profile| profile.active)
        .ok_or(StatusCode::FORBIDDEN)?;
    if profile.noise_key == alloy_primitives::B256::ZERO {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    execute(
        &state.orderbook,
        Command::SolverReserved {
            order_id,
            solver_id,
            noise_public_key: profile.noise_key.to_vec(),
        },
    )
    .await
}

async fn relay_encrypted_proof(
    State(state): State<ApiState>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
    Json(request): Json<EncryptedProofRequest>,
) -> Result<StatusCode, StatusCode> {
    let order_commitment = commitment_from_headers(&headers)?;
    state
        .orderbook
        .relay_encrypted_proof(order_id, order_commitment, request.ciphertext)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(status_for_error)
}

async fn execution_started(
    State(state): State<ApiState>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
    Json(request): Json<ExecutionStartedRequest>,
) -> Result<StatusCode, StatusCode> {
    let solver_id = solver_id_from_headers(&headers)?;
    execute(
        &state.orderbook,
        Command::ExecutionStarted {
            order_id,
            solver_id,
            tx_hash: request.tx_hash,
        },
    )
    .await
}

async fn settlement(
    State(state): State<ApiState>,
    Path(order_id): Path<OrderId>,
    Json(request): Json<SettlementRequest>,
) -> Result<StatusCode, StatusCode> {
    execute(
        &state.orderbook,
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

async fn user_events_ws(ws: WebSocketUpgrade, State(state): State<ApiState>) -> impl IntoResponse {
    let events = state.orderbook.subscribe();
    ws.on_upgrade(move |socket| forward_user_events(socket, events, state.orderbook))
}

async fn solver_events_ws(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    let solver_id = solver_id_from_headers(&headers)?;
    let events = state.orderbook.subscribe();
    Ok(ws.on_upgrade(move |socket| async move {
        crate::service_log!("orderbook", "solver connected solver={solver_id}");
        forward_service_events(socket, events, ServiceStream::Solver(solver_id)).await;
        crate::service_log!("orderbook", "solver disconnected solver={solver_id}");
    }))
}

async fn chain_events_ws(ws: WebSocketUpgrade, State(state): State<ApiState>) -> impl IntoResponse {
    let events = state.orderbook.subscribe();
    ws.on_upgrade(move |socket| async move {
        crate::service_log!("orderbook", "chain connected");
        forward_service_events(socket, events, ServiceStream::Chain).await;
        crate::service_log!("orderbook", "chain disconnected");
    })
}

async fn forward_user_events(
    mut socket: WebSocket,
    mut events: tokio::sync::broadcast::Receiver<OrderEvent>,
    orderbook: OrderbookHandle,
) {
    let mut subscriptions = HashSet::new();

    loop {
        tokio::select! {
            message = socket.recv() => {
                let Some(Ok(message)) = message else {
                    return;
                };
                let Message::Text(text) = message else {
                    if matches!(message, Message::Close(_)) {
                        return;
                    }
                    continue;
                };
                let Ok(UserEventClientMessage::Subscribe {
                    order_id,
                    order_commitment,
                }) = serde_json::from_str(&text) else {
                    continue;
                };

                let authorized = match orderbook
                    .get_order_by_commitment(order_id, order_commitment)
                    .await
                {
                    Ok(Some(_)) => true,
                    Ok(None) => false,
                    Err(_) => return,
                };
                let response = if authorized {
                    subscriptions.insert(order_id);
                    UserEventServerMessage::Subscribed { order_id }
                } else {
                    UserEventServerMessage::Rejected { order_id }
                };
                if send_json(&mut socket, &response).await.is_err() {
                    return;
                }
            }
            event = events.recv(), if !subscriptions.is_empty() => {
                let event = match event {
                    Ok(event) => event,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                };
                if subscriptions.contains(&event.order_id())
                    && send_json(&mut socket, &UserEventServerMessage::Event { event })
                        .await
                        .is_err()
                {
                    return;
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ServiceStream {
    Solver(SolverId),
    Chain,
}

async fn forward_service_events(
    mut socket: WebSocket,
    mut events: tokio::sync::broadcast::Receiver<OrderEvent>,
    stream: ServiceStream,
) {
    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        };

        let relevant = match stream {
            ServiceStream::Solver(solver_id) => match &event {
                OrderEvent::SolverReservationRequested { .. } => true,
                OrderEvent::ProofRelayed {
                    solver_id: assigned,
                    ..
                } => *assigned == solver_id,
                _ => false,
            },
            ServiceStream::Chain => matches!(event, OrderEvent::ExecutionStarted { .. }),
        };
        if !relevant {
            continue;
        }

        if send_json(&mut socket, &event).await.is_err() {
            return;
        }
    }
}

async fn send_json(socket: &mut WebSocket, value: &impl Serialize) -> Result<(), ()> {
    let json = serde_json::to_string(value).map_err(|_| ())?;
    socket
        .send(Message::Text(json.into()))
        .await
        .map_err(|_| ())
}

fn commitment_from_headers(headers: &HeaderMap) -> Result<OrderCommitment, StatusCode> {
    headers
        .get(ORDER_COMMITMENT_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .ok_or(StatusCode::NOT_FOUND)
}

fn solver_id_from_headers(headers: &HeaderMap) -> Result<SolverId, StatusCode> {
    headers
        .get(SOLVER_ADDRESS_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .ok_or(StatusCode::UNAUTHORIZED)
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

fn status_for_price_validation(error: &PriceValidationError) -> StatusCode {
    match error {
        PriceValidationError::Pricing(_) => StatusCode::SERVICE_UNAVAILABLE,
        PriceValidationError::UnsupportedMarket
        | PriceValidationError::Arithmetic
        | PriceValidationError::DeviationExceeded { .. } => StatusCode::UNPROCESSABLE_ENTITY,
    }
}
