use std::collections::HashSet;

use axum::{
    Json, Router,
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
pub use kage_types::api_types::{
    ApiErrorResponse, CreateOrderRequest, CreateOrderResponse, EncryptedProofRequest,
    ExecutionStartedRequest, ORDER_COMMITMENT_HEADER, SOLVER_ADDRESS_HEADER, SettlementRequest,
    UserEventClientMessage, UserEventServerMessage,
};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    core::{
        command::Command,
        engine::{OrderError, OrderbookHandle, ServiceError, SolverProofDelivery},
        events::OrderEvent,
        guards::{CreateOrderInput, OrderPolicy, validate_create_order},
    },
    logging::short_id,
    order::{Order, OrderCommitment, OrderId, OrderV1, SolverId},
    pricing::{PriceValidationError, PricingValidator},
    readiness::{ReadinessSnapshot, ServiceReadiness},
    registry::SolverRegistry,
    storage::RepositoryError,
};

#[derive(Clone)]
struct ApiState {
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    order_policy: OrderPolicy,
    pricing_validator: Option<PricingValidator>,
    readiness: ServiceReadiness,
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

type ApiError = (StatusCode, Json<ApiErrorResponse>);

pub fn router(orderbook: OrderbookHandle, registry: SolverRegistry) -> Router {
    router_with_policy(orderbook, registry, OrderPolicy::default())
}

pub fn router_with_policy(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    order_policy: OrderPolicy,
) -> Router {
    router_with_state(
        orderbook,
        registry,
        order_policy,
        None,
        ServiceReadiness::always_ready(),
    )
}

pub fn router_with_pricing(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    order_policy: OrderPolicy,
    pricing_validator: PricingValidator,
) -> Router {
    router_with_state(
        orderbook,
        registry,
        order_policy,
        Some(pricing_validator),
        ServiceReadiness::always_ready(),
    )
}

pub fn router_with_readiness(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    order_policy: OrderPolicy,
    pricing_validator: PricingValidator,
    readiness: ServiceReadiness,
) -> Router {
    router_with_state(
        orderbook,
        registry,
        order_policy,
        Some(pricing_validator),
        readiness,
    )
}

fn router_with_state(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    order_policy: OrderPolicy,
    pricing_validator: Option<PricingValidator>,
    readiness: ServiceReadiness,
) -> Router {
    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness_health))
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
            readiness,
        })
}

async fn liveness() -> StatusCode {
    StatusCode::OK
}

async fn readiness_health(State(state): State<ApiState>) -> (StatusCode, Json<ReadinessSnapshot>) {
    let snapshot = state.readiness.snapshot();
    let status = if snapshot.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(snapshot))
}

async fn create_order(
    State(state): State<ApiState>,
    Json(request): Json<CreateOrderRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let order_id = Uuid::new_v4();

    let validated = validate_create_order(
        request.into(),
        chrono::Utc::now().timestamp_millis(),
        &state.order_policy,
    )
    .map_err(|error| {
        api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_order",
            error.to_string(),
            Vec::new(),
        )
    })?;
    let terms = validated.terms;

    let existing = state
        .orderbook
        .find_order_by_commitment(validated.order_commitment)
        .await
        .map_err(api_error_for_service)?;
    if existing.is_none() {
        let readiness = state.readiness.snapshot();
        if !readiness.ready {
            crate::service_error!(
                "orderbook",
                "create rejected request={} status=503 reason=service_not_ready missing={}",
                short_id(order_id),
                readiness.missing.join(",")
            );
            return Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "service_not_ready",
                "orderbook is not accepting new orders",
                readiness.missing,
            ));
        }
        if let Some(validator) = &state.pricing_validator
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
            let code = if matches!(error, PriceValidationError::Pricing(_)) {
                "pricing_unavailable"
            } else {
                "invalid_quote"
            };
            return Err(api_error(status, code, error.to_string(), Vec::new()));
        }
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
        .map_err(api_error_for_service)?;
    let expires_at_ms = outcome.order.expires_at_ms.ok_or_else(|| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid_order_state",
            "created order is missing its expiry",
            Vec::new(),
        )
    })?;
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
) -> Result<Json<OrderV1>, StatusCode> {
    let order_commitment = commitment_from_headers(&headers)?;
    let order = state
        .orderbook
        .get_order_by_commitment(order_id, order_commitment)
        .await
        .map_err(status_for_error)?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(OrderV1::from(&order)))
}

async fn reserving_orders(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<Vec<OrderV1>>, StatusCode> {
    solver_id_from_headers(&headers)?;
    state
        .orderbook
        .reserving_orders()
        .await
        .map(|orders| Json(order_views(&orders)))
        .map_err(status_for_error)
}

async fn executing_orders(State(state): State<ApiState>) -> Result<Json<Vec<OrderV1>>, StatusCode> {
    state
        .orderbook
        .executing_orders()
        .await
        .map(|orders| Json(order_views(&orders)))
        .map_err(status_for_error)
}

fn order_views(orders: &[Order]) -> Vec<OrderV1> {
    orders.iter().map(OrderV1::from).collect()
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
    if profile.noise_public_key == alloy_primitives::B256::ZERO {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    execute(
        &state.orderbook,
        Command::SolverReserved {
            order_id,
            solver_id,
            noise_public_key: profile.noise_public_key.to_vec(),
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
    let profile = state
        .registry
        .get(solver_id)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .filter(|profile| profile.active)
        .ok_or(StatusCode::FORBIDDEN)?;
    if profile.noise_public_key == alloy_primitives::B256::ZERO {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let events = state.orderbook.subscribe();
    let readiness = state.readiness.clone();
    Ok(ws.on_upgrade(move |socket| async move {
        let connection = readiness.solver_connection();
        crate::service_log!("orderbook", "solver connected solver={solver_id}");
        forward_service_events(socket, events, ServiceStream::Solver(solver_id)).await;
        drop(connection);
        crate::service_log!("orderbook", "solver disconnected solver={solver_id}");
    }))
}

async fn chain_events_ws(ws: WebSocketUpgrade, State(state): State<ApiState>) -> impl IntoResponse {
    let events = state.orderbook.subscribe();
    let readiness = state.readiness.clone();
    ws.on_upgrade(move |socket| async move {
        let connection = readiness.chain_connection();
        crate::service_log!("orderbook", "chain connected");
        forward_service_events(socket, events, ServiceStream::Chain).await;
        drop(connection);
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
        tokio::select! {
            message = socket.recv() => match message {
                Some(Ok(Message::Ping(payload))) => {
                    if socket.send(Message::Pong(payload)).await.is_err() {
                        return;
                    }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                Some(Ok(_)) => {}
            },
            event = events.recv() => {
                let event = match event {
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
                if relevant && send_json(&mut socket, &event).await.is_err() {
                    return;
                }
            }
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

fn api_error(
    status: StatusCode,
    code: impl Into<String>,
    message: impl Into<String>,
    missing: Vec<String>,
) -> ApiError {
    (
        status,
        Json(ApiErrorResponse {
            code: code.into(),
            message: message.into(),
            missing,
        }),
    )
}

fn api_error_for_service(error: ServiceError) -> ApiError {
    let status = status_for_error(error);
    let message = match status {
        StatusCode::SERVICE_UNAVAILABLE => "order service is unavailable",
        StatusCode::UNPROCESSABLE_ENTITY => "order failed lifecycle validation",
        StatusCode::NOT_FOUND => "order was not found",
        StatusCode::CONFLICT => "order conflicts with its current state",
        _ => "order request failed",
    };
    api_error(status, "order_service_error", message, Vec::new())
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, U256};
    use tokio::net::TcpListener;

    use super::*;
    use crate::{core::engine::start_orderbook, readiness::ServiceReadiness};

    fn request(commitment: u8) -> CreateOrderRequest {
        CreateOrderRequest {
            order_commitment: B256::repeat_byte(commitment),
            chain_id: crate::core::guards::MOCK_CHAIN_ID,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
            amount_in: U256::from(1_u8),
            amount_out: U256::from(2_u8),
            ttl_seconds: None,
        }
    }

    #[tokio::test]
    async fn readiness_gates_only_new_order_admission() {
        let readiness = ServiceReadiness::new();
        let orderbook = start_orderbook("sqlite::memory:").await.unwrap();
        let inspection = orderbook.clone();
        let app = router_with_state(
            orderbook,
            SolverRegistry::from_profiles([]),
            OrderPolicy::default(),
            None,
            readiness.clone(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = reqwest::Client::new();
        let url = format!("http://{address}");

        assert_eq!(
            client
                .get(format!("{url}/health/live"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            client
                .get(format!("{url}/health/ready"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        readiness.set_pricing(true);
        readiness.set_registry(true);
        readiness.set_actor(true);
        let solver = readiness.solver_connection();
        let chain = readiness.chain_connection();
        assert_eq!(
            client
                .get(format!("{url}/health/ready"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        let accepted = request(1);
        assert_eq!(
            client
                .post(format!("{url}/orders"))
                .json(&accepted)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );

        drop(chain);
        let rejected = request(2);
        let response = client
            .post(format!("{url}/orders"))
            .json(&rejected)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let error = response.json::<ApiErrorResponse>().await.unwrap();
        assert_eq!(error.code, "service_not_ready");
        assert_eq!(error.missing, vec!["chain"]);
        assert!(
            inspection
                .find_order_by_commitment(rejected.order_commitment)
                .await
                .unwrap()
                .is_none()
        );

        let _chain = readiness.chain_connection();
        assert_eq!(
            client
                .post(format!("{url}/orders"))
                .json(&rejected)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );

        drop(solver);
        assert_eq!(
            client
                .post(format!("{url}/orders"))
                .json(&accepted)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            client
                .post(format!("{url}/orders"))
                .json(&request(3))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        server.abort();
    }
}
