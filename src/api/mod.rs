mod auth;
mod error;
mod websocket;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
pub use kage_types::api_types::{
    ApiErrorResponse, CreateOrderRequest, CreateOrderResponse, EncryptedProofRequest,
    ExecutionStartedRequest, ORDER_COMMITMENT_HEADER, UserEventClientMessage,
    UserEventServerMessage,
};

use crate::{
    core::{
        command::Command,
        engine::{OrderbookHandle, SolverProofDelivery},
        guards::{CreateOrderInput, OrderPolicy, validate_create_order},
    },
    logging::short_id,
    order::{OrderId, OrderV1},
    pricing::{PriceValidationError, PricingValidator},
    readiness::{ReadinessSnapshot, ServiceReadiness},
    registry::SolverRegistry,
    session::{ChallengeResponse, SessionRequest, SessionResponse, SolverSessions, domain},
};
use error::{
    ApiError, api_error, api_error_for_service, status_for_error, status_for_price_validation,
};
use uuid::Uuid;

#[derive(Clone)]
struct ApiState {
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    sessions: SolverSessions,
    order_policy: OrderPolicy,
    pricing_validator: Option<PricingValidator>,
    readiness: ServiceReadiness,
}

fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

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
        SolverSessions::new(domain(crate::config::Network::Localnet, 0)),
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
        SolverSessions::new(domain(crate::config::Network::Localnet, 0)),
        order_policy,
        Some(pricing_validator),
        ServiceReadiness::always_ready(),
    )
}

pub fn router_with_readiness(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    sessions: SolverSessions,
    order_policy: OrderPolicy,
    pricing_validator: PricingValidator,
    readiness: ServiceReadiness,
) -> Router {
    router_with_state(
        orderbook,
        registry,
        sessions,
        order_policy,
        Some(pricing_validator),
        readiness,
    )
}

fn router_with_state(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    sessions: SolverSessions,
    order_policy: OrderPolicy,
    pricing_validator: Option<PricingValidator>,
    readiness: ServiceReadiness,
) -> Router {
    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness_health))
        .route("/orders", post(create_order))
        .route("/orders/{order_id}", get(get_order))
        .route("/solver/challenge", post(solver_challenge))
        .route("/solver/session", post(solver_session))
        .route("/solver/jobs", get(reserving_orders))
        .route("/solver/proofs", get(take_solver_proofs))
        .route("/orders/{order_id}/reserve", post(reserve_order))
        .route("/orders/{order_id}/decline", post(decline_order))
        .route(
            "/orders/{order_id}/encrypted-proof",
            post(relay_encrypted_proof),
        )
        .route(
            "/orders/{order_id}/execution-started",
            post(execution_started),
        )
        .route("/events/user/ws", get(websocket::user_events_ws))
        .route("/events/solver/ws", get(websocket::solver_events_ws))
        .with_state(ApiState {
            orderbook,
            registry,
            sessions,
            order_policy,
            pricing_validator,
            readiness,
        })
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

async fn solver_challenge(State(state): State<ApiState>) -> Json<ChallengeResponse> {
    Json(state.sessions.issue_challenge(now_ms()))
}

async fn solver_session(
    State(state): State<ApiState>,
    Json(request): Json<SessionRequest>,
) -> Result<Json<SessionResponse>, StatusCode> {
    let now = now_ms();
    let solver_id = state.sessions.recover(&request, now).map_err(|error| {
        crate::service_warn!("orderbook", "solver authentication failed reason={error}");
        StatusCode::UNAUTHORIZED
    })?;
    auth::active_solver(&state, solver_id)?;

    tracing::debug!(target: "orderbook", %solver_id, "solver authenticated");
    Ok(Json(state.sessions.open(solver_id, now)))
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
            crate::service_warn!(
                "orderbook",
                "create rejected order_id={} status=503 reason=service_not_ready missing={}",
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
            crate::service_warn!(
                "orderbook",
                "create rejected order_id={} chain_id={} token_in={} token_out={} amount_in={} amount_out={} status={} reason={error}",
                short_id(order_id),
                terms.chain_id,
                terms.token_in,
                terms.token_out,
                terms.amount_in,
                terms.amount_out,
                status.as_u16()
            );
            let code = match error {
                PriceValidationError::Pricing(_) => "pricing_unavailable",
                PriceValidationError::OrderValueExceeded { .. } => "order_value_limit",
                _ => "invalid_quote",
            };
            return Err(api_error(status, code, error.to_string(), Vec::new()));
        }
    }

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
        crate::service_log!(
            "orderbook",
            "order created order_id={} expires_at_ms={expires_at_ms}",
            short_id(order_id)
        );
        StatusCode::CREATED
    } else {
        tracing::debug!(
            target: "orderbook",
            order_id = %short_id(outcome.order.id),
            "create replayed"
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
    let order_commitment = auth::commitment_from_headers(&headers)?;
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
    auth::authenticated_solver(&state, &headers)?;
    state
        .orderbook
        .reserving_orders()
        .await
        .map(|orders| Json(orders.iter().map(OrderV1::from).collect()))
        .map_err(status_for_error)
}

async fn take_solver_proofs(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<Vec<SolverProofDelivery>>, StatusCode> {
    let solver_id = auth::authenticated_solver(&state, &headers)?;
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
    let solver_id = auth::authenticated_solver(&state, &headers)?;
    let profile = auth::active_solver(&state, solver_id)?;
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

async fn decline_order(
    State(state): State<ApiState>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    let solver_id = auth::authenticated_solver(&state, &headers)?;
    let order = state
        .orderbook
        .get_order(order_id)
        .await
        .map_err(status_for_error)?
        .ok_or(StatusCode::NOT_FOUND)?;
    if order.solver != Some(solver_id) {
        return Err(StatusCode::FORBIDDEN);
    }

    crate::service_warn!(
        "orderbook",
        "solver declined order_id={} solver={solver_id}",
        short_id(order_id)
    );
    execute(
        &state.orderbook,
        Command::SolverDeclined {
            order_id,
            solver_id,
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
    let order_commitment = auth::commitment_from_headers(&headers)?;
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
    let solver_id = auth::authenticated_solver(&state, &headers)?;
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

async fn execute(orderbook: &OrderbookHandle, command: Command) -> Result<StatusCode, StatusCode> {
    orderbook
        .execute(command)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(status_for_error)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, U256};
    use axum::http::StatusCode;
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
            SolverSessions::new("kage-orderbook:test:0"),
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
        readiness.set_engine(true);
        readiness.set_chain(true);
        let solver = readiness.solver_connection();
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

        readiness.set_chain(false);
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

        readiness.set_chain(true);
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
