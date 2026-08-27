mod auth;
mod error;
mod websocket;

use std::time::Duration;

use crate::{
    assignment::{AssignmentIssueError, AssignmentIssuer},
    config::ApiSettings,
    core::{
        command::Command,
        engine::OrderbookHandle,
        guards::{CreateOrderInput, OrderPolicy, validate_create_order},
    },
    logging::short_id,
    order::{OrderId, OrderV1, SolverJobV1},
    pricing::{PriceValidationError, PricingValidator},
    readiness::{ReadinessSnapshot, ServiceReadiness},
    registry::SolverRegistry,
    session::{
        ChallengeRequest, ChallengeResponse, SessionRequest, SessionResponse, SolverSessions,
        domain,
    },
};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, Method, StatusCode,
        header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE},
    },
    response::IntoResponse,
    routing::{get, post},
};
use error::{
    ApiError, api_error, api_error_for_service, status_for_error, status_for_price_validation,
};
pub use kage_types::api_types::{
    ApiErrorResponse, CreateOrderRequest, CreateOrderResponse, ORDER_COMMITMENT_HEADER,
    UserEventClientMessage, UserEventServerMessage,
};
use tower_governor::{GovernorLayer, governor::GovernorConfigBuilder};
use tower_http::{cors::CorsLayer, limit::RequestBodyLimitLayer, timeout::TimeoutLayer};
use uuid::Uuid;

#[derive(Clone)]
struct ApiState {
    assignment_issuer: AssignmentIssuer,
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    sessions: SolverSessions,
    order_policy: OrderPolicy,
    pricing_validator: Option<PricingValidator>,
    readiness: ServiceReadiness,
    api: ApiSettings,
}

fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

pub fn router(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    assignment_issuer: AssignmentIssuer,
) -> Router {
    router_with_policy(
        orderbook,
        registry,
        OrderPolicy::default(),
        assignment_issuer,
    )
}

pub fn router_with_policy(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    order_policy: OrderPolicy,
    assignment_issuer: AssignmentIssuer,
) -> Router {
    router_with_state(
        orderbook,
        registry,
        SolverSessions::new(
            domain(crate::config::Network::Localnet, 0),
            crate::config::Network::Localnet,
        ),
        order_policy,
        None,
        ServiceReadiness::always_ready(),
        ApiSettings::default(),
        assignment_issuer,
    )
}

pub fn router_with_pricing(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    order_policy: OrderPolicy,
    pricing_validator: PricingValidator,
    assignment_issuer: AssignmentIssuer,
) -> Router {
    router_with_state(
        orderbook,
        registry,
        SolverSessions::new(
            domain(crate::config::Network::Localnet, 0),
            crate::config::Network::Localnet,
        ),
        order_policy,
        Some(pricing_validator),
        ServiceReadiness::always_ready(),
        ApiSettings::default(),
        assignment_issuer,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn router_with_assignment(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    sessions: SolverSessions,
    order_policy: OrderPolicy,
    pricing_validator: PricingValidator,
    readiness: ServiceReadiness,
    api: ApiSettings,
    assignment_issuer: AssignmentIssuer,
) -> Router {
    router_with_state(
        orderbook,
        registry,
        sessions,
        order_policy,
        Some(pricing_validator),
        readiness,
        api,
        assignment_issuer,
    )
}

#[allow(clippy::too_many_arguments)]
fn router_with_state(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    sessions: SolverSessions,
    order_policy: OrderPolicy,
    pricing_validator: Option<PricingValidator>,
    readiness: ServiceReadiness,
    api: ApiSettings,
    assignment_issuer: AssignmentIssuer,
) -> Router {
    let mut rate_limit = GovernorConfigBuilder::default();
    rate_limit
        .period(Duration::from_millis(api.rate_limit_replenish_ms))
        .burst_size(api.rate_limit_burst);
    let rate_limit = rate_limit
        .use_headers()
        .finish()
        .expect("validated API rate limit settings");
    let rate_limit_limiter = rate_limit.limiter().clone();
    tokio::spawn(async move {
        let mut cleanup = tokio::time::interval(Duration::from_secs(60));
        cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            cleanup.tick().await;
            rate_limit_limiter.retain_recent();
        }
    });
    let cors = cors_layer(&api);
    let request_timeout = Duration::from_millis(api.request_timeout_ms);
    let max_body_bytes = api.max_body_bytes;

    let versioned = Router::new()
        .route("/orders", post(create_order))
        .route("/orders/{order_id}", get(get_order))
        .route("/orders/{order_id}/assignment", get(get_assignment))
        .route("/solver/challenge", post(solver_challenge))
        .route("/solver/session", post(solver_session))
        .route("/solver/jobs", get(reserving_orders))
        .route("/orders/{order_id}/reserve", post(reserve_order))
        .route("/orders/{order_id}/decline", post(decline_order))
        .route("/events/user/ws", get(websocket::user_events_ws))
        .route("/events/solver/ws", get(websocket::solver_events_ws))
        .layer(GovernorLayer::new(rate_limit))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::GATEWAY_TIMEOUT,
            request_timeout,
        ))
        .layer(RequestBodyLimitLayer::new(max_body_bytes))
        .layer(cors);

    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness_health))
        .nest("/v1", versioned)
        .with_state(ApiState {
            assignment_issuer,
            orderbook,
            registry,
            sessions,
            order_policy,
            pricing_validator,
            readiness,
            api,
        })
}

fn cors_layer(api: &ApiSettings) -> CorsLayer {
    let mut cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([
            AUTHORIZATION,
            CONTENT_TYPE,
            HeaderName::from_static(ORDER_COMMITMENT_HEADER),
        ])
        .max_age(Duration::from_secs(api.cors_max_age_seconds));
    if !api.allowed_origins.is_empty() {
        let origins = api
            .allowed_origins
            .iter()
            .map(|origin| {
                HeaderValue::from_str(origin).expect("validated API allowed origin header")
            })
            .collect::<Vec<_>>();
        cors = cors.allow_origin(origins);
    }
    cors
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

async fn solver_challenge(
    State(state): State<ApiState>,
    Json(request): Json<ChallengeRequest>,
) -> Result<Json<ChallengeResponse>, StatusCode> {
    state
        .sessions
        .issue_challenge(request.solver_endpoint, now_ms())
        .map(Json)
        .map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)
}

async fn solver_session(
    State(state): State<ApiState>,
    Json(request): Json<SessionRequest>,
) -> Result<Json<SessionResponse>, StatusCode> {
    let now = now_ms();
    let solver = state.sessions.recover(&request, now).map_err(|error| {
        crate::service_warn!("orderbook", "solver authentication failed reason={error}");
        StatusCode::UNAUTHORIZED
    })?;
    auth::active_solver(&state, solver.solver_id)?;

    tracing::debug!(target: "orderbook", solver_id = %solver.solver_id, "solver authenticated");
    Ok(Json(state.sessions.open(solver, now)))
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

async fn get_assignment(
    State(state): State<ApiState>,
    Path(order_id): Path<OrderId>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let order_commitment = auth::commitment_from_headers(&headers).map_err(|_| {
        api_error(
            StatusCode::NOT_FOUND,
            "order_not_found",
            "order was not found",
            Vec::new(),
        )
    })?;
    let order = state
        .orderbook
        .get_order_by_commitment(order_id, order_commitment)
        .await
        .map_err(api_error_for_service)?
        .ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                "order_not_found",
                "order was not found",
                Vec::new(),
            )
        })?;
    let solver_id = order.solver.ok_or_else(|| {
        api_error(
            StatusCode::CONFLICT,
            "assignment_not_ready",
            "order is not assigned to a solver",
            Vec::new(),
        )
    })?;
    let solver_endpoint = state
        .sessions
        .solver_endpoint(solver_id, now_ms())
        .ok_or_else(|| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "solver_endpoint_unavailable",
                "assigned solver must reconnect before proof delivery",
                Vec::new(),
            )
        })?;
    let assignment = state
        .assignment_issuer
        .issue(&order, &solver_endpoint, now_ms())
        .map_err(|error| {
            let (status, code) = match error {
                AssignmentIssueError::NotReady => (StatusCode::CONFLICT, "assignment_not_ready"),
                AssignmentIssueError::Expired => (StatusCode::GONE, "order_expired"),
                AssignmentIssueError::Signing => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "assignment_signing_unavailable",
                ),
            };
            api_error(status, code, error.to_string(), Vec::new())
        })?;
    tracing::debug!(
        target: "orderbook",
        order_id = %short_id(order_id),
        solver_id = %assignment.ticket.claims.solver_id,
        "direct assignment issued"
    );
    Ok(([(CACHE_CONTROL, "no-store")], Json(assignment)))
}

async fn reserving_orders(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<Vec<SolverJobV1>>, StatusCode> {
    auth::authenticated_solver(&state, &headers)?;
    state
        .orderbook
        .reserving_orders()
        .await
        .map(|orders| Json(orders.iter().map(SolverJobV1::from).collect()))
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

async fn execute(orderbook: &OrderbookHandle, command: Command) -> Result<StatusCode, StatusCode> {
    orderbook
        .execute(command)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(status_for_error)
}

#[cfg(test)]
mod tests {
    use alloy::signers::local::PrivateKeySigner;
    use alloy_primitives::{Address, B256, U256};
    use axum::http::{Method, StatusCode};
    use kage_types::assignment::SolverAssignmentV1;
    use tokio::{net::TcpListener, task::JoinHandle};

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

    fn assignment_issuer() -> AssignmentIssuer {
        AssignmentIssuer::for_test(PrivateKeySigner::from_slice(&[7; 32]).unwrap(), 60_000)
    }

    async fn spawn_api(api: ApiSettings) -> (String, JoinHandle<()>) {
        let orderbook = start_orderbook("sqlite::memory:").await.unwrap();
        let app = router_with_state(
            orderbook,
            SolverRegistry::from_profiles([]),
            SolverSessions::new("kage-orderbook:test:0", crate::config::Network::Localnet),
            OrderPolicy::default(),
            None,
            ServiceReadiness::always_ready(),
            api,
            assignment_issuer(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        (format!("http://{address}"), server)
    }

    #[tokio::test]
    async fn exposes_application_routes_only_under_v1() {
        let (url, server) = spawn_api(ApiSettings::default()).await;
        let client = reqwest::Client::new();

        assert_eq!(
            client
                .post(format!("{url}/solver/challenge"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            client
                .post(format!("{url}/v1/solver/challenge"))
                .json(&serde_json::json!({
                    "solver_endpoint": "http://127.0.0.1:3100"
                }))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        server.abort();
    }

    #[tokio::test]
    async fn assignment_ticket_is_exposed_only_to_the_order_owner_when_ready() {
        let orderbook = start_orderbook("sqlite::memory:").await.unwrap();
        let inspection = orderbook.clone();
        let ticket_signer = PrivateKeySigner::from_slice(&[7; 32]).unwrap();
        let solver_id = Address::repeat_byte(3);
        let issuer = AssignmentIssuer::for_test(ticket_signer.clone(), 60_000);
        let sessions =
            SolverSessions::new("kage-orderbook:test:0", crate::config::Network::Localnet);
        sessions.open(
            crate::session::AuthenticatedSolver {
                solver_id,
                solver_endpoint: "https://solver.kage.test".to_owned(),
            },
            now_ms(),
        );
        let app = router_with_state(
            orderbook,
            SolverRegistry::from_profiles([]),
            sessions,
            OrderPolicy::default(),
            None,
            ServiceReadiness::always_ready(),
            ApiSettings::default(),
            issuer,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        let client = reqwest::Client::new();
        let commitment = B256::repeat_byte(8);
        let mut create = request(8);
        create.order_commitment = commitment;
        let created = client
            .post(format!("http://{address}/v1/orders"))
            .json(&create)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<CreateOrderResponse>()
            .await
            .unwrap();
        let assignment_url = format!("http://{address}/v1/orders/{}/assignment", created.order_id);

        assert_eq!(
            client.get(&assignment_url).send().await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            client
                .get(&assignment_url)
                .header(ORDER_COMMITMENT_HEADER, commitment.to_string())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );

        inspection
            .execute(Command::SolverReserved {
                order_id: created.order_id,
                solver_id,
                noise_public_key: vec![9; 32],
            })
            .await
            .unwrap();

        assert_eq!(
            client
                .get(&assignment_url)
                .header(ORDER_COMMITMENT_HEADER, B256::repeat_byte(7).to_string(),)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        let response = client
            .get(&assignment_url)
            .header(ORDER_COMMITMENT_HEADER, commitment.to_string())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        let body = response.text().await.unwrap();
        assert!(!body.contains(&commitment.to_string()));
        let assignment: SolverAssignmentV1 = serde_json::from_str(&body).unwrap();
        assert_eq!(assignment.ticket.claims.order_id, created.order_id);
        assert_eq!(assignment.ticket.claims.solver_id, solver_id);
        assert_eq!(
            assignment.ticket.claims.solver_endpoint,
            "https://solver.kage.test"
        );
        let signature =
            alloy_primitives::Signature::try_from(assignment.ticket.signature.as_slice()).unwrap();
        assert_eq!(
            signature
                .recover_address_from_msg(assignment.ticket.claims.signing_bytes())
                .unwrap(),
            ticket_signer.address()
        );

        server.abort();
    }

    #[tokio::test]
    async fn enforces_body_limit_and_keeps_health_outside_rate_limit() {
        let api = ApiSettings {
            max_body_bytes: 128,
            rate_limit_burst: 2,
            rate_limit_replenish_ms: 60_000,
            ..ApiSettings::default()
        };
        let (url, server) = spawn_api(api).await;
        let client = reqwest::Client::new();

        let oversized = client
            .post(format!("{url}/v1/orders"))
            .header("content-type", "application/json")
            .body(vec![b' '; 129])
            .send()
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);

        assert_eq!(
            client
                .post(format!("{url}/v1/solver/challenge"))
                .json(&serde_json::json!({
                    "solver_endpoint": "http://127.0.0.1:3100"
                }))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            client
                .post(format!("{url}/v1/solver/challenge"))
                .json(&serde_json::json!({
                    "solver_endpoint": "http://127.0.0.1:3100"
                }))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            client
                .post(format!("{url}/v1/solver/challenge"))
                .json(&serde_json::json!({
                    "solver_endpoint": "http://127.0.0.1:3100"
                }))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            client
                .get(format!("{url}/health/live"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        server.abort();
    }

    #[tokio::test]
    async fn cors_preflight_uses_exact_origin_allowlist() {
        let api = ApiSettings {
            allowed_origins: vec!["https://app.example.com".to_owned()],
            ..ApiSettings::default()
        };
        let (url, server) = spawn_api(api).await;
        let client = reqwest::Client::new();

        let allowed = client
            .request(Method::OPTIONS, format!("{url}/v1/orders"))
            .header("origin", "https://app.example.com")
            .header("access-control-request-method", "POST")
            .header("access-control-request-headers", "content-type")
            .send()
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
        assert_eq!(
            allowed
                .headers()
                .get("access-control-allow-origin")
                .unwrap(),
            "https://app.example.com"
        );

        let denied = client
            .request(Method::OPTIONS, format!("{url}/v1/orders"))
            .header("origin", "https://attacker.example")
            .header("access-control-request-method", "POST")
            .send()
            .await
            .unwrap();
        assert!(
            denied
                .headers()
                .get("access-control-allow-origin")
                .is_none()
        );

        server.abort();
    }

    #[tokio::test]
    async fn readiness_gates_only_new_order_admission() {
        let readiness = ServiceReadiness::new();
        let orderbook = start_orderbook("sqlite::memory:").await.unwrap();
        let inspection = orderbook.clone();
        let app = router_with_state(
            orderbook,
            SolverRegistry::from_profiles([]),
            SolverSessions::new("kage-orderbook:test:0", crate::config::Network::Localnet),
            OrderPolicy::default(),
            None,
            readiness.clone(),
            ApiSettings::default(),
            assignment_issuer(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
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
                .post(format!("{url}/v1/orders"))
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
            .post(format!("{url}/v1/orders"))
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
                .post(format!("{url}/v1/orders"))
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
                .post(format!("{url}/v1/orders"))
                .json(&accepted)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            client
                .post(format!("{url}/v1/orders"))
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
