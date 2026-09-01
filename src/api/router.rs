use std::{collections::HashSet, sync::Arc, time::Duration};

use alloy_primitives::Address;
use axum::{
    Json, Router,
    extract::State,
    http::{
        HeaderName, HeaderValue, Method, StatusCode,
        header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE},
    },
    response::IntoResponse,
    routing::{get, post},
};
use tower_governor::{GovernorLayer, governor::GovernorConfigBuilder};
use tower_http::{cors::CorsLayer, limit::RequestBodyLimitLayer, timeout::TimeoutLayer};

use super::{
    ApiState, ORDER_ACCESS_TOKEN_HEADER,
    health::{liveness, readiness_health},
    now_ms,
    solver::{
        capabilities::register_solver_capabilities,
        jobs::reserving_orders,
        reservations::{decline_order, reserve_order},
        results::solver_order_result,
        sessions::{solver_challenge, solver_session},
    },
    user::{
        complaints::{create_complaint, get_complaint},
        orders::{create_encrypted_order, get_order},
        preview::create_preview,
    },
    websocket,
};
use crate::{
    NamedTask, Shutdown,
    assignment::AssignmentIssuer,
    complaint::{ComplaintEvidenceCipher, ComplaintVerifier},
    config::{ApiSettings, ProofOrderSettings},
    core::engine::OrderbookHandle,
    preview::PreviewService,
    readiness::ServiceReadiness,
    registry::SolverRegistry,
    session::SolverSessions,
    storage::ProofOrderRepository,
};

pub struct ApiRuntime {
    pub router: Router,
    pub tasks: Vec<NamedTask>,
}

#[allow(clippy::too_many_arguments)]
pub fn router(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    sessions: SolverSessions,
    preview: PreviewService,
    proof_orders: ProofOrderRepository,
    complaint_verifier: ComplaintVerifier,
    complaint_evidence_cipher: ComplaintEvidenceCipher,
    readiness: ServiceReadiness,
    api: ApiSettings,
    assignment_issuer: AssignmentIssuer,
    allowed_solvers: HashSet<Address>,
    proof_order_settings: ProofOrderSettings,
) -> Router {
    let runtime = build_router_with_components(
        orderbook,
        registry,
        sessions,
        Some(preview),
        proof_orders,
        Some(complaint_verifier),
        Some(complaint_evidence_cipher),
        readiness,
        api,
        assignment_issuer,
        Arc::new(allowed_solvers),
        proof_order_settings,
        Shutdown::new(),
        true,
    );
    // This compatibility entry point preserves the previous detached-task
    // behavior. Production startup uses `supervised_router` and owns the handles.
    drop(runtime.tasks);
    runtime.router
}

#[allow(clippy::too_many_arguments)]
pub fn supervised_router(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    sessions: SolverSessions,
    preview: PreviewService,
    proof_orders: ProofOrderRepository,
    complaint_verifier: ComplaintVerifier,
    complaint_evidence_cipher: ComplaintEvidenceCipher,
    readiness: ServiceReadiness,
    api: ApiSettings,
    assignment_issuer: AssignmentIssuer,
    allowed_solvers: HashSet<Address>,
    proof_order_settings: ProofOrderSettings,
    shutdown: Shutdown,
) -> ApiRuntime {
    build_router_with_components(
        orderbook,
        registry,
        sessions,
        Some(preview),
        proof_orders,
        Some(complaint_verifier),
        Some(complaint_evidence_cipher),
        readiness,
        api,
        assignment_issuer,
        Arc::new(allowed_solvers),
        proof_order_settings,
        shutdown,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(super) fn router_with_components(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    sessions: SolverSessions,
    preview: Option<PreviewService>,
    proof_orders: ProofOrderRepository,
    complaint_verifier: Option<ComplaintVerifier>,
    complaint_evidence_cipher: Option<ComplaintEvidenceCipher>,
    readiness: ServiceReadiness,
    api: ApiSettings,
    assignment_issuer: AssignmentIssuer,
    allowed_solvers: Arc<HashSet<Address>>,
    proof_order_settings: ProofOrderSettings,
) -> Router {
    build_router_with_components(
        orderbook,
        registry,
        sessions,
        preview,
        proof_orders,
        complaint_verifier,
        complaint_evidence_cipher,
        readiness,
        api,
        assignment_issuer,
        allowed_solvers,
        proof_order_settings,
        Shutdown::new(),
        false,
    )
    .router
}

#[allow(clippy::too_many_arguments)]
fn build_router_with_components(
    orderbook: OrderbookHandle,
    registry: SolverRegistry,
    sessions: SolverSessions,
    preview: Option<PreviewService>,
    proof_orders: ProofOrderRepository,
    complaint_verifier: Option<ComplaintVerifier>,
    complaint_evidence_cipher: Option<ComplaintEvidenceCipher>,
    readiness: ServiceReadiness,
    api: ApiSettings,
    assignment_issuer: AssignmentIssuer,
    allowed_solvers: Arc<HashSet<Address>>,
    proof_order_settings: ProofOrderSettings,
    shutdown: Shutdown,
    supervise_background: bool,
) -> ApiRuntime {
    let mut tasks = Vec::new();
    let mut rate_limit = GovernorConfigBuilder::default();
    rate_limit
        .period(Duration::from_millis(api.rate_limit_replenish_ms))
        .burst_size(api.rate_limit_burst);
    let rate_limit = rate_limit
        .use_headers()
        .finish()
        .expect("validated API rate limit settings");
    let rate_limit_limiter = rate_limit.limiter().clone();
    if supervise_background {
        let cleanup_shutdown = shutdown.clone();
        let handle = tokio::spawn(async move {
            let mut cleanup = tokio::time::interval(Duration::from_secs(60));
            cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = cleanup_shutdown.cancelled() => return,
                    _ = cleanup.tick() => rate_limit_limiter.retain_recent(),
                }
            }
        });
        tasks.push(NamedTask::new("rate_limit_cleanup", handle));
    }
    let cors = cors_layer(&api);
    let request_timeout = Duration::from_millis(api.request_timeout_ms);
    let max_body_bytes = api.max_body_bytes;

    if supervise_background && let Some(preview) = preview.clone() {
        let cleanup_shutdown = shutdown.clone();
        let handle = tokio::spawn(async move {
            let mut cleanup = tokio::time::interval(Duration::from_secs(60));
            cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = cleanup_shutdown.cancelled() => return,
                    _ = cleanup.tick() => {
                        match preview.cleanup(now_ms() as i64).await {
                            Ok(erased) if erased > 0 => {
                                crate::service_log!("orderbook", "preview cleanup snapshots={erased}");
                            }
                            Ok(_) => {}
                            Err(error) => {
                                crate::service_error!("orderbook", "preview cleanup failed error={error}");
                            }
                        }
                    }
                }
            }
        });
        tasks.push(NamedTask::new("preview_cleanup", handle));
    }
    let versioned = Router::new()
        .route("/preview", post(create_preview))
        .route("/orders", post(create_encrypted_order))
        .route("/orders/{order_id}", get(get_order))
        .route("/solver/challenge", post(solver_challenge))
        .route("/solver/session", post(solver_session))
        .route("/solver/capabilities", post(register_solver_capabilities))
        .route("/solver/jobs", get(reserving_orders))
        .route("/orders/{order_id}/reserve", post(reserve_order))
        .route("/orders/{order_id}/decline", post(decline_order))
        .route("/orders/{order_id}/result", post(solver_order_result))
        .route(
            "/orders/{order_id}/complaint",
            post(create_complaint).get(get_complaint),
        )
        .route("/events/user/ws", get(websocket::user_events_ws))
        .route("/events/solver/ws", get(websocket::solver_events_ws))
        .layer(GovernorLayer::new(rate_limit))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::GATEWAY_TIMEOUT,
            request_timeout,
        ))
        .layer(RequestBodyLimitLayer::new(max_body_bytes))
        .layer(cors);

    let router = Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness_health))
        .route("/metrics/retention", get(retention_metrics))
        .nest("/v1", versioned)
        .with_state(ApiState {
            assignment_issuer,
            orderbook,
            registry,
            sessions,
            readiness,
            api,
            preview,
            proof_orders,
            complaint_verifier,
            complaint_evidence_cipher,
            allowed_solvers,
            proof_order_settings,
            shutdown,
        });

    ApiRuntime { router, tasks }
}

async fn retention_metrics(State(state): State<ApiState>) -> impl IntoResponse {
    let snapshot = state.proof_orders.retention_metrics();
    ([(CACHE_CONTROL, "no-store")], Json(snapshot))
}

fn cors_layer(api: &ApiSettings) -> CorsLayer {
    let mut cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([
            AUTHORIZATION,
            CONTENT_TYPE,
            HeaderName::from_static(ORDER_ACCESS_TOKEN_HEADER),
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
