use std::{collections::HashSet, time::Duration};

use axum::{
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, header::ORIGIN},
    response::IntoResponse,
};
use serde::Serialize;

use super::{ApiState, UserEventClientMessage, UserEventServerMessage, auth};
use crate::{config::ApiSettings, core::events::OrderEvent, order::SolverId};
use tokio::time::Instant;

pub(super) async fn user_events_ws(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    ensure_allowed_origin(&state.api, &headers)?;
    let events = state.orderbook.subscribe();
    let api = state.api.clone();
    let max_message_bytes = api.websocket_max_message_bytes;
    let proof_orders = state.proof_orders;
    Ok(ws
        .max_message_size(max_message_bytes)
        .max_frame_size(max_message_bytes)
        .on_upgrade(move |socket| forward_user_events(socket, events, proof_orders, api)))
}

pub(super) async fn solver_events_ws(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    ensure_allowed_origin(&state.api, &headers)?;
    let session_token = auth::bearer_token(&headers)?;
    let session = state
        .sessions
        .resolve_session(&session_token, super::now_ms())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let solver_id = session.solver_id;
    auth::active_solver(&state, solver_id)?;
    let events = state.orderbook.subscribe();
    let readiness = state.readiness.clone();
    let max_message_bytes = state.api.websocket_max_message_bytes;
    let stream = SolverStream {
        solver_id,
        session_token,
        session_expires_at_ms: session.expires_at_ms,
        state,
    };
    Ok(ws
        .max_message_size(max_message_bytes)
        .max_frame_size(max_message_bytes)
        .on_upgrade(move |socket| async move {
            let connection = readiness.solver_connection();
            tracing::debug!(target: "orderbook", %solver_id, "solver connected");
            forward_service_events(socket, events, stream).await;
            drop(connection);
            tracing::debug!(target: "orderbook", %solver_id, "solver disconnected");
        }))
}

fn ensure_allowed_origin(api: &ApiSettings, headers: &HeaderMap) -> Result<(), StatusCode> {
    let Some(origin) = headers.get(ORIGIN) else {
        return Ok(());
    };
    if api
        .allowed_origins
        .iter()
        .any(|allowed| origin.as_bytes() == allowed.as_bytes())
    {
        return Ok(());
    }

    Err(StatusCode::FORBIDDEN)
}

async fn forward_user_events(
    mut socket: WebSocket,
    mut events: tokio::sync::broadcast::Receiver<OrderEvent>,
    proof_orders: crate::storage::ProofOrderRepository,
    api: ApiSettings,
) {
    let mut subscriptions = HashSet::new();
    let mut messages = MessageBudget::new();
    let mut last_seen = Instant::now();
    let mut heartbeat =
        tokio::time::interval(Duration::from_millis(api.websocket_heartbeat_interval_ms));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;

    loop {
        tokio::select! {
            message = socket.recv() => {
                let Some(Ok(message)) = message else {
                    return;
                };
                let now = Instant::now();
                last_seen = now;
                if !messages.allow(
                    now,
                    Duration::from_millis(api.websocket_message_window_ms),
                    api.websocket_message_burst,
                ) {
                    close(&mut socket).await;
                    return;
                }
                let text = match message {
                    Message::Text(text) => text,
                    Message::Ping(payload) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Close(_) => return,
                    _ => continue,
                };
                let Ok(UserEventClientMessage::Subscribe {
                    order_id,
                    access_token,
                }) = serde_json::from_str(&text) else {
                    continue;
                };

                let authorized = match proof_orders
                    .authenticated_snapshot(order_id, auth::access_token_hash(access_token))
                    .await
                {
                    Ok(Some(_)) => true,
                    Ok(None) => false,
                    Err(_) => return,
                };
                let response = if authorized {
                    if !subscriptions.contains(&order_id)
                        && subscriptions.len() >= api.websocket_max_subscriptions
                    {
                        close(&mut socket).await;
                        return;
                    }
                    subscriptions.insert(order_id);
                    UserEventServerMessage::Subscribed { order_id }
                } else {
                    UserEventServerMessage::Rejected { order_id }
                };
                if send_json(&mut socket, &response).await.is_err() {
                    return;
                }
            }
            _ = heartbeat.tick() => {
                if last_seen.elapsed()
                    >= Duration::from_millis(api.websocket_idle_timeout_ms)
                {
                    close(&mut socket).await;
                    return;
                }
                if socket.send(Message::Ping(Vec::new().into())).await.is_err() {
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

struct SolverStream {
    solver_id: SolverId,
    session_token: String,
    session_expires_at_ms: u64,
    state: ApiState,
}

async fn forward_service_events(
    mut socket: WebSocket,
    mut events: tokio::sync::broadcast::Receiver<OrderEvent>,
    stream: SolverStream,
) {
    let api = &stream.state.api;
    let mut messages = MessageBudget::new();
    let mut last_seen = Instant::now();
    let mut heartbeat =
        tokio::time::interval(Duration::from_millis(api.websocket_heartbeat_interval_ms));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    let mut auth_recheck = tokio::time::interval(Duration::from_millis(api.solver_auth_recheck_ms));
    auth_recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    auth_recheck.tick().await;
    let session_expiry = tokio::time::sleep(Duration::from_millis(
        stream.session_expires_at_ms.saturating_sub(super::now_ms()),
    ));
    tokio::pin!(session_expiry);

    loop {
        tokio::select! {
            message = socket.recv() => {
                let Some(Ok(message)) = message else {
                    return;
                };
                let now = Instant::now();
                last_seen = now;
                if !messages.allow(
                    now,
                    Duration::from_millis(api.websocket_message_window_ms),
                    api.websocket_message_burst,
                ) {
                    close(&mut socket).await;
                    return;
                }
                match message {
                    Message::Ping(payload) => {
                        let Ok(()) = socket.send(Message::Pong(payload)).await else {
                            return;
                        };
                    }
                    Message::Close(_) => return,
                    _ => {}
                }
            }
            _ = &mut session_expiry => {
                close(&mut socket).await;
                return;
            }
            _ = auth_recheck.tick() => {
                let current = stream
                    .state
                    .sessions
                    .resolve(&stream.session_token, super::now_ms())
                    == Some(stream.solver_id);
                if !current || auth::active_solver(&stream.state, stream.solver_id).is_err() {
                    close(&mut socket).await;
                    return;
                }
            }
            _ = heartbeat.tick() => {
                if last_seen.elapsed()
                    >= Duration::from_millis(api.websocket_idle_timeout_ms)
                {
                    close(&mut socket).await;
                    return;
                }
                if socket.send(Message::Ping(Vec::new().into())).await.is_err() {
                    return;
                }
            }
            event = events.recv() => {
                let event = match event {
                    Ok(event) => event,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                };

                let solver_id = stream.solver_id;
                let relevant = match &event {
                    OrderEvent::SolverReservationRequested { order_id, .. } => stream
                        .state
                        .proof_orders
                        .is_live_target(
                            *order_id,
                            solver_id,
                            i64::try_from(super::now_ms()).unwrap_or(i64::MAX),
                        )
                        .await
                        .unwrap_or(false),
                    OrderEvent::OrderExpired { order_id } => stream
                        .state
                        .orderbook
                        .get_order(*order_id)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|order| order.solver == Some(solver_id)),
                    _ => false,
                };
                if relevant && send_json(&mut socket, &event).await.is_err() {
                    return;
                }
            }
        }
    }
}

struct MessageBudget {
    window_started: Instant,
    used: u32,
}

impl MessageBudget {
    fn new() -> Self {
        Self {
            window_started: Instant::now(),
            used: 0,
        }
    }

    fn allow(&mut self, now: Instant, window: Duration, burst: u32) -> bool {
        if now.duration_since(self.window_started) >= window {
            self.window_started = now;
            self.used = 0;
        }
        if self.used >= burst {
            return false;
        }
        self.used += 1;
        true
    }
}

async fn close(socket: &mut WebSocket) {
    let _ = socket.send(Message::Close(None)).await;
}

async fn send_json(socket: &mut WebSocket, value: &impl Serialize) -> Result<(), ()> {
    let json = serde_json::to_string(value).map_err(|_| ())?;
    socket
        .send(Message::Text(json.into()))
        .await
        .map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header::ORIGIN};

    use super::*;

    fn headers(origin: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(origin) = origin {
            headers.insert(ORIGIN, HeaderValue::from_str(origin).unwrap());
        }
        headers
    }

    #[test]
    fn websocket_origin_policy_allows_non_browser_and_allowlist_only() {
        let api = ApiSettings {
            allowed_origins: vec!["https://app.example.com".to_owned()],
            ..ApiSettings::default()
        };

        assert!(ensure_allowed_origin(&api, &headers(None)).is_ok());
        assert!(ensure_allowed_origin(&api, &headers(Some("https://app.example.com")),).is_ok());
        assert_eq!(
            ensure_allowed_origin(&api, &headers(Some("https://attacker.example")),),
            Err(StatusCode::FORBIDDEN)
        );
    }

    #[test]
    fn websocket_message_budget_is_fixed_window_and_fail_closed() {
        let mut budget = MessageBudget::new();
        let start = budget.window_started;
        assert!(budget.allow(start, Duration::from_secs(1), 2));
        assert!(budget.allow(start, Duration::from_secs(1), 2));
        assert!(!budget.allow(start, Duration::from_secs(1), 2));
        assert!(budget.allow(start + Duration::from_secs(1), Duration::from_secs(1), 2));
    }
}
