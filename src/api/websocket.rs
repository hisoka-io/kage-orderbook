use std::collections::HashSet;

use axum::{
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::Serialize;

use super::{ApiState, UserEventClientMessage, UserEventServerMessage, auth};
use crate::{
    core::{engine::OrderbookHandle, events::OrderEvent},
    order::SolverId,
};

pub(super) async fn user_events_ws(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let events = state.orderbook.subscribe();
    ws.on_upgrade(move |socket| forward_user_events(socket, events, state.orderbook))
}

pub(super) async fn solver_events_ws(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    let solver_id = auth::authenticated_solver(&state, &headers)?;
    auth::active_solver(&state, solver_id)?;
    let events = state.orderbook.subscribe();
    let readiness = state.readiness.clone();
    Ok(ws.on_upgrade(move |socket| async move {
        let connection = readiness.solver_connection();
        tracing::debug!(target: "orderbook", %solver_id, "solver connected");
        forward_service_events(
            socket,
            events,
            SolverStream {
                solver_id,
                orderbook: state.orderbook,
            },
        )
        .await;
        drop(connection);
        tracing::debug!(target: "orderbook", %solver_id, "solver disconnected");
    }))
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

struct SolverStream {
    solver_id: SolverId,
    orderbook: OrderbookHandle,
}

async fn forward_service_events(
    mut socket: WebSocket,
    mut events: tokio::sync::broadcast::Receiver<OrderEvent>,
    stream: SolverStream,
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

                let SolverStream {
                    solver_id,
                    orderbook,
                } = &stream;
                let relevant = match &event {
                    OrderEvent::SolverReservationRequested { .. } => true,
                    OrderEvent::ProofRelayed {
                        solver_id: assigned,
                        ..
                    } => assigned == solver_id,
                    OrderEvent::OrderFilled { order_id, .. }
                    | OrderEvent::OrderExpired { order_id } => orderbook
                        .get_order(*order_id)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|order| order.solver == Some(*solver_id)),
                    _ => false,
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
