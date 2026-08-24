use std::collections::{HashMap, HashSet};

use futures_util::{SinkExt, StreamExt};
use kage_orderbook::logging::short_id;
use kage_types::{
    api_types::{
        EncryptedProofRequest, ORDER_COMMITMENT_HEADER, UserEventClientMessage,
        UserEventServerMessage,
    },
    events::OrderEvent,
    identifiers::{OrderCommitment, OrderId},
};
use tokio::sync::oneshot;
use tokio_tungstenite::connect_async;

use super::proof_transport;

pub async fn run(
    http_url: String,
    ws_url: String,
    commitments: HashMap<OrderId, OrderCommitment>,
    ready: oneshot::Sender<()>,
) {
    let client = reqwest::Client::new();
    let (mut socket, _) = connect_async(&ws_url).await.unwrap();
    let mut pending = commitments.keys().copied().collect::<HashSet<_>>();
    for (order_id, order_commitment) in &commitments {
        let message = UserEventClientMessage::Subscribe {
            order_id: *order_id,
            order_commitment: *order_commitment,
        };
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                serde_json::to_string(&message).unwrap().into(),
            ))
            .await
            .unwrap();
    }

    while !pending.is_empty() {
        let message = socket.next().await.unwrap().unwrap();
        if !message.is_text() {
            continue;
        }
        match serde_json::from_str::<UserEventServerMessage>(message.to_text().unwrap()).unwrap() {
            UserEventServerMessage::Subscribed { order_id } => {
                pending.remove(&order_id);
            }
            UserEventServerMessage::Rejected { order_id } => {
                panic!("subscription rejected for {order_id}");
            }
            UserEventServerMessage::Event { .. } => {}
        }
    }
    let _ = ready.send(());

    while let Some(message) = socket.next().await {
        let message = message.unwrap();
        if !message.is_text() {
            continue;
        }

        let UserEventServerMessage::Event { event } =
            serde_json::from_str(message.to_text().unwrap()).unwrap()
        else {
            continue;
        };
        if let OrderEvent::SolverSessionReady {
            order_id,
            noise_public_key,
            ..
        } = event
        {
            let Some(order_commitment) = commitments.get(&order_id) else {
                continue;
            };
            let proof = format!("proof:{order_id}").into_bytes();
            let ciphertext =
                proof_transport::encrypt_for_solver(order_id, &noise_public_key, &proof).unwrap();

            client
                .post(format!("{http_url}/v1/orders/{order_id}/encrypted-proof"))
                .header(ORDER_COMMITMENT_HEADER, order_commitment.to_string())
                .json(&EncryptedProofRequest { ciphertext })
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap();

            kage_orderbook::service_log!(
                "user",
                "encrypted proof sent order={}",
                short_id(order_id)
            );
        }
    }
}
