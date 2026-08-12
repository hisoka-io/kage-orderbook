use futures_util::StreamExt;
use kage_orderbook::api::EncryptedProofRequest;
use kage_orderbook::core::events::OrderEvent;
use kage_orderbook::logging::short_id;
use tokio::sync::oneshot;
use tokio_tungstenite::connect_async;

pub async fn run(http_url: String, ws_url: String, ready: oneshot::Sender<()>) {
    let client = reqwest::Client::new();
    let (mut socket, _) = connect_async(&ws_url).await.unwrap();
    let _ = ready.send(());

    while let Some(message) = socket.next().await {
        let message = message.unwrap();
        if !message.is_text() {
            continue;
        }

        let event: OrderEvent = serde_json::from_str(message.to_text().unwrap()).unwrap();
        if let OrderEvent::SolverSessionReady {
            order_id,
            noise_public_key,
            ..
        } = event
        {
            let proof = format!("proof:{order_id}").into_bytes();
            let ciphertext = xor(&proof, &noise_public_key);

            client
                .post(format!("{http_url}/orders/{order_id}/encrypted-proof"))
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

fn xor(bytes: &[u8], key: &[u8]) -> Vec<u8> {
    bytes
        .iter()
        .zip(key.iter().cycle())
        .map(|(byte, key)| byte ^ key)
        .collect()
}
