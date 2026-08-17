use std::time::Duration;

use futures_util::StreamExt;
use kage_orderbook::logging::short_id;
use kage_types::{
    api_types::SettlementRequest,
    events::OrderEvent,
    identifiers::{OrderId, TxHash},
    orders::ChainOrderV1 as Order,
};
use tokio::sync::oneshot;
use tokio_tungstenite::connect_async;

pub async fn run(http_url: String, ws_url: String, ready: oneshot::Sender<()>) {
    let client = reqwest::Client::new();
    let (mut socket, _) = connect_async(&ws_url).await.unwrap();

    let jobs: Vec<Order> = client
        .get(format!("{http_url}/chain/jobs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    for order in jobs {
        if let Some(tx_hash) = order.tx_hash {
            settle(&client, &http_url, order.id, tx_hash).await;
        }
    }

    let _ = ready.send(());

    while let Some(message) = socket.next().await {
        let message = message.unwrap();
        if !message.is_text() {
            continue;
        }

        let event: OrderEvent = serde_json::from_str(message.to_text().unwrap()).unwrap();
        if let OrderEvent::ExecutionStarted { order_id, tx_hash } = event {
            settle(&client, &http_url, order_id, tx_hash).await;
        }
    }
}

async fn settle(client: &reqwest::Client, http_url: &str, order_id: OrderId, tx_hash: TxHash) {
    tokio::time::sleep(Duration::from_millis(1)).await;
    client
        .post(format!("{http_url}/orders/{order_id}/settlement"))
        .json(&SettlementRequest { tx_hash })
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    kage_orderbook::service_log!(
        "chain",
        "settlement confirmed order={} tx_hash={tx_hash}",
        short_id(order_id)
    );
}
