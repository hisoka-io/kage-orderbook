use std::error::Error;
use std::time::Duration;

use futures_util::StreamExt;
use kage_orderbook::api::SettlementRequest;
use kage_orderbook::core::events::OrderEvent;
use kage_orderbook::logging::short_id;
use kage_orderbook::order::{Order, OrderId, TxHash};
use reqwest::StatusCode;
use tokio_tungstenite::connect_async;

type BoxError = Box<dyn Error + Send + Sync>;

#[tokio::main]
async fn main() {
    let http_url =
        std::env::var("ORDERBOOK_HTTP_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".to_owned());
    let ws_url = std::env::var("ORDERBOOK_WS_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:3000/events/ws".to_owned());
    let delay = std::env::var("CHAIN_DELAY_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(500));

    loop {
        if let Err(error) = run(&http_url, &ws_url, delay).await {
            kage_orderbook::service_error!("chain", "disconnected error={error}");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn run(http_url: &str, ws_url: &str, delay: Duration) -> Result<(), BoxError> {
    let client = reqwest::Client::new();
    let (mut socket, _) = connect_async(ws_url).await?;
    let jobs = client
        .get(format!("{http_url}/chain/jobs"))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<Order>>()
        .await?;

    for order in jobs {
        if let Some(tx_hash) = order.tx_hash {
            settle(&client, http_url, order.id, tx_hash, delay).await?;
        }
    }

    while let Some(message) = socket.next().await {
        let message = message?;
        if !message.is_text() {
            continue;
        }

        let event: OrderEvent = serde_json::from_str(message.to_text()?)?;
        if let OrderEvent::ExecutionStarted { order_id, tx_hash } = event {
            kage_orderbook::service_log!(
                "chain",
                "execution observed order={} tx_hash={tx_hash}",
                short_id(order_id)
            );
            settle(&client, http_url, order_id, tx_hash, delay).await?;
        }
    }

    Ok(())
}

async fn settle(
    client: &reqwest::Client,
    http_url: &str,
    order_id: OrderId,
    tx_hash: TxHash,
    delay: Duration,
) -> Result<(), BoxError> {
    tokio::time::sleep(delay).await;
    let response = client
        .post(format!("{http_url}/orders/{order_id}/settlement"))
        .json(&SettlementRequest { tx_hash })
        .send()
        .await?;

    if response.status() == StatusCode::CONFLICT {
        return Ok(());
    }
    response.error_for_status()?;

    kage_orderbook::service_log!(
        "chain",
        "settlement confirmed order={} tx_hash={tx_hash}",
        short_id(order_id)
    );
    Ok(())
}
