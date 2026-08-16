use std::{collections::HashMap, str::FromStr, sync::Arc};

use alloy_primitives::U256;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;

use super::{
    PricingConfig,
    cache::{PricePoint, PricingSnapshot, PricingStatus},
    now_ms,
};

const MAX_EVENT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
struct FeedRequest {
    assets: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct WirePoint {
    price_e18: String,
    observed_at_ms: u64,
    sequence: u64,
}

#[derive(Debug, Deserialize)]
struct WireTick {
    asset: String,
    price_e18: String,
    observed_at_ms: u64,
    sequence: u64,
}

#[derive(Debug)]
struct SseEvent {
    name: String,
    data: String,
}

#[derive(Default)]
struct SseDecoder {
    buffer: Vec<u8>,
}

#[derive(Debug, Error)]
enum ClientError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("pricing feed closed")]
    Closed,
    #[error("pricing feed connection timeout")]
    ConnectionTimeout,
    #[error("pricing feed idle timeout")]
    IdleTimeout,
    #[error("invalid pricing event: {0}")]
    Invalid(String),
}

pub(super) async fn run(
    config: PricingConfig,
    assets: Vec<String>,
    sender: watch::Sender<Arc<PricingSnapshot>>,
) {
    let client = reqwest::Client::new();
    let mut last_error = None;
    loop {
        update(&sender, PricingSnapshot::set_connecting);

        let error = consume(&client, &config, &assets, &sender)
            .await
            .expect_err("pricing feed consumer does not complete successfully");
        let was_active = matches!(
            sender
                .borrow()
                .status(now_ms(), config.max_age.as_millis() as u64),
            PricingStatus::Ready | PricingStatus::Stale
        );
        update(&sender, PricingSnapshot::set_disconnected);
        let message = error.to_string();
        if was_active || last_error.as_deref() != Some(message.as_str()) {
            crate::service_error!("pricing", "disconnected error={message}");
        }
        last_error = Some(message);
        tokio::time::sleep(config.reconnect_delay).await;
    }
}

async fn consume(
    client: &reqwest::Client,
    config: &PricingConfig,
    assets: &[String],
    sender: &watch::Sender<Arc<PricingSnapshot>>,
) -> Result<(), ClientError> {
    let response = tokio::time::timeout(
        config.idle_timeout,
        client
            .post(&config.feed_url)
            .bearer_auth(&config.token)
            .json(&FeedRequest {
                assets: assets.to_vec(),
            })
            .send(),
    )
    .await
    .map_err(|_| ClientError::ConnectionTimeout)??
    .error_for_status()?;
    crate::service_log!("pricing", "active assets={}", assets.join(","));

    let mut stream = response.bytes_stream();
    let mut decoder = SseDecoder::default();
    loop {
        let next = tokio::time::timeout(config.idle_timeout, stream.next())
            .await
            .map_err(|_| ClientError::IdleTimeout)?;
        let bytes = next.ok_or(ClientError::Closed)??;
        for event in decoder.push(&bytes)? {
            apply_event(event, sender)?;
        }
    }
}

fn apply_event(
    event: SseEvent,
    sender: &watch::Sender<Arc<PricingSnapshot>>,
) -> Result<(), ClientError> {
    match event.name.as_str() {
        "snapshot" => {
            let wire: HashMap<String, Option<WirePoint>> = serde_json::from_str(&event.data)
                .map_err(|error| ClientError::Invalid(error.to_string()))?;
            let prices = wire
                .into_iter()
                .filter_map(|(asset, point)| point.map(|point| (asset, point)))
                .map(|(asset, point)| Ok((asset.to_uppercase(), parse_point(point)?)))
                .collect::<Result<HashMap<_, _>, ClientError>>()?;
            update(sender, |snapshot| snapshot.replace_prices(prices));
        }
        "tick" => {
            let tick: WireTick = serde_json::from_str(&event.data)
                .map_err(|error| ClientError::Invalid(error.to_string()))?;
            let asset = tick.asset.to_uppercase();
            let point = parse_point(WirePoint {
                price_e18: tick.price_e18,
                observed_at_ms: tick.observed_at_ms,
                sequence: tick.sequence,
            })?;
            update(sender, |snapshot| {
                snapshot.apply_tick(asset, point);
            });
        }
        _ => {}
    }
    Ok(())
}

fn parse_point(wire: WirePoint) -> Result<PricePoint, ClientError> {
    let price_e18 = U256::from_str(&wire.price_e18)
        .map_err(|_| ClientError::Invalid("price_e18 is not a U256".into()))?;
    if price_e18 == U256::ZERO {
        return Err(ClientError::Invalid("price_e18 must be positive".into()));
    }
    Ok(PricePoint {
        price_e18,
        observed_at_ms: wire.observed_at_ms,
        sequence: wire.sequence,
    })
}

fn update(sender: &watch::Sender<Arc<PricingSnapshot>>, mutate: impl FnOnce(&mut PricingSnapshot)) {
    let current = sender.borrow().clone();
    let mut next = (*current).clone();
    mutate(&mut next);
    sender.send_replace(Arc::new(next));
}

impl SseDecoder {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, ClientError> {
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        while let Some((index, delimiter_len)) = find_boundary(&self.buffer) {
            let block = self.buffer[..index].to_vec();
            self.buffer.drain(..index + delimiter_len);
            if let Some(event) = parse_event(&block)? {
                events.push(event);
            }
        }
        if self.buffer.len() > MAX_EVENT_BYTES {
            return Err(ClientError::Invalid("SSE event exceeds size limit".into()));
        }
        Ok(events)
    }
}

fn find_boundary(bytes: &[u8]) -> Option<(usize, usize)> {
    for index in 0..bytes.len() {
        if bytes.get(index..index + 2) == Some(&[10, 10]) {
            return Some((index, 2));
        }
        if bytes.get(index..index + 4) == Some(&[13, 10, 13, 10]) {
            return Some((index, 4));
        }
    }
    None
}

fn parse_event(block: &[u8]) -> Result<Option<SseEvent>, ClientError> {
    let block =
        std::str::from_utf8(block).map_err(|error| ClientError::Invalid(error.to_string()))?;
    let mut name = "message".to_owned();
    let mut data = Vec::new();
    for line in block.lines() {
        let line = line.trim_end_matches("\r");
        if let Some(value) = line.strip_prefix("event:") {
            name = value.trim_start().to_owned();
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.trim_start());
        }
    }
    if data.is_empty() {
        return Ok(None);
    }
    Ok(Some(SseEvent {
        name,
        data: data.join("\n"),
    }))
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use axum::{
        Json, Router,
        body::{Body, Bytes},
        http::{HeaderMap, StatusCode, header::AUTHORIZATION},
        response::{IntoResponse, Response},
        routing::post,
    };
    use futures_util::stream;

    use super::*;
    use crate::pricing::{PricingStatus, spawn};

    #[test]
    fn decoder_handles_split_crlf_events() {
        let mut decoder = SseDecoder::default();
        assert!(decoder.push(b"event: tick\r\nda").unwrap().is_empty());
        let events = decoder.push(b"ta: {\"asset\":\"ETH\"}\r\n\r\n").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "tick");
        assert_eq!(events[0].data, "{\"asset\":\"ETH\"}");
    }

    async fn feed(headers: HeaderMap, Json(request): Json<FeedRequest>) -> Response {
        if headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            != Some("Bearer secret")
        {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        assert_eq!(request.assets, vec!["ETH"]);
        let observed_at_ms = now_ms();
        let body = concat!(
            "event: snapshot\n",
            "data: {\"ETH\":{\"price_e18\":\"3200000000000000000000\",",
            "\"observed_at_ms\":$TIME,\"sequence\":1}}\n\n",
            "event: tick\n",
            "data: {\"asset\":\"ETH\",\"price_e18\":\"3201000000000000000000\",",
            "\"observed_at_ms\":$TIME,\"sequence\":2}\n\n"
        )
        .replace("$TIME", &observed_at_ms.to_string());
        let stream = stream::once(async move { Ok::<Bytes, Infallible>(Bytes::from(body)) })
            .chain(stream::pending());
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(stream))
            .unwrap()
    }

    #[tokio::test]
    async fn subscribes_with_bearer_token_and_updates_cache() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/feed", post(feed)))
                .await
                .unwrap();
        });
        let mut pricing = spawn(PricingConfig {
            feed_url: format!("http://{address}/feed"),
            token: "secret".into(),
            assets: vec!["ETH".into()],
            max_age: Duration::from_secs(5),
            reconnect_delay: Duration::from_millis(10),
            idle_timeout: Duration::from_secs(5),
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if pricing.status() == PricingStatus::Ready
                    && pricing
                        .price("ETH")
                        .is_some_and(|point| point.sequence == 2)
                {
                    break;
                }
                pricing.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        server.abort();
    }

    #[tokio::test]
    async fn reconnects_when_the_server_never_sends_headers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let server_accepted = Arc::clone(&accepted);
        let server = tokio::spawn(async move {
            let mut connections = Vec::new();
            loop {
                let (connection, _) = listener.accept().await.unwrap();
                server_accepted.fetch_add(1, Ordering::Relaxed);
                connections.push(connection);
            }
        });
        let _pricing = spawn(PricingConfig {
            feed_url: format!("http://{address}/feed"),
            token: "secret".into(),
            assets: vec!["ETH".into()],
            max_age: Duration::from_secs(5),
            reconnect_delay: Duration::from_millis(10),
            idle_timeout: Duration::from_millis(20),
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while accepted.load(Ordering::Relaxed) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        server.abort();
    }
}
