use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::io;
use std::time::Duration;

use alloy_primitives::{Address, B256, U256};
use futures_util::{SinkExt, StreamExt};
use kage_orderbook::api::{
    CreateOrderRequest, CreateOrderResponse, EncryptedProofRequest, ORDER_COMMITMENT_HEADER,
    UserEventClientMessage, UserEventServerMessage,
};
use kage_orderbook::core::events::OrderEvent;
use kage_orderbook::core::guards::MOCK_CHAIN_ID;
use kage_orderbook::logging::short_id;
use kage_orderbook::order::{Order, OrderId, OrderState, TradeTerms};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use uuid::Uuid;

type BoxError = Box<dyn Error + Send + Sync>;

struct Config {
    orders: usize,
    interval: Duration,
    timeout: Duration,
    seed: u64,
    http_url: String,
    ws_url: String,
}

impl Config {
    fn from_args() -> Result<Self, BoxError> {
        let mut config = Self {
            orders: 5,
            interval: Duration::from_millis(250),
            timeout: Duration::from_secs(30),
            seed: 42,
            http_url: std::env::var("ORDERBOOK_HTTP_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:3000".to_owned()),
            ws_url: std::env::var("ORDERBOOK_WS_URL")
                .unwrap_or_else(|_| "ws://127.0.0.1:3000/events/user/ws".to_owned()),
        };

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--orders" => config.orders = value(&mut args, &arg)?.parse()?,
                "--interval-ms" => {
                    config.interval = Duration::from_millis(value(&mut args, &arg)?.parse()?)
                }
                "--timeout-secs" => {
                    config.timeout = Duration::from_secs(value(&mut args, &arg)?.parse()?)
                }
                "--seed" => config.seed = value(&mut args, &arg)?.parse()?,
                "--http-url" => config.http_url = value(&mut args, &arg)?,
                "--ws-url" => config.ws_url = value(&mut args, &arg)?,
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                _ => return Err(invalid(format!("unknown argument: {arg}")).into()),
            }
        }

        if config.orders == 0 {
            return Err(invalid("--orders must be greater than zero").into());
        }

        Ok(config)
    }
}

fn value(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, io::Error> {
    args.next()
        .ok_or_else(|| invalid(format!("missing value for {name}")))
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn print_help() {
    println!(
        "mock_user [--orders N] [--interval-ms N] [--timeout-secs N] [--seed N] \
         [--http-url URL] [--ws-url URL]"
    );
}

struct Generator(u64);

impl Generator {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.0
    }

    fn terms(&mut self) -> TradeTerms {
        let amount_in = self.next() % 1_000 + 1;
        TradeTerms {
            chain_id: MOCK_CHAIN_ID,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
            amount_in: U256::from(amount_in),
            amount_out: U256::from(amount_in * 2),
            expires_at_ms: chrono::Utc::now().timestamp_millis() + 60_000,
        }
    }
}

fn new_order_commitment() -> B256 {
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    bytes[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    B256::from(bytes)
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let config = Config::from_args()?;
    let client = reqwest::Client::new();
    let (socket, _) = connect_async(&config.ws_url).await?;
    let (mut socket_tx, mut socket_rx) = socket.split();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (subscription_tx, mut subscription_rx) = mpsc::unbounded_channel();

    let reader = tokio::spawn(async move {
        while let Some(message) = socket_rx.next().await {
            let message = match message {
                Ok(message) => message,
                Err(_) => return,
            };
            if !message.is_text() {
                continue;
            }

            let Ok(message) =
                serde_json::from_str::<UserEventServerMessage>(message.to_text().unwrap_or(""))
            else {
                continue;
            };
            match message {
                UserEventServerMessage::Subscribed { order_id } => {
                    if subscription_tx.send((order_id, true)).is_err() {
                        return;
                    }
                }
                UserEventServerMessage::Rejected { order_id } => {
                    if subscription_tx.send((order_id, false)).is_err() {
                        return;
                    }
                }
                UserEventServerMessage::Event { event } => {
                    if event_tx.send(event).is_err() {
                        return;
                    }
                }
            }
        }
    });

    let mut generator = Generator(config.seed);
    let mut orders = HashMap::new();

    for index in 0..config.orders {
        let terms = generator.terms();
        let request = CreateOrderRequest {
            order_commitment: new_order_commitment(),
            chain_id: terms.chain_id,
            token_in: terms.token_in,
            token_out: terms.token_out,
            amount_in: terms.amount_in,
            amount_out: terms.amount_out,
            ttl_seconds: None,
        };
        let response = client
            .post(format!("{}/orders", config.http_url))
            .json(&request)
            .send()
            .await?
            .error_for_status()?
            .json::<CreateOrderResponse>()
            .await?;

        let subscription = UserEventClientMessage::Subscribe {
            order_id: response.order_id,
            order_commitment: request.order_commitment,
        };
        socket_tx
            .send(tokio_tungstenite::tungstenite::Message::Text(
                serde_json::to_string(&subscription)?.into(),
            ))
            .await?;
        let (subscribed_order, accepted) = subscription_rx
            .recv()
            .await
            .ok_or_else(|| invalid("user event stream closed"))?;
        if subscribed_order != response.order_id || !accepted {
            return Err(invalid("order event subscription rejected").into());
        }

        kage_orderbook::service_log!(
            "user",
            "created order={} token_in={} token_out={} amount_in={} amount_out={} expires_at_ms={}",
            short_id(response.order_id),
            terms.token_in,
            terms.token_out,
            terms.amount_in,
            terms.amount_out,
            response.expires_at_ms
        );
        orders.insert(response.order_id, request.order_commitment);

        if index + 1 < config.orders {
            tokio::time::sleep(config.interval).await;
        }
    }

    let trails = wait_for_filled(
        &client,
        &config.http_url,
        &mut event_rx,
        &orders,
        config.timeout,
    )
    .await?;

    for (order_id, order_commitment) in &orders {
        let order = client
            .get(format!("{}/orders/{order_id}", config.http_url))
            .header(ORDER_COMMITMENT_HEADER, order_commitment.to_string())
            .send()
            .await?
            .error_for_status()?
            .json::<Order>()
            .await?;

        if order.state != OrderState::Filled {
            return Err(invalid(format!(
                "order {} ended in {:?}",
                short_id(*order_id),
                order.state
            ))
            .into());
        }
    }

    for (order_id, trail) in trails {
        kage_orderbook::service_log!(
            "user",
            "order={} trail={}",
            short_id(order_id),
            trail.join(" -> ")
        );
    }
    kage_orderbook::service_log!(
        "user",
        "{}/{} orders reached Filled",
        orders.len(),
        config.orders
    );

    reader.abort();
    Ok(())
}

async fn wait_for_filled(
    client: &reqwest::Client,
    http_url: &str,
    events: &mut mpsc::UnboundedReceiver<OrderEvent>,
    orders: &HashMap<OrderId, B256>,
    timeout: Duration,
) -> Result<HashMap<OrderId, Vec<String>>, BoxError> {
    tokio::time::timeout(timeout, async {
        let mut filled = HashSet::new();
        let mut proofs_sent = HashSet::new();
        let mut trails: HashMap<OrderId, Vec<String>> = HashMap::new();

        while filled.len() < orders.len() {
            let event = events
                .recv()
                .await
                .ok_or_else(|| invalid("event stream closed"))?;
            let order_id = event.order_id();
            let Some(order_commitment) = orders.get(&order_id) else {
                continue;
            };

            let label = match event {
                OrderEvent::OrderCreated { .. } => "Created",
                OrderEvent::OrderValidated { .. } => "Validated",
                OrderEvent::SolverReservationRequested { .. } => "Reserving",
                OrderEvent::SolverAssigned { .. } => "Assigned",
                OrderEvent::SolverSessionReady {
                    solver_id,
                    noise_public_key,
                    ..
                } => {
                    kage_orderbook::service_log!(
                        "user",
                        "solver session ready order={} solver={solver_id} key_bytes={}",
                        short_id(order_id),
                        noise_public_key.len()
                    );
                    if proofs_sent.insert(order_id) {
                        let proof = format!("proof:{order_id}").into_bytes();
                        let ciphertext = xor(&proof, &noise_public_key);
                        let ciphertext_bytes = ciphertext.len();
                        client
                            .post(format!("{http_url}/orders/{order_id}/encrypted-proof"))
                            .header(ORDER_COMMITMENT_HEADER, order_commitment.to_string())
                            .json(&EncryptedProofRequest { ciphertext })
                            .send()
                            .await
                            .map_err(|error| invalid(error.to_string()))?
                            .error_for_status()
                            .map_err(|error| invalid(error.to_string()))?;
                        kage_orderbook::service_log!(
                            "user",
                            "encrypted proof sent order={} bytes={ciphertext_bytes}",
                            short_id(order_id)
                        );
                    }
                    "AwaitingUserProof"
                }
                OrderEvent::ProofRelayed { .. } => "ProofRelayed",
                OrderEvent::ExecutionStarted { .. } => "Executing",
                OrderEvent::OrderFilled { .. } => {
                    filled.insert(order_id);
                    "Filled"
                }
                OrderEvent::OrderExpired { .. } => "Expired",
            };
            trails.entry(order_id).or_default().push(label.to_owned());
        }

        Ok::<_, io::Error>(trails)
    })
    .await
    .map_err(|_| invalid("timed out waiting for orders to reach Filled"))?
    .map_err(Into::into)
}

fn xor(bytes: &[u8], key: &[u8]) -> Vec<u8> {
    bytes
        .iter()
        .zip(key.iter().cycle())
        .map(|(byte, key)| byte ^ key)
        .collect()
}
