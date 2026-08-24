use std::{
    collections::{HashMap, HashSet},
    error::Error,
    io,
    time::Duration,
};

use alloy_primitives::{B256, U256, U512};
use futures_util::{SinkExt, StreamExt};
use kage_orderbook::{
    config::{AppConfig, Network},
    logging::short_id,
    pricing::{self, PricePoint, PricingConfig, PricingHandle},
    proof::transport as proof_transport,
};
use kage_types::{
    api_types::{
        CreateOrderRequest, CreateOrderResponse, EncryptedProofRequest, ORDER_COMMITMENT_HEADER,
        UserEventClientMessage, UserEventServerMessage,
    },
    events::OrderEvent,
    identifiers::OrderId,
    orders::{OrderState, OrderV1},
};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use uuid::Uuid;

mod proof_validation;
mod prover_worker;

use prover_worker::{ProofOrderV1, ProverWorker};

type BoxError = Box<dyn Error + Send + Sync>;

struct Config {
    orders: usize,
    interval: Duration,
    timeout: Duration,
    prover_timeout: Duration,
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
            prover_timeout: Duration::from_secs(60),
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
                "--prover-timeout-secs" => {
                    config.prover_timeout = Duration::from_secs(value(&mut args, &arg)?.parse()?)
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
         [--prover-timeout-secs N] [--http-url URL] [--ws-url URL]"
    );
}

struct SubmittedOrder {
    commitment: B256,
    proof_order: ProofOrderV1,
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

    fn amount_in(
        &mut self,
        decimals: u8,
        price_e18: U256,
        max_order_usd_cents: u64,
    ) -> Result<(U256, u64), io::Error> {
        // Exercise a useful range while staying comfortably below the configured limit.
        let usage_bps = 1_000 + self.next() % 8_001;
        let target_usd_cents =
            u64::try_from(u128::from(max_order_usd_cents) * u128::from(usage_bps) / 10_000)
                .map_err(|_| invalid("mock USD target overflow"))?
                .max(1);
        let amount = amount_for_usd_cents(target_usd_cents, price_e18, decimals)?;
        if amount == U256::ZERO {
            return Err(invalid(
                "configured order limit produces a zero token amount",
            ));
        }
        Ok((amount, target_usd_cents))
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
    let network = Network::bootstrap(None)?;
    kage_orderbook::logging::init();
    let options = Config::from_args()?;
    let app_config = AppConfig::load(network)?;
    let chain = app_config
        .chains
        .first()
        .ok_or_else(|| invalid("no configured chain"))?;
    let market = chain
        .markets
        .first()
        .ok_or_else(|| invalid("no configured market"))?;
    let token_in = chain
        .tokens
        .iter()
        .find(|token| token.symbol == market.token_in)
        .ok_or_else(|| invalid("market input token is missing"))?;
    let token_out = chain
        .tokens
        .iter()
        .find(|token| token.symbol == market.token_out)
        .ok_or_else(|| invalid("market output token is missing"))?;
    let mut pricing = pricing::spawn(PricingConfig {
        feed_url: std::env::var("KAGE_PRICING_FEED_URL")?,
        token: std::env::var("KAGE_PRICING_FEED_TOKEN")?,
        assets: app_config.pricing_assets(),
        max_age: Duration::from_millis(app_config.pricing.max_age_ms),
        reconnect_delay: Duration::from_millis(app_config.pricing.reconnect_delay_ms),
        idle_timeout: Duration::from_millis(app_config.pricing.idle_timeout_ms),
    });
    let client = reqwest::Client::new();
    let (socket, _) = connect_async(&options.ws_url).await?;
    let (mut socket_tx, mut socket_rx) = socket.split();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (subscription_tx, mut subscription_rx) = mpsc::unbounded_channel();
    let mut prover = ProverWorker::spawn(options.prover_timeout)?;
    kage_orderbook::service_log!("user", "prover worker started protocol=v1");

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

    let mut generator = Generator(options.seed);
    let mut orders = HashMap::new();

    for index in 0..options.orders {
        let (price_in, price_out) = wait_for_prices(
            &mut pricing,
            &token_in.pricing_asset,
            &token_out.pricing_asset,
        )
        .await?;
        let (amount_in, target_usd_cents) = generator.amount_in(
            token_in.decimals,
            price_in.price_e18,
            app_config.order.max_order_usd_cents,
        )?;
        let amount_out = fair_output(
            amount_in,
            price_in.price_e18,
            price_out.price_e18,
            token_in.decimals,
            token_out.decimals,
        )?;
        let request = CreateOrderRequest {
            order_commitment: new_order_commitment(),
            chain_id: chain.chain_id,
            token_in: token_in.address,
            token_out: token_out.address,
            amount_in,
            amount_out,
            ttl_seconds: None,
        };
        let response = client
            .post(format!("{}/orders", options.http_url))
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
            "created order={} token_in={} token_out={} amount_in={} amount_out={} target_usd_cents={} expires_at_ms={}",
            short_id(response.order_id),
            request.token_in,
            request.token_out,
            request.amount_in,
            request.amount_out,
            target_usd_cents,
            response.expires_at_ms
        );
        orders.insert(
            response.order_id,
            SubmittedOrder {
                commitment: request.order_commitment,
                proof_order: ProofOrderV1 {
                    order_id: response.order_id,
                    chain_id: request.chain_id,
                    token_in: request.token_in.to_string().to_lowercase(),
                    token_out: request.token_out.to_string().to_lowercase(),
                    amount_in: request.amount_in.to_string(),
                    amount_out: request.amount_out.to_string(),
                    expires_at_ms: response.expires_at_ms,
                },
            },
        );

        if index + 1 < options.orders {
            tokio::time::sleep(options.interval).await;
        }
    }

    let trails = wait_for_filled(
        &client,
        &options.http_url,
        &mut event_rx,
        &orders,
        &mut prover,
        options.timeout,
    )
    .await?;

    for (order_id, submitted) in &orders {
        let order = client
            .get(format!("{}/orders/{order_id}", options.http_url))
            .header(ORDER_COMMITMENT_HEADER, submitted.commitment.to_string())
            .send()
            .await?
            .error_for_status()?
            .json::<OrderV1>()
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
        options.orders
    );

    prover.shutdown().await?;
    reader.abort();
    Ok(())
}

async fn wait_for_filled(
    client: &reqwest::Client,
    http_url: &str,
    events: &mut mpsc::UnboundedReceiver<OrderEvent>,
    orders: &HashMap<OrderId, SubmittedOrder>,
    prover: &mut ProverWorker,
    timeout: Duration,
) -> Result<HashMap<OrderId, Vec<String>>, BoxError> {
    tokio::time::timeout(timeout, async {
        let mut filled = HashSet::new();
        let mut proofs_sent = HashSet::new();
        let mut trails: HashMap<OrderId, Vec<String>> = HashMap::new();

        for (order_id, submitted) in orders {
            let order = client
                .get(format!("{http_url}/orders/{order_id}"))
                .header(ORDER_COMMITMENT_HEADER, submitted.commitment.to_string())
                .send()
                .await
                .map_err(|error| invalid(error.to_string()))?
                .error_for_status()
                .map_err(|error| invalid(error.to_string()))?
                .json::<OrderV1>()
                .await
                .map_err(|error| invalid(error.to_string()))?;
            if order.state == OrderState::AwaitingUserProof
                && let Some(noise_public_key) = order.solver_noise_public_key
            {
                send_proof(
                    client,
                    http_url,
                    *order_id,
                    submitted.commitment,
                    submitted.proof_order.clone(),
                    &noise_public_key,
                    prover,
                )
                .await?;
                proofs_sent.insert(*order_id);
            }
        }

        while filled.len() < orders.len() {
            let event = events
                .recv()
                .await
                .ok_or_else(|| invalid("event stream closed"))?;
            let order_id = event.order_id();
            let Some(submitted) = orders.get(&order_id) else {
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
                        send_proof(
                            client,
                            http_url,
                            order_id,
                            submitted.commitment,
                            submitted.proof_order.clone(),
                            &noise_public_key,
                            prover,
                        )
                        .await?;
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

async fn wait_for_prices(
    pricing: &mut PricingHandle,
    asset_in: &str,
    asset_out: &str,
) -> Result<(PricePoint, PricePoint), BoxError> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(pair) = pricing.fresh_pair(asset_in, asset_out) {
                return Ok::<_, io::Error>(pair);
            }
            pricing.changed().await.map_err(io::Error::other)?;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "pricing feed did not become ready"))?
    .map_err(Into::into)
}

fn pow10(decimals: u8) -> Result<U256, io::Error> {
    U256::from(10_u8)
        .checked_pow(U256::from(decimals))
        .ok_or_else(|| invalid("token decimal scale overflow"))
}

fn amount_for_usd_cents(usd_cents: u64, price_e18: U256, decimals: u8) -> Result<U256, io::Error> {
    if usd_cents == 0 || price_e18 == U256::ZERO {
        return Err(invalid(
            "USD value and token price must be greater than zero",
        ));
    }
    let price_scale = U512::from(10_u8)
        .checked_pow(U512::from(18_u8))
        .ok_or_else(|| invalid("price scale overflow"))?;
    let numerator = U512::from(usd_cents)
        .checked_mul(U512::from(pow10(decimals)?))
        .and_then(|value| value.checked_mul(price_scale))
        .ok_or_else(|| invalid("mock amount numerator overflow"))?;
    let denominator = U512::from(price_e18)
        .checked_mul(U512::from(100_u8))
        .ok_or_else(|| invalid("mock amount denominator overflow"))?;
    let amount = numerator / denominator;
    U256::checked_from_limbs_slice(amount.as_limbs())
        .ok_or_else(|| invalid("mock amount does not fit U256"))
}

fn fair_output(
    amount_in: U256,
    price_in: U256,
    price_out: U256,
    decimals_in: u8,
    decimals_out: u8,
) -> Result<U256, io::Error> {
    let numerator = U512::from(amount_in)
        .checked_mul(U512::from(price_in))
        .and_then(|value| value.checked_mul(U512::from(pow10(decimals_out).ok()?)))
        .ok_or_else(|| invalid("quote numerator overflow"))?;
    let denominator = U512::from(price_out)
        .checked_mul(U512::from(pow10(decimals_in)?))
        .ok_or_else(|| invalid("quote denominator overflow"))?;
    let output = numerator / denominator;
    U256::checked_from_limbs_slice(output.as_limbs())
        .ok_or_else(|| invalid("quote does not fit U256"))
}

async fn send_proof(
    client: &reqwest::Client,
    http_url: &str,
    order_id: OrderId,
    order_commitment: B256,
    proof_order: ProofOrderV1,
    noise_public_key: &[u8],
    prover: &mut ProverWorker,
) -> Result<(), io::Error> {
    let proof = prover.prove(proof_order).await.map_err(io::Error::other)?;
    let proof = serde_json::to_vec(&proof).map_err(io::Error::other)?;
    let ciphertext = proof_transport::encrypt_for_solver(order_id, noise_public_key, &proof)
        .map_err(io::Error::other)?;
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
        "real encrypted proof sent order={} proof_bytes={} ciphertext_bytes={ciphertext_bytes}",
        short_id(order_id),
        proof.len()
    );
    Ok(())
}

#[cfg(test)]
mod amount_tests {
    use super::*;

    #[test]
    fn generated_eth_orders_stay_below_the_configured_usd_limit() {
        let price_e18 = U256::from(2_000_u64) * U256::from(10_u64).pow(U256::from(18_u8));
        let mut generator = Generator(42);

        for _ in 0..100 {
            let (amount, target_usd_cents) = generator.amount_in(18, price_e18, 25_000).unwrap();
            assert!((2_500..=22_500).contains(&target_usd_cents));

            let value_cents_scaled =
                U512::from(amount) * U512::from(price_e18) * U512::from(100_u8);
            let limit_cents_scaled = U512::from(25_000_u64)
                * U512::from(10_u64).pow(U512::from(18_u8))
                * U512::from(10_u64).pow(U512::from(18_u8));
            assert!(value_cents_scaled < limit_cents_scaled);
        }
    }

    #[test]
    fn converts_target_usd_value_and_keeps_the_output_quote_fair() {
        let price_scale = U256::from(10_u64).pow(U256::from(18_u8));
        let eth_price = U256::from(2_000_u64) * price_scale;
        let amount_in = amount_for_usd_cents(22_500, eth_price, 18).unwrap();
        let amount_out = fair_output(amount_in, eth_price, price_scale, 18, 6).unwrap();

        assert_eq!(amount_in, U256::from(112_500_000_000_000_000_u64));
        assert_eq!(amount_out, U256::from(225_000_000_u64));
    }
}
