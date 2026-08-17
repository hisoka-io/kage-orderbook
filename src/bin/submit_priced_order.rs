use std::{error::Error, io, time::Duration};

use alloy_primitives::{B256, U256, U512};
use kage_orderbook::{
    config::AppConfig,
    pricing::{self, PricingConfig},
};
use kage_types::api_types::{ApiErrorResponse, CreateOrderRequest, CreateOrderResponse};
use reqwest::StatusCode;
use uuid::Uuid;

type BoxError = Box<dyn Error + Send + Sync>;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("submit-priced-order failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), BoxError> {
    dotenvy::dotenv().ok();
    let wrong_quote = parse_args()?;
    let config = AppConfig::load()?;
    let feed_url = std::env::var("KAGE_PRICING_FEED_URL")?;
    let token = std::env::var("KAGE_PRICING_FEED_TOKEN")?;
    let http_url =
        std::env::var("ORDERBOOK_HTTP_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".to_owned());
    let chain = config
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
        feed_url,
        token,
        assets: config.pricing_assets(),
        max_age: Duration::from_millis(config.pricing.max_age_ms),
        reconnect_delay: Duration::from_millis(config.pricing.reconnect_delay_ms),
        idle_timeout: Duration::from_millis(config.pricing.idle_timeout_ms),
    });

    let (price_in, price_out) = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(pair) = pricing.fresh_pair(&token_in.pricing_asset, &token_out.pricing_asset)
            {
                return Ok::<_, io::Error>(pair);
            }
            pricing.changed().await.map_err(io::Error::other)?;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "pricing feed did not become ready"))??;

    let amount_in = pow10(token_in.decimals)?;
    let fair_amount_out = fair_output(
        amount_in,
        price_in.price_e18,
        price_out.price_e18,
        token_in.decimals,
        token_out.decimals,
    )?;
    let token_limit_bps = token_in
        .max_price_deviation_bps
        .min(token_out.max_price_deviation_bps);
    let max_deviation_bps = market
        .max_price_deviation_bps
        .unwrap_or(token_limit_bps)
        .min(token_limit_bps);
    let wrong_deviation_bps = u32::from(max_deviation_bps) + 10;
    let amount_out = if wrong_quote {
        add_deviation(fair_amount_out, wrong_deviation_bps)?
    } else {
        fair_amount_out
    };
    let request = CreateOrderRequest {
        order_commitment: commitment(),
        chain_id: chain.chain_id,
        token_in: token_in.address,
        token_out: token_out.address,
        amount_in,
        amount_out,
        ttl_seconds: None,
    };

    let response = reqwest::Client::new()
        .post(format!("{http_url}/orders"))
        .json(&request)
        .send()
        .await?;
    let status = response.status();
    if wrong_quote {
        if status != StatusCode::UNPROCESSABLE_ENTITY {
            return Err(response_error(response, "expected quote rejection")
                .await
                .into());
        }
        println!(
            "wrong quote rejected: status={status} market={}→{} amount_in={} fair_amount_out={} submitted_amount_out={} limit_bps={} submitted_deviation_bps={}",
            token_in.symbol,
            token_out.symbol,
            amount_in,
            fair_amount_out,
            amount_out,
            max_deviation_bps,
            wrong_deviation_bps
        );
    } else {
        if status != StatusCode::CREATED && status != StatusCode::OK {
            return Err(response_error(response, "order was not accepted")
                .await
                .into());
        }
        let created = response.json::<CreateOrderResponse>().await?;
        println!(
            "order accepted: status={status} order_id={} market={}→{} amount_in={} amount_out={}",
            created.order_id, token_in.symbol, token_out.symbol, amount_in, amount_out
        );
    }
    Ok(())
}

async fn response_error(response: reqwest::Response, fallback: &str) -> io::Error {
    let status = response.status();
    match response.json::<ApiErrorResponse>().await {
        Ok(error) if error.code == "service_not_ready" && !error.missing.is_empty() => {
            io::Error::other(format!(
                "orderbook is not ready; missing dependencies: {} ({status})",
                error.missing.join(", ")
            ))
        }
        Ok(error) => io::Error::other(format!(
            "{fallback}: {} [{}] ({status})",
            error.message, error.code
        )),
        Err(_) => io::Error::other(format!("{fallback} ({status})")),
    }
}

fn parse_args() -> Result<bool, io::Error> {
    let mut wrong_quote = false;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--wrong-quote" => wrong_quote = true,
            "--help" | "-h" => {
                println!("submit_priced_order [--wrong-quote]");
                std::process::exit(0);
            }
            _ => return Err(invalid(format!("unknown argument: {arg}"))),
        }
    }
    Ok(wrong_quote)
}

fn pow10(decimals: u8) -> Result<U256, io::Error> {
    U256::from(10_u8)
        .checked_pow(U256::from(decimals))
        .ok_or_else(|| invalid("token decimal scale overflow"))
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

fn add_deviation(amount: U256, deviation_bps: u32) -> Result<U256, io::Error> {
    let adjusted = U512::from(amount)
        .checked_mul(U512::from(10_000_u32 + deviation_bps))
        .ok_or_else(|| invalid("wrong quote amount overflow"))?
        / U512::from(10_000_u32);
    let adjusted = adjusted
        .checked_add(U512::ONE)
        .ok_or_else(|| invalid("wrong quote amount overflow"))?;
    U256::checked_from_limbs_slice(adjusted.as_limbs())
        .ok_or_else(|| invalid("wrong quote amount does not fit U256"))
}

fn commitment() -> B256 {
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    bytes[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    B256::from(bytes)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
