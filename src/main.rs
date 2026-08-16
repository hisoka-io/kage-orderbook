use std::time::Duration;

use kage_orderbook::api;
use kage_orderbook::config::AppConfig;
use kage_orderbook::core::engine::start_orderbook_with_repository;
use kage_orderbook::pricing::{self, PricingConfig, PricingValidator};
use kage_orderbook::readiness::ServiceReadiness;
use kage_orderbook::registry::SolverRegistry;
use kage_orderbook::storage::OrderRepository;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let config = AppConfig::load()?;
    let database_url = std::env::var("DATABASE_URL")?;
    let listen_address = std::env::var("KAGE_ORDERBOOK_LISTEN_ADDR")?;
    let registry_url = std::env::var("KAGE_REGISTRY_URL")?;
    let pricing_feed_url = std::env::var("KAGE_PRICING_FEED_URL")?;
    let pricing_token = std::env::var("KAGE_PRICING_FEED_TOKEN")?;

    let pricing = pricing::spawn(PricingConfig {
        feed_url: pricing_feed_url,
        token: pricing_token,
        assets: config.pricing_assets(),
        max_age: Duration::from_millis(config.pricing.max_age_ms),
        reconnect_delay: Duration::from_millis(config.pricing.reconnect_delay_ms),
        idle_timeout: Duration::from_millis(config.pricing.idle_timeout_ms),
    });
    let pricing_validator = PricingValidator::new(pricing.clone(), &config);
    let repository = OrderRepository::connect_with_options(
        &database_url,
        Duration::from_millis(config.database.busy_timeout_ms),
        config.database.max_connections,
    )
    .await?;
    let orderbook =
        start_orderbook_with_repository(repository, config.runtime.command_capacity).await?;
    let registry = SolverRegistry::http(registry_url);
    let readiness = ServiceReadiness::new();
    readiness.monitor_pricing(pricing, Duration::from_millis(250));
    readiness.monitor_registry(registry.clone(), Duration::from_secs(1));
    readiness.monitor_actor(orderbook.clone(), Duration::from_millis(250));
    let app = api::router_with_readiness(
        orderbook,
        registry,
        config.order_policy(),
        pricing_validator,
        readiness,
    );
    let listener = TcpListener::bind(&listen_address).await?;

    kage_orderbook::service_log!("orderbook", "started address={listen_address}");
    axum::serve(listener, app).await?;
    Ok(())
}
