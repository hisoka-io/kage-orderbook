use std::time::Duration;

use kage_registry::{Config as RegistryConfig, RegistryIndexer};

use kage_orderbook::{
    api,
    chain::SettlementWatcher,
    config::{AppConfig, Network},
    core::engine::start_orderbook_with_repository,
    pricing::{self, PricingConfig, PricingValidator},
    readiness::ServiceReadiness,
    registry::SolverRegistry,
    session::{SolverSessions, domain},
    storage::OrderRepository,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let network = Network::bootstrap(std::env::args().nth(1))?;
    kage_orderbook::logging::init();
    let config = AppConfig::load(network)?;
    let database_url = std::env::var("DATABASE_URL")?;
    let listen_address = std::env::var("KAGE_ORDERBOOK_LISTEN_ADDR")?;
    let rpc_url = std::env::var("KAGE_RPC_URL")?;
    let pricing_feed_url = std::env::var("KAGE_PRICING_FEED_URL")?;
    let pricing_token = std::env::var("KAGE_PRICING_FEED_TOKEN")?;

    let [chain] = config.chains.as_slice() else {
        return Err("exactly one chain must be configured".into());
    };

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
    repository.bind_network(network).await?;
    let orderbook =
        start_orderbook_with_repository(repository, config.runtime.command_capacity).await?;
    let indexer = RegistryIndexer::init(RegistryConfig {
        confirmations: chain.confirmations,
        ..RegistryConfig::new(rpc_url.clone(), chain.registry, chain.registry_deploy_block)
    })
    .await?;
    let registry = SolverRegistry::chain(indexer);
    let readiness = ServiceReadiness::new();
    SettlementWatcher::connect(
        &rpc_url,
        chain.darkpool,
        chain.confirmations,
        orderbook.clone(),
    )
    .await?
    .spawn();
    readiness.set_chain(true);
    readiness.monitor_pricing(pricing, Duration::from_millis(250));
    readiness.monitor_registry(registry.clone(), Duration::from_secs(1));
    readiness.monitor_engine(orderbook.clone(), Duration::from_millis(250));
    let app = api::router_with_readiness_and_settings(
        orderbook,
        registry,
        SolverSessions::new(domain(network, chain.chain_id)),
        config.order_policy(),
        pricing_validator,
        readiness.clone(),
        config.api.clone(),
    );
    let listener = TcpListener::bind(&listen_address).await?;

    kage_orderbook::service_log!(
        "orderbook",
        "listening network={} address={} chain_id={} registry_contract={} max_order_usd=${}.{:02}",
        network,
        listen_address,
        chain.chain_id,
        chain.registry,
        config.order.max_order_usd_cents / 100,
        config.order.max_order_usd_cents % 100
    );
    readiness.report();
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}
