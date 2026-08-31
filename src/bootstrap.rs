use std::{sync::Arc, time::Duration};

use kage_price_estimate::oracle::PricingOracle;
use kage_registry::{Config as RegistryConfig, RegistryIndexer};
use tokio::net::TcpListener;

use kage_orderbook::{
    api,
    assignment::AssignmentIssuer,
    complaint::{ComplaintEvidenceCipher, ComplaintVerifier},
    config::{AppConfig, Network},
    core::engine::start_orderbook_with_repository_and_policy,
    preview::PreviewService,
    pricing::EmbeddedPricing,
    readiness::ServiceReadiness,
    registry::SolverRegistry,
    session::{SolverSessions, domain},
    storage::OrderRepository,
};

pub async fn run(network_argument: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let network = Network::bootstrap(network_argument)?;
    kage_orderbook::logging::init();
    let config = AppConfig::load(network)?;
    let database_url = std::env::var("DATABASE_URL")?;
    let listen_address = std::env::var("KAGE_ORDERBOOK_LISTEN_ADDR")?;
    let rpc_url = std::env::var("KAGE_RPC_URL")?;
    let assignment_issuer = AssignmentIssuer::from_env()?;

    let [chain] = config.chains.as_slice() else {
        return Err("exactly one chain must be configured".into());
    };

    let oracle_config = config
        .pricing_oracle
        .clone()
        .ok_or("pricing_oracle must be configured")?;
    let (pricing_handle, pricing_runtime) = PricingOracle::start(Arc::new(oracle_config))?;
    tokio::spawn(async move {
        let error = pricing_runtime.wait_for_failure().await;
        tracing::error!(target: "pricing", %error, "embedded pricing runtime failed");
    });
    let pricing = EmbeddedPricing::new(pricing_handle);

    let repository = OrderRepository::connect_with_options(
        &database_url,
        Duration::from_millis(config.database.busy_timeout_ms),
        config.database.max_connections,
    )
    .await?;
    repository.bind_network(network).await?;
    let proof_orders = repository.proof_orders();
    let previews = repository.previews();
    let orderbook = start_orderbook_with_repository_and_policy(
        repository,
        config.runtime.command_capacity,
        config.proof_orders.clone(),
    )
    .await?;

    let indexer = RegistryIndexer::init(RegistryConfig {
        confirmations: chain.confirmations,
        ..RegistryConfig::new(rpc_url.clone(), chain.registry, chain.registry_deploy_block)
    })
    .await?;
    let registry = SolverRegistry::chain(indexer);
    let readiness = ServiceReadiness::new();
    readiness.set_chain(true);
    readiness.monitor_embedded_pricing(pricing.clone(), Duration::from_millis(250));
    readiness.monitor_registry(registry.clone(), Duration::from_secs(1));
    readiness.monitor_engine(orderbook.clone(), Duration::from_millis(250));

    kage_orderbook::service_log!(
        "orderbook",
        "proof assignment signing enabled signer={}",
        assignment_issuer.signer_address()
    );
    let sessions = SolverSessions::new(domain(network, chain.chain_id));
    let preview = PreviewService::new(
        pricing,
        sessions.clone(),
        registry.clone(),
        previews,
        proof_orders.clone(),
        &config,
    );
    let complaint_verifier = ComplaintVerifier::new(
        rpc_url,
        chain.darkpool,
        config.proof_orders.complaint_finality,
    );
    let complaint_evidence_cipher = ComplaintEvidenceCipher::from_env()?;
    let app = api::router(
        orderbook,
        registry,
        sessions,
        preview,
        proof_orders,
        complaint_verifier,
        complaint_evidence_cipher,
        readiness.clone(),
        config.api.clone(),
        assignment_issuer,
        config.allowed_solvers.iter().copied().collect(),
        config.proof_orders.clone(),
    );
    let listener = TcpListener::bind(&listen_address).await?;

    kage_orderbook::service_log!(
        "orderbook",
        "listening network={} address={} chain_id={} registry_contract={}",
        network,
        listen_address,
        chain.chain_id,
        chain.registry
    );
    readiness.report();
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}
