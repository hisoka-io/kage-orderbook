use std::{sync::Arc, time::Duration};

use kage_price_estimate::oracle::PricingOracle;
use kage_registry::{Config as RegistryConfig, RegistryIndexer};
use tokio::net::TcpListener;

use kage_orderbook::{
    NamedTask, Shutdown, TaskFailure, TaskSupervisor, api,
    assignment::AssignmentIssuer,
    complaint::{ComplaintEvidenceCipher, ComplaintVerifier},
    config::{AppConfig, Network},
    core::engine::{AdmissionGate, start_supervised_orderbook_with_admission},
    preview::PreviewService,
    pricing::EmbeddedPricing,
    readiness::ServiceReadiness,
    registry::SolverRegistry,
    session::{SolverSessions, domain},
    storage::OrderRepository,
};

enum ExitReason {
    Signal(&'static str),
    SignalError(std::io::Error),
    CriticalTask(TaskFailure),
}

pub async fn run(network_argument: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let network = Network::bootstrap(network_argument)?;
    kage_orderbook::logging::init();
    let config = AppConfig::load(network)?;
    let database_url = std::env::var("DATABASE_URL")?;
    let listen_address = std::env::var("KAGE_ORDERBOOK_LISTEN_ADDR")?;
    let rpc_url = std::env::var("KAGE_RPC_URL")?;
    let assignment_issuer = AssignmentIssuer::from_env()?;
    let shutdown = Shutdown::new();
    let mut supervisor = TaskSupervisor::new();

    let [chain] = config.chains.as_slice() else {
        return Err("exactly one chain must be configured".into());
    };

    let oracle_config = config
        .pricing_oracle
        .clone()
        .ok_or("pricing_oracle must be configured")?;
    let (pricing_handle, pricing_runtime) = PricingOracle::start(Arc::new(oracle_config))?;
    let pricing_shutdown = shutdown.clone();
    supervisor.spawn("embedded_pricing_runtime", async move {
        tokio::select! {
            error = pricing_runtime.wait_for_failure() => {
                tracing::error!(target: "pricing", %error, "embedded pricing runtime failed");
            }
            _ = pricing_shutdown.cancelled() => {}
        }
    });
    let pricing = EmbeddedPricing::new(pricing_handle);

    let repository = OrderRepository::connect_with_options(
        &database_url,
        Duration::from_millis(config.database.busy_timeout_ms),
        config.database.max_connections,
    )
    .await?;
    let repository_shutdown = repository.clone();
    repository.bind_network(network).await?;
    let proof_orders = repository.proof_orders();
    let previews = repository.previews();

    let indexer = RegistryIndexer::init(RegistryConfig {
        confirmations: chain.confirmations,
        ..RegistryConfig::new(rpc_url.clone(), chain.registry, chain.registry_deploy_block)
    })
    .await?;
    let registry = SolverRegistry::chain(indexer);
    let sessions = SolverSessions::new(domain(network, chain.chain_id));
    let admission = AdmissionGate::from_config(sessions.clone(), registry.clone(), &config);
    let orderbook_runtime = start_supervised_orderbook_with_admission(
        repository,
        config.runtime.command_capacity,
        config.proof_orders.clone(),
        admission,
        shutdown.clone(),
    )
    .await?;
    let orderbook = orderbook_runtime.handle;
    supervisor.supervise(orderbook_runtime.task);
    let readiness = ServiceReadiness::new();
    readiness.set_chain(true);
    supervisor.supervise(NamedTask::new(
        "pricing_readiness",
        readiness.monitor_embedded_pricing_until_shutdown(
            pricing.clone(),
            Duration::from_millis(250),
            shutdown.clone(),
        ),
    ));
    supervisor.supervise(NamedTask::new(
        "registry_readiness",
        readiness.monitor_registry_until_shutdown(
            registry.clone(),
            Duration::from_secs(1),
            shutdown.clone(),
        ),
    ));
    supervisor.supervise(NamedTask::new(
        "engine_readiness",
        readiness.monitor_engine_until_shutdown(
            orderbook.clone(),
            Duration::from_millis(250),
            shutdown.clone(),
        ),
    ));

    kage_orderbook::service_log!(
        "orderbook",
        "proof assignment signing enabled signer={}",
        assignment_issuer.signer_address()
    );
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
    let api_runtime = api::supervised_router(
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
        shutdown.clone(),
    );
    for task in api_runtime.tasks {
        supervisor.supervise(task);
    }
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
    let http_shutdown = shutdown.clone();
    supervisor.spawn("http_server", async move {
        if let Err(error) = axum::serve(
            listener,
            api_runtime
                .router
                .into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async move { http_shutdown.cancelled().await })
        .await
        {
            tracing::error!(target: "runtime", %error, "HTTP server failed");
        }
    });

    let exit = tokio::select! {
        signal = shutdown_signal() => match signal {
            Ok(signal) => ExitReason::Signal(signal),
            Err(error) => ExitReason::SignalError(error),
        },
        failure = supervisor.next_failure() => ExitReason::CriticalTask(failure),
    };

    match &exit {
        ExitReason::Signal(signal) => {
            tracing::info!(target: "runtime", %signal, "graceful shutdown started");
            readiness.begin_shutdown();
        }
        ExitReason::SignalError(error) => {
            tracing::error!(target: "runtime", %error, "signal listener failed");
            readiness.fail_liveness();
        }
        ExitReason::CriticalTask(failure) => {
            tracing::error!(target: "runtime", task = failure.name(), %failure, "fatal task exit");
            readiness.fail_liveness();
        }
    }

    shutdown.start();
    let grace = Duration::from_millis(config.runtime.shutdown_grace_ms);
    let drained = supervisor.drain(grace).await;
    repository_shutdown.close().await;
    if !drained {
        return Err(format!(
            "shutdown did not complete cleanly within {} ms",
            grace.as_millis()
        )
        .into());
    }

    match exit {
        ExitReason::Signal(signal) => {
            tracing::info!(target: "runtime", %signal, "graceful shutdown complete");
            Ok(())
        }
        ExitReason::SignalError(error) => Err(error.into()),
        ExitReason::CriticalTask(failure) => Err(failure.into()),
    }
}

async fn shutdown_signal() -> Result<&'static str, std::io::Error> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                Ok("ctrl_c")
            }
            _ = terminate.recv() => Ok("sigterm"),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok("ctrl_c")
    }
}
