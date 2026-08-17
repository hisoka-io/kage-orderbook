use std::error::Error;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use futures_util::StreamExt;
use kage_orderbook::api::{ExecutionStartedRequest, SOLVER_ADDRESS_HEADER};
use kage_orderbook::core::engine::SolverProofDelivery;
use kage_orderbook::core::events::OrderEvent;
use kage_orderbook::logging::short_id;
use kage_orderbook::order::{Order, OrderId, SolverId};
use kage_orderbook::proof::IntentProofV1;
use kage_orderbook::readiness::ReadinessSnapshot;
use kage_orderbook::registry::SolverProfile;
use reqwest::StatusCode;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

#[path = "mock_support/proof_transport.rs"]
mod proof_transport;

type BoxError = Box<dyn Error + Send + Sync>;
const DEFAULT_MOCK_PRIVATE_KEY: [u8; 32] = [0x33; 32];

#[derive(Debug, thiserror::Error)]
#[error("mock registry registration failed: {0}")]
struct RegistryRegistrationError(#[source] reqwest::Error);

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let http_url =
        std::env::var("ORDERBOOK_HTTP_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".to_owned());
    let ws_url = std::env::var("ORDERBOOK_WS_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:3000/events/solver/ws".to_owned());
    let registry_url =
        std::env::var("KAGE_REGISTRY_URL").unwrap_or_else(|_| "http://127.0.0.1:4000".to_owned());
    let solver_id = std::env::var("SOLVER_ADDRESS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| Address::repeat_byte(0x11));
    let noise_private_key: [u8; 32] = match std::env::var("SOLVER_NOISE_PRIVATE_KEY") {
        Ok(value) => value
            .parse::<B256>()
            .expect("SOLVER_NOISE_PRIVATE_KEY must contain exactly 32 hex bytes")
            .into(),
        Err(std::env::VarError::NotPresent) => DEFAULT_MOCK_PRIVATE_KEY,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("SOLVER_NOISE_PRIVATE_KEY must be valid UTF-8")
        }
    };
    let noise_public_key = B256::from(
        proof_transport::public_key(&noise_private_key)
            .expect("SOLVER_NOISE_PRIVATE_KEY must be a non-zero X25519 private key"),
    );

    kage_orderbook::service_log!(
        "solver",
        "started solver={solver_id} noise_public_key=derived"
    );

    let mut waiting_for = None;
    loop {
        let mut connected = false;
        let result = run(
            &http_url,
            &ws_url,
            &registry_url,
            solver_id,
            &noise_private_key,
            noise_public_key,
            &mut connected,
        )
        .await;
        if connected {
            waiting_for = None;
        }
        if let Err(error) = result {
            if error.downcast_ref::<RegistryRegistrationError>().is_some() {
                if waiting_for.as_deref() != Some("registry") {
                    kage_orderbook::service_log!(
                        "solver",
                        "waiting reason=registry_unavailable retry_in_ms=1000"
                    );
                    waiting_for = Some("registry".to_owned());
                }
            } else if is_service_unavailable(&error) {
                let missing = missing_dependencies(&http_url)
                    .await
                    .unwrap_or_else(|| "unknown".to_owned());
                if waiting_for.as_deref() != Some(missing.as_str()) {
                    kage_orderbook::service_log!(
                        "solver",
                        "waiting reason=orderbook_not_ready missing={missing} retry_in_ms=1000"
                    );
                    waiting_for = Some(missing);
                }
            } else {
                waiting_for = None;
                kage_orderbook::service_error!("solver", "connection failed error={error}");
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn run(
    http_url: &str,
    ws_url: &str,
    registry_url: &str,
    solver_id: SolverId,
    noise_private_key: &[u8; 32],
    noise_public_key: B256,
    connected: &mut bool,
) -> Result<(), BoxError> {
    let client = reqwest::Client::new();
    register_solver(&client, registry_url, solver_id, noise_public_key)
        .await
        .map_err(RegistryRegistrationError)?;
    let mut ws_request = ws_url.into_client_request()?;
    ws_request.headers_mut().insert(
        SOLVER_ADDRESS_HEADER,
        HeaderValue::from_str(&solver_id.to_string())?,
    );
    let (mut socket, _) = connect_async(ws_request).await?;
    *connected = true;
    kage_orderbook::service_log!("solver", "connected orderbook={http_url}");
    let jobs = client
        .get(format!("{http_url}/solver/jobs"))
        .header(SOLVER_ADDRESS_HEADER, solver_id.to_string())
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<Order>>()
        .await?;

    for order in jobs {
        reserve(&client, http_url, order.id, solver_id).await?;
    }
    execute_proofs(&client, http_url, solver_id, noise_private_key).await?;

    let mut registry_heartbeat = tokio::time::interval(Duration::from_secs(5));
    registry_heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    registry_heartbeat.tick().await;

    loop {
        tokio::select! {
            _ = registry_heartbeat.tick() => {
                register_solver(&client, registry_url, solver_id, noise_public_key)
                    .await
                    .map_err(RegistryRegistrationError)?;
            }
            message = socket.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                let message = message?;
                if !message.is_text() {
                    continue;
                }

                let event: OrderEvent = serde_json::from_str(message.to_text()?)?;
                match event {
                    OrderEvent::SolverReservationRequested { order_id } => {
                        kage_orderbook::service_log!(
                            "solver",
                            "available order={}",
                            short_id(order_id)
                        );
                        reserve(&client, http_url, order_id, solver_id).await?;
                    }
                    OrderEvent::ProofRelayed {
                        solver_id: assigned_solver,
                        ..
                    } if assigned_solver == solver_id => {
                        kage_orderbook::service_log!(
                            "solver",
                            "encrypted proof available solver={solver_id}"
                        );
                        execute_proofs(&client, http_url, solver_id, noise_private_key).await?;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn register_solver(
    client: &reqwest::Client,
    registry_url: &str,
    solver_id: SolverId,
    noise_public_key: B256,
) -> Result<(), reqwest::Error> {
    client
        .put(format!(
            "{}/solvers/{solver_id}",
            registry_url.trim_end_matches('/')
        ))
        .json(&SolverProfile {
            noise_public_key,
            active: true,
        })
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

fn is_service_unavailable(error: &BoxError) -> bool {
    if error
        .downcast_ref::<reqwest::Error>()
        .and_then(reqwest::Error::status)
        .is_some_and(|status| status == StatusCode::SERVICE_UNAVAILABLE)
    {
        return true;
    }

    matches!(
        error.downcast_ref::<tokio_tungstenite::tungstenite::Error>(),
        Some(tokio_tungstenite::tungstenite::Error::Http(response))
            if response.status()
                == tokio_tungstenite::tungstenite::http::StatusCode::SERVICE_UNAVAILABLE
    )
}

async fn missing_dependencies(http_url: &str) -> Option<String> {
    let snapshot = reqwest::get(format!("{http_url}/health/ready"))
        .await
        .ok()?
        .json::<ReadinessSnapshot>()
        .await
        .ok()?;
    let missing = snapshot
        .missing
        .into_iter()
        .filter(|dependency| dependency != "solver")
        .collect::<Vec<_>>();
    (!missing.is_empty()).then(|| missing.join(","))
}

async fn reserve(
    client: &reqwest::Client,
    http_url: &str,
    order_id: OrderId,
    solver_id: SolverId,
) -> Result<(), BoxError> {
    let response = client
        .post(format!("{http_url}/orders/{order_id}/reserve"))
        .header(SOLVER_ADDRESS_HEADER, solver_id.to_string())
        .send()
        .await?;

    if response.status() != StatusCode::CONFLICT {
        response.error_for_status()?;
        kage_orderbook::service_log!(
            "solver",
            "reserved order={} solver={solver_id}",
            short_id(order_id)
        );
    } else {
        kage_orderbook::service_log!(
            "solver",
            "reservation skipped order={} reason=already_reserved",
            short_id(order_id)
        );
    }
    Ok(())
}

async fn execute_proofs(
    client: &reqwest::Client,
    http_url: &str,
    solver_id: SolverId,
    noise_private_key: &[u8; 32],
) -> Result<(), BoxError> {
    let proofs = client
        .get(format!("{http_url}/solver/proofs"))
        .header(SOLVER_ADDRESS_HEADER, solver_id.to_string())
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<SolverProofDelivery>>()
        .await?;

    for delivery in proofs {
        let proof = match proof_transport::decrypt_from_user(
            delivery.order_id,
            noise_private_key,
            &delivery.ciphertext,
        ) {
            Ok(proof) => proof,
            Err(error) => {
                kage_orderbook::service_error!(
                    "solver",
                    "proof rejected order={} reason=noise_decryption_failed error={error}",
                    short_id(delivery.order_id)
                );
                continue;
            }
        };
        kage_orderbook::service_log!(
            "solver",
            "Noise proof decrypted order={} ciphertext_bytes={}",
            short_id(delivery.order_id),
            delivery.ciphertext.len()
        );
        let proof = match serde_json::from_slice::<IntentProofV1>(&proof) {
            Ok(proof) => proof,
            Err(error) => {
                kage_orderbook::service_error!(
                    "solver",
                    "proof rejected order={} reason=invalid_envelope error={error}",
                    short_id(delivery.order_id)
                );
                continue;
            }
        };
        if let Err(error) = proof.validate() {
            kage_orderbook::service_error!(
                "solver",
                "proof rejected order={} reason=validation_failed error={error}",
                short_id(delivery.order_id),
            );
            continue;
        }
        kage_orderbook::service_log!(
            "solver",
            "proof envelope accepted order={} proof_fields={} public_inputs={}",
            short_id(delivery.order_id),
            proof.proof_as_fields.len(),
            proof.public_inputs.len()
        );

        let tx_hash = tx_hash(delivery.order_id);
        client
            .post(format!(
                "{http_url}/orders/{}/execution-started",
                delivery.order_id
            ))
            .header(SOLVER_ADDRESS_HEADER, solver_id.to_string())
            .json(&ExecutionStartedRequest { tx_hash })
            .send()
            .await?
            .error_for_status()?;

        kage_orderbook::service_log!(
            "solver",
            "execution submitted order={} tx_hash={tx_hash}",
            short_id(delivery.order_id)
        );
    }

    Ok(())
}

fn tx_hash(order_id: OrderId) -> B256 {
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(order_id.as_bytes());
    bytes[16..].copy_from_slice(order_id.as_bytes());
    B256::from(bytes)
}
