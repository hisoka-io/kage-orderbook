use std::{ffi::OsString, io, path::PathBuf, process::Stdio, time::Duration};

use kage_types::proof::IntentProofV1;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};
use uuid::Uuid;

const PROOF_PROTOCOL_VERSION: u8 = 1;
const MAX_PROOF_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Serialize)]
pub struct ProofOrderV1 {
    pub order_id: Uuid,
    pub chain_id: u64,
    pub token_in: String,
    pub token_out: String,
    pub amount_in: String,
    pub amount_out: String,
    pub expires_at_ms: i64,
}

pub struct ProverWorker {
    child: Child,
    session: ProverSession<BufReader<ChildStdout>, ChildStdin>,
}

impl ProverWorker {
    pub fn spawn(timeout: Duration) -> Result<Self, ProverWorkerError> {
        let tools_root = std::env::var_os("KAGE_TOOLS_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tools"));
        let node = std::env::var_os("KAGE_NODE_BIN").unwrap_or_else(|| OsString::from("node"));
        let mut command = Command::new(node);
        command
            .arg("--import")
            .arg("tsx")
            .arg("mock-kage-user/src/worker.ts")
            .current_dir(&tools_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);

        let mut child = command.spawn().map_err(ProverWorkerError::Start)?;
        let stdin = child.stdin.take().ok_or(ProverWorkerError::MissingStdin)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(ProverWorkerError::MissingStdout)?;
        Ok(Self {
            child,
            session: ProverSession::new(BufReader::new(stdout), stdin, timeout),
        })
    }

    pub async fn prove(&mut self, order: ProofOrderV1) -> Result<IntentProofV1, ProverWorkerError> {
        if let Some(status) = self.child.try_wait().map_err(ProverWorkerError::Io)? {
            return Err(ProverWorkerError::Exited(status.to_string()));
        }
        let result = self.session.prove(order).await;
        if result.is_err()
            && let Some(status) = self.child.try_wait().map_err(ProverWorkerError::Io)?
        {
            return Err(ProverWorkerError::Exited(status.to_string()));
        }
        result
    }

    pub async fn shutdown(mut self) -> Result<(), ProverWorkerError> {
        self.session.close().await?;
        drop(self.session);
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .map_err(|_| ProverWorkerError::ShutdownTimeout)?
            .map_err(ProverWorkerError::Io)?;
        if status.success() {
            Ok(())
        } else {
            Err(ProverWorkerError::Exited(status.to_string()))
        }
    }
}

struct ProverSession<R, W> {
    reader: R,
    writer: W,
    timeout: Duration,
}

impl<R, W> ProverSession<R, W>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    fn new(reader: R, writer: W, timeout: Duration) -> Self {
        Self {
            reader,
            writer,
            timeout,
        }
    }

    async fn prove(&mut self, order: ProofOrderV1) -> Result<IntentProofV1, ProverWorkerError> {
        let request_id = Uuid::new_v4();
        let request = ProofRequestV1 {
            version: PROOF_PROTOCOL_VERSION,
            request_type: "prove_swap_intent",
            request_id,
            wallet_fixture: "default",
            order,
        };
        let mut line = serde_json::to_vec(&request).map_err(ProverWorkerError::EncodeRequest)?;
        line.push(b'\n');
        self.writer
            .write_all(&line)
            .await
            .map_err(ProverWorkerError::Io)?;
        self.writer.flush().await.map_err(ProverWorkerError::Io)?;

        let mut response_bytes = Vec::new();
        let bytes_read = tokio::time::timeout(
            self.timeout,
            self.reader.read_until(b'\n', &mut response_bytes),
        )
        .await
        .map_err(|_| ProverWorkerError::Timeout(self.timeout))?
        .map_err(ProverWorkerError::Io)?;
        if bytes_read == 0 {
            return Err(ProverWorkerError::ClosedOutput);
        }
        if response_bytes.len() > MAX_PROOF_RESPONSE_BYTES {
            return Err(ProverWorkerError::ResponseTooLarge);
        }
        if response_bytes.last() != Some(&b'\n') {
            return Err(ProverWorkerError::TruncatedResponse);
        }

        let response: ProofResponseV1 =
            serde_json::from_slice(&response_bytes).map_err(ProverWorkerError::DecodeResponse)?;
        if response.version != PROOF_PROTOCOL_VERSION || response.response_type != "proof_response"
        {
            return Err(ProverWorkerError::InvalidResponse(
                "unsupported response version or type".to_owned(),
            ));
        }
        if response.request_id != Some(request_id) {
            return Err(ProverWorkerError::InvalidResponse(
                "response request_id does not match the request".to_owned(),
            ));
        }
        if response.ok {
            if response.error.is_some() {
                return Err(ProverWorkerError::InvalidResponse(
                    "successful response included an error".to_owned(),
                ));
            }
            let proof = response.proof.ok_or_else(|| {
                ProverWorkerError::InvalidResponse(
                    "successful response is missing its proof".to_owned(),
                )
            })?;
            crate::proof_validation::validate(&proof).map_err(ProverWorkerError::InvalidProof)?;
            Ok(proof)
        } else {
            if response.proof.is_some() {
                return Err(ProverWorkerError::InvalidResponse(
                    "failed response included a proof".to_owned(),
                ));
            }
            let error = response.error.ok_or_else(|| {
                ProverWorkerError::InvalidResponse(
                    "failed response is missing its error".to_owned(),
                )
            })?;
            Err(ProverWorkerError::Rejected {
                code: error.code,
                message: error.message,
            })
        }
    }

    async fn close(&mut self) -> Result<(), ProverWorkerError> {
        self.writer.shutdown().await.map_err(ProverWorkerError::Io)
    }
}

#[derive(Serialize)]
struct ProofRequestV1<'a> {
    version: u8,
    #[serde(rename = "type")]
    request_type: &'a str,
    request_id: Uuid,
    wallet_fixture: &'a str,
    order: ProofOrderV1,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProofResponseV1 {
    version: u8,
    #[serde(rename = "type")]
    response_type: String,
    request_id: Option<Uuid>,
    ok: bool,
    proof: Option<IntentProofV1>,
    error: Option<ProofErrorV1>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProofErrorV1 {
    code: String,
    message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ProverWorkerError {
    #[error("failed to start prover worker: {0}")]
    Start(io::Error),
    #[error("prover worker stdin is unavailable")]
    MissingStdin,
    #[error("prover worker stdout is unavailable")]
    MissingStdout,
    #[error("prover worker I/O failed: {0}")]
    Io(io::Error),
    #[error("failed to encode prover request: {0}")]
    EncodeRequest(serde_json::Error),
    #[error("invalid prover response: {0}")]
    DecodeResponse(serde_json::Error),
    #[error("prover response is invalid: {0}")]
    InvalidResponse(String),
    #[error("prover returned an invalid proof: {0}")]
    InvalidProof(crate::proof_validation::ProofValidationError),
    #[error("prover rejected the request: {code}: {message}")]
    Rejected { code: String, message: String },
    #[error("prover worker timed out after {0:?}")]
    Timeout(Duration),
    #[error("prover worker closed stdout before responding")]
    ClosedOutput,
    #[error("prover worker returned a response larger than 2 MiB")]
    ResponseTooLarge,
    #[error("prover worker returned a truncated response")]
    TruncatedResponse,
    #[error("prover worker exited: {0}")]
    Exited(String),
    #[error("prover worker did not stop within 10 seconds")]
    ShutdownTimeout,
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::proof_validation::{
        INTENT_PROOF_FIELDS, INTENT_PUBLIC_INPUTS, INTENT_VERIFICATION_KEY_FIELDS,
        INTENT_VERIFICATION_KEY_HASH,
    };
    use tokio::io::{BufReader, duplex, split};

    use super::*;

    #[tokio::test]
    async fn correlates_a_request_with_its_proof_response() {
        let (client, server) = duplex(MAX_PROOF_RESPONSE_BYTES);
        let (client_read, client_write) = split(client);
        let (server_read, mut server_write) = split(server);
        let server = tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            let mut request = String::new();
            reader.read_line(&mut request).await.unwrap();
            let request: serde_json::Value = serde_json::from_str(&request).unwrap();
            let response = serde_json::json!({
                "version": 1,
                "type": "proof_response",
                "request_id": request["request_id"],
                "ok": true,
                "proof": valid_proof(),
            });
            server_write
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        });
        let mut session = ProverSession::new(
            BufReader::new(client_read),
            client_write,
            Duration::from_secs(1),
        );
        let proof = session.prove(test_order()).await.unwrap();
        assert_eq!(proof.proof_as_fields.len(), INTENT_PROOF_FIELDS);
        server.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires tools dependencies and a built ../darkpool prover"]
    async fn generates_a_real_proof_through_the_worker() {
        let mut worker = ProverWorker::spawn(Duration::from_secs(120)).unwrap();
        let order = real_test_order();
        let order_id = order.order_id;
        let proof = worker.prove(order).await.unwrap();
        crate::proof_validation::validate(&proof).unwrap();
        let plaintext = serde_json::to_vec(&proof).unwrap();
        let private_key = [0x33; 32];
        let public_key = crate::proof_transport::public_key(&private_key).unwrap();
        let envelope =
            crate::proof_transport::encrypt_for_solver(order_id, &public_key, &plaintext).unwrap();
        let decrypted =
            crate::proof_transport::decrypt_from_user(order_id, &private_key, &envelope).unwrap();
        assert_eq!(decrypted, plaintext);
        worker.shutdown().await.unwrap();
    }

    fn valid_proof() -> IntentProofV1 {
        IntentProofV1 {
            version: 1,
            circuit: "swap_intent".to_owned(),
            proof_system: "ultra_honk".to_owned(),
            verifier_target: "noir-recursive".to_owned(),
            proof: format!("0x{}", "0".repeat(INTENT_PROOF_FIELDS * 64)),
            proof_as_fields: vec![field("00"); INTENT_PROOF_FIELDS],
            public_inputs: vec![field("00"); INTENT_PUBLIC_INPUTS],
            verification_key_fields: vec![field("00"); INTENT_VERIFICATION_KEY_FIELDS],
            verification_key_hash: INTENT_VERIFICATION_KEY_HASH.to_owned(),
        }
    }

    fn field(suffix: &str) -> String {
        format!("0x{suffix:0>64}")
    }

    fn test_order() -> ProofOrderV1 {
        ProofOrderV1 {
            order_id: Uuid::new_v4(),
            chain_id: 31_337,
            token_in: "0x0101010101010101010101010101010101010101".to_owned(),
            token_out: "0x0202020202020202020202020202020202020202".to_owned(),
            amount_in: "1000000000000000000".to_owned(),
            amount_out: "2000000000".to_owned(),
            expires_at_ms: 2_000_000_000_000,
        }
    }

    fn real_test_order() -> ProofOrderV1 {
        ProofOrderV1 {
            expires_at_ms: chrono::Utc::now().timestamp_millis() + 300_000,
            amount_out: "1900000000".to_owned(),
            ..test_order()
        }
    }
}
