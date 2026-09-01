use std::{env, fmt, sync::Arc};

use alloy_primitives::{B256, keccak256};
use chacha20poly1305::{
    KeyInit, XChaCha20Poly1305, XNonce,
    aead::{Aead, Payload},
};
use kage_types::{identifiers::OrderId, proof_orders::ComplaintEvidenceKind};
use thiserror::Error;

const EVIDENCE_KEY_ENV: &str = "KAGE_COMPLAINT_EVIDENCE_KEY";
const EVIDENCE_AAD_DOMAIN: &[u8] = b"kage:complaint-evidence:v1";
const EVIDENCE_KEY_ID_DOMAIN: &[u8] = b"kage:complaint-evidence-key-id:v1";

#[derive(Clone)]
pub struct ComplaintEvidenceCipher {
    key: Arc<[u8; 32]>,
    key_id: B256,
}

impl fmt::Debug for ComplaintEvidenceCipher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ComplaintEvidenceCipher")
            .field("key", &"[REDACTED]")
            .field("key_id", &self.key_id)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComplaintSecretOpening {
    pub nullifier: B256,
    pub salt: B256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptedComplaintOpening {
    pub key_id: B256,
    pub nonce: [u8; 24],
    pub ciphertext: Vec<u8>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ComplaintEvidenceError {
    #[error("{EVIDENCE_KEY_ENV} must be set")]
    MissingKey,
    #[error("{EVIDENCE_KEY_ENV} must contain exactly 32 non-zero bytes encoded as hex")]
    InvalidKey,
    #[error("complaint evidence encryption failed")]
    Encryption,
    #[error("complaint evidence cannot be decrypted with the configured key")]
    Decryption,
}

impl ComplaintEvidenceCipher {
    pub fn from_env() -> Result<Self, ComplaintEvidenceError> {
        let encoded = env::var(EVIDENCE_KEY_ENV).map_err(|_| ComplaintEvidenceError::MissingKey)?;
        let decoded = alloy_primitives::hex::decode(encoded.trim().trim_start_matches("0x"))
            .map_err(|_| ComplaintEvidenceError::InvalidKey)?;
        let key = <[u8; 32]>::try_from(decoded).map_err(|_| ComplaintEvidenceError::InvalidKey)?;
        Self::new(key)
    }

    pub fn new(key: [u8; 32]) -> Result<Self, ComplaintEvidenceError> {
        if key.iter().all(|byte| *byte == 0) {
            return Err(ComplaintEvidenceError::InvalidKey);
        }
        let mut id_input = Vec::with_capacity(EVIDENCE_KEY_ID_DOMAIN.len() + key.len());
        id_input.extend_from_slice(EVIDENCE_KEY_ID_DOMAIN);
        id_input.extend_from_slice(&key);
        Ok(Self {
            key: Arc::new(key),
            key_id: keccak256(id_input),
        })
    }

    pub fn key_id(&self) -> B256 {
        self.key_id
    }

    pub fn encrypt(
        &self,
        order_id: OrderId,
        evidence_kind: ComplaintEvidenceKind,
        opening: ComplaintSecretOpening,
    ) -> Result<EncryptedComplaintOpening, ComplaintEvidenceError> {
        let random = B256::random();
        let mut nonce = [0_u8; 24];
        nonce.copy_from_slice(&random[..24]);
        let mut plaintext = [0_u8; 64];
        plaintext[..32].copy_from_slice(opening.nullifier.as_slice());
        plaintext[32..].copy_from_slice(opening.salt.as_slice());
        let aad = evidence_aad(order_id, evidence_kind);
        let encrypted = XChaCha20Poly1305::new(self.key.as_ref().into()).encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: &aad,
            },
        );
        plaintext.fill(0);
        let ciphertext = encrypted.map_err(|_| ComplaintEvidenceError::Encryption)?;
        Ok(EncryptedComplaintOpening {
            key_id: self.key_id,
            nonce,
            ciphertext,
        })
    }

    pub fn decrypt(
        &self,
        order_id: OrderId,
        evidence_kind: ComplaintEvidenceKind,
        evidence: &EncryptedComplaintOpening,
    ) -> Result<ComplaintSecretOpening, ComplaintEvidenceError> {
        if evidence.key_id != self.key_id {
            return Err(ComplaintEvidenceError::Decryption);
        }
        let aad = evidence_aad(order_id, evidence_kind);
        let mut plaintext = XChaCha20Poly1305::new(self.key.as_ref().into())
            .decrypt(
                XNonce::from_slice(&evidence.nonce),
                Payload {
                    msg: &evidence.ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| ComplaintEvidenceError::Decryption)?;
        if plaintext.len() != 64 {
            plaintext.fill(0);
            return Err(ComplaintEvidenceError::Decryption);
        }
        let nullifier = B256::from_slice(&plaintext[..32]);
        let salt = B256::from_slice(&plaintext[32..]);
        plaintext.fill(0);
        Ok(ComplaintSecretOpening { nullifier, salt })
    }
}

fn evidence_aad(order_id: OrderId, evidence_kind: ComplaintEvidenceKind) -> Vec<u8> {
    let mut aad = Vec::with_capacity(EVIDENCE_AAD_DOMAIN.len() + 16 + 1);
    aad.extend_from_slice(EVIDENCE_AAD_DOMAIN);
    aad.extend_from_slice(order_id.as_bytes());
    aad.push(match evidence_kind {
        ComplaintEvidenceKind::NoResponseAfterDisclosure => 1,
        ComplaintEvidenceKind::AcceptedNotSettled => 2,
    });
    aad
}
