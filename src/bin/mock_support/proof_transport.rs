#![allow(dead_code)]

use snow::params::NoiseParams;
use uuid::Uuid;
use x25519_dalek::{X25519_BASEPOINT_BYTES, x25519};

pub const NOISE_PROTOCOL: &str = "Noise_N_25519_ChaChaPoly_BLAKE2s";

const MAGIC: &[u8; 4] = b"KNP1";
const MAX_NOISE_MESSAGE_BYTES: usize = 65_535;
const PLAINTEXT_CHUNK_BYTES: usize = 60 * 1024;
const MAX_PLAINTEXT_BYTES: usize = 900 * 1024;
const MAX_CHUNKS: usize = MAX_PLAINTEXT_BYTES.div_ceil(PLAINTEXT_CHUNK_BYTES);

pub fn public_key(private_key: &[u8; 32]) -> Result<[u8; 32], ProofTransportError> {
    if private_key.iter().all(|byte| *byte == 0) {
        return Err(ProofTransportError::InvalidPrivateKey);
    }
    Ok(x25519(*private_key, X25519_BASEPOINT_BYTES))
}

pub fn encrypt_for_solver(
    order_id: Uuid,
    solver_public_key: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, ProofTransportError> {
    if solver_public_key.len() != 32 || solver_public_key.iter().all(|byte| *byte == 0) {
        return Err(ProofTransportError::InvalidPublicKey);
    }
    if plaintext.is_empty() {
        return Err(ProofTransportError::EmptyPayload);
    }
    if plaintext.len() > MAX_PLAINTEXT_BYTES {
        return Err(ProofTransportError::PayloadTooLarge);
    }

    let prologue = prologue(order_id);
    let mut handshake = snow::Builder::new(params())
        .prologue(&prologue)?
        .remote_public_key(solver_public_key)?
        .build_initiator()?;
    let mut handshake_message = vec![0_u8; MAX_NOISE_MESSAGE_BYTES];
    let handshake_len = handshake.write_message(&[], &mut handshake_message)?;
    handshake_message.truncate(handshake_len);
    let mut transport = handshake.into_transport_mode()?;

    let mut messages = Vec::with_capacity(plaintext.len().div_ceil(PLAINTEXT_CHUNK_BYTES));
    for chunk in plaintext.chunks(PLAINTEXT_CHUNK_BYTES) {
        let mut message = vec![0_u8; MAX_NOISE_MESSAGE_BYTES];
        let length = transport.write_message(chunk, &mut message)?;
        message.truncate(length);
        messages.push(message);
    }
    encode_envelope(&handshake_message, &messages)
}

pub fn decrypt_from_user(
    order_id: Uuid,
    solver_private_key: &[u8; 32],
    envelope: &[u8],
) -> Result<Vec<u8>, ProofTransportError> {
    if solver_private_key.iter().all(|byte| *byte == 0) {
        return Err(ProofTransportError::InvalidPrivateKey);
    }
    let decoded = decode_envelope(envelope)?;
    let prologue = prologue(order_id);
    let mut handshake = snow::Builder::new(params())
        .prologue(&prologue)?
        .local_private_key(solver_private_key)?
        .build_responder()?;
    let mut empty_payload = [0_u8; 1];
    if handshake.read_message(decoded.handshake, &mut empty_payload)? != 0 {
        return Err(ProofTransportError::MalformedEnvelope);
    }
    let mut transport = handshake.into_transport_mode()?;
    let mut plaintext = Vec::new();

    for message in decoded.messages {
        let mut chunk = vec![0_u8; MAX_NOISE_MESSAGE_BYTES];
        let length = transport.read_message(message, &mut chunk)?;
        if plaintext.len().saturating_add(length) > MAX_PLAINTEXT_BYTES {
            return Err(ProofTransportError::PayloadTooLarge);
        }
        plaintext.extend_from_slice(&chunk[..length]);
    }
    if plaintext.is_empty() {
        return Err(ProofTransportError::EmptyPayload);
    }
    Ok(plaintext)
}

fn params() -> NoiseParams {
    NOISE_PROTOCOL
        .parse()
        .expect("checked-in Noise protocol name must be valid")
}

fn prologue(order_id: Uuid) -> Vec<u8> {
    let mut value = b"kage-orderbook/proof/v1".to_vec();
    value.extend_from_slice(order_id.as_bytes());
    value
}

fn encode_envelope(handshake: &[u8], messages: &[Vec<u8>]) -> Result<Vec<u8>, ProofTransportError> {
    let handshake_len =
        u16::try_from(handshake.len()).map_err(|_| ProofTransportError::MalformedEnvelope)?;
    let message_count =
        u16::try_from(messages.len()).map_err(|_| ProofTransportError::MalformedEnvelope)?;
    let mut envelope = Vec::with_capacity(
        8 + handshake.len()
            + messages
                .iter()
                .map(|message| 4 + message.len())
                .sum::<usize>(),
    );
    envelope.extend_from_slice(MAGIC);
    envelope.extend_from_slice(&handshake_len.to_be_bytes());
    envelope.extend_from_slice(&message_count.to_be_bytes());
    envelope.extend_from_slice(handshake);
    for message in messages {
        let length =
            u32::try_from(message.len()).map_err(|_| ProofTransportError::MalformedEnvelope)?;
        envelope.extend_from_slice(&length.to_be_bytes());
        envelope.extend_from_slice(message);
    }
    Ok(envelope)
}

fn decode_envelope(envelope: &[u8]) -> Result<DecodedEnvelope<'_>, ProofTransportError> {
    if envelope.len() < 8 || &envelope[..4] != MAGIC {
        return Err(ProofTransportError::MalformedEnvelope);
    }
    let handshake_len = usize::from(u16::from_be_bytes([envelope[4], envelope[5]]));
    let message_count = usize::from(u16::from_be_bytes([envelope[6], envelope[7]]));
    if handshake_len == 0 || message_count == 0 || message_count > MAX_CHUNKS {
        return Err(ProofTransportError::MalformedEnvelope);
    }
    let handshake_end = 8_usize
        .checked_add(handshake_len)
        .filter(|end| *end <= envelope.len())
        .ok_or(ProofTransportError::MalformedEnvelope)?;
    let handshake = &envelope[8..handshake_end];
    let mut cursor = handshake_end;
    let mut messages = Vec::with_capacity(message_count);

    for _ in 0..message_count {
        let length_end = cursor
            .checked_add(4)
            .filter(|end| *end <= envelope.len())
            .ok_or(ProofTransportError::MalformedEnvelope)?;
        let length = u32::from_be_bytes(
            envelope[cursor..length_end]
                .try_into()
                .map_err(|_| ProofTransportError::MalformedEnvelope)?,
        ) as usize;
        if length == 0 || length > MAX_NOISE_MESSAGE_BYTES {
            return Err(ProofTransportError::MalformedEnvelope);
        }
        cursor = length_end;
        let message_end = cursor
            .checked_add(length)
            .filter(|end| *end <= envelope.len())
            .ok_or(ProofTransportError::MalformedEnvelope)?;
        messages.push(&envelope[cursor..message_end]);
        cursor = message_end;
    }
    if cursor != envelope.len() {
        return Err(ProofTransportError::MalformedEnvelope);
    }
    Ok(DecodedEnvelope {
        handshake,
        messages,
    })
}

struct DecodedEnvelope<'a> {
    handshake: &'a [u8],
    messages: Vec<&'a [u8]>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProofTransportError {
    #[error("solver Noise public key must be exactly 32 non-zero bytes")]
    InvalidPublicKey,
    #[error("solver Noise private key must be exactly 32 non-zero bytes")]
    InvalidPrivateKey,
    #[error("proof payload must not be empty")]
    EmptyPayload,
    #[error("proof payload exceeds the 900 KiB transport limit")]
    PayloadTooLarge,
    #[error("malformed Noise proof envelope")]
    MalformedEnvelope,
    #[error("Noise protocol failed: {0}")]
    Noise(#[from] snow::Error),
}
