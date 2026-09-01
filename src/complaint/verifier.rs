use alloy_primitives::{Address, B256, keccak256};
use serde::Deserialize;
use thiserror::Error;

use crate::config::ComplaintFinalityPolicy;

#[derive(Clone)]
pub struct ComplaintVerifier {
    rpc_url: String,
    darkpool: Address,
    finality: ComplaintFinalityPolicy,
    client: reqwest::Client,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedNullifierStatus {
    pub spent: bool,
    pub block_number: u64,
    pub block_hash: B256,
    pub block_timestamp: u64,
}

#[derive(Debug, Error)]
pub enum ComplaintVerificationError {
    #[error("darkpool RPC request failed")]
    Request(#[from] reqwest::Error),
    #[error("darkpool RPC returned an error")]
    Rpc,
    #[error("darkpool RPC returned an invalid verification block")]
    InvalidBlock,
    #[error("the chain does not have enough history for the configured confirmation policy")]
    InsufficientHistory,
    #[error("the configured canonical verification block predates proof expiry")]
    VerificationBlockTooOld,
    #[error("darkpool returned an invalid isNullifierSpent result")]
    InvalidResult,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    #[allow(dead_code)]
    message: String,
}

#[derive(Deserialize)]
struct RpcBlock {
    number: String,
    hash: B256,
    timestamp: String,
}

impl ComplaintVerifier {
    pub fn new(rpc_url: String, darkpool: Address, finality: ComplaintFinalityPolicy) -> Self {
        Self {
            rpc_url,
            darkpool,
            finality,
            client: reqwest::Client::new(),
        }
    }

    pub fn darkpool(&self) -> Address {
        self.darkpool
    }

    pub async fn is_nullifier_spent(
        &self,
        nullifier: B256,
        not_before_timestamp_secs: u64,
    ) -> Result<VerifiedNullifierStatus, ComplaintVerificationError> {
        let block = self.verification_block(not_before_timestamp_secs).await?;
        let block_number =
            parse_quantity(&block.number).ok_or(ComplaintVerificationError::InvalidBlock)?;
        let block_timestamp =
            parse_quantity(&block.timestamp).ok_or(ComplaintVerificationError::InvalidBlock)?;
        let selector = &keccak256(b"isNullifierSpent(bytes32)")[..4];
        let mut call_data = Vec::with_capacity(36);
        call_data.extend_from_slice(selector);
        call_data.extend_from_slice(nullifier.as_slice());
        let result: String = self
            .rpc(
                "eth_call",
                serde_json::json!([{
                    "to": self.darkpool.to_string(),
                    "data": format!("0x{}", alloy_primitives::hex::encode(call_data))
                }, {
                    "blockHash": block.hash,
                    "requireCanonical": true
                }]),
            )
            .await?;
        let bytes = alloy_primitives::hex::decode(result.trim_start_matches("0x"))
            .map_err(|_| ComplaintVerificationError::InvalidResult)?;
        if bytes.len() != 32 || bytes[..31].iter().any(|byte| *byte != 0) {
            return Err(ComplaintVerificationError::InvalidResult);
        }
        let spent = match bytes[31] {
            0 => false,
            1 => true,
            _ => return Err(ComplaintVerificationError::InvalidResult),
        };
        Ok(VerifiedNullifierStatus {
            spent,
            block_number,
            block_hash: block.hash,
            block_timestamp,
        })
    }

    async fn verification_block(
        &self,
        not_before_timestamp_secs: u64,
    ) -> Result<RpcBlock, ComplaintVerificationError> {
        let (tag, expected_number) = match self.finality {
            ComplaintFinalityPolicy::Finalized => ("finalized".to_owned(), None),
            ComplaintFinalityPolicy::Confirmations { count } => {
                let latest: String = self.rpc("eth_blockNumber", serde_json::json!([])).await?;
                let latest =
                    parse_quantity(&latest).ok_or(ComplaintVerificationError::InvalidBlock)?;
                let confirmed = latest
                    .checked_sub(count)
                    .ok_or(ComplaintVerificationError::InsufficientHistory)?;
                (format!("0x{confirmed:x}"), Some(confirmed))
            }
        };
        let block: RpcBlock = self
            .rpc("eth_getBlockByNumber", serde_json::json!([tag, false]))
            .await?;
        let number =
            parse_quantity(&block.number).ok_or(ComplaintVerificationError::InvalidBlock)?;
        let timestamp =
            parse_quantity(&block.timestamp).ok_or(ComplaintVerificationError::InvalidBlock)?;
        if block.hash == B256::ZERO || expected_number.is_some_and(|expected| expected != number) {
            return Err(ComplaintVerificationError::InvalidBlock);
        }
        if timestamp < not_before_timestamp_secs {
            return Err(ComplaintVerificationError::VerificationBlockTooOld);
        }
        Ok(block)
    }

    async fn rpc<T: serde::de::DeserializeOwned>(
        &self,
        method: &'static str,
        params: serde_json::Value,
    ) -> Result<T, ComplaintVerificationError> {
        let response = self
            .client
            .post(&self.rpc_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .await?
            .error_for_status()?
            .json::<RpcResponse<T>>()
            .await?;
        if response.error.is_some() {
            return Err(ComplaintVerificationError::Rpc);
        }
        response
            .result
            .ok_or(ComplaintVerificationError::InvalidBlock)
    }
}

pub(super) fn parse_quantity(value: &str) -> Option<u64> {
    u64::from_str_radix(value.strip_prefix("0x")?, 16).ok()
}
