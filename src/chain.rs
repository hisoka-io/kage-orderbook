use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    core::{command::Command, engine::OrderbookHandle, events::OrderEvent},
    logging::short_id,
};
use alloy::rpc::types::TransactionReceipt;
use alloy::{
    consensus::Transaction as _,
    primitives::{Address, TxHash},
    providers::{DynProvider, Provider, ProviderBuilder},
    sol,
    sol_types::SolCall,
};

sol! {
    #[sol(rpc)]
    interface IDarkPool {
        function kageSwap(bytes proof, bytes32[] publicInputs) external;
    }
}

const RECEIPT_TIMEOUT: Duration = Duration::from_secs(300);
const POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub struct SettlementWatcher {
    provider: DynProvider,
    darkpool: Address,
    confirmations: u64,
    orderbook: OrderbookHandle,
    claimed: Arc<Mutex<HashSet<TxHash>>>,
}

#[derive(Debug, thiserror::Error)]
pub enum WatchError {
    #[error("invalid rpc url: {0}")]
    RpcUrl(String),
    #[error("no contract at darkpool {0}")]
    NotAContract(Address),
    #[error(transparent)]
    Rpc(#[from] alloy::transports::TransportError),
    #[error("{0} was not mined within the receipt timeout")]
    NotMined(TxHash),
}

impl SettlementWatcher {
    pub async fn connect(
        rpc_url: &str,
        darkpool: Address,
        confirmations: u64,
        orderbook: OrderbookHandle,
    ) -> Result<Self, WatchError> {
        let url = rpc_url
            .parse()
            .map_err(|_| WatchError::RpcUrl(rpc_url.to_owned()))?;
        let provider = ProviderBuilder::new().connect_http(url).erased();
        if provider.get_code_at(darkpool).await?.is_empty() {
            return Err(WatchError::NotAContract(darkpool));
        }

        Ok(Self {
            provider,
            darkpool,
            confirmations,
            orderbook,
            claimed: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut events = self.orderbook.subscribe();
            match self.orderbook.executing_orders().await {
                Ok(orders) => {
                    for order in orders {
                        if let Some(tx_hash) = order.tx_hash {
                            self.clone().watch(order.id, tx_hash);
                        }
                    }
                }
                Err(error) => {
                    crate::service_error!("chain", "backlog unavailable error={error:?}");
                }
            }

            while let Ok(event) = events.recv().await {
                if let OrderEvent::ExecutionStarted { order_id, tx_hash } = event {
                    self.clone().watch(order_id, tx_hash);
                }
            }
        })
    }

    fn watch(self, order_id: crate::order::OrderId, tx_hash: TxHash) {
        tokio::spawn(async move {
            let command = match self.confirm(tx_hash).await {
                Ok(true) => {
                    tracing::debug!(
                        target: "chain",
                        order_id = %short_id(order_id),
                        %tx_hash,
                        "settlement confirmed"
                    );
                    Command::SettlementObserved { order_id, tx_hash }
                }
                Ok(false) => {
                    crate::service_error!(
                        "chain",
                        "settlement failed order_id={} tx_hash={tx_hash}",
                        short_id(order_id)
                    );
                    Command::ExecutionFailed { order_id, tx_hash }
                }
                Err(error) => {
                    crate::service_error!(
                        "chain",
                        "settlement unresolved order_id={} tx_hash={tx_hash} error={error}",
                        short_id(order_id)
                    );
                    return;
                }
            };
            if let Err(error) = self.orderbook.execute(command).await {
                crate::service_error!(
                    "chain",
                    "settlement not recorded order_id={} error={error:?}",
                    short_id(order_id)
                );
            }
        });
    }

    async fn receipt(&self, tx_hash: TxHash) -> Result<TransactionReceipt, WatchError> {
        let deadline = Instant::now() + RECEIPT_TIMEOUT;
        loop {
            if let Some(receipt) = self.provider.get_transaction_receipt(tx_hash).await? {
                let mined_at = receipt.block_number.unwrap_or_default();
                let head = self.provider.get_block_number().await?;
                if head.saturating_sub(mined_at) >= self.confirmations {
                    return Ok(receipt);
                }
            }
            if Instant::now() >= deadline {
                return Err(WatchError::NotMined(tx_hash));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn confirm(&self, tx_hash: TxHash) -> Result<bool, WatchError> {
        if !self
            .claimed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(tx_hash)
        {
            return Ok(false);
        }

        let receipt = self.receipt(tx_hash).await?;
        if !receipt.status() || receipt.to != Some(self.darkpool) {
            return Ok(false);
        }

        let Some(transaction) = self.provider.get_transaction_by_hash(tx_hash).await? else {
            return Ok(false);
        };
        Ok(transaction
            .input()
            .starts_with(&IDarkPool::kageSwapCall::SELECTOR))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_kage_swap_selector_matches_the_abi() {
        assert_eq!(
            IDarkPool::kageSwapCall::SELECTOR,
            alloy::primitives::keccak256("kageSwap(bytes,bytes32[])")[..4]
        );
    }
}
