use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use alloy_primitives::{Address, TxHash, keccak256};
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use tokio::net::TcpListener;

fn kage_swap_selector() -> String {
    alloy_primitives::hex::encode(&keccak256("kageSwap(bytes,bytes32[])")[..4])
}

#[derive(Clone, Default)]
pub struct MockChain {
    transactions: Arc<Mutex<HashMap<TxHash, Transaction>>>,
    head: Arc<AtomicU64>,
    darkpool: Address,
}

#[derive(Clone, Copy)]
struct Transaction {
    succeeded: bool,
    to: Address,
    is_kage_swap: bool,
}

impl MockChain {
    pub fn new(darkpool: Address) -> Self {
        Self {
            transactions: Arc::new(Mutex::new(HashMap::new())),
            head: Arc::new(AtomicU64::new(0x64)),
            darkpool,
        }
    }

    pub fn darkpool(&self) -> Address {
        self.darkpool
    }

    pub fn settle(&self, tx_hash: TxHash) {
        self.insert(tx_hash, true, self.darkpool, true);
    }

    pub fn revert(&self, tx_hash: TxHash) {
        self.insert(tx_hash, false, self.darkpool, true);
    }

    pub fn unrelated(&self, tx_hash: TxHash) {
        self.insert(tx_hash, true, self.darkpool, false);
    }

    fn insert(&self, tx_hash: TxHash, succeeded: bool, to: Address, is_kage_swap: bool) {
        self.transactions.lock().unwrap().insert(
            tx_hash,
            Transaction {
                succeeded,
                to,
                is_kage_swap,
            },
        );
    }

    pub async fn spawn(self) -> String {
        let app = Router::new().route("/", post(rpc)).with_state(self);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}")
    }
}

async fn rpc(
    axum::extract::State(chain): axum::extract::State<MockChain>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let id = request["id"].clone();
    let method = request["method"].as_str().unwrap_or_default();
    let params = &request["params"];
    let result = match method {
        "eth_getCode" => json!("0x60006000"),
        "eth_chainId" => json!("0x7a69"),
        "eth_blockNumber" => json!(format!("0x{:x}", chain.head.fetch_add(1, Ordering::AcqRel))),
        "eth_getBlockByNumber" => block(&chain),
        "eth_getTransactionReceipt" => receipt(&chain, params),
        "eth_getTransactionByHash" => transaction(&chain, params),
        _ => Value::Null,
    };
    Json(json!({"jsonrpc": "2.0", "id": id, "result": result}))
}

fn block(chain: &MockChain) -> Value {
    let number = chain.head.fetch_add(1, Ordering::AcqRel);
    json!({
        "hash": alloy_primitives::B256::repeat_byte(0xab),
        "parentHash": alloy_primitives::B256::repeat_byte(0xaa),
        "sha3Uncles": alloy_primitives::B256::ZERO,
        "miner": Address::ZERO,
        "stateRoot": alloy_primitives::B256::ZERO,
        "transactionsRoot": alloy_primitives::B256::ZERO,
        "receiptsRoot": alloy_primitives::B256::ZERO,
        "logsBloom": format!("0x{}", "00".repeat(256)),
        "difficulty": "0x0",
        "number": format!("0x{number:x}"),
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x0",
        "timestamp": "0x0",
        "extraData": "0x",
        "mixHash": alloy_primitives::B256::ZERO,
        "nonce": "0x0000000000000000",
        "baseFeePerGas": "0x1",
        "totalDifficulty": "0x0",
        "size": "0x0",
        "uncles": [],
        "transactions": [],
    })
}

fn lookup(chain: &MockChain, params: &Value) -> Option<(TxHash, Transaction)> {
    let tx_hash: TxHash = params[0].as_str()?.parse().ok()?;
    let transaction = *chain.transactions.lock().unwrap().get(&tx_hash)?;
    Some((tx_hash, transaction))
}

fn receipt(chain: &MockChain, params: &Value) -> Value {
    let Some((tx_hash, transaction)) = lookup(chain, params) else {
        return Value::Null;
    };
    json!({
        "transactionHash": tx_hash,
        "transactionIndex": "0x0",
        "blockHash": alloy_primitives::B256::repeat_byte(0xab),
        "blockNumber": "0x64",
        "from": Address::repeat_byte(0xcd),
        "to": transaction.to,
        "cumulativeGasUsed": "0x1",
        "gasUsed": "0x1",
        "effectiveGasPrice": "0x1",
        "contractAddress": Value::Null,
        "logs": [],
        "logsBloom": format!("0x{}", "00".repeat(256)),
        "type": "0x2",
        "status": if transaction.succeeded { "0x1" } else { "0x0" },
    })
}

fn transaction(chain: &MockChain, params: &Value) -> Value {
    let Some((tx_hash, transaction)) = lookup(chain, params) else {
        return Value::Null;
    };
    let input = if transaction.is_kage_swap {
        format!("0x{}", kage_swap_selector())
    } else {
        "0xdeadbeef".to_owned()
    };
    json!({
        "hash": tx_hash,
        "nonce": "0x0",
        "blockHash": alloy_primitives::B256::repeat_byte(0xab),
        "blockNumber": "0x64",
        "transactionIndex": "0x0",
        "from": Address::repeat_byte(0xcd),
        "to": transaction.to,
        "value": "0x0",
        "gas": "0x1",
        "gasPrice": "0x1",
        "input": input,
        "type": "0x0",
        "chainId": "0x7a69",
        "v": "0x1b",
        "r": "0x1",
        "s": "0x1",
    })
}
