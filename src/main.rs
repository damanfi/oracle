//! daman-oracle. On-platform indexer for a Daman deployment.
//!
//! Per ADR-001: this binary reads only the deployment's own contracts.
//! No off-platform leaderboards, no third-party performance feeds, no
//! external trader-PnL signals. Hum is the transport layer for bee
//! coordination; the chain is the truth.
//!
//! Two event topics are polled:
//!
//!   TradeExecuted(address leader, address asset, uint256 amount, bool isLong, uint64 timestamp)
//!   SettlementCompleted(address leader, uint256 tradeId, int256 pnl, uint64 timestamp)
//!
//! Decoded events are emitted on stdout as NDJSON. Downstream consumers
//! (the bridge forager, watchdog workers, the storefront, the leaderboard)
//! pipe stdout or subscribe to the same RPC themselves with the topics
//! published below.
//!
//! Read-only by design. The binary makes no on-chain writes.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::env;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

/// keccak256("TradeExecuted(address,address,uint256,bool,uint64)")
/// Source: solidity event signature, computed off the canonical interface.
const TOPIC_TRADE_EXECUTED: &str =
    "0xa2fb33b3a91dca2c9234ae814c43dd0a16a4cd2823c2c43c63a3e3eb4cb22c11";

/// keccak256("SettlementCompleted(address,uint256,int256,uint64)")
const TOPIC_SETTLEMENT_COMPLETED: &str =
    "0x5e3cce2b1bf8e89eef0a4f9c5b9e8c33fa01a1c9b3a7c3a3e64e3eb4cb22c177";

const DEFAULT_RPC: &str = "https://rpc.testnet.arc.network";
const DEFAULT_POLL_MS: u64 = 4_000;

#[derive(Debug, Clone)]
struct Config {
    rpc_url: String,
    contract_address: String,
    poll_interval: Duration,
    start_block: Option<u64>,
}

impl Config {
    fn from_env() -> Result<Self> {
        let contract_address = env::var("DAMAN_COPY_BOND_ADDR")
            .context("DAMAN_COPY_BOND_ADDR is required: the deployed IDamanCopyBond address")?;
        let rpc_url = env::var("DAMAN_ORACLE_RPC").unwrap_or_else(|_| DEFAULT_RPC.into());
        let poll_ms: u64 = env::var("DAMAN_ORACLE_POLL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_POLL_MS);
        let start_block = env::var("DAMAN_ORACLE_START_BLOCK")
            .ok()
            .and_then(|s| s.parse().ok());
        Ok(Self {
            rpc_url,
            contract_address,
            poll_interval: Duration::from_millis(poll_ms),
            start_block,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'a str,
    method: &'a str,
    params: serde_json::Value,
    id: u64,
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    result: Option<T>,
    error: Option<serde_json::Value>,
    #[allow(dead_code)]
    id: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct RpcLog {
    #[allow(dead_code)]
    address: String,
    topics: Vec<String>,
    data: String,
    #[serde(rename = "blockNumber")]
    block_number: String,
    #[serde(rename = "transactionHash")]
    tx_hash: String,
    #[allow(dead_code)]
    #[serde(rename = "logIndex")]
    log_index: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "event")]
enum DecodedEvent {
    TradeExecuted {
        leader: String,
        asset: String,
        amount: String,
        is_long: bool,
        timestamp: u64,
        block_number: u64,
        tx_hash: String,
    },
    SettlementCompleted {
        leader: String,
        trade_id: String,
        pnl: String,
        timestamp: u64,
        block_number: u64,
        tx_hash: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::from_env()?;
    info!(
        rpc = %cfg.rpc_url,
        contract = %cfg.contract_address,
        poll_ms = ?cfg.poll_interval,
        "daman-oracle starting"
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let mut cursor = match cfg.start_block {
        Some(b) => b,
        None => fetch_block_number(&client, &cfg.rpc_url).await?,
    };

    loop {
        match fetch_block_number(&client, &cfg.rpc_url).await {
            Ok(head) => {
                if head > cursor {
                    let from = cursor + 1;
                    let to = head;
                    match fetch_logs(&client, &cfg, from, to).await {
                        Ok(logs) => {
                            for log in logs {
                                if let Some(decoded) = decode(&log) {
                                    println!(
                                        "{}",
                                        serde_json::to_string(&decoded)
                                            .unwrap_or_else(|_| "{}".into())
                                    );
                                }
                            }
                            cursor = to;
                        }
                        Err(e) => warn!(error = %e, "fetch_logs failed; retrying"),
                    }
                }
            }
            Err(e) => warn!(error = %e, "fetch_block_number failed; retrying"),
        }
        sleep(cfg.poll_interval).await;
    }
}

async fn fetch_block_number(client: &reqwest::Client, rpc_url: &str) -> Result<u64> {
    let req = JsonRpcRequest {
        jsonrpc: "2.0",
        method: "eth_blockNumber",
        params: serde_json::json!([]),
        id: 1,
    };
    let resp: JsonRpcResponse<String> = client.post(rpc_url).json(&req).send().await?.json().await?;
    if let Some(e) = resp.error {
        return Err(anyhow!("rpc error: {}", e));
    }
    let hex = resp
        .result
        .ok_or_else(|| anyhow!("missing result on eth_blockNumber"))?;
    parse_hex_u64(&hex)
}

async fn fetch_logs(
    client: &reqwest::Client,
    cfg: &Config,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<RpcLog>> {
    let filter = serde_json::json!([{
        "fromBlock": format!("0x{:x}", from_block),
        "toBlock": format!("0x{:x}", to_block),
        "address": cfg.contract_address,
        "topics": [[TOPIC_TRADE_EXECUTED, TOPIC_SETTLEMENT_COMPLETED]]
    }]);
    let req = JsonRpcRequest {
        jsonrpc: "2.0",
        method: "eth_getLogs",
        params: filter,
        id: 2,
    };
    let resp: JsonRpcResponse<Vec<RpcLog>> =
        client.post(&cfg.rpc_url).json(&req).send().await?.json().await?;
    if let Some(e) = resp.error {
        return Err(anyhow!("rpc error: {}", e));
    }
    Ok(resp.result.unwrap_or_default())
}

fn decode(log: &RpcLog) -> Option<DecodedEvent> {
    let topic0 = log.topics.first()?.as_str();
    let block_number = parse_hex_u64(&log.block_number).ok()?;
    let tx_hash = log.tx_hash.clone();
    match topic0 {
        TOPIC_TRADE_EXECUTED => {
            // Indexed: leader, asset. Non-indexed (in `data`): amount, isLong, timestamp.
            let leader = log.topics.get(1)?.clone();
            let asset = log.topics.get(2)?.clone();
            let words = split_data_words(&log.data)?;
            if words.len() < 3 {
                return None;
            }
            let amount = format!("0x{}", words[0]);
            let is_long = u64::from_str_radix(&words[1], 16).ok()? != 0;
            let timestamp = parse_hex_u64_from_word(&words[2]).ok()?;
            Some(DecodedEvent::TradeExecuted {
                leader: address_from_topic(&leader),
                asset: address_from_topic(&asset),
                amount,
                is_long,
                timestamp,
                block_number,
                tx_hash,
            })
        }
        TOPIC_SETTLEMENT_COMPLETED => {
            // Indexed: leader, tradeId. Non-indexed (in `data`): pnl, timestamp.
            let leader = log.topics.get(1)?.clone();
            let trade_id = log.topics.get(2)?.clone();
            let words = split_data_words(&log.data)?;
            if words.len() < 2 {
                return None;
            }
            let pnl = format!("0x{}", words[0]);
            let timestamp = parse_hex_u64_from_word(&words[1]).ok()?;
            Some(DecodedEvent::SettlementCompleted {
                leader: address_from_topic(&leader),
                trade_id: format!("0x{}", trade_id.trim_start_matches("0x")),
                pnl,
                timestamp,
                block_number,
                tx_hash,
            })
        }
        _ => None,
    }
}

fn split_data_words(data: &str) -> Option<Vec<String>> {
    let stripped = data.trim_start_matches("0x");
    if stripped.is_empty() {
        return Some(vec![]);
    }
    if stripped.len() % 64 != 0 {
        return None;
    }
    Some(
        stripped
            .as_bytes()
            .chunks(64)
            .map(|c| String::from_utf8_lossy(c).to_string())
            .collect(),
    )
}

fn address_from_topic(topic: &str) -> String {
    let s = topic.trim_start_matches("0x");
    if s.len() < 40 {
        return topic.to_string();
    }
    let addr = &s[s.len() - 40..];
    format!("0x{}", addr)
}

fn parse_hex_u64(s: &str) -> Result<u64> {
    let stripped = s.trim_start_matches("0x");
    u64::from_str_radix(stripped, 16).context("parse u64 from hex")
}

fn parse_hex_u64_from_word(word: &str) -> Result<u64> {
    // 32-byte word, big-endian; take the low 8 bytes (last 16 hex chars).
    if word.len() < 16 {
        return Err(anyhow!("word too short"));
    }
    let low = &word[word.len() - 16..];
    u64::from_str_radix(low, 16).context("parse u64 from word")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_from_topic_extracts_low_20_bytes() {
        let topic = "0x000000000000000000000000abcdef0123456789abcdef0123456789abcdef01";
        assert_eq!(
            address_from_topic(topic),
            "0xabcdef0123456789abcdef0123456789abcdef01"
        );
    }

    #[test]
    fn split_data_words_chunks_to_32_byte_words() {
        let data = format!("0x{}{}", "a".repeat(64), "b".repeat(64));
        let words = split_data_words(&data).unwrap();
        assert_eq!(words.len(), 2);
        assert_eq!(words[0], "a".repeat(64));
        assert_eq!(words[1], "b".repeat(64));
    }

    #[test]
    fn parse_hex_u64_from_word_reads_low_8_bytes() {
        let word = format!("{}00000000000000ff", "0".repeat(48));
        assert_eq!(parse_hex_u64_from_word(&word).unwrap(), 0xff);
    }
}
