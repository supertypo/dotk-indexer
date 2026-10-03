use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleError, RecycleResult};
use kaspa_addresses::Address;
use kaspa_consensus_core::tx::Transaction;
use kaspa_rpc_core::RpcResult;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::model::RpcDataVerbosityLevel;
use kaspa_wrpc_client::KaspaRpcClient;
use kaspa_wrpc_client::prelude::ConnectStrategy;

use super::block::{AcceptedTx, ChainBlock, VccResponse};
use super::node::{DagInfo, Kaspad, Node, TIP_DEADLINE, Unresumable, UtxoHit};

/// kaspad serves one message at a time per socket (`enable_async_handling: false`), so on one
/// shared client a self-test probe blocks every virtual-chain poll behind it.
const POOL_SIZE: usize = 8;

const CONNECT_RETRY: Duration = Duration::from_secs(5);

const CONNECT_DEADLINE: Duration = Duration::from_secs(10);

const PROBE_DEADLINE: Duration = Duration::from_mins(1);

/// A half-open socket still reports itself connected, so only an answer proves it alive.
const CHECK_DEADLINE: Duration = Duration::from_secs(3);

/// Each slot walks the candidate list on its own, so with only `resolver` the slots can land on
/// different public nodes. `Fallback`, not `Retry`, because `Retry` blocks inside one candidate
/// forever and hangs the acquire.
struct KaspadManager {
    network: String,
    candidates: Vec<String>,
    synced: AtomicBool,
}

impl Manager for KaspadManager {
    type Type = Arc<KaspaRpcClient>;
    type Error = anyhow::Error;

    async fn create(&self) -> Result<Self::Type> {
        let c = dotk_core::net::connect(&self.network, &self.candidates, ConnectStrategy::Fallback, Some(CONNECT_DEADLINE)).await?;
        if !c.is_synced {
            log::warn!("node at {} reports not synced", c.url);
        }
        log::info!("node: {}", c.url);
        Ok(Arc::new(c.client))
    }

    /// Sync state never refuses a connection, because a new one reaches the same node.
    async fn recycle(&self, conn: &mut Self::Type, _: &Metrics) -> RecycleResult<anyhow::Error> {
        let why = if conn.is_connected() {
            match tokio::time::timeout(CHECK_DEADLINE, conn.get_info()).await {
                Ok(Ok(info)) => {
                    if self.synced.swap(info.is_synced, Ordering::Relaxed) != info.is_synced {
                        if info.is_synced {
                            log::info!("node reports synced again");
                        } else {
                            log::warn!("node reports not synced");
                        }
                    }
                    return Ok(());
                }
                Ok(Err(e)) => format!("getInfo failed: {e}"),
                Err(_) => format!("no answer to getInfo within {} s", CHECK_DEADLINE.as_secs()),
            }
        } else {
            "the connection is closed".to_string()
        };
        log::warn!("dropping a kaspad connection: {why}");
        disconnect(conn.clone());
        Err(RecycleError::Message(why.into()))
    }
}

/// Dropping the client alone leaves its socket and tasks running, and a close on a dead socket
/// can hang, so nothing waits for it.
fn disconnect(client: Arc<KaspaRpcClient>) {
    tokio::spawn(async move {
        let _ = tokio::time::timeout(CHECK_DEADLINE, client.disconnect()).await;
    });
}

async fn bounded<T, F, Fut>(client: Arc<KaspaRpcClient>, deadline: Duration, what: &'static str, call: F) -> Result<T>
where
    F: FnOnce(Arc<KaspaRpcClient>) -> Fut,
    Fut: Future<Output = RpcResult<T>>,
{
    match tokio::time::timeout(deadline, call(client)).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(anyhow::Error::from(e).context(what)),
        Err(_) => Err(anyhow!("{what}: the node sent no answer within {} s", deadline.as_secs())),
    }
}

/// A request that fails or misses `deadline` takes its connection out of the pool, so the next
/// request does not queue behind it.
async fn pooled<T, F, Fut>(conn: Object<KaspadManager>, deadline: Duration, what: &'static str, call: F) -> Result<T>
where
    F: FnOnce(Arc<KaspaRpcClient>) -> Fut,
    Fut: Future<Output = RpcResult<T>>,
{
    bounded(Arc::clone(&conn), deadline, what, call).await.inspect_err(|_| disconnect(Object::take(conn)))
}

/// The node answers by the addresses that it was asked about, so an entry without an address is
/// malformed.
fn utxo_hits(entries: Vec<kaspa_rpc_core::RpcUtxosByAddressesEntry>) -> Result<Vec<UtxoHit>> {
    entries
        .into_iter()
        .map(|e| {
            let Some(address) = e.address.as_ref() else {
                anyhow::bail!("UTXO entry {} has no address", e.outpoint.transaction_id);
            };
            Ok(UtxoHit {
                address: address.to_string(),
                txid: e.outpoint.transaction_id.as_bytes(),
                index: e.outpoint.index,
                amount: e.utxo_entry.amount,
                covenant_id: e.utxo_entry.covenant_id.map(|c| c.to_string()),
                block_daa_score: e.utxo_entry.block_daa_score,
            })
        })
        .collect()
}

/// The node reports an unknown start hash only in its message text, under the call's context.
fn unresumable(e: anyhow::Error) -> anyhow::Error {
    let m = format!("{e:#}");
    if m.contains("cannot find header") || m.contains("retention root") { Unresumable(e).into() } else { e }
}

fn chain_block(acd: &kaspa_rpc_core::RpcChainBlockAcceptedTransactions) -> Result<ChainBlock> {
    let h = &acd.chain_block_header;
    let (Some(hash), Some(blue_score), Some(daa_score), Some(timestamp)) = (h.hash, h.blue_score, h.daa_score, h.timestamp) else {
        anyhow::bail!("chain block header missing fields at High verbosity");
    };
    let txs = acd.accepted_transactions.iter().map(accepted_tx).collect::<Result<_>>()?;
    Ok(ChainBlock { hash: hash.as_bytes(), blue_score, daa_score, timestamp, txs })
}

fn accepted_tx(tx: &kaspa_rpc_core::RpcOptionalTransaction) -> Result<AcceptedTx> {
    let Some(txid) = tx.verbose_data.as_ref().and_then(|v| v.transaction_id) else {
        anyhow::bail!("accepted transaction missing its id at High verbosity");
    };
    let mut spent = Vec::with_capacity(tx.inputs.len());
    for input in &tx.inputs {
        let (Some(prev_txid), Some(prev_index)) =
            input.previous_outpoint.as_ref().map_or((None, None), |o| (o.transaction_id, o.index))
        else {
            anyhow::bail!("accepted transaction input missing its previous outpoint at High verbosity");
        };
        spent.push((prev_txid.as_bytes(), prev_index));
    }
    let mut outputs = Vec::with_capacity(tx.outputs.len());
    for output in &tx.outputs {
        let (Some(value), Some(spk)) = (output.value, output.script_public_key.as_ref()) else {
            anyhow::bail!("accepted transaction output missing its value or script at High verbosity");
        };
        outputs.push((value, spk.script().to_vec()));
    }
    let Some(payload) = tx.payload.clone() else {
        anyhow::bail!("accepted transaction {txid} has no payload at High verbosity");
    };
    let mut sig_scripts = Vec::with_capacity(tx.inputs.len());
    for input in &tx.inputs {
        let Some(sig_script) = input.signature_script.clone() else {
            anyhow::bail!("accepted transaction {txid} has an input without a signature script at High verbosity");
        };
        sig_scripts.push(sig_script);
    }
    Ok(AcceptedTx {
        txid: txid.as_bytes(),
        sig_scripts,
        spent,
        outputs,
        payload,
        lineage: tx.outputs.first().and_then(|o| o.covenant.as_ref()).and_then(|c| c.0.as_ref()).map(|b| b.0.covenant_id.to_string()),
    })
}

pub struct PooledKaspad {
    pool: Pool<KaspadManager>,
}

impl PooledKaspad {
    pub async fn connect(network: &str, candidates: &[String]) -> Result<Self> {
        let manager = KaspadManager { network: network.to_string(), candidates: candidates.to_vec(), synced: AtomicBool::new(true) };
        let pool = Pool::builder(manager).max_size(POOL_SIZE).build()?;
        loop {
            let e = match pool.get().await {
                Ok(_) => return Ok(Self { pool }),
                Err(deadpool::managed::PoolError::Backend(e)) => e,
                Err(e) => anyhow!("{e}"),
            };
            log::warn!("no kaspad reachable ({e:#}), retrying in {} s", CONNECT_RETRY.as_secs());
            tokio::time::sleep(CONNECT_RETRY).await;
        }
    }

    async fn client(&self) -> Result<Object<KaspadManager>> {
        self.pool.get().await.map_err(|e| match e {
            deadpool::managed::PoolError::Backend(e) => e.context("acquiring a kaspad connection"),
            e => anyhow!("acquiring a kaspad connection: {e}"),
        })
    }
}

#[async_trait]
impl Kaspad for PooledKaspad {
    async fn virtual_chain(&self, start: [u8; 32], min_confirmations: u64, deadline: Duration) -> Result<VccResponse> {
        // High verbosity carries the payload, where a card is published, and each input's
        // previous outpoint, which names the card a sweep spends.
        let conn = self.client().await?;
        let res = bounded(Arc::clone(&conn), deadline, "getVirtualChainFromBlockV2", |client| async move {
            client
                .get_virtual_chain_from_block_v2(
                    kaspa_rpc_core::RpcHash::from_bytes(start),
                    Some(RpcDataVerbosityLevel::High),
                    Some(min_confirmations),
                )
                .await
        })
        .await;
        // A malformed answer drops its connection like a failed call, so the retry can reach
        // another node.
        let converted = res.map_err(unresumable).and_then(|res| {
            anyhow::ensure!(
                res.added_chain_block_hashes.len() == res.chain_block_accepted_transactions.len(),
                "v2 response misaligned: {} added hashes, {} acceptance entries",
                res.added_chain_block_hashes.len(),
                res.chain_block_accepted_transactions.len()
            );
            let added = res.chain_block_accepted_transactions.iter().map(chain_block).collect::<Result<_>>()?;
            Ok(VccResponse { removed: res.removed_chain_block_hashes.iter().map(|h| h.as_bytes()).collect(), added })
        });
        if converted.is_err() {
            disconnect(Object::take(conn));
        }
        converted
    }

    async fn utxos_by_addresses(&self, addrs: &[Address]) -> Result<Vec<UtxoHit>> {
        let addrs = addrs.to_vec();
        let conn = self.client().await?;
        let hits = bounded(Arc::clone(&conn), PROBE_DEADLINE, "getUtxosByAddresses", |client| async move {
            client.get_utxos_by_addresses(addrs).await
        })
        .await
        .and_then(utxo_hits);
        if hits.is_err() {
            disconnect(Object::take(conn));
        }
        hits
    }

    async fn dag_info(&self) -> Result<DagInfo> {
        let info =
            pooled(self.client().await?, TIP_DEADLINE, "getBlockDagInfo", |client| async move { client.get_block_dag_info().await })
                .await?;
        let parent = info.virtual_parent_hashes.first().ok_or_else(|| anyhow!("node reports no virtual parents"))?;
        Ok(DagInfo { virtual_daa: info.virtual_daa_score, virtual_parent: parent.as_bytes() })
    }

    async fn sink_blue_score(&self) -> Result<u64> {
        pooled(self.client().await?, TIP_DEADLINE, "getSinkBlueScore", |client| async move { client.get_sink_blue_score().await })
            .await
    }

    async fn market(&self) -> dotk_core::fees::Market {
        match self.client().await {
            Ok(client) => dotk_core::net::market(&client).await,
            Err(_) => dotk_core::fees::Market::default(),
        }
    }

    /// A failed submit keeps its connection, because a rejection is an answer.
    async fn submit(&self, tx: &Transaction) -> Result<String> {
        let tx = kaspa_rpc_core::RpcTransaction::from(tx);
        let conn = self.client().await?;
        let id = bounded(Arc::clone(&conn), TIP_DEADLINE, "submitTransaction", |client| async move {
            client.submit_transaction(tx, false).await
        })
        .await?;
        Ok(id.to_string())
    }

    async fn pin<'a>(&'a self) -> Result<Box<dyn Node + 'a>> {
        Ok(Box::new(Pinned { conn: Mutex::new(Some(self.client().await?)) }))
    }
}

struct Pinned {
    conn: Mutex<Option<Object<KaspadManager>>>,
}

impl Pinned {
    /// A poisoned lock still holds a whole connection or none.
    fn slot(&self) -> std::sync::MutexGuard<'_, Option<Object<KaspadManager>>> {
        self.conn.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn client(&self) -> Result<Arc<KaspaRpcClient>> {
        self.slot().as_ref().map(|c| Arc::clone(c)).ok_or_else(|| anyhow!("the pinned kaspad connection was discarded"))
    }

    async fn call<T, F, Fut>(&self, deadline: Duration, what: &'static str, call: F) -> Result<T>
    where
        F: FnOnce(Arc<KaspaRpcClient>) -> Fut,
        Fut: Future<Output = RpcResult<T>>,
    {
        bounded(self.client()?, deadline, what, call).await.inspect_err(|_| self.discard())
    }
}

#[async_trait]
impl Node for Pinned {
    async fn utxos_by_addresses(&self, addrs: &[Address]) -> Result<Vec<UtxoHit>> {
        let addrs = addrs.to_vec();
        let hits =
            self.call(PROBE_DEADLINE, "getUtxosByAddresses", |client| async move { client.get_utxos_by_addresses(addrs).await });
        utxo_hits(hits.await?)
    }

    async fn sink_blue_score(&self) -> Result<u64> {
        self.call(TIP_DEADLINE, "getSinkBlueScore", |client| async move { client.get_sink_blue_score().await }).await
    }

    fn discard(&self) {
        if let Some(conn) = self.slot().take() {
            disconnect(Object::take(conn));
        }
    }
}

#[cfg(test)]
#[path = "kaspad_tests.rs"]
mod tests;
