use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use kaspa_addresses::Address;
use kaspa_consensus_core::tx::Transaction;

use super::block::VccResponse;

/// A stalled node is noticed after this, not after the wRPC client's own 60 s.
pub const TIP_DEADLINE: Duration = Duration::from_secs(10);

/// A catch-up response carries hundreds of blocks at High verbosity. The wRPC client cuts any
/// request at 60 s of its own, so no wider deadline takes effect.
pub const CATCHUP_DEADLINE: Duration = Duration::from_mins(1);

#[derive(Debug, Clone)]
pub struct UtxoHit {
    pub address: String,
    pub txid: [u8; 32],
    pub index: u32,
    pub amount: u64,
    pub covenant_id: Option<String>,
    pub block_daa_score: u64,
}

#[derive(Debug, Clone)]
pub struct DagInfo {
    pub virtual_daa: u64,
    pub virtual_parent: [u8; 32],
}

/// The start hash is unknown or outside retention, so the checkpoint cannot be resumed. It
/// displays as the node's own error.
#[derive(Debug)]
pub struct Unresumable(pub anyhow::Error);

impl std::fmt::Display for Unresumable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

impl std::error::Error for Unresumable {}

#[async_trait]
pub trait Kaspad: Send + Sync {
    /// Fails with [`Unresumable`] when the node does not know `start`.
    async fn virtual_chain(&self, start: [u8; 32], min_confirmations: u64, deadline: Duration) -> Result<VccResponse>;
    async fn utxos_by_addresses(&self, addrs: &[Address]) -> Result<Vec<UtxoHit>>;
    async fn dag_info(&self) -> Result<DagInfo>;
    async fn sink_blue_score(&self) -> Result<u64>;
    /// The fee market that `dotk_core::fees` prices in. A node that cannot answer yields a zero
    /// feerate, so the fee is the relay floor.
    async fn market(&self) -> dotk_core::fees::Market;
    async fn submit(&self, tx: &Transaction) -> Result<String>;
    /// One connection, held until dropped, so every answer comes from the same node.
    async fn pin<'a>(&'a self) -> Result<Box<dyn Node + 'a>>;
}

/// Two nodes answer for two chain states, and a repair that mixes them writes the older one.
#[async_trait]
pub trait Node: Send + Sync {
    async fn utxos_by_addresses(&self, addrs: &[Address]) -> Result<Vec<UtxoHit>>;
    async fn sink_blue_score(&self) -> Result<u64>;
    /// Leaves the pool, so the next pin can reach another node.
    fn discard(&self) {}
}
