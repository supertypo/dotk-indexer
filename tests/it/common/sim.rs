use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use kaspa_addresses::{Address, Prefix, Version as AddrVersion};
use kaspa_consensus_core::tx::Transaction;

use dotk_indexer::chain::{AcceptedTx, ChainBlock, VccResponse};
use dotk_indexer::chain::{DagInfo, Kaspad, Node, Unresumable, UtxoHit};

#[derive(Clone)]
pub(crate) struct SimOut {
    pub value: u64,
    pub spk: Vec<u8>,
    pub covenant_id: Option<String>,
}

#[derive(Clone)]
pub(crate) struct SimTx {
    pub txid: [u8; 32],
    pub sig_scripts: Vec<Vec<u8>>,
    /// (txid, index, sequence) of each spent outpoint.
    pub inputs: Vec<([u8; 32], u32, u64)>,
    pub outputs: Vec<SimOut>,
    pub payload: Vec<u8>,
}

impl SimTx {
    pub(crate) fn accepted(&self) -> AcceptedTx {
        AcceptedTx {
            txid: self.txid,
            sig_scripts: self.sig_scripts.clone(),
            spent: self.inputs.iter().map(|(t, i, _)| (*t, *i)).collect(),
            outputs: self.outputs.iter().map(|o| (o.value, o.spk.clone())).collect(),
            payload: self.payload.clone(),
            lineage: self.outputs.first().and_then(|o| o.covenant_id.clone()),
        }
    }
}

#[derive(Clone)]
struct SimBlock {
    parent: [u8; 32],
    blue: u64,
    daa: u64,
    timestamp: u64,
    txs: Vec<SimTx>,
}

#[derive(Clone)]
struct SimUtxo {
    value: u64,
    spk: Vec<u8>,
    covenant_id: Option<String>,
    daa: u64,
}

type UtxoHook = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

struct SimState {
    blocks: HashMap<[u8; 32], SimBlock>,
    canonical: Vec<[u8; 32]>,
    utxos: HashMap<([u8; 32], u32), SimUtxo>,
    mempool: Vec<SimTx>,
    report_covenant_ids: bool,
    virtual_daa: u64,
    next_hash: u64,
    prefix: Prefix,
    submitted: Vec<[u8; 32]>,
    /// A tick that declines submits nothing, so this shows what it reached.
    queried: HashSet<String>,
    utxo_calls: usize,
    fail_utxos_after: Option<usize>,
    answer_empty: bool,
    lagging_polls: usize,
    failing_polls: usize,
    /// The deadline of every poll, and whether that poll failed.
    polls: Vec<(Duration, bool)>,
    utxo_hook: Option<(usize, UtxoHook)>,
    addr_hook: Option<(String, UtxoHook)>,
    feerate: f64,
    probe_sink_lag: u64,
    pins: usize,
}

pub(crate) struct SimKaspad {
    state: Mutex<SimState>,
}

fn now_ms() -> u64 {
    u64::try_from(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()).unwrap()
}

fn spk_address(prefix: Prefix, spk: &[u8]) -> Option<Address> {
    match spk {
        [0xaa, 0x20, hash @ .., 0x87] if hash.len() == 32 => Some(Address::new(prefix, AddrVersion::ScriptHash, hash)),
        [0x20, key @ .., 0xac] if key.len() == 32 => Some(Address::new(prefix, AddrVersion::PubKey, key)),
        _ => None,
    }
}

impl SimKaspad {
    pub(crate) fn new(prefix: Prefix, genesis_txs: Vec<SimTx>) -> Self {
        let mut st = SimState {
            blocks: HashMap::new(),
            canonical: Vec::new(),
            utxos: HashMap::new(),
            mempool: Vec::new(),
            report_covenant_ids: true,
            virtual_daa: 100,
            next_hash: 1,
            prefix,
            submitted: Vec::new(),
            queried: HashSet::new(),
            utxo_calls: 0,
            fail_utxos_after: None,
            answer_empty: false,
            lagging_polls: 0,
            failing_polls: 0,
            polls: Vec::new(),
            utxo_hook: None,
            addr_hook: None,
            feerate: 1.0,
            probe_sink_lag: 0,
            pins: 0,
        };
        Self::append(&mut st, genesis_txs);
        Self { state: Mutex::new(st) }
    }

    fn gen_hash(st: &mut SimState) -> [u8; 32] {
        let mut h = [0xB1u8; 32];
        h[..8].copy_from_slice(&st.next_hash.to_le_bytes());
        st.next_hash += 1;
        h
    }

    fn apply_tx(utxos: &mut HashMap<([u8; 32], u32), SimUtxo>, tx: &SimTx, daa: u64) {
        for (txid, index, _) in &tx.inputs {
            utxos.remove(&(*txid, *index));
        }
        for (i, out) in tx.outputs.iter().enumerate() {
            utxos.insert(
                (tx.txid, u32::try_from(i).unwrap()),
                SimUtxo { value: out.value, spk: out.spk.clone(), covenant_id: out.covenant_id.clone(), daa },
            );
        }
    }

    fn append(st: &mut SimState, mut txs: Vec<SimTx>) -> [u8; 32] {
        txs.append(&mut st.mempool);
        st.virtual_daa += 1;
        let parent = st.canonical.last().copied().unwrap_or([0u8; 32]);
        let blue = st.canonical.last().map_or(1, |h| st.blocks[h].blue + 1);
        let daa = st.virtual_daa;
        for tx in &txs {
            Self::apply_tx(&mut st.utxos, tx, daa);
        }
        let hash = Self::gen_hash(st);
        st.blocks.insert(hash, SimBlock { parent, blue, daa, timestamp: now_ms(), txs });
        st.canonical.push(hash);
        hash
    }

    /// The block also mines the mempool.
    pub(crate) fn add_block(&self, txs: Vec<SimTx>) -> [u8; 32] {
        Self::append(&mut self.state.lock().unwrap(), txs)
    }

    pub(crate) fn chain_hashes(&self) -> Vec<[u8; 32]> {
        self.state.lock().unwrap().canonical.clone()
    }

    pub(crate) fn pad(&self, n: usize) {
        for _ in 0..n {
            self.add_block(vec![]);
        }
    }

    /// Detached blocks stay known as a dead branch. Replacement blue scores continue above the old tip.
    pub(crate) fn reorg(&self, depth: usize, replacement: Vec<Vec<SimTx>>) {
        let mut st = self.state.lock().unwrap();
        assert!(depth < st.canonical.len(), "cannot reorg out the genesis block");
        let old_tip_blue = st.blocks[st.canonical.last().unwrap()].blue;
        for _ in 0..depth {
            st.canonical.pop();
        }
        st.utxos.clear();
        for h in st.canonical.clone() {
            let b = st.blocks[&h].clone();
            for tx in &b.txs {
                Self::apply_tx(&mut st.utxos, tx, b.daa);
            }
        }
        let mut blue = old_tip_blue;
        for txs in replacement {
            let hash = Self::append(&mut st, txs);
            blue += 1;
            st.blocks.get_mut(&hash).unwrap().blue = blue;
        }
    }

    /// With `false`, every registry UTXO looks foreign to the covenant id filter.
    pub(crate) fn set_report_covenant_ids(&self, on: bool) {
        self.state.lock().unwrap().report_covenant_ids = on;
    }

    pub(crate) fn advance_daa(&self, n: u64) {
        self.state.lock().unwrap().virtual_daa += n;
    }

    /// Resets what `was_queried`, `utxo_calls` and the call hooks count.
    pub(crate) fn clear_queried(&self) {
        let mut st = self.state.lock().unwrap();
        st.queried.clear();
        st.utxo_calls = 0;
    }

    /// Runs `hook` once, after call `n` since the last [`SimKaspad::clear_queried`] has its answer
    /// and before the caller receives it.
    pub(crate) fn after_utxo_calls<F, Fut>(&self, n: usize, hook: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.state.lock().unwrap().utxo_hook = Some((n, Box::new(move || Box::pin(hook()))));
    }

    /// Runs `hook` once, after the next call that asks for `addr` has its answer and before the
    /// caller receives it.
    pub(crate) fn after_query_of<F, Fut>(&self, addr: &Address, hook: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.state.lock().unwrap().addr_hook = Some((addr.to_string(), Box::new(move || Box::pin(hook()))));
    }

    /// In sompi per gram.
    pub(crate) fn set_feerate(&self, feerate: f64) {
        self.state.lock().unwrap().feerate = feerate;
    }

    pub(crate) fn lag_polls(&self, n: usize) {
        self.state.lock().unwrap().lagging_polls = n;
    }

    /// The next `n` polls fail as a node that does not answer in time.
    pub(crate) fn fail_polls(&self, n: usize) {
        self.state.lock().unwrap().failing_polls = n;
    }

    pub(crate) fn polls(&self) -> Vec<(Duration, bool)> {
        self.state.lock().unwrap().polls.clone()
    }

    pub(crate) fn set_answer_empty(&self, on: bool) {
        self.state.lock().unwrap().answer_empty = on;
    }

    /// A pinned node then reports its sink `n` blue scores behind the chain.
    pub(crate) fn set_probe_sink_lag(&self, n: u64) {
        self.state.lock().unwrap().probe_sink_lag = n;
    }

    pub(crate) fn pin_calls(&self) -> usize {
        self.state.lock().unwrap().pins
    }

    /// Every UTXO call after the first `n` fails, until `heal_utxos`.
    pub(crate) fn fail_utxos_after(&self, n: usize) {
        self.state.lock().unwrap().fail_utxos_after = Some(n);
    }

    pub(crate) fn heal_utxos(&self) {
        self.state.lock().unwrap().fail_utxos_after = None;
    }

    pub(crate) fn clear_hooks(&self) {
        let mut st = self.state.lock().unwrap();
        st.utxo_hook = None;
        st.addr_hook = None;
    }

    pub(crate) fn utxo_calls(&self) -> usize {
        self.state.lock().unwrap().utxo_calls
    }

    pub(crate) fn was_queried(&self, addr: &Address) -> bool {
        self.state.lock().unwrap().queried.contains(&addr.to_string())
    }

    pub(crate) fn submitted_count(&self) -> usize {
        self.state.lock().unwrap().submitted.len()
    }

    pub(crate) fn submitted_sig_scripts(&self) -> Vec<Vec<Vec<u8>>> {
        let st = self.state.lock().unwrap();
        st.submitted
            .iter()
            .filter_map(|txid| {
                st.mempool
                    .iter()
                    .chain(st.canonical.iter().flat_map(|h| st.blocks[h].txs.iter()))
                    .find(|t| &t.txid == txid)
                    .map(|t| t.sig_scripts.clone())
            })
            .collect()
    }
}

impl SimState {
    /// The node's answer from a known `start`.
    fn chain_from(&self, start: [u8; 32], min_confirmations: u64) -> VccResponse {
        // Every block from `start` down to the fork is removed, high to low.
        let canonical_idx: HashMap<[u8; 32], usize> = self.canonical.iter().enumerate().map(|(i, h)| (*h, i)).collect();
        let mut removed = Vec::new();
        let mut cursor = start;
        let fork_idx = loop {
            if let Some(i) = canonical_idx.get(&cursor) {
                break *i;
            }
            removed.push(cursor);
            cursor = self.blocks[&cursor].parent;
        };
        let sink_blue_score = self.blocks[self.canonical.last().unwrap()].blue;
        let mut added: Vec<ChainBlock> = self.canonical[fork_idx + 1..]
            .iter()
            .map(|h| {
                let b = &self.blocks[h];
                ChainBlock {
                    hash: *h,
                    blue_score: b.blue,
                    daa_score: b.daa,
                    timestamp: b.timestamp,
                    txs: b.txs.iter().map(SimTx::accepted).collect(),
                }
            })
            .collect();
        // `min_confirmations` trims added only, by blue distance from the sink, and 0 disables it.
        if min_confirmations > 0 {
            while let Some(last) = added.last() {
                if sink_blue_score.saturating_sub(last.blue_score) > min_confirmations {
                    break;
                }
                added.pop();
            }
        }
        VccResponse { removed, added }
    }
}

#[async_trait]
impl Kaspad for SimKaspad {
    async fn virtual_chain(&self, start: [u8; 32], min_confirmations: u64, deadline: Duration) -> Result<VccResponse> {
        let mut st = self.state.lock().unwrap();
        let failing = st.failing_polls > 0;
        st.polls.push((deadline, failing));
        if failing {
            st.failing_polls -= 1;
            return Err(anyhow!("the node sent no answer within {} s", deadline.as_secs()));
        }
        if st.lagging_polls > 0 {
            st.lagging_polls -= 1;
            return Err(Unresumable(anyhow!("cannot find header {}", faster_hex::hex_string(&start))).into());
        }
        if !st.blocks.contains_key(&start) {
            return Err(Unresumable(anyhow!("cannot find header {}", faster_hex::hex_string(&start))).into());
        }
        Ok(st.chain_from(start, min_confirmations))
    }

    async fn utxos_by_addresses(&self, addrs: &[Address]) -> Result<Vec<UtxoHit>> {
        let (hits, hook, addr_hook) = {
            let mut st = self.state.lock().unwrap();
            st.utxo_calls += 1;
            st.queried.extend(addrs.iter().map(ToString::to_string));
            if st.fail_utxos_after.is_some_and(|n| st.utxo_calls > n) {
                anyhow::bail!("simulated node failure answering utxos_by_addresses call {}", st.utxo_calls);
            }
            if st.answer_empty {
                return Ok(vec![]);
            }
            let wanted: HashSet<String> = addrs.iter().map(ToString::to_string).collect();
            let hits: Vec<UtxoHit> = st
                .utxos
                .iter()
                .filter_map(|((txid, index), u)| {
                    let addr = spk_address(st.prefix, &u.spk)?.to_string();
                    wanted.contains(&addr).then(|| UtxoHit {
                        address: addr,
                        txid: *txid,
                        index: *index,
                        amount: u.value,
                        covenant_id: st.report_covenant_ids.then(|| u.covenant_id.clone()).flatten(),
                        block_daa_score: u.daa,
                    })
                })
                .collect();
            let calls = st.utxo_calls;
            let hook = if st.utxo_hook.as_ref().is_some_and(|(n, _)| *n == calls) { st.utxo_hook.take() } else { None };
            let addr_hook = if st.addr_hook.as_ref().is_some_and(|(a, _)| wanted.contains(a)) { st.addr_hook.take() } else { None };
            (hits, hook, addr_hook)
        };
        if let Some((_, hook)) = hook {
            hook().await;
        }
        if let Some((_, hook)) = addr_hook {
            hook().await;
        }
        Ok(hits)
    }

    async fn dag_info(&self) -> Result<DagInfo> {
        let st = self.state.lock().unwrap();
        Ok(DagInfo { virtual_daa: st.virtual_daa, virtual_parent: *st.canonical.last().unwrap() })
    }

    async fn sink_blue_score(&self) -> Result<u64> {
        let st = self.state.lock().unwrap();
        Ok(st.blocks[st.canonical.last().unwrap()].blue)
    }

    async fn market(&self) -> dotk_core::fees::Market {
        self.state.lock().unwrap().feerate.into()
    }

    async fn pin<'a>(&'a self) -> Result<Box<dyn Node + 'a>> {
        self.state.lock().unwrap().pins += 1;
        Ok(Box::new(SimNode(self)))
    }

    async fn submit(&self, tx: &Transaction) -> Result<String> {
        let mut st = self.state.lock().unwrap();
        let mut inputs = Vec::new();
        for input in &tx.inputs {
            let key = (input.previous_outpoint.transaction_id.as_bytes(), input.previous_outpoint.index);
            let Some(u) = st.utxos.get(&key) else {
                return Err(anyhow!("orphan: missing outpoint {}", input.previous_outpoint));
            };
            if input.sequence > 0 && u.daa + input.sequence > st.virtual_daa {
                return Err(anyhow!("immature: sequence lock not satisfied"));
            }
            inputs.push((key.0, key.1, input.sequence));
        }
        let sim_tx = SimTx {
            txid: tx.id().as_bytes(),
            sig_scripts: tx.inputs.iter().map(|i| i.signature_script.clone()).collect(),
            inputs,
            outputs: tx
                .outputs
                .iter()
                .map(|o| SimOut {
                    value: o.value,
                    spk: o.script_public_key.script().to_vec(),
                    covenant_id: o.covenant.as_ref().map(|c| c.covenant_id.to_string()),
                })
                .collect(),
            payload: tx.payload.clone(),
        };
        let txid = faster_hex::hex_string(&sim_tx.txid);
        // The mempool takes the outpoints at once, and the next block mines the transaction.
        for (t, i, _) in &sim_tx.inputs {
            st.utxos.remove(&(*t, *i));
        }
        st.submitted.push(sim_tx.txid);
        st.mempool.push(sim_tx);
        Ok(txid)
    }
}

struct SimNode<'a>(&'a SimKaspad);

#[async_trait]
impl Node for SimNode<'_> {
    async fn utxos_by_addresses(&self, addrs: &[Address]) -> Result<Vec<UtxoHit>> {
        Kaspad::utxos_by_addresses(self.0, addrs).await
    }

    async fn sink_blue_score(&self) -> Result<u64> {
        let lag = self.0.state.lock().unwrap().probe_sink_lag;
        Ok(Kaspad::sink_blue_score(self.0).await?.saturating_sub(lag))
    }
}
