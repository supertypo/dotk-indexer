#[derive(Debug, Clone, Default)]
pub struct AcceptedTx {
    pub txid: [u8; 32],
    pub sig_scripts: Vec<Vec<u8>>,
    /// Index-aligned with `sig_scripts`.
    pub spent: Vec<([u8; 32], u32)>,
    /// `(value, script public key)` of every output, in order.
    pub outputs: Vec<(u64, Vec<u8>)>,
    pub payload: Vec<u8>,
    /// The covenant id of output 0 (hex). It is the only thing that tells one deployment of the
    /// same templates from another.
    pub lineage: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ChainBlock {
    pub hash: [u8; 32],
    pub blue_score: u64,
    pub daa_score: u64,
    /// Header timestamp, ms since epoch.
    pub timestamp: u64,
    pub txs: Vec<AcceptedTx>,
}

#[derive(Debug, Clone, Default)]
pub struct VccResponse {
    /// High to low, as the node reports them.
    pub removed: Vec<[u8; 32]>,
    /// Low to high, index-aligned with the node's acceptance data.
    pub added: Vec<ChainBlock>,
}
