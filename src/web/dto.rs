use dotk_core::cards;
use dotk_core::state::{DeedState, GapState, Status};

use crate::model::{OwnerTypeParam, SpenderTypeParam};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::app::{self, Deployment, SelfTestReport};
use crate::convert::hex32;
use crate::db;
use crate::model::{CardChange, DeedRow, HistoryOp, RowKind};

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct NameResponse {
    /// The bare canonical name that the key hashes from, without the display suffix `.k`.
    pub(super) name: String,
    #[schema(value_type = OwnerTypeParam)]
    pub(super) owner_type: u8,
    /// The raw owner payload, hex. For every scheme but 0x04 it is the owner address's payload.
    pub(super) owner: String,
    /// The owner as a Kaspa address. Absent only for a covenant-id owner (0x04).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub(super) address: Option<String>,
    /// A UTXO at this address carrying `registryCovenantId` proves this answer.
    pub(super) deed_address: String,
    /// The name's live card, if it has one.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub(super) card: Option<CardOut>,
    pub(super) registry_covenant_id: String,
}

/// One card: a name's records, as the indexer observed them minted.
///
/// The indexer proves nothing about a card. A reader proves one against its own node: a live
/// UTXO sits at `cardAddress`, that UTXO is output 1 of the transaction that created the deed's
/// current UTXO, `blob` hashes to `recordsHash`, and `blob` is at most 16 KiB. The indexer checks
/// the last two before it serves a card.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct CardOut {
    /// The bare name the card's key belongs to. Absent only on a spender lookup where the key
    /// has no row.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    name: Option<String>,
    key: String,
    /// The minting transfer.
    outpoint_txid: String,
    /// Always 1.
    outpoint_index: u32,
    value: u64,
    /// The key that can sweep the card: a key-owned scheme byte, its payload, and the address.
    #[schema(value_type = SpenderTypeParam)]
    spender_type: u8,
    spender: String,
    spender_address: String,
    /// The card's P2SH address, where its UTXO sits.
    card_address: String,
    /// `blake3(blob)`, the hash the card's redeem script commits to.
    records_hash: String,
    /// The record blob, hex, in deterministic CBOR.
    blob: String,
    /// The decoded blob: ENSIP-5 text records, local flags such as `primary`, and opaque values
    /// that a client keeps. Absent when the blob is not a record map.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<std::collections::BTreeMap<String, RecordValueOut>>, nullable = false)]
    records: Option<cards::Records>,
    /// Whether the card speaks for its name now. Always true on a name or owner lookup. A
    /// spender lookup also lists retired cards, which are left to sweep.
    live: bool,
}

impl CardOut {
    pub(super) fn new(deployment: &Deployment, hit: db::CardHit) -> Self {
        let card = hit.card;
        Self {
            name: hit.name,
            key: hex32(&card.state.key),
            outpoint_txid: hex32(&card.txid),
            outpoint_index: card.idx,
            value: card.value,
            spender_type: card.state.spender_type as u8,
            spender: hex32(&card.state.spender),
            spender_address: dotk_core::address::owner_address(deployment.prefix, card.state.spender_type, &card.state.spender)
                .map(|a| a.to_string())
                .unwrap_or_default(),
            card_address: card.state.address(deployment.prefix).to_string(),
            records_hash: hex32(&card.state.records),
            records: cards::decode_records(&card.blob).ok(),
            blob: faster_hex::hex_string(&card.blob),
            live: hit.live,
        }
    }
}

// The schema of `cards::RecordValue`.
/// A record value: text for ENSIP-5 keys, a flag for a local key such as `primary`, or an
/// opaque value (one CBOR item as hex) that a client writes back unchanged.
#[derive(ToSchema)]
#[serde(untagged)]
#[schema(as = RecordValue)]
#[expect(dead_code, reason = "schema only")]
pub(super) enum RecordValueOut {
    Text(String),
    Flag(bool),
    Opaque { opaque: String },
}

/// The unswept cards naming one spender, live or retired, so a wallet can find cards to reclaim.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct SpenderCardsResponse {
    #[schema(value_type = SpenderTypeParam)]
    pub(super) spender_type: u8,
    pub(super) spender: String,
    pub(super) address: String,
    /// One page, in outpoint order.
    pub(super) cards: Vec<CardOut>,
    /// The `after` of the next page, `{txid}:{index}`. Absent on the last page.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub(super) next: Option<String>,
    pub(super) registry_covenant_id: String,
}

/// Every live name one owner holds. The owner is the `(ownerType, owner)` pair a deed stores,
/// and `address` is that pair as a Kaspa address.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct OwnerResponse {
    #[schema(value_type = OwnerTypeParam)]
    pub(super) owner_type: u8,
    /// The raw owner payload, hex.
    pub(super) owner: String,
    /// The owner as a Kaspa address. Absent only for a covenant-id owner (0x04).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub(super) address: Option<String>,
    /// Every live name this owner holds that the last self-test proved.
    pub(super) names: Vec<String>,
    /// The live card of each name that has one.
    pub(super) cards: Vec<CardOut>,
    pub(super) registry_covenant_id: String,
}

/// One deed: the state its address hashes from, plus the provenance the indexer observed.
///
/// The address hashes from `status ‖ key ‖ ownerType ‖ owner ‖ name`, with `key` and the padded
/// `name` taken from the enclosing `KeyResponse`. An ACTIVE deed carries `ownerType` and
/// `owner`. A PENDING deed carries `claim` instead, because its owner slot holds the claim hash.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct DeedOut {
    /// The on-chain status that the address hashes from. Derive an address from this, never
    /// from `kind`.
    status: StatusOut,
    /// ACTIVE only: the KCC-2 scheme byte and the owner payload it names.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<OwnerTypeParam>, nullable = false)]
    owner_type: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    owner: Option<String>,
    /// Where this deed lives: `P2SH(deed template ‖ the state above)`. A caller probes it against
    /// its own node.
    deed_address: String,
    /// PENDING only: the claim hash the split committed to. An evict needs it to rebuild
    /// `{PENDING, key, 0x00, claim, zero}`, and nothing public reveals it.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    claim: Option<String>,
    /// Transaction of the deed UTXO as last observed. Absent when this indexer never observed
    /// the split, or when the chain refuted its claim.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    outpoint_txid: Option<String>,
    /// Output index of the deed UTXO as last observed.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    outpoint_index: Option<u32>,
    /// DAA score of the block that accepted the deed's split. It carries through activate and
    /// transfer, so it is the registration time, and a PENDING deed is evictable at
    /// `acceptedDaa + params.t_evict` from `/genesis`. Always present on a PENDING deed. Absent on an ACTIVE deed whose
    /// registration this indexer did not observe.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    accepted_daa: Option<u64>,
}

impl DeedOut {
    pub(super) fn new(deployment: &Deployment, state: DeedState, row: &DeedRow) -> anyhow::Result<Self> {
        let active = state.status == Status::Active;
        Ok(Self {
            status: state.status.into(),
            owner_type: active.then_some(state.owner_type as u8),
            owner: active.then(|| hex32(&state.owner)),
            deed_address: deployment.deed_address(&state)?.to_string(),
            claim: row.claim.map(|c| hex32(&c)),
            outpoint_txid: row.outpoint_txid.map(|t| hex32(&t)),
            outpoint_index: row.outpoint_index,
            accepted_daa: row.accepted_daa,
        })
    }
}

/// The on-chain status of a deed.
#[derive(Serialize, ToSchema, Clone, Copy)]
#[schema(as = DeedStatus)]
pub(super) enum StatusOut {
    Pending,
    Active,
}

impl From<Status> for StatusOut {
    fn from(s: Status) -> Self {
        match s {
            Status::Pending => Self::Pending,
            Status::Active => Self::Active,
        }
    }
}

/// The indexer's knowledge of a row. `ownerUnknown` has no on-chain counterpart.
#[derive(Serialize, ToSchema, Clone, Copy)]
#[serde(rename_all = "camelCase")]
#[schema(as = RowKind)]
pub(super) enum RowKindOut {
    Active,
    Pending,
    OwnerUnknown,
}

impl From<RowKind> for RowKindOut {
    fn from(k: RowKind) -> Self {
        match k {
            RowKind::Active => Self::Active,
            RowKind::Pending => Self::Pending,
            RowKind::OwnerUnknown => Self::OwnerUnknown,
        }
    }
}

/// A row's kind, or `free` for a key without a row.
#[derive(Serialize, ToSchema, Clone, Copy)]
#[serde(rename_all = "camelCase")]
#[schema(as = KeyKind)]
pub(super) enum KeyKindOut {
    Active,
    Pending,
    OwnerUnknown,
    Free,
}

impl From<RowKind> for KeyKindOut {
    fn from(k: RowKind) -> Self {
        match k {
            RowKind::Active => Self::Active,
            RowKind::Pending => Self::Pending,
            RowKind::OwnerUnknown => Self::OwnerUnknown,
        }
    }
}

/// One gap: the open interval `(lo, hi)` of unregistered keyspace, hex. Its P2SH address hashes
/// from these two bounds.
#[derive(Serialize, ToSchema)]
pub(super) struct GapOut {
    lo: String,
    hi: String,
}

impl From<GapState> for GapOut {
    fn from(g: GapState) -> Self {
        Self { lo: hex32(&g.lo), hi: hex32(&g.hi) }
    }
}

/// The two gaps flanking a live key. The predecessor leads at merge seat 0 (`merge`) and the
/// successor delegates at seat 2 (`absorbed`). Swapping them builds a transaction the covenant
/// rejects.
#[derive(Serialize, ToSchema)]
#[schema(as = NeighbourGaps)]
pub(super) struct NeighborGaps {
    pub(super) predecessor: GapOut,
    pub(super) successor: GapOut,
}

/// Everything a client needs to act on one key: its deed and the gaps a transaction touching it
/// must co-spend.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct KeyResponse {
    pub(super) key: String,
    /// The indexer's row knowledge. This is not the on-chain status. Read `deed.status` to
    /// derive an address.
    pub(super) kind: KeyKindOut,
    /// The bare name. Absent when the row has none, which is always the case for PENDING.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub(super) name: Option<String>,
    /// The key's own deed. Absent for `free` and `ownerUnknown`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub(super) deed: Option<DeedOut>,
    /// `free` only: the gap that strictly contains the key, which a split spends.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub(super) covering: Option<GapOut>,
    /// Occupied keys only: the two gaps flanking the key. With the deed, they are all a release
    /// needs. Never present together with `covering`.
    #[serde(rename = "neighbours", skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub(super) neighbors: Option<NeighborGaps>,
    pub(super) registry_covenant_id: String,
    /// Whether the last self-test passed and left no blind spot around this key. This is
    /// coverage, not freshness: it is as old as that self-test, which finished at `provenAt`. A
    /// client can probe the deed address itself.
    pub(super) proven: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub(super) proven_at: Option<u64>,
}

/// What an event did to a key. A `sweep` spends a card on its own and leaves the deed
/// unchanged.
#[derive(Serialize, ToSchema, Clone, Copy)]
#[serde(rename_all = "lowercase")]
#[schema(as = HistoryOp)]
pub(super) enum HistoryOpOut {
    Register,
    Activate,
    Transfer,
    Release,
    Evict,
    Discover,
    Sweep,
}

impl From<HistoryOp> for HistoryOpOut {
    fn from(op: HistoryOp) -> Self {
        match op {
            HistoryOp::Register => Self::Register,
            HistoryOp::Activate => Self::Activate,
            HistoryOp::Transfer => Self::Transfer,
            HistoryOp::Release => Self::Release,
            HistoryOp::Evict => Self::Evict,
            HistoryOp::Discover => Self::Discover,
            HistoryOp::Sweep => Self::Sweep,
        }
    }
}

/// What an event did to a name's card.
#[derive(Serialize, ToSchema, Clone, Copy)]
#[serde(rename_all = "lowercase")]
#[schema(as = CardChange)]
pub(super) enum CardChangeOut {
    Set,
    Updated,
    Deleted,
}

impl CardChangeOut {
    fn of(change: CardChange) -> Option<Self> {
        match change {
            CardChange::None => None,
            CardChange::Set => Some(Self::Set),
            CardChange::Updated => Some(Self::Updated),
            CardChange::Deleted => Some(Self::Deleted),
        }
    }
}

/// One event on a key, and the deed's state right after it. `kind` is absent when the event
/// ended the deed (a release or an evict).
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct HistoryEntry {
    op: HistoryOpOut,
    /// Apply order within the accepting block.
    seq: i32,
    /// Blue score of the accepting chain block.
    blue_score: u64,
    /// DAA score of the accepting chain block, the clock every age in this API uses.
    daa_score: u64,
    /// Header timestamp of the accepting chain block, in ms.
    block_time: u64,
    block_hash: String,
    /// The transaction that made the change.
    txid: String,
    /// The row's kind after the event. Absent when the deed ended here.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    kind: Option<RowKindOut>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<OwnerTypeParam>, nullable = false)]
    owner_type: Option<u8>,
    /// The KCC-2 owner payload, hex. Present only when the entry left the deed ACTIVE. No
    /// address is served, because a client derives it from the pair.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    owner: Option<String>,
    /// PENDING only: the claim hash the split committed to.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    claim: Option<String>,
    /// Absent when the event left the records alone.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    card_change: Option<CardChangeOut>,
}

impl HistoryEntry {
    pub(super) fn new(row: &crate::model::HistoryRow) -> Self {
        let state = row.state.as_ref();
        Self {
            op: row.op.into(),
            seq: row.seq,
            blue_score: row.blue_score,
            daa_score: row.daa_score,
            block_time: row.block_time,
            block_hash: hex32(&row.block_hash),
            txid: hex32(&row.txid),
            kind: state.map(|s| s.kind.into()),
            name: state.and_then(|s| s.name.clone()),
            owner_type: state.and_then(|s| s.owner_type),
            owner: state.and_then(|s| s.owner.as_ref().map(hex32)),
            claim: state.and_then(|s| s.claim.as_ref().map(hex32)),
            card_change: CardChangeOut::of(row.card),
        }
    }
}

/// One key's history, newest first, one page at a time.
///
/// It holds only what this indexer observed. Kaspa prunes old blocks, so that can be less than
/// the full past, and `complete` says which it is.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct HistoryResponse {
    pub(super) key: String,
    /// Total entries for this key.
    pub(super) total: u64,
    pub(super) limit: u32,
    pub(super) offset: u32,
    /// Whether the oldest entry held is the key's own registration. `false` for an empty
    /// history too.
    pub(super) complete: bool,
    pub(super) entries: Vec<HistoryEntry>,
    pub(super) registry_covenant_id: String,
}

/// The whole keyspace at a glance: two distributions and their totals. Always a live database
/// read and never proven, so it carries no `proven`.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct KeyspaceResponse {
    pub(super) registry_covenant_id: String,
    pub(super) totals: KeyspaceTotals,
    pub(super) keys_by_prefix: KeysByPrefix,
    pub(super) gaps_by_width: GapsByWidth,
    /// Registrations per UTC day, oldest first, only days with at least one. Counted from
    /// `register` events, so a name released or evicted since still counts.
    pub(super) registrations_by_day: Vec<DayCount>,
    /// The last processed chain block, which is what this answer is current to.
    #[schema(required = true)]
    pub(super) last_block: Option<BlockRef>,
}

/// One UTC day and how many names were registered on it.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct DayCount {
    /// The day in `YYYY-MM-DD`, UTC, from the accepting block's header timestamp.
    day: String,
    count: i64,
}

impl DayCount {
    pub(super) fn new((days, count): (i64, i64)) -> Self {
        Self { day: civil_date(days), count }
    }
}

/// `YYYY-MM-DD` for a count of days since 1970-01-01, by Howard Hinnant's civil-from-days.
fn civil_date(days: i64) -> String {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

/// Registry totals. The deed counts are sums of `keysByPrefix`, so they always agree with it.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct KeyspaceTotals {
    pub(super) active: i64,
    pub(super) pending: i64,
    pub(super) owner_unknown: i64,
    /// Live gaps, counted independently. The partition invariant makes this deeds + 1, so a
    /// client can check it.
    pub(super) gaps: i64,
}

/// Registered keys per leading key byte: 256 buckets per array, in order. Kinds stay separate,
/// so each client chooses which to merge. Merging adjacent buckets gives the exact histogram
/// over one bit less of prefix.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct KeysByPrefix {
    pub(super) active: Vec<i64>,
    pub(super) pending: Vec<i64>,
    pub(super) owner_unknown: Vec<i64>,
}

/// Gap widths as multiples of the average gap, so the shape stays comparable as the registry
/// grows. With uniform keys it decays exponentially.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct GapsByWidth {
    /// Gaps per bucket, narrowest first. Bucket `i` holds the gaps at least
    /// `i / bucketsPerAverage` and under `(i + 1) / bucketsPerAverage` times the average.
    pub(super) counts: Vec<i64>,
    /// How many buckets span one average gap: 4, so each is a quarter of it.
    pub(super) buckets_per_average: i64,
    /// Gaps wider than the last bucket. Not part of `counts`.
    pub(super) wider: i64,
}

/// The last self-test's verdict, served on every `/health`. The failure coordinates are in
/// `SelfTestDetail`, served only on request, because pages poll this body.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct SelfTestSummary {
    proven: bool,
    /// Wall-clock unix ms at which the run started.
    started_ms: u64,
    /// Wall-clock unix ms at which the run ended.
    finished_ms: u64,
    /// Derived gaps confirmed against the UTXO set: the completeness half of the proof.
    gaps_checked: u64,
    /// Deed addresses confirmed. Lower than the row count when owner-unknown rows exist, because
    /// those have no address to derive.
    deeds_checked: u64,
}

impl From<&SelfTestReport> for SelfTestSummary {
    fn from(t: &SelfTestReport) -> Self {
        Self {
            proven: t.proven,
            started_ms: t.started_ms,
            finished_ms: t.finished_ms,
            gaps_checked: t.gaps_checked,
            deeds_checked: t.deeds_checked,
        }
    }
}

/// What the last self-test could not prove, and what repair did, served only for
/// `?detail=true`. Each list holds at most 100 entries, and the `…Omitted` field beside it
/// counts the rest.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct SelfTestDetail {
    /// Whether this run refreshed the `/snapshot` proof.
    published: bool,
    /// How many of `deedsChecked` were PENDING deeds. This counts what the run proved, where
    /// `/health`'s `pending` counts what the table holds now.
    pending_checked: u64,
    attempts: u32,
    /// Outpoints that failed 3 times in a row or only the last probe, as `outpoint:<hex key>`.
    /// Failing gaps and deeds appear in `blindSpots` and `ownerUnknown` instead.
    failing_outpoints: Vec<String>,
    #[serde(skip_serializing_if = "is_zero_u64")]
    failing_outpoints_omitted: u64,
    /// Residual blind spots after repair, as [lo, hi] hex key brackets: keyspace whose
    /// registrations this indexer never observed, or a gap that failed only the last probe.
    blind_spots: Vec<[String; 2]>,
    #[serde(skip_serializing_if = "is_zero_u64")]
    blind_spots_omitted: u64,
    /// Registered rows whose deed address is refuted, as [hex key, name]. Repair demotes these
    /// to owner-unknown, so an entry here means the pipeline kept touching the row or the
    /// refutation came only in the last probe.
    owner_unknown: Vec<[String; 2]>,
    #[serde(skip_serializing_if = "is_zero_u64")]
    owner_unknown_omitted: u64,
    /// What repair did: rows dropped, demoted, re-adopted, bridged or rewritten, plus gap cache
    /// rows reconciled.
    repaired: RepairSummary,
}

/// A node outage fails every gap and deed at once, so each list needs a cap.
const DETAIL_LIST_MAX: usize = 100;

#[expect(clippy::trivially_copy_pass_by_ref, reason = "serde passes a reference")]
fn is_zero_u64(n: &u64) -> bool {
    *n == 0
}

fn capped<T: Clone>(all: &[T]) -> (Vec<T>, u64) {
    (all.iter().take(DETAIL_LIST_MAX).cloned().collect(), u64::try_from(all.len().saturating_sub(DETAIL_LIST_MAX)).unwrap_or(u64::MAX))
}

impl From<&SelfTestReport> for SelfTestDetail {
    fn from(t: &SelfTestReport) -> Self {
        let (failing_outpoints, failing_outpoints_omitted) = capped(&t.failing_outpoints);
        let (blind_spots, blind_spots_omitted) = capped(&t.blind_spots);
        let (owner_unknown, owner_unknown_omitted) = capped(&t.owner_unknown);
        Self {
            published: t.published,
            pending_checked: t.pending_checked,
            attempts: t.attempts,
            failing_outpoints,
            failing_outpoints_omitted,
            blind_spots,
            blind_spots_omitted,
            owner_unknown,
            owner_unknown_omitted,
            repaired: RepairSummary::from(&t.repaired),
        }
    }
}

/// The last fully processed chain block.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct BlockRef {
    hash: String,
    daa_score: u64,
    blue_score: u64,
    /// Header timestamp, ms since epoch.
    timestamp: u64,
}

impl From<&app::BlockRef> for BlockRef {
    fn from(b: &app::BlockRef) -> Self {
        Self { hash: b.hash.clone(), daa_score: b.daa_score, blue_score: b.blue_score, timestamp: b.timestamp }
    }
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct RepairSummary {
    /// Rows removed because the chain shows their key unregistered.
    dropped: u64,
    /// Rows with refuted ownership, kept as owner-unknown.
    demoted: u64,
    /// Registered keys the table lacked, adopted with their proven state.
    readopted: u64,
    /// Registered keys the table lacked, adopted as owner-unknown.
    bridged: u64,
    /// Rows whose outpoint, value and acceptance DAA were rewritten from the observed UTXO.
    rewritten: u64,
    /// Gap cache rows adopted, corrected or dropped. Not a verdict input.
    gaps_synced: u64,
    /// Card rows the probe marked swept. Not a verdict input.
    cards_swept: u64,
    /// Card rows the probe marked unswept again.
    cards_restored: u64,
    /// Passes the repair took to converge.
    iterations: u32,
}

impl From<&app::RepairSummary> for RepairSummary {
    fn from(r: &app::RepairSummary) -> Self {
        Self {
            dropped: r.dropped,
            demoted: r.demoted,
            readopted: r.readopted,
            bridged: r.bridged,
            rewritten: r.rewritten,
            gaps_synced: r.gaps_synced,
            cards_swept: r.cards_swept,
            cards_restored: r.cards_restored,
            iterations: r.iterations,
        }
    }
}

/// Every operation refuses parameters it does not define, so each distinct URL, and so each
/// edge cache entry, is a distinct answer.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NoQuery {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HealthQuery {
    pub(super) detail: Option<bool>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct HealthResponse {
    pub(super) healthy: bool,
    #[schema(required = true)]
    pub(super) last_block: Option<BlockRef>,
    /// The sink's blue score as last observed, or `null` before the first observation.
    /// `(tipBlueScore - tipDistance - lastBlock.blueScore) / netBps` is how many seconds behind
    /// this indexer is. It freezes during a node outage, so the lag then reads low.
    #[schema(required = true)]
    pub(super) tip_blue_score: Option<u64>,
    /// The network's nominal blocks per second.
    pub(super) net_bps: u64,
    pub(super) active: i64,
    pub(super) pending: i64,
    /// Rows whose key is proven registered but whose owner is refuted or was never observed.
    pub(super) owner_unknown: i64,
    /// The registry's change marker, or `null` before any event. Poll it to
    /// decide whether to re-read `/snapshot`. Compare for difference, not order, because a reorg
    /// can lower it.
    #[schema(required = true)]
    pub(super) history_seq: Option<i64>,
    /// Confirmation depth in blue score. The indexer does not read the chain blocks within this
    /// distance below the tip.
    pub(super) tip_distance: u64,
    /// Lowest blue score the journal can undo exactly. 0 means full coverage. `null` only after
    /// an import without a journal, until this process journals a block of its own.
    #[schema(required = true)]
    pub(super) journal_coverage: Option<u64>,
    /// Whether the indexer is caught up with the chain now: `lastBlock` is under 10 s old and under
    /// 60 s of blocks behind `tipBlueScore - tipDistance`. Never latched. `healthy` tolerates
    /// 10 s of `false`.
    pub(super) caught_up: bool,
    /// The last self-test's verdict, or `null` before the first run has finished.
    #[schema(required = true)]
    pub(super) self_test: Option<SelfTestSummary>,
    /// Where that run failed, present only for `?detail=true` and only once a run has finished.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub(super) self_test_detail: Option<SelfTestDetail>,
    /// Which registry this indexer serves. A client compares it with its own pinned id before it
    /// acts on any answer.
    pub(super) registry_covenant_id: String,
    /// The network the manifest was deployed on. With `registryCovenantId`, it identifies the
    /// registry.
    pub(super) network: String,
}

// A hand-written mirror of `dotk_core::watch::GenesisFile`, which knows nothing of OpenAPI. A test
// holds the two to the same fields.
/// The deployment manifest, byte for byte as built into this indexer.
///
/// Unknown fields stay open, because the manifest is relayed verbatim and newer tooling can add
/// fields. `gapAbi` and `deedAbi` are open objects, because their shape belongs to silverscript.
#[derive(ToSchema)]
#[serde(rename_all = "camelCase")]
#[expect(dead_code, reason = "schema only")]
pub(super) struct Manifest {
    /// Manifest shape version. A client refuses a version it does not read.
    #[schema(nullable = false)]
    version: Option<u32>,
    /// The Kaspa network this registry was deployed on.
    network: String,
    /// The registry's covenant id.
    registry_covenant_id: String,
    /// The gap covenant's ABI artifact: bytecode, state span, dispatch tags and argument types.
    #[schema(value_type = Object)]
    gap_abi: serde_json::Value,
    /// The deed covenant's ABI artifact.
    #[schema(value_type = Object)]
    deed_abi: serde_json::Value,
    /// A client that rebuilds the gap template and gets another hash refuses the manifest.
    gap_template_hash: String,
    /// The same for the deed template.
    deed_template_hash: String,
    /// The protocol parameters compiled into the templates, so a forged copy yields addresses
    /// that hold no UTXO.
    #[schema(value_type = Object)]
    params: serde_json::Value,
    /// The covenant id's full preimage: the outpoint the deployer spent, which output was the
    /// genesis gap, and that output with its redeem script and state. A client can recompute the
    /// id and check the genesis output group had exactly one member, with no chain access.
    #[schema(value_type = Option<Object>, nullable = false)]
    genesis_binding: Option<serde_json::Value>,
    /// Fields this build does not know.
    #[serde(flatten)]
    #[schema(value_type = Object)]
    extra: std::collections::HashMap<String, serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SpenderCardsQuery {
    pub(super) after: Option<String>,
    pub(super) limit: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HistoryQuery {
    pub(super) limit: Option<u32>,
    pub(super) offset: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotQuery {
    /// `bool`'s `FromStr` rejects `0`, `1` and `""`, so `?proven=0` never gets the proven body.
    pub(super) proven: Option<bool>,
    /// `false` leaves `events` empty rather than absent, so the schema does not change with the
    /// query and the importer reads it as a journal-less snapshot.
    pub(super) events: Option<bool>,
}

#[cfg(test)]
#[path = "dto_tests.rs"]
mod tests;
