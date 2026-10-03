//! The snapshot wire types and their conversions to and from rows.

use anyhow::{Context, Result};
use dotk_core::cards::{self, CardState};
use dotk_core::state::OwnerType;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::convert::sql_i64;
use crate::convert::{hex32, unhex32};
use crate::db::{EventRow, GapImage};
use crate::model::{CardMark, CardRow, DeedRow, GapRow, OwnerTypeParam, RowKind, SpenderTypeParam};

/// The registry for mirrors and bootstrap. The canonical core is `registryCovenantId` and the
/// key-sorted `deeds`. The resume section is `vcpCheckpoint` and `events`, the journal tail.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    /// Set by the self-test that proved this snapshot against the UTXO set. This field and
    /// `provenAt` are outside the canonical core, and an importer ignores both.
    #[serde(default)]
    #[schema(required = true)]
    pub proven: bool,
    /// The producing self-test's `finishedMs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub proven_at: Option<u64>,
    pub registry_covenant_id: String,
    pub vcp_checkpoint: String,
    /// The `historySeq` that `/health` publishes, read in the same transaction as the rows.
    /// It tells a polling client which registry state it received. History ids are local to one
    /// indexer, so this is outside the canonical core, and an importer ignores it.
    // Serialized even when null, to match `/health`. A browser reads an absent field as
    // `undefined`, which never equals `null`, and a polling client then downloads the snapshot
    // again on every refresh.
    #[serde(default)]
    #[schema(required = true)]
    pub history_seq: Option<i64>,
    /// The `historyEpoch` of `/history`, from the same transaction as `historySeq`. It is outside
    /// the canonical core, and an importer ignores it.
    #[serde(default)]
    #[schema(required = true)]
    pub history_epoch: Option<String>,
    pub deeds: Vec<ExportDeed>,
    /// Unswept cards only. Cards are outside the canonical core, and the self-test proves
    /// nothing about them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cards: Vec<ExportCard>,
    pub events: Vec<ExportEvent>,
}

/// Binary fields are hex.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExportCard {
    pub txid: String,
    pub idx: u32,
    pub key: String,
    pub records: String,
    #[schema(value_type = SpenderTypeParam)]
    pub spender_type: u8,
    pub spender: String,
    pub blob: String,
    pub value: u64,
}

/// Binary fields are hex.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExportDeed {
    pub key: String,
    #[serde(flatten)]
    pub row: ExportRow,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExportRow {
    /// 0 ACTIVE, 1 PENDING, 2 owner-unknown.
    pub kind: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<OwnerTypeParam>, nullable = false)]
    pub owner_type: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub claim: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub outpoint_txid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub outpoint_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub value: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub accepted_daa: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExportEvent {
    pub block_hash: String,
    pub seq: i32,
    pub blue_score: u64,
    pub key: String,
    /// The deed pre-image. Absent means the row did not exist before the event.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub prev: Option<ExportRow>,
    /// Pre-images of the gap rows the event wrote. An entry without `hi` means no row existed
    /// at that `lo`, and undo deletes it. A split always creates one, so a journal without these
    /// cannot undo a split.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prev_gaps: Vec<ExportGap>,
    /// `existed: false` for a mint, which undo deletes. Otherwise the `sweptAt` the row carried
    /// before the event.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prev_cards: Vec<ExportCardMark>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExportCardMark {
    pub txid: String,
    pub idx: u32,
    pub existed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub swept_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExportGap {
    pub lo: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub hi: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub outpoint_txid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub outpoint_index: Option<u32>,
}

/// Postgres stores a `u64` as `bigint`, so a value above `i64::MAX` turns negative. A negative
/// `accepted_daa` makes the evictor offer the key again every tick, and a negative `blue_score`
/// escalates every reorg for the life of the process.
fn bigint_u64(v: u64, what: &str) -> Result<u64> {
    sql_i64(v).with_context(|| format!("{what} is {v}, above i64::MAX, so postgres stores it negative"))?;
    Ok(v)
}

impl From<&([u8; 32], DeedRow)> for ExportDeed {
    fn from((key, row): &([u8; 32], DeedRow)) -> Self {
        Self { key: hex32(key), row: ExportRow::from(row) }
    }
}

impl TryFrom<&ExportDeed> for ([u8; 32], DeedRow) {
    type Error = anyhow::Error;

    fn try_from(d: &ExportDeed) -> Result<Self> {
        let key = unhex32(&d.key).context("snapshot deed key")?;
        let row = DeedRow::try_from((&key, &d.row)).with_context(|| format!("snapshot deed {}", d.key))?;
        Ok((key, row))
    }
}

impl From<&DeedRow> for ExportRow {
    fn from(r: &DeedRow) -> Self {
        Self {
            kind: r.kind as u8,
            name: r.name.clone(),
            owner_type: r.owner_type,
            owner: r.owner.map(|v| hex32(&v)),
            claim: r.claim.map(|v| hex32(&v)),
            outpoint_txid: r.outpoint_txid.map(|v| hex32(&v)),
            outpoint_index: r.outpoint_index,
            value: r.value,
            accepted_daa: r.accepted_daa,
        }
    }
}

/// Nothing in a snapshot is trusted. A PENDING deed's address ignores `name`, so the
/// self-test cannot catch a name on a pending row, and import refuses it. A panic in
/// `DeedRow::deed_state` is a crash loop on every boot.
impl TryFrom<(&[u8; 32], &ExportRow)> for DeedRow {
    type Error = anyhow::Error;

    fn try_from((key, r): (&[u8; 32], &ExportRow)) -> Result<Self> {
        let kind = RowKind::try_from(i16::from(r.kind)).map_err(|k| anyhow::anyhow!("unknown row kind {k}"))?;
        if let Some(name) = &r.name {
            dotk_core::names::validate(name).map_err(|why| anyhow::anyhow!("name {name:?} is not a valid registry name: {why}"))?;
            anyhow::ensure!(
                &dotk_core::names::key_of(name) == key,
                "name {name:?} does not hash to the key this row is filed under. The deed address commits to the key, \
                 so the two halves describe no deed that exists"
            );
        }
        if let Some(ot) = r.owner_type {
            anyhow::ensure!(OwnerType::from_byte(ot).is_some(), "unknown owner type {ot:#04x}");
        }
        anyhow::ensure!(
            r.outpoint_txid.is_some() == r.outpoint_index.is_some() && r.outpoint_txid.is_some() == r.value.is_some(),
            "part of a provenance: outpoint txid, index and value must all be present or all absent"
        );
        ensure_kind_agrees(kind, r)?;
        Ok(DeedRow {
            kind,
            name: r.name.clone(),
            owner: r.owner.as_deref().map(unhex32).transpose()?,
            owner_type: r.owner_type,
            claim: r.claim.as_deref().map(unhex32).transpose()?,
            outpoint_txid: r.outpoint_txid.as_deref().map(unhex32).transpose()?,
            outpoint_index: r.outpoint_index,
            value: r.value.map(|v| bigint_u64(v, "value")).transpose()?,
            accepted_daa: r.accepted_daa.map(|v| bigint_u64(v, "acceptedDaa")).transpose()?,
        })
    }
}

/// `DeedRow::deed_state` dispatches on `kind` and `DeedRow::entry` on `claim`.
fn ensure_kind_agrees(kind: RowKind, r: &ExportRow) -> Result<()> {
    let present = |label: &str, yes: bool| -> Result<()> {
        anyhow::ensure!(yes, "a {} row must carry {label}", kind.label());
        Ok(())
    };
    let absent = |label: &str, no: bool| -> Result<()> {
        anyhow::ensure!(no, "a {} row must not carry {label}", kind.label());
        Ok(())
    };
    match kind {
        RowKind::Active => {
            present("a name", r.name.is_some())?;
            present("an owner", r.owner.is_some())?;
            present("an owner type", r.owner_type.is_some())?;
            absent("a claim", r.claim.is_none())?;
        }
        RowKind::Pending => {
            present("a claim", r.claim.is_some())?;
            present("an accepted DAA score", r.accepted_daa.is_some())?;
            present("an outpoint", r.outpoint_txid.is_some())?;
            absent("a name, since the name of a pending deed is still secret", r.name.is_none())?;
            absent("an owner", r.owner.is_none())?;
            absent("an owner type", r.owner_type.is_none())?;
        }
        RowKind::OwnerUnknown => {
            absent("an owner", r.owner.is_none())?;
            absent("an owner type", r.owner_type.is_none())?;
            absent("a claim", r.claim.is_none())?;
            absent("an outpoint", r.outpoint_txid.is_none())?;
            absent("a value", r.value.is_none())?;
            absent("an accepted DAA score", r.accepted_daa.is_none())?;
        }
    }
    Ok(())
}

impl From<&CardRow> for ExportCard {
    fn from(c: &CardRow) -> Self {
        Self {
            txid: hex32(&c.txid),
            idx: c.idx,
            key: hex32(&c.state.key),
            records: hex32(&c.state.records),
            spender_type: c.state.spender_type as u8,
            spender: hex32(&c.state.spender),
            blob: faster_hex::hex_string(&c.blob),
            value: c.value,
        }
    }
}

/// Checks the blob here, because the self-test probes only the card's address, which
/// commits to `records` and not to the blob.
impl TryFrom<&ExportCard> for CardRow {
    type Error = anyhow::Error;

    fn try_from(c: &ExportCard) -> Result<Self> {
        let mut blob = vec![0u8; c.blob.len() / 2];
        anyhow::ensure!(c.blob.len().is_multiple_of(2), "blob is not hex");
        faster_hex::hex_decode(c.blob.as_bytes(), &mut blob).context("blob is not hex")?;
        anyhow::ensure!(
            blob.len() <= cards::CARD_BLOB_MAX,
            "blob is {} bytes, over the {} byte cap",
            blob.len(),
            cards::CARD_BLOB_MAX
        );
        let records = unhex32(&c.records).context("records")?;
        anyhow::ensure!(cards::records_of(&blob) == records, "blob does not hash to the records the card commits to");
        anyhow::ensure!(c.idx == 1, "card at output {}: a card is output 1 of its transfer", c.idx);
        let spender_type =
            OwnerType::from_byte(c.spender_type).with_context(|| format!("unknown spender type {:#04x}", c.spender_type))?;
        let state = CardState::new(unhex32(&c.key).context("key")?, records, spender_type, unhex32(&c.spender).context("spender")?)?;
        Ok(CardRow {
            txid: unhex32(&c.txid).context("txid")?,
            idx: c.idx,
            state,
            blob,
            value: bigint_u64(c.value, "value")?,
            swept_at: None,
        })
    }
}

impl From<&EventRow> for ExportEvent {
    fn from(e: &EventRow) -> Self {
        Self {
            block_hash: hex32(&e.block_hash),
            seq: e.seq,
            blue_score: e.blue_score,
            key: hex32(&e.key),
            prev: e.prev.as_ref().map(ExportRow::from),
            prev_gaps: e.prev_gaps.iter().map(ExportGap::from).collect(),
            prev_cards: e.prev_cards.iter().map(ExportCardMark::from).collect(),
        }
    }
}

impl TryFrom<&ExportEvent> for EventRow {
    type Error = anyhow::Error;

    fn try_from(e: &ExportEvent) -> Result<Self> {
        let key = unhex32(&e.key).context("snapshot event key")?;
        let prev = e
            .prev
            .as_ref()
            .map(|p| DeedRow::try_from((&key, p)))
            .transpose()
            .with_context(|| format!("snapshot event image {}", e.key))?;
        let prev_gaps =
            e.prev_gaps.iter().map(GapImage::try_from).collect::<Result<_>>().with_context(|| format!("snapshot event {}", e.key))?;
        let prev_cards =
            e.prev_cards.iter().map(CardMark::try_from).collect::<Result<_>>().with_context(|| format!("snapshot event {}", e.key))?;
        let blue_score = bigint_u64(e.blue_score, "blueScore").with_context(|| format!("snapshot event {}", e.key))?;
        Ok(EventRow {
            block_hash: unhex32(&e.block_hash).context("snapshot event blockHash")?,
            seq: e.seq,
            blue_score,
            key,
            prev,
            prev_gaps,
            prev_cards,
        })
    }
}

impl From<&CardMark> for ExportCardMark {
    fn from(m: &CardMark) -> Self {
        Self { txid: hex32(&m.txid), idx: m.idx, existed: m.existed, swept_at: m.swept_at }
    }
}

impl TryFrom<&ExportCardMark> for CardMark {
    type Error = anyhow::Error;

    fn try_from(m: &ExportCardMark) -> Result<Self> {
        Ok(CardMark {
            txid: unhex32(&m.txid).context("snapshot card mark txid")?,
            idx: m.idx,
            existed: m.existed,
            swept_at: m.swept_at.map(|v| bigint_u64(v, "sweptAt")).transpose()?,
        })
    }
}

impl From<&GapImage> for ExportGap {
    fn from((lo, row): &GapImage) -> Self {
        Self {
            lo: hex32(lo),
            hi: row.map(|g| hex32(&g.hi)),
            outpoint_txid: row.and_then(|g| g.outpoint_txid).map(|v| hex32(&v)),
            outpoint_index: row.and_then(|g| g.outpoint_index),
        }
    }
}

impl TryFrom<&ExportGap> for GapImage {
    type Error = anyhow::Error;

    fn try_from(e: &ExportGap) -> Result<Self> {
        let lo = unhex32(&e.lo).context("snapshot gap lo")?;
        let row = e.hi.as_deref().map(|hi| gap_row(&lo, hi, e)).transpose()?;
        Ok((lo, row))
    }
}

/// A gap is the open interval (lo, hi), and undo restores these images verbatim.
fn gap_row(lo: &[u8; 32], hi: &str, e: &ExportGap) -> Result<GapRow> {
    let hi = unhex32(hi)?;
    anyhow::ensure!(*lo < hi, "gap image lo {} is not below hi {}, so that interval covers no keyspace", hex32(lo), hex32(&hi));
    anyhow::ensure!(
        e.outpoint_txid.is_some() == e.outpoint_index.is_some(),
        "half an outpoint on gap image {}: txid and index must both be present or both absent",
        hex32(lo)
    );
    Ok(GapRow { hi, outpoint_txid: e.outpoint_txid.as_deref().map(unhex32).transpose()?, outpoint_index: e.outpoint_index })
}

#[cfg(test)]
#[path = "format_tests.rs"]
mod tests;
