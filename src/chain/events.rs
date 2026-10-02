//! Applies and undoes registry events, journaling each row's pre-image so undo is a strict LIFO
//! restore.

use anyhow::{Context, Result};
use dotk_core::cards::{self, CardState};
use dotk_core::params::Params;
use dotk_core::registry::{KEY_MAX, KEY_MIN};
use dotk_core::watch::{RegistryOp, WatchTemplates};
use sqlx::PgConnection;

use crate::chain::{AcceptedTx, ChainBlock};
use crate::convert::hex32;
use crate::db::{self, GapImage};
use crate::model::{CardChange, CardMark, CardRow, DeedRow, GapRow, HistoryOp, HistoryRow, RowKind};

#[derive(Debug, Clone, Copy)]
pub struct ApplyOutcome {
    pub events: u32,
    pub needs_selftest: bool,
}

/// `new: None` deletes the row.
struct GapWrite {
    lo: [u8; 32],
    new: Option<GapRow>,
}

/// Only the covenant id on output 0 tells which registry a transaction belongs to, because
/// another deployment of the same templates decodes identically. A bound transaction spends a
/// registry UTXO. A decode failure therefore schedules a self-test, because a dropped event
/// serves an owner the chain refutes.
pub async fn apply_block(
    conn: &mut PgConnection,
    watch: &WatchTemplates,
    params: &Params,
    registry_covenant_id: &str,
    block: &ChainBlock,
) -> Result<ApplyOutcome> {
    let mut out = ApplyOutcome { events: 0, needs_selftest: false };
    for tx in &block.txs {
        let bound = tx.lineage.as_deref() == Some(registry_covenant_id);
        let registry_shaped = tx.sig_scripts.iter().any(|s| watch.matches_fingerprint(s));
        if !bound {
            if registry_shaped && matches!(watch.classify(&tx.sig_scripts), Ok(Some(_))) {
                // Another deployment is expected traffic. An unbound one means the node does
                // not report covenant bindings, so the pipeline indexes nothing while healthy.
                match tx.lineage.as_deref() {
                    Some(other) => {
                        log::debug!("ignoring registry transaction {} of lineage {other}", hex32(&tx.txid));
                    }
                    None => log::warn!(
                        "registry transaction {} carries no covenant binding on output 0. Is the node reporting covenant ids?",
                        hex32(&tx.txid)
                    ),
                }
            }
            // A sweep outside the registry is still an event, so undo can clear its mark.
            apply_standalone_sweeps(conn, block, tx, &mut out).await?;
            continue;
        }
        if !registry_shaped {
            log::debug!("bound transaction {} carries no registry redeem: the genesis mint", hex32(&tx.txid));
            apply_standalone_sweeps(conn, block, tx, &mut out).await?;
            continue;
        }
        match watch.classify(&tx.sig_scripts) {
            Ok(Some(op)) => apply_op(conn, params, block, tx, op, &mut out).await?,
            undecoded => {
                // Only a wrong artifact or decoder gets here. The self-test repairs the state.
                let why = undecoded.err().unwrap_or_else(|| "input 0 is no registry redeem".to_string());
                log::error!("undecodable registry transaction {}: {why}", hex32(&tx.txid));
                out.needs_selftest = true;
            }
        }
    }
    Ok(out)
}

struct Change {
    key: [u8; 32],
    new: Option<DeedRow>,
    gaps: Vec<GapWrite>,
    history_op: HistoryOp,
    /// A name in escrow carries no records, so a card beside a covenant-id owner is not one.
    into_escrow: bool,
    is_evict: bool,
}

struct Journal {
    event: db::EventRow,
    swept_live: Vec<SweptLive>,
    card: CardChange,
}

async fn apply_op(
    conn: &mut PgConnection,
    params: &Params,
    block: &ChainBlock,
    tx: &AcceptedTx,
    op: RegistryOp,
    out: &mut ApplyOutcome,
) -> Result<()> {
    discover_bounds(conn, block, tx, &op, out).await?;
    let Some(mut change) = derive_change(params, block, tx, op) else {
        out.needs_selftest = true;
        return Ok(());
    };
    let prev = db::get_deed(conn, &change.key).await?;
    let card_before = card_live_before(conn, prev.as_ref(), &change.key, block.blue_score).await?;
    carry_accepted_daa(&mut change.new, prev.as_ref());
    if change.is_evict && matches!(prev.as_ref().map(|p| p.kind), Some(RowKind::Active)) {
        // An evict only deletes a PENDING deed, so the stored state was wrong. The chain wins.
        log::warn!("evict hit ACTIVE row {}, scheduling a self-test", hex32(&change.key));
        out.needs_selftest = true;
    }
    let prev_gaps = gap_images(conn, &change.gaps).await?;
    let cards = write_cards(conn, block, tx, &change).await?;
    let seq = event_seq(*out)?;
    // Duplicate delivery. A card mark written above stays unjournaled, and the next probe
    // restores it if the block is undone. A repair can write the event's image first, so only
    // the journal tells a duplicate.
    if db::event_exists(conn, &block.hash, seq).await? {
        return Ok(());
    }
    write_rows(conn, &change, prev.is_some()).await?;
    let journal = Journal {
        event: db::EventRow {
            block_hash: block.hash,
            seq,
            blue_score: block.blue_score,
            key: change.key,
            prev,
            prev_gaps,
            prev_cards: cards.marks,
        },
        swept_live: cards.swept_live,
        card: CardChange::of(card_before, cards.minted),
    };
    journal_event(conn, block, tx, &change, &journal).await?;
    out.events += 1;
    Ok(())
}

fn event_seq(out: ApplyOutcome) -> Result<i32> {
    i32::try_from(out.events).context("the event sequence number of a block exceeds i32")
}

/// Every gap a spend republishes names its two bound keys. An unknown one enters as an
/// owner-unknown row, journaled so undo stays exact.
async fn discover_bounds(
    conn: &mut PgConnection,
    block: &ChainBlock,
    tx: &AcceptedTx,
    op: &RegistryOp,
    out: &mut ApplyOutcome,
) -> Result<()> {
    let bounds: [[u8; 32]; 2] = match op {
        RegistryOp::Split { gap, .. } => [gap.lo, gap.hi],
        RegistryOp::Release { pred, succ, .. } | RegistryOp::Evict { pred, succ, .. } => [pred.lo, succ.hi],
        _ => [KEY_MIN, KEY_MAX],
    };
    for bound in bounds {
        if bound == KEY_MIN || bound == KEY_MAX || db::get_deed(conn, &bound).await?.is_some() {
            continue;
        }
        let discovered = DeedRow::owner_unknown(None);
        db::upsert_deed(conn, &bound, &discovered).await?;
        let seq = event_seq(*out)?;
        db::append_event(conn, &bare_event(block, seq, bound, Vec::new())).await?;
        // Without this, the key's history opens on a transfer of a name nobody saw registered.
        db::append_history(conn, &history_row(block, tx, seq, bound, HistoryOp::Discover, Some(discovered), CardChange::None)).await?;
        out.events += 1;
    }
    Ok(())
}

/// `None` for a transfer whose name does not decode. A failed batch halts the pipeline after its
/// retries, so the self-test arbitrates.
fn derive_change(params: &Params, block: &ChainBlock, tx: &AcceptedTx, op: RegistryOp) -> Option<Change> {
    let outpoint = |index: u32| (tx.txid, index);
    let is_evict = matches!(op, RegistryOp::Evict { .. });
    // From the decoded operation, never inferred from row images, which the audit can correct.
    let history_op = match &op {
        RegistryOp::Split { .. } => HistoryOp::Register,
        RegistryOp::Activate { .. } => HistoryOp::Activate,
        RegistryOp::Transfer { .. } => HistoryOp::Transfer,
        RegistryOp::Release { .. } => HistoryOp::Release,
        RegistryOp::Evict { .. } => HistoryOp::Evict,
    };
    let into_escrow = matches!(op, RegistryOp::Transfer { new_owner_type: dotk_core::OwnerType::CovenantId, .. });
    let (key, new, gaps) = match op {
        RegistryOp::Split { gap, new_key, claim } => (
            new_key,
            Some(DeedRow::pending(claim, outpoint(2), params.bond + params.deposit, block.daa_score)),
            vec![
                GapWrite { lo: gap.lo, new: Some(GapRow::observed(new_key, outpoint(0))) },
                GapWrite { lo: new_key, new: Some(GapRow::observed(gap.hi, outpoint(1))) },
            ],
        ),
        RegistryOp::Activate { spent, name, owner_type, owner } => {
            (spent.key, Some(DeedRow::active(name, owner_type as u8, owner, outpoint(0), params.bond)), vec![])
        }
        // The spent deed carries the name, so a missing row recreates fully.
        RegistryOp::Transfer { spent, new_owner_type, new_owner } => match dotk_core::names::name_from_padded(&spent.name) {
            Ok(name) => (spent.key, Some(DeedRow::active(name, new_owner_type as u8, new_owner, outpoint(0), params.bond)), vec![]),
            Err(e) => {
                log::error!("transfer in tx {} carries an undecodable name ({e}), scheduling a self-test", hex32(&tx.txid));
                return None;
            }
        },
        RegistryOp::Release { pred, dying, succ } | RegistryOp::Evict { pred, dying, succ } => (
            dying.key,
            None,
            vec![GapWrite { lo: pred.lo, new: Some(GapRow::observed(succ.hi, outpoint(0))) }, GapWrite { lo: dying.key, new: None }],
        ),
    };
    Some(Change { key, new, gaps, history_op, into_escrow, is_evict })
}

/// `swept_at` is compared, not read as a flag, because the probe stamps the node's tip, which is
/// ahead of the pipeline. This runs before `mark_sweeps`, because an update sweeps the card it
/// replaces in this transaction.
async fn card_live_before(conn: &mut PgConnection, prev: Option<&DeedRow>, key: &[u8; 32], blue_score: u64) -> Result<bool> {
    Ok(match prev.filter(|p| p.kind == RowKind::Active).and_then(|p| p.outpoint_txid) {
        Some(txid) => {
            db::get_card(conn, &txid, 1).await?.is_some_and(|c| c.state.key == *key && c.swept_at.is_none_or(|at| at >= blue_score))
        }
        None => false,
    })
}

/// An ACTIVE row keeps the registration time of its split.
fn carry_accepted_daa(new: &mut Option<DeedRow>, prev: Option<&DeedRow>) {
    if let Some(row) = new.as_mut().filter(|r| r.kind == RowKind::Active) {
        row.accepted_daa = row.accepted_daa.or_else(|| prev.and_then(|p| p.accepted_daa));
    }
}

async fn gap_images(conn: &mut PgConnection, gaps: &[GapWrite]) -> Result<Vec<GapImage>> {
    let mut images = Vec::with_capacity(gaps.len());
    for write in gaps {
        images.push((write.lo, db::get_gap(conn, &write.lo).await?));
    }
    Ok(images)
}

struct CardWrites {
    marks: Vec<CardMark>,
    swept_live: Vec<SweptLive>,
    minted: bool,
}

/// Card writes are idempotent, so they can go before the duplicate check.
async fn write_cards(conn: &mut PgConnection, block: &ChainBlock, tx: &AcceptedTx, change: &Change) -> Result<CardWrites> {
    let (mut marks, swept_live) = mark_sweeps(conn, block, tx).await?;
    let mut minted = false;
    if matches!(change.history_op, HistoryOp::Transfer) && !change.into_escrow {
        let mint = insert_mint(conn, tx, &change.key).await?;
        minted = mint.minted;
        marks.extend(mint.mark);
    } else if change.into_escrow && !tx.payload.is_empty() {
        log::warn!("transfer {} into escrow declares a card, ignored", hex32(&tx.txid));
    }
    Ok(CardWrites { marks, swept_live, minted })
}

async fn write_rows(conn: &mut PgConnection, change: &Change, had_row: bool) -> Result<()> {
    match &change.new {
        Some(row) => db::upsert_deed(conn, &change.key, row).await?,
        None if had_row => db::delete_deed(conn, &change.key).await?,
        None => {}
    }
    for write in &change.gaps {
        match &write.new {
            Some(row) => db::upsert_gap(conn, &write.lo, row).await?,
            None => db::delete_gap(conn, &write.lo).await?,
        }
    }
    Ok(())
}

async fn journal_event(
    conn: &mut PgConnection,
    block: &ChainBlock,
    tx: &AcceptedTx,
    change: &Change,
    journal: &Journal,
) -> Result<()> {
    let (key, seq) = (change.key, journal.event.seq);
    db::append_event(conn, &journal.event).await?;
    // After the journal entry, because undo deletes history only where a journal row exists.
    append_sweep_history(conn, block, tx, seq, &journal.swept_live, Some(key)).await?;
    // History stores the post-image, so reading it never depends on state the audit can correct.
    let row = history_row(block, tx, seq, key, change.history_op, change.new.clone(), journal.card);
    db::append_history(conn, &row).await
}

/// `mark` is set only when this delivery inserted the row, because a duplicate delivery
/// journals nothing while `minted` stays true.
struct Mint {
    minted: bool,
    mark: Option<CardMark>,
}

impl Mint {
    const NONE: Self = Self { minted: false, mark: None };
}

/// A bad card payload is skipped and the transfer stands, because a card never fails a block.
async fn insert_mint(conn: &mut PgConnection, tx: &AcceptedTx, key: &[u8; 32]) -> Result<Mint> {
    let txid = || hex32(&tx.txid);
    let mint = match cards::decode_payload(&tx.payload) {
        Ok(Some(mint)) => mint,
        Ok(None) => return Ok(Mint::NONE),
        Err(e) => {
            log::warn!("transfer {} carries a malformed card payload: {e}", txid());
            return Ok(Mint::NONE);
        }
    };
    if mint.state.key != *key {
        log::warn!("transfer {} declares a card for another key, ignored", txid());
        return Ok(Mint::NONE);
    }
    let Some((value, spk)) = tx.outputs.get(1) else {
        log::warn!("transfer {} declares a card but carries no output 1, ignored", txid());
        return Ok(Mint::NONE);
    };
    if *spk != mint.state.spk() {
        log::warn!("transfer {} declares a card its output 1 does not pay to, ignored", txid());
        return Ok(Mint::NONE);
    }
    let row = CardRow { txid: tx.txid, idx: 1, state: mint.state, blob: mint.blob, value: *value, swept_at: None };
    let inserted = db::insert_card_if_absent(conn, &row).await?;
    Ok(Mint { minted: true, mark: inserted.then_some(CardMark { txid: row.txid, idx: row.idx, existed: false, swept_at: None }) })
}

/// A swept card that was live. Only a live sweep removes a name's records.
struct SweptLive {
    key: [u8; 32],
    deed: DeedRow,
}

/// Mark swept every card this transaction spends. A mark already there is the probe's or an
/// import's, because the chain spends an outpoint once and undo restores the stream's own, so
/// the stream stamps and journals its sweep whatever it finds.
///
/// The deed rows still hold their pre-event images here, which liveness needs.
async fn mark_sweeps(conn: &mut PgConnection, block: &ChainBlock, tx: &AcceptedTx) -> Result<(Vec<CardMark>, Vec<SweptLive>)> {
    let mut marks = Vec::new();
    let mut live = Vec::new();
    for (i, sig) in tx.sig_scripts.iter().enumerate() {
        if CardState::swept_by(sig).is_none() {
            continue;
        }
        let Some(&(txid, idx)) = tx.spent.get(i) else { continue };
        let Some(card) = db::get_card(conn, &txid, idx).await? else { continue };
        db::set_card_swept(conn, &txid, idx, Some(block.blue_score)).await?;
        marks.push(CardMark { txid, idx, existed: true, swept_at: card.swept_at });
        if idx == 1
            && let Some(deed) = db::get_deed(conn, &card.state.key).await?
            && deed.kind == RowKind::Active
            && deed.outpoint_txid == Some(txid)
        {
            live.push(SweptLive { key: card.state.key, deed });
        }
    }
    Ok((marks, live))
}

/// One `sweep` entry per name whose live card the transaction spent. Only the stream writes
/// these, because a probe sweep has no transaction for a reader to check and no block to undo.
async fn append_sweep_history(
    conn: &mut PgConnection,
    block: &ChainBlock,
    tx: &AcceptedTx,
    seq: i32,
    swept: &[SweptLive],
    skip: Option<[u8; 32]>,
) -> Result<()> {
    for card in swept {
        if Some(card.key) == skip {
            continue;
        }
        let row = history_row(block, tx, seq, card.key, HistoryOp::Sweep, Some(card.deed.clone()), CardChange::Deleted);
        db::append_history(conn, &row).await?;
    }
    Ok(())
}

/// Journaled under `KEY_MIN` with no deed pre-image, so undo only clears the marks. Under the
/// card's own key, undo writes the deed row back over any self-test repair.
async fn apply_standalone_sweeps(conn: &mut PgConnection, block: &ChainBlock, tx: &AcceptedTx, out: &mut ApplyOutcome) -> Result<()> {
    if !tx.sig_scripts.iter().any(|s| CardState::swept_by(s).is_some()) {
        return Ok(());
    }
    let seq = event_seq(*out)?;
    if db::event_exists(conn, &block.hash, seq).await? {
        return Ok(());
    }
    let (marks, swept_live) = mark_sweeps(conn, block, tx).await?;
    if marks.is_empty() {
        return Ok(());
    }
    db::append_event(conn, &bare_event(block, seq, KEY_MIN, marks)).await?;
    append_sweep_history(conn, block, tx, seq, &swept_live, None).await?;
    out.events += 1;
    Ok(())
}

/// An event with no deed or gap pre-image.
fn bare_event(block: &ChainBlock, seq: i32, key: [u8; 32], prev_cards: Vec<CardMark>) -> db::EventRow {
    db::EventRow { block_hash: block.hash, seq, blue_score: block.blue_score, key, prev: None, prev_gaps: Vec::new(), prev_cards }
}

fn history_row(
    block: &ChainBlock,
    tx: &AcceptedTx,
    seq: i32,
    key: [u8; 32],
    op: HistoryOp,
    state: Option<DeedRow>,
    card: CardChange,
) -> HistoryRow {
    HistoryRow {
        key,
        op,
        card,
        seq,
        blue_score: block.blue_score,
        daa_score: block.daa_score,
        block_time: block.timestamp,
        block_hash: block.hash,
        txid: tx.txid,
        state,
    }
}

pub async fn undo_block(conn: &mut PgConnection, block_hash: &[u8; 32]) -> Result<u32> {
    let rows = db::events_for_block_desc(conn, block_hash).await?;
    let n = u32::try_from(rows.len()).context("a block journals more than u32::MAX events")?;
    for ev in rows {
        match ev.prev {
            Some(prev) => db::upsert_deed(conn, &ev.key, &prev).await?,
            None => db::delete_deed(conn, &ev.key).await?,
        }
        for (lo, before) in ev.prev_gaps {
            match before {
                Some(row) => db::upsert_gap(conn, &lo, &row).await?,
                // "Absent" is a real pre-image, because a split creates a row at the new key.
                None => db::delete_gap(conn, &lo).await?,
            }
        }
        for mark in ev.prev_cards {
            if mark.existed {
                db::set_card_swept(conn, &mark.txid, mark.idx, mark.swept_at).await?;
            } else {
                db::delete_card(conn, &mark.txid, mark.idx).await?;
            }
        }
    }
    if n > 0 {
        db::delete_events_for_block(conn, block_hash).await?;
        // Only with the state undo. A block with a pruned journal keeps its history.
        db::delete_history_for_block(conn, block_hash).await?;
    }
    Ok(n)
}
