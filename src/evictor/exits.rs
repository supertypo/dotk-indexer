//! Builds, funds and submits one exit, whose bounty excludes the deposit the covenant pins to the devfund.

use anyhow::Result;
use dotk_core::assemble::{AssembledTx, FundingUtxo};
use dotk_core::fees::{assemble_unfunded_evict_with_auto_fee, assemble_with_auto_fee};
use dotk_core::intents::{DeedUtxo, GapUtxo, Outpoint, TxIntent};
use dotk_core::state::{DeedState, GapState, Status};

use super::Exit;
use super::payout::{Payout, Wallet, sign_funding};
use crate::app::{App, Backends};
use crate::audit::probe_addresses;
use crate::chain::UtxoHit;
use crate::convert::hex32;
use crate::db;
use crate::derive;

/// The deed and its two flanking gaps, as derived from the table.
type ExitStates = (DeedState, GapState, GapState);

async fn exit_shape(
    backends: &Backends,
    key: &[u8; 32],
    pick: impl Fn(&derive::Neighborhood) -> Option<ExitStates>,
) -> Result<Option<ExitStates>> {
    let mut conn = backends.db.acquire().await?;
    let n = db::adjacent_rows(&mut conn, key).await?;
    Ok(pick(&derive::neighborhood(key, n.at.as_ref(), n.pred.as_ref(), n.succ.as_ref())))
}

/// `None` for a free key, or for an owner-unknown row, whose state cannot be hashed.
fn shape(n: &derive::Neighborhood) -> Option<ExitStates> {
    let deed = n.deed?;
    let (pred, succ) = n.neighbors?;
    Some((deed, pred, succ))
}

/// The owner's activate can land between the scan and this lookup, and that lost race stays silent.
fn evictable(n: &derive::Neighborhood) -> Option<ExitStates> {
    shape(n).filter(|(deed, _, _)| deed.status == Status::Pending)
}

/// Exposure changes with the gap bounds, so `build_stranger_release` derives the witness again
/// and refuses a stale state before it costs a fee.
pub(super) async fn attempt(
    app: &App,
    key: &[u8; 32],
    payout: &Payout,
    wallet: &mut Option<Wallet>,
    market: dotk_core::fees::Market,
    ripe_before: u64,
    exit: Exit,
) -> Result<()> {
    let pick: fn(&derive::Neighborhood) -> Option<ExitStates> = match exit {
        Exit::Evict => evictable,
        Exit::StrangerRelease => shape,
    };
    let Some(states) = exit_shape(&app.backends, key, pick).await? else {
        return Ok(());
    };
    if exit == Exit::StrangerRelease {
        let (deed_state, pred_state, succ_state) = &states;
        let Some(seat) = app.deployment.watch.exposed_flank(deed_state, pred_state, succ_state) else {
            return Ok(());
        };
        log::info!(
            "{} names its own {seat:?} gap as owner, a name only a stranger can end. Releasing at seat {}",
            hex32(key),
            seat.index()
        );
    }
    let (watch, params) = (&app.deployment.watch, &app.deployment.genesis.params);
    let at = Attempt { app, key, payout, market, ripe_before };
    submit_exit(&at, wallet, states, |pred, deed, succ| {
        match exit {
            Exit::Evict => watch.build_evict(pred, deed, succ, params),
            Exit::StrangerRelease => watch.build_stranger_release(pred, deed, succ, params),
        }
        .map_err(anyhow::Error::msg)
    })
    .await
}

struct Attempt<'a> {
    app: &'a App,
    key: &'a [u8; 32],
    payout: &'a Payout,
    market: dotk_core::fees::Market,
    ripe_before: u64,
}

async fn submit_exit(
    at: &Attempt<'_>,
    wallet: &mut Option<Wallet>,
    states: ExitStates,
    build: impl Fn(&GapUtxo, &DeedUtxo, &GapUtxo) -> Result<TxIntent>,
) -> Result<()> {
    let funding = wallet.as_ref().and_then(|w| w.peek()).cloned();
    let Some((pred, deed, succ)) = locate(at, states).await? else {
        return Ok(());
    };
    let intent = build(&pred, &deed, &succ)?;
    let assembled = assemble(at.payout, &at.app.deployment.args.network, at.market, &intent, funding.as_ref())?;
    ensure_within_bounty(&assembled, &intent)?;
    // The input counts as spent whatever the node answers, so a rejection never reuses it in this tick.
    if let Some(w) = wallet.as_mut() {
        w.commit();
    }
    let (what, key) = (&intent.kind, hex32(at.key));
    match at.app.backends.kaspad.submit(&assembled.tx).await {
        Ok(txid) => log::info!("{what} of {key} accepted: bounty {} sompi, tx {txid}", intent.released),
        Err(e) if lost_race(&e) => log::debug!("{what} for {key} not accepted ({e:#}): already in the mempool, or the race is lost"),
        Err(e) => log::warn!("{what} for {key} not accepted: {e:#}"),
    }
    Ok(())
}

/// The node reports a transaction it already holds, or whose input another spent, only in its
/// message text.
fn lost_race(e: &anyhow::Error) -> bool {
    let m = format!("{e:#}");
    ["already accepted by the consensus", "already in the mempool", "already spent by transaction", "orphan is disallowed"]
        .iter()
        .any(|phrase| m.contains(phrase))
}

/// The three UTXOs as the chain holds them, or `None` when the exit is not due this tick.
async fn locate(at: &Attempt<'_>, (deed_state, pred_state, succ_state): ExitStates) -> Result<Option<(GapUtxo, DeedUtxo, GapUtxo)>> {
    let app = at.app;
    let deed_addr = app.deployment.deed_address(&deed_state)?;
    let pred_addr = app.deployment.gap_address(&pred_state)?;
    let succ_addr = app.deployment.gap_address(&succ_state)?;
    let node = app.backends.kaspad.pin().await?;
    let hits = probe_addresses(app, &*node, &[deed_addr.clone(), pred_addr.clone(), succ_addr.clone()]).await?;
    let Some(deed_hit) = hits.get(&deed_addr.to_string()) else {
        // The deed already moved, and the stream event cleans up the row.
        return Ok(None);
    };
    if deed_state.status == Status::Pending && deed_hit.block_daa_score > at.ripe_before {
        return Ok(None);
    }
    let (Some(pred_hit), Some(succ_hit)) = (hits.get(&pred_addr.to_string()), hits.get(&succ_addr.to_string())) else {
        log::info!("a gap flanking {} is not at its derived address, retrying next tick", hex32(at.key));
        return Ok(None);
    };
    let covenant_id = &app.deployment.genesis.registry_covenant_id;
    let outpoint = |hit: &UtxoHit| Outpoint { transaction_id: hex32(&hit.txid), index: hit.index };
    let gap_utxo =
        |hit: &UtxoHit, state| GapUtxo { outpoint: outpoint(hit), value: hit.amount, state, covenant_id: covenant_id.clone() };
    let deed = DeedUtxo { outpoint: outpoint(deed_hit), value: deed_hit.amount, state: deed_state, covenant_id: covenant_id.clone() };
    Ok(Some((gap_utxo(pred_hit, pred_state), deed, gap_utxo(succ_hit, succ_state))))
}

/// Matches on the secret, not on the funding input. A key-funded exit that lost its input must
/// fail, not fall through to the unfunded builder.
pub(super) fn assemble(
    payout: &Payout,
    network: &str,
    market: dotk_core::fees::Market,
    intent: &TxIntent,
    funding: Option<&FundingUtxo>,
) -> Result<AssembledTx> {
    match (&payout.secret, funding) {
        (Some(secret), Some(funding)) => {
            let mut assembled = assemble_with_auto_fee(intent, std::slice::from_ref(funding), &payout.spk_hex, network, market)?;
            sign_funding(&mut assembled, secret)?;
            Ok(assembled)
        }
        (Some(_), None) => anyhow::bail!("a key-funded {} reached assembly with no funding input", intent.kind),
        (None, _) => Ok(assemble_unfunded_evict_with_auto_fee(intent, &payout.spk_hex, network, market)?),
    }
}

/// Only this check keeps the fee below the bounty. `MAX_FEE_SOMPI` is per transaction, so without
/// this a broken feerate quote burns the wallet one exit at a time.
fn ensure_within_bounty(assembled: &AssembledTx, intent: &TxIntent) -> Result<()> {
    let outlay =
        assembled.entries.iter().map(|e| e.amount).sum::<u64>().saturating_sub(assembled.tx.outputs.iter().map(|o| o.value).sum());
    if outlay > intent.released {
        anyhow::bail!(
            "this {} pays {outlay} sompi to collect a bounty of {}. The feerate this node quoted makes it \
             cost more than it frees, so it is not submitted",
            intent.kind,
            intent.released
        );
    }
    Ok(())
}

#[cfg(test)]
#[path = "exits_tests.rs"]
mod tests;
