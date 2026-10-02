//! Probes the derived addresses of the registry and the cards against the node.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::Instant;

use anyhow::{Context, Result};
use dotk_core::state::GapState;
use kaspa_addresses::Address;
use sqlx::PgConnection;

use super::selftest::{CheckResult, Flux, Pass, ensure_running};
use crate::app::{App, Deployment};
use crate::chain::{Node, UtxoHit};
use crate::db;
use crate::derive;
use crate::model::{CardRow, DeedRow, GapRow, RowKind};

/// `daa` is the containing block's DAA score, which replaces a refuted `accepted_daa`. It is
/// within tip distance of the accepting score, which the eviction maturity horizon dwarfs.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Observed {
    pub(super) txid: [u8; 32],
    pub(super) index: u32,
    pub(super) value: u64,
    pub(super) daa: u64,
}

/// A missing card UTXO marks the card swept without confirmation rounds, because the next pass
/// clears the mark if the UTXO returns, and a row leaves only after it stays marked past the
/// journal retention. Returns (swept, restored).
pub(super) async fn probe_cards(pass: &mut Pass<'_>) -> Result<(u64, u64)> {
    let app = pass.app;
    // Only saves a wasted probe. The gate before the writes, with the conditional updates, keeps
    // a pass that fell behind from writing.
    pass.gate().await?;
    let mut conn = app.backends.db.acquire().await?;
    let rows = db::all_cards(&mut conn).await?;
    if rows.is_empty() {
        return Ok((0, 0));
    }
    // Keyed by outpoint, never by address. A record set saved again unchanged mints a card at the
    // address of the one it sweeps, and anyone can pay to a published card address.
    let addrs: Vec<Address> = rows.iter().map(|c| c.state.address(app.deployment.prefix)).collect();
    let mut found: HashMap<([u8; 32], u32), u64> = HashMap::new();
    for chunk in addrs.chunks(probe_chunk(&app.deployment)?) {
        ensure_running(&app.progress)?;
        for hit in pass.node.utxos_by_addresses(chunk).await? {
            found.insert((hit.txid, hit.index), hit.amount);
        }
    }
    if found.is_empty() && rows.iter().any(|c| c.swept_at.is_none()) {
        ensure_registry_answers(pass, &mut conn, rows.len()).await?;
    }
    pass.gate().await?;
    reconcile_cards(&mut conn, &rows, &found, pass.sink).await
}

async fn reconcile_cards(
    conn: &mut PgConnection,
    rows: &[CardRow],
    found: &HashMap<([u8; 32], u32), u64>,
    now: u64,
) -> Result<(u64, u64)> {
    let (mut swept, mut restored, mut rewritten) = (0u64, 0u64, 0u64);
    for card in rows {
        let hit = found.get(&(card.txid, card.idx)).copied();
        if let Some(amount) = hit.filter(|amount| *amount != card.value)
            && db::update_card_value_if_matches(conn, &card.txid, card.idx, card.value, amount).await?
        {
            rewritten += 1;
        }
        let present = hit.is_some();
        let to = match (present, card.swept_at) {
            (false, None) => Some(now),
            (true, Some(_)) => None,
            _ => continue,
        };
        if db::mark_card_if_matches(conn, &card.txid, card.idx, card.swept_at, to).await? {
            if present { restored += 1 } else { swept += 1 }
        }
    }
    if swept + restored + rewritten > 0 {
        log::info!("self-test: {swept} card(s) marked swept, {restored} restored, {rewritten} value(s) rewritten");
    }
    Ok((swept, restored))
}

/// A card carries no covenant id, so the registry's gaps, of which a live registry always holds
/// one, tell an empty answer from a broken probe.
async fn ensure_registry_answers(pass: &Pass<'_>, conn: &mut PgConnection, cards: usize) -> Result<()> {
    let app = pass.app;
    let deeds = db::all_deeds(conn).await?;
    let gaps: Vec<Address> = derive::derive_gaps(&deeds).iter().map(|g| app.deployment.gap_address(g)).collect::<Result<_>>()?;
    if probe_counted(app, &*pass.node, &gaps).await?.1.hits == 0 {
        anyhow::bail!("the node answered nothing for {cards} card(s) and nothing for the registry, so the card probe is unusable");
    }
    Ok(())
}

/// Gaps are a pure function of the deed key set, so a live gap at every derived address proves
/// completeness. Provenance is compared too, so the proof covers every exported byte.
pub(super) async fn probe_all(pass: &Pass<'_>, rows: &[([u8; 32], DeedRow)]) -> Result<CheckResult> {
    let app = pass.app;
    let targets = Targets::derive(&app.deployment, rows)?;
    let (hits, raw) = probe_counted(app, &*pass.node, &targets.addrs).await?;
    ensure_usable(&app.deployment, &hits, &raw)?;
    let (observed_gaps, failing_gaps) = observe_gaps(&targets.gap_by_addr, &hits);
    let mut failing_deeds = targets.failing_deeds;
    failing_deeds.extend(targets.deed_by_addr.iter().filter(|(a, _)| !hits.contains_key(*a)).map(|(_, k)| *k));
    let (flux, failing_outpoints) = check_outpoints(app, rows, &targets.addr_of_key, &hits);
    Ok(CheckResult {
        failing_gaps,
        failing_deeds,
        failing_outpoints,
        flux,
        observed_gaps,
        gaps_checked: u64::try_from(targets.gaps).context("gap count exceeds u64")?,
        deeds_checked: targets.deeds_checked,
        pending_checked: targets.pending_checked,
    })
}

struct Targets {
    gaps: usize,
    addrs: Vec<Address>,
    gap_by_addr: HashMap<String, GapState>,
    deed_by_addr: HashMap<String, [u8; 32]>,
    addr_of_key: HashMap<[u8; 32], String>,
    failing_deeds: HashSet<[u8; 32]>,
    deeds_checked: u64,
    pending_checked: u64,
}

impl Targets {
    fn derive(deployment: &Deployment, rows: &[([u8; 32], DeedRow)]) -> Result<Self> {
        let gaps = derive::derive_gaps(rows);
        let mut targets = Targets {
            gaps: gaps.len(),
            addrs: Vec::with_capacity(gaps.len() + rows.len()),
            gap_by_addr: HashMap::new(),
            deed_by_addr: HashMap::new(),
            addr_of_key: HashMap::new(),
            failing_deeds: HashSet::new(),
            deeds_checked: 0,
            pending_checked: 0,
        };
        for gap in &gaps {
            let addr = deployment.gap_address(gap)?;
            targets.gap_by_addr.insert(addr.to_string(), *gap);
            targets.addrs.push(addr);
        }
        for (key, row) in rows {
            match row.deed_state(*key) {
                Some(state) => {
                    let addr = deployment.deed_address(&state)?;
                    targets.addr_of_key.insert(*key, addr.to_string());
                    targets.deed_by_addr.insert(addr.to_string(), *key);
                    targets.addrs.push(addr);
                    targets.deeds_checked += 1;
                    targets.pending_checked += u64::from(row.kind == RowKind::Pending);
                }
                // The gaps around an owner-unknown row prove it. Any other row without an address
                // is corrupt, and repair handles it.
                None if row.kind != RowKind::OwnerUnknown => {
                    targets.failing_deeds.insert(*key);
                }
                None => {}
            }
        }
        Ok(targets)
    }
}

/// Every registry UTXO carries the covenant id, so hits without ours mean the answer is unusable,
/// and a repair against it deletes the whole registry. Zero hits is not a fault, because a cold
/// start against an aged registry probes empty addresses.
fn ensure_usable(deployment: &Deployment, hits: &HashMap<String, UtxoHit>, raw: &RawProbe) -> Result<()> {
    if raw.hits > 0 && hits.is_empty() {
        anyhow::bail!(
            "the node returned {} UTXO(s) at our derived addresses and not one carries the \
             registry covenant id {} ({} carried any covenant id at all), so the probe is unusable, \
             refusing to repair against it. Either this node does not report covenant ids, or \
             the manifest's registryCovenantId is for a different registry.",
            raw.hits,
            deployment.genesis.registry_covenant_id,
            raw.with_covenant_id
        );
    }
    Ok(())
}

type GapObservation = (Vec<([u8; 32], GapRow)>, BTreeMap<[u8; 32], GapState>);

fn observe_gaps(gap_by_addr: &HashMap<String, GapState>, hits: &HashMap<String, UtxoHit>) -> GapObservation {
    let mut observed = Vec::with_capacity(gap_by_addr.len());
    let mut failing = BTreeMap::new();
    for (addr, g) in gap_by_addr {
        match hits.get(addr) {
            Some(hit) => observed.push((g.lo, GapRow::observed(g.hi, (hit.txid, hit.index)))),
            None => {
                failing.insert(g.lo, *g);
            }
        }
    }
    (observed, failing)
}

/// A refuted provenance whose UTXO lies above the pipeline's last commit is the stream's to
/// write, and a rewrite would enter the event's journal as a pre-image the chain never held.
fn check_outpoints(
    app: &App,
    rows: &[([u8; 32], DeedRow)],
    addr_of_key: &HashMap<[u8; 32], String>,
    hits: &HashMap<String, UtxoHit>,
) -> (Flux, HashMap<[u8; 32], Observed>) {
    let committed_daa = app.progress.last_block_daa_score.load(Ordering::Relaxed);
    let mut flux = Flux::default();
    let mut failing = HashMap::new();
    for (key, row) in rows {
        let Some(hit) = addr_of_key.get(key).and_then(|a| hits.get(a)) else { continue };
        let seen = Observed { txid: hit.txid, index: hit.index, value: hit.amount, daa: hit.block_daa_score };
        let stored = row.outpoint_txid.zip(row.outpoint_index).zip(row.value).map(|((t, i), v)| (t, i, v));
        if stored == Some((seen.txid, seen.index, seen.value)) && !clock_refuted(row, seen.daa) {
            continue;
        }
        if seen.daa > committed_daa {
            flux.outpoints.insert(*key);
        } else {
            failing.insert(*key, seen);
        }
    }
    (flux, failing)
}

/// A PENDING deed's UTXO is the split's own output, so its DAA is the stored one exactly, and it
/// is the evictor's clock, so it is proven. An ACTIVE row's UTXO is a later transaction's, so its
/// column stays a claim that the chain only bounds.
fn clock_refuted(row: &DeedRow, seen_daa: u64) -> bool {
    match row.kind {
        RowKind::Pending => row.accepted_daa != Some(seen_daa),
        RowKind::Active => row.accepted_daa.is_some_and(|daa| daa > seen_daa),
        RowKind::OwnerUnknown => false,
    }
}

fn probe_chunk(deployment: &Deployment) -> Result<usize> {
    usize::try_from(deployment.args.selftest_probe_chunk).context("--selftest-probe-chunk exceeds usize")
}

/// Keeps only the hits that carry the registry covenant id, keyed by address.
pub(crate) async fn probe_addresses(app: &App, node: &dyn Node, addrs: &[Address]) -> Result<HashMap<String, UtxoHit>> {
    Ok(probe_counted(app, node, addrs).await?.0)
}

/// The counts before the covenant id filter, which tell a chain answer from a broken probe.
#[derive(Default)]
struct RawProbe {
    hits: usize,
    with_covenant_id: usize,
}

struct Probe {
    out: HashMap<String, UtxoHit>,
    raw: RawProbe,
    answered: usize,
    last_log: Instant,
}

impl Probe {
    fn new() -> Self {
        Self { out: HashMap::new(), raw: RawProbe::default(), answered: 0, last_log: Instant::now() }
    }

    fn log_progress(&mut self, deployment: &Deployment, total: usize) {
        if self.answered == total || self.last_log.elapsed() < deployment.args.progress_interval {
            return;
        }
        self.last_log = Instant::now();
        log::info!(
            "self-test: {}/{total} addresses answered ({}%), {} registry UTXO(s)",
            self.answered,
            self.answered * 100 / total,
            self.out.len(),
        );
    }
}

/// All or nothing. In a partial probe every missing address reads as a blind spot or a dead
/// deed, and repair then deletes rows and clears ownership.
async fn probe_counted(app: &App, node: &dyn Node, addrs: &[Address]) -> Result<(HashMap<String, UtxoHit>, RawProbe)> {
    let total = addrs.len();
    let mut probe = Probe::new();
    for chunk in addrs.chunks(probe_chunk(&app.deployment)?) {
        ensure_running(&app.progress)?;
        for hit in node.utxos_by_addresses(chunk).await? {
            probe.raw.hits += 1;
            probe.raw.with_covenant_id += usize::from(hit.covenant_id.is_some());
            if hit.covenant_id.as_deref() == Some(app.deployment.genesis.registry_covenant_id.as_str()) {
                probe.out.insert(hit.address.clone(), hit);
            }
        }
        probe.answered += chunk.len();
        probe.log_progress(&app.deployment, total);
    }
    Ok((probe.out, probe.raw))
}

/// Reconciles the gaps cache from observation, outside the verdict, because no fact the snapshot
/// or the proof rests on lives in the cache. Every write is guarded like a repair, and a row the
/// stream rewrote since the pass started is left alone, because the observation predates it.
pub(super) async fn sync_gap_cache(pass: &mut Pass<'_>, check: &CheckResult, since: u64) -> Result<u64> {
    pass.gate().await?;
    let mut conn = pass.app.backends.db.acquire().await?;
    let cached: HashMap<[u8; 32], GapRow> = db::all_gaps(&mut conn).await?.into_iter().collect();
    let los: Vec<[u8; 32]> = check.observed_gaps.iter().map(|(lo, _)| *lo).chain(cached.keys().copied()).collect();
    let touched = db::gaps_touched_since(&mut conn, &los, since).await?;
    let mut synced = 0u64;
    for (lo, want) in &check.observed_gaps {
        let have = cached.get(lo);
        if have == Some(want) || touched.contains(lo) {
            continue;
        }
        if db::upsert_gap_if_matches(&mut conn, lo, have, want).await? {
            synced += 1;
        }
    }
    let derived: HashSet<[u8; 32]> = check
        .observed_gaps
        .iter()
        .map(|(lo, _)| *lo)
        .chain(check.failing_gaps.keys().copied())
        .chain(check.flux.gaps.keys().copied())
        .collect();
    for (lo, row) in &cached {
        if !derived.contains(lo) && !touched.contains(lo) && db::delete_gap_if_matches(&mut conn, lo, row).await? {
            synced += 1;
        }
    }
    Ok(synced)
}
