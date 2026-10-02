//! Repairs a confirmed deviation between the table and the chain.

use std::collections::HashSet;

use anyhow::Result;
use dotk_core::registry::{KEY_MAX, KEY_MIN};

use super::probe::{probe_addresses, probe_all};
use super::selftest::{CheckResult, Pass};
use crate::app::RepairSummary;
use crate::convert::hex32;
use crate::db;
use crate::derive;
use crate::model::{DeedRow, RowKind};

const MAX_REPAIR_ITERATIONS: u32 = 16;

#[derive(Clone, Copy)]
enum Repair {
    Drop,
    Demote,
}

/// A row the stream touched since the probe fails the guard and is spared for the next pass.
async fn apply_repair(pass: &mut Pass<'_>, op: Repair, targets: &[([u8; 32], DeedRow)], total: &mut u64) -> Result<bool> {
    if targets.is_empty() {
        return Ok(false);
    }
    pass.gate().await?;
    let mut conn = pass.app.backends.db.acquire().await?;
    let mut applied = 0u64;
    for (key, probed) in targets {
        let landed = match op {
            Repair::Drop => db::delete_deed_if_matches(&mut conn, key, probed).await?,
            Repair::Demote => db::demote_deed_if_matches(&mut conn, key, probed).await?,
        };
        if landed {
            applied += 1;
        } else {
            log::info!("repair: {} changed under the audit, so sparing it", hex32(key));
        }
    }
    match op {
        Repair::Drop => {
            log::info!("repair: dropped {applied} of {} row(s) whose key the chain shows unregistered", targets.len());
        }
        Repair::Demote => {
            log::info!("repair: demoted {applied} of {} row(s) whose ownership the chain refuted", targets.len());
        }
    }
    *total += applied;
    Ok(applied > 0)
}

#[derive(Default)]
struct ChainVerdicts {
    registered: HashSet<[u8; 32]>,
    unregistered: HashSet<[u8; 32]>,
}

/// Two batched rounds whose answers do not depend on the suspect rows. Both are sound but
/// one-sided, so a suspect can end up in neither set.
async fn ask_the_chain(pass: &Pass<'_>, rows: &[([u8; 32], DeedRow)], suspects: &[[u8; 32]]) -> Result<ChainVerdicts> {
    if suspects.is_empty() {
        return Ok(ChainVerdicts::default());
    }
    let keys: Vec<[u8; 32]> = rows.iter().map(|(k, _)| *k).collect();
    let suspect: HashSet<[u8; 32]> = suspects.iter().copied().collect();
    let registered = prove_registered(pass, &keys, &suspect).await?;
    let unresolved: HashSet<[u8; 32]> = suspect.difference(&registered).copied().collect();
    let unregistered = prove_unregistered(pass, &keys, &unresolved).await?;
    Ok(ChainVerdicts { registered, unregistered })
}

/// Probes the two gaps each key implies, measured from the nearest keys that are not suspect.
/// Deed keys or the keyspace ends bound every live gap, so one live flank of a key strictly
/// inside the keyspace proves it registered.
async fn prove_registered(pass: &Pass<'_>, keys: &[[u8; 32]], suspect: &HashSet<[u8; 32]>) -> Result<HashSet<[u8; 32]>> {
    let app = pass.app;
    let mut addrs = Vec::with_capacity(suspect.len() * 2);
    let mut flanks: Vec<([u8; 32], String, String)> = Vec::with_capacity(suspect.len());
    for key in suspect {
        let (below, above) = derive::around(keys, key, suspect);
        let (lower, upper) = derive::flanks(key, below, above);
        let (lower, upper) = (app.deployment.gap_address(&lower)?, app.deployment.gap_address(&upper)?);
        flanks.push((*key, lower.to_string(), upper.to_string()));
        addrs.push(lower);
        addrs.push(upper);
    }
    let hits = probe_addresses(app, &*pass.node, &addrs).await?;
    let inside = |key: &[u8; 32]| *key != KEY_MIN && *key != KEY_MAX;
    Ok(flanks
        .into_iter()
        .filter(|(key, lower, upper)| inside(key) && (hits.contains_key(lower) || hits.contains_key(upper)))
        .map(|(key, _, _)| key)
        .collect())
}

/// Probes one gap across each maximal run of adjacent unresolved suspects, and a live one proves
/// every key in the run unregistered.
async fn prove_unregistered(pass: &Pass<'_>, keys: &[[u8; 32]], unresolved: &HashSet<[u8; 32]>) -> Result<HashSet<[u8; 32]>> {
    let app = pass.app;
    let runs = runs_of(keys, unresolved);
    let mut spans = Vec::with_capacity(runs.len());
    let mut addrs = Vec::with_capacity(runs.len());
    for run in &runs {
        let (below, _) = derive::around(keys, run.first().expect("a run is never empty"), unresolved);
        let (_, above) = derive::around(keys, run.last().expect("a run is never empty"), unresolved);
        let a = app.deployment.gap_address(&derive::span(below, above))?;
        spans.push(a.to_string());
        addrs.push(a);
    }
    let hits = probe_addresses(app, &*pass.node, &addrs).await?;
    let mut out = HashSet::new();
    for (run, span) in runs.iter().zip(spans) {
        if hits.contains_key(&span) {
            out.extend(run.iter().copied());
        }
    }
    Ok(out)
}

/// `keys` is sorted, so a run is a consecutive stretch.
fn runs_of(keys: &[[u8; 32]], members: &HashSet<[u8; 32]>) -> Vec<Vec<[u8; 32]>> {
    let mut runs: Vec<Vec<[u8; 32]>> = Vec::new();
    let mut current: Vec<[u8; 32]> = Vec::new();
    for key in keys {
        if members.contains(key) {
            current.push(*key);
        } else if !current.is_empty() {
            runs.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        runs.push(current);
    }
    runs
}

type Rows = Vec<([u8; 32], DeedRow)>;

struct Confirmed {
    deeds: HashSet<[u8; 32]>,
    outpoints: HashSet<[u8; 32]>,
    bounds: HashSet<[u8; 32]>,
}

/// Repair to a fixpoint: drop or demote rows the chain refutes, re-adopt candidates mined
/// from the journal's pre-images, and re-probe, until an iteration changes nothing.
pub(super) async fn repair(pass: &mut Pass<'_>, mut rows: Rows, mut check: CheckResult) -> Result<(Rows, CheckResult, RepairSummary)> {
    let app = pass.app;
    let mut summary = RepairSummary::default();
    let confirmed = Confirmed {
        deeds: check.failing_deeds.clone(),
        outpoints: check.failing_outpoints.keys().copied().collect(),
        bounds: check.failing_gaps.values().flat_map(|g| [g.lo, g.hi]).collect(),
    };
    let candidates = {
        let mut conn = app.backends.db.acquire().await?;
        db::mine_candidates(&mut conn).await?
    };
    let mut settled = false;
    for _ in 0..MAX_REPAIR_ITERATIONS {
        pass.gate().await?;
        summary.iterations += 1;
        let verdicts = ask_the_chain(pass, &rows, &suspects(&rows, &check)).await?;
        let mut changed = false;
        let drops = refuted_registrations(&rows, &verdicts, &confirmed);
        changed |= apply_repair(pass, Repair::Drop, &drops, &mut summary.dropped).await?;
        let demotions = refuted_owners(&rows, &check, &verdicts, &confirmed);
        changed |= apply_repair(pass, Repair::Demote, &demotions, &mut summary.demoted).await?;
        changed |= rewrite_outpoints(pass, &rows, &check, &confirmed, &mut summary.rewritten).await?;
        rows = all_deeds(pass).await?;
        if let Some((key, row)) = find_adoption(pass, &rows, &candidates).await? {
            changed |= adopt(pass, &key, &row, &mut summary).await?;
        }
        rows = all_deeds(pass).await?;
        check = probe_all(pass, &rows).await?;
        if check.ok() || !changed {
            settled = true;
            break;
        }
    }
    if !settled {
        log::warn!("repair gave up after {MAX_REPAIR_ITERATIONS} rounds with {} failure(s) left", check.failing_ids().len());
    }
    Ok((rows, check, summary))
}

async fn all_deeds(pass: &Pass<'_>) -> Result<Rows> {
    let mut conn = pass.app.backends.db.acquire().await?;
    db::all_deeds(&mut conn).await
}

/// Owner-unknown rows next to a blind spot have no address to fail, so only this reaches a stale
/// bridge.
fn suspects(rows: &[([u8; 32], DeedRow)], check: &CheckResult) -> Vec<[u8; 32]> {
    rows.iter()
        .filter(|(k, r)| {
            check.failing_deeds.contains(k)
                || (r.kind == RowKind::OwnerUnknown && check.failing_gaps.values().any(|g| g.hi == *k || g.lo == *k))
        })
        .map(|(k, _)| *k)
        .collect()
}

fn refuted_registrations(rows: &[([u8; 32], DeedRow)], verdicts: &ChainVerdicts, confirmed: &Confirmed) -> Rows {
    rows.iter()
        .filter(|(k, r)| {
            verdicts.unregistered.contains(k)
                && (confirmed.deeds.contains(k) || (r.kind == RowKind::OwnerUnknown && confirmed.bounds.contains(k)))
        })
        .cloned()
        .collect()
}

/// A dead deed address on a registered key is stale ownership from a missed transfer. The
/// demoted row upgrades in place at the key's next ownership event.
fn refuted_owners(rows: &[([u8; 32], DeedRow)], check: &CheckResult, verdicts: &ChainVerdicts, confirmed: &Confirmed) -> Rows {
    rows.iter()
        .filter(|(k, r)| {
            confirmed.deeds.contains(k)
                && check.failing_deeds.contains(k)
                && r.kind != RowKind::OwnerUnknown
                && verdicts.registered.contains(k)
        })
        .cloned()
        .collect()
}

/// A failing outpoint has a live deed address, so only its provenance is stale.
async fn rewrite_outpoints(
    pass: &mut Pass<'_>,
    rows: &[([u8; 32], DeedRow)],
    check: &CheckResult,
    confirmed: &Confirmed,
    total: &mut u64,
) -> Result<bool> {
    let outpoints: Vec<_> = check.failing_outpoints.iter().filter(|(k, _)| confirmed.outpoints.contains(*k)).collect();
    if outpoints.is_empty() {
        return Ok(false);
    }
    pass.gate().await?;
    let mut conn = pass.app.backends.db.acquire().await?;
    let mut rewritten = 0u64;
    for (k, seen) in &outpoints {
        let Ok(i) = rows.binary_search_by(|(rk, _)| rk.cmp(k)) else {
            continue;
        };
        if db::update_deed_outpoint_if_matches(&mut conn, k, &rows[i].1, &seen.txid, seen.index, seen.value, seen.daa).await? {
            rewritten += 1;
        } else {
            log::info!("repair: {} changed under the audit, so sparing its provenance", hex32(k));
        }
    }
    log::info!("repair: rewrote {rewritten} of {} provenance record(s) from the chain", outpoints.len());
    *total += rewritten;
    Ok(rewritten > 0)
}

/// Two live gaps around a journal candidate's key prove it registered. Its row is adopted in full
/// only if the remembered deed address is live, and otherwise as owner-unknown. One per iteration,
/// because each adoption splits a gap and invalidates the later derivations.
async fn find_adoption(
    pass: &Pass<'_>,
    rows: &[([u8; 32], DeedRow)],
    candidates: &[([u8; 32], DeedRow)],
) -> Result<Option<([u8; 32], DeedRow)>> {
    let app = pass.app;
    let keys: Vec<[u8; 32]> = rows.iter().map(|(k, _)| *k).collect();
    let present: HashSet<[u8; 32]> = keys.iter().copied().collect();
    let mut tried: HashSet<[u8; 32]> = HashSet::new();
    for (key, prev) in candidates {
        if present.contains(key) || !tried.insert(*key) {
            continue;
        }
        let (below, above) = derive::around(&keys, key, &HashSet::new());
        let (lower, upper) = derive::flanks(key, below, above);
        let (lower, upper) = (app.deployment.gap_address(&lower)?, app.deployment.gap_address(&upper)?);
        let mut probe = vec![lower.clone(), upper.clone()];
        let deed_addr = prev.deed_state(*key).map(|s| app.deployment.deed_address(&s)).transpose()?;
        if let Some(a) = &deed_addr {
            probe.push(a.clone());
        }
        let hits = probe_addresses(app, &*pass.node, &probe).await?;
        if !hits.contains_key(&lower.to_string()) || !hits.contains_key(&upper.to_string()) {
            continue;
        }
        let deed_live = deed_addr.is_some_and(|a| hits.contains_key(&a.to_string()));
        let adopted = if deed_live { prev.clone() } else { DeedRow::owner_unknown(prev.name.clone()) };
        return Ok(Some((*key, adopted)));
    }
    Ok(None)
}

async fn adopt(pass: &mut Pass<'_>, key: &[u8; 32], row: &DeedRow, summary: &mut RepairSummary) -> Result<bool> {
    pass.gate().await?;
    let mut conn = pass.app.backends.db.acquire().await?;
    if !db::insert_deed_if_absent(&mut conn, key, row).await? {
        log::info!("repair: {} appeared under the audit, so keeping the stream's row", hex32(key));
        return Ok(false);
    }
    if row.kind == RowKind::OwnerUnknown {
        summary.bridged += 1;
    } else {
        summary.readopted += 1;
    }
    Ok(true)
}
