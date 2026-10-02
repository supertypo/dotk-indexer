//! Follows the virtual chain, with fetching batch N+1 overlapping the commit of batch N.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::app::{App, BlockRef, Progress};
use crate::chain::events;
use crate::chain::{CATCHUP_DEADLINE, TIP_DEADLINE, Unresumable};
use crate::chain::{ChainBlock, VccResponse};
use crate::convert::hex32;
use crate::db;

const CATCHUP_THRESHOLD: usize = 200;

/// With `ERR_DELAY` between tries, this rides out about five minutes of database outage.
const PROCESS_FAILURE_LIMIT: u32 = 60;

const ERR_DELAY: Duration = Duration::from_secs(5);

const UNRESUMABLE_AFTER: Duration = Duration::from_mins(1);

pub async fn run(app: Arc<App>, start: [u8; 32]) -> Result<()> {
    app.refresh_counts().await;
    let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<VccResponse>(1);
    let fetcher = tokio::spawn(fetch_loop(app.clone(), start, batch_tx));

    let mut last_progress = Instant::now();
    while let Some(res) = batch_rx.recv().await {
        let mut failures = 0u32;
        loop {
            if app.progress.is_shutdown() {
                batch_rx.close(); // unblocks a fetcher mid-send
                return fetcher.await.context("the fetch task ended abnormally")?;
            }
            match process_response(&app, &res).await {
                Ok(()) => break,
                Err(e) => {
                    failures += 1;
                    if failures >= PROCESS_FAILURE_LIMIT {
                        fetcher.abort();
                        return Err(e.context(format!(
                            "a virtual-chain batch failed to commit {failures} times in a row, halting the pipeline"
                        )));
                    }
                    log::error!("processing a virtual-chain batch failed, retrying: {e:#}");
                    app.progress.sleep(ERR_DELAY).await;
                }
            }
        }
        if let Some(last) = res.added.last() {
            log_progress(&app, last.blue_score, &mut last_progress);
        }
    }
    fetcher.await.context("the fetch task ended abnormally")?
}

/// The timer advances only on a logged line, so falling behind again is reported at once.
fn log_progress(app: &App, processed: u64, last: &mut Instant) {
    if last.elapsed() < app.deployment.args.progress_interval {
        return;
    }
    if let Some(behind) = behind(&app.progress, processed, app.deployment.net_bps) {
        *last = Instant::now();
        log::info!("catching up, processed {processed}, {behind} (~{}) behind", coarse(behind / app.deployment.net_bps));
    }
}

/// Exactly `tip_distance` below the sink counts as no lag. `None` under a second of blocks, or
/// under a minute once caught up.
fn behind(progress: &Progress, processed: u64, net_bps: u64) -> Option<u64> {
    let behind = progress.lag(processed);
    let floor = net_bps * if progress.caught_up() { 60 } else { 1 };
    (behind >= floor).then_some(behind)
}

fn coarse(secs: u64) -> String {
    const UNITS: [u64; 4] = [86_400, 3_600, 60, 1];
    let unit = UNITS.into_iter().find(|u| secs >= *u).unwrap_or(1);
    humantime::format_duration(Duration::from_secs(secs / unit * unit)).to_string()
}

/// The cursor advances optimistically, and the 1-slot channel bounds how far fetch runs ahead.
async fn fetch_loop(app: Arc<App>, mut start: [u8; 32], batch_tx: tokio::sync::mpsc::Sender<VccResponse>) -> Result<()> {
    let mut poller = Poller::default();
    loop {
        let fetched = tokio::select! {
            biased;
            () = app.progress.until_shutdown() => return Ok(()),
            answer = async {
                poller.refresh_tip(&app).await;
                poller.poll(&app, start).await
            } => answer?,
        };
        let Some(res) = fetched else { continue };
        // Empty answers count too, so a stalled chain shows as the last block's age.
        app.progress.observe_caught_up();
        let added = res.added.len();
        poller.last_added = Some(added);
        if let Some(last) = res.added.last() {
            start = last.hash;
            if batch_tx.send(res).await.is_err() {
                return Ok(()); // the processor halted
            }
        } else if !res.removed.is_empty() {
            // The node reports these removals again with the winning chain.
            log::info!("removed-only response ({} blocks), waiting for the winning chain", res.removed.len());
        }
        if added < CATCHUP_THRESHOLD {
            app.progress.sleep(app.deployment.args.vcp_interval).await;
        }
    }
}

#[derive(Default)]
struct Poller {
    /// A warn stream that stops does not say the node came back, so the end of an outage logs.
    outage: Option<(u32, Instant)>,
    last_tip: Option<Instant>,
    /// An empty answer sets this to 0, so a stall at the tip keeps the short deadline.
    last_added: Option<usize>,
    unknown_since: Option<Instant>,
}

impl Poller {
    async fn refresh_tip(&mut self, app: &App) {
        if self.last_tip.is_some_and(|t| t.elapsed() < app.deployment.args.progress_interval) {
            return;
        }
        self.last_tip = Some(Instant::now());
        // The pool can answer from a node behind the last one, so the tip never goes down.
        match app.backends.kaspad.sink_blue_score().await {
            Ok(tip) => {
                app.progress.tip_blue_score.fetch_max(tip, Ordering::Relaxed);
            }
            Err(e) => log::debug!("sink blue score unavailable: {e:#}"),
        }
    }

    /// `None` after a failed poll that the caller retries.
    async fn poll(&mut self, app: &App, start: [u8; 32]) -> Result<Option<VccResponse>> {
        let deadline = poll_deadline(self.last_added, self.outage.is_some());
        let e = match app.backends.kaspad.virtual_chain(start, app.deployment.args.vcp_tip_distance, deadline).await {
            Ok(res) => {
                self.unknown_since = None;
                if let Some((failures, since)) = self.outage.take() {
                    log::info!("node answering again after {failures} failed polls ({} s)", since.elapsed().as_secs());
                }
                return Ok(Some(res));
            }
            Err(e) => e,
        };
        if e.is::<Unresumable>() {
            if self.unknown_since.get_or_insert_with(Instant::now).elapsed() >= UNRESUMABLE_AFTER {
                anyhow::bail!(
                    "chain block {} stayed unknown to the node for {} s (pruned or foreign), so it cannot resume, \
                     re-import a fresh /snapshot body or --initialize-db ({e})",
                    hex32(&start),
                    UNRESUMABLE_AFTER.as_secs()
                );
            }
            log::warn!("chain block {} is unknown to the node, retrying: {e}", hex32(&start));
        } else {
            self.unknown_since = None;
            let (failures, since) = self.outage.get_or_insert_with(|| (0, Instant::now()));
            *failures += 1;
            if since.elapsed() >= app.deployment.args.vcp_outage_limit {
                anyhow::bail!(
                    "the node answered none of {failures} polls in {} s, so the process exits for a restart ({e:#})",
                    since.elapsed().as_secs()
                );
            }
            log::warn!("virtual chain poll failed: {e:#}");
        }
        app.progress.sleep(ERR_DELAY).await;
        Ok(None)
    }
}

/// Reads the poll's own history, not `caught_up()`, which turns false within seconds of a stall
/// and gives the stalled poll the long deadline. A poll after a failed one gets the long deadline,
/// because a loaded node takes longer than the tip deadline for the first answer after an outage,
/// and a poll cut short leaves that answer standing.
fn poll_deadline(last_added: Option<usize>, failed: bool) -> Duration {
    match last_added {
        _ if failed => CATCHUP_DEADLINE,
        Some(added) if added < CATCHUP_THRESHOLD => TIP_DEADLINE,
        _ => CATCHUP_DEADLINE,
    }
}

/// One response, one Postgres transaction, so readers never see a torn batch. A response that
/// adds no block changes nothing.
pub async fn process_response(app: &App, res: &VccResponse) -> Result<()> {
    let (Some(first), Some(last)) = (res.added.first(), res.added.last()) else { return Ok(()) };
    let last_hex = hex32(&last.hash);
    let mut outcome = BatchOutcome::default();

    let mut tx = app.backends.db.begin().await?;
    if db::get_var_for_update(&mut tx, db::VAR_VCP_CHECKPOINT).await?.as_deref() == Some(last_hex.as_str()) {
        drop(tx);
        log::warn!("the batch ending at {last_hex} is already committed, so it asks for a self-test in place of its outcome");
        outcome.needs_selftest = true;
        app.caches.counts_stale.store(true, Ordering::Relaxed);
        after_commit(app, res, first, last, outcome).await;
        return Ok(());
    }
    for removed in &res.removed {
        outcome.undone_events += events::undo_block(&mut tx, removed).await?;
    }
    for block in &res.added {
        let stale = db::delete_imported_events_for_block(&mut tx, &block.hash).await?;
        if stale > 0 {
            log::warn!("dropped {stale} imported journal entries for block {}", hex32(&block.hash));
            outcome.needs_selftest = true;
        }
        let out = events::apply_block(
            &mut tx,
            &app.deployment.watch,
            &app.deployment.genesis.params,
            &app.deployment.genesis.registry_covenant_id,
            block,
        )
        .await?;
        outcome.applied_events += out.events;
        outcome.needs_selftest |= out.needs_selftest;
    }
    db::set_var(&mut tx, db::VAR_VCP_CHECKPOINT, &last_hex).await?;
    tx.commit().await?;
    after_commit(app, res, first, last, outcome).await;
    Ok(())
}

#[derive(Default)]
struct BatchOutcome {
    needs_selftest: bool,
    undone_events: u32,
    applied_events: u32,
}

async fn after_commit(app: &App, res: &VccResponse, first: &ChainBlock, last: &ChainBlock, mut outcome: BatchOutcome) {
    outcome.needs_selftest |= reorg_below_coverage(&app.progress, res, first.blue_score);
    if outcome.undone_events > 0 {
        log::info!("reorg: {} events undone, {} added", outcome.undone_events, outcome.applied_events);
    } else if outcome.applied_events > 0 {
        log::info!("{} added events", outcome.applied_events);
    }
    // 0 keeps the journal forever. The parser enforces the finality floor.
    if app.deployment.args.journal_retention > 0 {
        let watermark = last.blue_score.saturating_sub(app.deployment.args.journal_retention);
        if watermark > 0
            && let Err(e) = prune_below(app, watermark).await
        {
            log::warn!("pruning below blue score {watermark} failed: {e:#}");
        }
    }
    publish_last_block(app, last).await;
    if outcome.applied_events + outcome.undone_events > 0 || app.caches.counts_stale.load(Ordering::Relaxed) {
        app.refresh_counts().await;
    }
    app.progress.observe_caught_up();
    if outcome.needs_selftest {
        app.verdict.trigger_selftest();
    }
}

/// A reorg below journal coverage is not undone exactly. Gate on blocks removed, not
/// events undone. Zero undone events can mean eventless blocks or a missing journal, and only the
/// coverage floor tells them apart.
fn reorg_below_coverage(progress: &Progress, res: &VccResponse, first_added: u64) -> bool {
    let mut below = false;
    if !res.removed.is_empty() {
        let floor = progress.coverage_floor.load(Ordering::Relaxed);
        if floor > 0 && first_added < floor {
            log::error!("reorg reaches below journal coverage (blue {first_added} < floor {floor}), escalating to full validation");
            below = true;
        }
    }
    // Lowered after the check, so a journal-less import (floor `u64::MAX`) does not escalate
    // every reorg.
    progress.coverage_floor.fetch_min(first_added, Ordering::Relaxed);
    below
}

async fn publish_last_block(app: &App, last: &ChainBlock) {
    // The batch is a lower bound on the tip, so `/health` never publishes a tip behind its last
    // block.
    app.progress.tip_blue_score.fetch_max(last.blue_score + app.deployment.args.vcp_tip_distance, Ordering::Relaxed);
    app.verdict.health.write().await.last_block =
        Some(BlockRef { hash: hex32(&last.hash), daa_score: last.daa_score, blue_score: last.blue_score, timestamp: last.timestamp });
    app.progress.last_block_ms.store(last.timestamp, Ordering::Relaxed);
    app.progress.last_block_blue_score.store(last.blue_score, Ordering::Relaxed);
    app.progress.last_block_daa_score.store(last.daa_score, Ordering::Relaxed);
}

async fn prune_below(app: &App, watermark: u64) -> Result<()> {
    let mut conn = app.backends.db.acquire().await?;
    let pruned = db::prune_events_below(&mut conn, watermark).await?;
    if pruned > 0 {
        log::debug!("pruned {pruned} journal rows below blue score {watermark}");
    }
    // The watermark is at least finality below the tip, so this never escalates.
    app.progress.coverage_floor.fetch_max(watermark, Ordering::Relaxed);
    // No reorg reaches a card swept that long ago, so the row leaves with its journal.
    let purged = db::purge_swept_cards_below(&mut conn, watermark).await?;
    if purged > 0 {
        log::debug!("purged {purged} swept card rows below blue score {watermark}");
    }
    Ok(())
}

#[cfg(test)]
#[path = "vcp_tests.rs"]
mod tests;
