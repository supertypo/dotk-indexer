//! The self-test proves the registry against the UTXO set and publishes the proof.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use dotk_core::state::GapState;

use super::gate::Withheld;
use super::probe::{Observed, probe_all, probe_cards, sync_gap_cache};
use super::repair::repair;
use crate::app::{App, Backends, Progress, Proof, RepairSummary, SelfTestReport, Verdict};
use crate::chain::Node;
use crate::convert::hex32;
use crate::db;
use crate::model::{DeedRow, GapRow};
use crate::snapshot::{self, Snapshot};

const CONFIRMATIONS: u32 = 3;
/// Between startup passes that found nothing to publish.
const STARTUP_RETRY: Duration = Duration::from_secs(30);
/// How often a waiting pass re-reads the chain position.
const POLL: Duration = Duration::from_millis(250);
/// Each attempt pins a fresh connection, which can reach another node.
const NODE_ATTEMPTS: u32 = 3;

/// A pass runs only while the indexer is caught up. A pass due while it is behind waits.
pub async fn run(app: Arc<App>) {
    if !wait_caught_up(&app.progress).await {
        return;
    }
    log::info!("running the mandatory startup self-test");
    run_startup(&app).await;
    run_periodic(&app).await;
}

/// Mid-probe churn can leave a passing run with nothing to publish, and the periodic run can
/// be up to an hour off. A failing verdict heals through the stream.
async fn run_startup(app: &App) {
    loop {
        let outcome = run_pass(app).await;
        if app.progress.is_shutdown() {
            return;
        }
        if matches!(outcome, Outcome::Behind) {
            if !wait_caught_up(&app.progress).await {
                return;
            }
            continue;
        }
        let verdict = app.verdict.health.read().await.selftest.as_ref().map(|t| t.proven);
        let retry = match verdict {
            None => true,
            Some(true) => app.verdict.proof.read().await.is_none() && has_checkpoint(&app.backends).await,
            Some(false) => false,
        };
        if !retry {
            return;
        }
        if !app.progress.sleep(STARTUP_RETRY).await {
            return;
        }
    }
}

/// A follow-up comes due and the indexer catches up on the clock, so the loop wakes on a tick
/// as well as on a trigger.
async fn run_periodic(app: &App) {
    let interval = app.deployment.args.selftest_interval;
    let mut last_run = Instant::now();
    // A pass owed stays owed until one runs to its end, because a trigger outlives the lag.
    let mut pending = false;
    let mut asked = false;
    loop {
        let triggered = tokio::select! {
            biased;
            () = app.progress.until_shutdown() => return,
            () = app.verdict.selftest_trigger.notified() => true,
            () = tokio::time::sleep(POLL) => false,
        };
        asked |= triggered || app.verdict.followup_due();
        pending |= asked || last_run.elapsed() >= interval;
        if pending && app.progress.caught_up() {
            let outcome = run_pass(app).await;
            pending = matches!(outcome, Outcome::Behind);
            if !pending {
                // An errored pass can drop a confirmed deviation, so it owes a retry.
                if matches!(outcome, Outcome::Errored) {
                    app.verdict.owe_followup(true);
                }
                asked = false;
            }
            last_run = Instant::now();
        }
    }
}

/// `false` if shutdown comes first.
async fn wait_caught_up(progress: &Progress) -> bool {
    while !progress.caught_up() {
        if !progress.sleep(POLL).await {
            return false;
        }
    }
    true
}

/// A pass stops at its next gate, probe chunk or wait after the shutdown request.
#[derive(Debug)]
pub(super) struct ShuttingDown;

impl std::fmt::Display for ShuttingDown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the indexer is shutting down")
    }
}

impl std::error::Error for ShuttingDown {}

pub(super) fn ensure_running(progress: &Progress) -> Result<()> {
    if progress.is_shutdown() { Err(ShuttingDown.into()) } else { Ok(()) }
}

async fn confirm_delay(app: &App) -> Result<()> {
    if app.progress.sleep(app.deployment.args.selftest_confirm_delay).await { Ok(()) } else { Err(ShuttingDown.into()) }
}

/// A lagging table fails every name that changed meanwhile, and repair would rewrite rows the
/// stream is about to replay. So a pass abandons before it probes or writes while behind.
#[derive(Debug)]
struct FellBehind;

impl std::fmt::Display for FellBehind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the indexer is behind the chain, so this pass is abandoned before it writes")
    }
}

impl std::error::Error for FellBehind {}

fn ensure_caught_up(progress: &Progress) -> Result<()> {
    if progress.caught_up() { Ok(()) } else { Err(FellBehind.into()) }
}

/// A node behind the pipeline's last commit, or whose sink falls back mid-pass, answers for an
/// older chain, and a repair against it undoes what the pipeline applied.
#[derive(Debug)]
struct StaleNode {
    sink: u64,
    required: u64,
}

impl std::fmt::Display for StaleNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the node's sink is at blue score {}, below the {} this pass requires", self.sink, self.required)
    }
}

impl std::error::Error for StaleNode {}

/// One node for the whole pass, gated before every probe and every write.
pub(super) struct Pass<'a> {
    pub(super) app: &'a App,
    pub(super) node: Box<dyn Node + 'a>,
    pub(super) sink: u64,
}

impl<'a> Pass<'a> {
    async fn pin(app: &'a App) -> Result<Self> {
        let mut pass = Self { app, node: app.backends.kaspad.pin().await?, sink: 0 };
        pass.gate().await?;
        Ok(pass)
    }

    /// A synced node closes a lag of a few blue scores on its own, so the gate waits one
    /// confirmation delay for it. A sink below the node's own earlier answer is another node.
    pub(super) async fn gate(&mut self) -> Result<()> {
        let delay = self.app.deployment.args.selftest_confirm_delay;
        let patience = Instant::now() + delay;
        loop {
            ensure_running(&self.app.progress)?;
            ensure_caught_up(&self.app.progress)?;
            let sink = self.node.sink_blue_score().await?;
            let required = self.app.progress.last_block_blue_score.load(Ordering::Relaxed).max(self.sink);
            if sink >= required {
                self.sink = sink;
                return Ok(());
            }
            if sink < self.sink || Instant::now() >= patience {
                self.node.discard();
                return Err(StaleNode { sink, required }.into());
            }
            if !self.app.progress.sleep(delay.min(POLL)).await {
                return Err(ShuttingDown.into());
            }
        }
    }
}

#[derive(Debug)]
pub enum Outcome {
    Verdict(SelfTestReport),
    /// Nothing was written and the previous verdict stands.
    Errored,
    /// As `Errored`, but the pass is still owed.
    Behind,
}

/// An error reads as `false`, because a broken database raises its own alarm.
async fn has_checkpoint(backends: &Backends) -> bool {
    match backends.db.acquire().await {
        Ok(mut conn) => matches!(db::has_checkpoint(&mut conn).await, Ok(true)),
        Err(_) => false,
    }
}

/// Returns this pass's own report, or `None` if it wrote nothing. Read a pass's result from
/// here, never from `app.verdict.health`, which holds whichever pass wrote last.
pub async fn run_once(app: &App) -> Option<SelfTestReport> {
    match run_pass(app).await {
        Outcome::Verdict(report) => Some(report),
        Outcome::Errored | Outcome::Behind => None,
    }
}

pub async fn run_pass(app: &App) -> Outcome {
    match selftest(app).await {
        Ok((mut report, publishable)) => {
            log_verdict(&report);
            app.refresh_counts().await;
            if let Some(export) = publishable {
                report.published = publish_proof(&app.verdict, export, report.finished_ms).await;
            }
            app.verdict.health.write().await.selftest = Some(report.clone());
            Outcome::Verdict(report)
        }
        Err(e) if e.is::<FellBehind>() => {
            log::warn!("self-test abandoned (state unchanged, previous result kept): {e}");
            Outcome::Behind
        }
        Err(e) if e.is::<StaleNode>() => {
            log::warn!("self-test abandoned (previous result kept): {e}");
            Outcome::Errored
        }
        Err(e) if e.is::<ShuttingDown>() => {
            log::info!("self-test abandoned: {e}");
            Outcome::Errored
        }
        Err(e) => {
            log::error!("self-test errored (state unchanged, previous result kept): {e:#}");
            Outcome::Errored
        }
    }
}

fn log_verdict(report: &SelfTestReport) {
    if report.proven {
        // Gaps are rows + 1, so the surplus over deeds + 1 counts owner-unknown rows.
        let bridged = match report.gaps_checked.saturating_sub(report.deeds_checked + 1) {
            0 => String::new(),
            n => format!(", {n} owner-unknown proven by their gaps"),
        };
        log::info!(
            "self-test PASSED: {} gaps + {} deeds ({} active, {} pending) proven in {} ms{bridged}",
            report.gaps_checked,
            report.deeds_checked,
            report.deeds_checked - report.pending_checked,
            report.pending_checked,
            report.finished_ms.saturating_sub(report.started_ms)
        );
    } else {
        log::error!(
            "self-test FAILED: {} failing outpoint(s), {} residual blind spot(s), \
             {} refuted owner(s); /health is now 503",
            report.failing_outpoints.len(),
            report.blind_spots.len(),
            report.owner_unknown.len()
        );
    }
}

async fn publish_proof(verdict: &Verdict, mut export: Snapshot, proven_ms: u64) -> bool {
    export.proven = true;
    export.proven_at = Some(proven_ms);
    match snapshot::bodies(&mut export) {
        Ok(bodies) => {
            *verdict.proof.write().await = Some(Proof { bodies, proven_ms });
            true
        }
        Err(e) => {
            log::error!("serializing the proven snapshot (previous proof kept): {e:#}");
            false
        }
    }
}

/// Keys the chain and the table disagree on while the stream still writes them, or that failed
/// only the last probe of a pass that proved. No verdict fails on these, or an owner who
/// re-transfers faster than the pipeline fails every attempt. A deed in flux is withheld and
/// exported as owner-unknown. A gap or a provenance in flux is exported as
/// stored, because the export is consistent at its own checkpoint and an importer replays it.
#[derive(Clone, Default)]
pub(super) struct Flux {
    pub(super) gaps: BTreeMap<[u8; 32], GapState>,
    pub(super) deeds: HashSet<[u8; 32]>,
    pub(super) outpoints: HashSet<[u8; 32]>,
}

impl Flux {
    fn is_empty(&self) -> bool {
        self.gaps.is_empty() && self.deeds.is_empty() && self.outpoints.is_empty()
    }

    /// A provenance in flux withholds nothing from lookups.
    fn gates(&self) -> bool {
        !self.gaps.is_empty() || !self.deeds.is_empty()
    }
}

#[derive(Clone)]
pub(super) struct CheckResult {
    /// Derived gaps the chain does not hold, keyed by `lo`.
    pub(super) failing_gaps: BTreeMap<[u8; 32], GapState>,
    pub(super) failing_deeds: HashSet<[u8; 32]>,
    /// Disjoint from `failing_deeds`, because a mismatch needs a hit.
    pub(super) failing_outpoints: HashMap<[u8; 32], Observed>,
    pub(super) flux: Flux,
    pub(super) observed_gaps: Vec<([u8; 32], GapRow)>,
    pub(super) gaps_checked: u64,
    pub(super) deeds_checked: u64,
    pub(super) pending_checked: u64,
}

impl CheckResult {
    pub(super) fn ok(&self) -> bool {
        self.failing_gaps.is_empty() && self.failing_deeds.is_empty() && self.failing_outpoints.is_empty()
    }

    fn confirmed_by(&self, raw: &CheckResult) -> CheckResult {
        CheckResult {
            failing_gaps: raw
                .failing_gaps
                .iter()
                .filter(|(lo, _)| self.failing_gaps.contains_key(*lo))
                .map(|(lo, g)| (*lo, *g))
                .collect(),
            failing_deeds: self.failing_deeds.intersection(&raw.failing_deeds).copied().collect(),
            failing_outpoints: raw
                .failing_outpoints
                .iter()
                .filter(|(k, _)| self.failing_outpoints.contains_key(*k))
                .map(|(k, o)| (*k, *o))
                .collect(),
            flux: raw.flux.clone(),
            observed_gaps: raw.observed_gaps.clone(),
            gaps_checked: raw.gaps_checked,
            deeds_checked: raw.deeds_checked,
            pending_checked: raw.pending_checked,
        }
    }

    pub(super) fn failing_ids(&self) -> Vec<String> {
        let mut v: Vec<String> = self.failing_gaps.keys().map(|lo| format!("gap:{}", hex32(lo))).collect();
        v.extend(self.failing_deeds.iter().map(|k| format!("deed:{}", hex32(k))));
        v.extend(self.failing_outpoint_ids());
        v.sort();
        v
    }

    /// The report carries only these, because a failing gap is served as a `blindSpots` bracket
    /// and a failing deed as an `ownerUnknown` row.
    fn failing_outpoint_ids(&self) -> Vec<String> {
        let mut v: Vec<String> =
            self.failing_outpoints.keys().chain(&self.flux.outpoints).map(|k| format!("outpoint:{}", hex32(k))).collect();
        v.sort();
        v
    }

    fn blind_spots(&self) -> Vec<[String; 2]> {
        let mut v: Vec<[String; 2]> =
            self.failing_gaps.values().chain(self.flux.gaps.values()).map(|g| [hex32(&g.lo), hex32(&g.hi)]).collect();
        v.sort();
        v
    }

    fn withheld(&self) -> Arc<Withheld> {
        Arc::new(Withheld::new(
            self.failing_gaps.values().chain(self.flux.gaps.values()).map(|g| [g.lo, g.hi]).collect(),
            self.failing_deeds.iter().chain(&self.flux.deeds).copied().collect(),
            self.failing_outpoints.keys().chain(&self.flux.outpoints).copied().collect(),
        ))
    }

    fn owner_unknown(&self, rows: &[([u8; 32], DeedRow)]) -> Vec<[String; 2]> {
        let mut v: Vec<[String; 2]> = self
            .failing_deeds
            .iter()
            .chain(&self.flux.deeds)
            .map(|k| {
                let name = rows.binary_search_by(|(rk, _)| rk.cmp(k)).ok().and_then(|i| rows[i].1.name.clone()).unwrap_or_default();
                [hex32(k), name]
            })
            .collect();
        v.sort();
        v
    }

    /// Moves every confirmed failure the stream rewrote since `since` into the flux. A gap counts
    /// only when an event rewrote it, and not when the stream discovered a key inside it.
    async fn settle_flux(&mut self, backends: &Backends, since: u64) -> Result<()> {
        if self.ok() {
            return Ok(());
        }
        let mut conn = backends.db.acquire().await?;
        let los: Vec<[u8; 32]> = self.failing_gaps.keys().copied().collect();
        for lo in db::gaps_touched_since(&mut conn, &los, since).await? {
            let Some(g) = self.failing_gaps.get(&lo).copied() else { continue };
            if !db::discovered_inside_since(&mut conn, &g.lo, &g.hi, since).await? {
                self.failing_gaps.remove(&lo);
                self.flux.gaps.insert(lo, g);
            }
        }
        let keys: Vec<[u8; 32]> = self.failing_deeds.iter().chain(self.failing_outpoints.keys()).copied().collect();
        for k in db::keys_touched_since(&mut conn, &keys, since).await? {
            if self.failing_deeds.remove(&k) {
                self.flux.deeds.insert(k);
            }
            if self.failing_outpoints.remove(&k).is_some() {
                self.flux.outpoints.insert(k);
            }
        }
        Ok(())
    }
}

/// The export is publishable when every failure of its probe is the stream's. The fresh flux
/// joins the report, so the proof and the lookup gate withhold the same keys.
async fn publish_fresh(
    app: &App,
    raw: &CheckResult,
    check: &mut CheckResult,
    export: Option<Snapshot>,
    since: u64,
) -> Result<Option<Snapshot>> {
    let mut fresh = raw.clone();
    fresh.settle_flux(&app.backends, since).await?;
    if !fresh.ok() {
        // The pipeline commits a beat behind the chain, so the journal can miss the last transfer.
        confirm_delay(app).await?;
        fresh.settle_flux(&app.backends, since).await?;
        if !fresh.ok() {
            // Unconfirmed, so no repair, but withheld until the owed pass decides.
            fresh.flux.gaps.extend(fresh.failing_gaps.iter().map(|(lo, g)| (*lo, *g)));
            fresh.flux.deeds.extend(fresh.failing_deeds.iter().copied());
            fresh.flux.outpoints.extend(fresh.failing_outpoints.keys().copied());
            check.flux = fresh.flux;
            return Ok(None);
        }
    }
    check.flux = fresh.flux;
    Ok(export.map(|mut export| {
        withhold(&mut export, &check.flux.deeds);
        export
    }))
}

/// The ownership of a key in flux leaves the proof as a demotion leaves the table: the key and
/// its name stay, so the importer's stream upgrades the row at the key's next event.
fn withhold(export: &mut Snapshot, keys: &HashSet<[u8; 32]>) {
    let keys: HashSet<String> = keys.iter().map(hex32).collect();
    for deed in export.deeds.iter_mut().filter(|d| keys.contains(&d.key)) {
        deed.row = snapshot::ExportRow::from(&DeedRow::owner_unknown(deed.row.name.take()));
    }
}

/// The indexer can be caught up before its first checkpoint, and then there is no export.
async fn export_pass(app: &App) -> Result<(Option<Snapshot>, Vec<([u8; 32], DeedRow)>)> {
    let window = snapshot::export_event_window(app.deployment.net_bps);
    match snapshot::export_rows(&app.backends.db, &app.deployment.genesis.registry_covenant_id, window, Duration::ZERO).await {
        Ok((export, rows)) => Ok((Some(export), rows)),
        Err(e) if e.is::<snapshot::NoCheckpoint>() => {
            let mut conn = app.backends.db.acquire().await?;
            Ok((None, db::all_deeds(&mut conn).await?))
        }
        Err(e) => Err(e),
    }
}

/// A pass probes the rows of its own `/snapshot` export, so a clean pass proves exactly the
/// bytes it publishes.
async fn selftest(app: &App) -> Result<(SelfTestReport, Option<Snapshot>)> {
    let mut attempt = 1;
    loop {
        match selftest_on_one_node(app).await {
            Err(e) if e.is::<StaleNode>() && attempt < NODE_ATTEMPTS => {
                log::warn!("{e}, pinning another node (attempt {attempt} of {NODE_ATTEMPTS})");
                attempt += 1;
            }
            done => return done,
        }
    }
}

async fn selftest_on_one_node(app: &App) -> Result<(SelfTestReport, Option<Snapshot>)> {
    let mut pass = Pass::pin(app).await?;
    // Imported cards are checked before the first verdict lets a name serve one.
    let first = if app.verdict.judged().await {
        None
    } else {
        Some(probe_cards(&mut pass).await.context("the card probe failed before the first verdict")?)
    };
    let (mut report, publishable, owed) = selftest_registry(&mut pass).await?;
    let cards = match first {
        Some(counts) => Ok(counts),
        None => probe_cards(&mut pass).await,
    };
    let cards_failed = match cards {
        Ok((swept, restored)) => {
            (report.repaired.cards_swept, report.repaired.cards_restored) = (swept, restored);
            false
        }
        Err(e) if e.is::<ShuttingDown>() => return Err(e),
        Err(e) => {
            log::warn!("card probe skipped this pass (registry verdict unaffected): {e:#}");
            true
        }
    };
    app.verdict.owe_followup(owed || cards_failed);
    Ok((report, publishable))
}

/// The third value is whether a follow-up pass is owed.
async fn selftest_registry(pass: &mut Pass<'_>) -> Result<(SelfTestReport, Option<Snapshot>, bool)> {
    let app = pass.app;
    let started_ms = App::now_ms();
    let start_blue_score = app.progress.last_block_blue_score.load(Ordering::Relaxed);
    let probed = confirm_deviation(pass).await?;
    let attempts = probed.attempts;
    let has_export = probed.export.is_some();
    let mut judged = judge(pass, probed, start_blue_score).await?;
    judged.repaired.gaps_synced += sync_gap_cache(pass, &judged.check, start_blue_score).await?;
    let check = &judged.check;
    log_flux(&check.flux);
    // Only a pass proves what the stream healed, and only a pass publishes the proof it missed.
    let owed = check.flux.gates() || judged.unconfirmed || (check.ok() && has_export && judged.publishable.is_none());
    let report = SelfTestReport {
        proven: check.ok(),
        published: false,
        started_ms,
        finished_ms: App::now_ms(),
        gaps_checked: check.gaps_checked,
        deeds_checked: check.deeds_checked,
        pending_checked: check.pending_checked,
        attempts,
        failing_outpoints: check.failing_outpoint_ids(),
        blind_spots: check.blind_spots(),
        owner_unknown: check.owner_unknown(&judged.rows),
        repaired: judged.repaired,
        withheld: check.withheld(),
    };
    Ok((report, judged.publishable, owed))
}

struct Probed {
    export: Option<Snapshot>,
    rows: Vec<([u8; 32], DeedRow)>,
    raw: CheckResult,
    /// The failures of every attempt so far.
    check: CheckResult,
    attempts: u32,
}

/// The same item must fail on every attempt, which screens out chain changes racing the probe.
async fn confirm_deviation(pass: &mut Pass<'_>) -> Result<Probed> {
    let app = pass.app;
    let (export, rows) = export_pass(app).await?;
    let raw = probe_all(pass, &rows).await?;
    let mut p = Probed { export, rows, check: raw.clone(), raw, attempts: 1 };
    while !p.check.ok() && p.attempts < CONFIRMATIONS {
        confirm_delay(app).await?;
        // A lag confirms exactly like an inconsistency, and this gate tells them apart.
        pass.gate().await?;
        p.attempts += 1;
        (p.export, p.rows) = export_pass(app).await?;
        p.raw = probe_all(pass, &p.rows).await?;
        p.check = p.check.confirmed_by(&p.raw);
    }
    Ok(p)
}

struct Judged {
    rows: Vec<([u8; 32], DeedRow)>,
    check: CheckResult,
    repaired: RepairSummary,
    publishable: Option<Snapshot>,
    unconfirmed: bool,
}

async fn judge(pass: &mut Pass<'_>, probed: Probed, start_blue_score: u64) -> Result<Judged> {
    let app = pass.app;
    let Probed { export, rows, raw, mut check, attempts } = probed;
    check.settle_flux(&app.backends, start_blue_score).await?;
    if check.ok() {
        let publishable = publish_fresh(app, &raw, &mut check, export, start_blue_score).await?;
        return Ok(Judged { rows, check, repaired: RepairSummary::default(), publishable, unconfirmed: false });
    }
    log::warn!("self-test deviation confirmed {attempts} times: {:?}, repairing", check.failing_ids());
    let confirmed = check.clone();
    let (rows, mut check, repaired) = repair(pass, rows, check).await?;
    check.settle_flux(&app.backends, start_blue_score).await?;
    let unconfirmed = !check.failing_deeds.is_subset(&confirmed.failing_deeds)
        || check.failing_outpoints.keys().any(|k| !confirmed.failing_outpoints.contains_key(k))
        || check.failing_gaps.keys().any(|lo| !confirmed.failing_gaps.contains_key(lo));
    let mut publishable = None;
    if check.ok() {
        // Repair changed rows past any export, so prove a fresh one.
        let (repaired_export, repaired_rows) = export_pass(app).await?;
        if repaired_export.is_some() && probe_all(pass, &repaired_rows).await?.ok() {
            publishable = repaired_export;
        }
    }
    Ok(Judged { rows, check, repaired, publishable, unconfirmed })
}

fn log_flux(flux: &Flux) {
    if !flux.is_empty() {
        log::info!(
            "self-test: {} gap(s), {} deed(s) and {} provenance record(s) in flux, withheld until the next pass",
            flux.gaps.len(),
            flux.deeds.len(),
            flux.outpoints.len()
        );
    }
}

#[cfg(test)]
#[path = "selftest_tests.rs"]
mod tests;
