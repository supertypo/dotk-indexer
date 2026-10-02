use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, RwLock};

use super::{App, SnapshotBodies};
use crate::audit::Withheld;

const FOLLOWUP_MS: u64 = 60_000;

/// The last fully processed chain block.
#[derive(Debug, Clone)]
pub struct BlockRef {
    pub hash: String,
    pub daa_score: u64,
    pub blue_score: u64,
    /// Header timestamp, ms since epoch.
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct SelfTestReport {
    /// No confirmed deviation. A key in flux is withheld, not failing.
    pub proven: bool,
    pub published: bool,
    pub started_ms: u64,
    pub finished_ms: u64,
    pub gaps_checked: u64,
    /// Lower than the row count when owner-unknown rows exist, because they have no address.
    pub deeds_checked: u64,
    /// The PENDING part of `deeds_checked`, as proven by this run, not as the table holds now.
    pub pending_checked: u64,
    pub attempts: u32,
    /// `outpoint:<hex key>` ids whose stored provenance failed 3 times in a row, or failed only
    /// the last probe of a pass that proved, or whose UTXO the stream is still writing.
    pub failing_outpoints: Vec<String>,
    /// Residual [lo, hi] hex key brackets after repair: keyspace this indexer never observed, a
    /// gap the stream rewrote during the pass, or one that failed only the last probe.
    pub blind_spots: Vec<[String; 2]>,
    /// Residual [hex key, name] rows whose deed address is refuted but whose demotion did not
    /// apply, whose key the stream wrote during the pass, or that failed only the last probe.
    pub owner_unknown: Vec<[String; 2]>,
    pub repaired: RepairSummary,
    /// What lookups withhold under this verdict: the same keys the lists above name.
    pub withheld: Arc<Withheld>,
}

#[derive(Debug, Clone, Default)]
pub struct RepairSummary {
    pub dropped: u64,
    /// Rows with refuted ownership, kept as owner-unknown.
    pub demoted: u64,
    pub readopted: u64,
    pub bridged: u64,
    /// Rows whose outpoint, value and acceptance DAA were rewritten from the observed UTXO.
    pub rewritten: u64,
    /// Gap cache rows adopted, corrected or dropped. Not a verdict input.
    pub gaps_synced: u64,
    /// Card rows the probe marked swept. Not a verdict input.
    pub cards_swept: u64,
    /// Card rows the probe marked unswept again.
    pub cards_restored: u64,
    pub iterations: u32,
}

/// The exact `/snapshot` bytes the last passing self-test proved.
#[derive(Debug)]
pub struct Proof {
    pub bodies: SnapshotBodies,
    /// The body's `provenAt`, which anchors its expiry.
    pub proven_ms: u64,
}

#[derive(Debug, Default)]
pub struct Health {
    pub last_block: Option<BlockRef>,
    pub selftest: Option<SelfTestReport>,
}

pub struct Verdict {
    pub health: RwLock<Health>,
    /// Replaced only by a passing self-test.
    pub proof: RwLock<Option<Proof>>,
    pub selftest_trigger: Notify,
    /// When the pass owed by a flux, an unconfirmed repair, a missed publication or a failed
    /// card probe is due, 0 for none.
    pub followup_due_ms: AtomicU64,
    /// The delay of the next owed pass. Doubles while passes keep owing, up to the interval.
    pub followup_delay_ms: AtomicU64,
    followup_cap_ms: u64,
}

impl Verdict {
    pub fn new(selftest_interval: Duration) -> Self {
        let interval = crate::convert::millis(selftest_interval);
        Self {
            health: RwLock::new(Health::default()),
            proof: RwLock::new(None),
            selftest_trigger: Notify::new(),
            followup_due_ms: AtomicU64::new(0),
            followup_delay_ms: AtomicU64::new(FOLLOWUP_MS),
            followup_cap_ms: interval.max(FOLLOWUP_MS),
        }
    }

    /// Whether a self-test of this process has reached a verdict. Until then the tables can
    /// hold unchecked imported rows, so nothing read from them is served.
    pub async fn judged(&self) -> bool {
        self.health.read().await.selftest.is_some()
    }

    pub fn trigger_selftest(&self) {
        self.selftest_trigger.notify_one();
    }

    /// Owes one pass after the current delay, which lets the stream settle, and doubles the
    /// delay up to the interval. A pass that owes none settles the debt.
    pub fn owe_followup(&self, owed: bool) {
        if owed {
            let delay = self.followup_delay_ms.load(Ordering::Relaxed);
            if self.followup_due_ms.compare_exchange(0, App::now_ms() + delay, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                self.followup_delay_ms.store(delay.saturating_mul(2).min(self.followup_cap_ms), Ordering::Relaxed);
            }
        } else {
            self.followup_due_ms.store(0, Ordering::Relaxed);
            self.followup_delay_ms.store(FOLLOWUP_MS, Ordering::Relaxed);
        }
    }

    pub async fn withheld(&self) -> Arc<Withheld> {
        match &self.health.read().await.selftest {
            Some(t) => t.withheld.clone(),
            None => Arc::new(Withheld::all()),
        }
    }

    pub fn followup_due(&self) -> bool {
        let due = self.followup_due_ms.load(Ordering::Relaxed);
        due != 0 && App::now_ms() >= due && self.followup_due_ms.compare_exchange(due, 0, Ordering::Relaxed, Ordering::Relaxed).is_ok()
    }
}
