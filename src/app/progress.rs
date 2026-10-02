use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::watch;

use super::App;

/// Maximum header age of the last committed block for the indexer to count as caught up.
/// Against a stalled node the blue-score distance stays at zero, so this is the rule that fires.
pub const CAUGHT_UP_MAX_AGE_MS: u64 = 10_000;
/// Maximum distance behind the tip, in seconds of blocks, for the same verdict. A backstop for
/// a host clock that is behind.
pub const CAUGHT_UP_MAX_LAG_SECS: u64 = 60;
/// How long the indexer can lag before `/health` answers 503, so one slow poll does not flap
/// every client's endpoint choice.
pub const UNHEALTHY_AFTER_MS: u64 = 10_000;

#[derive(Debug)]
pub struct Progress {
    /// The lowest blue score undo is exact for. A reorg below it escalates to full validation.
    /// An import seeds it (`u64::MAX` without a journal), each batch lowers it to the first
    /// block this process journaled, and pruning raises it to the retention watermark.
    pub coverage_floor: AtomicU64,
    /// The last committed block's header time (ms) and scores, 0 before the first commit.
    pub last_block_ms: AtomicU64,
    pub last_block_blue_score: AtomicU64,
    pub last_block_daa_score: AtomicU64,
    /// When the indexer was last observed caught up (ms since epoch), 0 for never.
    pub caught_up_ms: AtomicU64,
    /// The last observed verdict, so each transition logs once.
    pub was_caught_up: AtomicBool,
    /// The sink's blue score as last observed, 0 before the first observation. Only ever
    /// raised, so a node outage freezes it rather than clearing it.
    pub tip_blue_score: AtomicU64,
    shutdown: watch::Sender<bool>,
    tip_distance: u64,
    net_bps: u64,
}

impl Progress {
    pub fn new(coverage_floor: u64, tip_distance: u64, net_bps: u64) -> Self {
        Self {
            coverage_floor: AtomicU64::new(coverage_floor),
            last_block_ms: AtomicU64::new(0),
            last_block_blue_score: AtomicU64::new(0),
            last_block_daa_score: AtomicU64::new(0),
            caught_up_ms: AtomicU64::new(0),
            was_caught_up: AtomicBool::new(false),
            tip_blue_score: AtomicU64::new(0),
            shutdown: watch::Sender::new(false),
            tip_distance,
            net_bps,
        }
    }

    pub fn request_shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    pub fn is_shutdown(&self) -> bool {
        *self.shutdown.borrow()
    }

    pub async fn until_shutdown(&self) {
        let mut shutdown = self.shutdown.subscribe();
        // The sender lives as long as `self`, so the wait ends only on the request.
        let _ = shutdown.wait_for(|requested| *requested).await;
    }

    /// `false` if shutdown comes first.
    pub async fn sleep(&self, dur: Duration) -> bool {
        tokio::select! {
            () = tokio::time::sleep(dur) => !self.is_shutdown(),
            () = self.until_shutdown() => false,
        }
    }

    /// Blue scores between `processed` and the sink, less the distance the follow loop keeps.
    pub fn lag(&self, processed: u64) -> u64 {
        self.tip_blue_score.load(Ordering::Relaxed).saturating_sub(self.tip_distance).saturating_sub(processed)
    }

    fn behind(&self) -> u64 {
        self.lag(self.last_block_blue_score.load(Ordering::Relaxed))
    }

    /// The tip freezes while the node is unreachable, so only the age check notices an outage
    /// or a stall at the tip.
    pub fn caught_up(&self) -> bool {
        let stamped = self.last_block_ms.load(Ordering::Relaxed);
        if stamped == 0 {
            return false;
        }
        let age = App::now_ms().saturating_sub(stamped);
        age < CAUGHT_UP_MAX_AGE_MS && self.behind() < CAUGHT_UP_MAX_LAG_SECS * self.net_bps
    }

    /// The follow loop calls this on every poll and commit, which makes it the clock
    /// `lag_tolerated` measures against.
    pub fn observe_caught_up(&self) -> bool {
        let caught_up = self.caught_up();
        if caught_up {
            self.caught_up_ms.store(App::now_ms(), Ordering::Relaxed);
        }
        if self.was_caught_up.swap(caught_up, Ordering::Relaxed) != caught_up {
            if caught_up {
                log::info!("caught up to the virtual chain");
            } else {
                let age = App::now_ms().saturating_sub(self.last_block_ms.load(Ordering::Relaxed));
                let behind = self.behind();
                log::warn!("fell behind the virtual chain: last block {age} ms old, {behind} blue scores behind the tip");
            }
        }
        caught_up
    }

    pub fn lag_tolerated(&self) -> bool {
        if self.observe_caught_up() {
            return true;
        }
        match self.caught_up_ms.load(Ordering::Relaxed) {
            0 => false,
            at => App::now_ms().saturating_sub(at) <= UNHEALTHY_AFTER_MS,
        }
    }
}
