use std::sync::Arc;

use dotk_core::watch::{GenesisFile, WatchTemplates};
use kaspa_addresses::Prefix;
use sqlx::PgPool;

use crate::chain::Kaspad;
use crate::config::CliArgs;

mod backends;
mod caches;
mod deployment;
mod progress;
mod verdict;

pub use backends::Backends;
pub use caches::{Caches, SnapshotBodies, Tagged};
pub use deployment::Deployment;
pub use progress::{CAUGHT_UP_MAX_AGE_MS, CAUGHT_UP_MAX_LAG_SECS, Progress, UNHEALTHY_AFTER_MS};
pub use verdict::{BlockRef, Proof, RepairSummary, SelfTestReport, Verdict};

/// What the network and the manifest determine, derived once at boot.
pub struct Derived {
    pub watch: WatchTemplates,
    pub prefix: Prefix,
    pub net_bps: u64,
}

impl Derived {
    pub fn new(network: &str, genesis: &GenesisFile) -> anyhow::Result<Derived> {
        Ok(Derived {
            watch: genesis.watch_templates().map_err(anyhow::Error::msg)?,
            prefix: dotk_core::address::prefix_for(network)?,
            net_bps: dotk_core::fees::net_bps(network)?,
        })
    }
}

pub struct App {
    pub deployment: Deployment,
    pub backends: Backends,
    pub progress: Progress,
    pub verdict: Verdict,
    pub caches: Caches,
}

impl App {
    pub fn new(
        args: CliArgs,
        db: PgPool,
        kaspad: Arc<dyn Kaspad>,
        genesis: GenesisFile,
        genesis_raw: impl Into<axum::body::Bytes>,
        coverage_floor: u64,
    ) -> anyhow::Result<App> {
        let derived = Derived::new(&args.network, &genesis)?;
        Ok(Self::with(args, db, kaspad, genesis, genesis_raw.into(), derived, coverage_floor))
    }

    pub fn with(
        args: CliArgs,
        db: PgPool,
        kaspad: Arc<dyn Kaspad>,
        genesis: GenesisFile,
        genesis_raw: axum::body::Bytes,
        derived: Derived,
        coverage_floor: u64,
    ) -> App {
        let Derived { watch, prefix, net_bps } = derived;
        let web_db = crate::db::sibling(&db, args.web_db_pool_size, args.web_request_timeout);
        App {
            progress: Progress::new(coverage_floor, args.vcp_tip_distance, net_bps),
            verdict: Verdict::new(args.selftest_interval),
            caches: Caches::default(),
            backends: Backends { db, web_db, kaspad },
            deployment: Deployment { genesis_raw: Tagged::new(genesis_raw), args, genesis, watch, prefix, net_bps },
        }
    }

    pub async fn refresh_counts(&self) {
        self.caches.refresh_counts(&self.backends.db).await;
    }

    pub fn now_ms() -> u64 {
        crate::convert::millis(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default())
    }
}
