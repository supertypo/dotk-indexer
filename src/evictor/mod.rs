//! Ends ripe pending deeds and exposed p2sh deeds for their bond and gap value.

mod exits;
mod payout;
mod scan;

use std::sync::Arc;

use anyhow::Result;

use self::exits::attempt;
use self::payout::wallet_utxos;
use crate::app::App;
use crate::convert::hex32;
pub(crate) use payout::Payout;
pub use scan::scan_targets;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    Evict,
    StrangerRelease,
}

pub async fn run(app: Arc<App>) {
    let payout = match Payout::from_args(&app.deployment.args, app.deployment.prefix) {
        Ok(Some(p)) => p,
        Ok(None) => return,
        Err(e) => {
            log::error!("evictor disabled: {e:#}");
            return;
        }
    };
    if !payout.is_unfunded() {
        log::info!("evictor funding and paying out at {}", payout.address);
    }

    loop {
        if !app.progress.sleep(app.deployment.args.evictor_tick).await {
            return;
        }
        // Any verdict suffices. Every exit re-proves its deed and both flanks on chain.
        if !app.progress.caught_up() || !app.verdict.judged().await {
            continue;
        }
        if let Err(e) = tick(&app, &payout).await {
            log::warn!("evictor tick failed: {e:#}");
        }
    }
}

async fn tick(app: &App, payout: &Payout) -> Result<()> {
    let dag = app.backends.kaspad.dag_info().await?;
    let market = app.backends.kaspad.market().await;
    let ripe_before = dag.virtual_daa.saturating_sub(app.deployment.genesis.params.t_evict);
    let targets = {
        let mut conn = app.backends.db.acquire().await?;
        scan_targets(&mut conn, (!payout.is_unfunded()).then_some(&app.deployment.watch), ripe_before).await?
    };
    if targets.is_empty() {
        return Ok(());
    }
    let mut wallet = match payout.secret {
        Some(_) => Some(wallet_utxos(&app.backends, payout).await?),
        None => None,
    };
    for (key, exit) in targets {
        // Saves a read and a probe per deed. `submit_exit` refuses this case too.
        if wallet.as_ref().is_some_and(|w| w.peek().is_none()) {
            log::warn!(
                "more actionable deeds than wallet UTXOs at {}: {} and everything above it must wait, \
                 because an exit must carry a signed funding input or its bounty is a bearer value. The \
                 next tick continues once this tick's change confirms",
                payout.address,
                hex32(&key)
            );
            return Ok(());
        }
        if let Err(e) = attempt(app, &key, payout, &mut wallet, market, ripe_before, exit).await {
            log::warn!("exit attempt for {} failed: {e:#}", hex32(&key));
        }
        if app.progress.is_shutdown() {
            return Ok(());
        }
    }
    Ok(())
}
