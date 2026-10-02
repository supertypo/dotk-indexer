use std::collections::HashSet;

use anyhow::{Context, Result};
use dotk_core::watch::GenesisFile;
use sqlx::{PgConnection, PgPool};

use super::export::EXPORT_EVENT_CAP;
use super::format::Snapshot;
use crate::chain::{CATCHUP_DEADLINE, Kaspad, Unresumable};
use crate::convert::fit;
use crate::convert::unhex32;
use crate::db::{self, EventRow};
use crate::model::{CardRow, DeedRow};

const SINK_MARGIN_SECS: u64 = 600;

pub async fn verify_checkpoint(kaspad: &dyn Kaspad, snapshot: &Snapshot, net_bps: u64) -> Result<()> {
    anyhow::ensure!(
        fit::<_, i64>(snapshot.events.len())? <= EXPORT_EVENT_CAP,
        "the body carries {} journal events, above the {EXPORT_EVENT_CAP} an export writes",
        snapshot.events.len()
    );
    let checkpoint = unhex32(&snapshot.vcp_checkpoint).context("the body's vcpCheckpoint")?;
    // From an old checkpoint the answer holds a catch-up's worth of blocks.
    let after = match kaspad.virtual_chain(checkpoint, 0, CATCHUP_DEADLINE).await {
        Ok(res) => res,
        Err(e) if e.is::<Unresumable>() => {
            return Err(e.context(format!(
                "the body's checkpoint {} is unknown to the node (pruned or foreign), so the pipeline can never resume from it",
                snapshot.vcp_checkpoint
            )));
        }
        Err(e) => return Err(e.context("asking the node about the body's checkpoint")),
    };
    let ceiling = match after.added.first() {
        Some(next) if after.removed.is_empty() => next.blue_score.saturating_sub(1),
        _ => kaspad
            .sink_blue_score()
            .await
            .context("asking the node for its sink")?
            .saturating_add(net_bps.saturating_mul(SINK_MARGIN_SECS)),
    };
    if let Some(e) = snapshot.events.iter().find(|e| e.blue_score > ceiling) {
        anyhow::bail!("the body journals key {} at blue score {}, above the {ceiling} its checkpoint allows", e.key, e.blue_score);
    }
    Ok(())
}

/// Imports into a freshly initialized database, in one transaction. Full UTXO validation runs
/// after catch-up. Returns the checkpoint and the imported journal's coverage floor.
pub async fn import(pool: &PgPool, snapshot: &Snapshot, genesis: &GenesisFile) -> Result<([u8; 32], Option<u64>)> {
    anyhow::ensure!(
        snapshot.registry_covenant_id == genesis.registry_covenant_id,
        "snapshot is for registry {} but this deployment declares {}, so refusing",
        snapshot.registry_covenant_id,
        genesis.registry_covenant_id
    );
    let checkpoint = unhex32(&snapshot.vcp_checkpoint).context("snapshot vcpCheckpoint")?;
    let mut tx = pool.begin().await?;
    let mut seen: HashSet<[u8; 32]> = HashSet::with_capacity(snapshot.deeds.len());
    for d in &snapshot.deeds {
        let (key, row) = <([u8; 32], DeedRow)>::try_from(d)?;
        anyhow::ensure!(seen.insert(key), "snapshot names the deed key {} twice, so one of the two rows vanishes", d.key);
        db::upsert_deed(&mut tx, &key, &row).await?;
    }
    for c in &snapshot.cards {
        let row = CardRow::try_from(c).with_context(|| format!("snapshot card {}:{}", c.txid, c.idx))?;
        anyhow::ensure!(db::insert_card_if_absent(&mut tx, &row).await?, "snapshot names the card {}:{} twice", c.txid, c.idx);
    }
    let coverage = import_events(&mut tx, snapshot).await?;
    db::set_var(&mut tx, db::VAR_VCP_CHECKPOINT, &snapshot.vcp_checkpoint).await?;
    tx.commit().await?;
    Ok((checkpoint, coverage))
}

/// Returns the lowest blue score the journal covers.
async fn import_events(conn: &mut PgConnection, snapshot: &Snapshot) -> Result<Option<u64>> {
    let mut coverage: Option<u64> = None;
    for e in &snapshot.events {
        let e = EventRow::try_from(e)?;
        db::append_event(conn, &e).await?;
        coverage = Some(coverage.map_or(e.blue_score, |c: u64| c.min(e.blue_score)));
    }
    Ok(coverage)
}
