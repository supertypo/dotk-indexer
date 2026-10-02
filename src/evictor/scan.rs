//! The scan for deeds the evictor can end.

use anyhow::Result;
use dotk_core::state::{DeedState, OwnerType, Status, ZERO32};

use super::Exit;
use crate::db;
use crate::derive;

/// The keys this tick acts on, lowest first, so neither scan starves the other on a wallet that
/// runs out. `watch` is `Some` only in funded mode, because a rewritten release takes the deed's
/// own bond and gap value. A `StrangerRelease` is a candidate that `attempt`
/// derives again from a fresh read.
pub async fn scan_targets(
    conn: &mut sqlx::PgConnection,
    watch: Option<&dotk_core::watch::WatchTemplates>,
    ripe_before: u64,
) -> Result<Vec<([u8; 32], Exit)>> {
    let mut targets: Vec<_> = db::ripe_pending(conn, ripe_before).await?.into_iter().map(|k| (k, Exit::Evict)).collect();
    if let Some(watch) = watch {
        for c in db::script_hash_owned_neighborhoods(conn).await? {
            let deed =
                DeedState { status: Status::Active, key: c.key, owner_type: OwnerType::ScriptHash, owner: c.owner, name: ZERO32 };
            let (pred, succ) = derive::flanks(&c.key, c.below, c.above);
            if watch.exposed_flank(&deed, &pred, &succ).is_some() {
                targets.push((c.key, Exit::StrangerRelease));
            }
        }
    }
    targets.sort_by_key(|(key, _)| *key);
    Ok(targets)
}
