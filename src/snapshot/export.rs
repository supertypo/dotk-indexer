use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::PgPool;

use super::format::{ExportCard, ExportDeed, ExportEvent, Snapshot};
use crate::app::{SnapshotBodies, Tagged};
use crate::convert::sql_u64;
use crate::db;
use crate::model::DeedRow;

/// Seconds of chain the resume section covers, which bridges only a shallow reorg after import. A section the size of
/// retention is open to abuse, because cheap self-transfers put hundreds of megabytes of journal into every download.
const EXPORT_EVENT_SECONDS: u64 = 10;

/// Backstop on the resume section's row count, because a block journals as many events as its mass admits.
pub const EXPORT_EVENT_CAP: i64 = 20_000;

/// In blue scores, because the journal is keyed on them.
pub fn export_event_window(net_bps: u64) -> u64 {
    net_bps * EXPORT_EVENT_SECONDS
}

/// The one [`export`] failure that is a state, not a fault, so callers answer 503 for it.
#[derive(Debug)]
pub(crate) struct NoCheckpoint;

impl std::fmt::Display for NoCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("no vcp checkpoint: the indexer has not processed anything yet")
    }
}

impl std::error::Error for NoCheckpoint {}

/// One repeatable read transaction, so a batch that commits mid-export cannot tear the resume
/// section away from the canonical core.
pub async fn export(pool: &PgPool, registry_covenant_id: &str, resume_window: u64, statement_timeout: Duration) -> Result<Snapshot> {
    Ok(export_rows(pool, registry_covenant_id, resume_window, statement_timeout).await?.0)
}

/// Also returns the raw deed rows, so the audit's verdict is about exactly these bytes.
/// `statement_timeout` replaces the session's for this transaction, and zero disables it.
pub(crate) async fn export_rows(
    pool: &PgPool,
    registry_covenant_id: &str,
    resume_window: u64,
    statement_timeout: Duration,
) -> Result<(Snapshot, Vec<([u8; 32], DeedRow)>)> {
    let mut conn = pool.acquire().await?;
    let mut tx = db::begin_repeatable_read(&mut conn).await?;
    db::set_local_statement_timeout(&mut tx, statement_timeout).await?;
    let checkpoint = db::get_var(&mut tx, db::VAR_VCP_CHECKPOINT).await?.ok_or(NoCheckpoint)?;
    let history_seq: Option<i64> = sqlx::query_scalar("SELECT max(id) FROM history").fetch_one(&mut *tx).await?;
    let deeds = db::all_deeds(&mut tx).await?;
    let cards = db::unswept_cards(&mut tx).await?;
    let events = resume_events(&mut tx, resume_window).await?;
    tx.commit().await?;
    let export = Snapshot {
        proven: false,
        proven_at: None,
        registry_covenant_id: registry_covenant_id.to_string(),
        vcp_checkpoint: checkpoint,
        history_seq,
        deeds: deeds.iter().map(ExportDeed::from).collect(),
        cards: cards.iter().map(ExportCard::from).collect(),
        events: events.iter().map(ExportEvent::from).collect(),
    };
    Ok((export, deeds))
}

/// The window ends at the newest journal entry, not the observed tip, because during catch-up
/// only the journal describes what this snapshot holds.
async fn resume_events(conn: &mut sqlx::PgConnection, resume_window: u64) -> Result<Vec<db::EventRow>> {
    let newest: Option<u64> = sqlx::query_scalar::<_, Option<i64>>("SELECT max(blue_score) FROM events")
        .fetch_one(&mut *conn)
        .await?
        .map(sql_u64)
        .transpose()?;
    let events = match newest {
        Some(top) => db::events_for_resume(conn, top.saturating_sub(resume_window), EXPORT_EVENT_CAP).await?,
        None => Vec::new(),
    };
    if newest.is_some() && events.is_empty() {
        // Only the cap does this, because it trims a blue score whole.
        log::warn!(
            "the resume section came out empty: one blue score carries more than {EXPORT_EVENT_CAP} journal \
             events, so trimming it to a whole block left nothing. An importer of this snapshot escalates its \
             first reorg to a full validation."
        );
    }
    Ok(events)
}

/// Serializes both shapes `/snapshot` serves. The events move out and back, because cloning copies the large deeds.
pub(crate) fn bodies(export: &mut Snapshot) -> Result<SnapshotBodies> {
    let events = std::mem::take(&mut export.events);
    let without_events = serde_json::to_vec(export);
    export.events = events;
    let without_events = without_events.context("serializing the snapshot without its resume section")?;
    let with_events = serde_json::to_vec(export).context("serializing the snapshot")?;
    Ok(SnapshotBodies { with_events: Tagged::new(with_events), without_events: Tagged::new(without_events) })
}
