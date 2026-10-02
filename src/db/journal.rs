use std::collections::HashSet;

use anyhow::Result;
use sqlx::{PgConnection, Row};

use super::{CardMarkSql, DeedStateSql, GapImage, GapRowSql, gap_image_from_sql, gap_image_sql, key32};
use crate::convert::{fit, sql_i64, sql_u64};
use crate::model::{CardMark, DeedRow, HistoryOp};

#[derive(Debug)]
pub struct EventRow {
    pub block_hash: [u8; 32],
    pub seq: i32,
    pub blue_score: u64,
    pub key: [u8; 32],
    pub prev: Option<DeedRow>,
    pub prev_gaps: Vec<GapImage>,
    pub prev_cards: Vec<CardMark>,
}

pub async fn event_exists(conn: &mut PgConnection, block_hash: &[u8; 32], seq: i32) -> Result<bool> {
    let hit: Option<i32> = sqlx::query_scalar("SELECT 1 FROM events WHERE block_hash = $1 AND seq = $2")
        .bind(block_hash.to_vec())
        .bind(seq)
        .fetch_optional(conn)
        .await?;
    Ok(hit.is_some())
}

pub async fn keys_touched_since(conn: &mut PgConnection, keys: &[[u8; 32]], blue_score: u64) -> Result<HashSet<[u8; 32]>> {
    let rows: Vec<Vec<u8>> = sqlx::query_scalar("SELECT DISTINCT key FROM events WHERE blue_score > $1 AND key = ANY($2)")
        .bind(sql_i64(blue_score)?)
        .bind(keys.iter().map(|k| k.to_vec()).collect::<Vec<_>>())
        .fetch_all(conn)
        .await?;
    rows.iter().map(|k| k.as_slice().try_into().map_err(|_| anyhow::anyhow!("a journal key is not 32 bytes"))).collect()
}

/// A hit means the table missed a registration strictly inside `lo..hi`.
pub async fn discovered_inside_since(conn: &mut PgConnection, lo: &[u8; 32], hi: &[u8; 32], blue_score: u64) -> Result<bool> {
    let hit: Option<i32> =
        sqlx::query_scalar("SELECT 1 FROM history WHERE op = $1 AND key > $2 AND key < $3 AND blue_score > $4 LIMIT 1")
            .bind(HistoryOp::Discover as i16)
            .bind(lo.to_vec())
            .bind(hi.to_vec())
            .bind(sql_i64(blue_score)?)
            .fetch_optional(conn)
            .await?;
    Ok(hit.is_some())
}

pub async fn gaps_touched_since(conn: &mut PgConnection, los: &[[u8; 32]], blue_score: u64) -> Result<HashSet<[u8; 32]>> {
    let rows: Vec<Vec<u8>> =
        sqlx::query_scalar("SELECT DISTINCT (g).lo FROM events, unnest(prev_gaps) g WHERE blue_score > $1 AND (g).lo = ANY($2)")
            .bind(sql_i64(blue_score)?)
            .bind(los.iter().map(|k| k.to_vec()).collect::<Vec<_>>())
            .fetch_all(conn)
            .await?;
    rows.iter().map(|k| k.as_slice().try_into().map_err(|_| anyhow::anyhow!("a journal gap key is not 32 bytes"))).collect()
}

pub async fn append_event(conn: &mut PgConnection, event: &EventRow) -> Result<()> {
    let gaps: Vec<GapRowSql> = event.prev_gaps.iter().map(|(lo, row)| gap_image_sql(lo, row.as_ref())).collect::<Result<_>>()?;
    let cards: Vec<CardMarkSql> = event.prev_cards.iter().map(CardMarkSql::try_from).collect::<Result<_>>()?;
    sqlx::query(
        "INSERT INTO events (block_hash, seq, blue_score, key, prev, prev_gaps, prev_cards) VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(event.block_hash.as_slice())
    .bind(event.seq)
    .bind(sql_i64(event.blue_score)?)
    .bind(event.key.as_slice())
    .bind(event.prev.as_ref().map(DeedStateSql::try_from).transpose()?)
    .bind(gaps)
    .bind(cards)
    .execute(conn)
    .await?;
    Ok(())
}

fn event_from_pg(r: &sqlx::postgres::PgRow) -> Result<EventRow> {
    let prev: Option<DeedStateSql> = r.try_get("prev")?;
    let prev_gaps: Vec<GapRowSql> = r.try_get("prev_gaps")?;
    let prev_cards: Vec<CardMarkSql> = r.try_get("prev_cards")?;
    Ok(EventRow {
        block_hash: key32(r.try_get("block_hash")?, "block hash")?,
        seq: r.try_get("seq")?,
        blue_score: sql_u64(r.try_get("blue_score")?)?,
        key: key32(r.try_get("key")?, "event key")?,
        prev: prev.map(DeedRow::try_from).transpose()?,
        prev_gaps: prev_gaps.into_iter().map(gap_image_from_sql).collect::<Result<_>>()?,
        prev_cards: prev_cards.into_iter().map(CardMark::try_from).collect::<Result<_>>()?,
    })
}

pub async fn events_for_block_desc(conn: &mut PgConnection, block_hash: &[u8; 32]) -> Result<Vec<EventRow>> {
    let rows = sqlx::query("SELECT * FROM events WHERE block_hash = $1 ORDER BY seq DESC")
        .bind(block_hash.as_slice())
        .fetch_all(conn)
        .await?;
    rows.iter().map(event_from_pg).collect()
}

pub async fn delete_imported_events_for_block(conn: &mut PgConnection, block_hash: &[u8; 32]) -> Result<u64> {
    Ok(sqlx::query("DELETE FROM events WHERE block_hash = $1 AND NOT EXISTS (SELECT 1 FROM history WHERE block_hash = $1)")
        .bind(block_hash.as_slice())
        .execute(conn)
        .await?
        .rows_affected())
}

pub async fn delete_events_for_block(conn: &mut PgConnection, block_hash: &[u8; 32]) -> Result<u64> {
    Ok(sqlx::query("DELETE FROM events WHERE block_hash = $1").bind(block_hash.as_slice()).execute(conn).await?.rows_affected())
}

/// Callers never pass a watermark above `sink − finality`.
pub async fn prune_events_below(conn: &mut PgConnection, blue_score: u64) -> Result<u64> {
    Ok(sqlx::query("DELETE FROM events WHERE blue_score < $1").bind(sql_i64(blue_score)?).execute(conn).await?.rows_affected())
}

pub async fn journal_coverage(conn: &mut PgConnection) -> Result<Option<u64>> {
    let v: Option<i64> = sqlx::query_scalar("SELECT min(blue_score) FROM events").fetch_one(conn).await?;
    v.map(sql_u64).transpose()
}

/// The `/snapshot` resume section, oldest first. The cap keeps the newest rows, because the section bridges a reorg at
/// the tip. A binding cap drops the oldest blue score whole, because undo restores a block strictly LIFO and an import
/// can write two blocks with one blue score. The extra row from `cap + 1` tells a binding cap from an exact fit.
pub async fn events_for_resume(conn: &mut PgConnection, blue_score_floor: u64, cap: i64) -> Result<Vec<EventRow>> {
    let rows = sqlx::query(
        "SELECT * FROM (
           SELECT * FROM events WHERE blue_score >= $1
           ORDER BY blue_score DESC, block_hash DESC, seq DESC
           LIMIT $2
         ) newest ORDER BY blue_score, block_hash, seq",
    )
    .bind(sql_i64(blue_score_floor)?)
    .bind(cap.saturating_add(1))
    .fetch_all(conn)
    .await?;
    let mut out: Vec<EventRow> = rows.iter().map(event_from_pg).collect::<Result<_>>()?;
    if fit::<_, i64>(out.len())? > cap
        && let Some(oldest) = out.first().map(|e| e.blue_score)
    {
        out.retain(|e| e.blue_score != oldest);
    }
    Ok(out)
}

/// Pre-images of deleted or changed rows, newest first, for the audit's re-adoption.
pub async fn mine_candidates(conn: &mut PgConnection) -> Result<Vec<([u8; 32], DeedRow)>> {
    // On a row type `prev IS NOT NULL` means every field is non-null, and `NOT (prev IS NULL)` tests the column.
    let rows = sqlx::query("SELECT key, prev FROM events WHERE NOT (prev IS NULL) ORDER BY blue_score DESC, seq DESC LIMIT 10000")
        .fetch_all(conn)
        .await?;
    rows.iter()
        .map(|r| {
            let prev: DeedStateSql = r.try_get("prev")?;
            Ok((key32(r.try_get("key")?, "event key")?, DeedRow::try_from(prev)?))
        })
        .collect()
}
