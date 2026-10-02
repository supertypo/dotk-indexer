use anyhow::Result;
use sqlx::{PgConnection, Row};

use super::{DeedStateSql, begin_repeatable_read, key32};
use crate::convert::{sql_i64, sql_u64};
use crate::model::{CardChange, DeedRow, HistoryOp, HistoryRow};

/// Runs in the pipeline's batch transaction beside the journal entry. Only undo deletes history, and a repair writes
/// none, because `/keyspace` counts registrations from here.
pub async fn append_history(conn: &mut PgConnection, row: &HistoryRow) -> Result<()> {
    sqlx::query(
        "INSERT INTO history (key, op, card, seq, blue_score, daa_score, block_time, block_hash, txid, state) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(row.key.as_slice())
    .bind(row.op as i16)
    .bind(row.card as i16)
    .bind(row.seq)
    .bind(sql_i64(row.blue_score)?)
    .bind(sql_i64(row.daa_score)?)
    .bind(sql_i64(row.block_time)?)
    .bind(row.block_hash.as_slice())
    .bind(row.txid.as_slice())
    .bind(row.state.as_ref().map(DeedStateSql::try_from).transpose()?)
    .execute(conn)
    .await?;
    Ok(())
}

fn history_from_pg(r: &sqlx::postgres::PgRow) -> Result<HistoryRow> {
    let op: i16 = r.try_get("op")?;
    let card: i16 = r.try_get("card")?;
    let state: Option<DeedStateSql> = r.try_get("state")?;
    Ok(HistoryRow {
        key: key32(r.try_get("key")?, "history key")?,
        op: HistoryOp::try_from(op).map_err(|op| anyhow::anyhow!("unknown history op {op}"))?,
        card: CardChange::try_from(card).map_err(|card| anyhow::anyhow!("unknown history card change {card}"))?,
        seq: r.try_get("seq")?,
        blue_score: sql_u64(r.try_get("blue_score")?)?,
        daa_score: sql_u64(r.try_get("daa_score")?)?,
        block_time: sql_u64(r.try_get("block_time")?)?,
        block_hash: key32(r.try_get("block_hash")?, "history block hash")?,
        txid: key32(r.try_get("txid")?, "history txid")?,
        state: state.map(DeedRow::try_from).transpose()?,
    })
}

/// `history_key` serves this order up to the `id` tiebreak, so a page never sorts the whole key.
pub async fn history_page(conn: &mut PgConnection, key: &[u8; 32], limit: i64, offset: i64) -> Result<Vec<HistoryRow>> {
    let rows = sqlx::query("SELECT * FROM history WHERE key = $1 ORDER BY blue_score DESC, seq DESC, id DESC LIMIT $2 OFFSET $3")
        .bind(key.as_slice())
        .bind(limit)
        .bind(offset)
        .fetch_all(conn)
        .await?;
    rows.iter().map(history_from_pg).collect()
}

/// One page with the key's total and oldest op, read in one snapshot so they agree. A history is
/// complete when its oldest entry is the key's registration.
pub async fn history_page_with_extent(
    conn: &mut PgConnection,
    key: &[u8; 32],
    limit: i64,
    offset: i64,
) -> Result<(Vec<HistoryRow>, i64, Option<HistoryOp>)> {
    let mut tx = begin_repeatable_read(conn).await?;
    let rows = history_page(&mut tx, key, limit, offset).await?;
    let total: i64 =
        sqlx::query_scalar("SELECT count(*) FROM history WHERE key = $1").bind(key.as_slice()).fetch_one(&mut *tx).await?;
    let oldest: Option<i16> = sqlx::query_scalar("SELECT op FROM history WHERE key = $1 ORDER BY blue_score, seq, id LIMIT 1")
        .bind(key.as_slice())
        .fetch_optional(&mut *tx)
        .await?;
    tx.commit().await?;
    let oldest = oldest.map(|op| HistoryOp::try_from(op).map_err(|op| anyhow::anyhow!("unknown history op {op}"))).transpose()?;
    Ok((rows, total, oldest))
}

pub async fn delete_history_for_block(conn: &mut PgConnection, block_hash: &[u8; 32]) -> Result<u64> {
    Ok(sqlx::query("DELETE FROM history WHERE block_hash = $1").bind(block_hash.as_slice()).execute(conn).await?.rows_affected())
}
