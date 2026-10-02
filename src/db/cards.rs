use anyhow::Result;
use dotk_core::cards::CardState;
use dotk_core::state::OwnerType;
use sqlx::{PgConnection, Row};

use super::key32;
use crate::convert::{sql_i32, sql_i64, sql_u8, sql_u32, sql_u64};
use crate::model::CardRow;

fn card_from_pg(r: &sqlx::postgres::PgRow) -> Result<CardRow> {
    let spender_type: i16 = r.try_get("spender_type")?;
    let spender_type =
        OwnerType::from_byte(sql_u8(spender_type)?).ok_or_else(|| anyhow::anyhow!("unknown card spender scheme {spender_type}"))?;
    Ok(CardRow {
        txid: key32(r.try_get("txid")?, "card txid")?,
        idx: sql_u32(r.try_get("idx")?)?,
        state: CardState::new(
            key32(r.try_get("key")?, "card key")?,
            key32(r.try_get("records")?, "card records")?,
            spender_type,
            key32(r.try_get("spender")?, "card spender")?,
        )?,
        blob: r.try_get("blob")?,
        value: sql_u64(r.try_get("value")?)?,
        swept_at: r.try_get::<Option<i64>, _>("swept_at")?.map(sql_u64).transpose()?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardHit {
    pub card: CardRow,
    pub name: Option<String>,
    pub live: bool,
}

const CARD_HIT: &str = "SELECT c.*, d.name AS deed_name, \
       (d.kind = 0 AND d.outpoint_txid = c.txid AND c.idx = 1) AS live \
     FROM cards c LEFT JOIN deeds d ON d.key = c.key";

fn card_hit_from_pg(r: &sqlx::postgres::PgRow) -> Result<CardHit> {
    Ok(CardHit { card: card_from_pg(r)?, name: r.try_get("deed_name")?, live: r.try_get::<Option<bool>, _>("live")?.unwrap_or(false) })
}

pub async fn get_card(conn: &mut PgConnection, txid: &[u8; 32], idx: u32) -> Result<Option<CardRow>> {
    let row = sqlx::query("SELECT * FROM cards WHERE txid = $1 AND idx = $2")
        .bind(txid.as_slice())
        .bind(sql_i32(idx)?)
        .fetch_optional(conn)
        .await?;
    row.map(|r| card_from_pg(&r)).transpose()
}

/// An existing row is a duplicate delivery of the same transfer. The journal records whether the row was inserted.
pub async fn insert_card_if_absent(conn: &mut PgConnection, card: &CardRow) -> Result<bool> {
    let res = sqlx::query(
        "INSERT INTO cards (txid, idx, key, records, spender_type, spender, blob, value, swept_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (txid, idx) DO NOTHING",
    )
    .bind(card.txid.as_slice())
    .bind(sql_i32(card.idx)?)
    .bind(card.state.key.as_slice())
    .bind(card.state.records.as_slice())
    .bind(i16::from(card.state.spender_type as u8))
    .bind(card.state.spender.as_slice())
    .bind(&card.blob)
    .bind(sql_i64(card.value)?)
    .bind(card.swept_at.map(sql_i64).transpose()?)
    .execute(conn)
    .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn delete_card(conn: &mut PgConnection, txid: &[u8; 32], idx: u32) -> Result<()> {
    sqlx::query("DELETE FROM cards WHERE txid = $1 AND idx = $2").bind(txid.as_slice()).bind(sql_i32(idx)?).execute(conn).await?;
    Ok(())
}

pub async fn update_card_value_if_matches(conn: &mut PgConnection, txid: &[u8; 32], idx: u32, from: u64, to: u64) -> Result<bool> {
    let res = sqlx::query("UPDATE cards SET value = $4 WHERE txid = $1 AND idx = $2 AND value = $3")
        .bind(txid.as_slice())
        .bind(sql_i32(idx)?)
        .bind(sql_i64(from)?)
        .bind(sql_i64(to)?)
        .execute(conn)
        .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn set_card_swept(conn: &mut PgConnection, txid: &[u8; 32], idx: u32, swept_at: Option<u64>) -> Result<bool> {
    let res = sqlx::query("UPDATE cards SET swept_at = $3 WHERE txid = $1 AND idx = $2")
        .bind(txid.as_slice())
        .bind(sql_i32(idx)?)
        .bind(swept_at.map(sql_i64).transpose()?)
        .execute(conn)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Writes `to` only if the row still carries `from`, so a mark the stream set since the probe survives.
pub async fn mark_card_if_matches(
    conn: &mut PgConnection,
    txid: &[u8; 32],
    idx: u32,
    from: Option<u64>,
    to: Option<u64>,
) -> Result<bool> {
    let res = sqlx::query("UPDATE cards SET swept_at = $4 WHERE txid = $1 AND idx = $2 AND swept_at IS NOT DISTINCT FROM $3")
        .bind(txid.as_slice())
        .bind(sql_i32(idx)?)
        .bind(from.map(sql_i64).transpose()?)
        .bind(to.map(sql_i64).transpose()?)
        .execute(conn)
        .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn live_card_by_key(conn: &mut PgConnection, key: &[u8; 32]) -> Result<Option<CardHit>> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "{CARD_HIT} WHERE c.key = $1 AND c.idx = 1 AND c.swept_at IS NULL AND d.kind = 0 AND d.outpoint_txid = c.txid"
    )))
    .bind(key.as_slice())
    .fetch_optional(conn)
    .await?;
    row.map(|r| card_hit_from_pg(&r)).transpose()
}

pub async fn live_cards_by_owner(conn: &mut PgConnection, owner_type: u8, owner: &[u8; 32]) -> Result<Vec<CardHit>> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "{CARD_HIT} WHERE d.kind = 0 AND d.owner_type = $1 AND d.owner = $2 AND d.outpoint_txid = c.txid AND c.idx = 1 \
         AND c.swept_at IS NULL ORDER BY d.name"
    )))
    .bind(i16::from(owner_type))
    .bind(owner.as_slice())
    .fetch_all(conn)
    .await?;
    rows.iter().map(card_hit_from_pg).collect()
}

/// Live or not, so a wallet finds the cards it left behind.
pub async fn cards_by_spender(
    conn: &mut PgConnection,
    spender_type: u8,
    spender: &[u8; 32],
    after: Option<([u8; 32], u32)>,
    limit: i64,
) -> Result<Vec<CardHit>> {
    // Two statements keep the cursor an index bound under a generic plan.
    let first =
        format!("{CARD_HIT} WHERE c.spender_type = $1 AND c.spender = $2 AND c.swept_at IS NULL ORDER BY c.txid, c.idx LIMIT $3");
    let later = format!(
        "{CARD_HIT} WHERE c.spender_type = $1 AND c.spender = $2 AND c.swept_at IS NULL AND (c.txid, c.idx) > ($4, $5) \
         ORDER BY c.txid, c.idx LIMIT $3"
    );
    let query = sqlx::query(sqlx::AssertSqlSafe(if after.is_some() { later } else { first }))
        .bind(i16::from(spender_type))
        .bind(spender.as_slice())
        .bind(limit);
    let query = match after {
        Some((txid, idx)) => query.bind(txid.to_vec()).bind(sql_i32(idx)?),
        None => query,
    };
    query.fetch_all(conn).await?.iter().map(card_hit_from_pg).collect()
}

/// Swept rows included, so the self-test restores a card whose sweep the chain reorged out.
pub async fn all_cards(conn: &mut PgConnection) -> Result<Vec<CardRow>> {
    let rows = sqlx::query("SELECT * FROM cards ORDER BY txid, idx").fetch_all(conn).await?;
    rows.iter().map(card_from_pg).collect()
}

pub async fn unswept_cards(conn: &mut PgConnection) -> Result<Vec<CardRow>> {
    let rows = sqlx::query("SELECT * FROM cards WHERE swept_at IS NULL ORDER BY txid, idx").fetch_all(conn).await?;
    rows.iter().map(card_from_pg).collect()
}

/// Past the journal's retention watermark no reorg can bring the UTXO back, so nothing clears the mark.
pub async fn purge_swept_cards_below(conn: &mut PgConnection, blue_score: u64) -> Result<u64> {
    Ok(sqlx::query("DELETE FROM cards WHERE swept_at < $1").bind(sql_i64(blue_score)?).execute(conn).await?.rows_affected())
}
