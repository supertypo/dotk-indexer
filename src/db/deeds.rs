use anyhow::Result;
use sqlx::postgres::PgArguments;
use sqlx::{FromRow, PgConnection, Row};

use super::{DeedStateSql, begin_repeatable_read, key32, opt_key32};
use crate::convert::{sql_i32, sql_i64};
use crate::model::{DeedRow, RowKind};

fn deed_from_pg(r: &sqlx::postgres::PgRow) -> Result<([u8; 32], DeedRow)> {
    let key: Vec<u8> = r.try_get("key")?;
    let sql = DeedStateSql::from_row(r)?;
    Ok((key32(&key, "deed key")?, DeedRow::try_from(sql)?))
}

pub async fn get_deed(conn: &mut PgConnection, key: &[u8; 32]) -> Result<Option<DeedRow>> {
    let row = sqlx::query("SELECT * FROM deeds WHERE key = $1").bind(key.as_slice()).fetch_optional(conn).await?;
    row.map(|r| deed_from_pg(&r).map(|(_, d)| d)).transpose()
}

const INSERT_DEED: &str =
    "INSERT INTO deeds (key, kind, name, owner_type, owner, claim, outpoint_txid, outpoint_index, value, accepted_daa)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";

pub async fn upsert_deed(conn: &mut PgConnection, key: &[u8; 32], row: &DeedRow) -> Result<()> {
    let sql = format!(
        "{INSERT_DEED} ON CONFLICT (key) DO UPDATE SET kind = $2, name = $3, owner_type = $4, owner = $5, claim = $6,
         outpoint_txid = $7, outpoint_index = $8, value = $9, accepted_daa = $10"
    );
    bind_deed(sql, key, row)?.execute(conn).await?;
    Ok(())
}

pub async fn delete_deed(conn: &mut PgConnection, key: &[u8; 32]) -> Result<()> {
    sqlx::query("DELETE FROM deeds WHERE key = $1").bind(key.as_slice()).execute(conn).await?;
    Ok(())
}

/// Repair races the pipeline, so every audit repair write requires the row to still match the
/// image the probe judged. `$1`..`$10` bind the key and the nine columns, so a statement's own
/// parameters start at `$11`. `IS NOT DISTINCT FROM`, because `=` never matches a NULL column.
const MATCHES_IMAGE: &str = "key = $1 AND kind = $2
       AND name IS NOT DISTINCT FROM $3 AND owner_type IS NOT DISTINCT FROM $4
       AND owner IS NOT DISTINCT FROM $5 AND claim IS NOT DISTINCT FROM $6
       AND outpoint_txid IS NOT DISTINCT FROM $7 AND outpoint_index IS NOT DISTINCT FROM $8
       AND value IS NOT DISTINCT FROM $9 AND accepted_daa IS NOT DISTINCT FROM $10";

fn bind_deed(sql: String, key: &[u8; 32], row: &DeedRow) -> Result<sqlx::query::Query<'static, sqlx::Postgres, PgArguments>> {
    let s = DeedStateSql::try_from(row)?;
    Ok(sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(key.to_vec())
        .bind(s.kind)
        .bind(s.name)
        .bind(s.owner_type)
        .bind(s.owner)
        .bind(s.claim)
        .bind(s.outpoint_txid)
        .bind(s.outpoint_index)
        .bind(s.value)
        .bind(s.accepted_daa))
}

pub async fn delete_deed_if_matches(conn: &mut PgConnection, key: &[u8; 32], expected: &DeedRow) -> Result<bool> {
    let sql = format!("DELETE FROM deeds WHERE {MATCHES_IMAGE}");
    Ok(bind_deed(sql, key, expected)?.execute(conn).await?.rows_affected() > 0)
}

pub async fn update_deed_outpoint_if_matches(
    conn: &mut PgConnection,
    key: &[u8; 32],
    expected: &DeedRow,
    txid: &[u8; 32],
    index: u32,
    value: u64,
    accepted_daa: u64,
) -> Result<bool> {
    let sql = format!(
        "UPDATE deeds SET outpoint_txid = $11, outpoint_index = $12, value = $13,
                          accepted_daa = CASE WHEN kind = 1 THEN $14 WHEN accepted_daa > $14 THEN NULL ELSE accepted_daa END
         WHERE {MATCHES_IMAGE}"
    );
    let res = bind_deed(sql, key, expected)?
        .bind(txid.to_vec())
        .bind(sql_i32(index)?)
        .bind(sql_i64(value)?)
        .bind(sql_i64(accepted_daa)?)
        .execute(conn)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Keeps only the name. A row that kept its claim derives as PENDING at a dead address and fails forever.
pub async fn demote_deed_if_matches(conn: &mut PgConnection, key: &[u8; 32], expected: &DeedRow) -> Result<bool> {
    let sql = format!(
        "UPDATE deeds SET kind = 2, owner_type = NULL, owner = NULL, claim = NULL,
                          outpoint_txid = NULL, outpoint_index = NULL, value = NULL,
                          accepted_daa = NULL
         WHERE {MATCHES_IMAGE}"
    );
    Ok(bind_deed(sql, key, expected)?.execute(conn).await?.rows_affected() > 0)
}

/// A row the pipeline wrote first wins, because it is always at least as fresh.
pub async fn insert_deed_if_absent(conn: &mut PgConnection, key: &[u8; 32], row: &DeedRow) -> Result<bool> {
    let sql = format!("{INSERT_DEED} ON CONFLICT (key) DO NOTHING");
    Ok(bind_deed(sql, key, row)?.execute(conn).await?.rows_affected() > 0)
}

pub async fn all_deeds(conn: &mut PgConnection) -> Result<Vec<([u8; 32], DeedRow)>> {
    let rows = sqlx::query("SELECT * FROM deeds ORDER BY key").fetch_all(conn).await?;
    rows.iter().map(deed_from_pg).collect()
}

#[derive(Debug)]
pub struct AdjacentRows {
    pub at: Option<DeedRow>,
    pub pred: Option<([u8; 32], DeedRow)>,
    pub succ: Option<([u8; 32], DeedRow)>,
}

/// Repeatable read, because a batch that commits between the reads yields a covering gap whose
/// bounds the batch spent, and the caller then builds against a spent address.
pub async fn adjacent_rows(conn: &mut PgConnection, key: &[u8; 32]) -> Result<AdjacentRows> {
    let mut tx = begin_repeatable_read(conn).await?;
    let at = sqlx::query("SELECT * FROM deeds WHERE key = $1").bind(key.as_slice()).fetch_optional(&mut *tx).await?;
    let succ =
        sqlx::query("SELECT * FROM deeds WHERE key > $1 ORDER BY key LIMIT 1").bind(key.as_slice()).fetch_optional(&mut *tx).await?;
    let pred = sqlx::query("SELECT * FROM deeds WHERE key < $1 ORDER BY key DESC LIMIT 1")
        .bind(key.as_slice())
        .fetch_optional(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(AdjacentRows {
        at: at.map(|r| deed_from_pg(&r).map(|(_, d)| d)).transpose()?,
        pred: pred.map(|r| deed_from_pg(&r)).transpose()?,
        succ: succ.map(|r| deed_from_pg(&r)).transpose()?,
    })
}

/// `deeds_name` is not unique, because a unique index turns chain-accepted corruption into a failed batch that stalls
/// the follow loop. The self-test repairs such rows instead.
pub async fn deed_by_name(conn: &mut PgConnection, name: &str) -> Result<Option<([u8; 32], DeedRow)>> {
    let row = sqlx::query("SELECT * FROM deeds WHERE kind = 0 AND name = $1").bind(name).fetch_optional(conn).await?;
    row.map(|r| deed_from_pg(&r)).transpose()
}

/// `cutoff` is `virtual_daa − t_evict`.
pub async fn ripe_pending(conn: &mut PgConnection, cutoff: u64) -> Result<Vec<[u8; 32]>> {
    let rows = sqlx::query_scalar::<_, Vec<u8>>("SELECT key FROM deeds WHERE kind = 1 AND accepted_daa <= $1 ORDER BY key")
        .bind(sql_i64(cutoff)?)
        .fetch_all(conn)
        .await?;
    rows.iter().map(|k| key32(k, "deed key")).collect()
}

/// ACTIVE `p2sh/v1` deeds, approved by mere co-presence of an input at the owner's script hash, with the keys that
/// bound their flanking gaps. One statement reads both, so the caller needs no transaction per candidate.
pub async fn script_hash_owned_neighborhoods(conn: &mut PgConnection) -> Result<Vec<ScriptOwnedCandidate>> {
    // The flank lookups must not filter on `kind`, because every row bounds a gap.
    let rows = sqlx::query(
        "SELECT c.key, c.owner, p.key AS pred, s.key AS succ FROM deeds c \
         LEFT JOIN LATERAL (SELECT key FROM deeds WHERE key < c.key ORDER BY key DESC LIMIT 1) p ON true \
         LEFT JOIN LATERAL (SELECT key FROM deeds WHERE key > c.key ORDER BY key ASC LIMIT 1) s ON true \
         WHERE c.kind = 0 AND c.owner_type = 3 ORDER BY c.key",
    )
    .fetch_all(conn)
    .await?;
    // Skips a row that does not decode, because a malformed candidate must never block the eviction of a ripe squat.
    Ok(rows.iter().filter_map(|r| candidate(r).ok().flatten()).collect())
}

fn candidate(r: &sqlx::postgres::PgRow) -> Result<Option<ScriptOwnedCandidate>> {
    let Some(owner) = opt_key32(r.try_get::<Option<Vec<u8>>, _>("owner")?.as_deref(), "owner")? else { return Ok(None) };
    Ok(Some(ScriptOwnedCandidate {
        key: key32(r.try_get::<Vec<u8>, _>("key")?.as_slice(), "key")?,
        owner,
        below: opt_key32(r.try_get::<Option<Vec<u8>>, _>("pred")?.as_deref(), "pred")?,
        above: opt_key32(r.try_get::<Option<Vec<u8>>, _>("succ")?.as_deref(), "succ")?,
    }))
}

/// A missing `below` or `above` stands for the keyspace bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScriptOwnedCandidate {
    pub key: [u8; 32],
    pub owner: [u8; 32],
    pub below: Option<[u8; 32]>,
    pub above: Option<[u8; 32]>,
}

pub async fn deeds_by_owner(conn: &mut PgConnection, owner_type: u8, owner: &[u8; 32]) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar::<_, String>("SELECT name FROM deeds WHERE kind = 0 AND owner_type = $1 AND owner = $2 ORDER BY name")
        .bind(i16::from(owner_type))
        .bind(owner.as_slice())
        .fetch_all(conn)
        .await?)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RegistryCounts {
    pub active: i64,
    pub pending: i64,
    pub owner_unknown: i64,
    /// `max(history.id)`, the change marker, because a transfer moves none of the counts. Undo
    /// deletes rows, so it is not monotonic, and a client only asks whether it changed.
    pub history_seq: Option<i64>,
}

/// Repeatable read, so the tuple is a state that existed and matches a `/snapshot`, or a client that watches
/// `historySeq` never settles. One grouped scan, because `deeds` has no index on `kind`.
pub async fn counts(conn: &mut PgConnection) -> Result<RegistryCounts> {
    let mut tx = begin_repeatable_read(conn).await?;
    let by_kind: Vec<(i16, i64)> = sqlx::query_as("SELECT kind, count(*) FROM deeds GROUP BY kind").fetch_all(&mut *tx).await?;
    let history_seq: Option<i64> = sqlx::query_scalar("SELECT max(id) FROM history").fetch_one(&mut *tx).await?;
    tx.commit().await?;
    let of = |kind: RowKind| by_kind.iter().find(|(k, _)| *k == kind as i16).map_or(0, |(_, n)| *n);
    Ok(RegistryCounts {
        active: of(RowKind::Active),
        pending: of(RowKind::Pending),
        owner_unknown: of(RowKind::OwnerUnknown),
        history_seq,
    })
}
