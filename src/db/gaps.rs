use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::PgArguments;
use sqlx::{FromRow, PgConnection, Row};

use super::{GapRowSql, begin_repeatable_read, gap_image_from_sql, set_local_statement_timeout};
use crate::convert::{fit, sql_i32};
use crate::model::{GapRow, HistoryOp, RowKind};

fn gap_from_pg(r: &sqlx::postgres::PgRow) -> Result<([u8; 32], GapRow)> {
    let (lo, row) = gap_image_from_sql(GapRowSql::from_row(r)?)?;
    Ok((lo, row.context("gap hi is null")?))
}

/// `$1`..`$4` bind `lo` and the row's three columns.
fn bind_gap(sql: &'static str, lo: &[u8; 32], row: &GapRow) -> Result<sqlx::query::Query<'static, sqlx::Postgres, PgArguments>> {
    Ok(sqlx::query(sql)
        .bind(lo.to_vec())
        .bind(row.hi.to_vec())
        .bind(row.outpoint_txid.map(|v| v.to_vec()))
        .bind(row.outpoint_index.map(sql_i32).transpose()?))
}

pub async fn get_gap(conn: &mut PgConnection, lo: &[u8; 32]) -> Result<Option<GapRow>> {
    let row = sqlx::query("SELECT * FROM gaps WHERE lo = $1").bind(lo.as_slice()).fetch_optional(conn).await?;
    row.map(|r| gap_from_pg(&r).map(|(_, g)| g)).transpose()
}

pub async fn upsert_gap(conn: &mut PgConnection, lo: &[u8; 32], row: &GapRow) -> Result<()> {
    bind_gap(
        "INSERT INTO gaps (lo, hi, outpoint_txid, outpoint_index) VALUES ($1, $2, $3, $4)
         ON CONFLICT (lo) DO UPDATE SET hi = $2, outpoint_txid = $3, outpoint_index = $4",
        lo,
        row,
    )?
    .execute(conn)
    .await?;
    Ok(())
}

pub async fn delete_gap(conn: &mut PgConnection, lo: &[u8; 32]) -> Result<()> {
    sqlx::query("DELETE FROM gaps WHERE lo = $1").bind(lo.as_slice()).execute(conn).await?;
    Ok(())
}

pub async fn all_gaps(conn: &mut PgConnection) -> Result<Vec<([u8; 32], GapRow)>> {
    let rows = sqlx::query("SELECT * FROM gaps ORDER BY lo").fetch_all(conn).await?;
    rows.iter().map(gap_from_pg).collect()
}

pub async fn upsert_gap_if_matches(conn: &mut PgConnection, lo: &[u8; 32], expected: Option<&GapRow>, row: &GapRow) -> Result<bool> {
    let res = match expected {
        None => bind_gap(
            "INSERT INTO gaps (lo, hi, outpoint_txid, outpoint_index) VALUES ($1, $2, $3, $4) ON CONFLICT (lo) DO NOTHING",
            lo,
            row,
        )?,
        Some(e) => bind_gap(
            "UPDATE gaps SET hi = $2, outpoint_txid = $3, outpoint_index = $4
             WHERE lo = $1 AND hi = $5 AND outpoint_txid IS NOT DISTINCT FROM $6
               AND outpoint_index IS NOT DISTINCT FROM $7",
            lo,
            row,
        )?
        .bind(e.hi.to_vec())
        .bind(e.outpoint_txid.map(|v| v.to_vec()))
        .bind(e.outpoint_index.map(sql_i32).transpose()?),
    };
    Ok(res.execute(conn).await?.rows_affected() > 0)
}

pub async fn delete_gap_if_matches(conn: &mut PgConnection, lo: &[u8; 32], expected: &GapRow) -> Result<bool> {
    let res = bind_gap(
        "DELETE FROM gaps WHERE lo = $1 AND hi = $2 AND outpoint_txid IS NOT DISTINCT FROM $3
           AND outpoint_index IS NOT DISTINCT FROM $4",
        lo,
        expected,
    )?
    .execute(conn)
    .await?;
    Ok(res.rows_affected() > 0)
}

const KEY_PREFIX_BUCKETS: usize = 256;

/// Widths are multiples of the average gap, so the shape compares across registry sizes.
const GAP_WIDTH_BUCKETS: usize = 32;
pub const GAP_WIDTH_BUCKETS_PER_AVERAGE: i64 = 4;

#[derive(Debug)]
pub struct KeyspaceSummary {
    /// `[bucket][kind]`, with the kind indexed by [`RowKind`] as `i16`.
    pub keys_by_prefix: Vec<[i64; 3]>,
    /// Observed, not asserted, so a client can check that it equals deeds + 1.
    pub gaps: i64,
    pub gaps_by_width: Vec<i64>,
    /// Kept out of `gaps_by_width`, so a client that sums the array never misplaces an open-ended entry.
    pub gaps_wider: i64,
    /// `(days since the epoch, count)`, oldest first, only days that have one. Counted from
    /// `register` history entries, so a name released or evicted since still counts on its day.
    pub registrations_by_day: Vec<(i64, i64)>,
}

const DAY_MS: i64 = 86_400_000;

/// A key's leading 7 bytes as a `bigint`, because Postgres cannot subtract `bytea` and eight bytes
/// overflow a signed `bigint`.
const KEY_PREFIX_BIGINT: &str = "
    get_byte({k},0)::bigint * 281474976710656 + get_byte({k},1)::bigint * 1099511627776
  + get_byte({k},2)::bigint * 4294967296      + get_byte({k},3)::bigint * 16777216
  + get_byte({k},4)::bigint * 65536           + get_byte({k},5)::bigint * 256
  + get_byte({k},6)::bigint";

const KEYSPACE_UNITS: i64 = 1 << 56;

/// Repeatable read, because the width buckets scale by the gap count read one statement earlier. `statement_timeout`
/// replaces the session's for this transaction, and zero disables it.
pub async fn keyspace_summary(conn: &mut PgConnection, statement_timeout: Duration) -> Result<KeyspaceSummary> {
    let mut tx = begin_repeatable_read(conn).await?;
    set_local_statement_timeout(&mut tx, statement_timeout).await?;

    let mut keys_by_prefix = vec![[0i64; 3]; KEY_PREFIX_BUCKETS];
    let rows =
        sqlx::query("SELECT get_byte(key, 0) AS bucket, kind, count(*) AS n FROM deeds GROUP BY 1, 2").fetch_all(&mut *tx).await?;
    for r in &rows {
        let bucket: i32 = r.try_get("bucket")?;
        let kind: i16 = r.try_get("kind")?;
        let n: i64 = r.try_get("n")?;
        let slot = RowKind::try_from(kind).map_err(|kind| anyhow::anyhow!("unknown row kind {kind}"))? as usize;
        keys_by_prefix[fit::<_, usize>(bucket)?][slot] = n;
    }

    let gaps: i64 = sqlx::query_scalar("SELECT count(*) FROM gaps").fetch_one(&mut *tx).await?;
    let (gaps_by_width, gaps_wider) = gap_widths(&mut tx, gaps).await?;

    let registrations_by_day = sqlx::query_as::<_, (i64, i64)>(REGISTRATIONS_BY_DAY_SQL).bind(DAY_MS).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(KeyspaceSummary { keys_by_prefix, gaps, gaps_by_width, gaps_wider, registrations_by_day })
}

/// `op = 0` is a literal, not a bind, because a generic prepared plan cannot match the partial
/// index `history_register` against a parameter.
const REGISTRATIONS_BY_DAY_SQL: &str = "SELECT block_time / $1 AS day, count(*) FROM history WHERE op = 0 GROUP BY 1 ORDER BY 1";
const _: () = assert!(HistoryOp::Register as i16 == 0, "the literal above is the register op");

/// `width_bucket` answers n+1 above the range, which counts as `gaps_wider`.
async fn gap_widths(tx: &mut PgConnection, gaps: i64) -> Result<(Vec<i64>, i64)> {
    let mut counts = vec![0i64; GAP_WIDTH_BUCKETS];
    if gaps == 0 {
        return Ok((counts, 0));
    }
    let span = fit::<_, i64>(GAP_WIDTH_BUCKETS)? / GAP_WIDTH_BUCKETS_PER_AVERAGE * (KEYSPACE_UNITS / gaps);
    let sql = format!(
        "SELECT width_bucket(({hi}) - ({lo}), 0, $1, $2) AS b, count(*) AS n FROM gaps GROUP BY 1",
        hi = KEY_PREFIX_BIGINT.replace("{k}", "hi"),
        lo = KEY_PREFIX_BIGINT.replace("{k}", "lo"),
    );
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(span).bind(fit::<_, i32>(GAP_WIDTH_BUCKETS)?).fetch_all(&mut *tx).await?;
    let mut wider = 0i64;
    for r in &rows {
        let b: i32 = r.try_get("b")?;
        let n: i64 = r.try_get("n")?;
        match usize::try_from(b) {
            // `width_bucket` is 1-based, and `lo < hi` rules out bucket 0.
            Ok(i) if (1..=GAP_WIDTH_BUCKETS).contains(&i) => counts[i - 1] = n,
            _ => wider += n,
        }
    }
    Ok((counts, wider))
}
