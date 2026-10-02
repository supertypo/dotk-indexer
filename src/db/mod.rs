//! The Postgres schema, pools and queries.

mod cards;
mod deeds;
mod gaps;
mod history;
mod journal;
mod vars;

pub use cards::{
    CardHit, all_cards, cards_by_spender, delete_card, get_card, insert_card_if_absent, live_card_by_key, live_cards_by_owner,
    mark_card_if_matches, purge_swept_cards_below, set_card_swept, unswept_cards, update_card_value_if_matches,
};
pub use deeds::{
    RegistryCounts, adjacent_rows, all_deeds, counts, deed_by_name, deeds_by_owner, delete_deed, delete_deed_if_matches,
    demote_deed_if_matches, get_deed, insert_deed_if_absent, ripe_pending, script_hash_owned_neighborhoods,
    update_deed_outpoint_if_matches, upsert_deed,
};
pub use gaps::{
    GAP_WIDTH_BUCKETS_PER_AVERAGE, all_gaps, delete_gap, delete_gap_if_matches, get_gap, keyspace_summary, upsert_gap,
    upsert_gap_if_matches,
};
pub use history::{append_history, delete_history_for_block, history_page, history_page_with_extent};
pub use journal::{
    EventRow, append_event, delete_events_for_block, delete_imported_events_for_block, discovered_inside_since, event_exists,
    events_for_block_desc, events_for_resume, gaps_touched_since, journal_coverage, keys_touched_since, mine_candidates,
    prune_events_below,
};
pub use vars::{VAR_VCP_CHECKPOINT, get_var, get_var_for_update, has_checkpoint, set_var};

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Connection, PgConnection, Postgres, Transaction};

use crate::convert::{sql_i32, sql_i64, sql_u8, sql_u32, sql_u64};
use crate::model::{CardMark, DeedRow, GapRow, RowKind};

/// Field order must match the `deed_state` type, which is not the row type of `deeds`. A migration that adds a column
/// to `deeds` must also run `ALTER TYPE deed_state ADD ATTRIBUTE`, or undo loses the column. `gap_row` and `gaps` pair
/// the same way.
#[derive(Debug, Clone, sqlx::Type, sqlx::FromRow)]
#[sqlx(type_name = "deed_state")]
pub struct DeedStateSql {
    pub kind: i16,
    pub name: Option<String>,
    pub owner_type: Option<i16>,
    pub owner: Option<Vec<u8>>,
    pub claim: Option<Vec<u8>>,
    pub outpoint_txid: Option<Vec<u8>>,
    pub outpoint_index: Option<i32>,
    pub value: Option<i64>,
    pub accepted_daa: Option<i64>,
}

/// Field order must match `gap_row`. `hi: None` means no row existed before the event, so undo deletes it.
#[derive(Debug, Clone, sqlx::Type, sqlx::FromRow)]
#[sqlx(type_name = "gap_row")]
pub struct GapRowSql {
    pub lo: Vec<u8>,
    pub hi: Option<Vec<u8>>,
    pub outpoint_txid: Option<Vec<u8>>,
    pub outpoint_index: Option<i32>,
}

pub type GapImage = ([u8; 32], Option<GapRow>);

/// Field order must match `card_mark`.
#[derive(Debug, Clone, sqlx::Type)]
#[sqlx(type_name = "card_mark")]
pub struct CardMarkSql {
    pub txid: Vec<u8>,
    pub idx: i32,
    pub existed: bool,
    pub swept_at: Option<i64>,
}

impl TryFrom<&CardMark> for CardMarkSql {
    type Error = anyhow::Error;
    fn try_from(m: &CardMark) -> Result<Self> {
        Ok(Self { txid: m.txid.to_vec(), idx: sql_i32(m.idx)?, existed: m.existed, swept_at: m.swept_at.map(sql_i64).transpose()? })
    }
}

impl TryFrom<CardMarkSql> for CardMark {
    type Error = anyhow::Error;
    fn try_from(s: CardMarkSql) -> Result<Self> {
        Ok(Self {
            txid: key32(&s.txid, "card txid")?,
            idx: sql_u32(s.idx)?,
            existed: s.existed,
            swept_at: s.swept_at.map(sql_u64).transpose()?,
        })
    }
}

fn opt_key32(v: Option<&[u8]>, what: &str) -> Result<Option<[u8; 32]>> {
    v.map(|b| key32(b, what)).transpose()
}

fn key32(v: &[u8], what: &str) -> Result<[u8; 32]> {
    <[u8; 32]>::try_from(v).map_err(|_| anyhow::anyhow!("{what} is not 32 bytes"))
}

impl TryFrom<&DeedRow> for DeedStateSql {
    type Error = anyhow::Error;
    fn try_from(r: &DeedRow) -> Result<Self> {
        Ok(Self {
            kind: r.kind as i16,
            name: r.name.clone(),
            owner_type: r.owner_type.map(i16::from),
            owner: r.owner.map(|v| v.to_vec()),
            claim: r.claim.map(|v| v.to_vec()),
            outpoint_txid: r.outpoint_txid.map(|v| v.to_vec()),
            outpoint_index: r.outpoint_index.map(sql_i32).transpose()?,
            value: r.value.map(sql_i64).transpose()?,
            accepted_daa: r.accepted_daa.map(sql_i64).transpose()?,
        })
    }
}

impl TryFrom<DeedStateSql> for DeedRow {
    type Error = anyhow::Error;
    fn try_from(s: DeedStateSql) -> Result<Self> {
        Ok(Self {
            kind: RowKind::try_from(s.kind).map_err(|kind| anyhow::anyhow!("unknown row kind {kind}"))?,
            name: s.name,
            owner_type: s.owner_type.map(sql_u8).transpose()?,
            owner: opt_key32(s.owner.as_deref(), "owner")?,
            claim: opt_key32(s.claim.as_deref(), "claim")?,
            outpoint_txid: opt_key32(s.outpoint_txid.as_deref(), "outpoint_txid")?,
            outpoint_index: s.outpoint_index.map(sql_u32).transpose()?,
            value: s.value.map(sql_u64).transpose()?,
            accepted_daa: s.accepted_daa.map(sql_u64).transpose()?,
        })
    }
}

fn gap_image_sql(lo: &[u8; 32], row: Option<&GapRow>) -> Result<GapRowSql> {
    Ok(GapRowSql {
        lo: lo.to_vec(),
        hi: row.map(|g| g.hi.to_vec()),
        outpoint_txid: row.and_then(|g| g.outpoint_txid).map(|v| v.to_vec()),
        outpoint_index: row.and_then(|g| g.outpoint_index).map(sql_i32).transpose()?,
    })
}

fn gap_image_from_sql(s: GapRowSql) -> Result<GapImage> {
    let lo = key32(&s.lo, "gap lo")?;
    let row =
        s.hi.map(|hi| -> Result<GapRow> {
            Ok(GapRow {
                hi: key32(&hi, "gap hi")?,
                outpoint_txid: opt_key32(s.outpoint_txid.as_deref(), "outpoint_txid")?,
                outpoint_index: s.outpoint_index.map(sql_u32).transpose()?,
            })
        })
        .transpose()?;
    Ok((lo, row))
}

/// The pipeline's pool. Requests use a [`sibling`], so a request flood never holds a batch back from committing.
pub async fn connect(url: &str) -> Result<PgPool> {
    Ok(pool_options(PIPELINE_CONNECTIONS).connect(url).await?)
}

/// One snapshot for a multi-statement read.
pub async fn begin_repeatable_read(conn: &mut PgConnection) -> Result<Transaction<'_, Postgres>> {
    Ok(conn.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY").await?)
}

pub async fn set_local_statement_timeout(conn: &mut PgConnection, statement_timeout: Duration) -> Result<()> {
    sqlx::query("SELECT set_config('statement_timeout', $1, true)")
        .bind(statement_timeout.as_millis().to_string())
        .execute(conn)
        .await?;
    Ok(())
}

/// Reuses the options of a connected pool, so a wrong URL fails once, up front. `statement_timeout` is set per
/// session, because a connection pooler can refuse it as a startup parameter.
pub fn sibling(pool: &PgPool, max_connections: u32, statement_timeout: Duration) -> PgPool {
    let ms = statement_timeout.as_millis().to_string();
    pool_options(max_connections)
        .after_connect(move |conn, _| {
            let ms = ms.clone();
            Box::pin(async move {
                sqlx::query("SELECT set_config('statement_timeout', $1, false)").bind(ms).execute(conn).await.map(|_| ())
            })
        })
        .connect_lazy_with((*pool.connect_options()).clone())
}

const PIPELINE_CONNECTIONS: u32 = 10;

fn pool_options(max_connections: u32) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(max_connections)
        // The sqlx default of 30 s stalls every call on an unreachable database.
        .acquire_timeout(Duration::from_secs(5))
}

/// Counts the sqlx ledger too, so `--initialize-db` drops a database that has applied migrations but no `deeds`.
pub async fn schema_present(pool: &PgPool) -> Result<bool> {
    let row = sqlx::query_scalar::<_, bool>("SELECT to_regclass('_sqlx_migrations') IS NOT NULL OR to_regclass('deeds') IS NOT NULL")
        .fetch_one(pool)
        .await?;
    Ok(row)
}

/// Whether the migration ledger is this indexer's, so a bootstrap drops only tables it created.
/// An empty ledger with no `deeds` is a first migration that died before its one transaction.
pub async fn ledger_is_ours(pool: &PgPool) -> Result<bool> {
    let ledger: bool = sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL").fetch_one(pool).await?;
    if !ledger {
        return Ok(false);
    }
    let sums: Vec<Vec<u8>> = sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations").fetch_all(pool).await?;
    if sums.is_empty() {
        return Ok(sqlx::query_scalar::<_, bool>("SELECT to_regclass('deeds') IS NULL").fetch_one(pool).await?);
    }
    let init = migrator().iter().find(|m| m.version == 0).context("the init migration is missing")?.checksum.to_vec();
    Ok(sums.contains(&init))
}

pub async fn never_synced(pool: &PgPool) -> Result<bool> {
    let tables: bool = sqlx::query_scalar(
        "SELECT to_regclass('vars') IS NOT NULL AND to_regclass('deeds') IS NOT NULL AND to_regclass('events') IS NOT NULL",
    )
    .fetch_one(pool)
    .await?;
    if !tables {
        return Ok(true);
    }
    let synced: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM vars WHERE key = $1) OR EXISTS (SELECT 1 FROM deeds) OR EXISTS (SELECT 1 FROM events)",
    )
    .bind(VAR_VCP_CHECKPOINT)
    .fetch_one(pool)
    .await?;
    Ok(!synced)
}

pub async fn drop_schema(pool: &PgPool) -> Result<()> {
    // Tables go first, because Postgres refuses to drop a type that a column uses.
    sqlx::raw_sql(
        "DROP TABLE IF EXISTS history; DROP TABLE IF EXISTS events; \
         DROP TYPE IF EXISTS deed_state; DROP TYPE IF EXISTS gap_row; DROP TYPE IF EXISTS card_mark; \
         DROP TABLE IF EXISTS deeds; DROP TABLE IF EXISTS gaps; DROP TABLE IF EXISTS cards; DROP TABLE IF EXISTS vars; \
         DROP TABLE IF EXISTS _sqlx_migrations",
    )
    .execute(pool)
    .await?;
    Ok(())
}

fn migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("./migrations")
}

pub async fn migrate(pool: &PgPool) -> Result<()> {
    migrator().run(pool).await?;
    Ok(())
}
