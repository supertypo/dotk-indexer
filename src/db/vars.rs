use anyhow::Result;
use sqlx::PgConnection;

pub const VAR_VCP_CHECKPOINT: &str = "vcp_checkpoint";
pub const VAR_HISTORY_EPOCH: &str = "history_epoch";

pub async fn get_var(conn: &mut PgConnection, key: &str) -> Result<Option<String>> {
    Ok(sqlx::query_scalar::<_, String>("SELECT value FROM vars WHERE key = $1").bind(key).fetch_optional(conn).await?)
}

pub async fn has_checkpoint(conn: &mut PgConnection) -> Result<bool> {
    Ok(get_var(conn, VAR_VCP_CHECKPOINT).await?.is_some())
}

pub async fn get_var_for_update(conn: &mut PgConnection, key: &str) -> Result<Option<String>> {
    Ok(sqlx::query_scalar::<_, String>("SELECT value FROM vars WHERE key = $1 FOR UPDATE").bind(key).fetch_optional(conn).await?)
}

pub async fn set_var(conn: &mut PgConnection, key: &str, value: &str) -> Result<()> {
    sqlx::query("INSERT INTO vars (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2")
        .bind(key)
        .bind(value)
        .execute(conn)
        .await?;
    Ok(())
}

/// Writes a new random epoch. The indexer calls it at every start. On a restored database, the
/// sequence reissues the `history` ids after the backup, so only a new epoch keeps a follower from
/// skipping them.
pub async fn rotate_history_epoch(conn: &mut PgConnection) -> Result<()> {
    sqlx::query(
        "INSERT INTO vars (key, value) VALUES ($1, md5(random()::text || clock_timestamp()::text)) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
    )
    .bind(VAR_HISTORY_EPOCH)
    .execute(conn)
    .await?;
    Ok(())
}
