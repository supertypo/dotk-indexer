use crate::common::*;
use dotk_core::watch::GenesisFile;
use dotk_indexer::db;
use dotk_indexer::identity;

fn other_genesis() -> GenesisFile {
    GenesisFile { registry_covenant_id: "dd".repeat(32), ..genesis_file() }
}

fn ident(g: &GenesisFile) -> identity::Identity {
    identity::of(g, &g.watch_templates().expect("watch templates")).expect("identity")
}

/// A checkpoint makes the database count as processed.
async fn mark_processed(pool: &sqlx::PgPool) {
    let mut conn = pool.acquire().await.unwrap();
    db::set_var(&mut conn, db::VAR_VCP_CHECKPOINT, &"ab".repeat(32)).await.unwrap();
}

#[tokio::test]
async fn a_foreign_genesis_over_a_populated_db_is_refused() {
    let pool = fresh_pool("identity_foreign").await;
    db::migrate(&pool).await.unwrap();
    let ours = ident(&genesis_file());
    identity::write(&pool, &ours).await.unwrap();
    mark_processed(&pool).await;

    identity::check(&pool, &ident(&other_genesis())).await.expect_err("a different deployment must not resume this database");

    identity::check(&pool, &ours).await.unwrap();
}

/// `params.devfund_spk` builds the pipeline's transaction filter, and no template hash covers it.
#[test]
fn a_manifest_whose_devfund_contradicts_its_bytecode_is_rejected() {
    let mut g = genesis_file();
    g.params.devfund_spk = format!("20{}ac", "22".repeat(32));
    let watch = g.watch_templates().expect("the bytecode still matches its declared hashes, which is the point");
    let err = identity::of(&g, &watch).expect_err("params and bytecode disagree about the devfund").to_string();
    assert!(err.contains("devfund_spk"), "{err}");
}

#[tokio::test]
async fn only_a_database_with_nothing_committed_is_bootstrapped_again() {
    let pool = fresh_pool("never_synced").await;
    assert!(db::never_synced(&pool).await.unwrap());
    db::migrate(&pool).await.unwrap();
    identity::write(&pool, &ident(&genesis_file())).await.unwrap();
    assert!(db::never_synced(&pool).await.unwrap());

    mark_processed(&pool).await;
    assert!(!db::never_synced(&pool).await.unwrap());
    sqlx::query("DELETE FROM vars WHERE key = $1").bind(db::VAR_VCP_CHECKPOINT).execute(&pool).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();
    db::append_event(&mut conn, &event([0x11; 32], 0, 1, dotk_core::key_of("alice"), None)).await.unwrap();
    assert!(!db::never_synced(&pool).await.unwrap(), "a journal entry is committed work");
}

/// A database with a missing identity field is refused, never adopted.
#[tokio::test]
async fn a_missing_identity_field_is_refused() {
    let pool = fresh_pool("identity_missing").await;
    db::migrate(&pool).await.unwrap();
    let ours = ident(&genesis_file());
    identity::write(&pool, &ours).await.unwrap();

    let mut conn = pool.acquire().await.unwrap();
    sqlx::query("DELETE FROM vars WHERE key = $1").bind("deed_template_hash").execute(&mut *conn).await.unwrap();
    mark_processed(&pool).await;
    identity::check(&pool, &ours).await.expect_err("a field this indexer always writes is missing");
}

/// `--initialize-db` drops everything, so a leftover type or table must not block the migration
/// that follows.
#[tokio::test]
async fn a_wiped_schema_migrates_again() {
    let pool = fresh_pool("schema_wipe").await;
    assert!(!db::schema_present(&pool).await.unwrap());
    db::migrate(&pool).await.unwrap();
    identity::write(&pool, &ident(&genesis_file())).await.unwrap();
    mark_processed(&pool).await;
    assert!(db::schema_present(&pool).await.unwrap());

    db::drop_schema(&pool).await.unwrap();
    assert!(!db::schema_present(&pool).await.unwrap());
    assert!(db::never_synced(&pool).await.unwrap());
    db::migrate(&pool).await.unwrap();
    assert!(db::schema_present(&pool).await.unwrap());
    assert!(db::never_synced(&pool).await.unwrap(), "the wipe took the checkpoint with it");
}
