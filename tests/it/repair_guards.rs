use crate::common::*;
use dotk_indexer::model::{DeedRow, GapRow, RowKind};

/// A PENDING row's `accepted_daa` is the evictor's clock, and the UTXO at the deed address carries it.
/// An ACTIVE row's age is a claim that the chain bounds only from above.
#[tokio::test]
async fn a_self_test_rewrites_a_refuted_pending_clock_and_leaves_an_active_age() {
    let (h, mut w) = standard_boot("proven_clock", fast_args()).await;
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a, w.split("squatter", &OWNER_B)]);
    let squat = dotk_core::key_of("squatter");
    let alice = dotk_core::key_of("alice");
    h.wait_until("both rows", || async { h.deed_row(&squat).await.is_some() && h.deed_row(&alice).await.is_some() }).await;
    let pending = h.deed_row(&squat).await.unwrap();
    let active = h.deed_row(&alice).await.unwrap();
    assert!(h.selftest_now().await.proven);

    {
        let mut conn = h.app.backends.db.acquire().await.unwrap();
        dotk_indexer::db::upsert_deed(&mut conn, &squat, &DeedRow { accepted_daa: Some(0), ..pending.clone() }).await.unwrap();
        dotk_indexer::db::upsert_deed(&mut conn, &alice, &DeedRow { accepted_daa: Some(1), ..active.clone() }).await.unwrap();
    }
    let report = h.selftest_now().await;
    assert!(report.proven && report.published, "{report:?}");
    assert_eq!((report.repaired.rewritten, report.repaired.dropped), (1, 0), "{report:?}");
    assert_eq!(h.deed_row(&squat).await.unwrap(), pending, "the clock is restored from the chain");
    assert_eq!(h.deed_row(&alice).await.unwrap().accepted_daa, Some(1), "an ACTIVE age is a claim, not repaired");
    h.stop().await;
}

async fn guarded_conn(name: &str) -> sqlx::pool::PoolConnection<sqlx::Postgres> {
    let pool = fresh_pool(name).await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    pool.acquire().await.unwrap()
}

// Repair races the stream, so every repair write is conditional on the probed image.
#[tokio::test]
async fn a_repair_delete_or_adoption_yields_to_the_pipeline() {
    let mut conn = guarded_conn("guarded_deletes").await;
    let key = [7u8; 32];

    let probed = DeedRow::active("alice".into(), 0, OWNER_A, ([0xaa; 32], 0), 20_000_000);
    dotk_indexer::db::upsert_deed(&mut conn, &key, &probed).await.unwrap();
    let mutated = DeedRow::active("alice".into(), 0, OWNER_B, ([0xbb; 32], 0), 20_000_000);
    dotk_indexer::db::upsert_deed(&mut conn, &key, &mutated).await.unwrap();
    assert!(
        !dotk_indexer::db::delete_deed_if_matches(&mut conn, &key, &probed).await.unwrap(),
        "a row mutated since the probe must be spared"
    );
    assert!(dotk_indexer::db::get_deed(&mut conn, &key).await.unwrap().is_some());
    assert!(
        dotk_indexer::db::delete_deed_if_matches(&mut conn, &key, &mutated).await.unwrap(),
        "a row still matching its probed image is dropped"
    );
    assert!(dotk_indexer::db::get_deed(&mut conn, &key).await.unwrap().is_none());

    let adopted = DeedRow::pending([9u8; 32], ([1u8; 32], 2), 120_000_000, 5);
    assert!(dotk_indexer::db::insert_deed_if_absent(&mut conn, &key, &adopted).await.unwrap());
    assert!(
        !dotk_indexer::db::insert_deed_if_absent(&mut conn, &key, &probed).await.unwrap(),
        "the stream's row must win over a late adoption"
    );
    let row = dotk_indexer::db::get_deed(&mut conn, &key).await.unwrap().unwrap();
    assert_eq!(row, adopted);
}

#[tokio::test]
async fn a_repair_rewrite_or_demotion_yields_to_the_pipeline() {
    let mut conn = guarded_conn("guarded_rewrites").await;
    let key = [7u8; 32];
    let probed = DeedRow::active("alice".into(), 0, OWNER_A, ([0xaa; 32], 0), 20_000_000);
    let adopted = DeedRow::pending([9u8; 32], ([1u8; 32], 2), 120_000_000, 5);
    dotk_indexer::db::upsert_deed(&mut conn, &key, &adopted).await.unwrap();

    assert!(
        !dotk_indexer::db::update_deed_outpoint_if_matches(&mut conn, &key, &probed, &[2u8; 32], 1, 7, 9).await.unwrap(),
        "a row mutated since the probe must keep its provenance"
    );
    assert_eq!(dotk_indexer::db::get_deed(&mut conn, &key).await.unwrap().unwrap(), adopted);
    assert!(dotk_indexer::db::update_deed_outpoint_if_matches(&mut conn, &key, &adopted, &[2u8; 32], 1, 7, 9).await.unwrap());
    let row = dotk_indexer::db::get_deed(&mut conn, &key).await.unwrap().unwrap();
    assert_eq!(
        (row.outpoint_txid, row.outpoint_index, row.value, row.accepted_daa),
        (Some([2u8; 32]), Some(1), Some(7), Some(9)),
        "the observed chain evidence replaces every provenance field"
    );

    let active = DeedRow::active("alice".into(), 0, OWNER_A, ([0xaa; 32], 0), 20_000_000);
    dotk_indexer::db::upsert_deed(&mut conn, &key, &active).await.unwrap();
    assert!(!dotk_indexer::db::demote_deed_if_matches(&mut conn, &key, &adopted).await.unwrap(), "a stale image demotes nothing");
    assert!(dotk_indexer::db::demote_deed_if_matches(&mut conn, &key, &active).await.unwrap());
    let row = dotk_indexer::db::get_deed(&mut conn, &key).await.unwrap().unwrap();
    assert_eq!(row, DeedRow::owner_unknown(Some("alice".into())), "the key and its name survive, the ownership does not");
}

#[tokio::test]
async fn a_repair_gap_write_yields_to_the_pipeline() {
    let mut conn = guarded_conn("guarded_gaps").await;
    // A repair that overwrites a gap the stream just republished puts back a stale outpoint.
    let lo = [3u8; 32];
    let observed = GapRow { hi: [4u8; 32], outpoint_txid: Some([0xcc; 32]), outpoint_index: Some(0) };
    assert!(dotk_indexer::db::upsert_gap_if_matches(&mut conn, &lo, None, &observed).await.unwrap(), "an absent gap is adopted");
    let stream_wrote = GapRow { hi: [4u8; 32], outpoint_txid: Some([0xdd; 32]), outpoint_index: Some(1) };
    assert!(
        !dotk_indexer::db::upsert_gap_if_matches(&mut conn, &lo, None, &stream_wrote).await.unwrap(),
        "the stream's gap row must win over a late adoption"
    );
    let stale = GapRow { hi: [4u8; 32], outpoint_txid: Some([0x11; 32]), outpoint_index: Some(9) };
    assert!(
        !dotk_indexer::db::upsert_gap_if_matches(&mut conn, &lo, Some(&stale), &stream_wrote).await.unwrap(),
        "a gap mutated since the probe must be spared"
    );
    assert!(dotk_indexer::db::upsert_gap_if_matches(&mut conn, &lo, Some(&observed), &stream_wrote).await.unwrap());
    assert_eq!(dotk_indexer::db::get_gap(&mut conn, &lo).await.unwrap(), Some(stream_wrote));

    assert!(!dotk_indexer::db::delete_gap_if_matches(&mut conn, &lo, &observed).await.unwrap(), "a stale image deletes nothing");
    assert!(dotk_indexer::db::get_gap(&mut conn, &lo).await.unwrap().is_some());
    assert!(dotk_indexer::db::delete_gap_if_matches(&mut conn, &lo, &stream_wrote).await.unwrap());
    assert!(dotk_indexer::db::get_gap(&mut conn, &lo).await.unwrap().is_none());
}

/// A repair corrects the indexer's own reading. Nothing happened to the name, so history stays as it was.
#[tokio::test]
async fn a_repair_never_writes_history() {
    let (h, mut w) = standard_boot("repair_history", fast_args()).await;
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split]);
    h.sim.add_block(vec![activate]);
    let key = dotk_core::key_of("alice");
    h.wait_until("ACTIVE row", || async { matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Active) }).await;
    let before = h.all_history().await;
    assert_eq!(before.len(), 2, "the registration and the activation, and nothing else");

    let phantom = dotk_core::key_of("phantom-name");
    {
        let mut conn = h.app.backends.db.acquire().await.unwrap();
        let row = DeedRow::active("phantom-name".into(), 0, [0xEE; 32], ([0xEE; 32], 0), test_params().bond);
        dotk_indexer::db::upsert_deed(&mut conn, &phantom, &row).await.unwrap();
        let refuted = DeedRow::active("alice".into(), 0, OWNER_B, ([0xaa; 32], 0), test_params().bond);
        dotk_indexer::db::upsert_deed(&mut conn, &key, &refuted).await.unwrap();
    }
    let report = h.selftest_now().await;
    assert!(report.repaired.dropped + report.repaired.demoted >= 1, "the pass must actually repair something: {report:?}");
    assert!(h.deed_row(&phantom).await.is_none(), "the phantom really was dropped, or this proves nothing");

    assert_eq!(h.all_history().await, before, "a repair leaves history exactly as it found it");
    h.stop().await;
}
