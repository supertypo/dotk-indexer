use std::sync::atomic::Ordering;

use crate::common::*;
use dotk_core::state::GapState;
use dotk_indexer::chain::Kaspad;
use dotk_indexer::model::{DeedRow, HistoryOp, RowKind};

/// The history of `alice` after a register, an activate, a transfer and a release.
async fn assert_lifecycle_history(h: &Harness, key: &[u8; 32]) {
    let history = h.history(key).await;
    assert_eq!(
        history.iter().map(|e| e.op).collect::<Vec<_>>(),
        vec![HistoryOp::Register, HistoryOp::Activate, HistoryOp::Transfer, HistoryOp::Release]
    );
    assert_eq!(history[0].state.as_ref().unwrap().kind, RowKind::Pending);
    assert_eq!(history[1].state.as_ref().unwrap().owner, Some(OWNER_A));
    assert_eq!(history[1].state.as_ref().unwrap().name.as_deref(), Some("alice"));
    assert_eq!(history[2].state.as_ref().unwrap().owner, Some(OWNER_B), "the transfer names the new owner");
    assert!(history[3].state.is_none(), "a release ends the deed, so there is no state after it");
    assert!(history.iter().all(|e| e.txid != [0u8; 32] && e.block_hash != [0u8; 32] && e.daa_score > 0));
    assert!(history.windows(2).all(|w| w[0].blue_score <= w[1].blue_score), "chain order");
}

#[tokio::test]
async fn a_name_is_indexed_and_proven_through_register_activate_transfer_and_release() {
    let (h, mut w) = standard_boot("lifecycle", fast_args()).await;
    let key = dotk_core::key_of("alice");

    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split]);
    h.wait_until("PENDING row", || async { matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Pending) }).await;
    let row = h.deed_row(&key).await.unwrap();
    assert!(row.claim.is_some() && row.accepted_daa.is_some() && row.outpoint_txid.is_some());
    let registered = row.accepted_daa;
    assert_eq!(row.value, Some(test_params().bond + test_params().deposit), "a pending deed holds bond + deposit");
    let mut gaps = h.gap_rows().await;
    gaps.sort_by_key(|g| g.lo);
    assert_eq!(gaps, vec![GapState { lo: WHOLE_KEYSPACE.lo, hi: key }, GapState { lo: key, hi: WHOLE_KEYSPACE.hi }]);
    let report = h.selftest_now().await;
    assert!(report.proven);
    assert_eq!((report.gaps_checked, report.deeds_checked, report.pending_checked), (2, 1, 1), "the claim is proven, not skipped");

    h.sim.add_block(vec![activate]);
    h.wait_until("ACTIVE row", || async { matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Active) }).await;
    let row = h.deed_row(&key).await.unwrap();
    assert_eq!((row.name.as_deref(), row.owner, row.value), (Some("alice"), Some(OWNER_A), Some(test_params().bond)));
    assert!(row.claim.is_none(), "the claim is spent knowledge once the name is revealed");
    assert_eq!(row.accepted_daa, registered, "activation carries the registration DAA over from the pending row");
    let report = h.selftest_now().await;
    assert_eq!((report.deeds_checked, report.pending_checked), (1, 0), "the same deed, now counted active");
    assert!(report.proven, "the carried registration DAA is not part of any address, so the row still proves");

    h.sim.add_block(vec![w.transfer("alice", &OWNER_B)]);
    h.wait_until("transferred owner", || async { matches!(h.deed_row(&key).await, Some(r) if r.owner == Some(OWNER_B)) }).await;
    assert_eq!(h.deed_row(&key).await.unwrap().accepted_daa, registered, "a transfer changes the owner, not the age");

    h.sim.add_block(vec![w.release("alice")]);
    h.wait_until("released row gone", || async { h.deed_row(&key).await.is_none() }).await;
    assert_eq!(h.gap_rows().await, vec![WHOLE_KEYSPACE], "zero residue: the keyspace is one gap again");
    assert!(h.selftest_now().await.proven);

    assert_lifecycle_history(&h, &key).await;
    h.stop().await;
}

/// In a merged partial probe, every missing address looks refuted, and repair deletes rows on that.
#[tokio::test]
async fn a_chunk_that_never_answered_fails_the_whole_probe() {
    let mut args = fast_args();
    args.selftest_probe_chunk = 2;
    let (h, mut w) = standard_boot("probe_partial", args).await;

    let names = ["alice", "bob", "carol"];
    for name in names {
        let key = dotk_core::key_of(name);
        let (split, activate) = w.register(name, &OWNER_A);
        h.sim.add_block(vec![split]);
        h.wait_until("PENDING row", || async { matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Pending) }).await;
        h.sim.add_block(vec![activate]);
        h.wait_until("ACTIVE row", || async { matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Active) }).await;
    }
    let passing = h.selftest_now().await;
    assert!(passing.proven, "the fixture must be provable before the node starts failing");

    h.sim.fail_utxos_after(1);
    assert!(dotk_indexer::audit::run_once(&h.app).await.is_none(), "the pass errors out rather than reaching a verdict");

    let after = h.app.verdict.health.read().await.selftest.clone().expect("a verdict");
    assert_eq!(after.finished_ms, passing.finished_ms, "an errored pass must not replace the verdict");
    for name in names {
        assert!(h.deed_row(&dotk_core::key_of(name)).await.is_some(), "{name} must survive a probe that errored");
    }
    h.stop().await;
}

#[tokio::test]
async fn a_reorg_replays_the_registration_onto_the_winning_chain() {
    let (h, mut w) = standard_boot("regular_reorg", fast_args()).await;
    let key = dotk_core::key_of("alice");

    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split.clone()]);
    h.sim.add_block(vec![activate.clone()]);
    h.wait_until("ACTIVE", || async { matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Active) }).await;

    h.sim.reorg(2, vec![vec![], vec![split], vec![activate], vec![]]);
    h.wait_until("ACTIVE again after reorg", || async {
        matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Active && r.name.as_deref() == Some("alice"))
    })
    .await;
    assert!(h.selftest_now().await.proven);

    // The row is ACTIVE before the reorg too, so only the block hashes show it applied.
    h.wait_until("history rewritten onto the winning chain", || async {
        let live: std::collections::HashSet<[u8; 32]> = h.sim.chain_hashes().into_iter().collect();
        let history = h.history(&key).await;
        history.len() == 2 && history.iter().all(|e| live.contains(&e.block_hash))
    })
    .await;
    let history = h.history(&key).await;
    assert_eq!(history.iter().map(|e| e.op).collect::<Vec<_>>(), vec![HistoryOp::Register, HistoryOp::Activate]);

    h.stop().await;
}

#[tokio::test]
async fn a_reorg_that_drops_every_block_restores_the_empty_registry() {
    let (h, mut w) = standard_boot("deep_reorg", fast_args()).await;
    let alice = dotk_core::key_of("alice");
    let bob = dotk_core::key_of("bob");

    let (s1, a1) = w.register("alice", &OWNER_A);
    let (s2, _a2) = w.register("bob", &OWNER_B);
    let rel = w.release("alice");
    h.sim.add_block(vec![s1]);
    h.sim.add_block(vec![a1, s2]);
    h.sim.add_block(vec![rel]);
    h.wait_until("bob pending + alice released", || async {
        h.deed_row(&alice).await.is_none() && matches!(h.deed_row(&bob).await, Some(r) if r.kind == RowKind::Pending)
    })
    .await;

    h.sim.reorg(3, vec![vec![], vec![]]);
    h.wait_until("empty registry restored", || async { h.deed_row(&alice).await.is_none() && h.deed_row(&bob).await.is_none() }).await;
    h.wait_until("the gap cache is back to the genesis gap", || async { h.gap_rows().await == vec![WHOLE_KEYSPACE] }).await;
    assert!(h.all_history().await.is_empty(), "the whole history goes with the blocks that carried it");
    assert!(h.selftest_now().await.proven);
    h.stop().await;
}

/// With a tip distance, the replacement chain waits behind the margin, so the node reports removed blocks and none added.
#[tokio::test]
async fn a_removed_only_answer_moves_neither_the_checkpoint_nor_the_rows() {
    let mut args = fast_args();
    args.vcp_tip_distance = 3;
    let (h, mut w) = standard_boot("removed_only", args).await;
    let key = dotk_core::key_of("alice");

    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split]);
    let activate_block = h.sim.add_block(vec![activate]);
    h.sim.pad(4); // the margin clears exactly up to the activate block
    let expect_cp = dotk_indexer::convert::hex32(&activate_block);
    h.wait_until("checkpoint settles on the activate block", || async { h.checkpoint().await.as_deref() == Some(expect_cp.as_str()) })
        .await;
    assert!(matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Active));

    h.sim.reorg(5, vec![vec![]]);
    let polled = h.sim.polls().len();
    h.wait_until("the removed-only answer was polled", || async { h.sim.polls().len() >= polled + 2 }).await;
    assert_eq!(h.checkpoint().await.as_deref(), Some(expect_cp.as_str()), "removed-only must not move the checkpoint");
    assert!(matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Active), "removed-only must not undo");

    h.sim.pad(4);
    h.wait_until("activate undone once the margin clears", || async {
        matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Pending)
    })
    .await;
    assert!(h.selftest_now().await.proven);
    h.stop().await;
}

#[tokio::test]
async fn a_reorg_that_lands_across_a_restart_is_undone_from_the_journal() {
    let (h, mut w) = standard_boot("across_restart", fast_args()).await;
    let alice = dotk_core::key_of("alice");
    let bob = dotk_core::key_of("bob");

    let (s1, a1) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s1, a1]);
    let (s2, _) = w.register("bob", &OWNER_B);
    h.sim.add_block(vec![s2]);
    h.wait_until("both rows", || async { h.deed_row(&alice).await.is_some() && h.deed_row(&bob).await.is_some() }).await;

    let (sim, pool) = (h.sim.clone(), h.app.backends.db.clone());
    h.stop().await;

    let mut conn = pool.acquire().await.unwrap();
    let cp = dotk_indexer::db::get_var(&mut conn, dotk_indexer::db::VAR_VCP_CHECKPOINT).await.unwrap().unwrap();
    drop(conn);
    let start = dotk_indexer::convert::unhex32(&cp).unwrap();
    let h = Harness::boot(pool, sim, fast_args(), start);

    h.sim.reorg(1, vec![vec![], vec![]]);
    h.wait_until("bob undone across the restart", || async { h.deed_row(&bob).await.is_none() }).await;
    assert!(matches!(h.deed_row(&alice).await, Some(r) if r.kind == RowKind::Active));
    assert!(h.selftest_now().await.proven);
    h.stop().await;
}

/// A self-transfer loop costs only fees, so the retention window must not decide how large a snapshot is.
#[tokio::test]
async fn resume_section_is_a_tail_of_chain_not_the_retention_window() {
    let pool = fresh_pool("resume_window").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();
    let key = dotk_core::key_of("alice");
    for blue in 1u64..=1000 {
        let mut block = [0u8; 32];
        block[..8].copy_from_slice(&blue.to_be_bytes());
        dotk_indexer::db::append_event(&mut conn, &event(block, 0, blue, key, None)).await.unwrap();
    }
    let net_bps = 10;
    let window = dotk_indexer::snapshot::export_event_window(net_bps);

    let kept = dotk_indexer::db::events_for_resume(&mut conn, 1000 - window, i64::MAX).await.unwrap();
    assert_eq!(kept.len(), 101, "blue scores 900..=1000 inclusive");
    assert_eq!(kept.first().unwrap().blue_score, 900, "oldest kept is the window edge");
    assert_eq!(kept.last().unwrap().blue_score, 1000, "the tip is always in");
    assert_eq!(dotk_indexer::db::journal_coverage(&mut conn).await.unwrap(), Some(1));
}

/// One chain block that merges a side branch of card sweeps can journal tens of thousands of events.
#[tokio::test]
async fn a_block_of_more_than_i16_max_events_undoes_in_order() {
    let pool = fresh_pool("wide_seq").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();
    let key = dotk_core::key_of("alice");
    let block = [0x5e; 32];
    let images = [
        DeedRow::owner_unknown(Some("first".into())),
        DeedRow::owner_unknown(Some("second".into())),
        DeedRow::owner_unknown(Some("third".into())),
    ];
    for (seq, prev) in [32_767i32, 32_768, 70_000].into_iter().zip(&images) {
        dotk_indexer::db::append_event(&mut conn, &event(block, seq, 1, key, Some(prev.clone()))).await.unwrap();
    }
    dotk_indexer::db::upsert_deed(&mut conn, &key, &DeedRow::owner_unknown(Some("fourth".into()))).await.unwrap();
    let undone = dotk_indexer::chain::undo_block(&mut conn, &block).await.unwrap();
    assert_eq!(undone, 3);
    let row = dotk_indexer::db::get_deed(&mut conn, &key).await.unwrap().expect("the row the block found");
    assert_eq!(row.name.as_deref(), Some("first"), "undo ends on the pre-image of the lowest seq");
}

/// A reorg that starts at the coverage floor does not escalate, so an importer that holds half a block undoes only that half.
#[tokio::test]
async fn the_resume_cap_never_splits_a_block() {
    let pool = fresh_pool("resume_cap").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();
    let key = dotk_core::key_of("alice");
    for blue in 1u64..=10 {
        let mut block = [0u8; 32];
        block[..8].copy_from_slice(&blue.to_be_bytes());
        for seq in 0..5i32 {
            dotk_indexer::db::append_event(&mut conn, &event(block, seq, blue, key, None)).await.unwrap();
        }
    }
    for cap in [1i64, 3, 5, 7, 10, 12, 20, 23, 50, 500] {
        let kept = dotk_indexer::db::events_for_resume(&mut conn, 0, cap).await.unwrap();
        let mut per_block: std::collections::HashMap<[u8; 32], usize> = std::collections::HashMap::new();
        for e in &kept {
            *per_block.entry(e.block_hash).or_default() += 1;
        }
        assert!(per_block.values().all(|n| *n == 5), "cap {cap} left a partial block: {:?}", per_block.values().collect::<Vec<_>>());
        assert!(i64::try_from(kept.len()).unwrap() <= cap, "cap {cap} was exceeded");
        assert_eq!(i64::try_from(kept.len()).unwrap(), (cap / 5 * 5).min(50), "cap {cap} trimmed a block it did not split");
        if let Some(oldest) = kept.first() {
            assert_eq!(kept.last().unwrap().blue_score, 10, "cap {cap} must keep the tip");
            assert_eq!(oldest.blue_score, 10 - (kept.len() as u64 / 5) + 1, "cap {cap} kept a contiguous newest run");
        }
    }
    assert!(dotk_indexer::db::events_for_resume(&mut conn, 0, 3).await.unwrap().is_empty());

    // `import` can write two blocks at one blue score, so the trim must cut a shared score whole.
    let mut sibling = [0u8; 32];
    sibling[..8].copy_from_slice(&11u64.to_be_bytes());
    sibling[31] = 0xff;
    for seq in 0..5i32 {
        dotk_indexer::db::append_event(&mut conn, &event(sibling, seq, 10, key, None)).await.unwrap();
    }
    let kept = dotk_indexer::db::events_for_resume(&mut conn, 0, 13).await.unwrap();
    assert_eq!(kept.len(), 10, "both blue-10 blocks kept whole, and blue 9 is what the cap cut");
    assert!(kept.iter().all(|e| e.blue_score == 10), "the trim took the oldest score, not the newest");
    let kept = dotk_indexer::db::events_for_resume(&mut conn, 0, 7).await.unwrap();
    assert!(kept.iter().all(|e| e.blue_score != 10), "a split blue score must be trimmed whole, not one of its blocks");
}

/// The parser refuses a retention below finality, so the test sets the field directly.
#[tokio::test]
async fn pruning_keeps_the_retention_window_and_purges_swept_cards() {
    const RETENTION: u64 = 4;
    let mut args = fast_args();
    args.journal_retention = RETENTION;
    let (h, mut w) = standard_boot("journal_pruning", args).await;
    let (alice, bob) = (dotk_core::key_of("alice"), dotk_core::key_of("bob"));
    let (s1, a1) = w.register("alice", &OWNER_A);
    let (s2, a2) = w.register("bob", &OWNER_A);
    h.sim.add_block(vec![s1, a1, s2, a2]);
    h.sim.add_block(vec![
        w.transfer_with_card("alice", &OWNER_B, &records_named("alice"), &OWNER_B),
        w.transfer_with_card("bob", &OWNER_A, &records_named("bob"), &OWNER_A),
    ]);
    // Retires alice's card, and the sweep then marks it.
    h.sim.add_block(vec![w.transfer("alice", &OWNER_A)]);
    h.sim.add_block(vec![w.sweep_cards(&OWNER_B)]);
    let swept_at = h.sim.chain_hashes().len() as u64; // the blue score of the tip
    h.sim.pad(usize::try_from(RETENTION).unwrap() + 2);
    let (s3, a3) = w.register("carol", &OWNER_A);
    h.sim.add_block(vec![s3, a3]);
    let tip = h.sim.sink_blue_score().await.unwrap();
    h.wait_until("the pipeline reached the tip", || async { h.app.progress.last_block_blue_score.load(Ordering::Relaxed) == tip })
        .await;

    let watermark = tip - RETENTION;
    assert!(swept_at < watermark);
    let mut conn = h.app.backends.db.acquire().await.unwrap();
    let events = dotk_indexer::db::events_for_resume(&mut conn, 0, 1000).await.unwrap();
    assert_eq!(events.len(), 2, "only carol's split and activate are at or above the watermark {watermark}");
    assert!(events.iter().all(|e| e.blue_score == tip && e.key == dotk_core::key_of("carol")));
    let cards = dotk_indexer::db::all_cards(&mut conn).await.unwrap();
    assert_eq!(cards.len(), 1, "the swept card is purged");
    assert_eq!((cards[0].state.key, cards[0].swept_at), (bob, None), "and the live card is kept");
    drop(conn);
    assert_eq!(h.app.progress.coverage_floor.load(Ordering::Relaxed), watermark, "the floor follows the watermark");

    // A shallow reorg must not touch the rows below the pruned window.
    h.sim.reorg(1, vec![vec![], vec![]]);
    h.sim.pad(1);
    let tip = h.sim.sink_blue_score().await.unwrap();
    h.wait_until("the reorg was committed", || async { h.app.progress.last_block_blue_score.load(Ordering::Relaxed) == tip }).await;
    assert!(h.deed_row(&alice).await.is_some());
    assert!(h.selftest_now().await.proven);
    h.stop().await;
}

/// A twin deployment decodes identically and uses the same addresses. Only the covenant id on output 0 differs.
#[tokio::test]
async fn twin_registry_events_are_ignored() {
    let (h, mut w) = standard_boot("twin_registry", fast_args()).await;
    let mallory = dotk_core::key_of("mallory");
    let alice = dotk_core::key_of("alice");

    let mut twin = w.clone();
    let mut foreign_split = twin.split("mallory", &OWNER_B);
    let twin_covenant_id = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    for o in &mut foreign_split.outputs {
        if o.covenant_id.is_some() {
            o.covenant_id = Some(twin_covenant_id.to_string());
        }
    }
    // Flipped input txids keep the twin's spend off the UTXOs of this registry.
    for (txid, ..) in &mut foreign_split.inputs {
        txid[0] ^= 0xFF;
    }
    assert_eq!(foreign_split.sig_scripts, w.clone().split("mallory", &OWNER_B).sig_scripts, "byte-identical to a split of ours");
    h.sim.add_block(vec![foreign_split]);
    // Once alice appears, the pipeline is past the twin's split.
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    h.wait_until("ACTIVE row for alice", || async { matches!(h.deed_row(&alice).await, Some(r) if r.kind == RowKind::Active) }).await;

    assert!(h.deed_row(&mallory).await.is_none(), "the twin's registration is not a row here");
    let mut gaps = h.gap_rows().await;
    gaps.sort_by_key(|g| g.lo);
    assert_eq!(gaps, vec![GapState { lo: WHOLE_KEYSPACE.lo, hi: alice }, GapState { lo: alice, hi: WHOLE_KEYSPACE.hi }]);
    assert!(h.history(&mallory).await.is_empty(), "nor is it an event in anyone's history");
    let report = h.selftest_now().await;
    assert!(report.proven, "the mirror proves against its own lineage alone");
    assert_eq!((report.gaps_checked, report.deeds_checked), (2, 1));
    h.stop().await;
}

#[tokio::test]
async fn a_lagging_node_does_not_stop_the_pipeline() {
    let (h, mut w) = standard_boot("lagging_node", fast_args()).await;
    h.sim.lag_polls(1);
    let key = dotk_core::key_of("alice");
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    h.wait_until_for("the pipeline got past the lagging answer", std::time::Duration::from_secs(30), || async {
        matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Active)
    })
    .await;
    h.stop().await;
}
