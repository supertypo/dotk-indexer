use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::common::sim::SimKaspad;
use crate::common::*;
use axum::http::StatusCode;
use dotk_indexer::chain::Kaspad;
use dotk_indexer::model::RowKind;
use dotk_indexer::snapshot;
use kaspa_addresses::Prefix;

fn window(h: &Harness) -> u64 {
    snapshot::export_event_window(h.app.deployment.net_bps)
}

fn export_active(name: &str, owner: &[u8; 32]) -> snapshot::ExportDeed {
    snapshot::ExportDeed {
        key: dotk_indexer::convert::hex32(&dotk_core::key_of(name)),
        row: snapshot::ExportRow {
            kind: 0,
            name: Some(name.into()),
            owner_type: Some(0),
            owner: Some(faster_hex::hex_string(owner)),
            claim: None,
            outpoint_txid: None,
            outpoint_index: None,
            value: None,
            accepted_daa: None,
        },
    }
}

#[tokio::test]
async fn an_exported_snapshot_imports_into_a_second_indexer_that_proves_it() {
    // A snapshot carries no gaps. B derives them from the imported keys and probes their outpoints itself.
    let (ha, mut w) = standard_boot("import_a", fast_args()).await;
    let alice = dotk_core::key_of("alice");
    let (s, a) = w.register("alice", &OWNER_A);
    ha.sim.add_block(vec![s, a]);
    let squat = name_above(&alice, "sq");
    let sq = w.split(&squat, &OWNER_B);
    ha.sim.add_block(vec![sq]);
    ha.wait_until("registry built", || async { ha.deed_row(&dotk_core::key_of(&squat)).await.is_some() }).await;

    let snap_a = snapshot::export(&ha.app.backends.db, REGISTRY_COVENANT_ID, window(&ha), std::time::Duration::ZERO).await.unwrap();
    let body = serde_json::to_string(&snap_a).unwrap();

    let pool_b = fresh_pool("import_b").await;
    dotk_indexer::db::migrate(&pool_b).await.unwrap();
    let parsed: snapshot::Snapshot = serde_json::from_str(&body).unwrap();
    let (checkpoint, coverage) = snapshot::import(&pool_b, &parsed, &genesis_file()).await.unwrap();
    let hb = Harness::boot_with(pool_b, ha.sim.clone(), fast_args(), checkpoint, coverage.unwrap_or(u64::MAX), SelfTest::Ambient);
    hb.sim.pad(1); // `caught_up` depends on the age of the last block

    let report = hb.wait_selftest().await;
    assert!(report.proven, "imported snapshot must validate: {report:?}");

    let snap_b = snapshot::export(&hb.app.backends.db, REGISTRY_COVENANT_ID, window(&hb), std::time::Duration::ZERO).await.unwrap();
    assert_eq!(
        serde_json::to_string(&snap_a.deeds).unwrap(),
        serde_json::to_string(&snap_b.deeds).unwrap(),
        "the canonical core is byte-identical between mirrors"
    );
    assert_eq!(snap_a.registry_covenant_id, snap_b.registry_covenant_id);
    assert_eq!(hb.gap_rows().await.len(), 3, "two live keys derive three gaps");

    let tr = w.transfer("alice", &OWNER_B);
    hb.sim.add_block(vec![tr]);
    hb.wait_until("B resumes the stream", || async { matches!(hb.deed_row(&alice).await, Some(r) if r.owner == Some(OWNER_B)) }).await;
    ha.stop().await;
    hb.stop().await;
}

#[tokio::test]
async fn an_import_refuses_another_registry_and_a_key_given_twice() {
    let pool = fresh_pool("import_foreign").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let snapshot = snapshot::Snapshot { registry_covenant_id: "dd".repeat(32), ..snapshot_at("00".repeat(32), vec![], vec![]) };
    let err = snapshot::import(&pool, &snapshot, &genesis_file()).await.unwrap_err();
    assert!(err.to_string().contains("refusing"), "{err}");

    // The key is the primary key, so a second row overwrites the first.
    let twice = snapshot::Snapshot {
        registry_covenant_id: REGISTRY_COVENANT_ID.into(),
        deeds: vec![export_active("alice", &OWNER_A), export_active("alice", &OWNER_B)],
        ..snapshot
    };
    let err = snapshot::import(&pool, &twice, &genesis_file()).await.unwrap_err();
    assert!(err.to_string().contains("twice"), "{err}");
}

/// Boots on an imported snapshot whose `alice` names a forged owner, caught up and not yet judged.
async fn forged_import_boot() -> Harness {
    let (mut w, genesis_tx) = World::new();
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx]));
    let (s, a) = w.register("alice", &OWNER_A);
    sim.add_block(vec![s, a]);
    import_and_boot("unjudged_import", sim, vec![export_active("alice", &[0x5Au8; 32])]).await
}

/// A mirror can forge a row that passes every import check, so the API answers 503 until this process judges it.
#[tokio::test]
async fn nothing_from_the_tables_is_served_before_the_first_verdict() {
    let h = forged_import_boot().await;
    let web = Web::new(&h);
    assert!(h.deed_row(&dotk_core::key_of("alice")).await.is_some());
    for uri in [
        "/v1/names/alice",
        "/v1/keys/0000000000000000000000000000000000000000000000000000000000000000",
        "/v1/keys/0000000000000000000000000000000000000000000000000000000000000000/history",
        "/v1/owners/0/5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a",
        "/v1/spenders/0/5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a/cards",
        "/v1/keyspace",
    ] {
        let (status, cache, body) = web.get_json(uri).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{uri}: {body}");
        assert_eq!(body["code"], "not_ready", "{uri}");
        assert_eq!(cache, "no-store", "{uri}");
    }
    for uri in ["/v1/names/alice/key", "/v1/snapshot?proven=false"] {
        let (status, _, body) = web.get(uri).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{uri} reads the tables: {body}");
    }

    let report = h.selftest_now().await;
    assert_eq!(report.repaired.demoted, 1, "{report:?}");
    let (status, _, _) = web.get("/v1/names/alice").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "owner-unknown resolves nothing, and the forged owner was never served");
    let (status, _, _) = web.get("/v1/keyspace").await;
    assert_eq!(status, StatusCode::OK);
    h.stop().await;
}

#[tokio::test]
async fn a_name_inside_a_blind_spot_is_withheld_not_denied() {
    let (mut w, genesis_tx) = World::new();
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx]));
    for name in ["alice", "bob"] {
        let (s, a) = w.register(name, &OWNER_A);
        sim.add_block(vec![s, a]);
    }
    let h = import_and_boot("blind_spot_withheld", sim, vec![export_active("alice", &OWNER_A)]).await;
    assert!(!h.selftest_now().await.proven);

    let web = Web::new(&h);
    let get = |uri: String| {
        let web = &web;
        async move {
            let (status, _, body) = web.get(&uri).await;
            (status, serde_json::from_str::<serde_json::Value>(&body).unwrap_or(serde_json::Value::Null))
        }
    };
    let (status, body) = get("/v1/names/bob".into()).await;
    assert_eq!((status, body["code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("not_ready")), "{body}");
    let (status, body) = get(format!("/v1/owners/0/{}", dotk_indexer::convert::hex32(&OWNER_A))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["names"], serde_json::json!(["alice"]), "the proven entries, and nothing guessed");
    let (status, _) = get("/v1/names/alice".into()).await;
    assert_eq!(status, StatusCode::OK, "a proven row outside the blind spot is still served");
    h.stop().await;
}

async fn import_and_boot(name: &str, sim: Arc<SimKaspad>, deeds: Vec<snapshot::ExportDeed>) -> Harness {
    let tip = sim.dag_info().await.unwrap().virtual_parent;
    let snapshot =
        snapshot::Snapshot { proven: true, proven_at: Some(1), ..snapshot_at(dotk_indexer::convert::hex32(&tip), deeds, vec![]) };
    let pool = fresh_pool(name).await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let (checkpoint, _) = snapshot::import(&pool, &snapshot, &genesis_file()).await.unwrap();
    let h = Harness::boot_with(pool, sim, fast_args(), checkpoint, u64::MAX, SelfTest::Driven);
    h.sim.pad(1);
    h.wait_until("caught up", || async { h.app.progress.caught_up() }).await;
    h
}

#[tokio::test]
async fn a_forged_owner_beside_a_blind_spot_is_demoted() {
    let (mut w, genesis_tx) = World::new();
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx]));
    let mut names = ["alice", "bob", "carol"];
    names.sort_by_key(|n| dotk_core::key_of(n));
    for name in names {
        let (s, a) = w.register(name, &OWNER_A);
        sim.add_block(vec![s, a]);
    }
    let h = import_and_boot(
        "forged_beside_blind_spot",
        sim,
        vec![export_active(names[0], &OWNER_A), export_active(names[1], &[0x5A; 32])],
    )
    .await;

    let report = h.selftest_now().await;
    assert_eq!((report.repaired.demoted, report.repaired.dropped), (1, 0), "{report:?}");
    assert_eq!(h.deed_row(&dotk_core::key_of(names[1])).await.unwrap().kind, RowKind::OwnerUnknown);
    assert_eq!(h.deed_row(&dotk_core::key_of(names[0])).await.unwrap().owner, Some(OWNER_A));
    h.stop().await;
}

#[tokio::test]
async fn a_row_at_a_keyspace_end_is_dropped() {
    let (mut w, genesis_tx) = World::new();
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx]));
    let (s, a) = w.register("alice", &OWNER_A);
    sim.add_block(vec![s, a]);
    let floor = snapshot::ExportDeed {
        key: dotk_indexer::convert::hex32(&dotk_core::registry::KEY_MIN),
        row: snapshot::ExportRow { kind: 2, name: None, owner_type: None, owner: None, ..export_active("alice", &OWNER_A).row },
    };
    let h = import_and_boot("row_at_keyspace_end", sim, vec![floor, export_active("alice", &OWNER_A)]).await;

    let report = h.selftest_now().await;
    assert_eq!(report.repaired.dropped, 1, "{report:?}");
    assert!(report.proven, "{report:?}");
    h.stop().await;
}

#[tokio::test]
async fn an_import_refuses_a_pending_row_without_its_outpoint() {
    let pending = snapshot::ExportDeed {
        key: dotk_indexer::convert::hex32(&dotk_core::key_of("alice")),
        row: snapshot::ExportRow {
            kind: 1,
            name: None,
            owner_type: None,
            owner: None,
            claim: Some("99".repeat(32)),
            accepted_daa: Some(1),
            ..export_active("alice", &OWNER_A).row
        },
    };
    let snapshot = snapshot_at("11".repeat(32), vec![pending], vec![]);
    let pool = fresh_pool("import_pending_outpoint").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let e = snapshot::import(&pool, &snapshot, &genesis_file()).await.unwrap_err();
    assert!(format!("{e:#}").contains("outpoint"), "{e:#}");
}

fn journal_entry(block: [u8; 32], seq: i32, blue_score: u64, name: &str) -> snapshot::ExportEvent {
    snapshot::ExportEvent {
        block_hash: dotk_indexer::convert::hex32(&block),
        seq,
        blue_score,
        key: dotk_indexer::convert::hex32(&dotk_core::key_of(name)),
        prev: None,
        prev_gaps: vec![],
        prev_cards: vec![],
    }
}

fn journal_body(checkpoint: [u8; 32], events: Vec<snapshot::ExportEvent>) -> snapshot::Snapshot {
    snapshot_at(dotk_indexer::convert::hex32(&checkpoint), vec![export_active("alice", &OWNER_A)], events)
}

#[tokio::test]
async fn a_journal_scored_past_the_checkpoint_is_refused() {
    let (mut w, genesis_tx) = World::new();
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx]));
    let genesis = sim.dag_info().await.unwrap().virtual_parent;
    let (s, a) = w.register("alice", &OWNER_A);
    sim.add_block(vec![s, a]);
    sim.pad(1);
    let chain = sim.virtual_chain(genesis, 0, std::time::Duration::from_secs(5)).await.unwrap().added;
    let checkpoint = &chain[0];
    let honest = journal_entry(checkpoint.hash, 0, checkpoint.blue_score, "alice");
    let verify = |events| {
        let sim = sim.clone();
        async move { snapshot::verify_checkpoint(sim.as_ref(), &journal_body(checkpoint.hash, events), 10).await }
    };

    verify(vec![honest.clone()]).await.expect("the checkpoint block's own entry");
    for blue in [checkpoint.blue_score + 1, i64::MAX as u64] {
        let e = verify(vec![journal_entry([0xCD; 32], 0, blue, "alice")]).await.unwrap_err();
        assert!(e.to_string().contains("above the"), "{e:#}");
    }
    let e = verify(vec![honest.clone(); usize::try_from(snapshot::EXPORT_EVENT_CAP).unwrap() + 1]).await.unwrap_err();
    assert!(e.to_string().contains("an export writes"), "{e:#}");
    sim.reorg(2, vec![vec![]]);
    verify(vec![honest]).await.expect("a checkpoint off the node's chain keeps an honest journal");
}

#[tokio::test]
async fn an_imported_entry_for_a_later_block_gives_way_to_the_pipeline() {
    let (mut w, genesis_tx) = World::new();
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx]));
    let (s, a) = w.register("alice", &OWNER_A);
    sim.add_block(vec![s, a]);
    let checkpoint = sim.dag_info().await.unwrap().virtual_parent;
    let (s, a) = w.register("bob", &OWNER_A);
    let later = sim.add_block(vec![s, a]);
    let pool = fresh_pool("import_later_entry").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let body = journal_body(checkpoint, vec![journal_entry(later, 0, 1, "bob"), journal_entry(later, 1, 1, "bob")]);
    let (start, _) = snapshot::import(&pool, &body, &genesis_file()).await.unwrap();

    let h = Harness::boot_with(pool, sim, fast_args(), start, 1, SelfTest::Driven);
    let bob = dotk_core::key_of("bob");
    h.wait_until("bob applied", || async { matches!(h.deed_row(&bob).await, Some(r) if r.kind == RowKind::Active) }).await;
    tokio::time::timeout(std::time::Duration::from_secs(1), h.app.verdict.selftest_trigger.notified())
        .await
        .expect("a self-test follows");
    h.stop().await;
}

#[tokio::test]
async fn a_shallow_imported_journal_escalates_on_a_deep_reorg() {
    let (mut w, genesis_tx) = World::new();
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx]));
    let (s, a) = w.register("alice", &OWNER_A);
    sim.add_block(vec![s, a]);
    let tip = sim.dag_info().await.unwrap().virtual_parent;

    let snapshot = journal_body(tip, vec![]);
    let pool = fresh_pool("import_shallow").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let (checkpoint, coverage) = snapshot::import(&pool, &snapshot, &genesis_file()).await.unwrap();
    assert!(coverage.is_none());
    let h = Harness::boot_with(pool, sim, fast_args(), checkpoint, u64::MAX, SelfTest::Ambient);
    h.caught_up_without_a_block(); // the first journaled block must be the block under test
    let baseline = h.wait_selftest().await;
    assert!(baseline.proven);

    // Alice never registers on the winning chain, and no journal reaches back that far.
    let alice = dotk_core::key_of("alice");
    h.sim.reorg(1, vec![vec![], vec![]]);
    h.wait_until("escalated validation repaired to chain truth", || async { h.deed_row(&alice).await.is_none() }).await;
    assert!(h.next_verdict().await.proven);
    h.stop().await;
}

/// The pipeline resumes from the checkpoint, so one the node does not know is a database that fails at every start.
#[tokio::test]
async fn a_checkpoint_the_node_does_not_know_is_refused_before_the_import() {
    let (mut w, genesis_tx) = World::new();
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx]));
    let (s, a) = w.register("alice", &OWNER_A);
    sim.add_block(vec![s, a]);
    let tip = sim.dag_info().await.unwrap().virtual_parent;
    let snapshot = |checkpoint: [u8; 32]| journal_body(checkpoint, vec![]);
    snapshot::verify_checkpoint(sim.as_ref(), &snapshot(tip), 10).await.expect("a chain block the node holds");
    let e = snapshot::verify_checkpoint(sim.as_ref(), &snapshot([0xAB; 32]), 10).await.unwrap_err();
    assert!(e.to_string().contains("unknown to the node"), "{e:#}");
}

/// A journal-less import starts the floor at `u64::MAX`, so without the drop every later reorg escalates.
#[tokio::test]
async fn journal_coverage_floor_drops_once_the_pipeline_journals_its_own_blocks() {
    let (mut w, genesis_tx) = World::new();
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx]));
    let tip = sim.dag_info().await.unwrap().virtual_parent;
    let snapshot = snapshot_at(dotk_indexer::convert::hex32(&tip), vec![], vec![]);
    let pool = fresh_pool("coverage_floor").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let (checkpoint, coverage) = snapshot::import(&pool, &snapshot, &genesis_file()).await.unwrap();
    assert!(coverage.is_none());
    let h = Harness::boot_with(pool, sim, fast_args(), checkpoint, u64::MAX, SelfTest::Ambient);
    h.caught_up_without_a_block(); // the first journaled block must be the block under test
    let baseline = h.wait_selftest().await;
    assert!(baseline.proven);
    assert_eq!(h.app.progress.coverage_floor.load(Ordering::Relaxed), u64::MAX, "nothing journaled yet");

    let alice = dotk_core::key_of("alice");
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice indexed", || async { h.deed_row(&alice).await.is_some() }).await;
    h.wait_until("the floor drops to the first self-journaled block", || async {
        h.app.progress.coverage_floor.load(Ordering::Relaxed) < u64::MAX
    })
    .await;

    // The winning chain starts at the floor, so the journal undoes the block exactly.
    h.sim.reorg(1, vec![vec![], vec![]]);
    h.wait_until("the registration was undone", || async { h.deed_row(&alice).await.is_none() }).await;
    // A triggered pass starts at once and takes far less than this on an empty registry.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(
        h.app.verdict.health.read().await.selftest.as_ref().unwrap().finished_ms,
        baseline.finished_ms,
        "exact undo above the coverage floor must not escalate to a full validation"
    );
    h.stop().await;
}
