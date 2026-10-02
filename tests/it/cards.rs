use crate::common::*;
use axum::http::StatusCode;
use dotk_core::cards::{self, CARD_VALUE, RecordValue, Records};
use dotk_indexer::convert::hex32;
use dotk_indexer::db;
use dotk_indexer::model::{CardRow, HistoryOp, RowKind};
use dotk_indexer::snapshot;

fn records(url: &str, primary: bool) -> Records {
    let mut r = Records::new();
    r.insert("url".into(), RecordValue::Text(url.into()));
    if primary {
        r.insert(cards::PRIMARY_KEY.into(), RecordValue::Flag(true));
    }
    r
}

async fn card_rows(h: &Harness) -> Vec<CardRow> {
    let mut conn = h.app.backends.db.acquire().await.unwrap();
    db::all_cards(&mut conn).await.unwrap()
}

async fn live_card(h: &Harness, name: &str) -> Option<db::CardHit> {
    let mut conn = h.app.backends.db.acquire().await.unwrap();
    db::live_card_by_key(&mut conn, &dotk_core::key_of(name)).await.unwrap()
}

async fn spender_cards(h: &Harness, spender: &[u8; 32]) -> Vec<db::CardHit> {
    let mut conn = h.app.backends.db.acquire().await.unwrap();
    db::cards_by_spender(&mut conn, 0, spender, None, i64::MAX).await.unwrap()
}

async fn get_json(h: &Harness, uri: &str) -> (StatusCode, serde_json::Value) {
    let (status, _, v) = Web::new(h).get_json(uri).await;
    (status, v)
}

#[tokio::test]
async fn a_transfer_mints_a_card_the_name_serves() {
    let (h, mut w) = standard_boot("card_mint", fast_args()).await;
    let key = dotk_core::key_of("alice");
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    h.wait_until("ACTIVE", || async { matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Active) }).await;

    let tx = w.transfer_with_card("alice", &OWNER_B, &records("https://alice.example", true), &OWNER_B);
    h.sim.add_block(vec![tx.clone()]);
    h.wait_until("a card row", || async { !card_rows(&h).await.is_empty() }).await;

    let hit = live_card(&h, "alice").await.expect("the name's one card");
    assert!(hit.live);
    assert_eq!(hit.name.as_deref(), Some("alice"));
    assert_eq!((hit.card.txid, hit.card.idx, hit.card.value), (tx.txid, 1, CARD_VALUE), "output 1: the tail after the deed");
    assert_eq!(hit.card.state.spender, OWNER_B);
    assert_eq!(cards::decode_records(&hit.card.blob).unwrap(), records("https://alice.example", true));
    assert!(hit.card.swept_at.is_none());

    let row = h.deed_row(&key).await.unwrap();
    assert_eq!((row.owner, row.outpoint_txid, row.outpoint_index), (Some(OWNER_B), Some(tx.txid), Some(0)));
    let history = h.history(&key).await;
    assert_eq!(history.last().unwrap().op, HistoryOp::Transfer, "a mint is not a history event of its own");
    assert_eq!(history.len(), 3);

    let (status, body) = get_json(&h, "/v1/names/alice").await;
    assert_eq!(status, StatusCode::OK);
    let card = &body["card"];
    assert_eq!(card["records"]["url"], "https://alice.example");
    assert_eq!(card["records"]["primary"], true);
    assert_eq!(card["outpointTxid"], hex32(&tx.txid));
    assert_eq!(card["live"], true);
    assert_eq!(card["cardAddress"], hit.card.state.address(kaspa_addresses::Prefix::Testnet).to_string());
    let (_, body) = get_json(&h, &format!("/v1/owners/0/{}", hex32(&OWNER_B))).await;
    assert_eq!(body["names"][0], "alice");
    assert_eq!(body["cards"][0]["name"], "alice");
    let report = h.selftest_now().await;
    assert!(report.proven, "{report:?}");
    assert_eq!((report.repaired.cards_swept, report.repaired.cards_restored), (0, 0));
    h.stop().await;
}

#[tokio::test]
async fn a_later_transfer_retires_the_card_and_a_sweep_reclaims_it() {
    let (h, mut w) = standard_boot("card_retire", fast_args()).await;
    let key = dotk_core::key_of("alice");
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    h.sim.add_block(vec![w.transfer_with_card("alice", &OWNER_B, &records("u1", false), &OWNER_B)]);
    h.wait_until("a card row", || async { !card_rows(&h).await.is_empty() }).await;
    let minted = card_rows(&h).await[0].clone();

    // The card retires with no write, because its txid stops matching the deed's.
    h.sim.add_block(vec![w.transfer("alice", &OWNER_A)]);
    h.wait_until("transferred back", || async { matches!(h.deed_row(&key).await, Some(r) if r.owner == Some(OWNER_A)) }).await;
    assert!(live_card(&h, "alice").await.is_none(), "a retired card is not served for the name");
    let (_, body) = get_json(&h, "/v1/names/alice").await;
    assert!(body["card"].is_null(), "a name with no live card serves no card field");
    let left = spender_cards(&h, &OWNER_B).await;
    assert_eq!(left.len(), 1, "but it is still the spender's to reclaim");
    assert!(!left[0].live);
    let (status, body) = get_json(&h, &format!("/v1/spenders/0/{}/cards", hex32(&OWNER_B))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["cards"][0]["live"], false);
    assert_eq!(body["cards"][0]["name"], "alice");

    let history_before = h.all_history().await;
    let sweep = w.sweep_cards(&OWNER_B);
    h.sim.add_block(vec![sweep]);
    h.wait_until("swept", || async { card_rows(&h).await[0].swept_at.is_some() }).await;
    assert!(spender_cards(&h, &OWNER_B).await.is_empty(), "a swept card is omitted everywhere");
    assert_eq!(h.all_history().await, history_before, "a sweep is nothing that happened to the name");
    assert_eq!(h.deed_row(&key).await.unwrap().owner, Some(OWNER_A));

    h.sim.reorg(1, vec![vec![], vec![]]);
    h.wait_until("sweep undone", || async { card_rows(&h).await[0].swept_at.is_none() }).await;
    assert_eq!(spender_cards(&h, &OWNER_B).await.len(), 1);
    assert_eq!(card_rows(&h).await[0].txid, minted.txid);
    assert!(h.selftest_now().await.proven);
    h.stop().await;
}

#[tokio::test]
async fn the_probe_marks_a_vanished_card_and_restores_a_returned_one() {
    let (h, mut w) = standard_boot("card_probe", fast_args()).await;
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    h.sim.add_block(vec![w.transfer_with_card("alice", &OWNER_A, &records("u1", false), &OWNER_A)]);
    h.wait_until("a card row", || async { !card_rows(&h).await.is_empty() }).await;
    let card = card_rows(&h).await[0].clone();
    h.sim.clear_queried();
    assert!(h.selftest_now().await.proven);
    let card_call = h.sim.utxo_calls();

    let mut conn = h.app.backends.db.acquire().await.unwrap();
    db::set_card_swept(&mut conn, &card.txid, card.idx, Some(1)).await.unwrap();
    h.sim.clear_queried();
    h.sim.after_utxo_calls(card_call, {
        let app = h.app.clone();
        move || async move { fall_behind(&app.progress) }
    });
    let behind = h.selftest_now().await;
    assert_eq!(behind.repaired.cards_restored, 0, "{behind:?}");
    assert_eq!(card_rows(&h).await[0].swept_at, Some(1), "no mark is written once the indexer is behind");
    h.caught_up_without_a_block();
    let report = h.selftest_now().await;
    assert!(report.proven);
    assert_eq!((report.repaired.cards_swept, report.repaired.cards_restored), (0, 1), "{report:?}");
    assert!(card_rows(&h).await[0].swept_at.is_none());

    h.sim.add_block(vec![w.sweep_cards(&OWNER_A)]);
    h.wait_until("swept in the stream", || async { card_rows(&h).await[0].swept_at.is_some() }).await;
    db::set_card_swept(&mut conn, &card.txid, card.idx, None).await.unwrap();
    let report = h.selftest_now().await;
    assert!(report.proven);
    assert_eq!((report.repaired.cards_swept, report.repaired.cards_restored), (1, 0), "{report:?}");
    let marked = card_rows(&h).await[0].swept_at.unwrap();
    assert!(marked > 0, "marked at the tip's blue score");
    h.stop().await;
}

#[tokio::test]
async fn a_card_nobody_can_prove_is_never_served() {
    let (h, mut w) = standard_boot("card_planted", fast_args()).await;
    let (sa, aa) = w.register("alice", &OWNER_A);
    let (sb, ab) = w.register("bob", &OWNER_B);
    h.sim.add_block(vec![sa, aa, sb, ab]);
    let alice = dotk_core::key_of("alice");
    h.wait_until("both ACTIVE", || async { matches!(h.deed_row(&alice).await, Some(r) if r.kind == RowKind::Active) }).await;

    // A card for alice's key beside bob's deed.
    let mut planted = w.transfer_with_card("bob", &OWNER_B, &records("evil", true), &OWNER_B);
    let alice_state = cards::CardState::new(alice, cards::records_of(b"x"), dotk_core::OwnerType::Pubkey, OWNER_B).unwrap();
    planted.payload = cards::encode_payload(Some(&cards::CardMint { state: alice_state.clone(), blob: b"x".to_vec() })).unwrap();
    planted.outputs[1].spk = alice_state.spk();
    let mut silent = w.transfer_with_card("bob", &OWNER_B, &records("quiet", false), &OWNER_B);
    silent.payload = vec![];
    silent.inputs.truncate(1);
    silent.sig_scripts.truncate(1);
    let mut broken = w.transfer_with_card("bob", &OWNER_B, &records("broken", false), &OWNER_B);
    broken.payload[4] = 9;
    broken.inputs.truncate(1);
    broken.sig_scripts.truncate(1);
    // The card at output 2 behind a filler.
    let mut misplaced = w.transfer_with_card("bob", &OWNER_B, &records("late", false), &OWNER_B);
    misplaced.inputs.truncate(1);
    misplaced.sig_scripts.truncate(1);
    misplaced.outputs.insert(1, sim::SimOut { value: CARD_VALUE, spk: vec![0x51], covenant_id: None });
    let mut doubled = w.transfer_with_card("bob", &OWNER_B, &records("twice", false), &OWNER_B);
    doubled.inputs.truncate(1);
    doubled.sig_scripts.truncate(1);
    let second = doubled.payload[5..].to_vec();
    doubled.payload.extend_from_slice(&second);
    for tx in [planted, silent, broken, misplaced, doubled.clone()] {
        h.sim.add_block(vec![tx]);
    }
    let bob = dotk_core::key_of("bob");
    h.wait_until("all five transfers applied", || async {
        matches!(h.deed_row(&bob).await, Some(r) if r.outpoint_txid == Some(doubled.txid))
    })
    .await;
    assert!(card_rows(&h).await.is_empty(), "none of the five is a card this indexer holds");
    assert!(live_card(&h, "alice").await.is_none() && live_card(&h, "bob").await.is_none());
    assert_eq!(h.history(&bob).await.iter().filter(|e| e.op == HistoryOp::Transfer).count(), 5, "every transfer stood");
    assert!(h.selftest_now().await.proven);
    h.stop().await;
}

/// The covenant does not pin output 1, so a transfer into escrow can carry a card. Readers refuse
/// one beside a covenant-id owner, and the indexer must not serve what a reader refuses.
#[tokio::test]
async fn a_transfer_into_escrow_retires_the_card_and_mints_none() {
    let (h, mut w) = standard_boot("card_escrow", fast_args()).await;
    let (s, a) = w.register("alice", &OWNER_A);
    let minted = w.transfer_with_card("alice", &OWNER_A, &records("first", true), &OWNER_A);
    h.sim.add_block(vec![s, a, minted]);
    h.wait_until("the first card is live", || async { live_card(&h, "alice").await.is_some() }).await;

    let alice = dotk_core::key_of("alice");
    let escrow = [0x42u8; 32];
    let mut into_escrow = w.transfer_to("alice", dotk_core::OwnerType::CovenantId, &escrow);
    let blob = cards::encode_records(&records("escrowed", true)).unwrap();
    let state = cards::CardState::new(alice, cards::records_of(&blob), dotk_core::OwnerType::Pubkey, OWNER_A).unwrap();
    into_escrow.outputs.push(sim::SimOut { value: CARD_VALUE, spk: state.spk(), covenant_id: None });
    into_escrow.payload = cards::encode_payload(Some(&cards::CardMint { state, blob })).unwrap();
    h.sim.add_block(vec![into_escrow.clone()]);
    h.wait_until("the transfer into escrow applied", || async {
        matches!(h.deed_row(&alice).await, Some(r) if r.outpoint_txid == Some(into_escrow.txid))
    })
    .await;
    assert!(live_card(&h, "alice").await.is_none(), "a name in escrow serves no records");
    assert_eq!(card_rows(&h).await.len(), 1, "the first card stays a row its spender can reclaim, and the planted one is none");
    assert!(h.selftest_now().await.proven);

    // Back to a key, a transfer mints as before.
    let back = w.transfer_with_card("alice", &OWNER_B, &records("again", true), &OWNER_B);
    h.sim.add_block(vec![back.clone()]);
    h.wait_until("the card after escrow is live", || async { live_card(&h, "alice").await.is_some() }).await;
    h.stop().await;
}

#[tokio::test]
async fn a_snapshot_carries_the_cards_and_their_journal_marks() {
    let (h, mut w) = standard_boot("card_snapshot", fast_args()).await;
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    h.sim.add_block(vec![w.transfer_with_card("alice", &OWNER_A, &records("u1", false), &OWNER_A)]);
    h.sim.add_block(vec![w.transfer_with_card("alice", &OWNER_A, &records("u2", true), &OWNER_A)]);
    h.wait_until("two rows", || async { card_rows(&h).await.len() == 2 }).await;

    let export = snapshot::export(&h.app.backends.db, REGISTRY_COVENANT_ID, 1_000, std::time::Duration::ZERO).await.unwrap();
    assert_eq!(export.cards.len(), 1, "only the unswept card travels");
    let marks: Vec<_> = export.events.iter().flat_map(|e| e.prev_cards.iter()).collect();
    assert_eq!(marks.len(), 3, "two mints and one sweep are journaled");
    assert_eq!(marks.iter().filter(|m| !m.existed).count(), 2);

    let pool = fresh_pool("card_snapshot_import").await;
    db::migrate(&pool).await.unwrap();
    snapshot::import(&pool, &export, &genesis_file()).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();
    let live = db::live_card_by_key(&mut conn, &dotk_core::key_of("alice")).await.unwrap().expect("the imported card");
    assert_eq!(cards::decode_records(&live.card.blob).unwrap(), records("u2", true));

    let mut forged = export.clone();
    forged.cards[0].blob = faster_hex::hex_string(b"not the blob");
    let pool = fresh_pool("card_snapshot_forged").await;
    db::migrate(&pool).await.unwrap();
    let err = snapshot::import(&pool, &forged, &genesis_file()).await.err().unwrap();
    assert!(format!("{err:#}").contains("does not hash"), "{err:#}");

    let mut misplaced = export.clone();
    misplaced.cards[0].idx = 2;
    let pool = fresh_pool("card_snapshot_misplaced").await;
    db::migrate(&pool).await.unwrap();
    let err = snapshot::import(&pool, &misplaced, &genesis_file()).await.err().unwrap();
    assert!(format!("{err:#}").contains("output 1"), "{err:#}");
    h.stop().await;
}

#[tokio::test]
async fn no_first_verdict_without_a_card_probe() {
    let (h, mut w) = standard_boot("card_first_verdict", fast_args()).await;
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    h.sim.add_block(vec![w.transfer_with_card("alice", &OWNER_B, &records("https://alice.example", false), &OWNER_B)]);
    h.wait_until("a card row", || async { !card_rows(&h).await.is_empty() }).await;
    h.app.verdict.health.write().await.selftest = None;

    h.sim.clear_queried();
    h.sim.fail_utxos_after(0);
    assert!(dotk_indexer::audit::run_once(&h.app).await.is_none());
    assert!(!h.app.verdict.judged().await);
    assert_eq!(h.sim.utxo_calls(), 1, "the card probe goes first, and the registry pass is not run");
    h.sim.heal_utxos();
    assert!(h.selftest_now().await.proven);
    assert!(h.app.verdict.judged().await);
    h.stop().await;
}

/// A node whose UTXO index answers nothing makes every row look refuted and every card swept.
#[tokio::test]
async fn an_empty_answer_repairs_nothing() {
    let (h, mut w) = standard_boot("card_empty_answer", fast_args()).await;
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    h.sim.add_block(vec![w.transfer_with_card("alice", &OWNER_B, &records("https://alice.example", false), &OWNER_B)]);
    h.wait_until("a card row", || async { !card_rows(&h).await.is_empty() }).await;
    let unswept = || async { card_rows(&h).await.iter().all(|c| c.swept_at.is_none()) };
    assert!(h.selftest_now().await.proven);

    h.sim.set_answer_empty(true);
    let report = h.selftest_now().await;
    let r = &report.repaired;
    assert_eq!((r.cards_swept, r.demoted, r.dropped), (0, 0, 0), "{report:?}");
    assert!(unswept().await);

    h.app.verdict.health.write().await.selftest = None;
    assert!(dotk_indexer::audit::run_once(&h.app).await.is_none(), "no first verdict on an unusable card probe");
    assert!(unswept().await);

    h.sim.set_answer_empty(false);
    assert!(h.selftest_now().await.proven);
    assert!(unswept().await);
    h.stop().await;
}

#[tokio::test]
async fn a_spenders_cards_come_a_page_at_a_time() {
    let (h, mut w) = standard_boot("card_pages", fast_args()).await;
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    for i in 0..5 {
        h.sim.add_block(vec![w.transfer_minting_for("alice", &OWNER_A, &records(&format!("u{i}"), false), &OWNER_B)]);
    }
    h.wait_until("five cards", || async { spender_cards(&h, &OWNER_B).await.len() == 5 }).await;
    let all: Vec<String> =
        spender_cards(&h, &OWNER_B).await.iter().map(|c| format!("{}:{}", hex32(&c.card.txid), c.card.idx)).collect();

    let base = format!("/v1/spenders/0/{}/cards", hex32(&OWNER_B));
    let (mut seen, mut uri, mut pages) = (Vec::new(), format!("{base}?limit=2"), 0);
    loop {
        let (status, body) = get_json(&h, &uri).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        pages += 1;
        for c in body["cards"].as_array().unwrap() {
            seen.push(format!("{}:{}", c["outpointTxid"].as_str().unwrap(), c["outpointIndex"]));
        }
        match body["next"].as_str() {
            Some(next) => uri = format!("{base}?limit=2&after={next}"),
            None => break,
        }
    }
    assert_eq!((pages, seen), (3, all));

    for bad in ["limit=0", "limit=101"] {
        let (status, body) = get_json(&h, &format!("{base}?{bad}")).await;
        assert_eq!((status, body["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_query")), "{bad}: {body}");
    }
    h.stop().await;
}

/// The probe reads the node's sink, which is ahead of the pipeline.
#[tokio::test]
async fn a_sweep_the_probe_marked_first_still_journals() {
    let mut args = fast_args();
    args.vcp_tip_distance = 2;
    let (h, mut w) = standard_boot("card_probe_first", args).await;
    let alice = dotk_core::key_of("alice");
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    h.sim.add_block(vec![w.transfer_with_card("alice", &OWNER_A, &records("u1", false), &OWNER_A)]);
    h.sim.pad(3);
    h.wait_until("a card row", || async { !card_rows(&h).await.is_empty() }).await;
    assert!(h.selftest_now().await.proven);

    h.sim.add_block(vec![w.sweep_cards(&OWNER_A)]);
    let report = h.selftest_now().await;
    assert_eq!(report.repaired.cards_swept, 1, "{report:?}");
    assert!(card_rows(&h).await[0].swept_at.is_some(), "the probe's mark");

    h.sim.pad(3);
    h.wait_until("the sweep in history", || async { h.history(&alice).await.iter().any(|e| e.op == HistoryOp::Sweep) }).await;
    assert!(card_rows(&h).await[0].swept_at.is_some());
    let mut conn = h.app.backends.db.acquire().await.unwrap();
    let journaled: i64 =
        sqlx::query_scalar("SELECT count(*) FROM events WHERE cardinality(prev_cards) > 0").fetch_one(&mut *conn).await.unwrap();
    assert!(journaled >= 2, "the mint and the sweep both journal their card mark");
    h.stop().await;
}

/// The outpoint and the address prove the card, so a refuted value is rewritten, not withheld.
#[tokio::test]
async fn a_card_with_a_refuted_value_is_rewritten() {
    let (h, mut w) = standard_boot("card_value", fast_args()).await;
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    h.sim.add_block(vec![w.transfer_with_card("alice", &OWNER_A, &records("u1", false), &OWNER_A)]);
    h.wait_until("a card row", || async { !card_rows(&h).await.is_empty() }).await;
    assert!(h.selftest_now().await.proven);

    let card = card_rows(&h).await[0].clone();
    let mut conn = h.app.backends.db.acquire().await.unwrap();
    sqlx::query("UPDATE cards SET value = value + 1 WHERE txid = $1 AND idx = $2")
        .bind(card.txid.as_slice())
        .bind(i32::try_from(card.idx).unwrap())
        .execute(&mut *conn)
        .await
        .unwrap();
    let report = h.selftest_now().await;
    assert_eq!(report.repaired.cards_swept, 0, "{report:?}");
    let after = card_rows(&h).await[0].clone();
    assert_eq!((after.value, after.swept_at), (card.value, None), "the chain's value, and the card stays live");
    h.stop().await;
}
