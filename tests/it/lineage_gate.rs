use crate::common::sim::{SimOut, SimTx};
use crate::common::*;
use dotk_core::cards::{CARD_VALUE, RecordValue, Records};
use dotk_core::watch::push_data;
use dotk_indexer::chain::ChainBlock;
use dotk_indexer::chain::apply_block;
use dotk_indexer::db;
use dotk_indexer::model::RowKind;

/// A fee-level spoof that holds only the devfund fingerprint.
fn spoof_script(h: &Harness) -> Vec<u8> {
    push_data(&h.app.deployment.watch.fingerprint)
}

async fn dry_apply(h: &Harness, txs: Vec<SimTx>) -> dotk_indexer::chain::ApplyOutcome {
    let block =
        ChainBlock { hash: [0xEE; 32], blue_score: 999, daa_score: 999, timestamp: 1, txs: txs.iter().map(SimTx::accepted).collect() };
    let mut tx = h.app.backends.db.begin().await.unwrap();
    let out =
        apply_block(&mut tx, &h.app.deployment.watch, &h.app.deployment.genesis.params, REGISTRY_COVENANT_ID, &block).await.unwrap();
    tx.rollback().await.unwrap();
    out
}

/// The txid does not cover signature scripts, so a miner can put `OP_1NEGATE` into any transfer
/// or release of a curve-owned deed.
#[tokio::test]
async fn a_transfer_and_a_release_with_witness_minus_one_are_indexed() {
    let (h, mut w) = standard_boot("witness_minus_one", fast_args()).await;
    let key = dotk_core::key_of("alice");
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    h.wait_until("ACTIVE row", || async { matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Active) }).await;

    let transfer = w.transfer_with_witness("alice", &OWNER_B, -1);
    assert!(transfer.sig_scripts[0].contains(&0x4f), "the witness rides as OP_1NEGATE");
    h.sim.add_block(vec![transfer]);
    h.wait_until("transferred owner", || async { matches!(h.deed_row(&key).await, Some(r) if r.owner == Some(OWNER_B)) }).await;

    h.sim.add_block(vec![w.release_with_witness("alice", -1)]);
    h.wait_until("released row gone", || async { h.deed_row(&key).await.is_none() }).await;
    assert_eq!(h.gap_rows().await, vec![WHOLE_KEYSPACE]);
    assert!(h.selftest_now().await.proven);
    h.stop().await;
}

/// The covenant binding is the gate. The fingerprint only tells an operation from the genesis mint.
#[tokio::test]
async fn a_bound_transaction_the_decoder_cannot_read_is_reported_not_dropped() {
    let (h, _w) = standard_boot("undecodable_bound", fast_args()).await;
    let spoof = spoof_script(&h);
    let bound = |covenant_id: Option<&str>| SimTx {
        txid: [0xAB; 32],
        sig_scripts: vec![spoof.clone()],
        inputs: vec![([0xAC; 32], 0, 0)],
        outputs: vec![SimOut { value: 1, spk: vec![0x51], covenant_id: covenant_id.map(str::to_string) }],
        payload: vec![],
    };

    let out = dry_apply(&h, vec![bound(Some(REGISTRY_COVENANT_ID))]).await;
    assert_eq!(out.events, 0, "nothing decoded, so nothing is written");
    assert!(out.needs_selftest, "a registry spend that does not decode is an inconsistency");

    let out = dry_apply(&h, vec![bound(None)]).await;
    assert!(!out.needs_selftest, "unbound, the same scripts are a fee-level spoof");
    let out = dry_apply(&h, vec![bound(Some("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"))]).await;
    assert!(!out.needs_selftest, "bound elsewhere, they are another registry's business");

    // The genesis mint is bound but redeems no registry input. A checkpoint before it replays it.
    let mint = SimTx {
        txid: [0xAD; 32],
        sig_scripts: vec![vec![0x51]],
        inputs: vec![([0xAE; 32], 0, 0)],
        outputs: vec![SimOut { value: 1, spk: vec![0x51], covenant_id: Some(REGISTRY_COVENANT_ID.into()) }],
        payload: vec![],
    };
    let out = dry_apply(&h, vec![mint]).await;
    assert_eq!((out.events, out.needs_selftest), (0, false), "the mint is not an operation and not an inconsistency");
    h.stop().await;
}

#[tokio::test]
async fn a_sweep_beside_a_fingerprint_mention_is_still_marked() {
    let (h, mut w) = standard_boot("spoofed_sweep", fast_args()).await;
    let key = dotk_core::key_of("alice");
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    let mut records = Records::new();
    records.insert("url".into(), RecordValue::Text("https://example.com".into()));
    h.sim.add_block(vec![w.transfer_with_card("alice", &OWNER_B, &records, &OWNER_B)]);
    h.wait_until("a card row", || async {
        let mut conn = h.app.backends.db.acquire().await.unwrap();
        !db::all_cards(&mut conn).await.unwrap().is_empty()
    })
    .await;
    assert_eq!(h.deed_row(&key).await.unwrap().owner, Some(OWNER_B));

    let mut sweep = w.sweep_cards(&OWNER_B);
    sweep.sig_scripts.insert(0, spoof_script(&h));
    sweep.inputs.insert(0, ([0xAF; 32], 0, 0));
    sweep.outputs[0].value += CARD_VALUE;
    assert_eq!(sweep.outputs[0].covenant_id, None);
    h.sim.add_block(vec![sweep]);
    h.wait_until("swept", || async {
        let mut conn = h.app.backends.db.acquire().await.unwrap();
        db::all_cards(&mut conn).await.unwrap()[0].swept_at.is_some()
    })
    .await;
    assert!(h.selftest_now().await.proven);
    h.stop().await;
}
