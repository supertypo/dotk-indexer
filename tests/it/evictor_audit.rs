use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::common::sim::{SimKaspad, SimTx};
use crate::common::*;
use dotk_core::registry::{KEY_MAX, KEY_MIN};
use dotk_core::state::{GapState, OwnerType};
use dotk_indexer::audit::{Outcome, run_once, run_pass};
use dotk_indexer::chain::Kaspad;
use dotk_indexer::convert::hex32;
use dotk_indexer::model::{DeedRow, RowKind};
use kaspa_addresses::Prefix;

fn evictor_args() -> dotk_indexer::config::CliArgs {
    let mut args = fast_args();
    // An evict has no covenant-seat signature, so only the funding input's SIGHASH_ALL commits to the payout.
    args.evictor_key = Some(PAYOUT_KEY.into());
    args
}

fn squat_blocks(w: &mut World) -> (Vec<Vec<SimTx>>, [u8; 32]) {
    let (s, a) = w.register("alice", &OWNER_A);
    let squat = name_above(&dotk_core::key_of("alice"), "squat");
    let sq = w.split(&squat, &OWNER_B);
    (vec![vec![s, a], vec![sq]], dotk_core::key_of(&squat))
}

/// Activation fees land in the devfund too, so an eviction shows only as a delta.
async fn devfund_amounts(h: &Harness) -> Vec<u64> {
    let mut v: Vec<u64> = h.sim.utxos_by_addresses(&[devfund_address()]).await.unwrap().into_iter().map(|u| u.amount).collect();
    v.sort_unstable();
    v
}

#[tokio::test]
async fn evict_collects_the_bounty() {
    let (h, mut w) = standard_boot("evict", evictor_args()).await;
    let (blocks, key) = squat_blocks(&mut w);
    for b in blocks {
        h.sim.add_block(b);
    }
    h.wait_until("squat tracked", || async { matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Pending) }).await;
    let devfund_before = devfund_amounts(&h).await;

    h.sim.advance_daa(30);
    h.wait_until("evict submitted", || async { h.sim.submitted_count() >= 1 }).await;
    h.sim.add_block(vec![]);
    h.wait_until("squat row deleted by the evict event", || async { h.deed_row(&key).await.is_none() }).await;

    let hits = h.sim.utxos_by_addresses(&[payout_address()]).await.unwrap();
    let t = templates();
    assert_eq!(hits.len(), 1, "one payout output: the bounty and the funding change are the same output");
    let bounty = hits[0].amount - PAYOUT_SEED_VALUE;
    assert!(bounty > t.params.bond, "the bounty exceeds the name bond (the merge frees a gap too)");
    assert!(bounty < t.params.bond + t.params.gap_value, "minus a real network fee");
    // An eviction that drops the deposit passes the checks above, so the test finds it at the devfund.
    let mut devfund_after = devfund_amounts(&h).await;
    for amount in &devfund_before {
        let at = devfund_after.iter().position(|a| a == amount).expect("a devfund utxo disappeared");
        devfund_after.remove(at);
    }
    assert_eq!(devfund_after, vec![t.params.deposit], "the eviction added exactly the deposit to the devfund");

    let evicted = h.sim.submitted_sig_scripts();
    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0].len(), 4, "three covenant seats plus the funding input");
    assert!(!evicted[0][3].is_empty(), "the funding input carries a signature");
    assert!(h.next_verdict().await.proven);
    h.stop().await;
}

/// A proven PENDING squat, and an evictor wallet rich enough to pay a fee above its bounty.
async fn outlay_boot() -> (Harness, World, SimTx, [u8; 32]) {
    let pool = fresh_pool("evict_outlay").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let (mut w, genesis_tx) = World::new();
    // Rich enough to pay a fee above the bounty, so assembly succeeds and only the outlay check can refuse.
    let mut seed = payout_seed_tx();
    seed.outputs[0].value = 2 * dotk_core::MAX_FEE_SOMPI;
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx, seed.clone()]));
    let start = sim.dag_info().await.unwrap().virtual_parent;
    let h = Harness::boot(pool, sim, evictor_args(), start);
    let (blocks, key) = squat_blocks(&mut w);
    for b in blocks {
        h.sim.add_block(b);
    }
    h.wait_until("squat tracked", || async { matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Pending) }).await;
    assert!(h.wait_selftest().await.proven);
    (h, w, seed, key)
}

/// The bounty of the exit of `key`, and the fee that exit pays at a feerate when `seed` funds it.
fn exit_pricing(h: &Harness, w: &World, key: &[u8; 32], seed: &SimTx) -> (u64, impl Fn(f64) -> u64 + use<>) {
    use dotk_core::{FundingUtxo, Outpoint};
    let (pred, succ) = w.flanks(key);
    let intent = h.app.deployment.watch.build_evict(&pred, w.deed(key), &succ, &h.app.deployment.genesis.params).unwrap();
    let spk = faster_hex::hex_string(kaspa_txscript::pay_to_address_script(&payout_address()).script());
    let funding = FundingUtxo {
        outpoint: Outpoint { transaction_id: faster_hex::hex_string(&seed.txid), index: 0 },
        value: seed.outputs[0].value,
        spk: spk.clone(),
    };
    let network = h.app.deployment.args.network.clone();
    let bounty = intent.released;
    let fee_at = move |feerate: f64| {
        let asm = dotk_core::fees::assemble_with_auto_fee(&intent, std::slice::from_ref(&funding), &spk, &network, feerate).unwrap();
        asm.entries.iter().map(|e| e.amount).sum::<u64>() - asm.tx.outputs.iter().map(|o| o.value).sum::<u64>()
    };
    (bounty, fee_at)
}

/// `MAX_FEE_SOMPI` bounds one transaction and a tick builds one exit per wallet input, so only the
/// outlay check keeps a fee under its bounty.
#[tokio::test]
#[expect(clippy::cast_precision_loss, reason = "sompi amounts stay far below 2^53")]
async fn an_exit_that_costs_more_than_its_bounty_is_not_submitted() {
    use dotk_core::MAX_FEE_SOMPI;
    let (h, w, seed, key) = outlay_boot().await;
    let (bounty, fee_at) = exit_pricing(&h, &w, &key, &seed);
    // Far above the relay floor the fee is proportional to the feerate.
    let per_feerate = fee_at(1000.0) as f64 / 1000.0;
    let (over, under) = ((bounty + MAX_FEE_SOMPI) as f64 / 2.0 / per_feerate, bounty as f64 * 0.98 / per_feerate);
    let (fee_over, fee_under) = (fee_at(over), fee_at(under));
    assert!(
        bounty < fee_over && fee_over < MAX_FEE_SOMPI,
        "a fee of {fee_over} must sit between the bounty {bounty} and the {MAX_FEE_SOMPI} ceiling, or this test proves nothing"
    );
    assert!(bounty * 95 / 100 < fee_under && fee_under < bounty, "a fee of {fee_under} must sit just under the bounty {bounty}");

    h.sim.set_feerate(over);
    h.sim.clear_queried();
    h.sim.advance_daa(30);
    let deed_addr = pending_deed_address(&h, &key).await;
    h.wait_until("the evictor reached the squat", || async { h.sim.was_queried(&deed_addr) }).await;
    let probes = h.sim.utxo_calls();
    h.wait_until("ten more probes", || async { h.sim.utxo_calls() >= probes + 10 }).await;
    assert_eq!(h.sim.submitted_count(), 0, "an exit paying {fee_over} to free {bounty} must not be submitted");
    let wallet = h.sim.utxos_by_addresses(&[payout_address()]).await.unwrap();
    assert!(wallet.len() == 1 && wallet[0].txid == seed.txid, "the wallet input is not consumed");

    h.sim.set_feerate(under);
    h.wait_until("the evict submitted once it pays", || async { h.sim.submitted_count() >= 1 }).await;
    h.sim.add_block(vec![]);
    h.wait_until("squat row deleted by the evict event", || async { h.deed_row(&key).await.is_none() }).await;
    let wallet = h.sim.utxos_by_addresses(&[payout_address()]).await.unwrap();
    assert_eq!(wallet.len(), 1, "the bounty and the funding change are the same output");
    assert_eq!(wallet[0].amount, seed.outputs[0].value + bounty - fee_under, "the kept input paid the fee this feerate prices");
    h.stop().await;
}

/// The wallet holds one input and the candidate order is fixed, so a target that takes the input
/// without acting will starve the squat behind it on every tick.
#[tokio::test]
async fn a_target_that_declines_at_the_probe_does_not_consume_the_wallet() {
    let (h, mut w) = standard_boot("evict_probe_decline", evictor_args()).await;
    let (blocks, squat_key) = squat_blocks(&mut w);
    for b in blocks {
        h.sim.add_block(b);
    }
    h.wait_until("squat tracked", || async { h.deed_row(&squat_key).await.is_some() }).await;
    assert!(h.wait_selftest().await.proven);

    // A ripe PENDING row at the lowest key that the chain never held, so the probe declines it first on every tick.
    let phantom = [0x00u8; 32];
    {
        let mut conn = h.app.backends.db.acquire().await.unwrap();
        let row = DeedRow::pending([0x99; 32], ([0xEE; 32], 0), 120_000_000, 1);
        dotk_indexer::db::upsert_deed(&mut conn, &phantom, &row).await.unwrap();
    }
    h.sim.advance_daa(30);

    h.wait_until("the squat is evicted despite the phantom ahead of it", || async { h.sim.submitted_count() >= 1 }).await;
    let state = dotk_core::state::DeedState::pending(phantom, [0x99; 32]);
    let probed = h.app.deployment.deed_address(&state).unwrap();
    assert!(h.sim.was_queried(&probed), "the phantom was reached and probed");
    h.stop().await;
}

#[tokio::test]
async fn a_clock_set_too_early_does_not_starve_a_ripe_squat() {
    let (h, mut w) = standard_boot("evict_early_clock", evictor_args()).await;
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    let lo_name = name_above(&dotk_core::key_of("alice"), "first");
    let mid = name_above(&dotk_core::key_of(&lo_name), "mid");
    let hi_name = name_above(&dotk_core::key_of(&mid), "second");
    let (lo, hi) = (dotk_core::key_of(&lo_name), dotk_core::key_of(&hi_name));
    let (ms, ma) = w.register(&mid, &OWNER_A);
    h.sim.add_block(vec![ms, ma, w.split(&hi_name, &OWNER_B)]);
    h.wait_until("the older squat tracked", || async { h.deed_row(&hi).await.is_some() }).await;
    assert!(h.wait_selftest().await.proven);
    h.sim.advance_daa(15);
    h.sim.add_block(vec![w.split(&lo_name, &OWNER_B)]);
    h.wait_until("the younger squat tracked", || async { h.deed_row(&lo).await.is_some() }).await;
    sqlx::query("UPDATE deeds SET accepted_daa = 1 WHERE key = $1").bind(lo.as_slice()).execute(&h.app.backends.db).await.unwrap();

    h.sim.advance_daa(10);
    h.wait_until("the ripe squat evicted", || async { h.sim.submitted_count() >= 1 }).await;
    h.sim.add_block(vec![]);
    h.wait_until("its row deleted", || async { h.deed_row(&hi).await.is_none() }).await;
    assert!(h.deed_row(&lo).await.is_some());
    h.stop().await;
}

/// Once an exit takes the last input, every deed above it waits for the change to confirm.
#[tokio::test]
async fn an_exhausted_wallet_ends_the_tick() {
    let (h, mut w) = standard_boot("evict_wallet_bound", evictor_args()).await;
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    // A live name between the squats keeps the upper squat's flanks out of the lower one's evict.
    let lo_name = name_above(&dotk_core::key_of("alice"), "first");
    let mid = name_above(&dotk_core::key_of(&lo_name), "mid");
    let hi_name = name_above(&dotk_core::key_of(&mid), "second");
    let (lo, hi) = (dotk_core::key_of(&lo_name), dotk_core::key_of(&hi_name));
    let (ms, ma) = w.register(&mid, &OWNER_A);
    let (f, sec) = (w.split(&lo_name, &OWNER_B), w.split(&hi_name, &OWNER_B));
    h.sim.add_block(vec![ms, ma, f, sec]);
    h.wait_until("both squats tracked", || async { h.deed_row(&hi).await.is_some() && h.deed_row(&lo).await.is_some() }).await;
    // The startup pass probes every derived address, so the window opens after it.
    assert!(h.wait_selftest().await.proven);
    h.sim.clear_queried();
    h.sim.advance_daa(30);

    // The mempool holds the change, so the wallet stays empty on every later tick.
    h.wait_until("the lower squat's evict submitted", || async { h.sim.submitted_count() >= 1 }).await;
    let probes = h.sim.utxo_calls();
    h.wait_until("ten more probes", || async { h.sim.utxo_calls() >= probes + 10 }).await;

    let (lo_addr, hi_addr) = (pending_deed_address(&h, &lo).await, pending_deed_address(&h, &hi).await);
    assert!(h.sim.was_queried(&lo_addr), "the lower squat takes the only input");
    assert!(!h.sim.was_queried(&hi_addr), "and the tick stops there, so the deed above it is never probed");
    h.stop().await;
}

async fn pending_deed_address(h: &Harness, key: &[u8; 32]) -> kaspa_addresses::Address {
    let claim = h.deed_row(key).await.and_then(|r| r.claim).expect("a PENDING row with a claim");
    h.app.deployment.deed_address(&dotk_core::state::DeedState::pending(*key, claim)).unwrap()
}

/// A `p2sh/v1` owner that is the script hash of a flanking gap approves every exit merge, because each one carries that gap.
/// A stranger can end such a deed, and its owner can never move it.
#[tokio::test]
async fn the_second_scan_releases_a_deed_owned_by_its_own_neighbor() {
    let (h, mut w) = standard_boot("evict_stranger", evictor_args()).await;
    let alice = dotk_core::key_of("alice");
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice active", || async { matches!(h.deed_row(&alice).await, Some(r) if r.kind == RowKind::Active) }).await;
    assert_eq!(h.sim.submitted_count(), 0, "a healthy registration is not the evictor's business");

    let owner = gap_script_hash(&w.flanks(&alice).0.state);
    let tr = w.transfer_to_script("alice", &owner);
    h.sim.add_block(vec![tr]);
    h.wait_until("script-owned", || async {
        matches!(h.deed_row(&alice).await, Some(r) if r.owner_type == Some(OwnerType::ScriptHash as u8))
    })
    .await;
    let devfund_before = devfund_amounts(&h).await;

    h.wait_until("stranger release submitted", || async { h.sim.submitted_count() >= 1 }).await;
    h.sim.add_block(vec![]);
    h.wait_until("row deleted by the release event", || async { h.deed_row(&alice).await.is_none() }).await;

    let released = h.sim.submitted_sig_scripts();
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].len(), 4, "three covenant seats plus the funding input");
    assert!(!released[0][3].is_empty(), "the funding input carries the signature that commits to the payout");
    assert_eq!(devfund_amounts(&h).await, devfund_before, "a release pays the devfund nothing: the deposit went back at activation");

    let t = templates();
    let hits = h.sim.utxos_by_addresses(&[payout_address()]).await.unwrap();
    assert_eq!(hits.len(), 1, "the bounty and the funding change are the same output");
    let bounty = hits[0].amount - PAYOUT_SEED_VALUE;
    assert!(bounty > t.params.bond && bounty < t.params.bond + t.params.gap_value, "bond plus gap value less the fee, got {bounty}");
    assert!(h.next_verdict().await.proven, "the registry is left exactly as any other exit leaves it");
    h.stop().await;
}

/// An unfunded evictor that reaches the second scan still submits nothing, so the gate is measured here and not
/// through its effects.
#[tokio::test]
async fn only_a_funded_evictor_scans_for_deeds_a_stranger_can_end() {
    use dotk_indexer::evictor::{Exit, scan_targets};
    let pool = fresh_pool("scan_targets").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();
    let watch = genesis_file().watch_templates().unwrap();
    let k = |b: u8| [b; 32];
    let hash_of = |lo, hi| dotk_core::blake2b(&templates().gap.materialize(&GapState { lo, hi }.encode()).unwrap());
    // A deed at a keyspace bound has the flanks that a scan blind to real neighbors also derives, so every deed sits inside.
    // The exposed deeds sort below the squat, so only a merged order of the two scans passes.
    let rows = [
        (k(1), DeedRow::active("neighbor-below".into(), 0, k(0xb1), (k(0xa1), 0), 20_000_000)),
        (k(2), DeedRow::active("stuck-by-pred".into(), 3, hash_of(k(1), k(2)), (k(0xa2), 0), 20_000_000)),
        (k(3), DeedRow::active("healthy".into(), 3, k(0x5c), (k(0xa3), 0), 20_000_000)),
        (k(4), DeedRow::active("stuck-by-succ".into(), 3, hash_of(k(4), k(5)), (k(0xa4), 0), 20_000_000)),
        (k(5), DeedRow::pending(k(0x55), (k(0xa5), 2), 120_000_000, 100)),
    ];
    for (key, row) in &rows {
        dotk_indexer::db::upsert_deed(&mut conn, key, row).await.unwrap();
    }

    assert_eq!(
        scan_targets(&mut conn, Some(&watch), 200).await.unwrap(),
        vec![(k(2), Exit::StrangerRelease), (k(4), Exit::StrangerRelease), (k(5), Exit::Evict)],
        "both scans in ONE ascending order; both flank directions found; and NOT the healthy \
         script-owned name at k(3), which is a candidate the verdict rejects"
    );
    assert_eq!(
        scan_targets(&mut conn, None, 200).await.unwrap(),
        vec![(k(5), Exit::Evict)],
        "an unfunded evictor never even looks at the second scan's candidates"
    );
}

/// `hidden` splits before boot and stays unseen, so the registry never proves. The squat's own
/// flanks are real, and an exit proves its deed and both flanks on chain, so the evictor runs.
#[tokio::test]
async fn evictor_runs_on_a_failing_verdict() {
    let hidden = "hidden-low";
    let hidden_key = dotk_core::key_of(hidden);
    let mid = name_above(&hidden_key, "mid");
    let squat = name_above(&dotk_core::key_of(&mid), "top");
    let squat_key = dotk_core::key_of(&squat);

    let pool = fresh_pool("evict_unvalidated").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let (mut w, genesis_tx) = World::new();
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx, payout_seed_tx()]));
    let sh = w.split(hidden, &OWNER_B);
    sim.add_block(vec![sh]);
    let sm = w.split(&mid, &OWNER_B);
    sim.add_block(vec![sm]);
    let start = sim.dag_info().await.unwrap().virtual_parent;
    let h = Harness::boot(pool, sim, evictor_args(), start);

    let ss = w.split(&squat, &OWNER_B);
    h.sim.add_block(vec![ss]);
    h.wait_until("squat tracked", || async { matches!(h.deed_row(&squat_key).await, Some(r) if r.kind == RowKind::Pending) }).await;
    h.sim.advance_daa(30);

    let report = h.wait_selftest().await;
    assert!(!report.proven, "the never-observed split leaves a blind spot: {report:?}");
    h.wait_until("evict submitted on the chain's own evidence", || async { h.sim.submitted_count() >= 1 }).await;
    h.stop().await;
}

/// An owner who re-transfers a name faster than the pipeline follows keeps its key failing on
/// every attempt. The journal shows the key in flux, so it is withheld and no verdict fails.
#[tokio::test]
async fn a_name_in_flux_is_withheld_and_fails_no_verdict() {
    let mut args = fast_args();
    args.selftest_probe_chunk = 1;
    let (h, mut w) = standard_boot("flux_withheld", args).await;
    let alice = dotk_core::key_of("alice");
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice", || async { h.deed_row(&alice).await.is_some() }).await;
    assert!(h.selftest_now().await.proven);

    // Twelve distinct owners, so any lag of the table behind the chain moves the deed address.
    let queue: Vec<_> = (1..=12).map(|seed| w.transfer("alice", &curve_owner(seed))).collect();
    let queue = Arc::new(Mutex::new(VecDeque::from(queue)));
    h.sim.clear_queried();
    churn(&h.sim, queue.clone(), 1);
    let report = h.selftest_now().await;
    assert!(report.proven && report.repaired.demoted == 0, "{report:?}");
    assert!(report.owner_unknown.iter().any(|[k, _]| *k == hex32(&alice)), "withheld: {report:?}");
    assert!(h.app.verdict.withheld().await.withholds_key(&alice));
    assert_ne!(h.app.verdict.followup_due_ms.load(Ordering::Relaxed), 0, "a pass is owed to prove the healed key");

    let mined = 12 - std::mem::take(&mut *queue.lock().unwrap()).len();
    h.wait_until("every transfer applied", || async { h.history(&alice).await.len() == 2 + mined }).await;
    let report = h.selftest_now().await;
    assert!(report.proven && report.owner_unknown.is_empty(), "{report:?}");
    assert_eq!(h.app.verdict.followup_due_ms.load(Ordering::Relaxed), 0, "the clean pass settles the debt");
    h.stop().await;
}

/// Mines one queued transfer after each chain call, so the chain moves under the pass.
fn churn(sim: &Arc<SimKaspad>, queue: TxQueue, call: usize) {
    sim.after_utxo_calls(call, {
        let sim = sim.clone();
        move || async move {
            let next = queue.lock().unwrap().pop_front();
            if let Some(tx) = next {
                sim.add_block(vec![tx]);
                churn(&sim, queue, call + 1);
            }
        }
    });
}

/// A different name moves during each attempt, so no attempt is clean on its own and the
/// confirmed set is empty. The pass proves, and it still publishes with the last fresh key
/// withheld, because a proof that waited for a quiet probe would go stale on a busy registry.
#[tokio::test]
async fn a_fresh_transfer_on_every_attempt_still_publishes() {
    let mut args = fast_args();
    args.selftest_probe_chunk = 1;
    args.selftest_confirm_delay = Duration::from_millis(200);
    let (h, mut w) = standard_boot("fresh_every_attempt", args).await;
    let names = ["alice", "bob", "carol"];
    let keys: Vec<[u8; 32]> = names.iter().map(|n| dotk_core::key_of(n)).collect();
    for name in names {
        let (s, a) = w.register(name, &OWNER_A);
        h.sim.add_block(vec![s, a]);
    }
    h.wait_until("every name", || async { h.deed_row(&keys[2]).await.is_some() && h.history(&keys[0]).await.len() == 2 }).await;
    assert!(h.selftest_now().await.proven);

    // The lowest gap is probed first in every attempt, and a hook on it moves the next name.
    let lowest = gap_address(&h, KEY_MIN, *keys.iter().min().unwrap());
    let queue: Vec<_> = names.iter().map(|n| w.transfer(n, &OWNER_B)).collect();
    let queue = Arc::new(Mutex::new(VecDeque::from(queue)));
    split_after(&h.sim, queue.clone(), lowest.clone());
    let report = h.selftest_now().await;
    h.sim.clear_hooks();
    assert!(report.proven && report.published, "{report:?}");
    let fresh: Vec<[u8; 32]> =
        keys[1..].iter().copied().filter(|k| report.owner_unknown.iter().any(|[r, _]| *r == hex32(k))).collect();
    assert_eq!(fresh.len(), 1, "the last attempt's fresh key is withheld: {report:?}");
    assert!(h.app.verdict.withheld().await.withholds_key(&fresh[0]));
    assert_ne!(h.app.verdict.followup_due_ms.load(Ordering::Relaxed), 0);

    let moved = 3 - std::mem::take(&mut *queue.lock().unwrap()).len();
    h.wait_until("every transfer applied", || async {
        let mut n = 0;
        for k in &keys {
            n += h.history(k).await.len().saturating_sub(2);
        }
        n == moved
    })
    .await;
    let report = h.selftest_now().await;
    assert!(report.proven && report.published && report.owner_unknown.is_empty(), "{report:?}");
    h.stop().await;
}

/// A split lands in the gap above alice during every attempt, so that gap fails each time. The
/// journal shows it rewritten, so the pass proves and publishes, and the next quiet pass is clean.
#[tokio::test]
async fn a_gap_in_flux_still_publishes_the_proof() {
    let mut args = fast_args();
    args.selftest_probe_chunk = 1;
    let (h, mut w) = standard_boot("flux_gap", args).await;
    let alice = dotk_core::key_of("alice");
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice", || async { h.deed_row(&alice).await.is_some() }).await;
    assert!(h.selftest_now().await.proven);

    // Each split lands below the last, so the gap that starts at alice is the one rewritten.
    let mut above = KEY_MAX;
    let mut splits = VecDeque::new();
    for i in 0..3 {
        let name = name_inside(&alice, &above, &format!("split-{i}"));
        above = dotk_core::key_of(&name);
        splits.push_back(w.split(&name, &OWNER_B));
    }
    // The lowest gap keeps its address across the pass, and every attempt probes it first.
    let lowest = gap_address(&h, KEY_MIN, alice);
    let splits = Arc::new(Mutex::new(splits));
    split_after(&h.sim, splits.clone(), lowest);
    let report = h.selftest_now().await;
    assert!(report.proven && report.published, "{report:?}");
    assert!(report.blind_spots.iter().any(|[lo, _]| *lo == hex32(&alice)), "the gap is reported in flux: {report:?}");
    assert_eq!(report.repaired.dropped + report.repaired.demoted + report.repaired.bridged, 0, "{report:?}");

    let mined = 3 - std::mem::take(&mut *splits.lock().unwrap()).len();
    let deeds = || async {
        let mut conn = h.app.backends.db.acquire().await.unwrap();
        dotk_indexer::db::all_deeds(&mut conn).await.unwrap().len()
    };
    h.wait_until("every split applied", || async { deeds().await == 1 + mined }).await;
    let report = h.selftest_now().await;
    assert!(report.proven && report.published && report.blind_spots.is_empty(), "{report:?}");
    h.stop().await;
}

/// The table misses two registrations. A split that lands beside the hole during the last
/// attempt rewrites the failing gap, but the key it discovers shows the hole, so no proof goes
/// out with that gap as free.
#[tokio::test]
async fn a_hole_beside_a_gap_in_flux_is_never_published() {
    let mut args = fast_args();
    args.selftest_probe_chunk = 1;
    let (h, mut w) = standard_boot("flux_hole", args).await;
    let alice = dotk_core::key_of("alice");
    let (x1_key, x2_key) = alice_and_two_above(&h, &mut w, ["hole-one", "hole-two"]).await;
    {
        let mut conn = h.app.backends.db.acquire().await.unwrap();
        for key in [&x1_key, &x2_key] {
            dotk_indexer::db::delete_deed(&mut conn, key).await.unwrap();
        }
    }

    // Two attempts probe two gaps and one deed each, so call 7 opens the third attempt, after
    // its export. The hook waits for the stream to discover the hole's lower key.
    let split = w.split(&name_inside(&alice, &x1_key, "beside"), &OWNER_B);
    h.sim.clear_queried();
    h.sim.after_utxo_calls(7, {
        let (sim, app) = (h.sim.clone(), h.app.clone());
        move || async move {
            sim.add_block(vec![split]);
            let discovered = async {
                loop {
                    let mut conn = app.backends.db.acquire().await.unwrap();
                    if dotk_indexer::db::get_deed(&mut conn, &x1_key).await.unwrap().is_some() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            };
            tokio::time::timeout(Duration::from_secs(10), discovered).await.expect("the stream found the hole's lower key");
        }
    });
    let report = h.selftest_now().await;
    assert!(h.sim.utxo_calls() >= 7, "the hook ran: {report:?}");
    if report.published {
        let proof = h.app.verdict.proof.read().await;
        let body: serde_json::Value = serde_json::from_slice(&proof.as_ref().unwrap().bodies.with_events.bytes).unwrap();
        let keys: Vec<&str> = body["deeds"].as_array().unwrap().iter().map(|d| d["key"].as_str().unwrap()).collect();
        assert!(keys.contains(&hex32(&x2_key).as_str()), "a published proof holds every registered key: {report:?}");
    }
    h.stop().await;
}

fn gap_address(h: &Harness, lo: [u8; 32], hi: [u8; 32]) -> kaspa_addresses::Address {
    h.app.deployment.gap_address(&GapState { lo, hi }).unwrap()
}

/// Registers `alice` and two names above it in one block, proves them, and returns the keys of the two.
async fn alice_and_two_above(h: &Harness, w: &mut World, salts: [&str; 2]) -> ([u8; 32], [u8; 32]) {
    let first = name_above(&dotk_core::key_of("alice"), salts[0]);
    let second = name_above(&dotk_core::key_of(&first), salts[1]);
    let keys = (dotk_core::key_of(&first), dotk_core::key_of(&second));
    let mut block = Vec::new();
    for name in ["alice", &first, &second] {
        let (s, a) = w.register(name, &OWNER_A);
        block.extend([s, a]);
    }
    h.sim.add_block(block);
    h.wait_until("all three", || async { h.deed_row(&keys.1).await.is_some() }).await;
    assert!(h.selftest_now().await.proven);
    keys
}

type TxQueue = Arc<Mutex<VecDeque<SimTx>>>;

/// Mines one queued split after each probe of `addr`, so the chain moves under every attempt.
fn split_after(sim: &Arc<SimKaspad>, queue: TxQueue, addr: kaspa_addresses::Address) {
    let key = addr.clone();
    sim.after_query_of(&key, {
        let sim = sim.clone();
        move || async move {
            let next = queue.lock().unwrap().pop_front();
            if let Some(tx) = next {
                sim.add_block(vec![tx]);
                split_after(&sim, queue, addr);
            }
        }
    });
}

/// A UTXO the pipeline has not reached is withheld and left to the stream, so the transfer's
/// own event keeps its journal entry and its history row.
#[tokio::test]
async fn a_provenance_ahead_of_the_pipeline_is_withheld_not_rewritten() {
    let mut args = fast_args();
    args.vcp_tip_distance = 2;
    let (h, mut w) = standard_boot("flux_ahead", args).await;
    let alice = dotk_core::key_of("alice");
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.sim.pad(3);
    h.wait_until("alice", || async { h.deed_row(&alice).await.is_some() }).await;
    assert!(h.selftest_now().await.proven);

    let before = h.deed_row(&alice).await.unwrap();
    let t = w.transfer("alice", &OWNER_A);
    let txid = t.txid;
    h.sim.add_block(vec![t]);
    let report = h.selftest_now().await;
    assert!(report.proven && report.repaired.rewritten == 0, "{report:?}");
    assert_eq!(report.failing_outpoints, vec![format!("outpoint:{}", hex32(&alice))], "{report:?}");
    assert_eq!(h.deed_row(&alice).await.unwrap(), before, "left to the stream");

    h.sim.pad(3);
    h.wait_until("the transfer applied", || async { h.deed_row(&alice).await.unwrap().outpoint_txid == Some(txid) }).await;
    assert_eq!(h.history(&alice).await.len(), 3);
    let report = h.selftest_now().await;
    assert!(report.proven && report.failing_outpoints.is_empty(), "{report:?}");
    h.stop().await;
}

/// A row that already holds a transfer's image, because a repair wrote it first, still owes
/// that transfer its journal entry and its history row.
#[tokio::test]
async fn an_event_whose_image_is_already_stored_is_still_journaled() {
    let (h, mut w) = standard_boot("prewritten_image", fast_args()).await;
    let alice = dotk_core::key_of("alice");
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice", || async { h.deed_row(&alice).await.is_some() }).await;

    let prev = h.deed_row(&alice).await.unwrap();
    let t = w.transfer("alice", &OWNER_A);
    {
        let mut conn = h.app.backends.db.acquire().await.unwrap();
        let image = DeedRow { outpoint_txid: Some(t.txid), outpoint_index: Some(0), ..prev };
        dotk_indexer::db::upsert_deed(&mut conn, &alice, &image).await.unwrap();
    }
    h.sim.add_block(vec![t]);
    h.wait_until("the transfer in history", || async { h.history(&alice).await.len() == 3 }).await;
    let mut conn = h.app.backends.db.acquire().await.unwrap();
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM events WHERE key = $1").bind(alice.to_vec()).fetch_one(&mut *conn).await.unwrap();
    assert_eq!(events, 3, "the journal holds the transfer");
    h.stop().await;
}

#[tokio::test]
async fn selftest_neither_runs_nor_repairs_while_behind() {
    // Behind the chain, a name mismatches the node only because the stream has not replayed it yet.
    // A pass that finds the indexer behind at any step abandons itself.
    let (h, mut w) = standard_boot("repair_behind", fast_args()).await;
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice", || async { h.deed_row(&dotk_core::key_of("alice")).await.is_some() }).await;
    let good = h.selftest_now().await;
    assert!(good.proven);

    let phantom = plant_phantom(&h).await;
    fall_behind(&h.app.progress);
    h.sim.clear_queried();
    assert!(matches!(run_pass(&h.app).await, Outcome::Behind), "abandoned, not a verdict");
    let after = h.app.verdict.health.read().await.selftest.clone().unwrap();
    assert_eq!(after.finished_ms, good.finished_ms, "the previous verdict stands, unchanged");
    assert!(h.deed_row(&phantom).await.is_some(), "and the phantom was not touched");
    assert_eq!(h.sim.utxo_calls(), 0, "the chain was not even asked");

    h.sim.pad(1);
    h.wait_until("caught up again", || async { h.app.progress.caught_up() }).await;
    let report = h.selftest_now().await;
    assert!(report.proven && report.repaired.dropped == 1, "{report:?}");
    assert!(h.deed_row(&phantom).await.is_none());
    h.stop().await;
}

#[tokio::test]
async fn a_pass_owed_while_behind_runs_once_caught_up_again() {
    let (h, mut w) = standard_boot_ambient("owed_behind", fast_args()).await;
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice", || async { h.deed_row(&dotk_core::key_of("alice")).await.is_some() }).await;
    h.wait_until("the startup pass has proven and published", || async {
        h.app.verdict.health.read().await.selftest.as_ref().is_some_and(|t| t.proven) && h.app.verdict.proof.read().await.is_some()
    })
    .await;
    let before = h.app.verdict.health.read().await.selftest.clone().unwrap();

    let phantom = plant_phantom(&h).await;
    fall_behind(&h.app.progress);
    h.app.verdict.trigger_selftest();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        h.app.verdict.health.read().await.selftest.clone().unwrap().finished_ms,
        before.finished_ms,
        "no pass ran while behind"
    );
    assert!(h.deed_row(&phantom).await.is_some());

    // The trigger is owed, so the block that catches the indexer up releases the pass.
    h.sim.pad(1);
    h.wait_until("the owed pass ran", || async {
        h.app.verdict.health.read().await.selftest.as_ref().unwrap().finished_ms > before.finished_ms
    })
    .await;
    let after = h.app.verdict.health.read().await.selftest.clone().unwrap();
    assert!(after.proven && after.repaired.dropped == 1, "{after:?}");
    assert!(h.deed_row(&phantom).await.is_none());
    h.stop().await;
}

/// An ACTIVE row that exists nowhere on-chain.
async fn plant_phantom(h: &Harness) -> [u8; 32] {
    let phantom = dotk_core::key_of("phantom-name");
    let mut conn = h.app.backends.db.acquire().await.unwrap();
    let row = DeedRow::active("phantom-name".into(), 0, [0xEE; 32], ([0xEE; 32], 0), test_params().bond);
    dotk_indexer::db::upsert_deed(&mut conn, &phantom, &row).await.unwrap();
    phantom
}

type Tables = (Vec<([u8; 32], DeedRow)>, Vec<([u8; 32], dotk_indexer::model::GapRow)>);

async fn tables(backends: &dotk_indexer::app::Backends) -> Tables {
    let mut conn = backends.db.acquire().await.unwrap();
    (dotk_indexer::db::all_deeds(&mut conn).await.unwrap(), dotk_indexer::db::all_gaps(&mut conn).await.unwrap())
}

/// The indexer falls behind after each chain call of a repairing pass in turn. The pass must stop
/// at once, with no write, no verdict and at most one more chain call.
#[tokio::test]
async fn a_pass_that_falls_behind_midway_writes_nothing() {
    abandons_midway("repair_behind_midway", |h| fall_behind(&h.app.progress), |o| matches!(o, Outcome::Behind)).await;
}

/// The node falls behind the pipeline's last commit after each chain call in turn.
#[tokio::test]
async fn a_pass_whose_node_falls_behind_midway_writes_nothing() {
    abandons_midway("repair_fallback_midway", |h| h.sim.set_probe_sink_lag(1), |o| matches!(o, Outcome::Errored)).await;
}

/// With a tip distance of 2, a sink one behind is still at the pipeline's last commit, so only
/// the node's own earlier answer refutes it, and the pass starts over on another pin.
#[tokio::test]
async fn a_node_whose_sink_falls_back_midway_is_replaced() {
    let mut args = fast_args();
    args.vcp_tip_distance = 2;
    let (h, mut w) = standard_boot("repair_sink_regression", args).await;
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.sim.pad(3);
    h.wait_until("alice", || async { h.deed_row(&dotk_core::key_of("alice")).await.is_some() }).await;
    assert!(h.selftest_now().await.proven);

    let phantom = plant_phantom(&h).await;
    let pins = h.sim.pin_calls();
    let sim = h.sim.clone();
    h.sim.clear_queried();
    h.sim.after_utxo_calls(1, move || async move { sim.set_probe_sink_lag(1) });
    let report = h.selftest_now().await;
    assert!(report.proven && report.repaired.dropped == 1, "{report:?}");
    assert_eq!(h.sim.pin_calls() - pins, 2, "the first pin was discarded at its next gate");
    assert!(h.deed_row(&phantom).await.is_none());
    h.stop().await;
}

/// A node behind the pipeline's last commit answers for an older chain, so the pass abandons
/// before it probes.
#[tokio::test]
async fn a_pass_on_a_node_behind_the_pipeline_writes_nothing() {
    let (h, mut w) = standard_boot("repair_stale_node", fast_args()).await;
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice", || async { h.deed_row(&dotk_core::key_of("alice")).await.is_some() }).await;
    let good = h.selftest_now().await;
    assert!(good.proven);

    let phantom = plant_phantom(&h).await;
    h.sim.set_probe_sink_lag(1);
    h.sim.clear_queried();
    assert!(matches!(run_pass(&h.app).await, Outcome::Errored), "abandoned, not a verdict");
    assert_eq!(
        h.app.verdict.health.read().await.selftest.clone().unwrap().finished_ms,
        good.finished_ms,
        "the previous verdict stands"
    );
    assert!(h.deed_row(&phantom).await.is_some(), "and the phantom was not touched");
    assert_eq!(h.sim.utxo_calls(), 0, "the node was not even probed");

    h.sim.set_probe_sink_lag(0);
    let report = h.selftest_now().await;
    assert!(report.proven && report.repaired.dropped == 1, "{report:?}");
    assert!(h.deed_row(&phantom).await.is_none());
    h.stop().await;
}

async fn abandons_midway(name: &str, sabotage: fn(&Harness), abandoned: fn(&Outcome) -> bool) {
    let (h, mut w) = standard_boot(name, fast_args()).await;
    let h = Arc::new(h);
    let alice = dotk_core::key_of("alice");
    let (stale_key, hidden_key) = alice_and_two_above(&h, &mut w, ["stale", "hidden"]).await;

    let (alice_row, stale_row) = (h.deed_row(&alice).await.unwrap(), h.deed_row(&stale_key).await.unwrap());
    let damage = || async {
        let mut conn = h.app.backends.db.acquire().await.unwrap();
        let moved = DeedRow { outpoint_txid: Some([0xEE; 32]), ..alice_row.clone() };
        dotk_indexer::db::upsert_deed(&mut conn, &alice, &moved).await.unwrap();
        let sold = DeedRow { owner: Some(OWNER_B), ..stale_row.clone() };
        dotk_indexer::db::upsert_deed(&mut conn, &stale_key, &sold).await.unwrap();
        dotk_indexer::db::delete_deed(&mut conn, &hidden_key).await.unwrap();
    };
    damage().await;
    h.sim.clear_queried();
    let report = h.selftest_now().await;
    let r = &report.repaired;
    assert!(report.proven && (r.rewritten, r.demoted, r.readopted + r.bridged) == (1, 1, 1), "{report:?}");
    let calls = h.sim.utxo_calls();

    for n in 1..=calls {
        damage().await;
        h.caught_up_without_a_block();
        h.sim.set_probe_sink_lag(0);
        abandon_after_call(&h, n, sabotage, abandoned).await;
    }
    Arc::try_unwrap(h).ok().expect("no hook holds the harness").stop().await;
}

/// Runs a pass sabotaged after utxo call `n`, and checks that it left the verdict and the tables as they were.
async fn abandon_after_call(h: &Arc<Harness>, n: usize, sabotage: fn(&Harness), abandoned: fn(&Outcome) -> bool) {
    let before = h.app.verdict.health.read().await.selftest.clone().unwrap().finished_ms;
    let at_fall = Arc::new(tokio::sync::Mutex::new(None));
    h.sim.clear_queried();
    h.sim.after_utxo_calls(n, {
        let (h, at_fall) = (h.clone(), at_fall.clone());
        move || async move {
            sabotage(&h);
            *at_fall.lock().await = Some(tables(&h.app.backends).await);
        }
    });
    assert!(abandoned(&run_pass(&h.app).await), "sabotaged after call {n}: abandoned, not a verdict");
    assert_eq!(
        h.app.verdict.health.read().await.selftest.clone().unwrap().finished_ms,
        before,
        "call {n}: the previous verdict stands"
    );
    let at_fall = at_fall.lock().await.take().expect("the hook ran");
    assert!(tables(&h.app.backends).await == at_fall, "call {n}: the tables are as they were at the sabotage");
    assert!(h.sim.utxo_calls() <= n + 1, "call {n}: the chain was asked {} times", h.sim.utxo_calls());
}

/// Repair must read the chain from the nearest keys that are not suspect, or each bad row refutes the next and repair stalls.
#[tokio::test]
async fn selftest_repair_resolves_a_run_of_adjacent_bad_rows() {
    let (h, mut w) = standard_boot("repair_run", fast_args()).await;
    let alice = dotk_core::key_of("alice");
    let (s1, a1) = w.register("alice", &OWNER_A);
    let bob = name_above(&alice, "bob");
    let bob_key = dotk_core::key_of(&bob);
    let (s2, a2) = w.register(&bob, &OWNER_A);
    h.sim.add_block(vec![s1, a1, s2, a2]);
    h.wait_until("both live", || async { h.deed_row(&bob_key).await.is_some() }).await;
    assert!(h.selftest_now().await.proven);

    let p1 = name_inside(&alice, &bob_key, "phantom-a");
    let p2 = name_inside(&dotk_core::key_of(&p1), &bob_key, "phantom-b");
    {
        let mut conn = h.app.backends.db.acquire().await.unwrap();
        for name in [&p1, &p2] {
            let row = DeedRow::active(name.clone(), 0, [0xEE; 32], ([0xEE; 32], 0), test_params().bond);
            dotk_indexer::db::upsert_deed(&mut conn, &dotk_core::key_of(name), &row).await.unwrap();
        }
        let refuted = DeedRow::active(bob.clone(), 0, OWNER_B, ([0xEE; 32], 0), test_params().bond);
        dotk_indexer::db::upsert_deed(&mut conn, &bob_key, &refuted).await.unwrap();
    }

    let report = h.selftest_now().await;
    assert!(report.proven, "a run of adjacent bad rows must still converge: {report:?}");
    assert_eq!(report.repaired.dropped, 2, "both phantoms are refuted: {report:?}");
    assert_eq!(report.repaired.demoted, 1, "the real row between them is kept: {report:?}");
    assert!(report.owner_unknown.is_empty(), "nothing residual once demoted: {report:?}");
    assert!(h.deed_row(&dotk_core::key_of(&p1)).await.is_none() && h.deed_row(&dotk_core::key_of(&p2)).await.is_none());
    assert_eq!(h.deed_row(&bob_key).await.unwrap().kind, RowKind::OwnerUnknown);
    assert!(h.deed_row(&alice).await.unwrap().kind == RowKind::Active, "the untouched neighbor is untouched");
    h.stop().await;
}

/// The journal's pre-images splice the key back in, and the chain holds both resulting gaps.
/// The remembered owner's deed is spent, so the row returns as owner-unknown.
#[tokio::test]
async fn selftest_readopts_a_wrongly_deleted_row() {
    let (h, mut w) = standard_boot("repair_readopt", fast_args()).await;
    let alice = dotk_core::key_of("alice");
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    let tr = w.transfer("alice", &OWNER_B);
    h.sim.add_block(vec![tr]);
    h.wait_until("transferred", || async { matches!(h.deed_row(&alice).await, Some(r) if r.owner == Some(OWNER_B)) }).await;

    {
        let mut conn = h.app.backends.db.acquire().await.unwrap();
        dotk_indexer::db::delete_deed(&mut conn, &alice).await.unwrap();
    }
    let report = h.selftest_now().await;
    assert!(report.proven, "re-adoption must converge: {report:?}");
    assert!(report.repaired.readopted + report.repaired.bridged >= 1);
    let row = h.deed_row(&alice).await.expect("row is back");
    assert_eq!(row.kind, RowKind::OwnerUnknown, "the remembered owner's deed is spent, so the key re-enters as structure");
    assert_eq!(row.name.as_deref(), Some("alice"), "the name is the one fact worth keeping");

    let tr2 = w.transfer("alice", &OWNER_A);
    h.sim.add_block(vec![tr2]);
    h.wait_until("OWNER-UNKNOWN upgraded", || async {
        matches!(h.deed_row(&alice).await, Some(r) if r.kind == RowKind::Active && r.owner == Some(OWNER_A))
    })
    .await;
    assert!(h.selftest_now().await.proven);
    h.stop().await;
}

#[tokio::test]
async fn selftest_refuses_to_repair_against_a_covenant_id_blind_node() {
    // Every registry UTXO carries the covenant id. UTXOs with none mean the probe is unusable, not that
    // the chain refutes the whole registry.
    let (h, mut w) = standard_boot("probe_fault", fast_args()).await;
    let alice = dotk_core::key_of("alice");
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice active", || async { matches!(h.deed_row(&alice).await, Some(r) if r.kind == RowKind::Active) }).await;
    let good = h.selftest_now().await;
    assert!(good.proven);

    h.sim.set_report_covenant_ids(false);
    assert!(run_once(&h.app).await.is_none(), "the pass errors out rather than reaching a verdict");
    let after = h.app.verdict.health.read().await.selftest.clone().unwrap();
    assert_eq!(after.finished_ms, good.finished_ms, "so the previous verdict stands, unchanged");
    assert!(h.deed_row(&alice).await.is_some(), "and not one row was dropped");

    h.sim.set_report_covenant_ids(true);
    assert!(h.selftest_now().await.proven);
    h.stop().await;
}

#[tokio::test]
async fn cold_start_aged_heals_from_the_stream() {
    // Every gap a spend republishes names two deed keys, so a cold start from the tip rebuilds structure
    // from the stream.
    let pool = fresh_pool("cold_aged").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let (mut w, genesis_tx) = World::new();
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx]));
    // The indexer never sees this registration.
    let (s, a) = w.register("alice", &OWNER_A);
    sim.add_block(vec![s, a]);
    let start = sim.dag_info().await.unwrap().virtual_parent;
    let h = Harness::boot_ambient(pool, sim, fast_args(), start);
    h.sim.pad(1); // `caught_up` depends on the age of the last block

    let report = h.wait_selftest().await;
    assert!(!report.proven, "an aged registry cannot prove from a cold start");
    assert!(!report.blind_spots.is_empty(), "the missing structure is bracketed");

    // A split above alice republishes the gap (alice, max), so her key arrives without her name.
    let alice = dotk_core::key_of("alice");
    let probe = name_above(&alice, "heal");
    let sp = w.split(&probe, &OWNER_B);
    h.sim.add_block(vec![sp]);
    h.wait_until("alice's key bridged from the touch", || async {
        matches!(h.deed_row(&alice).await, Some(r) if r.kind == RowKind::OwnerUnknown)
    })
    .await;
    assert!(h.next_verdict().await.proven, "the partition closed: structure is whole");

    let tr = w.transfer("alice", &OWNER_B);
    h.sim.add_block(vec![tr]);
    h.wait_until("alice named by her ownership event", || async {
        matches!(h.deed_row(&alice).await, Some(r) if r.kind == RowKind::Active && r.name.as_deref() == Some("alice"))
    })
    .await;
    assert!(h.next_verdict().await.proven);
    h.stop().await;
}
