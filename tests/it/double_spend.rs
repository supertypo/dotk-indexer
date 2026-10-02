//! Op A applies and reorgs out, a conflicting op B wins, and the tables converge to B's world.

use std::collections::BTreeSet;

use crate::common::sim::SimTx;
use crate::common::*;
use dotk_core::state::Status;
use dotk_indexer::convert::hex32;

/// (key, kind, name, `owner_type`, owner, claim)
type Expected = BTreeSet<(String, u8, Option<String>, Option<u8>, Option<String>, Option<String>)>;

fn expected(w: &World) -> Expected {
    w.deeds
        .iter()
        .map(|d| match d.state.status {
            Status::Active => (
                hex32(&d.state.key),
                0u8,
                Some(w.name_of(&d.state.key)),
                Some(d.state.owner_type as u8),
                Some(faster_hex::hex_string(&d.state.owner)),
                None,
            ),
            // A PENDING deed's `owner` field is its claim, so the name stays secret.
            Status::Pending => (hex32(&d.state.key), 1u8, None, None, None, Some(faster_hex::hex_string(&d.state.owner))),
        })
        .collect()
}

async fn actual(h: &Harness) -> Expected {
    let mut conn = h.app.backends.db.acquire().await.unwrap();
    dotk_indexer::db::all_deeds(&mut conn)
        .await
        .unwrap()
        .into_iter()
        .map(|(k, r)| {
            (
                hex32(&k),
                u8::try_from(r.kind as i16).unwrap(),
                r.name,
                r.owner_type,
                r.owner.map(|o| faster_hex::hex_string(&o)),
                r.claim.map(|c| faster_hex::hex_string(&c)),
            )
        })
        .collect()
}

async fn run_case(
    dbname: &str,
    setup: impl FnOnce(&mut World) -> Vec<Vec<SimTx>>,
    build_a: impl FnOnce(&mut World) -> Vec<SimTx>,
    build_b: impl FnOnce(&mut World) -> Vec<SimTx>,
) {
    let (h, mut w) = standard_boot(dbname, fast_args()).await;
    for block in setup(&mut w) {
        h.sim.add_block(block);
    }
    // A builds on a clone, so `w` stays the common ancestor that B builds on.
    let mut wa = w.clone();
    let a_block = h.sim.add_block(build_a(&mut wa));
    let a_cp = hex32(&a_block);
    h.wait_until("op A processed", || async { h.checkpoint().await.as_deref() == Some(a_cp.as_str()) }).await;

    let b_txs = build_b(&mut w);
    h.sim.reorg(1, vec![b_txs, vec![]]);

    let want = expected(&w);
    h.wait_until("deeds converge to the winning chain", || async { actual(&h).await == want }).await;
    let report = h.selftest_now().await;
    assert!(report.proven, "{dbname}: converged state must prove: {report:?}");
    h.stop().await;
}

fn pending_fixture(w: &mut World) -> Vec<Vec<SimTx>> {
    let (s, a) = w.register("alice", &OWNER_A);
    let squat = name_above(&dotk_core::key_of("alice"), "squat");
    let sq = w.split(&squat, &OWNER_B);
    vec![vec![s, a], vec![sq]]
}

fn active_fixture(w: &mut World) -> Vec<Vec<SimTx>> {
    let (s, a) = w.register("alice", &OWNER_A);
    vec![vec![s, a]]
}

// A gap outpoint: `split`, `merge` at seat 0, or `absorbed` at seat 2.

#[tokio::test]
async fn a_split_racing_a_split_converges_to_the_winner() {
    run_case(
        "ds_spl_spl",
        active_fixture,
        |w| {
            let g = w.flanks(&dotk_core::key_of("alice")).1.state;
            vec![w.split(&name_inside(&g.lo, &g.hi, "x"), &OWNER_A)]
        },
        |w| {
            let g = w.flanks(&dotk_core::key_of("alice")).1.state;
            vec![w.split(&name_inside(&g.lo, &g.hi, "y"), &OWNER_B)]
        },
    )
    .await;
}

#[tokio::test]
async fn a_split_racing_a_release_converges_to_the_winner() {
    // The gap below alice is her release's seat 0. A split of it double-spends the exit.
    run_case(
        "ds_spl_rel",
        active_fixture,
        |w| {
            let g = w.flanks(&dotk_core::key_of("alice")).0.state;
            vec![w.split(&name_inside(&g.lo, &g.hi, "below"), &OWNER_B)]
        },
        |w| vec![w.release("alice")],
    )
    .await;
}

#[tokio::test]
async fn a_release_racing_a_split_converges_to_the_winner() {
    run_case(
        "ds_rel_spl",
        active_fixture,
        |w| vec![w.release("alice")],
        |w| {
            let g = w.flanks(&dotk_core::key_of("alice")).0.state;
            vec![w.split(&name_inside(&g.lo, &g.hi, "below"), &OWNER_B)]
        },
    )
    .await;
}

#[tokio::test]
async fn a_split_racing_an_evict_converges_to_the_winner() {
    // The gap above the squat is its evict's seat 2 (`absorbed`).
    run_case(
        "ds_spl_evi",
        pending_fixture,
        |w| {
            let key = pending_key(w);
            let g = w.flanks(&key).1.state;
            vec![w.split(&name_inside(&g.lo, &g.hi, "above"), &OWNER_A)]
        },
        |w| {
            let key = pending_key(w);
            vec![w.evict(&key)]
        },
    )
    .await;
}

#[tokio::test]
async fn an_evict_racing_a_split_converges_to_the_winner() {
    run_case(
        "ds_evi_spl",
        pending_fixture,
        |w| {
            let key = pending_key(w);
            vec![w.evict(&key)]
        },
        |w| {
            let key = pending_key(w);
            let g = w.flanks(&key).1.state;
            vec![w.split(&name_inside(&g.lo, &g.hi, "above"), &OWNER_A)]
        },
    )
    .await;
}

#[tokio::test]
async fn two_adjacent_exits_racing_converge_to_the_winner() {
    // The gap between two neighbors is seat 2 of the lower exit and seat 0 of the upper one.
    run_case(
        "ds_adj_exits",
        |w| {
            let (s1, a1) = w.register("alice", &OWNER_A);
            let succ = name_above(&dotk_core::key_of("alice"), "succ");
            let (s2, a2) = w.register(&succ, &OWNER_B);
            vec![vec![s1, a1], vec![s2, a2]]
        },
        |w| vec![w.release("alice")],
        |w| {
            let succ = w.name_of(&w.flanks(&dotk_core::key_of("alice")).1.state.hi);
            vec![w.release(&succ)]
        },
    )
    .await;
}

// A deed outpoint: activate or evict while PENDING, transfer or release while ACTIVE.

#[tokio::test]
async fn an_activate_racing_an_evict_converges_to_the_winner() {
    run_case(
        "ds_act_evi",
        pending_fixture,
        |w| {
            let name = w.name_of(&pending_key(w));
            vec![w.activate(&name, &OWNER_B)]
        },
        |w| {
            let key = pending_key(w);
            vec![w.evict(&key)]
        },
    )
    .await;
}

#[tokio::test]
async fn an_evict_racing_an_activate_converges_to_the_winner() {
    run_case(
        "ds_evi_act",
        pending_fixture,
        |w| {
            let key = pending_key(w);
            vec![w.evict(&key)]
        },
        |w| {
            let name = w.name_of(&pending_key(w));
            vec![w.activate(&name, &OWNER_B)]
        },
    )
    .await;
}

#[tokio::test]
async fn an_evict_racing_an_evict_converges_to_the_winner() {
    run_case(
        "ds_evi_evi",
        pending_fixture,
        |w| {
            let key = pending_key(w);
            vec![w.evict(&key)]
        },
        |w| {
            let key = pending_key(w);
            vec![w.evict(&key)]
        },
    )
    .await;
}

#[tokio::test]
async fn an_activate_racing_an_activate_converges_to_the_winner() {
    run_case(
        "ds_act_act",
        pending_fixture,
        |w| {
            let name = w.name_of(&pending_key(w));
            vec![w.activate(&name, &OWNER_B)]
        },
        |w| {
            let name = w.name_of(&pending_key(w));
            vec![w.activate(&name, &OWNER_B)]
        },
    )
    .await;
}

#[tokio::test]
async fn a_transfer_racing_a_transfer_converges_to_the_winner() {
    run_case("ds_tra_tra", active_fixture, |w| vec![w.transfer("alice", &OWNER_B)], |w| vec![w.transfer("alice", &[0xC3; 32])]).await;
}

#[tokio::test]
async fn a_transfer_racing_a_release_converges_to_the_winner() {
    run_case("ds_tra_rel", active_fixture, |w| vec![w.transfer("alice", &OWNER_B)], |w| vec![w.release("alice")]).await;
}

#[tokio::test]
async fn a_release_racing_a_transfer_converges_to_the_winner() {
    run_case("ds_rel_tra", active_fixture, |w| vec![w.release("alice")], |w| vec![w.transfer("alice", &OWNER_B)]).await;
}
