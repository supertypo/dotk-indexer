use std::sync::Arc;
use std::time::Duration;

use crate::common::sim::SimKaspad;
use crate::common::*;
use dotk_indexer::chain::Kaspad;
use dotk_indexer::db;
use kaspa_addresses::Prefix;

#[tokio::test]
async fn a_committed_batch_delivered_again_changes_nothing() {
    let pool = fresh_pool("replay").await;
    db::migrate(&pool).await.unwrap();
    let (mut w, genesis_tx) = World::new();
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx]));
    let start = sim.dag_info().await.unwrap().virtual_parent;
    let app = test_app(pool.clone(), sim.clone(), fast_args(), 0);
    let (split, activate) = w.register("alice", &OWNER_A);
    sim.add_block(vec![split]);
    sim.add_block(vec![activate]);
    let res = sim.virtual_chain(start, 0, Duration::from_secs(5)).await.unwrap();

    dotk_indexer::chain::process_response(&app, &res).await.unwrap();
    let journal = || async { sqlx::query_scalar::<_, i64>("SELECT count(*) FROM events").fetch_one(&pool).await.unwrap() };
    let before = journal().await;
    // Consumes the permit the first batch stored, so only the replay's trigger counts.
    let _ = futures::FutureExt::now_or_never(app.verdict.selftest_trigger.notified());

    dotk_indexer::chain::process_response(&app, &res).await.expect("a committed batch is not applied twice");
    assert_eq!(journal().await, before);
    tokio::time::timeout(Duration::from_secs(1), app.verdict.selftest_trigger.notified())
        .await
        .expect("the lost outcome asks for a self-test");
}
