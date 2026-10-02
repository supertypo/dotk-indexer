use crate::common::*;
use axum::http::StatusCode;
use dotk_indexer::model::RowKind;

#[tokio::test]
async fn a_web_flood_holds_no_pipeline_connection() {
    let (h, mut w) = standard_boot("pool_isolation", fast_args()).await;
    let key = dotk_core::key_of("alice");

    // Holds every web connection past the acquire timeout.
    let mut held = Vec::new();
    for _ in 0..h.app.deployment.args.web_db_pool_size {
        held.push(h.app.backends.web_db.acquire().await.expect("a web connection"));
    }

    let (status, cache, _) = Web::new(&h).get("/v1/names/alice").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "the web pool is exhausted");
    assert_eq!(cache, "no-store");

    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    h.wait_until("ACTIVE row under a web flood", || async { matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Active) })
        .await;
    assert!(h.selftest_now().await.proven);

    drop(held);
    h.stop().await;
}
