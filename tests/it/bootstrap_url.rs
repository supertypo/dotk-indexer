use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use axum::Router;
use axum::routing::get;
use dotk_indexer::snapshot;

fn body() -> String {
    serde_json::json!({
        "proven": true,
        "registryCovenantId": "11".repeat(32),
        "vcpCheckpoint": "22".repeat(32),
        "deeds": [{ "key": "33".repeat(32), "kind": 0, "name": "alice", "ownerType": 0, "owner": "44".repeat(32) }],
        "events": [],
    })
    .to_string()
}

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

#[tokio::test]
async fn a_served_body_is_fetched_whole() {
    let base = serve(Router::new().route("/v1/snapshot", get(|| async { body() }))).await;
    let snapshot = snapshot::fetch(&format!("{base}/v1/snapshot"), 1 << 30).await.unwrap();
    assert_eq!(snapshot.registry_covenant_id, "11".repeat(32));
    assert_eq!(snapshot.deeds.len(), 1);
    assert_eq!(snapshot.deeds[0].row.name.as_deref(), Some("alice"));
}

#[tokio::test]
async fn a_body_above_the_cap_is_refused_before_it_is_read() {
    let read = Arc::new(AtomicU32::new(0));
    let seen = read.clone();
    let base = serve(Router::new().route(
        "/v1/snapshot",
        get(move || {
            read.fetch_add(1, Ordering::SeqCst);
            async { body() }
        }),
    ))
    .await;
    let e = snapshot::fetch(&format!("{base}/v1/snapshot"), 16).await.unwrap_err();
    assert!(e.to_string().contains("above --snapshot-max-bytes 16"), "{e:#}");
    assert_eq!(seen.load(Ordering::SeqCst), 1, "refused on the first answer, never retried");
}

/// A chunked body declares no length, so the cap is measured on what arrives.
#[tokio::test]
async fn a_body_without_a_length_is_abandoned_past_the_cap() {
    let base = serve(Router::new().route(
        "/v1/snapshot",
        get(|| async {
            let stream = futures::stream::iter((0..64).map(|_| Ok::<_, std::io::Error>(vec![b'{'; 1024])));
            axum::body::Body::from_stream(stream)
        }),
    ))
    .await;
    let e = snapshot::fetch(&format!("{base}/v1/snapshot"), 4096).await.unwrap_err();
    assert!(e.to_string().contains("above --snapshot-max-bytes 4096"), "{e:#}");
}
