use crate::common::*;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use dotk_core::cards::{RecordValue, Records};
use dotk_indexer::config::CliArgs;
use dotk_indexer::model::RowKind;

fn gateway_args() -> CliArgs {
    let mut args = fast_args();
    args.gateway_domain = "kaspa.name".into();
    args.gateway_app_url = "https://dotk.example".into();
    args
}

fn url_records(url: &str) -> Records {
    let mut r = Records::new();
    r.insert("url".into(), RecordValue::Text(url.into()));
    r
}

async fn ask(h: &Harness, host: &str, path: &str) -> (StatusCode, axum::http::HeaderMap, String) {
    let req = Request::builder().uri(path).header(header::HOST, host).body(Body::empty()).unwrap();
    let res = Web::new(h).send(req).await;
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    (status, headers, String::from_utf8_lossy(&bytes).into_owned())
}

fn location(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers.get(header::LOCATION).and_then(|v| v.to_str().ok())
}

async fn register(h: &Harness, w: &mut World, name: &str) {
    let key = dotk_core::key_of(name);
    let (split, activate) = w.register(name, &OWNER_A);
    h.sim.add_block(vec![split, activate]);
    h.wait_until("ACTIVE", || async { matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Active) }).await;
}

async fn mint(h: &Harness, w: &mut World, name: &str, records: &Records) {
    let key = dotk_core::key_of(name);
    let tx = w.transfer_with_card(name, &OWNER_B, records, &OWNER_B);
    h.sim.add_block(vec![tx.clone()]);
    h.wait_until("the card's transfer", || async { matches!(h.deed_row(&key).await, Some(r) if r.outpoint_txid == Some(tx.txid)) })
        .await;
}

#[tokio::test]
async fn a_name_with_a_url_redirects() {
    let (h, mut w) = standard_boot("gateway_redirect", gateway_args()).await;
    register(&h, &mut w, "alice").await;
    mint(&h, &mut w, "alice", &url_records("https://alice.example/home")).await;

    let startup = h.app.verdict.health.write().await.selftest.take();
    assert!(startup.is_some(), "the harness ran a startup pass");
    let (status, headers, _) = ask(&h, "alice.kaspa.name", "/").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "no redirect before the first verdict, as the API");
    assert_eq!(location(&headers), None);
    h.selftest_now().await;

    let (status, headers, _) = ask(&h, "alice.kaspa.name", "/").await;
    assert_eq!(status, StatusCode::FOUND, "302, never 301: the record changes with the card");
    assert_eq!(location(&headers), Some("https://alice.example/home"));
    assert_eq!(
        headers.get(header::CACHE_CONTROL).unwrap(),
        &format!("public, max-age={}", h.app.deployment.args.cache_ttl.as_secs()),
        "the lookup's own TTL, so an edge caches the redirect no longer than the lookup"
    );

    // Nothing of the request is appended to the record.
    let (status, headers, _) = ask(&h, "ALICE.kaspa.name:443", "/anything/at/all?x=1").await;
    assert_eq!(status, StatusCode::FOUND, "case and port are dropped from Host first");
    assert_eq!(location(&headers), Some("https://alice.example/home"));

    mint(&h, &mut w, "alice", &url_records("alice.example/next")).await;
    let (status, headers, _) = ask(&h, "alice.kaspa.name", "/").await;
    assert_eq!((status, location(&headers)), (StatusCode::FOUND, Some("https://alice.example/next")));

    let key = dotk_core::key_of("alice");
    let tx = w.transfer("alice", &OWNER_A);
    h.sim.add_block(vec![tx.clone()]);
    h.wait_until("the plain transfer", || async { matches!(h.deed_row(&key).await, Some(r) if r.outpoint_txid == Some(tx.txid)) })
        .await;
    let (status, headers, body) = ask(&h, "alice.kaspa.name", "/").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(location(&headers), None);
    assert!(body.contains("alice.k has no website"), "{body}");
    assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store", "a 404 outlives nothing");
    h.stop().await;
}

#[tokio::test]
async fn a_pending_name_is_taken() {
    let (h, mut w) = standard_boot("gateway_pending", gateway_args()).await;
    let key = dotk_core::key_of("alice");
    let (split, _) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split]);
    h.wait_until("PENDING", || async { matches!(h.deed_row(&key).await, Some(r) if r.kind == RowKind::Pending) }).await;

    let (status, _, body) = ask(&h, "alice.kaspa.name", "/").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("alice.k is taken"), "{body}");
    let (status, _, body) = ask(&h, "api.dotk.name", "/v1/names/alice").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("pending"), "{body}");

    let refuted = dotk_indexer::audit::Withheld::new(vec![], vec![key], vec![]);
    h.app.verdict.health.write().await.selftest.as_mut().unwrap().withheld = std::sync::Arc::new(refuted);
    let (status, _, body) = ask(&h, "alice.kaspa.name", "/").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let (status, _, _) = ask(&h, "api.dotk.name", "/v1/names/alice").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    h.stop().await;
}

/// Each refusal page names its reason, because a visitor acts on it: register, wait, or leave.
#[tokio::test]
async fn every_refusal_page_names_its_reason() {
    let (h, mut w) = standard_boot("gateway_pages", gateway_args()).await;
    register(&h, &mut w, "alice").await;
    mint(&h, &mut w, "alice", &url_records("https://alice.example")).await;

    let (status, _, body) = ask(&h, "bob.kaspa.name", "/").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("bob.k is free") && body.contains("https://dotk.example/names/bob"), "{body}");
    // A subname never inherits the parent's website.
    let (status, _, body) = ask(&h, "pay.alice.kaspa.name", "/").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("is not a name"), "{body}");
    h.stop().await;
}
