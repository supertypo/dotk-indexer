use std::sync::atomic::Ordering;

use crate::common::*;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use dotk_indexer::model::DeedRow;
use dotk_indexer::snapshot;
use kaspa_addresses::{Address, Prefix, Version as AddrVersion};
use serde_json::json;

/// `/health` answers from what the pipeline counted, so a pool that a request flood holds busy
/// cannot turn it into a 503.
#[tokio::test]
async fn health_follows_the_pipeline_not_the_pool() {
    let (h, mut w) = standard_boot("health_memo", fast_args()).await;
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice", || async { h.deed_row(&dotk_core::key_of("alice")).await.is_some() }).await;
    assert!(h.selftest_now().await.proven);

    let web = Web::new(&h);
    let health = || async {
        let (status, _, body) = web.get_json("/v1/health").await;
        (status, body)
    };

    let (status, body) = health().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["active"], 1, "counted");
    assert_eq!(body["ownerUnknown"], 0, "and the healing backlog is published beside them");
    assert_eq!(body["healthy"], true);

    h.app.backends.db.close().await;
    h.app.backends.web_db.close().await;
    let (status, body) = health().await;
    assert_eq!(status, StatusCode::OK, "the counts come from memory, so a pool that is busy or gone is no verdict");
    assert_eq!(body["active"], 1);

    let lookup = web.send(Request::builder().uri("/v1/names/alice").body(Body::empty()).unwrap()).await;
    assert_eq!(lookup.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = axum::body::to_bytes(lookup.into_body(), 1 << 20).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["code"], "internal", "the caller still learns which kind of failure this is");
    let published = v["error"].as_str().unwrap();
    for leaked in ["pool", "postgres", "database", "closed", "schema", "sql", h.app.deployment.args.database_url.as_str()] {
        assert!(
            !published.to_lowercase().contains(&leaked.to_lowercase()),
            "the body names the operator's machine: {published:?} contains {leaked:?}"
        );
    }
    // No `h.stop()`, because it waits out the pipeline's retry backoff on the closed pool.
}

#[tokio::test]
async fn health_reports_caught_up_live_and_503s_once_the_lag_persists() {
    // `caughtUp` is a verdict about now, never a latch. `healthy` follows it with 10 s of slack, so one
    // retried poll does not flap the endpoint choice of every client.
    let (h, mut w) = standard_boot("health_live", fast_args()).await;
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice", || async { h.deed_row(&dotk_core::key_of("alice")).await.is_some() }).await;
    assert!(h.selftest_now().await.proven);

    let web = Web::new(&h);
    let health = || async {
        let (status, _, body) = web.get_json("/v1/health").await;
        (status, body)
    };
    let (status, body) = health().await;
    assert_eq!((status, &body["caughtUp"], &body["healthy"]), (StatusCode::OK, &json!(true), &json!(true)), "{body}");

    let now = dotk_indexer::app::App::now_ms();
    h.app.progress.last_block_ms.store(now - dotk_indexer::app::CAUGHT_UP_MAX_AGE_MS - 5_000, Ordering::Relaxed);
    let (status, body) = health().await;
    assert_eq!((status, &body["caughtUp"], &body["healthy"]), (StatusCode::OK, &json!(false), &json!(true)), "{body}");

    h.app.progress.caught_up_ms.store(now - dotk_indexer::app::UNHEALTHY_AFTER_MS - 1_000, Ordering::Relaxed);
    let (status, body) = health().await;
    assert_eq!(
        (status, &body["caughtUp"], &body["healthy"]),
        (StatusCode::SERVICE_UNAVAILABLE, &json!(false), &json!(false)),
        "{body}"
    );

    h.sim.pad(1);
    h.wait_until("caught up again", || async { h.app.progress.caught_up() }).await;
    let (status, body) = health().await;
    assert_eq!((status, &body["caughtUp"], &body["healthy"]), (StatusCode::OK, &json!(true), &json!(true)), "{body}");

    // A fresh block a minute of blocks short of the tip is not caught up either.
    let tip = h.app.progress.tip_blue_score.load(Ordering::Relaxed);
    h.app.progress.tip_blue_score.store(tip + dotk_indexer::app::CAUGHT_UP_MAX_LAG_SECS * h.app.deployment.net_bps, Ordering::Relaxed);
    let (status, body) = health().await;
    assert_eq!((status, &body["caughtUp"], &body["healthy"]), (StatusCode::OK, &json!(false), &json!(true)), "{body}");
    h.app.progress.tip_blue_score.store(tip, Ordering::Relaxed);
    assert!(h.app.progress.caught_up());
    h.stop().await;
}

/// A booted indexer holding `alice`, and the self-test that proved it.
async fn web_boot(name: &str) -> (Harness, World, Web, dotk_indexer::app::SelfTestReport) {
    let (h, mut w) = standard_boot(name, fast_args()).await;
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice", || async { h.deed_row(&dotk_core::key_of("alice")).await.is_some() }).await;
    let report = h.selftest_now().await;
    assert!(report.proven && report.published, "{report:?}");
    let web = Web::new(&h);
    (h, w, web, report)
}

fn owner_a_address() -> String {
    Address::new(Prefix::Testnet, AddrVersion::PubKey, &OWNER_A).to_string()
}

#[tokio::test]
async fn health_and_snapshot_answer_503_before_the_first_verdict() {
    let (h, _w) = standard_boot("web_unjudged", fast_args()).await;
    let web = Web::new(&h);
    // The harness runs the startup pass at boot, so this puts back the state before any self-test.
    h.app.verdict.health.write().await.selftest = None;
    *h.app.verdict.proof.write().await = None;
    let (status, cache, body) = web.get("/v1/health").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("\"healthy\":false"));
    assert_eq!(cache, "no-store", "health is never edge-cached");
    let (status, cache, _) = web.get("/v1/snapshot").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "unproven /snapshot must 503");
    assert_eq!(cache, "no-store");
    h.stop().await;
}

#[tokio::test]
async fn health_reports_the_registry_and_keeps_failure_detail_off_the_polled_body() {
    let (h, _w, web, _) = web_boot("web_health").await;
    let (status, _, body) = web.get("/v1/health").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("\"active\":1") && body.contains("\"healthy\":true"), "{body}");

    let (_, _, v) = web.get_json("/v1/health").await;
    assert_eq!(v["netBps"], 10);
    assert_eq!(v["registryCovenantId"], h.app.deployment.genesis.registry_covenant_id.as_str());
    assert_eq!(v["network"], h.app.deployment.genesis.network.as_str());
    let tip = v["tipBlueScore"].as_u64().expect("the follow loop observes the tip");
    let behind = tip.saturating_sub(v["lastBlock"]["blueScore"].as_u64().unwrap());
    assert!(behind <= h.app.deployment.args.vcp_tip_distance, "caught up, but {behind} blue scores behind");

    // Every open page polls this body, so failure detail comes only with `?detail=true`.
    assert_eq!(v["selfTest"]["proven"], true);
    assert!(v["selfTest"]["gapsChecked"].is_number() && v["selfTest"]["deedsChecked"].is_number());
    assert!(v["selfTestDetail"].is_null(), "the coordinates are not on the polled body");
    assert!(v["selfTest"]["repaired"].is_null() && v["selfTest"]["failingOutpoints"].is_null(), "{v}");
    h.stop().await;
}

#[tokio::test]
async fn health_detail_carries_the_repair_and_refuses_a_bad_flag() {
    let (h, _w, web, _) = web_boot("web_health_detail").await;
    let (status, _, v) = web.get_json("/v1/health?detail=true").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["selfTest"]["proven"], true, "the verdict is served either way");
    assert!(v["selfTestDetail"]["repaired"].is_object(), "what repair did: {v}");
    assert!(v["selfTestDetail"]["failingOutpoints"].as_array().unwrap().is_empty(), "a passing run fails nothing");
    assert!(v["selfTestDetail"]["blindSpots"].as_array().unwrap().is_empty());
    assert_eq!(v["selfTestDetail"]["published"], true, "this run refreshed the /snapshot proof");
    assert!(v["selfTestDetail"]["blindSpotsOmitted"].is_null(), "nothing omitted, nothing claimed");

    let (status, _, v) = web.get_json("/v1/health?detail=yes").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "a query value outside its type is refused, never guessed at");
    assert_eq!(v["code"], "invalid_query");
    h.stop().await;
}

#[tokio::test]
async fn a_name_lookup_serves_the_owner_and_answers_404_400_and_cors() {
    let (h, _w, web, _) = web_boot("web_names").await;
    let (status, cache, body) = web.get("/v1/names/Alice.k").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cache, format!("public, max-age={}", h.app.deployment.args.cache_ttl.as_secs()));
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["name"], "alice");
    assert_eq!(v["ownerType"], 0);
    assert_eq!(v["registryCovenantId"], REGISTRY_COVENANT_ID);
    assert_eq!(v["address"], owner_a_address().as_str());
    assert_eq!(v["owner"], faster_hex::hex_string(&OWNER_A).as_str());
    assert!(v["deedAddress"].as_str().unwrap().starts_with("kaspatest:"));

    let (status, cache, _) = web.get("/v1/names/unregistered").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(cache, "no-store", "errors are never edge-cached");
    let (status, _, _) = web.get("/v1/names/not_valid!").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "syntactically invalid is 400, not 404");

    let res = web
        .send(Request::builder().uri("/v1/names/alice").header("origin", "http://localhost:7788").body(Body::empty()).unwrap())
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers().get("access-control-allow-origin").map(|v| v.to_str().unwrap()),
        Some("*"),
        "a browser cannot read a response without this header"
    );
    h.stop().await;
}

#[tokio::test]
async fn an_address_lookup_lists_the_names_it_owns() {
    let (h, _w, web, _) = web_boot("web_addresses").await;
    let owner_addr = owner_a_address();
    let (status, _, v) = web.get_json(&format!("/v1/addresses/{owner_addr}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["names"][0], "alice");
    assert_eq!(v["ownerType"], 0);
    assert_eq!(v["owner"], faster_hex::hex_string(&OWNER_A).as_str());
    assert_eq!(v["address"], owner_addr.as_str());
    h.stop().await;
}

#[tokio::test]
async fn key_history_pages_from_the_newest_and_refuses_a_bad_limit() {
    let (h, _w, web, _) = web_boot("web_key_history").await;
    let history = format!("/v1/keys/{}/history", dotk_indexer::convert::hex32(&dotk_core::key_of("alice")));
    for bad in ["limit=0", "limit=101"] {
        let (status, _, v) = web.get_json(&format!("{history}?{bad}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}: {v}");
    }
    let (status, _, v) = web.get_json(&format!("{history}?limit=1&offset=1")).await;
    assert_eq!(status, StatusCode::OK);
    let entries = v["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["op"], "register", "offset counts from the newest");
    let (_, _, v) = web.get_json(&format!("{history}?limit=1")).await;
    assert_eq!(v["entries"].as_array().unwrap().len(), 1, "the limit caps the page: {v}");
    assert_eq!(v["total"], 2);
    h.stop().await;
}

#[tokio::test]
async fn the_snapshot_serves_the_proof_and_the_live_export_with_their_own_ttls() {
    let (h, _w, web, report) = web_boot("web_snapshot").await;
    // The proof carries the timestamp of the run that published it, so /health and /snapshot agree.
    let (status, cache, body) = web.get("/v1/snapshot").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cache, format!("public, max-age={}", h.app.deployment.args.cache_ttl_snapshot.as_secs()));
    let snap: snapshot::Snapshot = serde_json::from_str(&body).unwrap();
    assert_eq!(snap.deeds.len(), 1);
    assert_eq!(snap.registry_covenant_id, REGISTRY_COVENANT_ID);
    assert!(snap.proven, "a served snapshot is always proven");
    assert_eq!(snap.proven_at, Some(report.finished_ms));

    // Same path, different query. The live body is cacheable only while its memo keeps it byte-identical.
    let (status, cache, body) = web.get("/v1/snapshot?proven=false").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(cache, format!("public, max-age={}", h.app.deployment.args.cache_ttl_snapshot_live.as_secs()));
    assert_ne!(
        cache,
        format!("public, max-age={}", h.app.deployment.args.cache_ttl_snapshot.as_secs()),
        "live must not inherit the proof's TTL"
    );
    let live: snapshot::Snapshot = serde_json::from_str(&body).unwrap();
    assert!(!live.proven, "a live export claims nothing");
    assert_eq!(live.registry_covenant_id, REGISTRY_COVENANT_ID);

    let (status, cache, _) = web.get("/v1/snapshot?proven=true").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cache, format!("public, max-age={}", h.app.deployment.args.cache_ttl_snapshot.as_secs()), "explicit true = default");
    h.stop().await;
}

#[tokio::test]
async fn events_false_leaves_the_journal_out_of_either_snapshot() {
    let (h, _w, web, _) = web_boot("web_snapshot_events").await;
    // The proven body is pre-serialized and the live one is memoized, so each applies `events` its own way.
    for path in ["/v1/snapshot?proven=false", "/v1/snapshot?proven=true"] {
        let (_, _, full) = web.get(path).await;
        let (status, _, bare) = web.get(&format!("{path}&events=false")).await;
        assert_eq!(status, StatusCode::OK, "{bare}");
        let full: snapshot::Snapshot = serde_json::from_str(&full).unwrap();
        let bare: snapshot::Snapshot = serde_json::from_str(&bare).unwrap();
        assert!(!full.events.is_empty(), "{path}: this registry journaled a split and an activate");
        assert!(bare.events.is_empty(), "{path}: events=false leaves the section empty");
        assert_eq!(
            serde_json::to_string(&full.deeds).unwrap(),
            serde_json::to_string(&bare.deeds).unwrap(),
            "{path}: the canonical core is the same registry either way"
        );
        assert_eq!(full.vcp_checkpoint, bare.vcp_checkpoint, "{path}");
        assert_eq!(full.proven, bare.proven, "{path}");
    }
    h.stop().await;
}

#[tokio::test]
async fn a_snapshot_query_that_does_not_parse_is_refused() {
    let (h, _w, web, _) = web_boot("web_snapshot_query").await;
    // Only `true` and `false` parse. A silent default serves a body the client did not ask for.
    for bad in [
        "/v1/snapshot?proven=0",
        "/v1/snapshot?proven=1",
        "/v1/snapshot?proven=",
        "/v1/snapshot?proven",
        "/v1/snapshot?events=0",
        "/v1/snapshot?events=1",
        "/v1/snapshot?events=",
        "/v1/snapshot?proven=false&events=yes",
    ] {
        let (status, cache, body) = web.get(bad).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad} must not be guessed at: {body}");
        assert_eq!(cache, "no-store");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap_or_else(|e| panic!("{bad}: {e}: {body}"));
        assert!(v["error"].is_string(), "{bad} must answer as ErrorResponse json, not axum's text/plain: {body}");
    }
    h.stop().await;
}

#[tokio::test]
async fn genesis_serves_the_manifest_bytes_unchanged() {
    let (h, _w, web, _) = web_boot("web_genesis").await;
    // Re-serializing `GenesisFile` drops unknown fields, so the endpoint serves the loaded bytes.
    let (status, cache, body) = web.get("/v1/genesis").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cache, format!("public, max-age={}", dotk_indexer::web::GENESIS_CACHE_TTL.as_secs()));
    assert_eq!(body.as_bytes(), &h.app.deployment.genesis_raw.bytes[..], "/genesis serves the manifest bytes unchanged");
    h.stop().await;
}

#[tokio::test]
async fn an_undecodable_path_segment_answers_the_json_error_of_its_route() {
    let (h, _w, web, _) = web_boot("web_path_segments").await;
    // An undecodable path segment gets the JSON error and the code of any other bad spelling.
    for (uri, code) in [
        ("/v1/names/%ff", "invalid_name"),
        ("/v1/names/%ff/key", "invalid_name"),
        ("/v1/keys/%ff", "invalid_key"),
        ("/v1/keys/%ff/history", "invalid_key"),
        ("/v1/owners/%ff/x", "invalid_owner_type"),
        ("/v1/spenders/%ff/x/cards", "invalid_owner_type"),
        ("/v1/addresses/%ff", "invalid_address"),
    ] {
        let res = web.send(Request::builder().uri(uri).body(Body::empty()).unwrap()).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(res.headers().get("content-type").unwrap(), "application/json", "{uri}");
        assert_eq!(res.headers().get("cache-control").unwrap(), "no-store", "{uri}");
        let body = axum::body::to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap_or_else(|e| panic!("{uri}: {e}"));
        assert_eq!(v["code"], code, "{uri}: {v}");
    }
    h.stop().await;
}

#[tokio::test]
async fn a_cors_preflight_is_answered_with_no_store() {
    let (h, _w, web, _) = web_boot("web_preflight").await;
    // The CORS layer answers a preflight before the router, and the answer still carries Cache-Control.
    for preflight in [false, true] {
        let mut req = Request::builder().method("OPTIONS").uri("/v1/health").header("origin", "http://localhost:7788");
        if preflight {
            req = req.header("access-control-request-method", "GET");
        }
        let res = web.send(req.body(Body::empty()).unwrap()).await;
        assert_eq!(res.status(), StatusCode::OK, "preflight {preflight}");
        assert_eq!(res.headers().get("cache-control").unwrap(), "no-store", "preflight {preflight}");
        if preflight {
            assert_eq!(res.headers().get("access-control-max-age").unwrap(), "86400");
        }
    }
    h.stop().await;
}

/// Adds a PENDING squat above `alice` and an owner-unknown row, and returns their keys.
async fn key_kinds(h: &Harness, w: &mut World) -> ([u8; 32], [u8; 32]) {
    // The key route answers every key, never with a 404. A PENDING deed is name-blind, so only its key reaches it.
    let squat = name_above(&dotk_core::key_of("alice"), "squatter");
    let squat_tx = w.split(&squat, &OWNER_B);
    h.sim.add_block(vec![squat_tx]);
    let squat_key = dotk_core::key_of(&squat);
    h.wait_until("the squat is pending", || async { h.deed_row(&squat_key).await.is_some() }).await;
    // Only repair and bridging create an owner-unknown row, so the test writes one directly.
    let unknown_key = [0xEEu8; 32];
    let mut conn = h.app.backends.db.acquire().await.unwrap();
    dotk_indexer::db::upsert_deed(&mut conn, &unknown_key, &DeedRow::owner_unknown(None)).await.unwrap();
    (squat_key, unknown_key)
}

#[tokio::test]
async fn an_active_key_answers_with_its_deed_and_both_neighbors() {
    let (h, mut w, web, report) = web_boot("web_keys_active").await;
    key_kinds(&h, &mut w).await;
    let alice_hex = dotk_indexer::convert::hex32(&dotk_core::key_of("alice"));
    let (status, cache, v) = web.get_json(&format!("/v1/keys/{alice_hex}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cache, format!("public, max-age={}", h.app.deployment.args.cache_ttl.as_secs()));
    assert_eq!(v["key"], alice_hex.as_str());
    assert_eq!(v["kind"], "active");
    assert_eq!(v["name"], "alice");
    assert_eq!(v["deed"]["status"], "Active", "the spelling clients hash straight into an address");
    assert_eq!(v["deed"]["owner"], faster_hex::hex_string(&OWNER_A).as_str());
    assert_eq!(v["deed"]["ownerType"], 0, "an ACTIVE deed names its owner's scheme");
    assert!(v["deed"]["key"].is_null() && v["deed"]["name"].is_null(), "the deed does not repeat what the body says");
    assert!(v["covering"].is_null(), "a live key is a gap bound, never inside one");
    // An exit merge builds from this one answer.
    assert_eq!(v["neighbours"]["predecessor"]["hi"], alice_hex.as_str());
    assert_eq!(v["neighbours"]["successor"]["lo"], alice_hex.as_str());
    let (_, _, by_name_lookup) = web.get_json("/v1/names/alice").await;
    assert_eq!(v["deed"]["deedAddress"], by_name_lookup["deedAddress"], "one deed, one address, whichever route asked");
    assert_eq!(v["registryCovenantId"], REGISTRY_COVENANT_ID);
    assert_eq!(v["proven"], true, "the last self-test bracketed no blind spot here");
    assert_eq!(v["provenAt"], report.finished_ms, "a client can see how stale that coverage claim is");
    let (status, _, by_name) = web.get_json("/v1/names/Alice.k/key").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(by_name, v);
    h.stop().await;
}

#[tokio::test]
async fn a_pending_key_names_no_owner_and_an_owner_unknown_key_keeps_its_flanks() {
    let (h, mut w, web, _) = web_boot("web_keys_held").await;
    let (squat_key, unknown_key) = key_kinds(&h, &mut w).await;
    let squat_hex = dotk_indexer::convert::hex32(&squat_key);
    let (_, _, v) = web.get_json(&format!("/v1/keys/{squat_hex}")).await;
    assert_eq!(v["kind"], "pending");
    assert!(v["name"].is_null(), "a PENDING row has no name to give");
    assert_eq!(v["deed"]["status"], "Pending");
    // A PENDING deed carries its claim in the owner slot, but those bytes are not an owner.
    assert!(v["deed"]["claim"].is_string(), "the claim is the only place it comes back from");
    assert!(v["deed"]["owner"].is_null() && v["deed"]["ownerType"].is_null(), "a PENDING deed names no owner");
    assert!(
        v["deed"]["deedAddress"].as_str().unwrap().starts_with("kaspatest:"),
        "a PENDING deed sits at an address too: status is one of the five fields it hashes from"
    );
    assert_eq!(v["deed"]["outpointIndex"], 2, "the newborn deed an evict spends");
    assert!(v["deed"]["acceptedDaa"].is_number(), "the evictor's maturity clock");
    assert!(v["neighbours"]["predecessor"].is_object() && v["neighbours"]["successor"].is_object());

    // `ownerUnknown` has no on-chain state, so no deed is served, but the key keeps its flanks.
    let (_, _, v) = web.get_json(&format!("/v1/keys/{}", dotk_indexer::convert::hex32(&unknown_key))).await;
    assert_eq!(v["kind"], "ownerUnknown");
    assert!(v["deed"].is_null() && v["name"].is_null());
    assert!(v["neighbours"]["predecessor"].is_object());
    h.stop().await;
}

#[tokio::test]
async fn a_free_key_answers_with_its_covering_gap() {
    let (h, mut w, web, _) = web_boot("web_keys_free").await;
    let (squat_key, unknown_key) = key_kinds(&h, &mut w).await;
    let free_hex = dotk_indexer::convert::hex32(&dotk_core::key_of("never-registered"));
    let (status, _, v) = web.get_json(&format!("/v1/keys/{free_hex}")).await;
    assert_eq!(status, StatusCode::OK, "a free key is an answer: precisely what a split needs");
    assert_eq!(v["kind"], "free");
    assert!(v["deed"].is_null() && v["neighbours"].is_null());
    let covering = &v["covering"];
    assert!(
        covering["lo"].as_str().unwrap() < free_hex.as_str() && free_hex.as_str() < covering["hi"].as_str().unwrap(),
        "the covering gap must strictly contain the key: {covering}"
    );
    let (status, _, v) = web.get_json("/v1/names/never-registered/key").await;
    assert_eq!((status, &v["kind"]), (StatusCode::OK, &serde_json::json!("free")));

    // Below every live key, the covering gap starts at KEY_MIN, which is no deed's key.
    let low_hex = format!("{}01", "00".repeat(31));
    let (status, _, v) = web.get_json(&format!("/v1/keys/{low_hex}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["kind"], "free");
    assert_eq!(v["covering"]["lo"], "00".repeat(32).as_str());
    let mut live_keys = [dotk_core::key_of("alice"), squat_key, unknown_key];
    live_keys.sort_unstable();
    assert_eq!(v["covering"]["hi"], dotk_indexer::convert::hex32(&live_keys[0]).as_str(), "up to the lowest live key");
    h.stop().await;
}

#[tokio::test]
async fn a_malformed_key_or_name_answers_400() {
    let (h, _w, web, _) = web_boot("web_keys_malformed").await;
    let (status, cache, body) = web.get("/v1/keys/nothexatall").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "only a malformed key 400s");
    assert_eq!(cache, "no-store");
    assert!(serde_json::from_str::<serde_json::Value>(&body).unwrap()["error"].is_string(), "{body}");
    let (status, _, _) = web.get("/v1/names/not_valid!/key").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    h.stop().await;
}

#[tokio::test]
async fn the_docs_page_carries_the_servers_entry() {
    let (h, _w, web, _) = web_boot("web_docs").await;
    // The `servers` entry resolves the relative paths of the spec against the API base.
    for docs_uri in ["/v1", "/v1/"] {
        let (status, _, body) = web.get(docs_uri).await;
        assert_eq!(status, StatusCode::OK, "{docs_uri}");
        assert!(body.contains("\"servers\"") && body.contains("\"/v1\""), "{docs_uri} must carry the servers entry");
    }
    h.stop().await;
}

#[tokio::test]
async fn the_proven_snapshot_serves_the_last_proof_until_a_pass_replaces_it() {
    let (h, mut w) = standard_boot("proven_keep", fast_args()).await;
    let web = Web::new(&h);
    let get_snapshot = || async {
        let (status, _, body) = web.get("/v1/snapshot").await;
        (status, body)
    };

    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice", || async { h.deed_row(&dotk_core::key_of("alice")).await.is_some() }).await;
    let baseline = h.selftest_now().await;
    assert!(baseline.proven && baseline.published, "{baseline:?}");
    let (status, body) = get_snapshot().await;
    assert_eq!(status, StatusCode::OK);
    let proven: snapshot::Snapshot = serde_json::from_str(&body).unwrap();
    assert_eq!(proven.proven_at, Some(baseline.finished_ms));

    // Without sig scripts the indexer decodes nothing, so this split is a blind spot that no journal can bridge.
    let mut hidden = w.split(&name_above(&dotk_core::key_of("alice"), "shadow"), &OWNER_B);
    hidden.sig_scripts = vec![];
    h.sim.add_block(vec![hidden]);
    let failing = h.selftest_now().await;
    assert!(!failing.proven && !failing.published, "{failing:?}");

    let (status, body2) = get_snapshot().await;
    assert_eq!(status, StatusCode::OK, "a failing self-test must not clear the proof");
    assert_eq!(body, body2, "the served proof is the unchanged previous one");
    h.stop().await;
}

/// The proof expires at twice `--selftest-interval`, so a registry that stops proving stops serving.
#[tokio::test]
async fn the_proven_snapshot_expires_without_a_fresh_proof() {
    let mut args = fast_args();
    args.selftest_interval = std::time::Duration::from_millis(150);
    let (h, mut w) = standard_boot_ambient("proven_expiry", args).await;
    let web = Web::new(&h);
    let status_of = || async { web.get("/v1/snapshot").await.0 };

    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("a proof is published", || async { status_of().await == StatusCode::OK }).await;

    // A split the pipeline cannot decode, so no later pass proves.
    let mut hidden = w.split(&name_above(&dotk_core::key_of("alice"), "shadow"), &OWNER_B);
    hidden.sig_scripts = vec![];
    h.sim.add_block(vec![hidden]);
    h.wait_until("the proof ages out into 503", || async { status_of().await == StatusCode::SERVICE_UNAVAILABLE }).await;
    h.stop().await;
}

/// The seconds a `Cache-Control` value lets a cache keep the body. A memo's countdown ends at `no-store`.
fn max_age(cache_control: &str) -> Option<u64> {
    if cache_control == "no-store" {
        return Some(0);
    }
    cache_control.strip_prefix("public, max-age=")?.parse().ok()
}

/// A booted indexer holding `alice`, with the ambient self-test task.
async fn etag_boot(name: &str) -> (Harness, World, Web) {
    let (h, mut w) = standard_boot_ambient(name, fast_args()).await;
    let web = Web::new(&h);
    let (split, activate) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![split]);
    h.sim.add_block(vec![activate]);
    h.wait_until("alice", || async { h.deed_row(&dotk_core::key_of("alice")).await.is_some() }).await;
    h.wait_selftest().await;
    (h, w, web)
}

#[tokio::test]
async fn a_polled_body_is_answered_from_its_entity_tag() {
    let (h, _w, web) = etag_boot("web_etag").await;
    let get = |uri, tag| web.get_tagged(uri, tag);
    for uri in ["/v1/snapshot", "/v1/keyspace", "/v1/genesis", "/v1/openapi.json"] {
        let (status, etag, cache, len) = get(uri, None).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        let etag = etag.unwrap_or_else(|| panic!("{uri} must carry an entity tag"));
        assert!(len > 0, "{uri}");

        let (status, again, cache_304, len) = get(uri, Some(etag.clone())).await;
        assert_eq!(status, StatusCode::NOT_MODIFIED, "{uri}");
        assert_eq!(again.as_deref(), Some(etag.as_str()), "{uri} must repeat the tag it matched");
        // A memoized body's max-age counts down, so it can drop by a second between the two requests.
        let same_caching = match (cache.as_deref().and_then(max_age), cache_304.as_deref().and_then(max_age)) {
            (Some(ok), Some(not_modified)) => ok.abs_diff(not_modified) <= 1,
            _ => cache_304 == cache,
        };
        assert!(same_caching, "{uri}: a 304 carries the caching headers of the 200. 200: {cache:?}, 304: {cache_304:?}");
        assert_eq!(len, 0, "{uri}: the point is not sending the body");

        let (status, _, _, len) = get(uri, Some("\"0000000000000000000000000000000000000000000000000000000000000000\"".into())).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert!(len > 0, "{uri}");
    }

    h.stop().await;
}

#[tokio::test]
async fn an_entity_tag_matches_only_its_own_body() {
    let (h, mut w, web) = etag_boot("web_etag_other").await;
    let get = |uri, tag| web.get_tagged(uri, tag);
    let (_, full, _, _) = get("/v1/snapshot", None).await;
    let (status, _, _, len) = get("/v1/snapshot?events=false", full.clone()).await;
    assert_eq!(status, StatusCode::OK, "a different shape is a different body");
    assert!(len > 0);

    let (_, before, _, _) = get("/v1/keyspace", None).await;
    let (split, activate) = w.register("bob", &OWNER_A);
    h.sim.add_block(vec![split]);
    h.sim.add_block(vec![activate]);
    h.wait_until("bob registered", || async { h.deed_row(&dotk_core::key_of("bob")).await.is_some() }).await;
    // Stands in for the `--cache-ttl` window passing.
    h.app.caches.keyspace.clear();
    let (status, after, _, len) = get("/v1/keyspace", before.clone()).await;
    assert_eq!(status, StatusCode::OK, "the registry moved, so the stale tag must not match");
    assert_ne!(after, before);
    assert!(len > 0);

    h.stop().await;
}

/// A registration score above the DAA of the deed's own UTXO is a claim the chain refutes, so
/// the row goes back to never having observed its split.
#[tokio::test]
async fn an_active_clock_above_its_utxo_is_refuted() {
    let (h, mut w) = standard_boot("active_clock_bound", fast_args()).await;
    let alice = dotk_core::key_of("alice");
    let (s, a) = w.register("alice", &OWNER_A);
    h.sim.add_block(vec![s, a]);
    h.wait_until("alice", || async { h.deed_row(&alice).await.is_some() }).await;
    assert!(h.selftest_now().await.proven);

    let active = h.deed_row(&alice).await.unwrap();
    let mut conn = h.app.backends.db.acquire().await.unwrap();
    dotk_indexer::db::upsert_deed(&mut conn, &alice, &DeedRow { accepted_daa: Some(1_000_000), ..active.clone() }).await.unwrap();
    let report = h.selftest_now().await;
    assert!(report.proven && report.repaired.rewritten == 1, "{report:?}");
    assert_eq!(h.deed_row(&alice).await.unwrap(), DeedRow { accepted_daa: None, ..active });
    h.stop().await;
}

/// The operator's page, its script and its logo resolve under the configured base path, so a
/// proxy that mounts the indexer below a prefix shows a live page. Each page carries the policy
/// its own assets need, and the API carries the strict default.
#[tokio::test]
async fn the_status_page_and_its_assets_serve_under_the_base_path() {
    let mut args = fast_args();
    args.base_path = "/api".into();
    let (h, _) = standard_boot("status_page", args).await;
    let web = Web::new(&h);
    let page = web.send(Request::builder().uri("/api/").body(Body::empty()).unwrap()).await;
    assert_eq!(page.status(), StatusCode::OK);
    let csp = page.headers()["content-security-policy"].to_str().unwrap().to_string();
    assert!(csp.contains("script-src 'self'") && csp.contains("connect-src 'self'"), "{csp}");
    let body = String::from_utf8(axum::body::to_bytes(page.into_body(), 1 << 20).await.unwrap().to_vec()).unwrap();
    assert!(body.contains(r#"data-base="/api/v1""#) && body.contains("/api/status.js") && body.contains("/api/logo.svg"));
    for (path, content_type) in [
        ("/api/status.js", "text/javascript; charset=utf-8"),
        ("/api/logo.svg", "image/svg+xml"),
        ("/api/fonts/inter-latin.woff2", "font/woff2"),
    ] {
        let res = web.send(Request::builder().uri(path).body(Body::empty()).unwrap()).await;
        assert_eq!(res.status(), StatusCode::OK, "{path}");
        assert_eq!(res.headers()["content-type"], content_type, "{path}");
    }
    let docs = web.send(Request::builder().uri("/api/v1").body(Body::empty()).unwrap()).await;
    let csp = docs.headers()["content-security-policy"].to_str().unwrap().to_string();
    assert!(csp.contains("script-src https://cdn.jsdelivr.net") && csp.contains("font-src 'self'"), "{csp}");
    let body = String::from_utf8(axum::body::to_bytes(docs.into_body(), 1 << 22).await.unwrap().to_vec()).unwrap();
    assert!(body.contains("url(/api/fonts/inter-latin.woff2)") && body.contains(r#""withDefaultFonts":false"#));
    let json = web.send(Request::builder().uri("/api/v1/genesis").body(Body::empty()).unwrap()).await;
    assert_eq!(json.headers()["content-security-policy"], "default-src 'none'; frame-ancestors 'none'");
    assert_eq!(json.headers()["x-content-type-options"], "nosniff");
    h.stop().await;
}
