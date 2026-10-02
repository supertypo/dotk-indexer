use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Context;
use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use dotk_core::state::OwnerType;

use super::dto::{
    BlockRef, CardOut, DayCount, DeedOut, GapOut, GapsByWidth, HealthQuery, HealthResponse, HistoryEntry, HistoryQuery,
    HistoryResponse, KeyKindOut, KeyResponse, KeysByPrefix, KeyspaceResponse, KeyspaceTotals, Manifest, NameResponse, NeighborGaps,
    NoQuery, OwnerResponse, SelfTestDetail, SelfTestSummary, SnapshotQuery, SpenderCardsQuery, SpenderCardsResponse,
};
use super::error::{ApiError, ErrorCode, ErrorResponse, Internal, NotReady, err, internal};
use super::middleware::{CachePolicy, cached, live_cache_policy, tagged_json};
use super::params::{card_cursor, checked, page_limit, path, path_key, path_owner_type, ready, valid_name};
use super::server::{DEPLOYMENT_TAG, HEALTH_TAG, REGISTRY_TAG, SNAPSHOT_TAG};
use crate::app::{App, Backends, SnapshotBodies, Tagged};
use crate::audit::{NameLookup, look_up_name};
use crate::convert::hex32;
use crate::db;
use crate::derive;
use crate::model::RowKind;
use crate::model::{OwnerTypeParam, SpenderTypeParam};
use crate::snapshot;

/// The manifest never changes.
pub const GENESIS_CACHE_TTL: Duration = Duration::from_hours(1);

async fn acquire(backends: &Backends, context: &str) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, ApiError> {
    backends.web_db.acquire().await.map_err(|e| internal(&format!("acquiring a connection for {context}"), e))
}

#[utoipa::path(
    method(get),
    path = "/names/{name}",
    tag = REGISTRY_TAG,
    summary = "Get a name's owner",
    params(("name" = String, Path, description = "Name, with or without `.k`")),
    responses(
        (status = StatusCode::OK, description = "Success", body = NameResponse),
        (status = StatusCode::BAD_REQUEST, description = "Not a valid name", body = ErrorResponse),
        (status = StatusCode::NOT_FOUND, description = "Not registered", body = ErrorResponse),
        (status = StatusCode::SERVICE_UNAVAILABLE, description = "No self-test verdict yet, or the last one did not prove this name", body = ErrorResponse),
        Internal
    )
)]
pub(super) async fn get_name(
    State(app): State<Arc<App>>,
    raw: Result<Path<String>, PathRejection>,
    query: Result<Query<NoQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let raw = path(raw, ErrorCode::InvalidName, "name")?;
    checked(query)?;
    let name = valid_name(&raw)?;
    ready(&app.verdict).await?;
    let mut conn = acquire(&app.backends, "/names").await?;
    let (key, row) = match look_up_name(&app.verdict, &mut conn, &name).await.map_err(|e| internal("looking a name up", e))? {
        NameLookup::Active(key, row) => (key, row),
        NameLookup::Held => return Err(err(ErrorCode::NotFound, "registered, but pending or with an owner this indexer cannot name")),
        NameLookup::Unproven => return Err(unproven()),
        NameLookup::Free => return Err(err(ErrorCode::NotFound, "not registered")),
    };
    let state = row.deed_state(key).ok_or_else(|| internal("deriving deed state", "the row carries no owner"))?;
    let deed = app.deployment.deed_address(&state).map_err(|e| internal("deriving the deed address for /names", e))?;
    let owner = state.owner;
    let card = db::live_card_by_key(&mut conn, &key).await.map_err(|e| internal("reading a name's card", e))?;
    Ok(axum::Json(NameResponse {
        name,
        owner_type: state.owner_type as u8,
        owner: hex32(&owner),
        address: dotk_core::address::owner_address(app.deployment.prefix, state.owner_type, &owner).map(|a| a.to_string()),
        deed_address: deed.to_string(),
        card: card.map(|h| CardOut::new(&app.deployment, h)),
        registry_covenant_id: app.deployment.genesis.registry_covenant_id.clone(),
    })
    .into_response())
}

fn unproven() -> ApiError {
    err(ErrorCode::NotReady, "the last self-test did not prove this part of the registry")
}

async fn owner_names(app: &App, owner_type: OwnerType, owner: [u8; 32]) -> Result<Response, ApiError> {
    let withheld = app.verdict.withheld().await;
    let mut conn = acquire(&app.backends, "an owner lookup").await?;
    let names =
        db::deeds_by_owner(&mut conn, owner_type as u8, &owner).await.map_err(|e| internal("listing the names an owner holds", e))?;
    let cards =
        db::live_cards_by_owner(&mut conn, owner_type as u8, &owner).await.map_err(|e| internal("listing an owner's cards", e))?;
    Ok(axum::Json(OwnerResponse {
        owner_type: owner_type as u8,
        owner: hex32(&owner),
        address: dotk_core::address::owner_address(app.deployment.prefix, owner_type, &owner).map(|a| a.to_string()),
        names: names.into_iter().filter(|n| !withheld.withholds_key(&dotk_core::key_of(n))).collect(),
        cards: cards
            .into_iter()
            .filter(|h| !withheld.withholds_key(&h.card.state.key))
            .map(|h| CardOut::new(&app.deployment, h))
            .collect(),
        registry_covenant_id: app.deployment.genesis.registry_covenant_id.clone(),
    })
    .into_response())
}

const SPENDER_CARDS_LIMIT_MAX: u32 = 100;

#[utoipa::path(
    method(get),
    path = "/spenders/{spenderType}/{spender}/cards",
    tag = REGISTRY_TAG,
    summary = "Get the cards a spender can sweep",
    params(
        ("spenderType" = SpenderTypeParam, Path),
        ("spender" = String, Path, pattern = "^[0-9a-f]{64}$", description = "Spender key payload: 64 hex characters"),
        ("after" = Option<String>, Query, pattern = "^[0-9a-f]{64}:(0|[1-9][0-9]*)$", description = "The previous page's `next`. Absent for the first page."),
        ("limit" = Option<u32>, Query, minimum = 1, maximum = 100, description = "Cards per page, 1–100. Default 100.")
    ),
    responses(
        (status = StatusCode::OK, description = "Success. A page of the unswept cards naming the spender, live or retired, with `next` while more follow. A spender with none answers an empty list, never 404", body = SpenderCardsResponse),
        (status = StatusCode::BAD_REQUEST, description = "Not a key-owned scheme, not a 32-byte hex payload, or an `after` or `limit` this operation refuses", body = ErrorResponse),
        NotReady,
        Internal
    )
)]
pub(super) async fn get_spender_cards(
    State(app): State<Arc<App>>,
    raw: Result<Path<(String, String)>, PathRejection>,
    query: Result<Query<SpenderCardsQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let (raw_type, raw_spender) = path(raw, ErrorCode::InvalidOwnerType, "owner type and owner")?;
    let query = checked(query)?;
    let limit = page_limit(query.limit, SPENDER_CARDS_LIMIT_MAX, SPENDER_CARDS_LIMIT_MAX)?;
    let after = query
        .after
        .as_deref()
        .map(|a| {
            card_cursor(a).ok_or_else(|| err(ErrorCode::InvalidQuery, "after is a card outpoint, {txid}:{index}, as `next` spells it"))
        })
        .transpose()?;
    let spender_type = path_owner_type(&raw_type)
        .filter(|t| t.needs_signature())
        .ok_or_else(|| err(ErrorCode::InvalidOwnerType, "a spender scheme is 0, 133 or 134, in decimal and unpadded"))?;
    let spender =
        path_key(&raw_spender).ok_or_else(|| err(ErrorCode::InvalidKey, "a spender payload is 64 lowercase hex characters"))?;
    ready(&app.verdict).await?;
    let mut conn = acquire(&app.backends, "a spender lookup").await?;
    let mut hits = db::cards_by_spender(&mut conn, spender_type as u8, &spender, after, i64::from(limit) + 1)
        .await
        .map_err(|e| internal("listing a spender's cards", e))?;
    let page = usize::try_from(limit).unwrap_or(usize::MAX);
    let next = if hits.len() > page {
        hits.truncate(page);
        hits.last().map(|h| format!("{}:{}", hex32(&h.card.txid), h.card.idx))
    } else {
        None
    };
    Ok(axum::Json(SpenderCardsResponse {
        spender_type: spender_type as u8,
        spender: hex32(&spender),
        address: dotk_core::address::owner_address(app.deployment.prefix, spender_type, &spender)
            .map(|a| a.to_string())
            .unwrap_or_default(),
        cards: hits.into_iter().map(|h| CardOut::new(&app.deployment, h)).collect(),
        next,
        registry_covenant_id: app.deployment.genesis.registry_covenant_id.clone(),
    })
    .into_response())
}

#[utoipa::path(
    method(get),
    path = "/owners/{ownerType}/{owner}",
    tag = REGISTRY_TAG,
    summary = "Get an owner's names",
    params(
        ("ownerType" = OwnerTypeParam, Path),
        ("owner" = String, Path, pattern = "^[0-9a-f]{64}$", description = "Owner payload: 64 hex characters")
    ),
    responses(
        (status = StatusCode::OK, description = "Success. An owner holding nothing answers an empty list, never 404", body = OwnerResponse),
        (status = StatusCode::BAD_REQUEST, description = "Not an owner scheme this registry mints, or not a 32-byte hex payload", body = ErrorResponse),
        NotReady,
        Internal
    )
)]
pub(super) async fn get_owner(
    State(app): State<Arc<App>>,
    raw: Result<Path<(String, String)>, PathRejection>,
    query: Result<Query<NoQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let (raw_type, raw_owner) = path(raw, ErrorCode::InvalidOwnerType, "owner type and owner")?;
    checked(query)?;
    let owner_type = path_owner_type(&raw_type)
        .ok_or_else(|| err(ErrorCode::InvalidOwnerType, "an owner scheme is 0, 3, 4, 133 or 134, in decimal and unpadded"))?;
    let owner = path_key(&raw_owner).ok_or_else(|| err(ErrorCode::InvalidKey, "an owner payload is 64 lowercase hex characters"))?;
    ready(&app.verdict).await?;
    owner_names(&app, owner_type, owner).await
}

#[utoipa::path(
    method(get),
    path = "/addresses/{address}",
    tag = REGISTRY_TAG,
    summary = "Get an address's names",
    params(("address" = String, Path, description = "Kaspa address of any owner kind")),
    responses(
        (status = StatusCode::OK, description = "Success", body = OwnerResponse),
        (status = StatusCode::BAD_REQUEST, description = "Unsupported address kind", body = ErrorResponse),
        NotReady,
        Internal
    )
)]
pub(super) async fn get_address(
    State(app): State<Arc<App>>,
    raw: Result<Path<String>, PathRejection>,
    query: Result<Query<NoQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let raw = path(raw, ErrorCode::InvalidAddress, "address")?;
    checked(query)?;
    // `parse` checks the network prefix, because the owner pair is network-independent and an
    // address from another chain otherwise answers 200. It also refuses a wrong-length payload
    // before the address crate panics on it.
    let addr =
        dotk_core::address::parse(&raw, app.deployment.prefix).map_err(|e| err(ErrorCode::InvalidAddress, format!("address: {e}")))?;
    let (owner_type, owner) = dotk_core::address::owner_of(&addr).map_err(|e| err(ErrorCode::InvalidAddress, e.to_string()))?;
    ready(&app.verdict).await?;
    owner_names(&app, owner_type, owner).await
}

async fn key_neighborhood(app: &App, key: [u8; 32]) -> Result<Response, ApiError> {
    let mut conn = acquire(&app.backends, "a key lookup").await?;
    let n = db::adjacent_rows(&mut conn, &key).await.map_err(|e| internal("reading a key neighborhood", e))?;
    let states = derive::neighborhood(&key, n.at.as_ref(), n.pred.as_ref(), n.succ.as_ref());
    let key_hex = hex32(&key);

    // A partition check here is a tautology, because derived gaps partition any sorted key set.
    // The last self-test's verdict and blind spots are the only real signal.
    let (proven, proven_at) = match &app.verdict.health.read().await.selftest {
        Some(t) => (t.proven && !t.withheld.withholds_key(&key) && !t.withheld.withholds_provenance(&key), Some(t.finished_ms)),
        None => (false, None),
    };

    let row = n.at.as_ref();
    let kind = row.map_or(KeyKindOut::Free, |r| r.kind.into());
    let deed = states
        .deed
        .zip(row)
        .map(|(state, r)| DeedOut::new(&app.deployment, state, r))
        .transpose()
        .map_err(|e| internal("deriving the deed address for a key", e))?;
    Ok(axum::Json(KeyResponse {
        key: key_hex,
        kind,
        name: row.and_then(|r| r.name.clone()),
        deed,
        covering: states.covering.map(GapOut::from),
        neighbors: states
            .neighbors
            .map(|(pred, succ)| NeighborGaps { predecessor: GapOut::from(pred), successor: GapOut::from(succ) }),
        registry_covenant_id: app.deployment.genesis.registry_covenant_id.clone(),
        proven,
        proven_at,
    })
    .into_response())
}

#[utoipa::path(
    method(get),
    path = "/keys/{key}",
    tag = REGISTRY_TAG,
    summary = "Get a key's neighborhood",
    params(("key" = String, Path, pattern = "^[0-9a-f]{64}$", description = "Name key: 64 hex characters, `blake3(name)`")),
    responses(
        (status = StatusCode::OK, description = "Success. An unused key answers `kind: \"free\"`, never 404", body = KeyResponse),
        (status = StatusCode::BAD_REQUEST, description = "Not a 32-byte hex key", body = ErrorResponse),
        NotReady,
        Internal
    )
)]
pub(super) async fn get_key(
    State(app): State<Arc<App>>,
    raw: Result<Path<String>, PathRejection>,
    query: Result<Query<NoQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let raw = path(raw, ErrorCode::InvalidKey, "key")?;
    checked(query)?;
    let key = path_key(&raw).ok_or_else(|| err(ErrorCode::InvalidKey, "a name key is 64 lowercase hex characters"))?;
    ready(&app.verdict).await?;
    key_neighborhood(&app, key).await
}

#[utoipa::path(
    method(get),
    path = "/names/{name}/key",
    tag = REGISTRY_TAG,
    summary = "Get a name's key neighborhood",
    params(("name" = String, Path, description = "Name, with or without `.k`")),
    responses(
        (status = StatusCode::OK, description = "Success. An unregistered name answers `kind: \"free\"`, never 404", body = KeyResponse),
        (status = StatusCode::BAD_REQUEST, description = "Not a valid name", body = ErrorResponse),
        NotReady,
        Internal
    )
)]
pub(super) async fn get_key_by_name(
    State(app): State<Arc<App>>,
    raw: Result<Path<String>, PathRejection>,
    query: Result<Query<NoQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let raw = path(raw, ErrorCode::InvalidName, "name")?;
    checked(query)?;
    let name = valid_name(&raw)?;
    ready(&app.verdict).await?;
    key_neighborhood(&app, dotk_core::names::key_of(&name)).await
}

const HISTORY_LIMIT_DEFAULT: u32 = 5;
const HISTORY_LIMIT_MAX: u32 = 100;

#[utoipa::path(
    method(get),
    path = "/keys/{key}/history",
    tag = REGISTRY_TAG,
    summary = "Get a key's history",
    params(
        ("key" = String, Path, pattern = "^[0-9a-f]{64}$", description = "Name key: 64 hex characters, `blake3(name)`"),
        ("limit" = Option<u32>, Query, minimum = 1, maximum = 100, description = "Entries per page, 1–100. Default 5."),
        ("offset" = Option<u32>, Query, description = "Entries to skip, newest first. Default 0.")
    ),
    responses(
        (status = StatusCode::OK, description = "Success. A key nothing is known about answers an empty list, never 404", body = HistoryResponse),
        (status = StatusCode::BAD_REQUEST, description = "Not a 32-byte hex key, or a limit outside 1–100", body = ErrorResponse),
        NotReady,
        Internal
    )
)]
pub(super) async fn get_key_history(
    State(app): State<Arc<App>>,
    raw: Result<Path<String>, PathRejection>,
    query: Result<Query<HistoryQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let raw = path(raw, ErrorCode::InvalidKey, "key")?;
    let key = path_key(&raw).ok_or_else(|| err(ErrorCode::InvalidKey, "a name key is 64 lowercase hex characters"))?;
    let query = checked(query)?;
    let limit = page_limit(query.limit, HISTORY_LIMIT_DEFAULT, HISTORY_LIMIT_MAX)?;
    let offset = query.offset.unwrap_or(0);
    ready(&app.verdict).await?;
    let mut conn = acquire(&app.backends, "a key history").await?;
    let (rows, total, oldest) = db::history_page_with_extent(&mut conn, &key, i64::from(limit), i64::from(offset))
        .await
        .map_err(|e| internal("reading a page of key history", e))?;
    Ok(axum::Json(HistoryResponse {
        key: hex32(&key),
        total: u64::try_from(total).unwrap_or(0),
        limit,
        offset,
        complete: oldest == Some(crate::model::HistoryOp::Register),
        entries: rows.iter().map(HistoryEntry::new).collect(),
        registry_covenant_id: app.deployment.genesis.registry_covenant_id.clone(),
    })
    .into_response())
}

#[utoipa::path(
    method(get),
    path = "/keyspace",
    tag = REGISTRY_TAG,
    summary = "Get the keyspace summary",
    params(("If-None-Match" = Option<String>, Header, nullable = false, description = "Entity tags from earlier answers. A match answers 304.")),
    responses(
        (status = StatusCode::OK, description = "Success. An empty registry answers all-zero buckets, never 404", body = KeyspaceResponse, headers(("ETag" = String, description = "Strong entity tag of the body. Every later answer with the same tag has the same bytes."))),
        (status = StatusCode::NOT_MODIFIED, description = "`If-None-Match` names the current body", headers(("ETag" = String))),
        (status = StatusCode::BAD_REQUEST, description = "A query string, which this operation does not take", body = ErrorResponse),
        NotReady,
        Internal
    )
)]
pub(super) async fn get_keyspace(
    State(app): State<Arc<App>>,
    query: Result<Query<NoQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    checked(query)?;
    ready(&app.verdict).await?;
    let ttl = app.deployment.args.cache_ttl;
    let (body, age) = app
        .caches
        .keyspace
        .get(ttl, app.deployment.args.web_build_timeout, build_keyspace(app.clone()))
        .await
        .map_err(|e| internal("building /keyspace", e))?;
    Ok(cached(tagged_json(&body), live_cache_policy(ttl, age)))
}

async fn build_keyspace(app: Arc<App>) -> anyhow::Result<Tagged> {
    // Read before the counts, so the counts are at least as new as the label.
    let last_block = app.verdict.health.read().await.last_block.as_ref().map(BlockRef::from);
    let mut conn = app.backends.web_db.acquire().await.context("acquiring a connection")?;
    let summary = db::keyspace_summary(&mut conn, app.deployment.args.web_build_timeout).await.context("summarizing the keyspace")?;
    let kind_column = |kind: RowKind| summary.keys_by_prefix.iter().map(|b| b[kind as usize]).collect::<Vec<_>>();
    let (active, pending, owner_unknown) =
        (kind_column(RowKind::Active), kind_column(RowKind::Pending), kind_column(RowKind::OwnerUnknown));
    let totals = KeyspaceTotals {
        active: active.iter().sum(),
        pending: pending.iter().sum(),
        owner_unknown: owner_unknown.iter().sum(),
        gaps: summary.gaps,
    };
    let body = KeyspaceResponse {
        registry_covenant_id: app.deployment.genesis.registry_covenant_id.clone(),
        totals,
        keys_by_prefix: KeysByPrefix { active, pending, owner_unknown },
        gaps_by_width: GapsByWidth {
            counts: summary.gaps_by_width,
            buckets_per_average: db::GAP_WIDTH_BUCKETS_PER_AVERAGE,
            wider: summary.gaps_wider,
        },
        registrations_by_day: summary.registrations_by_day.into_iter().map(DayCount::new).collect(),
        last_block,
    };
    serde_json::to_vec(&body).map(Tagged::new).context("serializing the keyspace summary")
}

#[utoipa::path(
    method(get),
    path = "/snapshot",
    tag = SNAPSHOT_TAG,
    summary = "Get the registry snapshot",
    params(
        ("proven" = Option<bool>, Query, description = "Default `true`: the self-test-proven snapshot. `false`: a live, unproven export."),
        ("events" = Option<bool>, Query, description = "Default `true`: carry the resume section. `false`: leave `events` empty, which is what a client that only reads `deeds` wants."),
        ("If-None-Match" = Option<String>, Header, nullable = false, description = "Entity tags from earlier answers. A match answers 304.")
    ),
    responses(
        (status = StatusCode::OK, description = "The snapshot. `proven` says which kind it is", body = snapshot::Snapshot, headers(("ETag" = String, description = "Strong entity tag of the body. Every later answer with the same tag has the same bytes."))),
        (status = StatusCode::NOT_MODIFIED, description = "`If-None-Match` names the current body", headers(("ETag" = String))),
        (status = StatusCode::BAD_REQUEST, description = "`proven` or `events` is neither `true` nor `false`", body = ErrorResponse),
        (status = StatusCode::SERVICE_UNAVAILABLE, description = "No self-test verdict yet, or for the proven body, no proof yet or an expired one", body = ErrorResponse),
        Internal
    )
)]
pub(super) async fn get_snapshot(
    State(app): State<Arc<App>>,
    query: Result<Query<SnapshotQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    // Never `Option<Query<T>>`, which silently falls back to the default and serves a proven
    // body to someone who asked for a live one.
    let query = checked(query)?;
    let with_events = query.events.unwrap_or(true);
    if query.proven.unwrap_or(true) { proven_snapshot(&app, with_events).await } else { live_snapshot(&app, with_events).await }
}

async fn proven_snapshot(app: &App, with_events: bool) -> Result<Response, ApiError> {
    let (body, proven_ms) = match &*app.verdict.proof.read().await {
        Some(p) => (p.bodies.pick(with_events), p.proven_ms),
        None => return Err(err(ErrorCode::NotReady, "no proven snapshot yet")),
    };
    let expiry = app.deployment.args.selftest_interval * 2;
    let age_ms = App::now_ms().saturating_sub(proven_ms);
    if u128::from(age_ms) > expiry.as_millis() {
        return Err(err(ErrorCode::StaleProof, format!("the last proof is stale (proven {}s ago)", age_ms / 1000)));
    }
    Ok(cached(tagged_json(&body), CachePolicy::Ttl(app.deployment.args.cache_ttl_snapshot)))
}

async fn live_snapshot(app: &Arc<App>, with_events: bool) -> Result<Response, ApiError> {
    ready(&app.verdict).await?;
    let ttl = app.deployment.args.cache_ttl_snapshot_live;
    match app.caches.live_snapshot.get(ttl, app.deployment.args.web_build_timeout, export_live(app.clone())).await {
        Ok((bodies, age)) => Ok(cached(tagged_json(&bodies.pick(with_events)), live_cache_policy(ttl, age))),
        // No checkpoint yet is a state, not a fault.
        Err(e) if e.is::<snapshot::NoCheckpoint>() => Err(err(ErrorCode::NotReady, snapshot::NoCheckpoint.to_string())),
        Err(e) => Err(internal("building the live snapshot", e)),
    }
}

async fn export_live(app: Arc<App>) -> anyhow::Result<SnapshotBodies> {
    let export = snapshot::export(
        &app.backends.web_db,
        &app.deployment.genesis.registry_covenant_id,
        snapshot::export_event_window(app.deployment.net_bps),
        app.deployment.args.web_build_timeout,
    );
    let mut export = export.await.context("exporting")?;
    snapshot::bodies(&mut export).context("serializing")
}

#[utoipa::path(
    method(get),
    path = "/genesis",
    tag = DEPLOYMENT_TAG,
    summary = "Get the deployment manifest",
    params(("If-None-Match" = Option<String>, Header, nullable = false, description = "Entity tags from earlier answers. A match answers 304.")),
    responses(
        (status = StatusCode::OK, description = "The deployment manifest verbatim: template bytecode, abi and params", body = Manifest, headers(("ETag" = String, description = "Strong entity tag of the body. Every later answer with the same tag has the same bytes."))),
        (status = StatusCode::NOT_MODIFIED, description = "`If-None-Match` names the current body", headers(("ETag" = String))),
        (status = StatusCode::BAD_REQUEST, description = "A query string, which this operation does not take", body = ErrorResponse)
    )
)]
pub(super) async fn get_genesis(
    State(app): State<Arc<App>>,
    query: Result<Query<NoQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    checked(query)?;
    // The raw bytes, because `GenesisFile` has no catch-all field and re-encoding drops unknown
    // fields.
    Ok(cached(tagged_json(&app.deployment.genesis_raw), CachePolicy::Ttl(GENESIS_CACHE_TTL)))
}

#[utoipa::path(
    method(get),
    path = "/health",
    tag = HEALTH_TAG,
    summary = "Get health details",
    params(("detail" = Option<bool>, Query, description = "Default `false`: the self-test verdict alone. `true`: the failure coordinates and repair counts with it.")),
    responses(
        (status = StatusCode::OK, description = "Healthy: caught up (or behind for under 10 s) and proven valid", body = HealthResponse),
        (status = StatusCode::SERVICE_UNAVAILABLE, description = "Unhealthy, same body", body = HealthResponse),
        (status = StatusCode::BAD_REQUEST, description = "`detail` is neither `true` nor `false`", body = ErrorResponse)
    )
)]
pub(super) async fn get_health(
    State(app): State<Arc<App>>,
    query: Result<Query<HealthQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let query = checked(query)?;
    let (last_block, self_test) = {
        let h = app.verdict.health.read().await;
        (h.last_block.as_ref().map(BlockRef::from), h.selftest.clone())
    };
    // Only lag has slack, so a retried poll does not flap. A failed self-test is a 503 at once.
    // A database that is gone stalls the pipeline, which shows as lag.
    let counts = *app.caches.counts.read().await;
    let healthy = app.progress.lag_tolerated() && self_test.as_ref().is_some_and(|t| t.proven) && counts.is_some();
    let counts = counts.unwrap_or_default();
    let body = HealthResponse {
        healthy,
        last_block,
        tip_blue_score: match app.progress.tip_blue_score.load(Ordering::Relaxed) {
            0 => None,
            tip => Some(tip),
        },
        net_bps: app.deployment.net_bps,
        active: counts.active,
        pending: counts.pending,
        owner_unknown: counts.owner_unknown,
        history_seq: counts.history_seq,
        tip_distance: app.deployment.args.vcp_tip_distance,
        journal_coverage: match app.progress.coverage_floor.load(Ordering::Relaxed) {
            u64::MAX => None,
            floor => Some(floor),
        },
        caught_up: app.progress.caught_up(),
        self_test: self_test.as_ref().map(SelfTestSummary::from),
        self_test_detail: self_test.as_ref().filter(|_| query.detail == Some(true)).map(SelfTestDetail::from),
        registry_covenant_id: app.deployment.genesis.registry_covenant_id.clone(),
        network: app.deployment.genesis.network.clone(),
    };
    let status = if healthy { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    // A cached 200 outlives a switch to 503.
    Ok(cached((status, axum::Json(body)).into_response(), CachePolicy::NoStore))
}
