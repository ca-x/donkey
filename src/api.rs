use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    cache::CacheEntryView,
    db::{self, domain_mapping},
    domainfold::{self, ConvertInput, ConvertOutput, MappingInput},
    error::ApiResult,
    nodes::{NodeInput, NodeView},
    registry_routes::{RegistryRouteInput, RegistryRouteView},
    state::AppState,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/dashboard", get(dashboard))
        .route("/nodes", get(list_nodes).post(create_node))
        .route("/nodes/{id}", put(update_node).delete(delete_node))
        .route("/nodes/{id}/probe", post(probe_node))
        .route(
            "/registry-routes",
            get(list_registry_routes).post(create_registry_route),
        )
        .route(
            "/registry-routes/{id}",
            put(update_registry_route).delete(delete_registry_route),
        )
        .route("/cache", get(list_cache))
        .route("/cache/{key}", axum::routing::delete(delete_cache))
        .route("/mappings", get(list_mappings).post(create_mapping))
        .route("/mappings/{id}", put(update_mapping).delete(delete_mapping))
        .route("/domainfold/convert", post(convert_url))
        .route("/runtime", get(runtime))
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    version: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

#[derive(Serialize)]
struct Dashboard {
    nodes: Vec<NodeView>,
    cache_entries: usize,
    cache_bytes: u64,
    cache_hits: u64,
    healthy_nodes: usize,
}

async fn dashboard(State(state): State<AppState>) -> ApiResult<Json<Dashboard>> {
    let nodes = state.nodes.list().await?;
    let cache = state.cache.stats().await?;
    Ok(Json(Dashboard {
        healthy_nodes: nodes
            .iter()
            .filter(|node| node.metric.healthy && node.node.enabled)
            .count(),
        cache_entries: cache.entries,
        cache_bytes: cache.bytes,
        cache_hits: cache.hits,
        nodes,
    }))
}

async fn list_nodes(State(state): State<AppState>) -> ApiResult<Json<Vec<NodeView>>> {
    Ok(Json(state.nodes.list().await?))
}

async fn create_node(
    State(state): State<AppState>,
    Json(input): Json<NodeInput>,
) -> ApiResult<(StatusCode, Json<NodeView>)> {
    Ok((StatusCode::CREATED, Json(state.nodes.create(input).await?)))
}

async fn update_node(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(input): Json<NodeInput>,
) -> ApiResult<Json<NodeView>> {
    Ok(Json(state.nodes.update(id, input).await?))
}

async fn delete_node(State(state): State<AppState>, Path(id): Path<Uuid>) -> ApiResult<StatusCode> {
    state.nodes.delete(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn probe_node(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<NodeView>> {
    Ok(Json(state.nodes.probe(id).await?))
}

async fn list_registry_routes(
    State(state): State<AppState>,
) -> ApiResult<Json<Vec<RegistryRouteView>>> {
    Ok(Json(state.registry_routes.list().await?))
}

async fn create_registry_route(
    State(state): State<AppState>,
    Json(input): Json<RegistryRouteInput>,
) -> ApiResult<(StatusCode, Json<RegistryRouteView>)> {
    Ok((
        StatusCode::CREATED,
        Json(state.registry_routes.create(input).await?),
    ))
}

async fn update_registry_route(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(input): Json<RegistryRouteInput>,
) -> ApiResult<Json<RegistryRouteView>> {
    Ok(Json(state.registry_routes.update(id, input).await?))
}

async fn delete_registry_route(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<StatusCode> {
    state.registry_routes.delete(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct ListQuery {
    #[serde(default = "default_limit")]
    limit: u64,
}

fn default_limit() -> u64 {
    100
}

async fn list_cache(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Json<Vec<CacheEntryView>>> {
    Ok(Json(
        state
            .cache
            .list(query.limit)
            .await?
            .into_iter()
            .map(Into::into)
            .collect(),
    ))
}

async fn delete_cache(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> ApiResult<StatusCode> {
    state.cache.remove(&key).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_mappings(
    State(state): State<AppState>,
) -> ApiResult<Json<Vec<domain_mapping::Model>>> {
    Ok(Json(db::list_mappings(&state.db).await?))
}

async fn create_mapping(
    State(state): State<AppState>,
    Json(input): Json<MappingInput>,
) -> ApiResult<(StatusCode, Json<domain_mapping::Model>)> {
    Ok((
        StatusCode::CREATED,
        Json(domainfold::create(&state.db, input).await?),
    ))
}

async fn update_mapping(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(input): Json<MappingInput>,
) -> ApiResult<Json<domain_mapping::Model>> {
    Ok(Json(domainfold::update(&state.db, id, input).await?))
}

async fn delete_mapping(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<StatusCode> {
    domainfold::delete(&state.db, id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn convert_url(
    State(state): State<AppState>,
    Json(input): Json<ConvertInput>,
) -> ApiResult<Json<ConvertOutput>> {
    Ok(Json(domainfold::convert(&state.db, &input.url).await?))
}

#[derive(Serialize)]
struct RuntimeConfig {
    admin_addr: String,
    registry_addr: String,
    tls_enabled: bool,
    private_upstreams: bool,
    chunk_size: u64,
    chunk_concurrency: usize,
    parallel_threshold: u64,
    scheduler_policy: String,
    max_cache_bytes: u64,
    cache_used_bytes: u64,
    cache_entries: usize,
    cache_policy: String,
    cache_high_watermark: f64,
    cache_low_watermark: f64,
    cache_ttl_seconds: Option<u64>,
    max_export_bytes: u64,
    export_ttl_seconds: u64,
    admin_external_tls: bool,
    admin_external_loopback: bool,
}

async fn runtime(State(state): State<AppState>) -> ApiResult<Json<RuntimeConfig>> {
    let cache = state.cache.stats().await?;
    Ok(Json(RuntimeConfig {
        admin_addr: state.config.admin_addr.to_string(),
        registry_addr: state.config.registry_addr.to_string(),
        tls_enabled: state.config.tls_cert.is_some(),
        private_upstreams: state.config.allow_private_upstreams,
        chunk_size: state.config.chunk_size,
        chunk_concurrency: state.config.chunk_concurrency,
        parallel_threshold: state.config.parallel_threshold,
        scheduler_policy: state.config.scheduler_policy.to_string(),
        max_cache_bytes: state.config.max_cache_bytes,
        cache_used_bytes: cache.bytes,
        cache_entries: cache.entries,
        cache_policy: state.config.cache_policy.to_string(),
        cache_high_watermark: state.config.cache_high_watermark,
        cache_low_watermark: state.config.cache_low_watermark,
        cache_ttl_seconds: state.config.cache_ttl.map(|value| value.as_secs()),
        max_export_bytes: state.config.max_export_bytes,
        export_ttl_seconds: state.config.export_ttl.as_secs(),
        admin_external_tls: state.config.admin_external_tls,
        admin_external_loopback: state.config.admin_external_loopback,
    }))
}
