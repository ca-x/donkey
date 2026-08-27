use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
        .route("/cache/clear", axum::routing::delete(clear_cache))
        .route("/cache/{key}", axum::routing::delete(delete_cache))
        .route("/mappings", get(list_mappings).post(create_mapping))
        .route("/mappings/{id}", put(update_mapping).delete(delete_mapping))
        .route("/domainfold/convert", post(convert_url))
        .route("/runtime", get(runtime).put(update_runtime))
        .route("/runtime/export", get(export_runtime))
        .route("/runtime/import", axum::routing::post(import_runtime))
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
    registry_requests: u64,
    registry_bytes: u64,
}

async fn dashboard(State(state): State<AppState>) -> ApiResult<Json<Dashboard>> {
    let nodes = state.nodes.list().await?;
    let cache = state.cache.stats().await?;
    let (registry_requests, registry_bytes) = state.traffic.snapshot();
    Ok(Json(Dashboard {
        healthy_nodes: nodes
            .iter()
            .filter(|node| node.metric.healthy && node.node.enabled)
            .count(),
        cache_entries: cache.entries,
        cache_bytes: cache.bytes,
        cache_hits: cache.hits,
        nodes,
        registry_requests,
        registry_bytes,
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

async fn clear_cache(State(state): State<AppState>) -> ApiResult<Json<serde_json::Value>> {
    let freed = state.cache.clear_all().await?;
    Ok(Json(serde_json::json!({ "freed_bytes": freed })))
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
    resumable_threshold: u64,
    upstream_timeout_seconds: u64,
    stream_fallback_timeout_seconds: u64,
    partial_ttl_seconds: u64,
    health_interval_seconds: u64,
    max_export_bytes: u64,
    export_ttl_seconds: u64,
    scheduler_policy: String,
    max_cache_bytes: u64,
    cache_used_bytes: u64,
    cache_entries: usize,
    cache_policy: String,
    cache_high_watermark: f64,
    cache_low_watermark: f64,
    cache_ttl_seconds: Option<u64>,
    admin_external_tls: bool,
    admin_external_loopback: bool,
    registry_external_tls: bool,
    registry_auth_enabled: bool,
}

async fn runtime(State(state): State<AppState>) -> ApiResult<Json<RuntimeConfig>> {
    let cache = state.cache.stats().await?;
    Ok(Json(runtime_config(
        &effective_config(&state).await?,
        cache,
    )))
}

#[derive(Clone, Serialize, Deserialize)]
struct RuntimeSettingsInput {
    chunk_size: u64,
    chunk_concurrency: usize,
    parallel_threshold: u64,
    resumable_threshold: u64,
    scheduler_policy: String,
    upstream_timeout_seconds: u64,
    stream_fallback_timeout_seconds: u64,
    partial_ttl_seconds: u64,
    max_cache_bytes: u64,
    cache_policy: String,
    cache_high_watermark: f64,
    cache_low_watermark: f64,
    cache_ttl_seconds: Option<u64>,
    health_interval_seconds: u64,
    max_export_bytes: u64,
    export_ttl_seconds: u64,
}

#[derive(Serialize, Deserialize)]
struct RuntimeSettingsExport {
    format: String,
    version: u32,
    settings: RuntimeSettingsInput,
    #[serde(default)]
    registry_routes: Vec<crate::registry_routes::RegistryRouteView>,
    #[serde(default)]
    nodes: Vec<ExportNode>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ExportNode {
    name: String,
    url: String,
    registry_route: String,
    enabled: bool,
    priority: i32,
    max_concurrency: u16,
    cf_preferred: bool,
    connect_ip: Option<String>,
    auth_mode: String,
    auth_username: Option<String>,
    auth_header: Option<String>,
}

async fn update_runtime(
    State(state): State<AppState>,
    Json(input): Json<RuntimeSettingsInput>,
) -> ApiResult<Json<RuntimeConfig>> {
    persist_runtime(&state, &input).await?;
    let cache = state.cache.stats().await?;
    Ok(Json(runtime_config(
        &effective_config(&state).await?,
        cache,
    )))
}

async fn export_runtime(State(state): State<AppState>) -> ApiResult<Json<RuntimeSettingsExport>> {
    let config = effective_config(&state).await?;
    let nodes = state
        .nodes
        .list()
        .await?
        .into_iter()
        .map(|node| ExportNode {
            name: node.node.name,
            url: node.node.url,
            registry_route: node.route.key,
            enabled: node.node.enabled,
            priority: node.node.priority,
            max_concurrency: node.max_concurrency,
            cf_preferred: node.node.cf_preferred,
            connect_ip: node.node.connect_ip,
            auth_mode: node.node.auth_mode,
            auth_username: node.node.auth_username,
            auth_header: node.node.auth_header,
        })
        .collect();
    Ok(Json(RuntimeSettingsExport {
        format: "donkey-runtime-settings".into(),
        version: 1,
        settings: RuntimeSettingsInput {
            chunk_size: config.chunk_size,
            chunk_concurrency: config.chunk_concurrency,
            parallel_threshold: config.parallel_threshold,
            resumable_threshold: config.resumable_threshold,
            scheduler_policy: config.scheduler_policy.to_string(),
            upstream_timeout_seconds: config.upstream_timeout.as_secs(),
            stream_fallback_timeout_seconds: config.stream_fallback_timeout.as_secs(),
            partial_ttl_seconds: config.partial_ttl.as_secs(),
            max_cache_bytes: config.max_cache_bytes,
            cache_policy: config.cache_policy.to_string(),
            cache_high_watermark: config.cache_high_watermark,
            cache_low_watermark: config.cache_low_watermark,
            cache_ttl_seconds: config.cache_ttl.map(|value| value.as_secs()),
            health_interval_seconds: config.health_interval.as_secs(),
            max_export_bytes: config.max_export_bytes,
            export_ttl_seconds: config.export_ttl.as_secs(),
        },
        registry_routes: state.registry_routes.list().await?.into_iter().collect(),
        nodes,
    }))
}

async fn import_runtime(
    State(state): State<AppState>,
    Json(export): Json<RuntimeSettingsExport>,
) -> ApiResult<Json<RuntimeConfig>> {
    if export.format != "donkey-runtime-settings" || export.version != 1 {
        return Err(crate::error::AppError::bad_request(
            "unsupported settings export format",
        ));
    }
    let mut route_ids = HashMap::new();
    for route in export.registry_routes {
        let input = crate::registry_routes::RegistryRouteInput {
            key: route.key.clone(),
            name: route.name,
            canonical_registry: route.canonical_registry,
            path_prefix: route.path_prefix,
            repository_mode: route.repository_mode,
            is_default: route.is_default,
            enabled: route.enabled,
        };
        let result = if let Some(existing) = state
            .registry_routes
            .list()
            .await?
            .into_iter()
            .find(|item| item.key == route.key)
        {
            state.registry_routes.update(existing.id, input).await?
        } else {
            state.registry_routes.create(input).await?
        };
        route_ids.insert(result.key, result.id);
    }
    let available_routes = state.registry_routes.list().await?;
    for node in export.nodes {
        if node.auth_mode != "none" {
            return Err(crate::error::AppError::bad_request(format!(
                "node '{}' requires credentials after import",
                node.name
            )));
        }
        let route_id = route_ids
            .get(&node.registry_route)
            .copied()
            .or_else(|| {
                available_routes
                    .iter()
                    .find(|route| route.key == node.registry_route)
                    .map(|route| route.id)
            })
            .ok_or_else(|| {
                crate::error::AppError::bad_request(format!(
                    "unknown Registry route '{}'",
                    node.registry_route
                ))
            })?;
        let input = NodeInput {
            name: node.name.clone(),
            url: node.url.clone(),
            registry_route_id: route_id,
            enabled: node.enabled,
            priority: node.priority,
            max_concurrency: node.max_concurrency,
            cf_preferred: node.cf_preferred,
            connect_ip: node.connect_ip,
            auth_mode: "none".into(),
            auth_username: None,
            auth_header: None,
            auth_secret: None,
        };
        let existing = state
            .nodes
            .list()
            .await?
            .into_iter()
            .find(|item| item.node.url == node.url && item.node.registry_route_id == route_id);
        if let Some(existing) = existing {
            state.nodes.update(existing.node.id, input).await?;
        } else {
            state.nodes.create(input).await?;
        }
    }
    persist_runtime(&state, &export.settings).await?;
    let cache = state.cache.stats().await?;
    Ok(Json(runtime_config(
        &effective_config(&state).await?,
        cache,
    )))
}

async fn persist_runtime(state: &AppState, input: &RuntimeSettingsInput) -> ApiResult<()> {
    if !(256 * 1024..=32 * 1024 * 1024).contains(&input.chunk_size)
        || !(1..=64).contains(&input.chunk_concurrency)
        || !(1024 * 1024..=u64::MAX).contains(&input.parallel_threshold)
        || !(1024 * 1024..=u64::MAX).contains(&input.resumable_threshold)
        || !(1..=3600).contains(&input.upstream_timeout_seconds)
        || !(1..=3600).contains(&input.stream_fallback_timeout_seconds)
        || !(60..=7 * 24 * 3600).contains(&input.partial_ttl_seconds)
        || !(64 * 1024 * 1024..=u64::MAX).contains(&input.max_cache_bytes)
        || !(0.5..=1.0).contains(&input.cache_high_watermark)
        || !(0.1..=0.99).contains(&input.cache_low_watermark)
        || input.cache_low_watermark >= input.cache_high_watermark
        || !(1..=86400).contains(&input.health_interval_seconds)
        || input.max_export_bytes < 64 * 1024 * 1024
        || !(60..=365 * 24 * 3600).contains(&input.export_ttl_seconds)
    {
        return Err(crate::error::AppError::bad_request(
            "runtime settings are out of range",
        ));
    }
    if !matches!(input.scheduler_policy.as_str(), "balanced" | "speed-first")
        || !matches!(input.cache_policy.as_str(), "balanced" | "lru" | "lfu")
    {
        return Err(crate::error::AppError::bad_request(
            "runtime settings contain an invalid policy",
        ));
    }
    let values = vec![
        ("chunk_size", input.chunk_size.to_string()),
        ("chunk_concurrency", input.chunk_concurrency.to_string()),
        ("parallel_threshold", input.parallel_threshold.to_string()),
        ("resumable_threshold", input.resumable_threshold.to_string()),
        ("scheduler_policy", input.scheduler_policy.clone()),
        (
            "upstream_timeout_seconds",
            input.upstream_timeout_seconds.to_string(),
        ),
        (
            "stream_fallback_timeout_seconds",
            input.stream_fallback_timeout_seconds.to_string(),
        ),
        ("partial_ttl_seconds", input.partial_ttl_seconds.to_string()),
        ("max_cache_bytes", input.max_cache_bytes.to_string()),
        ("cache_policy", input.cache_policy.clone()),
        (
            "cache_high_watermark",
            input.cache_high_watermark.to_string(),
        ),
        ("cache_low_watermark", input.cache_low_watermark.to_string()),
        (
            "cache_ttl_seconds",
            input.cache_ttl_seconds.unwrap_or(0).to_string(),
        ),
        (
            "health_interval_seconds",
            input.health_interval_seconds.to_string(),
        ),
        ("max_export_bytes", input.max_export_bytes.to_string()),
        ("export_ttl_seconds", input.export_ttl_seconds.to_string()),
    ];
    let values = values
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect::<Vec<_>>();
    db::replace_runtime_settings(&state.db, &values).await?;
    Ok(())
}

async fn effective_config(
    state: &AppState,
) -> Result<crate::config::Config, crate::error::AppError> {
    let mut config = (*state.config).clone();
    let persisted = db::load_runtime_settings(&state.db).await?;
    config
        .apply_runtime_overrides(&persisted)
        .map_err(crate::error::AppError::Internal)?;
    Ok(config)
}

fn runtime_config(
    config: &crate::config::Config,
    cache: crate::cache::CacheStats,
) -> RuntimeConfig {
    RuntimeConfig {
        admin_addr: config.admin_addr.to_string(),
        registry_addr: config.registry_addr.to_string(),
        tls_enabled: config.tls_cert.is_some(),
        private_upstreams: config.allow_private_upstreams,
        chunk_size: config.chunk_size,
        chunk_concurrency: config.chunk_concurrency,
        parallel_threshold: config.parallel_threshold,
        resumable_threshold: config.resumable_threshold,
        upstream_timeout_seconds: config.upstream_timeout.as_secs(),
        stream_fallback_timeout_seconds: config.stream_fallback_timeout.as_secs(),
        partial_ttl_seconds: config.partial_ttl.as_secs(),
        health_interval_seconds: config.health_interval.as_secs(),
        max_export_bytes: config.max_export_bytes,
        export_ttl_seconds: config.export_ttl.as_secs(),
        scheduler_policy: config.scheduler_policy.to_string(),
        max_cache_bytes: config.max_cache_bytes,
        cache_used_bytes: cache.bytes,
        cache_entries: cache.entries,
        cache_policy: config.cache_policy.to_string(),
        cache_high_watermark: config.cache_high_watermark,
        cache_low_watermark: config.cache_low_watermark,
        cache_ttl_seconds: config.cache_ttl.map(|value| value.as_secs()),
        admin_external_tls: config.admin_external_tls,
        admin_external_loopback: config.admin_external_loopback,
        registry_external_tls: config.registry_external_tls,
        registry_auth_enabled: config.registry_auth.is_some(),
    }
}
