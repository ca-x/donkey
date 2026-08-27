use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post, put},
};
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, DbBackend, EntityTrait, IntoActiveModel, Statement,
    TransactionTrait,
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
        .route(
            "/pull-events",
            get(list_pull_events).delete(clear_pull_events),
        )
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

#[derive(Serialize)]
struct PullEventView {
    id: Uuid,
    registry_route_id: Uuid,
    repository: String,
    reference: String,
    resolved_digest: Option<String>,
    status_code: i32,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl From<db::pull_event::Model> for PullEventView {
    fn from(event: db::pull_event::Model) -> Self {
        Self {
            id: event.id,
            registry_route_id: event.registry_route_id,
            repository: event.repository,
            reference: event.reference,
            resolved_digest: event.resolved_digest,
            status_code: event.status_code,
            created_at: event.created_at,
        }
    }
}

async fn list_pull_events(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Json<Vec<PullEventView>>> {
    Ok(Json(
        db::list_pull_events(&state.db, query.limit)
            .await?
            .into_iter()
            .map(Into::into)
            .collect(),
    ))
}

async fn clear_pull_events(State(state): State<AppState>) -> ApiResult<Json<serde_json::Value>> {
    let deleted = db::clear_pull_events(&state.db).await?;
    Ok(Json(serde_json::json!({ "deleted": deleted })))
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
    pull_logging_enabled: bool,
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
    pull_logging_enabled: bool,
}

#[derive(Serialize, Deserialize)]
struct RuntimeSettingsExport {
    format: String,
    version: u32,
    settings: Option<RuntimeSettingsInput>,
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
    let effective = effective_config(&state).await?;
    state.apply_runtime_config(&effective).await;
    let cache = state.cache.stats().await?;
    Ok(Json(runtime_config(&effective, cache)))
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
        settings: Some(RuntimeSettingsInput {
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
            pull_logging_enabled: config.pull_logging_enabled,
        }),
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
    if let Some(settings) = export.settings.as_ref() {
        validate_runtime_settings(settings)?;
    }
    validate_import_snapshot(&state, &export).await?;
    apply_runtime_snapshot(&state, &export).await?;
    let effective = effective_config(&state).await?;
    state.apply_runtime_config(&effective).await;
    let cache = state.cache.stats().await?;
    Ok(Json(runtime_config(&effective, cache)))
}

async fn validate_import_snapshot(
    state: &AppState,
    export: &RuntimeSettingsExport,
) -> ApiResult<()> {
    let mut keys = std::collections::HashSet::new();
    for route in &export.registry_routes {
        if !keys.insert(route.key.as_str()) {
            return Err(crate::error::AppError::bad_request(format!(
                "duplicate Registry route key '{}'",
                route.key
            )));
        }
        if route.key.trim().is_empty() || route.canonical_registry.trim().is_empty() {
            return Err(crate::error::AppError::bad_request(
                "Registry route key and canonical registry are required",
            ));
        }
    }
    let existing = state.registry_routes.list().await?;
    for node in &export.nodes {
        if node.auth_mode != "none" {
            return Err(crate::error::AppError::bad_request(format!(
                "node '{}' requires credentials after import",
                node.name
            )));
        }
        if !(1..=256).contains(&node.max_concurrency) {
            return Err(crate::error::AppError::bad_request(format!(
                "node '{}' has an invalid concurrency limit",
                node.name
            )));
        }
        if !keys.contains(node.registry_route.as_str())
            && !existing
                .iter()
                .any(|route| route.key == node.registry_route)
        {
            return Err(crate::error::AppError::bad_request(format!(
                "unknown Registry route '{}'",
                node.registry_route
            )));
        }
    }
    Ok(())
}

struct PreparedRoute {
    model: db::registry_route::Model,
    exists: bool,
}

struct PreparedNode {
    model: db::node::Model,
    max_concurrency: u16,
    exists: bool,
}

async fn apply_runtime_snapshot(state: &AppState, export: &RuntimeSettingsExport) -> ApiResult<()> {
    let now = chrono::Utc::now();
    let existing_routes = db::registry_route::Entity::find().all(&state.db).await?;
    let mut route_ids = existing_routes
        .iter()
        .map(|route| (route.key.clone(), route.id))
        .collect::<HashMap<_, _>>();
    let mut routes = Vec::with_capacity(export.registry_routes.len());
    for route in &export.registry_routes {
        let normalized = crate::registry_routes::normalize_input(RegistryRouteInput {
            key: route.key.clone(),
            name: route.name.clone(),
            canonical_registry: route.canonical_registry.clone(),
            path_prefix: route.path_prefix.clone(),
            repository_mode: route.repository_mode.clone(),
            is_default: route.is_default,
            enabled: route.enabled,
        })?;
        let existing = existing_routes
            .iter()
            .find(|candidate| candidate.key == normalized.key);
        let id = existing.map_or_else(Uuid::new_v4, |route| route.id);
        let created_at = existing.map_or(now, |route| route.created_at);
        route_ids.insert(normalized.key.clone(), id);
        routes.push(PreparedRoute {
            model: db::registry_route::Model {
                id,
                key: normalized.key,
                name: normalized.name,
                canonical_registry: normalized.canonical_registry,
                path_prefix: normalized.path_prefix,
                repository_mode: normalized.repository_mode.as_str().to_owned(),
                is_default: normalized.is_default,
                enabled: normalized.enabled,
                created_at,
                updated_at: now,
            },
            exists: existing.is_some(),
        });
    }

    let existing_nodes = db::list_nodes(&state.db).await?;
    let mut nodes = Vec::with_capacity(export.nodes.len());
    for node in &export.nodes {
        let route_id = route_ids
            .get(&node.registry_route)
            .copied()
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
            connect_ip: node.connect_ip.clone(),
            auth_mode: "none".into(),
            auth_username: None,
            auth_header: None,
            auth_secret: None,
        };
        crate::nodes::validate_input(&input)?;
        if let Some(ip) = input.connect_ip.as_deref() {
            ip.parse::<std::net::IpAddr>().map_err(|_| {
                crate::error::AppError::bad_request("connect_ip must be an IP address")
            })?;
        }
        let validated = crate::security::validate_upstream(&input.url, &state.config).await?;
        let canonical = validated.url.to_string();
        let existing = existing_nodes.iter().find(|candidate| {
            candidate.registry_route_id == route_id && candidate.url == canonical
        });
        nodes.push(PreparedNode {
            model: db::node::Model {
                id: existing.map_or_else(Uuid::new_v4, |node| node.id),
                name: input.name.trim().to_owned(),
                url: canonical,
                registry_route_id: route_id,
                enabled: input.enabled,
                priority: input.priority,
                cf_preferred: input.cf_preferred,
                connect_ip: input.connect_ip,
                auth_mode: "none".into(),
                auth_username: None,
                auth_header: None,
                auth_secret_enc: None,
                created_at: existing.map_or(now, |node| node.created_at),
                updated_at: now,
            },
            max_concurrency: input.max_concurrency,
            exists: existing.is_some(),
        });
    }

    let transaction = state.db.begin().await?;
    let result =
        apply_prepared_snapshot(&transaction, &routes, &nodes, export.settings.as_ref()).await;
    match result {
        Ok(()) => transaction.commit().await?,
        Err(error) => {
            transaction.rollback().await?;
            return Err(error);
        }
    }
    Ok(())
}

async fn apply_prepared_snapshot(
    transaction: &sea_orm::DatabaseTransaction,
    routes: &[PreparedRoute],
    nodes: &[PreparedNode],
    settings: Option<&RuntimeSettingsInput>,
) -> ApiResult<()> {
    if routes.iter().any(|route| route.model.is_default) {
        transaction
            .execute_unprepared("UPDATE registry_routes SET is_default = 0")
            .await?;
    }
    for route in routes {
        transaction
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE registry_routes SET path_prefix = NULL WHERE id = ?",
                [route.model.id.into()],
            ))
            .await?;
    }
    for route in routes {
        if route.exists {
            route
                .model
                .clone()
                .into_active_model()
                .update(transaction)
                .await?;
        } else {
            route
                .model
                .clone()
                .into_active_model()
                .insert(transaction)
                .await?;
        }
    }
    for node in nodes {
        if node.exists {
            node.model
                .clone()
                .into_active_model()
                .update(transaction)
                .await?;
        } else {
            node.model
                .clone()
                .into_active_model()
                .insert(transaction)
                .await?;
            db::node_metric::Model {
                node_id: node.model.id,
                healthy: true,
                latency_ms: 0,
                speed_bps: 0,
                success_rate: 1.0,
                current_bps: 0,
                total_bytes: 0,
                last_checked_at: None,
                last_error: None,
            }
            .into_active_model()
            .insert(transaction)
            .await?;
        }
        transaction
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO node_limits(node_id, max_concurrency) VALUES (?, ?) ON CONFLICT(node_id) DO UPDATE SET max_concurrency = excluded.max_concurrency",
                [
                    node.model.id.to_string().into(),
                    i64::from(node.max_concurrency).into(),
                ],
            ))
            .await?;
    }
    if let Some(settings) = settings {
        transaction
            .execute_unprepared("DELETE FROM runtime_settings")
            .await?;
        for (key, value) in runtime_setting_values(settings) {
            transaction
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "INSERT INTO runtime_settings(key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    [key.into(), value.into()],
                ))
                .await?;
        }
    }
    Ok(())
}

fn validate_runtime_settings(input: &RuntimeSettingsInput) -> ApiResult<()> {
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
    Ok(())
}

async fn persist_runtime(state: &AppState, input: &RuntimeSettingsInput) -> ApiResult<()> {
    validate_runtime_settings(input)?;
    db::replace_runtime_settings(&state.db, &runtime_setting_values(input)).await?;
    Ok(())
}

fn runtime_setting_values(input: &RuntimeSettingsInput) -> Vec<(String, String)> {
    vec![
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
        (
            "pull_logging_enabled",
            input.pull_logging_enabled.to_string(),
        ),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value))
    .collect()
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
        pull_logging_enabled: config.pull_logging_enabled,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runtime_snapshot_rolls_back_all_writes_on_node_conflict() {
        let directory = tempfile::tempdir().unwrap();
        let state = AppState::new(crate::Config::for_test(directory.path().to_owned()))
            .await
            .unwrap();
        let node = ExportNode {
            name: "duplicate import".into(),
            url: "http://127.0.0.1:5000/".into(),
            registry_route: crate::registry_routes::DOCKER_HUB_ROUTE_KEY.into(),
            enabled: true,
            priority: 1,
            max_concurrency: 4,
            cf_preferred: false,
            connect_ip: None,
            auth_mode: "none".into(),
            auth_username: None,
            auth_header: None,
        };
        let export = RuntimeSettingsExport {
            format: "donkey-runtime-settings".into(),
            version: 1,
            settings: None,
            registry_routes: Vec::new(),
            nodes: vec![node.clone(), node],
        };

        validate_import_snapshot(&state, &export).await.unwrap();
        assert!(apply_runtime_snapshot(&state, &export).await.is_err());
        assert!(db::list_nodes(&state.db).await.unwrap().is_empty());
        assert!(
            db::load_runtime_settings(&state.db)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn runtime_snapshot_commits_routes_nodes_and_settings_together() {
        let directory = tempfile::tempdir().unwrap();
        let state = AppState::new(crate::Config::for_test(directory.path().to_owned()))
            .await
            .unwrap();
        let mut export = export_runtime(State(state.clone())).await.unwrap().0;
        export.nodes.push(ExportNode {
            name: "atomic node".into(),
            url: "http://127.0.0.1:5001/".into(),
            registry_route: crate::registry_routes::DOCKER_HUB_ROUTE_KEY.into(),
            enabled: true,
            priority: 2,
            max_concurrency: 3,
            cf_preferred: false,
            connect_ip: None,
            auth_mode: "none".into(),
            auth_username: None,
            auth_header: None,
        });

        validate_import_snapshot(&state, &export).await.unwrap();
        apply_runtime_snapshot(&state, &export).await.unwrap();

        let nodes = state.nodes.list().await.unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].node.name, "atomic node");
        assert_eq!(nodes[0].max_concurrency, 3);
        assert_eq!(state.registry_routes.list().await.unwrap().len(), 2);
        assert!(
            !db::load_runtime_settings(&state.db)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
