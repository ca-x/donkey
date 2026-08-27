use std::{sync::Arc, time::Instant};

use chrono::Utc;
use http::{HeaderName, HeaderValue};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, ExprTrait, QueryFilter, sea_query::Expr,
};
use serde::{Deserialize, Serialize, Serializer};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::{
    config::{Config, SchedulerPolicy},
    crypto::CredentialCipher,
    db::{self, node, node_metric},
    error::{ApiResult, AppError},
    registry_routes::RegistryRouteSummary,
    security,
};

const NODE_ROUTE_CONFLICT: &str = "Registry route changed or no longer exists";
const NODE_URL_CONFLICT: &str = "node URL already exists in this Registry route";

#[derive(Clone)]
pub struct NodeService {
    config: Arc<Config>,
    runtime: Arc<RwLock<NodeRuntimeConfig>>,
    db: DatabaseConnection,
    cipher: CredentialCipher,
}

#[derive(Clone, Copy)]
struct NodeRuntimeConfig {
    scheduler_policy: SchedulerPolicy,
    upstream_timeout: std::time::Duration,
    health_interval: std::time::Duration,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeInput {
    pub name: String,
    pub url: String,
    pub registry_route_id: Uuid,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_priority")]
    pub priority: i32,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: u16,
    #[serde(default)]
    pub cf_preferred: bool,
    pub connect_ip: Option<String>,
    #[serde(default)]
    pub auth_mode: String,
    pub auth_username: Option<String>,
    pub auth_header: Option<String>,
    pub auth_secret: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NodeView {
    pub node: node::Model,
    pub metric: node_metric::Model,
    pub score: f64,
    pub auth_configured: bool,
    pub route: RegistryRouteSummary,
    pub max_concurrency: u16,
}

#[derive(Serialize)]
struct NodeViewWire<'a> {
    node: NodeSafeView<'a>,
    metric: &'a node_metric::Model,
    score: f64,
    auth_configured: bool,
    max_concurrency: u16,
    route: &'a RegistryRouteSummary,
}

#[derive(Serialize)]
struct NodeSafeView<'a> {
    id: &'a Uuid,
    name: &'a str,
    url: &'a str,
    registry_route_id: &'a Uuid,
    enabled: bool,
    priority: i32,
    cf_preferred: bool,
    connect_ip: &'a Option<String>,
    auth_mode: &'a str,
    auth_username: &'a Option<String>,
    auth_header: &'a Option<String>,
    created_at: &'a chrono::DateTime<Utc>,
    updated_at: &'a chrono::DateTime<Utc>,
}

impl Serialize for NodeView {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        NodeViewWire {
            node: NodeSafeView {
                id: &self.node.id,
                name: &self.node.name,
                url: &self.node.url,
                registry_route_id: &self.node.registry_route_id,
                enabled: self.node.enabled,
                priority: self.node.priority,
                cf_preferred: self.node.cf_preferred,
                connect_ip: &self.node.connect_ip,
                auth_mode: &self.node.auth_mode,
                auth_username: &self.node.auth_username,
                auth_header: &self.node.auth_header,
                created_at: &self.node.created_at,
                updated_at: &self.node.updated_at,
            },
            metric: &self.metric,
            score: self.score,
            auth_configured: self.auth_configured,
            max_concurrency: self.max_concurrency,
            route: &self.route,
        }
        .serialize(serializer)
    }
}

fn default_true() -> bool {
    true
}

fn default_priority() -> i32 {
    100
}

fn default_max_concurrency() -> u16 {
    4
}

impl NodeService {
    pub fn new(config: Arc<Config>, db: DatabaseConnection) -> ApiResult<Self> {
        let cipher = CredentialCipher::from_config(&config)?;
        Ok(Self {
            runtime: Arc::new(RwLock::new(NodeRuntimeConfig {
                scheduler_policy: config.scheduler_policy,
                upstream_timeout: config.upstream_timeout,
                health_interval: config.health_interval,
            })),
            config,
            db,
            cipher,
        })
    }

    pub async fn update_runtime(&self, config: &Config) {
        let mut runtime = self.runtime.write().await;
        runtime.scheduler_policy = config.scheduler_policy;
        runtime.upstream_timeout = config.upstream_timeout;
        runtime.health_interval = config.health_interval;
    }

    pub async fn health_interval(&self) -> std::time::Duration {
        self.runtime.read().await.health_interval
    }

    pub async fn list(&self) -> ApiResult<Vec<NodeView>> {
        let mut views = Vec::new();
        for node in db::list_nodes(&self.db).await? {
            views.push(self.view(node).await?);
        }
        views.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.node.priority.cmp(&b.node.priority))
                .then_with(|| a.node.url.cmp(&b.node.url))
        });
        Ok(views)
    }

    pub async fn enabled_registry_nodes(
        &self,
        registry_route_id: Uuid,
    ) -> ApiResult<Vec<NodeView>> {
        let mut views = Vec::new();
        for node in db::list_nodes_for_route(&self.db, registry_route_id)
            .await?
            .into_iter()
            .filter(|node| node.enabled)
        {
            views.push(self.view(node).await?);
        }
        Ok(views)
    }

    pub async fn create(&self, input: NodeInput) -> ApiResult<NodeView> {
        validate_input(&input)?;
        self.require_registry_auth_for_secret(&input.auth_mode)?;
        let route = crate::db::registry_route::Entity::find_by_id(input.registry_route_id)
            .one(&self.db)
            .await?
            .ok_or_else(|| AppError::conflict(NODE_ROUTE_CONFLICT))?;
        let validated = security::validate_upstream(&input.url, &self.config).await?;
        let canonical = validated.url.to_string();
        if db::get_node_by_route_and_url(&self.db, route.id, &canonical)
            .await?
            .is_some()
        {
            return Err(AppError::conflict(NODE_URL_CONFLICT));
        }
        if let Some(ip) = &input.connect_ip {
            ip.parse::<std::net::IpAddr>()
                .map_err(|_| AppError::bad_request("connect_ip must be an IP address"))?;
        }
        let auth_secret_enc = self.seal_secret(&input.auth_mode, input.auth_secret.as_deref())?;
        let now = Utc::now();
        let node = node::Model {
            id: Uuid::new_v4(),
            name: input.name.trim().to_owned(),
            url: canonical,
            registry_route_id: route.id,
            enabled: input.enabled,
            priority: input.priority,
            cf_preferred: input.cf_preferred,
            connect_ip: input.connect_ip,
            auth_mode: normalized_auth_mode(&input.auth_mode)?.to_owned(),
            auth_username: trimmed(input.auth_username),
            auth_header: normalized_header(input.auth_header)?,
            auth_secret_enc,
            created_at: now,
            updated_at: now,
        };
        let node = db::insert_node(&self.db, node)
            .await
            .map_err(map_node_write_error)?;
        db::set_node_max_concurrency(&self.db, node.id, input.max_concurrency).await?;
        let metric = empty_metric(node.id);
        db::upsert_metric(&self.db, metric.clone()).await?;
        self.view_with(node, metric, route).await
    }

    pub async fn update(&self, id: Uuid, input: NodeInput) -> ApiResult<NodeView> {
        validate_input(&input)?;
        self.require_registry_auth_for_secret(&input.auth_mode)?;
        let route = crate::db::registry_route::Entity::find_by_id(input.registry_route_id)
            .one(&self.db)
            .await?
            .ok_or_else(|| AppError::conflict(NODE_ROUTE_CONFLICT))?;
        let validated = security::validate_upstream(&input.url, &self.config).await?;
        if let Some(ip) = &input.connect_ip {
            ip.parse::<std::net::IpAddr>()
                .map_err(|_| AppError::bad_request("connect_ip must be an IP address"))?;
        }
        let mut node = db::get_node(&self.db, id)
            .await?
            .ok_or_else(|| AppError::not_found("node"))?;
        let canonical = validated.url.to_string();
        if db::get_node_by_route_and_url(&self.db, route.id, &canonical)
            .await?
            .is_some_and(|existing| existing.id != id)
        {
            return Err(AppError::conflict(NODE_URL_CONFLICT));
        }
        node.name = input.name.trim().to_owned();
        node.url = canonical;
        node.registry_route_id = route.id;
        node.enabled = input.enabled;
        node.priority = input.priority;
        node.cf_preferred = input.cf_preferred;
        node.connect_ip = input.connect_ip;
        node.auth_mode = normalized_auth_mode(&input.auth_mode)?.to_owned();
        node.auth_username = trimmed(input.auth_username);
        node.auth_header = normalized_header(input.auth_header)?;
        if node.auth_mode == "none" {
            node.auth_secret_enc = None;
        } else if let Some(secret) = input.auth_secret.as_deref() {
            node.auth_secret_enc = self.seal_secret(&node.auth_mode, Some(secret))?;
        }
        node.updated_at = Utc::now();
        let node = db::save_node(&self.db, node)
            .await
            .map_err(map_node_write_error)?;
        db::set_node_max_concurrency(&self.db, id, input.max_concurrency).await?;
        let metric = db::metric_for(&self.db, id)
            .await?
            .unwrap_or_else(|| empty_metric(id));
        self.view_with(node, metric, route).await
    }

    pub async fn delete(&self, id: Uuid) -> ApiResult<()> {
        if db::delete_node(&self.db, id).await? == 0 {
            return Err(AppError::not_found("node"));
        }
        Ok(())
    }

    pub async fn oci_auth_for_node(
        &self,
        id: Uuid,
    ) -> ApiResult<(node::Model, oci_client::secrets::RegistryAuth)> {
        let node = db::get_node(&self.db, id)
            .await?
            .ok_or_else(|| AppError::not_found("node"))?;
        let auth = match node.auth_mode.as_str() {
            "none" => oci_client::secrets::RegistryAuth::Anonymous,
            "basic" => {
                let username = node.auth_username.clone().ok_or_else(|| {
                    AppError::bad_request("Basic node credential has no username")
                })?;
                let secret = self.decrypt_node_secret(&node)?;
                oci_client::secrets::RegistryAuth::Basic(username, secret)
            }
            "bearer" => oci_client::secrets::RegistryAuth::Bearer(self.decrypt_node_secret(&node)?),
            _ => {
                return Err(AppError::bad_request(
                    "custom-header nodes cannot be used by image tools",
                ));
            }
        };
        Ok((node, auth))
    }

    pub async fn registry_node(&self, id: Uuid) -> ApiResult<NodeView> {
        let node = db::get_node(&self.db, id)
            .await?
            .ok_or_else(|| AppError::not_found("node"))?;
        let view = self.view(node).await?;
        if !view.node.enabled || !view.route.enabled {
            return Err(AppError::bad_request(
                "selected image source node or its Registry route is disabled",
            ));
        }
        Ok(view)
    }

    pub async fn probe(&self, id: Uuid) -> ApiResult<NodeView> {
        let node = db::get_node(&self.db, id)
            .await?
            .ok_or_else(|| AppError::not_found("node"))?;
        let metric = self.probe_model(&node).await;
        db::upsert_metric(&self.db, metric.clone()).await?;
        let route = crate::db::registry_route::Entity::find_by_id(node.registry_route_id)
            .one(&self.db)
            .await?
            .ok_or_else(|| AppError::internal(anyhow::anyhow!("node has no Registry route")))?;
        self.view_with(node, metric, route).await
    }

    async fn view(&self, node: node::Model) -> ApiResult<NodeView> {
        let route = crate::db::registry_route::Entity::find_by_id(node.registry_route_id)
            .one(&self.db)
            .await?
            .ok_or_else(|| AppError::internal(anyhow::anyhow!("node has no Registry route")))?;
        let metric = db::metric_for(&self.db, node.id)
            .await?
            .unwrap_or_else(|| empty_metric(node.id));
        self.view_with(node, metric, route).await
    }

    async fn view_with(
        &self,
        node: node::Model,
        metric: node_metric::Model,
        route: crate::db::registry_route::Model,
    ) -> ApiResult<NodeView> {
        let policy = self.runtime.read().await.scheduler_policy;
        let score = score(&node, &metric, policy);
        let auth_configured = node.auth_secret_enc.is_some();
        let max_concurrency = db::get_node_max_concurrency(&self.db, node.id).await?;
        Ok(NodeView {
            node,
            metric,
            score,
            auth_configured,
            route: (&route).into(),
            max_concurrency,
        })
    }

    pub async fn probe_all(&self) {
        let Ok(nodes) = db::list_nodes(&self.db).await else {
            return;
        };
        let mut jobs = JoinSet::new();
        for node in nodes.into_iter().filter(|node| node.enabled) {
            let service = self.clone();
            jobs.spawn(async move { (node.id, service.probe_model(&node).await) });
        }
        while let Some(Ok((_, metric))) = jobs.join_next().await {
            if let Err(error) = db::upsert_metric(&self.db, metric).await {
                tracing::warn!(?error, "failed to store node probe");
            }
        }
    }

    async fn probe_model(&self, node: &node::Model) -> node_metric::Model {
        let started = Instant::now();
        let result = async {
            let upstream = security::validate_upstream(&node.url, &self.config).await?;
            let timeout = self.runtime.read().await.upstream_timeout;
            let client = security::client_for(&upstream, timeout)?;
            let url = upstream.url.join("v2/").map_err(AppError::internal)?;
            let request = self.apply_auth(client.get(url), node)?;
            let response = request
                .send()
                .await
                .map_err(|error| AppError::Upstream(error.to_string()))?;
            let status = response.status();
            let bearer_challenge = status == reqwest::StatusCode::UNAUTHORIZED
                && response
                    .headers()
                    .get(http::header::WWW_AUTHENTICATE)
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| value.to_ascii_lowercase().starts_with("bearer "));
            if !status.is_success() && !bearer_challenge {
                return Err(AppError::Upstream(format!("status {status}")));
            }
            Ok::<_, AppError>(())
        }
        .await;
        let previous = db::metric_for(&self.db, node.id)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| empty_metric(node.id));
        let healthy = result.is_ok();
        let sample = if healthy { 1.0 } else { 0.0 };
        node_metric::Model {
            node_id: node.id,
            healthy,
            latency_ms: started.elapsed().as_millis().min(i64::MAX as u128) as i64,
            speed_bps: previous.speed_bps,
            success_rate: previous.success_rate * 0.8 + sample * 0.2,
            current_bps: previous.current_bps,
            total_bytes: previous.total_bytes,
            last_checked_at: Some(Utc::now()),
            last_error: result
                .err()
                .map(|error| error.to_string())
                .map(|value| truncate(&value, 240)),
        }
    }

    pub async fn record_transfer(
        &self,
        id: Uuid,
        bytes: u64,
        elapsed: std::time::Duration,
        success: bool,
    ) {
        let sample = if success { 1.0_f64 } else { 0.0_f64 };
        let mut update = node_metric::Entity::update_many()
            .col_expr(
                node_metric::Column::SuccessRate,
                Expr::col(node_metric::Column::SuccessRate)
                    .mul(0.85)
                    .add(sample * 0.15),
            )
            .col_expr(node_metric::Column::LastCheckedAt, Expr::value(Utc::now()))
            .filter(node_metric::Column::NodeId.eq(id));
        if success && elapsed.as_secs_f64() > 0.0 {
            let bps = (bytes as f64 / elapsed.as_secs_f64()).min(i64::MAX as f64) as i64;
            let byte_count = std::cmp::min(bytes, i64::MAX as u64) as i64;
            update = update
                .col_expr(node_metric::Column::Healthy, Expr::value(true))
                .col_expr(node_metric::Column::CurrentBps, Expr::value(bps))
                .col_expr(node_metric::Column::SpeedBps, Expr::value(bps))
                .col_expr(
                    node_metric::Column::TotalBytes,
                    Expr::col(node_metric::Column::TotalBytes).add(byte_count),
                );
        }
        if let Err(error) = update.exec(&self.db).await {
            tracing::warn!(?error, "failed to update node transfer metric");
        }
    }

    pub fn apply_auth(
        &self,
        request: reqwest::RequestBuilder,
        node: &node::Model,
    ) -> ApiResult<reqwest::RequestBuilder> {
        if node.auth_mode == "none" {
            return Ok(request);
        }
        let encrypted = node.auth_secret_enc.as_deref().ok_or_else(|| {
            AppError::bad_request("upstream authentication is configured without a secret")
        })?;
        let secret = self.cipher.decrypt(encrypted)?;
        match node.auth_mode.as_str() {
            "basic" => {
                let username = node.auth_username.as_deref().ok_or_else(|| {
                    AppError::bad_request("Basic authentication requires a username")
                })?;
                Ok(request.basic_auth(username, Some(secret)))
            }
            "bearer" => Ok(request.bearer_auth(secret)),
            "header" => {
                let name = node
                    .auth_header
                    .as_deref()
                    .ok_or_else(|| {
                        AppError::bad_request("custom header authentication requires a header name")
                    })?
                    .parse::<HeaderName>()
                    .map_err(|_| AppError::bad_request("invalid authentication header name"))?;
                let value = HeaderValue::from_str(&secret)
                    .map_err(|_| AppError::bad_request("invalid authentication header value"))?;
                Ok(request.header(name, value))
            }
            _ => Err(AppError::bad_request("unsupported authentication mode")),
        }
    }

    fn seal_secret(&self, mode: &str, secret: Option<&str>) -> ApiResult<Option<String>> {
        if mode == "none" {
            return Ok(None);
        }
        let secret = secret
            .filter(|value| !value.is_empty())
            .ok_or_else(|| AppError::bad_request("authentication secret is required"))?;
        if secret.len() > 4096 {
            return Err(AppError::bad_request("authentication secret is too long"));
        }
        self.cipher.encrypt(secret).map(Some)
    }

    fn require_registry_auth_for_secret(&self, mode: &str) -> ApiResult<()> {
        if mode != "none" && self.config.registry_auth_value().is_none() {
            return Err(AppError::bad_request(
                "DONKEY_REGISTRY_AUTH is required before adding an authenticated upstream",
            ));
        }
        Ok(())
    }

    fn decrypt_node_secret(&self, node: &node::Model) -> ApiResult<String> {
        let encrypted = node
            .auth_secret_enc
            .as_deref()
            .ok_or_else(|| AppError::bad_request("node credential has no secret"))?;
        self.cipher.decrypt(encrypted)
    }
}

fn map_node_write_error(error: sea_orm::DbErr) -> AppError {
    AppError::map_constraint(error, NODE_URL_CONFLICT, NODE_ROUTE_CONFLICT)
}

pub fn score(node: &node::Model, metric: &node_metric::Model, policy: SchedulerPolicy) -> f64 {
    if !node.enabled {
        return -1.0;
    }
    let health = if metric.healthy {
        1.0
    } else if metric.last_checked_at.is_none() {
        0.55
    } else {
        0.08
    };
    let success = metric.success_rate.clamp(0.05, 1.0);
    let priority = 1.0 / (1.0 + std::cmp::max(node.priority, 0) as f64 / 100.0);
    match policy {
        SchedulerPolicy::Balanced => {
            let speed = (std::cmp::max(metric.speed_bps, 1) as f64).ln_1p();
            let latency = 1.0 / (1.0 + std::cmp::max(metric.latency_ms, 0) as f64 / 250.0);
            health * (0.55 * speed + 2.0 * latency + priority) * success.max(0.25)
        }
        SchedulerPolicy::SpeedFirst => {
            let speed = std::cmp::max(metric.speed_bps, 1) as f64;
            health * success.powi(2) * speed + priority
        }
    }
}

fn empty_metric(node_id: Uuid) -> node_metric::Model {
    node_metric::Model {
        node_id,
        healthy: true,
        latency_ms: 0,
        speed_bps: 0,
        success_rate: 1.0,
        current_bps: 0,
        total_bytes: 0,
        last_checked_at: None,
        last_error: None,
    }
}

pub(crate) fn validate_input(input: &NodeInput) -> ApiResult<()> {
    if input.name.trim().is_empty() || input.name.chars().count() > 80 {
        return Err(AppError::bad_request(
            "name must be between 1 and 80 characters",
        ));
    }
    if !(0..=1000).contains(&input.priority) {
        return Err(AppError::bad_request("priority must be between 0 and 1000"));
    }
    if !(1..=64).contains(&input.max_concurrency) {
        return Err(AppError::bad_request(
            "max_concurrency must be between 1 and 64",
        ));
    }
    let mode = normalized_auth_mode(&input.auth_mode)?;
    if mode == "basic" && input.auth_username.as_deref().is_none_or(str::is_empty) {
        return Err(AppError::bad_request(
            "Basic authentication requires a username",
        ));
    }
    if mode == "header" && input.auth_header.as_deref().is_none_or(str::is_empty) {
        return Err(AppError::bad_request(
            "custom header authentication requires a header name",
        ));
    }
    Ok(())
}

fn normalized_auth_mode(mode: &str) -> ApiResult<&str> {
    let mode = if mode.is_empty() { "none" } else { mode };
    if matches!(mode, "none" | "basic" | "bearer" | "header") {
        Ok(mode)
    } else {
        Err(AppError::bad_request("unsupported authentication mode"))
    }
}

fn normalized_header(value: Option<String>) -> ApiResult<Option<String>> {
    let Some(value) = trimmed(value) else {
        return Ok(None);
    };
    let name = value
        .parse::<HeaderName>()
        .map_err(|_| AppError::bad_request("invalid authentication header name"))?;
    if matches!(
        name.as_str(),
        "authorization"
            | "host"
            | "proxy-authorization"
            | "cookie"
            | "connection"
            | "transfer-encoding"
    ) {
        return Err(AppError::bad_request(
            "this authentication header is not allowed",
        ));
    }
    Ok(Some(name.to_string()))
}

fn trimmed(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn truncate(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry_routes::{
        DOCKER_HUB_ROUTE_ID, GHCR_ROUTE_ID, RegistryRouteInput, RegistryRouteService,
    };
    use axum::http::StatusCode;
    use secrecy::SecretString;

    fn plain_node_input(registry_route_id: Uuid) -> NodeInput {
        NodeInput {
            name: "shared mirror".into(),
            url: "http://127.0.0.1:5000".into(),
            registry_route_id,
            enabled: true,
            priority: 10,
            max_concurrency: 4,
            cf_preferred: false,
            connect_ip: None,
            auth_mode: "none".into(),
            auth_username: None,
            auth_header: None,
            auth_secret: None,
        }
    }

    fn assert_one_success_one_conflict<T>(left: ApiResult<T>, right: ApiResult<T>) {
        let mut successes = 0;
        let mut conflicts = 0;
        for result in [left, right] {
            match result {
                Ok(_) => successes += 1,
                Err(error) => {
                    assert_eq!(error.status(), StatusCode::CONFLICT);
                    let message = error.to_string().to_ascii_lowercase();
                    assert!(!message.contains("sql"));
                    assert!(!message.contains("constraint"));
                    conflicts += 1;
                }
            }
        }
        assert_eq!(successes, 1);
        assert_eq!(conflicts, 1);
    }

    #[test]
    fn healthy_fast_node_scores_higher() {
        let now = Utc::now();
        let base = node::Model {
            id: Uuid::new_v4(),
            name: "node".into(),
            url: "https://registry.example/".into(),
            registry_route_id: DOCKER_HUB_ROUTE_ID,
            enabled: true,
            priority: 100,
            cf_preferred: false,
            connect_ip: None,
            auth_mode: "none".into(),
            auth_username: None,
            auth_header: None,
            auth_secret_enc: None,
            created_at: now,
            updated_at: now,
        };
        let mut slow = empty_metric(base.id);
        slow.speed_bps = 100_000;
        slow.latency_ms = 200;
        let mut fast = slow.clone();
        fast.speed_bps = 10_000_000;
        fast.latency_ms = 20;
        assert!(
            score(&base, &fast, SchedulerPolicy::Balanced)
                > score(&base, &slow, SchedulerPolicy::Balanced)
        );
        assert!(
            score(&base, &fast, SchedulerPolicy::SpeedFirst)
                > score(&base, &slow, SchedulerPolicy::SpeedFirst)
        );
    }

    #[tokio::test]
    async fn encrypts_upstream_secret_and_never_serializes_it() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.registry_auth = Some(SecretString::from("client:password"));
        config.credential_key = Some(SecretString::from("11".repeat(32)));
        let db = db::connect(&config.database_url).await.unwrap();
        let service = NodeService::new(Arc::new(config), db).unwrap();
        let view = service
            .create(NodeInput {
                name: "authenticated".into(),
                url: "http://127.0.0.1:5000".into(),
                registry_route_id: DOCKER_HUB_ROUTE_ID,
                enabled: true,
                priority: 10,
                max_concurrency: 4,
                cf_preferred: false,
                connect_ip: None,
                auth_mode: "basic".into(),
                auth_username: Some("1ms".into()),
                auth_header: None,
                auth_secret: Some("top-secret".into()),
            })
            .await
            .unwrap();
        assert!(view.node.auth_secret_enc.is_some());
        let json = serde_json::to_string(&view).unwrap();
        assert!(!json.contains("top-secret"));
        assert!(!json.contains(view.node.auth_secret_enc.as_deref().unwrap()));
        assert!(!json.contains("auth_secret_enc"));
        assert!(!json.contains("route_prefix"));
        assert!(!json.contains("\"kind\""));
        assert_eq!(view.route.id, DOCKER_HUB_ROUTE_ID);
    }

    #[tokio::test]
    async fn mirror_urls_are_unique_within_but_not_across_routes() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config::for_test(directory.path().to_owned());
        let db = db::connect(&config.database_url).await.unwrap();
        let service = NodeService::new(Arc::new(config), db).unwrap();
        service
            .create(plain_node_input(DOCKER_HUB_ROUTE_ID))
            .await
            .unwrap();
        service
            .create(plain_node_input(GHCR_ROUTE_ID))
            .await
            .unwrap();
        assert!(
            service
                .create(plain_node_input(DOCKER_HUB_ROUTE_ID))
                .await
                .is_err()
        );
        assert_eq!(
            service
                .enabled_registry_nodes(DOCKER_HUB_ROUTE_ID)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            service
                .enabled_registry_nodes(GHCR_ROUTE_ID)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn concurrent_node_create_conflict_is_stable() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config::for_test(directory.path().to_owned());
        let db = db::connect(&config.database_url).await.unwrap();
        let service = NodeService::new(Arc::new(config), db).unwrap();
        let (left, right) = tokio::join!(
            service.create(plain_node_input(DOCKER_HUB_ROUTE_ID)),
            service.create(plain_node_input(DOCKER_HUB_ROUTE_ID)),
        );
        assert_one_success_one_conflict(left, right);
    }

    #[tokio::test]
    async fn route_delete_racing_node_create_has_one_stable_conflict() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config::for_test(directory.path().to_owned());
        let db = db::connect(&config.database_url).await.unwrap();
        let nodes = NodeService::new(Arc::new(config), db.clone()).unwrap();
        let routes = RegistryRouteService::new(db);
        let route = routes
            .create(RegistryRouteInput {
                key: "race-delete".into(),
                name: "Race delete".into(),
                canonical_registry: "registry.example".into(),
                path_prefix: Some("race-delete".into()),
                repository_mode: "passthrough".into(),
                is_default: false,
                enabled: true,
            })
            .await
            .unwrap();

        let (delete, create) = tokio::join!(
            routes.delete(route.id),
            nodes.create(plain_node_input(route.id)),
        );
        assert_one_success_one_conflict(delete, create.map(|_| ()));
    }
}
