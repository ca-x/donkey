use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use chrono::Utc;
use dashmap::DashMap;
use http::{HeaderName, HeaderValue};
use sea_orm::sea_query::CaseStatement;
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
    live_rates: Arc<DashMap<Uuid, Arc<Mutex<RateWindow>>>>,
}

#[derive(Clone, Copy)]
struct NodeRuntimeConfig {
    scheduler_policy: SchedulerPolicy,
    upstream_timeout: std::time::Duration,
    health_interval: std::time::Duration,
}

const LIVE_RATE_WINDOW: Duration = Duration::from_secs(10);

#[derive(Default)]
struct RateWindow {
    samples: VecDeque<RateSample>,
}

struct RateSample {
    started_at: Instant,
    finished_at: Instant,
    bytes: u64,
}

impl RateWindow {
    fn record(&mut self, now: Instant, bytes: u64, elapsed: Duration) {
        self.samples.push_back(RateSample {
            started_at: now.checked_sub(elapsed).unwrap_or(now),
            finished_at: now,
            bytes,
        });
        self.prune(now);
    }

    fn rate(&mut self, now: Instant) -> u64 {
        self.prune(now);
        let Some(first) = self.samples.front() else {
            return 0;
        };
        let window_start = now.checked_sub(LIVE_RATE_WINDOW).unwrap_or(now);
        let started_at = first.started_at.max(window_start);
        let seconds = now.duration_since(started_at).as_secs_f64().max(0.25);
        let bytes = self
            .samples
            .iter()
            .map(|sample| {
                let sample_seconds = sample
                    .finished_at
                    .duration_since(sample.started_at)
                    .as_secs_f64();
                if sample_seconds <= 0.0 {
                    return sample.bytes as f64;
                }
                let overlap_start = sample.started_at.max(started_at);
                let overlap_seconds = sample
                    .finished_at
                    .duration_since(overlap_start)
                    .as_secs_f64();
                sample.bytes as f64 * (overlap_seconds / sample_seconds).clamp(0.0, 1.0)
            })
            .sum::<f64>();
        (bytes / seconds).min(u64::MAX as f64) as u64
    }

    fn prune(&mut self, now: Instant) {
        while self
            .samples
            .front()
            .is_some_and(|sample| now.duration_since(sample.finished_at) > LIVE_RATE_WINDOW)
        {
            self.samples.pop_front();
        }
    }
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
    #[serde(default = "default_connect_ip_type")]
    pub connect_ip_type: String,
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
    pub live_bps: u64,
}

#[derive(Serialize)]
struct NodeViewWire<'a> {
    node: NodeSafeView<'a>,
    metric: &'a node_metric::Model,
    score: f64,
    auth_configured: bool,
    max_concurrency: u16,
    live_bps: u64,
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
    connect_ip_type: &'a str,
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
                connect_ip_type: &self.node.connect_ip_type,
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
            live_bps: self.live_bps,
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
    8
}

fn default_connect_ip_type() -> String {
    "ip".into()
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
            live_rates: Arc::new(DashMap::new()),
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
        if let Some(target) = &input.connect_ip {
            normalize_connect_ip_type(&input.connect_ip_type)?;
            security::validate_connect_target_syntax(target, &input.connect_ip_type)?;
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
            connect_ip_type: normalize_connect_ip_type(&input.connect_ip_type)?.to_owned(),
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
        if let Some(target) = &input.connect_ip {
            normalize_connect_ip_type(&input.connect_ip_type)?;
            security::validate_connect_target_syntax(target, &input.connect_ip_type)?;
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
        node.connect_ip_type = normalize_connect_ip_type(&input.connect_ip_type)?.to_owned();
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
        let deleted = db::delete_node(&self.db, id)
            .await
            .map_err(|error| match &error {
                sea_orm::DbErr::Custom(message)
                    if message == "node is referenced by an active job or sync rule" =>
                {
                    AppError::conflict(message.clone())
                }
                _ => error.into(),
            })?;
        if deleted == 0 {
            return Err(AppError::not_found("node"));
        }
        self.live_rates.remove(&id);
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
        let live_bps = self.live_rate(node.id);
        Ok(NodeView {
            node,
            metric,
            score,
            auth_configured,
            route: (&route).into(),
            max_concurrency,
            live_bps,
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
        if success && bytes > 0 {
            self.record_live_transfer(id, bytes, elapsed);
        }
        let sample = if success { 1.0_f64 } else { 0.0_f64 };
        let mut update = node_metric::Entity::update_many()
            .col_expr(
                node_metric::Column::SuccessRate,
                Expr::col(node_metric::Column::SuccessRate)
                    .mul(0.85)
                    .add(sample * 0.15),
            )
            .filter(node_metric::Column::NodeId.eq(id));
        if success && elapsed.as_secs_f64() > 0.0 {
            let bps = (bytes as f64 / elapsed.as_secs_f64()).min(i64::MAX as f64) as i64;
            let byte_count = std::cmp::min(bytes, i64::MAX as u64) as i64;
            update = update
                .col_expr(node_metric::Column::Healthy, Expr::value(true))
                .col_expr(node_metric::Column::CurrentBps, Expr::value(bps))
                .col_expr(
                    node_metric::Column::SpeedBps,
                    CaseStatement::new()
                        .case(
                            Expr::col(node_metric::Column::SpeedBps).lte(0),
                            Expr::value(bps),
                        )
                        .finally(
                            Expr::col(node_metric::Column::SpeedBps)
                                .mul(7)
                                .add(bps.saturating_mul(3))
                                .div(10),
                        )
                        .into(),
                )
                .col_expr(
                    node_metric::Column::TotalBytes,
                    Expr::col(node_metric::Column::TotalBytes).add(byte_count),
                );
        }
        if let Err(error) = update.exec(&self.db).await {
            tracing::warn!(?error, "failed to update node transfer metric");
        }
    }

    fn record_live_transfer(&self, id: Uuid, bytes: u64, elapsed: Duration) {
        let window = self
            .live_rates
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(RateWindow::default())))
            .clone();
        if let Ok(mut window) = window.lock() {
            window.record(Instant::now(), bytes, elapsed);
        }
    }

    /// Record bytes as they arrive from an upstream response.  This is kept
    /// entirely in memory so the proxy hot path never waits on SQLite.
    pub(crate) fn record_live_bytes(&self, id: Uuid, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let window = self
            .live_rates
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(RateWindow::default())))
            .clone();
        if let Ok(mut window) = window.lock() {
            window.record(Instant::now(), bytes, Duration::ZERO);
        }
    }

    fn live_rate(&self, id: Uuid) -> u64 {
        self.live_rates
            .get(&id)
            .and_then(|window| {
                window
                    .lock()
                    .ok()
                    .map(|mut window| window.rate(Instant::now()))
            })
            .unwrap_or(0)
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
    normalize_connect_ip_type(&input.connect_ip_type)?;
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

pub(crate) fn normalize_connect_ip_type(value: &str) -> ApiResult<&str> {
    match value {
        "" | "ip" => Ok("ip"),
        "domain" => Ok("domain"),
        _ => Err(AppError::bad_request(
            "connect_ip_type must be ip or domain",
        )),
    }
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
    use httpmock::prelude::*;
    use sea_orm::{ActiveModelTrait, EntityTrait, IntoActiveModel};
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
            connect_ip_type: "ip".into(),
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
            connect_ip_type: "ip".into(),
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

    #[test]
    fn live_rate_aggregates_concurrent_transfers_and_expires() {
        let now = Instant::now();
        let mut window = RateWindow::default();
        window.record(now, 1_000, Duration::from_secs(1));
        window.record(now, 3_000, Duration::from_secs(1));
        assert_eq!(window.rate(now), 4_000);
        let mut slow = RateWindow::default();
        slow.record(now, 2_000, Duration::from_secs(20));
        assert_eq!(slow.rate(now), 100);
        assert_eq!(
            window.rate(now + LIVE_RATE_WINDOW + Duration::from_millis(1)),
            0
        );
    }

    #[test]
    fn node_input_defaults_to_eight_connections() {
        let input: NodeInput = serde_json::from_value(serde_json::json!({
            "name": "default concurrency",
            "url": "https://registry.example/",
            "registry_route_id": crate::registry_routes::DOCKER_HUB_ROUTE_ID,
            "auth_mode": "none"
        }))
        .unwrap();
        assert_eq!(input.max_concurrency, 8);
    }

    #[tokio::test]
    async fn live_bytes_are_visible_before_a_transfer_finishes() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config::for_test(directory.path().to_owned());
        let db = db::connect(&config.database_url).await.unwrap();
        let service = NodeService::new(Arc::new(config), db).unwrap();
        let node = service
            .create(plain_node_input(DOCKER_HUB_ROUTE_ID))
            .await
            .unwrap();

        service.record_live_bytes(node.node.id, 2 * 1024 * 1024);
        let current = service
            .list()
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.node.id == node.node.id)
            .unwrap()
            .live_bps;
        assert!(current > 0);
    }

    #[tokio::test]
    async fn transfer_metrics_keep_latest_and_smoothed_rates_distinct() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config::for_test(directory.path().to_owned());
        let db = db::connect(&config.database_url).await.unwrap();
        let service = NodeService::new(Arc::new(config), db.clone()).unwrap();
        let node = service
            .create(plain_node_input(DOCKER_HUB_ROUTE_ID))
            .await
            .unwrap();

        service
            .record_transfer(node.node.id, 1_000, std::time::Duration::from_secs(1), true)
            .await;
        service
            .record_transfer(node.node.id, 3_000, std::time::Duration::from_secs(1), true)
            .await;

        let metric = db::metric_for(&db, node.node.id).await.unwrap().unwrap();
        assert_eq!(metric.current_bps, 3_000);
        assert_eq!(metric.speed_bps, 1_600);
        assert!(metric.last_checked_at.is_none());
    }

    #[tokio::test]
    async fn probe_persists_real_round_trip_latency() {
        let upstream = MockServer::start_async().await;
        upstream
            .mock_async(|when, then| {
                when.method(GET).path("/v2/");
                then.status(200).delay(std::time::Duration::from_millis(30));
            })
            .await;
        let directory = tempfile::tempdir().unwrap();
        let config = Config::for_test(directory.path().to_owned());
        let db = db::connect(&config.database_url).await.unwrap();
        let service = NodeService::new(Arc::new(config), db.clone()).unwrap();
        let node = service
            .create(NodeInput {
                url: upstream.base_url(),
                ..plain_node_input(DOCKER_HUB_ROUTE_ID)
            })
            .await
            .unwrap();

        let measured = service.probe(node.node.id).await.unwrap();
        let stored = service
            .list()
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.node.id == node.node.id)
            .unwrap();

        assert!(measured.metric.latency_ms >= 25);
        assert_eq!(stored.metric.latency_ms, measured.metric.latency_ms);
        assert!(stored.metric.last_checked_at.is_some());
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
                connect_ip_type: "ip".into(),
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

    #[tokio::test]
    async fn node_delete_is_blocked_by_sync_rule_and_cleans_runtime_rows() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.credential_key = Some(SecretString::from("22".repeat(32)));
        let db = db::connect(&config.database_url).await.unwrap();
        let service = NodeService::new(Arc::new(config), db.clone()).unwrap();
        let node = service
            .create(plain_node_input(
                crate::registry_routes::DOCKER_HUB_ROUTE_ID,
            ))
            .await
            .unwrap();
        let now = Utc::now();
        let destination_credential_id = Uuid::new_v4();
        crate::db::registry_credential::Model {
            id: destination_credential_id,
            name: "node deletion test".into(),
            registry: "registry.example".into(),
            auth_mode: "bearer".into(),
            username: None,
            secret_enc: "encrypted-test-secret".into(),
            generation: 1,
            created_at: now,
            updated_at: now,
        }
        .into_active_model()
        .insert(&db)
        .await
        .unwrap();
        let rule = crate::db::image_sync_rule::Model {
            id: Uuid::new_v4(),
            name: "uses node".into(),
            enabled: true,
            source_ref: "docker.io/library/redis:latest".into(),
            source_node_id: Some(node.node.id),
            source_credential_id: None,
            destination_ref: "registry.example/redis:latest".into(),
            destination_credential_id,
            platform_os: "linux".into(),
            platform_arch: "amd64".into(),
            cron: "0 * * * * *".into(),
            timezone: "UTC".into(),
            last_digest: None,
            last_run_at: None,
            next_run_at: Some(now),
            created_at: now,
            updated_at: now,
        }
        .into_active_model()
        .insert(&db)
        .await
        .unwrap();

        assert!(matches!(
            service.delete(node.node.id).await,
            Err(AppError::Conflict(_))
        ));
        crate::db::image_sync_rule::Entity::delete_by_id(rule.id)
            .exec(&db)
            .await
            .unwrap();
        service.delete(node.node.id).await.unwrap();
        assert!(db::get_node(&db, node.node.id).await.unwrap().is_none());
        assert!(db::metric_for(&db, node.node.id).await.unwrap().is_none());
    }
}
