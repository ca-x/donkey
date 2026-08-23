use std::{sync::Arc, time::Instant};

use chrono::Utc;
use http::{HeaderName, HeaderValue};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, ExprTrait, QueryFilter, sea_query::Expr,
};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::{
    config::{Config, SchedulerPolicy},
    crypto::CredentialCipher,
    db::{self, node, node_metric},
    error::{ApiResult, AppError},
    security,
};

#[derive(Clone)]
pub struct NodeService {
    config: Arc<Config>,
    db: DatabaseConnection,
    cipher: CredentialCipher,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NodeInput {
    pub name: String,
    pub url: String,
    pub kind: String,
    pub route_prefix: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_priority")]
    pub priority: i32,
    #[serde(default)]
    pub cf_preferred: bool,
    pub connect_ip: Option<String>,
    #[serde(default)]
    pub auth_mode: String,
    pub auth_username: Option<String>,
    pub auth_header: Option<String>,
    pub auth_secret: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeView {
    pub node: node::Model,
    pub metric: node_metric::Model,
    pub score: f64,
    pub auth_configured: bool,
}

fn default_true() -> bool {
    true
}

fn default_priority() -> i32 {
    100
}

impl NodeService {
    pub fn new(config: Arc<Config>, db: DatabaseConnection) -> ApiResult<Self> {
        let cipher = CredentialCipher::from_config(&config)?;
        Ok(Self { config, db, cipher })
    }

    pub async fn list(&self) -> ApiResult<Vec<NodeView>> {
        let mut views = Vec::new();
        for node in db::list_nodes(&self.db).await? {
            let metric = db::metric_for(&self.db, node.id)
                .await?
                .unwrap_or_else(|| empty_metric(node.id));
            let score = score(&node, &metric, self.config.scheduler_policy);
            let auth_configured = node.auth_secret_enc.is_some();
            views.push(NodeView {
                node,
                metric,
                score,
                auth_configured,
            });
        }
        views.sort_by(|a, b| b.score.total_cmp(&a.score));
        Ok(views)
    }

    pub async fn enabled_registry_nodes(
        &self,
        route_prefix: Option<&str>,
    ) -> ApiResult<Vec<NodeView>> {
        Ok(self
            .list()
            .await?
            .into_iter()
            .filter(|view| {
                view.node.enabled
                    && matches!(view.node.kind.as_str(), "dockerhub" | "ghcr" | "registry")
                    && view.node.route_prefix.as_deref() == route_prefix
            })
            .collect())
    }

    pub async fn route_prefixes(&self) -> ApiResult<Vec<String>> {
        let mut prefixes = db::list_nodes(&self.db)
            .await?
            .into_iter()
            .filter(|node| node.enabled)
            .filter_map(|node| node.route_prefix)
            .collect::<Vec<_>>();
        prefixes.sort();
        prefixes.dedup();
        Ok(prefixes)
    }

    pub async fn create(&self, input: NodeInput) -> ApiResult<NodeView> {
        validate_input(&input)?;
        self.require_registry_auth_for_secret(&input.auth_mode)?;
        let validated = security::validate_upstream(&input.url, &self.config).await?;
        let canonical = validated.url.to_string();
        let route_prefix = normalize_route_prefix(input.route_prefix, &input.kind)?;
        if db::get_node_by_url_and_prefix(&self.db, &canonical, route_prefix.as_deref())
            .await?
            .is_some()
        {
            return Err(AppError::bad_request(
                "an upstream with this URL already exists",
            ));
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
            kind: input.kind,
            route_prefix,
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
        let node = db::insert_node(&self.db, node).await?;
        let metric = empty_metric(node.id);
        db::upsert_metric(&self.db, metric.clone()).await?;
        let score = score(&node, &metric, self.config.scheduler_policy);
        let auth_configured = node.auth_secret_enc.is_some();
        Ok(NodeView {
            node,
            metric,
            score,
            auth_configured,
        })
    }

    pub async fn update(&self, id: Uuid, input: NodeInput) -> ApiResult<NodeView> {
        validate_input(&input)?;
        self.require_registry_auth_for_secret(&input.auth_mode)?;
        let validated = security::validate_upstream(&input.url, &self.config).await?;
        let mut node = db::get_node(&self.db, id)
            .await?
            .ok_or_else(|| AppError::not_found("node"))?;
        node.name = input.name.trim().to_owned();
        node.url = validated.url.to_string();
        node.route_prefix = normalize_route_prefix(input.route_prefix, &input.kind)?;
        node.kind = input.kind;
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
        let node = db::save_node(&self.db, node).await?;
        let metric = db::metric_for(&self.db, id)
            .await?
            .unwrap_or_else(|| empty_metric(id));
        let score = score(&node, &metric, self.config.scheduler_policy);
        let auth_configured = node.auth_secret_enc.is_some();
        Ok(NodeView {
            node,
            metric,
            score,
            auth_configured,
        })
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
        if !node.enabled || !matches!(node.kind.as_str(), "dockerhub" | "ghcr" | "registry") {
            return Err(AppError::bad_request(
                "selected image source node is disabled or not a Registry node",
            ));
        }
        let metric = db::metric_for(&self.db, id)
            .await?
            .unwrap_or_else(|| empty_metric(id));
        let score = score(&node, &metric, self.config.scheduler_policy);
        let auth_configured = node.auth_secret_enc.is_some();
        Ok(NodeView {
            node,
            metric,
            score,
            auth_configured,
        })
    }

    pub async fn probe(&self, id: Uuid) -> ApiResult<NodeView> {
        let node = db::get_node(&self.db, id)
            .await?
            .ok_or_else(|| AppError::not_found("node"))?;
        let metric = self.probe_model(&node).await;
        db::upsert_metric(&self.db, metric.clone()).await?;
        let score = score(&node, &metric, self.config.scheduler_policy);
        let auth_configured = node.auth_secret_enc.is_some();
        Ok(NodeView {
            node,
            metric,
            score,
            auth_configured,
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
            let client = security::client_for(&upstream, self.config.upstream_timeout)?;
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

fn validate_input(input: &NodeInput) -> ApiResult<()> {
    if input.name.trim().is_empty() || input.name.chars().count() > 80 {
        return Err(AppError::bad_request(
            "name must be between 1 and 80 characters",
        ));
    }
    if !security::valid_node_kind(&input.kind) {
        return Err(AppError::bad_request("unsupported node kind"));
    }
    if !(0..=1000).contains(&input.priority) {
        return Err(AppError::bad_request("priority must be between 0 and 1000"));
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

fn normalize_route_prefix(value: Option<String>, kind: &str) -> ApiResult<Option<String>> {
    let value = trimmed(value).or_else(|| (kind == "ghcr").then(|| "ghcr".to_owned()));
    let Some(value) = value else {
        return Ok(None);
    };
    let normalized = value.trim_matches('/').to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.len() > 32
        || !normalized.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
    {
        return Err(AppError::bad_request("invalid Registry route prefix"));
    }
    Ok(Some(normalized))
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
    use secrecy::SecretString;

    #[test]
    fn healthy_fast_node_scores_higher() {
        let now = Utc::now();
        let base = node::Model {
            id: Uuid::new_v4(),
            name: "node".into(),
            url: "https://registry.example/".into(),
            kind: "registry".into(),
            route_prefix: None,
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
                kind: "registry".into(),
                route_prefix: None,
                enabled: true,
                priority: 10,
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
    }
}
