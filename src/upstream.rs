use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use http::{HeaderMap, Method, header};
use moka::future::Cache;
use reqwest::{RequestBuilder, Response, StatusCode};
use serde::Deserialize;
use tokio::sync::RwLock;
use url::Url;
use www_authenticate_parser::{Challenge, parse_header};

use crate::{
    config::Config,
    error::{ApiResult, AppError},
    nodes::{NodeService, NodeView},
    security,
};

#[derive(Clone)]
pub struct UpstreamService {
    config: Arc<Config>,
    timeout: Arc<RwLock<Duration>>,
    nodes: NodeService,
    tokens: Cache<String, TokenEntry>,
}

#[derive(Clone, Copy, Debug)]
pub enum RangeMode {
    Suppress,
    ForwardClient,
    Exact(u64, u64),
}

#[derive(Clone)]
struct TokenEntry {
    value: Arc<str>,
    expires_at: Instant,
}

#[derive(Clone, Debug)]
struct BearerChallenge {
    realm: String,
    service: Option<String>,
    scope: Option<String>,
}

struct RequestSpec<'a> {
    method: Method,
    url: Url,
    headers: &'a HeaderMap,
    range: RangeMode,
    bearer: Option<&'a str>,
    apply_node_auth: bool,
}

#[derive(Deserialize)]
struct TokenResponse {
    token: Option<String>,
    access_token: Option<String>,
    expires_in: Option<u64>,
}

impl UpstreamService {
    pub fn new(config: Arc<Config>, nodes: NodeService) -> Self {
        Self {
            timeout: Arc::new(RwLock::new(config.upstream_timeout)),
            config,
            nodes,
            tokens: Cache::builder().max_capacity(2_000).build(),
        }
    }

    pub async fn update_runtime(&self, config: &Config) {
        *self.timeout.write().await = config.upstream_timeout;
    }

    pub async fn send(
        &self,
        node: &NodeView,
        method: Method,
        request_path: &str,
        headers: &HeaderMap,
        range: RangeMode,
    ) -> ApiResult<Response> {
        let upstream = security::validate_upstream(&node.node.url, &self.config).await?;
        let url = upstream
            .url
            .join(request_path.trim_start_matches('/'))
            .map_err(AppError::internal)?;
        let mut response = self
            .send_once(
                node,
                RequestSpec {
                    method: method.clone(),
                    url: url.clone(),
                    headers,
                    range,
                    bearer: None,
                    apply_node_auth: true,
                },
            )
            .await?;

        if response.status() == StatusCode::UNAUTHORIZED
            && node.node.auth_mode != "bearer"
            && let Some(challenge) = bearer_challenge(response.headers(), request_path)
        {
            let token = self.token(node, &challenge).await?;
            response = self
                .send_once(
                    node,
                    RequestSpec {
                        method: method.clone(),
                        url: url.clone(),
                        headers,
                        range,
                        bearer: Some(&token),
                        apply_node_auth: node.node.auth_mode == "header",
                    },
                )
                .await?;
        }

        let mut redirect_base = url;
        for _ in 0..3 {
            if !response.status().is_redirection() {
                return Ok(response);
            }
            let location = response
                .headers()
                .get(header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| AppError::Upstream("upstream redirect has no Location".into()))?;
            let redirect = redirect_base.join(location).map_err(AppError::internal)?;
            let validated = security::validate_target_url(redirect.as_str(), &self.config).await?;
            redirect_base = validated.url.clone();
            let timeout = *self.timeout.read().await;
            let client = security::client_for(&validated, timeout)?;
            let request = forward_headers(
                client.request(method.clone(), validated.url),
                headers,
                range,
            );
            response = request
                .send()
                .await
                .map_err(|error| AppError::Upstream(error.to_string()))?;
        }
        if response.status().is_redirection() {
            return Err(AppError::Upstream("too many upstream redirects".into()));
        }
        Ok(response)
    }

    async fn send_once(&self, node: &NodeView, spec: RequestSpec<'_>) -> ApiResult<Response> {
        let mut validated = security::validate_target_url(spec.url.as_str(), &self.config).await?;
        self.apply_connect_ip(node, &mut validated)?;
        let timeout = *self.timeout.read().await;
        let client = security::client_for(&validated, timeout)?;
        let mut request = forward_headers(
            client.request(spec.method, validated.url),
            spec.headers,
            spec.range,
        );
        if let Some(token) = spec.bearer {
            request = request.bearer_auth(token);
        }
        if spec.apply_node_auth {
            request = self.nodes.apply_auth(request, &node.node)?;
        }
        request
            .send()
            .await
            .map_err(|error| AppError::Upstream(error.to_string()))
    }

    async fn token(&self, node: &NodeView, challenge: &BearerChallenge) -> ApiResult<Arc<str>> {
        let key = format!(
            "{}\0{}\0{}\0{}\0{}\0{}",
            node.node.id,
            node.node.registry_route_id,
            node.node.updated_at.timestamp_millis(),
            challenge.realm,
            challenge.service.as_deref().unwrap_or_default(),
            challenge.scope.as_deref().unwrap_or_default()
        );
        if let Some(entry) = self.tokens.get(&key).await
            && entry.expires_at > Instant::now() + Duration::from_secs(10)
        {
            return Ok(entry.value);
        }
        let mut realm = Url::parse(&challenge.realm)
            .map_err(|_| AppError::Upstream("invalid Bearer token realm".into()))?;
        self.validate_token_realm(node, &realm)?;
        {
            let mut query = realm.query_pairs_mut();
            if let Some(service) = &challenge.service {
                query.append_pair("service", service);
            }
            if let Some(scope) = &challenge.scope {
                query.append_pair("scope", scope);
            }
        }
        let mut validated = security::validate_target_url(realm.as_str(), &self.config).await?;
        self.apply_connect_ip(node, &mut validated)?;
        let client = security::client_for(&validated, self.config.upstream_timeout)?;
        let request = client
            .get(validated.url)
            .header(header::ACCEPT, "application/json");
        let request = self.nodes.apply_auth(request, &node.node)?;
        let response = request
            .send()
            .await
            .map_err(|error| AppError::Upstream(error.to_string()))?;
        if !response.status().is_success() {
            return Err(AppError::Upstream(format!(
                "token service returned {}",
                response.status()
            )));
        }
        if response
            .content_length()
            .is_some_and(|length| length > 1024 * 1024)
        {
            return Err(AppError::Upstream("token response is too large".into()));
        }
        let mut body = Vec::with_capacity(response.content_length().unwrap_or(1024) as usize);
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| AppError::Upstream(error.to_string()))?;
            if body.len().saturating_add(chunk.len()) > 1024 * 1024 {
                return Err(AppError::Upstream("token response is too large".into()));
            }
            body.extend_from_slice(&chunk);
        }
        let payload = serde_json::from_slice::<TokenResponse>(&body)
            .map_err(|error| AppError::Upstream(format!("invalid token response: {error}")))?;
        let value: Arc<str> = payload
            .token
            .or(payload.access_token)
            .filter(|token| !token.is_empty() && token.len() <= 16 * 1024)
            .ok_or_else(|| AppError::Upstream("token response has no token".into()))?
            .into();
        let ttl = payload.expires_in.unwrap_or(300).clamp(30, 3600);
        self.tokens
            .insert(
                key,
                TokenEntry {
                    value: value.clone(),
                    expires_at: Instant::now() + Duration::from_secs(ttl),
                },
            )
            .await;
        Ok(value)
    }

    fn apply_connect_ip(
        &self,
        node: &NodeView,
        upstream: &mut security::ValidatedUpstream,
    ) -> ApiResult<()> {
        let Some(value) = &node.node.connect_ip else {
            return Ok(());
        };
        let node_host = Url::parse(&node.node.url)
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned));
        if node_host.as_deref() != upstream.url.host_str() {
            return Ok(());
        }
        let ip = value
            .parse()
            .map_err(|_| AppError::bad_request("node connect_ip is invalid"))?;
        if !self.config.allow_private_upstreams && security::is_non_public(ip) {
            return Err(AppError::bad_request("private connect_ip is disabled"));
        }
        upstream.addresses = Arc::from([ip]);
        Ok(())
    }

    fn validate_token_realm(&self, node: &NodeView, realm: &Url) -> ApiResult<()> {
        if node.node.auth_secret_enc.is_none() {
            return Ok(());
        }
        let node_host = Url::parse(&node.node.url)
            .ok()
            .and_then(|url| url.host_str().map(str::to_ascii_lowercase));
        let realm_host = realm.host_str().map(str::to_ascii_lowercase);
        let same_origin = node_host == realm_host;
        let official_docker_auth = node.route.canonical_registry == "docker.io"
            && realm_host.as_deref() == Some("auth.docker.io");
        if same_origin || official_docker_auth {
            Ok(())
        } else {
            Err(AppError::Upstream(
                "Bearer token realm is not trusted for this credentialed node".into(),
            ))
        }
    }
}

fn forward_headers(
    mut request: RequestBuilder,
    headers: &HeaderMap,
    range: RangeMode,
) -> RequestBuilder {
    for name in [
        header::ACCEPT,
        header::IF_NONE_MATCH,
        header::IF_MODIFIED_SINCE,
    ] {
        if let Some(value) = headers.get(&name) {
            request = request.header(name, value);
        }
    }
    match range {
        RangeMode::Suppress => {}
        RangeMode::ForwardClient => {
            if let Some(value) = headers.get(header::RANGE) {
                request = request.header(header::RANGE, value);
            }
        }
        RangeMode::Exact(start, end) => {
            request = request.header(header::RANGE, format!("bytes={start}-{end}"));
        }
    }
    request.header(header::ACCEPT_ENCODING, "identity")
}

fn bearer_challenge(headers: &HeaderMap, request_path: &str) -> Option<BearerChallenge> {
    let value = headers.get(header::WWW_AUTHENTICATE)?.to_str().ok()?;
    let (scheme, challenge) = parse_header(value).ok()?;
    if !scheme.to_string().eq_ignore_ascii_case("bearer") {
        return None;
    }
    let Challenge::Fields(fields) = challenge else {
        return None;
    };
    let realm = fields.get("realm")?.to_owned();
    let service = fields.get("service").cloned();
    let scope = fields
        .get("scope")
        .cloned()
        .or_else(|| repository_scope(request_path));
    Some(BearerChallenge {
        realm,
        service,
        scope,
    })
}

fn repository_scope(path: &str) -> Option<String> {
    let rest = path.strip_prefix("/v2/")?;
    let repository = rest
        .split_once("/manifests/")
        .or_else(|| rest.split_once("/blobs/"))?
        .0;
    if repository.is_empty() {
        None
    } else {
        Some(format!("repository:{repository}:pull"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_real_mirror_challenge() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::WWW_AUTHENTICATE,
            r#"Bearer realm="https://mirror.example/docker/token",service="registry.docker.io""#
                .parse()
                .unwrap(),
        );
        let challenge = bearer_challenge(&headers, "/v2/library/alpine/manifests/latest").unwrap();
        assert_eq!(
            challenge.scope.as_deref(),
            Some("repository:library/alpine:pull")
        );
    }

    #[test]
    fn internal_full_fetch_never_forwards_client_range() {
        let client = reqwest::Client::new();
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, "bytes=100-199".parse().unwrap());

        let suppressed = forward_headers(
            client.get("https://example.com/blob"),
            &headers,
            RangeMode::Suppress,
        )
        .build()
        .unwrap();
        assert!(suppressed.headers().get(header::RANGE).is_none());

        let forwarded = forward_headers(
            client.get("https://example.com/blob"),
            &headers,
            RangeMode::ForwardClient,
        )
        .build()
        .unwrap();
        assert_eq!(
            forwarded.headers().get(header::RANGE).unwrap(),
            "bytes=100-199"
        );
    }
}
