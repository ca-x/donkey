use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::StreamExt;
use http_content_range::ContentRange;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;
use tower_http::services::ServeFile;

use crate::{
    cache::{CacheLease, CacheStore, CachedObject},
    db::registry_route,
    error::{ApiResult, AppError},
    registry_routes::RepositoryMode,
    state::AppState,
};

pub async fn handle(State(state): State<AppState>, request: Request) -> Response {
    let count_traffic = request.uri().path().starts_with("/v2");
    let traffic = state.traffic.clone();
    if count_traffic {
        traffic.record_request();
    }
    let response = match handle_inner(state, request).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    };
    if count_traffic {
        traffic.record_response(&response);
    }
    response
}

async fn handle_inner(state: AppState, request: Request) -> ApiResult<Response> {
    if crate::helpers::is_helper_path(request.uri().path()) {
        return crate::helpers::serve(&state, request).await;
    }
    if !request.uri().path().starts_with("/v2") {
        return match crate::domainfold::proxy_if_mapping(&state, request).await? {
            Ok(response) => Ok(response),
            Err(_) => Err(AppError::not_found("route")),
        };
    }
    // Docker Engine probes this endpoint before using a registry mirror.  It
    // is a local capability check and must not depend on an upstream node;
    // otherwise a slow/unavailable source makes Docker fall back to a direct
    // Docker Hub connection.
    if matches!(request.uri().path(), "/v2" | "/v2/") {
        let mut response = StatusCode::OK.into_response();
        response.headers_mut().insert(
            "docker-distribution-api-version",
            header::HeaderValue::from_static("registry/2.0"),
        );
        return Ok(response);
    }
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        return Ok((
            StatusCode::METHOD_NOT_ALLOWED,
            "pull-through proxy is read-only",
        )
            .into_response());
    }
    let path = request.uri().path().to_owned();
    if path.len() > 4096 || !path.starts_with("/v2") || path.contains("..") {
        return Err(AppError::bad_request("invalid Registry path"));
    }

    let resolved = route_registry_path(&state, request.uri()).await?;
    let upstream_path = resolved.upstream_path;
    let registry_route_id = resolved.route.id;
    let nodes = state
        .nodes
        .enabled_registry_nodes(registry_route_id)
        .await?;
    if nodes.is_empty() {
        return Err(AppError::unavailable(
            "resolved Registry route has no enabled nodes",
        ));
    }
    if upstream_path.contains("/blobs/") {
        let digest = upstream_path
            .rsplit_once("/blobs/")
            .map(|(_, digest)| digest)
            .ok_or_else(|| AppError::bad_request("invalid Blob path"))?;
        if !valid_sha256_digest(digest) {
            return Err(AppError::bad_request("invalid Blob digest"));
        }
        if request.method() == Method::HEAD {
            let key = CacheStore::key(
                &upstream_path,
                request
                    .headers()
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok()),
            );
            if let Some(object) = state.cache.get(&key).await? {
                return serve_cached(&state.cache, object, request).await;
            }
        } else {
            // Keep the verified/cache-aware path for normal-sized blobs. If
            // it cannot produce a response promptly (large/slow layers),
            // switch to a streaming retry so Docker receives headers quickly.
            let stream_fallback_timeout = state.scheduler.stream_fallback_timeout().await;
            match tokio::time::timeout(
                stream_fallback_timeout,
                state.scheduler.fetch_blob(
                    &upstream_path,
                    request.headers(),
                    Some(digest),
                    nodes.clone(),
                ),
            )
            .await
            {
                Ok(result) => {
                    let object = result?;
                    return serve_cached(&state.cache, object, request).await;
                }
                Err(_) => {
                    return stream_blob(
                        &state,
                        &upstream_path,
                        request.headers().clone(),
                        Some(digest),
                        nodes,
                    )
                    .await;
                }
            }
        }
    }
    let method = request.method().clone();
    let headers = request.headers().clone();
    proxy_passthrough(&state, method, upstream_path, headers, nodes).await
}

async fn stream_blob(
    state: &AppState,
    path: &str,
    headers: HeaderMap,
    expected_digest: Option<&str>,
    nodes: Vec<crate::nodes::NodeView>,
) -> ApiResult<Response> {
    let nodes = state.scheduler.ordered_stream_nodes(&nodes, path).await;
    let mut last_error = None;
    for (node_index, node) in nodes.iter().enumerate() {
        let upstream = match state
            .upstream
            .send(
                node,
                Method::GET,
                path,
                &headers,
                crate::upstream::RangeMode::ForwardClient,
            )
            .await
        {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                last_error = Some(AppError::Upstream(format!(
                    "{} returned {}",
                    node.node.name,
                    response.status()
                )));
                continue;
            }
            Err(error) => {
                last_error = Some(error);
                continue;
            }
        };

        let status = upstream.status();
        let response_headers = upstream.headers().clone();
        let Some(window) = stream_window(&upstream) else {
            return Ok(to_response(upstream));
        };
        let media_type = response_headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);
        let cache = state.cache.clone();
        let key = CacheStore::key(
            path,
            headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok()),
        );
        let lease = cache.lock(&key).await;
        let expected = expected_digest.map(str::to_owned);
        let relay = StreamRelay {
            upstream: state.upstream.clone(),
            nodes: nodes.clone(),
            current_node: node_index,
            response: upstream,
            path: path.to_owned(),
            headers,
            window,
            cache,
            key,
            media_type,
            expected_digest: expected,
            _lease: lease,
        };
        tokio::spawn(async move {
            let errors = tx.clone();
            if let Err(error) = relay_stream_blob(relay, tx).await {
                let _ = errors
                    .send(Err(std::io::Error::other(error.to_string())))
                    .await;
            }
        });

        let body = futures_util::stream::unfold(rx, |mut rx| async {
            rx.recv().await.map(|chunk| (chunk, rx))
        });
        let mut response = Response::new(Body::from_stream(body));
        *response.status_mut() = status;
        copy_response_headers(&response_headers, response.headers_mut());
        response.headers_mut().insert(
            "docker-distribution-api-version",
            header::HeaderValue::from_static("registry/2.0"),
        );
        return Ok(response);
    }
    Err(last_error.unwrap_or_else(|| AppError::unavailable("all upstream nodes failed")))
}

#[derive(Clone, Copy)]
struct StreamWindow {
    start: u64,
    end: u64,
    total: u64,
}

struct StreamRelay {
    upstream: crate::upstream::UpstreamService,
    nodes: Vec<crate::nodes::NodeView>,
    current_node: usize,
    response: reqwest::Response,
    path: String,
    headers: HeaderMap,
    window: StreamWindow,
    cache: CacheStore,
    key: String,
    media_type: String,
    expected_digest: Option<String>,
    _lease: CacheLease,
}

struct ResumeRequest<'a> {
    upstream: &'a crate::upstream::UpstreamService,
    nodes: &'a [crate::nodes::NodeView],
    current_node: usize,
    path: &'a str,
    headers: &'a HeaderMap,
    start: u64,
    end: u64,
    total: u64,
}

fn stream_window(response: &reqwest::Response) -> Option<StreamWindow> {
    if response.status() == StatusCode::PARTIAL_CONTENT {
        let range = response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(ContentRange::parse)?;
        let ContentRange::Bytes(range) = range else {
            return None;
        };
        let length = range
            .last_byte
            .checked_sub(range.first_byte)?
            .checked_add(1)?;
        if response.content_length() != Some(length) {
            return None;
        }
        return Some(StreamWindow {
            start: range.first_byte,
            end: range.last_byte,
            total: range.complete_length,
        });
    }
    if response.status().is_success() {
        let total = response.content_length().filter(|size| *size > 0)?;
        return Some(StreamWindow {
            start: 0,
            end: total - 1,
            total,
        });
    }
    None
}

async fn relay_stream_blob(
    relay: StreamRelay,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> ApiResult<()> {
    let StreamRelay {
        upstream,
        nodes,
        mut current_node,
        response,
        path,
        headers,
        window,
        cache,
        key,
        media_type,
        expected_digest,
        _lease,
    } = relay;
    let complete_blob = window.start == 0 && window.end.checked_add(1) == Some(window.total);
    let stable_partial_dir = cache.temp_dir().join(&key);
    let temporary_range = if complete_blob {
        None
    } else {
        Some(
            tempfile::Builder::new()
                .prefix("donkey-range-")
                .tempdir_in(cache.temp_dir())?,
        )
    };
    let partial = if let Some(temporary_range) = temporary_range.as_ref() {
        temporary_range.path().join("object.partial")
    } else {
        tokio::fs::create_dir_all(&stable_partial_dir).await?;
        stable_partial_dir.join("object.partial")
    };
    let mut existing = if complete_blob {
        tokio::fs::metadata(&partial)
            .await
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    } else {
        0
    };
    if existing > window.total {
        tokio::fs::remove_file(&partial).await?;
        existing = 0;
    }
    let mut hasher = Sha256::new();
    let mut offset = window.start;
    let mut response = response;

    if complete_blob && existing > 0 {
        let mut saved = tokio::fs::File::open(&partial).await?;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let read = saved.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            let chunk = Bytes::copy_from_slice(&buffer[..read]);
            hasher.update(&chunk);
            offset += read as u64;
            tx.send(Ok(chunk))
                .await
                .map_err(|_| AppError::Upstream("client disconnected during Blob replay".into()))?;
        }
        if offset <= window.end {
            let (next_node, next_response) = resume_stream_response(ResumeRequest {
                upstream: &upstream,
                nodes: &nodes,
                current_node,
                path: &path,
                headers: &headers,
                start: offset,
                end: window.end,
                total: window.total,
            })
            .await?;
            current_node = next_node;
            response = next_response;
        }
    }

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&partial)
        .await?;

    while offset <= window.end {
        let mut body = response.bytes_stream();
        let mut stream_error = None;
        while let Some(chunk) = body.next().await {
            match chunk {
                Ok(chunk) => {
                    let remaining = window.end - offset + 1;
                    if chunk.len() as u64 > remaining {
                        return Err(AppError::Integrity);
                    }
                    if complete_blob {
                        hasher.update(&chunk);
                    }
                    file.write_all(&chunk).await?;
                    offset += chunk.len() as u64;
                    tx.send(Ok(chunk)).await.map_err(|_| {
                        AppError::Upstream("client disconnected during Blob stream".into())
                    })?;
                }
                Err(error) => {
                    stream_error = Some(error.to_string());
                    break;
                }
            }
        }
        if offset > window.end {
            break;
        }
        tracing::warn!(
            path,
            offset,
            expected_end = window.end,
            error = stream_error
                .as_deref()
                .unwrap_or("upstream body ended early"),
            "Blob stream interrupted; resuming from another node"
        );
        let (next_node, next_response) = resume_stream_response(ResumeRequest {
            upstream: &upstream,
            nodes: &nodes,
            current_node,
            path: &path,
            headers: &headers,
            start: offset,
            end: window.end,
            total: window.total,
        })
        .await?;
        current_node = next_node;
        response = next_response;
    }

    file.flush().await?;
    if complete_blob {
        if let Some(expected) = expected_digest.as_deref() {
            let actual = format!("sha256:{:x}", hasher.finalize());
            if actual != expected {
                return Err(AppError::Integrity);
            }
        }
        cache
            .admit(&key, &partial, &media_type, expected_digest)
            .await?;
        let _ = tokio::fs::remove_dir(&stable_partial_dir).await;
    }
    Ok(())
}

async fn resume_stream_response(
    request: ResumeRequest<'_>,
) -> ApiResult<(usize, reqwest::Response)> {
    use backoff::backoff::Backoff;

    if request.nodes.is_empty() {
        return Err(AppError::unavailable(
            "no nodes are available for Blob resume",
        ));
    }
    let mut policy = backoff::ExponentialBackoffBuilder::new()
        .with_initial_interval(std::time::Duration::from_millis(100))
        .with_max_interval(std::time::Duration::from_secs(2))
        .with_max_elapsed_time(Some(std::time::Duration::from_secs(30)))
        .build();
    let mut last_error = None;
    loop {
        for step in 1..=request.nodes.len() {
            let index = (request.current_node + step) % request.nodes.len();
            match request
                .upstream
                .send(
                    &request.nodes[index],
                    Method::GET,
                    request.path,
                    request.headers,
                    crate::upstream::RangeMode::Exact(request.start, request.end),
                )
                .await
            {
                Ok(response)
                    if response.status() == StatusCode::PARTIAL_CONTENT
                        && stream_window(&response).is_some_and(|window| {
                            window.start == request.start
                                && window.end == request.end
                                && window.total == request.total
                        }) =>
                {
                    return Ok((index, response));
                }
                Ok(response) => {
                    last_error = Some(format!(
                        "{} returned an invalid resume response ({})",
                        request.nodes[index].node.name,
                        response.status()
                    ));
                }
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        let Some(delay) = policy.next_backoff() else {
            break;
        };
        tokio::time::sleep(delay).await;
    }
    Err(AppError::Upstream(last_error.unwrap_or_else(|| {
        "all nodes failed to resume the Blob stream".into()
    })))
}

async fn serve_cached(
    cache: &CacheStore,
    object: CachedObject,
    request: Request,
) -> ApiResult<Response> {
    let guard = cache.lock(&object.key).await;
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    if let Some(digest) = object.digest.as_deref()
        && etag_matches(headers.get(header::IF_NONE_MATCH), digest)
    {
        drop(guard);
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        set_registry_validators(response.headers_mut(), digest);
        return Ok(response);
    }
    let mut file_request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .map_err(AppError::internal)?;
    *file_request.headers_mut() = headers;
    let response = ServeFile::new(object.path)
        .oneshot(file_request)
        .await
        .map_err(AppError::internal)?;
    drop(guard);
    let (parts, body) = response.into_parts();
    let mut response = Response::from_parts(parts, Body::new(body));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        object
            .media_type
            .parse()
            .unwrap_or_else(|_| header::HeaderValue::from_static("application/octet-stream")),
    );
    response.headers_mut().insert(
        "docker-distribution-api-version",
        header::HeaderValue::from_static("registry/2.0"),
    );
    if let Some(digest) = object.digest {
        set_registry_validators(response.headers_mut(), &digest);
    }
    Ok(response)
}

fn set_registry_validators(headers: &mut HeaderMap, digest: &str) {
    if let Ok(value) = digest.parse() {
        headers.insert("docker-content-digest", value);
    }
    if let Ok(value) = format!("\"{digest}\"").parse() {
        headers.insert(header::ETAG, value);
    }
    headers.insert(
        "docker-distribution-api-version",
        header::HeaderValue::from_static("registry/2.0"),
    );
}

fn etag_matches(value: Option<&header::HeaderValue>, digest: &str) -> bool {
    value
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(',').any(|candidate| {
                let candidate = candidate
                    .trim()
                    .strip_prefix("W/")
                    .unwrap_or(candidate.trim());
                candidate == "*" || candidate.trim_matches('"') == digest
            })
        })
}

fn valid_sha256_digest(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn proxy_passthrough(
    state: &AppState,
    method: Method,
    path: String,
    request_headers: HeaderMap,
    nodes: Vec<crate::nodes::NodeView>,
) -> ApiResult<Response> {
    let mut last_error = None;
    for node in nodes {
        let result = async {
            let upstream_response = state
                .upstream
                .send(
                    &node,
                    method.clone(),
                    &path,
                    &request_headers,
                    crate::upstream::RangeMode::ForwardClient,
                )
                .await?;
            if retryable_upstream_status(upstream_response.status()) {
                return Err(AppError::Upstream(format!(
                    "{} returned {}",
                    node.node.name,
                    upstream_response.status()
                )));
            }
            Ok::<_, AppError>(to_response(upstream_response))
        }
        .await;
        match result {
            Ok(response) => return Ok(response),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| AppError::unavailable("Registry route is unavailable")))
}

/// Statuses that indicate a transient upstream failure and should trigger a
/// retry on the next configured node. Client errors such as 401/403/404 are
/// returned as-is because another mirror is unlikely to change authorization
/// or repository existence.
fn retryable_upstream_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_EARLY | StatusCode::TOO_MANY_REQUESTS
    ) || status.is_server_error()
}

#[derive(Debug)]
struct ResolvedRegistryPath {
    upstream_path: String,
    route: registry_route::Model,
}

async fn route_registry_path(state: &AppState, uri: &http::Uri) -> ApiResult<ResolvedRegistryPath> {
    let path = uri.path();
    let Some(rest) = path.strip_prefix("/v2/") else {
        let route = require_enabled_route(state.registry_routes.default_route().await?)?;
        return Ok(ResolvedRegistryPath {
            upstream_path: path_and_query(uri),
            route,
        });
    };
    let marker = ["/manifests/", "/blobs/", "/tags/"]
        .into_iter()
        .filter_map(|marker| rest.rfind(marker).map(|index| (marker, index)))
        .max_by_key(|(_, index)| *index);
    let Some((_marker, marker_index)) = marker else {
        let route = require_enabled_route(state.registry_routes.default_route().await?)?;
        return Ok(ResolvedRegistryPath {
            upstream_path: path_and_query(uri),
            route,
        });
    };
    let mut repository = rest[..marker_index].to_owned();
    let suffix = &rest[marker_index..];
    let first = repository.split('/').next().unwrap_or_default();
    let route = if let Some(route) = state.registry_routes.by_path_prefix(first).await? {
        let prefix = route.path_prefix.as_deref().ok_or_else(|| {
            AppError::internal(anyhow::anyhow!("prefixed route has no path prefix"))
        })?;
        repository = repository
            .strip_prefix(prefix)
            .and_then(|value| value.strip_prefix('/'))
            .ok_or_else(|| AppError::bad_request("Registry route has no repository"))?
            .to_owned();
        route
    } else {
        state.registry_routes.default_route().await?
    };
    let route = require_enabled_route(route)?;
    if RepositoryMode::parse(&route.repository_mode)? == RepositoryMode::DockerHubLibrary
        && !repository.contains('/')
    {
        repository = format!("library/{repository}");
    }
    if repository.is_empty() {
        return Err(AppError::bad_request("Registry repository is empty"));
    }
    let mut normalized = format!("/v2/{repository}{suffix}");
    if let Some(query) = uri.query() {
        normalized.push('?');
        normalized.push_str(query);
    }
    Ok(ResolvedRegistryPath {
        upstream_path: normalized,
        route,
    })
}

fn require_enabled_route(route: registry_route::Model) -> ApiResult<registry_route::Model> {
    if route.enabled {
        Ok(route)
    } else {
        Err(AppError::unavailable(format!(
            "Registry route '{}' is disabled",
            route.key
        )))
    }
}

fn path_and_query(uri: &http::Uri) -> String {
    uri.path_and_query()
        .map(|value| value.as_str())
        .unwrap_or_else(|| uri.path())
        .to_owned()
}

fn to_response(upstream: reqwest::Response) -> Response {
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let body = Body::from_stream(
        upstream
            .bytes_stream()
            .map(|chunk| chunk.map_err(std::io::Error::other)),
    );
    let mut response = Response::new(body);
    *response.status_mut() = status;
    copy_response_headers(&headers, response.headers_mut());
    response
}

fn copy_response_headers(source: &HeaderMap, destination: &mut HeaderMap) {
    for name in [
        header::CONTENT_TYPE,
        header::CONTENT_LENGTH,
        header::CONTENT_RANGE,
        header::ACCEPT_RANGES,
        header::ETAG,
        header::LAST_MODIFIED,
        header::LOCATION,
        header::WWW_AUTHENTICATE,
        header::CACHE_CONTROL,
    ] {
        if let Some(value) = source.get(&name) {
            destination.insert(name, value.clone());
        }
    }
    for name in ["docker-content-digest", "docker-distribution-api-version"] {
        if let Some(value) = source.get(name) {
            destination.insert(name, value.clone());
        }
    }
}

pub fn cache_key_for(request: &Request) -> String {
    CacheStore::key(
        request.uri().path(),
        request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Config,
        nodes::NodeInput,
        registry_routes::{DOCKER_HUB_ROUTE_ID, GHCR_ROUTE_ID},
    };
    use sea_orm::{ActiveModelTrait, IntoActiveModel};

    #[tokio::test]
    async fn routes_registry_prefix_and_preserves_query() {
        let directory = tempfile::tempdir().unwrap();
        let state = AppState::new(Config::for_test(directory.path().to_owned()))
            .await
            .unwrap();
        let node = state
            .nodes
            .create(NodeInput {
                name: "ghcr".into(),
                url: "http://127.0.0.1:5001".into(),
                registry_route_id: GHCR_ROUTE_ID,
                enabled: true,
                priority: 1,
                max_concurrency: 4,
                cf_preferred: false,
                connect_ip: None,
                auth_mode: "none".into(),
                auth_username: None,
                auth_header: None,
                auth_secret: None,
            })
            .await
            .unwrap();
        let uri: http::Uri = "/v2/ghcr/org/image/manifests/latest?ns=value"
            .parse()
            .unwrap();
        let resolved = route_registry_path(&state, &uri).await.unwrap();
        assert_eq!(
            resolved.upstream_path,
            "/v2/org/image/manifests/latest?ns=value"
        );
        assert_eq!(resolved.route.id, GHCR_ROUTE_ID);

        let short: http::Uri = "/v2/nginx/manifests/latest".parse().unwrap();
        let resolved = route_registry_path(&state, &short).await.unwrap();
        assert_eq!(resolved.upstream_path, "/v2/library/nginx/manifests/latest");
        assert_eq!(resolved.route.id, DOCKER_HUB_ROUTE_ID);

        let ghcr_short: http::Uri = "/v2/ghcr/nginx/manifests/latest".parse().unwrap();
        let resolved = route_registry_path(&state, &ghcr_short).await.unwrap();
        assert_eq!(resolved.upstream_path, "/v2/nginx/manifests/latest");

        let mut disabled = node.node.clone();
        disabled.enabled = false;
        disabled.updated_at = chrono::Utc::now();
        crate::db::save_node(&state.db, disabled).await.unwrap();
        let resolved = route_registry_path(&state, &uri).await.unwrap();
        assert_eq!(resolved.route.id, GHCR_ROUTE_ID);
        assert_eq!(
            resolved.upstream_path,
            "/v2/org/image/manifests/latest?ns=value"
        );
    }

    #[tokio::test]
    async fn disabled_or_empty_route_is_explicitly_unavailable() {
        let directory = tempfile::tempdir().unwrap();
        let state = AppState::new(Config::for_test(directory.path().to_owned()))
            .await
            .unwrap();
        let uri: http::Uri = "/v2/ghcr/org/image/manifests/latest".parse().unwrap();
        let resolved = route_registry_path(&state, &uri).await.unwrap();
        assert_eq!(resolved.route.id, GHCR_ROUTE_ID);

        let response = handle_inner(
            state.clone(),
            Request::builder()
                .uri(uri.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap_err();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let route = state.registry_routes.get(GHCR_ROUTE_ID).await.unwrap();
        let mut disabled = route.into_active_model();
        disabled.enabled = sea_orm::ActiveValue::Set(false);
        disabled.update(&state.db).await.unwrap();
        let error = route_registry_path(&state, &uri).await.unwrap_err();
        assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn cached_blob_get_and_head_require_an_enabled_route_node() {
        let directory = tempfile::tempdir().unwrap();
        let state = AppState::new(Config::for_test(directory.path().to_owned()))
            .await
            .unwrap();
        let node = state
            .nodes
            .create(NodeInput {
                name: "docker".into(),
                url: "http://127.0.0.1:5001".into(),
                registry_route_id: DOCKER_HUB_ROUTE_ID,
                enabled: true,
                priority: 1,
                max_concurrency: 4,
                cf_preferred: false,
                connect_ip: None,
                auth_mode: "none".into(),
                auth_username: None,
                auth_header: None,
                auth_secret: None,
            })
            .await
            .unwrap();
        let digest = format!("sha256:{}", "a".repeat(64));
        let client_path = format!("/v2/test/blobs/{digest}");
        let upstream_path = format!("/v2/library/test/blobs/{digest}");
        let cache_key = CacheStore::key(&upstream_path, None);
        let temporary = directory.path().join("primed-blob");
        tokio::fs::write(&temporary, b"cached").await.unwrap();
        state
            .cache
            .admit(
                &cache_key,
                &temporary,
                "application/octet-stream",
                Some(digest),
            )
            .await
            .unwrap();

        let mut disabled = node.node;
        disabled.enabled = false;
        disabled.updated_at = chrono::Utc::now();
        crate::db::save_node(&state.db, disabled).await.unwrap();

        for method in [Method::GET, Method::HEAD] {
            let result = handle_inner(
                state.clone(),
                Request::builder()
                    .method(method.clone())
                    .uri(&client_path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
            let error = match result {
                Err(error) => error,
                Ok(response) => panic!("{method} unexpectedly returned {}", response.status()),
            };
            assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
    }

    #[tokio::test]
    async fn uses_the_rightmost_operation_marker_after_named_repository_components() {
        let directory = tempfile::tempdir().unwrap();
        let state = AppState::new(Config::for_test(directory.path().to_owned()))
            .await
            .unwrap();
        let uri: http::Uri = "/v2/team/manifests/blobs/tags/image/tags/list?n=10"
            .parse()
            .unwrap();
        let resolved = route_registry_path(&state, &uri).await.unwrap();
        assert_eq!(resolved.route.id, DOCKER_HUB_ROUTE_ID);
        assert_eq!(
            resolved.upstream_path,
            "/v2/team/manifests/blobs/tags/image/tags/list?n=10"
        );

        let short: http::Uri = "/v2/busybox/manifests/latest?source=test".parse().unwrap();
        let resolved = route_registry_path(&state, &short).await.unwrap();
        assert_eq!(
            resolved.upstream_path,
            "/v2/library/busybox/manifests/latest?source=test"
        );
    }

    #[test]
    fn cached_digest_etag_accepts_strong_weak_and_list_validators() {
        let digest = "sha256:abc";
        assert!(etag_matches(
            Some(&"\"sha256:abc\"".parse().unwrap()),
            digest
        ));
        assert!(etag_matches(
            Some(&"W/\"sha256:abc\"".parse().unwrap()),
            digest
        ));
        assert!(etag_matches(
            Some(&"\"other\", \"sha256:abc\"".parse().unwrap()),
            digest
        ));
        assert!(etag_matches(Some(&"*".parse().unwrap()), digest));
        assert!(!etag_matches(
            Some(&"\"sha256:def\"".parse().unwrap()),
            digest
        ));
    }
}
