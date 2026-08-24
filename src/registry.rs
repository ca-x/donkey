use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use tower::ServiceExt;
use tower_http::services::ServeFile;

use crate::{
    cache::{CacheStore, CachedObject},
    db::registry_route,
    error::{ApiResult, AppError},
    registry_routes::RepositoryMode,
    state::AppState,
};

pub async fn handle(State(state): State<AppState>, request: Request) -> Response {
    match handle_inner(state, request).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn handle_inner(state: AppState, request: Request) -> ApiResult<Response> {
    if !request.uri().path().starts_with("/v2") {
        return match crate::domainfold::proxy_if_mapping(&state, request).await? {
            Ok(response) => Ok(response),
            Err(_) => Err(AppError::not_found("route")),
        };
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
    if upstream_path.contains("/blobs/") {
        let digest = upstream_path
            .rsplit_once("/blobs/")
            .map(|(_, digest)| digest)
            .filter(|digest| digest.starts_with("sha256:"));
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
            let object = state
                .scheduler
                .fetch_blob(&upstream_path, request.headers(), digest, registry_route_id)
                .await?;
            return serve_cached(&state.cache, object, request).await;
        }
    }
    let method = request.method().clone();
    let headers = request.headers().clone();
    proxy_passthrough(&state, method, upstream_path, headers, registry_route_id).await
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

async fn proxy_passthrough(
    state: &AppState,
    method: Method,
    path: String,
    request_headers: HeaderMap,
    registry_route_id: uuid::Uuid,
) -> ApiResult<Response> {
    let nodes = state
        .nodes
        .enabled_registry_nodes(registry_route_id)
        .await?;
    if nodes.is_empty() {
        return Err(AppError::unavailable(
            "resolved Registry route has no enabled nodes",
        ));
    }
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
            if upstream_response.status().is_server_error() {
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
        .find_map(|marker| rest.rfind(marker).map(|index| (marker, index)));
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
