use axum::{
    body::Body,
    extract::Request,
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use futures_util::StreamExt;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::{
    db::{self, domain_mapping},
    error::{ApiResult, AppError},
    security,
    state::AppState,
};

#[derive(Clone, Debug, Deserialize)]
pub struct MappingInput {
    pub source_host: String,
    pub upstream_base: String,
    pub public_base: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct ConvertInput {
    pub url: String,
}

#[derive(Debug, Serialize)]
pub struct ConvertOutput {
    pub original_url: String,
    pub accelerated_url: String,
    pub mapping_id: Uuid,
}

fn default_true() -> bool {
    true
}

pub async fn create(
    db: &DatabaseConnection,
    input: MappingInput,
) -> ApiResult<domain_mapping::Model> {
    let input = validate(input)?;
    let now = Utc::now();
    db::insert_mapping(
        db,
        domain_mapping::Model {
            id: Uuid::new_v4(),
            source_host: input.source_host,
            upstream_base: input.upstream_base,
            public_base: input.public_base,
            enabled: input.enabled,
            created_at: now,
            updated_at: now,
        },
    )
    .await
    .map_err(Into::into)
}

pub async fn update(
    db: &DatabaseConnection,
    id: Uuid,
    input: MappingInput,
) -> ApiResult<domain_mapping::Model> {
    let input = validate(input)?;
    let mut model = db::get_mapping(db, id)
        .await?
        .ok_or_else(|| AppError::not_found("mapping"))?;
    model.source_host = input.source_host;
    model.upstream_base = input.upstream_base;
    model.public_base = input.public_base;
    model.enabled = input.enabled;
    model.updated_at = Utc::now();
    db::save_mapping(db, model).await.map_err(Into::into)
}

pub async fn delete(db: &DatabaseConnection, id: Uuid) -> ApiResult<()> {
    if db::delete_mapping(db, id).await? == 0 {
        return Err(AppError::not_found("mapping"));
    }
    Ok(())
}

pub async fn convert(db: &DatabaseConnection, raw: &str) -> ApiResult<ConvertOutput> {
    if raw.len() > 4096 {
        return Err(AppError::bad_request("URL is too long"));
    }
    let original = Url::parse(raw).map_err(|_| AppError::bad_request("invalid URL"))?;
    if !matches!(original.scheme(), "http" | "https") {
        return Err(AppError::bad_request(
            "only HTTP and HTTPS URLs can be converted",
        ));
    }
    let host = original
        .host_str()
        .ok_or_else(|| AppError::bad_request("URL has no host"))?;
    let mappings = db::list_mappings(db).await?;
    let mapping = mappings
        .into_iter()
        .filter(|mapping| mapping.enabled)
        .find(|mapping| host.eq_ignore_ascii_case(&mapping.source_host))
        .ok_or_else(|| AppError::bad_request("this domain has no acceleration mapping"))?;

    let upstream = Url::parse(&mapping.upstream_base).map_err(AppError::internal)?;
    if original.scheme() != upstream.scheme()
        || original.host_str() != upstream.host_str()
        || original.port_or_known_default() != upstream.port_or_known_default()
        || upstream.username() != ""
        || upstream.password().is_some()
    {
        return Err(AppError::bad_request(
            "URL does not match the mapping upstream",
        ));
    }
    let upstream_path = upstream.path().trim_end_matches('/');
    if !upstream_path.is_empty()
        && original.path() != upstream_path
        && !original
            .path()
            .strip_prefix(upstream_path)
            .is_some_and(|suffix| suffix.starts_with('/'))
    {
        return Err(AppError::bad_request(
            "URL does not match the mapping upstream path",
        ));
    }
    let suffix = original
        .path()
        .strip_prefix(upstream_path)
        .unwrap_or(original.path());
    let mut accelerated = Url::parse(&mapping.public_base)
        .map_err(AppError::internal)?
        .join(suffix.trim_start_matches('/'))
        .map_err(AppError::internal)?;
    accelerated.set_query(original.query());
    Ok(ConvertOutput {
        original_url: original.to_string(),
        accelerated_url: accelerated.to_string(),
        mapping_id: mapping.id,
    })
}

pub async fn proxy_if_mapping(
    state: &AppState,
    request: Request,
) -> ApiResult<Result<Response, Request>> {
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<http::uri::Authority>().ok())
        .map(|authority| {
            authority
                .host()
                .trim_matches(['[', ']'])
                .to_ascii_lowercase()
        });
    let Some(host) = host else {
        return Ok(Err(request));
    };
    let mappings = db::list_mappings(&state.db).await?;
    let mapping = mappings
        .into_iter()
        .filter(|mapping| mapping.enabled)
        .find(|mapping| {
            Url::parse(&mapping.public_base)
                .ok()
                .and_then(|url| {
                    url.host_str()
                        .map(|value| value.eq_ignore_ascii_case(&host))
                })
                .unwrap_or(false)
        });
    let Some(mapping) = mapping else {
        return Ok(Err(request));
    };
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        return Ok(Ok(StatusCode::METHOD_NOT_ALLOWED.into_response()));
    }
    let public = Url::parse(&mapping.public_base).map_err(AppError::internal)?;
    let public_prefix = public.path().trim_end_matches('/');
    let request_path = request.uri().path();
    let suffix = if public_prefix.is_empty() {
        request_path
    } else if request_path == public_prefix {
        ""
    } else {
        request_path
            .strip_prefix(&format!("{public_prefix}/"))
            .ok_or_else(|| {
                AppError::bad_request("request path does not match the DomainFold mapping")
            })?
    };
    let mut upstream = Url::parse(&mapping.upstream_base)
        .map_err(AppError::internal)?
        .join(suffix.trim_start_matches('/'))
        .map_err(AppError::internal)?;
    upstream.set_query(request.uri().query());
    let method = request.method().clone();
    let headers = request.headers().clone();
    let response = send_domain_request(state, method, upstream, &headers).await?;
    Ok(Ok(response))
}

async fn send_domain_request(
    state: &AppState,
    method: Method,
    mut url: Url,
    headers: &HeaderMap,
) -> ApiResult<Response> {
    for redirect_count in 0..=3 {
        let validated = security::validate_target_url(url.as_str(), &state.config).await?;
        let client = security::client_for(&validated, state.config.upstream_timeout)?;
        let mut builder = client.request(method.clone(), validated.url);
        for name in [
            header::ACCEPT,
            header::RANGE,
            header::IF_NONE_MATCH,
            header::IF_MODIFIED_SINCE,
        ] {
            if let Some(value) = headers.get(&name) {
                builder = builder.header(name, value);
            }
        }
        let upstream = builder
            .header(header::ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(|error| AppError::Upstream(error.to_string()))?;
        if upstream.status().is_redirection() {
            if redirect_count == 3 {
                return Err(AppError::Upstream("too many DomainFold redirects".into()));
            }
            let location = upstream
                .headers()
                .get(header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| AppError::Upstream("DomainFold redirect has no Location".into()))?;
            url = url.join(location).map_err(AppError::internal)?;
            continue;
        }
        let status = upstream.status();
        let response_headers = upstream.headers().clone();
        let body = Body::from_stream(
            upstream
                .bytes_stream()
                .map(|chunk| chunk.map_err(std::io::Error::other)),
        );
        let mut response = Response::new(body);
        *response.status_mut() = status;
        for name in [
            header::CONTENT_TYPE,
            header::CONTENT_LENGTH,
            header::CONTENT_RANGE,
            header::CONTENT_DISPOSITION,
            header::ACCEPT_RANGES,
            header::ETAG,
            header::LAST_MODIFIED,
            header::CACHE_CONTROL,
        ] {
            if let Some(value) = response_headers.get(&name) {
                response.headers_mut().insert(name, value.clone());
            }
        }
        return Ok(response);
    }
    Err(AppError::Upstream("DomainFold request failed".into()))
}

fn validate(mut input: MappingInput) -> ApiResult<MappingInput> {
    input.source_host = input.source_host.trim().to_ascii_lowercase();
    if input.source_host.is_empty()
        || input.source_host.len() > 253
        || input.source_host.contains('/')
        || input.source_host.contains(':')
    {
        return Err(AppError::bad_request("invalid source host"));
    }
    input.upstream_base = normalize_base(&input.upstream_base)?;
    input.public_base = normalize_base(&input.public_base)?;
    Ok(input)
}

fn normalize_base(raw: &str) -> ApiResult<String> {
    let mut url = Url::parse(raw).map_err(|_| AppError::bad_request("invalid mapping base URL"))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(AppError::bad_request("mapping base must be an HTTP(S) URL"));
    }
    url.set_query(None);
    url.set_fragment(None);
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use axum::body::to_bytes;
    use httpmock::prelude::*;

    #[tokio::test]
    async fn converts_query_and_path() {
        let db = db::connect("sqlite::memory:").await.unwrap();
        create(
            &db,
            MappingInput {
                source_host: "github.com".into(),
                upstream_base: "https://github.com/".into(),
                public_base: "https://gh.example:5443/".into(),
                enabled: true,
            },
        )
        .await
        .unwrap();
        let output = convert(&db, "https://github.com/org/repo/releases/a.zip?download=1")
            .await
            .unwrap();
        assert_eq!(
            output.accelerated_url,
            "https://gh.example:5443/org/repo/releases/a.zip?download=1"
        );
    }

    #[tokio::test]
    async fn rejects_same_host_paths_outside_mapping_prefix() {
        let db = db::connect("sqlite::memory:").await.unwrap();
        create(
            &db,
            MappingInput {
                source_host: "downloads.example".into(),
                upstream_base: "https://downloads.example/releases/".into(),
                public_base: "https://mirror.example/".into(),
                enabled: true,
            },
        )
        .await
        .unwrap();
        let error = convert(&db, "https://downloads.example/private/secret.tar.gz")
            .await
            .unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn proxies_only_an_explicit_public_mapping() {
        let upstream = MockServer::start_async().await;
        let response_mock = upstream
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/files/release.bin")
                    .query_param("download", "1");
                then.status(200)
                    .header("content-type", "application/octet-stream")
                    .body("mapped-content");
            })
            .await;
        let directory = tempfile::tempdir().unwrap();
        let state = AppState::new(Config::for_test(directory.path().to_owned()))
            .await
            .unwrap();
        create(
            &state.db,
            MappingInput {
                source_host: "downloads.example".into(),
                upstream_base: format!("{}/files/", upstream.base_url()),
                public_base: "http://donkey.test:5443/dl/".into(),
                enabled: true,
            },
        )
        .await
        .unwrap();
        let request = Request::builder()
            .uri("/dl/release.bin?download=1")
            .header(header::HOST, "donkey.test:5443")
            .body(Body::empty())
            .unwrap();
        let response = proxy_if_mapping(&state, request).await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(&body[..], b"mapped-content");
        response_mock.assert_async().await;

        let invalid = Request::builder()
            .uri("/dl-evil/release.bin")
            .header(header::HOST, "donkey.test:5443")
            .body(Body::empty())
            .unwrap();
        assert!(proxy_if_mapping(&state, invalid).await.is_err());
    }
}
