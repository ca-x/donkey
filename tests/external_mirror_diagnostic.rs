//! Manual network diagnostic for real Docker Hub mirrors.
//!
//! This test is deliberately ignored because mirror availability and routing
//! differ by region. Run it explicitly with:
//! `cargo test --test external_mirror_diagnostic -- --ignored --nocapture`

use std::time::{Duration, Instant};

use axum::{Router, body::Body, http::header};
use donkey::{
    AppState, Config, nodes::NodeInput, registry_routes::DOCKER_HUB_ROUTE_ID,
    server::registry_router,
};
use http::{HeaderMap, Method, Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

const BASELINE: &[&str] = &["https://docker.1ms.run/"];
const MULTI_SOURCE: &[&str] = &[
    "https://docker.1panel.live/",
    "https://docker.m.daocloud.io/",
    "https://docker.xuanyuan.run/",
];
const MANIFEST_ACCEPT: &str = "application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json";

struct ProbeResult {
    digest: String,
    bytes: usize,
    elapsed: Duration,
}

#[tokio::test]
#[ignore = "manual external mirror diagnostic; regional network access is not CI-stable"]
async fn compare_single_1ms_with_multi_source_donkey() {
    let baseline = probe(BASELINE).await;
    let multi = probe(MULTI_SOURCE).await;

    eprintln!(
        "redis:latest largest layer\nbaseline: digest={} bytes={} elapsed={:?}\nmulti:    digest={} bytes={} elapsed={:?}",
        baseline.digest, baseline.bytes, baseline.elapsed, multi.digest, multi.bytes, multi.elapsed
    );
    assert_eq!(baseline.digest, multi.digest);
    assert_eq!(baseline.bytes, multi.bytes);
}

async fn probe(endpoints: &[&str]) -> ProbeResult {
    let directory = tempfile::tempdir().unwrap();
    let mut config = Config::for_test(directory.path().to_owned());
    config.chunk_size = 2 * 1024 * 1024;
    config.chunk_concurrency = 8;
    config.parallel_threshold = 4 * 1024 * 1024;
    config.stream_fallback_timeout = Duration::from_secs(1);
    config.upstream_timeout = Duration::from_secs(30);
    let state = AppState::new(config).await.unwrap();
    for (index, endpoint) in endpoints.iter().enumerate() {
        state
            .nodes
            .create(NodeInput {
                name: format!("external-diagnostic-{index}"),
                url: (*endpoint).to_owned(),
                registry_route_id: DOCKER_HUB_ROUTE_ID,
                enabled: true,
                priority: index as i32,
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
    }
    let router = registry_router(state);
    let index = manifest(router.clone(), "latest").await;
    let manifest = if let Some(manifests) = index.get("manifests").and_then(Value::as_array) {
        let descriptor = manifests
            .iter()
            .find(|descriptor| {
                descriptor.pointer("/platform/os").and_then(Value::as_str) == Some("linux")
                    && descriptor
                        .pointer("/platform/architecture")
                        .and_then(Value::as_str)
                        == Some("amd64")
            })
            .expect("linux/amd64 descriptor");
        manifest(
            router.clone(),
            descriptor["digest"].as_str().expect("manifest digest"),
        )
        .await
    } else {
        index
    };
    let layer = manifest["layers"]
        .as_array()
        .expect("image layers")
        .iter()
        .max_by_key(|layer| layer["size"].as_u64().unwrap_or(0))
        .expect("largest layer");
    let digest = layer["digest"].as_str().expect("layer digest").to_owned();
    let expected = layer["size"].as_u64().expect("layer size") as usize;
    let started = Instant::now();
    let response = request(
        router,
        Method::GET,
        &format!("/v2/library/redis/blobs/{digest}"),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(body.len(), expected);
    ProbeResult {
        digest,
        bytes: body.len(),
        elapsed,
    }
}

async fn manifest(router: Router, reference: &str) -> Value {
    let response = request(
        router,
        Method::GET,
        &format!("/v2/library/redis/manifests/{reference}"),
        Some(MANIFEST_ACCEPT),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn request(
    router: Router,
    method: Method,
    uri: &str,
    accept: Option<&str>,
) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    if let Some(accept) = accept {
        headers.insert(header::ACCEPT, accept.parse().unwrap());
    }
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    *request.headers_mut() = headers;
    router.oneshot(request).await.unwrap()
}
