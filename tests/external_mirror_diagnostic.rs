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
const ALL_SOURCE: &[&str] = &[
    "https://docker.1ms.run/",
    "https://docker.1panel.live/",
    "https://docker.m.daocloud.io/",
    "https://docker.xuanyuan.run/",
];
const MANIFEST_ACCEPT: &str = "application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json";

struct ProbeResult {
    digest: String,
    bytes: usize,
    elapsed: Duration,
    node_metrics: Vec<(String, u64, u64)>,
    parallel_blobs: u64,
    retries: u64,
    chunk_size: u64,
}

#[tokio::test]
#[ignore = "manual external mirror diagnostic; regional network access is not CI-stable"]
async fn compare_single_1ms_with_multi_source_donkey() {
    let baseline = probe("redis", "latest", BASELINE).await;
    let multi = probe("redis", "latest", MULTI_SOURCE).await;

    eprintln!(
        "redis:latest largest layer\nbaseline: digest={} bytes={} elapsed={:?} nodes={:?} parallel={} retries={} chunk={}\nmulti:    digest={} bytes={} elapsed={:?} nodes={:?} parallel={} retries={} chunk={}",
        baseline.digest,
        baseline.bytes,
        baseline.elapsed,
        baseline.node_metrics,
        baseline.parallel_blobs,
        baseline.retries,
        baseline.chunk_size,
        multi.digest,
        multi.bytes,
        multi.elapsed,
        multi.node_metrics,
        multi.parallel_blobs,
        multi.retries,
        multi.chunk_size
    );
    assert_eq!(baseline.digest, multi.digest);
    assert_eq!(baseline.bytes, multi.bytes);
}

#[tokio::test]
#[ignore = "manual external mirror diagnostic; regional network access is not CI-stable"]
async fn compare_golang_single_1ms_with_multi_source_donkey() {
    let baseline = probe("golang", "latest", BASELINE).await;
    let multi = probe("golang", "latest", MULTI_SOURCE).await;

    eprintln!(
        "golang:latest largest layer\nbaseline: digest={} bytes={} elapsed={:?} nodes={:?} parallel={} retries={} chunk={}\nmulti:    digest={} bytes={} elapsed={:?} nodes={:?} parallel={} retries={} chunk={}",
        baseline.digest,
        baseline.bytes,
        baseline.elapsed,
        baseline.node_metrics,
        baseline.parallel_blobs,
        baseline.retries,
        baseline.chunk_size,
        multi.digest,
        multi.bytes,
        multi.elapsed,
        multi.node_metrics,
        multi.parallel_blobs,
        multi.retries,
        multi.chunk_size
    );
    assert_eq!(baseline.digest, multi.digest);
    assert_eq!(baseline.bytes, multi.bytes);
}

#[tokio::test]
#[ignore = "manual external mirror diagnostic; regional network access is not CI-stable"]
async fn probe_golang_multi_source_donkey() {
    let multi = probe("golang", "latest", MULTI_SOURCE).await;
    eprintln!(
        "golang:latest multi-source largest layer\ndigest={} bytes={} elapsed={:?} nodes={:?} parallel={} retries={} chunk={}",
        multi.digest,
        multi.bytes,
        multi.elapsed,
        multi.node_metrics,
        multi.parallel_blobs,
        multi.retries,
        multi.chunk_size
    );
}

#[tokio::test]
#[ignore = "manual external mirror diagnostic; regional network access is not CI-stable"]
async fn probe_golang_all_configured_sources_donkey() {
    let multi = probe("golang", "latest", ALL_SOURCE).await;
    eprintln!(
        "golang:latest all-source largest layer\ndigest={} bytes={} elapsed={:?} nodes={:?} parallel={} retries={} chunk={}",
        multi.digest,
        multi.bytes,
        multi.elapsed,
        multi.node_metrics,
        multi.parallel_blobs,
        multi.retries,
        multi.chunk_size
    );
}

async fn probe(repository: &str, reference: &str, endpoints: &[&str]) -> ProbeResult {
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
    let router = registry_router(state.clone());
    let index = manifest(router.clone(), repository, reference).await;
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
            repository,
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
        &format!("/v2/library/{repository}/blobs/{digest}"),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = match axum::body::to_bytes(response.into_body(), usize::MAX).await {
        Ok(body) => body,
        Err(error) => {
            eprintln!(
                "Blob body failed: {error}; nodes={:?}",
                state
                    .nodes
                    .list()
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|node| {
                        (
                            node.node.name,
                            node.metric.speed_bps,
                            node.metric.total_bytes,
                        )
                    })
                    .collect::<Vec<_>>()
            );
            panic!("Blob body failed: {error}");
        }
    };
    let elapsed = started.elapsed();
    assert_eq!(body.len(), expected);
    let node_metrics = state
        .nodes
        .list()
        .await
        .unwrap()
        .into_iter()
        .map(|node| {
            (
                node.node.name,
                node.metric.speed_bps.max(0) as u64,
                node.metric.total_bytes.max(0) as u64,
            )
        })
        .collect();
    let scheduler = state.scheduler.stats();
    ProbeResult {
        digest,
        bytes: body.len(),
        elapsed,
        node_metrics,
        parallel_blobs: scheduler.parallel_blobs,
        retries: scheduler.retry_attempts,
        chunk_size: scheduler.last_chunk_size,
    }
}

async fn manifest(router: Router, repository: &str, reference: &str) -> Value {
    let response = request(
        router,
        Method::GET,
        &format!("/v2/library/{repository}/manifests/{reference}"),
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
