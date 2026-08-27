use std::{path::Path, sync::Arc, time::Instant};

use crate::{
    cache::{CacheStore, CachedObject},
    config::{Config, SchedulerPolicy},
    error::{ApiResult, AppError},
    node_selection::{NodeLease, NodeSelector},
    nodes::{NodeService, NodeView},
    upstream::{RangeMode, UpstreamService},
};
use futures_util::{StreamExt, stream};
use http::{HeaderMap, header};
use http_content_range::ContentRange;
use moka::future::Cache;
use reqwest::StatusCode;
use sha2::{Digest, Sha256};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::RwLock,
};
use uuid::Uuid;

#[derive(Clone)]
pub struct Scheduler {
    runtime: Arc<RwLock<SchedulerRuntimeConfig>>,
    nodes: NodeService,
    cache: CacheStore,
    upstream: UpstreamService,
    selector: NodeSelector,
    capabilities: Cache<String, BlobCapabilities>,
}

#[derive(Clone, Copy)]
struct SchedulerRuntimeConfig {
    chunk_size: u64,
    adaptive_chunking_enabled: bool,
    parallel_threshold: u64,
    resumable_threshold: u64,
    chunk_concurrency: usize,
    scheduler_policy: SchedulerPolicy,
    stream_fallback_timeout: std::time::Duration,
}

#[derive(Clone, Copy)]
pub struct StreamDownloadConfig {
    pub chunk_size: u64,
    pub concurrency: usize,
    pub parallel_threshold: u64,
}

#[derive(Clone, Debug)]
struct BlobCapabilities {
    size: u64,
    supports_range: bool,
    media_type: String,
}

#[derive(Clone, Debug)]
struct Chunk {
    index: usize,
    start: u64,
    end: u64,
    total_size: u64,
}

struct ParallelDownloadRequest<'a> {
    nodes: &'a [NodeView],
    request_path: &'a str,
    request_headers: &'a HeaderMap,
    total_size: u64,
    temp_dir: &'a Path,
    merged: &'a Path,
    runtime: SchedulerRuntimeConfig,
}

impl Scheduler {
    pub fn new(
        config: Arc<Config>,
        nodes: NodeService,
        cache: CacheStore,
        upstream: UpstreamService,
    ) -> Self {
        Self {
            runtime: Arc::new(RwLock::new(SchedulerRuntimeConfig {
                chunk_size: config.chunk_size,
                adaptive_chunking_enabled: config.adaptive_chunking_enabled,
                parallel_threshold: config.parallel_threshold,
                resumable_threshold: config.resumable_threshold,
                chunk_concurrency: config.chunk_concurrency,
                scheduler_policy: config.scheduler_policy,
                stream_fallback_timeout: config.stream_fallback_timeout,
            })),
            nodes,
            cache,
            upstream,
            selector: NodeSelector::new(),
            capabilities: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(std::time::Duration::from_secs(600))
                .build(),
        }
    }

    pub async fn update_runtime(&self, config: &Config) {
        let mut runtime = self.runtime.write().await;
        runtime.chunk_size = config.chunk_size;
        runtime.adaptive_chunking_enabled = config.adaptive_chunking_enabled;
        runtime.parallel_threshold = config.parallel_threshold;
        runtime.resumable_threshold = config.resumable_threshold;
        runtime.chunk_concurrency = config.chunk_concurrency;
        runtime.scheduler_policy = config.scheduler_policy;
        runtime.stream_fallback_timeout = config.stream_fallback_timeout;
    }

    pub async fn stream_fallback_timeout(&self) -> std::time::Duration {
        self.runtime.read().await.stream_fallback_timeout
    }

    pub async fn ordered_stream_nodes(&self, nodes: &[NodeView], path: &str) -> Vec<NodeView> {
        let digest = Sha256::digest(path.as_bytes());
        let sequence = u64::from_be_bytes(digest[..8].try_into().expect("sha256 prefix")) as usize;
        let policy = self.runtime.read().await.scheduler_policy;
        self.ordered_nodes(nodes, sequence, policy)
            .into_iter()
            .cloned()
            .collect()
    }

    pub async fn stream_download_config(
        &self,
        total_size: u64,
        node_capacity: usize,
    ) -> StreamDownloadConfig {
        let runtime = *self.runtime.read().await;
        StreamDownloadConfig {
            chunk_size: effective_chunk_size(runtime, total_size, node_capacity),
            concurrency: runtime.chunk_concurrency,
            parallel_threshold: runtime.parallel_threshold,
        }
    }

    pub async fn fetch_blob(
        &self,
        request_path: &str,
        request_headers: &HeaderMap,
        expected_digest: Option<&str>,
        nodes: Vec<NodeView>,
    ) -> ApiResult<CachedObject> {
        if nodes.is_empty() {
            return Err(AppError::unavailable(
                "resolved Registry route has no enabled nodes",
            ));
        }
        let authorization = request_headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok());
        let key = CacheStore::key(request_path, authorization);
        if let Some(object) = self.cache.get(&key).await? {
            return Ok(object);
        }

        let guard = self.cache.lock(&key).await;
        if let Some(object) = self.cache.get(&key).await? {
            drop(guard);
            return Ok(object);
        }

        let result = self
            .fetch_uncached(&key, request_path, request_headers, expected_digest, &nodes)
            .await;
        drop(guard);
        result
    }

    async fn fetch_uncached(
        &self,
        key: &str,
        request_path: &str,
        request_headers: &HeaderMap,
        expected_digest: Option<&str>,
        nodes: &[NodeView],
    ) -> ApiResult<CachedObject> {
        let mut runtime = *self.runtime.read().await;
        let capability_key = format!("{}:{}", nodes[0].node.url, request_path);
        let capabilities = if let Some(value) = self.capabilities.get(&capability_key).await {
            value
        } else {
            let detected = self
                .detect_capabilities(nodes, request_path, request_headers)
                .await?;
            self.capabilities
                .insert(capability_key, detected.clone())
                .await;
            detected
        };
        let node_capacity = nodes
            .iter()
            .map(|node| usize::from(node.max_concurrency))
            .sum();
        runtime.chunk_size = effective_chunk_size(runtime, capabilities.size, node_capacity);

        let partial_dir = self.cache.temp_dir().join(key);
        tokio::fs::create_dir_all(&partial_dir).await?;
        let merged = partial_dir.join("object.partial");
        let can_parallel = capabilities.supports_range
            && capabilities.size >= runtime.parallel_threshold
            && nodes.len() > 1;

        if can_parallel {
            tracing::info!(
                path = request_path,
                size = capabilities.size,
                chunks = chunks(capabilities.size, runtime.chunk_size).len(),
                nodes = nodes.len(),
                "starting parallel Blob fetch"
            );
            if let Err(error) = self
                .download_parallel(ParallelDownloadRequest {
                    nodes,
                    request_path,
                    request_headers,
                    total_size: capabilities.size,
                    temp_dir: &partial_dir,
                    merged: &merged,
                    runtime,
                })
                .await
            {
                tracing::warn!(
                    ?error,
                    path = request_path,
                    "parallel fetch failed; falling back to one upstream"
                );
                let _ = tokio::fs::remove_file(&merged).await;
                self.download_whole(nodes, request_path, request_headers, &merged)
                    .await?;
            }
        } else if capabilities.supports_range
            && capabilities.size >= runtime.resumable_threshold
            && tokio::fs::metadata(&merged)
                .await
                .is_ok_and(|metadata| metadata.len() > 0 && metadata.len() < capabilities.size)
        {
            self.download_resume(
                nodes,
                request_path,
                request_headers,
                capabilities.size,
                &merged,
            )
            .await?;
        } else {
            self.download_whole(nodes, request_path, request_headers, &merged)
                .await?;
        }

        let actual_size = tokio::fs::metadata(&merged).await?.len();
        if capabilities.size > 0 && actual_size != capabilities.size {
            return Err(AppError::Integrity);
        }
        if let Some(digest) = expected_digest {
            verify_file(&merged, digest).await?;
        }

        let result = self
            .cache
            .admit(
                key,
                &merged,
                &capabilities.media_type,
                expected_digest.map(str::to_owned),
            )
            .await;
        if result.is_ok() {
            let _ = tokio::fs::remove_dir_all(&partial_dir).await;
        }
        result
    }

    async fn download_resume(
        &self,
        nodes: &[NodeView],
        request_path: &str,
        request_headers: &HeaderMap,
        total_size: u64,
        destination: &Path,
    ) -> ApiResult<()> {
        let mut last_error = None;
        let mut attempted = false;
        let policy = self.runtime.read().await.scheduler_policy;
        for node in self.ordered_nodes(nodes, 0, policy) {
            let offset = tokio::fs::metadata(destination).await?.len();
            if offset >= total_size {
                return Ok(());
            }
            if self.at_capacity(node.node.id, node.max_concurrency) {
                continue;
            }
            let lease = self.acquire(node.node.id, node.max_concurrency).await;
            attempted = true;
            let started = Instant::now();
            let result = async {
                let response = self
                    .upstream
                    .send(
                        node,
                        http::Method::GET,
                        request_path,
                        request_headers,
                        RangeMode::Exact(offset, total_size - 1),
                    )
                    .await?;
                if response.status() != StatusCode::PARTIAL_CONTENT
                    || !content_range_matches(
                        response.headers(),
                        offset,
                        total_size - 1,
                        total_size,
                    )
                {
                    return Err(AppError::Integrity);
                }
                append_response_to_file(response, destination, total_size - offset).await
            }
            .await;
            let bytes = tokio::fs::metadata(destination)
                .await
                .map(|m| m.len().saturating_sub(offset))
                .unwrap_or(0);
            self.observe_node(node.node.id, bytes, started.elapsed(), result.is_ok());
            drop(lease);
            self.nodes
                .record_transfer(node.node.id, bytes, started.elapsed(), result.is_ok())
                .await;
            match result {
                Ok(()) => return Ok(()),
                Err(error) => {
                    if matches!(&error, AppError::Integrity) {
                        let _ = tokio::fs::remove_file(destination).await;
                    }
                    last_error = Some(error)
                }
            }
        }
        if !attempted {
            tokio::time::sleep(capacity_backoff()).await;
            return Box::pin(self.download_resume(
                nodes,
                request_path,
                request_headers,
                total_size,
                destination,
            ))
            .await;
        }
        Err(last_error.unwrap_or_else(|| AppError::Upstream("all nodes failed resume".into())))
    }

    async fn detect_capabilities(
        &self,
        nodes: &[NodeView],
        request_path: &str,
        request_headers: &HeaderMap,
    ) -> ApiResult<BlobCapabilities> {
        let mut last_error = None;
        let mut baseline: Option<BlobCapabilities> = None;
        let mut all_compatible = true;
        for node in nodes {
            let started = Instant::now();
            let result = async {
                let response = self
                    .upstream
                    .send(
                        node,
                        http::Method::HEAD,
                        request_path,
                        request_headers,
                        RangeMode::Suppress,
                    )
                    .await?;
                if response.status().is_success()
                    && let Some(capabilities) = capabilities_from_head(&response)
                {
                    Ok(capabilities)
                } else {
                    let probe = self
                        .upstream
                        .send(
                            node,
                            http::Method::GET,
                            request_path,
                            request_headers,
                            RangeMode::Exact(0, 0),
                        )
                        .await?;
                    capabilities_from_probe(&probe).ok_or_else(|| {
                        AppError::Upstream(format!(
                            "{} does not expose Blob length or Range metadata",
                            node.node.name
                        ))
                    })
                }
            }
            .await;
            self.nodes
                .record_transfer(node.node.id, 0, started.elapsed(), result.is_ok())
                .await;
            match result {
                Ok(value) => {
                    if let Some(previous) = &baseline {
                        all_compatible &= previous.size == value.size
                            && previous.media_type == value.media_type
                            && previous.supports_range == value.supports_range;
                    } else {
                        baseline = Some(value);
                    }
                }
                Err(error) => {
                    all_compatible = false;
                    last_error = Some(error)
                }
            }
        }
        if let Some(mut capabilities) = baseline {
            capabilities.supports_range &= all_compatible;
            return Ok(capabilities);
        }
        Err(last_error.unwrap_or_else(|| AppError::Upstream("capability detection failed".into())))
    }

    async fn download_parallel(&self, request: ParallelDownloadRequest<'_>) -> ApiResult<()> {
        let chunks = chunks(request.total_size, request.runtime.chunk_size);
        let node_capacity = request
            .nodes
            .iter()
            .map(|node| usize::from(node.max_concurrency))
            .sum::<usize>();
        let concurrency = request
            .runtime
            .chunk_concurrency
            .min(node_capacity.max(1))
            .min(chunks.len())
            .max(1);
        let results = stream::iter(chunks.clone())
            .map(|chunk| {
                let scheduler = self.clone();
                let nodes = request.nodes.to_vec();
                let request_path = request.request_path.to_owned();
                let request_headers = request.request_headers.clone();
                let part_path = request.temp_dir.join(format!("{:08}.part", chunk.index));
                async move {
                    if tokio::fs::metadata(&part_path)
                        .await
                        .is_ok_and(|metadata| metadata.len() == chunk.end - chunk.start + 1)
                    {
                        return Ok((chunk.index, part_path));
                    }
                    scheduler
                        .download_chunk(
                            &nodes,
                            &request_path,
                            &request_headers,
                            &chunk,
                            &part_path,
                            request.runtime.scheduler_policy,
                        )
                        .await
                        .map(|_| (chunk.index, part_path))
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;

        let mut ordered = vec![None; chunks.len()];
        for result in results {
            let (index, path) = result?;
            ordered[index] = Some(path);
        }
        let mut output = File::create(request.merged).await?;
        for path in ordered {
            let path = path.ok_or(AppError::Integrity)?;
            let mut input = File::open(path).await?;
            tokio::io::copy(&mut input, &mut output).await?;
        }
        output.flush().await?;
        Ok(())
    }

    async fn download_chunk(
        &self,
        nodes: &[NodeView],
        request_path: &str,
        request_headers: &HeaderMap,
        chunk: &Chunk,
        destination: &Path,
        policy: SchedulerPolicy,
    ) -> ApiResult<()> {
        let mut last_error = None;
        let mut attempted = false;
        for node in self.ordered_nodes(nodes, chunk.index, policy) {
            if self.at_capacity(node.node.id, node.max_concurrency) {
                continue;
            }
            let lease = self.acquire(node.node.id, node.max_concurrency).await;
            attempted = true;
            let started = Instant::now();
            let result = async {
                let existing = tokio::fs::metadata(destination)
                    .await
                    .map(|metadata| metadata.len())
                    .unwrap_or(0);
                let chunk_size = chunk.end - chunk.start + 1;
                if existing > chunk_size {
                    let _ = tokio::fs::remove_file(destination).await;
                    return Err(AppError::Integrity);
                }
                let start = chunk.start + existing;
                if start > chunk.end {
                    return Ok(());
                }
                let response = self
                    .upstream
                    .send(
                        node,
                        http::Method::GET,
                        request_path,
                        request_headers,
                        RangeMode::Exact(start, chunk.end),
                    )
                    .await?;
                if response.status() != StatusCode::PARTIAL_CONTENT {
                    return Err(AppError::Upstream(format!(
                        "{} ignored Range with status {}",
                        node.node.name,
                        response.status()
                    )));
                }
                let expected = chunk.end - start + 1;
                if response.content_length() != Some(expected) {
                    return Err(AppError::Integrity);
                }
                if !content_range_matches(response.headers(), start, chunk.end, chunk.total_size) {
                    return Err(AppError::Integrity);
                }
                if existing > 0 {
                    append_response_to_file(response, destination, expected).await
                } else {
                    stream_response_to_file(response, destination, Some(expected)).await
                }
            }
            .await;
            self.observe_node(
                node.node.id,
                chunk.end - chunk.start + 1,
                started.elapsed(),
                result.is_ok(),
            );
            drop(lease);
            self.nodes
                .record_transfer(
                    node.node.id,
                    chunk.end - chunk.start + 1,
                    started.elapsed(),
                    result.is_ok(),
                )
                .await;
            match result {
                Ok(()) => return Ok(()),
                Err(error) => {
                    if matches!(&error, AppError::Integrity) {
                        let _ = tokio::fs::remove_file(destination).await;
                    }
                    last_error = Some(error)
                }
            }
        }
        if !attempted {
            tokio::time::sleep(capacity_backoff()).await;
            return Box::pin(self.download_chunk(
                nodes,
                request_path,
                request_headers,
                chunk,
                destination,
                policy,
            ))
            .await;
        }
        Err(last_error.unwrap_or_else(|| AppError::Upstream("all nodes failed a chunk".into())))
    }

    async fn download_whole(
        &self,
        nodes: &[NodeView],
        request_path: &str,
        request_headers: &HeaderMap,
        destination: &Path,
    ) -> ApiResult<()> {
        let mut last_error = None;
        let policy = self.runtime.read().await.scheduler_policy;
        for node in self.ordered_nodes(nodes, 0, policy) {
            if self.at_capacity(node.node.id, node.max_concurrency) {
                continue;
            }
            let lease = self.acquire(node.node.id, node.max_concurrency).await;
            let started = Instant::now();
            let result = async {
                let response = self
                    .upstream
                    .send(
                        node,
                        http::Method::GET,
                        request_path,
                        request_headers,
                        RangeMode::Suppress,
                    )
                    .await?;
                if !response.status().is_success() {
                    return Err(AppError::Upstream(format!(
                        "{} returned {}",
                        node.node.name,
                        response.status()
                    )));
                }
                let length = response.content_length();
                stream_response_to_file(response, destination, length).await
            }
            .await;
            let bytes = tokio::fs::metadata(destination)
                .await
                .map(|m| m.len())
                .unwrap_or(0);
            self.observe_node(node.node.id, bytes, started.elapsed(), result.is_ok());
            drop(lease);
            self.nodes
                .record_transfer(node.node.id, bytes, started.elapsed(), result.is_ok())
                .await;
            match result {
                Ok(()) => return Ok(()),
                Err(error) => {
                    // Keep a transport-truncated file for a future request to
                    // resume. Integrity failures are unsafe to resume and
                    // must start from a clean file.
                    if matches!(&error, AppError::Integrity) {
                        let _ = tokio::fs::remove_file(destination).await;
                    }
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| AppError::Upstream("all nodes failed".into())))
    }

    fn ordered_nodes<'a>(
        &self,
        nodes: &'a [NodeView],
        sequence: usize,
        policy: SchedulerPolicy,
    ) -> Vec<&'a NodeView> {
        self.selector.order(nodes, sequence, policy)
    }

    async fn acquire(&self, node_id: Uuid, max_concurrency: u16) -> NodeLease {
        loop {
            if let Some(lease) = self.try_acquire(node_id, max_concurrency) {
                return lease;
            }
            tokio::time::sleep(capacity_backoff()).await;
        }
    }

    pub(crate) fn try_acquire(&self, node_id: Uuid, max_concurrency: u16) -> Option<NodeLease> {
        self.selector.try_acquire(node_id, max_concurrency)
    }

    fn at_capacity(&self, node_id: Uuid, max_concurrency: u16) -> bool {
        self.selector.at_capacity(node_id, max_concurrency)
    }

    pub(crate) fn observe_node(
        &self,
        node_id: Uuid,
        bytes: u64,
        elapsed: std::time::Duration,
        success: bool,
    ) {
        self.selector.observe(node_id, bytes, elapsed, success);
    }
}

fn capacity_backoff() -> std::time::Duration {
    use backoff::backoff::Backoff;
    backoff::ExponentialBackoffBuilder::new()
        .with_initial_interval(std::time::Duration::from_millis(10))
        .with_max_interval(std::time::Duration::from_millis(250))
        .with_max_elapsed_time(None)
        .build()
        .next_backoff()
        .unwrap_or_else(|| std::time::Duration::from_millis(250))
}

fn effective_chunk_size(
    runtime: SchedulerRuntimeConfig,
    total_size: u64,
    node_capacity: usize,
) -> u64 {
    if !runtime.adaptive_chunking_enabled {
        return runtime.chunk_size;
    }
    const MIB: u64 = 1024 * 1024;
    const MIN_CHUNK: u64 = 2 * MIB;
    const MAX_CHUNK: u64 = 8 * MIB;
    const TARGET_WAVES: u64 = 4;
    let parallelism = runtime.chunk_concurrency.min(node_capacity.max(1)).max(1) as u64;
    let target_chunks = parallelism.saturating_mul(TARGET_WAVES);
    let ideal = total_size.saturating_add(target_chunks.saturating_sub(1)) / target_chunks;
    ideal
        .saturating_add(MIB - 1)
        .div_euclid(MIB)
        .saturating_mul(MIB)
        .clamp(MIN_CHUNK, MAX_CHUNK)
}

fn capabilities_from_head(response: &reqwest::Response) -> Option<BlobCapabilities> {
    Some(BlobCapabilities {
        size: positive_content_length(response.headers())?,
        supports_range: identity_encoded(response)
            && response
                .headers()
                .get(header::ACCEPT_RANGES)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.eq_ignore_ascii_case("bytes")),
        media_type: response_media_type(response),
    })
}

fn positive_content_length(headers: &HeaderMap) -> Option<u64> {
    let mut values = headers.get_all(header::CONTENT_LENGTH).iter();
    let value = values.next()?.as_bytes();
    if values.next().is_some() || value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return None;
    }
    value
        .iter()
        .try_fold(0_u64, |length, digit| {
            length.checked_mul(10)?.checked_add(u64::from(digit - b'0'))
        })
        .filter(|length| *length > 0)
}

fn capabilities_from_probe(response: &reqwest::Response) -> Option<BlobCapabilities> {
    if response.status() == StatusCode::PARTIAL_CONTENT {
        let range = response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(ContentRange::parse)?;
        let ContentRange::Bytes(range) = range else {
            return None;
        };
        if range.first_byte != 0 || range.last_byte != 0 {
            return None;
        }
        return Some(BlobCapabilities {
            size: range.complete_length,
            supports_range: identity_encoded(response),
            media_type: response_media_type(response),
        });
    }
    if response.status().is_success() {
        return response.content_length().map(|size| BlobCapabilities {
            size,
            supports_range: false,
            media_type: response_media_type(response),
        });
    }
    None
}

fn identity_encoded(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|value| value.eq_ignore_ascii_case("identity"))
}

fn response_media_type(response: &reqwest::Response) -> String {
    response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned()
}

fn content_range_matches(headers: &HeaderMap, start: u64, end: u64, total: u64) -> bool {
    matches!(
        headers
            .get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(ContentRange::parse),
        Some(ContentRange::Bytes(value))
            if value.first_byte == start
                && value.last_byte == end
                && value.complete_length == total
    )
}

async fn stream_response_to_file(
    response: reqwest::Response,
    destination: &Path,
    expected: Option<u64>,
) -> ApiResult<()> {
    let mut file = File::create(destination).await?;
    let mut received = 0_u64;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| AppError::Upstream(error.to_string()))?;
        received = received.saturating_add(chunk.len() as u64);
        if let Some(expected) = expected
            && received > expected
        {
            return Err(AppError::Integrity);
        }
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    if expected.is_some_and(|value| value != received) {
        return Err(AppError::Integrity);
    }
    Ok(())
}

async fn append_response_to_file(
    response: reqwest::Response,
    destination: &Path,
    expected: u64,
) -> ApiResult<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(destination)
        .await?;
    let mut received = 0_u64;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| AppError::Upstream(error.to_string()))?;
        received = received.saturating_add(chunk.len() as u64);
        if received > expected {
            return Err(AppError::Integrity);
        }
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    if received != expected {
        return Err(AppError::Integrity);
    }
    Ok(())
}

fn chunks(total: u64, chunk_size: u64) -> Vec<Chunk> {
    let mut result = Vec::new();
    let mut start = 0;
    while start < total {
        let end = (start + chunk_size - 1).min(total - 1);
        result.push(Chunk {
            index: result.len(),
            start,
            end,
            total_size: total,
        });
        start = end + 1;
    }
    result
}

async fn verify_file(path: &Path, expected_digest: &str) -> ApiResult<()> {
    let expected = expected_digest
        .strip_prefix("sha256:")
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| AppError::bad_request("unsupported or invalid Blob digest"))?;
    let mut file = File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual = hex::encode(hasher.finalize());
    if !constant_time_eq::constant_time_eq(actual.as_bytes(), expected.as_bytes()) {
        return Err(AppError::Integrity);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_runtime(adaptive: bool) -> SchedulerRuntimeConfig {
        SchedulerRuntimeConfig {
            chunk_size: 512 * 1024,
            adaptive_chunking_enabled: adaptive,
            parallel_threshold: 1024 * 1024,
            resumable_threshold: 1024 * 1024,
            chunk_concurrency: 4,
            scheduler_policy: SchedulerPolicy::Balanced,
            stream_fallback_timeout: std::time::Duration::from_secs(1),
        }
    }

    #[test]
    fn splits_without_gaps_or_overlap() {
        let parts = chunks(11, 4);
        assert_eq!(
            parts.iter().map(|p| (p.start, p.end)).collect::<Vec<_>>(),
            vec![(0, 3), (4, 7), (8, 10)]
        );
    }

    #[test]
    fn adaptive_chunk_size_stays_between_two_and_eight_mib() {
        let runtime = test_runtime(true);
        assert_eq!(
            effective_chunk_size(runtime, 8 * 1024 * 1024, 4),
            2 * 1024 * 1024
        );
        assert_eq!(
            effective_chunk_size(runtime, 1024 * 1024 * 1024, 4),
            8 * 1024 * 1024
        );
        let medium = effective_chunk_size(runtime, 64 * 1024 * 1024, 4);
        assert!((2 * 1024 * 1024..=8 * 1024 * 1024).contains(&medium));
    }

    #[test]
    fn disabled_adaptive_chunking_uses_exact_configured_size() {
        assert_eq!(
            effective_chunk_size(test_runtime(false), 1024 * 1024 * 1024, 16),
            512 * 1024
        );
    }

    #[test]
    fn content_range_must_match_the_assigned_chunk() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_RANGE, "bytes 10-19/100".parse().unwrap());
        assert!(content_range_matches(&headers, 10, 19, 100));
        assert!(!content_range_matches(&headers, 0, 9, 100));
        assert!(!content_range_matches(&headers, 10, 19, 200));
    }

    #[test]
    fn head_length_requires_one_positive_decimal_wire_header() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_LENGTH, "16".parse().unwrap());
        assert_eq!(positive_content_length(&headers), Some(16));

        for invalid in ["0", "+16", " 16", "16 ", "16, 16", "invalid"] {
            headers.insert(header::CONTENT_LENGTH, invalid.parse().unwrap());
            assert_eq!(positive_content_length(&headers), None, "{invalid}");
        }

        headers.insert(
            header::CONTENT_LENGTH,
            "18446744073709551616".parse().unwrap(),
        );
        assert_eq!(positive_content_length(&headers), None);

        headers.insert(header::CONTENT_LENGTH, "16".parse().unwrap());
        headers.append(header::CONTENT_LENGTH, "16".parse().unwrap());
        assert_eq!(positive_content_length(&headers), None);
    }

    #[tokio::test]
    async fn verifies_digest_without_loading_whole_blob() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob");
        tokio::fs::write(&path, b"hello world").await.unwrap();
        verify_file(
            &path,
            "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
        )
        .await
        .unwrap();
    }
}
