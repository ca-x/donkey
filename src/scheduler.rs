use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use crate::{
    blob_planner::{
        BlobDownloadPlan, BlobMeta, BlobPlanner, DownloadStrategy, NodeSnapshot, PlannerConfig,
        PlannerError,
    },
    cache::{CacheStore, CachedObject},
    config::{Config, SchedulerPolicy},
    error::{ApiResult, AppError},
    node_selection::{NodeLease, NodeSelector},
    nodes::{NodeService, NodeView},
    resource_budget::{GLOBAL_STREAM_MEMORY_BYTES, STREAM_MEMORY_BYTES, StreamMemoryBudget},
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
    sync::{RwLock, Semaphore},
};
use uuid::Uuid;

#[derive(Clone)]
pub struct Scheduler {
    runtime: Arc<RwLock<SchedulerRuntimeConfig>>,
    nodes: NodeService,
    cache: CacheStore,
    upstream: UpstreamService,
    selector: NodeSelector,
    stats: Arc<SchedulerCounters>,
    capabilities: Cache<String, BlobCapabilities>,
    stream_memory: Arc<Semaphore>,
}

pub(crate) struct BlobFetchResult {
    pub object: CachedObject,
    pub cache_hit: bool,
}

#[derive(Default)]
struct SchedulerCounters {
    parallel_blobs: AtomicU64,
    resume_attempts: AtomicU64,
    retry_attempts: AtomicU64,
    last_chunk_size: AtomicU64,
}

#[derive(Clone, Copy)]
pub struct SchedulerStats {
    pub parallel_blobs: u64,
    pub resume_attempts: u64,
    pub retry_attempts: u64,
    pub last_chunk_size: u64,
    pub cooling_nodes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchedulerAlgorithmKind {
    Current,
    ProjectedCompletion,
}

impl SchedulerAlgorithmKind {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "current-balanced" | "current" => Some(Self::Current),
            "projected-completion" | "projected" => Some(Self::ProjectedCompletion),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current-balanced",
            Self::ProjectedCompletion => "projected-completion",
        }
    }
}

#[derive(Clone, Copy)]
struct SchedulerRuntimeConfig {
    chunk_size: u64,
    adaptive_chunking_enabled: bool,
    automatic_concurrency_enabled: bool,
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
    size: Option<u64>,
    supports_range: bool,
    media_type: String,
}

struct DetectedCapabilities {
    size: Option<u64>,
    media_type: String,
    nodes: Vec<NodeView>,
    range_nodes: Vec<NodeView>,
    pending_nodes: Vec<NodeView>,
    fallback_nodes: Vec<NodeView>,
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
        Self::new_from_config(config, nodes, cache, upstream, None)
    }

    pub fn new_with_algorithm(
        config: Arc<Config>,
        nodes: NodeService,
        cache: CacheStore,
        upstream: UpstreamService,
        algorithm: SchedulerAlgorithmKind,
    ) -> Self {
        Self::new_from_config(config, nodes, cache, upstream, Some(algorithm))
    }

    fn new_from_config(
        config: Arc<Config>,
        nodes: NodeService,
        cache: CacheStore,
        upstream: UpstreamService,
        algorithm_override: Option<SchedulerAlgorithmKind>,
    ) -> Self {
        let algorithm = algorithm_override.unwrap_or_else(|| {
            SchedulerAlgorithmKind::parse(&config.scheduler_algorithm)
                .unwrap_or(SchedulerAlgorithmKind::Current)
        });
        let implementation: Arc<dyn crate::scheduling::SchedulerAlgorithm> = match algorithm {
            SchedulerAlgorithmKind::Current => Arc::new(crate::scheduling::CurrentAlgorithm),
            SchedulerAlgorithmKind::ProjectedCompletion => {
                Arc::new(crate::scheduling::ProjectedCompletionAlgorithm)
            }
        };
        let selector = NodeSelector::with_capacity(implementation, nodes.capacity.clone());
        Self {
            runtime: Arc::new(RwLock::new(SchedulerRuntimeConfig {
                chunk_size: config.chunk_size,
                adaptive_chunking_enabled: config.adaptive_chunking_enabled,
                automatic_concurrency_enabled: config.automatic_concurrency_enabled,
                parallel_threshold: config.parallel_threshold,
                resumable_threshold: config.resumable_threshold,
                chunk_concurrency: config.chunk_concurrency,
                scheduler_policy: config.scheduler_policy,
                stream_fallback_timeout: config.stream_fallback_timeout,
            })),
            nodes,
            cache,
            upstream,
            selector,
            stats: Arc::new(SchedulerCounters::default()),
            capabilities: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(std::time::Duration::from_secs(600))
                .build(),
            stream_memory: Arc::new(Semaphore::new(GLOBAL_STREAM_MEMORY_BYTES as usize)),
        }
    }

    pub fn algorithm_name(&self) -> &'static str {
        self.selector.algorithm_name()
    }

    pub async fn update_runtime(&self, config: &Config) {
        let mut runtime = self.runtime.write().await;
        runtime.chunk_size = config.chunk_size;
        runtime.adaptive_chunking_enabled = config.adaptive_chunking_enabled;
        runtime.automatic_concurrency_enabled = config.automatic_concurrency_enabled;
        runtime.parallel_threshold = config.parallel_threshold;
        runtime.resumable_threshold = config.resumable_threshold;
        runtime.chunk_concurrency = config.chunk_concurrency;
        runtime.scheduler_policy = config.scheduler_policy;
        runtime.stream_fallback_timeout = config.stream_fallback_timeout;
        drop(runtime);
        let algorithm = SchedulerAlgorithmKind::parse(&config.scheduler_algorithm)
            .unwrap_or(SchedulerAlgorithmKind::Current);
        let implementation: Arc<dyn crate::scheduling::SchedulerAlgorithm> = match algorithm {
            SchedulerAlgorithmKind::Current => Arc::new(crate::scheduling::CurrentAlgorithm),
            SchedulerAlgorithmKind::ProjectedCompletion => {
                Arc::new(crate::scheduling::ProjectedCompletionAlgorithm)
            }
        };
        self.selector.set_algorithm(implementation);
    }

    pub async fn stream_fallback_timeout(&self) -> std::time::Duration {
        self.runtime.read().await.stream_fallback_timeout
    }

    pub async fn ordered_stream_nodes(&self, nodes: &[NodeView], path: &str) -> Vec<NodeView> {
        let sequence = Self::sequence_for(path, 0);
        let policy = self.runtime.read().await.scheduler_policy;
        self.ordered_nodes_with_context(
            nodes,
            crate::scheduling::ScheduleContext {
                sequence,
                blob_size: 0,
                range_required: false,
                chunk: None,
            },
            policy,
        )
        .into_iter()
        .cloned()
        .collect()
    }

    pub(crate) fn sequence_for(path: &str, part: u64) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(path.as_bytes());
        // Keep the initial stream choice compatible with the pre-Trait
        // scheduler (which hashed the path alone), while salting subsequent
        // chunks/resume offsets so equal chunk indexes from different Blobs do
        // not repeatedly hit the same node.
        if part != 0 {
            hasher.update([0]);
            hasher.update(part.to_be_bytes());
        }
        let digest = hasher.finalize();
        u64::from_be_bytes(digest[..8].try_into().expect("sha256 prefix"))
    }

    pub(crate) async fn ordered_parallel_nodes(
        &self,
        nodes: &[NodeView],
        context: crate::scheduling::ScheduleContext,
    ) -> Vec<NodeView> {
        let policy = self.runtime.read().await.scheduler_policy;
        self.ordered_nodes_with_context(nodes, context, policy)
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
        let chunk_size = effective_chunk_size(runtime, total_size, node_capacity);
        StreamDownloadConfig {
            chunk_size,
            concurrency: bounded_stream_concurrency(
                chunk_size,
                effective_concurrency(runtime, node_capacity),
            ),
            parallel_threshold: runtime.parallel_threshold,
        }
    }

    pub fn stats(&self) -> SchedulerStats {
        SchedulerStats {
            parallel_blobs: self.stats.parallel_blobs.load(Ordering::Relaxed),
            resume_attempts: self.stats.resume_attempts.load(Ordering::Relaxed),
            retry_attempts: self.stats.retry_attempts.load(Ordering::Relaxed),
            last_chunk_size: self.stats.last_chunk_size.load(Ordering::Relaxed),
            cooling_nodes: self.selector.cooling_count(),
        }
    }

    pub(crate) fn transfer_metrics(&self) -> crate::transfer_metrics::TransferMetrics {
        self.cache.metrics()
    }

    pub(crate) fn stream_memory_budget(&self) -> StreamMemoryBudget {
        StreamMemoryBudget::new(self.stream_memory.clone())
    }

    pub(crate) fn record_parallel_blob(&self, chunk_size: u64) {
        self.stats.parallel_blobs.fetch_add(1, Ordering::Relaxed);
        self.stats
            .last_chunk_size
            .store(chunk_size, Ordering::Relaxed);
    }

    pub(crate) fn record_resume(&self) {
        self.stats.resume_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry(&self) {
        self.stats.retry_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) async fn fetch_blob(
        &self,
        request_path: &str,
        request_headers: &HeaderMap,
        expected_digest: Option<&str>,
        nodes: Vec<NodeView>,
    ) -> ApiResult<BlobFetchResult> {
        if nodes.is_empty() {
            return Err(AppError::unavailable(
                "resolved Registry route has no enabled nodes",
            ));
        }
        let authorization = request_headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok());
        let public_route = nodes
            .iter()
            .all(|node| node.node.auth_mode == "none" && node.node.auth_secret_enc.is_none());
        let key = self
            .cache
            .request_key(
                &nodes[0].route.id.to_string(),
                request_path,
                authorization,
                public_route,
            )
            .await;
        if let Some(object) = self.cache.get_for_blob_request(&key).await? {
            return Ok(BlobFetchResult {
                object,
                cache_hit: true,
            });
        }

        let guard = self.cache.lock(&key).await?;
        if let Some(object) = self.cache.get(&key).await? {
            drop(guard);
            return Ok(BlobFetchResult {
                object,
                cache_hit: true,
            });
        }

        let result = self
            .fetch_uncached(&key, request_path, request_headers, expected_digest, &nodes)
            .await;
        drop(guard);
        result.map(|object| BlobFetchResult {
            object,
            cache_hit: false,
        })
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
        let detected = self
            .detect_capabilities(nodes, request_path, request_headers)
            .await?;
        let size = detected.size.unwrap_or_default();
        let node_capacity: usize = detected
            .range_nodes
            .iter()
            .map(|node| usize::from(node.max_concurrency))
            .sum();
        let range_ids = detected
            .range_nodes
            .iter()
            .map(|node| node.node.id)
            .collect::<std::collections::HashSet<_>>();
        let snapshots = detected
            .nodes
            .iter()
            .map(|node| NodeSnapshot {
                url: node.node.url.clone(),
                supports_range: range_ids.contains(&node.node.id),
                max_concurrency: usize::from(node.max_concurrency),
                throughput_bps: (node.metric.speed_bps > 0).then_some(node.metric.speed_bps as u64),
                latency_ms: (node.metric.last_checked_at.is_some() && node.metric.latency_ms > 0)
                    .then_some(node.metric.latency_ms as u64),
                success_rate: node.metric.success_rate,
                cooling: self.selector.is_cooling_node(node.node.id),
            })
            .collect::<Vec<_>>();
        let planner = BlobPlanner::new(PlannerConfig {
            small_blob_threshold: runtime.parallel_threshold,
            max_concurrent_chunks: effective_concurrency(runtime, node_capacity.max(1)),
            allow_unmeasured_parallel: runtime.parallel_threshold < 1024 * 1024,
            min_chunk_size: if runtime.adaptive_chunking_enabled {
                PlannerConfig::default().min_chunk_size
            } else {
                runtime.chunk_size
            },
            max_chunk_size: if runtime.adaptive_chunking_enabled {
                PlannerConfig::default().max_chunk_size
            } else {
                runtime.chunk_size
            },
            ..PlannerConfig::default()
        });
        let plan = if size == 0 {
            BlobDownloadPlan {
                strategy: DownloadStrategy::SingleRequest {
                    node_url: detected.nodes[0].node.url.clone(),
                },
            }
        } else {
            match planner.plan(
                &BlobMeta {
                    digest: expected_digest.unwrap_or_default().to_owned(),
                    size,
                    media_type: detected.media_type.clone(),
                },
                false,
                &snapshots,
            ) {
                Ok(plan) => plan,
                Err(PlannerError::NoUsableNodes) => BlobDownloadPlan {
                    strategy: DownloadStrategy::SingleRequest {
                        node_url: detected
                            .nodes
                            .first()
                            .map(|node| node.node.url.clone())
                            .ok_or_else(|| AppError::unavailable("no compatible nodes"))?,
                    },
                },
                Err(PlannerError::InvalidConfiguration) => {
                    return Err(AppError::internal(anyhow::anyhow!(
                        "invalid Blob planner configuration"
                    )));
                }
            }
        };
        let (can_parallel, planned_chunk_size, preferred_single) = match plan {
            BlobDownloadPlan {
                strategy: DownloadStrategy::MultiSourceChunked { chunk_size, .. },
            } => (true, chunk_size, None),
            BlobDownloadPlan {
                strategy: DownloadStrategy::CacheHit,
            } => (false, runtime.chunk_size, None),
            BlobDownloadPlan {
                strategy: DownloadStrategy::SingleRequest { node_url },
            } => (false, runtime.chunk_size, Some(node_url)),
        };
        runtime.chunk_size = planned_chunk_size;
        let fallback_nodes = preferred_single
            .as_deref()
            .map(|preferred| {
                let mut nodes = detected.nodes.clone();
                if let Some(index) = nodes.iter().position(|node| node.node.url == preferred) {
                    nodes.rotate_left(index);
                }
                nodes
            })
            .unwrap_or_else(|| detected.nodes.clone());

        let partial_dir = self.cache.temp_dir().join(key);
        tokio::fs::create_dir_all(&partial_dir).await?;
        let merged = partial_dir.join("object.partial");
        if can_parallel {
            self.record_parallel_blob(runtime.chunk_size);
            tracing::info!(
                path = request_path,
                size,
                chunks = chunks(size, runtime.chunk_size).len(),
                nodes = detected.range_nodes.len(),
                "starting parallel Blob fetch"
            );
            if let Err(error) = self
                .download_parallel(ParallelDownloadRequest {
                    nodes: &detected.range_nodes,
                    request_path,
                    request_headers,
                    total_size: size,
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
                self.download_whole(
                    &fallback_nodes,
                    &detected.pending_nodes,
                    request_path,
                    request_headers,
                    &merged,
                    detected.size,
                )
                .await?;
            }
        } else if !detected.range_nodes.is_empty()
            && size >= runtime.resumable_threshold
            && tokio::fs::metadata(&merged)
                .await
                .is_ok_and(|metadata| metadata.len() > 0 && metadata.len() < size)
        {
            self.download_resume(
                &detected.range_nodes,
                &detected.pending_nodes,
                request_path,
                request_headers,
                size,
                &merged,
            )
            .await?;
        } else {
            self.download_whole(
                &fallback_nodes,
                &detected.pending_nodes,
                request_path,
                request_headers,
                &merged,
                detected.size,
            )
            .await?;
        }

        let actual_size = tokio::fs::metadata(&merged).await?.len();
        if detected
            .size
            .is_some_and(|expected| actual_size != expected)
        {
            if let Some(digest) = expected_digest
                && detected.fallback_nodes.len() > 1
            {
                self.recover_verified_blob(
                    &detected.fallback_nodes,
                    request_path,
                    request_headers,
                    &merged,
                    digest,
                    detected.size,
                )
                .await?;
            } else {
                return Err(AppError::Integrity);
            }
        }
        if let Some(digest) = expected_digest
            && let Err(error) = verify_file(&merged, digest).await
        {
            if detected.fallback_nodes.len() < 2 {
                return Err(error);
            }
            tracing::warn!(
                path = request_path,
                "Blob Digest mismatch after transfer; retrying from alternate nodes"
            );
            self.recover_verified_blob(
                &detected.fallback_nodes,
                request_path,
                request_headers,
                &merged,
                digest,
                detected.size,
            )
            .await?;
        }

        let result = self
            .cache
            .admit(
                key,
                &merged,
                &detected.media_type,
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
        pending_nodes: &[NodeView],
        request_path: &str,
        request_headers: &HeaderMap,
        total_size: u64,
        destination: &Path,
    ) -> ApiResult<()> {
        self.record_resume();
        let mut last_error = None;
        let policy = self.runtime.read().await.scheduler_policy;
        for group in [nodes, pending_nodes] {
            if group.is_empty() {
                continue;
            }
            loop {
                let capacity_changed = self.capacity_changed();
                let mut attempted = false;
                let offset = match tokio::fs::metadata(destination).await {
                    Ok(metadata) => metadata.len(),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                    Err(error) => return Err(error.into()),
                };
                if offset >= total_size {
                    return Ok(());
                }
                let candidates = self.ordered_nodes_with_context(
                    group,
                    crate::scheduling::ScheduleContext {
                        sequence: Self::sequence_for(request_path, offset),
                        blob_size: total_size,
                        range_required: true,
                        chunk: Some((offset, total_size - 1)),
                    },
                    policy,
                );
                for node in candidates {
                    let Some(lease) = self.try_acquire(node.node.id, node.max_concurrency) else {
                        continue;
                    };
                    attempted = true;
                    // A previous source may have appended bytes before failing,
                    // or an integrity error may have discarded the partial file.
                    let offset = match tokio::fs::metadata(destination).await {
                        Ok(metadata) => metadata.len(),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                        Err(error) => return Err(error.into()),
                    };
                    if offset >= total_size {
                        return if offset == total_size {
                            Ok(())
                        } else {
                            Err(AppError::Integrity)
                        };
                    }
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
                        if offset == 0 {
                            stream_response_to_file(
                                response,
                                destination,
                                Some(total_size),
                                Some((&self.nodes, node.node.id)),
                                Some(self.cache.metrics()),
                            )
                            .await
                        } else {
                            append_response_to_file(
                                response,
                                destination,
                                total_size - offset,
                                Some((&self.nodes, node.node.id)),
                                Some(self.cache.metrics()),
                            )
                            .await
                        }
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
                            self.record_retry();
                            if matches!(&error, AppError::Integrity) {
                                let _ = tokio::fs::remove_file(destination).await;
                            }
                            last_error = Some(error)
                        }
                    }
                }
                if attempted {
                    break;
                }
                capacity_changed.await;
            }
        }
        Err(last_error.unwrap_or_else(|| AppError::Upstream("all nodes failed resume".into())))
    }

    async fn detect_node_capabilities(
        &self,
        node: &NodeView,
        request_path: &str,
        request_headers: &HeaderMap,
    ) -> ApiResult<BlobCapabilities> {
        let capability_key = format!(
            "{}:{}:{}",
            node.node.id,
            node.node.updated_at.timestamp_millis(),
            request_path
        );
        let started = Instant::now();
        if let Some(value) = self.capabilities.get(&capability_key).await {
            return Ok(value);
        }
        let result = async {
            let _lease = self.nodes.acquire_probe(node).await?;
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
        if let Ok(value) = &result {
            self.capabilities
                .insert(capability_key, value.clone())
                .await;
        }
        self.nodes
            .record_transfer(node.node.id, 0, started.elapsed(), result.is_ok())
            .await;
        result
    }

    async fn detect_capabilities(
        &self,
        nodes: &[NodeView],
        request_path: &str,
        request_headers: &HeaderMap,
    ) -> ApiResult<DetectedCapabilities> {
        let mut last_error = None;
        let mut baseline: Option<BlobCapabilities> = None;
        let mut compatible = Vec::new();
        let mut observed = std::collections::HashSet::new();
        let parallel_threshold = self.runtime.read().await.parallel_threshold;
        let mut discovery_deadline = None;
        let mut probes = stream::iter(nodes.to_vec())
            .map(|node| {
                let scheduler = self.clone();
                let path = request_path.to_owned();
                let headers = request_headers.clone();
                async move {
                    let result = scheduler
                        .detect_node_capabilities(&node, &path, &headers)
                        .await;
                    (node, result)
                }
            })
            .buffer_unordered(8);
        loop {
            let next = if let Some(deadline) = discovery_deadline {
                match tokio::time::timeout_at(deadline, probes.next()).await {
                    Ok(next) => next,
                    Err(_) => break,
                }
            } else {
                probes.next().await
            };
            let Some((node, result)) = next else {
                break;
            };
            observed.insert(node.node.id);
            match result {
                Ok(value) => {
                    let compatible_with_baseline = baseline
                        .as_ref()
                        .is_none_or(|previous| previous.size == value.size);
                    if compatible_with_baseline {
                        if baseline.is_none() {
                            baseline = Some(value.clone());
                        }
                        compatible.push((node.clone(), value));
                        let Some(size) = baseline.as_ref().and_then(|value| value.size) else {
                            break;
                        };
                        if size < parallel_threshold {
                            break;
                        }
                        discovery_deadline.get_or_insert_with(|| {
                            tokio::time::Instant::now() + std::time::Duration::from_millis(100)
                        });
                    }
                }
                Err(error) => {
                    self.record_retry();
                    last_error = Some(error)
                }
            }
        }
        if let Some(baseline) = baseline {
            let range_nodes = compatible
                .iter()
                .filter(|(_, capabilities)| capabilities.supports_range)
                .map(|(node, _)| node.clone())
                .collect::<Vec<_>>();
            let compatible_nodes = compatible
                .into_iter()
                .map(|(node, _)| node)
                .collect::<Vec<_>>();
            let pending = nodes
                .iter()
                .filter(|node| !observed.contains(&node.node.id));
            let pending_nodes = pending.clone().cloned().collect();
            let fallback_nodes = compatible_nodes
                .iter()
                .cloned()
                .chain(pending.cloned())
                .collect();
            return Ok(DetectedCapabilities {
                size: baseline.size,
                media_type: baseline.media_type,
                nodes: compatible_nodes.clone(),
                range_nodes,
                pending_nodes,
                fallback_nodes,
            });
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
        let concurrency = effective_concurrency(request.runtime, node_capacity)
            .min(chunks.len())
            .max(1);
        let results = stream::iter(chunks.clone())
            .map(|chunk| {
                let scheduler = self.clone();
                let nodes = request.nodes.to_vec();
                let request_path = request.request_path.to_owned();
                let request_headers = request.request_headers.clone();
                // A retry may choose a different chunk size. Persist the byte
                // range identity so an old layout cannot supply a wrong prefix.
                let part_path = request.temp_dir.join(format!(
                    "{}-{}-{}.part",
                    chunk.total_size, chunk.start, chunk.end
                ));
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
        loop {
            let capacity_changed = self.capacity_changed();
            let mut attempted = false;
            let candidates = self.ordered_nodes_with_context(
                nodes,
                crate::scheduling::ScheduleContext {
                    sequence: Self::sequence_for(request_path, chunk.index as u64),
                    blob_size: chunk.total_size,
                    range_required: true,
                    chunk: Some((chunk.start, chunk.end)),
                },
                policy,
            );
            for node in candidates {
                let Some(lease) = self.try_acquire(node.node.id, node.max_concurrency) else {
                    continue;
                };
                attempted = true;
                let started = Instant::now();
                let existing = tokio::fs::metadata(destination)
                    .await
                    .map(|metadata| metadata.len())
                    .unwrap_or(0);
                let result = async {
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
                    if !content_range_matches(
                        response.headers(),
                        start,
                        chunk.end,
                        chunk.total_size,
                    ) {
                        return Err(AppError::Integrity);
                    }
                    if existing > 0 {
                        append_response_to_file(
                            response,
                            destination,
                            expected,
                            Some((&self.nodes, node.node.id)),
                            Some(self.cache.metrics()),
                        )
                        .await
                    } else {
                        stream_response_to_file(
                            response,
                            destination,
                            Some(expected),
                            Some((&self.nodes, node.node.id)),
                            Some(self.cache.metrics()),
                        )
                        .await
                    }
                }
                .await;
                let bytes = tokio::fs::metadata(destination)
                    .await
                    .map(|metadata| metadata.len().saturating_sub(existing))
                    .unwrap_or(0);
                self.observe_node(node.node.id, bytes, started.elapsed(), result.is_ok());
                drop(lease);
                self.nodes
                    .record_transfer(node.node.id, bytes, started.elapsed(), result.is_ok())
                    .await;
                match result {
                    Ok(()) => return Ok(()),
                    Err(error) => {
                        self.record_retry();
                        if matches!(&error, AppError::Integrity) {
                            let _ = tokio::fs::remove_file(destination).await;
                        }
                        last_error = Some(error)
                    }
                }
            }
            if attempted {
                break;
            }
            capacity_changed.await;
        }
        Err(last_error.unwrap_or_else(|| AppError::Upstream("all nodes failed a chunk".into())))
    }

    async fn download_whole(
        &self,
        nodes: &[NodeView],
        pending_nodes: &[NodeView],
        request_path: &str,
        request_headers: &HeaderMap,
        destination: &Path,
        total_size: Option<u64>,
    ) -> ApiResult<()> {
        let mut last_error = None;
        let policy = self.runtime.read().await.scheduler_policy;
        for group in [nodes, pending_nodes] {
            if group.is_empty() {
                continue;
            }
            loop {
                let capacity_changed = self.capacity_changed();
                let mut attempted = false;
                let candidates = self.ordered_nodes_with_context(
                    group,
                    crate::scheduling::ScheduleContext {
                        sequence: Self::sequence_for(request_path, total_size.unwrap_or_default()),
                        blob_size: total_size.unwrap_or_default(),
                        range_required: false,
                        chunk: None,
                    },
                    policy,
                );
                for node in candidates {
                    let Some(lease) = self.try_acquire(node.node.id, node.max_concurrency) else {
                        continue;
                    };
                    attempted = true;
                    let started = Instant::now();
                    let existing = if let Some(total) = total_size.filter(|total| *total > 0) {
                        tokio::fs::metadata(destination)
                            .await
                            .ok()
                            .and_then(|metadata| (metadata.len() < total).then_some(metadata.len()))
                    } else {
                        None
                    };
                    let result = async {
                        let range = existing.and_then(|offset| {
                            total_size
                                .and_then(|total| total.checked_sub(1).map(|end| (offset, end)))
                        });
                        let response = self
                            .upstream
                            .send(
                                node,
                                http::Method::GET,
                                request_path,
                                request_headers,
                                range.map_or(RangeMode::Suppress, |(start, end)| {
                                    RangeMode::Exact(start, end)
                                }),
                            )
                            .await?;
                        if !response.status().is_success() {
                            return Err(AppError::Upstream(format!(
                                "{} returned {}",
                                node.node.name,
                                response.status()
                            )));
                        }
                        if let Some(start) = existing {
                            let Some(total) = total_size else {
                                return Err(AppError::Integrity);
                            };
                            let Some(end) = total.checked_sub(1) else {
                                return Err(AppError::Integrity);
                            };
                            if response.status() != StatusCode::PARTIAL_CONTENT
                                || !content_range_matches(response.headers(), start, end, total)
                            {
                                return Err(AppError::Integrity);
                            }
                            append_response_to_file(
                                response,
                                destination,
                                end - start + 1,
                                Some((&self.nodes, node.node.id)),
                                Some(self.cache.metrics()),
                            )
                            .await
                        } else {
                            let length = response.content_length();
                            stream_response_to_file(
                                response,
                                destination,
                                length,
                                Some((&self.nodes, node.node.id)),
                                Some(self.cache.metrics()),
                            )
                            .await
                        }
                    }
                    .await;
                    let bytes = tokio::fs::metadata(destination)
                        .await
                        .map(|m| m.len().saturating_sub(existing.unwrap_or(0)))
                        .unwrap_or(0);
                    self.observe_node(node.node.id, bytes, started.elapsed(), result.is_ok());
                    drop(lease);
                    self.nodes
                        .record_transfer(node.node.id, bytes, started.elapsed(), result.is_ok())
                        .await;
                    match result {
                        Ok(()) => return Ok(()),
                        Err(error) => {
                            self.record_retry();
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
                if attempted {
                    break;
                }
                capacity_changed.await;
            }
        }
        Err(last_error.unwrap_or_else(|| AppError::Upstream("all nodes failed".into())))
    }

    async fn recover_verified_blob(
        &self,
        nodes: &[NodeView],
        request_path: &str,
        request_headers: &HeaderMap,
        destination: &Path,
        expected_digest: &str,
        total_size: Option<u64>,
    ) -> ApiResult<()> {
        let policy = self.runtime.read().await.scheduler_policy;
        let mut last_error = None;
        let candidates = self.ordered_nodes_with_context(
            nodes,
            crate::scheduling::ScheduleContext {
                sequence: Self::sequence_for(request_path, total_size.unwrap_or_default()),
                blob_size: total_size.unwrap_or_default(),
                range_required: false,
                chunk: None,
            },
            policy,
        );
        for node in candidates {
            let started = Instant::now();
            let result = match self
                .download_whole(
                    std::slice::from_ref(node),
                    &[],
                    request_path,
                    request_headers,
                    destination,
                    total_size,
                )
                .await
            {
                Ok(()) => verify_file(destination, expected_digest).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => return Ok(()),
                Err(error) => {
                    self.observe_node(node.node.id, 0, started.elapsed(), false);
                    self.nodes
                        .record_transfer(node.node.id, 0, started.elapsed(), false)
                        .await;
                    let _ = tokio::fs::remove_file(destination).await;
                    self.record_retry();
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or(AppError::Integrity))
    }

    fn ordered_nodes_with_context<'a>(
        &self,
        nodes: &'a [NodeView],
        context: crate::scheduling::ScheduleContext,
        policy: SchedulerPolicy,
    ) -> Vec<&'a NodeView> {
        self.selector.order_with_context(nodes, context, policy)
    }

    pub(crate) fn try_acquire(&self, node_id: Uuid, max_concurrency: u16) -> Option<NodeLease> {
        self.selector.try_acquire(node_id, max_concurrency)
    }

    pub(crate) fn capacity_changed(&self) -> impl Future<Output = ()> + Send + use<> {
        self.selector.capacity_changed()
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
    let parallelism = effective_concurrency(runtime, node_capacity) as u64;
    let target_chunks = parallelism.saturating_mul(TARGET_WAVES);
    let ideal = total_size.saturating_add(target_chunks.saturating_sub(1)) / target_chunks;
    ideal
        .saturating_add(MIB - 1)
        .div_euclid(MIB)
        .saturating_mul(MIB)
        .clamp(MIN_CHUNK, MAX_CHUNK)
}

fn bounded_stream_concurrency(chunk_size: u64, desired: usize) -> usize {
    let memory_slots = u64::from(STREAM_MEMORY_BYTES)
        .checked_div(chunk_size.max(1))
        .unwrap_or(1)
        .max(1)
        .min(usize::MAX as u64) as usize;
    desired.min(memory_slots).max(1)
}

fn effective_concurrency(runtime: SchedulerRuntimeConfig, node_capacity: usize) -> usize {
    let node_capacity = node_capacity.max(1);
    if runtime.automatic_concurrency_enabled {
        node_capacity.min(64)
    } else {
        runtime.chunk_concurrency.min(node_capacity).max(1)
    }
}

fn capabilities_from_head(response: &reqwest::Response) -> Option<BlobCapabilities> {
    Some(BlobCapabilities {
        size: Some(positive_content_length(response.headers())?),
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
            size: Some(range.complete_length),
            supports_range: identity_encoded(response),
            media_type: response_media_type(response),
        });
    }
    if response.status().is_success() {
        return Some(BlobCapabilities {
            size: response.content_length(),
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
    live: Option<(&NodeService, Uuid)>,
    metrics: Option<crate::transfer_metrics::TransferMetrics>,
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
        if let Some((nodes, node_id)) = live {
            nodes.record_live_bytes(node_id, chunk.len() as u64);
        }
        if let Some(metrics) = &metrics {
            metrics.record_upstream_bytes(chunk.len() as u64);
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
    live: Option<(&NodeService, Uuid)>,
    metrics: Option<crate::transfer_metrics::TransferMetrics>,
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
        if let Some((nodes, node_id)) = live {
            nodes.record_live_bytes(node_id, chunk.len() as u64);
        }
        if let Some(metrics) = &metrics {
            metrics.record_upstream_bytes(chunk.len() as u64);
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

    #[tokio::test]
    async fn unknown_length_success_remains_a_usable_single_source() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let fixture = tokio::spawn(async move {
            let router = axum::Router::new().fallback(|| async {
                axum::body::Body::from_stream(stream::iter([Ok::<_, std::io::Error>(
                    bytes::Bytes::from_static(b"unknown length"),
                )]))
            });
            axum::serve(listener, router).await.unwrap();
        });
        let response = reqwest::get(format!("http://{address}/")).await.unwrap();
        assert!(response.content_length().is_none());
        let capabilities = capabilities_from_probe(&response)
            .expect("unknown length must disable ranges, not discard a successful source");
        assert!(!capabilities.supports_range);
        fixture.abort();
    }

    #[tokio::test]
    async fn small_blob_discovery_does_not_wait_for_an_unrelated_slow_node() {
        let fast = httpmock::MockServer::start_async().await;
        let slow = httpmock::MockServer::start_async().await;
        for (server, delay) in [(&fast, 0), (&slow, 2_000)] {
            server
                .mock_async(|when, then| {
                    when.method(httpmock::Method::HEAD)
                        .path("/v2/audit/blobs/data");
                    then.status(200)
                        .header("content-length", "1024")
                        .delay(std::time::Duration::from_millis(delay));
                })
                .await;
            server
                .mock_async(|when, then| {
                    when.method(httpmock::Method::GET)
                        .path("/v2/audit/blobs/data");
                    then.status(200)
                        .body(vec![b'f'; 1024])
                        .delay(std::time::Duration::from_millis(delay));
                })
                .await;
        }
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.scheduler_policy = SchedulerPolicy::SpeedFirst;
        let state = crate::AppState::new(config).await.unwrap();
        let mut nodes = Vec::new();
        for (priority, server) in [&fast, &slow].into_iter().enumerate() {
            nodes.push(
                state
                    .nodes
                    .create(crate::nodes::NodeInput {
                        name: format!("discovery-{priority}"),
                        url: server.base_url(),
                        registry_route_id: crate::registry_routes::DOCKER_HUB_ROUTE_ID,
                        enabled: true,
                        priority: 1 - priority as i32,
                        max_concurrency: 1,
                        cf_preferred: false,
                        connect_ip_type: "ip".into(),
                        connect_ip: None,
                        auth_mode: "none".into(),
                        auth_username: None,
                        auth_header: None,
                        auth_secret: None,
                    })
                    .await
                    .unwrap(),
            );
        }
        let headers = HeaderMap::new();
        let detected = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            state
                .scheduler
                .detect_capabilities(&nodes, "/v2/audit/blobs/data", &headers),
        )
        .await
        .expect("a ready source must not wait for unrelated probe completion")
        .unwrap();
        assert!(
            detected
                .nodes
                .iter()
                .any(|node| node.node.id == nodes[0].node.id)
        );
        assert_eq!(state.nodes.capacity.active(nodes[1].node.id), 0);
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            state
                .scheduler
                .fetch_blob("/v2/audit/blobs/data", &headers, None, nodes),
        )
        .await
        .expect("unprobed nodes must not displace an available confirmed source")
        .unwrap();
    }

    #[tokio::test]
    async fn changing_chunk_layout_does_not_reuse_bytes_from_another_offset() {
        let server = httpmock::MockServer::start_async().await;
        let bytes = b"abcdefghijklmnop";
        for chunk_size in [4, 8] {
            for chunk in chunks(bytes.len() as u64, chunk_size) {
                server
                    .mock_async(|when, then| {
                        when.method(httpmock::Method::GET)
                            .path("/v2/test/blobs/data")
                            .header("range", format!("bytes={}-{}", chunk.start, chunk.end));
                        then.status(206)
                            .header(
                                "content-range",
                                format!("bytes {}-{}/16", chunk.start, chunk.end),
                            )
                            .body(&bytes[chunk.start as usize..=chunk.end as usize]);
                    })
                    .await;
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let state = crate::AppState::new(Config::for_test(directory.path().to_owned()))
            .await
            .unwrap();
        let node = state
            .nodes
            .create(crate::nodes::NodeInput {
                name: "layout".into(),
                url: server.base_url(),
                registry_route_id: crate::registry_routes::DOCKER_HUB_ROUTE_ID,
                enabled: true,
                priority: 0,
                max_concurrency: 4,
                cf_preferred: false,
                connect_ip_type: "ip".into(),
                connect_ip: None,
                auth_mode: "none".into(),
                auth_username: None,
                auth_header: None,
                auth_secret: None,
            })
            .await
            .unwrap();
        let nodes = [node];
        let headers = HeaderMap::new();
        let merged = directory.path().join("merged");
        for chunk_size in [4, 8] {
            let mut runtime = test_runtime(false);
            runtime.chunk_size = chunk_size;
            state
                .scheduler
                .download_parallel(ParallelDownloadRequest {
                    nodes: &nodes,
                    request_path: "/v2/test/blobs/data",
                    request_headers: &headers,
                    total_size: bytes.len() as u64,
                    temp_dir: directory.path(),
                    merged: &merged,
                    runtime,
                })
                .await
                .unwrap();
            assert_eq!(tokio::fs::read(&merged).await.unwrap(), bytes);
        }
    }

    fn test_runtime(adaptive: bool) -> SchedulerRuntimeConfig {
        SchedulerRuntimeConfig {
            chunk_size: 512 * 1024,
            adaptive_chunking_enabled: adaptive,
            automatic_concurrency_enabled: false,
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
    fn sequence_salts_chunks_but_keeps_initial_stream_hash_stable() {
        assert_eq!(
            Scheduler::sequence_for("/v2/library/redis/blobs/sha256:x", 0),
            {
                let digest = Sha256::digest("/v2/library/redis/blobs/sha256:x".as_bytes());
                u64::from_be_bytes(digest[..8].try_into().unwrap())
            }
        );
        assert_ne!(
            Scheduler::sequence_for("/v2/library/redis/blobs/sha256:x", 1),
            Scheduler::sequence_for("/v2/library/redis/blobs/sha256:x", 2)
        );
    }

    #[test]
    fn automatic_concurrency_sums_node_capacity_with_safety_cap() {
        let mut runtime = test_runtime(true);
        runtime.automatic_concurrency_enabled = true;
        runtime.chunk_concurrency = 2;
        assert_eq!(effective_concurrency(runtime, 24), 24);
        assert_eq!(effective_concurrency(runtime, 80), 64);
    }

    #[test]
    fn manual_concurrency_uses_configured_limit() {
        let mut runtime = test_runtime(true);
        runtime.chunk_concurrency = 12;
        assert_eq!(effective_concurrency(runtime, 24), 12);
        assert_eq!(effective_concurrency(runtime, 8), 8);
    }

    #[test]
    fn streaming_concurrency_respects_memory_budget() {
        assert_eq!(bounded_stream_concurrency(8 * 1024 * 1024, 64), 8);
        assert_eq!(bounded_stream_concurrency(2 * 1024 * 1024, 64), 32);
        assert_eq!(bounded_stream_concurrency(128 * 1024 * 1024, 64), 1);
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
