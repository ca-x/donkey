//! Pure Blob download planning.
//!
//! The planner deliberately does not perform I/O or reserve a node.  It turns
//! immutable Blob metadata and the latest node observations into a conservative
//! strategy.  The Scheduler still owns cache leases, authentication, retries,
//! Digest verification, and the final node choice for each request.

use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobMeta {
    pub digest: String,
    pub size: u64,
    pub media_type: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NodeSnapshot {
    pub url: String,
    pub supports_range: bool,
    pub max_concurrency: usize,
    pub throughput_bps: Option<u64>,
    pub latency_ms: Option<u64>,
    pub success_rate: f64,
    pub cooling: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DownloadStrategy {
    CacheHit,
    SingleRequest {
        node_url: String,
    },
    MultiSourceChunked {
        chunk_size: u64,
        num_chunks: usize,
        recommended_concurrency: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobDownloadPlan {
    pub strategy: DownloadStrategy,
}

#[derive(Clone, Copy, Debug)]
pub struct PlannerConfig {
    /// Blobs below this size stay on a single request.  This is a bandwidth
    /// budget, not a promise that a smaller Blob cannot be parallelized.
    pub small_blob_threshold: u64,
    pub min_chunk_size: u64,
    pub max_chunk_size: u64,
    pub min_parallel_benefit: f64,
    pub max_concurrent_chunks: usize,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        const MIB: u64 = 1024 * 1024;
        Self {
            small_blob_threshold: 8 * MIB,
            min_chunk_size: 2 * MIB,
            max_chunk_size: 8 * MIB,
            min_parallel_benefit: 1.2,
            max_concurrent_chunks: 16,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerError {
    NoUsableNodes,
    InvalidConfiguration,
}

impl fmt::Display for PlannerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NoUsableNodes => "no usable nodes available for Blob planning",
            Self::InvalidConfiguration => "invalid Blob planner configuration",
        })
    }
}

impl std::error::Error for PlannerError {}

#[derive(Clone, Copy, Debug)]
pub struct BlobPlanner {
    config: PlannerConfig,
}

impl BlobPlanner {
    pub const fn new(config: PlannerConfig) -> Self {
        Self { config }
    }

    pub fn plan(
        &self,
        blob: &BlobMeta,
        cached: bool,
        nodes: &[NodeSnapshot],
    ) -> Result<BlobDownloadPlan, PlannerError> {
        if cached {
            return Ok(BlobDownloadPlan {
                strategy: DownloadStrategy::CacheHit,
            });
        }
        if blob.size == 0
            || self.config.min_chunk_size == 0
            || self.config.max_chunk_size < self.config.min_chunk_size
            || self.config.max_concurrent_chunks == 0
            || self.config.min_parallel_benefit < 1.0
        {
            return Err(PlannerError::InvalidConfiguration);
        }

        let usable = nodes
            .iter()
            .filter(|node| {
                !node.cooling
                    && node.max_concurrency > 0
                    && node.success_rate > 0.0
                    && !node.url.is_empty()
            })
            .collect::<Vec<_>>();
        let Some(best_single) = usable.iter().min_by(|left, right| {
            estimated_time_ms(blob.size, left).total_cmp(&estimated_time_ms(blob.size, right))
        }) else {
            return Err(PlannerError::NoUsableNodes);
        };

        let range_nodes = usable
            .iter()
            .copied()
            .filter(|node| node.supports_range)
            .collect::<Vec<_>>();
        let requested_concurrency = range_nodes
            .iter()
            .map(|node| node.max_concurrency)
            .sum::<usize>()
            .min(self.config.max_concurrent_chunks);
        if blob.size < self.config.small_blob_threshold
            || range_nodes.is_empty()
            || requested_concurrency < 2
        {
            return Ok(single_plan(best_single));
        }

        let chunk_size = self.chunk_size(blob.size, requested_concurrency);
        let num_chunks = blob.size.div_ceil(chunk_size) as usize;
        let benefit = estimated_benefit(blob.size, best_single, &range_nodes, chunk_size);
        // Before throughput is observed, capability-based parallelism is the
        // only evidence available.  Once every selected node has a sample,
        // require a measured margin so parallelism does not add needless
        // requests on equally fast sources.
        if benefit.is_some_and(|value| value < self.config.min_parallel_benefit) {
            return Ok(single_plan(best_single));
        }

        Ok(BlobDownloadPlan {
            strategy: DownloadStrategy::MultiSourceChunked {
                chunk_size,
                num_chunks,
                recommended_concurrency: requested_concurrency,
            },
        })
    }

    fn chunk_size(&self, blob_size: u64, concurrency: usize) -> u64 {
        const MIB: u64 = 1024 * 1024;
        let target_chunks = (concurrency as u64).saturating_mul(4).max(1);
        let ideal = blob_size.div_ceil(target_chunks);
        ideal
            .div_ceil(MIB)
            .saturating_mul(MIB)
            .clamp(self.config.min_chunk_size, self.config.max_chunk_size)
    }
}

fn single_plan(node: &NodeSnapshot) -> BlobDownloadPlan {
    BlobDownloadPlan {
        strategy: DownloadStrategy::SingleRequest {
            node_url: node.url.clone(),
        },
    }
}

fn estimated_time_ms(size: u64, node: &NodeSnapshot) -> f64 {
    let throughput = node.throughput_bps.unwrap_or(0).max(1) as f64;
    let latency = node.latency_ms.unwrap_or(0) as f64;
    latency + size as f64 * 1_000.0 / throughput
}

fn estimated_benefit(
    size: u64,
    best_single: &NodeSnapshot,
    nodes: &[&NodeSnapshot],
    _chunk_size: u64,
) -> Option<f64> {
    if nodes
        .iter()
        .any(|node| node.throughput_bps.is_none() || node.latency_ms.is_none())
    {
        return None;
    }
    let single = estimated_time_ms(size, best_single);
    let aggregate_throughput = nodes
        .iter()
        .filter_map(|node| node.throughput_bps)
        .map(|throughput| throughput as f64)
        .sum::<f64>();
    if aggregate_throughput <= 0.0 {
        return None;
    }
    let coordination_latency = nodes
        .iter()
        .filter_map(|node| node.latency_ms)
        .max()
        .unwrap_or_default() as f64;
    let multi = coordination_latency + size as f64 * 1_000.0 / aggregate_throughput;
    Some(single / multi.max(1.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(url: &str, speed: Option<u64>, range: bool) -> NodeSnapshot {
        NodeSnapshot {
            url: url.into(),
            supports_range: range,
            max_concurrency: 2,
            throughput_bps: speed,
            latency_ms: speed.map(|_| 20),
            success_rate: 1.0,
            cooling: false,
        }
    }

    #[test]
    fn cache_hit_never_selects_a_node() {
        let blob = BlobMeta {
            digest: "sha256:abc".into(),
            size: 100,
            media_type: "application/octet-stream".into(),
        };
        let plan = BlobPlanner::new(PlannerConfig::default())
            .plan(&blob, true, &[])
            .unwrap();
        assert_eq!(plan.strategy, DownloadStrategy::CacheHit);
    }

    #[test]
    fn small_blob_uses_one_request() {
        let blob = BlobMeta {
            digest: "sha256:abc".into(),
            size: 4 * 1024 * 1024,
            media_type: "application/octet-stream".into(),
        };
        let nodes = [node("a", None, true), node("b", None, true)];
        let plan = BlobPlanner::new(PlannerConfig::default())
            .plan(&blob, false, &nodes)
            .unwrap();
        assert!(matches!(
            plan.strategy,
            DownloadStrategy::SingleRequest { .. }
        ));
    }

    #[test]
    fn unknown_throughput_still_allows_capability_based_parallelism_for_large_blobs() {
        let blob = BlobMeta {
            digest: "sha256:abc".into(),
            size: 32 * 1024 * 1024,
            media_type: "application/octet-stream".into(),
        };
        let nodes = [node("a", None, true), node("b", None, true)];
        let plan = BlobPlanner::new(PlannerConfig::default())
            .plan(&blob, false, &nodes)
            .unwrap();
        assert!(matches!(
            plan.strategy,
            DownloadStrategy::MultiSourceChunked { .. }
        ));
    }

    #[test]
    fn one_range_node_can_use_its_configured_capacity_before_measurements_exist() {
        let blob = BlobMeta {
            digest: "sha256:abc".into(),
            size: 32 * 1024 * 1024,
            media_type: "application/octet-stream".into(),
        };
        let plan = BlobPlanner::new(PlannerConfig::default())
            .plan(&blob, false, &[node("a", None, true)])
            .unwrap();
        assert!(matches!(
            plan.strategy,
            DownloadStrategy::MultiSourceChunked { .. }
        ));
    }

    #[test]
    fn measured_slow_source_does_not_trigger_needless_parallelism() {
        let blob = BlobMeta {
            digest: "sha256:abc".into(),
            size: 32 * 1024 * 1024,
            media_type: "application/octet-stream".into(),
        };
        let nodes = [
            node("a", Some(10 * 1024 * 1024), true),
            node("b", Some(1_024 * 1024), true),
        ];
        let plan = BlobPlanner::new(PlannerConfig::default())
            .plan(&blob, false, &nodes)
            .unwrap();
        assert!(matches!(
            plan.strategy,
            DownloadStrategy::SingleRequest { .. }
        ));
    }

    #[test]
    fn cooling_and_incompatible_nodes_are_filtered() {
        let blob = BlobMeta {
            digest: "sha256:abc".into(),
            size: 32 * 1024 * 1024,
            media_type: "application/octet-stream".into(),
        };
        let mut cooling = node("cooling", None, true);
        cooling.cooling = true;
        let nodes = [cooling, node("single", None, false)];
        let plan = BlobPlanner::new(PlannerConfig::default())
            .plan(&blob, false, &nodes)
            .unwrap();
        assert_eq!(
            plan.strategy,
            DownloadStrategy::SingleRequest {
                node_url: "single".into()
            }
        );
    }

    #[test]
    fn no_nodes_returns_an_error_instead_of_panicking() {
        let blob = BlobMeta {
            digest: "sha256:abc".into(),
            size: 32 * 1024 * 1024,
            media_type: "application/octet-stream".into(),
        };
        assert_eq!(
            BlobPlanner::new(PlannerConfig::default()).plan(&blob, false, &[]),
            Err(PlannerError::NoUsableNodes)
        );
    }
}
