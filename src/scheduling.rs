//! Pluggable node-ordering algorithms.
//!
//! The executor owns leases, cooldowns, retries, and integrity checks.  An
//! algorithm only receives a snapshot of candidates and returns their order;
//! this keeps experiments comparable and prevents a strategy from bypassing
//! the safety boundaries of the downloader.

use crate::{config::SchedulerPolicy, nodes::NodeView};

#[derive(Clone, Copy, Debug)]
pub(crate) struct ScheduleContext {
    pub sequence: u64,
    pub blob_size: u64,
    pub range_required: bool,
    pub chunk: Option<(u64, u64)>,
}

pub(crate) struct ScheduleCandidate<'a> {
    pub node: &'a NodeView,
    pub capacity_bps: f64,
    pub latency_ms: u64,
    pub success_rate: f64,
    pub active: usize,
    pub max_concurrency: usize,
    pub measured: bool,
    pub recently_recovered: bool,
}

pub(crate) trait SchedulerAlgorithm: Send + Sync {
    fn name(&self) -> &'static str;

    fn order<'a>(
        &self,
        candidates: Vec<ScheduleCandidate<'a>>,
        context: ScheduleContext,
        policy: SchedulerPolicy,
    ) -> Vec<&'a NodeView>;
}

#[derive(Default)]
pub(crate) struct CurrentAlgorithm;

/// A latency-aware alternative used for comparison.  It ranks the next
/// request by projected completion time and penalizes unreliable/loaded nodes.
#[derive(Default)]
pub(crate) struct ProjectedCompletionAlgorithm;

impl SchedulerAlgorithm for CurrentAlgorithm {
    fn name(&self) -> &'static str {
        "current-balanced"
    }

    fn order<'a>(
        &self,
        mut candidates: Vec<ScheduleCandidate<'a>>,
        context: ScheduleContext,
        policy: SchedulerPolicy,
    ) -> Vec<&'a NodeView> {
        match policy {
            SchedulerPolicy::Balanced => {
                let measured = candidates.iter().any(|candidate| candidate.measured);
                if measured {
                    if let Some(index) = candidates
                        .iter()
                        .position(|candidate| candidate.recently_recovered)
                    {
                        candidates.rotate_left(index);
                    } else {
                        let unmeasured = candidates
                            .iter()
                            .enumerate()
                            .filter_map(|(index, candidate)| (!candidate.measured).then_some(index))
                            .collect::<Vec<_>>();
                        if !unmeasured.is_empty() {
                            let selected =
                                unmeasured[(context.sequence as usize) % unmeasured.len()];
                            candidates.rotate_left(selected);
                        } else {
                            candidates.sort_by(|left, right| {
                                effective_capacity(right)
                                    .total_cmp(&effective_capacity(left))
                                    .then_with(|| {
                                        left.node.node.priority.cmp(&right.node.node.priority)
                                    })
                                    .then_with(|| left.node.node.url.cmp(&right.node.node.url))
                            });
                            let max_capacity = candidates
                                .iter()
                                .map(|candidate| effective_capacity(candidate))
                                .fold(0.0, f64::max)
                                .max(1.0);
                            // A modulo ticket is biased when the first node has a
                            // large weight (for example, all 13 chunks can land
                            // in its first 100 tickets).  Weighted rendezvous
                            // hashing gives every request a deterministic winner
                            // while preserving the relative capacities.
                            let selected = candidates
                                .iter()
                                .enumerate()
                                .map(|(index, candidate)| {
                                    let weight =
                                        (effective_capacity(candidate) / max_capacity).max(0.001);
                                    let unit = rendezvous_unit(context.sequence, candidate);
                                    (index, -unit.ln() / weight)
                                })
                                .min_by(|left, right| left.1.total_cmp(&right.1))
                                .map(|(index, _)| index)
                                .unwrap_or(0);
                            candidates.rotate_left(selected);
                        }
                    }
                } else if !candidates.is_empty() {
                    let offset = context.sequence as usize % candidates.len();
                    candidates.rotate_left(offset);
                }
            }
            SchedulerPolicy::SpeedFirst => candidates.sort_by(|left, right| {
                effective_capacity(right)
                    .total_cmp(&effective_capacity(left))
                    .then_with(|| left.node.node.priority.cmp(&right.node.node.priority))
                    .then_with(|| left.node.node.url.cmp(&right.node.node.url))
            }),
        }
        candidates
            .into_iter()
            .map(|candidate| candidate.node)
            .collect()
    }
}

impl SchedulerAlgorithm for ProjectedCompletionAlgorithm {
    fn name(&self) -> &'static str {
        "projected-completion"
    }

    fn order<'a>(
        &self,
        mut candidates: Vec<ScheduleCandidate<'a>>,
        context: ScheduleContext,
        _policy: SchedulerPolicy,
    ) -> Vec<&'a NodeView> {
        candidates.sort_by(|left, right| {
            projected_cost(left, context)
                .total_cmp(&projected_cost(right, context))
                .then_with(|| {
                    rendezvous_unit(context.sequence, left)
                        .total_cmp(&rendezvous_unit(context.sequence, right))
                })
                .then_with(|| left.node.node.priority.cmp(&right.node.node.priority))
                .then_with(|| left.node.node.url.cmp(&right.node.node.url))
        });
        candidates
            .into_iter()
            .map(|candidate| candidate.node)
            .collect()
    }
}

fn projected_cost(candidate: &ScheduleCandidate<'_>, context: ScheduleContext) -> f64 {
    let throughput = candidate.capacity_bps.max(256.0 * 1024.0);
    let reliability = candidate.success_rate.clamp(0.05, 1.0);
    let load = candidate.active as f64 / candidate.max_concurrency.max(1) as f64;
    let queue = 1.0 + load;
    let request_bytes = match (context.range_required, context.chunk) {
        (true, Some((start, end))) => end.saturating_sub(start).saturating_add(1),
        (_, Some((start, end))) => end.saturating_sub(start).saturating_add(1),
        (_, None) => context.blob_size.max(1),
    } as f64;
    candidate.latency_ms as f64 / 1_000.0
        + (queue * request_bytes) / (throughput * reliability * reliability)
}

fn effective_capacity(candidate: &ScheduleCandidate<'_>) -> f64 {
    let load = 1.0 + candidate.active as f64 / candidate.max_concurrency.max(1) as f64;
    candidate.capacity_bps.max(256.0 * 1024.0) * candidate.success_rate.clamp(0.05, 1.0).powi(2)
        / load
}

fn rendezvous_unit(sequence: u64, candidate: &ScheduleCandidate<'_>) -> f64 {
    // SplitMix64 is fast, deterministic, and does not require a process-wide
    // RNG lock on the download hot path.  The node URL is unique within a
    // route, so hashing it alone keeps affinity stable when metrics change.
    let mut state = sequence;
    for byte in candidate.node.node.url.as_bytes() {
        state = (state ^ u64::from(*byte)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        state ^= state >> 27;
    }
    state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    state = (state ^ (state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    state = (state ^ (state >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    state ^= state >> 31;
    // Keep the logarithm finite and strictly positive.
    ((state as f64 + 1.0) / (u64::MAX as f64 + 2.0)).clamp(f64::MIN_POSITIVE, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn candidate<'a>(
        node: &'a NodeView,
        capacity_bps: f64,
        latency_ms: u64,
    ) -> ScheduleCandidate<'a> {
        ScheduleCandidate {
            node,
            capacity_bps,
            latency_ms,
            success_rate: 1.0,
            active: 0,
            max_concurrency: node.max_concurrency as usize,
            measured: true,
            recently_recovered: false,
        }
    }

    fn node(id: Uuid, priority: i32) -> NodeView {
        let now = Utc::now();
        NodeView {
            node: crate::db::node::Model {
                id,
                name: format!("node-{priority}"),
                url: format!("https://node-{priority}.example/"),
                registry_route_id: crate::registry_routes::DOCKER_HUB_ROUTE_ID,
                enabled: true,
                priority,
                cf_preferred: false,
                connect_ip_type: "ip".into(),
                connect_ip: None,
                auth_mode: "none".into(),
                auth_username: None,
                auth_header: None,
                auth_secret_enc: None,
                created_at: now,
                updated_at: now,
            },
            metric: crate::db::node_metric::Model {
                node_id: id,
                healthy: true,
                latency_ms: i64::from(latency_for_test(priority)),
                speed_bps: 0,
                success_rate: 1.0,
                current_bps: 0,
                total_bytes: 0,
                last_checked_at: Some(now),
                last_error: None,
            },
            score: 1.0,
            auth_configured: false,
            route: crate::registry_routes::RegistryRouteSummary {
                id: crate::registry_routes::DOCKER_HUB_ROUTE_ID,
                key: "dockerhub".into(),
                name: "Docker Hub".into(),
                canonical_registry: "docker.io".into(),
                path_prefix: None,
                repository_mode: "docker_hub_library".into(),
                enabled: true,
            },
            max_concurrency: 4,
            live_bps: 0,
        }
    }

    const fn latency_for_test(priority: i32) -> i32 {
        priority * 10
    }

    #[test]
    fn projected_algorithm_prefers_fast_low_latency_candidate() {
        let fast = node(Uuid::new_v4(), 1);
        let slow = node(Uuid::new_v4(), 2);
        let ordered = ProjectedCompletionAlgorithm.order(
            vec![
                candidate(&slow, 256.0 * 1024.0, 200),
                candidate(&fast, 8.0 * 1024.0 * 1024.0, 10),
            ],
            ScheduleContext {
                sequence: 0,
                blob_size: 100 * 1024 * 1024,
                range_required: true,
                chunk: Some((0, 1024)),
            },
            SchedulerPolicy::Balanced,
        );
        assert_eq!(ordered[0].node.id, fast.node.id);
    }

    #[test]
    fn projected_algorithm_explores_equal_candidates_instead_of_starving_one() {
        let first = node(Uuid::new_v4(), 1);
        let second = node(Uuid::new_v4(), 2);
        let algorithm = ProjectedCompletionAlgorithm;
        let mut winners = [0_usize; 2];
        for sequence in 0..64 {
            let ordered = algorithm.order(
                vec![
                    candidate(&first, 4.0 * 1024.0 * 1024.0, 20),
                    candidate(&second, 4.0 * 1024.0 * 1024.0, 20),
                ],
                ScheduleContext {
                    sequence,
                    blob_size: 64 * 1024 * 1024,
                    range_required: true,
                    chunk: Some((0, 1024 * 1024 - 1)),
                },
                SchedulerPolicy::Balanced,
            );
            let index = if ordered[0].node.id == first.node.id {
                0
            } else {
                1
            };
            winners[index] += 1;
        }
        assert!(winners[0] > 0 && winners[1] > 0);
    }

    #[test]
    fn current_algorithm_distributes_equal_capacity_across_chunks() {
        let first = node(Uuid::new_v4(), 1);
        let second = node(Uuid::new_v4(), 2);
        let candidates = [
            candidate(&first, 8.0 * 1024.0 * 1024.0, 10),
            candidate(&second, 8.0 * 1024.0 * 1024.0, 10),
        ];
        let algorithm = CurrentAlgorithm;
        let mut first_count: i32 = 0;
        let mut second_count: i32 = 0;
        for sequence in 0..64 {
            let ordered = algorithm.order(
                candidates
                    .iter()
                    .map(|candidate| ScheduleCandidate {
                        node: candidate.node,
                        capacity_bps: candidate.capacity_bps,
                        latency_ms: candidate.latency_ms,
                        success_rate: candidate.success_rate,
                        active: candidate.active,
                        max_concurrency: candidate.max_concurrency,
                        measured: candidate.measured,
                        recently_recovered: candidate.recently_recovered,
                    })
                    .collect(),
                ScheduleContext {
                    sequence,
                    blob_size: 64 * 1024 * 1024,
                    range_required: true,
                    chunk: Some((sequence * 1024, sequence * 1024 + 1023)),
                },
                SchedulerPolicy::Balanced,
            );
            if ordered[0].node.id == first.node.id {
                first_count += 1;
            } else {
                second_count += 1;
            }
        }
        assert!(first_count > 0 && second_count > 0);
        assert!((first_count - second_count).unsigned_abs() < 24);
    }

    #[test]
    fn current_algorithm_gives_slower_nodes_some_bounded_traffic() {
        let fast = node(Uuid::new_v4(), 1);
        let slow = node(Uuid::new_v4(), 2);
        let algorithm = CurrentAlgorithm;
        let mut fast_count = 0;
        let mut slow_count = 0;
        for sequence in 0..256 {
            let ordered = algorithm.order(
                vec![
                    candidate(&fast, 10.0 * 1024.0 * 1024.0, 10),
                    candidate(&slow, 2.0 * 1024.0 * 1024.0, 10),
                ],
                ScheduleContext {
                    sequence,
                    blob_size: 256 * 1024 * 1024,
                    range_required: true,
                    chunk: Some((sequence * 1024, sequence * 1024 + 1023)),
                },
                SchedulerPolicy::Balanced,
            );
            if ordered[0].node.id == fast.node.id {
                fast_count += 1;
            } else {
                slow_count += 1;
            }
        }
        assert!(fast_count > slow_count);
        assert!(slow_count > 0);
        assert!(
            slow_count >= 16,
            "slow node received only {slow_count} assignments"
        );
    }

    #[test]
    fn algorithms_combine_throughput_latency_reliability_and_load_differently() {
        let nodes = [
            node(Uuid::new_v4(), 1),
            node(Uuid::new_v4(), 2),
            node(Uuid::new_v4(), 3),
            node(Uuid::new_v4(), 4),
        ];
        // A is consistently fast, B has low RTT, C is fast but unreliable,
        // and D is overloaded. These values are synthetic so the test is
        // deterministic and does not depend on an external mirror.
        let metrics = [
            (20.0 * 1024.0 * 1024.0, 20, 0.98, 1, 4),
            (12.0 * 1024.0 * 1024.0, 5, 0.98, 0, 4),
            (50.0 * 1024.0 * 1024.0, 500, 0.70, 0, 4),
            (8.0 * 1024.0 * 1024.0, 10, 0.98, 4, 4),
        ];
        let context_for = |sequence| ScheduleContext {
            sequence,
            blob_size: 512 * 1024 * 1024,
            range_required: true,
            chunk: Some((0, 8 * 1024 * 1024 - 1)),
        };
        let mut current_counts = [0_usize; 4];
        let mut projected_counts = [0_usize; 4];
        let mut current_active = [0_usize; 4];
        let mut projected_active = [0_usize; 4];
        for sequence in 0..512 {
            let candidates = |active: &[usize; 4]| {
                metrics
                    .iter()
                    .enumerate()
                    .map(
                        |(index, (capacity_bps, latency_ms, success_rate, _, max))| {
                            ScheduleCandidate {
                                node: &nodes[index],
                                capacity_bps: *capacity_bps,
                                latency_ms: *latency_ms,
                                success_rate: *success_rate,
                                active: active[index],
                                max_concurrency: *max,
                                measured: true,
                                recently_recovered: false,
                            }
                        },
                    )
                    .collect::<Vec<_>>()
            };
            let current = CurrentAlgorithm.order(
                candidates(&current_active),
                context_for(sequence),
                SchedulerPolicy::Balanced,
            )[0]
            .node
            .priority as usize
                - 1;
            let projected = ProjectedCompletionAlgorithm.order(
                candidates(&projected_active),
                context_for(sequence),
                SchedulerPolicy::Balanced,
            )[0]
            .node
            .priority as usize
                - 1;
            current_counts[current] += 1;
            projected_counts[projected] += 1;
            current_active[current] += 1;
            projected_active[projected] += 1;
            // Complete one request on every fourth scheduling decision to
            // model a bounded in-flight window rather than static load.
            if sequence % 4 == 3 {
                current_active
                    .iter_mut()
                    .for_each(|active| *active = active.saturating_sub(1));
                projected_active
                    .iter_mut()
                    .for_each(|active| *active = active.saturating_sub(1));
            }
        }
        assert!(current_counts.iter().filter(|count| **count > 0).count() >= 2);
        assert!(current_counts[2] > current_counts[0]);
        assert!(projected_counts[0] >= projected_counts[1]);
        assert!(projected_counts[0] > projected_counts[2]);
        assert!(projected_counts.iter().filter(|count| **count > 0).count() >= 2);
    }
}
