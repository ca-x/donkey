use std::{sync::Arc, time::Instant};

use backoff::backoff::Backoff;
use dashmap::DashMap;
use uuid::Uuid;

use crate::{config::SchedulerPolicy, nodes::NodeView};

#[derive(Clone)]
pub(crate) struct NodeSelector {
    speeds: Arc<DashMap<Uuid, f64>>,
    active: Arc<DashMap<Uuid, usize>>,
    failures: Arc<DashMap<Uuid, FailureState>>,
}

struct FailureState {
    backoff: backoff::ExponentialBackoff,
    cooldown_until: Instant,
}

impl NodeSelector {
    pub(crate) fn new() -> Self {
        Self {
            speeds: Arc::new(DashMap::new()),
            active: Arc::new(DashMap::new()),
            failures: Arc::new(DashMap::new()),
        }
    }

    pub(crate) fn order<'a>(
        &self,
        nodes: &'a [NodeView],
        sequence: usize,
        policy: SchedulerPolicy,
    ) -> Vec<&'a NodeView> {
        let mut candidates = nodes
            .iter()
            .filter(|node| node.node.enabled && node.route.enabled)
            .collect::<Vec<_>>();
        let ready = candidates
            .iter()
            .copied()
            .filter(|node| !self.is_cooling(node.node.id))
            .collect::<Vec<_>>();
        if !ready.is_empty() {
            candidates = ready;
        }
        match policy {
            SchedulerPolicy::Balanced => {
                if !candidates.is_empty() {
                    let offset = sequence % candidates.len();
                    candidates.rotate_left(offset);
                }
            }
            SchedulerPolicy::SpeedFirst => candidates.sort_by(|left, right| {
                self.available_capacity(right)
                    .total_cmp(&self.available_capacity(left))
                    .then_with(|| left.node.priority.cmp(&right.node.priority))
                    .then_with(|| left.node.url.cmp(&right.node.url))
            }),
        }
        candidates
    }

    pub(crate) fn try_acquire(&self, node_id: Uuid, max_concurrency: u16) -> Option<NodeLease> {
        let mut active = self.active.entry(node_id).or_insert(0);
        if *active >= usize::from(max_concurrency) {
            return None;
        }
        *active += 1;
        drop(active);
        Some(NodeLease {
            node_id,
            active: self.active.clone(),
        })
    }

    pub(crate) fn at_capacity(&self, node_id: Uuid, max_concurrency: u16) -> bool {
        self.active
            .get(&node_id)
            .is_some_and(|value| *value >= usize::from(max_concurrency))
    }

    pub(crate) fn observe(
        &self,
        node_id: Uuid,
        bytes: u64,
        elapsed: std::time::Duration,
        success: bool,
    ) {
        if success {
            self.failures.remove(&node_id);
            if bytes > 0 && elapsed.as_secs_f64() > 0.0 {
                let sample = bytes as f64 / elapsed.as_secs_f64();
                self.speeds
                    .entry(node_id)
                    .and_modify(|value| *value = *value * 0.7 + sample * 0.3)
                    .or_insert(sample);
            }
            return;
        }
        self.speeds.entry(node_id).and_modify(|value| *value *= 0.5);
        let mut failure = self
            .failures
            .entry(node_id)
            .or_insert_with(|| FailureState {
                backoff: backoff::ExponentialBackoffBuilder::new()
                    .with_initial_interval(std::time::Duration::from_millis(250))
                    .with_max_interval(std::time::Duration::from_secs(10))
                    .with_randomization_factor(0.0)
                    .with_max_elapsed_time(None)
                    .build(),
                cooldown_until: Instant::now(),
            });
        let delay = failure
            .backoff
            .next_backoff()
            .unwrap_or_else(|| std::time::Duration::from_secs(10));
        failure.cooldown_until = Instant::now() + delay;
    }

    fn is_cooling(&self, node_id: Uuid) -> bool {
        self.failures
            .get(&node_id)
            .is_some_and(|failure| failure.cooldown_until > Instant::now())
    }

    fn available_capacity(&self, node: &NodeView) -> f64 {
        let measured = self
            .speeds
            .get(&node.node.id)
            .map(|value| *value)
            .unwrap_or(node.metric.speed_bps.max(0) as f64);
        let active = self
            .active
            .get(&node.node.id)
            .map(|value| *value)
            .unwrap_or(0);
        speed_first_capacity(measured, node.metric.success_rate, active)
    }
}

pub(crate) struct NodeLease {
    node_id: Uuid,
    active: Arc<DashMap<Uuid, usize>>,
}

impl Drop for NodeLease {
    fn drop(&mut self) {
        if let Some(mut value) = self.active.get_mut(&self.node_id) {
            *value = value.saturating_sub(1);
        }
    }
}

fn speed_first_capacity(measured_bps: f64, success_rate: f64, active: usize) -> f64 {
    let discovery_floor = 256.0 * 1024.0;
    measured_bps.max(discovery_floor) * success_rate.clamp(0.05, 1.0).powi(2) / (active + 1) as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn node_view(id: Uuid, priority: i32) -> NodeView {
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
                latency_ms: 10,
                speed_bps: 1024,
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
        }
    }

    #[test]
    fn lease_enforces_capacity_and_releases_on_drop() {
        let selector = NodeSelector::new();
        let id = Uuid::new_v4();
        let first = selector.try_acquire(id, 1).unwrap();
        assert!(selector.try_acquire(id, 1).is_none());
        drop(first);
        assert!(selector.try_acquire(id, 1).is_some());
    }

    #[test]
    fn failure_enters_cooldown_and_success_clears_it() {
        let selector = NodeSelector::new();
        let id = Uuid::new_v4();
        selector.observe(id, 0, std::time::Duration::from_millis(1), false);
        assert!(selector.is_cooling(id));
        selector.observe(id, 1024, std::time::Duration::from_millis(1), true);
        assert!(!selector.is_cooling(id));
    }

    #[test]
    fn speed_first_prefers_available_capacity_not_only_raw_speed() {
        let fast_idle = speed_first_capacity(8_000_000.0, 0.98, 0);
        let fast_busy = speed_first_capacity(8_000_000.0, 0.98, 7);
        let medium_idle = speed_first_capacity(2_000_000.0, 0.99, 0);
        assert!(fast_idle > medium_idle);
        assert!(medium_idle > fast_busy);
    }

    #[test]
    fn cooling_node_is_excluded_when_an_alternative_is_ready() {
        let selector = NodeSelector::new();
        let first = node_view(Uuid::new_v4(), 1);
        let second = node_view(Uuid::new_v4(), 2);
        selector.observe(first.node.id, 0, std::time::Duration::from_millis(1), false);
        let nodes = [first.clone(), second.clone()];
        let ordered = selector.order(&nodes, 0, SchedulerPolicy::Balanced);
        assert_eq!(ordered.len(), 1);
        assert_eq!(ordered[0].node.id, second.node.id);

        selector.observe(
            first.node.id,
            1024,
            std::time::Duration::from_millis(1),
            true,
        );
        assert_eq!(
            selector.order(&nodes, 0, SchedulerPolicy::Balanced)[0]
                .node
                .id,
            first.node.id
        );
    }
}
