use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use backoff::backoff::Backoff;
use dashmap::DashMap;
use uuid::Uuid;

use crate::{
    config::SchedulerPolicy,
    nodes::NodeView,
    scheduling::{ScheduleCandidate, ScheduleContext, SchedulerAlgorithm},
};

#[derive(Clone)]
pub(crate) struct NodeSelector {
    speeds: Arc<DashMap<Uuid, Arc<Mutex<SpeedWindow>>>>,
    active: Arc<DashMap<Uuid, usize>>,
    failures: Arc<DashMap<Uuid, FailureState>>,
    recovered: Arc<DashMap<Uuid, Instant>>,
    algorithm: Arc<RwLock<Arc<dyn SchedulerAlgorithm>>>,
}

struct FailureState {
    backoff: backoff::ExponentialBackoff,
    cooldown_until: Instant,
}

const SPEED_WINDOW: Duration = Duration::from_secs(60);
const MAX_SPEED_SAMPLES: usize = 8;

#[derive(Default)]
struct SpeedWindow {
    samples: VecDeque<SpeedSample>,
}

struct SpeedSample {
    at: Instant,
    bps: f64,
}

impl SpeedWindow {
    fn record(&mut self, now: Instant, bps: f64) {
        self.samples.push_back(SpeedSample { at: now, bps });
        self.prune(now);
        while self.samples.len() > MAX_SPEED_SAMPLES {
            self.samples.pop_front();
        }
    }

    fn estimate(&mut self, now: Instant) -> Option<f64> {
        self.prune(now);
        if self.samples.is_empty() {
            return None;
        }
        let mut values = self
            .samples
            .iter()
            .map(|sample| sample.bps)
            .collect::<Vec<_>>();
        values.sort_by(f64::total_cmp);
        if values.len() >= 5 {
            // Trim a high outlier only when it is clearly separated from the
            // window's median.  Always dropping the newest/highest sample
            // would make a genuinely recovered node look slower than it is.
            let median = values[values.len() / 2];
            let highest = *values.last().unwrap_or(&median);
            if median > 0.0 && highest > median * 4.0 {
                values.pop();
            }
        }
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }

    fn prune(&mut self, now: Instant) {
        while self
            .samples
            .front()
            .is_some_and(|sample| now.duration_since(sample.at) > SPEED_WINDOW)
        {
            self.samples.pop_front();
        }
    }
}

impl NodeSelector {
    pub(crate) fn new() -> Self {
        Self::with_algorithm(Arc::new(crate::scheduling::CurrentAlgorithm))
    }

    pub(crate) fn with_algorithm(algorithm: Arc<dyn SchedulerAlgorithm>) -> Self {
        Self {
            speeds: Arc::new(DashMap::new()),
            active: Arc::new(DashMap::new()),
            failures: Arc::new(DashMap::new()),
            recovered: Arc::new(DashMap::new()),
            algorithm: Arc::new(RwLock::new(algorithm)),
        }
    }

    pub(crate) fn algorithm_name(&self) -> &'static str {
        self.algorithm
            .read()
            .expect("scheduler algorithm lock poisoned")
            .name()
    }

    pub(crate) fn set_algorithm(&self, algorithm: Arc<dyn SchedulerAlgorithm>) {
        *self
            .algorithm
            .write()
            .expect("scheduler algorithm lock poisoned") = algorithm;
    }

    #[cfg(test)]
    pub(crate) fn order<'a>(
        &self,
        nodes: &'a [NodeView],
        sequence: usize,
        policy: SchedulerPolicy,
    ) -> Vec<&'a NodeView> {
        self.order_with_context(
            nodes,
            ScheduleContext {
                sequence: sequence as u64,
                blob_size: 0,
                range_required: false,
                chunk: None,
            },
            policy,
        )
    }

    pub(crate) fn order_with_context<'a>(
        &self,
        nodes: &'a [NodeView],
        context: ScheduleContext,
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
        let now = Instant::now();
        let candidates = candidates
            .into_iter()
            .map(|node| ScheduleCandidate {
                node,
                capacity_bps: self.measured_speed(node),
                latency_ms: node.metric.latency_ms.max(0) as u64,
                success_rate: node.metric.success_rate,
                active: self
                    .active
                    .get(&node.node.id)
                    .map(|value| *value)
                    .unwrap_or(0),
                max_concurrency: usize::from(node.max_concurrency),
                measured: self.speeds.contains_key(&node.node.id) || node.metric.speed_bps > 0,
                recently_recovered: self
                    .recovered
                    .get(&node.node.id)
                    .is_some_and(|until| *until > now),
            })
            .collect();
        let algorithm = self
            .algorithm
            .read()
            .expect("scheduler algorithm lock poisoned")
            .clone();
        algorithm.order(candidates, context, policy)
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
            if self.failures.remove(&node_id).is_some() {
                self.recovered
                    .insert(node_id, Instant::now() + Duration::from_secs(30));
            }
            if bytes > 0 && elapsed.as_secs_f64() > 0.0 {
                let sample = bytes as f64 / elapsed.as_secs_f64();
                let window = self
                    .speeds
                    .entry(node_id)
                    .or_insert_with(|| Arc::new(Mutex::new(SpeedWindow::default())))
                    .clone();
                if let Ok(mut window) = window.lock() {
                    window.record(Instant::now(), sample);
                }
            }
            return;
        }
        self.recovered.remove(&node_id);
        let window = self
            .speeds
            .entry(node_id)
            .or_insert_with(|| Arc::new(Mutex::new(SpeedWindow::default())))
            .clone();
        if let Ok(mut window) = window.lock() {
            window.record(Instant::now(), 0.0);
        }
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

    pub(crate) fn is_cooling_node(&self, node_id: Uuid) -> bool {
        self.is_cooling(node_id)
    }

    pub(crate) fn cooling_count(&self) -> usize {
        let now = Instant::now();
        self.failures
            .iter()
            .filter(|failure| failure.cooldown_until > now)
            .count()
    }

    fn measured_speed(&self, node: &NodeView) -> f64 {
        self.speeds
            .get(&node.node.id)
            .and_then(|window| window.lock().ok()?.estimate(Instant::now()))
            .unwrap_or(node.metric.speed_bps.max(0) as f64)
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

#[cfg(test)]
fn speed_first_capacity(
    measured_bps: f64,
    success_rate: f64,
    active: usize,
    max_concurrency: usize,
) -> f64 {
    let discovery_floor = 256.0 * 1024.0;
    let load = 1.0 + active as f64 / max_concurrency.max(1) as f64;
    measured_bps.max(discovery_floor) * success_rate.clamp(0.05, 1.0).powi(2) / load
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
            live_bps: 0,
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
        let fast_idle = speed_first_capacity(8_000_000.0, 0.98, 0, 8);
        let fast_busy = speed_first_capacity(8_000_000.0, 0.98, 7, 8);
        let medium_idle = speed_first_capacity(2_000_000.0, 0.99, 0, 8);
        assert!(fast_idle > medium_idle);
        assert!(fast_idle > fast_busy);
    }

    #[test]
    fn speed_window_trims_outliers_and_expires_old_samples() {
        let now = Instant::now();
        let mut window = SpeedWindow::default();
        for speed in [10.0, 11.0, 9.0, 10.0, 1_000.0] {
            window.record(now, speed);
        }
        let estimate = window.estimate(now).unwrap();
        assert!(estimate < 12.0);
        assert!(
            window
                .estimate(now + SPEED_WINDOW + Duration::from_millis(1))
                .is_none()
        );
    }

    #[test]
    fn balanced_prefers_measured_capacity_after_initial_exploration() {
        let selector = NodeSelector::new();
        let fast = node_view(Uuid::new_v4(), 1);
        let slow = node_view(Uuid::new_v4(), 2);
        selector.observe(
            fast.node.id,
            8 * 1024 * 1024,
            std::time::Duration::from_secs(1),
            true,
        );
        selector.observe(
            slow.node.id,
            256 * 1024,
            std::time::Duration::from_secs(1),
            true,
        );

        let nodes = [fast.clone(), slow];
        let fast_first = (0..64)
            .filter(|sequence| {
                selector
                    .order(&nodes, *sequence, SchedulerPolicy::Balanced)
                    .first()
                    .is_some_and(|node| node.node.id == fast.node.id)
            })
            .count();
        assert!(
            fast_first > 48,
            "fast node won only {fast_first}/64 selections"
        );
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
