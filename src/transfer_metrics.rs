use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;

const WINDOW_SECONDS: u64 = 5 * 60;
const BUCKET_SECONDS: u64 = 5;
const WINDOW_BUCKETS: usize = (WINDOW_SECONDS / BUCKET_SECONDS) as usize;
const HISTOGRAM_BOUNDS_MS: [u64; 15] = [
    1,
    5,
    10,
    25,
    50,
    100,
    250,
    500,
    1_000,
    2_500,
    5_000,
    10_000,
    30_000,
    60_000,
    u64::MAX,
];

#[derive(Clone)]
pub struct TransferMetrics {
    inner: Arc<Inner>,
}

struct Inner {
    started_at: DateTime<Utc>,
    started_instant: std::time::Instant,
    lifetime: Counters,
    lifetime_histograms: Histograms,
    window: Box<[WindowSlot]>,
}

struct WindowSlot {
    generation: AtomicU64,
    counters: Counters,
    histograms: Histograms,
}

struct Counters {
    blob_get_requests: AtomicU64,
    blob_get_hits: AtomicU64,
    blob_get_misses: AtomicU64,
    blob_head_requests: AtomicU64,
    blob_head_hits: AtomicU64,
    cache_bytes_served: AtomicU64,
    cache_admissions_started: AtomicU64,
    cache_admissions_succeeded: AtomicU64,
    cache_admissions_failed: AtomicU64,
    cache_bytes_admitted: AtomicU64,
    upstream_requests: AtomicU64,
    upstream_bytes_fetched: AtomicU64,
}

struct Histograms {
    cold_ttfb_ms: Histogram,
    cold_complete_ms: Histogram,
    warm_ttfb_ms: Histogram,
    warm_complete_ms: Histogram,
}

struct Histogram {
    buckets: [AtomicU64; HISTOGRAM_BOUNDS_MS.len()],
}

#[derive(Clone, Debug, Serialize)]
pub struct TransferMetricsSnapshot {
    pub started_at: DateTime<Utc>,
    pub window_seconds: u64,
    pub lifetime: MetricsView,
    pub window: MetricsView,
}

#[derive(Clone, Debug, Serialize)]
pub struct MetricsView {
    pub blob_get_requests: u64,
    pub blob_get_hits: u64,
    pub blob_get_misses: u64,
    pub blob_head_requests: u64,
    pub blob_head_hits: u64,
    pub cache_hit_ratio: Option<f64>,
    pub cache_bytes_served: u64,
    pub cache_admissions_started: u64,
    pub cache_admissions_succeeded: u64,
    pub cache_admissions_failed: u64,
    pub cache_bytes_admitted: u64,
    pub upstream_requests: u64,
    pub upstream_bytes_fetched: u64,
    pub cold_ttfb_ms: HistogramView,
    pub cold_complete_ms: HistogramView,
    pub warm_ttfb_ms: HistogramView,
    pub warm_complete_ms: HistogramView,
}

#[derive(Clone, Debug, Serialize)]
pub struct HistogramView {
    pub bounds_ms: Vec<u64>,
    pub buckets: Vec<u64>,
    pub samples: u64,
    pub p50_ms: Option<u64>,
    pub p95_ms: Option<u64>,
    pub p99_ms: Option<u64>,
}

impl Default for TransferMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl TransferMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                started_at: Utc::now(),
                started_instant: std::time::Instant::now(),
                lifetime: Counters::new(),
                lifetime_histograms: Histograms::new(),
                window: (0..WINDOW_BUCKETS)
                    .map(|_| WindowSlot::new())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            }),
        }
    }

    pub fn record_blob_get(&self, hit: bool) {
        self.record_counter(|c| {
            add(&c.blob_get_requests, 1);
            add(
                if hit {
                    &c.blob_get_hits
                } else {
                    &c.blob_get_misses
                },
                1,
            );
        });
    }

    pub fn record_blob_head(&self, hit: bool) {
        self.record_counter(|c| {
            add(&c.blob_head_requests, 1);
            if hit {
                add(&c.blob_head_hits, 1);
            }
        });
    }

    pub fn record_cache_bytes_served(&self, bytes: u64) {
        self.record_counter(|c| add(&c.cache_bytes_served, bytes));
    }

    pub fn record_cache_admission_started(&self) {
        self.record_counter(|c| add(&c.cache_admissions_started, 1));
    }

    pub fn record_cache_admission_succeeded(&self, bytes: u64) {
        self.record_counter(|c| {
            add(&c.cache_admissions_succeeded, 1);
            add(&c.cache_bytes_admitted, bytes);
        });
    }

    pub fn record_cache_admission_failed(&self) {
        self.record_counter(|c| add(&c.cache_admissions_failed, 1));
    }

    pub fn record_upstream_request(&self) {
        self.record_counter(|c| add(&c.upstream_requests, 1));
    }

    pub fn record_upstream_bytes(&self, bytes: u64) {
        self.record_counter(|c| add(&c.upstream_bytes_fetched, bytes));
    }

    pub fn record_cold_ttfb(&self, elapsed: Duration) {
        self.record_histogram(|h| h.cold_ttfb_ms.record(elapsed));
    }

    pub fn record_cold_complete(&self, elapsed: Duration) {
        self.record_histogram(|h| h.cold_complete_ms.record(elapsed));
    }

    pub fn record_warm_ttfb(&self, elapsed: Duration) {
        self.record_histogram(|h| h.warm_ttfb_ms.record(elapsed));
    }

    pub fn record_warm_complete(&self, elapsed: Duration) {
        self.record_histogram(|h| h.warm_complete_ms.record(elapsed));
    }

    pub fn snapshot(&self) -> TransferMetricsSnapshot {
        let generation = self.current_generation();
        TransferMetricsSnapshot {
            started_at: self.inner.started_at,
            window_seconds: WINDOW_SECONDS,
            lifetime: view_from(&self.inner.lifetime, &self.inner.lifetime_histograms),
            window: self.snapshot_window(generation),
        }
    }

    fn record_counter(&self, record: impl Fn(&Counters)) {
        record(&self.inner.lifetime);
        let slot = self.current_slot();
        record(&slot.counters);
    }

    fn record_histogram(&self, record: impl Fn(&Histograms)) {
        record(&self.inner.lifetime_histograms);
        let slot = self.current_slot();
        record(&slot.histograms);
    }

    fn current_slot(&self) -> &WindowSlot {
        let generation = self.current_generation();
        let slot = &self.inner.window[(generation as usize) % WINDOW_BUCKETS];
        slot.prepare(generation);
        slot
    }

    fn snapshot_window(&self, generation: u64) -> MetricsView {
        let counters = Counters::new();
        let histograms = Histograms::new();
        let oldest = generation.saturating_sub(WINDOW_BUCKETS as u64 - 1);
        for slot in self.inner.window.iter() {
            let marker = slot.generation.load(Ordering::Acquire);
            if marker % 2 == 0 {
                let slot_generation = marker / 2;
                if slot_generation >= oldest && slot_generation <= generation {
                    counters.add_snapshot(&slot.counters);
                    histograms.add_snapshot(&slot.histograms);
                }
            }
        }
        view_from(&counters, &histograms)
    }

    fn current_generation(&self) -> u64 {
        self.inner.started_instant.elapsed().as_secs() / BUCKET_SECONDS
    }
}

impl Counters {
    fn new() -> Self {
        Self {
            blob_get_requests: AtomicU64::new(0),
            blob_get_hits: AtomicU64::new(0),
            blob_get_misses: AtomicU64::new(0),
            blob_head_requests: AtomicU64::new(0),
            blob_head_hits: AtomicU64::new(0),
            cache_bytes_served: AtomicU64::new(0),
            cache_admissions_started: AtomicU64::new(0),
            cache_admissions_succeeded: AtomicU64::new(0),
            cache_admissions_failed: AtomicU64::new(0),
            cache_bytes_admitted: AtomicU64::new(0),
            upstream_requests: AtomicU64::new(0),
            upstream_bytes_fetched: AtomicU64::new(0),
        }
    }

    fn add_snapshot(&self, other: &Self) {
        add(
            &self.blob_get_requests,
            other.blob_get_requests.load(Ordering::Relaxed),
        );
        add(
            &self.blob_get_hits,
            other.blob_get_hits.load(Ordering::Relaxed),
        );
        add(
            &self.blob_get_misses,
            other.blob_get_misses.load(Ordering::Relaxed),
        );
        add(
            &self.blob_head_requests,
            other.blob_head_requests.load(Ordering::Relaxed),
        );
        add(
            &self.blob_head_hits,
            other.blob_head_hits.load(Ordering::Relaxed),
        );
        add(
            &self.cache_bytes_served,
            other.cache_bytes_served.load(Ordering::Relaxed),
        );
        add(
            &self.cache_admissions_started,
            other.cache_admissions_started.load(Ordering::Relaxed),
        );
        add(
            &self.cache_admissions_succeeded,
            other.cache_admissions_succeeded.load(Ordering::Relaxed),
        );
        add(
            &self.cache_admissions_failed,
            other.cache_admissions_failed.load(Ordering::Relaxed),
        );
        add(
            &self.cache_bytes_admitted,
            other.cache_bytes_admitted.load(Ordering::Relaxed),
        );
        add(
            &self.upstream_requests,
            other.upstream_requests.load(Ordering::Relaxed),
        );
        add(
            &self.upstream_bytes_fetched,
            other.upstream_bytes_fetched.load(Ordering::Relaxed),
        );
    }
}

impl Histograms {
    fn new() -> Self {
        Self {
            cold_ttfb_ms: Histogram::new(),
            cold_complete_ms: Histogram::new(),
            warm_ttfb_ms: Histogram::new(),
            warm_complete_ms: Histogram::new(),
        }
    }

    fn add_snapshot(&self, other: &Self) {
        self.cold_ttfb_ms.add_snapshot(&other.cold_ttfb_ms);
        self.cold_complete_ms.add_snapshot(&other.cold_complete_ms);
        self.warm_ttfb_ms.add_snapshot(&other.warm_ttfb_ms);
        self.warm_complete_ms.add_snapshot(&other.warm_complete_ms);
    }
}

impl Histogram {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn record(&self, elapsed: Duration) {
        let millis = elapsed.as_millis().min(u64::MAX as u128) as u64;
        let index = HISTOGRAM_BOUNDS_MS
            .iter()
            .position(|bound| millis <= *bound)
            .unwrap_or(HISTOGRAM_BOUNDS_MS.len() - 1);
        add(&self.buckets[index], 1);
    }

    fn add_snapshot(&self, other: &Self) {
        for (target, source) in self.buckets.iter().zip(other.buckets.iter()) {
            add(target, source.load(Ordering::Relaxed));
        }
    }

    fn view(&self) -> HistogramView {
        let buckets = self
            .buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect::<Vec<_>>();
        let samples = buckets.iter().copied().fold(0, u64::saturating_add);
        HistogramView {
            bounds_ms: HISTOGRAM_BOUNDS_MS.to_vec(),
            p50_ms: percentile(&buckets, samples, 0.50),
            p95_ms: percentile(&buckets, samples, 0.95),
            p99_ms: percentile(&buckets, samples, 0.99),
            buckets,
            samples,
        }
    }
}

impl WindowSlot {
    fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            counters: Counters::new(),
            histograms: Histograms::new(),
        }
    }

    fn prepare(&self, generation: u64) {
        let stable = generation.saturating_mul(2);
        loop {
            let observed = self.generation.load(Ordering::Acquire);
            if observed == stable {
                return;
            }
            if observed % 2 == 1 {
                std::hint::spin_loop();
                continue;
            }
            if self
                .generation
                .compare_exchange(observed, stable + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.counters.reset();
                self.histograms.reset();
                self.generation.store(stable, Ordering::Release);
                return;
            }
        }
    }
}

impl Counters {
    fn reset(&self) {
        for counter in [
            &self.blob_get_requests,
            &self.blob_get_hits,
            &self.blob_get_misses,
            &self.blob_head_requests,
            &self.blob_head_hits,
            &self.cache_bytes_served,
            &self.cache_admissions_started,
            &self.cache_admissions_succeeded,
            &self.cache_admissions_failed,
            &self.cache_bytes_admitted,
            &self.upstream_requests,
            &self.upstream_bytes_fetched,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
    }
}

impl Histograms {
    fn reset(&self) {
        for histogram in [
            &self.cold_ttfb_ms,
            &self.cold_complete_ms,
            &self.warm_ttfb_ms,
            &self.warm_complete_ms,
        ] {
            histogram.reset();
        }
    }
}

impl Histogram {
    fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
    }
}

fn view_from(counters: &Counters, histograms: &Histograms) -> MetricsView {
    let blob_get_requests = counters.blob_get_requests.load(Ordering::Relaxed);
    let blob_get_hits = counters.blob_get_hits.load(Ordering::Relaxed);
    let blob_get_misses = counters.blob_get_misses.load(Ordering::Relaxed);
    let denominator = blob_get_hits.saturating_add(blob_get_misses);
    MetricsView {
        blob_get_requests,
        blob_get_hits,
        blob_get_misses,
        blob_head_requests: counters.blob_head_requests.load(Ordering::Relaxed),
        blob_head_hits: counters.blob_head_hits.load(Ordering::Relaxed),
        cache_hit_ratio: (denominator > 0).then(|| blob_get_hits as f64 / denominator as f64),
        cache_bytes_served: counters.cache_bytes_served.load(Ordering::Relaxed),
        cache_admissions_started: counters.cache_admissions_started.load(Ordering::Relaxed),
        cache_admissions_succeeded: counters.cache_admissions_succeeded.load(Ordering::Relaxed),
        cache_admissions_failed: counters.cache_admissions_failed.load(Ordering::Relaxed),
        cache_bytes_admitted: counters.cache_bytes_admitted.load(Ordering::Relaxed),
        upstream_requests: counters.upstream_requests.load(Ordering::Relaxed),
        upstream_bytes_fetched: counters.upstream_bytes_fetched.load(Ordering::Relaxed),
        cold_ttfb_ms: histograms.cold_ttfb_ms.view(),
        cold_complete_ms: histograms.cold_complete_ms.view(),
        warm_ttfb_ms: histograms.warm_ttfb_ms.view(),
        warm_complete_ms: histograms.warm_complete_ms.view(),
    }
}

fn percentile(buckets: &[u64], samples: u64, quantile: f64) -> Option<u64> {
    if samples == 0 {
        return None;
    }
    let rank = ((samples as f64 * quantile).ceil() as u64).max(1);
    let mut cumulative = 0_u64;
    for (index, count) in buckets.iter().copied().enumerate() {
        cumulative = cumulative.saturating_add(count);
        if cumulative >= rank {
            return HISTOGRAM_BOUNDS_MS.get(index).copied();
        }
    }
    Some(u64::MAX)
}

fn add(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(amount))
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn ratios_and_histogram_percentiles_are_deterministic() {
        let metrics = TransferMetrics::new();
        metrics.record_blob_get(false);
        metrics.record_blob_get(true);
        metrics.record_warm_complete(Duration::from_millis(3));
        metrics.record_warm_complete(Duration::from_millis(700));
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.lifetime.blob_get_requests, 2);
        assert_eq!(snapshot.lifetime.blob_get_hits, 1);
        assert_eq!(snapshot.lifetime.blob_get_misses, 1);
        assert_eq!(snapshot.lifetime.cache_hit_ratio, Some(0.5));
        assert_eq!(snapshot.lifetime.warm_complete_ms.p50_ms, Some(5));
        assert_eq!(snapshot.lifetime.warm_complete_ms.p95_ms, Some(1_000));
    }

    #[test]
    fn counters_are_saturating_and_concurrent_updates_are_preserved() {
        let metrics = Arc::new(TransferMetrics::new());
        let mut workers = Vec::new();
        for _ in 0..8 {
            let metrics = metrics.clone();
            workers.push(thread::spawn(move || {
                for _ in 0..1_000 {
                    metrics.record_upstream_bytes(1);
                    metrics.record_blob_get(true);
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.lifetime.blob_get_requests, 8_000);
        assert_eq!(snapshot.lifetime.blob_get_hits, 8_000);
        assert_eq!(snapshot.lifetime.upstream_bytes_fetched, 8_000);
    }

    #[test]
    fn counters_do_not_wrap_at_u64_max() {
        let metrics = TransferMetrics::new();
        metrics
            .inner
            .lifetime
            .upstream_bytes_fetched
            .store(u64::MAX - 1, Ordering::Relaxed);
        metrics.record_upstream_bytes(2);
        assert_eq!(metrics.snapshot().lifetime.upstream_bytes_fetched, u64::MAX);
    }

    #[test]
    fn zero_requests_have_no_ratio_or_percentiles() {
        let snapshot = TransferMetrics::new().snapshot();
        assert_eq!(snapshot.lifetime.cache_hit_ratio, None);
        assert_eq!(snapshot.lifetime.cold_ttfb_ms.p50_ms, None);
        assert_eq!(snapshot.window.blob_get_requests, 0);
    }
}
