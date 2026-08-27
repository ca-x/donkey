use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{body::Body, response::Response};
use futures_util::StreamExt;

const MIN_RATE_SAMPLE: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub struct TrafficMetrics {
    requests: Arc<AtomicU64>,
    response_bytes: Arc<AtomicU64>,
    rate: Arc<Mutex<RateSampler>>,
}

#[derive(Clone, Copy, Debug)]
pub struct TrafficSnapshot {
    pub requests: u64,
    pub response_bytes: u64,
    pub current_bps: u64,
}

struct RateSampler {
    observed_at: Instant,
    observed_bytes: u64,
    current_bps: u64,
}

impl Default for TrafficMetrics {
    fn default() -> Self {
        Self {
            requests: Arc::new(AtomicU64::new(0)),
            response_bytes: Arc::new(AtomicU64::new(0)),
            rate: Arc::new(Mutex::new(RateSampler::new(Instant::now(), 0))),
        }
    }
}

impl TrafficMetrics {
    pub fn record_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn track_response(&self, response: Response) -> Response {
        let (parts, body) = response.into_parts();
        let metrics = self.clone();
        let stream = body.into_data_stream().map(move |result| {
            if let Ok(bytes) = &result {
                metrics.record_bytes(bytes.len() as u64);
            }
            result
        });
        Response::from_parts(parts, Body::from_stream(stream))
    }

    pub fn snapshot(&self) -> TrafficSnapshot {
        let response_bytes = self.response_bytes.load(Ordering::Relaxed);
        let current_bps = self
            .rate
            .lock()
            .map(|mut sampler| sampler.observe(Instant::now(), response_bytes))
            .unwrap_or(0);
        TrafficSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            response_bytes,
            current_bps,
        }
    }

    fn record_bytes(&self, bytes: u64) {
        self.response_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

impl RateSampler {
    fn new(observed_at: Instant, observed_bytes: u64) -> Self {
        Self {
            observed_at,
            observed_bytes,
            current_bps: 0,
        }
    }

    fn observe(&mut self, now: Instant, total_bytes: u64) -> u64 {
        let elapsed = now.duration_since(self.observed_at);
        if elapsed >= MIN_RATE_SAMPLE {
            let bytes = total_bytes.saturating_sub(self.observed_bytes);
            self.current_bps = (bytes as f64 / elapsed.as_secs_f64()).min(u64::MAX as f64) as u64;
            self.observed_at = now;
            self.observed_bytes = total_bytes;
        }
        self.current_bps
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn response_bytes_are_counted_as_the_body_is_consumed() {
        let metrics = TrafficMetrics::default();
        let response = metrics.track_response(Response::new(Body::from("payload")));
        assert_eq!(metrics.snapshot().response_bytes, 0);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();

        assert_eq!(body.as_ref(), b"payload");
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.response_bytes, 7);
    }

    #[test]
    fn rate_is_sampled_from_the_atomic_total_off_the_response_hot_path() {
        let started = Instant::now();
        let mut sampler = RateSampler::new(started, 1_000);

        assert_eq!(
            sampler.observe(started + Duration::from_secs(2), 9_000),
            4_000
        );
        assert_eq!(
            sampler.observe(
                started + Duration::from_secs(2) + Duration::from_millis(10),
                9_100
            ),
            4_000
        );
        assert_eq!(sampler.observe(started + Duration::from_secs(5), 9_100), 33);
        assert_eq!(sampler.observe(started + Duration::from_secs(8), 9_100), 0);
    }
}
