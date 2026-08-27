use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{body::Body, response::Response};
use futures_util::StreamExt;

const LIVE_WINDOW: Duration = Duration::from_secs(5);

#[derive(Clone, Default)]
pub struct TrafficMetrics {
    requests: Arc<AtomicU64>,
    response_bytes: Arc<AtomicU64>,
    live: Arc<Mutex<RateWindow>>,
}

#[derive(Clone, Copy, Debug)]
pub struct TrafficSnapshot {
    pub requests: u64,
    pub response_bytes: u64,
    pub current_bps: u64,
}

#[derive(Default)]
struct RateWindow {
    samples: VecDeque<(Instant, u64)>,
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
        let current_bps = self
            .live
            .lock()
            .map(|mut window| window.rate(Instant::now()))
            .unwrap_or(0);
        TrafficSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            response_bytes: self.response_bytes.load(Ordering::Relaxed),
            current_bps,
        }
    }

    fn record_bytes(&self, bytes: u64) {
        self.response_bytes.fetch_add(bytes, Ordering::Relaxed);
        if let Ok(mut window) = self.live.lock() {
            window.record(Instant::now(), bytes);
        }
    }
}

impl RateWindow {
    fn record(&mut self, now: Instant, bytes: u64) {
        self.samples.push_back((now, bytes));
        self.prune(now);
    }

    fn rate(&mut self, now: Instant) -> u64 {
        self.prune(now);
        let Some((first_at, _)) = self.samples.front() else {
            return 0;
        };
        let elapsed = now.duration_since(*first_at).as_secs_f64().max(0.25);
        let bytes = self
            .samples
            .iter()
            .fold(0_u64, |total, (_, bytes)| total.saturating_add(*bytes));
        (bytes as f64 / elapsed).min(u64::MAX as f64) as u64
    }

    fn prune(&mut self, now: Instant) {
        while self
            .samples
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > LIVE_WINDOW)
        {
            self.samples.pop_front();
        }
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
        assert!(snapshot.current_bps > 0);
    }
}
