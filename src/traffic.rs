use std::sync::atomic::{AtomicU64, Ordering};

use axum::{http::header, response::Response};

#[derive(Clone, Default)]
pub struct TrafficMetrics {
    requests: std::sync::Arc<AtomicU64>,
    response_bytes: std::sync::Arc<AtomicU64>,
}

impl TrafficMetrics {
    pub fn record_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_response(&self, response: &Response) {
        if let Some(length) = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
        {
            self.response_bytes.fetch_add(length, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.requests.load(Ordering::Relaxed),
            self.response_bytes.load(Ordering::Relaxed),
        )
    }
}
