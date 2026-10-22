use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::error::{ApiResult, AppError};

pub(crate) const MAX_NETWORK_REQUESTS: usize = 64;
pub(crate) const MAX_PROBES: usize = 8;
pub(crate) const STREAM_MEMORY_BYTES: u32 = 64 * 1024 * 1024;
pub(crate) const GLOBAL_STREAM_MEMORY_BYTES: u32 = 256 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct NodeCapacity {
    active: Arc<DashMap<Uuid, usize>>,
    changed: Arc<Notify>,
    requests: Arc<Semaphore>,
    probes: Arc<Semaphore>,
}

impl Default for NodeCapacity {
    fn default() -> Self {
        Self {
            active: Arc::new(DashMap::new()),
            changed: Arc::new(Notify::new()),
            requests: Arc::new(Semaphore::new(MAX_NETWORK_REQUESTS)),
            probes: Arc::new(Semaphore::new(MAX_PROBES)),
        }
    }
}

impl NodeCapacity {
    pub(crate) fn active(&self, node_id: Uuid) -> usize {
        self.active.get(&node_id).map_or(0, |value| *value)
    }

    pub(crate) fn try_acquire(&self, node_id: Uuid, max: u16) -> Option<NodeLease> {
        let permit = self.requests.clone().try_acquire_owned().ok()?;
        let mut active = self.active.entry(node_id).or_insert(0);
        if *active >= usize::from(max) {
            return None;
        }
        *active += 1;
        drop(active);
        Some(NodeLease {
            node_id,
            capacity: self.clone(),
            permit: Some(permit),
        })
    }

    pub(crate) async fn acquire(&self, node_id: Uuid, max: u16) -> ApiResult<NodeLease> {
        if max == 0 {
            return Err(AppError::unavailable("node request capacity is disabled"));
        }
        loop {
            let changed = self.changed();
            if let Some(lease) = self.try_acquire(node_id, max) {
                return Ok(lease);
            }
            changed.await;
        }
    }

    pub(crate) async fn acquire_probe(&self, node_id: Uuid, max: u16) -> ApiResult<ProbeLease> {
        let probe = self
            .probes
            .clone()
            .acquire_owned()
            .await
            .map_err(AppError::internal)?;
        let node = self.acquire(node_id, max).await?;
        Ok(ProbeLease {
            _probe: probe,
            _node: node,
        })
    }

    pub(crate) fn changed(&self) -> impl Future<Output = ()> + Send + use<> {
        let notified = self.changed.clone().notified_owned();
        async move {
            tokio::select! {
                () = notified => {},
                () = tokio::time::sleep(Duration::from_millis(250)) => {},
            }
        }
    }
}

pub(crate) struct NodeLease {
    node_id: Uuid,
    capacity: NodeCapacity,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for NodeLease {
    fn drop(&mut self) {
        if let Some(mut active) = self.capacity.active.get_mut(&self.node_id) {
            *active = active.saturating_sub(1);
        }
        drop(self.permit.take());
        self.capacity.changed.notify_waiters();
    }
}

pub(crate) struct ProbeLease {
    _probe: OwnedSemaphorePermit,
    _node: NodeLease,
}

#[derive(Clone)]
pub(crate) struct StreamMemoryBudget {
    local: Arc<Semaphore>,
    global: Arc<Semaphore>,
}

impl StreamMemoryBudget {
    pub(crate) fn new(global: Arc<Semaphore>) -> Self {
        Self {
            local: Arc::new(Semaphore::new(STREAM_MEMORY_BYTES as usize)),
            global,
        }
    }

    pub(crate) async fn acquire(&self, bytes: u64) -> ApiResult<MemoryLease> {
        let bytes = u32::try_from(bytes)
            .ok()
            .filter(|bytes| *bytes <= STREAM_MEMORY_BYTES)
            .ok_or_else(|| AppError::Upstream("stream chunk exceeds its memory budget".into()))?;
        let local = self
            .local
            .clone()
            .acquire_many_owned(bytes)
            .await
            .map_err(AppError::internal)?;
        let global = self
            .global
            .clone()
            .acquire_many_owned(bytes)
            .await
            .map_err(AppError::internal)?;
        Ok(MemoryLease {
            _permits: Arc::new(MemoryPermits {
                _local: local,
                _global: global,
            }),
        })
    }
}

struct MemoryPermits {
    _local: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub(crate) struct MemoryLease {
    _permits: Arc<MemoryPermits>,
}

impl MemoryLease {
    pub(crate) fn attach(self, bytes: Bytes) -> BudgetedBytes {
        BudgetedBytes {
            bytes,
            _lease: self,
        }
    }
}

pub(crate) struct BudgetedBytes {
    bytes: Bytes,
    _lease: MemoryLease,
}

impl BudgetedBytes {
    pub(crate) fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub(crate) fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    #[tokio::test]
    async fn clones_share_node_capacity_and_cancelled_probe_releases_its_slot() {
        let capacity = NodeCapacity::default();
        let node_id = Uuid::new_v4();
        let lease = capacity.try_acquire(node_id, 1).unwrap();
        assert!(capacity.clone().try_acquire(node_id, 1).is_none());
        let mut pending = Box::pin(capacity.acquire_probe(node_id, 1));
        assert!(pending.as_mut().now_or_never().is_none());
        assert_eq!(capacity.probes.available_permits(), MAX_PROBES - 1);
        drop(pending);
        assert_eq!(capacity.probes.available_permits(), MAX_PROBES);
        drop(lease);
        assert!(capacity.try_acquire(node_id, 1).is_some());
    }

    #[tokio::test]
    async fn memory_lease_covers_buffered_results_and_releases_on_delivery() {
        let global = Arc::new(Semaphore::new(12));
        let first = StreamMemoryBudget::new(global.clone());
        let second = StreamMemoryBudget::new(global.clone());
        let bytes = first
            .acquire(8)
            .await
            .unwrap()
            .attach(Bytes::from_static(b"12345678"));
        let other = second.acquire(4).await.unwrap();
        assert_eq!(global.available_permits(), 0);
        assert!(first.acquire(1).now_or_never().is_none());
        assert_eq!(bytes.into_bytes().as_ref(), b"12345678");
        assert_eq!(global.available_permits(), 8);
        drop(other);
        assert_eq!(global.available_permits(), 12);
    }

    #[tokio::test]
    async fn cancelled_global_memory_wait_releases_local_reservation() {
        let global = Arc::new(Semaphore::new(1));
        let budget = StreamMemoryBudget::new(global.clone());
        let lease = global.clone().acquire_owned().await.unwrap();
        let mut pending = Box::pin(budget.acquire(1));
        assert!(pending.as_mut().now_or_never().is_none());
        assert_eq!(
            budget.local.available_permits(),
            STREAM_MEMORY_BYTES as usize - 1
        );
        drop(pending);
        assert_eq!(
            budget.local.available_permits(),
            STREAM_MEMORY_BYTES as usize
        );
        drop(lease);
        assert!(budget.acquire(1).await.is_ok());
    }

    #[tokio::test]
    async fn shared_reservation_lives_until_all_queued_frames_are_delivered() {
        let global = Arc::new(Semaphore::new(16));
        let budget = StreamMemoryBudget::new(global.clone());
        let lease = budget.acquire(16).await.unwrap();
        let first = lease.clone().attach(Bytes::from_static(b"first"));
        let second = lease.clone().attach(Bytes::from_static(b"second"));
        drop(lease);
        assert_eq!(global.available_permits(), 0);
        assert_eq!(first.into_bytes().as_ref(), b"first");
        assert_eq!(global.available_permits(), 0);
        assert_eq!(second.into_bytes().as_ref(), b"second");
        assert_eq!(global.available_permits(), 16);
    }

    #[tokio::test]
    async fn stream_and_cross_blob_memory_limits_are_shared_and_recoverable() {
        let global = Arc::new(Semaphore::new(GLOBAL_STREAM_MEMORY_BYTES as usize));
        let mut leases = Vec::new();
        for _ in 0..4 {
            let budget = StreamMemoryBudget::new(global.clone());
            leases.push(
                budget
                    .acquire(u64::from(STREAM_MEMORY_BYTES))
                    .await
                    .unwrap(),
            );
            assert!(budget.acquire(1).now_or_never().is_none());
        }
        assert_eq!(global.available_permits(), 0);
        let pending = StreamMemoryBudget::new(global.clone());
        assert!(pending.acquire(1).now_or_never().is_none());
        drop(leases.pop());
        assert_eq!(global.available_permits(), STREAM_MEMORY_BYTES as usize);
        assert!(
            pending
                .acquire(u64::from(STREAM_MEMORY_BYTES))
                .await
                .is_ok()
        );
        drop(leases);
        assert_eq!(
            global.available_permits(),
            GLOBAL_STREAM_MEMORY_BYTES as usize
        );
    }
}
