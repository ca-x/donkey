use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use sea_orm::{DatabaseConnection, EntityTrait};

use crate::{
    db::{self, cache_entry},
    error::ApiResult,
};

#[derive(Clone, Debug)]
pub(super) struct CacheRecord {
    pub(super) key: String,
    pub(super) media_type: String,
    pub(super) path: String,
    pub(super) size_bytes: i64,
    pub(super) digest: Option<String>,
    pub(super) hit_count: i64,
    pub(super) created_at: DateTime<Utc>,
    pub(super) last_accessed_at: DateTime<Utc>,
}

impl From<cache_entry::Model> for CacheRecord {
    fn from(entry: cache_entry::Model) -> Self {
        Self {
            key: entry.key,
            media_type: entry.media_type,
            path: entry.path,
            size_bytes: entry.size_bytes,
            digest: entry.digest,
            hit_count: entry.hit_count,
            created_at: entry.created_at,
            last_accessed_at: entry.last_accessed_at,
        }
    }
}

impl From<CacheRecord> for cache_entry::Model {
    fn from(entry: CacheRecord) -> Self {
        Self {
            key: entry.key,
            media_type: entry.media_type,
            path: entry.path,
            size_bytes: entry.size_bytes,
            digest: entry.digest,
            hit_count: entry.hit_count,
            created_at: entry.created_at,
            last_accessed_at: entry.last_accessed_at,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct CacheAggregate {
    pub(super) entries: u64,
    pub(super) bytes: u64,
    pub(super) hits: u64,
}

#[derive(Clone)]
pub(super) struct CacheRepository {
    db: DatabaseConnection,
    pending_hits: Arc<DashMap<String, u64>>,
}

impl CacheRepository {
    pub(super) fn new(db: DatabaseConnection) -> Self {
        Self {
            db,
            pending_hits: Arc::new(DashMap::new()),
        }
    }

    pub(super) async fn find(&self, key: &str) -> ApiResult<Option<CacheRecord>> {
        Ok(cache_entry::Entity::find_by_id(key)
            .one(&self.db)
            .await?
            .map(Into::into))
    }

    pub(super) async fn insert(&self, record: CacheRecord) -> ApiResult<()> {
        db::insert_cache_entry(&self.db, record.into()).await?;
        Ok(())
    }

    pub(super) async fn delete(&self, key: &str) -> ApiResult<()> {
        db::delete_cache_entry(&self.db, key).await?;
        Ok(())
    }

    pub(super) async fn list(&self, limit: u64) -> ApiResult<Vec<CacheRecord>> {
        self.flush_hits().await?;
        Ok(db::list_cache_entries(&self.db, limit)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub(super) async fn all(&self) -> ApiResult<Vec<CacheRecord>> {
        Ok(db::all_cache_entries(&self.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub(super) async fn aggregate(&self) -> ApiResult<CacheAggregate> {
        self.flush_hits().await?;
        let aggregate = db::cache_aggregate(&self.db).await?;
        Ok(CacheAggregate {
            entries: aggregate.entries,
            bytes: aggregate.bytes,
            hits: aggregate.hits,
        })
    }

    pub(super) async fn expired(
        &self,
        cutoff: DateTime<Utc>,
        limit: u64,
    ) -> ApiResult<Vec<CacheRecord>> {
        Ok(db::expired_cache_candidates(&self.db, cutoff, limit)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub(super) async fn eviction_candidates(
        &self,
        policy: &str,
        limit: u64,
    ) -> ApiResult<Vec<CacheRecord>> {
        Ok(db::cache_eviction_candidates(&self.db, policy, limit)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub(super) async fn record_hit(&self, key: &str) -> ApiResult<()> {
        const FLUSH_THRESHOLD: u64 = 32;
        let pending = {
            let mut count = self.pending_hits.entry(key.to_owned()).or_insert(0);
            *count = count.saturating_add(1);
            if *count >= FLUSH_THRESHOLD {
                let pending = *count;
                *count = 0;
                Some(pending)
            } else {
                None
            }
        };
        if let Some(pending) = pending
            && let Err(error) = db::add_cache_hits(&self.db, key, pending).await
        {
            self.pending_hits
                .entry(key.to_owned())
                .and_modify(|count| *count = count.saturating_add(pending))
                .or_insert(pending);
            return Err(error.into());
        }
        Ok(())
    }

    pub(super) fn discard_pending_hits(&self, key: &str) {
        self.pending_hits.remove(key);
    }

    pub(super) async fn flush_hits(&self) -> ApiResult<()> {
        let keys = self
            .pending_hits
            .iter()
            .filter_map(|entry| (*entry.value() > 0).then(|| entry.key().clone()))
            .collect::<Vec<_>>();
        for key in keys {
            if let Some((_, count)) = self.pending_hits.remove(&key)
                && count > 0
                && let Err(error) = db::add_cache_hits(&self.db, &key, count).await
            {
                self.pending_hits
                    .entry(key)
                    .and_modify(|pending| *pending = pending.saturating_add(count))
                    .or_insert(count);
                return Err(error.into());
            }
        }
        Ok(())
    }
}
