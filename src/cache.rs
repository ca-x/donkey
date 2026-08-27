use std::{path::PathBuf, sync::Arc};

use chrono::Utc;
use dashmap::DashMap;
use moka::future::Cache;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};

use crate::{
    config::{CachePolicy, Config},
    error::{ApiResult, AppError},
};

mod object_store;
mod repository;

use object_store::ObjectStore;
use repository::{CacheRecord, CacheRepository};

#[derive(Clone)]
pub struct CacheStore {
    objects: ObjectStore,
    repository: CacheRepository,
    flights: Cache<String, Arc<Mutex<()>>>,
    active_flights: Arc<DashMap<String, ()>>,
    runtime: Arc<RwLock<CacheRuntimeConfig>>,
    capacity_lock: Arc<Mutex<()>>,
}

#[derive(Clone, Copy)]
struct CacheRuntimeConfig {
    partial_ttl: std::time::Duration,
    cache_ttl: Option<std::time::Duration>,
    max_cache_bytes: u64,
    cache_policy: CachePolicy,
    cache_high_watermark: f64,
    cache_low_watermark: f64,
}

#[derive(Clone, Debug)]
pub struct CachedObject {
    pub key: String,
    pub path: PathBuf,
    pub size: u64,
    pub media_type: String,
    pub digest: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct CacheEntryView {
    pub key: String,
    pub media_type: String,
    pub size_bytes: i64,
    pub digest: Option<String>,
    pub hit_count: i64,
    pub created_at: chrono::DateTime<Utc>,
    pub last_accessed_at: chrono::DateTime<Utc>,
}

impl From<CacheRecord> for CacheEntryView {
    fn from(entry: CacheRecord) -> Self {
        Self {
            key: entry.key,
            media_type: entry.media_type,
            size_bytes: entry.size_bytes,
            digest: entry.digest,
            hit_count: entry.hit_count,
            created_at: entry.created_at,
            last_accessed_at: entry.last_accessed_at,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CacheStats {
    pub entries: usize,
    pub bytes: u64,
    pub hits: u64,
}

pub struct CacheLease {
    key: String,
    active_flights: Arc<DashMap<String, ()>>,
    _guard: OwnedMutexGuard<()>,
}

impl Drop for CacheLease {
    fn drop(&mut self) {
        self.active_flights.remove(&self.key);
    }
}

impl CacheStore {
    pub async fn new(config: Arc<Config>, db: sea_orm::DatabaseConnection) -> ApiResult<Self> {
        let objects = ObjectStore::open(&config.data_dir, config.partial_ttl).await?;
        Ok(Self {
            objects,
            repository: CacheRepository::new(db),
            flights: Cache::builder()
                .max_capacity(10_000)
                .time_to_idle(std::time::Duration::from_secs(60 * 60))
                .build(),
            active_flights: Arc::new(DashMap::new()),
            runtime: Arc::new(RwLock::new(CacheRuntimeConfig {
                partial_ttl: config.partial_ttl,
                cache_ttl: config.cache_ttl,
                max_cache_bytes: config.max_cache_bytes,
                cache_policy: config.cache_policy,
                cache_high_watermark: config.cache_high_watermark,
                cache_low_watermark: config.cache_low_watermark,
            })),
            capacity_lock: Arc::new(Mutex::new(())),
        })
    }

    pub async fn update_runtime(&self, config: &Config) {
        let mut runtime = self.runtime.write().await;
        runtime.partial_ttl = config.partial_ttl;
        runtime.cache_ttl = config.cache_ttl;
        runtime.max_cache_bytes = config.max_cache_bytes;
        runtime.cache_policy = config.cache_policy;
        runtime.cache_high_watermark = config.cache_high_watermark;
        runtime.cache_low_watermark = config.cache_low_watermark;
    }

    pub fn key(request_path: &str, authorization: Option<&str>) -> String {
        let mut hasher = Sha256::new();
        let public_identity = request_path
            .rsplit_once("/blobs/")
            .map(|(_, digest)| digest)
            .filter(|digest| {
                digest.starts_with("sha256:")
                    && digest.len() == 71
                    && digest[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
            })
            .unwrap_or(request_path);
        hasher.update(public_identity.as_bytes());
        hasher.update([0]);
        if let Some(value) = authorization {
            hasher.update(value.as_bytes());
        }
        hex::encode(hasher.finalize())
    }

    pub fn object_path(&self, key: &str) -> PathBuf {
        self.objects.object_path(key)
    }

    pub fn temp_dir(&self) -> PathBuf {
        self.objects.temp_dir()
    }

    pub async fn get(&self, key: &str) -> ApiResult<Option<CachedObject>> {
        let Some(entry) = self.repository.find(key).await? else {
            return Ok(None);
        };
        let path = PathBuf::from(&entry.path);
        let metadata = match tokio::fs::metadata(&path).await {
            Ok(metadata) if metadata.is_file() && metadata.len() == entry.size_bytes as u64 => {
                metadata
            }
            _ => {
                self.repository.delete(key).await?;
                return Ok(None);
            }
        };
        self.repository.record_hit(key).await?;
        Ok(Some(CachedObject {
            key: entry.key,
            path,
            size: metadata.len(),
            media_type: entry.media_type,
            digest: entry.digest,
        }))
    }

    pub async fn admit(
        &self,
        key: &str,
        temporary: &std::path::Path,
        media_type: &str,
        digest: Option<String>,
    ) -> ApiResult<CachedObject> {
        let incoming = tokio::fs::metadata(temporary).await?.len();
        let _capacity_guard = self.capacity_lock.lock().await;
        self.reserve_capacity_locked(incoming, key).await?;
        let size = self.objects.commit(key, temporary).await?;
        let destination = self.object_path(key);
        let now = Utc::now();
        self.repository
            .insert(CacheRecord {
                key: key.to_owned(),
                media_type: media_type.to_owned(),
                path: destination.to_string_lossy().into_owned(),
                size_bytes: size.min(i64::MAX as u64) as i64,
                digest: digest.clone(),
                hit_count: 0,
                created_at: now,
                last_accessed_at: now,
            })
            .await?;
        Ok(CachedObject {
            key: key.to_owned(),
            path: destination,
            size,
            media_type: media_type.to_owned(),
            digest,
        })
    }

    pub async fn lock(&self, key: &str) -> CacheLease {
        let lock = self
            .flights
            .get_with(key.to_owned(), async { Arc::new(Mutex::new(())) })
            .await;
        let key = key.to_owned();
        let guard = lock.lock_owned().await;
        self.active_flights.insert(key.clone(), ());
        CacheLease {
            key,
            active_flights: self.active_flights.clone(),
            _guard: guard,
        }
    }

    pub async fn list(&self, limit: u64) -> ApiResult<Vec<CacheEntryView>> {
        Ok(self
            .repository
            .list(limit.min(10_000))
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub async fn stats(&self) -> ApiResult<CacheStats> {
        let aggregate = self.repository.aggregate().await?;
        Ok(CacheStats {
            entries: aggregate.entries.min(usize::MAX as u64) as usize,
            bytes: aggregate.bytes,
            hits: aggregate.hits,
        })
    }

    pub async fn remove(&self, key: &str) -> ApiResult<()> {
        if key.len() != 64 || !key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(AppError::bad_request("invalid cache key"));
        }
        let _guard = self.lock(key).await;
        self.repository.discard_pending_hits(key);
        self.remove_locked(key).await
    }

    pub async fn clear_all(&self) -> ApiResult<u64> {
        let _capacity_guard = self.capacity_lock.lock().await;
        let entries = self.repository.all().await?;
        let mut freed = 0_u64;
        for entry in &entries {
            self.objects.remove_indexed_path(&entry.path).await?;
            freed = freed.saturating_add(entry.size_bytes.max(0) as u64);
        }
        self.repository.clear().await?;
        // Remove orphaned object files and stale partials that are not present
        // in the index. Active downloads remain protected by the partial TTL.
        let partial_ttl = self.runtime.read().await.partial_ttl;
        self.objects
            .prune_orphans(partial_ttl, &self.active_flights)
            .await;
        Ok(freed)
    }

    async fn remove_locked(&self, key: &str) -> ApiResult<()> {
        self.objects.remove(key).await?;
        self.repository.delete(key).await?;
        Ok(())
    }

    pub async fn cleanup_expired(&self) -> ApiResult<u64> {
        let ttl = self.runtime.read().await.cache_ttl;
        let Some(ttl) = ttl else {
            return Ok(0);
        };
        let cutoff = Utc::now() - chrono::Duration::from_std(ttl).map_err(AppError::internal)?;
        let mut freed = 0_u64;
        loop {
            let entries = self.repository.expired(cutoff, 512).await?;
            if entries.is_empty() {
                break;
            }
            let mut removed = false;
            for entry in entries {
                if self.active_flights.contains_key(&entry.key) {
                    continue;
                }
                self.remove(&entry.key).await?;
                freed = freed.saturating_add(entry.size_bytes.max(0) as u64);
                removed = true;
            }
            if !removed {
                break;
            }
        }
        Ok(freed)
    }

    async fn reserve_capacity_locked(&self, incoming: u64, protected_key: &str) -> ApiResult<()> {
        let runtime = *self.runtime.read().await;
        if incoming > runtime.max_cache_bytes {
            return Err(AppError::bad_request(
                "object exceeds the configured cache capacity",
            ));
        }
        let current = self.repository.aggregate().await?.bytes;
        let high = (runtime.max_cache_bytes as f64 * runtime.cache_high_watermark) as u64;
        if current.saturating_add(incoming) <= high {
            return Ok(());
        }
        let target = (runtime.max_cache_bytes as f64 * runtime.cache_low_watermark) as u64;
        let target_after_admission = target.max(incoming);
        let need_to_free = current
            .saturating_add(incoming)
            .saturating_sub(target_after_admission);
        let mut freed = 0_u64;
        let policy = runtime.cache_policy.to_string();
        while freed < need_to_free {
            let mut entries = self.repository.eviction_candidates(&policy, 512).await?;
            if entries.is_empty() {
                break;
            }
            let now = Utc::now();
            entries.sort_by(|a, b| {
                retention_score(a, now, runtime.cache_policy).total_cmp(&retention_score(
                    b,
                    now,
                    runtime.cache_policy,
                ))
            });
            let mut removed = false;
            for entry in entries {
                if entry.key == protected_key || self.active_flights.contains_key(&entry.key) {
                    continue;
                }
                let size = entry.size_bytes.max(0) as u64;
                self.remove(&entry.key).await?;
                freed = freed.saturating_add(size);
                removed = true;
                if freed >= need_to_free {
                    break;
                }
            }
            if !removed {
                break;
            }
        }
        if freed < need_to_free {
            return Err(AppError::bad_request(
                "cache is full and no safe eviction candidates are available",
            ));
        }
        Ok(())
    }
}

fn retention_score(entry: &CacheRecord, now: chrono::DateTime<Utc>, policy: CachePolicy) -> f64 {
    let age_hours = (now - entry.last_accessed_at).num_seconds().max(0) as f64 / 3600.0;
    let hits = entry.hit_count.max(0) as f64;
    let size_mib = entry.size_bytes.max(0) as f64 / (1024.0 * 1024.0);
    match policy {
        CachePolicy::Lru => -age_hours,
        CachePolicy::Lfu => hits.ln_1p() - age_hours * 0.001,
        CachePolicy::Balanced => {
            20.0 / (1.0 + age_hours / 24.0) + hits.ln_1p() * 3.0 - size_mib.ln_1p()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CacheStore;
    use crate::Config;
    use futures_util::future::try_join_all;
    use sea_orm::EntityTrait;
    use std::sync::Arc;

    #[test]
    fn authorization_scopes_cache_keys_without_storing_secret() {
        let anonymous = CacheStore::key("/v2/a/blobs/sha256:abc", None);
        let private = CacheStore::key("/v2/a/blobs/sha256:abc", Some("Bearer secret"));
        assert_ne!(anonymous, private);
        assert!(!private.contains("secret"));
    }

    #[tokio::test]
    async fn concurrent_admissions_stay_below_capacity() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.max_cache_bytes = 100;
        config.cache_high_watermark = 0.9;
        config.cache_low_watermark = 0.5;
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let cache = CacheStore::new(Arc::new(config), db).await.unwrap();
        let first = directory.path().join("first.partial");
        let second = directory.path().join("second.partial");
        tokio::fs::write(&first, vec![1_u8; 80]).await.unwrap();
        tokio::fs::write(&second, vec![2_u8; 80]).await.unwrap();
        let a = cache.clone();
        let b = cache.clone();
        let first_key = "a".repeat(64);
        let second_key = "b".repeat(64);
        let (first_result, second_result) = tokio::join!(
            a.admit(&first_key, &first, "application/octet-stream", None),
            b.admit(&second_key, &second, "application/octet-stream", None),
        );
        first_result.unwrap();
        second_result.unwrap();
        let stats = cache.stats().await.unwrap();
        assert!(stats.bytes <= 100, "cache used {} bytes", stats.bytes);
        assert_eq!(stats.entries, 1);
    }

    #[tokio::test]
    async fn concurrent_cache_hits_do_not_lose_increments() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config::for_test(directory.path().to_owned());
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let cache = CacheStore::new(Arc::new(config), db).await.unwrap();
        let temporary = directory.path().join("object.partial");
        tokio::fs::write(&temporary, b"payload").await.unwrap();
        let key = "c".repeat(64);
        cache
            .admit(&key, &temporary, "application/octet-stream", None)
            .await
            .unwrap();

        let hits = 64;
        let results = try_join_all((0..hits).map(|_| cache.get(&key)))
            .await
            .unwrap();
        assert!(results.into_iter().all(|result| result.is_some()));
        assert_eq!(cache.stats().await.unwrap().hits, hits);
    }

    #[tokio::test]
    async fn cache_hits_are_batched_until_stats_flushes_them() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config::for_test(directory.path().to_owned());
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let cache = CacheStore::new(Arc::new(config), db.clone()).await.unwrap();
        let temporary = directory.path().join("batched.partial");
        tokio::fs::write(&temporary, b"payload").await.unwrap();
        let key = "d".repeat(64);
        cache
            .admit(&key, &temporary, "application/octet-stream", None)
            .await
            .unwrap();

        for _ in 0..10 {
            assert!(cache.get(&key).await.unwrap().is_some());
        }
        let stored = crate::db::cache_entry::Entity::find_by_id(&key)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.hit_count, 0);
        assert_eq!(cache.stats().await.unwrap().hits, 10);
    }
}
