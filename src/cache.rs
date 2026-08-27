use std::{path::PathBuf, sync::Arc};

use chrono::Utc;
use dashmap::DashMap;
use moka::future::Cache;
use sea_orm::{DatabaseConnection, EntityTrait};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};

use crate::{
    config::{CachePolicy, Config},
    db::{self, cache_entry},
    error::{ApiResult, AppError},
};

#[derive(Clone)]
pub struct CacheStore {
    root: Arc<PathBuf>,
    db: DatabaseConnection,
    flights: Cache<String, Arc<Mutex<()>>>,
    active_flights: Arc<DashMap<String, ()>>,
    pending_hits: Arc<DashMap<String, u64>>,
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

impl From<cache_entry::Model> for CacheEntryView {
    fn from(entry: cache_entry::Model) -> Self {
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
    pub async fn new(config: Arc<Config>, db: DatabaseConnection) -> ApiResult<Self> {
        let root = config.data_dir.join("cache");
        tokio::fs::create_dir_all(root.join("objects")).await?;
        tokio::fs::create_dir_all(root.join("tmp")).await?;
        cleanup_partial_files(&root.join("tmp"), config.partial_ttl).await;
        Ok(Self {
            root: Arc::new(root),
            db,
            flights: Cache::builder()
                .max_capacity(10_000)
                .time_to_idle(std::time::Duration::from_secs(60 * 60))
                .build(),
            active_flights: Arc::new(DashMap::new()),
            pending_hits: Arc::new(DashMap::new()),
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
        self.root.join("objects").join(&key[..2]).join(key)
    }

    pub fn temp_dir(&self) -> PathBuf {
        self.root.join("tmp")
    }

    pub async fn get(&self, key: &str) -> ApiResult<Option<CachedObject>> {
        let Some(entry) = db::cache_entry::Entity::find_by_id(key)
            .one(&self.db)
            .await?
        else {
            return Ok(None);
        };
        let path = PathBuf::from(&entry.path);
        let metadata = match tokio::fs::metadata(&path).await {
            Ok(metadata) if metadata.is_file() && metadata.len() == entry.size_bytes as u64 => {
                metadata
            }
            _ => {
                db::delete_cache_entry(&self.db, key).await?;
                return Ok(None);
            }
        };
        self.record_hit(key).await?;
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
        let destination = self.object_path(key);
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        match tokio::fs::rename(temporary, &destination).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = tokio::fs::remove_file(temporary).await;
            }
            Err(error) => return Err(error.into()),
        }
        let size = tokio::fs::metadata(&destination).await?.len();
        let now = Utc::now();
        db::insert_cache_entry(
            &self.db,
            cache_entry::Model {
                key: key.to_owned(),
                media_type: media_type.to_owned(),
                path: destination.to_string_lossy().into_owned(),
                size_bytes: size.min(i64::MAX as u64) as i64,
                digest: digest.clone(),
                hit_count: 0,
                created_at: now,
                last_accessed_at: now,
            },
        )
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

    pub async fn list(&self, limit: u64) -> ApiResult<Vec<cache_entry::Model>> {
        self.flush_hits().await?;
        db::list_cache_entries(&self.db, limit.min(10_000))
            .await
            .map_err(Into::into)
    }

    pub async fn stats(&self) -> ApiResult<CacheStats> {
        self.flush_hits().await?;
        let aggregate = db::cache_aggregate(&self.db).await?;
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
        self.pending_hits.remove(key);
        self.remove_locked(key).await
    }

    pub async fn clear_all(&self) -> ApiResult<u64> {
        let _capacity_guard = self.capacity_lock.lock().await;
        self.pending_hits.clear();
        let entries = db::all_cache_entries(&self.db).await?;
        let mut freed = 0_u64;
        for entry in &entries {
            match tokio::fs::remove_file(&entry.path).await {
                Ok(()) => freed = freed.saturating_add(entry.size_bytes.max(0) as u64),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        db::clear_cache_entries(&self.db).await?;
        // Remove orphaned object files and stale partials that are not present
        // in the index. Active downloads remain protected by the partial TTL.
        let _ = remove_orphan_files(&self.root.join("objects"), None, &self.active_flights).await;
        let partial_ttl = self.runtime.read().await.partial_ttl;
        let cutoff = std::time::SystemTime::now()
            .checked_sub(partial_ttl)
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let _ =
            remove_orphan_files(&self.root.join("tmp"), Some(cutoff), &self.active_flights).await;
        Ok(freed)
    }

    async fn record_hit(&self, key: &str) -> ApiResult<()> {
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

    async fn flush_hits(&self) -> ApiResult<()> {
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

    async fn remove_locked(&self, key: &str) -> ApiResult<()> {
        let path = self.object_path(key);
        match tokio::fs::remove_file(path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        db::delete_cache_entry(&self.db, key).await?;
        Ok(())
    }

    pub async fn cleanup_expired(&self) -> ApiResult<u64> {
        let ttl = self.runtime.read().await.cache_ttl;
        let Some(ttl) = ttl else {
            return Ok(0);
        };
        let cutoff = Utc::now() - chrono::Duration::from_std(ttl).map_err(AppError::internal)?;
        let entries = db::all_cache_entries(&self.db).await?;
        let mut freed = 0_u64;
        for entry in entries
            .into_iter()
            .filter(|entry| entry.last_accessed_at < cutoff)
        {
            if self.active_flights.contains_key(&entry.key) {
                continue;
            }
            self.remove(&entry.key).await?;
            freed = freed.saturating_add(entry.size_bytes.max(0) as u64);
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
        let mut entries = db::all_cache_entries(&self.db).await?;
        let current = entries
            .iter()
            .map(|entry| entry.size_bytes.max(0) as u64)
            .sum::<u64>();
        let high = (runtime.max_cache_bytes as f64 * runtime.cache_high_watermark) as u64;
        if current.saturating_add(incoming) <= high {
            return Ok(());
        }
        let target = (runtime.max_cache_bytes as f64 * runtime.cache_low_watermark) as u64;
        let target_after_admission = target.max(incoming);
        let need_to_free = current
            .saturating_add(incoming)
            .saturating_sub(target_after_admission);
        let now = Utc::now();
        entries.sort_by(|a, b| {
            retention_score(a, now, runtime.cache_policy).total_cmp(&retention_score(
                b,
                now,
                runtime.cache_policy,
            ))
        });
        let mut freed = 0_u64;
        for entry in entries {
            if entry.key == protected_key || self.active_flights.contains_key(&entry.key) {
                continue;
            }
            let size = entry.size_bytes.max(0) as u64;
            self.remove(&entry.key).await?;
            freed = freed.saturating_add(size);
            if freed >= need_to_free {
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

async fn remove_orphan_files(
    root: &std::path::Path,
    older_than: Option<std::time::SystemTime>,
    protected: &DashMap<String, ()>,
) -> std::io::Result<()> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        let mut entries = tokio::fs::read_dir(&path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let child = entry.path();
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                pending.push(child);
            } else if !protected.contains_key(
                &child
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string(),
            ) && older_than
                .is_none_or(|cutoff| metadata.modified().is_ok_and(|modified| modified < cutoff))
            {
                let _ = tokio::fs::remove_file(child).await;
            }
        }
        if path != root {
            let _ = tokio::fs::remove_dir(&path).await;
        }
    }
    Ok(())
}

async fn cleanup_partial_files(root: &std::path::Path, ttl: std::time::Duration) {
    let Ok(mut entries) = tokio::fs::read_dir(root).await else {
        return;
    };
    let cutoff = std::time::SystemTime::now()
        .checked_sub(ttl)
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        let Ok(metadata) = entry.metadata().await else {
            continue;
        };
        if metadata.is_dir() {
            let stale = metadata
                .modified()
                .ok()
                .is_some_and(|modified| modified < cutoff);
            if stale {
                let _ = tokio::fs::remove_dir_all(path).await;
            }
        }
    }
}

fn retention_score(
    entry: &cache_entry::Model,
    now: chrono::DateTime<Utc>,
    policy: CachePolicy,
) -> f64 {
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
