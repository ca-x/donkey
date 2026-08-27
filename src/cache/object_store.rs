use std::{collections::HashSet, path::PathBuf, sync::Arc, time::SystemTime};

use dashmap::DashMap;

use crate::error::{ApiResult, AppError};

#[derive(Clone)]
pub(super) struct ObjectStore {
    root: Arc<PathBuf>,
}

pub(super) struct ObjectWriteGuard {
    _lock: std::fs::File,
}

pub(super) struct MaintenanceGuard {
    _lock: std::fs::File,
}

impl ObjectStore {
    pub(super) async fn open(
        data_dir: &std::path::Path,
        _partial_ttl: std::time::Duration,
    ) -> ApiResult<Self> {
        let root = data_dir.join("cache");
        tokio::fs::create_dir_all(root.join("objects")).await?;
        tokio::fs::create_dir_all(root.join("tmp")).await?;
        Ok(Self {
            root: Arc::new(root),
        })
    }

    pub(super) fn object_path(&self, key: &str) -> PathBuf {
        self.root.join("objects").join(&key[..2]).join(key)
    }

    pub(super) fn temp_dir(&self) -> PathBuf {
        self.root.join("tmp")
    }

    pub(super) async fn commit(&self, key: &str, temporary: &std::path::Path) -> ApiResult<u64> {
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
        Ok(tokio::fs::metadata(destination).await?.len())
    }

    pub(super) async fn remove(&self, key: &str) -> ApiResult<()> {
        match tokio::fs::remove_file(self.object_path(key)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) async fn remove_indexed_path(&self, path: &str) -> ApiResult<()> {
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) async fn prune_orphans(
        &self,
        partial_ttl: std::time::Duration,
        protected: &DashMap<String, ()>,
    ) {
        let _ = remove_orphan_files(&self.root.join("objects"), None, protected, false).await;
        let cutoff = SystemTime::now()
            .checked_sub(partial_ttl)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let _ = remove_orphan_files(&self.root.join("tmp"), Some(cutoff), protected, true).await;
    }

    pub(super) async fn remove_unindexed(&self, indexed: &HashSet<String>) -> ApiResult<()> {
        let root = self.root.join("objects");
        let mut pending = vec![root.clone()];
        while let Some(path) = pending.pop() {
            let mut entries = tokio::fs::read_dir(&path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let child = entry.path();
                let metadata = entry.metadata().await?;
                if metadata.is_dir() {
                    pending.push(child);
                } else {
                    let key = entry.file_name().to_string_lossy().into_owned();
                    if !indexed.contains(&key) {
                        let _ = tokio::fs::remove_file(child).await;
                    }
                }
            }
            if path != root {
                let _ = tokio::fs::remove_dir(&path).await;
            }
        }
        Ok(())
    }

    pub(super) async fn lock_maintenance(&self) -> ApiResult<MaintenanceGuard> {
        Ok(MaintenanceGuard {
            _lock: lock_file(self.root.join("maintenance.lock"), true).await?,
        })
    }

    pub(super) async fn try_lock_maintenance(&self) -> ApiResult<Option<MaintenanceGuard>> {
        let path = self.root.join("maintenance.lock");
        let lock = tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?;
            match fs4::FileExt::try_lock(&file) {
                Ok(()) => Ok(Some(file)),
                Err(fs4::TryLockError::WouldBlock) => Ok(None),
                Err(fs4::TryLockError::Error(error)) => Err(error),
            }
        })
        .await
        .map_err(AppError::internal)??;
        Ok(lock.map(|lock| MaintenanceGuard { _lock: lock }))
    }

    pub(super) async fn lock_writer(&self) -> ApiResult<ObjectWriteGuard> {
        Ok(ObjectWriteGuard {
            _lock: lock_file(self.root.join("maintenance.lock"), false).await?,
        })
    }

    /// Serialize cache admissions across processes without blocking ordinary
    /// readers or streams.  The in-memory capacity mutex only protects clones
    /// in one process; this file lock closes the check/evict/commit race when
    /// multiple Donkey instances share the same data directory.
    pub(super) async fn lock_capacity(&self) -> ApiResult<ObjectWriteGuard> {
        Ok(ObjectWriteGuard {
            _lock: lock_file(self.root.join("capacity.lock"), true).await?,
        })
    }

    pub(super) async fn cleanup_partials(&self, ttl: std::time::Duration) {
        cleanup_partial_files(&self.root.join("tmp"), ttl).await;
    }
}

async fn lock_file(path: PathBuf, exclusive: bool) -> ApiResult<std::fs::File> {
    tokio::task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        if exclusive {
            fs4::FileExt::lock(&file)?;
        } else {
            fs4::FileExt::lock_shared(&file)?;
        }
        Ok::<_, std::io::Error>(file)
    })
    .await
    .map_err(AppError::internal)?
    .map_err(AppError::from)
}

async fn remove_orphan_files(
    root: &std::path::Path,
    older_than: Option<SystemTime>,
    protected: &DashMap<String, ()>,
    protect_by_top_level: bool,
) -> std::io::Result<()> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        let mut entries = tokio::fs::read_dir(&path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let child = entry.path();
            let metadata = entry.metadata().await?;
            let protected_key = if protect_by_top_level {
                child
                    .strip_prefix(root)
                    .ok()
                    .and_then(|relative| relative.components().next())
                    .and_then(|component| match component {
                        std::path::Component::Normal(value) => value.to_str(),
                        _ => None,
                    })
                    .map(str::to_owned)
            } else {
                child
                    .file_name()
                    .and_then(|value| value.to_str())
                    .map(str::to_owned)
            };
            if protected_key.is_some_and(|key| protected.contains_key(&key)) {
                continue;
            }
            if metadata.is_dir() {
                pending.push(child);
            } else if older_than
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
    let cutoff = SystemTime::now()
        .checked_sub(ttl)
        .unwrap_or(SystemTime::UNIX_EPOCH);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn maintenance_waits_for_object_commit_index_window() {
        let directory = tempfile::tempdir().unwrap();
        let first = ObjectStore::open(directory.path(), std::time::Duration::ZERO)
            .await
            .unwrap();
        let second = ObjectStore::open(directory.path(), std::time::Duration::ZERO)
            .await
            .unwrap();
        let temporary = directory.path().join("incoming");
        tokio::fs::write(&temporary, b"payload").await.unwrap();
        let write_guard = first.lock_writer().await.unwrap();
        first.commit(&"a".repeat(64), &temporary).await.unwrap();
        let waiting = tokio::spawn(async move { second.lock_maintenance().await });
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert!(!waiting.is_finished());
        drop(write_guard);
        drop(waiting.await.unwrap().unwrap());
    }
}
