use std::{path::PathBuf, sync::Arc, time::SystemTime};

use dashmap::DashMap;

use crate::error::ApiResult;

#[derive(Clone)]
pub(super) struct ObjectStore {
    root: Arc<PathBuf>,
}

impl ObjectStore {
    pub(super) async fn open(
        data_dir: &std::path::Path,
        partial_ttl: std::time::Duration,
    ) -> ApiResult<Self> {
        let root = data_dir.join("cache");
        tokio::fs::create_dir_all(root.join("objects")).await?;
        tokio::fs::create_dir_all(root.join("tmp")).await?;
        cleanup_partial_files(&root.join("tmp"), partial_ttl).await;
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
        let _ = remove_orphan_files(&self.root.join("objects"), None, protected).await;
        let cutoff = SystemTime::now()
            .checked_sub(partial_ttl)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let _ = remove_orphan_files(&self.root.join("tmp"), Some(cutoff), protected).await;
    }
}

async fn remove_orphan_files(
    root: &std::path::Path,
    older_than: Option<SystemTime>,
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
