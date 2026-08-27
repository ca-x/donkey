use std::{
    collections::HashSet,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use uuid::Uuid;

use crate::error::{ApiResult, AppError};

#[derive(Clone)]
pub(super) struct ArtifactStore {
    root: Arc<PathBuf>,
}

pub(super) struct BlobWriteGuard {
    _gc_lock: std::fs::File,
    temporary: PathBuf,
    destination: PathBuf,
}

pub(super) struct JobWriteGuard {
    _lock: std::fs::File,
}

impl ArtifactStore {
    pub(super) async fn open(data_dir: &Path) -> ApiResult<Self> {
        let root = data_dir.join("image-tools");
        tokio::fs::create_dir_all(root.join("jobs")).await?;
        tokio::fs::create_dir_all(root.join("blobs/sha256")).await?;
        tokio::fs::create_dir_all(root.join("partials")).await?;
        Ok(Self {
            root: Arc::new(root),
        })
    }

    pub(super) fn job_root(&self, id: Uuid) -> PathBuf {
        self.root.join("jobs").join(id.to_string())
    }

    pub(super) async fn lock_job(&self, id: Uuid) -> ApiResult<JobWriteGuard> {
        let job_root = self.job_root(id);
        tokio::fs::create_dir_all(&job_root).await?;
        let lock = tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(job_root.join(".job.lock"))?;
            fs4::FileExt::lock(&file)?;
            Ok::<_, std::io::Error>(file)
        })
        .await
        .map_err(AppError::internal)??;
        Ok(JobWriteGuard { _lock: lock })
    }

    pub(super) fn shared_blob_path(&self, digest: &str) -> ApiResult<PathBuf> {
        let value = digest
            .strip_prefix("sha256:")
            .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .ok_or_else(|| AppError::bad_request("only sha256 image blobs are supported"))?;
        Ok(self.root.join("blobs/sha256").join(value))
    }

    pub(super) async fn begin_blob_write(
        &self,
        job_id: Uuid,
        digest: &str,
    ) -> ApiResult<BlobWriteGuard> {
        let destination = self.shared_blob_path(digest)?;
        let file_name = destination
            .file_name()
            .ok_or_else(|| AppError::bad_request("invalid shared Blob path"))?;
        let temporary = self
            .root
            .join("partials")
            .join(format!("{job_id}-{}", file_name.to_string_lossy()));
        let lock_path = self.root.join("gc.lock");
        let gc_lock = tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock_path)?;
            fs4::FileExt::lock_shared(&file)?;
            Ok::<_, std::io::Error>(file)
        })
        .await
        .map_err(AppError::internal)??;
        Ok(BlobWriteGuard {
            _gc_lock: gc_lock,
            temporary,
            destination,
        })
    }

    pub(super) async fn write_blob_bytes(
        &self,
        job_id: Uuid,
        digest: &str,
        bytes: &[u8],
    ) -> ApiResult<PathBuf> {
        let guard = self.begin_blob_write(job_id, digest).await?;
        tokio::fs::write(guard.temporary_path(), bytes).await?;
        guard.commit().await
    }

    pub(super) async fn remove_job(&self, id: Uuid) -> ApiResult<()> {
        match tokio::fs::remove_dir_all(self.job_root(id)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) async fn usage(&self) -> ApiResult<u64> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || storage_usage_sync(&root))
            .await
            .map_err(AppError::internal)?
            .map_err(AppError::from)
    }

    pub(super) async fn gc_shared_blobs(&self) -> ApiResult<()> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let lock = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(root.join("gc.lock"))?;
            fs4::FileExt::lock(&lock)?;
            gc_shared_blobs_sync(&root)?;
            cleanup_partials_sync(&root.join("partials"))
        })
        .await
        .map_err(AppError::internal)?
        .map_err(AppError::from)
    }
}

impl BlobWriteGuard {
    pub(super) fn temporary_path(&self) -> &Path {
        &self.temporary
    }

    pub(super) async fn commit(self) -> ApiResult<PathBuf> {
        if tokio::fs::metadata(&self.destination).await.is_ok() {
            let _ = tokio::fs::remove_file(&self.temporary).await;
            return Ok(self.destination.clone());
        }
        match tokio::fs::rename(&self.temporary, &self.destination).await {
            Ok(()) => Ok(self.destination.clone()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = tokio::fs::remove_file(&self.temporary).await;
                Ok(self.destination.clone())
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for BlobWriteGuard {
    fn drop(&mut self) {
        match std::fs::remove_file(&self.temporary) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(path = %self.temporary.display(), ?error, "failed to clean image Blob partial");
            }
        }
    }
}

fn storage_usage_sync(root: &Path) -> std::io::Result<u64> {
    fn visit(root: &Path, path: &Path, total: &mut u64) -> std::io::Result<()> {
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() {
            return Ok(());
        }
        if metadata.is_file() {
            let relative = path.strip_prefix(root).unwrap_or(path);
            let parts = relative
                .components()
                .filter_map(|part| match part {
                    Component::Normal(value) => value.to_str(),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let is_layout_blob = parts.len() >= 6
                && parts[0] == "jobs"
                && parts[2] == "layout"
                && parts[3] == "blobs"
                && parts[4] == "sha256";
            if !is_layout_blob {
                *total = total.saturating_add(metadata.len());
            }
            return Ok(());
        }
        if metadata.is_dir() {
            for entry in std::fs::read_dir(path)? {
                visit(root, &entry?.path(), total)?;
            }
        }
        Ok(())
    }

    if !root.exists() {
        return Ok(0);
    }
    let mut total = 0;
    visit(root, root, &mut total)?;
    Ok(total)
}

fn gc_shared_blobs_sync(root: &Path) -> std::io::Result<()> {
    let mut referenced = HashSet::new();
    let jobs = root.join("jobs");
    if let Ok(job_entries) = std::fs::read_dir(jobs) {
        for job in job_entries.flatten() {
            let blobs = job.path().join("layout/blobs/sha256");
            if let Ok(entries) = std::fs::read_dir(blobs) {
                for entry in entries.flatten() {
                    if entry.file_type().is_ok_and(|kind| kind.is_file()) {
                        referenced.insert(entry.file_name());
                    }
                }
            }
        }
    }

    let shared = root.join("blobs/sha256");
    if let Ok(entries) = std::fs::read_dir(shared) {
        for entry in entries.flatten() {
            if entry.file_type().is_ok_and(|kind| kind.is_file())
                && !referenced.contains(&entry.file_name())
            {
                match std::fs::remove_file(entry.path()) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
    }
    Ok(())
}

fn cleanup_partials_sync(partials: &Path) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(partials) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_counts_shared_blobs_once_and_gc_removes_orphans() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let shared = root.join("blobs/sha256");
        let layout = root.join("jobs/job/layout/blobs/sha256");
        std::fs::create_dir_all(&shared).unwrap();
        std::fs::create_dir_all(&layout).unwrap();
        std::fs::write(shared.join("kept"), b"1234").unwrap();
        std::fs::write(shared.join("orphan"), b"12345678").unwrap();
        std::fs::hard_link(shared.join("kept"), layout.join("kept")).unwrap();
        std::fs::write(root.join("jobs/job/export.tar"), b"123456").unwrap();

        assert_eq!(storage_usage_sync(root).unwrap(), 18);
        gc_shared_blobs_sync(root).unwrap();
        assert!(shared.join("kept").exists());
        assert!(!shared.join("orphan").exists());
        assert_eq!(storage_usage_sync(root).unwrap(), 10);
    }

    #[tokio::test]
    async fn gc_waits_for_active_blob_writer() {
        let directory = tempfile::tempdir().unwrap();
        let writer_store = ArtifactStore::open(directory.path()).await.unwrap();
        let gc_store = ArtifactStore::open(directory.path()).await.unwrap();
        let digest = format!("sha256:{}", "a".repeat(64));
        let guard = writer_store
            .begin_blob_write(Uuid::new_v4(), &digest)
            .await
            .unwrap();
        tokio::fs::write(guard.temporary_path(), b"in progress")
            .await
            .unwrap();

        let gc = tokio::spawn(async move { gc_store.gc_shared_blobs().await });
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert!(!gc.is_finished());

        drop(guard);
        gc.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn dropped_writer_cleans_partial() {
        let directory = tempfile::tempdir().unwrap();
        let store = ArtifactStore::open(directory.path()).await.unwrap();
        let digest = format!("sha256:{}", "b".repeat(64));
        let guard = store
            .begin_blob_write(Uuid::new_v4(), &digest)
            .await
            .unwrap();
        let partial = guard.temporary_path().to_owned();
        tokio::fs::write(&partial, b"failed download")
            .await
            .unwrap();
        drop(guard);
        assert!(!partial.exists());
    }

    #[tokio::test]
    async fn gc_removes_crash_residue_from_partials() {
        let directory = tempfile::tempdir().unwrap();
        let store = ArtifactStore::open(directory.path()).await.unwrap();
        let residue = directory
            .path()
            .join("image-tools/partials/crashed-download");
        tokio::fs::write(&residue, b"partial bytes").await.unwrap();

        store.gc_shared_blobs().await.unwrap();

        assert!(!residue.exists());
    }

    #[tokio::test]
    async fn job_lock_serializes_writers_across_store_instances() {
        let directory = tempfile::tempdir().unwrap();
        let first = ArtifactStore::open(directory.path()).await.unwrap();
        let second = ArtifactStore::open(directory.path()).await.unwrap();
        let job_id = Uuid::new_v4();
        let first_guard = first.lock_job(job_id).await.unwrap();
        let waiting = tokio::spawn(async move { second.lock_job(job_id).await });
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert!(!waiting.is_finished());
        drop(first_guard);
        drop(waiting.await.unwrap().unwrap());
    }
}
