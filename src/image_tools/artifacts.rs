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

impl ArtifactStore {
    pub(super) async fn open(data_dir: &Path) -> ApiResult<Self> {
        let root = data_dir.join("image-tools");
        tokio::fs::create_dir_all(root.join("jobs")).await?;
        tokio::fs::create_dir_all(root.join("blobs/sha256")).await?;
        Ok(Self {
            root: Arc::new(root),
        })
    }

    pub(super) fn job_root(&self, id: Uuid) -> PathBuf {
        self.root.join("jobs").join(id.to_string())
    }

    pub(super) fn shared_blob_path(&self, digest: &str) -> ApiResult<PathBuf> {
        let value = digest
            .strip_prefix("sha256:")
            .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .ok_or_else(|| AppError::bad_request("only sha256 image blobs are supported"))?;
        Ok(self.root.join("blobs/sha256").join(value))
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
        tokio::task::spawn_blocking(move || gc_shared_blobs_sync(&root))
            .await
            .map_err(AppError::internal)?
            .map_err(AppError::from)
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
}
