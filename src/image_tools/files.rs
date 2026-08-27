use std::path::{Component, Path, PathBuf};

use axum::{body::Body, extract::Request, http::header, response::Response};
use serde::Serialize;
use tower::ServiceExt;
use tower_http::services::ServeFile;

use crate::error::{ApiResult, AppError};

#[derive(Debug, Serialize)]
pub(super) struct FileEntry {
    path: String,
    name: String,
    kind: &'static str,
    size: u64,
}

pub(super) struct FileBrowser;

impl FileBrowser {
    pub(super) async fn list(root: &Path, relative: &str) -> ApiResult<Vec<FileEntry>> {
        let directory = safe_join(root, relative)?;
        let mut entries = Vec::new();
        let mut reader = tokio::fs::read_dir(&directory).await?;
        while let Some(entry) = reader.next_entry().await? {
            let metadata = tokio::fs::symlink_metadata(entry.path()).await?;
            let kind = if metadata.is_dir() {
                "directory"
            } else if metadata.file_type().is_symlink() {
                "symlink"
            } else {
                "file"
            };
            let relative = entry
                .path()
                .strip_prefix(root)
                .map_err(AppError::internal)?
                .to_string_lossy()
                .replace('\\', "/");
            entries.push(FileEntry {
                path: relative,
                name: entry.file_name().to_string_lossy().into_owned(),
                kind,
                size: metadata.len(),
            });
        }
        entries.sort_by(|left, right| left.kind.cmp(right.kind).then(left.name.cmp(&right.name)));
        Ok(entries)
    }

    pub(super) async fn serve_file(
        root: &Path,
        relative: &str,
        request: Request,
    ) -> ApiResult<Response> {
        let path = safe_join(root, relative)?;
        let metadata = tokio::fs::symlink_metadata(&path).await?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(AppError::bad_request(
                "only regular files can be downloaded",
            ));
        }
        serve_path(path, None, request).await
    }

    pub(super) async fn serve_artifact(
        path: PathBuf,
        name: Option<&str>,
        request: Request,
    ) -> ApiResult<Response> {
        serve_path(path, name, request).await
    }
}

async fn serve_path(path: PathBuf, name: Option<&str>, request: Request) -> ApiResult<Response> {
    let mut response = ServeFile::new(path)
        .oneshot(request)
        .await
        .map_err(AppError::internal)?
        .map(Body::new);
    if let Some(name) = name {
        let value = format!(
            "attachment; filename=\"{}\"",
            name.replace(['\"', '\r', '\n'], "_")
        );
        response.headers_mut().insert(
            header::CONTENT_DISPOSITION,
            value.parse().map_err(AppError::internal)?,
        );
    }
    Ok(response)
}

fn safe_join(root: &Path, relative: &str) -> ApiResult<PathBuf> {
    let relative = Path::new(relative);
    if relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(AppError::bad_request("invalid image file path"));
    }
    let path = root.join(relative);
    let canonical_root = std::fs::canonicalize(root).map_err(AppError::from)?;
    let canonical = std::fs::canonicalize(&path).map_err(AppError::from)?;
    if !canonical.starts_with(canonical_root) {
        return Err(AppError::Forbidden);
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_parent_traversal() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("safe.txt"), b"safe").unwrap();
        assert!(safe_join(directory.path(), "safe.txt").is_ok());
        assert!(safe_join(directory.path(), "../secret").is_err());
        assert!(safe_join(directory.path(), "/etc/passwd").is_err());
    }
}
