use std::{
    collections::HashSet,
    path::{Component, Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path as AxumPath, Query, Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::Response,
    routing::{get, post, put},
};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use futures_util::StreamExt;
use oci_client::{
    Client, Reference, RegistryOperation,
    client::ClientConfig,
    errors::OciDistributionError,
    manifest::{OciImageManifest, OciManifest},
    secrets::RegistryAuth,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, QuerySelect, Set,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    fs::File,
    io::AsyncWriteExt,
    sync::{Mutex, Notify},
};
use tokio_util::io::ReaderStream;
use tower::ServiceExt;
use tower_http::services::ServeFile;
use url::Url;
use uuid::Uuid;

use crate::{
    config::Config,
    crypto::CredentialCipher,
    db::{image_job, image_sync_rule, registry_credential},
    error::{ApiResult, AppError},
    nodes::{NodeService, NodeView},
    upstream::{RangeMode, UpstreamService},
};

#[derive(Clone)]
pub struct ImageTools {
    config: Arc<Config>,
    db: DatabaseConnection,
    nodes: NodeService,
    upstream: UpstreamService,
    cipher: CredentialCipher,
    root: Arc<PathBuf>,
    wake: Arc<Notify>,
    last_cleanup: Arc<Mutex<Instant>>,
}

#[derive(Debug, Deserialize)]
pub struct CredentialInput {
    pub name: String,
    pub registry: String,
    pub auth_mode: String,
    pub username: Option<String>,
    pub secret: String,
}

#[derive(Debug, Serialize)]
pub struct CredentialView {
    pub id: Uuid,
    pub name: String,
    pub registry: String,
    pub auth_mode: String,
    pub username: Option<String>,
    pub credential_configured: bool,
    pub generation: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobInput {
    pub kind: String,
    pub source_ref: String,
    pub source_node_id: Option<Uuid>,
    pub source_credential_id: Option<Uuid>,
    pub destination_ref: Option<String>,
    pub destination_credential_id: Option<Uuid>,
    #[serde(default = "default_linux")]
    pub platform_os: String,
    #[serde(default = "default_amd64")]
    pub platform_arch: String,
    pub output_format: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SyncRuleInput {
    pub name: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub source_ref: String,
    pub source_node_id: Option<Uuid>,
    pub source_credential_id: Option<Uuid>,
    pub destination_ref: String,
    pub destination_credential_id: Uuid,
    #[serde(default = "default_linux")]
    pub platform_os: String,
    #[serde(default = "default_amd64")]
    pub platform_arch: String,
    pub cron: String,
    #[serde(default = "default_timezone")]
    pub timezone: String,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    #[serde(default = "default_limit")]
    limit: u64,
}

#[derive(Debug, Deserialize)]
struct FileQuery {
    #[serde(default)]
    path: String,
}

#[derive(Debug, Serialize)]
struct FileEntry {
    path: String,
    name: String,
    kind: &'static str,
    size: u64,
}

fn default_true() -> bool {
    true
}
fn default_linux() -> String {
    "linux".into()
}
fn default_amd64() -> String {
    "amd64".into()
}
fn default_timezone() -> String {
    "UTC".into()
}
fn default_limit() -> u64 {
    100
}

impl From<registry_credential::Model> for CredentialView {
    fn from(value: registry_credential::Model) -> Self {
        Self {
            id: value.id,
            name: value.name,
            registry: value.registry,
            auth_mode: value.auth_mode,
            username: value.username,
            credential_configured: !value.secret_enc.is_empty(),
            generation: value.generation,
            updated_at: value.updated_at,
        }
    }
}

impl ImageTools {
    pub async fn new(
        config: Arc<Config>,
        db: DatabaseConnection,
        nodes: NodeService,
    ) -> ApiResult<Self> {
        let root = config.data_dir.join("image-tools");
        tokio::fs::create_dir_all(root.join("jobs")).await?;
        tokio::fs::create_dir_all(root.join("blobs/sha256")).await?;
        let service = Self {
            cipher: CredentialCipher::from_config(&config)?,
            upstream: UpstreamService::new(config.clone(), nodes.clone()),
            config,
            db,
            nodes,
            root: Arc::new(root),
            wake: Arc::new(Notify::new()),
            last_cleanup: Arc::new(Mutex::new(Instant::now() - Duration::from_secs(60))),
        };
        image_job::Entity::update_many()
            .col_expr(
                image_job::Column::Status,
                sea_orm::sea_query::Expr::value("pending"),
            )
            .col_expr(
                image_job::Column::Stage,
                sea_orm::sea_query::Expr::value("recovered"),
            )
            .filter(image_job::Column::Status.eq("running"))
            .exec(&service.db)
            .await?;
        Ok(service)
    }

    pub fn router(self) -> Router {
        let router = Router::new()
            .route(
                "/credentials",
                get(list_credentials).post(create_credential),
            )
            .route(
                "/credentials/{id}",
                put(update_credential).delete(delete_credential),
            )
            .route("/jobs", get(list_jobs).post(create_job))
            .route("/jobs/{id}", get(get_job).delete(cancel_job))
            .route("/jobs/{id}/retry", post(retry_job))
            .route("/jobs/{id}/artifact", get(download_artifact))
            .route("/jobs/{id}/files", get(list_files))
            .route("/jobs/{id}/file", get(download_file))
            .route("/sync-rules", get(list_rules).post(create_rule))
            .route("/sync-rules/{id}", put(update_rule).delete(delete_rule))
            .route("/sync-rules/{id}/run", post(run_rule));
        router.with_state(self)
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
            loop {
                if let Err(error) = self.tick().await {
                    tracing::error!(?error, "image tools worker tick failed");
                }
                tokio::select! {
                    _ = self.wake.notified() => {},
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {},
                }
            }
        });
    }

    async fn tick(&self) -> ApiResult<()> {
        self.cleanup_storage(None).await?;
        self.enqueue_due_rules().await?;
        let Some(job) = image_job::Entity::find()
            .filter(image_job::Column::Status.eq("pending"))
            .order_by_asc(image_job::Column::CreatedAt)
            .one(&self.db)
            .await?
        else {
            return Ok(());
        };
        let mut active = job.clone().into_active_model();
        active.status = Set("running".into());
        active.stage = Set("resolving".into());
        active.started_at = Set(Some(Utc::now()));
        active.lease_until = Set(Some(Utc::now() + chrono::Duration::minutes(10)));
        active.updated_at = Set(Utc::now());
        active.update(&self.db).await?;

        let result = self.process_job(&job).await;
        let current = image_job::Entity::find_by_id(job.id)
            .one(&self.db)
            .await?
            .ok_or_else(|| AppError::not_found("image job"))?;
        let was_cancelled = current.cancel_requested;
        let mut active = current.into_active_model();
        active.lease_until = Set(None);
        active.finished_at = Set(Some(Utc::now()));
        active.updated_at = Set(Utc::now());
        match result {
            _ if was_cancelled => {
                active.status = Set("cancelled".into());
                active.stage = Set("cancelled".into());
                active.error = Set(None);
            }
            Ok(outcome) => {
                let value = match outcome {
                    JobOutcome::Completed => "completed",
                    JobOutcome::Skipped => "skipped",
                };
                active.status = Set(value.into());
                active.stage = Set(value.into());
                active.error = Set(None);
            }
            Err(error) => {
                active.status = Set("failed".into());
                active.stage = Set("failed".into());
                active.error = Set(Some(safe_error(&error)));
            }
        }
        active.update(&self.db).await?;
        Ok(())
    }

    async fn process_job(&self, job: &image_job::Model) -> ApiResult<JobOutcome> {
        let prepared = self.prepare_image(job).await?;
        self.update_job_manifest(job.id, &prepared).await?;
        self.check_cancelled(job.id).await?;
        if job.kind == "copy"
            && self
                .copy_already_completed(job, &prepared.manifest_digest)
                .await?
        {
            return Ok(JobOutcome::Skipped);
        }
        match job.kind.as_str() {
            "export" => self.export_image(job, &prepared).await?,
            "extract" => self.extract_image(job, &prepared).await?,
            "copy" => self.copy_image(job, &prepared).await?,
            _ => return Err(AppError::bad_request("unsupported image job kind")),
        }
        self.enforce_storage_quota(job.id).await?;
        Ok(JobOutcome::Completed)
    }

    async fn prepare_image(&self, job: &image_job::Model) -> ApiResult<PreparedImage> {
        let logical: Reference = job
            .source_ref
            .parse()
            .map_err(|_| AppError::bad_request("invalid source image reference"))?;
        if let Some(node_id) = job.source_node_id {
            return self.prepare_image_via_node(job, &logical, node_id).await;
        }
        let (source, auth) = self.source_transport(&logical, job).await?;
        let client = image_client(&job.platform_os, &job.platform_arch);
        let (manifest, digest, config_json, index_digest) = client
            .pull_manifest_and_config_and_list_digest(&source, &auth)
            .await
            .map_err(oci_error)?;
        let total_bytes = manifest
            .layers
            .iter()
            .map(|layer| layer.size.max(0) as u64)
            .sum::<u64>()
            .saturating_add(manifest.config.size.max(0) as u64);
        if total_bytes > self.config.max_export_bytes {
            return Err(AppError::bad_request(
                "image exceeds DONKEY_MAX_EXPORT_BYTES",
            ));
        }

        let job_root = self.root.join("jobs").join(job.id.to_string());
        let layout = job_root.join("layout");
        tokio::fs::create_dir_all(layout.join("blobs/sha256")).await?;
        let config_path = blob_path(&layout, &manifest.config.digest)?;
        let shared_config = self.shared_blob_path(&manifest.config.digest)?;
        if tokio::fs::metadata(&shared_config).await.is_err() {
            atomic_write(&shared_config, config_json.as_bytes()).await?;
        }
        link_or_copy(&shared_config, &config_path).await?;

        self.set_progress(job.id, "downloading", 0, total_bytes)
            .await?;
        let mut downloaded = manifest.config.size.max(0) as u64;
        let mut layer_paths = Vec::new();
        for layer in &manifest.layers {
            self.check_cancelled(job.id).await?;
            let path = blob_path(&layout, &layer.digest)?;
            let shared = self.shared_blob_path(&layer.digest)?;
            if tokio::fs::metadata(&shared).await.is_err() {
                let temp = shared.with_extension("partial");
                client
                    .pull_blob(&source, layer, File::create(&temp).await?)
                    .await
                    .map_err(oci_error)?;
                tokio::fs::rename(&temp, &shared).await?;
            }
            link_or_copy(&shared, &path).await?;
            downloaded = downloaded.saturating_add(layer.size.max(0) as u64);
            self.set_progress(job.id, "downloading", downloaded, total_bytes)
                .await?;
            layer_paths.push(path);
        }
        write_layout(&layout, &manifest).await?;
        Ok(PreparedImage {
            manifest,
            manifest_digest: digest,
            index_digest,
            config_json,
            layout,
            layer_paths,
            total_bytes,
        })
    }

    async fn prepare_image_via_node(
        &self,
        job: &image_job::Model,
        logical: &Reference,
        node_id: Uuid,
    ) -> ApiResult<PreparedImage> {
        let node = self.nodes.registry_node(node_id).await?;
        let reference = logical
            .digest()
            .or_else(|| logical.tag())
            .unwrap_or("latest");
        let manifest_path = format!("/v2/{}/manifests/{reference}", logical.repository());
        let (headers, bytes) = self
            .node_bytes(&node, &manifest_path, manifest_accept(), 16 * 1024 * 1024)
            .await?;
        let top_digest = verified_digest(&bytes, digest_header(&headers), logical.digest())?;
        let top_manifest: OciManifest = serde_json::from_slice(&bytes)
            .map_err(|error| AppError::Upstream(format!("invalid image manifest: {error}")))?;

        let (manifest, manifest_digest, index_digest) = match top_manifest {
            OciManifest::Image(manifest) => (manifest, top_digest, None),
            OciManifest::ImageIndex(index) => {
                let descriptor = index
                    .manifests
                    .iter()
                    .find(|entry| {
                        entry.platform.as_ref().is_some_and(|platform| {
                            platform.os.to_string() == job.platform_os
                                && platform.architecture.to_string() == job.platform_arch
                        })
                    })
                    .ok_or_else(|| {
                        AppError::bad_request("requested platform is absent from image index")
                    })?;
                let path = format!(
                    "/v2/{}/manifests/{}",
                    logical.repository(),
                    descriptor.digest
                );
                let (headers, bytes) = self
                    .node_bytes(&node, &path, manifest_accept(), 16 * 1024 * 1024)
                    .await?;
                let digest =
                    verified_digest(&bytes, digest_header(&headers), Some(&descriptor.digest))?;
                let manifest =
                    match serde_json::from_slice::<OciManifest>(&bytes).map_err(|error| {
                        AppError::Upstream(format!("invalid platform manifest: {error}"))
                    })? {
                        OciManifest::Image(manifest) => manifest,
                        OciManifest::ImageIndex(_) => {
                            return Err(AppError::Upstream(
                                "nested image indexes are not supported".into(),
                            ));
                        }
                    };
                (manifest, digest, Some(top_digest))
            }
        };

        let total_bytes = manifest
            .layers
            .iter()
            .map(|layer| layer.size.max(0) as u64)
            .sum::<u64>()
            .saturating_add(manifest.config.size.max(0) as u64);
        if total_bytes > self.config.max_export_bytes {
            return Err(AppError::bad_request(
                "image exceeds DONKEY_MAX_EXPORT_BYTES",
            ));
        }

        let config_path = format!(
            "/v2/{}/blobs/{}",
            logical.repository(),
            manifest.config.digest
        );
        let config_limit = (manifest.config.size.max(0) as u64)
            .saturating_add(1)
            .min(64 * 1024 * 1024) as usize;
        let (_, config_bytes) = self
            .node_bytes(
                &node,
                &config_path,
                "application/octet-stream",
                config_limit,
            )
            .await?;
        verified_digest(&config_bytes, None, Some(&manifest.config.digest))?;
        if config_bytes.len() as i64 != manifest.config.size {
            return Err(AppError::Integrity);
        }
        let config_json = String::from_utf8(config_bytes)
            .map_err(|_| AppError::Upstream("image config is not UTF-8 JSON".into()))?;

        let job_root = self.root.join("jobs").join(job.id.to_string());
        let layout = job_root.join("layout");
        tokio::fs::create_dir_all(layout.join("blobs/sha256")).await?;
        let layout_config = blob_path(&layout, &manifest.config.digest)?;
        let shared_config = self.shared_blob_path(&manifest.config.digest)?;
        if tokio::fs::metadata(&shared_config).await.is_err() {
            atomic_write(&shared_config, config_json.as_bytes()).await?;
        }
        link_or_copy(&shared_config, &layout_config).await?;

        self.set_progress(job.id, "downloading", 0, total_bytes)
            .await?;
        let mut downloaded = manifest.config.size.max(0) as u64;
        let mut layer_paths = Vec::new();
        for layer in &manifest.layers {
            self.check_cancelled(job.id).await?;
            let path = blob_path(&layout, &layer.digest)?;
            let shared = self.shared_blob_path(&layer.digest)?;
            if tokio::fs::metadata(&shared).await.is_err() {
                let temporary = shared.with_extension("partial");
                let result = self
                    .download_node_blob(&node, logical.repository(), layer, &temporary)
                    .await;
                if result.is_err() {
                    let _ = tokio::fs::remove_file(&temporary).await;
                }
                result?;
                tokio::fs::rename(&temporary, &shared).await?;
            }
            link_or_copy(&shared, &path).await?;
            downloaded = downloaded.saturating_add(layer.size.max(0) as u64);
            self.set_progress(job.id, "downloading", downloaded, total_bytes)
                .await?;
            layer_paths.push(path);
        }
        write_layout(&layout, &manifest).await?;
        Ok(PreparedImage {
            manifest,
            manifest_digest,
            index_digest,
            config_json,
            layout,
            layer_paths,
            total_bytes,
        })
    }

    async fn node_bytes(
        &self,
        node: &NodeView,
        path: &str,
        accept: &str,
        limit: usize,
    ) -> ApiResult<(HeaderMap, Vec<u8>)> {
        let mut last_error = None;
        for attempt in 1..=3 {
            match self.node_bytes_once(node, path, accept, limit).await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    last_error = Some(error);
                    if attempt < 3 {
                        tracing::warn!(
                            node_id = %node.node.id,
                            attempt,
                            "source Registry request failed; retrying"
                        );
                        tokio::time::sleep(Duration::from_millis(250 * attempt)).await;
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            AppError::Upstream("source Registry request failed without an error".into())
        }))
    }

    async fn node_bytes_once(
        &self,
        node: &NodeView,
        path: &str,
        accept: &str,
        limit: usize,
    ) -> ApiResult<(HeaderMap, Vec<u8>)> {
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, accept.parse().map_err(AppError::internal)?);
        let response = self
            .upstream
            .send(node, Method::GET, path, &headers, RangeMode::Suppress)
            .await?;
        if !response.status().is_success() {
            return Err(AppError::Upstream(format!(
                "source Registry returned {} for {path}",
                response.status()
            )));
        }
        if response
            .content_length()
            .is_some_and(|size| size > limit as u64)
        {
            return Err(AppError::Upstream(
                "source Registry response exceeds the allowed size".into(),
            ));
        }
        let response_headers = response.headers().clone();
        let mut body = Vec::with_capacity(response.content_length().unwrap_or(0) as usize);
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| AppError::Upstream(error.to_string()))?;
            if body.len().saturating_add(chunk.len()) > limit {
                return Err(AppError::Upstream(
                    "source Registry response exceeds the allowed size".into(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok((response_headers, body))
    }

    async fn download_node_blob(
        &self,
        node: &NodeView,
        repository: &str,
        descriptor: &oci_client::manifest::OciDescriptor,
        destination: &Path,
    ) -> ApiResult<()> {
        let mut last_error = None;
        for attempt in 1..=3 {
            match self
                .download_node_blob_once(node, repository, descriptor, destination)
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) => {
                    last_error = Some(error);
                    if attempt < 3 {
                        tracing::warn!(
                            node_id = %node.node.id,
                            digest = %descriptor.digest,
                            attempt,
                            "source Blob stream failed; retrying"
                        );
                        tokio::time::sleep(Duration::from_millis(250 * attempt)).await;
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            AppError::Upstream("source Blob download failed without an error".into())
        }))
    }

    async fn download_node_blob_once(
        &self,
        node: &NodeView,
        repository: &str,
        descriptor: &oci_client::manifest::OciDescriptor,
        destination: &Path,
    ) -> ApiResult<()> {
        let path = format!("/v2/{repository}/blobs/{}", descriptor.digest);
        let response = self
            .upstream
            .send(
                node,
                Method::GET,
                &path,
                &HeaderMap::new(),
                RangeMode::Suppress,
            )
            .await?;
        if !response.status().is_success() {
            return Err(AppError::Upstream(format!(
                "source Registry returned {} for Blob {}",
                response.status(),
                descriptor.digest
            )));
        }
        let started = Instant::now();
        let mut file = File::create(destination).await?;
        let mut hasher = Sha256::new();
        let mut size = 0_u64;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| AppError::Upstream(error.to_string()))?;
            size = size.saturating_add(chunk.len() as u64);
            if size > descriptor.size.max(0) as u64 {
                return Err(AppError::Integrity);
            }
            hasher.update(&chunk);
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        if size != descriptor.size.max(0) as u64
            || format!("sha256:{:x}", hasher.finalize()) != descriptor.digest
        {
            return Err(AppError::Integrity);
        }
        self.nodes
            .record_transfer(node.node.id, size, started.elapsed(), true)
            .await;
        Ok(())
    }

    async fn source_transport(
        &self,
        logical: &Reference,
        job: &image_job::Model,
    ) -> ApiResult<(Reference, RegistryAuth)> {
        let auth = match job.source_credential_id {
            Some(id) => self.credential_auth(id, logical.resolve_registry()).await?,
            None => RegistryAuth::Anonymous,
        };
        Ok((logical.clone(), auth))
    }

    async fn export_image(&self, job: &image_job::Model, image: &PreparedImage) -> ApiResult<()> {
        self.set_stage(job.id, "archiving").await?;
        let format = job.output_format.as_deref().unwrap_or("docker");
        if !matches!(format, "docker" | "oci") {
            return Err(AppError::bad_request("output format must be docker or oci"));
        }
        let name = safe_artifact_name(&job.source_ref, format);
        let output = self.root.join("jobs").join(job.id.to_string()).join(&name);
        let config =
            oci_spec_builder::image::ImageConfiguration::from_reader(image.config_json.as_bytes())
                .map_err(|error| {
                    AppError::internal(anyhow::anyhow!("invalid image config: {error}"))
                })?;
        let layers = image.layer_paths.clone();
        let media_types = image
            .manifest
            .layers
            .iter()
            .map(|layer| layer.media_type.clone())
            .collect::<Vec<_>>();
        let reference = job.source_ref.clone();
        let output_clone = output.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let file = std::fs::File::create(&output_clone)?;
            let mut builder = oci_tar_builder::Builder::default();
            builder.add_config(config, reference);
            for (path, media_type) in layers.iter().zip(media_types) {
                builder.add_layer_with_media_type(path, media_type);
            }
            builder.build(file)?;
            Ok(())
        })
        .await
        .map_err(AppError::internal)?
        .map_err(AppError::internal)?;
        self.set_artifact(job.id, &output, &name).await
    }

    #[cfg(unix)]
    async fn extract_image(&self, job: &image_job::Model, image: &PreparedImage) -> ApiResult<()> {
        self.set_stage(job.id, "extracting").await?;
        let rootfs = self
            .root
            .join("jobs")
            .join(job.id.to_string())
            .join("rootfs");
        if tokio::fs::metadata(&rootfs).await.is_ok() {
            tokio::fs::remove_dir_all(&rootfs).await?;
        }
        ocirender::convert_dir(&image.layout, &rootfs)
            .await
            .map_err(AppError::internal)?;
        let name = format!("{}-rootfs", job.id);
        self.set_artifact(job.id, &rootfs, &name).await
    }

    #[cfg(not(unix))]
    async fn extract_image(
        &self,
        _job: &image_job::Model,
        _image: &PreparedImage,
    ) -> ApiResult<()> {
        Err(AppError::bad_request(
            "root filesystem browsing is currently supported on Linux and macOS servers",
        ))
    }

    async fn copy_image(&self, job: &image_job::Model, image: &PreparedImage) -> ApiResult<()> {
        self.set_stage(job.id, "copying").await?;
        let destination_raw = job
            .destination_ref
            .as_deref()
            .ok_or_else(|| AppError::bad_request("copy job has no destination reference"))?;
        let destination: Reference = destination_raw
            .parse()
            .map_err(|_| AppError::bad_request("invalid destination image reference"))?;
        let credential_id = job
            .destination_credential_id
            .ok_or_else(|| AppError::bad_request("copy job has no destination credential"))?;
        let auth = self
            .credential_auth(credential_id, destination.resolve_registry())
            .await?;
        let client = image_client(&job.platform_os, &job.platform_arch);
        self.copy_to_registry(job, image, &destination, &auth, &client)
            .await
    }

    async fn copy_to_registry(
        &self,
        job: &image_job::Model,
        image: &PreparedImage,
        destination: &Reference,
        auth: &RegistryAuth,
        client: &Client,
    ) -> ApiResult<()> {
        client
            .auth(destination, auth, RegistryOperation::Push)
            .await
            .map_err(oci_error)?;

        for (layer, path) in image.manifest.layers.iter().zip(&image.layer_paths) {
            self.check_cancelled(job.id).await?;
            if !client
                .blob_exists(destination, &layer.digest)
                .await
                .map_err(oci_error)?
            {
                push_file(client, destination, path, &layer.digest).await?;
            }
        }
        if !client
            .blob_exists(destination, &image.manifest.config.digest)
            .await
            .map_err(oci_error)?
        {
            client
                .push_blob(
                    destination,
                    image.config_json.as_bytes().to_vec(),
                    &image.manifest.config.digest,
                )
                .await
                .map_err(oci_error)?;
        }
        client
            .push_manifest(destination, &OciManifest::Image(image.manifest.clone()))
            .await
            .map_err(oci_error)?;
        image_sync_rule::Entity::update_many()
            .col_expr(
                image_sync_rule::Column::LastDigest,
                sea_orm::sea_query::Expr::value(Some(image.manifest_digest.clone())),
            )
            .col_expr(
                image_sync_rule::Column::LastRunAt,
                sea_orm::sea_query::Expr::value(Some(Utc::now())),
            )
            .filter(image_sync_rule::Column::SourceRef.eq(job.source_ref.clone()))
            .filter(
                image_sync_rule::Column::DestinationRef
                    .eq(job.destination_ref.clone().unwrap_or_default()),
            )
            .exec(&self.db)
            .await?;
        Ok(())
    }

    async fn copy_already_completed(
        &self,
        job: &image_job::Model,
        digest: &str,
    ) -> ApiResult<bool> {
        Ok(image_job::Entity::find()
            .filter(image_job::Column::Id.ne(job.id))
            .filter(image_job::Column::Kind.eq("copy"))
            .filter(image_job::Column::Status.eq("completed"))
            .filter(image_job::Column::SourceRef.eq(job.source_ref.clone()))
            .filter(image_job::Column::DestinationRef.eq(job.destination_ref.clone()))
            .filter(image_job::Column::ResolvedDigest.eq(digest))
            .one(&self.db)
            .await?
            .is_some())
    }

    async fn credential_auth(&self, id: Uuid, expected_registry: &str) -> ApiResult<RegistryAuth> {
        let credential = registry_credential::Entity::find_by_id(id)
            .one(&self.db)
            .await?
            .ok_or_else(|| AppError::not_found("Registry credential"))?;
        if normalize_registry(&credential.registry)? != normalize_registry(expected_registry)? {
            return Err(AppError::bad_request(
                "Registry credential does not match the requested Registry",
            ));
        }
        let secret = self.cipher.decrypt(&credential.secret_enc)?;
        match credential.auth_mode.as_str() {
            "basic" => Ok(RegistryAuth::Basic(
                credential
                    .username
                    .ok_or_else(|| AppError::bad_request("Basic credential has no username"))?,
                secret,
            )),
            "bearer" => Ok(RegistryAuth::Bearer(secret)),
            _ => Err(AppError::bad_request(
                "unsupported Registry credential mode",
            )),
        }
    }

    async fn update_job_manifest(&self, id: Uuid, image: &PreparedImage) -> ApiResult<()> {
        image_job::Entity::update_many()
            .col_expr(
                image_job::Column::ResolvedDigest,
                sea_orm::sea_query::Expr::value(Some(image.manifest_digest.clone())),
            )
            .col_expr(
                image_job::Column::IndexDigest,
                sea_orm::sea_query::Expr::value(image.index_digest.clone()),
            )
            .col_expr(
                image_job::Column::TotalBytes,
                sea_orm::sea_query::Expr::value(image.total_bytes.min(i64::MAX as u64) as i64),
            )
            .filter(image_job::Column::Id.eq(id))
            .exec(&self.db)
            .await?;
        Ok(())
    }

    async fn set_progress(&self, id: Uuid, stage: &str, current: u64, total: u64) -> ApiResult<()> {
        image_job::Entity::update_many()
            .col_expr(
                image_job::Column::Stage,
                sea_orm::sea_query::Expr::value(stage),
            )
            .col_expr(
                image_job::Column::ProgressBytes,
                sea_orm::sea_query::Expr::value(current.min(i64::MAX as u64) as i64),
            )
            .col_expr(
                image_job::Column::TotalBytes,
                sea_orm::sea_query::Expr::value(total.min(i64::MAX as u64) as i64),
            )
            .col_expr(
                image_job::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(Utc::now()),
            )
            .filter(image_job::Column::Id.eq(id))
            .exec(&self.db)
            .await?;
        Ok(())
    }

    async fn set_stage(&self, id: Uuid, stage: &str) -> ApiResult<()> {
        image_job::Entity::update_many()
            .col_expr(
                image_job::Column::Stage,
                sea_orm::sea_query::Expr::value(stage),
            )
            .col_expr(
                image_job::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(Utc::now()),
            )
            .filter(image_job::Column::Id.eq(id))
            .exec(&self.db)
            .await?;
        Ok(())
    }

    async fn set_artifact(&self, id: Uuid, path: &Path, name: &str) -> ApiResult<()> {
        image_job::Entity::update_many()
            .col_expr(
                image_job::Column::ArtifactPath,
                sea_orm::sea_query::Expr::value(Some(path.to_string_lossy().into_owned())),
            )
            .col_expr(
                image_job::Column::ArtifactName,
                sea_orm::sea_query::Expr::value(Some(name.to_owned())),
            )
            .filter(image_job::Column::Id.eq(id))
            .exec(&self.db)
            .await?;
        Ok(())
    }

    async fn check_cancelled(&self, id: Uuid) -> ApiResult<()> {
        let cancelled = image_job::Entity::find_by_id(id)
            .one(&self.db)
            .await?
            .is_some_and(|job| job.cancel_requested);
        if cancelled {
            Err(AppError::bad_request("image job was cancelled"))
        } else {
            Ok(())
        }
    }

    async fn enqueue_due_rules(&self) -> ApiResult<()> {
        let now = Utc::now();
        let rules = image_sync_rule::Entity::find()
            .filter(image_sync_rule::Column::Enabled.eq(true))
            .filter(image_sync_rule::Column::NextRunAt.lte(now))
            .all(&self.db)
            .await?;
        for rule in rules {
            let key = format!(
                "sync-rule:{}:{}",
                rule.id,
                rule.next_run_at.unwrap_or(now).timestamp()
            );
            let input = JobInput {
                kind: "copy".into(),
                source_ref: rule.source_ref.clone(),
                source_node_id: rule.source_node_id,
                source_credential_id: rule.source_credential_id,
                destination_ref: Some(rule.destination_ref.clone()),
                destination_credential_id: Some(rule.destination_credential_id),
                platform_os: rule.platform_os.clone(),
                platform_arch: rule.platform_arch.clone(),
                output_format: None,
            };
            let _ = self.create_job(input, Some(key)).await?;
            let next = next_run(&rule.cron, &rule.timezone)?;
            let mut active = rule.into_active_model();
            active.last_run_at = Set(Some(now));
            active.next_run_at = Set(Some(next));
            active.updated_at = Set(now);
            active.update(&self.db).await?;
        }
        Ok(())
    }

    async fn create_job(
        &self,
        input: JobInput,
        idempotency_key: Option<String>,
    ) -> ApiResult<image_job::Model> {
        validate_job(&input)?;
        if let Some(key) = idempotency_key.as_deref()
            && let Some(existing) = image_job::Entity::find()
                .filter(image_job::Column::IdempotencyKey.eq(key))
                .one(&self.db)
                .await?
        {
            return Ok(existing);
        }
        let now = Utc::now();
        let model = image_job::Model {
            id: Uuid::new_v4(),
            kind: input.kind,
            status: "pending".into(),
            source_ref: input.source_ref,
            source_node_id: input.source_node_id,
            source_credential_id: input.source_credential_id,
            destination_ref: input.destination_ref,
            destination_credential_id: input.destination_credential_id,
            platform_os: input.platform_os,
            platform_arch: input.platform_arch,
            output_format: input.output_format,
            resolved_digest: None,
            index_digest: None,
            stage: "queued".into(),
            progress_bytes: 0,
            total_bytes: 0,
            artifact_path: None,
            artifact_name: None,
            error: None,
            idempotency_key,
            cancel_requested: false,
            lease_until: None,
            created_at: now,
            updated_at: now,
            started_at: None,
            finished_at: None,
        };
        let model = model.into_active_model().insert(&self.db).await?;
        self.wake.notify_one();
        Ok(model)
    }

    fn shared_blob_path(&self, digest: &str) -> ApiResult<PathBuf> {
        let value = digest
            .strip_prefix("sha256:")
            .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .ok_or_else(|| AppError::bad_request("only sha256 image blobs are supported"))?;
        Ok(self.root.join("blobs/sha256").join(value))
    }

    async fn cleanup_storage(&self, protected_job: Option<Uuid>) -> ApiResult<()> {
        if protected_job.is_none() {
            let mut last_cleanup = self.last_cleanup.lock().await;
            if last_cleanup.elapsed() < Duration::from_secs(60) {
                return Ok(());
            }
            *last_cleanup = Instant::now();
        }
        let cutoff = Utc::now()
            - chrono::Duration::from_std(self.config.export_ttl).map_err(AppError::internal)?;
        let terminal = ["completed", "failed", "cancelled", "skipped"];
        let expired = image_job::Entity::find()
            .filter(image_job::Column::Status.is_in(terminal))
            .filter(image_job::Column::FinishedAt.lt(cutoff))
            .order_by_asc(image_job::Column::FinishedAt)
            .all(&self.db)
            .await?;
        for job in expired {
            if Some(job.id) != protected_job {
                self.remove_job_storage(job.id).await?;
            }
        }

        self.gc_shared_blobs().await?;
        let mut used = storage_usage(self.root.clone()).await?;
        if used <= self.config.max_export_bytes {
            return Ok(());
        }

        let candidates = image_job::Entity::find()
            .filter(image_job::Column::Status.is_in(terminal))
            .order_by_asc(image_job::Column::FinishedAt)
            .all(&self.db)
            .await?;
        for job in candidates {
            if Some(job.id) == protected_job {
                continue;
            }
            self.remove_job_storage(job.id).await?;
            self.gc_shared_blobs().await?;
            used = storage_usage(self.root.clone()).await?;
            if used <= self.config.max_export_bytes {
                break;
            }
        }
        Ok(())
    }

    async fn enforce_storage_quota(&self, job_id: Uuid) -> ApiResult<()> {
        self.cleanup_storage(Some(job_id)).await?;
        if storage_usage(self.root.clone()).await? <= self.config.max_export_bytes {
            return Ok(());
        }
        self.remove_job_storage(job_id).await?;
        self.gc_shared_blobs().await?;
        Err(AppError::bad_request(
            "image tools storage exceeds DONKEY_MAX_EXPORT_BYTES",
        ))
    }

    async fn remove_job_storage(&self, id: Uuid) -> ApiResult<()> {
        let path = self.root.join("jobs").join(id.to_string());
        match tokio::fs::remove_dir_all(path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        image_job::Entity::update_many()
            .col_expr(
                image_job::Column::ArtifactPath,
                sea_orm::sea_query::Expr::value(Option::<String>::None),
            )
            .col_expr(
                image_job::Column::ArtifactName,
                sea_orm::sea_query::Expr::value(Option::<String>::None),
            )
            .col_expr(
                image_job::Column::IdempotencyKey,
                sea_orm::sea_query::Expr::value(Option::<String>::None),
            )
            .filter(image_job::Column::Id.eq(id))
            .exec(&self.db)
            .await?;
        Ok(())
    }

    async fn gc_shared_blobs(&self) -> ApiResult<()> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || gc_shared_blobs_sync(&root))
            .await
            .map_err(AppError::internal)?
            .map_err(AppError::from)
    }
}

struct PreparedImage {
    manifest: OciImageManifest,
    manifest_digest: String,
    index_digest: Option<String>,
    config_json: String,
    layout: PathBuf,
    layer_paths: Vec<PathBuf>,
    total_bytes: u64,
}

enum JobOutcome {
    Completed,
    Skipped,
}

pub fn router(service: ImageTools) -> Router {
    service.router()
}

async fn list_credentials(
    State(service): State<ImageTools>,
) -> ApiResult<Json<Vec<CredentialView>>> {
    Ok(Json(
        registry_credential::Entity::find()
            .order_by_asc(registry_credential::Column::Name)
            .all(&service.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect(),
    ))
}

async fn create_credential(
    State(service): State<ImageTools>,
    Json(input): Json<CredentialInput>,
) -> ApiResult<(StatusCode, Json<CredentialView>)> {
    service.ensure_secret_transport()?;
    let input = validate_credential(input, true)?;
    let now = Utc::now();
    let model = registry_credential::Model {
        id: Uuid::new_v4(),
        name: input.name,
        registry: normalize_registry(&input.registry)?,
        auth_mode: input.auth_mode,
        username: input.username,
        secret_enc: service.cipher.encrypt(&input.secret)?,
        generation: 1,
        created_at: now,
        updated_at: now,
    }
    .into_active_model()
    .insert(&service.db)
    .await?;
    Ok((StatusCode::CREATED, Json(model.into())))
}

async fn update_credential(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
    Json(input): Json<CredentialInput>,
) -> ApiResult<Json<CredentialView>> {
    service.ensure_secret_transport()?;
    let input = validate_credential(input, false)?;
    let model = registry_credential::Entity::find_by_id(id)
        .one(&service.db)
        .await?
        .ok_or_else(|| AppError::not_found("Registry credential"))?;
    let mut active = model.into_active_model();
    active.name = Set(input.name);
    active.registry = Set(normalize_registry(&input.registry)?);
    active.auth_mode = Set(input.auth_mode);
    active.username = Set(input.username);
    if !input.secret.is_empty() {
        active.secret_enc = Set(service.cipher.encrypt(&input.secret)?);
        active.generation = Set(active.generation.as_ref() + 1);
    }
    active.updated_at = Set(Utc::now());
    Ok(Json(active.update(&service.db).await?.into()))
}

async fn delete_credential(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    let rule_uses = image_sync_rule::Entity::find()
        .filter(
            Condition::any()
                .add(image_sync_rule::Column::DestinationCredentialId.eq(id))
                .add(image_sync_rule::Column::SourceCredentialId.eq(id)),
        )
        .one(&service.db)
        .await?
        .is_some();
    let active_job_uses = image_job::Entity::find()
        .filter(
            Condition::all()
                .add(image_job::Column::Status.is_in(["pending", "running"]))
                .add(
                    Condition::any()
                        .add(image_job::Column::DestinationCredentialId.eq(id))
                        .add(image_job::Column::SourceCredentialId.eq(id)),
                ),
        )
        .one(&service.db)
        .await?
        .is_some();
    if rule_uses || active_job_uses {
        return Err(AppError::bad_request(
            "credential is used by an active job or sync rule",
        ));
    }
    registry_credential::Entity::delete_by_id(id)
        .exec(&service.db)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_jobs(
    State(service): State<ImageTools>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Json<Vec<image_job::Model>>> {
    Ok(Json(
        image_job::Entity::find()
            .order_by_desc(image_job::Column::CreatedAt)
            .limit(query.limit.min(500))
            .all(&service.db)
            .await?,
    ))
}

async fn create_job(
    State(service): State<ImageTools>,
    headers: HeaderMap,
    Json(input): Json<JobInput>,
) -> ApiResult<(StatusCode, Json<image_job::Model>)> {
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    Ok((
        StatusCode::CREATED,
        Json(service.create_job(input, key).await?),
    ))
}

async fn get_job(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<image_job::Model>> {
    Ok(Json(
        image_job::Entity::find_by_id(id)
            .one(&service.db)
            .await?
            .ok_or_else(|| AppError::not_found("image job"))?,
    ))
}

async fn cancel_job(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    image_job::Entity::update_many()
        .col_expr(
            image_job::Column::CancelRequested,
            sea_orm::sea_query::Expr::value(true),
        )
        .filter(image_job::Column::Id.eq(id))
        .exec(&service.db)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn retry_job(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<image_job::Model>> {
    let model = image_job::Entity::find_by_id(id)
        .one(&service.db)
        .await?
        .ok_or_else(|| AppError::not_found("image job"))?;
    let mut active = model.into_active_model();
    active.status = Set("pending".into());
    active.stage = Set("queued".into());
    active.error = Set(None);
    active.cancel_requested = Set(false);
    active.finished_at = Set(None);
    active.updated_at = Set(Utc::now());
    let model = active.update(&service.db).await?;
    service.wake.notify_one();
    Ok(Json(model))
}

async fn download_artifact(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
    request: Request,
) -> ApiResult<Response> {
    let job = completed_job(&service.db, id).await?;
    let path = job
        .artifact_path
        .map(PathBuf::from)
        .filter(|path| path.is_file())
        .ok_or_else(|| AppError::not_found("image artifact"))?;
    serve_path(path, job.artifact_name.as_deref(), request).await
}

async fn list_files(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
    Query(query): Query<FileQuery>,
) -> ApiResult<Json<Vec<FileEntry>>> {
    let root = extracted_root(&service.db, id).await?;
    let directory = safe_join(&root, &query.path)?;
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
            .strip_prefix(&root)
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
    Ok(Json(entries))
}

async fn download_file(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
    Query(query): Query<FileQuery>,
    request: Request,
) -> ApiResult<Response> {
    let root = extracted_root(&service.db, id).await?;
    let path = safe_join(&root, &query.path)?;
    let metadata = tokio::fs::symlink_metadata(&path).await?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(AppError::bad_request(
            "only regular files can be downloaded",
        ));
    }
    serve_path(path, None, request).await
}

async fn list_rules(
    State(service): State<ImageTools>,
) -> ApiResult<Json<Vec<image_sync_rule::Model>>> {
    Ok(Json(
        image_sync_rule::Entity::find()
            .order_by_asc(image_sync_rule::Column::Name)
            .all(&service.db)
            .await?,
    ))
}

async fn create_rule(
    State(service): State<ImageTools>,
    Json(input): Json<SyncRuleInput>,
) -> ApiResult<(StatusCode, Json<image_sync_rule::Model>)> {
    let input = validate_rule(input)?;
    let now = Utc::now();
    let model = image_sync_rule::Model {
        id: Uuid::new_v4(),
        name: input.name,
        enabled: input.enabled,
        source_ref: input.source_ref,
        source_node_id: input.source_node_id,
        source_credential_id: input.source_credential_id,
        destination_ref: input.destination_ref,
        destination_credential_id: input.destination_credential_id,
        platform_os: input.platform_os,
        platform_arch: input.platform_arch,
        cron: input.cron.clone(),
        timezone: input.timezone.clone(),
        last_digest: None,
        last_run_at: None,
        next_run_at: Some(next_run(&input.cron, &input.timezone)?),
        created_at: now,
        updated_at: now,
    }
    .into_active_model()
    .insert(&service.db)
    .await?;
    service.wake.notify_one();
    Ok((StatusCode::CREATED, Json(model)))
}

async fn update_rule(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
    Json(input): Json<SyncRuleInput>,
) -> ApiResult<Json<image_sync_rule::Model>> {
    let input = validate_rule(input)?;
    let model = image_sync_rule::Entity::find_by_id(id)
        .one(&service.db)
        .await?
        .ok_or_else(|| AppError::not_found("sync rule"))?;
    let mut active = model.into_active_model();
    active.name = Set(input.name);
    active.enabled = Set(input.enabled);
    active.source_ref = Set(input.source_ref);
    active.source_node_id = Set(input.source_node_id);
    active.source_credential_id = Set(input.source_credential_id);
    active.destination_ref = Set(input.destination_ref);
    active.destination_credential_id = Set(input.destination_credential_id);
    active.platform_os = Set(input.platform_os);
    active.platform_arch = Set(input.platform_arch);
    active.cron = Set(input.cron.clone());
    active.timezone = Set(input.timezone.clone());
    active.next_run_at = Set(Some(next_run(&input.cron, &input.timezone)?));
    active.updated_at = Set(Utc::now());
    Ok(Json(active.update(&service.db).await?))
}

async fn delete_rule(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    image_sync_rule::Entity::delete_by_id(id)
        .exec(&service.db)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn run_rule(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<(StatusCode, Json<image_job::Model>)> {
    let rule = image_sync_rule::Entity::find_by_id(id)
        .one(&service.db)
        .await?
        .ok_or_else(|| AppError::not_found("sync rule"))?;
    let input = JobInput {
        kind: "copy".into(),
        source_ref: rule.source_ref,
        source_node_id: rule.source_node_id,
        source_credential_id: rule.source_credential_id,
        destination_ref: Some(rule.destination_ref),
        destination_credential_id: Some(rule.destination_credential_id),
        platform_os: rule.platform_os,
        platform_arch: rule.platform_arch,
        output_format: None,
    };
    Ok((
        StatusCode::CREATED,
        Json(service.create_job(input, None).await?),
    ))
}

impl ImageTools {
    fn ensure_secret_transport(&self) -> ApiResult<()> {
        if !self.config.admin_secret_transport_is_secure() {
            return Err(AppError::bad_request(
                "Registry credentials require HTTPS or a loopback admin listener",
            ));
        }
        if !self.cipher.is_configured() {
            return Err(AppError::bad_request(
                "DONKEY_CREDENTIAL_KEY is required for Registry credentials",
            ));
        }
        Ok(())
    }
}

async fn completed_job(db: &DatabaseConnection, id: Uuid) -> ApiResult<image_job::Model> {
    let job = image_job::Entity::find_by_id(id)
        .one(db)
        .await?
        .ok_or_else(|| AppError::not_found("image job"))?;
    if job.status != "completed" {
        return Err(AppError::bad_request("image job is not completed"));
    }
    Ok(job)
}

async fn extracted_root(db: &DatabaseConnection, id: Uuid) -> ApiResult<PathBuf> {
    let job = completed_job(db, id).await?;
    let path = job
        .artifact_path
        .map(PathBuf::from)
        .filter(|path| path.is_dir())
        .ok_or_else(|| AppError::not_found("extracted image"))?;
    Ok(path)
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
            name.replace(['"', '\r', '\n'], "_")
        );
        response.headers_mut().insert(
            header::CONTENT_DISPOSITION,
            value.parse().map_err(AppError::internal)?,
        );
    }
    Ok(response)
}

fn image_client(os: &str, arch: &str) -> Client {
    let os = os.to_owned();
    let arch = arch.to_owned();
    let config = ClientConfig {
        max_concurrent_download: 4,
        max_concurrent_upload: 4,
        read_timeout: Some(Duration::from_secs(120)),
        connect_timeout: Some(Duration::from_secs(15)),
        platform_resolver: Some(Box::new(move |manifests| {
            manifests
                .iter()
                .find(|entry| {
                    entry.platform.as_ref().is_some_and(|platform| {
                        platform.os.to_string() == os && platform.architecture.to_string() == arch
                    })
                })
                .map(|entry| entry.digest.clone())
        })),
        ..Default::default()
    };
    Client::new(config)
}

fn manifest_accept() -> &'static str {
    "application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json"
}

fn digest_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("docker-content-digest")
        .and_then(|value| value.to_str().ok())
}

fn verified_digest(
    bytes: &[u8],
    header_digest: Option<&str>,
    expected_digest: Option<&str>,
) -> ApiResult<String> {
    let computed = format!("sha256:{:x}", Sha256::digest(bytes));
    if header_digest.is_some_and(|digest| digest != computed)
        || expected_digest.is_some_and(|digest| digest != computed)
    {
        return Err(AppError::Integrity);
    }
    Ok(computed)
}

fn normalize_registry(value: &str) -> ApiResult<String> {
    let raw = value.trim().trim_end_matches('/');
    let normalized_url = if raw.contains("://") {
        raw.to_owned()
    } else {
        format!("https://{raw}")
    };
    let url = Url::parse(&normalized_url)
        .map_err(|_| AppError::bad_request("invalid Registry address"))?;
    if url.scheme() != "https" || url.host_str().is_none() {
        return Err(AppError::bad_request(
            "Registry credentials require an HTTPS Registry",
        ));
    }
    Ok(match url.port() {
        Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
        None => url.host_str().unwrap_or_default().to_owned(),
    })
}

fn validate_credential(
    mut input: CredentialInput,
    require_secret: bool,
) -> ApiResult<CredentialInput> {
    input.name = input.name.trim().to_owned();
    if input.name.is_empty() || input.name.len() > 80 {
        return Err(AppError::bad_request(
            "credential name must be 1-80 characters",
        ));
    }
    if !matches!(input.auth_mode.as_str(), "basic" | "bearer") {
        return Err(AppError::bad_request(
            "credential mode must be basic or bearer",
        ));
    }
    if input.auth_mode == "basic" && input.username.as_deref().is_none_or(str::is_empty) {
        return Err(AppError::bad_request(
            "Basic credential requires a username",
        ));
    }
    if (require_secret && input.secret.is_empty()) || input.secret.len() > 16 * 1024 {
        return Err(AppError::bad_request(
            "credential secret is empty or too long",
        ));
    }
    Ok(input)
}

fn validate_job(input: &JobInput) -> ApiResult<()> {
    if !matches!(input.kind.as_str(), "export" | "extract" | "copy") {
        return Err(AppError::bad_request(
            "job kind must be export, extract, or copy",
        ));
    }
    if input.source_ref.len() > 1024 || input.source_ref.parse::<Reference>().is_err() {
        return Err(AppError::bad_request("invalid source image reference"));
    }
    if input.source_node_id.is_some() && input.source_credential_id.is_some() {
        return Err(AppError::bad_request(
            "choose either a source acceleration node or a direct source credential",
        ));
    }
    if input.kind == "copy"
        && (input.destination_ref.is_none() || input.destination_credential_id.is_none())
    {
        return Err(AppError::bad_request(
            "copy jobs require destination_ref and destination_credential_id",
        ));
    }
    if !matches!(input.platform_os.as_str(), "linux" | "windows")
        || !matches!(
            input.platform_arch.as_str(),
            "amd64" | "arm64" | "arm" | "386"
        )
    {
        return Err(AppError::bad_request("unsupported image platform"));
    }
    Ok(())
}

fn validate_rule(input: SyncRuleInput) -> ApiResult<SyncRuleInput> {
    if input.name.trim().is_empty() || input.name.len() > 80 {
        return Err(AppError::bad_request(
            "sync rule name must be 1-80 characters",
        ));
    }
    validate_job(&JobInput {
        kind: "copy".into(),
        source_ref: input.source_ref.clone(),
        source_node_id: input.source_node_id,
        source_credential_id: input.source_credential_id,
        destination_ref: Some(input.destination_ref.clone()),
        destination_credential_id: Some(input.destination_credential_id),
        platform_os: input.platform_os.clone(),
        platform_arch: input.platform_arch.clone(),
        output_format: None,
    })?;
    let _ = next_run(&input.cron, &input.timezone)?;
    Ok(input)
}

fn next_run(expression: &str, timezone: &str) -> ApiResult<DateTime<Utc>> {
    let schedule = Schedule::from_str(expression)
        .map_err(|_| AppError::bad_request("invalid Cron expression"))?;
    let timezone: Tz = timezone
        .parse()
        .map_err(|_| AppError::bad_request("invalid IANA timezone"))?;
    schedule
        .upcoming(timezone)
        .next()
        .map(|value| value.with_timezone(&Utc))
        .ok_or_else(|| AppError::bad_request("Cron expression has no future occurrence"))
}

fn safe_artifact_name(reference: &str, format: &str) -> String {
    let stem = reference
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("{stem}.{format}.tar")
}

fn blob_path(layout: &Path, digest: &str) -> ApiResult<PathBuf> {
    let value = digest
        .strip_prefix("sha256:")
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| AppError::bad_request("only sha256 image blobs are supported"))?;
    Ok(layout.join("blobs/sha256").join(value))
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> ApiResult<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let temporary = path.with_extension("partial");
    tokio::fs::write(&temporary, bytes).await?;
    tokio::fs::rename(temporary, path).await?;
    Ok(())
}

async fn link_or_copy(source: &Path, destination: &Path) -> ApiResult<()> {
    if tokio::fs::metadata(destination).await.is_ok() {
        return Ok(());
    }
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    match tokio::fs::hard_link(source, destination).await {
        Ok(()) => Ok(()),
        Err(_) => {
            tokio::fs::copy(source, destination).await?;
            Ok(())
        }
    }
}

async fn write_layout(layout: &Path, manifest: &OciImageManifest) -> ApiResult<()> {
    let manifest_bytes = serde_json::to_vec(manifest).map_err(AppError::internal)?;
    let digest = format!("sha256:{}", hex::encode(Sha256::digest(&manifest_bytes)));
    atomic_write(&blob_path(layout, &digest)?, &manifest_bytes).await?;
    let index = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{
            "mediaType": manifest.media_type.as_deref().unwrap_or("application/vnd.oci.image.manifest.v1+json"),
            "digest": digest,
            "size": manifest_bytes.len()
        }]
    });
    atomic_write(
        &layout.join("index.json"),
        &serde_json::to_vec(&index).map_err(AppError::internal)?,
    )
    .await?;
    atomic_write(
        &layout.join("oci-layout"),
        br#"{"imageLayoutVersion":"1.0.0"}"#,
    )
    .await?;
    Ok(())
}

async fn push_file(
    client: &Client,
    reference: &Reference,
    path: &Path,
    digest: &str,
) -> ApiResult<()> {
    let stream = ReaderStream::new(File::open(path).await?)
        .map(|result| result.map_err(OciDistributionError::IoError));
    client
        .push_blob_stream(reference, stream, digest)
        .await
        .map_err(oci_error)?;
    Ok(())
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

fn safe_error(error: &AppError) -> String {
    match error {
        AppError::BadRequest(message) => message.chars().take(500).collect(),
        AppError::Upstream(_) => "upstream Registry request failed; check server logs".into(),
        AppError::Integrity => "image content integrity check failed".into(),
        _ => "image task failed; check server logs".into(),
    }
}

async fn storage_usage(root: Arc<PathBuf>) -> ApiResult<u64> {
    tokio::task::spawn_blocking(move || storage_usage_sync(&root))
        .await
        .map_err(AppError::internal)?
        .map_err(AppError::from)
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

fn oci_error(error: OciDistributionError) -> AppError {
    AppError::Upstream(error.to_string().chars().take(500).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use secrecy::SecretString;

    #[test]
    fn selected_source_digest_is_verified_independently_of_transport() {
        let bytes = br#"{"schemaVersion":2}"#;
        let digest = format!("sha256:{:x}", Sha256::digest(bytes));
        assert_eq!(
            verified_digest(bytes, Some(&digest), Some(&digest)).unwrap(),
            digest
        );
        assert!(verified_digest(bytes, None, Some("sha256:wrong")).is_err());
    }

    #[test]
    fn cron_and_timezone_produce_a_future_run() {
        let next = next_run("0 */5 * * * *", "Asia/Shanghai").unwrap();
        assert!(next > Utc::now());
    }

    #[test]
    fn file_browser_rejects_parent_traversal() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("safe.txt"), b"safe").unwrap();
        assert!(safe_join(directory.path(), "safe.txt").is_ok());
        assert!(safe_join(directory.path(), "../secret").is_err());
        assert!(safe_join(directory.path(), "/etc/passwd").is_err());
    }

    #[tokio::test]
    async fn idempotency_key_returns_the_existing_job() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.admin_auth = Some(SecretString::from("admin:password"));
        config.credential_key = Some(SecretString::from("33".repeat(32)));
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let config = Arc::new(config);
        let nodes = NodeService::new(config.clone(), db.clone()).unwrap();
        let service = ImageTools::new(config, db, nodes).await.unwrap();
        let input = JobInput {
            kind: "export".into(),
            source_ref: "docker.io/library/alpine:latest".into(),
            source_node_id: None,
            source_credential_id: None,
            destination_ref: None,
            destination_credential_id: None,
            platform_os: "linux".into(),
            platform_arch: "amd64".into(),
            output_format: Some("docker".into()),
        };
        let first = service
            .create_job(input.clone(), Some("same-request".into()))
            .await
            .unwrap();
        let second = service
            .create_job(input, Some("same-request".into()))
            .await
            .unwrap();
        assert_eq!(first.id, second.id);
    }

    #[tokio::test]
    async fn selected_source_node_applies_custom_header_to_all_registry_requests() {
        let upstream = MockServer::start_async().await;
        let config_bytes = br#"{"architecture":"amd64","os":"linux"}"#;
        let layer_bytes = b"verified layer bytes";
        let config_digest = format!("sha256:{:x}", Sha256::digest(config_bytes));
        let layer_digest = format!("sha256:{:x}", Sha256::digest(layer_bytes));
        let manifest = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest,
                "size": config_bytes.len()
            },
            "layers": [{
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": layer_digest,
                "size": layer_bytes.len()
            }]
        });
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        let manifest_digest = format!("sha256:{:x}", Sha256::digest(&manifest_bytes));
        let manifest_mock = upstream
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v2/library/test/manifests/latest")
                    .header("x-mirror-key", "top-secret");
                then.status(200)
                    .header("docker-content-digest", &manifest_digest)
                    .body(manifest_bytes);
            })
            .await;
        let config_path = format!("/v2/library/test/blobs/{config_digest}");
        let config_mock = upstream
            .mock_async(|when, then| {
                when.method(GET)
                    .path(config_path)
                    .header("x-mirror-key", "top-secret");
                then.status(200).body(config_bytes);
            })
            .await;
        let layer_path = format!("/v2/library/test/blobs/{layer_digest}");
        let layer_mock = upstream
            .mock_async(|when, then| {
                when.method(GET)
                    .path(layer_path)
                    .header("x-mirror-key", "top-secret");
                then.status(200).body(layer_bytes);
            })
            .await;

        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.admin_auth = Some(SecretString::from("admin:password"));
        config.registry_auth = Some(SecretString::from("client:password"));
        config.credential_key = Some(SecretString::from("44".repeat(32)));
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let config = Arc::new(config);
        let nodes = NodeService::new(config.clone(), db.clone()).unwrap();
        let node = nodes
            .create(crate::nodes::NodeInput {
                name: "header mirror".into(),
                url: upstream.base_url(),
                kind: "registry".into(),
                route_prefix: None,
                enabled: true,
                priority: 1,
                cf_preferred: false,
                connect_ip: None,
                auth_mode: "header".into(),
                auth_username: None,
                auth_header: Some("x-mirror-key".into()),
                auth_secret: Some("top-secret".into()),
            })
            .await
            .unwrap();
        let service = ImageTools::new(config, db, nodes).await.unwrap();
        let job = service
            .create_job(
                JobInput {
                    kind: "export".into(),
                    source_ref: "localhost/library/test:latest".into(),
                    source_node_id: Some(node.node.id),
                    source_credential_id: None,
                    destination_ref: None,
                    destination_credential_id: None,
                    platform_os: "linux".into(),
                    platform_arch: "amd64".into(),
                    output_format: Some("oci".into()),
                },
                None,
            )
            .await
            .unwrap();
        let reference = job.source_ref.parse().unwrap();
        let image = service
            .prepare_image_via_node(&job, &reference, node.node.id)
            .await
            .unwrap();

        assert_eq!(image.manifest_digest, manifest_digest);
        assert_eq!(image.config_json.as_bytes(), config_bytes);
        assert_eq!(
            tokio::fs::read(&image.layer_paths[0]).await.unwrap(),
            layer_bytes
        );
        manifest_mock.assert_async().await;
        config_mock.assert_async().await;
        layer_mock.assert_async().await;
    }

    #[tokio::test]
    async fn registry_copy_streams_missing_blobs_before_manifest() {
        let registry = MockServer::start_async().await;
        let base = registry.base_url();
        let upload_location = format!("{base}/upload/session");
        let ping = registry
            .mock_async(|when, then| {
                when.method(GET).path("/v2/");
                then.status(200);
            })
            .await;
        let config_bytes = br#"{"architecture":"amd64","os":"linux"}"#;
        let layer_bytes = b"copy layer bytes";
        let config_digest = format!("sha256:{:x}", Sha256::digest(config_bytes));
        let layer_digest = format!("sha256:{:x}", Sha256::digest(layer_bytes));
        let config_head_path = format!("/v2/team/copied/blobs/{config_digest}");
        let layer_head_path = format!("/v2/team/copied/blobs/{layer_digest}");
        let config_head = registry
            .mock_async(|when, then| {
                when.method(httpmock::Method::HEAD).path(config_head_path);
                then.status(404);
            })
            .await;
        let layer_head = registry
            .mock_async(|when, then| {
                when.method(httpmock::Method::HEAD).path(layer_head_path);
                then.status(404);
            })
            .await;
        let begin_location = upload_location.clone();
        let begin = registry
            .mock_async(|when, then| {
                when.method(POST).path("/v2/team/copied/blobs/uploads/");
                then.status(202).header("location", &begin_location);
            })
            .await;
        let chunk_location = upload_location.clone();
        let chunks = registry
            .mock_async(|when, then| {
                when.method(PATCH).path("/upload/session");
                then.status(202).header("location", &chunk_location);
            })
            .await;
        let finish_location = format!("{base}/v2/team/copied/blobs/uploaded");
        let finishes = registry
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/upload/session")
                    .query_param_exists("digest");
                then.status(201).header("location", &finish_location);
            })
            .await;
        let manifest_location = format!("{base}/v2/team/copied/manifests/latest");
        let manifest_push = registry
            .mock_async(|when, then| {
                when.method(PUT).path("/v2/team/copied/manifests/latest");
                then.status(201).header("location", &manifest_location);
            })
            .await;

        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.admin_auth = Some(SecretString::from("admin:password"));
        config.credential_key = Some(SecretString::from("55".repeat(32)));
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let config = Arc::new(config);
        let nodes = NodeService::new(config.clone(), db.clone()).unwrap();
        let service = ImageTools::new(config, db, nodes).await.unwrap();
        let credential_id = Uuid::new_v4();
        let destination_ref = format!("{}/team/copied:latest", registry.address());
        let job = service
            .create_job(
                JobInput {
                    kind: "copy".into(),
                    source_ref: "localhost/library/source:latest".into(),
                    source_node_id: None,
                    source_credential_id: None,
                    destination_ref: Some(destination_ref.clone()),
                    destination_credential_id: Some(credential_id),
                    platform_os: "linux".into(),
                    platform_arch: "amd64".into(),
                    output_format: None,
                },
                None,
            )
            .await
            .unwrap();
        let layer_path = directory.path().join("layer.tar.gz");
        tokio::fs::write(&layer_path, layer_bytes).await.unwrap();
        let manifest = OciImageManifest {
            schema_version: 2,
            media_type: Some("application/vnd.oci.image.manifest.v1+json".into()),
            config: oci_client::manifest::OciDescriptor {
                media_type: "application/vnd.oci.image.config.v1+json".into(),
                digest: config_digest.clone(),
                size: config_bytes.len() as i64,
                ..Default::default()
            },
            layers: vec![oci_client::manifest::OciDescriptor {
                media_type: "application/vnd.oci.image.layer.v1.tar+gzip".into(),
                digest: layer_digest,
                size: layer_bytes.len() as i64,
                ..Default::default()
            }],
            ..Default::default()
        };
        let prepared = PreparedImage {
            manifest,
            manifest_digest: "sha256:manifest".into(),
            index_digest: None,
            config_json: String::from_utf8(config_bytes.to_vec()).unwrap(),
            layout: directory.path().join("layout"),
            layer_paths: vec![layer_path],
            total_bytes: (config_bytes.len() + layer_bytes.len()) as u64,
        };
        let destination: Reference = destination_ref.parse().unwrap();
        let client = Client::new(ClientConfig {
            protocol: oci_client::client::ClientProtocol::Http,
            ..Default::default()
        });
        service
            .copy_to_registry(
                &job,
                &prepared,
                &destination,
                &RegistryAuth::Anonymous,
                &client,
            )
            .await
            .unwrap();

        assert!(ping.calls_async().await >= 1);
        config_head.assert_async().await;
        layer_head.assert_async().await;
        begin.assert_calls_async(2).await;
        chunks.assert_calls_async(2).await;
        finishes.assert_calls_async(2).await;
        manifest_push.assert_async().await;
    }

    #[tokio::test]
    async fn destination_credential_is_bound_to_its_registry_host() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.credential_key = Some(SecretString::from("66".repeat(32)));
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let config = Arc::new(config);
        let nodes = NodeService::new(config.clone(), db.clone()).unwrap();
        let service = ImageTools::new(config, db.clone(), nodes).await.unwrap();
        let id = Uuid::new_v4();
        let now = Utc::now();
        registry_credential::Model {
            id,
            name: "destination".into(),
            registry: "registry.example".into(),
            auth_mode: "basic".into(),
            username: Some("robot".into()),
            secret_enc: service.cipher.encrypt("registry-password").unwrap(),
            generation: 1,
            created_at: now,
            updated_at: now,
        }
        .into_active_model()
        .insert(&db)
        .await
        .unwrap();

        assert!(matches!(
            service.credential_auth(id, "other.example").await,
            Err(AppError::BadRequest(_))
        ));
        assert_eq!(
            service
                .credential_auth(id, "registry.example")
                .await
                .unwrap(),
            RegistryAuth::Basic("robot".into(), "registry-password".into())
        );
    }

    #[test]
    fn storage_usage_counts_shared_blobs_once_and_gc_removes_orphans() {
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
