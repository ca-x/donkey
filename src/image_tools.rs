use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::Response,
    routing::{get, post, put},
};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use futures_util::StreamExt;
#[cfg(test)]
use oci_client::client::ClientConfig;
use oci_client::{
    Client, Reference,
    manifest::{OciImageManifest, OciManifest},
    secrets::RegistryAuth,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, QuerySelect, Set, SqliteTransactionMode, TransactionOptions,
    TransactionTrait, UpdateMany,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    fs::File,
    io::AsyncWriteExt,
    sync::RwLock,
    sync::{Mutex, Notify},
};
use url::Url;
use uuid::Uuid;

use crate::{
    config::Config,
    crypto::CredentialCipher,
    db::{self, image_job, image_sync_rule, registry_credential},
    error::{ApiResult, AppError},
    nodes::{NodeService, NodeView},
    upstream::{RangeMode, UpstreamService},
};

mod archive;
mod artifacts;
mod files;
mod job_store;
mod registry;

use archive::{ArchiveInput, build_archive};
#[cfg(test)]
use archive::{build_docker_archive, build_oci_archive};
use artifacts::ArtifactStore;
use files::{FileBrowser, FileEntry};
use job_store::{JOB_LEASE_MINUTES, JobStore};
use registry::{DestinationRegistryAdapter, SourceRegistryAdapter, image_client};

#[derive(Clone)]
pub struct ImageTools {
    config: Arc<Config>,
    runtime: Arc<RwLock<ImageToolsRuntimeConfig>>,
    db: DatabaseConnection,
    nodes: NodeService,
    upstream: UpstreamService,
    cipher: CredentialCipher,
    artifacts: ArtifactStore,
    wake: Arc<Notify>,
    last_cleanup: Arc<Mutex<Instant>>,
    jobs: JobStore,
}

#[derive(Clone, Copy)]
struct ImageToolsRuntimeConfig {
    max_export_bytes: u64,
    export_ttl: Duration,
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

#[derive(Clone, Debug, Serialize)]
pub struct ImageJobView {
    pub id: Uuid,
    pub kind: String,
    pub status: String,
    pub source_ref: String,
    pub source_node_id: Option<Uuid>,
    pub source_credential_id: Option<Uuid>,
    pub destination_ref: Option<String>,
    pub destination_credential_id: Option<Uuid>,
    pub platform_os: String,
    pub platform_arch: String,
    pub output_format: Option<String>,
    pub resolved_digest: Option<String>,
    pub index_digest: Option<String>,
    pub stage: String,
    pub progress_bytes: i64,
    pub total_bytes: i64,
    pub artifact_name: Option<String>,
    pub error: Option<String>,
    pub cancel_requested: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<image_job::Model> for ImageJobView {
    fn from(job: image_job::Model) -> Self {
        Self {
            id: job.id,
            kind: job.kind,
            status: job.status,
            source_ref: job.source_ref,
            source_node_id: job.source_node_id,
            source_credential_id: job.source_credential_id,
            destination_ref: job.destination_ref,
            destination_credential_id: job.destination_credential_id,
            platform_os: job.platform_os,
            platform_arch: job.platform_arch,
            output_format: job.output_format,
            resolved_digest: job.resolved_digest,
            index_digest: job.index_digest,
            stage: job.stage,
            progress_bytes: job.progress_bytes,
            total_bytes: job.total_bytes,
            artifact_name: job.artifact_name,
            error: job.error,
            cancel_requested: job.cancel_requested,
            created_at: job.created_at,
            updated_at: job.updated_at,
        }
    }
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

#[derive(Clone, Debug, Serialize)]
pub struct ImageSyncRuleView {
    pub id: Uuid,
    pub name: String,
    pub enabled: bool,
    pub source_ref: String,
    pub source_node_id: Option<Uuid>,
    pub source_credential_id: Option<Uuid>,
    pub destination_ref: String,
    pub destination_credential_id: Uuid,
    pub platform_os: String,
    pub platform_arch: String,
    pub cron: String,
    pub timezone: String,
    pub last_digest: Option<String>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub next_run_at: Option<DateTime<Utc>>,
}

impl From<image_sync_rule::Model> for ImageSyncRuleView {
    fn from(rule: image_sync_rule::Model) -> Self {
        Self {
            id: rule.id,
            name: rule.name,
            enabled: rule.enabled,
            source_ref: rule.source_ref,
            source_node_id: rule.source_node_id,
            source_credential_id: rule.source_credential_id,
            destination_ref: rule.destination_ref,
            destination_credential_id: rule.destination_credential_id,
            platform_os: rule.platform_os,
            platform_arch: rule.platform_arch,
            cron: rule.cron,
            timezone: rule.timezone,
            last_digest: rule.last_digest,
            last_run_at: rule.last_run_at,
            next_run_at: rule.next_run_at,
        }
    }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JobKind {
    Export,
    Extract,
    Copy,
}

impl JobKind {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "export" => Some(Self::Export),
            "extract" => Some(Self::Extract),
            "copy" => Some(Self::Copy),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Export => "export",
            Self::Extract => "extract",
            Self::Copy => "copy",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JobStatus {
    Pending,
    Running,
    Completed,
    Skipped,
    Failed,
    Cancelled,
}

impl JobStatus {
    #[cfg(test)]
    const ALL: [Self; 6] = [
        Self::Pending,
        Self::Running,
        Self::Completed,
        Self::Skipped,
        Self::Failed,
        Self::Cancelled,
    ];

    fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "skipped" => Some(Self::Skipped),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Running | Self::Cancelled)
                | (
                    Self::Running,
                    Self::Completed | Self::Skipped | Self::Failed | Self::Cancelled
                )
                | (
                    Self::Failed | Self::Cancelled | Self::Skipped,
                    Self::Pending
                )
        )
    }
}

fn transition_update(
    from: JobStatus,
    to: JobStatus,
    now: DateTime<Utc>,
    error: Option<String>,
) -> ApiResult<UpdateMany<image_job::Entity>> {
    if !from.can_transition_to(to) {
        return Err(AppError::bad_request("illegal image job transition"));
    }

    let mut changes = image_job::ActiveModel {
        status: Set(to.as_str().into()),
        stage: Set(match to {
            JobStatus::Pending => "queued",
            JobStatus::Running => "resolving",
            _ => to.as_str(),
        }
        .into()),
        error: Set(error),
        updated_at: Set(now),
        ..Default::default()
    };
    match to {
        JobStatus::Running => {
            changes.started_at = Set(Some(now));
            changes.finished_at = Set(None);
            changes.lease_until = Set(Some(now + chrono::Duration::minutes(JOB_LEASE_MINUTES)));
            changes.cancel_requested = Set(false);
        }
        JobStatus::Pending => {
            changes.progress_bytes = Set(0);
            changes.total_bytes = Set(0);
            changes.started_at = Set(None);
            changes.finished_at = Set(None);
            changes.lease_until = Set(None);
            changes.cancel_requested = Set(false);
        }
        _ => {
            changes.finished_at = Set(Some(now));
            changes.lease_until = Set(None);
            changes.cancel_requested = Set(to == JobStatus::Cancelled);
        }
    }

    Ok(image_job::Entity::update_many()
        .set(changes)
        .filter(image_job::Column::Status.eq(from.as_str())))
}

fn abandoned_lease(now: DateTime<Utc>) -> Condition {
    Condition::any()
        .add(image_job::Column::LeaseUntil.is_null())
        .add(image_job::Column::LeaseUntil.lte(now))
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

fn retry_backoff(attempt: u32) -> Duration {
    use backoff::backoff::Backoff;
    let mut policy = backoff::ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(250))
        .with_max_interval(Duration::from_secs(2))
        .with_max_elapsed_time(None)
        .build();
    (0..attempt).for_each(|_| {
        let _ = policy.next_backoff();
    });
    policy
        .next_backoff()
        .unwrap_or_else(|| Duration::from_secs(2))
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
        let artifacts = ArtifactStore::open(&config.data_dir).await?;
        let service = Self {
            cipher: CredentialCipher::from_config(&config)?,
            upstream: UpstreamService::new(config.clone(), nodes.clone()),
            runtime: Arc::new(RwLock::new(ImageToolsRuntimeConfig {
                max_export_bytes: config.max_export_bytes,
                export_ttl: config.export_ttl,
            })),
            config,
            jobs: JobStore::new(db.clone()),
            db,
            nodes,
            artifacts,
            wake: Arc::new(Notify::new()),
            last_cleanup: Arc::new(Mutex::new(Instant::now() - Duration::from_secs(60))),
        };
        service.recover_abandoned_jobs(Utc::now()).await?;
        Ok(service)
    }

    pub async fn update_runtime(&self, config: &Config) {
        let mut runtime = self.runtime.write().await;
        runtime.max_export_bytes = config.max_export_bytes;
        runtime.export_ttl = config.export_ttl;
        self.upstream.update_runtime(config).await;
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
            .route("/jobs/{id}/purge", axum::routing::delete(purge_job))
            .route("/jobs/{id}/retry", post(retry_job))
            .route("/jobs/{id}/artifact", get(download_artifact))
            .route("/jobs/{id}/files", get(list_files))
            .route("/jobs/{id}/file", get(download_file))
            .route("/sync-rules", get(list_rules).post(create_rule))
            .route("/sync-rules/{id}", put(update_rule).delete(delete_rule))
            .route("/sync-rules/{id}/run", post(run_rule));
        router.with_state(self)
    }

    pub fn spawn(
        self,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if cancellation.is_cancelled() {
                    break;
                }
                if let Err(error) = self.tick(&cancellation).await {
                    tracing::error!(?error, "image tools worker tick failed");
                }
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = self.wake.notified() => {},
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {},
                }
            }
        })
    }

    async fn tick(&self, cancellation: &tokio_util::sync::CancellationToken) -> ApiResult<()> {
        self.cleanup_storage(None).await?;
        self.enqueue_due_rules().await?;
        self.recover_abandoned_jobs(Utc::now()).await?;
        let Some((job, attempt)) = self.jobs.claim_next().await? else {
            return Ok(());
        };

        let heartbeat = {
            let jobs = self.jobs.clone();
            let lease_lost = tokio_util::sync::CancellationToken::new();
            let heartbeat_lost = lease_lost.clone();
            let heartbeat = tokio::spawn(renew_job_lease(
                jobs,
                job.id,
                attempt,
                Duration::from_secs((JOB_LEASE_MINUTES as u64 * 60 / 3).max(1)),
                heartbeat_lost,
            ));
            (heartbeat, lease_lost)
        };
        let work_cancellation = tokio_util::sync::CancellationToken::new();
        let result = tokio::select! {
            result = self.process_job(&job, work_cancellation.clone()) => Some(result),
            _ = heartbeat.1.cancelled() => {
                work_cancellation.cancel();
                Some(Err(AppError::Conflict("image job lease was lost".into())))
            },
            _ = cancellation.cancelled() => {
                work_cancellation.cancel();
                None
            },
        };
        heartbeat.0.abort();
        let finished = match result {
            Some(result) => self.finish_job(job.id, attempt, result).await,
            None => Ok(()),
        };
        self.jobs.deactivate(job.id);
        finished
    }

    #[cfg(test)]
    async fn claim_next_job(&self) -> ApiResult<Option<image_job::Model>> {
        Ok(self.jobs.claim_next().await?.map(|(job, _)| job))
    }

    #[cfg(test)]
    async fn claim_selected_job(&self, id: Uuid) -> ApiResult<Option<image_job::Model>> {
        Ok(self.jobs.claim_selected(id).await?.map(|(job, _)| job))
    }

    async fn finish_job(
        &self,
        id: Uuid,
        attempt: i64,
        result: ApiResult<JobOutcome>,
    ) -> ApiResult<()> {
        if !self.jobs.owns(id, attempt).await? {
            return Err(AppError::Conflict("image job ownership has changed".into()));
        }
        let current = image_job::Entity::find_by_id(id)
            .one(&self.db)
            .await?
            .ok_or_else(|| AppError::not_found("image job"))?;
        let (target, error) = if current.cancel_requested {
            (JobStatus::Cancelled, None)
        } else {
            match result {
                Ok(JobOutcome::Completed) => (JobStatus::Completed, None),
                Ok(JobOutcome::Skipped) => (JobStatus::Skipped, None),
                Err(error) => (JobStatus::Failed, Some(safe_error(&error))),
            }
        };
        let finished = self
            .jobs
            .finish(
                id,
                attempt,
                target.as_str(),
                error.as_deref(),
                current.cancel_requested,
            )
            .await?;
        if finished {
            return Ok(());
        }

        Err(AppError::bad_request(
            "image job is no longer in a finishable state",
        ))
    }

    // Each worker owns a claimed job through image_job_owners and its
    // monotonically increasing attempt token. Expired leases may be reclaimed
    // by another worker; stale writes are rejected by ownership checks.
    async fn recover_abandoned_jobs(&self, now: DateTime<Utc>) -> ApiResult<u64> {
        let has_abandoned = image_job::Entity::find()
            .select_only()
            .column(image_job::Column::Id)
            .filter(image_job::Column::Status.eq(JobStatus::Running.as_str()))
            .filter(abandoned_lease(now))
            .into_tuple::<Uuid>()
            .one(&self.db)
            .await?
            .is_some();
        if !has_abandoned {
            return Ok(0);
        }

        let cancelled = transition_update(JobStatus::Running, JobStatus::Cancelled, now, None)?
            .filter(image_job::Column::CancelRequested.eq(true))
            .filter(abandoned_lease(now))
            .exec(&self.db)
            .await?;
        let recovered = image_job::Entity::update_many()
            .set(image_job::ActiveModel {
                status: Set(JobStatus::Pending.as_str().into()),
                stage: Set("recovered".into()),
                error: Set(None),
                cancel_requested: Set(false),
                lease_until: Set(None),
                started_at: Set(None),
                finished_at: Set(None),
                updated_at: Set(now),
                ..Default::default()
            })
            .filter(image_job::Column::Status.eq(JobStatus::Running.as_str()))
            .filter(image_job::Column::CancelRequested.eq(false))
            .filter(abandoned_lease(now))
            .exec(&self.db)
            .await?;
        Ok(cancelled.rows_affected + recovered.rows_affected)
    }

    async fn process_job(
        &self,
        job: &image_job::Model,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> ApiResult<JobOutcome> {
        let prepared = self.prepare_image(job).await?;
        self.update_job_manifest(job.id, &prepared).await?;
        self.check_cancelled(job.id).await?;
        if JobKind::parse(&job.kind) == Some(JobKind::Copy)
            && self
                .copy_already_completed(job, &prepared.manifest_digest)
                .await?
        {
            return Ok(JobOutcome::Skipped);
        }
        match JobKind::parse(&job.kind) {
            Some(JobKind::Export) => self.export_image(job, &prepared, cancellation).await?,
            Some(JobKind::Extract) => self.extract_image(job, &prepared).await?,
            Some(JobKind::Copy) => self.copy_image(job, &prepared).await?,
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
        let source = SourceRegistryAdapter::new(source, auth, &job.platform_os, &job.platform_arch);
        let pulled = source.pull_manifest().await?;
        let manifest = pulled.manifest;
        let digest = pulled.digest;
        let config_json = pulled.config_json;
        let index_digest = pulled.index_digest;
        let total_bytes = manifest
            .layers
            .iter()
            .map(|layer| layer.size.max(0) as u64)
            .sum::<u64>()
            .saturating_add(manifest.config.size.max(0) as u64);
        let max_export_bytes = self.runtime.read().await.max_export_bytes;
        if total_bytes > max_export_bytes {
            return Err(AppError::bad_request(
                "image exceeds DONKEY_MAX_EXPORT_BYTES",
            ));
        }

        let job_root = self.artifacts.job_root(job.id);
        let layout = job_root.join("layout");
        tokio::fs::create_dir_all(layout.join("blobs/sha256")).await?;
        let config_path = blob_path(&layout, &manifest.config.digest)?;
        let shared_config = self.artifacts.shared_blob_path(&manifest.config.digest)?;
        if tokio::fs::metadata(&shared_config).await.is_err() {
            self.artifacts
                .write_blob_bytes(job.id, &manifest.config.digest, config_json.as_bytes())
                .await?;
        }
        link_or_copy(&shared_config, &config_path).await?;

        self.set_progress(job.id, "downloading", 0, total_bytes)
            .await?;
        let mut downloaded = manifest.config.size.max(0) as u64;
        let mut layer_paths = Vec::new();
        for layer in &manifest.layers {
            self.check_cancelled(job.id).await?;
            let path = blob_path(&layout, &layer.digest)?;
            let shared = self.artifacts.shared_blob_path(&layer.digest)?;
            if tokio::fs::metadata(&shared).await.is_err() {
                let guard = self
                    .artifacts
                    .begin_blob_write(job.id, &layer.digest)
                    .await?;
                source
                    .pull_blob(layer, File::create(guard.temporary_path()).await?)
                    .await?;
                guard.commit().await?;
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
        let logical_registry =
            crate::registry_routes::normalize_registry_authority(logical.resolve_registry())?;
        let route_registry =
            crate::registry_routes::normalize_registry_authority(&node.route.canonical_registry)?;
        if logical_registry != route_registry {
            return Err(AppError::bad_request(
                "selected image source does not match its Registry route",
            ));
        }
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
        let max_export_bytes = self.runtime.read().await.max_export_bytes;
        if total_bytes > max_export_bytes {
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

        let job_root = self.artifacts.job_root(job.id);
        let layout = job_root.join("layout");
        tokio::fs::create_dir_all(layout.join("blobs/sha256")).await?;
        let layout_config = blob_path(&layout, &manifest.config.digest)?;
        let shared_config = self.artifacts.shared_blob_path(&manifest.config.digest)?;
        if tokio::fs::metadata(&shared_config).await.is_err() {
            self.artifacts
                .write_blob_bytes(job.id, &manifest.config.digest, config_json.as_bytes())
                .await?;
        }
        link_or_copy(&shared_config, &layout_config).await?;

        self.set_progress(job.id, "downloading", 0, total_bytes)
            .await?;
        let mut downloaded = manifest.config.size.max(0) as u64;
        let mut layer_paths = Vec::new();
        for layer in &manifest.layers {
            self.check_cancelled(job.id).await?;
            let path = blob_path(&layout, &layer.digest)?;
            let shared = self.artifacts.shared_blob_path(&layer.digest)?;
            if tokio::fs::metadata(&shared).await.is_err() {
                let guard = self
                    .artifacts
                    .begin_blob_write(job.id, &layer.digest)
                    .await?;
                let result = self
                    .download_node_blob(&node, logical.repository(), layer, guard.temporary_path())
                    .await;
                if result.is_err() {
                    let _ = tokio::fs::remove_file(guard.temporary_path()).await;
                }
                result?;
                guard.commit().await?;
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
                        tokio::time::sleep(retry_backoff(attempt)).await;
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
                        tokio::time::sleep(retry_backoff(attempt)).await;
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

    async fn export_image(
        &self,
        job: &image_job::Model,
        image: &PreparedImage,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> ApiResult<()> {
        self.set_stage(job.id, "archiving").await?;
        let format = job.output_format.as_deref().unwrap_or("docker");
        if !matches!(format, "docker" | "oci") {
            return Err(AppError::bad_request("output format must be docker or oci"));
        }
        let name = safe_artifact_name(&job.source_ref, format);
        let output = self.artifacts.job_root(job.id).join(&name);
        let output_clone = output.clone();
        let archive = if format == "docker" {
            ArchiveInput::Docker {
                config_json: image.config_json.clone(),
                reference: job.source_ref.clone(),
                layers: image.layer_paths.clone(),
                media_types: image
                    .manifest
                    .layers
                    .iter()
                    .map(|layer| layer.media_type.clone())
                    .collect(),
            }
        } else {
            ArchiveInput::Oci {
                layout: image.layout.clone(),
            }
        };
        let job_lock = self.artifacts.lock_job(job.id).await?;
        tokio::task::spawn_blocking(move || {
            let _job_lock = job_lock;
            build_archive(&output_clone, archive, cancellation)
        })
        .await
        .map_err(AppError::internal)?
        .map_err(AppError::internal)?;
        self.set_artifact(job.id, &output, &name).await
    }

    #[cfg(unix)]
    async fn extract_image(&self, job: &image_job::Model, image: &PreparedImage) -> ApiResult<()> {
        self.set_stage(job.id, "extracting").await?;
        let _job_lock = self.artifacts.lock_job(job.id).await?;
        let rootfs = self.artifacts.job_root(job.id).join("rootfs");
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
        let destination = DestinationRegistryAdapter::new(client, destination, auth);
        destination.authenticate().await?;

        for (layer, path) in image.manifest.layers.iter().zip(&image.layer_paths) {
            self.check_cancelled(job.id).await?;
            if !destination.blob_exists(&layer.digest).await? {
                destination.push_file(path, &layer.digest).await?;
            }
        }
        if !destination
            .blob_exists(&image.manifest.config.digest)
            .await?
        {
            destination
                .push_bytes(
                    image.config_json.as_bytes().to_vec(),
                    &image.manifest.config.digest,
                )
                .await?;
        }
        destination.push_manifest(image.manifest.clone()).await
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
        self.jobs
            .update_manifest(
                id,
                &image.manifest_digest,
                image.index_digest.as_deref(),
                image.total_bytes,
            )
            .await
    }

    async fn set_progress(&self, id: Uuid, stage: &str, current: u64, total: u64) -> ApiResult<()> {
        self.jobs.update_progress(id, stage, current, total).await
    }

    async fn set_stage(&self, id: Uuid, stage: &str) -> ApiResult<()> {
        self.jobs.update_stage(id, stage).await
    }

    async fn set_artifact(&self, id: Uuid, path: &Path, name: &str) -> ApiResult<()> {
        self.jobs.update_artifact(id, path, name).await
    }

    async fn check_cancelled(&self, id: Uuid) -> ApiResult<()> {
        if let Some(attempt) = self.jobs.active_attempt(id)
            && !self.jobs.owns(id, attempt).await?
        {
            return Err(AppError::Conflict("image job lease was lost".into()));
        }
        if self.jobs.is_cancelled(id).await? {
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
            let _ = self
                .create_rule_job(rule.id, rule.next_run_at.or(Some(now)), "schedule")
                .await?;
        }
        Ok(())
    }

    async fn create_rule_job(
        &self,
        rule_id: Uuid,
        scheduled_for: Option<DateTime<Utc>>,
        trigger: &'static str,
    ) -> ApiResult<Option<image_job::Model>> {
        let transaction = self
            .db
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await?;
        let rule = image_sync_rule::Entity::find_by_id(rule_id)
            .one(&transaction)
            .await?
            .ok_or_else(|| AppError::not_found("sync rule"))?;
        let now = Utc::now();
        if trigger == "schedule"
            && (!rule.enabled
                || rule.next_run_at != scheduled_for
                || rule.next_run_at.is_none_or(|next| next > now))
        {
            transaction.commit().await?;
            return Ok(None);
        }
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
        validate_job(&input)?;
        let idempotency_key = (trigger == "schedule").then(|| {
            format!(
                "sync-rule:{}:{}",
                rule.id,
                scheduled_for.unwrap_or(now).timestamp_millis()
            )
        });
        let existing = if let Some(key) = idempotency_key.as_deref() {
            image_job::Entity::find()
                .filter(image_job::Column::IdempotencyKey.eq(key))
                .one(&transaction)
                .await?
        } else {
            None
        };
        let job = if let Some(existing) = existing {
            existing
        } else {
            let job = pending_job(input, JobKind::Copy, idempotency_key, now)
                .into_active_model()
                .insert(&transaction)
                .await?;
            db::insert_image_job_lineage(&transaction, job.id, rule.id, scheduled_for, trigger)
                .await?;
            job
        };
        if trigger == "schedule" {
            let next = next_run(&rule.cron, &rule.timezone)?;
            let mut active = rule.into_active_model();
            active.next_run_at = Set(Some(next));
            active.updated_at = Set(now);
            active.update(&transaction).await?;
        }
        transaction.commit().await?;
        self.wake.notify_one();
        Ok(Some(job))
    }

    async fn create_job(
        &self,
        input: JobInput,
        idempotency_key: Option<String>,
    ) -> ApiResult<image_job::Model> {
        validate_job(&input)?;
        let kind = JobKind::parse(&input.kind)
            .ok_or_else(|| AppError::bad_request("unsupported image job kind"))?;
        if let Some(key) = idempotency_key.as_deref()
            && let Some(existing) = image_job::Entity::find()
                .filter(image_job::Column::IdempotencyKey.eq(key))
                .one(&self.db)
                .await?
        {
            return Ok(existing);
        }
        let now = Utc::now();
        let model = pending_job(input, kind, idempotency_key.clone(), now);
        let model = match model.into_active_model().insert(&self.db).await {
            Ok(model) => model,
            Err(error) if idempotency_key.is_some() => {
                if let Some(existing) = image_job::Entity::find()
                    .filter(image_job::Column::IdempotencyKey.eq(idempotency_key.clone()))
                    .one(&self.db)
                    .await?
                {
                    return Ok(existing);
                }
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        };
        self.wake.notify_one();
        Ok(model)
    }

    async fn cleanup_storage(&self, protected_job: Option<Uuid>) -> ApiResult<()> {
        if protected_job.is_none() {
            let mut last_cleanup = self.last_cleanup.lock().await;
            if last_cleanup.elapsed() < Duration::from_secs(60) {
                return Ok(());
            }
            *last_cleanup = Instant::now();
        }
        let runtime = *self.runtime.read().await;
        let cutoff = Utc::now()
            - chrono::Duration::from_std(runtime.export_ttl).map_err(AppError::internal)?;
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

        self.artifacts.gc_shared_blobs().await?;
        let mut used = self.artifacts.usage().await?;
        if used <= runtime.max_export_bytes {
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
            self.artifacts.gc_shared_blobs().await?;
            used = self.artifacts.usage().await?;
            if used <= runtime.max_export_bytes {
                break;
            }
        }
        Ok(())
    }

    async fn enforce_storage_quota(&self, job_id: Uuid) -> ApiResult<()> {
        self.cleanup_storage(Some(job_id)).await?;
        let max_export_bytes = self.runtime.read().await.max_export_bytes;
        if self.artifacts.usage().await? <= max_export_bytes {
            return Ok(());
        }
        self.remove_job_storage(job_id).await?;
        self.artifacts.gc_shared_blobs().await?;
        Err(AppError::bad_request(
            "image tools storage exceeds DONKEY_MAX_EXPORT_BYTES",
        ))
    }

    async fn remove_job_storage(&self, id: Uuid) -> ApiResult<()> {
        self.artifacts.remove_job(id).await?;
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
}

async fn renew_job_lease(
    jobs: JobStore,
    job_id: Uuid,
    attempt: i64,
    interval: Duration,
    lease_lost: tokio_util::sync::CancellationToken,
) {
    use backoff::backoff::Backoff;

    loop {
        tokio::time::sleep(interval).await;
        let mut retry = backoff::ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(250))
            .with_max_interval(Duration::from_secs(2))
            .with_max_elapsed_time(Some(Duration::from_secs(30)))
            .build();
        loop {
            match jobs.renew(job_id, attempt).await {
                Ok(true) => break,
                Ok(false) => {
                    lease_lost.cancel();
                    return;
                }
                Err(error) => {
                    let Some(delay) = retry.next_backoff() else {
                        tracing::error!(?error, %job_id, "image job lease renewal failed");
                        lease_lost.cancel();
                        return;
                    };
                    tokio::time::sleep(delay).await;
                }
            }
        }
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
) -> ApiResult<Json<Vec<ImageJobView>>> {
    Ok(Json(
        image_job::Entity::find()
            .order_by_desc(image_job::Column::CreatedAt)
            .limit(query.limit.min(500))
            .all(&service.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect(),
    ))
}

async fn create_job(
    State(service): State<ImageTools>,
    headers: HeaderMap,
    Json(input): Json<JobInput>,
) -> ApiResult<(StatusCode, Json<ImageJobView>)> {
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    Ok((
        StatusCode::CREATED,
        Json(service.create_job(input, key).await?.into()),
    ))
}

async fn get_job(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<ImageJobView>> {
    Ok(Json(
        image_job::Entity::find_by_id(id)
            .one(&service.db)
            .await?
            .ok_or_else(|| AppError::not_found("image job"))?
            .into(),
    ))
}

async fn cancel_job(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    let model = image_job::Entity::find_by_id(id)
        .one(&service.db)
        .await?
        .ok_or_else(|| AppError::not_found("image job"))?;
    let status = JobStatus::parse(&model.status)
        .ok_or_else(|| AppError::bad_request("image job has an unknown status"))?;
    cancel_job_from_status(&service, id, status).await
}

async fn purge_job(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    let job = image_job::Entity::find_by_id(id)
        .one(&service.db)
        .await?
        .ok_or_else(|| AppError::not_found("image job"))?;
    let status = JobStatus::parse(&job.status)
        .ok_or_else(|| AppError::bad_request("image job has an unknown status"))?;
    if matches!(status, JobStatus::Pending | JobStatus::Running) {
        return Err(AppError::bad_request(
            "running image jobs must be cancelled first",
        ));
    }
    service.remove_job_storage(id).await?;
    image_job::Entity::delete_by_id(id)
        .exec(&service.db)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn cancel_job_from_status(
    service: &ImageTools,
    id: Uuid,
    mut status: JobStatus,
) -> ApiResult<StatusCode> {
    loop {
        let now = Utc::now();
        let result = match status {
            JobStatus::Pending => {
                transition_update(JobStatus::Pending, JobStatus::Cancelled, now, None)?
                    .filter(image_job::Column::Id.eq(id))
                    .exec(&service.db)
                    .await?
            }
            JobStatus::Running => {
                image_job::Entity::update_many()
                    .col_expr(
                        image_job::Column::CancelRequested,
                        sea_orm::sea_query::Expr::value(true),
                    )
                    .col_expr(
                        image_job::Column::UpdatedAt,
                        sea_orm::sea_query::Expr::value(now),
                    )
                    .filter(image_job::Column::Id.eq(id))
                    .filter(image_job::Column::Status.eq(JobStatus::Running.as_str()))
                    .exec(&service.db)
                    .await?
            }
            _ => return Err(AppError::bad_request("image job cannot be cancelled")),
        };
        if result.rows_affected == 1 {
            return Ok(StatusCode::NO_CONTENT);
        }

        let model = image_job::Entity::find_by_id(id)
            .one(&service.db)
            .await?
            .ok_or_else(|| AppError::not_found("image job"))?;
        if model.status == JobStatus::Running.as_str() && model.cancel_requested {
            return Ok(StatusCode::NO_CONTENT);
        }
        status = JobStatus::parse(&model.status)
            .ok_or_else(|| AppError::bad_request("image job has an unknown status"))?;
    }
}

async fn retry_job(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<Json<ImageJobView>> {
    let model = image_job::Entity::find_by_id(id)
        .one(&service.db)
        .await?
        .ok_or_else(|| AppError::not_found("image job"))?;
    let status = JobStatus::parse(&model.status)
        .ok_or_else(|| AppError::bad_request("image job has an unknown status"))?;
    if !status.can_transition_to(JobStatus::Pending) {
        return Err(AppError::bad_request("image job cannot be retried"));
    }
    let result = transition_update(status, JobStatus::Pending, Utc::now(), None)?
        .filter(image_job::Column::Id.eq(id))
        .exec(&service.db)
        .await?;
    if result.rows_affected != 1 {
        if image_job::Entity::find_by_id(id)
            .one(&service.db)
            .await?
            .is_none()
        {
            return Err(AppError::not_found("image job"));
        }
        return Err(AppError::bad_request("image job cannot be retried"));
    }
    let model = image_job::Entity::find_by_id(id)
        .one(&service.db)
        .await?
        .ok_or_else(|| AppError::not_found("image job"))?;
    service.wake.notify_one();
    Ok(Json(model.into()))
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
    FileBrowser::serve_artifact(path, job.artifact_name.as_deref(), request).await
}

async fn list_files(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
    Query(query): Query<FileQuery>,
) -> ApiResult<Json<Vec<FileEntry>>> {
    let root = extracted_root(&service.db, id).await?;
    Ok(Json(FileBrowser::list(&root, &query.path).await?))
}

async fn download_file(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
    Query(query): Query<FileQuery>,
    request: Request,
) -> ApiResult<Response> {
    let root = extracted_root(&service.db, id).await?;
    FileBrowser::serve_file(&root, &query.path, request).await
}

async fn list_rules(State(service): State<ImageTools>) -> ApiResult<Json<Vec<ImageSyncRuleView>>> {
    Ok(Json(
        image_sync_rule::Entity::find()
            .order_by_asc(image_sync_rule::Column::Name)
            .all(&service.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect(),
    ))
}

async fn create_rule(
    State(service): State<ImageTools>,
    Json(input): Json<SyncRuleInput>,
) -> ApiResult<(StatusCode, Json<ImageSyncRuleView>)> {
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
    Ok((StatusCode::CREATED, Json(model.into())))
}

async fn update_rule(
    State(service): State<ImageTools>,
    AxumPath(id): AxumPath<Uuid>,
    Json(input): Json<SyncRuleInput>,
) -> ApiResult<Json<ImageSyncRuleView>> {
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
    Ok(Json(active.update(&service.db).await?.into()))
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
) -> ApiResult<(StatusCode, Json<ImageJobView>)> {
    let job = service
        .create_rule_job(id, None, "manual")
        .await?
        .ok_or_else(|| AppError::not_found("sync rule"))?;
    Ok((StatusCode::CREATED, Json(job.into())))
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
    if JobKind::parse(&input.kind).is_none() {
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

fn pending_job(
    input: JobInput,
    kind: JobKind,
    idempotency_key: Option<String>,
    now: DateTime<Utc>,
) -> image_job::Model {
    image_job::Model {
        id: Uuid::new_v4(),
        kind: kind.as_str().into(),
        status: JobStatus::Pending.as_str().into(),
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
    }
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

fn safe_error(error: &AppError) -> String {
    match error {
        AppError::BadRequest(message) => message.chars().take(500).collect(),
        AppError::Upstream(_) => "upstream Registry request failed; check server logs".into(),
        AppError::Integrity => "image content integrity check failed".into(),
        _ => "image task failed; check server logs".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use sea_orm::ConnectionTrait;
    use secrecy::SecretString;
    use std::{collections::HashMap, io::Read};

    async fn test_service() -> (tempfile::TempDir, ImageTools) {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.credential_key = Some(SecretString::from("77".repeat(32)));
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let config = Arc::new(config);
        let nodes = NodeService::new(config.clone(), db.clone()).unwrap();
        let service = ImageTools::new(config, db, nodes).await.unwrap();
        (directory, service)
    }

    fn test_job(status: JobStatus, lease_until: Option<DateTime<Utc>>) -> image_job::Model {
        let now = Utc::now();
        let terminal = matches!(
            status,
            JobStatus::Completed | JobStatus::Skipped | JobStatus::Failed | JobStatus::Cancelled
        );
        image_job::Model {
            id: Uuid::new_v4(),
            kind: JobKind::Export.as_str().into(),
            status: status.as_str().into(),
            source_ref: "docker.io/library/alpine:latest".into(),
            source_node_id: None,
            source_credential_id: None,
            destination_ref: None,
            destination_credential_id: None,
            platform_os: "linux".into(),
            platform_arch: "amd64".into(),
            output_format: Some("docker".into()),
            resolved_digest: None,
            index_digest: None,
            stage: status.as_str().into(),
            progress_bytes: 12,
            total_bytes: 34,
            artifact_path: None,
            artifact_name: None,
            error: terminal.then(|| "previous error".into()),
            idempotency_key: None,
            cancel_requested: status == JobStatus::Cancelled,
            lease_until,
            created_at: now,
            updated_at: now,
            started_at: (status != JobStatus::Pending).then_some(now),
            finished_at: terminal.then_some(now),
        }
    }

    async fn insert_test_job(
        db: &DatabaseConnection,
        status: JobStatus,
        lease_until: Option<DateTime<Utc>>,
    ) -> image_job::Model {
        test_job(status, lease_until)
            .into_active_model()
            .insert(db)
            .await
            .unwrap()
    }

    async fn insert_cancel_requested_job(
        db: &DatabaseConnection,
        lease_until: Option<DateTime<Utc>>,
    ) -> image_job::Model {
        let mut job = test_job(JobStatus::Running, lease_until);
        job.cancel_requested = true;
        job.into_active_model().insert(db).await.unwrap()
    }

    async fn insert_test_rule(
        db: &DatabaseConnection,
        next_run_at: DateTime<Utc>,
    ) -> image_sync_rule::Model {
        let now = Utc::now();
        let destination_credential_id = Uuid::new_v4();
        registry_credential::Model {
            id: destination_credential_id,
            name: "scheduled destination".into(),
            registry: "registry.example".into(),
            auth_mode: "bearer".into(),
            username: None,
            secret_enc: "encrypted-test-secret".into(),
            generation: 1,
            created_at: now,
            updated_at: now,
        }
        .into_active_model()
        .insert(db)
        .await
        .unwrap();
        image_sync_rule::Model {
            id: Uuid::new_v4(),
            name: "scheduled copy".into(),
            enabled: true,
            source_ref: "docker.io/library/alpine:latest".into(),
            source_node_id: None,
            source_credential_id: None,
            destination_ref: "registry.example/alpine:latest".into(),
            destination_credential_id,
            platform_os: "linux".into(),
            platform_arch: "amd64".into(),
            cron: "0 * * * * *".into(),
            timezone: "UTC".into(),
            last_digest: None,
            last_run_at: None,
            next_run_at: Some(next_run_at),
            created_at: now,
            updated_at: now,
        }
        .into_active_model()
        .insert(db)
        .await
        .unwrap()
    }

    #[test]
    fn job_status_transition_matrix_is_explicit_and_strings_are_stable() {
        for from in JobStatus::ALL {
            assert_eq!(JobStatus::parse(from.as_str()), Some(from));
            for to in JobStatus::ALL {
                let expected = matches!(
                    (from, to),
                    (
                        JobStatus::Pending,
                        JobStatus::Running | JobStatus::Cancelled
                    ) | (
                        JobStatus::Running,
                        JobStatus::Completed
                            | JobStatus::Skipped
                            | JobStatus::Failed
                            | JobStatus::Cancelled
                    ) | (
                        JobStatus::Failed | JobStatus::Cancelled | JobStatus::Skipped,
                        JobStatus::Pending
                    )
                );
                assert_eq!(
                    from.can_transition_to(to),
                    expected,
                    "{} -> {}",
                    from.as_str(),
                    to.as_str()
                );
            }
        }
        for (value, kind) in [
            ("export", JobKind::Export),
            ("extract", JobKind::Extract),
            ("copy", JobKind::Copy),
        ] {
            assert_eq!(JobKind::parse(value), Some(kind));
            assert_eq!(kind.as_str(), value);
        }
    }

    #[test]
    fn docker_archive_contains_loadable_manifest_references() {
        let directory = tempfile::tempdir().unwrap();
        let layer = directory.path().join("layer.tar.gz");
        std::fs::write(&layer, b"layer bytes").unwrap();
        let output = directory.path().join("image-docker.tar");
        let config = r#"{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]},"history":[],"config":{}}"#;
        build_docker_archive(
            &output,
            config,
            "docker.io/library/test:latest".into(),
            &[layer],
            &["application/vnd.oci.image.layer.v1.tar+gzip".into()],
        )
        .unwrap();

        let mut files = HashMap::new();
        let mut archive = tar::Archive::new(std::fs::File::open(output).unwrap());
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().into_owned();
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).unwrap();
            files.insert(path, bytes);
        }
        let manifest: Vec<serde_json::Value> =
            serde_json::from_slice(files.get("manifest.json").unwrap()).unwrap();
        let item = &manifest[0];
        let config_path = item["Config"].as_str().unwrap();
        assert!(files.contains_key(config_path));
        assert_eq!(
            item["RepoTags"].as_array().unwrap()[0].as_str(),
            Some("docker.io/library/test:latest")
        );
        for layer in item["Layers"].as_array().unwrap() {
            assert!(files.contains_key(layer.as_str().unwrap()));
        }
    }

    #[test]
    fn oci_archive_contains_only_oci_layout_roots() {
        let directory = tempfile::tempdir().unwrap();
        let layout = directory.path().join("layout");
        std::fs::create_dir_all(layout.join("blobs/sha256")).unwrap();
        std::fs::write(
            layout.join("oci-layout"),
            br#"{"imageLayoutVersion":"1.0.0"}"#,
        )
        .unwrap();
        std::fs::write(layout.join("index.json"), br#"{"schemaVersion":2}"#).unwrap();
        std::fs::write(layout.join("blobs/sha256/abc"), b"blob").unwrap();
        let output = directory.path().join("image-oci.tar");
        build_oci_archive(&output, &layout).unwrap();

        let mut archive = tar::Archive::new(std::fs::File::open(output).unwrap());
        let paths = archive
            .entries()
            .unwrap()
            .map(|entry| {
                entry
                    .unwrap()
                    .path()
                    .unwrap()
                    .to_string_lossy()
                    .trim_end_matches('/')
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert!(paths.iter().any(|path| path == "oci-layout"));
        assert!(paths.iter().any(|path| path == "index.json"));
        assert!(paths.iter().any(|path| path == "blobs/sha256/abc"));
        assert!(!paths.iter().any(|path| path == "manifest.json"));
    }

    #[test]
    fn docker_archive_fixture_for_ci_load() {
        let Some(output) = std::env::var_os("DONKEY_DOCKER_ARCHIVE_OUTPUT").map(PathBuf::from)
        else {
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let layer = directory.path().join("layer.tar");
        let file = std::fs::File::create(&layer).unwrap();
        let mut layer_builder = tar::Builder::new(file);
        let payload = b"donkey archive fixture\n";
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(payload.len() as u64);
        header.set_cksum();
        layer_builder
            .append_data(&mut header, "fixture.txt", &payload[..])
            .unwrap();
        layer_builder.finish().unwrap();
        let layer_bytes = std::fs::read(&layer).unwrap();
        let diff_id = format!("sha256:{:x}", Sha256::digest(&layer_bytes));
        let config = serde_json::json!({
            "architecture": "amd64",
            "os": "linux",
            "rootfs": { "type": "layers", "diff_ids": [diff_id] },
            "history": [{ "created_by": "donkey archive CI fixture" }],
            "config": { "Cmd": ["/bin/sh"] }
        })
        .to_string();
        build_docker_archive(
            &output,
            &config,
            "donkey/archive-fixture:latest".into(),
            &[layer],
            &["application/vnd.oci.image.layer.v1.tar".into()],
        )
        .unwrap();
        assert!(output.is_file());
    }

    #[tokio::test]
    async fn same_selected_id_has_only_one_claim_cas_winner() {
        let (_directory, service) = test_service().await;
        let job = insert_test_job(&service.db, JobStatus::Pending, None).await;

        let (first, second) = tokio::join!(
            service.claim_selected_job(job.id),
            service.claim_selected_job(job.id)
        );
        let claimed = [first.unwrap(), second.unwrap()];
        assert_eq!(claimed.iter().filter(|job| job.is_some()).count(), 1);
        assert_eq!(claimed.iter().flatten().next().unwrap().id, job.id);

        let stored = image_job::Entity::find_by_id(job.id)
            .one(&service.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, JobStatus::Running.as_str());
        assert_eq!(stored.stage, "resolving");
        assert!(stored.started_at.is_some());
        assert!(stored.lease_until.is_some_and(|lease| lease > Utc::now()));
    }

    #[tokio::test]
    async fn cancel_retries_when_claim_wins_after_pending_read() {
        let (_directory, service) = test_service().await;
        let job = insert_test_job(&service.db, JobStatus::Pending, None).await;
        let (read_tx, read_rx) = tokio::sync::oneshot::channel();
        let (claimed_tx, claimed_rx) = tokio::sync::oneshot::channel();
        let cancel_service = service.clone();

        let cancel = tokio::spawn(async move {
            let observed = image_job::Entity::find_by_id(job.id)
                .one(&cancel_service.db)
                .await
                .unwrap()
                .unwrap();
            let status = JobStatus::parse(&observed.status).unwrap();
            read_tx.send(()).unwrap();
            claimed_rx.await.unwrap();
            cancel_job_from_status(&cancel_service, job.id, status).await
        });
        read_rx.await.unwrap();
        assert!(service.claim_next_job().await.unwrap().is_some());
        claimed_tx.send(()).unwrap();

        assert_eq!(cancel.await.unwrap().unwrap(), StatusCode::NO_CONTENT);
        let stored = image_job::Entity::find_by_id(job.id)
            .one(&service.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, JobStatus::Running.as_str());
        assert!(stored.cancel_requested);
    }

    #[tokio::test]
    async fn startup_recovers_only_missing_or_expired_leases() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.credential_key = Some(SecretString::from("88".repeat(32)));
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let now = Utc::now();
        let missing = insert_test_job(&db, JobStatus::Running, None).await;
        let expired = insert_test_job(
            &db,
            JobStatus::Running,
            Some(now - chrono::Duration::minutes(1)),
        )
        .await;
        let live = insert_test_job(
            &db,
            JobStatus::Running,
            Some(now + chrono::Duration::minutes(5)),
        )
        .await;
        let config = Arc::new(config);
        let nodes = NodeService::new(config.clone(), db.clone()).unwrap();

        let service = ImageTools::new(config, db, nodes).await.unwrap();
        for id in [missing.id, expired.id] {
            let recovered = image_job::Entity::find_by_id(id)
                .one(&service.db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(recovered.status, JobStatus::Pending.as_str());
            assert_eq!(recovered.stage, "recovered");
            assert!(recovered.lease_until.is_none());
            assert!(recovered.started_at.is_none());
        }
        let live = image_job::Entity::find_by_id(live.id)
            .one(&service.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(live.status, JobStatus::Running.as_str());
        assert_eq!(live.lease_until, Some(now + chrono::Duration::minutes(5)));
    }

    #[tokio::test]
    async fn worker_stops_after_cancellation() {
        let (_directory, service) = test_service().await;
        let cancellation = tokio_util::sync::CancellationToken::new();
        let worker = service.spawn(cancellation.clone());
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn startup_finishes_abandoned_cancel_requests_without_reexecution() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.credential_key = Some(SecretString::from("99".repeat(32)));
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let now = Utc::now();
        let missing = insert_cancel_requested_job(&db, None).await;
        let expired =
            insert_cancel_requested_job(&db, Some(now - chrono::Duration::minutes(1))).await;
        let live = insert_cancel_requested_job(&db, Some(now + chrono::Duration::minutes(5))).await;
        let config = Arc::new(config);
        let nodes = NodeService::new(config.clone(), db.clone()).unwrap();

        let service = ImageTools::new(config, db, nodes).await.unwrap();
        for id in [missing.id, expired.id] {
            let cancelled = image_job::Entity::find_by_id(id)
                .one(&service.db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(cancelled.status, JobStatus::Cancelled.as_str());
            assert_eq!(cancelled.stage, JobStatus::Cancelled.as_str());
            assert!(cancelled.cancel_requested);
            assert!(cancelled.lease_until.is_none());
            assert!(cancelled.finished_at.is_some());
        }
        let live = image_job::Entity::find_by_id(live.id)
            .one(&service.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(live.status, JobStatus::Running.as_str());
        assert!(live.cancel_requested);
        assert_eq!(live.lease_until, Some(now + chrono::Duration::minutes(5)));
    }

    #[tokio::test]
    async fn later_tick_recovers_jobs_preserved_with_future_leases_at_startup() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.credential_key = Some(SecretString::from("aa".repeat(32)));
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let now = Utc::now();
        let mut abandoned = test_job(JobStatus::Running, Some(now + chrono::Duration::minutes(5)));
        abandoned.source_ref.clear();
        let abandoned = abandoned.into_active_model().insert(&db).await.unwrap();
        let cancelled =
            insert_cancel_requested_job(&db, Some(now + chrono::Duration::minutes(5))).await;
        let config = Arc::new(config);
        let nodes = NodeService::new(config.clone(), db.clone()).unwrap();
        let service = ImageTools::new(config, db, nodes).await.unwrap();

        for id in [abandoned.id, cancelled.id] {
            let preserved = image_job::Entity::find_by_id(id)
                .one(&service.db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(preserved.status, JobStatus::Running.as_str());
        }
        image_job::Entity::update_many()
            .col_expr(
                image_job::Column::LeaseUntil,
                sea_orm::sea_query::Expr::value(Some(now - chrono::Duration::minutes(1))),
            )
            .filter(image_job::Column::Id.is_in([abandoned.id, cancelled.id]))
            .exec(&service.db)
            .await
            .unwrap();

        service
            .tick(&tokio_util::sync::CancellationToken::new())
            .await
            .unwrap();

        let abandoned = image_job::Entity::find_by_id(abandoned.id)
            .one(&service.db)
            .await
            .unwrap()
            .unwrap();
        let cancelled = image_job::Entity::find_by_id(cancelled.id)
            .one(&service.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(abandoned.status, JobStatus::Failed.as_str());
        assert_eq!(cancelled.status, JobStatus::Cancelled.as_str());
    }

    #[tokio::test]
    async fn stage_and_progress_updates_refresh_running_lease() {
        let (_directory, service) = test_service().await;
        let old_lease = Utc::now() + chrono::Duration::seconds(1);
        let job = insert_test_job(&service.db, JobStatus::Running, Some(old_lease)).await;

        service.set_stage(job.id, "packing").await.unwrap();
        service
            .set_progress(job.id, "packing", 20, 40)
            .await
            .unwrap();

        let stored = image_job::Entity::find_by_id(job.id)
            .one(&service.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.stage, "packing");
        assert_eq!((stored.progress_bytes, stored.total_bytes), (20, 40));
        assert!(stored.lease_until.is_some_and(|lease| lease > old_lease));
    }

    #[tokio::test]
    async fn stale_worker_cannot_update_after_fencing_token_changes() {
        let (_directory, service) = test_service().await;
        let job = insert_test_job(&service.db, JobStatus::Pending, None).await;
        let now = Utc::now();
        let lease = now + chrono::Duration::minutes(JOB_LEASE_MINUTES);
        let attempt =
            db::claim_image_job(&service.db, job.id, service.jobs.worker_id(), now, lease)
                .await
                .unwrap()
                .unwrap();
        service.jobs.activate(job.id, attempt);

        // Simulate another worker taking over the job with a newer fencing token.
        let replacement_worker = Uuid::new_v4();
        service
            .db
            .execute_raw(sea_orm::Statement::from_sql_and_values(
                sea_orm::DbBackend::Sqlite,
                "UPDATE image_job_owners SET worker_id = ?, attempt = ? WHERE job_id = ?",
                [
                    replacement_worker.into(),
                    (attempt + 1).into(),
                    job.id.into(),
                ],
            ))
            .await
            .unwrap();

        let result = service.set_stage(job.id, "packing").await;
        assert!(matches!(result, Err(AppError::Conflict(_))));
        let stored = image_job::Entity::find_by_id(job.id)
            .one(&service.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.stage, "resolving");
    }

    #[tokio::test]
    async fn heartbeat_cancels_work_after_lease_is_lost() {
        let (_directory, service) = test_service().await;
        let job = insert_test_job(&service.db, JobStatus::Pending, None).await;
        let (_, attempt) = service.jobs.claim_selected(job.id).await.unwrap().unwrap();
        service
            .db
            .execute_raw(sea_orm::Statement::from_sql_and_values(
                sea_orm::DbBackend::Sqlite,
                "UPDATE image_job_owners SET worker_id = ?, attempt = ? WHERE job_id = ?",
                [Uuid::new_v4().into(), (attempt + 1).into(), job.id.into()],
            ))
            .await
            .unwrap();
        let lease_lost = tokio_util::sync::CancellationToken::new();
        let heartbeat = tokio::spawn(renew_job_lease(
            service.jobs.clone(),
            job.id,
            attempt,
            Duration::from_millis(1),
            lease_lost.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(1), lease_lost.cancelled())
            .await
            .unwrap();
        heartbeat.await.unwrap();
        assert!(matches!(
            service.check_cancelled(job.id).await,
            Err(AppError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn concurrent_schedule_enqueues_once_and_advances_rule_atomically() {
        let (_directory, service) = test_service().await;
        let scheduled_for = Utc::now() - chrono::Duration::seconds(1);
        let rule = insert_test_rule(&service.db, scheduled_for).await;
        let first = service.clone();
        let second = service.clone();
        let (left, right) = tokio::join!(
            first.create_rule_job(rule.id, Some(scheduled_for), "schedule"),
            second.create_rule_job(rule.id, Some(scheduled_for), "schedule")
        );
        assert!(left.unwrap().is_some() ^ right.unwrap().is_some());

        let jobs = image_job::Entity::find().all(&service.db).await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(
            db::image_job_sync_rule(&service.db, jobs[0].id)
                .await
                .unwrap(),
            Some(rule.id)
        );
        let stored = image_sync_rule::Entity::find_by_id(rule.id)
            .one(&service.db)
            .await
            .unwrap()
            .unwrap();
        assert!(stored.next_run_at.is_some_and(|next| next > Utc::now()));
        assert!(stored.last_run_at.is_none());
    }

    #[tokio::test]
    async fn manual_rule_job_keeps_schedule_and_records_lineage() {
        let (_directory, service) = test_service().await;
        let scheduled_for = Utc::now() + chrono::Duration::hours(1);
        let rule = insert_test_rule(&service.db, scheduled_for).await;
        let job = service
            .create_rule_job(rule.id, None, "manual")
            .await
            .unwrap()
            .unwrap();
        let stored = image_sync_rule::Entity::find_by_id(rule.id)
            .one(&service.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.next_run_at, Some(scheduled_for));
        assert_eq!(
            db::image_job_sync_rule(&service.db, job.id).await.unwrap(),
            Some(rule.id)
        );
    }

    #[tokio::test]
    async fn successful_job_updates_only_its_lineage_rule() {
        let (_directory, service) = test_service().await;
        let next = Utc::now() + chrono::Duration::hours(1);
        let first = insert_test_rule(&service.db, next).await;
        let second = insert_test_rule(&service.db, next).await;
        let job = service
            .create_rule_job(first.id, None, "manual")
            .await
            .unwrap()
            .unwrap();
        let now = Utc::now();
        let attempt = db::claim_image_job(
            &service.db,
            job.id,
            service.jobs.worker_id(),
            now,
            now + chrono::Duration::minutes(JOB_LEASE_MINUTES),
        )
        .await
        .unwrap()
        .unwrap();
        image_job::Entity::update_many()
            .col_expr(
                image_job::Column::ResolvedDigest,
                sea_orm::sea_query::Expr::value(Some("sha256:lineage")),
            )
            .filter(image_job::Column::Id.eq(job.id))
            .exec(&service.db)
            .await
            .unwrap();
        assert!(
            db::finish_image_job_owned(
                &service.db,
                db::ImageJobFinish {
                    job_id: job.id,
                    worker_id: service.jobs.worker_id(),
                    attempt,
                    status: "completed",
                    error: None,
                    now: Utc::now(),
                    cancel_requested: false,
                },
            )
            .await
            .unwrap()
        );
        assert!(
            db::image_job_owner(&service.db, job.id)
                .await
                .unwrap()
                .is_none()
        );
        let first = image_sync_rule::Entity::find_by_id(first.id)
            .one(&service.db)
            .await
            .unwrap()
            .unwrap();
        let second = image_sync_rule::Entity::find_by_id(second.id)
            .one(&service.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.last_digest.as_deref(), Some("sha256:lineage"));
        assert!(first.last_run_at.is_some());
        assert!(second.last_digest.is_none());
        assert!(second.last_run_at.is_none());
    }

    #[tokio::test]
    async fn cancel_is_guarded_by_pending_or_running_status() {
        let (_directory, service) = test_service().await;
        for status in JobStatus::ALL {
            let job = insert_test_job(&service.db, status, None).await;
            let result = cancel_job(State(service.clone()), AxumPath(job.id)).await;
            if matches!(status, JobStatus::Pending | JobStatus::Running) {
                assert_eq!(result.unwrap(), StatusCode::NO_CONTENT);
                let stored = image_job::Entity::find_by_id(job.id)
                    .one(&service.db)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(stored.cancel_requested);
                assert_eq!(
                    stored.status,
                    if status == JobStatus::Pending {
                        JobStatus::Cancelled.as_str()
                    } else {
                        JobStatus::Running.as_str()
                    }
                );
            } else {
                assert!(matches!(result, Err(AppError::BadRequest(_))));
            }
        }
        assert!(matches!(
            cancel_job(State(service), AxumPath(Uuid::new_v4())).await,
            Err(AppError::NotFound("image job"))
        ));
    }

    #[tokio::test]
    async fn retry_is_guarded_by_retryable_terminal_status() {
        let (_directory, service) = test_service().await;
        for status in JobStatus::ALL {
            let job = insert_test_job(&service.db, status, None).await;
            let result = retry_job(State(service.clone()), AxumPath(job.id)).await;
            if matches!(
                status,
                JobStatus::Failed | JobStatus::Cancelled | JobStatus::Skipped
            ) {
                let stored = result.unwrap().0;
                assert_eq!(stored.status, JobStatus::Pending.as_str());
                assert_eq!(stored.stage, "queued");
                assert!(!stored.cancel_requested);
                assert!(stored.error.is_none());
                let persisted = image_job::Entity::find_by_id(job.id)
                    .one(&service.db)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(persisted.started_at.is_none());
                assert!(persisted.finished_at.is_none());
                assert!(persisted.lease_until.is_none());
            } else {
                assert!(matches!(result, Err(AppError::BadRequest(_))));
            }
        }
        assert!(matches!(
            retry_job(State(service), AxumPath(Uuid::new_v4())).await,
            Err(AppError::NotFound("image job"))
        ));
    }

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
                registry_route_id: crate::registry_routes::DOCKER_HUB_ROUTE_ID,
                enabled: true,
                priority: 1,
                max_concurrency: 4,
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
                    source_ref: "docker.io/library/test:latest".into(),
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
    async fn selected_source_node_rejects_registry_mismatch_before_network_access() {
        let upstream = MockServer::start_async().await;
        let any_request = upstream
            .mock_async(|when, then| {
                when.any_request();
                then.status(500).body("must not be reached");
            })
            .await;

        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.registry_auth = Some(SecretString::from("client:password"));
        config.credential_key = Some(SecretString::from("66".repeat(32)));
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let config = Arc::new(config);
        let nodes = NodeService::new(config.clone(), db.clone()).unwrap();
        let node = nodes
            .create(crate::nodes::NodeInput {
                name: "docker-only mirror".into(),
                url: upstream.base_url(),
                registry_route_id: crate::registry_routes::DOCKER_HUB_ROUTE_ID,
                enabled: true,
                priority: 1,
                max_concurrency: 4,
                cf_preferred: false,
                connect_ip: None,
                auth_mode: "header".into(),
                auth_username: None,
                auth_header: Some("x-mirror-key".into()),
                auth_secret: Some("must-not-be-used".into()),
            })
            .await
            .unwrap();
        let service = ImageTools::new(config, db, nodes).await.unwrap();
        let mut job = test_job(JobStatus::Pending, None);
        job.source_ref = "ghcr.io/library/test:latest".into();
        job.source_node_id = Some(node.node.id);
        let reference = job.source_ref.parse().unwrap();

        let error = match service
            .prepare_image_via_node(&job, &reference, node.node.id)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("mismatched source Registry unexpectedly reached the node"),
        };

        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            error.to_string(),
            "selected image source does not match its Registry route"
        );
        any_request.assert_calls_async(0).await;
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
        let now = Utc::now();
        registry_credential::Model {
            id: credential_id,
            name: "copy destination".into(),
            registry: registry.address().to_string(),
            auth_mode: "bearer".into(),
            username: None,
            secret_enc: "encrypted-test-secret".into(),
            generation: 1,
            created_at: now,
            updated_at: now,
        }
        .into_active_model()
        .insert(&service.db)
        .await
        .unwrap();
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
}
