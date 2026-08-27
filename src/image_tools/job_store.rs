use std::path::Path;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use uuid::Uuid;

use crate::{
    db::{self, image_job},
    error::{ApiResult, AppError},
};

pub(super) const JOB_LEASE_MINUTES: i64 = 10;

#[derive(Clone)]
pub(super) struct JobStore {
    db: DatabaseConnection,
    worker_id: Uuid,
    active_attempts: std::sync::Arc<DashMap<Uuid, i64>>,
}

impl JobStore {
    pub(super) fn new(db: DatabaseConnection) -> Self {
        Self {
            db,
            worker_id: Uuid::new_v4(),
            active_attempts: std::sync::Arc::new(DashMap::new()),
        }
    }

    #[cfg(test)]
    pub(super) fn worker_id(&self) -> Uuid {
        self.worker_id
    }

    pub(super) fn active_attempt(&self, id: Uuid) -> Option<i64> {
        self.active_attempts.get(&id).map(|value| *value)
    }

    pub(super) fn activate(&self, id: Uuid, attempt: i64) {
        self.active_attempts.insert(id, attempt);
    }

    pub(super) fn deactivate(&self, id: Uuid) {
        self.active_attempts.remove(&id);
    }

    pub(super) async fn claim_next(&self) -> ApiResult<Option<(image_job::Model, i64)>> {
        let Some(id) = image_job::Entity::find()
            .select_only()
            .column(image_job::Column::Id)
            .filter(image_job::Column::Status.eq("pending"))
            .order_by_asc(image_job::Column::CreatedAt)
            .into_tuple::<Uuid>()
            .one(&self.db)
            .await?
        else {
            return Ok(None);
        };
        self.claim_selected(id).await
    }

    pub(super) async fn claim_selected(
        &self,
        id: Uuid,
    ) -> ApiResult<Option<(image_job::Model, i64)>> {
        let now = Utc::now();
        let Some(attempt) =
            db::claim_image_job(&self.db, id, self.worker_id, now, lease_until(now)).await?
        else {
            return Ok(None);
        };
        let job = image_job::Entity::find_by_id(id)
            .one(&self.db)
            .await?
            .ok_or_else(|| AppError::not_found("image job"))?;
        self.activate(id, attempt);
        Ok(Some((job, attempt)))
    }

    pub(super) async fn renew(&self, id: Uuid, attempt: i64) -> ApiResult<bool> {
        let now = Utc::now();
        Ok(
            db::renew_image_job(&self.db, id, self.worker_id, attempt, now, lease_until(now))
                .await?,
        )
    }

    pub(super) async fn owns(&self, id: Uuid, attempt: i64) -> ApiResult<bool> {
        Ok(db::image_job_owner(&self.db, id)
            .await?
            .is_some_and(|(worker, current)| worker == self.worker_id && current == attempt))
    }

    pub(super) async fn finish(
        &self,
        id: Uuid,
        attempt: i64,
        status: &str,
        error: Option<&str>,
        cancel_requested: bool,
    ) -> ApiResult<bool> {
        Ok(db::finish_image_job_owned(
            &self.db,
            db::ImageJobFinish {
                job_id: id,
                worker_id: self.worker_id,
                attempt,
                status,
                error,
                now: Utc::now(),
                cancel_requested,
            },
        )
        .await?)
    }

    pub(super) async fn update_manifest(
        &self,
        id: Uuid,
        manifest_digest: &str,
        index_digest: Option<&str>,
        total_bytes: u64,
    ) -> ApiResult<()> {
        if let Some(attempt) = self.active_attempt(id) {
            let updated = db::update_image_job_manifest_owned(
                &self.db,
                id,
                self.worker_id,
                attempt,
                manifest_digest,
                index_digest,
                total_bytes.min(i64::MAX as u64) as i64,
            )
            .await?;
            return ownership_result(updated);
        }
        image_job::Entity::update_many()
            .col_expr(
                image_job::Column::ResolvedDigest,
                sea_orm::sea_query::Expr::value(Some(manifest_digest.to_owned())),
            )
            .col_expr(
                image_job::Column::IndexDigest,
                sea_orm::sea_query::Expr::value(index_digest.map(str::to_owned)),
            )
            .col_expr(
                image_job::Column::TotalBytes,
                sea_orm::sea_query::Expr::value(total_bytes.min(i64::MAX as u64) as i64),
            )
            .filter(image_job::Column::Id.eq(id))
            .exec(&self.db)
            .await?;
        Ok(())
    }

    pub(super) async fn update_progress(
        &self,
        id: Uuid,
        stage: &str,
        current: u64,
        total: u64,
    ) -> ApiResult<()> {
        let now = Utc::now();
        if let Some(attempt) = self.active_attempt(id) {
            let updated = db::update_image_job_progress_owned(
                &self.db,
                db::ImageJobProgress {
                    job_id: id,
                    worker_id: self.worker_id,
                    attempt,
                    stage,
                    progress_bytes: current.min(i64::MAX as u64) as i64,
                    total_bytes: total.min(i64::MAX as u64) as i64,
                    lease_until: lease_until(now),
                    now,
                },
            )
            .await?;
            return ownership_result(updated);
        }
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
                sea_orm::sea_query::Expr::value(now),
            )
            .col_expr(
                image_job::Column::LeaseUntil,
                sea_orm::sea_query::Expr::value(Some(lease_until(now))),
            )
            .filter(image_job::Column::Id.eq(id))
            .filter(image_job::Column::Status.eq("running"))
            .exec(&self.db)
            .await?;
        Ok(())
    }

    pub(super) async fn update_stage(&self, id: Uuid, stage: &str) -> ApiResult<()> {
        let now = Utc::now();
        if let Some(attempt) = self.active_attempt(id) {
            let updated = db::update_image_job_stage_owned(
                &self.db,
                id,
                self.worker_id,
                attempt,
                stage,
                lease_until(now),
                now,
            )
            .await?;
            return ownership_result(updated);
        }
        image_job::Entity::update_many()
            .col_expr(
                image_job::Column::Stage,
                sea_orm::sea_query::Expr::value(stage),
            )
            .col_expr(
                image_job::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(now),
            )
            .col_expr(
                image_job::Column::LeaseUntil,
                sea_orm::sea_query::Expr::value(Some(lease_until(now))),
            )
            .filter(image_job::Column::Id.eq(id))
            .filter(image_job::Column::Status.eq("running"))
            .exec(&self.db)
            .await?;
        Ok(())
    }

    pub(super) async fn update_artifact(&self, id: Uuid, path: &Path, name: &str) -> ApiResult<()> {
        if let Some(attempt) = self.active_attempt(id) {
            let updated = db::update_image_job_artifact_owned(
                &self.db,
                id,
                self.worker_id,
                attempt,
                &path.to_string_lossy(),
                name,
            )
            .await?;
            return ownership_result(updated);
        }
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

    pub(super) async fn is_cancelled(&self, id: Uuid) -> ApiResult<bool> {
        Ok(image_job::Entity::find_by_id(id)
            .one(&self.db)
            .await?
            .is_some_and(|job| job.cancel_requested))
    }
}

fn lease_until(now: DateTime<Utc>) -> DateTime<Utc> {
    now + chrono::Duration::minutes(JOB_LEASE_MINUTES)
}

fn ownership_result(updated: bool) -> ApiResult<()> {
    if updated {
        Ok(())
    } else {
        Err(AppError::Conflict("image job ownership has changed".into()))
    }
}
