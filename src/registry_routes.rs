use std::str::FromStr;

use chrono::Utc;
use http::uri::Authority;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, DbErr, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    db::{self, registry_route},
    error::{ApiResult, AppError},
};

pub const DOCKER_HUB_ROUTE_ID: Uuid = Uuid::from_u128(0x4f6a_9e7b_2d17_4b6a_9b4f_7e76_b8d8_a001);
pub const GHCR_ROUTE_ID: Uuid = Uuid::from_u128(0x4f6a_9e7b_2d17_4b6a_9b4f_7e76_b8d8_a002);
pub const DOCKER_HUB_ROUTE_KEY: &str = "dockerhub";
pub const GHCR_ROUTE_KEY: &str = "ghcr";
const ROUTE_CONFIGURATION_CONFLICT: &str = "Registry route conflicts with existing configuration";
const ROUTE_IN_USE_CONFLICT: &str = "Registry route is in use by one or more nodes";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepositoryMode {
    DockerHubLibrary,
    Passthrough,
}

impl RepositoryMode {
    pub fn parse(value: &str) -> ApiResult<Self> {
        match value {
            "docker_hub_library" => Ok(Self::DockerHubLibrary),
            "passthrough" => Ok(Self::Passthrough),
            _ => Err(AppError::bad_request("unsupported repository mode")),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DockerHubLibrary => "docker_hub_library",
            Self::Passthrough => "passthrough",
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryRouteInput {
    pub key: String,
    pub name: String,
    pub canonical_registry: String,
    pub path_prefix: Option<String>,
    pub repository_mode: String,
    #[serde(default)]
    pub is_default: bool,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryRouteView {
    pub id: Uuid,
    pub key: String,
    pub name: String,
    pub canonical_registry: String,
    pub path_prefix: Option<String>,
    pub repository_mode: String,
    pub is_default: bool,
    pub enabled: bool,
    pub created_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RegistryRouteSummary {
    pub id: Uuid,
    pub key: String,
    pub name: String,
    pub canonical_registry: String,
    pub path_prefix: Option<String>,
    pub repository_mode: String,
    pub enabled: bool,
}

impl From<registry_route::Model> for RegistryRouteView {
    fn from(route: registry_route::Model) -> Self {
        Self {
            id: route.id,
            key: route.key,
            name: route.name,
            canonical_registry: route.canonical_registry,
            path_prefix: route.path_prefix,
            repository_mode: route.repository_mode,
            is_default: route.is_default,
            enabled: route.enabled,
            created_at: route.created_at,
            updated_at: route.updated_at,
        }
    }
}

impl From<&registry_route::Model> for RegistryRouteSummary {
    fn from(route: &registry_route::Model) -> Self {
        Self {
            id: route.id,
            key: route.key.clone(),
            name: route.name.clone(),
            canonical_registry: route.canonical_registry.clone(),
            path_prefix: route.path_prefix.clone(),
            repository_mode: route.repository_mode.clone(),
            enabled: route.enabled,
        }
    }
}

#[derive(Clone)]
pub struct RegistryRouteService {
    db: DatabaseConnection,
}

impl RegistryRouteService {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn list(&self) -> ApiResult<Vec<RegistryRouteView>> {
        Ok(registry_route::Entity::find()
            .order_by_desc(registry_route::Column::IsDefault)
            .order_by_asc(registry_route::Column::Key)
            .all(&self.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub async fn get(&self, id: Uuid) -> ApiResult<registry_route::Model> {
        registry_route::Entity::find_by_id(id)
            .one(&self.db)
            .await?
            .ok_or_else(|| AppError::not_found("Registry route"))
    }

    pub async fn by_path_prefix(
        &self,
        path_prefix: &str,
    ) -> ApiResult<Option<registry_route::Model>> {
        Ok(registry_route::Entity::find()
            .filter(registry_route::Column::PathPrefix.eq(path_prefix))
            .one(&self.db)
            .await?)
    }

    pub async fn default_route(&self) -> ApiResult<registry_route::Model> {
        registry_route::Entity::find()
            .filter(registry_route::Column::IsDefault.eq(true))
            .one(&self.db)
            .await?
            .ok_or_else(|| AppError::unavailable("default Registry route is not configured"))
    }

    pub async fn create(&self, input: RegistryRouteInput) -> ApiResult<RegistryRouteView> {
        let input = normalize_input(input)?;
        self.reject_conflicts(&input, None).await?;
        let now = Utc::now();
        let route = registry_route::Model {
            id: Uuid::new_v4(),
            key: input.key,
            name: input.name,
            canonical_registry: input.canonical_registry,
            path_prefix: input.path_prefix,
            repository_mode: input.repository_mode.as_str().to_owned(),
            is_default: input.is_default,
            enabled: input.enabled,
            created_at: now,
            updated_at: now,
        }
        .into_active_model()
        .insert(&self.db)
        .await
        .map_err(map_route_write_error)?;
        Ok(route.into())
    }

    pub async fn update(
        &self,
        id: Uuid,
        input: RegistryRouteInput,
    ) -> ApiResult<RegistryRouteView> {
        let input = normalize_input(input)?;
        let mut route = self.get(id).await?;
        if id == DOCKER_HUB_ROUTE_ID && input.key != DOCKER_HUB_ROUTE_KEY
            || id == GHCR_ROUTE_ID && input.key != GHCR_ROUTE_KEY
        {
            return Err(AppError::bad_request(
                "built-in Registry route keys cannot be changed",
            ));
        }
        self.reject_conflicts(&input, Some(id)).await?;
        route.key = input.key;
        route.name = input.name;
        route.canonical_registry = input.canonical_registry;
        route.path_prefix = input.path_prefix;
        route.repository_mode = input.repository_mode.as_str().to_owned();
        route.is_default = input.is_default;
        route.enabled = input.enabled;
        route.updated_at = Utc::now();
        let active = registry_route::ActiveModel {
            id: ActiveValue::Unchanged(route.id),
            key: ActiveValue::Set(route.key),
            name: ActiveValue::Set(route.name),
            canonical_registry: ActiveValue::Set(route.canonical_registry),
            path_prefix: ActiveValue::Set(route.path_prefix),
            repository_mode: ActiveValue::Set(route.repository_mode),
            is_default: ActiveValue::Set(route.is_default),
            enabled: ActiveValue::Set(route.enabled),
            created_at: ActiveValue::Unchanged(route.created_at),
            updated_at: ActiveValue::Set(route.updated_at),
        };
        Ok(active
            .update(&self.db)
            .await
            .map_err(map_route_write_error)?
            .into())
    }

    pub async fn delete(&self, id: Uuid) -> ApiResult<()> {
        let route = self.get(id).await?;
        if matches!(id, DOCKER_HUB_ROUTE_ID | GHCR_ROUTE_ID) {
            return Err(AppError::bad_request(
                "built-in Registry routes may be disabled but not deleted",
            ));
        }
        if db::count_nodes_for_route(&self.db, id).await? != 0 {
            return Err(AppError::conflict(ROUTE_IN_USE_CONFLICT));
        }
        let deleted = registry_route::Entity::delete_by_id(route.id)
            .exec(&self.db)
            .await
            .map_err(|error| {
                AppError::map_constraint(error, ROUTE_CONFIGURATION_CONFLICT, ROUTE_IN_USE_CONFLICT)
            })?;
        if deleted.rows_affected == 0 {
            return Err(AppError::not_found("Registry route"));
        }
        Ok(())
    }

    async fn reject_conflicts(
        &self,
        input: &NormalizedRouteInput,
        excluding: Option<Uuid>,
    ) -> ApiResult<()> {
        let without_current = |query: sea_orm::Select<registry_route::Entity>| match excluding {
            Some(id) => query.filter(registry_route::Column::Id.ne(id)),
            None => query,
        };
        if without_current(
            registry_route::Entity::find().filter(registry_route::Column::Key.eq(&input.key)),
        )
        .one(&self.db)
        .await?
        .is_some()
        {
            return Err(AppError::conflict("Registry route key already exists"));
        }
        if let Some(prefix) = &input.path_prefix
            && without_current(
                registry_route::Entity::find()
                    .filter(registry_route::Column::PathPrefix.eq(prefix)),
            )
            .one(&self.db)
            .await?
            .is_some()
        {
            return Err(AppError::conflict(
                "Registry route path prefix already exists",
            ));
        }
        if input.is_default
            && without_current(
                registry_route::Entity::find().filter(registry_route::Column::IsDefault.eq(true)),
            )
            .one(&self.db)
            .await?
            .is_some()
        {
            return Err(AppError::conflict(
                "a default Registry route already exists",
            ));
        }
        Ok(())
    }
}

fn map_route_write_error(error: DbErr) -> AppError {
    AppError::map_constraint(
        error,
        ROUTE_CONFIGURATION_CONFLICT,
        "Registry route reference conflict",
    )
}

pub(crate) struct NormalizedRouteInput {
    pub key: String,
    pub name: String,
    pub canonical_registry: String,
    pub path_prefix: Option<String>,
    pub repository_mode: RepositoryMode,
    pub is_default: bool,
    pub enabled: bool,
}

pub(crate) fn normalize_input(input: RegistryRouteInput) -> ApiResult<NormalizedRouteInput> {
    let key = normalize_identifier(&input.key, "Registry route key")?;
    let name = input.name.trim().to_owned();
    if name.is_empty() || name.chars().count() > 80 {
        return Err(AppError::bad_request(
            "name must be between 1 and 80 characters",
        ));
    }
    let canonical_registry = normalize_registry_authority(&input.canonical_registry)?;
    let path_prefix = input
        .path_prefix
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(|value| normalize_identifier(&value, "Registry route path prefix"))
        .transpose()?;
    if input.is_default != path_prefix.is_none() {
        return Err(AppError::bad_request(
            "only the default Registry route may omit a path prefix",
        ));
    }
    Ok(NormalizedRouteInput {
        key,
        name,
        canonical_registry,
        path_prefix,
        repository_mode: RepositoryMode::parse(&input.repository_mode)?,
        is_default: input.is_default,
        enabled: input.enabled,
    })
}

fn normalize_identifier(value: &str, label: &str) -> ApiResult<String> {
    let normalized = value.trim().to_ascii_lowercase();
    let mut bytes = normalized.bytes();
    let valid_first = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric());
    if !valid_first
        || normalized.len() > 32
        || !bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
    {
        return Err(AppError::bad_request(format!(
            "{label} must match [a-z0-9][a-z0-9_-]{{0,31}}"
        )));
    }
    Ok(normalized)
}

pub(crate) fn normalize_registry_authority(value: &str) -> ApiResult<String> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.len() > 261
        || normalized
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || matches!(byte, b'/' | b'?' | b'#' | b'@'))
    {
        return Err(AppError::bad_request(
            "canonical Registry must be a lowercase host[:port] with no scheme or path",
        ));
    }
    let authority = Authority::from_str(&normalized).map_err(|_| {
        AppError::bad_request(
            "canonical Registry must be a lowercase host[:port] with no scheme or path",
        )
    })?;
    if authority.port_u16() == Some(0) || !valid_registry_host(authority.host()) {
        return Err(AppError::bad_request(
            "canonical Registry must be a lowercase host[:port] with no scheme or path",
        ));
    }
    Ok(match normalized.as_str() {
        "docker.io" | "index.docker.io" | "registry-1.docker.io" => "docker.io".to_owned(),
        _ => normalized,
    })
}

fn valid_registry_host(host: &str) -> bool {
    let unbracketed = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'));
    if let Some(host) = unbracketed {
        return host.parse::<std::net::Ipv6Addr>().is_ok();
    }
    if host.parse::<std::net::Ipv4Addr>().is_ok() {
        return true;
    }
    !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .next()
                    .is_some_and(|byte| byte.is_ascii_alphanumeric())
                && label
                    .bytes()
                    .last()
                    .is_some_and(|byte| byte.is_ascii_alphanumeric())
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn default_true() -> bool {
    true
}

pub(crate) async fn seed_builtins(db: &DatabaseConnection) -> Result<(), DbErr> {
    seed_builtin(
        db,
        registry_route::Model {
            id: DOCKER_HUB_ROUTE_ID,
            key: DOCKER_HUB_ROUTE_KEY.to_owned(),
            name: "Docker Hub".to_owned(),
            canonical_registry: "docker.io".to_owned(),
            path_prefix: None,
            repository_mode: RepositoryMode::DockerHubLibrary.as_str().to_owned(),
            is_default: true,
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
    )
    .await?;
    seed_builtin(
        db,
        registry_route::Model {
            id: GHCR_ROUTE_ID,
            key: GHCR_ROUTE_KEY.to_owned(),
            name: "GitHub Container Registry".to_owned(),
            canonical_registry: "ghcr.io".to_owned(),
            path_prefix: Some("ghcr".to_owned()),
            repository_mode: RepositoryMode::Passthrough.as_str().to_owned(),
            is_default: false,
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
    )
    .await
}

async fn seed_builtin(db: &DatabaseConnection, route: registry_route::Model) -> Result<(), DbErr> {
    if registry_route::Entity::find()
        .filter(registry_route::Column::Key.eq(&route.key))
        .one(db)
        .await?
        .is_none()
    {
        route.into_active_model().insert(db).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use sea_orm::ConnectionTrait;

    fn custom_input(key: &str, prefix: &str) -> RegistryRouteInput {
        RegistryRouteInput {
            key: key.to_owned(),
            name: "Custom Registry".to_owned(),
            canonical_registry: "registry.example:5000".to_owned(),
            path_prefix: Some(prefix.to_owned()),
            repository_mode: "passthrough".to_owned(),
            is_default: false,
            enabled: true,
        }
    }

    fn assert_one_success_one_conflict<T>(left: ApiResult<T>, right: ApiResult<T>) {
        let mut successes = 0;
        let mut conflicts = 0;
        for result in [left, right] {
            match result {
                Ok(_) => successes += 1,
                Err(error) => {
                    assert_eq!(error.status(), StatusCode::CONFLICT);
                    let message = error.to_string().to_ascii_lowercase();
                    assert!(!message.contains("sql"));
                    assert!(!message.contains("constraint"));
                    conflicts += 1;
                }
            }
        }
        assert_eq!(successes, 1);
        assert_eq!(conflicts, 1);
    }

    #[tokio::test]
    async fn normalizes_route_fields_and_rejects_ambiguous_values() {
        let db = db::connect("sqlite::memory:").await.unwrap();
        let service = RegistryRouteService::new(db);
        let mut input = custom_input("  ACME_one ", " ACME ");
        input.canonical_registry = " Registry.Example:5000 ".to_owned();
        let route = service.create(input).await.unwrap();
        assert_eq!(route.key, "acme_one");
        assert_eq!(route.path_prefix.as_deref(), Some("acme"));
        assert_eq!(route.canonical_registry, "registry.example:5000");

        assert!(
            service
                .create(custom_input("another", "acme"))
                .await
                .is_err()
        );
        let mut invalid = custom_input("invalid", "invalid");
        invalid.canonical_registry = "https://registry.example/path".to_owned();
        assert!(service.create(invalid).await.is_err());
    }

    #[test]
    fn normalizes_docker_registry_authority_aliases_centrally() {
        for alias in ["docker.io", "INDEX.DOCKER.IO", "registry-1.docker.io"] {
            assert_eq!(normalize_registry_authority(alias).unwrap(), "docker.io");
        }
        assert_eq!(
            normalize_registry_authority(" Registry.Example:5000 ").unwrap(),
            "registry.example:5000"
        );
    }

    #[tokio::test]
    async fn builtins_are_seeded_once_and_cannot_be_deleted() {
        let db = db::connect("sqlite::memory:").await.unwrap();
        seed_builtins(&db).await.unwrap();
        let service = RegistryRouteService::new(db);
        let routes = service.list().await.unwrap();
        assert_eq!(routes.len(), 2);
        assert_eq!(routes.iter().filter(|route| route.is_default).count(), 1);
        assert!(routes.iter().any(|route| route.id == DOCKER_HUB_ROUTE_ID));
        assert!(routes.iter().any(|route| route.id == GHCR_ROUTE_ID));
        assert!(service.delete(DOCKER_HUB_ROUTE_ID).await.is_err());
    }

    #[tokio::test]
    async fn custom_route_crud_rejects_deletion_while_in_use() {
        let db = db::connect("sqlite::memory:").await.unwrap();
        let service = RegistryRouteService::new(db.clone());
        let created = service.create(custom_input("acme", "acme")).await.unwrap();
        let now = Utc::now();
        db::insert_node(
            &db,
            db::node::Model {
                id: Uuid::new_v4(),
                name: "acme mirror".into(),
                url: "https://mirror.example/".into(),
                registry_route_id: created.id,
                enabled: true,
                priority: 1,
                cf_preferred: false,
                connect_ip: None,
                auth_mode: "none".into(),
                auth_username: None,
                auth_header: None,
                auth_secret_enc: None,
                created_at: now,
                updated_at: now,
            },
        )
        .await
        .unwrap();
        assert!(service.delete(created.id).await.is_err());

        let mut updated = custom_input("acme", "acme-two");
        updated.name = "ACME Registry".into();
        let updated = service.update(created.id, updated).await.unwrap();
        assert_eq!(updated.name, "ACME Registry");
        assert_eq!(updated.path_prefix.as_deref(), Some("acme-two"));

        db::delete_node(
            &db,
            db::list_nodes_for_route(&db, created.id).await.unwrap()[0].id,
        )
        .await
        .unwrap();
        service.delete(created.id).await.unwrap();
        assert!(service.get(created.id).await.is_err());
    }

    #[tokio::test]
    async fn sqlite_restrict_trigger_maps_to_a_stable_conflict() {
        let db = db::connect("sqlite::memory:").await.unwrap();
        let service = RegistryRouteService::new(db.clone());
        let route = service
            .create(custom_input("trigger-race", "trigger-race"))
            .await
            .unwrap();
        db.execute_unprepared(
            "CREATE TRIGGER simulate_fk_restrict BEFORE DELETE ON registry_routes \
             WHEN OLD.key = 'trigger-race' BEGIN \
             SELECT RAISE(ABORT, 'FOREIGN KEY constraint failed'); END",
        )
        .await
        .unwrap();

        let error = service.delete(route.id).await.unwrap_err();
        assert_eq!(error.status(), StatusCode::CONFLICT);
        assert_eq!(error.to_string(), ROUTE_IN_USE_CONFLICT);
    }

    #[tokio::test]
    async fn concurrent_route_create_and_update_conflicts_are_stable() {
        let db = db::connect("sqlite::memory:").await.unwrap();
        let service = RegistryRouteService::new(db);

        let (left, right) = tokio::join!(
            service.create(custom_input("race", "race")),
            service.create(custom_input("race", "race")),
        );
        assert_one_success_one_conflict(left, right);

        let alpha = service
            .create(custom_input("alpha", "alpha"))
            .await
            .unwrap();
        let beta = service.create(custom_input("beta", "beta")).await.unwrap();
        let (left, right) = tokio::join!(
            service.update(alpha.id, custom_input("alpha", "shared")),
            service.update(beta.id, custom_input("beta", "shared")),
        );
        assert_one_success_one_conflict(left, right);
    }
}
