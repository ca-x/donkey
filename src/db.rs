use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectOptions, ConnectionTrait, Database,
    DatabaseConnection, DatabaseTransaction, DbBackend, DbErr, EntityTrait, IntoActiveModel,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Schema, Statement, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub mod registry_route {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "registry_routes")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: Uuid,
        #[sea_orm(unique)]
        pub key: String,
        pub name: String,
        pub canonical_registry: String,
        pub path_prefix: Option<String>,
        pub repository_mode: String,
        pub is_default: bool,
        pub enabled: bool,
        pub created_at: DateTime<Utc>,
        pub updated_at: DateTime<Utc>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {
        #[sea_orm(has_many = "super::node::Entity")]
        Node,
    }

    impl Related<super::node::Entity> for Entity {
        fn to() -> RelationDef {
            Relation::Node.def()
        }
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod node {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "nodes")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: Uuid,
        pub name: String,
        pub url: String,
        pub registry_route_id: Uuid,
        pub enabled: bool,
        pub priority: i32,
        pub cf_preferred: bool,
        pub connect_ip: Option<String>,
        pub auth_mode: String,
        pub auth_username: Option<String>,
        pub auth_header: Option<String>,
        pub auth_secret_enc: Option<String>,
        pub created_at: DateTime<Utc>,
        pub updated_at: DateTime<Utc>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {
        #[sea_orm(
            belongs_to = "super::registry_route::Entity",
            from = "Column::RegistryRouteId",
            to = "super::registry_route::Column::Id",
            on_update = "Cascade",
            on_delete = "Restrict"
        )]
        RegistryRoute,
    }

    impl Related<super::registry_route::Entity> for Entity {
        fn to() -> RelationDef {
            Relation::RegistryRoute.def()
        }
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod node_metric {
    use super::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
    #[sea_orm(table_name = "node_metrics")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub node_id: Uuid,
        pub healthy: bool,
        pub latency_ms: i64,
        pub speed_bps: i64,
        pub success_rate: f64,
        pub current_bps: i64,
        pub total_bytes: i64,
        pub last_checked_at: Option<DateTime<Utc>>,
        pub last_error: Option<String>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod cache_entry {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
    #[sea_orm(table_name = "cache_entries")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub key: String,
        pub media_type: String,
        pub path: String,
        pub size_bytes: i64,
        pub digest: Option<String>,
        pub hit_count: i64,
        pub created_at: DateTime<Utc>,
        pub last_accessed_at: DateTime<Utc>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod domain_mapping {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
    #[sea_orm(table_name = "domain_mappings")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: Uuid,
        #[sea_orm(unique)]
        pub source_host: String,
        pub upstream_base: String,
        pub public_base: String,
        pub enabled: bool,
        pub created_at: DateTime<Utc>,
        pub updated_at: DateTime<Utc>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod registry_credential {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
    #[sea_orm(table_name = "registry_credentials")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: Uuid,
        pub name: String,
        pub registry: String,
        pub auth_mode: String,
        pub username: Option<String>,
        #[serde(skip_serializing, skip_deserializing)]
        pub secret_enc: String,
        pub generation: i64,
        pub created_at: DateTime<Utc>,
        pub updated_at: DateTime<Utc>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod image_job {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
    #[sea_orm(table_name = "image_jobs")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
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
        #[serde(skip_serializing)]
        pub artifact_path: Option<String>,
        pub artifact_name: Option<String>,
        pub error: Option<String>,
        #[sea_orm(unique)]
        pub idempotency_key: Option<String>,
        pub cancel_requested: bool,
        pub lease_until: Option<DateTime<Utc>>,
        pub created_at: DateTime<Utc>,
        pub updated_at: DateTime<Utc>,
        pub started_at: Option<DateTime<Utc>>,
        pub finished_at: Option<DateTime<Utc>>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod image_sync_rule {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
    #[sea_orm(table_name = "image_sync_rules")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
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
        pub created_at: DateTime<Utc>,
        pub updated_at: DateTime<Utc>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod user {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
    #[sea_orm(table_name = "users")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: Uuid,
        #[sea_orm(unique)]
        pub identity_key: String,
        #[sea_orm(unique)]
        pub username: Option<String>,
        pub issuer: Option<String>,
        pub subject: String,
        pub display_name: String,
        pub email: Option<String>,
        #[serde(skip_serializing, skip_deserializing)]
        pub password_hash: Option<String>,
        pub role: String,
        pub enabled: bool,
        pub created_at: DateTime<Utc>,
        pub updated_at: DateTime<Utc>,
        pub last_login_at: Option<DateTime<Utc>>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod admin_session {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "admin_sessions")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub token_hash: String,
        pub user_id: Uuid,
        pub created_at: DateTime<Utc>,
        pub last_seen_at: DateTime<Utc>,
        pub expires_at: DateTime<Utc>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod oidc_login_state {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "oidc_login_states")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub state_hash: String,
        pub nonce: String,
        pub pkce_verifier: String,
        pub return_to: String,
        pub created_at: DateTime<Utc>,
        pub expires_at: DateTime<Utc>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub async fn connect(database_url: &str) -> Result<DatabaseConnection, DbErr> {
    let mut options = ConnectOptions::new(database_url.to_owned());
    options
        .max_connections(8)
        .min_connections(1)
        .sqlx_logging(false)
        .after_connect(|db| {
            Box::pin(async move {
                db.execute_unprepared(
                    "PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;",
                )
                .await?;
                Ok(())
            })
        });
    let db = Database::connect(options).await?;
    reject_legacy_node_schema(&db).await?;
    db.execute_unprepared("PRAGMA journal_mode=WAL;").await?;
    let schema = Schema::new(db.get_database_backend());

    db.execute(
        &schema
            .create_table_from_entity(registry_route::Entity)
            .if_not_exists()
            .to_owned(),
    )
    .await?;
    db.execute(
        &schema
            .create_table_from_entity(node::Entity)
            .if_not_exists()
            .to_owned(),
    )
    .await?;
    db.execute(
        &schema
            .create_table_from_entity(node_metric::Entity)
            .if_not_exists()
            .to_owned(),
    )
    .await?;
    db.execute(
        &schema
            .create_table_from_entity(cache_entry::Entity)
            .if_not_exists()
            .to_owned(),
    )
    .await?;
    db.execute(
        &schema
            .create_table_from_entity(domain_mapping::Entity)
            .if_not_exists()
            .to_owned(),
    )
    .await?;
    db.execute(
        &schema
            .create_table_from_entity(registry_credential::Entity)
            .if_not_exists()
            .to_owned(),
    )
    .await?;
    db.execute(
        &schema
            .create_table_from_entity(image_job::Entity)
            .if_not_exists()
            .to_owned(),
    )
    .await?;
    db.execute(
        &schema
            .create_table_from_entity(image_sync_rule::Entity)
            .if_not_exists()
            .to_owned(),
    )
    .await?;
    db.execute(
        &schema
            .create_table_from_entity(user::Entity)
            .if_not_exists()
            .to_owned(),
    )
    .await?;
    db.execute(
        &schema
            .create_table_from_entity(admin_session::Entity)
            .if_not_exists()
            .to_owned(),
    )
    .await?;
    db.execute(
        &schema
            .create_table_from_entity(oidc_login_state::Entity)
            .if_not_exists()
            .to_owned(),
    )
    .await?;
    run_migrations(&db).await?;
    crate::registry_routes::seed_builtins(&db).await?;
    Ok(db)
}

async fn reject_legacy_node_schema(db: &DatabaseConnection) -> Result<(), DbErr> {
    let nodes_exists = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = 'nodes'"
                .to_owned(),
        ))
        .await?
        .is_some();
    if !nodes_exists {
        return Ok(());
    }
    let columns = db
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA table_info(nodes)".to_owned(),
        ))
        .await?;
    let current = columns.iter().any(|row| {
        row.try_get::<String>("", "name")
            .is_ok_and(|name| name == "registry_route_id")
    });
    if current {
        Ok(())
    } else {
        Err(DbErr::Custom(
            "existing nodes schema is incompatible; delete and recreate the unused Donkey database"
                .to_owned(),
        ))
    }
}

#[derive(Clone, Copy)]
enum MigrationAction {
    Statements(&'static [&'static str]),
}

#[derive(Clone, Copy)]
struct Migration {
    version: i64,
    name: &'static str,
    action: MigrationAction,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "control-plane query indexes",
        action: MigrationAction::Statements(&[
            "CREATE INDEX IF NOT EXISTS idx_image_jobs_status_created_at ON image_jobs(status, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_image_jobs_status_finished_at ON image_jobs(status, finished_at)",
            "CREATE INDEX IF NOT EXISTS idx_image_sync_rules_enabled_next_run_at ON image_sync_rules(enabled, next_run_at)",
            "CREATE INDEX IF NOT EXISTS idx_admin_sessions_expires_at ON admin_sessions(expires_at)",
            "CREATE INDEX IF NOT EXISTS idx_oidc_login_states_expires_at ON oidc_login_states(expires_at)",
            "CREATE INDEX IF NOT EXISTS idx_cache_entries_last_accessed_at ON cache_entries(last_accessed_at)",
        ]),
    },
    Migration {
        version: 2,
        name: "Registry route invariants",
        action: MigrationAction::Statements(&[
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_registry_routes_path_prefix ON registry_routes(path_prefix) WHERE path_prefix IS NOT NULL",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_registry_routes_one_default ON registry_routes(is_default) WHERE is_default = 1",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_registry_route_url ON nodes(registry_route_id, url)",
            "CREATE INDEX IF NOT EXISTS idx_nodes_registry_route_enabled_priority ON nodes(registry_route_id, enabled, priority)",
        ]),
    },
    Migration {
        version: 3,
        name: "persisted runtime settings",
        action: MigrationAction::Statements(&[
            "CREATE TABLE IF NOT EXISTS runtime_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)",
        ]),
    },
    Migration {
        version: 4,
        name: "per-node concurrency limits",
        action: MigrationAction::Statements(&[
            "CREATE TABLE IF NOT EXISTS node_limits (node_id TEXT PRIMARY KEY NOT NULL, max_concurrency INTEGER NOT NULL DEFAULT 4)",
        ]),
    },
    Migration {
        version: 5,
        name: "image job ownership",
        action: MigrationAction::Statements(&[
            "CREATE TABLE IF NOT EXISTS image_job_owners (job_id TEXT PRIMARY KEY NOT NULL, worker_id TEXT NOT NULL, attempt INTEGER NOT NULL, claimed_at TEXT NOT NULL)",
        ]),
    },
    Migration {
        version: 6,
        name: "operational query indexes",
        action: MigrationAction::Statements(&[
            "CREATE INDEX IF NOT EXISTS idx_image_sync_rules_enabled_next_run ON image_sync_rules(enabled, next_run_at)",
            "CREATE INDEX IF NOT EXISTS idx_image_jobs_status_created ON image_jobs(status, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_image_jobs_status_finished ON image_jobs(status, finished_at)",
            "CREATE INDEX IF NOT EXISTS idx_admin_sessions_expires ON admin_sessions(expires_at)",
            "CREATE INDEX IF NOT EXISTS idx_oidc_login_states_expires ON oidc_login_states(expires_at)",
        ]),
    },
];

#[derive(Debug, Clone)]
pub struct RuntimeSetting {
    pub key: String,
    pub value: String,
}

pub async fn load_runtime_settings(db: &DatabaseConnection) -> Result<Vec<RuntimeSetting>, DbErr> {
    let rows = db
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT key, value FROM runtime_settings".to_owned(),
        ))
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(RuntimeSetting {
                key: row.try_get("", "key")?,
                value: row.try_get("", "value")?,
            })
        })
        .collect()
}

pub async fn replace_runtime_settings(
    db: &DatabaseConnection,
    settings: &[(String, String)],
) -> Result<(), DbErr> {
    let transaction = db.begin().await?;
    for (key, value) in settings {
        transaction
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO runtime_settings(key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP",
                [key.clone().into(), value.clone().into()],
            ))
            .await?;
    }
    transaction.commit().await
}

async fn run_migrations(db: &DatabaseConnection) -> Result<(), DbErr> {
    apply_migrations(db, MIGRATIONS).await
}

async fn apply_migrations(db: &DatabaseConnection, migrations: &[Migration]) -> Result<(), DbErr> {
    let transaction = db.begin().await?;
    if let Err(error) = apply_pending_migrations(&transaction, migrations).await {
        transaction.rollback().await?;
        return Err(error);
    }
    transaction.commit().await?;
    Ok(())
}

async fn apply_pending_migrations(
    transaction: &DatabaseTransaction,
    migrations: &[Migration],
) -> Result<(), DbErr> {
    transaction.execute_unprepared(
        "CREATE TABLE IF NOT EXISTS donkey_schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)",
    )
    .await?;

    let has_checksum = transaction
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA table_info(donkey_schema_migrations)".to_owned(),
        ))
        .await?
        .iter()
        .any(|row| row.try_get::<String>("", "name").is_ok_and(|name| name == "checksum"));
    if !has_checksum {
        transaction
            .execute_unprepared(
                "ALTER TABLE donkey_schema_migrations ADD COLUMN checksum TEXT NOT NULL DEFAULT ''",
            )
            .await?;
    }

    let applied_versions = transaction
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT version, checksum FROM donkey_schema_migrations ORDER BY version".to_owned(),
        ))
        .await?
        .into_iter()
        .map(|row| -> Result<(i64, String), DbErr> {
            Ok((
                row.try_get::<i64>("", "version")?,
                row.try_get::<String>("", "checksum").unwrap_or_default(),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let latest_known = migrations
        .iter()
        .map(|migration| migration.version)
        .max()
        .unwrap_or(0);
    if let Some(unknown) = applied_versions
        .iter()
        .map(|(version, _)| *version)
        .find(|version| *version > latest_known)
    {
        return Err(DbErr::Custom(format!(
            "database schema version {unknown} is newer than this binary supports"
        )));
    }

    for migration in migrations {
        if let Some((_, checksum)) = applied_versions
            .iter()
            .find(|(version, _)| *version == migration.version)
        {
            if !checksum.is_empty() && checksum.as_str() != migration_checksum(migration) {
                return Err(DbErr::Custom(format!(
                    "migration {} checksum mismatch",
                    migration.version
                )));
            }
            continue;
        }
        apply_migration(transaction, *migration).await?;
        transaction
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO donkey_schema_migrations(version, name, applied_at, checksum) VALUES (?, ?, CURRENT_TIMESTAMP, ?)",
                [migration.version.into(), migration.name.into(), migration_checksum(migration).into()],
            ))
            .await?;
    }
    Ok(())
}

fn migration_checksum(migration: &Migration) -> String {
    let mut hasher = Sha256::new();
    hasher.update(migration.name.as_bytes());
    let MigrationAction::Statements(statements) = migration.action;
    for statement in statements {
        hasher.update([0]);
        hasher.update(statement.as_bytes());
    }
    hex::encode(hasher.finalize())
}

async fn apply_migration(
    transaction: &DatabaseTransaction,
    migration: Migration,
) -> Result<(), DbErr> {
    match migration.action {
        MigrationAction::Statements(statements) => {
            for statement in statements {
                transaction.execute_unprepared(statement).await?;
            }
        }
    }
    Ok(())
}

pub async fn list_nodes(db: &DatabaseConnection) -> Result<Vec<node::Model>, DbErr> {
    node::Entity::find()
        .order_by_asc(node::Column::Priority)
        .order_by_asc(node::Column::Name)
        .all(db)
        .await
}

pub async fn get_node(db: &DatabaseConnection, id: Uuid) -> Result<Option<node::Model>, DbErr> {
    node::Entity::find_by_id(id).one(db).await
}

pub async fn get_node_by_route_and_url(
    db: &DatabaseConnection,
    registry_route_id: Uuid,
    url: &str,
) -> Result<Option<node::Model>, DbErr> {
    node::Entity::find()
        .filter(node::Column::RegistryRouteId.eq(registry_route_id))
        .filter(node::Column::Url.eq(url))
        .one(db)
        .await
}

pub async fn list_nodes_for_route(
    db: &DatabaseConnection,
    registry_route_id: Uuid,
) -> Result<Vec<node::Model>, DbErr> {
    node::Entity::find()
        .filter(node::Column::RegistryRouteId.eq(registry_route_id))
        .order_by_asc(node::Column::Priority)
        .order_by_asc(node::Column::Name)
        .all(db)
        .await
}

pub async fn count_nodes_for_route(
    db: &DatabaseConnection,
    registry_route_id: Uuid,
) -> Result<u64, DbErr> {
    node::Entity::find()
        .filter(node::Column::RegistryRouteId.eq(registry_route_id))
        .count(db)
        .await
}

pub async fn has_authenticated_nodes(db: &DatabaseConnection) -> Result<bool, DbErr> {
    Ok(node::Entity::find()
        .filter(node::Column::AuthSecretEnc.is_not_null())
        .one(db)
        .await?
        .is_some())
}

pub async fn insert_node(
    db: &DatabaseConnection,
    model: node::Model,
) -> Result<node::Model, DbErr> {
    model.into_active_model().insert(db).await
}

pub async fn get_node_max_concurrency(db: &DatabaseConnection, id: Uuid) -> Result<u16, DbErr> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT max_concurrency FROM node_limits WHERE node_id = ?",
            [id.to_string().into()],
        ))
        .await?;
    Ok(row
        .and_then(|row| row.try_get::<i64>("", "max_concurrency").ok())
        .unwrap_or(4)
        .clamp(1, u16::MAX as i64) as u16)
}

pub async fn set_node_max_concurrency(
    db: &DatabaseConnection,
    id: Uuid,
    max_concurrency: u16,
) -> Result<(), DbErr> {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO node_limits(node_id, max_concurrency) VALUES (?, ?) ON CONFLICT(node_id) DO UPDATE SET max_concurrency = excluded.max_concurrency",
        [id.to_string().into(), (max_concurrency as i64).into()],
    ))
    .await?;
    Ok(())
}

pub async fn save_node(db: &DatabaseConnection, model: node::Model) -> Result<node::Model, DbErr> {
    let active = node::ActiveModel {
        id: ActiveValue::Unchanged(model.id),
        name: ActiveValue::Set(model.name),
        url: ActiveValue::Set(model.url),
        registry_route_id: ActiveValue::Set(model.registry_route_id),
        enabled: ActiveValue::Set(model.enabled),
        priority: ActiveValue::Set(model.priority),
        cf_preferred: ActiveValue::Set(model.cf_preferred),
        connect_ip: ActiveValue::Set(model.connect_ip),
        auth_mode: ActiveValue::Set(model.auth_mode),
        auth_username: ActiveValue::Set(model.auth_username),
        auth_header: ActiveValue::Set(model.auth_header),
        auth_secret_enc: ActiveValue::Set(model.auth_secret_enc),
        created_at: ActiveValue::Unchanged(model.created_at),
        updated_at: ActiveValue::Set(model.updated_at),
    };
    active.update(db).await
}

pub async fn delete_node(db: &DatabaseConnection, id: Uuid) -> Result<u64, DbErr> {
    let deleted = node::Entity::delete_by_id(id).exec(db).await?.rows_affected;
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "DELETE FROM node_limits WHERE node_id = ?",
        [id.to_string().into()],
    ))
    .await?;
    Ok(deleted)
}

pub async fn metric_for(
    db: &DatabaseConnection,
    id: Uuid,
) -> Result<Option<node_metric::Model>, DbErr> {
    node_metric::Entity::find_by_id(id).one(db).await
}

pub async fn upsert_metric(
    db: &DatabaseConnection,
    metric: node_metric::Model,
) -> Result<(), DbErr> {
    if node_metric::Entity::find_by_id(metric.node_id)
        .one(db)
        .await?
        .is_some()
    {
        metric.into_active_model().update(db).await?;
    } else {
        metric.into_active_model().insert(db).await?;
    }
    Ok(())
}

pub async fn insert_cache_entry(
    db: &DatabaseConnection,
    entry: cache_entry::Model,
) -> Result<(), DbErr> {
    if cache_entry::Entity::find_by_id(&entry.key)
        .one(db)
        .await?
        .is_some()
    {
        entry.into_active_model().update(db).await?;
    } else {
        entry.into_active_model().insert(db).await?;
    }
    Ok(())
}

pub async fn touch_cache_entry(db: &DatabaseConnection, key: &str) -> Result<(), DbErr> {
    use sea_orm::ExprTrait;

    cache_entry::Entity::update_many()
        .col_expr(
            cache_entry::Column::HitCount,
            Expr::col(cache_entry::Column::HitCount).add(1),
        )
        .col_expr(cache_entry::Column::LastAccessedAt, Expr::value(Utc::now()))
        .filter(cache_entry::Column::Key.eq(key))
        .exec(db)
        .await?;
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CacheAggregate {
    pub(crate) entries: u64,
    pub(crate) bytes: u64,
    pub(crate) hits: u64,
}

pub(crate) async fn cache_aggregate(db: &DatabaseConnection) -> Result<CacheAggregate, DbErr> {
    let row = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT COUNT(*) AS entries, COALESCE(SUM(MAX(size_bytes, 0)), 0) AS bytes, COALESCE(SUM(MAX(hit_count, 0)), 0) AS hits FROM cache_entries".to_owned(),
        ))
        .await?
        .ok_or_else(|| DbErr::Custom("cache aggregate query returned no row".to_owned()))?;
    Ok(CacheAggregate {
        entries: row.try_get::<i64>("", "entries")?.max(0) as u64,
        bytes: row.try_get::<i64>("", "bytes")?.max(0) as u64,
        hits: row.try_get::<i64>("", "hits")?.max(0) as u64,
    })
}

pub async fn list_cache_entries(
    db: &DatabaseConnection,
    limit: u64,
) -> Result<Vec<cache_entry::Model>, DbErr> {
    cache_entry::Entity::find()
        .order_by_desc(cache_entry::Column::LastAccessedAt)
        .limit(limit)
        .all(db)
        .await
}

pub async fn all_cache_entries(db: &DatabaseConnection) -> Result<Vec<cache_entry::Model>, DbErr> {
    cache_entry::Entity::find()
        .order_by_asc(cache_entry::Column::LastAccessedAt)
        .all(db)
        .await
}

pub async fn delete_cache_entry(db: &DatabaseConnection, key: &str) -> Result<u64, DbErr> {
    Ok(cache_entry::Entity::delete_by_id(key)
        .exec(db)
        .await?
        .rows_affected)
}

pub async fn clear_cache_entries(db: &DatabaseConnection) -> Result<u64, DbErr> {
    Ok(cache_entry::Entity::delete_many()
        .exec(db)
        .await?
        .rows_affected)
}

pub async fn claim_image_job(
    db: &DatabaseConnection,
    job_id: Uuid,
    worker_id: Uuid,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
) -> Result<Option<i64>, DbErr> {
    let transaction = db.begin().await?;
    let attempt = transaction
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT attempt FROM image_job_owners WHERE job_id = ?",
            [job_id.to_string().into()],
        ))
        .await?
        .and_then(|row| row.try_get::<i64>("", "attempt").ok())
        .unwrap_or(0)
        .saturating_add(1);
    let changed = image_job::Entity::update_many()
        .col_expr(image_job::Column::Status, Expr::value("running"))
        .col_expr(image_job::Column::Stage, Expr::value("resolving"))
        .col_expr(
            image_job::Column::LeaseUntil,
            Expr::value(Some(lease_until)),
        )
        .col_expr(image_job::Column::StartedAt, Expr::value(Some(now)))
        .col_expr(image_job::Column::UpdatedAt, Expr::value(now))
        .filter(image_job::Column::Id.eq(job_id))
        .filter(image_job::Column::Status.eq("pending"))
        .exec(&transaction)
        .await?;
    if changed.rows_affected != 1 {
        transaction.commit().await?;
        return Ok(None);
    }
    transaction
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO image_job_owners(job_id, worker_id, attempt, claimed_at) VALUES (?, ?, ?, ?)
             ON CONFLICT(job_id) DO UPDATE SET worker_id = excluded.worker_id, attempt = excluded.attempt, claimed_at = excluded.claimed_at",
            [
                job_id.to_string().into(),
                worker_id.to_string().into(),
                attempt.into(),
                now.to_rfc3339().into(),
            ],
        ))
        .await?;
    transaction.commit().await?;
    Ok(Some(attempt))
}

pub async fn image_job_owner(
    db: &DatabaseConnection,
    job_id: Uuid,
) -> Result<Option<(Uuid, i64)>, DbErr> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT worker_id, attempt FROM image_job_owners WHERE job_id = ?",
            [job_id.to_string().into()],
        ))
        .await?;
    Ok(row.and_then(|row| {
        let worker = row.try_get::<String>("", "worker_id").ok()?.parse().ok()?;
        let attempt = row.try_get::<i64>("", "attempt").ok()?;
        Some((worker, attempt))
    }))
}

pub async fn clear_image_job_owner(db: &DatabaseConnection, job_id: Uuid) -> Result<(), DbErr> {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "DELETE FROM image_job_owners WHERE job_id = ?",
        [job_id.to_string().into()],
    ))
    .await?;
    Ok(())
}

pub struct ImageJobFinish<'a> {
    pub job_id: Uuid,
    pub worker_id: Uuid,
    pub attempt: i64,
    pub status: &'a str,
    pub error: Option<&'a str>,
    pub now: DateTime<Utc>,
    pub cancel_requested: bool,
}

pub async fn finish_image_job_owned(
    db: &DatabaseConnection,
    finish: ImageJobFinish<'_>,
) -> Result<bool, DbErr> {
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE image_jobs SET status = ?, error = ?, lease_until = NULL, finished_at = ?, updated_at = ? WHERE id = ? AND status = 'running' AND cancel_requested = ? AND EXISTS (SELECT 1 FROM image_job_owners WHERE job_id = ? AND worker_id = ? AND attempt = ?)",
            [
                finish.status.into(),
                finish.error.map(str::to_owned).into(),
                finish.now.into(),
                finish.now.into(),
                finish.job_id.into(),
                finish.cancel_requested.into(),
                finish.job_id.to_string().into(),
                finish.worker_id.to_string().into(),
                finish.attempt.into(),
            ],
        ))
        .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn renew_image_job(
    db: &DatabaseConnection,
    job_id: Uuid,
    worker_id: Uuid,
    attempt: i64,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
) -> Result<bool, DbErr> {
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE image_jobs SET lease_until = ?, updated_at = ? WHERE id = ? AND status = 'running' AND EXISTS (SELECT 1 FROM image_job_owners WHERE job_id = ? AND worker_id = ? AND attempt = ?)",
            [
                lease_until.into(),
                now.into(),
                job_id.into(),
                job_id.to_string().into(),
                worker_id.to_string().into(),
                attempt.into(),
            ],
        ))
        .await?;
    Ok(result.rows_affected() == 1)
}

/// Update a running image job's manifest only while the caller still owns the
/// current fencing token.  The ownership predicate is part of the SQL update
/// so a stale worker cannot write after a successful pre-check races with a
/// takeover by another worker.
pub async fn update_image_job_manifest_owned(
    db: &DatabaseConnection,
    job_id: Uuid,
    worker_id: Uuid,
    attempt: i64,
    resolved_digest: &str,
    index_digest: Option<&str>,
    total_bytes: i64,
) -> Result<bool, DbErr> {
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE image_jobs SET resolved_digest = ?, index_digest = ?, total_bytes = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'running' AND EXISTS (SELECT 1 FROM image_job_owners WHERE job_id = ? AND worker_id = ? AND attempt = ?)",
            [
                resolved_digest.into(),
                index_digest.map(str::to_owned).into(),
                total_bytes.into(),
                job_id.into(),
                job_id.to_string().into(),
                worker_id.to_string().into(),
                attempt.into(),
            ],
        ))
        .await?;
    Ok(result.rows_affected() == 1)
}

pub struct ImageJobProgress<'a> {
    pub job_id: Uuid,
    pub worker_id: Uuid,
    pub attempt: i64,
    pub stage: &'a str,
    pub progress_bytes: i64,
    pub total_bytes: i64,
    pub lease_until: DateTime<Utc>,
    pub now: DateTime<Utc>,
}

pub async fn update_image_job_progress_owned(
    db: &DatabaseConnection,
    progress: ImageJobProgress<'_>,
) -> Result<bool, DbErr> {
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE image_jobs SET stage = ?, progress_bytes = ?, total_bytes = ?, updated_at = ?, lease_until = ? WHERE id = ? AND status = 'running' AND EXISTS (SELECT 1 FROM image_job_owners WHERE job_id = ? AND worker_id = ? AND attempt = ?)",
            [
                progress.stage.into(),
                progress.progress_bytes.into(),
                progress.total_bytes.into(),
                progress.now.into(),
                progress.lease_until.into(),
                progress.job_id.into(),
                progress.job_id.to_string().into(),
                progress.worker_id.to_string().into(),
                progress.attempt.into(),
            ],
        ))
        .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn update_image_job_stage_owned(
    db: &DatabaseConnection,
    job_id: Uuid,
    worker_id: Uuid,
    attempt: i64,
    stage: &str,
    lease_until: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<bool, DbErr> {
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE image_jobs SET stage = ?, updated_at = ?, lease_until = ? WHERE id = ? AND status = 'running' AND EXISTS (SELECT 1 FROM image_job_owners WHERE job_id = ? AND worker_id = ? AND attempt = ?)",
            [
                stage.into(),
                now.into(),
                lease_until.into(),
                job_id.into(),
                job_id.to_string().into(),
                worker_id.to_string().into(),
                attempt.into(),
            ],
        ))
        .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn update_image_job_artifact_owned(
    db: &DatabaseConnection,
    job_id: Uuid,
    worker_id: Uuid,
    attempt: i64,
    artifact_path: &str,
    artifact_name: &str,
) -> Result<bool, DbErr> {
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE image_jobs SET artifact_path = ?, artifact_name = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'running' AND EXISTS (SELECT 1 FROM image_job_owners WHERE job_id = ? AND worker_id = ? AND attempt = ?)",
            [
                artifact_path.into(),
                artifact_name.into(),
                job_id.into(),
                job_id.to_string().into(),
                worker_id.to_string().into(),
                attempt.into(),
            ],
        ))
        .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn list_mappings(db: &DatabaseConnection) -> Result<Vec<domain_mapping::Model>, DbErr> {
    domain_mapping::Entity::find()
        .order_by_asc(domain_mapping::Column::SourceHost)
        .all(db)
        .await
}

pub async fn get_mapping(
    db: &DatabaseConnection,
    id: Uuid,
) -> Result<Option<domain_mapping::Model>, DbErr> {
    domain_mapping::Entity::find_by_id(id).one(db).await
}

pub async fn insert_mapping(
    db: &DatabaseConnection,
    model: domain_mapping::Model,
) -> Result<domain_mapping::Model, DbErr> {
    model.into_active_model().insert(db).await
}

pub async fn save_mapping(
    db: &DatabaseConnection,
    model: domain_mapping::Model,
) -> Result<domain_mapping::Model, DbErr> {
    model.into_active_model().update(db).await
}

pub async fn delete_mapping(db: &DatabaseConnection, id: Uuid) -> Result<u64, DbErr> {
    Ok(domain_mapping::Entity::delete_by_id(id)
        .exec(db)
        .await?
        .rows_affected)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPECTED_INDEXES: &[(&str, &str)] = &[
        ("image_jobs", "idx_image_jobs_status_created_at"),
        ("image_jobs", "idx_image_jobs_status_finished_at"),
        (
            "image_sync_rules",
            "idx_image_sync_rules_enabled_next_run_at",
        ),
        ("admin_sessions", "idx_admin_sessions_expires_at"),
        ("oidc_login_states", "idx_oidc_login_states_expires_at"),
        ("cache_entries", "idx_cache_entries_last_accessed_at"),
        ("registry_routes", "idx_registry_routes_path_prefix"),
        ("registry_routes", "idx_registry_routes_one_default"),
        ("nodes", "idx_nodes_registry_route_url"),
        ("nodes", "idx_nodes_registry_route_enabled_priority"),
        ("image_jobs", "idx_image_jobs_status_created"),
        ("image_jobs", "idx_image_jobs_status_finished"),
        ("image_sync_rules", "idx_image_sync_rules_enabled_next_run"),
        ("admin_sessions", "idx_admin_sessions_expires"),
        ("oidc_login_states", "idx_oidc_login_states_expires"),
    ];

    async fn create_v0_database(url: &str) {
        let old = Database::connect(url).await.unwrap();
        old.execute_unprepared(
            "CREATE TABLE nodes (id BLOB PRIMARY KEY NOT NULL, name TEXT NOT NULL, url TEXT NOT NULL, kind TEXT NOT NULL, enabled BOOLEAN NOT NULL, priority INTEGER NOT NULL, cf_preferred BOOLEAN NOT NULL, connect_ip TEXT, auth_mode TEXT NOT NULL, auth_username TEXT, auth_header TEXT, auth_secret_enc TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
        )
        .await
        .unwrap();
        old.close().await.unwrap();
    }

    async fn journal_mode(url: &str) -> String {
        let db = Database::connect(url).await.unwrap();
        let mode = db
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA journal_mode".to_owned(),
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<String>("", "journal_mode")
            .unwrap();
        db.close().await.unwrap();
        mode
    }

    fn directory_entries(path: &std::path::Path) -> Vec<String> {
        let mut entries = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        entries.sort();
        entries
    }

    async fn assert_expected_indexes(db: &DatabaseConnection) {
        for (table, expected) in EXPECTED_INDEXES {
            let indexes = db
                .query_all_raw(Statement::from_string(
                    DbBackend::Sqlite,
                    format!("PRAGMA index_list('{table}')"),
                ))
                .await
                .unwrap();
            assert!(
                indexes.iter().any(|row| {
                    row.try_get::<String>("", "name")
                        .is_ok_and(|name| name == *expected)
                }),
                "missing index {expected} on {table}"
            );
        }

        let mut actual = db
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE type = 'index' AND name GLOB 'idx_*' ORDER BY name".to_owned(),
            ))
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String>("", "name").unwrap())
            .collect::<Vec<_>>();
        let mut expected = EXPECTED_INDEXES
            .iter()
            .map(|(_, name)| (*name).to_owned())
            .collect::<Vec<_>>();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    }

    async fn migration_versions(db: &DatabaseConnection) -> Vec<(i64, i64)> {
        db.query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT version, COUNT(*) AS count FROM donkey_schema_migrations GROUP BY version ORDER BY version".to_owned(),
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.try_get::<i64>("", "version").unwrap(),
                row.try_get::<i64>("", "count").unwrap(),
            )
        })
        .collect()
    }

    #[tokio::test]
    async fn node_round_trip_uses_sea_orm() {
        let db = connect("sqlite::memory:").await.unwrap();
        let now = Utc::now();
        let node = node::Model {
            id: Uuid::new_v4(),
            name: "local".into(),
            url: "http://127.0.0.1:5000".into(),
            registry_route_id: crate::registry_routes::DOCKER_HUB_ROUTE_ID,
            enabled: true,
            priority: 10,
            cf_preferred: false,
            connect_ip: None,
            auth_mode: "none".into(),
            auth_username: None,
            auth_header: None,
            auth_secret_enc: None,
            created_at: now,
            updated_at: now,
        };
        insert_node(&db, node.clone()).await.unwrap();
        assert_eq!(get_node(&db, node.id).await.unwrap(), Some(node));
    }

    #[tokio::test]
    async fn old_database_is_rejected_before_schema_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory.path().join("old.db").display()
        );
        create_v0_database(&url).await;
        let mode_before = journal_mode(&url).await;
        let entries_before = directory_entries(directory.path());

        let error = connect(&url).await.unwrap_err();
        assert!(error.to_string().contains("delete and recreate"));
        assert_eq!(journal_mode(&url).await, mode_before);
        assert_eq!(directory_entries(directory.path()), entries_before);

        let unchanged = Database::connect(&url).await.unwrap();
        let columns = unchanged
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA table_info(nodes)",
            ))
            .await
            .unwrap();
        assert!(!columns.iter().any(|row| {
            row.try_get::<String>("", "name")
                .is_ok_and(|name| name == "registry_route_id")
        }));
        let added_tables = unchanged
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('registry_routes', 'donkey_schema_migrations')".to_owned(),
            ))
            .await
            .unwrap();
        assert!(added_tables.is_empty());
    }

    #[tokio::test]
    async fn fresh_database_has_expected_indexes() {
        let db = connect("sqlite::memory:").await.unwrap();
        assert_eq!(
            migration_versions(&db).await,
            vec![(1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1)]
        );
        assert_expected_indexes(&db).await;
        let routes = registry_route::Entity::find().all(&db).await.unwrap();
        assert_eq!(routes.len(), 2);
        assert_eq!(routes.iter().filter(|route| route.is_default).count(), 1);

        run_migrations(&db).await.unwrap();
        crate::registry_routes::seed_builtins(&db).await.unwrap();
        assert_eq!(registry_route::Entity::find().count(&db).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn runtime_settings_round_trip() {
        let db = connect("sqlite::memory:").await.unwrap();
        replace_runtime_settings(&db, &[("resumable_threshold".into(), "4194304".into())])
            .await
            .unwrap();
        let settings = load_runtime_settings(&db).await.unwrap();
        assert_eq!(settings[0].key, "resumable_threshold");
        assert_eq!(settings[0].value, "4194304");
    }

    #[tokio::test]
    async fn failed_migration_rolls_back_schema_and_version() {
        const FAILING_MIGRATION: &[Migration] = &[Migration {
            version: 7,
            name: "rollback test",
            action: MigrationAction::Statements(&[
                "CREATE TABLE migration_rollback_marker (id INTEGER PRIMARY KEY)",
                "INVALID MIGRATION STATEMENT",
            ]),
        }];

        let db = connect("sqlite::memory:").await.unwrap();
        assert!(apply_migrations(&db, FAILING_MIGRATION).await.is_err());

        let marker = db
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'migration_rollback_marker'".to_owned(),
            ))
            .await
            .unwrap();
        assert!(marker.is_empty());
        assert_eq!(
            migration_versions(&db).await,
            vec![(1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1)]
        );
    }

    #[tokio::test]
    async fn rejects_database_schema_newer_than_binary() {
        let db = connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared(
            "INSERT INTO donkey_schema_migrations(version, name, applied_at) VALUES (999, 'future', CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        let error = apply_migrations(&db, MIGRATIONS).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("newer than this binary supports")
        );
    }

    #[tokio::test]
    async fn rejects_modified_applied_migration() {
        let db = connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared(
            "UPDATE donkey_schema_migrations SET checksum = 'tampered' WHERE version = 1",
        )
        .await
        .unwrap();
        let error = run_migrations(&db).await.unwrap_err();
        assert!(error.to_string().contains("migration 1 checksum mismatch"));
    }

    #[tokio::test]
    async fn route_and_node_database_invariants_are_enforced() {
        let db = connect("sqlite::memory:").await.unwrap();
        let now = Utc::now();
        let custom = registry_route::Model {
            id: Uuid::new_v4(),
            key: "custom".into(),
            name: "Custom".into(),
            canonical_registry: "registry.example".into(),
            path_prefix: Some("custom".into()),
            repository_mode: "passthrough".into(),
            is_default: false,
            enabled: true,
            created_at: now,
            updated_at: now,
        };
        custom
            .clone()
            .into_active_model()
            .insert(&db)
            .await
            .unwrap();

        let mut duplicate_prefix = custom.clone();
        duplicate_prefix.id = Uuid::new_v4();
        duplicate_prefix.key = "other".into();
        assert!(
            duplicate_prefix
                .into_active_model()
                .insert(&db)
                .await
                .is_err()
        );

        let mut duplicate_default = custom.clone();
        duplicate_default.id = Uuid::new_v4();
        duplicate_default.key = "other-default".into();
        duplicate_default.path_prefix = Some("other-default".into());
        duplicate_default.is_default = true;
        assert!(
            duplicate_default
                .into_active_model()
                .insert(&db)
                .await
                .is_err()
        );

        let base_node = node::Model {
            id: Uuid::new_v4(),
            name: "mirror".into(),
            url: "https://mirror.example/".into(),
            registry_route_id: custom.id,
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
        };
        insert_node(&db, base_node.clone()).await.unwrap();
        let mut duplicate_node = base_node.clone();
        duplicate_node.id = Uuid::new_v4();
        assert!(insert_node(&db, duplicate_node).await.is_err());

        let mut other_route_node = base_node.clone();
        other_route_node.id = Uuid::new_v4();
        other_route_node.registry_route_id = crate::registry_routes::GHCR_ROUTE_ID;
        insert_node(&db, other_route_node).await.unwrap();

        assert!(
            registry_route::Entity::delete_by_id(custom.id)
                .exec(&db)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn cache_aggregate_is_zero_when_empty_and_exact_when_populated() {
        let db = connect("sqlite::memory:").await.unwrap();
        assert_eq!(
            cache_aggregate(&db).await.unwrap(),
            CacheAggregate {
                entries: 0,
                bytes: 0,
                hits: 0,
            }
        );

        let now = Utc::now();
        for (key, size_bytes, hit_count) in [("first", 12, -2), ("second", -4, 7)] {
            insert_cache_entry(
                &db,
                cache_entry::Model {
                    key: key.to_owned(),
                    media_type: "application/octet-stream".to_owned(),
                    path: format!("/tmp/{key}"),
                    size_bytes,
                    digest: None,
                    hit_count,
                    created_at: now,
                    last_accessed_at: now,
                },
            )
            .await
            .unwrap();
        }
        assert_eq!(
            cache_aggregate(&db).await.unwrap(),
            CacheAggregate {
                entries: 2,
                bytes: 12,
                hits: 7,
            }
        );
    }
}
