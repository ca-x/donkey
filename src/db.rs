use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, ConnectOptions, ConnectionTrait, Database,
    DatabaseConnection, DbBackend, DbErr, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    QuerySelect, Schema, Statement,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod node {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
    #[sea_orm(table_name = "nodes")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: Uuid,
        pub name: String,
        pub url: String,
        pub kind: String,
        pub route_prefix: Option<String>,
        pub enabled: bool,
        pub priority: i32,
        pub cf_preferred: bool,
        pub connect_ip: Option<String>,
        pub auth_mode: String,
        pub auth_username: Option<String>,
        pub auth_header: Option<String>,
        #[serde(skip_serializing, skip_deserializing)]
        pub auth_secret_enc: Option<String>,
        pub created_at: DateTime<Utc>,
        pub updated_at: DateTime<Utc>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

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
                    "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;",
                )
                .await?;
                Ok(())
            })
        });
    let db = Database::connect(options).await?;
    let schema = Schema::new(db.get_database_backend());

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
    Ok(db)
}

async fn run_migrations(db: &DatabaseConnection) -> Result<(), DbErr> {
    db.execute_unprepared(
        "CREATE TABLE IF NOT EXISTS donkey_schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)",
    )
    .await?;

    let columns = db
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA table_info(nodes)".to_owned(),
        ))
        .await?;
    let has_route_prefix = columns.iter().any(|row| {
        row.try_get::<String>("", "name")
            .is_ok_and(|name| name == "route_prefix")
    });
    if !has_route_prefix {
        db.execute_unprepared("ALTER TABLE nodes ADD COLUMN route_prefix TEXT")
            .await?;
    }
    db.execute_unprepared(
        "INSERT OR IGNORE INTO donkey_schema_migrations(version, name, applied_at) VALUES (1, 'node route prefix', CURRENT_TIMESTAMP)",
    )
    .await?;
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

pub async fn get_node_by_url(
    db: &DatabaseConnection,
    url: &str,
) -> Result<Option<node::Model>, DbErr> {
    node::Entity::find()
        .filter(node::Column::Url.eq(url))
        .one(db)
        .await
}

pub async fn get_node_by_url_and_prefix(
    db: &DatabaseConnection,
    url: &str,
    route_prefix: Option<&str>,
) -> Result<Option<node::Model>, DbErr> {
    let query = node::Entity::find().filter(node::Column::Url.eq(url));
    let query = match route_prefix {
        Some(prefix) => query.filter(node::Column::RoutePrefix.eq(prefix)),
        None => query.filter(node::Column::RoutePrefix.is_null()),
    };
    query.one(db).await
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

pub async fn save_node(db: &DatabaseConnection, model: node::Model) -> Result<node::Model, DbErr> {
    model.into_active_model().update(db).await
}

pub async fn delete_node(db: &DatabaseConnection, id: Uuid) -> Result<u64, DbErr> {
    Ok(node::Entity::delete_by_id(id).exec(db).await?.rows_affected)
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
    if let Some(model) = cache_entry::Entity::find_by_id(key).one(db).await? {
        let mut active = model.into_active_model();
        active.hit_count = Set(active.hit_count.as_ref() + 1);
        active.last_accessed_at = Set(Utc::now());
        active.update(db).await?;
    }
    Ok(())
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

    #[tokio::test]
    async fn node_round_trip_uses_sea_orm() {
        let db = connect("sqlite::memory:").await.unwrap();
        let now = Utc::now();
        let node = node::Model {
            id: Uuid::new_v4(),
            name: "local".into(),
            url: "http://127.0.0.1:5000".into(),
            kind: "dockerhub".into(),
            route_prefix: None,
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
    async fn migration_adds_route_prefix_to_an_existing_nodes_table() {
        let directory = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory.path().join("old.db").display()
        );
        let old = Database::connect(&url).await.unwrap();
        old.execute_unprepared(
            "CREATE TABLE nodes (id BLOB PRIMARY KEY NOT NULL, name TEXT NOT NULL, url TEXT NOT NULL, kind TEXT NOT NULL, enabled BOOLEAN NOT NULL, priority INTEGER NOT NULL, cf_preferred BOOLEAN NOT NULL, connect_ip TEXT, auth_mode TEXT NOT NULL, auth_username TEXT, auth_header TEXT, auth_secret_enc TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
        )
        .await
        .unwrap();
        old.close().await.unwrap();

        let migrated = connect(&url).await.unwrap();
        let columns = migrated
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA table_info(nodes)",
            ))
            .await
            .unwrap();
        assert!(columns.iter().any(|row| {
            row.try_get::<String>("", "name")
                .is_ok_and(|name| name == "route_prefix")
        }));
    }
}
