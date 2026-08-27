use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use sea_orm::DatabaseConnection;

use crate::{
    cache::CacheStore,
    config::Config,
    error::AppError,
    nodes::NodeService,
    registry_routes::RegistryRouteService,
    scheduler::{Scheduler, SchedulerAlgorithmKind},
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: DatabaseConnection,
    pub auth: crate::auth::AuthService,
    pub nodes: NodeService,
    pub registry_routes: RegistryRouteService,
    pub cache: CacheStore,
    pub upstream: crate::upstream::UpstreamService,
    pub scheduler: Scheduler,
    pub image_tools: crate::image_tools::ImageTools,
    pub traffic: crate::traffic::TrafficMetrics,
    pub runtime_flags: RuntimeFlags,
}

#[derive(Clone)]
pub struct RuntimeFlags {
    pull_logging_enabled: Arc<AtomicBool>,
    pull_log_retention_days: Arc<AtomicU64>,
    pull_log_max_entries: Arc<AtomicU64>,
}

impl RuntimeFlags {
    fn new(config: &Config) -> Self {
        Self {
            pull_logging_enabled: Arc::new(AtomicBool::new(config.pull_logging_enabled)),
            pull_log_retention_days: Arc::new(AtomicU64::new(config.pull_log_retention_days)),
            pull_log_max_entries: Arc::new(AtomicU64::new(config.pull_log_max_entries)),
        }
    }

    pub fn update(&self, config: &Config) {
        self.pull_logging_enabled
            .store(config.pull_logging_enabled, Ordering::Release);
        self.pull_log_retention_days
            .store(config.pull_log_retention_days, Ordering::Release);
        self.pull_log_max_entries
            .store(config.pull_log_max_entries, Ordering::Release);
    }

    pub fn pull_logging_enabled(&self) -> bool {
        self.pull_logging_enabled.load(Ordering::Acquire)
    }

    pub fn pull_log_retention_days(&self) -> u64 {
        self.pull_log_retention_days.load(Ordering::Acquire)
    }

    pub fn pull_log_max_entries(&self) -> u64 {
        self.pull_log_max_entries.load(Ordering::Acquire)
    }
}

impl AppState {
    pub async fn new(config: Config) -> Result<Self, AppError> {
        Self::new_from_config(config, None).await
    }

    pub async fn new_with_algorithm(
        config: Config,
        algorithm: SchedulerAlgorithmKind,
    ) -> Result<Self, AppError> {
        Self::new_from_config(config, Some(algorithm)).await
    }

    async fn new_from_config(
        config: Config,
        algorithm_override: Option<SchedulerAlgorithmKind>,
    ) -> Result<Self, AppError> {
        tokio::fs::create_dir_all(&config.data_dir).await?;
        let db = crate::db::connect(&config.database_url).await?;
        let mut config = config;
        let persisted = crate::db::load_runtime_settings(&db)
            .await
            .map_err(AppError::from)?;
        config
            .apply_runtime_overrides(&persisted)
            .map_err(AppError::Internal)?;
        let algorithm = algorithm_override.unwrap_or_else(|| {
            SchedulerAlgorithmKind::parse(&config.scheduler_algorithm)
                .unwrap_or(SchedulerAlgorithmKind::Current)
        });
        let config = Arc::new(config);
        if config.registry_auth_value().is_none() && crate::db::has_authenticated_nodes(&db).await?
        {
            return Err(AppError::bad_request(
                "DONKEY_REGISTRY_AUTH is required because authenticated upstream nodes are stored",
            ));
        }
        let nodes = NodeService::new(config.clone(), db.clone())?;
        let registry_routes = RegistryRouteService::new(db.clone());
        let auth = crate::auth::AuthService::new(config.clone(), db.clone()).await?;
        let cache = CacheStore::new(config.clone(), db.clone()).await?;
        let upstream = crate::upstream::UpstreamService::new(config.clone(), nodes.clone());
        let scheduler = Scheduler::new_with_algorithm(
            config.clone(),
            nodes.clone(),
            cache.clone(),
            upstream.clone(),
            algorithm,
        );
        let image_tools =
            crate::image_tools::ImageTools::new(config.clone(), db.clone(), nodes.clone()).await?;
        let traffic = crate::traffic::TrafficMetrics::default();
        let runtime_flags = RuntimeFlags::new(&config);
        Ok(Self {
            config,
            db,
            auth,
            nodes,
            registry_routes,
            cache,
            upstream,
            scheduler,
            image_tools,
            traffic,
            runtime_flags,
        })
    }

    pub async fn apply_runtime_config(&self, config: &Config) {
        self.scheduler.update_runtime(config).await;
        self.cache.update_runtime(config).await;
        self.upstream.update_runtime(config).await;
        self.nodes.update_runtime(config).await;
        self.image_tools.update_runtime(config).await;
        self.runtime_flags.update(config);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::NodeInput;
    use secrecy::SecretString;

    #[tokio::test]
    async fn refuses_anonymous_restart_with_stored_upstream_secret() {
        let directory = tempfile::tempdir().unwrap();
        let mut initial = Config::for_test(directory.path().to_owned());
        initial.registry_auth = Some(SecretString::from("client:password"));
        initial.credential_key = Some(SecretString::from("22".repeat(32)));
        let state = AppState::new(initial.clone()).await.unwrap();
        state
            .nodes
            .create(NodeInput {
                name: "private".into(),
                url: "http://127.0.0.1:5000".into(),
                registry_route_id: crate::registry_routes::DOCKER_HUB_ROUTE_ID,
                enabled: true,
                priority: 1,
                max_concurrency: 4,
                cf_preferred: false,
                connect_ip: None,
                auth_mode: "basic".into(),
                auth_username: Some("user".into()),
                auth_header: None,
                auth_secret: Some("secret".into()),
            })
            .await
            .unwrap();
        drop(state);

        initial.registry_auth = None;
        let error = AppState::new(initial).await.err().unwrap();
        assert!(error.to_string().contains("DONKEY_REGISTRY_AUTH"));
    }
}
