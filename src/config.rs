use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, bail};
use secrecy::{ExposeSecret, SecretString};

#[derive(Clone, Debug)]
pub struct Config {
    pub admin_addr: SocketAddr,
    pub registry_addr: SocketAddr,
    pub data_dir: PathBuf,
    pub database_url: String,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub admin_auth: Option<SecretString>,
    pub initial_admin_username: Option<String>,
    pub initial_admin_password: Option<SecretString>,
    pub admin_external_tls: bool,
    pub admin_external_loopback: bool,
    pub session_ttl: Duration,
    pub oidc: Option<OidcConfig>,
    pub registry_auth: Option<SecretString>,
    pub proxy_auth: Option<SecretString>,
    pub credential_key: Option<SecretString>,
    pub registry_external_tls: bool,
    pub connect_allow: Vec<String>,
    pub connect_remap: Vec<ConnectRemap>,
    pub allow_private_upstreams: bool,
    pub allow_insecure_upstreams: bool,
    pub chunk_size: u64,
    pub chunk_concurrency: usize,
    pub parallel_threshold: u64,
    pub resumable_threshold: u64,
    pub scheduler_policy: SchedulerPolicy,
    pub upstream_timeout: Duration,
    pub stream_fallback_timeout: Duration,
    pub partial_ttl: Duration,
    pub health_interval: Duration,
    pub max_cache_bytes: u64,
    pub cache_policy: CachePolicy,
    pub cache_high_watermark: f64,
    pub cache_low_watermark: f64,
    pub cache_ttl: Option<Duration>,
    pub max_export_bytes: u64,
    pub export_ttl: Duration,
    pub pull_logging_enabled: bool,
    pub pull_log_retention_days: u64,
    pub pull_log_max_entries: u64,
}

#[derive(Clone, Debug)]
pub struct OidcConfig {
    pub issuer: String,
    pub client_id: String,
    pub client_secret: SecretString,
    pub redirect_url: String,
    pub display_name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CachePolicy {
    Balanced,
    Lru,
    Lfu,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchedulerPolicy {
    Balanced,
    SpeedFirst,
}

impl std::fmt::Display for SchedulerPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Balanced => "balanced",
            Self::SpeedFirst => "speed-first",
        })
    }
}

impl std::fmt::Display for CachePolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Balanced => "balanced",
            Self::Lru => "lru",
            Self::Lfu => "lfu",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectRemap {
    pub source: String,
    pub target: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let data_dir = PathBuf::from(env_or("DONKEY_DATA_DIR", "./data"));
        let database_url = env::var("DONKEY_DATABASE_URL").unwrap_or_else(|_| {
            format!("sqlite://{}?mode=rwc", data_dir.join("donkey.db").display())
        });

        let tls_cert = optional_path("DONKEY_TLS_CERT");
        let tls_key = optional_path("DONKEY_TLS_KEY");
        if tls_cert.is_some() != tls_key.is_some() {
            bail!("DONKEY_TLS_CERT and DONKEY_TLS_KEY must be configured together");
        }

        let high = f64_env("DONKEY_CACHE_HIGH_WATERMARK", 0.90, 0.5, 1.0)?;
        let low = f64_env("DONKEY_CACHE_LOW_WATERMARK", 0.80, 0.1, 0.99)?;
        if low >= high {
            bail!("DONKEY_CACHE_LOW_WATERMARK must be lower than DONKEY_CACHE_HIGH_WATERMARK");
        }

        let initial_admin_username = optional_string("DONKEY_INITIAL_ADMIN_USERNAME");
        let initial_admin_password = optional_secret("DONKEY_INITIAL_ADMIN_PASSWORD");
        if initial_admin_username.is_some() != initial_admin_password.is_some() {
            bail!(
                "DONKEY_INITIAL_ADMIN_USERNAME and DONKEY_INITIAL_ADMIN_PASSWORD must be configured together"
            );
        }
        if initial_admin_username
            .as_deref()
            .is_some_and(|value| value.len() > 80 || value.trim().is_empty())
        {
            bail!("DONKEY_INITIAL_ADMIN_USERNAME must be 1-80 characters");
        }
        if initial_admin_password
            .as_ref()
            .is_some_and(|value| !(12..=1024).contains(&value.expose_secret().len()))
        {
            bail!("DONKEY_INITIAL_ADMIN_PASSWORD must be 12-1024 bytes");
        }
        let oidc = oidc_config()?;
        let session_ttl = duration_env("DONKEY_SESSION_TTL", "7d")?;
        if !(Duration::from_secs(5 * 60)..=Duration::from_secs(90 * 24 * 60 * 60))
            .contains(&session_ttl)
        {
            bail!("DONKEY_SESSION_TTL must be between 5m and 90d");
        }

        let config = Self {
            admin_addr: parse_addr("DONKEY_ADMIN_ADDR", "127.0.0.1:5003")?,
            registry_addr: parse_addr("DONKEY_REGISTRY_ADDR", "0.0.0.0:5443")?,
            data_dir,
            database_url,
            tls_cert,
            tls_key,
            admin_auth: optional_secret("DONKEY_ADMIN_AUTH"),
            initial_admin_username,
            initial_admin_password,
            admin_external_tls: bool_env("DONKEY_ADMIN_EXTERNAL_TLS", false)?,
            admin_external_loopback: bool_env("DONKEY_ADMIN_EXTERNAL_LOOPBACK", false)?,
            session_ttl,
            oidc,
            registry_auth: optional_secret("DONKEY_REGISTRY_AUTH"),
            proxy_auth: optional_secret("DONKEY_PROXY_AUTH"),
            credential_key: optional_secret("DONKEY_CREDENTIAL_KEY"),
            registry_external_tls: bool_env("DONKEY_REGISTRY_EXTERNAL_TLS", false)?,
            connect_allow: csv("DONKEY_CONNECT_ALLOW"),
            connect_remap: parse_remaps(&env::var("DONKEY_CONNECT_REMAP").unwrap_or_default())?,
            allow_private_upstreams: bool_env("DONKEY_ALLOW_PRIVATE_UPSTREAMS", false)?,
            allow_insecure_upstreams: bool_env("DONKEY_ALLOW_INSECURE_UPSTREAMS", false)?,
            chunk_size: u64_env(
                "DONKEY_CHUNK_SIZE",
                2 * 1024 * 1024,
                256 * 1024,
                32 * 1024 * 1024,
            )?,
            chunk_concurrency: usize_env("DONKEY_CHUNK_CONCURRENCY", 8, 1, 64)?,
            parallel_threshold: u64_env(
                "DONKEY_PARALLEL_THRESHOLD",
                8 * 1024 * 1024,
                1024 * 1024,
                1024 * 1024 * 1024,
            )?,
            resumable_threshold: u64_env(
                "DONKEY_RESUMABLE_THRESHOLD",
                8 * 1024 * 1024,
                1024 * 1024,
                u64::MAX,
            )?,
            scheduler_policy: scheduler_policy_env()?,
            upstream_timeout: duration_env("DONKEY_UPSTREAM_TIMEOUT", "30s")?,
            stream_fallback_timeout: duration_env("DONKEY_STREAM_FALLBACK_TIMEOUT", "10s")?,
            partial_ttl: duration_env("DONKEY_PARTIAL_TTL", "1h")?,
            health_interval: duration_env("DONKEY_HEALTH_INTERVAL", "60s")?,
            max_cache_bytes: u64_env(
                "DONKEY_MAX_CACHE_BYTES",
                50 * 1024 * 1024 * 1024,
                64 * 1024 * 1024,
                u64::MAX,
            )?,
            cache_policy: cache_policy_env()?,
            cache_high_watermark: high,
            cache_low_watermark: low,
            cache_ttl: optional_duration_env("DONKEY_CACHE_TTL")?,
            max_export_bytes: u64_env(
                "DONKEY_MAX_EXPORT_BYTES",
                20 * 1024 * 1024 * 1024,
                64 * 1024 * 1024,
                u64::MAX,
            )?,
            export_ttl: duration_env("DONKEY_EXPORT_TTL", "7d")?,
            pull_logging_enabled: bool_env("DONKEY_PULL_LOGGING_ENABLED", true)?,
            pull_log_retention_days: u64_env("DONKEY_PULL_LOG_RETENTION_DAYS", 30, 1, 3650)?,
            pull_log_max_entries: u64_env("DONKEY_PULL_LOG_MAX_ENTRIES", 10_000, 100, 1_000_000)?,
        };

        if config.admin_addr.ip().is_unspecified()
            && config.admin_auth.is_none()
            && config.initial_admin_username.is_none()
            && config.oidc.is_none()
        {
            bail!(
                "an admin authentication method is required when DONKEY_ADMIN_ADDR listens on all interfaces"
            );
        }
        if config.registry_auth.is_some()
            && config.tls_cert.is_none()
            && !config.registry_external_tls
        {
            bail!("Registry Basic auth requires TLS or DONKEY_REGISTRY_EXTERNAL_TLS=true");
        }
        Ok(config)
    }

    pub fn for_test(data_dir: PathBuf) -> Self {
        Self {
            admin_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            registry_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            database_url: format!("sqlite://{}?mode=rwc", data_dir.join("test.db").display()),
            data_dir,
            tls_cert: None,
            tls_key: None,
            admin_auth: None,
            initial_admin_username: None,
            initial_admin_password: None,
            admin_external_tls: false,
            admin_external_loopback: false,
            session_ttl: Duration::from_secs(7 * 24 * 60 * 60),
            oidc: None,
            registry_auth: None,
            proxy_auth: None,
            credential_key: None,
            registry_external_tls: false,
            connect_allow: Vec::new(),
            connect_remap: Vec::new(),
            allow_private_upstreams: true,
            allow_insecure_upstreams: true,
            chunk_size: 512 * 1024,
            chunk_concurrency: 4,
            parallel_threshold: 1024 * 1024,
            resumable_threshold: 8 * 1024 * 1024,
            scheduler_policy: SchedulerPolicy::Balanced,
            upstream_timeout: Duration::from_secs(5),
            stream_fallback_timeout: Duration::from_secs(10),
            partial_ttl: Duration::from_secs(3600),
            health_interval: Duration::from_secs(60),
            max_cache_bytes: 1024 * 1024 * 1024,
            cache_policy: CachePolicy::Balanced,
            cache_high_watermark: 0.9,
            cache_low_watermark: 0.8,
            cache_ttl: None,
            max_export_bytes: 1024 * 1024 * 1024,
            export_ttl: Duration::from_secs(7 * 24 * 60 * 60),
            pull_logging_enabled: true,
            pull_log_retention_days: 30,
            pull_log_max_entries: 10_000,
        }
    }

    /// Apply values persisted by the admin settings API. Environment values
    /// remain the initial defaults; persisted values win on subsequent starts.
    pub fn apply_runtime_overrides(
        &mut self,
        settings: &[crate::db::RuntimeSetting],
    ) -> anyhow::Result<()> {
        for setting in settings {
            match setting.key.as_str() {
                "chunk_size" => self.chunk_size = setting.value.parse()?,
                "chunk_concurrency" => self.chunk_concurrency = setting.value.parse()?,
                "parallel_threshold" => self.parallel_threshold = setting.value.parse()?,
                "resumable_threshold" => self.resumable_threshold = setting.value.parse()?,
                "scheduler_policy" => {
                    self.scheduler_policy = match setting.value.as_str() {
                        "balanced" => SchedulerPolicy::Balanced,
                        "speed-first" => SchedulerPolicy::SpeedFirst,
                        _ => anyhow::bail!("invalid persisted scheduler_policy"),
                    }
                }
                "upstream_timeout_seconds" => {
                    self.upstream_timeout = Duration::from_secs(setting.value.parse()?)
                }
                "stream_fallback_timeout_seconds" => {
                    self.stream_fallback_timeout = Duration::from_secs(setting.value.parse()?)
                }
                "partial_ttl_seconds" => {
                    self.partial_ttl = Duration::from_secs(setting.value.parse()?)
                }
                "max_cache_bytes" => self.max_cache_bytes = setting.value.parse()?,
                "cache_policy" => {
                    self.cache_policy = match setting.value.as_str() {
                        "balanced" => CachePolicy::Balanced,
                        "lru" => CachePolicy::Lru,
                        "lfu" => CachePolicy::Lfu,
                        _ => anyhow::bail!("invalid persisted cache_policy"),
                    }
                }
                "cache_high_watermark" => self.cache_high_watermark = setting.value.parse()?,
                "cache_low_watermark" => self.cache_low_watermark = setting.value.parse()?,
                "cache_ttl_seconds" => {
                    let seconds: u64 = setting.value.parse()?;
                    self.cache_ttl = (seconds > 0).then(|| Duration::from_secs(seconds));
                }
                "health_interval_seconds" => {
                    self.health_interval = Duration::from_secs(setting.value.parse()?)
                }
                "max_export_bytes" => self.max_export_bytes = setting.value.parse()?,
                "export_ttl_seconds" => {
                    self.export_ttl = Duration::from_secs(setting.value.parse()?)
                }
                "pull_logging_enabled" => self.pull_logging_enabled = setting.value.parse()?,
                "pull_log_retention_days" => {
                    self.pull_log_retention_days = setting.value.parse()?
                }
                "pull_log_max_entries" => self.pull_log_max_entries = setting.value.parse()?,
                _ => {}
            }
        }
        if self.cache_low_watermark >= self.cache_high_watermark {
            anyhow::bail!("persisted cache watermarks are invalid")
        }
        if !settings.is_empty()
            && (!(256 * 1024..=32 * 1024 * 1024).contains(&self.chunk_size)
                || !(1..=64).contains(&self.chunk_concurrency)
                || !(1024 * 1024..=u64::MAX).contains(&self.parallel_threshold)
                || !(1024 * 1024..=u64::MAX).contains(&self.resumable_threshold)
                || self.upstream_timeout.is_zero()
                || self.stream_fallback_timeout.is_zero()
                || self.partial_ttl.is_zero()
                || self.max_cache_bytes < 64 * 1024 * 1024
                || self.max_export_bytes < 64 * 1024 * 1024
                || !(1..=3650).contains(&self.pull_log_retention_days)
                || !(100..=1_000_000).contains(&self.pull_log_max_entries))
        {
            anyhow::bail!("persisted runtime settings are out of range")
        }
        Ok(())
    }

    pub fn admin_auth_value(&self) -> Option<&str> {
        self.admin_auth.as_ref().map(|value| value.expose_secret())
    }

    pub fn registry_auth_value(&self) -> Option<&str> {
        self.registry_auth
            .as_ref()
            .map(|value| value.expose_secret())
    }

    pub fn proxy_auth_value(&self) -> Option<&str> {
        self.proxy_auth.as_ref().map(|value| value.expose_secret())
    }

    pub fn admin_secret_transport_is_secure(&self) -> bool {
        self.admin_external_tls
            || self.admin_external_loopback
            || self.admin_addr.ip().is_loopback()
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn parse_addr(key: &str, default: &str) -> anyhow::Result<SocketAddr> {
    env_or(key, default)
        .parse()
        .with_context(|| format!("invalid {key}"))
}

fn optional_path(key: &str) -> Option<PathBuf> {
    env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn optional_secret(key: &str) -> Option<SecretString> {
    env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(Into::into)
}

fn optional_string(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.trim().is_empty())
}

fn oidc_config() -> anyhow::Result<Option<OidcConfig>> {
    let issuer = optional_string("DONKEY_OIDC_ISSUER");
    let client_id = optional_string("DONKEY_OIDC_CLIENT_ID");
    let client_secret = optional_secret("DONKEY_OIDC_CLIENT_SECRET");
    let redirect_url = optional_string("DONKEY_OIDC_REDIRECT_URL");
    let configured = [
        issuer.is_some(),
        client_id.is_some(),
        client_secret.is_some(),
        redirect_url.is_some(),
    ];
    if configured.iter().any(|value| *value) && !configured.iter().all(|value| *value) {
        bail!(
            "DONKEY_OIDC_ISSUER, CLIENT_ID, CLIENT_SECRET and REDIRECT_URL must be configured together"
        );
    }
    let Some(issuer) = issuer else {
        return Ok(None);
    };
    let issuer_url = url::Url::parse(&issuer).context("invalid DONKEY_OIDC_ISSUER")?;
    let redirect = url::Url::parse(redirect_url.as_deref().unwrap_or_default())
        .context("invalid DONKEY_OIDC_REDIRECT_URL")?;
    if issuer_url.scheme() != "https" || redirect.scheme() != "https" {
        bail!("OIDC issuer and redirect URL must use HTTPS");
    }
    let display_name = env_or("DONKEY_OIDC_DISPLAY_NAME", "Single sign-on");
    if display_name.trim().is_empty() || display_name.len() > 80 {
        bail!("DONKEY_OIDC_DISPLAY_NAME must be 1-80 characters");
    }
    Ok(Some(OidcConfig {
        issuer,
        client_id: client_id.unwrap_or_default(),
        client_secret: client_secret.expect("checked above"),
        redirect_url: redirect.to_string(),
        display_name,
    }))
}

fn csv(key: &str) -> Vec<String> {
    env::var(key)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn bool_env(key: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(key) {
        Ok(value) => value.parse().with_context(|| format!("invalid {key}")),
        Err(_) => Ok(default),
    }
}

fn duration_env(key: &str, default: &str) -> anyhow::Result<Duration> {
    humantime::parse_duration(&env_or(key, default)).with_context(|| format!("invalid {key}"))
}

fn optional_duration_env(key: &str) -> anyhow::Result<Option<Duration>> {
    env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| humantime::parse_duration(&value).with_context(|| format!("invalid {key}")))
        .transpose()
}

fn cache_policy_env() -> anyhow::Result<CachePolicy> {
    match env_or("DONKEY_CACHE_POLICY", "balanced")
        .to_ascii_lowercase()
        .as_str()
    {
        "balanced" => Ok(CachePolicy::Balanced),
        "lru" => Ok(CachePolicy::Lru),
        "lfu" => Ok(CachePolicy::Lfu),
        _ => bail!("DONKEY_CACHE_POLICY must be balanced, lru, or lfu"),
    }
}

fn scheduler_policy_env() -> anyhow::Result<SchedulerPolicy> {
    match env_or("DONKEY_SCHEDULER_POLICY", "balanced")
        .to_ascii_lowercase()
        .as_str()
    {
        "balanced" => Ok(SchedulerPolicy::Balanced),
        "speed-first" | "speed" => Ok(SchedulerPolicy::SpeedFirst),
        _ => bail!("DONKEY_SCHEDULER_POLICY must be balanced or speed-first"),
    }
}

fn f64_env(key: &str, default: f64, min: f64, max: f64) -> anyhow::Result<f64> {
    let value = env_or(key, &default.to_string())
        .parse::<f64>()
        .with_context(|| format!("invalid {key}"))?;
    if !value.is_finite() || !(min..=max).contains(&value) {
        bail!("{key} must be between {min} and {max}");
    }
    Ok(value)
}

fn u64_env(key: &str, default: u64, min: u64, max: u64) -> anyhow::Result<u64> {
    let value = env_or(key, &default.to_string())
        .parse::<u64>()
        .with_context(|| format!("invalid {key}"))?;
    if !(min..=max).contains(&value) {
        bail!("{key} must be between {min} and {max}");
    }
    Ok(value)
}

fn usize_env(key: &str, default: usize, min: usize, max: usize) -> anyhow::Result<usize> {
    let value = env_or(key, &default.to_string())
        .parse::<usize>()
        .with_context(|| format!("invalid {key}"))?;
    if !(min..=max).contains(&value) {
        bail!("{key} must be between {min} and {max}");
    }
    Ok(value)
}

fn parse_remaps(raw: &str) -> anyhow::Result<Vec<ConnectRemap>> {
    raw.split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|entry| {
            let (source, target) = entry
                .split_once('=')
                .with_context(|| format!("invalid CONNECT remap {entry}"))?;
            if source.is_empty()
                || target.is_empty()
                || !source.contains(':')
                || !target.contains(':')
            {
                bail!("CONNECT remap must use source:port=target:port");
            }
            Ok(ConnectRemap {
                source: source.to_ascii_lowercase(),
                target: target.to_owned(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{Config, parse_remaps};
    use std::path::PathBuf;

    #[test]
    fn parses_connect_remaps() {
        let remaps = parse_remaps("registry.example:443=127.0.0.1:5443").unwrap();
        assert_eq!(remaps[0].source, "registry.example:443");
        assert_eq!(remaps[0].target, "127.0.0.1:5443");
    }

    #[test]
    fn rejects_ambiguous_remaps() {
        assert!(parse_remaps("registry.example").is_err());
    }

    #[test]
    fn persisted_runtime_settings_override_environment_defaults() {
        let mut config = Config::for_test(PathBuf::from("./target/config-test"));
        config
            .apply_runtime_overrides(&[
                crate::db::RuntimeSetting {
                    key: "resumable_threshold".into(),
                    value: "4194304".into(),
                },
                crate::db::RuntimeSetting {
                    key: "stream_fallback_timeout_seconds".into(),
                    value: "22".into(),
                },
            ])
            .unwrap();
        assert_eq!(config.resumable_threshold, 4 * 1024 * 1024);
        assert_eq!(config.stream_fallback_timeout.as_secs(), 22);
    }
}
