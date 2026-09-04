use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{
        SaltString,
        rand_core::{OsRng, RngCore},
    },
};
use axum::{
    Extension, Json, Router,
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post, put},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use moka::future::Cache;
use openidconnect::{
    AccessTokenHash, AuthorizationCode, ClaimsVerificationError, ClientId, ClientSecret, CsrfToken,
    EndpointMaybeSet, EndpointNotSet, EndpointSet, IssuerUrl, Nonce, OAuth2TokenResponse,
    PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope, SignatureVerificationError,
    core::{CoreAuthenticationFlow, CoreClient, CoreIdToken, CoreProviderMetadata},
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    PaginatorTrait, QueryFilter, Set, SqliteTransactionMode, TransactionOptions, TransactionTrait,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock};
use tower_http::set_header::SetResponseHeaderLayer;
use uuid::Uuid;

use crate::{
    config::Config,
    db::{admin_session, oidc_login_state, user},
    error::{ApiResult, AppError},
};

const SESSION_COOKIE: &str = "donkey_session";
const MAX_FAILED_ATTEMPTS: usize = 5;
const FAILED_ATTEMPT_WINDOW: Duration = Duration::from_secs(15 * 60);

type OidcClient = CoreClient<
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointMaybeSet,
    EndpointMaybeSet,
>;

#[derive(Clone)]
pub struct AuthService {
    config: Arc<Config>,
    db: DatabaseConnection,
    oidc: Option<Arc<OidcRuntime>>,
    failed_logins: Cache<String, Arc<Mutex<VecDeque<Instant>>>>,
    dummy_password_hash: Arc<String>,
}

struct OidcRuntime {
    client: RwLock<OidcClient>,
    refresh_lock: Mutex<()>,
    http: openidconnect::reqwest::Client,
    issuer: String,
    display_name: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct AuthPrincipal {
    pub id: Option<Uuid>,
    pub username: String,
    pub display_name: String,
    pub role: String,
    pub legacy: bool,
    #[serde(rename = "local_password")]
    pub local_password: bool,
    #[serde(skip)]
    pub session_token_hash: Option<String>,
}

impl AuthPrincipal {
    pub fn is_admin(&self) -> bool {
        self.role == "admin"
    }
}

#[derive(Deserialize)]
struct LoginInput {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct AuthConfigView {
    local_enabled: bool,
    oidc_enabled: bool,
    oidc_name: Option<String>,
}

#[derive(Deserialize)]
struct OidcStartQuery {
    return_to: Option<String>,
}

#[derive(Deserialize)]
struct OidcCallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

impl AuthService {
    pub async fn new(config: Arc<Config>, db: DatabaseConnection) -> ApiResult<Self> {
        let dummy_password_hash =
            Arc::new(hash_password(SecretString::from("donkey-dummy-password-never-used")).await?);
        let oidc = match &config.oidc {
            Some(value) => {
                let http = openidconnect::reqwest::ClientBuilder::new()
                    .redirect(openidconnect::reqwest::redirect::Policy::none())
                    .connect_timeout(Duration::from_secs(10))
                    .timeout(Duration::from_secs(20))
                    .build()
                    .map_err(AppError::internal)?;
                let (client, discovered_issuer) = discover_oidc_client(value, &http).await?;
                Some(Arc::new(OidcRuntime {
                    client: RwLock::new(client),
                    refresh_lock: Mutex::new(()),
                    http,
                    issuer: discovered_issuer,
                    display_name: value.display_name.clone(),
                }))
            }
            None => None,
        };
        let service = Self {
            config,
            db,
            oidc,
            failed_logins: Cache::builder()
                .max_capacity(10_000)
                .time_to_idle(Duration::from_secs(60 * 60))
                .build(),
            dummy_password_hash,
        };
        service.bootstrap_local_admin().await?;
        service.cleanup_expired().await?;
        Ok(service)
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/config", get(auth_config))
            .route("/login", post(login))
            .route("/logout", post(logout))
            .route("/me", get(me))
            .route("/profile", put(update_profile))
            .route("/oidc/start", get(oidc_start))
            .route("/oidc/callback", get(oidc_callback))
            .layer(SetResponseHeaderLayer::overriding(
                header::CACHE_CONTROL,
                HeaderValue::from_static("no-store"),
            ))
            .with_state(self)
    }

    pub async fn authenticate(&self, headers: &HeaderMap) -> ApiResult<AuthPrincipal> {
        let token = cookie_value(headers, SESSION_COOKIE).ok_or(AppError::Unauthorized)?;
        let token_hash = token_hash(&token);
        let session = admin_session::Entity::find_by_id(&token_hash)
            .one(&self.db)
            .await?
            .ok_or(AppError::Unauthorized)?;
        if session.expires_at <= Utc::now() {
            admin_session::Entity::delete_by_id(&token_hash)
                .exec(&self.db)
                .await?;
            return Err(AppError::Unauthorized);
        }
        let account = user::Entity::find_by_id(session.user_id)
            .one(&self.db)
            .await?
            .filter(|account| account.enabled)
            .ok_or(AppError::Unauthorized)?;
        if session.last_seen_at < Utc::now() - chrono::Duration::minutes(5) {
            let mut active = session.into_active_model();
            active.last_seen_at = Set(Utc::now());
            active.update(&self.db).await?;
        }
        let mut principal = principal(account);
        principal.session_token_hash = Some(token_hash);
        Ok(principal)
    }

    pub fn authenticate_legacy_basic(&self, headers: &HeaderMap) -> Option<AuthPrincipal> {
        let expected = self.config.admin_auth_value()?;
        let decoded = headers
            .get(header::AUTHORIZATION)?
            .to_str()
            .ok()?
            .strip_prefix("Basic ")
            .and_then(|value| base64::engine::general_purpose::STANDARD.decode(value).ok())?;
        if decoded.len() != expected.len()
            || !constant_time_eq::constant_time_eq(&decoded, expected.as_bytes())
        {
            return None;
        }
        let username = expected.split_once(':').map_or("admin", |(name, _)| name);
        Some(AuthPrincipal {
            id: None,
            username: username.to_owned(),
            display_name: username.to_owned(),
            role: "admin".into(),
            legacy: true,
            local_password: false,
            session_token_hash: None,
        })
    }

    async fn bootstrap_local_admin(&self) -> ApiResult<()> {
        let (Some(username), Some(password)) = (
            self.config.initial_admin_username.as_deref(),
            self.config.initial_admin_password.as_ref(),
        ) else {
            return Ok(());
        };
        validate_username(username)?;
        validate_password(password.expose_secret())?;
        let password_hash = hash_password(password.clone()).await?;
        let now = Utc::now();
        let transaction = self
            .db
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await?;
        if user::Entity::find().count(&transaction).await? > 0 {
            transaction.commit().await?;
            return Ok(());
        }
        user::Model {
            id: Uuid::new_v4(),
            identity_key: local_identity_key(username),
            username: Some(username.to_owned()),
            issuer: None,
            subject: username.to_owned(),
            display_name: username.to_owned(),
            email: None,
            password_hash: Some(password_hash),
            role: "admin".into(),
            enabled: true,
            created_at: now,
            updated_at: now,
            last_login_at: None,
        }
        .into_active_model()
        .insert(&transaction)
        .await?;
        transaction.commit().await?;
        tracing::info!(username, "initial local administrator created");
        Ok(())
    }

    async fn login_local(&self, input: LoginInput) -> ApiResult<(AuthPrincipal, String)> {
        validate_username(&input.username)?;
        if input.password.len() > 1024 {
            return Err(AppError::Unauthorized);
        }
        let attempt_key = token_hash(&input.username.to_ascii_lowercase());
        self.reserve_login_attempt(&attempt_key).await?;
        let account = user::Entity::find()
            .filter(user::Column::Username.eq(input.username.clone()))
            .one(&self.db)
            .await?;
        let password_hash = account
            .as_ref()
            .and_then(|value| value.password_hash.clone())
            .unwrap_or_else(|| (*self.dummy_password_hash).clone());
        let valid = verify_password(password_hash, SecretString::from(input.password)).await?;
        let Some(account) = account.filter(|value| value.enabled && valid) else {
            return Err(AppError::Unauthorized);
        };
        self.clear_failed_login(&attempt_key).await;
        self.mark_login(account.id).await?;
        let token = self.create_session(account.id).await?;
        Ok((principal(account), token))
    }

    async fn reserve_login_attempt(&self, key: &str) -> ApiResult<()> {
        let attempts = self
            .failed_logins
            .get_with(key.to_owned(), async {
                Arc::new(Mutex::new(VecDeque::new()))
            })
            .await;
        let now = Instant::now();
        let mut values = attempts.lock().await;
        while values
            .front()
            .is_some_and(|time| now.duration_since(*time) > FAILED_ATTEMPT_WINDOW)
        {
            values.pop_front();
        }
        if values.len() >= MAX_FAILED_ATTEMPTS {
            Err(AppError::RateLimited)
        } else {
            values.push_back(now);
            Ok(())
        }
    }

    async fn clear_failed_login(&self, key: &str) {
        self.failed_logins.invalidate(key).await;
    }

    async fn create_session(&self, user_id: Uuid) -> ApiResult<String> {
        let mut bytes = [0_u8; 32];
        OsRng
            .try_fill_bytes(&mut bytes)
            .map_err(|error| AppError::internal(anyhow::anyhow!(error)))?;
        let token = URL_SAFE_NO_PAD.encode(bytes);
        let now = Utc::now();
        admin_session::Model {
            token_hash: token_hash(&token),
            user_id,
            created_at: now,
            last_seen_at: now,
            expires_at: now
                + chrono::Duration::from_std(self.config.session_ttl)
                    .map_err(AppError::internal)?,
        }
        .into_active_model()
        .insert(&self.db)
        .await?;
        Ok(token)
    }

    async fn revoke_session(&self, headers: &HeaderMap) -> ApiResult<()> {
        if let Some(token) = cookie_value(headers, SESSION_COOKIE) {
            admin_session::Entity::delete_by_id(token_hash(&token))
                .exec(&self.db)
                .await?;
        }
        Ok(())
    }

    async fn mark_login(&self, id: Uuid) -> ApiResult<()> {
        if let Some(account) = user::Entity::find_by_id(id).one(&self.db).await? {
            let mut active = account.into_active_model();
            active.last_login_at = Set(Some(Utc::now()));
            active.updated_at = Set(Utc::now());
            active.update(&self.db).await?;
        }
        Ok(())
    }

    pub async fn cleanup_expired(&self) -> ApiResult<()> {
        admin_session::Entity::delete_many()
            .filter(admin_session::Column::ExpiresAt.lt(Utc::now()))
            .exec(&self.db)
            .await?;
        oidc_login_state::Entity::delete_many()
            .filter(oidc_login_state::Column::ExpiresAt.lt(Utc::now()))
            .exec(&self.db)
            .await?;
        Ok(())
    }

    async fn oidc_authorize_url(&self, return_to: String) -> ApiResult<String> {
        self.cleanup_expired().await?;
        let runtime = self
            .oidc
            .as_ref()
            .ok_or_else(|| AppError::not_found("OIDC provider"))?;
        let return_to = safe_return_to(&return_to);
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let client = runtime.client.read().await;
        let (url, state, nonce) = client
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                CsrfToken::new_random,
                Nonce::new_random,
            )
            .add_scope(Scope::new("email".into()))
            .add_scope(Scope::new("profile".into()))
            .set_pkce_challenge(challenge)
            .url();
        let now = Utc::now();
        oidc_login_state::Model {
            state_hash: token_hash(state.secret()),
            nonce: nonce.secret().to_owned(),
            pkce_verifier: verifier.secret().to_owned(),
            return_to,
            created_at: now,
            expires_at: now + chrono::Duration::minutes(10),
        }
        .into_active_model()
        .insert(&self.db)
        .await?;
        Ok(url.to_string())
    }

    async fn finish_oidc(
        &self,
        code: String,
        state: String,
    ) -> ApiResult<(AuthPrincipal, String, String)> {
        let runtime = self.oidc.as_ref().ok_or(AppError::Unauthorized)?;
        let state_hash = token_hash(&state);
        let stored = oidc_login_state::Entity::find_by_id(&state_hash)
            .one(&self.db)
            .await?
            .ok_or(AppError::Unauthorized)?;
        let deleted = oidc_login_state::Entity::delete_by_id(&state_hash)
            .exec(&self.db)
            .await?;
        if deleted.rows_affected != 1 || stored.expires_at <= Utc::now() {
            return Err(AppError::Unauthorized);
        }
        let mut client = runtime.client.read().await.clone();
        let token_response = client
            .exchange_code(AuthorizationCode::new(code))
            .map_err(|_| AppError::Unauthorized)?
            .set_pkce_verifier(PkceCodeVerifier::new(stored.pkce_verifier))
            .request_async(&runtime.http)
            .await
            .map_err(|_| AppError::Unauthorized)?;
        let id_token = token_response
            .extra_fields()
            .id_token()
            .ok_or(AppError::Unauthorized)?;
        let nonce = Nonce::new(stored.nonce);
        if let Err(error) = verify_oidc_id_token(&client, id_token, &nonce) {
            if !is_missing_signing_key(&error) {
                return Err(AppError::Unauthorized);
            }
            let _refresh_guard = runtime.refresh_lock.lock().await;
            client = runtime.client.read().await.clone();
            if let Err(error) = verify_oidc_id_token(&client, id_token, &nonce) {
                if !is_missing_signing_key(&error) {
                    return Err(AppError::Unauthorized);
                }
                let config = self.config.oidc.as_ref().ok_or(AppError::Unauthorized)?;
                let (refreshed, issuer) = discover_oidc_client(config, &runtime.http)
                    .await
                    .map_err(|error| {
                        tracing::warn!(?error, "OIDC signing key refresh failed");
                        AppError::Unauthorized
                    })?;
                if issuer != runtime.issuer {
                    return Err(AppError::Unauthorized);
                }
                *runtime.client.write().await = refreshed.clone();
                client = refreshed;
            }
        }
        let verifier = client.id_token_verifier();
        let claims = id_token
            .claims(&verifier, &nonce)
            .map_err(|_| AppError::Unauthorized)?;
        if let Some(expected_hash) = claims.access_token_hash() {
            let actual_hash = AccessTokenHash::from_token(
                token_response.access_token(),
                id_token.signing_alg().map_err(|_| AppError::Unauthorized)?,
                id_token
                    .signing_key(&verifier)
                    .map_err(|_| AppError::Unauthorized)?,
            )
            .map_err(|_| AppError::Unauthorized)?;
            if &actual_hash != expected_hash {
                return Err(AppError::Unauthorized);
            }
        }
        let subject = claims.subject().as_str().to_owned();
        if subject.is_empty() || subject.len() > 512 {
            return Err(AppError::Unauthorized);
        }
        let email = claims.email().map(|value| value.as_str().to_owned());
        let account = self
            .provision_oidc_user(&runtime.issuer, &subject, email)
            .await?;
        self.mark_login(account.id).await?;
        let token = self.create_session(account.id).await?;
        Ok((principal(account), token, stored.return_to))
    }

    async fn provision_oidc_user(
        &self,
        issuer: &str,
        subject: &str,
        email: Option<String>,
    ) -> ApiResult<user::Model> {
        let identity_key = oidc_identity_key(issuer, subject);
        let transaction = self
            .db
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await?;
        if let Some(account) = user::Entity::find()
            .filter(user::Column::IdentityKey.eq(&identity_key))
            .one(&transaction)
            .await?
        {
            return if account.enabled {
                transaction.commit().await?;
                Ok(account)
            } else {
                transaction.rollback().await?;
                Err(AppError::Forbidden)
            };
        }
        let first = user::Entity::find().count(&transaction).await? == 0;
        let now = Utc::now();
        let account = user::Model {
            id: Uuid::new_v4(),
            identity_key,
            username: None,
            issuer: Some(issuer.to_owned()),
            subject: subject.to_owned(),
            display_name: email.clone().unwrap_or_else(|| "OIDC user".into()),
            email,
            password_hash: None,
            role: if first { "admin" } else { "member" }.into(),
            enabled: true,
            created_at: now,
            updated_at: now,
            last_login_at: None,
        }
        .into_active_model()
        .insert(&transaction)
        .await?;
        transaction.commit().await?;
        Ok(account)
    }

    fn session_cookie(&self, token: &str) -> ApiResult<HeaderValue> {
        let secure = if self.config.admin_external_tls {
            "; Secure"
        } else {
            ""
        };
        format!(
            "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}{}",
            self.config.session_ttl.as_secs(),
            secure
        )
        .parse()
        .map_err(AppError::internal)
    }

    fn clear_cookie(&self) -> HeaderValue {
        let secure = if self.config.admin_external_tls {
            "; Secure"
        } else {
            ""
        };
        HeaderValue::from_str(&format!(
            "{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0{secure}"
        ))
        .unwrap_or_else(|_| HeaderValue::from_static("donkey_session=; Max-Age=0"))
    }
}

async fn discover_oidc_client(
    config: &crate::config::OidcConfig,
    http: &openidconnect::reqwest::Client,
) -> ApiResult<(OidcClient, String)> {
    let issuer = IssuerUrl::new(config.issuer.clone())
        .map_err(|error| AppError::internal(anyhow::anyhow!(error)))?;
    let metadata = CoreProviderMetadata::discover_async(issuer, http)
        .await
        .map_err(|error| AppError::internal(anyhow::anyhow!("OIDC discovery failed: {error}")))?;
    let discovered_issuer = metadata.issuer().to_string();
    let client = CoreClient::from_provider_metadata(
        metadata,
        ClientId::new(config.client_id.clone()),
        Some(ClientSecret::new(
            config.client_secret.expose_secret().to_owned(),
        )),
    )
    .set_redirect_uri(
        RedirectUrl::new(config.redirect_url.clone())
            .map_err(|error| AppError::internal(anyhow::anyhow!(error)))?,
    );
    Ok((client, discovered_issuer))
}

fn verify_oidc_id_token(
    client: &OidcClient,
    id_token: &CoreIdToken,
    nonce: &Nonce,
) -> Result<(), ClaimsVerificationError> {
    id_token
        .claims(&client.id_token_verifier(), nonce)
        .map(|_| ())
}

fn is_missing_signing_key(error: &ClaimsVerificationError) -> bool {
    matches!(
        error,
        ClaimsVerificationError::SignatureVerification(SignatureVerificationError::NoMatchingKey)
    )
}

pub fn router(service: AuthService) -> Router {
    service.router()
}

async fn auth_config(State(service): State<AuthService>) -> ApiResult<Json<AuthConfigView>> {
    let local_enabled = user::Entity::find()
        .filter(user::Column::PasswordHash.is_not_null())
        .one(&service.db)
        .await?
        .is_some();
    Ok(Json(AuthConfigView {
        local_enabled,
        oidc_enabled: service.oidc.is_some(),
        oidc_name: service
            .oidc
            .as_ref()
            .map(|value| value.display_name.clone()),
    }))
}

async fn login(
    State(service): State<AuthService>,
    Json(input): Json<LoginInput>,
) -> ApiResult<Response> {
    let (principal, token) = service.login_local(input).await?;
    let mut response = Json(principal).into_response();
    response
        .headers_mut()
        .insert(header::SET_COOKIE, service.session_cookie(&token)?);
    Ok(response)
}

async fn logout(State(service): State<AuthService>, headers: HeaderMap) -> ApiResult<Response> {
    service.revoke_session(&headers).await?;
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .headers_mut()
        .insert(header::SET_COOKIE, service.clear_cookie());
    Ok(response)
}

async fn me(Extension(principal): Extension<AuthPrincipal>) -> Json<AuthPrincipal> {
    Json(principal)
}

#[derive(Deserialize)]
struct ProfileInput {
    display_name: String,
    username: Option<String>,
    current_password: Option<String>,
    new_password: Option<String>,
}

async fn update_profile(
    State(service): State<AuthService>,
    Extension(current_user): Extension<AuthPrincipal>,
    Json(input): Json<ProfileInput>,
) -> ApiResult<Json<AuthPrincipal>> {
    let id = current_user.id.ok_or(AppError::Unauthorized)?;
    if input.display_name.trim().is_empty() || input.display_name.chars().count() > 80 {
        return Err(AppError::bad_request(
            "display name must be 1-80 characters",
        ));
    }
    let mut account = user::Entity::find_by_id(id)
        .one(&service.db)
        .await?
        .ok_or(AppError::Unauthorized)?;
    let password_changed = input.new_password.is_some();
    let username_changed = input
        .username
        .as_deref()
        .is_some_and(|username| account.username.as_deref() != Some(username));
    let changing_credentials = username_changed || password_changed;
    if changing_credentials {
        let Some(hash) = account.password_hash.clone() else {
            return Err(AppError::bad_request(
                "OIDC accounts manage login credentials with the identity provider",
            ));
        };
        let current = input
            .current_password
            .as_deref()
            .ok_or(AppError::Unauthorized)?;
        if !verify_password(hash, SecretString::from(current.to_owned())).await? {
            return Err(AppError::Unauthorized);
        }
    }
    if username_changed && let Some(username) = input.username.as_deref() {
        validate_username(username)?;
        if user::Entity::find()
            .filter(user::Column::Username.eq(username))
            .filter(user::Column::Id.ne(id))
            .one(&service.db)
            .await?
            .is_some()
        {
            return Err(AppError::conflict("username is already in use"));
        }
        account.username = Some(username.to_owned());
    }
    if let Some(password) = input.new_password {
        validate_password(&password)?;
        account.password_hash = Some(hash_password(SecretString::from(password)).await?);
    }
    account.display_name = input.display_name.trim().to_owned();
    account.updated_at = Utc::now();
    let username = account.username.clone();
    let password_hash = account.password_hash.clone();
    let display_name = account.display_name.clone();
    let updated_at = account.updated_at;
    let mut active = account.into_active_model();
    active.username = Set(username);
    active.password_hash = Set(password_hash);
    active.display_name = Set(display_name);
    active.updated_at = Set(updated_at);
    let updated = active.update(&service.db).await?;
    if password_changed && let Some(current_token_hash) = current_user.session_token_hash.as_deref()
    {
        admin_session::Entity::delete_many()
            .filter(admin_session::Column::UserId.eq(id))
            .filter(admin_session::Column::TokenHash.ne(current_token_hash))
            .exec(&service.db)
            .await?;
    }
    Ok(Json(principal(updated)))
}

async fn oidc_start(
    State(service): State<AuthService>,
    Query(query): Query<OidcStartQuery>,
) -> ApiResult<Redirect> {
    let url = service
        .oidc_authorize_url(query.return_to.unwrap_or_else(|| "/".into()))
        .await?;
    Ok(Redirect::temporary(&url))
}

async fn oidc_callback(
    State(service): State<AuthService>,
    Query(query): Query<OidcCallbackQuery>,
) -> ApiResult<Response> {
    if query.error.is_some() {
        return Ok(Redirect::temporary("/login?error=oidc").into_response());
    }
    let (principal, token, return_to) = service
        .finish_oidc(
            query.code.ok_or(AppError::Unauthorized)?,
            query.state.ok_or(AppError::Unauthorized)?,
        )
        .await?;
    tracing::info!(user_id = ?principal.id, "OIDC login succeeded");
    let mut response = Redirect::temporary(&return_to).into_response();
    response
        .headers_mut()
        .insert(header::SET_COOKIE, service.session_cookie(&token)?);
    Ok(response)
}

async fn hash_password(password: SecretString) -> ApiResult<String> {
    tokio::task::spawn_blocking(move || {
        let salt = SaltString::generate(&mut OsRng);
        Argon2::default()
            .hash_password(password.expose_secret().as_bytes(), &salt)
            .map(|value| value.to_string())
            .map_err(|error| anyhow::anyhow!(error))
    })
    .await
    .map_err(AppError::internal)?
    .map_err(AppError::internal)
}

async fn verify_password(hash: String, password: SecretString) -> ApiResult<bool> {
    tokio::task::spawn_blocking(move || {
        let parsed = PasswordHash::new(&hash).map_err(|error| anyhow::anyhow!(error))?;
        Ok::<_, anyhow::Error>(
            Argon2::default()
                .verify_password(password.expose_secret().as_bytes(), &parsed)
                .is_ok(),
        )
    })
    .await
    .map_err(AppError::internal)?
    .map_err(AppError::internal)
}

fn validate_username(value: &str) -> ApiResult<()> {
    if value.trim() != value
        || value.len() < 2
        || value.len() > 80
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@'))
    {
        return Err(AppError::bad_request(
            "username must be 2-80 ASCII letters, digits, '.', '_', '-', or '@'",
        ));
    }
    Ok(())
}

fn validate_password(value: &str) -> ApiResult<()> {
    if value.len() < 12 || value.len() > 1024 {
        return Err(AppError::bad_request(
            "initial administrator password must be 12-1024 bytes",
        ));
    }
    Ok(())
}

fn principal(account: user::Model) -> AuthPrincipal {
    AuthPrincipal {
        id: Some(account.id),
        username: account
            .username
            .clone()
            .or(account.email.clone())
            .unwrap_or_else(|| account.subject.clone()),
        display_name: account.display_name,
        role: account.role,
        legacy: false,
        local_password: account.password_hash.is_some(),
        session_token_hash: None,
    }
}

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(key, value)| (key == name && !value.is_empty()).then(|| value.to_owned()))
}

fn token_hash(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn local_identity_key(username: &str) -> String {
    format!("local:{}", username.to_ascii_lowercase())
}

fn oidc_identity_key(issuer: &str, subject: &str) -> String {
    format!("oidc:{}:{}", token_hash(issuer), subject)
}

fn safe_return_to(value: &str) -> String {
    if value.starts_with('/')
        && !value.starts_with("//")
        && !value.contains(['\r', '\n'])
        && value.len() <= 1024
    {
        value.to_owned()
    } else {
        "/".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use openidconnect::{
        Audience, EndUserEmail, JsonWebKeyId, PrivateSigningKey, StandardClaims, SubjectIdentifier,
        core::{
            CoreEdDsaPrivateSigningKey, CoreIdToken, CoreIdTokenClaims, CoreJwsSigningAlgorithm,
        },
    };
    use std::collections::HashMap;

    #[tokio::test]
    async fn bootstrap_is_idempotent_and_session_revocation_is_server_side() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.initial_admin_username = Some("owner".into());
        config.initial_admin_password = Some(SecretString::from("a-secure-password"));
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let config = Arc::new(config);
        let service = AuthService::new(config.clone(), db.clone()).await.unwrap();
        AuthService::new(config, db.clone()).await.unwrap();
        assert_eq!(user::Entity::find().count(&db).await.unwrap(), 1);

        let (_, token) = service
            .login_local(LoginInput {
                username: "owner".into(),
                password: "a-secure-password".into(),
            })
            .await
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            format!("{SESSION_COOKIE}={token}").parse().unwrap(),
        );
        assert!(service.authenticate(&headers).await.unwrap().is_admin());
        service.revoke_session(&headers).await.unwrap();
        assert!(matches!(
            service.authenticate(&headers).await,
            Err(AppError::Unauthorized)
        ));
    }

    #[tokio::test]
    async fn concurrent_first_oidc_provisioning_creates_exactly_one_admin() {
        let directory = tempfile::tempdir().unwrap();
        let config = Arc::new(Config::for_test(directory.path().to_owned()));
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let service = AuthService::new(config, db.clone()).await.unwrap();
        let first = service.clone();
        let second = service.clone();
        let (left, right) = tokio::join!(
            first.provision_oidc_user("https://issuer.example", "subject-a", None),
            second.provision_oidc_user("https://issuer.example", "subject-b", None)
        );
        left.unwrap();
        right.unwrap();
        let accounts = user::Entity::find().all(&db).await.unwrap();
        assert_eq!(accounts.len(), 2);
        assert_eq!(
            accounts.iter().filter(|user| user.role == "admin").count(),
            1
        );
    }

    #[tokio::test]
    async fn independent_connections_serialize_first_oidc_admin_assignment() {
        let directory = tempfile::tempdir().unwrap();
        let config = Arc::new(Config::for_test(directory.path().to_owned()));
        let first_db = crate::db::connect(&config.database_url).await.unwrap();
        let second_db = crate::db::connect(&config.database_url).await.unwrap();
        let first = AuthService::new(config.clone(), first_db.clone())
            .await
            .unwrap();
        let second = AuthService::new(config, second_db).await.unwrap();

        let (left, right) = tokio::join!(
            first.provision_oidc_user("https://issuer.example", "connection-a", None),
            second.provision_oidc_user("https://issuer.example", "connection-b", None)
        );
        left.unwrap();
        right.unwrap();

        let accounts = user::Entity::find().all(&first_db).await.unwrap();
        assert_eq!(accounts.len(), 2);
        assert_eq!(
            accounts.iter().filter(|user| user.role == "admin").count(),
            1
        );
    }

    #[tokio::test]
    async fn repeated_failed_local_login_is_rate_limited() {
        let directory = tempfile::tempdir().unwrap();
        let config = Arc::new(Config::for_test(directory.path().to_owned()));
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let service = AuthService::new(config, db).await.unwrap();
        for _ in 0..MAX_FAILED_ATTEMPTS {
            assert!(matches!(
                service
                    .login_local(LoginInput {
                        username: "missing-user".into(),
                        password: "incorrect-password".into(),
                    })
                    .await,
                Err(AppError::Unauthorized)
            ));
        }
        assert!(matches!(
            service
                .login_local(LoginInput {
                    username: "missing-user".into(),
                    password: "incorrect-password".into(),
                })
                .await,
            Err(AppError::RateLimited)
        ));
    }

    #[tokio::test]
    async fn unchanged_login_name_does_not_require_password_for_profile_update() {
        let directory = tempfile::tempdir().unwrap();
        let config = Arc::new(Config::for_test(directory.path().to_owned()));
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let service = AuthService::new(config, db.clone()).await.unwrap();
        let now = Utc::now();
        let account = user::Model {
            id: Uuid::new_v4(),
            identity_key: "local:admin".into(),
            username: Some("admin".into()),
            issuer: None,
            subject: "admin".into(),
            display_name: "Old name".into(),
            email: None,
            password_hash: Some(
                hash_password(SecretString::from("donkey-test-password"))
                    .await
                    .unwrap(),
            ),
            role: "admin".into(),
            enabled: true,
            created_at: now,
            updated_at: now,
            last_login_at: None,
        }
        .into_active_model()
        .insert(&db)
        .await
        .unwrap();

        let response = update_profile(
            State(service),
            Extension(principal(account)),
            Json(ProfileInput {
                display_name: "New name".into(),
                username: Some("admin".into()),
                current_password: None,
                new_password: None,
            }),
        )
        .await
        .unwrap();

        assert_eq!(response.0.display_name, "New name");
        assert_eq!(
            user::Entity::find_by_id(response.0.id.unwrap())
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .display_name,
            "New name"
        );
    }

    #[tokio::test]
    async fn oidc_code_flow_verifies_pkce_nonce_signature_and_one_time_state() {
        let issuer = MockServer::start_async().await;
        let base = issuer.base_url();
        let discovery = issuer
            .mock_async(|when, then| {
                when.method(GET).path("/.well-known/openid-configuration");
                then.status(200).json_body(serde_json::json!({
                    "issuer": base,
                    "authorization_endpoint": format!("{base}/authorize"),
                    "token_endpoint": format!("{base}/token"),
                    "jwks_uri": format!("{base}/jwks"),
                    "response_types_supported": ["code"],
                    "subject_types_supported": ["public"],
                    "id_token_signing_alg_values_supported": ["HS256"]
                }));
            })
            .await;
        let jwks = issuer
            .mock_async(|when, then| {
                when.method(GET).path("/jwks");
                then.status(200)
                    .json_body(serde_json::json!({ "keys": [] }));
            })
            .await;
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.oidc = Some(crate::config::OidcConfig {
            issuer: base.clone(),
            client_id: "donkey-test".into(),
            client_secret: SecretString::from("client-secret"),
            redirect_url: "http://127.0.0.1/callback".into(),
            display_name: "Test OIDC".into(),
        });
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let service = AuthService::new(Arc::new(config), db.clone())
            .await
            .unwrap();
        let url = url::Url::parse(
            &service
                .oidc_authorize_url("/image-tools?tab=copy".into())
                .await
                .unwrap(),
        )
        .unwrap();
        let query = url.query_pairs().collect::<HashMap<_, _>>();
        assert_eq!(
            query.get("response_type").map(|value| value.as_ref()),
            Some("code")
        );
        assert_eq!(
            query
                .get("code_challenge_method")
                .map(|value| value.as_ref()),
            Some("S256")
        );
        assert!(query.contains_key("nonce"));
        assert!(query.contains_key("state"));
        let states = oidc_login_state::Entity::find().all(&db).await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].return_to, "/image-tools?tab=copy");
        assert_ne!(states[0].state_hash, query["state"]);
        let now = Utc::now().timestamp();
        let id_token = hs256_id_token(
            "client-secret",
            &serde_json::json!({
                "iss": base,
                "sub": "oidc-subject",
                "aud": "donkey-test",
                "exp": now + 300,
                "iat": now,
                "nonce": query["nonce"],
                "email": "operator@example.com",
                "email_verified": true
            }),
        );
        let token = issuer
            .mock_async(|when, then| {
                when.method(POST).path("/token");
                then.status(200).json_body(serde_json::json!({
                    "access_token": "access-token",
                    "token_type": "Bearer",
                    "expires_in": 300,
                    "id_token": id_token
                }));
            })
            .await;
        let (principal, session, return_to) = service
            .finish_oidc("authorization-code".into(), query["state"].to_string())
            .await
            .unwrap();
        assert!(principal.is_admin());
        assert_eq!(principal.username, "operator@example.com");
        assert_eq!(return_to, "/image-tools?tab=copy");
        assert!(!session.is_empty());
        assert!(matches!(
            service
                .finish_oidc("authorization-code".into(), query["state"].to_string())
                .await,
            Err(AppError::Unauthorized)
        ));
        discovery.assert_async().await;
        jwks.assert_async().await;
        token.assert_async().await;
    }

    #[tokio::test]
    async fn oidc_login_refreshes_rotated_signing_keys_without_restart() {
        const SIGNING_KEY: &str = "-----BEGIN PRIVATE KEY-----\n\
MC4CAQAwBQYDK2VwBCIEICWeYPLxoZKHZlQ6rkBi11E9JwchynXtljATLqym/XS9\n\
-----END PRIVATE KEY-----";

        let issuer = MockServer::start_async().await;
        let base = issuer.base_url();
        let discovery = issuer
            .mock_async(|when, then| {
                when.method(GET).path("/.well-known/openid-configuration");
                then.status(200).json_body(serde_json::json!({
                    "issuer": base,
                    "authorization_endpoint": format!("{base}/authorize"),
                    "token_endpoint": format!("{base}/token"),
                    "jwks_uri": format!("{base}/jwks"),
                    "response_types_supported": ["code"],
                    "subject_types_supported": ["public"],
                    "id_token_signing_alg_values_supported": ["EdDSA"]
                }));
            })
            .await;
        let stale_jwks = issuer
            .mock_async(|when, then| {
                when.method(GET).path("/jwks");
                then.status(200)
                    .json_body(serde_json::json!({ "keys": [] }));
            })
            .await;
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.oidc = Some(crate::config::OidcConfig {
            issuer: base.clone(),
            client_id: "donkey-test".into(),
            client_secret: SecretString::from("client-secret"),
            redirect_url: "http://127.0.0.1/callback".into(),
            display_name: "Test OIDC".into(),
        });
        let db = crate::db::connect(&config.database_url).await.unwrap();
        let service = AuthService::new(Arc::new(config), db.clone())
            .await
            .unwrap();
        stale_jwks.assert_calls_async(1).await;
        stale_jwks.delete_async().await;

        let signing_key = CoreEdDsaPrivateSigningKey::from_ed25519_pem(
            SIGNING_KEY,
            Some(JsonWebKeyId::new("rotated-key".into())),
        )
        .unwrap();
        let fresh_jwks = issuer
            .mock_async(|when, then| {
                when.method(GET).path("/jwks");
                then.status(200).json_body(serde_json::json!({
                    "keys": [signing_key.as_verification_key()]
                }));
            })
            .await;
        let url = url::Url::parse(&service.oidc_authorize_url("/".into()).await.unwrap()).unwrap();
        let query = url.query_pairs().collect::<HashMap<_, _>>();
        let state = query["state"].to_string();
        let stored = oidc_login_state::Entity::find_by_id(token_hash(&state))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let now = Utc::now();
        let claims = CoreIdTokenClaims::new(
            IssuerUrl::new(base.clone()).unwrap(),
            vec![Audience::new("donkey-test".into())],
            now + chrono::Duration::minutes(5),
            now,
            StandardClaims::new(SubjectIdentifier::new("rotated-user".into()))
                .set_email(Some(EndUserEmail::new("rotated@example.com".into())))
                .set_email_verified(Some(true)),
            Default::default(),
        )
        .set_nonce(Some(Nonce::new(stored.nonce)));
        let id_token = CoreIdToken::new(
            claims,
            &signing_key,
            CoreJwsSigningAlgorithm::EdDsa,
            None,
            None,
        )
        .unwrap();
        let token = issuer
            .mock_async(|when, then| {
                when.method(POST).path("/token");
                then.status(200).json_body(serde_json::json!({
                    "access_token": "access-token",
                    "token_type": "Bearer",
                    "expires_in": 300,
                    "id_token": serde_json::to_value(id_token).unwrap()
                }));
            })
            .await;

        let (principal, session, return_to) = service
            .finish_oidc("authorization-code".into(), state)
            .await
            .unwrap();

        assert_eq!(principal.username, "rotated@example.com");
        assert!(!session.is_empty());
        assert_eq!(return_to, "/");
        discovery.assert_calls_async(2).await;
        fresh_jwks.assert_calls_async(1).await;
        token.assert_async().await;
    }

    #[test]
    fn only_relative_return_paths_are_accepted() {
        assert_eq!(
            safe_return_to("/image-tools?tab=copy"),
            "/image-tools?tab=copy"
        );
        assert_eq!(safe_return_to("https://evil.example"), "/");
        assert_eq!(safe_return_to("//evil.example"), "/");
    }

    fn hs256_id_token(secret: &str, claims: &serde_json::Value) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
        let signing_input = format!("{header}.{payload}");
        let signature =
            URL_SAFE_NO_PAD.encode(hmac_sha256(secret.as_bytes(), signing_input.as_bytes()));
        format!("{signing_input}.{signature}")
    }

    fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
        let mut block = [0_u8; 64];
        if key.len() > block.len() {
            block[..32].copy_from_slice(&Sha256::digest(key));
        } else {
            block[..key.len()].copy_from_slice(key);
        }
        let mut inner_pad = [0x36_u8; 64];
        let mut outer_pad = [0x5c_u8; 64];
        for index in 0..64 {
            inner_pad[index] ^= block[index];
            outer_pad[index] ^= block[index];
        }
        let mut inner = Sha256::new();
        inner.update(inner_pad);
        inner.update(message);
        let inner = inner.finalize();
        let mut outer = Sha256::new();
        outer.update(outer_pad);
        outer.update(inner);
        outer.finalize().into()
    }
}
