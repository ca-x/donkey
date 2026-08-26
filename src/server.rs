use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderName, HeaderValue, StatusCode, header},
    middleware::{Next, from_fn_with_state},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use tower_http::{
    compression::CompressionLayer, limit::RequestBodyLimitLayer,
    set_header::SetResponseHeaderLayer, trace::TraceLayer,
};

use crate::{Config, api, connect, registry, state::AppState};

pub async fn run(config: Config) -> anyhow::Result<()> {
    let state = AppState::new(config).await?;
    if state.config.admin_addr.ip().is_unspecified()
        && !state.config.admin_external_tls
        && !state.config.admin_external_loopback
    {
        tracing::warn!(
            "admin listener is not loopback and external TLS is not declared; expose port 5003 only on a trusted loopback binding"
        );
    }
    state.image_tools.clone().spawn();
    let admin = admin_router(state.clone());
    let registry = registry_router(state.clone());

    let health_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(health_state.config.health_interval);
        loop {
            interval.tick().await;
            health_state.nodes.probe_all().await;
            if let Err(error) = health_state.cache.cleanup_expired().await {
                tracing::warn!(?error, "cache TTL cleanup failed");
            }
            if let Err(error) = health_state.auth.cleanup_expired().await {
                tracing::warn!(?error, "expired admin session cleanup failed");
            }
        }
    });

    let admin_listener = tokio::net::TcpListener::bind(state.config.admin_addr).await?;
    tracing::info!(address = %state.config.admin_addr, "admin and CONNECT listener ready");
    let admin_server = axum::serve(admin_listener, admin.into_make_service());

    let registry_addr = state.config.registry_addr;
    let registry_state = state.clone();
    let registry_server = async move {
        if let (Some(cert), Some(key)) = (
            &registry_state.config.tls_cert,
            &registry_state.config.tls_key,
        ) {
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
            tracing::info!(address = %registry_addr, "TLS Registry listener ready");
            axum_server::bind_rustls(registry_addr, tls)
                .serve(registry.into_make_service())
                .await?;
        } else {
            tracing::warn!(address = %registry_addr, "Registry listener is HTTP because no TLS certificate is configured");
            let listener = tokio::net::TcpListener::bind(registry_addr).await?;
            axum::serve(listener, registry.into_make_service()).await?;
        }
        Ok::<_, anyhow::Error>(())
    };

    tokio::select! {
        result = admin_server => result?,
        result = registry_server => result?,
        _ = shutdown_signal() => tracing::info!("shutdown signal received"),
    }
    Ok(())
}

pub fn admin_router(state: AppState) -> Router {
    let core = Router::new()
        .nest("/api", api::router())
        .fallback(connect::handle)
        .with_state(state.clone());
    Router::new()
        .nest("/api/auth", crate::auth::router(state.auth.clone()))
        .nest(
            "/api/image-tools",
            crate::image_tools::router(state.image_tools.clone()),
        )
        .merge(core)
        .layer(from_fn_with_state(state.clone(), admin_auth))
        .layer(RequestBodyLimitLayer::new(64 * 1024))
        .layer(CompressionLayer::new())
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("content-security-policy"),
            HeaderValue::from_static(
                "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
            ),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            header::X_FRAME_OPTIONS,
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(TraceLayer::new_for_http())
}

pub fn registry_router(state: AppState) -> Router {
    Router::new()
        .fallback(registry::handle)
        .layer(from_fn_with_state(state.clone(), registry_auth))
        .layer(RequestBodyLimitLayer::new(1024))
        .layer(SetResponseHeaderLayer::overriding(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn admin_auth(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    let path = request.uri().path();
    let public_api = path == "/api/health"
        || path == "/api/auth/config"
        || path == "/api/auth/login"
        || path == "/api/auth/oidc/start"
        || path == "/api/auth/oidc/callback";
    if request.method() == http::Method::CONNECT || !path.starts_with("/api/") || public_api {
        return next.run(request).await;
    }
    let principal = match state.auth.authenticate(request.headers()).await {
        Ok(principal) => Some(principal),
        Err(_) => state.auth.authenticate_legacy_basic(request.headers()),
    };
    let Some(principal) = principal else {
        return crate::error::AppError::Unauthorized.into_response();
    };
    let is_write = !matches!(
        *request.method(),
        http::Method::GET | http::Method::HEAD | http::Method::OPTIONS
    );
    if is_write && path != "/api/auth/logout" && !principal.is_admin() {
        return crate::error::AppError::Forbidden.into_response();
    }
    request.extensions_mut().insert(principal);
    next.run(request).await
}

async fn registry_auth(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    if crate::helpers::is_helper_path(request.uri().path()) {
        return next.run(request).await;
    }
    let Some(expected) = state.config.registry_auth_value() else {
        return next.run(request).await;
    };
    let valid = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Basic "))
        .and_then(|value| STANDARD.decode(value).ok())
        .is_some_and(|decoded| {
            decoded.len() == expected.len()
                && constant_time_eq::constant_time_eq(&decoded, expected.as_bytes())
        });
    if !valid {
        let mut response = StatusCode::UNAUTHORIZED.into_response();
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Basic realm=\"Donkey Registry\", charset=\"UTF-8\""),
        );
        return response;
    }
    request.headers_mut().remove(header::AUTHORIZATION);
    next.run(request).await
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http::Request;
    use secrecy::SecretString;
    use tower::ServiceExt;

    #[tokio::test]
    async fn registry_without_configured_auth_does_not_challenge_clients() {
        let directory = tempfile::tempdir().unwrap();
        let router = registry_router(
            AppState::new(Config::for_test(directory.path().to_owned()))
                .await
                .unwrap(),
        );

        let response = router
            .oneshot(Request::builder().uri("/v2/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::WWW_AUTHENTICATE).is_none());
    }

    #[tokio::test]
    async fn generated_helpers_are_public_even_when_registry_auth_is_enabled() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.registry_auth = Some(SecretString::from("client:password"));
        config.registry_external_tls = true;
        let router = registry_router(AppState::new(config).await.unwrap());

        for (path, marker) in [
            (
                "/helper",
                "DONKEY_URL=\"${DONKEY_URL:-https://registry.example}\"",
            ),
            ("/helper.win", "[string]$Url = 'https://registry.example',"),
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .header(header::HOST, "registry.example")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert!(response.headers().get(header::WWW_AUTHENTICATE).is_none());
            assert_eq!(
                response.headers().get(header::CACHE_CONTROL).unwrap(),
                "no-store, max-age=0"
            );
            let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .unwrap();
            assert!(String::from_utf8(body.to_vec()).unwrap().contains(marker));
        }

        let rejected = router
            .oneshot(
                Request::builder()
                    .uri("/helper")
                    .header(header::HOST, "registry.example;malicious-command")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn registry_auth_uses_docker_compatible_basic_challenge() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.registry_auth = Some(SecretString::from("docker-user:docker-password"));
        config.registry_external_tls = true;
        let router = registry_router(AppState::new(config).await.unwrap());

        let unauthenticated = router
            .clone()
            .oneshot(Request::builder().uri("/v2/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            unauthenticated
                .headers()
                .get(header::WWW_AUTHENTICATE)
                .unwrap(),
            "Basic realm=\"Donkey Registry\", charset=\"UTF-8\""
        );

        let wrong = STANDARD.encode("docker-user:wrong-password");
        let rejected = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v2/")
                    .header(header::AUTHORIZATION, format!("Basic {wrong}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            rejected.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Basic realm=\"Donkey Registry\", charset=\"UTF-8\""
        );

        let encoded = STANDARD.encode("docker-user:docker-password");
        let authenticated = router
            .oneshot(
                Request::builder()
                    .uri("/v2/")
                    .header(header::AUTHORIZATION, format!("Basic {encoded}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authenticated.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn connect_without_configured_auth_is_disabled_without_a_challenge() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.connect_remap.push(crate::config::ConnectRemap {
            source: "registry.example:443".into(),
            target: "127.0.0.1:9".into(),
        });
        let router = admin_router(AppState::new(config).await.unwrap());

        let response = router
            .oneshot(
                Request::builder()
                    .method(http::Method::CONNECT)
                    .uri("registry.example:443")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(response.headers().get(header::PROXY_AUTHENTICATE).is_none());
    }

    #[tokio::test]
    async fn connect_uses_proxy_auth_without_duplicate_admin_auth() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.admin_auth = Some(SecretString::from("admin:password"));
        config.proxy_auth = Some(SecretString::from("proxy:password"));
        config.connect_remap.push(crate::config::ConnectRemap {
            source: "registry.example:443".into(),
            target: "127.0.0.1:9".into(),
        });
        let router = admin_router(AppState::new(config).await.unwrap());

        let unauthenticated = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::CONNECT)
                    .uri("registry.example:443")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            unauthenticated.status(),
            StatusCode::PROXY_AUTHENTICATION_REQUIRED
        );
        assert_eq!(
            unauthenticated
                .headers()
                .get(header::PROXY_AUTHENTICATE)
                .unwrap(),
            "Basic realm=\"Donkey CONNECT\""
        );

        let encoded = STANDARD.encode("proxy:password");
        let response = router
            .oneshot(
                Request::builder()
                    .method(http::Method::CONNECT)
                    .uri("registry.example:443")
                    .header(header::PROXY_AUTHORIZATION, format!("Basic {encoded}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn local_admin_uses_cookie_session_and_logout_revokes_it() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.initial_admin_username = Some("owner".into());
        config.initial_admin_password = Some(SecretString::from("a-secure-password"));
        let router = admin_router(AppState::new(config).await.unwrap());

        let unauthenticated = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/runtime")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
        assert!(
            unauthenticated
                .headers()
                .get(header::WWW_AUTHENTICATE)
                .is_none(),
            "admin session endpoints must not trigger a browser Basic auth dialog"
        );

        let logged_in = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/auth/login")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"username":"owner","password":"a-secure-password"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(logged_in.status(), StatusCode::OK);
        let cookie = logged_in
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("SameSite=Lax"));
        let session_cookie = cookie.split(';').next().unwrap();

        let authenticated = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/runtime")
                    .header(header::COOKIE, session_cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authenticated.status(), StatusCode::OK);

        let logged_out = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/auth/logout")
                    .header(header::COOKIE, session_cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(logged_out.status(), StatusCode::NO_CONTENT);

        let revoked = router
            .oneshot(
                Request::builder()
                    .uri("/api/runtime")
                    .header(header::COOKIE, session_cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn legacy_admin_basic_is_opt_in_and_never_challenges_browsers() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.admin_auth = Some(SecretString::from("legacy:password"));
        let router = admin_router(AppState::new(config).await.unwrap());

        for authorization in [None, Some(STANDARD.encode("legacy:wrong"))] {
            let mut request = Request::builder().uri("/api/runtime");
            if let Some(encoded) = authorization {
                request = request.header(header::AUTHORIZATION, format!("Basic {encoded}"));
            }
            let response = router
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert!(response.headers().get(header::WWW_AUTHENTICATE).is_none());
        }

        let encoded = STANDARD.encode("legacy:password");
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/runtime")
                    .header(header::AUTHORIZATION, format!("Basic {encoded}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn member_session_can_read_but_cannot_mutate_control_plane() {
        use sea_orm::{ActiveModelTrait, IntoActiveModel};
        use sha2::{Digest, Sha256};

        let directory = tempfile::tempdir().unwrap();
        let state = AppState::new(Config::for_test(directory.path().to_owned()))
            .await
            .unwrap();
        let now = chrono::Utc::now();
        let member_id = uuid::Uuid::new_v4();
        crate::db::user::Model {
            id: member_id,
            identity_key: "oidc:test:member".into(),
            username: None,
            issuer: Some("https://issuer.example".into()),
            subject: "member".into(),
            display_name: "Member".into(),
            email: None,
            password_hash: None,
            role: "member".into(),
            enabled: true,
            created_at: now,
            updated_at: now,
            last_login_at: Some(now),
        }
        .into_active_model()
        .insert(&state.db)
        .await
        .unwrap();
        let token = "member-session-token";
        crate::db::admin_session::Model {
            token_hash: hex::encode(Sha256::digest(token.as_bytes())),
            user_id: member_id,
            created_at: now,
            last_seen_at: now,
            expires_at: now + chrono::Duration::hours(1),
        }
        .into_active_model()
        .insert(&state.db)
        .await
        .unwrap();
        let router = admin_router(state);
        let cookie = format!("donkey_session={token}");

        let read = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/runtime")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(read.status(), StatusCode::OK);

        let route_read = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/registry-routes")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(route_read.status(), StatusCode::OK);

        let route_write = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/registry-routes")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(route_write.status(), StatusCode::FORBIDDEN);

        let write = router
            .oneshot(
                Request::builder()
                    .method(http::Method::DELETE)
                    .uri("/api/cache/not-present")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(write.status(), StatusCode::FORBIDDEN);
    }
}
