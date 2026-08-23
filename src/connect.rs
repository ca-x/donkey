use std::{net::IpAddr, sync::Arc};

use axum::{
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use tokio::{
    io::copy_bidirectional,
    net::{TcpStream, lookup_host},
};

use crate::{config::Config, error::AppError, security, state::AppState};

pub async fn handle(State(state): State<AppState>, request: Request) -> Response {
    if request.method() != Method::CONNECT {
        return crate::static_files::serve(request).await;
    }
    match connect(state.config.clone(), request).await {
        Ok(response) => response,
        Err(AppError::Unauthorized) => {
            let mut response = StatusCode::PROXY_AUTHENTICATION_REQUIRED.into_response();
            response.headers_mut().insert(
                header::PROXY_AUTHENTICATE,
                http::HeaderValue::from_static("Basic realm=\"Donkey CONNECT\""),
            );
            response
        }
        Err(error) => error.into_response(),
    }
}

async fn connect(config: Arc<Config>, request: Request) -> Result<Response, AppError> {
    authorize_proxy(request.headers(), config.proxy_auth_value())?;
    let authority = request
        .uri()
        .authority()
        .map(|value| value.as_str().to_ascii_lowercase())
        .ok_or_else(|| AppError::bad_request("CONNECT request has no authority"))?;
    if authority.len() > 300 {
        return Err(AppError::bad_request("CONNECT target is too long"));
    }

    let target = if let Some(remap) = config
        .connect_remap
        .iter()
        .find(|item| item.source == authority)
    {
        remap.target.clone()
    } else {
        validate_connect_target(&authority, &config)
            .await?
            .to_string()
    };
    let upgrade = hyper::upgrade::on(request);
    tokio::spawn(async move {
        match upgrade.await {
            Ok(upgraded) => match TcpStream::connect(&target).await {
                Ok(mut upstream) => {
                    let mut upgraded = hyper_util::rt::TokioIo::new(upgraded);
                    if let Err(error) = copy_bidirectional(&mut upgraded, &mut upstream).await {
                        tracing::debug!(?error, %target, "CONNECT tunnel closed with error");
                    }
                }
                Err(error) => tracing::warn!(?error, %target, "CONNECT upstream failed"),
            },
            Err(error) => tracing::debug!(?error, "CONNECT upgrade failed"),
        }
    });
    Ok(StatusCode::OK.into_response())
}

fn authorize_proxy(headers: &HeaderMap, expected: Option<&str>) -> Result<(), AppError> {
    let Some(expected) = expected else {
        return Err(AppError::Forbidden);
    };
    let Some(encoded) = headers
        .get(header::PROXY_AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Basic "))
    else {
        return Err(AppError::Unauthorized);
    };
    let decoded = STANDARD
        .decode(encoded)
        .map_err(|_| AppError::Unauthorized)?;
    if decoded.len() != expected.len()
        || !constant_time_eq::constant_time_eq(&decoded, expected.as_bytes())
    {
        return Err(AppError::Unauthorized);
    }
    Ok(())
}

async fn validate_connect_target(
    authority: &str,
    config: &Config,
) -> Result<std::net::SocketAddr, AppError> {
    let (host, port) = split_authority(authority)?;
    if !config
        .connect_allow
        .iter()
        .any(|allowed| host == *allowed || host.ends_with(&format!(".{allowed}")))
    {
        return Err(AppError::Forbidden);
    }
    let addresses = lookup_host((host.as_str(), port))
        .await
        .map_err(|_| AppError::bad_request("CONNECT DNS lookup failed"))?;
    let mut selected = None;
    for address in addresses {
        if !config.allow_private_upstreams && security::is_non_public(address.ip()) {
            return Err(AppError::Forbidden);
        }
        selected.get_or_insert(address);
    }
    selected.ok_or_else(|| AppError::bad_request("CONNECT DNS returned no addresses"))
}

fn split_authority(authority: &str) -> Result<(String, u16), AppError> {
    let parsed = authority
        .parse::<http::uri::Authority>()
        .map_err(|_| AppError::bad_request("invalid CONNECT authority"))?;
    let host = parsed.host().trim_matches(['[', ']']).to_ascii_lowercase();
    let _ = host.parse::<IpAddr>();
    let port = parsed.port_u16().unwrap_or(443);
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ipv6_authority() {
        assert_eq!(
            split_authority("[2606:4700:4700::1111]:443").unwrap().1,
            443
        );
    }
}
