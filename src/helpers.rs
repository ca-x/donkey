use std::net::IpAddr;

use axum::{
    body::Body,
    extract::Request,
    http::{Method, StatusCode, header, uri::Authority},
    response::{IntoResponse, Response},
};

use crate::{error::AppError, state::AppState};

const SHELL_HELPER: &str = include_str!("../scripts/helper.sh");
const POWERSHELL_HELPER: &str = include_str!("../scripts/helper.ps1");

pub fn is_helper_path(path: &str) -> bool {
    matches!(path, "/helper" | "/helper.win")
}

pub async fn serve(state: &AppState, request: Request) -> Result<Response, AppError> {
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        return Ok(StatusCode::METHOD_NOT_ALLOWED.into_response());
    }

    let path = request.uri().path();
    let origin = public_origin(state, &request)?;
    let (content_type, filename, script) = match path {
        "/helper" => (
            "text/x-shellscript; charset=utf-8",
            "donkey-helper.sh",
            render_shell(&origin)?,
        ),
        "/helper.win" => (
            "text/plain; charset=utf-8",
            "donkey-helper.ps1",
            render_powershell(&origin)?,
        ),
        _ => return Err(AppError::not_found("helper")),
    };

    let body = if request.method() == Method::HEAD {
        Body::empty()
    } else {
        Body::from(script)
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "no-store, max-age=0")
        .header(
            "content-disposition",
            format!("inline; filename=\"{filename}\""),
        )
        .header("x-content-type-options", "nosniff")
        .body(body)
        .map_err(AppError::internal)
}

fn public_origin(state: &AppState, request: &Request) -> Result<String, AppError> {
    let raw = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| AppError::bad_request("Host header is required"))?;
    if raw.len() > 255
        || !raw.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'[' | b']' | b':')
        })
    {
        return Err(AppError::bad_request("invalid Host header"));
    }
    let authority: Authority = raw
        .parse()
        .map_err(|_| AppError::bad_request("invalid Host header"))?;
    let host = authority.host().trim_matches(['[', ']']);
    if host.parse::<IpAddr>().is_err()
        && (host.is_empty()
            || host.split('.').any(|label| {
                label.is_empty()
                    || label.starts_with('-')
                    || label.ends_with('-')
                    || !label
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            }))
    {
        return Err(AppError::bad_request("invalid Host header"));
    }
    let scheme = if state.config.tls_cert.is_some() || state.config.registry_external_tls {
        "https"
    } else {
        "http"
    };
    Ok(format!("{scheme}://{authority}"))
}

fn render_shell(origin: &str) -> Result<String, AppError> {
    replace_once(
        SHELL_HELPER,
        "DONKEY_URL=\"${DONKEY_URL:-}\"",
        &format!("DONKEY_URL=\"${{DONKEY_URL:-{origin}}}\""),
    )
}

fn render_powershell(origin: &str) -> Result<String, AppError> {
    replace_once(
        POWERSHELL_HELPER,
        "[string]$Url = $env:DONKEY_URL,",
        &format!("[string]$Url = '{origin}',"),
    )
}

fn replace_once(source: &str, pattern: &str, replacement: &str) -> Result<String, AppError> {
    if source.matches(pattern).count() != 1 {
        return Err(AppError::internal(anyhow::anyhow!(
            "helper template marker is invalid"
        )));
    }
    Ok(source.replacen(pattern, replacement, 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_helpers_use_the_request_origin_as_the_default() {
        let origin = "https://registry.example:5443";
        let shell = render_shell(origin).unwrap();
        let powershell = render_powershell(origin).unwrap();
        assert!(shell.contains("DONKEY_URL=\"${DONKEY_URL:-https://registry.example:5443}\""));
        assert!(powershell.contains("[string]$Url = 'https://registry.example:5443',"));
        assert!(!shell.contains("__DONKEY_URL__"));
        assert!(!powershell.contains("__DONKEY_URL__"));
    }
}
