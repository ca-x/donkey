use axum::{
    body::Body,
    extract::Request,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "frontend/dist/"]
struct Assets;

pub async fn serve(request: Request) -> Response {
    if !matches!(*request.method(), http::Method::GET | http::Method::HEAD) {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let requested = request.uri().path().trim_start_matches('/');
    let path = if requested.is_empty() {
        "index.html"
    } else {
        requested
    };
    let selected = Assets::get(path)
        .map(|asset| (asset, path == "index.html"))
        .or_else(|| {
            if path.contains('.') {
                None
            } else {
                Assets::get("index.html").map(|asset| (asset, true))
            }
        });
    let Some((asset, served_index)) = selected else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut response = Response::new(if request.method() == http::Method::HEAD {
        Body::empty()
    } else {
        Body::from(asset.data)
    });
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static(mime_for(path)),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static(if served_index {
            "no-store, no-cache, max-age=0, must-revalidate"
        } else {
            "public, max-age=31536000, immutable"
        }),
    );
    response
}

fn mime_for(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("webp") => "image/webp",
        Some("json") => "application/json",
        Some("woff2") => "font/woff2",
        _ => "text/html; charset=utf-8",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spa_fallback_is_never_immutable() {
        let response = serve(
            Request::builder()
                .uri("/nodes")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store, no-cache, max-age=0, must-revalidate"
        );
    }
}
