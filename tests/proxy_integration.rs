mod proxy_integration {
    use std::{
        collections::BTreeSet,
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{
        Router,
        body::{Body, to_bytes},
        extract::{Request as AxumRequest, State},
        http::{HeaderMap, Method, StatusCode, header},
        response::Response,
    };
    use donkey::{
        AppState, Config,
        nodes::NodeInput,
        registry_routes::{DOCKER_HUB_ROUTE_ID, GHCR_ROUTE_ID},
        server::registry_router,
    };
    use secrecy::SecretString;
    use sha2::{Digest, Sha256};
    use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
    use tower::ServiceExt;

    const REPOSITORY: &str = "team/widget";

    #[derive(Clone, Copy)]
    enum FixtureBehavior {
        ValidRange,
        RangeUnsupported,
        RetryableFailure,
        WrongContentRange,
        CorruptChunk,
        DropAfterPrefix,
    }

    #[derive(Default)]
    struct FixtureStats {
        head_count: AtomicUsize,
        get_count: AtomicUsize,
        ranges: Mutex<Vec<Option<String>>>,
        uris: Mutex<Vec<String>>,
        route_auth: Mutex<Vec<Option<String>>>,
    }

    #[derive(Clone)]
    struct FixtureState {
        behavior: FixtureBehavior,
        bytes: Arc<Vec<u8>>,
        stats: Arc<FixtureStats>,
    }

    struct Fixture {
        address: SocketAddr,
        state: FixtureState,
        server: JoinHandle<()>,
    }

    impl Fixture {
        async fn start(behavior: FixtureBehavior, bytes: Vec<u8>) -> Self {
            let state = FixtureState {
                behavior,
                bytes: Arc::new(bytes),
                stats: Arc::new(FixtureStats::default()),
            };
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let app = Router::new()
                .fallback(fixture_handler)
                .with_state(state.clone());
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self {
                address,
                state,
                server,
            }
        }

        fn url(&self) -> String {
            format!("http://{}/", self.address)
        }

        fn head_count(&self) -> usize {
            self.state.stats.head_count.load(Ordering::Relaxed)
        }

        fn get_count(&self) -> usize {
            self.state.stats.get_count.load(Ordering::Relaxed)
        }

        async fn ranges(&self) -> Vec<Option<String>> {
            self.state.stats.ranges.lock().await.clone()
        }

        async fn uris(&self) -> Vec<String> {
            self.state.stats.uris.lock().await.clone()
        }

        async fn route_auth(&self) -> Vec<Option<String>> {
            self.state.stats.route_auth.lock().await.clone()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    async fn fixture_handler(State(state): State<FixtureState>, request: AxumRequest) -> Response {
        state
            .stats
            .uris
            .lock()
            .await
            .push(request.uri().to_string());
        state.stats.route_auth.lock().await.push(
            request
                .headers()
                .get("x-route-key")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        );
        match *request.method() {
            Method::HEAD => {
                state.stats.head_count.fetch_add(1, Ordering::Relaxed);
                fixture_response(
                    StatusCode::OK,
                    &state.bytes,
                    matches!(
                        state.behavior,
                        FixtureBehavior::ValidRange
                            | FixtureBehavior::RetryableFailure
                            | FixtureBehavior::WrongContentRange
                            | FixtureBehavior::CorruptChunk
                            | FixtureBehavior::DropAfterPrefix
                    ),
                    None,
                )
            }
            Method::GET => {
                state.stats.get_count.fetch_add(1, Ordering::Relaxed);
                let requested_range = request
                    .headers()
                    .get(header::RANGE)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                state
                    .stats
                    .ranges
                    .lock()
                    .await
                    .push(requested_range.clone());
                match state.behavior {
                    FixtureBehavior::RetryableFailure => {
                        fixture_response(StatusCode::SERVICE_UNAVAILABLE, &[], false, None)
                    }
                    FixtureBehavior::RangeUnsupported => {
                        fixture_response(StatusCode::OK, &state.bytes, false, None)
                    }
                    FixtureBehavior::ValidRange => {
                        ranged_or_whole(&state.bytes, requested_range, false)
                    }
                    FixtureBehavior::WrongContentRange => {
                        ranged_or_whole(&state.bytes, requested_range, true)
                    }
                    FixtureBehavior::CorruptChunk => {
                        let mut corrupt = state.bytes.as_ref().clone();
                        if let Some(first) = corrupt.first_mut() {
                            *first ^= 0xff;
                        }
                        ranged_or_whole(&corrupt, requested_range, false)
                    }
                    FixtureBehavior::DropAfterPrefix => {
                        let (start, end) = requested_range
                            .as_deref()
                            .and_then(parse_range)
                            .unwrap_or((0, state.bytes.len().saturating_sub(1) as u64));
                        let end = end.min(state.bytes.len().saturating_sub(1) as u64);
                        let prefix_end = (start + 3).min(end);
                        let prefix = state.bytes[start as usize..=prefix_end as usize].to_vec();
                        let body = futures_util::stream::iter([
                            Ok::<_, std::io::Error>(bytes::Bytes::from(prefix)),
                            Err(std::io::Error::new(
                                std::io::ErrorKind::ConnectionReset,
                                "fixture drop",
                            )),
                        ]);
                        let mut response = Response::new(Body::from_stream(body));
                        response.headers_mut().insert(
                            header::CONTENT_LENGTH,
                            (end - start + 1).to_string().parse().unwrap(),
                        );
                        if requested_range.is_some() {
                            response.headers_mut().insert(
                                header::CONTENT_RANGE,
                                format!("bytes {start}-{end}/{}", state.bytes.len())
                                    .parse()
                                    .unwrap(),
                            );
                            response
                                .headers_mut()
                                .insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
                            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
                        }
                        response
                    }
                }
            }
            _ => fixture_response(StatusCode::METHOD_NOT_ALLOWED, &[], false, None),
        }
    }

    fn ranged_or_whole(
        bytes: &[u8],
        requested_range: Option<String>,
        wrong_content_range: bool,
    ) -> Response {
        let Some((start, end)) = requested_range.as_deref().and_then(parse_range) else {
            let body = if wrong_content_range {
                corrupt(bytes)
            } else {
                bytes.to_vec()
            };
            return fixture_response(StatusCode::OK, &body, true, None);
        };
        let end = end.min(bytes.len().saturating_sub(1) as u64);
        let body = bytes[start as usize..=end as usize].to_vec();
        let content_range = if wrong_content_range {
            format!(
                "bytes {}-{}/{}",
                start.saturating_add(1),
                end.saturating_add(1),
                bytes.len()
            )
        } else {
            format!("bytes {start}-{end}/{}", bytes.len())
        };
        fixture_response(
            StatusCode::PARTIAL_CONTENT,
            &body,
            true,
            Some(content_range),
        )
    }

    fn corrupt(bytes: &[u8]) -> Vec<u8> {
        let mut corrupt = bytes.to_vec();
        if let Some(first) = corrupt.first_mut() {
            *first ^= 0xff;
        }
        corrupt
    }

    fn parse_range(value: &str) -> Option<(u64, u64)> {
        let (start, end) = value.strip_prefix("bytes=")?.split_once('-')?;
        Some((start.parse().ok()?, end.parse().ok()?))
    }

    fn fixture_response(
        status: StatusCode,
        body: &[u8],
        accepts_ranges: bool,
        content_range: Option<String>,
    ) -> Response {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/octet-stream".parse().unwrap(),
        );
        headers.insert(
            header::CONTENT_LENGTH,
            body.len().to_string().parse().unwrap(),
        );
        if accepts_ranges {
            headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
        }
        if let Some(content_range) = content_range {
            headers.insert(header::CONTENT_RANGE, content_range.parse().unwrap());
        }
        let mut response = Response::builder()
            .status(status)
            .body(Body::from(body.to_vec()))
            .unwrap();
        *response.headers_mut() = headers;
        response
    }

    async fn proxy_state(fixtures: &[&Fixture]) -> (AppState, tempfile::TempDir) {
        let fixtures = fixtures
            .iter()
            .map(|fixture| (*fixture, DOCKER_HUB_ROUTE_ID))
            .collect::<Vec<_>>();
        routed_proxy_state(&fixtures).await
    }

    async fn routed_proxy_state(
        fixtures: &[(&Fixture, uuid::Uuid)],
    ) -> (AppState, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.chunk_size = 4;
        config.chunk_concurrency = 4;
        config.parallel_threshold = 1;
        let state = AppState::new(config).await.unwrap();
        for (index, (fixture, route_id)) in fixtures.iter().enumerate() {
            state
                .nodes
                .create(NodeInput {
                    name: format!("fixture-{index}"),
                    url: fixture.url(),
                    registry_route_id: *route_id,
                    enabled: true,
                    priority: index as i32,
                    cf_preferred: false,
                    connect_ip: None,
                    auth_mode: "none".into(),
                    auth_username: None,
                    auth_header: None,
                    auth_secret: None,
                })
                .await
                .unwrap();
        }
        (state, directory)
    }

    async fn authenticated_routed_proxy_state(
        fixtures: &[(&Fixture, uuid::Uuid, &str)],
    ) -> (AppState, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.registry_auth = Some(SecretString::from("client:password"));
        config.credential_key = Some(SecretString::from("88".repeat(32)));
        let state = AppState::new(config).await.unwrap();
        for (index, (fixture, route_id, secret)) in fixtures.iter().enumerate() {
            state
                .nodes
                .create(NodeInput {
                    name: format!("authenticated-fixture-{index}"),
                    url: fixture.url(),
                    registry_route_id: *route_id,
                    enabled: true,
                    priority: index as i32,
                    cf_preferred: false,
                    connect_ip: None,
                    auth_mode: "header".into(),
                    auth_username: None,
                    auth_header: Some("x-route-key".into()),
                    auth_secret: Some((*secret).to_owned()),
                })
                .await
                .unwrap();
        }
        (state, directory)
    }

    fn digest(bytes: &[u8]) -> String {
        format!("sha256:{:x}", Sha256::digest(bytes))
    }

    fn blob_path(digest: &str) -> String {
        format!("/v2/{REPOSITORY}/blobs/{digest}")
    }

    async fn request(router: Router, method: Method, uri: &str, range: Option<&str>) -> Response {
        let mut headers = HeaderMap::new();
        if let Some(range) = range {
            headers.insert(header::RANGE, range.parse().unwrap());
        }
        request_with_headers(router, method, uri, headers).await
    }

    async fn request_with_headers(
        router: Router,
        method: Method,
        uri: &str,
        headers: HeaderMap,
    ) -> Response {
        let mut request = http::Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap();
        *request.headers_mut() = headers;
        router.oneshot(request).await.unwrap()
    }

    mod fixture {
        use super::*;

        #[tokio::test]
        async fn serves_a_loopback_range_source_and_records_requests() {
            let bytes = b"fixture-bytes".to_vec();
            let fixture = Fixture::start(FixtureBehavior::ValidRange, bytes.clone()).await;
            let head = reqwest::Client::new()
                .head(format!(
                    "{}v2/{REPOSITORY}/blobs/{}",
                    fixture.url(),
                    digest(&bytes)
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(head.status(), StatusCode::OK);
            assert_eq!(head.headers()[header::CONTENT_LENGTH], "13");
            let response = reqwest::Client::new()
                .get(format!(
                    "{}v2/{REPOSITORY}/blobs/{}",
                    fixture.url(),
                    digest(&bytes)
                ))
                .header(header::RANGE, "bytes=2-5")
                .send()
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 2-5/13");
            assert_eq!(response.bytes().await.unwrap().as_ref(), b"xtur");
            assert_eq!(fixture.head_count(), 1);
            assert_eq!(fixture.get_count(), 1);
            assert_eq!(fixture.ranges().await, vec![Some("bytes=2-5".into())]);
        }
    }

    mod range {
        use super::*;

        #[tokio::test]
        async fn reconstructs_exact_bytes_from_multiple_ranges() {
            let bytes = b"0123456789abcdef".to_vec();
            let left = Fixture::start(FixtureBehavior::ValidRange, bytes.clone()).await;
            let right = Fixture::start(FixtureBehavior::ValidRange, bytes.clone()).await;
            let (state, _directory) = proxy_state(&[&left, &right]).await;
            let digest = digest(&bytes);

            let response = request(
                registry_router(state.clone()),
                Method::GET,
                &blob_path(&digest),
                None,
            )
            .await;

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .as_ref(),
                bytes
            );
            assert_eq!(state.cache.stats().await.unwrap().entries, 1);
            let ranges = left
                .ranges()
                .await
                .into_iter()
                .chain(right.ranges().await)
                .flatten()
                .collect::<BTreeSet<_>>();
            assert_eq!(
                ranges,
                BTreeSet::from([
                    "bytes=0-3".into(),
                    "bytes=4-7".into(),
                    "bytes=8-11".into(),
                    "bytes=12-15".into(),
                ])
            );
        }

        #[tokio::test]
        async fn replaces_a_retryable_source_with_a_healthy_source() {
            let bytes = b"0123456789abcdef".to_vec();
            let failing = Fixture::start(FixtureBehavior::RetryableFailure, bytes.clone()).await;
            let healthy = Fixture::start(FixtureBehavior::ValidRange, bytes.clone()).await;
            let (state, _directory) = proxy_state(&[&failing, &healthy]).await;
            let digest = digest(&bytes);

            let response = request(
                registry_router(state),
                Method::GET,
                &blob_path(&digest),
                None,
            )
            .await;

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .as_ref(),
                bytes
            );
            assert!(failing.get_count() >= 1);
            assert!(healthy.get_count() >= 4);
        }

        #[tokio::test]
        async fn rejects_wrong_content_range_without_cache_admission() {
            let bytes = b"0123456789abcdef".to_vec();
            let left = Fixture::start(FixtureBehavior::WrongContentRange, bytes.clone()).await;
            let right = Fixture::start(FixtureBehavior::WrongContentRange, bytes.clone()).await;
            let (state, _directory) = proxy_state(&[&left, &right]).await;

            let response = request(
                registry_router(state.clone()),
                Method::GET,
                &blob_path(&digest(&bytes)),
                None,
            )
            .await;

            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            assert_eq!(state.cache.stats().await.unwrap().entries, 0);
        }

        #[tokio::test]
        async fn rejects_corrupt_chunks_without_cache_admission() {
            let bytes = b"0123456789abcdef".to_vec();
            let left = Fixture::start(FixtureBehavior::CorruptChunk, bytes.clone()).await;
            let right = Fixture::start(FixtureBehavior::CorruptChunk, bytes.clone()).await;
            let (state, _directory) = proxy_state(&[&left, &right]).await;

            let response = request(
                registry_router(state.clone()),
                Method::GET,
                &blob_path(&digest(&bytes)),
                None,
            )
            .await;

            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            assert_eq!(state.cache.stats().await.unwrap().entries, 0);
        }
    }

    mod cache {
        use super::*;

        #[tokio::test]
        async fn falls_back_to_full_fetch_then_reuses_the_completed_cache_object() {
            let bytes = b"0123456789abcdef".to_vec();
            let fixture = Fixture::start(FixtureBehavior::RangeUnsupported, bytes.clone()).await;
            let (state, _directory) = proxy_state(&[&fixture]).await;
            let digest = digest(&bytes);
            let path = blob_path(&digest);
            let router = registry_router(state);

            let first = request(router.clone(), Method::GET, &path, None).await;
            assert_eq!(first.status(), StatusCode::OK);
            assert_eq!(
                to_bytes(first.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .as_ref(),
                bytes
            );
            assert_eq!(fixture.head_count(), 1);
            assert_eq!(fixture.get_count(), 1);
            assert_eq!(fixture.ranges().await, vec![None]);

            let second = request(router.clone(), Method::GET, &path, None).await;
            assert_eq!(second.status(), StatusCode::OK);
            assert_eq!(
                to_bytes(second.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .as_ref(),
                bytes
            );
            assert_eq!(fixture.get_count(), 1);

            let head = request(router.clone(), Method::HEAD, &path, None).await;
            assert_eq!(head.status(), StatusCode::OK);
            assert_eq!(head.headers()["docker-content-digest"], digest);
            assert_eq!(head.headers()[header::CONTENT_LENGTH], "16");
            assert_eq!(
                to_bytes(head.into_body(), usize::MAX).await.unwrap().len(),
                0
            );
            assert_eq!(fixture.head_count(), 1);

            let range = request(router, Method::GET, &path, Some("bytes=2-5")).await;
            assert_eq!(range.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(range.headers()[header::CONTENT_RANGE], "bytes 2-5/16");
            assert_eq!(
                to_bytes(range.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .as_ref(),
                b"2345"
            );
            assert_eq!(fixture.head_count(), 1);
            assert_eq!(fixture.get_count(), 1);
        }

        #[tokio::test]
        async fn authorization_scopes_do_not_share_cache_admission_across_routes() {
            let bytes = b"same-verified-blob".to_vec();
            let docker = Fixture::start(FixtureBehavior::RangeUnsupported, bytes.clone()).await;
            let ghcr = Fixture::start(FixtureBehavior::RangeUnsupported, bytes.clone()).await;
            let (state, _directory) =
                routed_proxy_state(&[(&docker, DOCKER_HUB_ROUTE_ID), (&ghcr, GHCR_ROUTE_ID)]).await;
            let digest = digest(&bytes);
            let docker_path = blob_path(&digest);
            let ghcr_path = format!("/v2/ghcr/{REPOSITORY}/blobs/{digest}");
            let router = registry_router(state.clone());

            for (path, authorization) in [
                (docker_path.as_str(), "Bearer docker-scope"),
                (ghcr_path.as_str(), "Bearer ghcr-scope"),
            ] {
                let mut headers = HeaderMap::new();
                headers.insert(header::AUTHORIZATION, authorization.parse().unwrap());
                let response =
                    request_with_headers(router.clone(), Method::GET, path, headers.clone()).await;
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(
                    to_bytes(response.into_body(), usize::MAX)
                        .await
                        .unwrap()
                        .as_ref(),
                    bytes
                );

                let cached = request_with_headers(router.clone(), Method::GET, path, headers).await;
                assert_eq!(cached.status(), StatusCode::OK);
            }

            assert_eq!(state.cache.stats().await.unwrap().entries, 2);
            assert_eq!(docker.head_count(), 1);
            assert_eq!(docker.get_count(), 1);
            assert_eq!(ghcr.head_count(), 1);
            assert_eq!(ghcr.get_count(), 1);
        }

        #[tokio::test]
        async fn resumes_partial_blob_from_another_node() {
            let bytes = vec![b'x'; 8 * 1024 * 1024 + 4];
            let failing = Fixture::start(FixtureBehavior::DropAfterPrefix, bytes.clone()).await;
            let healthy = Fixture::start(FixtureBehavior::ValidRange, bytes.clone()).await;
            let directory = tempfile::tempdir().unwrap();
            let mut config = Config::for_test(directory.path().to_owned());
            config.parallel_threshold = bytes.len() as u64 + 1;
            let state = AppState::new(config).await.unwrap();
            for (index, fixture) in [&failing, &healthy].into_iter().enumerate() {
                state
                    .nodes
                    .create(NodeInput {
                        name: format!("fixture-{index}"),
                        url: fixture.url(),
                        registry_route_id: DOCKER_HUB_ROUTE_ID,
                        enabled: true,
                        priority: index as i32,
                        cf_preferred: false,
                        connect_ip: None,
                        auth_mode: "none".into(),
                        auth_username: None,
                        auth_header: None,
                        auth_secret: None,
                    })
                    .await
                    .unwrap();
            }
            let digest = digest(&bytes);
            let path = blob_path(&digest);
            let key =
                donkey::cache::CacheStore::key(&format!("/v2/{REPOSITORY}/blobs/{digest}"), None);
            let partial_dir = directory.path().join("cache/tmp").join(&key);
            tokio::fs::create_dir_all(&partial_dir).await.unwrap();
            tokio::fs::write(partial_dir.join("object.partial"), &bytes[..8])
                .await
                .unwrap();
            let router = registry_router(state.clone());
            let second = request(router, Method::GET, &path, None).await;
            assert_eq!(second.status(), StatusCode::OK);
            assert_eq!(
                to_bytes(second.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .as_ref(),
                bytes
            );
            let healthy_ranges = healthy.ranges().await;
            assert!(healthy_ranges.iter().any(|range| {
                range
                    .as_deref()
                    .is_some_and(|value| value.starts_with("bytes=8-"))
            }));
            assert_eq!(state.cache.stats().await.unwrap().entries, 1);
        }
    }

    mod routes {
        use super::*;

        const CLIENT_AUTH: &str = "Basic Y2xpZW50OnBhc3N3b3Jk";

        fn authenticated_headers() -> HeaderMap {
            let mut headers = HeaderMap::new();
            headers.insert(header::AUTHORIZATION, CLIENT_AUTH.parse().unwrap());
            headers
        }

        #[tokio::test]
        async fn same_repository_tag_is_isolated_by_route_with_query_head_range_and_node_auth() {
            let docker_bytes = b"docker-manifest".to_vec();
            let ghcr_bytes = b"ghcr-manifest".to_vec();
            let docker = Fixture::start(FixtureBehavior::ValidRange, docker_bytes.clone()).await;
            let ghcr = Fixture::start(FixtureBehavior::ValidRange, ghcr_bytes.clone()).await;
            let (state, _directory) = authenticated_routed_proxy_state(&[
                (&docker, DOCKER_HUB_ROUTE_ID, "docker-route-secret"),
                (&ghcr, GHCR_ROUTE_ID, "ghcr-route-secret"),
            ])
            .await;
            let router = registry_router(state);

            let docker_response = request_with_headers(
                router.clone(),
                Method::GET,
                "/v2/team/widget/manifests/latest?channel=docker",
                authenticated_headers(),
            )
            .await;
            assert_eq!(docker_response.status(), StatusCode::OK);
            assert_eq!(
                to_bytes(docker_response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .as_ref(),
                docker_bytes
            );

            let ghcr_response = request_with_headers(
                router.clone(),
                Method::GET,
                "/v2/ghcr/team/widget/manifests/latest?channel=ghcr",
                authenticated_headers(),
            )
            .await;
            assert_eq!(ghcr_response.status(), StatusCode::OK);
            assert_eq!(
                to_bytes(ghcr_response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .as_ref(),
                ghcr_bytes
            );

            let head = request_with_headers(
                router.clone(),
                Method::HEAD,
                "/v2/ghcr/team/widget/manifests/latest?head=1",
                authenticated_headers(),
            )
            .await;
            assert_eq!(head.status(), StatusCode::OK);
            assert_eq!(head.headers()[header::CONTENT_LENGTH], "13");

            let mut range_headers = authenticated_headers();
            range_headers.insert(header::RANGE, "bytes=0-5".parse().unwrap());
            let range = request_with_headers(
                router,
                Method::GET,
                "/v2/team/widget/manifests/latest?range=1",
                range_headers,
            )
            .await;
            assert_eq!(range.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(range.headers()[header::CONTENT_RANGE], "bytes 0-5/15");
            assert_eq!(
                to_bytes(range.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .as_ref(),
                b"docker"
            );

            assert_eq!(
                docker.uris().await,
                vec![
                    "/v2/team/widget/manifests/latest?channel=docker",
                    "/v2/team/widget/manifests/latest?range=1",
                ]
            );
            assert_eq!(
                ghcr.uris().await,
                vec![
                    "/v2/team/widget/manifests/latest?channel=ghcr",
                    "/v2/team/widget/manifests/latest?head=1",
                ]
            );
            assert_eq!(
                docker.route_auth().await,
                vec![
                    Some("docker-route-secret".into()),
                    Some("docker-route-secret".into()),
                ]
            );
            assert_eq!(
                ghcr.route_auth().await,
                vec![
                    Some("ghcr-route-secret".into()),
                    Some("ghcr-route-secret".into()),
                ]
            );
        }

        #[tokio::test]
        async fn retryable_failure_never_crosses_registry_routes() {
            let docker_bytes = b"docker-only".to_vec();
            let failing =
                Fixture::start(FixtureBehavior::RetryableFailure, docker_bytes.clone()).await;
            let healthy = Fixture::start(FixtureBehavior::ValidRange, docker_bytes.clone()).await;
            let ghcr = Fixture::start(FixtureBehavior::ValidRange, b"ghcr-only".to_vec()).await;
            let (state, _directory) = routed_proxy_state(&[
                (&failing, DOCKER_HUB_ROUTE_ID),
                (&healthy, DOCKER_HUB_ROUTE_ID),
                (&ghcr, GHCR_ROUTE_ID),
            ])
            .await;

            let response = request(
                registry_router(state),
                Method::GET,
                "/v2/team/widget/manifests/latest",
                None,
            )
            .await;

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .as_ref(),
                docker_bytes
            );
            assert_eq!(failing.get_count(), 1);
            assert_eq!(healthy.get_count(), 1);
            assert_eq!(ghcr.head_count(), 0);
            assert_eq!(ghcr.get_count(), 0);
        }
    }
}
