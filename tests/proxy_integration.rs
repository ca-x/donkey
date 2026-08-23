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
    use donkey::{AppState, Config, nodes::NodeInput, server::registry_router};
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
    }

    #[derive(Default)]
    struct FixtureStats {
        head_count: AtomicUsize,
        get_count: AtomicUsize,
        ranges: Mutex<Vec<Option<String>>>,
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
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    async fn fixture_handler(State(state): State<FixtureState>, request: AxumRequest) -> Response {
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
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::for_test(directory.path().to_owned());
        config.chunk_size = 4;
        config.chunk_concurrency = 4;
        config.parallel_threshold = 1;
        let state = AppState::new(config).await.unwrap();
        for (index, fixture) in fixtures.iter().enumerate() {
            state
                .nodes
                .create(NodeInput {
                    name: format!("fixture-{index}"),
                    url: fixture.url(),
                    kind: "registry".into(),
                    route_prefix: None,
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

    fn digest(bytes: &[u8]) -> String {
        format!("sha256:{:x}", Sha256::digest(bytes))
    }

    fn blob_path(digest: &str) -> String {
        format!("/v2/{REPOSITORY}/blobs/{digest}")
    }

    async fn request(router: Router, method: Method, uri: &str, range: Option<&str>) -> Response {
        let mut builder = http::Request::builder().method(method).uri(uri);
        if let Some(range) = range {
            builder = builder.header(header::RANGE, range);
        }
        router
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap()
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
    }
}
