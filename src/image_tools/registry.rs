use std::{path::Path, time::Duration};

use futures_util::StreamExt;
use oci_client::{
    Client, Reference, RegistryOperation,
    client::ClientConfig,
    errors::OciDistributionError,
    manifest::{OciDescriptor, OciImageManifest, OciManifest},
    secrets::RegistryAuth,
};
use tokio::{
    fs::File,
    io::{AsyncWrite, AsyncWriteExt},
};
use tokio_util::io::ReaderStream;

use crate::error::{ApiResult, AppError};

pub(super) struct PulledManifest {
    pub(super) manifest: OciImageManifest,
    pub(super) digest: String,
    pub(super) config_json: String,
    pub(super) index_digest: Option<String>,
}

pub(super) struct SourceRegistryAdapter {
    client: Client,
    reference: Reference,
    auth: RegistryAuth,
}

impl SourceRegistryAdapter {
    pub(super) fn new(reference: Reference, auth: RegistryAuth, os: &str, arch: &str) -> Self {
        Self {
            client: image_client(os, arch),
            reference,
            auth,
        }
    }

    pub(super) async fn pull_manifest(&self) -> ApiResult<PulledManifest> {
        let (manifest, digest, index_digest) = self
            .client
            .pull_image_manifest_and_list_digest(&self.reference, &self.auth)
            .await
            .map_err(oci_error)?;
        if !(0..=64 * 1024 * 1024).contains(&manifest.config.size) {
            return Err(AppError::Upstream("image config exceeds 64 MiB".into()));
        }
        let mut config = Vec::new();
        self.read_blob(&manifest.config, &mut config).await?;
        let config_json = String::from_utf8(config)
            .map_err(|_| AppError::Upstream("image config is not UTF-8 JSON".into()))?;
        Ok(PulledManifest {
            manifest,
            digest,
            config_json,
            index_digest,
        })
    }

    pub(super) async fn pull_blob(
        &self,
        descriptor: &OciDescriptor,
        output: File,
    ) -> ApiResult<()> {
        self.read_blob(descriptor, output).await
    }

    async fn read_blob<T: AsyncWrite + Unpin>(
        &self,
        descriptor: &OciDescriptor,
        mut output: T,
    ) -> ApiResult<()> {
        let expected = u64::try_from(descriptor.size).map_err(|_| AppError::Integrity)?;
        // The SDK verifies digests; also enforce size before each write so a bad
        // descriptor cannot grow a temporary file or config buffer without bound.
        let mut stream = self
            .client
            .pull_blob_stream(&self.reference, descriptor)
            .await
            .map_err(oci_error)?;
        let mut size = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| oci_error(OciDistributionError::IoError(error)))?;
            size = size.saturating_add(chunk.len() as u64);
            if size > expected {
                return Err(AppError::Integrity);
            }
            output.write_all(&chunk).await?;
        }
        if size != expected {
            return Err(AppError::Integrity);
        }
        output.flush().await?;
        Ok(())
    }
}

pub(super) struct DestinationRegistryAdapter<'a> {
    client: &'a Client,
    reference: &'a Reference,
    auth: &'a RegistryAuth,
}

impl<'a> DestinationRegistryAdapter<'a> {
    pub(super) fn new(
        client: &'a Client,
        reference: &'a Reference,
        auth: &'a RegistryAuth,
    ) -> Self {
        Self {
            client,
            reference,
            auth,
        }
    }

    pub(super) async fn authenticate(&self) -> ApiResult<()> {
        self.client
            .auth(self.reference, self.auth, RegistryOperation::Push)
            .await
            .map(|_| ())
            .map_err(oci_error)
    }

    pub(super) async fn blob_exists(&self, digest: &str) -> ApiResult<bool> {
        self.client
            .blob_exists(self.reference, digest)
            .await
            .map_err(oci_error)
    }

    pub(super) async fn push_file(&self, path: &Path, digest: &str) -> ApiResult<()> {
        let stream = ReaderStream::new(File::open(path).await?)
            .map(|result| result.map_err(OciDistributionError::IoError));
        self.client
            .push_blob_stream(self.reference, stream, digest)
            .await
            .map(|_| ())
            .map_err(oci_error)
    }

    pub(super) async fn push_bytes(&self, bytes: Vec<u8>, digest: &str) -> ApiResult<()> {
        self.client
            .push_blob(self.reference, bytes, digest)
            .await
            .map(|_| ())
            .map_err(oci_error)
    }

    pub(super) async fn push_manifest(&self, manifest: OciImageManifest) -> ApiResult<()> {
        self.client
            .push_manifest(self.reference, &OciManifest::Image(manifest))
            .await
            .map_err(oci_error)?;
        Ok(())
    }
}

pub(super) fn image_client(os: &str, arch: &str) -> Client {
    let os = os.to_owned();
    let arch = arch.to_owned();
    let config = ClientConfig {
        max_concurrent_download: 4,
        max_concurrent_upload: 4,
        read_timeout: Some(Duration::from_secs(120)),
        connect_timeout: Some(Duration::from_secs(15)),
        platform_resolver: Some(Box::new(move |manifests| {
            manifests
                .iter()
                .find(|entry| {
                    entry.platform.as_ref().is_some_and(|platform| {
                        platform.os.to_string() == os && platform.architecture.to_string() == arch
                    })
                })
                .map(|entry| entry.digest.clone())
        })),
        ..Default::default()
    };
    Client::new(config)
}

fn oci_error(error: OciDistributionError) -> AppError {
    AppError::Upstream(error.to_string().chars().take(500).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::{Method::GET, MockServer};
    use sha2::{Digest, Sha256};

    #[tokio::test]
    async fn direct_inspection_reads_only_metadata_and_bounds_layer_writes() {
        let server = MockServer::start_async().await;
        let config = br#"{"architecture":"amd64","os":"linux"}"#;
        let config_digest = format!("sha256:{:x}", Sha256::digest(config));
        let layer = b"abc";
        let layer_digest = format!("sha256:{:x}", Sha256::digest(layer));
        server
            .mock_async(|when, then| {
                when.method(GET).path("/v2/");
                then.status(200);
            })
            .await;
        let manifest = serde_json::json!({
            "schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len() },
            "layers": [{ "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": layer_digest, "size": layer.len() }]
        });
        server
            .mock_async(|when, then| {
                when.method(GET).path("/v2/library/test/manifests/latest");
                then.status(200)
                    .header("content-type", "application/vnd.oci.image.manifest.v1+json")
                    .json_body(manifest);
            })
            .await;
        let config_request = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path(format!("/v2/library/test/blobs/{config_digest}"));
                then.status(200).body(config);
            })
            .await;
        let layer_request = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path(format!("/v2/library/test/blobs/{layer_digest}"));
                then.status(200).body(layer);
            })
            .await;
        let source = SourceRegistryAdapter {
            client: Client::new(ClientConfig {
                protocol: oci_client::client::ClientProtocol::Http,
                ..Default::default()
            }),
            reference: format!("{}/library/test:latest", server.address())
                .parse()
                .unwrap(),
            auth: RegistryAuth::Anonymous,
        };
        let pulled = source.pull_manifest().await.unwrap();
        assert_eq!(pulled.config_json.as_bytes(), config);
        config_request.assert_calls_async(1).await;
        layer_request.assert_calls_async(0).await;
        let descriptor = &pulled.manifest.layers[0];
        let mut output = Vec::new();
        source.read_blob(descriptor, &mut output).await.unwrap();
        assert_eq!(output, layer);
        let mut wrong_size = descriptor.clone();
        wrong_size.size = 2;
        output.clear();
        assert!(matches!(
            source.read_blob(&wrong_size, &mut output).await,
            Err(AppError::Integrity)
        ));
        assert!(output.len() <= 2);
        wrong_size.size = 4;
        assert!(matches!(
            source.read_blob(&wrong_size, &mut Vec::new()).await,
            Err(AppError::Integrity)
        ));
    }
}
