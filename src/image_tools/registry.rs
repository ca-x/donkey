use std::{path::Path, time::Duration};

use futures_util::StreamExt;
use oci_client::{
    Client, Reference, RegistryOperation,
    client::ClientConfig,
    errors::OciDistributionError,
    manifest::{OciDescriptor, OciImageManifest, OciManifest},
    secrets::RegistryAuth,
};
use tokio::fs::File;
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
        let (manifest, digest, config_json, index_digest) = self
            .client
            .pull_manifest_and_config_and_list_digest(&self.reference, &self.auth)
            .await
            .map_err(oci_error)?;
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
        self.client
            .pull_blob(&self.reference, descriptor, output)
            .await
            .map_err(oci_error)
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
