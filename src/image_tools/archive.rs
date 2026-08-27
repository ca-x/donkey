use std::{
    io::Write,
    path::{Path, PathBuf},
};

pub(super) enum ArchiveInput {
    Docker {
        config_json: String,
        reference: String,
        layers: Vec<PathBuf>,
        media_types: Vec<String>,
    },
    Oci {
        layout: PathBuf,
    },
}

pub(super) fn build_archive(
    output: &Path,
    archive: ArchiveInput,
    cancellation: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    match archive {
        ArchiveInput::Docker {
            config_json,
            reference,
            layers,
            media_types,
        } => build_docker_archive_with_cancellation(
            output,
            &config_json,
            reference,
            &layers,
            &media_types,
            cancellation,
        ),
        ArchiveInput::Oci { layout } => {
            build_oci_archive_with_cancellation(output, &layout, cancellation)
        }
    }
}

#[cfg(test)]
pub(super) fn build_docker_archive(
    output: &Path,
    config_json: &str,
    reference: String,
    layers: &[PathBuf],
    media_types: &[String],
) -> anyhow::Result<()> {
    build_docker_archive_with_cancellation(
        output,
        config_json,
        reference,
        layers,
        media_types,
        tokio_util::sync::CancellationToken::new(),
    )
}

fn build_docker_archive_with_cancellation(
    output: &Path,
    config_json: &str,
    reference: String,
    layers: &[PathBuf],
    media_types: &[String],
    cancellation: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    let config = oci_spec_builder::image::ImageConfiguration::from_reader(config_json.as_bytes())
        .map_err(|error| anyhow::anyhow!("invalid image config: {error}"))?;
    let file = std::fs::File::create(output)?;
    let mut builder = oci_tar_builder::Builder::default();
    builder.add_config(config, reference);
    for (path, media_type) in layers.iter().zip(media_types) {
        builder.add_layer_with_media_type(path, media_type.clone());
    }
    builder.build(CancellableWriter::new(file, cancellation))
}

#[cfg(test)]
pub(super) fn build_oci_archive(output: &Path, layout: &Path) -> anyhow::Result<()> {
    build_oci_archive_with_cancellation(output, layout, tokio_util::sync::CancellationToken::new())
}

fn build_oci_archive_with_cancellation(
    output: &Path,
    layout: &Path,
    cancellation: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    let file = std::fs::File::create(output)?;
    let mut builder = tar::Builder::new(CancellableWriter::new(file, cancellation));
    builder.append_path_with_name(layout.join("oci-layout"), "oci-layout")?;
    builder.append_path_with_name(layout.join("index.json"), "index.json")?;
    builder.append_dir_all("blobs", layout.join("blobs"))?;
    builder.finish()?;
    Ok(())
}

struct CancellableWriter<W> {
    inner: W,
    cancellation: tokio_util::sync::CancellationToken,
}

impl<W> CancellableWriter<W> {
    fn new(inner: W, cancellation: tokio_util::sync::CancellationToken) -> Self {
        Self {
            inner,
            cancellation,
        }
    }
}

impl<W: Write> Write for CancellableWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(std::io::Error::other("image archive cancelled"));
        }
        self.inner.write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(std::io::Error::other("image archive cancelled"));
        }
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_archive_stops_before_writing_content() {
        let directory = tempfile::tempdir().unwrap();
        let layout = directory.path().join("layout");
        std::fs::create_dir_all(layout.join("blobs/sha256")).unwrap();
        std::fs::write(
            layout.join("oci-layout"),
            br#"{"imageLayoutVersion":"1.0.0"}"#,
        )
        .unwrap();
        std::fs::write(layout.join("index.json"), br#"{"schemaVersion":2}"#).unwrap();
        let output = directory.path().join("cancelled.tar");
        let cancellation = tokio_util::sync::CancellationToken::new();
        cancellation.cancel();

        let result = build_archive(&output, ArchiveInput::Oci { layout }, cancellation);

        assert!(result.is_err());
        assert!(std::fs::metadata(output).unwrap().len() < 1024);
    }
}
