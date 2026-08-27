use std::path::{Path, PathBuf};

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

pub(super) fn build_archive(output: &Path, archive: ArchiveInput) -> anyhow::Result<()> {
    match archive {
        ArchiveInput::Docker {
            config_json,
            reference,
            layers,
            media_types,
        } => build_docker_archive(output, &config_json, reference, &layers, &media_types),
        ArchiveInput::Oci { layout } => build_oci_archive(output, &layout),
    }
}

pub(super) fn build_docker_archive(
    output: &Path,
    config_json: &str,
    reference: String,
    layers: &[PathBuf],
    media_types: &[String],
) -> anyhow::Result<()> {
    let config = oci_spec_builder::image::ImageConfiguration::from_reader(config_json.as_bytes())
        .map_err(|error| anyhow::anyhow!("invalid image config: {error}"))?;
    let file = std::fs::File::create(output)?;
    let mut builder = oci_tar_builder::Builder::default();
    builder.add_config(config, reference);
    for (path, media_type) in layers.iter().zip(media_types) {
        builder.add_layer_with_media_type(path, media_type.clone());
    }
    builder.build(file)
}

pub(super) fn build_oci_archive(output: &Path, layout: &Path) -> anyhow::Result<()> {
    let file = std::fs::File::create(output)?;
    let mut builder = tar::Builder::new(file);
    builder.append_path_with_name(layout.join("oci-layout"), "oci-layout")?;
    builder.append_path_with_name(layout.join("index.json"), "index.json")?;
    builder.append_dir_all("blobs", layout.join("blobs"))?;
    builder.finish()?;
    Ok(())
}
