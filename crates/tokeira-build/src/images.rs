//! The container images that tokeira-build's Dagger pipelines start from.
//!
//! Each reference carries the digest of the image's multi-platform index, so
//! a tag that moves upstream cannot change what a pipeline runs, and the
//! engine resolves the reference without consulting `dagger.lock`. The tag
//! stays to record what the digest was resolved from; the digest decides.
//!
//! To advance an image, resolve the index digest of the new tag, for example
//! with `docker buildx imagetools inspect <image>:<tag>`, and replace the
//! reference. The provisioner's build image is not here: callers supply it,
//! and the provisioner pipeline refuses one without a digest.

use crate::BuildError;

/// The Rust release in [`RUST_BUILDER_IMAGE`]. `rust-toolchain.toml` must
/// name it.
pub(crate) const RUST_BUILDER_TOOLCHAIN: &str = "1.99.1";

/// The builder for the tokeirad image and the release train's Cargo steps.
pub(crate) const RUST_BUILDER_IMAGE: &str = "rust:1.99.1-slim-bookworm@sha256:f2c004e3d2d8b547e8cf7ffbc29205385f4e71d23e53e4e383065db5e9f822cd";

/// The runtime base of the tokeirad image.
pub(crate) const TOKEIRAD_RUNTIME_IMAGE: &str = "cgr.dev/chainguard/glibc-dynamic:latest@sha256:48ce6d7d3006f17054a54ef41aac5222ac308282db6d67ed508cf024a6df04de";

/// The base of the release train's observation and GitHub steps.
pub(crate) const OBSERVATION_IMAGE: &str =
    "debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc";

/// Refuse a workspace toolchain that [`RUST_BUILDER_IMAGE`] does not carry.
pub(crate) fn require_builder_toolchain(toolchain: &str) -> Result<(), BuildError> {
    if toolchain == RUST_BUILDER_TOOLCHAIN {
        return Ok(());
    }
    Err(BuildError::Validation {
        reason: format!(
            "rust-toolchain.toml names Rust {toolchain}, but the builder image carries Rust \
             {RUST_BUILDER_TOOLCHAIN}; advance RUST_BUILDER_IMAGE in \
             crates/tokeira-build/src/images.rs with the toolchain"
        ),
    })
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;

    /// Every image constant, by the name a pipeline passes to `from`.
    const PINNED_IMAGES: [(&str, &str); 3] = [
        ("RUST_BUILDER_IMAGE", RUST_BUILDER_IMAGE),
        ("TOKEIRAD_RUNTIME_IMAGE", TOKEIRAD_RUNTIME_IMAGE),
        ("OBSERVATION_IMAGE", OBSERVATION_IMAGE),
    ];

    /// The one `from` argument that is not a constant here: the provisioner's
    /// caller-supplied build image, whose digest the pipeline validates.
    const VALIDATED_BUILD_IMAGE: &str = "&request.build_image";

    fn digest_pinned(reference: &str) -> bool {
        let Some((name, digest)) = reference.split_once("@sha256:") else {
            return false;
        };
        !name.is_empty()
            && !name.contains(char::is_whitespace)
            && digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }

    #[test]
    fn every_image_carries_a_digest() {
        for (name, reference) in PINNED_IMAGES {
            assert!(
                digest_pinned(reference),
                "{name} is not pinned by digest: {reference}"
            );
        }
        assert!(!digest_pinned("rust:1.99.1-slim-bookworm"));
        assert!(!digest_pinned("rust@sha256:abc"));
    }

    #[test]
    fn the_builder_image_carries_the_pinned_toolchain() {
        assert!(RUST_BUILDER_IMAGE.starts_with(&format!("rust:{RUST_BUILDER_TOOLCHAIN}-")));
    }

    #[test]
    fn the_builder_toolchain_is_the_workspace_toolchain() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let toolchain = crate::rust_toolchain_version(&workspace).expect("workspace toolchain");
        assert_eq!(
            toolchain, RUST_BUILDER_TOOLCHAIN,
            "advance RUST_BUILDER_IMAGE with rust-toolchain.toml"
        );
        require_builder_toolchain(&toolchain).expect("the workspace toolchain is admitted");
    }

    #[test]
    fn another_toolchain_is_refused() {
        let error = require_builder_toolchain("1.98.0").expect_err("an unpinned toolchain");
        assert!(error.to_string().contains("RUST_BUILDER_IMAGE"), "{error}");
    }

    /// Every `from` call in this crate's sources names a constant above or the
    /// validated build image, so no pipeline starts from a floating tag.
    #[test]
    fn every_pipeline_starts_from_a_pinned_image() {
        let mut sources = Vec::new();
        collect_sources(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut sources,
        );
        let mut calls = 0;
        for path in sources {
            let text = std::fs::read_to_string(&path).expect("read source");
            for call in [".from(", ".from_opts("] {
                for (at, _) in text.match_indices(call) {
                    let line_start = text[..at].rfind('\n').map_or(0, |newline| newline + 1);
                    let in_comment = text[line_start..at].trim_start().starts_with("//");
                    let in_literal = text[..at].ends_with('"');
                    if in_comment || in_literal {
                        continue;
                    }
                    let argument = text[at + call.len()..]
                        .split([')', ','])
                        .next()
                        .unwrap_or_default()
                        .trim();
                    calls += 1;
                    assert!(
                        argument == VALIDATED_BUILD_IMAGE
                            || PINNED_IMAGES.iter().any(|(name, _)| *name == argument),
                        "{}: `{call}{argument})` names no pinned image",
                        path.display()
                    );
                }
            }
        }
        assert!(calls > 0, "the scan found no `from` calls");
    }

    fn collect_sources(directory: &Path, sources: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(directory).expect("read source directory") {
            let path = entry.expect("source entry").path();
            if path.is_dir() {
                collect_sources(&path, sources);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                sources.push(path);
            }
        }
    }
}
