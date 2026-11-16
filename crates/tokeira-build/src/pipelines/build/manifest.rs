//! The build-metadata manifest that a versioned `tokeirad` build compiles in.
//!
//! `tokeira-build-info` embeds whatever manifest `TOKEIRA_BUILD_MANIFEST_PATH`
//! names. The image build therefore derives the manifest twice and refuses to
//! build when the two derivations disagree, then refuses to export a binary
//! whose `--version --json` report differs from the manifest in any field.

use std::path::Path;

use dagger_sdk::Container;
use serde_json::Value;

use super::{ImageBuildProvenance, image_build_provenance};
use crate::{BuildError, rust_toolchain_version};

/// Lists the workspace's packages, so the manifest names the version Cargo
/// compiles into `tokeirad`.
const PACKAGE_METADATA_COMMAND: [&str; 6] = [
    "cargo",
    "metadata",
    "--locked",
    "--no-deps",
    "--format-version",
    "1",
];

/// Prints, as JSON, the compatibility-matrix digests of the uploaded workspace,
/// not those compiled into the `tkr` that drives the build.
const MATRIX_DIGESTS_COMMAND: [&str; 8] = [
    "cargo",
    "run",
    "--locked",
    "--quiet",
    "-p",
    "tokeira-compatibility",
    "--example",
    "matrix_digests",
];

/// How errors name [`MATRIX_DIGESTS_COMMAND`].
const MATRIX_DIGESTS: &str = "tokeira-compatibility's `matrix_digests` example";

/// Sets each derivation's builder commands apart, so the engine runs the
/// second derivation's commands instead of answering from the first's.
const DERIVATION_VARIABLE: &str = "TOKEIRA_MANIFEST_DERIVATION";

/// The workspace file that holds the Temporal compatibility pins.
const TEMPORAL_PINS_PATH: &str = "crates/tokeira-build-info/src/pinned.rs";

/// The build mode every manifest-carrying build records.
const VERSIONED: &str = "versioned";

/// Every value a versioned `tokeirad` build embeds through its manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VersionedBuildManifest {
    pub(super) tokeira_version: String,
    pub(super) git_sha: String,
    pub(super) source_revision: String,
    pub(super) temporal_proto_version: String,
    pub(super) temporal_server_compat: String,
    pub(super) rust_toolchain: String,
    pub(super) source_tree_hash: String,
    pub(super) feature_matrix_digest: String,
    pub(super) sdk_matrix_digest: String,
}

impl VersionedBuildManifest {
    /// Derive the manifest twice from `source`, the builder holding the
    /// uploaded workspace, and require the two derivations to agree.
    pub(super) async fn derive_twice(
        workspace_root: &Path,
        source: &Container,
    ) -> Result<Self, BuildError> {
        let first = Self::derive(workspace_root, source, "first").await?;
        let second = Self::derive(workspace_root, source, "second").await?;
        let disagreeing = first
            .fields()
            .into_iter()
            .zip(second.fields())
            .filter(|((_, _, mine), (_, _, theirs))| mine != theirs)
            .map(|((key, _, _), _)| key)
            .collect::<Vec<_>>();
        if disagreeing.is_empty() {
            return Ok(first);
        }
        Err(BuildError::Validation {
            reason: format!(
                "two derivations of the versioned build manifest disagree on {}",
                disagreeing.join(", ")
            ),
        })
    }

    /// One derivation. Git provenance comes from the host worktree, because
    /// `.git` stays out of the upload; the package version and the matrix
    /// digests come from the uploaded workspace, in the builder.
    async fn derive(
        workspace_root: &Path,
        source: &Container,
        derivation: &str,
    ) -> Result<Self, BuildError> {
        let ImageBuildProvenance {
            git_sha,
            source_revision,
            source_tree_hash,
        } = image_build_provenance(workspace_root)?;
        let (temporal_proto_version, temporal_server_compat) = temporal_pins(workspace_root)?;
        let rust_toolchain = rust_toolchain_version(workspace_root)?;

        let builder = source.with_env_variable(DERIVATION_VARIABLE, derivation);
        let metadata = builder
            .with_exec(PACKAGE_METADATA_COMMAND.to_vec())
            .stdout()
            .await?;
        let digests = builder
            .with_exec(MATRIX_DIGESTS_COMMAND.to_vec())
            .stdout()
            .await?;
        let digests = parse_json(&digests, MATRIX_DIGESTS)?;

        Ok(Self {
            tokeira_version: package_version(&metadata, "tokeirad")?,
            git_sha,
            source_revision,
            temporal_proto_version,
            temporal_server_compat,
            rust_toolchain,
            source_tree_hash,
            feature_matrix_digest: string_field(&digests, "feature_matrix_digest", MATRIX_DIGESTS)?,
            sdk_matrix_digest: string_field(&digests, "sdk_matrix_digest", MATRIX_DIGESTS)?,
        })
    }

    /// Each manifest key, the `tokeirad --version --json` field that reports
    /// it, and its value, in the manifest's line order.
    fn fields(&self) -> [(&'static str, &'static str, &str); 10] {
        [
            ("TOKEIRA_VERSION", "tokeira_version", &self.tokeira_version),
            ("TOKEIRA_GIT_SHA", "tokeira_git_sha", &self.git_sha),
            (
                "TOKEIRA_SOURCE_REVISION",
                "source_revision",
                &self.source_revision,
            ),
            (
                "TEMPORAL_PROTO_VERSION",
                "temporal_proto_version",
                &self.temporal_proto_version,
            ),
            (
                "TEMPORAL_SERVER_COMPAT",
                "temporal_server_compat",
                &self.temporal_server_compat,
            ),
            ("RUST_TOOLCHAIN", "rust_toolchain", &self.rust_toolchain),
            (
                "SOURCE_TREE_HASH",
                "source_tree_hash",
                &self.source_tree_hash,
            ),
            (
                "FEATURE_MATRIX_DIGEST",
                "feature_matrix_digest",
                &self.feature_matrix_digest,
            ),
            (
                "SDK_MATRIX_DIGEST",
                "sdk_matrix_digest",
                &self.sdk_matrix_digest,
            ),
            ("BUILD_MODE", "build_mode", VERSIONED),
        ]
    }

    /// The manifest file's text: one `KEY=value` line per field.
    pub(super) fn render(&self) -> Result<String, BuildError> {
        let mut text = String::new();
        for (key, _, value) in self.fields() {
            if value.is_empty() || value.trim() != value || value.contains(char::is_control) {
                return Err(BuildError::Validation {
                    reason: format!(
                        "the versioned build manifest cannot carry {key} `{}`",
                        value.escape_debug()
                    ),
                });
            }
            text.push_str(key);
            text.push('=');
            text.push_str(value);
            text.push('\n');
        }
        Ok(text)
    }

    /// Require the built binary's `--version --json` report to carry every
    /// manifest field, and the server version the version and Git SHA compose.
    pub(super) fn require_reported(&self, report: &str) -> Result<(), BuildError> {
        let report = parse_json(report, "`tokeirad --version --json`")?;
        let server_version = format!("{}+{}", self.tokeira_version, self.git_sha);
        let mismatches = self
            .fields()
            .into_iter()
            .map(|(_, field, value)| (field, value))
            .chain([("server_version", server_version.as_str())])
            .filter_map(|(field, value)| {
                let reported = report.get(field).and_then(Value::as_str);
                (reported != Some(value)).then(|| match reported {
                    Some(reported) => {
                        format!("{field} `{reported}` where the manifest has `{value}`")
                    }
                    None => format!("no {field} where the manifest has `{value}`"),
                })
            })
            .collect::<Vec<_>>();
        if mismatches.is_empty() {
            return Ok(());
        }
        Err(BuildError::Validation {
            reason: format!("the built tokeirad reports {}", mismatches.join("; ")),
        })
    }
}

/// The Temporal proto version and server compatibility claim the workspace
/// pins, read as `tokeira-build-info`'s build script reads them.
fn temporal_pins(workspace_root: &Path) -> Result<(String, String), BuildError> {
    let path = workspace_root.join(TEMPORAL_PINS_PATH);
    let source = std::fs::read_to_string(&path).map_err(|error| BuildError::Validation {
        reason: format!("failed to read {}: {error}", path.display()),
    })?;
    let pin = |name: &str| {
        let declaration = format!("pub const {name}: &str = \"");
        source
            .lines()
            .find_map(|line| {
                let (value, _) = line
                    .trim_start()
                    .strip_prefix(declaration.as_str())?
                    .split_once("\";")?;
                Some(value.to_owned())
            })
            .ok_or_else(|| BuildError::Validation {
                reason: format!("{} declares no `{name}`", path.display()),
            })
    };
    Ok((
        pin("TEMPORAL_PROTO_VERSION")?,
        pin("TEMPORAL_SERVER_COMPAT")?,
    ))
}

fn package_version(metadata: &str, package: &str) -> Result<String, BuildError> {
    let metadata = parse_json(metadata, "`cargo metadata`")?;
    metadata
        .get("packages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|candidate| candidate.get("name").and_then(Value::as_str) == Some(package))
        .and_then(|candidate| candidate.get("version"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| BuildError::Validation {
            reason: format!("`cargo metadata` lists no version for package `{package}`"),
        })
}

fn parse_json(text: &str, command: &str) -> Result<Value, BuildError> {
    serde_json::from_str(text).map_err(|error| BuildError::Validation {
        reason: format!("{command} printed invalid JSON: {error}"),
    })
}

fn string_field(document: &Value, field: &str, command: &str) -> Result<String, BuildError> {
    document
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| BuildError::Validation {
            reason: format!("{command} printed no `{field}`"),
        })
}

/// What a `tokeirad` built with `manifest` prints for `--version --json`.
#[cfg(test)]
pub(super) fn reported_by(manifest: &VersionedBuildManifest) -> Value {
    serde_json::json!({
        "server_version": format!("{}+{}", manifest.tokeira_version, manifest.git_sha),
        "tokeira_version": manifest.tokeira_version,
        "tokeira_git_sha": manifest.git_sha,
        "source_revision": manifest.source_revision,
        "temporal_proto_version": manifest.temporal_proto_version,
        "temporal_server_compat": manifest.temporal_server_compat,
        "rust_toolchain": manifest.rust_toolchain,
        "source_tree_hash": manifest.source_tree_hash,
        "feature_matrix_digest": manifest.feature_matrix_digest,
        "sdk_matrix_digest": manifest.sdk_matrix_digest,
        "build_mode": "versioned",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> VersionedBuildManifest {
        VersionedBuildManifest {
            tokeira_version: "0.7.0".to_owned(),
            git_sha: "0123abcd".to_owned(),
            source_revision: "0123abcd0123abcd0123abcd0123abcd0123abcd".to_owned(),
            temporal_proto_version: "v1.62.11".to_owned(),
            temporal_server_compat: "1.31.0".to_owned(),
            rust_toolchain: "1.99.1".to_owned(),
            source_tree_hash: "ab".repeat(32),
            feature_matrix_digest: "0123456789abcdef".to_owned(),
            sdk_matrix_digest: "fedcba9876543210".to_owned(),
        }
    }

    #[test]
    fn the_manifest_renders_every_key_tokeira_build_info_reads() {
        let rendered = manifest().render().expect("renders");

        assert_eq!(
            rendered,
            format!(
                "TOKEIRA_VERSION=0.7.0\n\
                 TOKEIRA_GIT_SHA=0123abcd\n\
                 TOKEIRA_SOURCE_REVISION=0123abcd0123abcd0123abcd0123abcd0123abcd\n\
                 TEMPORAL_PROTO_VERSION=v1.62.11\n\
                 TEMPORAL_SERVER_COMPAT=1.31.0\n\
                 RUST_TOOLCHAIN=1.99.1\n\
                 SOURCE_TREE_HASH={}\n\
                 FEATURE_MATRIX_DIGEST=0123456789abcdef\n\
                 SDK_MATRIX_DIGEST=fedcba9876543210\n\
                 BUILD_MODE=versioned\n",
                "ab".repeat(32)
            )
        );
    }

    #[test]
    fn a_value_that_would_not_survive_the_manifest_format_is_refused() {
        for value in ["", " padded", "two\nlines"] {
            let mut manifest = manifest();
            manifest.feature_matrix_digest = value.to_owned();
            let error = manifest.render().expect_err("refused");
            assert!(
                error.to_string().contains("FEATURE_MATRIX_DIGEST"),
                "{error}"
            );
        }
    }

    #[test]
    fn a_report_carrying_every_field_is_accepted() {
        manifest()
            .require_reported(&reported_by(&manifest()).to_string())
            .expect("accepted");
    }

    #[test]
    fn a_report_that_differs_in_any_field_is_refused() {
        let expected = reported_by(&manifest());
        let fields = expected
            .as_object()
            .expect("an object")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(fields.len(), 11);
        for field in fields {
            let mut changed = expected.clone();
            changed[&field] = Value::String("dev".to_owned());
            let error = manifest()
                .require_reported(&changed.to_string())
                .expect_err("a changed field is refused");
            assert!(
                error.to_string().contains(&format!("{field} `dev`")),
                "{error}"
            );

            let mut missing = expected.clone();
            missing.as_object_mut().expect("an object").remove(&field);
            let error = manifest()
                .require_reported(&missing.to_string())
                .expect_err("a missing field is refused");
            assert!(
                error.to_string().contains(&format!("no {field} ")),
                "{error}"
            );
        }
    }

    #[test]
    fn the_temporal_pins_are_read_from_the_workspace() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let (proto, server) = temporal_pins(&workspace_root).expect("the workspace pins");

        assert!(proto.starts_with('v'), "{proto}");
        assert!(!server.is_empty());
    }

    #[test]
    fn the_package_version_comes_from_cargo_metadata() {
        let metadata = serde_json::json!({
            "packages": [
                { "name": "tkr", "version": "0.1.0" },
                { "name": "tokeirad", "version": "0.7.0" },
            ],
        })
        .to_string();

        assert_eq!(
            package_version(&metadata, "tokeirad").expect("listed"),
            "0.7.0"
        );
        assert!(package_version(&metadata, "absent").is_err());
    }
}
