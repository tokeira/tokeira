//! Compatibility metadata inspection commands.
//!
//! The local path reads compile-time metadata and static matrices directly
//! from `tokeira-build-info` and `tokeira-compatibility`. Remote inspection is
//! reserved for the compatibility service wiring task; until that service
//! exists, the command fails explicitly instead of pretending to query a server.

use std::{fs, path::Path};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::Value;
use tokeira_build_info::BuildInfo;
use tokeira_compatibility::{FEATURE_MATRIX, SDK_MATRIX, feature_matrix_digest, sdk_matrix_digest};

use crate::{cli::CompatCommand, output::build_info};

pub(crate) fn run(command: CompatCommand, global_json: bool) -> Result<()> {
    match command {
        CompatCommand::Show {
            remote,
            json,
            verbose,
        } => run_show(remote, global_json || json, verbose),
        CompatCommand::Diff {
            a,
            b,
            fail_on_incompatible,
        } => run_diff(&a, &b, global_json, fail_on_incompatible),
    }
}

fn run_show(remote: Option<String>, json: bool, verbose: bool) -> Result<()> {
    if let Some(remote) = remote {
        bail!(
            "remote compatibility inspection for {remote} requires the Tokeira compatibility service"
        );
    }

    let info = tokeira_build_info::summary();
    let document = LocalCompatibilityDocument::from_build(info);

    if json {
        println!("{}", serde_json::to_string_pretty(&document)?);
    } else if verbose {
        println!("{}", build_info::format_version_verbose(&info));
        println!("feature_matrix_digest: {}", document.feature_matrix_digest);
        println!("sdk_matrix_digest: {}", document.sdk_matrix_digest);
        println!("features:");
        for feature in document.features {
            println!(
                "  {} ({:?}) — {} RPC(s)",
                feature.id, feature.state, feature.rpc_count
            );
        }
        println!("sdks:");
        for sdk in document.sdks {
            println!(
                "  {} {:?} min={} max_tested={}",
                sdk.language,
                sdk.verification_state,
                sdk.minimum_supported_version,
                sdk.maximum_tested_version
            );
        }
    } else {
        println!("{}", build_info::format_version_short(&info));
        println!("features: {}", document.features.len());
        println!("sdks: {}", document.sdks.len());
    }

    Ok(())
}

fn run_diff(a: &str, b: &str, json: bool, fail_on_incompatible: bool) -> Result<()> {
    if looks_remote(a) || looks_remote(b) {
        bail!("remote compatibility diff requires the Tokeira compatibility service")
    }

    let left = read_json_document(a)?;
    let right = read_json_document(b)?;
    let diff = CompatibilityDiff::from_values(a, b, &left, &right);

    if json {
        println!("{}", serde_json::to_string_pretty(&diff)?);
    } else if diff.changes.is_empty() {
        println!("compatibility documents match");
    } else {
        println!("compatibility documents differ:");
        for change in &diff.changes {
            println!("  {}: {} -> {}", change.field, change.left, change.right);
        }
    }

    if fail_on_incompatible && diff.incompatible {
        bail!("compatibility documents contain incompatible differences");
    }

    Ok(())
}

fn looks_remote(value: &str) -> bool {
    value.contains("://")
}

fn read_json_document(path: &str) -> Result<Value> {
    let content = fs::read_to_string(Path::new(path))
        .with_context(|| format!("failed to read compatibility document `{path}`"))?;
    serde_json::from_str(&content)
        .with_context(|| format!("failed to parse compatibility document `{path}` as JSON"))
}

#[derive(Debug, Serialize)]
struct CompatibilityDiff<'a> {
    left: &'a str,
    right: &'a str,
    incompatible: bool,
    changes: Vec<FieldChange>,
}

impl<'a> CompatibilityDiff<'a> {
    fn from_values(left_name: &'a str, right_name: &'a str, left: &Value, right: &Value) -> Self {
        let fields = [
            "build_info.tokeira_version",
            "build_info.tokeira_git_sha",
            "build_info.temporal_proto_version",
            "build_info.temporal_server_compat",
            "build_info.source_tree_hash",
            "build_info.embedded_feature_matrix_digest",
            "build_info.embedded_sdk_matrix_digest",
            "feature_matrix_digest",
            "sdk_matrix_digest",
        ];
        let changes = fields
            .iter()
            .filter_map(|field| FieldChange::from_path(field, left, right))
            .collect::<Vec<_>>();
        let incompatible = changes.iter().any(FieldChange::is_incompatible);

        Self {
            left: left_name,
            right: right_name,
            incompatible,
            changes,
        }
    }
}

#[derive(Debug, Serialize)]
struct FieldChange {
    field: &'static str,
    left: String,
    right: String,
}

impl FieldChange {
    fn from_path(field: &'static str, left: &Value, right: &Value) -> Option<Self> {
        let left_value = value_at_path(left, field);
        let right_value = value_at_path(right, field);
        if left_value == right_value {
            return None;
        }
        Some(Self {
            field,
            left: render_value(left_value),
            right: render_value(right_value),
        })
    }

    fn is_incompatible(&self) -> bool {
        matches!(
            self.field,
            "build_info.temporal_proto_version"
                | "build_info.temporal_server_compat"
                | "build_info.embedded_feature_matrix_digest"
                | "build_info.embedded_sdk_matrix_digest"
                | "feature_matrix_digest"
                | "sdk_matrix_digest"
        )
    }
}

fn value_at_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

fn render_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(value)) => value.clone(),
        Some(value) => value.to_string(),
        None => "<missing>".to_string(),
    }
}

#[derive(Debug, Serialize)]
struct LocalCompatibilityDocument {
    build_info: BuildInfoDocument,
    feature_matrix_digest: String,
    sdk_matrix_digest: String,
    features: Vec<FeatureSummary>,
    sdks: Vec<SdkSummary>,
}

impl LocalCompatibilityDocument {
    fn from_build(info: BuildInfo) -> Self {
        Self {
            build_info: BuildInfoDocument::from(info),
            feature_matrix_digest: feature_matrix_digest(),
            sdk_matrix_digest: sdk_matrix_digest(),
            features: FEATURE_MATRIX
                .iter()
                .map(|entry| FeatureSummary {
                    id: entry.id,
                    name: entry.name,
                    state: entry.state,
                    capability_field: entry.capability_field,
                    dynamic_config_key: entry.dynamic_config_key,
                    rpc_count: entry.rpcs.len(),
                })
                .collect(),
            sdks: SDK_MATRIX
                .iter()
                .map(|entry| SdkSummary {
                    language: entry.language,
                    minimum_supported_version: entry.minimum_supported_version,
                    maximum_tested_version: entry.maximum_tested_version,
                    verification_state: entry.verification_state,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
struct BuildInfoDocument {
    tokeira_version: &'static str,
    tokeira_git_sha: &'static str,
    temporal_proto_version: &'static str,
    temporal_server_compat: &'static str,
    rust_toolchain: &'static str,
    source_tree_hash: &'static str,
    embedded_feature_matrix_digest: &'static str,
    embedded_sdk_matrix_digest: &'static str,
    build_mode: &'static str,
}

impl From<BuildInfo> for BuildInfoDocument {
    fn from(info: BuildInfo) -> Self {
        Self {
            tokeira_version: info.tokeira_version,
            tokeira_git_sha: info.tokeira_git_sha,
            temporal_proto_version: info.temporal_proto_version,
            temporal_server_compat: info.temporal_server_compat,
            rust_toolchain: info.rust_toolchain,
            source_tree_hash: info.source_tree_hash,
            embedded_feature_matrix_digest: info.feature_matrix_digest,
            embedded_sdk_matrix_digest: info.sdk_matrix_digest,
            build_mode: info.build_mode,
        }
    }
}

#[derive(Debug, Serialize)]
struct FeatureSummary {
    id: &'static str,
    name: &'static str,
    state: tokeira_compatibility::FeatureState,
    capability_field: Option<&'static str>,
    dynamic_config_key: Option<&'static str>,
    rpc_count: usize,
}

#[derive(Debug, Serialize)]
struct SdkSummary {
    language: &'static str,
    minimum_supported_version: &'static str,
    maximum_tested_version: &'static str,
    verification_state: tokeira_compatibility::SdkVerificationState,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use serde_json::json;

    use crate::cli::{Cli, Command, CompatArgs, CompatCommand};

    #[test]
    fn parses_compat_show_commands() {
        assert!(matches!(
            Cli::try_parse_from(["tkr", "compat", "show"])
                .expect("parse")
                .command,
            Command::Compat(CompatArgs {
                command: CompatCommand::Show {
                    remote: None,
                    json: false,
                    verbose: false,
                }
            })
        ));

        assert!(matches!(
            Cli::try_parse_from([
                "tkr",
                "compat",
                "show",
                "--remote",
                "grpc://example:7233",
                "--json",
            ])
            .expect("parse")
            .command,
            Command::Compat(CompatArgs {
                command: CompatCommand::Show {
                    remote: Some(remote),
                    json: true,
                    verbose: false,
                }
            }) if remote == "grpc://example:7233"
        ));
    }

    #[test]
    fn parses_compat_diff_commands() {
        assert!(matches!(
            Cli::try_parse_from([
                "tkr",
                "compat",
                "diff",
                "--a",
                "grpc://a:7233",
                "--b",
                "grpc://b:7233",
            ])
            .expect("parse")
            .command,
            Command::Compat(CompatArgs {
                command: CompatCommand::Diff {
                    a,
                    b,
                    fail_on_incompatible: false,
                }
            }) if a == "grpc://a:7233" && b == "grpc://b:7233"
        ));

        assert!(matches!(
            Cli::try_parse_from([
                "tkr",
                "compat",
                "diff",
                "--a",
                "left.json",
                "--b",
                "right.json",
                "--fail-on-incompatible",
            ])
            .expect("parse")
            .command,
            Command::Compat(CompatArgs {
                command: CompatCommand::Diff {
                    a,
                    b,
                    fail_on_incompatible: true,
                }
            }) if a == "left.json" && b == "right.json"
        ));
    }

    #[test]
    fn diff_marks_matrix_digest_changes_incompatible() {
        let left = json!({
            "build_info": {
                "temporal_proto_version": "v1",
                "temporal_server_compat": "1",
                "embedded_feature_matrix_digest": "a",
                "embedded_sdk_matrix_digest": "s"
            },
            "feature_matrix_digest": "a",
            "sdk_matrix_digest": "s"
        });
        let right = json!({
            "build_info": {
                "temporal_proto_version": "v1",
                "temporal_server_compat": "1",
                "embedded_feature_matrix_digest": "b",
                "embedded_sdk_matrix_digest": "s"
            },
            "feature_matrix_digest": "b",
            "sdk_matrix_digest": "s"
        });

        let diff = CompatibilityDiff::from_values("left", "right", &left, &right);

        assert!(diff.incompatible);
        assert_eq!(diff.changes.len(), 2);
    }
}
