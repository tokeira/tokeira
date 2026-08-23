//! Rendering helpers for compile-time build metadata.
//!
//! `tokeira-build-info` intentionally owns only immutable metadata. Human and
//! JSON presentation live in the CLI layer so the metadata crate stays usable
//! from low-level crates without pulling in formatting or serialization policy.

use tokeira_build_info::BuildInfo;

pub(crate) fn format_version_short(info: &BuildInfo) -> String {
    format!(
        "tokeira {}\ngit {}\nbuild {}",
        info.tokeira_version, info.tokeira_git_sha, info.build_mode
    )
}

pub(crate) fn format_version_verbose(info: &BuildInfo) -> String {
    [
        format!("tokeira_version: {}", info.tokeira_version),
        format!("tokeira_git_sha: {}", info.tokeira_git_sha),
        format!("temporal_proto_version: {}", info.temporal_proto_version),
        format!("temporal_server_compat: {}", info.temporal_server_compat),
        format!("rust_toolchain: {}", info.rust_toolchain),
        format!("source_tree_hash: {}", info.source_tree_hash),
        format!("feature_matrix_digest: {}", info.feature_matrix_digest),
        format!("sdk_matrix_digest: {}", info.sdk_matrix_digest),
        format!("build_mode: {}", info.build_mode),
    ]
    .join("\n")
}

pub(crate) fn format_version_json(info: &BuildInfo) -> String {
    let value = serde_json::json!({
        "tokeira_version": info.tokeira_version,
        "tokeira_git_sha": info.tokeira_git_sha,
        "temporal_proto_version": info.temporal_proto_version,
        "temporal_server_compat": info.temporal_server_compat,
        "rust_toolchain": info.rust_toolchain,
        "source_tree_hash": info.source_tree_hash,
        "feature_matrix_digest": info.feature_matrix_digest,
        "sdk_matrix_digest": info.sdk_matrix_digest,
        "build_mode": info.build_mode,
    });

    match serde_json::to_string_pretty(&value) {
        Ok(rendered) => rendered,
        Err(error) => format!(r#"{{"error":"failed to render build info: {error}"}}"#),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INFO: BuildInfo = BuildInfo {
        tokeira_version: "0.1.0",
        tokeira_git_sha: "12345678",
        temporal_proto_version: "v1.47.0",
        temporal_server_compat: "1.27.0",
        rust_toolchain: "1.95",
        source_tree_hash: "abc",
        feature_matrix_digest: "feature",
        sdk_matrix_digest: "sdk",
        build_mode: "dev",
        schema_min_supported_version: 1,
        schema_target_version: 67,
        schema_max_readable_version: 67,
        schema_migration_set_digest: "sha256:fixture",
    };

    #[test]
    fn short_format_contains_operator_identifiers() {
        let rendered = format_version_short(&INFO);
        assert!(rendered.contains("0.1.0"));
        assert!(rendered.contains("12345678"));
        assert!(rendered.contains("dev"));
    }

    #[test]
    fn json_format_uses_stable_field_names() {
        let rendered = format_version_json(&INFO);
        assert!(rendered.contains("\"temporal_proto_version\""));
        assert!(rendered.contains("\"feature_matrix_digest\""));
    }
}
