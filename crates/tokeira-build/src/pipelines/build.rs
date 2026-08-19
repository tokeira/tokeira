use std::path::PathBuf;

use dagger_sdk::{Client, HostDirectoryOpts};

use crate::{Arch, BuildError, rust_toolchain_version};

/// Paths excluded from the workspace upload that feeds the Dagger build.
///
/// The exclude list is deliberately conservative: anything cargo might
/// touch during `cargo build` stays in the upload — which is why the
/// vendored SDK crates (`vendor/`) ship: the workspace manifest's path
/// dependencies must resolve inside the engine. The removed directories
/// are build outputs (`target/`), version control metadata, editor state,
/// or documentation/spec sources the Rust toolchain never reads.
///
/// Without this list, Dagger hashes and ships the full `target/` tree
/// (regularly multi-gigabyte) on every invocation, dominating the cold
/// build time and invalidating Dagger's directory cache on trivial edits.
///
/// Patterns are gitignore-style and interpreted by the Dagger engine. The
/// top-level `target` match prevents the workspace-level artifact dir from
/// being shipped; `**/target` catches per-crate target dirs created by
/// running cargo inside a crate subdirectory.
const TOKEIRAD_WORKSPACE_EXCLUDES: &[&str] = &[
    "target",
    "**/target",
    ".git",
    ".github",
    ".vscode",
    ".idea",
    ".kiro",
    ".claude",
    ".DS_Store",
    "artifacts",
    "dev",
    "docs",
    "fixtures",
    "ops",
    "schemas",
    "spec",
    "spikes",
    "tokeira.code-workspace",
    "tokeirad.log",
    "**/*.log",
];

#[derive(Debug, Clone)]
pub struct TokeiradBuildRequest {
    pub arch: Arch,
    pub tag: Option<String>,
    pub workspace_root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct TokeiradBuildResult {
    pub image_name: String,
    pub tags: Vec<String>,
    pub arch: Arch,
    pub toolchain_version: String,
}

/// Build the tokeirad runtime image and load it into the host image store.
///
/// Two persistent cache volumes (the cargo registry and a per-target
/// `target/` dir) replace the old three-stage cargo-chef choreography: a
/// cold build pays full compilation once, and an incremental rebuild
/// recompiles only what changed — exactly where chef was weakest. The
/// built binary is copied out of the cache mount before the image stage,
/// because cache-mount contents do not survive into container layers.
pub async fn build_tokeirad_image(
    request: &TokeiradBuildRequest,
    client: &Client,
) -> Result<TokeiradBuildResult, BuildError> {
    let toolchain = rust_toolchain_version(&request.workspace_root)?;
    let query = client.query();

    // Upload only the sources needed to build `tokeirad`.
    let opts = HostDirectoryOpts::default().with_exclude(TOKEIRAD_WORKSPACE_EXCLUDES.to_vec());
    let workspace = query
        .host()
        .directory_opts(request.workspace_root.display().to_string(), &opts);

    let rust_target = request.arch.rust_target();
    let registry_cache = query.cache_volume("tokeira-cargo-registry");
    // Per-target cache: aarch64 and x86_64 artifacts never invalidate each
    // other.
    let target_cache = query.cache_volume(format!("tokeira-build-target-{rust_target}"));

    let built_path = format!("target/{rust_target}/release/tokeirad");
    let builder = query
        .container()
        .from(format!("rust:{toolchain}-slim-bookworm"))
        .with_exec(vec![
            "sh",
            "-c",
            "apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev protobuf-compiler libprotobuf-dev ca-certificates && rm -rf /var/lib/apt/lists/*",
        ])
        .with_env_variable("CARGO_TERM_COLOR", "never")
        .with_env_variable("RUSTUP_TOOLCHAIN", &toolchain)
        .with_exec(vec!["rustup", "target", "add", rust_target])
        .with_mounted_cache(registry_cache, "/usr/local/cargo/registry")
        .with_mounted_cache(target_cache, "/app/target")
        .with_directory("/app", workspace)
        .with_workdir("/app")
        .with_exec(vec![
            "cargo",
            "build",
            "--release",
            "--target",
            rust_target,
            "--bin",
            "tokeirad",
            "-p",
            "tokeirad",
        ])
        // The binary must leave the cache mount to survive into the layer.
        .with_exec(vec!["cp", &built_path, "/tokeirad"])
        .with_exec(vec!["strip", "/tokeirad"]);

    let runtime = query
        .container()
        .from("cgr.dev/chainguard/glibc-dynamic:latest")
        .with_file("/usr/local/bin/tokeirad", builder.file("/tokeirad"))
        .with_user("nonroot")
        .with_entrypoint(vec!["/usr/local/bin/tokeirad"]);

    let latest_tag = "tokeirad:latest".to_owned();
    runtime.export_image(&latest_tag).await?;

    let mut tags = vec![latest_tag];
    if let Some(extra_tag) = request.tag.as_deref()
        && extra_tag != "latest"
    {
        let tag = format!("tokeirad:{extra_tag}");
        runtime.export_image(&tag).await?;
        tags.push(tag);
    }

    Ok(TokeiradBuildResult {
        image_name: "tokeirad".to_owned(),
        tags,
        arch: request.arch,
        toolchain_version: toolchain,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::canned_client;

    #[tokio::test]
    async fn build_tokeirad_image_issues_the_arm64_sequence() {
        let workspace = workspace_with_toolchain("1.95");
        let (client, wire) = canned_client().await;
        let request = TokeiradBuildRequest {
            arch: Arch::Arm64,
            tag: None,
            workspace_root: workspace.path().to_path_buf(),
        };

        let result = build_tokeirad_image(&request, &client)
            .await
            .expect("build pipeline");

        assert_eq!(result.image_name, "tokeirad");
        assert_eq!(result.tags, vec!["tokeirad:latest"]);
        assert_eq!(result.toolchain_version, "1.95");

        // Lazy object arguments (the workspace directory, cache volumes, the
        // built binary) resolve through their own id requests, so the whole
        // chain — base image, system deps, cache mounts, cross-target build,
        // cache-mount copy-out, runtime stage, export — is asserted over the
        // full transcript.
        let transcript = wire.transcript();
        for fragment in [
            "rust:1.95-slim-bookworm",
            "apt-get update",
            "tokeira-cargo-registry",
            "tokeira-build-target-aarch64-unknown-linux-gnu",
            "rustup",
            "aarch64-unknown-linux-gnu",
            "cgr.dev/chainguard/glibc-dynamic:latest",
            "nonroot",
            "exportImage",
            "tokeirad:latest",
        ] {
            assert!(
                transcript.contains(fragment),
                "pipeline transcript missing `{fragment}`:\n{transcript}"
            );
        }
    }

    #[tokio::test]
    async fn build_tokeirad_image_exports_extra_tag_after_latest() {
        let workspace = workspace_with_toolchain("1.95");
        let (client, wire) = canned_client().await;
        let request = TokeiradBuildRequest {
            arch: Arch::Arm64,
            tag: Some("v1.2.3".to_owned()),
            workspace_root: workspace.path().to_path_buf(),
        };

        let result = build_tokeirad_image(&request, &client)
            .await
            .expect("build pipeline");

        assert_eq!(
            result.tags,
            vec!["tokeirad:latest".to_owned(), "tokeirad:v1.2.3".to_owned()]
        );
        let exports: Vec<String> = wire
            .requests()
            .into_iter()
            .filter(|query| query.contains("exportImage"))
            .collect();
        assert_eq!(exports.len(), 2, "one export per tag");
        assert!(exports[0].contains("tokeirad:latest"));
        assert!(exports[1].contains("tokeirad:v1.2.3"));
    }

    #[tokio::test]
    async fn build_tokeirad_image_uses_amd64_target() {
        let workspace = workspace_with_toolchain("1.95");
        let (client, wire) = canned_client().await;
        let request = TokeiradBuildRequest {
            arch: Arch::Amd64,
            tag: None,
            workspace_root: workspace.path().to_path_buf(),
        };

        build_tokeirad_image(&request, &client)
            .await
            .expect("build pipeline");

        let transcript = wire.transcript();
        assert!(transcript.contains("x86_64-unknown-linux-gnu"));
        assert!(transcript.contains("tokeira-build-target-x86_64-unknown-linux-gnu"));
    }

    #[tokio::test]
    async fn build_tokeirad_image_excludes_build_outputs_from_workspace_upload() {
        let workspace = workspace_with_toolchain("1.95");
        let (client, wire) = canned_client().await;
        let request = TokeiradBuildRequest {
            arch: Arch::Arm64,
            tag: None,
            workspace_root: workspace.path().to_path_buf(),
        };

        build_tokeirad_image(&request, &client)
            .await
            .expect("build pipeline");

        let upload = wire
            .requests()
            .into_iter()
            .find(|query| query.contains("directory") && query.contains("exclude"))
            .expect("workspace upload with an exclude filter");
        // The key survival invariant: `target/` MUST be excluded or every
        // invocation ships multi-GB of Rust build output to the engine —
        // and `vendor/` MUST NOT be, or the workspace's path dependencies
        // cannot resolve inside it.
        for fragment in ["target", ".git", "artifacts"] {
            assert!(
                upload.contains(fragment),
                "exclude filter missing `{fragment}`:\n{upload}"
            );
        }
        assert!(
            !upload.contains("\"vendor\""),
            "the vendored SDK must ship with the workspace:\n{upload}"
        );
    }

    fn workspace_with_toolchain(version: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("rust-toolchain.toml"),
            format!("[toolchain]\nchannel = \"{version}\"\n"),
        )
        .expect("write toolchain");
        dir
    }
}
