use std::path::PathBuf;

use crate::{Arch, BuildError, DaggerClient, rust_toolchain_version};

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

pub fn build_tokeirad_image(
    request: &TokeiradBuildRequest,
    dagger: &dyn DaggerClient,
) -> Result<TokeiradBuildResult, BuildError> {
    let toolchain = rust_toolchain_version(&request.workspace_root)?;
    let workspace = dagger.host_directory(&request.workspace_root)?;

    let chef_base_image = format!("rust:{toolchain}-slim-bookworm");
    let mut chef = dagger.container_from(&chef_base_image)?;
    chef = chef.with_exec(&[
        "sh",
        "-c",
        "apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev protobuf-compiler ca-certificates && rm -rf /var/lib/apt/lists/*",
    ])?;
    chef = chef.with_exec(&["cargo", "install", "cargo-chef", "--locked"])?;
    chef = chef.with_workdir("/app")?;
    chef = chef.with_env("CARGO_TERM_COLOR", "never")?;
    chef = chef.with_env("RUSTUP_TOOLCHAIN", &toolchain)?;
    chef = chef.with_exec(&["rustup", "target", "add", request.arch.rust_target()])?;

    let mut planner = chef.clone_ref()?;
    planner = planner.with_directory("/app", &*workspace)?;
    planner = planner.with_exec(&["cargo", "chef", "prepare", "--recipe-path", "recipe.json"])?;
    let recipe = planner.file("/app/recipe.json")?;

    let mut cacher = chef.clone_ref()?;
    cacher = cacher.with_file("/app/recipe.json", &*recipe)?;
    cacher = cacher.with_exec(&[
        "cargo",
        "chef",
        "cook",
        "--release",
        "--target",
        request.arch.rust_target(),
        "--bin",
        "tokeirad",
        "--recipe-path",
        "recipe.json",
    ])?;

    let mut builder = cacher;
    builder = builder.with_directory("/app", &*workspace)?;
    builder = builder.with_exec(&[
        "cargo",
        "build",
        "--release",
        "--target",
        request.arch.rust_target(),
        "--bin",
        "tokeirad",
        "-p",
        "tokeirad",
    ])?;
    builder = builder.with_exec(&[
        "strip",
        &format!(
            "/app/target/{}/release/tokeirad",
            request.arch.rust_target()
        ),
    ])?;
    let binary = builder.file(&format!(
        "/app/target/{}/release/tokeirad",
        request.arch.rust_target()
    ))?;

    let mut runtime = dagger.container_from("cgr.dev/chainguard/glibc-dynamic:latest")?;
    runtime = runtime.with_file("/usr/local/bin/tokeirad", &*binary)?;
    runtime = runtime.with_user("nonroot")?;
    runtime = runtime.with_entrypoint(&["/usr/local/bin/tokeirad"])?;

    let latest_tag = "tokeirad:latest".to_owned();
    runtime.export_image(&latest_tag)?;

    let mut tags = vec![latest_tag];
    if let Some(extra_tag) = request.tag.as_deref()
        && extra_tag != "latest"
    {
        let tag = format!("tokeirad:{extra_tag}");
        runtime.export_image(&tag)?;
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
    use crate::testing::{MockCall, MockDaggerClient};

    #[test]
    fn build_tokeirad_image_records_arm64_sequence() {
        let workspace = workspace_with_toolchain("1.95");
        let mock = MockDaggerClient::default();
        let request = TokeiradBuildRequest {
            arch: Arch::Arm64,
            tag: None,
            workspace_root: workspace.path().to_path_buf(),
        };

        let result = build_tokeirad_image(&request, &mock).expect("build pipeline");

        assert_eq!(result.image_name, "tokeirad");
        assert_eq!(result.tags, vec!["tokeirad:latest"]);
        let calls = mock.calls();
        assert_call_order(
            &calls,
            &[
                MockCall::ContainerFrom("rust:1.95-slim-bookworm".to_owned()),
                MockCall::WithExec(vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    "apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev protobuf-compiler ca-certificates && rm -rf /var/lib/apt/lists/*".to_owned(),
                ]),
                MockCall::WithExec(vec![
                    "cargo".to_owned(),
                    "install".to_owned(),
                    "cargo-chef".to_owned(),
                    "--locked".to_owned(),
                ]),
                MockCall::WithExec(vec![
                    "rustup".to_owned(),
                    "target".to_owned(),
                    "add".to_owned(),
                    "aarch64-unknown-linux-gnu".to_owned(),
                ]),
                MockCall::WithExec(vec![
                    "cargo".to_owned(),
                    "chef".to_owned(),
                    "prepare".to_owned(),
                    "--recipe-path".to_owned(),
                    "recipe.json".to_owned(),
                ]),
                MockCall::WithExec(vec![
                    "cargo".to_owned(),
                    "chef".to_owned(),
                    "cook".to_owned(),
                    "--release".to_owned(),
                    "--target".to_owned(),
                    "aarch64-unknown-linux-gnu".to_owned(),
                    "--bin".to_owned(),
                    "tokeirad".to_owned(),
                    "--recipe-path".to_owned(),
                    "recipe.json".to_owned(),
                ]),
                MockCall::WithExec(vec![
                    "cargo".to_owned(),
                    "build".to_owned(),
                    "--release".to_owned(),
                    "--target".to_owned(),
                    "aarch64-unknown-linux-gnu".to_owned(),
                    "--bin".to_owned(),
                    "tokeirad".to_owned(),
                    "-p".to_owned(),
                    "tokeirad".to_owned(),
                ]),
                MockCall::WithExec(vec![
                    "strip".to_owned(),
                    "/app/target/aarch64-unknown-linux-gnu/release/tokeirad".to_owned(),
                ]),
                MockCall::ContainerFrom("cgr.dev/chainguard/glibc-dynamic:latest".to_owned()),
                MockCall::WithUser("nonroot".to_owned()),
                MockCall::WithEntrypoint(vec!["/usr/local/bin/tokeirad".to_owned()]),
                MockCall::ExportImage("tokeirad:latest".to_owned()),
            ],
        );
        assert_eq!(export_count(&calls), 1);
    }

    #[test]
    fn build_tokeirad_image_exports_extra_tag_after_latest() {
        let workspace = workspace_with_toolchain("1.95");
        let mock = MockDaggerClient::default();
        let request = TokeiradBuildRequest {
            arch: Arch::Arm64,
            tag: Some("v1.2.3".to_owned()),
            workspace_root: workspace.path().to_path_buf(),
        };

        let result = build_tokeirad_image(&request, &mock).expect("build pipeline");

        assert_eq!(
            result.tags,
            vec!["tokeirad:latest".to_owned(), "tokeirad:v1.2.3".to_owned()]
        );
        let exports: Vec<_> = mock
            .calls()
            .into_iter()
            .filter(|call| matches!(call, MockCall::ExportImage(_)))
            .collect();
        assert_eq!(
            exports,
            vec![
                MockCall::ExportImage("tokeirad:latest".to_owned()),
                MockCall::ExportImage("tokeirad:v1.2.3".to_owned()),
            ]
        );
    }

    #[test]
    fn build_tokeirad_image_uses_amd64_target() {
        let workspace = workspace_with_toolchain("1.95");
        let mock = MockDaggerClient::default();
        let request = TokeiradBuildRequest {
            arch: Arch::Amd64,
            tag: None,
            workspace_root: workspace.path().to_path_buf(),
        };

        build_tokeirad_image(&request, &mock).expect("build pipeline");

        assert!(mock.calls().contains(&MockCall::WithExec(vec![
            "rustup".to_owned(),
            "target".to_owned(),
            "add".to_owned(),
            "x86_64-unknown-linux-gnu".to_owned(),
        ])));
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

    fn assert_call_order(calls: &[MockCall], expected: &[MockCall]) {
        let mut next = 0;
        for call in calls {
            if expected.get(next) == Some(call) {
                next += 1;
            }
        }
        assert_eq!(next, expected.len(), "missing ordered calls in {calls:?}");
    }

    fn export_count(calls: &[MockCall]) -> usize {
        calls
            .iter()
            .filter(|call| matches!(call, MockCall::ExportImage(_)))
            .count()
    }
}
