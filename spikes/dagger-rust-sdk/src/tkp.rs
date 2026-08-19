//! The bound-provisioner (tkp) build, SDK-idiomatically.
//!
//! Parity target: `tokeira-build/src/pipelines/provisioner.rs`, with the
//! same honest inputs — the real assembled bound source over the real
//! frozen snapshot — and the same purity stance: **deliberately cold, no
//! cache mounts** (a build that mounts one is not pure w.r.t. its declared
//! inputs). What changes is the client: async end to end, typed errors,
//! captured test output, and a straight `File::export`.

use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use sha2::Digest as _;
use tokeira_build::{discover_workspace_descriptors, snapshot_source_closure};

pub(crate) async fn build(build_image: Option<&str>) -> Result<()> {
    let workspace = crate::workspace_root()?;
    let toolchain = crate::toolchain(&workspace)?;
    // A spike accepts a floating default; production keeps the digest-pin
    // gate (`engine_identity_for` refuses floating tags before any engine
    // call — that validation is untouched by the client swap).
    let build_image = build_image
        .map(String::from)
        .unwrap_or(format!("rust:{toolchain}-slim-bookworm"));
    let total = Instant::now();

    // The real inputs, via the same machinery as the production pipeline.
    let descriptors = discover_workspace_descriptors(&workspace)?;
    let platform = descriptors
        .platforms
        .iter()
        .find(|platform| platform.id.as_str() == "compose")
        .ok_or_else(|| anyhow!("compose platform not discovered"))?;
    let frontend = descriptors
        .frontends
        .iter()
        .find(|frontend| frontend.format.as_str() == "tkd")
        .ok_or_else(|| anyhow!("tkd frontend not discovered"))?;
    let bound_source = tokeira_build::assemble_bound_provisioner(&workspace, platform, frontend)?;
    // The scoped-workspace snapshot request: the frozen tree is a complete,
    // valid cargo workspace (closure members + the generated root, with the
    // cargo-authored scoped lock) — the same request the production pipeline
    // freezes.
    let snapshot = snapshot_source_closure(&bound_source.snapshot_request(&workspace)?)?;
    let staging = tempfile_dir()?;
    let source_dir = staging.path().join("source");
    tokeira_build::materialize_snapshot(&workspace, &snapshot.tree_oid, &source_dir)?;
    bound_source.materialize_in(&source_dir)?;
    println!(
        "assembled + frozen source (tree {}, {} files)",
        &snapshot.tree_oid[..12],
        snapshot.file_count
    );

    let client = dagger_sdk::connect().await.context("connect")?;
    let query = client.query();
    let source = query.host().directory(source_dir.display().to_string());

    let base = query
        .container()
        .from(&build_image)
        .with_exec(vec![
            "sh",
            "-c",
            "apt-get update && apt-get install -y --no-install-recommends \
             pkg-config libssl-dev protobuf-compiler libprotobuf-dev ca-certificates \
             && rm -rf /var/lib/apt/lists/*",
        ])
        .with_env_variable("CARGO_TERM_COLOR", "never")
        .with_env_variable("RUSTUP_TOOLCHAIN", &toolchain)
        .with_workdir("/src")
        .with_directory("/src", source);

    // Test step, production shape: the closure's own crates, `--locked`, at
    // the snapshot root. This is the step the original spike proved
    // impossible against a torn snapshot — the scoped-workspace freeze makes
    // it real, and running it here validates that fix hermetically.
    let tests = Instant::now();
    let mut test_args: Vec<String> = vec!["cargo".into(), "test".into(), "--locked".into()];
    for name in &bound_source.closure().crate_names {
        test_args.push("-p".into());
        test_args.push(name.clone());
    }
    let test_container = base
        .clone()
        .with_exec(test_args.iter().map(String::as_str).collect());
    let test_output = match test_container.stdout().await {
        Ok(output) => output,
        Err(error) => {
            // The typed exec error carries the container's own output —
            // the diagnosis the old client could never produce. Both
            // streams: cargo narrates compilation on stderr but the test
            // harness reports names and panics on stdout.
            if let dagger_sdk::QueryError::Exec { error: exec, .. } = &error {
                let tail = |stream: Option<&str>| -> String {
                    stream
                        .unwrap_or("<not captured>")
                        .lines()
                        .rev()
                        .take(60)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect::<Vec<_>>()
                        .join("\n")
                };
                eprintln!(
                    "test step failed (exit {:?}, command {:?}):\n--- stderr tail:\n{}\n--- stdout tail:\n{}",
                    exec.exit_code(),
                    exec.command(),
                    tail(exec.stderr()),
                    tail(exec.stdout()),
                );
            }
            return Err(error).context("test step");
        }
    };
    println!(
        "tests passed in {:?} ({} bytes of output, capturable at last)",
        tests.elapsed(),
        test_output.len()
    );

    // One native-target build (production cross-builds per target; the
    // client mechanics under evaluation are identical).
    let compile = Instant::now();
    let built = base
        .with_exec(vec![
            "cargo",
            "build",
            "--locked",
            "--release",
            "-p",
            "tokeira-bound-provisioner",
        ])
        .with_exec(vec![
            "sh",
            "-c",
            "cp /src/target/release/tkp /tkp && strip /tkp",
        ]);
    let out_dir = tempfile_dir()?;
    let out_path = out_dir.path().join("tkp");
    built
        .file("/tkp")
        .export(out_path.display().to_string())
        .await
        .context("export tkp")?;
    let bytes = std::fs::read(&out_path)?;
    let digest = hex::encode(sha2::Sha256::digest(&bytes));
    println!(
        "tkp built + exported in {:?} — {} bytes, sha256 {}…",
        compile.elapsed(),
        bytes.len(),
        &digest[..12]
    );

    client.close().await.context("close")?;
    println!("tkp build complete in {:?}", total.elapsed());
    Ok(())
}

fn tempfile_dir() -> Result<tempfile::TempDir> {
    tempfile::tempdir().map_err(Into::into)
}
