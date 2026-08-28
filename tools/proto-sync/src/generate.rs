//! Regenerate `tokeira-proto`'s checked-in bindings from the vendored protos.
//!
//! The bindings are committed under `crates/tokeira-proto/src/generated/` so
//! the published crate is self-contained: building it from a registry archive
//! needs neither the repository-root `proto/` tree nor a `protoc` install.
//! Regeneration is a repository-maintenance step (this tool), run whenever the
//! vendored protos or the pinned codegen stack move; CI's clean-tree check
//! fails if a regen was forgotten.
//!
//! Two surfaces, mirroring the crate's module split:
//!
//! - `generated/upstream/` — the Temporal-compatible API (tonic/prost), plus
//!   its reflection descriptor set and the upstream OpenAPI documents copied
//!   verbatim from `proto/upstream/`.
//! - `generated/tokeira/` — Tokeira's own packages: the connect-rust
//!   controller surface (buffa + connect-rust service traits, Tokeira's
//!   external interface), the legacy tonic controller output, and the
//!   provider-neutral compute contract.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use walkdir::WalkDir;

pub(crate) fn run(workspace_root: &Path) -> Result<()> {
    let proto_root = workspace_root.join("proto");
    let upstream_dir = proto_root.join("upstream");
    let internal_dir = proto_root.join("tokeira");
    let compute_dir = internal_dir.join("compute");
    let controller_dir = internal_dir.join("internal/controller");

    let generated_root = workspace_root.join("crates/tokeira-proto/src/generated");
    let upstream_out = generated_root.join("upstream");
    let tokeira_out = generated_root.join("tokeira");

    for dir in [&upstream_out, &tokeira_out] {
        if dir.exists() {
            fs::remove_dir_all(dir).with_context(|| format!("remove {}", dir.display()))?;
        }
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    }

    // ── Temporal surface (upstream API) — tonic/prost ───────────────────────
    let upstream_protos = discover_protos(&upstream_dir)?;
    if upstream_protos.is_empty() {
        bail!("no vendored protos under {}", upstream_dir.display());
    }
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .btree_map(["."])
        .out_dir(&upstream_out)
        .file_descriptor_set_path(upstream_out.join("tokeira_public_descriptor.bin"))
        .compile(&upstream_protos, &[upstream_dir.as_path()])
        .context("compile upstream Temporal protos")?;

    // ── Tokeira surface: internal packages — tonic/prost (legacy) ───────────
    // The internal packages import nothing from `temporal.api`, so this pass
    // emits only `tokeira.*` files; the assertion below fails the regen if a
    // future import breaks that assumption (prost would then emit a transitive
    // subset of the Temporal packages here, shadowing the complete upstream
    // output at include time).
    let mut internal_protos = discover_protos(&internal_dir)?;
    internal_protos.retain(|path| !path.starts_with(&compute_dir));
    if !internal_protos.is_empty() {
        tonic_build::configure()
            .build_client(true)
            .build_server(true)
            .btree_map(["."])
            .out_dir(&tokeira_out)
            .file_descriptor_set_path(tokeira_out.join("tokeira_internal_descriptor.bin"))
            .compile(
                &internal_protos,
                &[proto_root.as_path(), upstream_dir.as_path()],
            )
            .context("compile Tokeira internal protos")?;
    }

    // ── Tokeira surface: compute provider contract — tonic/prost ────────────
    // Imports Temporal Payload; the extern mapping resolves those references
    // into the upstream surface instead of regenerating the packages here.
    let compute_protos = discover_protos(&compute_dir)?;
    if !compute_protos.is_empty() {
        tonic_build::configure()
            .build_client(false)
            .build_server(false)
            .btree_map(["."])
            .out_dir(&tokeira_out)
            .extern_path(".temporal.api", "crate::public::temporal::api")
            .compile(
                &compute_protos,
                &[proto_root.as_path(), upstream_dir.as_path()],
            )
            .context("compile Tokeira compute protos")?;
    }

    for entry in
        fs::read_dir(&tokeira_out).with_context(|| format!("list {}", tokeira_out.display()))?
    {
        let name = entry?.file_name();
        let name = name.to_string_lossy().into_owned();
        if name.starts_with("temporal.") {
            bail!(
                "internal codegen emitted `{name}`: a Tokeira package now imports `temporal.api` \
                 without an extern mapping, which would shadow the upstream surface"
            );
        }
    }

    // ── Tokeira surface: controller — connect-rust (buffa + service stubs) ──
    // Tokeira's external interface. The explicit out_dir makes connectrpc-build
    // emit sibling-relative `include!` paths suited to checked-in code.
    let controller_protos = discover_protos(&controller_dir)?;
    if !controller_protos.is_empty() {
        connectrpc_build::Config::new()
            .out_dir(&tokeira_out)
            .files(
                &controller_protos
                    .iter()
                    .map(|p| p.to_str().expect("proto path is valid UTF-8"))
                    .collect::<Vec<_>>(),
            )
            .includes(&[proto_root.to_str().expect("proto root is valid UTF-8")])
            .include_file("_connectrpc_controller.rs")
            .compile()
            .context("compile Tokeira controller connect-rust bindings")?;
    }

    // ── Prune outputs the crate never includes ──────────────────────────────
    // The passes compile whole proto trees, so they also emit packages
    // `tokeira-proto` deliberately does not expose (and, for the compatibility
    // and conformance packages, that other crates own through their own
    // codegen). Deleting them keeps the published archive free of dead
    // generated code; if the crate starts including one of these, remove it
    // from this list.
    let pruned = [
        upstream_out.join("google.api.rs"),
        upstream_out.join("temporal.api.nexusservices.workerservice.v1.rs"),
        tokeira_out.join("tokeira.compatibility.v1.rs"),
        tokeira_out.join("tokeira.conformance.v1.rs"),
    ];
    for path in pruned {
        fs::remove_file(&path).with_context(|| format!("prune {}", path.display()))?;
    }

    // ── Service protos vendored into tokeira-compatibility ──────────────────
    // The coverage layer slices these at compile time and the published archive
    // cannot reach the repository proto tree; the crate carries verbatim copies,
    // refreshed here and parity-tested against `proto/upstream/` in that crate.
    let compat_data = workspace_root.join("crates/tokeira-compatibility/data");
    for (from, to) in [
        (
            "temporal/api/workflowservice/v1/service.proto",
            "workflowservice.service.proto",
        ),
        (
            "temporal/api/operatorservice/v1/service.proto",
            "operatorservice.service.proto",
        ),
    ] {
        let from = upstream_dir.join(from);
        let to = compat_data.join(to);
        fs::copy(&from, &to)
            .with_context(|| format!("copy {} to {}", from.display(), to.display()))?;
    }

    // ── Upstream OpenAPI documents, copied verbatim ─────────────────────────
    let openapi_out = upstream_out.join("openapi");
    fs::create_dir_all(&openapi_out)
        .with_context(|| format!("create {}", openapi_out.display()))?;
    for name in ["openapiv2.swagger.json", "openapiv3.yaml"] {
        let from = upstream_dir.join("temporalproto/openapi").join(name);
        let to = openapi_out.join(name);
        fs::copy(&from, &to)
            .with_context(|| format!("copy {} to {}", from.display(), to.display()))?;
    }

    println!(
        "regenerated tokeira-proto bindings into {}",
        generated_root.display()
    );
    Ok(())
}

fn discover_protos(root: &Path) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut protos = WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "proto"))
        .map(|entry| entry.into_path())
        .collect::<Vec<_>>();
    protos.sort();
    Ok(protos)
}
