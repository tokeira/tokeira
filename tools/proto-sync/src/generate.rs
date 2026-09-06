//! Regenerate `tokeira-proto`'s checked-in bindings from the vendored protos.
//!
//! The bindings are committed under `crates/tokeira-proto/src/generated/` so
//! the published crate is self-contained: building it from a registry archive
//! needs neither the repository-root `proto/` tree nor a proto compiler.
//! Regeneration is a repository-maintenance step (this tool), run whenever the
//! vendored protos or the pinned codegen stack move; `proto-sync check`
//! regenerates into a scratch directory and fails if the tree would change.
//!
//! The protos are compiled by `protox`, a pure-Rust compiler. Its encoded
//! descriptor set keeps extension options, which is what the HTTP API's
//! `google.api.http` routes are read from at runtime; the `prost_types`
//! representation cannot carry extension fields, so it is produced from those
//! bytes only to drive `tonic-prost-build`, which needs no custom options.
//! Imports and source info are included, which is what `protoc` produced for
//! the previous generator, so the descriptor sets keep their content and the
//! generated code keeps its documentation comments.
//!
//! Two surfaces, mirroring the crate's module split:
//!
//! - `generated/upstream/` — the Temporal-compatible API (tonic/prost), plus
//!   its reflection descriptor set and the upstream OpenAPI documents copied
//!   verbatim from `proto/upstream/`.
//! - `generated/tokeira/` — Tokeira's own packages: the connect-rust
//!   controller surface (buffa + connect-rust service traits, Tokeira's
//!   external interface), the tonic controller output, and the
//!   provider-neutral compute contract.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};
use prost::Message as _;
use prost_types::FileDescriptorSet;
use walkdir::WalkDir;

/// Where a generation run writes: the bindings tree and the compatibility
/// crate's verbatim service-proto copies.
struct Outputs {
    generated_root: PathBuf,
    compat_data: PathBuf,
}

impl Outputs {
    fn checked_in(workspace_root: &Path) -> Self {
        Self {
            generated_root: workspace_root.join("crates/tokeira-proto/src/generated"),
            compat_data: workspace_root.join("crates/tokeira-compatibility/data"),
        }
    }

    fn scratch(root: &Path) -> Self {
        Self {
            generated_root: root.join("generated"),
            compat_data: root.join("compat-data"),
        }
    }
}

/// Regenerate the checked-in bindings in place.
pub(crate) fn run(workspace_root: &Path) -> Result<()> {
    let outputs = Outputs::checked_in(workspace_root);
    generate(workspace_root, &outputs)?;
    println!(
        "regenerated tokeira-proto bindings into {}",
        outputs.generated_root.display()
    );
    Ok(())
}

/// Regenerate into a scratch directory and fail if the checked-in tree differs.
pub(crate) fn check(workspace_root: &Path) -> Result<()> {
    let scratch_root =
        std::env::temp_dir().join(format!("proto-sync-check-{}", std::process::id()));
    if scratch_root.exists() {
        fs::remove_dir_all(&scratch_root)
            .with_context(|| format!("clear {}", scratch_root.display()))?;
    }
    let scratch = Outputs::scratch(&scratch_root);
    let checked_in = Outputs::checked_in(workspace_root);
    let result = generate(workspace_root, &scratch).and_then(|()| {
        compare_trees(&checked_in.generated_root, &scratch.generated_root)?;
        for name in COMPAT_COPIES.iter().map(|(_, to)| *to) {
            compare_files(
                &checked_in.compat_data.join(name),
                &scratch.compat_data.join(name),
            )?;
        }
        Ok(())
    });
    let _ = fs::remove_dir_all(&scratch_root);
    result
}

const COMPAT_COPIES: [(&str, &str); 2] = [
    (
        "temporal/api/workflowservice/v1/service.proto",
        "workflowservice.service.proto",
    ),
    (
        "temporal/api/operatorservice/v1/service.proto",
        "operatorservice.service.proto",
    ),
];

/// A compiled proto tree: the encoded descriptor set with its extension
/// options intact, and the `prost_types` view code generation consumes.
struct Compiled {
    encoded: Vec<u8>,
    set: FileDescriptorSet,
}

/// Compile `protos` against `includes` with imports and source info included.
fn compile(protos: &[PathBuf], includes: &[&Path]) -> Result<Compiled> {
    let mut compiler = protox::Compiler::new(includes)?;
    compiler
        .include_source_info(true)
        .include_imports(true)
        .open_files(protos)?;
    let encoded = compiler.encode_file_descriptor_set();
    let set = FileDescriptorSet::decode(encoded.as_slice())
        .context("decode the compiled descriptor set")?;
    Ok(Compiled { encoded, set })
}

fn generate(workspace_root: &Path, outputs: &Outputs) -> Result<()> {
    let proto_root = workspace_root.join("proto");
    let upstream_dir = proto_root.join("upstream");
    let internal_dir = proto_root.join("tokeira");
    let compute_dir = internal_dir.join("compute");
    let controller_dir = internal_dir.join("internal/controller");

    let upstream_out = outputs.generated_root.join("upstream");
    let tokeira_out = outputs.generated_root.join("tokeira");

    for dir in [&upstream_out, &tokeira_out] {
        if dir.exists() {
            fs::remove_dir_all(dir).with_context(|| format!("remove {}", dir.display()))?;
        }
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    }
    fs::create_dir_all(&outputs.compat_data)
        .with_context(|| format!("create {}", outputs.compat_data.display()))?;

    // ── Temporal surface (upstream API) — tonic/prost ───────────────────────
    let upstream_protos = discover_protos(&upstream_dir)?;
    if upstream_protos.is_empty() {
        bail!("no vendored protos under {}", upstream_dir.display());
    }
    let upstream = compile(&upstream_protos, &[upstream_dir.as_path()])
        .context("compile upstream Temporal protos")?;
    write_descriptor_set(
        &upstream_out.join("tokeira_public_descriptor.bin"),
        &upstream.encoded,
    )?;
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .btree_map(".")
        .emit_rerun_if_changed(false)
        .out_dir(&upstream_out)
        .compile_fds(upstream.set)
        .context("generate upstream Temporal bindings")?;

    // ── Tokeira surface: internal packages — tonic/prost ────────────────────
    // The internal packages import nothing from `temporal.api`, so this pass
    // emits only `tokeira.*` files; the assertion below fails the regen if a
    // future import breaks that assumption (prost would then emit a transitive
    // subset of the Temporal packages here, shadowing the complete upstream
    // output at include time).
    let mut internal_protos = discover_protos(&internal_dir)?;
    internal_protos.retain(|path| !path.starts_with(&compute_dir));
    if !internal_protos.is_empty() {
        let internal = compile(
            &internal_protos,
            &[proto_root.as_path(), upstream_dir.as_path()],
        )
        .context("compile Tokeira internal protos")?;
        write_descriptor_set(
            &tokeira_out.join("tokeira_internal_descriptor.bin"),
            &internal.encoded,
        )?;
        tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .btree_map(".")
            .emit_rerun_if_changed(false)
            .out_dir(&tokeira_out)
            .compile_fds(internal.set)
            .context("generate Tokeira internal bindings")?;
    }

    // ── Tokeira surface: compute provider contract — tonic/prost ────────────
    // Imports Temporal Payload; the extern mapping resolves those references
    // into the upstream surface instead of regenerating the packages here.
    let compute_protos = discover_protos(&compute_dir)?;
    if !compute_protos.is_empty() {
        let compute = compile(
            &compute_protos,
            &[proto_root.as_path(), upstream_dir.as_path()],
        )
        .context("compile Tokeira compute protos")?;
        tonic_prost_build::configure()
            .build_client(false)
            .build_server(false)
            .btree_map(".")
            .emit_rerun_if_changed(false)
            .out_dir(&tokeira_out)
            .extern_path(".temporal.api", "crate::public::temporal::api")
            .compile_fds(compute.set)
            .context("generate Tokeira compute bindings")?;
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
    for (from, to) in COMPAT_COPIES {
        let from = upstream_dir.join(from);
        let to = outputs.compat_data.join(to);
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

    Ok(())
}

fn write_descriptor_set(path: &Path, encoded: &[u8]) -> Result<()> {
    fs::write(path, encoded).with_context(|| format!("write {}", path.display()))
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

/// Every file under `expected` exists under `actual` with identical bytes,
/// and `actual` carries no file `expected` lacks.
fn compare_trees(expected: &Path, actual: &Path) -> Result<()> {
    let expected_files = relative_files(expected)?;
    let actual_files = relative_files(actual)?;
    let missing: Vec<_> = expected_files
        .iter()
        .filter(|file| !actual_files.contains(file))
        .collect();
    let extra: Vec<_> = actual_files
        .iter()
        .filter(|file| !expected_files.contains(file))
        .collect();
    ensure!(
        missing.is_empty() && extra.is_empty(),
        "generated tree differs from {}: missing {missing:?}, unexpected {extra:?}",
        expected.display()
    );
    for file in &expected_files {
        compare_files(&expected.join(file), &actual.join(file))?;
    }
    Ok(())
}

fn compare_files(expected: &Path, actual: &Path) -> Result<()> {
    let expected_bytes =
        fs::read(expected).with_context(|| format!("read {}", expected.display()))?;
    let actual_bytes = fs::read(actual).with_context(|| format!("read {}", actual.display()))?;
    ensure!(
        expected_bytes == actual_bytes,
        "{} is not what the pinned generator produces; run `cargo run -p proto-sync -- generate`",
        expected.display()
    );
    Ok(())
}

fn relative_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| {
            entry
                .path()
                .strip_prefix(root)
                .map(Path::to_path_buf)
                .context("walked path lies under its root")
        })
        .collect::<Result<Vec<_>>>()?;
    files.sort();
    Ok(files)
}
