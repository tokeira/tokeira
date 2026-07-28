//! Structural guards for pure-kernel and admission-policy boundaries.
//!
//! The pure kernel (`tokeira-kernel`) must never depend on this crate: a
//! runtime-mutable override read inside the deterministic transition engine
//! would make the same history replay differently, breaking "history is
//! authority". The overridable set is deliberately confined to the
//! runtime/edge/projection planes, which consult overrides live at request
//! time. See `.kiro/specs/conformance-config-override/`.
//!
//! The worker-compute controller is likewise an effectful runtime/control-plane
//! feature. Its durable outbox and Nexus provider calls must not add commands,
//! fields, or serialized state to the kernel, and its provider request is not a
//! worker authorization grant. See `.kiro/specs/worker-compute-controller/`.

use std::{
    fs,
    path::{Path, PathBuf},
};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("conformance crate is under the workspace crates directory")
        .to_path_buf()
}

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    let mut sources = Vec::new();
    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory)
            .unwrap_or_else(|err| panic!("read {}: {err}", directory.display()));
        for entry in entries {
            let path = entry.expect("kernel directory entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                sources.push(path);
            }
        }
    }
    sources.sort();
    sources
}

/// Feature: conformance-config-override, Property 5: kernel purity and replay determinism.
///
/// `tokeira-kernel`'s manifest names neither `tokeira-conformance` nor
/// `tokeira-auth` under any feature. That is the structural proxy for "the
/// kernel cannot read mutable policy or an override registry". Paired with the
/// unchanged `Kernel::apply` signature (no config parameter), this keeps kernel
/// transitions a pure function of their inputs.
#[test]
fn kernel_does_not_depend_on_conformance_registry() {
    // `CARGO_MANIFEST_DIR` is this crate (`crates/tokeira-conformance`) at
    // compile time, so the kernel manifest is a stable sibling path — not
    // CWD-dependent.
    let kernel_manifest = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates directory")
        .join("tokeira-kernel")
        .join("Cargo.toml");

    let contents = std::fs::read_to_string(&kernel_manifest)
        .unwrap_or_else(|err| panic!("read {}: {err}", kernel_manifest.display()));

    assert!(
        !contents.contains("tokeira-conformance"),
        "tokeira-kernel must not depend on tokeira-conformance; a runtime-mutable \
         override read inside the pure kernel would break replay determinism (Property 5)"
    );
    assert!(
        !contents.contains("tokeira-auth"),
        "tokeira-kernel must not depend on tokeira-auth; authentication and authorization \
         are admission concerns and cannot influence replay of an accepted command"
    );
}

/// Feature: worker-compute-controller, kernel and authorization boundaries.
///
/// Worker compute remains absent from every kernel source file, so it cannot add a
/// command, state field, transition, or postcard-serialized kernel value. The
/// effectful runtime also has no authorization dependency and the provider request
/// schema has no credential/grant field; guest-worker access remains owned by the
/// separate `scoped-worker-authorization` feature.
#[test]
fn worker_compute_stays_outside_kernel_and_authorization() {
    let root = repository_root();
    let kernel = root.join("crates/tokeira-kernel");
    for source in rust_sources(&kernel.join("src")) {
        let contents = fs::read_to_string(&source)
            .unwrap_or_else(|err| panic!("read {}: {err}", source.display()));
        for forbidden in [
            "worker_compute",
            "WorkerCompute",
            "InvokeWorker",
            "ComputeConfig",
        ] {
            assert!(
                !contents.contains(forbidden),
                "{} contains forbidden worker-compute kernel surface {forbidden}",
                source.display()
            );
        }
    }

    let runtime_manifest = fs::read_to_string(root.join("crates/tokeira-runtime/Cargo.toml"))
        .expect("read runtime manifest");
    assert!(
        !runtime_manifest.contains("tokeira-auth"),
        "worker-compute runtime must not issue or consume worker authorization grants"
    );

    let provider_proto = fs::read_to_string(root.join("proto/tokeira/compute/v1/provider.proto"))
        .expect("read worker-compute provider contract");
    let request = provider_proto
        .split_once("message InvokeWorkerRequest {")
        .and_then(|(_, suffix)| suffix.split_once("\n}"))
        .map(|(body, _)| body.to_ascii_lowercase())
        .expect("InvokeWorkerRequest declaration");
    for forbidden in [
        "authorization",
        "bearer",
        "credential",
        "jwt",
        "sts",
        "task_token",
    ] {
        assert!(
            !request.contains(forbidden),
            "provider request must not contain worker access field {forbidden}"
        );
    }
}
