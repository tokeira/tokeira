//! Structural guard for the kernel-purity boundary.
//!
//! The pure kernel (`tokeira-kernel`) must never depend on this crate: a
//! runtime-mutable override read inside the deterministic transition engine
//! would make the same history replay differently, breaking "history is
//! authority". The overridable set is deliberately confined to the
//! runtime/edge/projection planes, which consult overrides live at request
//! time. See `.kiro/specs/conformance-config-override/`.

use std::path::Path;

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
