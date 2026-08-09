//! Closure resolution against this repository's real workspace: the resolved
//! set must contain the provisioner stack and the workspace build files, and
//! the canonical lock serialization must be deterministic.

use std::path::{Path, PathBuf};

use tokeira_build::resolve_source_closure;

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/tokeira-build sits two levels below the workspace root")
        .to_path_buf()
}

#[test]
fn compose_platform_closure_contains_the_platform_stack() {
    let closure = resolve_source_closure(&workspace_root(), "tokeira-compose-deployment")
        .expect("closure resolves");

    let dirs: Vec<String> = closure
        .crate_dirs
        .iter()
        .map(|d| d.to_string_lossy().into_owned())
        .collect();
    for expected in [
        "platforms/compose",
        "crates/tokeira-compose",
        "crates/tokeira-aws",
        "crates/tokeira-platform",
    ] {
        assert!(
            dirs.iter().any(|d| d == expected),
            "closure must reach {expected}; got {dirs:?}"
        );
    }
    assert!(
        !dirs.iter().any(|d| d == "apps/tkr"),
        "tkr is not in the platform closure — a tkr-only change must not re-key the engine"
    );
    // The shell is deliberately NOT in a platform's closure: the generated
    // root marries platform and shell as separate dependency roots, so a
    // shell-only change re-keys through the generated closure, never
    // through the platform's.
    assert!(
        !dirs.iter().any(|d| d == "crates/tokeira-provisioner-cli"),
        "the shell must not enter a platform's closure; got {dirs:?}"
    );

    let files: Vec<String> = closure
        .workspace_files
        .iter()
        .map(|f| f.to_string_lossy().into_owned())
        .collect();
    assert!(files.iter().any(|f| f == "Cargo.toml"));
    assert!(files.iter().any(|f| f == "Cargo.lock"));

    assert!(
        closure.locked.iter().any(|d| d.name == "serde"),
        "the locked set carries reachable third-party deps"
    );
    assert!(
        closure.locked.iter().all(|d| d.source.is_none()
            || d.checksum.is_some()
            || d.source
                .as_deref()
                .map(|s| s.starts_with("git"))
                .unwrap_or(false)),
        "registry deps carry their Cargo.lock checksum"
    );
}

#[test]
fn canonical_lock_bytes_are_deterministic() {
    let root = workspace_root();
    let a = resolve_source_closure(&root, "tokeira-compose-deployment").expect("closure");
    let b = resolve_source_closure(&root, "tokeira-compose-deployment").expect("closure");
    assert_eq!(a.canonical_lock_bytes(), b.canonical_lock_bytes());
    assert!(
        a.canonical_lock_bytes()
            .starts_with(b"tokeira-lock-closure/v1\n")
    );
}

#[test]
fn an_unknown_seed_is_refused() {
    let err = resolve_source_closure(&workspace_root(), "no-such-package")
        .expect_err("unknown seed refuses");
    assert!(err.to_string().contains("no-such-package"));
}
