//! The staged scoped workspace against the *real* repository: the scoped
//! lock must be exact — cargo re-resolving the staged workspace must neither
//! add nor prune a single entry, or `--locked` builds refuse. A toy fixture
//! cannot prove this (its closure is trivially exact); only the full closure
//! exercises dev-dependencies, feature unification, and multi-version
//! third-party graphs.

use std::{path::PathBuf, process::Command};

use tokeira_build::{assemble_bound_provisioner, discover_workspace_descriptors};

#[test]
fn staged_scoped_lock_is_exact_for_the_real_compose_closure() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let descriptors = discover_workspace_descriptors(&workspace).expect("workspace discovery");
    let platform = descriptors
        .platforms
        .iter()
        .find(|platform| platform.id.as_str() == "compose")
        .expect("compose platform discovered");
    let frontend = descriptors
        .frontends
        .iter()
        .find(|frontend| frontend.format.as_str() == "tkd")
        .expect("tkd frontend discovered");
    let source =
        assemble_bound_provisioner(&workspace, platform, frontend).expect("bound source assembles");

    let staging = tempfile::tempdir().expect("staging dir");
    source
        .stage_native_workspace(&workspace, staging.path())
        .expect("stage the scoped workspace");
    let staged_lock = staging.path().join("Cargo.lock");
    let before = std::fs::read_to_string(&staged_lock).expect("staged lock");

    // `cargo metadata` re-resolves the staged workspace and rewrites the lock
    // when it disagrees — the same movement `--locked` builds refuse.
    let output = Command::new(env!("CARGO"))
        .current_dir(staging.path())
        .args(["metadata", "--offline", "--format-version", "1"])
        .output()
        .expect("run cargo metadata");
    assert!(
        output.status.success(),
        "staged workspace does not resolve: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let after = std::fs::read_to_string(&staged_lock).expect("staged lock after resolve");

    if before != after {
        let before_set: std::collections::BTreeSet<&str> = before
            .lines()
            .filter(|line| line.starts_with("name = ") || line.starts_with("version = "))
            .collect();
        let after_set: std::collections::BTreeSet<&str> = after
            .lines()
            .filter(|line| line.starts_with("name = ") || line.starts_with("version = "))
            .collect();
        panic!(
            "the scoped lock is not exact.\nonly in scoped: {:?}\nonly in re-resolve: {:?}",
            before_set.difference(&after_set).collect::<Vec<_>>(),
            after_set.difference(&before_set).collect::<Vec<_>>(),
        );
    }
}
