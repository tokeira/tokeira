//! Source-snapshot behavior (task 17): content-addressed determinism, the
//! staged+unstaged capture, the untracked-`.rs` policy, closure scoping, and
//! the leave-no-trace invariant. Repos are arranged with the real `git`
//! binary — snapshots must hold against repositories exactly as git creates
//! them — and snapshot objects are read back with `git cat-file`, proving the
//! written objects are ordinary, tooling-visible git objects.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

use tokeira_build::{SnapshotError, SnapshotRequest, snapshot_source_closure};

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(dir)
        // Fixtures must not inherit host git config: a global
        // `core.autocrlf` or an `init.templateDir` with hooks would perturb
        // the repositories these invariants are proven against.
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@t.invalid")
        .env("GIT_AUTHOR_DATE", "2026-01-01T00:00:00Z")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@t.invalid")
        .env("GIT_COMMITTER_DATE", "2026-01-01T00:00:00Z")
        .args(args)
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// A repo with one committed crate: `crates/alpha/{Cargo.toml, src/lib.rs}`.
fn seed_repo(dir: &Path) {
    git(dir, &["init", "-q", "-b", "main"]);
    std::fs::create_dir_all(dir.join("crates/alpha/src")).expect("mkdir");
    std::fs::write(
        dir.join("crates/alpha/Cargo.toml"),
        "[package]\nname = \"alpha\"\n",
    )
    .expect("write");
    std::fs::write(dir.join("crates/alpha/src/lib.rs"), "pub fn a() {}\n").expect("write");
    git(dir, &["add", "."]);
    git(dir, &["commit", "-q", "-m", "seed"]);
}

fn request(dir: &Path) -> SnapshotRequest {
    SnapshotRequest {
        repo_root: dir.to_path_buf(),
        closure_paths: vec![PathBuf::from("crates/alpha")],
        include_untracked: false,
    }
}

#[test]
fn snapshot_is_deterministic_and_survives_rewrites() {
    let tmp = tempfile::tempdir().expect("tempdir");
    seed_repo(tmp.path());

    let first = snapshot_source_closure(&request(tmp.path())).expect("snapshot");
    // Rewrite a file with identical bytes (new mtime): content-addressing
    // must not care.
    std::fs::write(
        tmp.path().join("crates/alpha/src/lib.rs"),
        "pub fn a() {}\n",
    )
    .expect("rewrite");
    let second = snapshot_source_closure(&request(tmp.path())).expect("snapshot");

    assert_eq!(first.tree_oid, second.tree_oid, "same content, same tree");
    assert_eq!(
        first.commit_oid, second.commit_oid,
        "same (tree, parent), same audit commit — fixed identity + timestamps"
    );
    assert_eq!(first.file_count, 2);
}

#[test]
fn unstaged_edits_are_captured_from_the_worktree() {
    // The build compiles the worktree, so the snapshot must freeze worktree
    // bytes — staged or not.
    let tmp = tempfile::tempdir().expect("tempdir");
    seed_repo(tmp.path());
    std::fs::write(
        tmp.path().join("crates/alpha/src/lib.rs"),
        "pub fn a_edited() {}\n",
    )
    .expect("edit");

    let snapshot = snapshot_source_closure(&request(tmp.path())).expect("snapshot");

    let head_tree = git(tmp.path(), &["rev-parse", "HEAD^{tree}"]);
    assert_ne!(snapshot.tree_oid, head_tree, "the edit re-keys the tree");
    let frozen = git(
        tmp.path(),
        &[
            "cat-file",
            "-p",
            &format!("{}:crates/alpha/src/lib.rs", snapshot.tree_oid),
        ],
    );
    assert_eq!(
        frozen, "pub fn a_edited() {}",
        "worktree bytes are the truth"
    );
}

#[test]
fn untracked_rs_refuses_by_default_and_opts_in_explicitly() {
    let tmp = tempfile::tempdir().expect("tempdir");
    seed_repo(tmp.path());
    std::fs::write(
        tmp.path().join("crates/alpha/src/new_mod.rs"),
        "pub fn n() {}\n",
    )
    .expect("write untracked");

    // Refuse-by-default, with the offending files listed (decision 9).
    let err = snapshot_source_closure(&request(tmp.path())).expect_err("refuses");
    match err {
        SnapshotError::UntrackedSources { files } => {
            assert_eq!(files, vec!["crates/alpha/src/new_mod.rs".to_string()]);
        }
        other => panic!("expected UntrackedSources, got {other}"),
    }

    // Explicit opt-in stages it.
    let snapshot = snapshot_source_closure(&SnapshotRequest {
        include_untracked: true,
        ..request(tmp.path())
    })
    .expect("opt-in snapshots");
    let listing = git(
        tmp.path(),
        &["ls-tree", "-r", "--name-only", &snapshot.tree_oid],
    );
    assert!(listing.contains("crates/alpha/src/new_mod.rs"));
}

#[test]
fn untracked_non_source_files_are_outside_the_contract() {
    let tmp = tempfile::tempdir().expect("tempdir");
    seed_repo(tmp.path());
    std::fs::write(tmp.path().join("crates/alpha/scratch.txt"), "notes\n").expect("write");

    let snapshot = snapshot_source_closure(&request(tmp.path())).expect("no refusal");
    let listing = git(
        tmp.path(),
        &["ls-tree", "-r", "--name-only", &snapshot.tree_oid],
    );
    assert!(
        !listing.contains("scratch.txt"),
        "untracked non-.rs is neither refused nor swept in"
    );
}

#[test]
fn closure_paths_scope_the_tree() {
    let tmp = tempfile::tempdir().expect("tempdir");
    seed_repo(tmp.path());
    std::fs::create_dir_all(tmp.path().join("crates/beta/src")).expect("mkdir");
    std::fs::write(tmp.path().join("crates/beta/src/lib.rs"), "pub fn b() {}\n").expect("write");
    git(tmp.path(), &["add", "."]);
    git(tmp.path(), &["commit", "-q", "-m", "beta"]);

    let snapshot = snapshot_source_closure(&request(tmp.path())).expect("snapshot");
    let listing = git(
        tmp.path(),
        &["ls-tree", "-r", "--name-only", &snapshot.tree_oid],
    );
    assert!(listing.contains("crates/alpha/src/lib.rs"));
    assert!(
        !listing.contains("crates/beta"),
        "tracked-but-outside-closure stays out"
    );
}

#[test]
fn snapshot_touches_neither_worktree_nor_index_nor_refs() {
    let tmp = tempfile::tempdir().expect("tempdir");
    seed_repo(tmp.path());
    // A dirty worktree makes the invariant observable.
    std::fs::write(
        tmp.path().join("crates/alpha/src/lib.rs"),
        "pub fn dirty() {}\n",
    )
    .expect("edit");

    let status_before = git(tmp.path(), &["status", "--porcelain"]);
    let head_before = git(tmp.path(), &["rev-parse", "HEAD"]);
    let refs_before = git(tmp.path(), &["for-each-ref"]);

    snapshot_source_closure(&request(tmp.path())).expect("snapshot");

    assert_eq!(git(tmp.path(), &["status", "--porcelain"]), status_before);
    assert_eq!(git(tmp.path(), &["rev-parse", "HEAD"]), head_before);
    assert_eq!(
        git(tmp.path(), &["for-each-ref"]),
        refs_before,
        "no ref is written — record-oid-only (decision 10)"
    );
}

#[test]
fn audit_commit_parents_on_head_and_is_parentless_when_unborn() {
    // Born HEAD → the audit commit records it as parent.
    let tmp = tempfile::tempdir().expect("tempdir");
    seed_repo(tmp.path());
    let snapshot = snapshot_source_closure(&request(tmp.path())).expect("snapshot");
    let head = git(tmp.path(), &["rev-parse", "HEAD"]);
    let commit = git(tmp.path(), &["cat-file", "-p", &snapshot.commit_oid]);
    assert!(
        commit.contains(&format!("parent {head}")),
        "audit commit parents the current HEAD: {commit}"
    );
    assert!(
        commit.contains(&format!("tree {}", snapshot.tree_oid)),
        "audit commit wraps the snapshot tree"
    );

    // Unborn HEAD (staged files, no commit) → parentless fallback.
    let unborn = tempfile::tempdir().expect("tempdir");
    git(unborn.path(), &["init", "-q", "-b", "main"]);
    std::fs::create_dir_all(unborn.path().join("crates/alpha/src")).expect("mkdir");
    std::fs::write(
        unborn.path().join("crates/alpha/src/lib.rs"),
        "pub fn a() {}\n",
    )
    .expect("write");
    git(unborn.path(), &["add", "."]);
    let snapshot = snapshot_source_closure(&request(unborn.path())).expect("snapshot");
    let commit = git(unborn.path(), &["cat-file", "-p", &snapshot.commit_oid]);
    assert!(
        !commit.contains("parent "),
        "unborn HEAD yields a parentless audit commit: {commit}"
    );
}

#[test]
fn an_empty_closure_is_refused() {
    let tmp = tempfile::tempdir().expect("tempdir");
    seed_repo(tmp.path());
    let err = snapshot_source_closure(&SnapshotRequest {
        closure_paths: vec![PathBuf::from("crates/absent")],
        ..request(tmp.path())
    })
    .expect_err("nothing to snapshot");
    assert!(matches!(err, SnapshotError::EmptyClosure));
}

// The freeze → materialize round trip (task 18.1's build input): the
// materialized tree carries the frozen worktree bytes — including an
// unstaged edit — and the executable bit, and reads only the object
// database (a post-snapshot worktree edit must not leak in).
#[test]
fn materialized_snapshot_reproduces_the_frozen_closure() {
    use tokeira_build::materialize_snapshot;

    let tmp = tempfile::tempdir().expect("tempdir");
    seed_repo(tmp.path());
    // An executable script, committed with its mode.
    let script = tmp.path().join("crates/alpha/build.sh");
    std::fs::write(&script, "#!/bin/sh\n").expect("write");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    }
    git(tmp.path(), &["add", "."]);
    git(tmp.path(), &["commit", "-q", "-m", "script"]);
    // An unstaged edit — the frozen truth.
    std::fs::write(
        tmp.path().join("crates/alpha/src/lib.rs"),
        "pub fn frozen() {}\n",
    )
    .expect("edit");

    let snapshot = snapshot_source_closure(&request(tmp.path())).expect("snapshot");
    // Mutate the worktree AFTER the freeze: materialization must not see it.
    std::fs::write(
        tmp.path().join("crates/alpha/src/lib.rs"),
        "pub fn after_the_freeze() {}\n",
    )
    .expect("post-freeze edit");

    let dest = tempfile::tempdir().expect("tempdir");
    let count =
        materialize_snapshot(tmp.path(), &snapshot.tree_oid, dest.path()).expect("materialize");
    assert_eq!(count, snapshot.file_count, "every frozen file materializes");

    let lib = std::fs::read_to_string(dest.path().join("crates/alpha/src/lib.rs"))
        .expect("materialized file");
    assert_eq!(
        lib, "pub fn frozen() {}\n",
        "the frozen bytes, not the post-freeze worktree"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(dest.path().join("crates/alpha/build.sh"))
            .expect("script metadata")
            .permissions()
            .mode();
        assert_ne!(mode & 0o111, 0, "the executable bit survives");
    }
}
