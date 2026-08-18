//! Source snapshotting.
//!
//! Freezes the provisioner source closure into an **immutable,
//! content-addressed git tree** — the in-process equivalent of a
//! temporary-index `write-tree`. `EngineIdentity` is a function of source, so
//! "source" must be a frozen snapshot, never a live tree: with several agents
//! mutating worktrees concurrently, deriving identity from the live tree could
//! record an identity that disagrees with the bytes built.
//!
//! **Invariant: snapshot → derive → build, atomic w.r.t. source.** The
//! snapshot captures the worktree content of every *tracked* file under the
//! closure paths — staged and unstaged alike, since the worktree bytes are
//! what a native build would compile — and writes blobs and trees straight
//! into the repository's object database. The working tree, the real index,
//! and every ref (including `refs/stash`) are left untouched; the new objects
//! are reachable from nothing until the caller records their ids.
//!
//! Untracked `.rs` files under the closure are a real build input the
//! snapshot would silently omit, so they **refuse by default, listed**;
//! [`SnapshotRequest::include_untracked`] opts them in
//! explicitly. Untracked files of other kinds are outside the closure
//! contract and are ignored. (The scan is a plain filesystem walk: a
//! `.gitignore`d `.rs` file inside a closure path would be reported as
//! untracked — acceptable, since ignoring source that would be compiled is
//! itself a smell; none exist in this workspace.)
//!
//! [`SnapshotRequest::content_overrides`] freezes caller-supplied bytes for
//! named tracked paths in place of their worktree bytes. This exists for
//! exactly one purpose: the workspace `Cargo.toml`/`Cargo.lock` are frozen in
//! their **closure-scoped** form (members and lock entries reduced to what
//! the snapshot actually contains), so the frozen tree is a complete, valid
//! cargo workspace rather than a torn copy of the full one. The override
//! bytes are derived deterministically from admitted inputs by the caller;
//! an override that names a path the freeze never visits is refused — it
//! would mean the scoped file silently failed to enter the identity.
//!
//! The tree is wrapped in a **deterministic audit commit**: fixed synthetic
//! identity, fixed timestamps, message derived from the tree — so the same
//! `(tree, parent)` always yields the same commit oid. The commit is a
//! reachable *handle* for humans (`git show <oid>`), not the identity key:
//! identity keys on the **tree** oid (pure content; a re-snapshot under a new
//! HEAD re-keys the commit but never the tree). By default only the oids are
//! recorded — no ref is written; pinning
//! `refs/tokeira/snapshots/<engine-identity>` is `TrustedCi` policy.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use gix::{
    bstr::{BStr, BString, ByteSlice},
    object::tree::EntryKind,
};

/// What to snapshot: the repository, the closure paths within it, and the
/// untracked-source policy.
#[derive(Debug, Clone)]
pub struct SnapshotRequest {
    /// Any directory inside the repository whose worktree is snapshotted
    /// (discovery walks upward from here). Each agent worktree snapshots its
    /// own state — worktree isolation plus create-time snapshotting give
    /// per-agent fidelity.
    pub repo_root: PathBuf,
    /// Repository-relative files and directories forming the source closure
    /// (from [`resolve_source_closure`](crate::resolve_source_closure)).
    pub closure_paths: Vec<PathBuf>,
    /// Stage untracked `.rs` files found under the closure instead of
    /// refusing (inclusion is an explicit operator act).
    pub include_untracked: bool,
    /// Bytes to freeze for a repo-relative tracked path instead of its
    /// worktree bytes (keys use `/` separators). Every override must be
    /// consumed by the freeze, or the snapshot refuses.
    pub content_overrides: BTreeMap<String, Vec<u8>>,
}

/// The frozen closure: content-addressed ids, recorded — not ref-pinned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSnapshot {
    /// The snapshot **tree** oid — the pure-content key `EngineIdentity`'s
    /// source digest derives from. Hex.
    pub tree_oid: String,
    /// The deterministic audit commit wrapping the tree. Hex. A handle for
    /// inspection, never an identity input.
    pub commit_oid: String,
    /// Number of files frozen into the tree.
    pub file_count: usize,
}

/// Failure to freeze the closure.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// Untracked `.rs` files inside the closure would be a silently-omitted
    /// build input — refused unless explicitly included.
    #[error(
        "untracked .rs files in the source closure (stage them, or opt in with \
         --include-untracked): {}",
        files.join(", ")
    )]
    UntrackedSources { files: Vec<String> },
    /// The closure produced no files — an empty snapshot is always a broken
    /// request (wrong roots, wrong repo), never a valid identity input.
    #[error("the source closure matched no tracked files — nothing to snapshot")]
    EmptyClosure,
    /// A content override named a path the freeze never visited: the scoped
    /// bytes would silently miss the identity, so the request is broken.
    #[error(
        "content overrides for paths outside the frozen closure: {}",
        paths.join(", ")
    )]
    UnusedOverride { paths: Vec<String> },
    #[error("repository discovery failed: {0}")]
    Discover(#[from] Box<gix::discover::Error>),
    #[error("the repository has no working tree (bare) — nothing to snapshot")]
    Bare,
    #[error("failed to read the repository index: {0}")]
    Index(#[from] Box<gix::worktree::open_index::Error>),
    #[error("failed to read {path}: {source}")]
    ReadFile {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to walk {path}: {source}")]
    Walk {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to write a git object: {0}")]
    WriteObject(String),
    #[error("invalid snapshot oid '{0}'")]
    InvalidOid(String),
    #[error("failed to read a snapshot object: {0}")]
    ReadObject(String),
    #[error("failed to materialize {path}: {source}")]
    WriteFile {
        path: String,
        source: std::io::Error,
    },
}

/// Freeze the worktree content of the closure into a content-addressed tree +
/// deterministic audit commit. Touches neither the working tree, the real
/// index, nor any ref.
pub fn snapshot_source_closure(request: &SnapshotRequest) -> Result<SourceSnapshot, SnapshotError> {
    let repo = gix::discover(&request.repo_root).map_err(Box::new)?;
    let workdir = repo.workdir().ok_or(SnapshotError::Bare)?.to_owned();
    let index = repo.index_or_empty().map_err(Box::new)?;

    // Every path the real index tracks, repo-relative with '/' separators.
    let tracked: BTreeSet<BString> = index
        .entries()
        .iter()
        .map(|entry| entry.path(&index).to_owned())
        .collect();

    // The file set to freeze: tracked files under the closure paths…
    let mut to_freeze: BTreeSet<BString> = tracked
        .iter()
        .filter(|path| under_closure(path.as_bstr(), &request.closure_paths))
        .cloned()
        .collect();

    // …plus the untracked-source policy: scan the closure directories for
    // files the index does not know. Untracked `.rs` refuses by default.
    let mut untracked_sources = Vec::new();
    for closure_path in &request.closure_paths {
        let absolute = workdir.join(closure_path);
        if absolute.is_dir() {
            walk_untracked(&workdir, &absolute, &tracked, &mut untracked_sources)?;
        }
    }
    untracked_sources.sort();
    if !untracked_sources.is_empty() {
        if request.include_untracked {
            to_freeze.extend(untracked_sources.iter().map(|p| BString::from(p.as_str())));
        } else {
            return Err(SnapshotError::UntrackedSources {
                files: untracked_sources,
            });
        }
    }

    // Freeze: write each file's worktree bytes as a blob, then build the tree
    // bottom-up through gix's editor (which owns git's tree-entry ordering
    // rules and writes the subtrees to the odb).
    let empty_tree = gix::ObjectId::empty_tree(repo.object_hash());
    let mut editor = repo
        .edit_tree(empty_tree)
        .map_err(|e| SnapshotError::WriteObject(e.to_string()))?;
    let mut file_count = 0usize;
    let mut unused_overrides: BTreeSet<&str> = request
        .content_overrides
        .keys()
        .map(String::as_str)
        .collect();
    for path in &to_freeze {
        // `&str` spelled out: `typed_path` (via `tough`) adds a blanket
        // `AsRef<Utf8Path<_>> for Cow<'_, str>` that makes a bare `as_ref()`
        // ambiguous.
        let relative: &str = &path.to_str_lossy();
        if let Some(bytes) = request.content_overrides.get(relative) {
            unused_overrides.remove(relative);
            let blob = repo
                .write_blob(bytes)
                .map_err(|e| SnapshotError::WriteObject(e.to_string()))?;
            editor
                .upsert(path.as_bstr(), EntryKind::Blob, blob.detach())
                .map_err(|e| SnapshotError::WriteObject(e.to_string()))?;
            file_count += 1;
            continue;
        }
        let absolute = workdir.join(relative);
        // Tracked-but-deleted in the worktree: the worktree is the truth the
        // build would see, so an unstaged deletion is simply absent.
        let Ok(metadata) = std::fs::symlink_metadata(&absolute) else {
            continue;
        };
        let (kind, bytes) = if metadata.is_symlink() {
            let target =
                std::fs::read_link(&absolute).map_err(|source| SnapshotError::ReadFile {
                    path: path.to_string(),
                    source,
                })?;
            (
                EntryKind::Link,
                target.as_os_str().as_encoded_bytes().to_vec(),
            )
        } else if metadata.is_dir() {
            // An index entry can never be a directory; defensive skip.
            continue;
        } else {
            #[cfg(unix)]
            let executable = {
                use std::os::unix::fs::PermissionsExt;
                metadata.permissions().mode() & 0o111 != 0
            };
            #[cfg(not(unix))]
            let executable = false;
            let bytes = std::fs::read(&absolute).map_err(|source| SnapshotError::ReadFile {
                path: path.to_string(),
                source,
            })?;
            (
                if executable {
                    EntryKind::BlobExecutable
                } else {
                    EntryKind::Blob
                },
                bytes,
            )
        };
        let blob = repo
            .write_blob(&bytes)
            .map_err(|e| SnapshotError::WriteObject(e.to_string()))?;
        editor
            .upsert(path.as_bstr(), kind, blob.detach())
            .map_err(|e| SnapshotError::WriteObject(e.to_string()))?;
        file_count += 1;
    }
    if file_count == 0 {
        return Err(SnapshotError::EmptyClosure);
    }
    if !unused_overrides.is_empty() {
        return Err(SnapshotError::UnusedOverride {
            paths: unused_overrides.iter().map(|p| (*p).to_string()).collect(),
        });
    }
    let tree_oid = editor
        .write()
        .map_err(|e| SnapshotError::WriteObject(e.to_string()))?
        .detach();

    let commit_oid = write_audit_commit(&repo, tree_oid)?;

    Ok(SourceSnapshot {
        tree_oid: tree_oid.to_string(),
        commit_oid: commit_oid.to_string(),
        file_count,
    })
}

/// Wrap `tree` in the deterministic audit commit: fixed synthetic
/// identity and timestamps, message derived from the tree, parent = the
/// current `HEAD` commit (parentless on an unborn `HEAD`). Same `(tree,
/// parent)` → same commit oid; the committer is supplied here, never read
/// from git config. No ref is written.
fn write_audit_commit(
    repo: &gix::Repository,
    tree: gix::ObjectId,
) -> Result<gix::ObjectId, SnapshotError> {
    let signature = gix::actor::Signature {
        name: "tokeira snapshot".into(),
        email: "snapshot@tokeira.invalid".into(),
        time: gix::date::Time::new(0, 0),
    };
    let parents: Vec<gix::ObjectId> = repo
        .head_id()
        .ok()
        .map(|id| id.detach())
        .into_iter()
        .collect();
    let commit = gix::objs::Commit {
        tree,
        parents: parents.into(),
        author: signature.clone(),
        committer: signature,
        encoding: None,
        message: format!("tokeira source snapshot\n\ntree {tree}\n").into(),
        extra_headers: Vec::new(),
    };
    Ok(repo
        .write_object(&commit)
        .map_err(|e| SnapshotError::WriteObject(e.to_string()))?
        .detach())
}

/// Materialize a snapshot **tree** into `dest` — the file set a hermetic
/// build consumes ("feed the snapshot to the build, never the
/// live tree"). Reads only the object database; the working tree and index
/// play no part, so a worktree edit after the snapshot cannot leak into the
/// build. Returns the number of files written.
///
/// Submodule (commit) entries are impossible here — the snapshot writer only
/// freezes blobs and links — and are refused as corruption if encountered.
pub fn materialize_snapshot(
    repo_root: &Path,
    tree_oid_hex: &str,
    dest: &Path,
) -> Result<usize, SnapshotError> {
    let repo = gix::discover(repo_root).map_err(Box::new)?;
    let tree = gix::ObjectId::from_hex(tree_oid_hex.as_bytes())
        .map_err(|e| SnapshotError::InvalidOid(format!("{tree_oid_hex}: {e}")))?;
    std::fs::create_dir_all(dest).map_err(|source| SnapshotError::WriteFile {
        path: dest.display().to_string(),
        source,
    })?;
    let mut file_count = 0usize;
    materialize_tree(&repo, tree, dest, &mut file_count)?;
    Ok(file_count)
}

fn materialize_tree(
    repo: &gix::Repository,
    tree: gix::ObjectId,
    dest: &Path,
    file_count: &mut usize,
) -> Result<(), SnapshotError> {
    let read = |oid: gix::ObjectId| -> Result<Vec<u8>, SnapshotError> {
        Ok(repo
            .find_object(oid)
            .map_err(|e| SnapshotError::ReadObject(e.to_string()))?
            .detach()
            .data)
    };
    let tree = repo
        .find_object(tree)
        .map_err(|e| SnapshotError::ReadObject(e.to_string()))?
        .try_into_tree()
        .map_err(|e| SnapshotError::ReadObject(e.to_string()))?;
    // Decode the tree before iterating: entries borrow the tree's data.
    let entries: Vec<(gix::object::tree::EntryKind, String, gix::ObjectId)> = tree
        .iter()
        .map(|entry| {
            let entry = entry.map_err(|e| SnapshotError::ReadObject(e.to_string()))?;
            Ok((
                entry.mode().kind(),
                entry.filename().to_str_lossy().into_owned(),
                entry.oid().to_owned(),
            ))
        })
        .collect::<Result<_, SnapshotError>>()?;
    for (kind, name, oid) in entries {
        let path = dest.join(&name);
        let write_err = |source: std::io::Error| SnapshotError::WriteFile {
            path: path.display().to_string(),
            source,
        };
        match kind {
            gix::object::tree::EntryKind::Tree => {
                std::fs::create_dir_all(&path).map_err(write_err)?;
                materialize_tree(repo, oid, &path, file_count)?;
            }
            gix::object::tree::EntryKind::Blob | gix::object::tree::EntryKind::BlobExecutable => {
                let bytes = read(oid)?;
                // Skip identical bytes so a re-materialization into a
                // persistent staging directory preserves mtimes — cargo's
                // fingerprints stay valid and incremental builds survive.
                let unchanged = std::fs::read(&path).is_ok_and(|current| current == bytes);
                if !unchanged {
                    std::fs::write(&path, bytes).map_err(write_err)?;
                }
                #[cfg(unix)]
                if kind == gix::object::tree::EntryKind::BlobExecutable {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                        .map_err(write_err)?;
                }
                *file_count += 1;
            }
            gix::object::tree::EntryKind::Link => {
                let target = read(oid)?;
                let target = std::path::Path::new(std::str::from_utf8(&target).map_err(|e| {
                    SnapshotError::ReadObject(format!("non-utf8 symlink target: {e}"))
                })?)
                .to_path_buf();
                let unchanged = std::fs::read_link(&path).is_ok_and(|current| current == target);
                if !unchanged {
                    let _ = std::fs::remove_file(&path);
                    #[cfg(unix)]
                    std::os::unix::fs::symlink(&target, &path).map_err(write_err)?;
                    #[cfg(not(unix))]
                    std::fs::write(&path, target.as_os_str().as_encoded_bytes())
                        .map_err(write_err)?;
                }
                *file_count += 1;
            }
            gix::object::tree::EntryKind::Commit => {
                return Err(SnapshotError::ReadObject(format!(
                    "unexpected submodule entry '{name}' — snapshots freeze only blobs and links"
                )));
            }
        }
    }
    Ok(())
}

/// Whether repo-relative `path` falls under any closure path (a file match or
/// a directory prefix on a component boundary).
fn under_closure(path: &BStr, closure_paths: &[PathBuf]) -> bool {
    closure_paths.iter().any(|closure_path| {
        let prefix = closure_path.to_string_lossy();
        let prefix = prefix.trim_end_matches('/');
        let bytes = path.as_bytes();
        bytes == prefix.as_bytes()
            || (bytes.len() > prefix.len()
                && bytes.starts_with(prefix.as_bytes())
                && bytes[prefix.len()] == b'/')
    })
}

/// Recursively collect untracked `.rs` files under `dir` (absolute), as
/// repo-relative '/'-separated strings. Skips `.git`.
fn walk_untracked(
    workdir: &Path,
    dir: &Path,
    tracked: &BTreeSet<BString>,
    untracked_sources: &mut Vec<String>,
) -> Result<(), SnapshotError> {
    let entries = std::fs::read_dir(dir).map_err(|source| SnapshotError::Walk {
        path: dir.display().to_string(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| SnapshotError::Walk {
            path: dir.display().to_string(),
            source,
        })?;
        let path = entry.path();
        let name = entry.file_name();
        if name == ".git" {
            continue;
        }
        let file_type = entry.file_type().map_err(|source| SnapshotError::Walk {
            path: path.display().to_string(),
            source,
        })?;
        if file_type.is_dir() {
            walk_untracked(workdir, &path, tracked, untracked_sources)?;
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let Ok(relative) = path.strip_prefix(workdir) else {
                continue;
            };
            let relative = relative.to_string_lossy().replace('\\', "/");
            if !tracked.contains(&BString::from(relative.as_str())) {
                untracked_sources.push(relative);
            }
        }
    }
    Ok(())
}
