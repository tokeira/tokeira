//! Publish → load → verify → extract, over the filesystem transport, with
//! the real three-part Compose definition set as the fixture.

use std::path::{Path, PathBuf};

use spike_tuf_platform_definition::{
    consume::{fetch_definition_set, load_repository},
    keys::RoleKeyFiles,
    publish::{PublishOptions, RoleSources, SharedKeySource, publish_set},
    set::{DefinitionSet, SetIdentity, SourceResolver as _, load_set_from_dir},
};
use tough::{FilesystemTransport, key_source::LocalKeySource};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/compose-set")
}

fn compose_set() -> DefinitionSet {
    load_set_from_dir(&fixture_dir(), "deployment.tkd", "tkd").expect("fixture set loads")
}

fn role_sources(files: &RoleKeyFiles) -> RoleSources {
    let source = |path: &PathBuf| SharedKeySource::new(LocalKeySource { path: path.clone() });
    RoleSources {
        root: source(&files.root),
        targets: source(&files.targets),
        snapshot: source(&files.snapshot),
        timestamp: source(&files.timestamp),
    }
}

fn dir_url(path: &Path, sub: &str) -> url::Url {
    url::Url::from_directory_path(path.canonicalize().expect("canonicalize"))
        .expect("dir url")
        .join(sub)
        .expect("join")
}

/// The layout golden vectors, computed independently (python hashlib) so the
/// mirrored identity cannot drift from its own test.
#[test]
fn identity_layout_matches_independent_vectors() {
    let single = SetIdentity::compute_single("tkd", b"x");
    assert_eq!(single.algorithm, "sha256-v1");
    assert_eq!(
        single.digest,
        "bb59fa39fd235403f766d2fe91db753b3033fc3df27b6c20146e41d62ac978a1"
    );

    let set = SetIdentity::compute_set(
        "tkd",
        b"mod a;\n",
        &[("a".to_owned(), std::sync::Arc::from(&b"A"[..]))],
    );
    assert_eq!(set.algorithm, "sha256-set-v1");
    assert_eq!(
        set.digest,
        "be9dadaa31447ee1b988535f5add4a582710914db105ed08ebe6b314060ce8ba"
    );
}

#[test]
fn identity_is_order_sensitive() {
    let a = ("a".to_owned(), std::sync::Arc::from(&b"A"[..]));
    let b = ("b".to_owned(), std::sync::Arc::from(&b"B"[..]));
    let fwd = SetIdentity::compute_set("tkd", b"root", &[a.clone(), b.clone()]);
    let rev = SetIdentity::compute_set("tkd", b"root", &[b, a]);
    assert_ne!(
        fwd.digest, rev.digest,
        "served order participates in identity; TUF alone would not capture it"
    );
}

#[test]
fn fixture_set_declares_parts_in_mod_order() {
    let set = compose_set();
    let names: Vec<&str> = set.parts.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["platform", "observability"]);
}

#[tokio::test]
async fn publish_load_verify_roundtrip() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let keys = RoleKeyFiles::generate(&tmp.path().join("keys")).expect("keygen");
    let sources = role_sources(&keys);
    let set = compose_set();
    let repo_dir = tmp.path().join("repo");

    let published = publish_set(&set, &sources, &repo_dir, &PublishOptions::default())
        .await
        .expect("publish");

    // Consistent-snapshot layout on disk.
    for name in [
        "1.root.json",
        "1.targets.json",
        "1.snapshot.json",
        "timestamp.json",
    ] {
        assert!(
            published.metadata_dir.join(name).is_file(),
            "expected metadata file {name}"
        );
    }
    let target_files: Vec<String> = std::fs::read_dir(&published.targets_dir)
        .expect("targets dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(target_files.len(), 3, "root doc + two parts");
    for name in &target_files {
        let (prefix, rest) = name.split_once('.').expect("digest-prefixed name");
        assert_eq!(prefix.len(), 64, "sha256 prefix on {name}");
        assert!(rest.ends_with(".tkd"), "original name retained in {name}");
    }

    // Load through the TUF client and extract the seed.
    let repo = load_repository(
        &published.trusted_root,
        dir_url(&repo_dir, "metadata/"),
        dir_url(&repo_dir, "targets/"),
        FilesystemTransport,
        None,
    )
    .await
    .expect("load");
    let fetched = fetch_definition_set(&repo).await.expect("fetch");

    assert_eq!(
        fetched.identity,
        set.identity(),
        "identity survives the trip"
    );
    assert_eq!(fetched.claim.identity, fetched.identity);
    assert_eq!(fetched.set.root, set.root);
    assert_eq!(fetched.set.parts.len(), 2);

    // The verified set stands behind the product resolver seam.
    let resolver = fetched.part_sources();
    let platform = resolver.resolve("platform").expect("platform part");
    assert_eq!(&*platform, &*set.parts[0].1);
    let missing = resolver.resolve("nonexistent");
    assert!(missing.is_err(), "unclaimed parts are refused");
}

/// Rotate the online (targets) key: author root v2 with the same root key,
/// republish at repo version 2 into the same object namespace, and load
/// starting from the v1 trust anchor. The client walks 1.root.json →
/// 2.root.json and accepts the new chain — upgrade-in-place on a create-only
/// layout, no client re-pinning.
#[tokio::test]
async fn online_key_rotation_via_root_walk() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let set = compose_set();
    let repo_dir = tmp.path().join("repo");

    let keys_v1 = RoleKeyFiles::generate(&tmp.path().join("keys-v1")).expect("keygen v1");
    let sources_v1 = role_sources(&keys_v1);
    let published_v1 = publish_set(&set, &sources_v1, &repo_dir, &PublishOptions::default())
        .await
        .expect("publish v1");

    // v2: fresh online keys, same root key.
    let keys_v2 = RoleKeyFiles::generate(&tmp.path().join("keys-v2")).expect("keygen v2");
    let mut sources_v2 = role_sources(&keys_v2);
    sources_v2.root = sources_v1.root.clone();

    let repo_v2_dir = tmp.path().join("repo-v2");
    publish_set(
        &set,
        &sources_v2,
        &repo_v2_dir,
        &PublishOptions {
            root_version: 2,
            repo_version: 2,
            ..PublishOptions::default()
        },
    )
    .await
    .expect("publish v2");

    // "Upload" v2 into the same namespace: versioned names coexist, the two
    // mutable heads move forward.
    for entry in std::fs::read_dir(repo_v2_dir.join("metadata")).expect("v2 metadata") {
        let entry = entry.expect("entry");
        std::fs::copy(
            entry.path(),
            repo_dir.join("metadata").join(entry.file_name()),
        )
        .expect("copy metadata");
    }
    for entry in std::fs::read_dir(repo_v2_dir.join("targets")).expect("v2 targets") {
        let entry = entry.expect("entry");
        let dest = repo_dir.join("targets").join(entry.file_name());
        if !dest.exists() {
            std::fs::copy(entry.path(), dest).expect("copy target");
        }
    }

    // Trust anchor is still v1.
    let repo = load_repository(
        &published_v1.trusted_root,
        dir_url(&repo_dir, "metadata/"),
        dir_url(&repo_dir, "targets/"),
        FilesystemTransport,
        None,
    )
    .await
    .expect("load after rotation");
    assert_eq!(repo.root().signed.version.get(), 2, "root walked to v2");
    let fetched = fetch_definition_set(&repo)
        .await
        .expect("fetch after rotation");
    assert_eq!(fetched.identity, set.identity());
}

/// A second publication of identical content at the same version writes
/// byte-identical immutable objects — republish is idempotent, which is what
/// makes the create-only S3 policy workable.
#[tokio::test]
async fn republish_same_version_is_byte_identical_for_targets() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let set = compose_set();
    let keys = RoleKeyFiles::generate(&tmp.path().join("keys")).expect("keygen");
    let sources = role_sources(&keys);

    let a = publish_set(
        &set,
        &sources,
        &tmp.path().join("a"),
        &PublishOptions::default(),
    )
    .await
    .expect("publish a");
    let b = publish_set(
        &set,
        &sources,
        &tmp.path().join("b"),
        &PublishOptions::default(),
    )
    .await
    .expect("publish b");

    // Target objects are digest-named and content-addressed: identical.
    let names = |dir: &Path| -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .expect("dir")
            .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    };
    assert_eq!(names(&a.targets_dir), names(&b.targets_dir));
    for name in names(&a.targets_dir) {
        assert_eq!(
            std::fs::read(a.targets_dir.join(&name)).expect("a bytes"),
            std::fs::read(b.targets_dir.join(&name)).expect("b bytes"),
            "target {name} republished identically"
        );
    }
    // Metadata is *not* byte-identical (fresh expirations/signatures) — the
    // reason only version-named metadata, not content-named metadata, works
    // as the immutable class.
    assert_ne!(
        std::fs::read(a.metadata_dir.join("1.targets.json")).expect("a targets.json"),
        std::fs::read(b.metadata_dir.join("1.targets.json")).expect("b targets.json"),
    );
}
