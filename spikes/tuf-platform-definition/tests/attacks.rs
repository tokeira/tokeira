//! What a compromised or stale repository host can and cannot do to a
//! consumer. These are the properties the hand-rolled evidence envelope in
//! the platform-source-set spec does not provide and TUF does.

use std::path::{Path, PathBuf};

use spike_tuf_platform_definition::{
    consume::{fetch_definition_set, load_repository},
    keys::RoleKeyFiles,
    publish::{PublishOptions, RoleSources, SharedKeySource, publish_set},
    set::{DefinitionSet, load_set_from_dir},
};
use tough::{
    ExpirationEnforcement, FilesystemTransport, RepositoryLoader, key_source::LocalKeySource,
};

fn compose_set() -> DefinitionSet {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/compose-set");
    load_set_from_dir(&dir, "deployment.tkd", "tkd").expect("fixture set loads")
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

/// An expired freshness statement is refused by default — a frozen S3 bucket
/// eventually fails closed instead of serving a stale definition forever.
/// `ExpirationEnforcement::Unsafe` is the documented, deliberate escape
/// hatch (break-glass reads of an abandoned repository).
#[tokio::test]
async fn expired_timestamp_fails_closed() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let keys = RoleKeyFiles::generate(&tmp.path().join("keys")).expect("keygen");
    let sources = role_sources(&keys);
    let set = compose_set();
    let repo_dir = tmp.path().join("repo");

    let published = publish_set(
        &set,
        &sources,
        &repo_dir,
        &PublishOptions {
            // The freshness window is already over when the consumer loads.
            timestamp_lifetime: jiff::Span::new().hours(-1),
            ..PublishOptions::default()
        },
    )
    .await
    .expect("publish");

    let err = load_repository(
        &published.trusted_root,
        dir_url(&repo_dir, "metadata/"),
        dir_url(&repo_dir, "targets/"),
        FilesystemTransport,
        None,
    )
    .await
    .expect_err("expired timestamp must refuse the load");
    assert!(
        format!("{err:#}").to_lowercase().contains("expire"),
        "refusal names the expiration: {err:#}"
    );

    // Break-glass: explicit unsafe expiration handling loads and verifies.
    let repo = RepositoryLoader::new(
        &published.trusted_root,
        dir_url(&repo_dir, "metadata/"),
        dir_url(&repo_dir, "targets/"),
    )
    .transport(FilesystemTransport)
    .expiration_enforcement(ExpirationEnforcement::Unsafe)
    .load()
    .await
    .expect("unsafe-expiration load");
    let fetched = fetch_definition_set(&repo).await.expect("fetch");
    assert_eq!(fetched.identity, set.identity());
}

/// Rewinding the repository to an older (still unexpired, correctly signed)
/// publication is detected by a consumer that keeps a datastore: the trusted
/// timestamp version can never move backwards (TUF §5.4.3.1).
#[tokio::test]
async fn rollback_to_older_publication_is_refused_with_datastore() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let keys = RoleKeyFiles::generate(&tmp.path().join("keys")).expect("keygen");
    let sources = role_sources(&keys);
    let set = compose_set();

    // Two honest publications of the same set, versions 1 and 2.
    let v1_dir = tmp.path().join("v1");
    let v1 = publish_set(&set, &sources, &v1_dir, &PublishOptions::default())
        .await
        .expect("publish v1");
    let v2_dir = tmp.path().join("v2");
    publish_set(
        &set,
        &sources,
        &v2_dir,
        &PublishOptions {
            repo_version: 2,
            ..PublishOptions::default()
        },
    )
    .await
    .expect("publish v2");

    let datastore = tmp.path().join("datastore");
    std::fs::create_dir_all(&datastore).expect("datastore dir");

    // The consumer has seen v2…
    load_repository(
        &v1.trusted_root,
        dir_url(&v2_dir, "metadata/"),
        dir_url(&v2_dir, "targets/"),
        FilesystemTransport,
        Some(datastore.clone()),
    )
    .await
    .expect("load v2");

    // …so a host serving v1 again is a rollback, not a valid state.
    let err = load_repository(
        &v1.trusted_root,
        dir_url(&v1_dir, "metadata/"),
        dir_url(&v1_dir, "targets/"),
        FilesystemTransport,
        Some(datastore.clone()),
    )
    .await
    .expect_err("older timestamp version must be refused");
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("rollback") || msg.contains("version"),
        "refusal names the rollback: {err:#}"
    );

    // Without the datastore the same rewind loads — persistence is what buys
    // cross-session rollback protection. Recorded as a finding: the
    // deployment dir must own this directory.
    load_repository(
        &v1.trusted_root,
        dir_url(&v1_dir, "metadata/"),
        dir_url(&v1_dir, "targets/"),
        FilesystemTransport,
        None,
    )
    .await
    .expect("fresh consumer accepts the older-but-valid publication");
}

/// Metadata tampering: any bit-flip in signed metadata kills the load.
#[tokio::test]
async fn tampered_targets_metadata_is_refused() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let keys = RoleKeyFiles::generate(&tmp.path().join("keys")).expect("keygen");
    let sources = role_sources(&keys);
    let set = compose_set();
    let repo_dir = tmp.path().join("repo");
    let published = publish_set(&set, &sources, &repo_dir, &PublishOptions::default())
        .await
        .expect("publish");

    // Rewrite the set claim inside targets metadata without re-signing:
    // claim a different part order (which would change evaluation).
    let path = published.metadata_dir.join("1.targets.json");
    let mut doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("read targets.json"))
            .expect("parse targets.json");
    let parts = &mut doc["signed"]["targets"]["deployment.tkd"]["custom"]["tokeira:definition-set"]
        ["parts"];
    assert_eq!(
        *parts,
        serde_json::json!(["platform", "observability"]),
        "claim found in metadata"
    );
    *parts = serde_json::json!(["observability", "platform"]);
    std::fs::write(&path, serde_json::to_vec(&doc).expect("serialize")).expect("write tampered");

    let err = load_repository(
        &published.trusted_root,
        dir_url(&repo_dir, "metadata/"),
        dir_url(&repo_dir, "targets/"),
        FilesystemTransport,
        None,
    )
    .await
    .expect_err("tampered metadata must fail signature/hash checks");
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("hash") || msg.contains("signature") || msg.contains("verif"),
        "refusal names the integrity failure: {err:#}"
    );
}

/// Target tampering: replacing a part's bytes (keeping its name) is caught
/// at read time against the signed hash — the consumer never sees the bytes.
#[tokio::test]
async fn tampered_target_bytes_are_refused() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let keys = RoleKeyFiles::generate(&tmp.path().join("keys")).expect("keygen");
    let sources = role_sources(&keys);
    let set = compose_set();
    let repo_dir = tmp.path().join("repo");
    let published = publish_set(&set, &sources, &repo_dir, &PublishOptions::default())
        .await
        .expect("publish");

    // Corrupt every stored target object (digest-named, so we corrupt all
    // three rather than resolving which is which).
    for entry in std::fs::read_dir(&published.targets_dir).expect("targets") {
        let entry = entry.expect("entry");
        let mut bytes = std::fs::read(entry.path()).expect("read");
        let last = bytes.last_mut().expect("non-empty");
        *last ^= 0x01;
        std::fs::write(entry.path(), bytes).expect("write");
    }

    let repo = load_repository(
        &published.trusted_root,
        dir_url(&repo_dir, "metadata/"),
        dir_url(&repo_dir, "targets/"),
        FilesystemTransport,
        None,
    )
    .await
    .expect("metadata still loads — targets are checked on read");
    let err = fetch_definition_set(&repo)
        .await
        .expect_err("tampered target bytes must be refused");
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("hash") || msg.contains("digest") || msg.contains("verif"),
        "refusal names the integrity failure: {err:#}"
    );
}
